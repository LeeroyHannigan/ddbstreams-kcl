//! DynamoDB Streams `StreamSource` support for `ddbstreams-kcl-core`.
//!
//! This module contains the correctness-critical, network-free logic:
//! turning `DescribeStream` results into the shard lineage the core engine
//! consumes. The async `aws-sdk-dynamodbstreams` adapter (the trivial glue that
//! fetches pages / calls `GetRecords`) lands next behind an optional `aws`
//! feature so this logic always builds and tests offline.
//!
//! Grounded in `DynamoDBStreamsShardDetector` / `DynamoDBStreamsShardSyncer`
//! (awslabs/dynamodb-streams-kinesis-adapter, Apache-2.0). See core/REFERENCES.md.

use ddbstreams_kcl_core::{ShardId, ShardMeta};
use std::collections::HashSet;

/// A shard as returned by DynamoDB Streams `DescribeStream`.
///
/// NOTE: the DynamoDB Streams `Shard` shape exposes a SINGLE `ParentShardId`
/// (unlike Kinesis, which adds `AdjacentParentShardId` for merge children). So
/// a DDB Streams shard graph is effectively a tree: 0 or 1 parent per shard.
/// We still map into `ShardMeta.parents: Vec<_>` for generality.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DdbShard {
    pub shard_id: ShardId,
    pub parent_shard_id: Option<ShardId>,
    /// `SequenceNumberRange.EndingSequenceNumber`. `None` = the shard is still
    /// open (accepting records). A closed shard has reached `SHARD_END`.
    pub ending_sequence_number: Option<String>,
}

impl DdbShard {
    /// Open shard (no ending sequence number yet).
    pub fn new(shard_id: impl Into<ShardId>, parent: Option<&str>) -> Self {
        Self {
            shard_id: shard_id.into(),
            parent_shard_id: parent.map(|p| p.to_string()),
            ending_sequence_number: None,
        }
    }

    /// Closed shard (has an ending sequence number → reached SHARD_END).
    pub fn closed(shard_id: impl Into<ShardId>, parent: Option<&str>, ending_seq: impl Into<String>) -> Self {
        Self {
            shard_id: shard_id.into(),
            parent_shard_id: parent.map(|p| p.to_string()),
            ending_sequence_number: Some(ending_seq.into()),
        }
    }

    pub fn is_open(&self) -> bool {
        self.ending_sequence_number.is_none()
    }
}

/// DynamoDB Streams `DescribeStream` `ShardFilter`.
///
/// Unlike Kinesis (`AT_TRIM_HORIZON` / `AT_LATEST` / `AT_TIMESTAMP`), DynamoDB
/// Streams supports the **`CHILD_SHARDS`** filter type: given a (read-only)
/// parent shard id, `DescribeStream` returns that shard's child shards directly,
/// avoiding a full paginated re-scan during incremental shard sync.
///
/// Grounded in `DynamoDBStreamsShardDetector.listShardsWithFilter` /
/// `AmazonDynamoDBStreamsAdapterClient.describeStreamWithFilter` (Apache-2.0).
/// The adapter falls back to full paginated `DescribeStream` if the filtered
/// call errors — callers here should do the same (see `merge_child_shards`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ShardFilterType {
    ChildShards,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardFilter {
    pub filter_type: ShardFilterType,
    /// The parent shard whose children we want. Required for `CHILD_SHARDS`.
    pub shard_id: Option<ShardId>,
}

impl ShardFilter {
    /// Build the `CHILD_SHARDS` filter targeting `parent` — the newer,
    /// scan-avoiding path for discovering a completed parent's children.
    pub fn child_shards(parent: impl Into<ShardId>) -> Self {
        Self {
            filter_type: ShardFilterType::ChildShards,
            shard_id: Some(parent.into()),
        }
    }

    /// Wire value for the DynamoDB `ShardFilterType`.
    pub fn type_as_string(&self) -> &'static str {
        match self.filter_type {
            ShardFilterType::ChildShards => "CHILD_SHARDS",
        }
    }
}

/// Build the shard graph from full (paginated) `DescribeStream` pages.
///
/// `DescribeStream` is paginated (via `LastEvaluatedShardId`); a shard may
/// appear across pages, so we de-duplicate by `shard_id` preserving first sight.
/// A parent id NOT present in the pages means the parent has been trimmed/aged
/// out (~24h retention); it is dropped from the child's `parents` so the child
/// is not blocked forever — mirroring KCL treating an absent parent lease as
/// already-complete.
pub fn build_shard_graph<I>(pages: I) -> Vec<ShardMeta>
where
    I: IntoIterator<Item = Vec<DdbShard>>,
{
    let mut seen: HashSet<ShardId> = HashSet::new();
    let mut shards: Vec<DdbShard> = Vec::new();
    for page in pages {
        for shard in page {
            if seen.insert(shard.shard_id.clone()) {
                shards.push(shard);
            }
        }
    }
    let present: HashSet<&ShardId> = shards.iter().map(|s| &s.shard_id).collect();
    shards
        .iter()
        .map(|s| ShardMeta {
            id: s.shard_id.clone(),
            parents: parents_of(s, &present),
        })
        .collect()
}

/// Incremental sync: merge child shards discovered via a `CHILD_SHARDS`
/// `ShardFilter` (see [`ShardFilter::child_shards`]) into an existing graph.
///
/// New shards are appended (dedup by id), preserving order; already-known shards
/// are ignored. This is the newer, scan-avoiding path used when a parent reaches
/// `SHARD_END`: fetch just its children instead of re-paginating the whole
/// stream. On a filtered-call error the caller should fall back to
/// [`build_shard_graph`] over a full `DescribeStream` — matching the adapter.
pub fn merge_child_shards(existing: Vec<ShardMeta>, discovered: Vec<DdbShard>) -> Vec<ShardMeta> {
    let mut seen: HashSet<ShardId> = existing.iter().map(|m| m.id.clone()).collect();
    // Parents may reference existing shards (the completed parent) or siblings
    // discovered in the same batch.
    let mut present: HashSet<ShardId> = seen.clone();
    present.extend(discovered.iter().map(|s| s.shard_id.clone()));
    let present_refs: HashSet<&ShardId> = present.iter().collect();

    let mut out = existing;
    for s in discovered {
        if seen.insert(s.shard_id.clone()) {
            out.push(ShardMeta {
                id: s.shard_id.clone(),
                parents: parents_of(&s, &present_refs),
            });
        }
    }
    out
}

/// Map a shard's single `ParentShardId` to `ShardMeta.parents`, dropping a
/// parent that is not present (trimmed/absent) so the child is root-eligible.
fn parents_of(s: &DdbShard, present: &HashSet<&ShardId>) -> Vec<ShardId> {
    match &s.parent_shard_id {
        Some(p) if present.contains(p) => vec![p.clone()],
        _ => vec![],
    }
}

/// Sentinel ending sequence number stamped on a parent shard that appeared open
/// but has children (see [`close_open_parents`]). Any non-`None` value marks the
/// shard closed for [`DdbShard::is_open`]; the exact value is not a real DDB
/// sequence number and must not be used for iteration.
pub const CLOSED_BY_SHARD_SYNC: &str = "__closed_by_shard_sync__";

/// Close "open" parents (adapter shard-sync Phase 2 / `ShardGraphTracker`).
///
/// DynamoDB Streams can transiently report a parent shard as open (no ending
/// sequence number) even though it already has child shards — the
/// "parent-open-child-open" inconsistency. Left as-is, such a parent never
/// reaches `SHARD_END`, so the engine's parent-before-child gate would block its
/// children indefinitely. Any shard that is referenced as a parent by a present
/// shard MUST have already ended, so we mark it closed.
///
/// Grounded in `DynamoDBStreamsShardDetector.describeStream` Phase 2
/// (`ShardGraphTracker.closeOpenParents`) — awslabs/dynamodb-streams-kinesis-adapter.
pub fn close_open_parents(mut shards: Vec<DdbShard>) -> Vec<DdbShard> {
    let parent_ids: HashSet<ShardId> = shards
        .iter()
        .filter_map(|s| s.parent_shard_id.clone())
        .collect();
    for s in &mut shards {
        if s.is_open() && parent_ids.contains(&s.shard_id) {
            s.ending_sequence_number = Some(CLOSED_BY_SHARD_SYNC.to_string());
        }
    }
    shards
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids_with_parents(metas: &[ShardMeta]) -> Vec<(&str, Vec<&str>)> {
        metas
            .iter()
            .map(|m| {
                (
                    m.id.as_str(),
                    m.parents.iter().map(|p| p.as_str()).collect::<Vec<_>>(),
                )
            })
            .collect()
    }

    #[test]
    fn split_lineage_single_parent() {
        let pages = vec![vec![
            DdbShard::new("shard-0", None),
            DdbShard::new("shard-1", Some("shard-0")),
        ]];
        let g = build_shard_graph(pages);
        assert_eq!(
            ids_with_parents(&g),
            vec![("shard-0", vec![]), ("shard-1", vec!["shard-0"])]
        );
    }

    #[test]
    fn dedup_across_pages_preserves_first_seen_order() {
        let pages = vec![
            vec![DdbShard::new("a", None), DdbShard::new("b", Some("a"))],
            vec![DdbShard::new("b", Some("a")), DdbShard::new("c", Some("b"))],
        ];
        let g = build_shard_graph(pages);
        assert_eq!(
            ids_with_parents(&g),
            vec![("a", vec![]), ("b", vec!["a"]), ("c", vec!["b"])]
        );
    }

    #[test]
    fn trimmed_parent_is_dropped_so_child_is_not_blocked() {
        let pages = vec![vec![DdbShard::new("shard-9", Some("shard-old-gone"))]];
        let g = build_shard_graph(pages);
        assert_eq!(ids_with_parents(&g), vec![("shard-9", vec![])]);
    }

    #[test]
    fn child_shards_filter_targets_parent() {
        let f = ShardFilter::child_shards("shard-parent");
        assert_eq!(f.type_as_string(), "CHILD_SHARDS");
        assert_eq!(f.shard_id.as_deref(), Some("shard-parent"));
    }

    #[test]
    fn merge_child_shards_appends_children_of_completed_parent() {
        // Existing graph: a single (now-closed) parent.
        let existing = build_shard_graph(vec![vec![DdbShard::closed("p", None, "seq-100")]]);
        // CHILD_SHARDS filter on "p" returns its two children.
        let discovered = vec![
            DdbShard::new("c1", Some("p")),
            DdbShard::new("c2", Some("p")),
        ];
        let g = merge_child_shards(existing, discovered);
        assert_eq!(
            ids_with_parents(&g),
            vec![("p", vec![]), ("c1", vec!["p"]), ("c2", vec!["p"])]
        );
    }

    #[test]
    fn merge_child_shards_ignores_already_known() {
        let existing = build_shard_graph(vec![vec![
            DdbShard::closed("p", None, "seq-1"),
            DdbShard::new("c1", Some("p")),
        ]]);
        // "c1" already known; only "c2" is new.
        let discovered = vec![
            DdbShard::new("c1", Some("p")),
            DdbShard::new("c2", Some("p")),
        ];
        let g = merge_child_shards(existing, discovered);
        assert_eq!(
            ids_with_parents(&g),
            vec![("p", vec![]), ("c1", vec!["p"]), ("c2", vec!["p"])]
        );
    }

    #[test]
    fn close_open_parents_closes_a_parent_that_looks_open() {
        // "p" is reported open (no ending seq) but has a child "c" → must close.
        let shards = vec![
            DdbShard::new("p", None),          // open, but is a parent
            DdbShard::new("c", Some("p")),     // leaf, genuinely open
        ];
        let out = close_open_parents(shards);
        let p = out.iter().find(|s| s.shard_id == "p").unwrap();
        let c = out.iter().find(|s| s.shard_id == "c").unwrap();
        assert!(!p.is_open(), "parent with children must be closed");
        assert_eq!(p.ending_sequence_number.as_deref(), Some(CLOSED_BY_SHARD_SYNC));
        assert!(c.is_open(), "childless leaf stays open");
    }

    #[test]
    fn close_open_parents_leaves_already_closed_parents_untouched() {
        let shards = vec![
            DdbShard::closed("p", None, "seq-real"),
            DdbShard::new("c", Some("p")),
        ];
        let out = close_open_parents(shards);
        let p = out.iter().find(|s| s.shard_id == "p").unwrap();
        // Real ending sequence number preserved (not overwritten by the sentinel).
        assert_eq!(p.ending_sequence_number.as_deref(), Some("seq-real"));
    }
}
