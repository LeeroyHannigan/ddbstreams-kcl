//! DynamoDB Streams `StreamSource` support for `ddbstreams-kcl-core`.
//!
//! This module contains the correctness-critical, network-free logic:
//! turning paginated `DescribeStream` results into the shard lineage the core
//! engine consumes. The async `aws-sdk-dynamodbstreams` adapter (the trivial
//! glue that fetches pages and calls `GetRecords`) lands next behind an
//! optional `aws` feature so this logic always builds and tests offline.
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
}

impl DdbShard {
    pub fn new(shard_id: impl Into<ShardId>, parent: Option<&str>) -> Self {
        Self {
            shard_id: shard_id.into(),
            parent_shard_id: parent.map(|p| p.to_string()),
        }
    }
}

/// Build the shard graph from `DescribeStream` pages.
///
/// `DescribeStream` is paginated (via `LastEvaluatedShardId`); a shard may
/// appear across pages, so we de-duplicate by `shard_id` preserving first sight.
/// Order is preserved (first-seen order) so callers get a stable list.
///
/// A parent id that is NOT present in the returned pages means the parent has
/// been trimmed/aged out (DDB Streams retains ~24h). Such a parent is dropped
/// from the child's `parents` so the child is not blocked forever waiting on a
/// shard that no longer exists — mirroring KCL treating an absent parent lease
/// as already-complete.
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

    // Set of shard ids actually present — used to drop trimmed parents.
    let present: HashSet<&ShardId> = shards.iter().map(|s| &s.shard_id).collect();

    shards
        .iter()
        .map(|s| {
            let parents = match &s.parent_shard_id {
                Some(p) if present.contains(p) => vec![p.clone()],
                // No parent, or parent trimmed/absent → treat as root-eligible.
                _ => vec![],
            };
            ShardMeta {
                id: s.shard_id.clone(),
                parents,
            }
        })
        .collect()
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
            // "b" repeats on the next page (overlap); "c" is new.
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
        // "shard-9" references a parent that is not in the returned pages
        // (aged out of the 24h window). It should become root-eligible.
        let pages = vec![vec![DdbShard::new("shard-9", Some("shard-old-gone"))]];
        let g = build_shard_graph(pages);
        assert_eq!(ids_with_parents(&g), vec![("shard-9", vec![])]);
    }
}
