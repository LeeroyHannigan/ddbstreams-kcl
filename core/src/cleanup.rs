//! Lineage-replay-safe lease cleanup (pure, offline-tested).
//!
//! A completed (SHARD_END) parent lease must be kept as a tombstone until its
//! children are actually being processed — deleting it too early can cause the
//! shard's lineage to be rediscovered and replayed. This mirrors KCL
//! `LeaseCleanupManager.cleanupLeaseForCompletedShard`. See core/REFERENCES.md.

use crate::ShardMeta;
use std::collections::HashMap;

/// Lease state relevant to cleanup.
#[derive(Clone, Copy, Debug, Default)]
pub struct LeaseState {
    pub completed: bool,
    /// The lease has started making progress (checkpointed past TRIM_HORIZON or
    /// completed) — i.e. a worker is actively processing it.
    pub processing: bool,
}

/// Return the completed parent lease keys that are safe to delete.
///
/// Rule: a completed shard's lease is deletable once it has children AND every
/// child has a lease that is present and either processing or completed. If any
/// child lease is missing or not yet processing, the parent is retained (as a
/// tombstone). Completed shards with no known children are also retained
/// (children may not be discovered yet). Deterministic (sorted output).
pub fn leases_safe_to_delete(
    shards: &[ShardMeta],
    state: &HashMap<String, LeaseState>,
) -> Vec<String> {
    // parent shard id -> its children.
    let mut children: HashMap<&str, Vec<&str>> = HashMap::new();
    for s in shards {
        for p in &s.parents {
            children.entry(p.as_str()).or_default().push(s.id.as_str());
        }
    }

    let mut deletable: Vec<String> = shards
        .iter()
        .filter(|s| state.get(&s.id).map(|st| st.completed).unwrap_or(false))
        .filter(|s| {
            let kids = match children.get(s.id.as_str()) {
                Some(k) if !k.is_empty() => k,
                _ => return false, // no known children → retain
            };
            kids.iter().all(|c| {
                state
                    .get(*c)
                    .map(|st| st.processing || st.completed)
                    .unwrap_or(false) // missing child lease → retain
            })
        })
        .map(|s| s.id.clone())
        .collect();

    deletable.sort();
    deletable
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(id: &str, parents: &[&str]) -> ShardMeta {
        ShardMeta { id: id.into(), parents: parents.iter().map(|p| p.to_string()).collect() }
    }
    fn st(completed: bool, processing: bool) -> LeaseState {
        LeaseState { completed, processing }
    }

    #[test]
    fn deletes_completed_parent_once_child_processing() {
        let shards = vec![meta("p", &[]), meta("c", &["p"])];
        let state = HashMap::from([
            ("p".to_string(), st(true, true)),
            ("c".to_string(), st(false, true)), // child actively processing
        ]);
        assert_eq!(leases_safe_to_delete(&shards, &state), vec!["p"]);
    }

    #[test]
    fn retains_parent_while_child_not_yet_processing() {
        let shards = vec![meta("p", &[]), meta("c", &["p"])];
        let state = HashMap::from([
            ("p".to_string(), st(true, true)),
            ("c".to_string(), st(false, false)), // child created but not started
        ]);
        assert!(leases_safe_to_delete(&shards, &state).is_empty());
    }

    #[test]
    fn retains_parent_when_child_lease_missing() {
        let shards = vec![meta("p", &[]), meta("c", &["p"])];
        let state = HashMap::from([("p".to_string(), st(true, true))]); // no "c" lease
        assert!(leases_safe_to_delete(&shards, &state).is_empty());
    }

    #[test]
    fn merge_parents_deletable_once_shared_child_processing() {
        let shards = vec![meta("p1", &[]), meta("p2", &[]), meta("c", &["p1", "p2"])];
        let state = HashMap::from([
            ("p1".to_string(), st(true, true)),
            ("p2".to_string(), st(true, true)),
            ("c".to_string(), st(false, true)),
        ]);
        assert_eq!(leases_safe_to_delete(&shards, &state), vec!["p1", "p2"]);
    }

    #[test]
    fn does_not_delete_incomplete_parent() {
        let shards = vec![meta("p", &[]), meta("c", &["p"])];
        let state = HashMap::from([
            ("p".to_string(), st(false, true)), // parent not completed
            ("c".to_string(), st(false, true)),
        ]);
        assert!(leases_safe_to_delete(&shards, &state).is_empty());
    }

    #[test]
    fn retains_completed_leaf_with_no_children() {
        let shards = vec![meta("solo", &[])];
        let state = HashMap::from([("solo".to_string(), st(true, true))]);
        assert!(leases_safe_to_delete(&shards, &state).is_empty());
    }
}
