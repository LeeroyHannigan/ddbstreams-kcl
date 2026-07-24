//! Leader-based single shard syncer (pure support).
//!
//! ## Why
//! In the KCL **v1** MultiLang model, *every* worker calls `DescribeStream` on
//! its own poll loop. On a fleet that multiplies `DescribeStream` load by the
//! worker count and is a well-known operational pain (throttling, cost). KCL
//! **3** fixed this by electing a single leader that runs shard discovery
//! centrally and shares the result through the lease table.
//!
//! ## Model
//! We follow the KCL 3 approach, JVM-free:
//!   * Exactly one worker holds a reserved **leader lease** ([`LEADER_LEASE_KEY`]),
//!     elected via the same optimistic-lock + expiry logic as shard leases
//!     ([`crate::coordinator`]).
//!   * Only the leader calls `DescribeStream`. It publishes each newly-eligible
//!     shard as a lease row carrying that shard's `parents`
//!     ([`crate::coordinator::RawLease::parents`]).
//!   * Every other worker reconstructs the shard graph from the lease table via
//!     [`shard_metas_from_leases`] — and therefore never calls `DescribeStream`.
//!
//! This collapses `DescribeStream` call volume from *(workers × cycles)* to
//! *(1 × cycles)*, and keeps our lineage-driven sync (no Kinesis hash-range
//! contiguity validation, so none of KCL's false-"hole" stalls on DynamoDB
//! streams).
//!
//! This module holds only the *pure* pieces (the sentinel key + graph
//! reconstruction). Leader election and the discovery/publish action need the
//! async lease store, so they live in the worker fleet.

use crate::coordinator::RawLease;
use crate::ShardMeta;

/// Reserved lease key that elects the single shard-sync leader.
///
/// It is a sentinel, NOT a shard: it is filtered out of every shard enumeration
/// (see [`shard_metas_from_leases`]) and must never be handed to the shard
/// coordinator's take logic. The unusual name avoids colliding with any real
/// DynamoDB Streams shard id (which look like `shardId-00000000000000000000-...`).
pub const LEADER_LEASE_KEY: &str = "__shard_sync_leader__";

/// Reconstruct the shard graph (`id` + `parents`) from lease rows, skipping the
/// leader sentinel.
///
/// This is the non-leader worker's substitute for `DescribeStream`: the leader
/// has already published every eligible shard — with its parents — as a lease
/// row, so a worker derives both the shard set and the parent-before-child
/// dependencies purely from the lease table it already scans for coordination.
pub fn shard_metas_from_leases(rows: &[RawLease]) -> Vec<ShardMeta> {
    rows.iter()
        .filter(|r| {
            r.lease_key != LEADER_LEASE_KEY && !crate::coordinator::is_heartbeat_key(&r.lease_key)
        })
        .map(|r| ShardMeta {
            id: r.lease_key.clone(),
            parents: r.parents.clone(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lease(key: &str, parents: &[&str]) -> RawLease {
        RawLease {
            lease_key: key.into(),
            owner: None,
            lease_counter: 0,
            completed: false,
            checkpoint: None,
            parents: parents.iter().map(|p| p.to_string()).collect(),
        }
    }

    #[test]
    fn reconstructs_graph_and_skips_leader_sentinel() {
        let rows = vec![
            lease(LEADER_LEASE_KEY, &[]),
            lease("shard-0", &[]),
            lease("shard-1", &["shard-0"]),
        ];
        let metas = shard_metas_from_leases(&rows);
        let ids: Vec<&str> = metas.iter().map(|m| m.id.as_str()).collect();
        assert_eq!(ids, vec!["shard-0", "shard-1"], "sentinel excluded");
        let child = metas.iter().find(|m| m.id == "shard-1").unwrap();
        assert_eq!(
            child.parents,
            vec!["shard-0"],
            "parents carried on the lease"
        );
    }

    #[test]
    fn merge_child_carries_both_parents() {
        let rows = vec![
            lease("p-a", &[]),
            lease("p-b", &[]),
            lease("child", &["p-a", "p-b"]),
        ];
        let child = shard_metas_from_leases(&rows)
            .into_iter()
            .find(|m| m.id == "child")
            .unwrap();
        assert_eq!(child.parents, vec!["p-a", "p-b"]);
    }

    #[test]
    fn empty_table_yields_no_shards() {
        assert!(shard_metas_from_leases(&[]).is_empty());
    }
}
