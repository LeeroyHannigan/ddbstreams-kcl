//! DynamoDB-backed KCL-style lease coordination for ddbstreams-kcl.
//!
//! The lease is the binding between a worker and a shard. Distributed workers
//! coordinate through a DynamoDB lease table using **optimistic locking on
//! `leaseCounter`**: every mutation is conditional on the counter it read, so
//! only one worker can win a given transition. This mirrors KCL's
//! `DynamoDBLeaseRefresher` / `DynamoDBLeaseTaker` (Apache-2.0). See
//! core/REFERENCES.md.
//!
//! Pure lease model + logic here is always built and unit-tested offline; the
//! async DynamoDB store (`dynamodb` module) is behind the `aws` feature.

pub type ShardId = String;

/// KCL single-stream lease key == shard id.
///
/// NOTE: multi-stream mode uses a different key format
/// (`account:region$account$table$label:1:shardId`); implementing it is a TODO
/// and MUST be matched exactly if ever co-running with Java KCL v3 on a shared
/// lease table.
pub fn single_stream_lease_key(shard_id: &str) -> String {
    shard_id.to_string()
}

/// In-memory view of a lease-table row.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Lease {
    pub lease_key: String,
    pub lease_owner: Option<String>,
    /// Incremented on every heartbeat / ownership transfer. The optimistic-lock
    /// token: mutations are conditional on the value the writer last read.
    pub lease_counter: u64,
    /// Opaque DynamoDB Streams checkpoint (last processed sequence number).
    pub checkpoint: Option<String>,
    /// Shard fully processed (SHARD_END). Kept as a tombstone until children are
    /// processing, to prevent lineage replay (KCL `LeaseCleanupManager`).
    pub completed: bool,
}

impl Lease {
    /// A lease is takeable by another worker if it is unowned, or if its counter
    /// has NOT advanced since `last_seen_counter` (the owner stopped
    /// heartbeating). Grounded in KCL `DynamoDBLeaseTaker` expiry detection.
    pub fn is_takeable(&self, last_seen_counter: u64) -> bool {
        self.lease_owner.is_none() || self.lease_counter == last_seen_counter
    }
}

#[cfg(feature = "aws")]
pub mod dynamodb;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_stream_lease_key_is_shard_id() {
        assert_eq!(single_stream_lease_key("shardId-42"), "shardId-42");
    }

    #[test]
    fn unowned_lease_is_takeable() {
        let l = Lease { lease_key: "s".into(), lease_owner: None, lease_counter: 7, ..Default::default() };
        assert!(l.is_takeable(7));
        assert!(l.is_takeable(0));
    }

    #[test]
    fn owned_lease_takeable_only_if_counter_stalled() {
        let l = Lease {
            lease_key: "s".into(),
            lease_owner: Some("w1".into()),
            lease_counter: 5,
            ..Default::default()
        };
        // Observed counter still 5 later → owner not heartbeating → takeable.
        assert!(l.is_takeable(5));
        // Counter advanced past what we last saw → owner alive → not takeable.
        assert!(!l.is_takeable(4));
    }
}
