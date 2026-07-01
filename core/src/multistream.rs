//! Multi-stream lease-key namespacing.
//!
//! When one worker fleet consumes more than one stream, shard ids from
//! different streams can collide, so the lease key must be namespaced by the
//! stream. This mirrors KCL's `MultiStreamLease`, whose lease key is
//! `StreamIdentifier.serialize() + ":" + shardId` (see the
//! amazon-kinesis-client `MultiStreamLease` / `StreamIdentifier`, Apache-2.0).
//!
//! For DynamoDB Streams the natural stream identity is the stream ARN. The ARN
//! itself contains colons (region, account, and the ISO-8601 creation time in
//! the `.../stream/<timestamp>` suffix), so we always split on the **last**
//! colon: a DynamoDB shard id (`shardId-0000...-abcd`) never contains one, so
//! the final colon is unambiguously the delimiter we added. The mapping is
//! therefore lossless and reversible.
//!
//! Single-stream mode uses the bare shard id as the key (what the fleet does
//! today), exactly like KCL's single-stream `Lease`.

use crate::ShardId;

/// Build a lease key that namespaces `shard` under `stream` (a stream ARN, or
/// any stream identifier without a trailing-token collision). Reversible via
/// [`parse_lease_key`].
pub fn multi_stream_lease_key(stream: &str, shard: &str) -> String {
    format!("{stream}:{shard}")
}

/// The single-stream lease key: the bare shard id (KCL single-stream `Lease`).
pub fn single_stream_lease_key(shard: &str) -> String {
    shard.to_string()
}

/// Split a multi-stream lease key back into `(stream, shard)`. Splits on the
/// last colon because shard ids never contain one. Returns `None` if there is
/// no colon (i.e. a single-stream / bare-shard key).
pub fn parse_lease_key(key: &str) -> Option<(&str, &str)> {
    key.rsplit_once(':')
}

/// Extract just the shard id from a lease key, whether single- or multi-stream.
pub fn shard_of(key: &str) -> ShardId {
    match parse_lease_key(key) {
        Some((_stream, shard)) => shard.to_string(),
        None => key.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_a_plain_stream_name() {
        let key = multi_stream_lease_key("orders-stream", "shardId-000001-abcd");
        assert_eq!(key, "orders-stream:shardId-000001-abcd");
        assert_eq!(parse_lease_key(&key), Some(("orders-stream", "shardId-000001-abcd")));
        assert_eq!(shard_of(&key), "shardId-000001-abcd");
    }

    #[test]
    fn round_trips_a_stream_arn_with_embedded_colons() {
        // A real DynamoDB Streams ARN: colons in region/account AND in the
        // ISO-8601 creation timestamp. Splitting on the last colon must still
        // recover the ARN and the shard id exactly.
        let arn = "arn:aws:dynamodb:us-east-1:123456789012:table/Orders/stream/2026-07-01T21:52:30.123";
        let shard = "shardId-00000001600000000000-a1b2c3d4";
        let key = multi_stream_lease_key(arn, shard);
        let (stream, got_shard) = parse_lease_key(&key).unwrap();
        assert_eq!(stream, arn, "the full ARN (with its colons) is recovered");
        assert_eq!(got_shard, shard);
        assert_eq!(shard_of(&key), shard);
    }

    #[test]
    fn single_stream_key_is_the_bare_shard() {
        let key = single_stream_lease_key("shardId-000001-abcd");
        assert_eq!(key, "shardId-000001-abcd");
        // No colon → treated as single-stream: no (stream, shard) split.
        assert_eq!(parse_lease_key(&key), None);
        // shard_of still returns the shard id.
        assert_eq!(shard_of(&key), "shardId-000001-abcd");
    }

    #[test]
    fn distinct_streams_do_not_collide_on_same_shard_id() {
        let shard = "shardId-000001";
        let a = multi_stream_lease_key("stream-A", shard);
        let b = multi_stream_lease_key("stream-B", shard);
        assert_ne!(a, b, "same shard id under different streams yields distinct keys");
    }
}
