//! Pluggable metrics sink (dependency-free core abstraction).
//!
//! The engine emits observations through a [`MetricsSink`]; concrete exporters
//! (OpenTelemetry/OTLP, CloudWatch EMF, a language-binding callback) implement
//! it without pulling any exporter dependency into `core`. The default
//! [`NoopSink`] makes metrics strictly opt-in and zero-cost when unused.
//!
//! The headline metric is [`ShardMetrics::millis_behind_latest`] — the
//! DynamoDB-Streams analog of Kinesis `MillisBehindLatest` (consumer lag),
//! which KCL/KCA expose as the primary health signal. Unlike Kinesis, DDB
//! Streams `GetRecords` returns no lag field, so the source derives it from the
//! newest record's `ApproximateCreationDateTime`.

use std::sync::Arc;

/// Per-batch observation, emitted once per delivered (non-empty) batch.
#[derive(Clone, Debug)]
pub struct ShardMetrics<'a> {
    pub shard_id: &'a str,
    /// Records delivered in this batch.
    pub records: u64,
    /// Total payload bytes delivered in this batch.
    pub bytes: u64,
    /// Consumer lag in ms (`now - newest ApproximateCreationDateTime`), when known.
    pub millis_behind_latest: Option<i64>,
}

/// Sink for engine metrics. All methods are cheap and non-blocking; an exporter
/// should aggregate/export off the hot path. Every method has a no-op default so
/// implementors only override what they emit.
pub trait MetricsSink: Send + Sync {
    /// A batch of records was delivered on a shard.
    fn on_batch(&self, _m: &ShardMetrics<'_>) {}
    /// A shard reached SHARD_END (completed).
    fn on_shard_end(&self, _shard_id: &str) {}
    /// The leader issued a `DescribeStream` (full sync or a CHILD_SHARDS query).
    fn on_describe_stream(&self) {}
    /// This worker acquired (or created) a lease.
    fn on_lease_acquired(&self, _shard_id: &str) {}
    /// This worker lost a lease (stolen by another worker or expired).
    fn on_lease_lost(&self, _shard_id: &str) {}
    /// Number of shard leases this worker currently holds, reported once per
    /// coordination cycle. Exported as a gauge for rebalance/failover health.
    fn on_leases_held(&self, _count: u64) {}
    /// Milliseconds a shard's batch waited to acquire a processing slot, emitted
    /// only when `max_processing_concurrency` is set. ~0 means the cap is not
    /// binding; a growing value means the cap is throttling processing (raise it
    /// or scale out). Not emitted when unbounded.
    fn on_processing_slot_wait(&self, _shard_id: &str, _wait_ms: u64) {}
    /// The configured processing-concurrency cap, reported once per cycle when
    /// `max_processing_concurrency` is set (gauge). Reflects online resizes.
    fn on_max_processing_concurrency(&self, _cap: u64) {}
}

/// Default sink that records nothing — metrics are opt-in and cost nothing when
/// left disabled.
pub struct NoopSink;
impl MetricsSink for NoopSink {}

/// Convenience alias for a shared sink handed to the fleet.
pub type SharedMetricsSink = Arc<dyn MetricsSink>;

/// A shared [`NoopSink`].
pub fn noop_sink() -> SharedMetricsSink {
    Arc::new(NoopSink)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// (shard_id, records, bytes, millis_behind_latest) captured per batch.
    type CapturedBatch = (String, u64, u64, Option<i64>);

    #[derive(Default)]
    struct Capture {
        batches: Mutex<Vec<CapturedBatch>>,
        describes: Mutex<u64>,
    }
    impl MetricsSink for Capture {
        fn on_batch(&self, m: &ShardMetrics<'_>) {
            self.batches.lock().unwrap().push((
                m.shard_id.to_string(),
                m.records,
                m.bytes,
                m.millis_behind_latest,
            ));
        }
        fn on_describe_stream(&self) {
            *self.describes.lock().unwrap() += 1;
        }
    }

    #[test]
    fn sink_receives_batch_and_lag() {
        let c = Capture::default();
        c.on_batch(&ShardMetrics {
            shard_id: "s0",
            records: 3,
            bytes: 120,
            millis_behind_latest: Some(450),
        });
        c.on_describe_stream();
        let b = c.batches.lock().unwrap();
        assert_eq!(b.len(), 1);
        assert_eq!(b[0], ("s0".to_string(), 3, 120, Some(450)));
        assert_eq!(*c.describes.lock().unwrap(), 1);
    }

    #[test]
    fn noop_sink_is_inert() {
        let s = NoopSink;
        s.on_batch(&ShardMetrics {
            shard_id: "s",
            records: 1,
            bytes: 1,
            millis_behind_latest: Some(0),
        });
        s.on_shard_end("s");
        s.on_describe_stream();
        s.on_lease_acquired("s");
        s.on_lease_lost("s");
        s.on_leases_held(3);
        s.on_processing_slot_wait("s", 12);
        s.on_max_processing_concurrency(4);
        // Nothing to assert — just proving the default impls are callable/inert.
    }

    #[derive(Default)]
    struct LeaseCapture {
        acquired: Mutex<Vec<String>>,
        lost: Mutex<Vec<String>>,
        held: Mutex<Vec<u64>>,
    }
    impl MetricsSink for LeaseCapture {
        fn on_lease_acquired(&self, s: &str) {
            self.acquired.lock().unwrap().push(s.to_string());
        }
        fn on_lease_lost(&self, s: &str) {
            self.lost.lock().unwrap().push(s.to_string());
        }
        fn on_leases_held(&self, n: u64) {
            self.held.lock().unwrap().push(n);
        }
    }

    #[test]
    fn sink_receives_lease_lifecycle_events() {
        let c = LeaseCapture::default();
        c.on_lease_acquired("s0");
        c.on_lease_lost("s0");
        c.on_leases_held(2);
        assert_eq!(*c.acquired.lock().unwrap(), vec!["s0".to_string()]);
        assert_eq!(*c.lost.lock().unwrap(), vec!["s0".to_string()]);
        assert_eq!(*c.held.lock().unwrap(), vec![2]);
    }
}
