//! In-process SCALE test for the bounded processing pool: the engine's memory
//! profile at a shard count no live sandbox can reach (thousands of shards).
//!
//! Runs the REAL `Fleet` engine against a mock stream source with `SHARDS`
//! open, mostly-idle shards (the "long-lived table with thousands of
//! single-digit-RPS shards" profile), with a small pool, and asserts:
//!   1. the process's RSS growth while owning ALL shards is bounded (MB-scale,
//!      nothing per-shard beyond a registry entry),
//!   2. concurrent fetches never exceed the pool size at any instant,
//!   3. the run completes (every shard is polled — no starvation at scale).
//!
//! This is the acceptance evidence for "footprint O(pool), not O(shards)"
//! at the scale the feature is for. The live soak covers the end-to-end
//! path at the sandbox's warm-throughput ceiling; this covers the asymptote.
//!
//! Ignored by default (it is a ~multi-second RSS measurement, not a unit
//! test). Run with:
//!   cargo test -p amazon-dynamodb-streams-consumer-worker --test scale_pool -- --ignored --nocapture

use amazon_dynamodb_streams_consumer_core::coordinator::RawLease;
use amazon_dynamodb_streams_consumer_core::{Record, RecordBatch, ShardId, ShardMeta};
use amazon_dynamodb_streams_consumer_worker::fleet::{Fleet, FleetConfig};
use amazon_dynamodb_streams_consumer_worker::{
    AsyncShardConsumer, AsyncStreamSource, LeaseHandle, LeaseView, ShardConsumerFactory,
    WorkerError,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const SHARDS: usize = 2_000;
const POOL: usize = 8;

/// Read this process's VmRSS in KB from /proc (Linux-only, like the live test).
fn rss_kb() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            return rest
                .trim()
                .trim_end_matches(" kB")
                .trim()
                .parse()
                .unwrap_or(0);
        }
    }
    0
}

/// Mock source: `SHARDS` shards, each with exactly one record then SHARD_END.
/// Fetch concurrency is probed; a small per-call sleep makes overlap real.
struct ScaleSource {
    metas: Vec<ShardMeta>,
    cur: Arc<AtomicUsize>,
    max: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl AsyncStreamSource for ScaleSource {
    async fn describe_shards(&self) -> Result<Vec<ShardMeta>, WorkerError> {
        Ok(self.metas.clone())
    }
    async fn get_records(
        &self,
        shard: &str,
        after: Option<String>,
    ) -> Result<RecordBatch, WorkerError> {
        let now = self.cur.fetch_add(1, Ordering::SeqCst) + 1;
        self.max.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_micros(200)).await;
        let records = if after.is_none() {
            vec![Record {
                shard_id: shard.to_string(),
                seq: format!("{shard}-1"),
                data: vec![0u8; 256],
            }]
        } else {
            vec![]
        };
        self.cur.fetch_sub(1, Ordering::SeqCst);
        Ok(RecordBatch {
            records,
            shard_end: true,
            millis_behind_latest: None,
        })
    }
}

/// In-memory lease store (mirrors the unit tests' FakeLeases, minimal).
#[derive(Default)]
struct MemState {
    owner: Option<String>,
    counter: u64,
    completed: bool,
    checkpoint: Option<String>,
    parents: Vec<String>,
}
#[derive(Default, Clone)]
struct MemLeases {
    rows: Arc<Mutex<HashMap<String, MemState>>>,
}
#[async_trait::async_trait]
impl amazon_dynamodb_streams_consumer_worker::AsyncLeaseStore for MemLeases {
    async fn get(&self, key: &str) -> Result<Option<LeaseView>, WorkerError> {
        Ok(self.rows.lock().unwrap().get(key).map(|r| LeaseView {
            completed: r.completed,
        }))
    }
    async fn list(&self) -> Result<Vec<RawLease>, WorkerError> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .iter()
            .map(|(k, r)| RawLease {
                lease_key: k.clone(),
                owner: r.owner.clone(),
                lease_counter: r.counter,
                completed: r.completed,
                checkpoint: r.checkpoint.clone(),
                parents: r.parents.clone(),
            })
            .collect())
    }
    async fn acquire(&self, key: &str, owner: &str) -> Result<LeaseHandle, WorkerError> {
        let mut rows = self.rows.lock().unwrap();
        let r = rows.entry(key.to_string()).or_default();
        r.owner = Some(owner.to_string());
        r.counter += 1;
        Ok(LeaseHandle {
            owner: owner.to_string(),
            counter: r.counter,
            checkpoint: r.checkpoint.clone(),
        })
    }
    async fn renew(&self, key: &str, _o: &str, counter: u64) -> Result<u64, WorkerError> {
        let mut rows = self.rows.lock().unwrap();
        let r = rows.get_mut(key).ok_or("no lease")?;
        r.counter = counter + 1;
        Ok(r.counter)
    }
    async fn checkpoint(
        &self,
        key: &str,
        _o: &str,
        counter: u64,
        s: &str,
    ) -> Result<u64, WorkerError> {
        let mut rows = self.rows.lock().unwrap();
        let r = rows.get_mut(key).ok_or("no lease")?;
        r.checkpoint = Some(s.to_string());
        r.counter = counter + 1;
        Ok(r.counter)
    }
    async fn mark_complete(&self, key: &str, _o: &str, _c: u64) -> Result<(), WorkerError> {
        let mut rows = self.rows.lock().unwrap();
        let r = rows.get_mut(key).ok_or("no lease")?;
        r.completed = true;
        Ok(())
    }
    async fn release(&self, key: &str, _o: &str, counter: u64) -> Result<(), WorkerError> {
        let mut rows = self.rows.lock().unwrap();
        let r = rows.get_mut(key).ok_or("no lease")?;
        r.owner = None;
        r.counter = counter + 1;
        Ok(())
    }
    async fn delete_lease(&self, key: &str) -> Result<(), WorkerError> {
        let mut rows = self.rows.lock().unwrap();
        if rows.get(key).map(|r| r.completed).unwrap_or(false) {
            rows.remove(key);
        }
        Ok(())
    }
    async fn create_shard_lease(
        &self,
        key: &str,
        parents: &[ShardId],
        checkpoint: Option<&str>,
    ) -> Result<(), WorkerError> {
        self.rows
            .lock()
            .unwrap()
            .entry(key.to_string())
            .or_insert_with(|| MemState {
                parents: parents.to_vec(),
                checkpoint: checkpoint.map(|s| s.to_string()),
                ..Default::default()
            });
        Ok(())
    }
    async fn try_acquire_leadership(
        &self,
        key: &str,
        owner: &str,
        expected: Option<u64>,
    ) -> Result<Option<u64>, WorkerError> {
        let mut rows = self.rows.lock().unwrap();
        match expected {
            None => {
                if rows.contains_key(key) {
                    return Ok(None);
                }
                rows.insert(
                    key.to_string(),
                    MemState {
                        owner: Some(owner.to_string()),
                        counter: 1,
                        ..Default::default()
                    },
                );
                Ok(Some(1))
            }
            Some(c) => match rows.get_mut(key) {
                Some(r) if r.counter == c => {
                    r.owner = Some(owner.to_string());
                    r.counter = c + 1;
                    Ok(Some(c + 1))
                }
                _ => Ok(None),
            },
        }
    }
}

struct CountingConsumer {
    delivered: Arc<AtomicUsize>,
}
#[async_trait::async_trait]
impl AsyncShardConsumer for CountingConsumer {
    async fn deliver(&mut self, records: &[Record]) -> Result<Option<String>, WorkerError> {
        self.delivered.fetch_add(records.len(), Ordering::SeqCst);
        Ok(records.last().map(|r| r.seq.clone()))
    }
    async fn shard_ended(&mut self) -> Result<(), WorkerError> {
        Ok(())
    }
}
struct CountingFactory {
    delivered: Arc<AtomicUsize>,
}
impl ShardConsumerFactory for CountingFactory {
    fn create(&self, _shard: &ShardId) -> Box<dyn AsyncShardConsumer + Send> {
        Box::new(CountingConsumer {
            delivered: self.delivered.clone(),
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "multi-second RSS measurement; run explicitly with -- --ignored"]
async fn pool_footprint_bounded_at_2000_shards() {
    let metas: Vec<ShardMeta> = (0..SHARDS)
        .map(|i| ShardMeta {
            id: format!("shardId-{i:05}"),
            parents: vec![],
        })
        .collect();
    let max_fetch = Arc::new(AtomicUsize::new(0));
    let source = ScaleSource {
        metas,
        cur: Arc::new(AtomicUsize::new(0)),
        max: max_fetch.clone(),
    };
    let delivered = Arc::new(AtomicUsize::new(0));
    let fleet = Fleet::new(
        source,
        MemLeases::default(),
        Arc::new(CountingFactory {
            delivered: delivered.clone(),
        }),
        FleetConfig {
            owner: "scale-w1".into(),
            max_leases: SHARDS + 1,
            lease_duration_ms: 60_000,
            poll_interval_ms: 1,
            initial_position: Default::default(),
        },
    )
    .with_max_processing_concurrency(Some(POOL));

    let rss_before = rss_kb();
    fleet.run_until_complete(200).await.unwrap();
    let rss_after = rss_kb();

    let grown_mb = (rss_after.saturating_sub(rss_before)) as f64 / 1024.0;
    println!(
        "scale: shards={SHARDS} pool={POOL} delivered={} max_concurrent_fetch={} rss_before={}KB rss_after={}KB growth={grown_mb:.1}MB",
        delivered.load(Ordering::SeqCst),
        max_fetch.load(Ordering::SeqCst),
        rss_before,
        rss_after,
    );

    assert!(
        max_fetch.load(Ordering::SeqCst) <= POOL,
        "observed {} concurrent fetches at {SHARDS} shards; pool={POOL} must bound them",
        max_fetch.load(Ordering::SeqCst)
    );

    assert_eq!(
        delivered.load(Ordering::SeqCst),
        SHARDS,
        "every one of {SHARDS} shards delivered its record (no starvation at scale)"
    );
    // The registry + schedule for 2,000 shards is KB-scale; batches/buffers are
    // O(pool). 64MB of growth would indicate a per-shard live cost — the bug
    // this engine exists to remove. (Generous ceiling: allocator slack, test
    // harness noise.)
    assert!(
        grown_mb < 64.0,
        "RSS grew {grown_mb:.1}MB owning {SHARDS} shards — per-shard footprint leak"
    );
}
