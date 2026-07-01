//! Async worker for ddbstreams-kcl.
//!
//! Composes an [`AsyncStreamSource`] (DynamoDB Streams) + an [`AsyncLeaseStore`]
//! (DynamoDB) + a [`RecordProcessor`] (customer logic) into a single
//! ordering-preserving consumer. This is the end-to-end "core": it acquires a
//! lease per shard, delivers records in sequence order, checkpoints, and marks
//! shards complete at `SHARD_END` — enforcing **parent-before-child** across
//! resharding (a shard is only worked once every parent's lease is complete).
//!
//! Single-worker for now (no lease stealing/failover across hosts yet). The
//! engine + async traits are always built and unit-tested with in-memory fakes;
//! the concrete AWS trait impls live in the `aws` module behind the `aws` feature.

use ddbstreams_kcl_core::{RecordBatch, RecordProcessor, ShardId, ShardMeta};
use ddbstreams_kcl_core::coordinator::RawLease;
use std::collections::HashSet;

#[cfg(feature = "aws")]
pub mod aws;

pub mod fleet;

pub type WorkerError = Box<dyn std::error::Error + Send + Sync>;

/// Ownership handle returned by [`AsyncLeaseStore::acquire`].
#[derive(Clone, Debug)]
pub struct LeaseHandle {
    pub owner: String,
    pub counter: u64,
    /// Opaque checkpoint to resume from (`None` = `TRIM_HORIZON`).
    pub checkpoint: Option<String>,
}

/// Minimal read view of a lease used for completion checks.
#[derive(Clone, Debug)]
pub struct LeaseView {
    pub completed: bool,
}

/// Async stream source (DynamoDB Streams `DescribeStream`/`GetRecords`).
#[async_trait::async_trait]
pub trait AsyncStreamSource {
    async fn describe_shards(&self) -> Result<Vec<ShardMeta>, WorkerError>;
    async fn get_records(
        &self,
        shard: &str,
        after: Option<String>,
    ) -> Result<RecordBatch, WorkerError>;
}

/// Async lease store (DynamoDB optimistic-lock leases).
#[async_trait::async_trait]
pub trait AsyncLeaseStore {
    async fn get(&self, lease_key: &str) -> Result<Option<LeaseView>, WorkerError>;
    /// Scan all lease rows (for the coordinator's take decisions).
    async fn list(&self) -> Result<Vec<RawLease>, WorkerError>;
    async fn acquire(&self, lease_key: &str, owner: &str) -> Result<LeaseHandle, WorkerError>;
    /// Heartbeat: bump the counter conditional on ownership. Returns new counter.
    async fn renew(&self, lease_key: &str, owner: &str, counter: u64) -> Result<u64, WorkerError>;
    async fn checkpoint(
        &self,
        lease_key: &str,
        owner: &str,
        counter: u64,
        seq: &str,
    ) -> Result<u64, WorkerError>;
    async fn mark_complete(
        &self,
        lease_key: &str,
        owner: &str,
        counter: u64,
    ) -> Result<(), WorkerError>;
}

pub struct Worker<S, L> {
    source: S,
    leases: L,
    owner: String,
}

impl<S: AsyncStreamSource, L: AsyncLeaseStore> Worker<S, L> {
    pub fn new(source: S, leases: L, owner: impl Into<String>) -> Self {
        Self { source, leases, owner: owner.into() }
    }

    /// Drive all shards to completion in dependency order. Returns when every
    /// shard's lease is complete, or when a cycle makes no progress (drain model,
    /// suited to a bounded/closing set of shards). A long-running LATEST consumer
    /// should instead call [`Worker::run_once`] in a loop with backoff.
    pub async fn run<P: RecordProcessor>(&self, processor: &mut P) -> Result<(), WorkerError> {
        loop {
            let (complete, progressed) = self.run_once(processor).await?;
            if complete || !progressed {
                return Ok(());
            }
        }
    }

    /// One describe→process pass. Returns `(all_complete, progressed)`.
    /// `progressed` is true if at least one eligible shard was worked this pass.
    pub async fn run_once<P: RecordProcessor>(
        &self,
        processor: &mut P,
    ) -> Result<(bool, bool), WorkerError> {
        let shards = self.source.describe_shards().await?;

        // Completion snapshot for this cycle.
        let mut completed: HashSet<ShardId> = HashSet::new();
        for m in &shards {
            if let Some(v) = self.leases.get(&m.id).await? {
                if v.completed {
                    completed.insert(m.id.clone());
                }
            }
        }
        if !shards.is_empty() && shards.iter().all(|m| completed.contains(&m.id)) {
            return Ok((true, false));
        }

        let mut progressed = false;
        for meta in &shards {
            if !eligible(meta, &completed) {
                continue;
            }
            progressed = true;
            self.process_shard(meta, processor).await?;
        }
        Ok((false, progressed))
    }

    async fn process_shard<P: RecordProcessor>(
        &self,
        meta: &ShardMeta,
        processor: &mut P,
    ) -> Result<(), WorkerError> {
        let handle = self.leases.acquire(&meta.id, &self.owner).await?;
        let mut counter = handle.counter;
        let mut after = handle.checkpoint;

        processor.initialize(&meta.id);
        loop {
            let batch = self.source.get_records(&meta.id, after.clone()).await?;
            if !batch.records.is_empty() {
                processor.process_records(&batch.records);
                // Records arrive in order → the last is the newest. Checkpoint
                // it (opaque token); the counter advances under optimistic lock.
                let last = batch.records.last().unwrap().seq.clone();
                counter = self
                    .leases
                    .checkpoint(&meta.id, &self.owner, counter, &last)
                    .await?;
                after = Some(last);
            }
            if batch.shard_end {
                processor.shard_ended(&meta.id);
                self.leases
                    .mark_complete(&meta.id, &self.owner, counter)
                    .await?;
                return Ok(());
            }
            if batch.records.is_empty() {
                // No data yet; drain model returns. A LATEST consumer backs off.
                return Ok(());
            }
        }
    }
}

/// Parent-before-child: eligible iff not already complete and every parent's
/// lease is complete.
fn eligible(meta: &ShardMeta, completed: &HashSet<ShardId>) -> bool {
    !completed.contains(&meta.id) && meta.parents.iter().all(|p| completed.contains(p))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ddbstreams_kcl_core::Record;
    use std::collections::HashMap;
    use std::sync::Mutex;

    fn rec(shard: &str, seq: &str) -> Record {
        Record { shard_id: shard.into(), seq: seq.into(), data: vec![] }
    }

    struct FakeSource {
        metas: Vec<ShardMeta>,
        data: HashMap<ShardId, Vec<Record>>,
    }
    #[async_trait::async_trait]
    impl AsyncStreamSource for FakeSource {
        async fn describe_shards(&self) -> Result<Vec<ShardMeta>, WorkerError> {
            Ok(self.metas.clone())
        }
        async fn get_records(&self, shard: &str, after: Option<String>) -> Result<RecordBatch, WorkerError> {
            let all = self.data.get(shard).cloned().unwrap_or_default();
            let records = match after {
                None => all,
                Some(tok) => match all.iter().position(|r| r.seq == tok) {
                    Some(i) => all[i + 1..].to_vec(),
                    None => all,
                },
            };
            Ok(RecordBatch { records, shard_end: true })
        }
    }

    #[derive(Default)]
    struct FakeLeaseState {
        owner: Option<String>,
        counter: u64,
        checkpoint: Option<String>,
        completed: bool,
    }
    #[derive(Default)]
    struct FakeLeases {
        rows: Mutex<HashMap<String, FakeLeaseState>>,
    }
    #[async_trait::async_trait]
    impl AsyncLeaseStore for FakeLeases {
        async fn get(&self, key: &str) -> Result<Option<LeaseView>, WorkerError> {
            Ok(self.rows.lock().unwrap().get(key).map(|r| LeaseView { completed: r.completed }))
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
                })
                .collect())
        }
        async fn renew(&self, key: &str, _owner: &str, counter: u64) -> Result<u64, WorkerError> {
            let mut rows = self.rows.lock().unwrap();
            let r = rows.get_mut(key).ok_or("no lease")?;
            r.counter = counter + 1;
            Ok(r.counter)
        }
        async fn acquire(&self, key: &str, owner: &str) -> Result<LeaseHandle, WorkerError> {
            let mut rows = self.rows.lock().unwrap();
            let r = rows.entry(key.to_string()).or_default();
            r.owner = Some(owner.to_string());
            r.counter += 1;
            Ok(LeaseHandle { owner: owner.to_string(), counter: r.counter, checkpoint: r.checkpoint.clone() })
        }
        async fn checkpoint(&self, key: &str, _owner: &str, counter: u64, seq: &str) -> Result<u64, WorkerError> {
            let mut rows = self.rows.lock().unwrap();
            let r = rows.get_mut(key).ok_or("no lease")?;
            r.checkpoint = Some(seq.to_string());
            r.counter = counter + 1;
            Ok(r.counter)
        }
        async fn mark_complete(&self, key: &str, _owner: &str, _counter: u64) -> Result<(), WorkerError> {
            self.rows.lock().unwrap().get_mut(key).ok_or("no lease")?.completed = true;
            Ok(())
        }
    }

    #[derive(Default)]
    struct RecordingProcessor {
        events: Vec<String>,
    }
    impl RecordProcessor for RecordingProcessor {
        fn initialize(&mut self, s: &ShardId) { self.events.push(format!("init:{s}")); }
        fn process_records(&mut self, rs: &[Record]) {
            for r in rs { self.events.push(format!("rec:{}:{}", r.shard_id, r.seq)); }
        }
        fn shard_ended(&mut self, s: &ShardId) { self.events.push(format!("end:{s}")); }
    }

    #[tokio::test]
    async fn worker_preserves_parent_before_child_and_checkpoints() {
        let mut data = HashMap::new();
        data.insert("parent".to_string(), vec![rec("parent", "1"), rec("parent", "2")]);
        data.insert("child".to_string(), vec![rec("child", "3")]);
        let source = FakeSource {
            metas: vec![
                ShardMeta { id: "child".into(), parents: vec!["parent".into()] },
                ShardMeta { id: "parent".into(), parents: vec![] },
            ],
            data,
        };
        let leases = FakeLeases::default();
        let worker = Worker::new(source, leases, "w1");
        let mut proc = RecordingProcessor::default();
        worker.run(&mut proc).await.unwrap();

        assert_eq!(
            proc.events,
            vec![
                "init:parent", "rec:parent:1", "rec:parent:2", "end:parent",
                "init:child", "rec:child:3", "end:child",
            ]
        );
        // Lease checkpoint persisted for the child's last record.
        let rows = worker.leases.rows.lock().unwrap();
        assert_eq!(rows["parent"].checkpoint.as_deref(), Some("2"));
        assert!(rows["parent"].completed && rows["child"].completed);
    }

    #[tokio::test]
    async fn worker_resumes_from_existing_checkpoint() {
        let mut data = HashMap::new();
        data.insert("s".to_string(), vec![rec("s", "10"), rec("s", "11"), rec("s", "12")]);
        let source = FakeSource {
            metas: vec![ShardMeta { id: "s".into(), parents: vec![] }],
            data,
        };
        let leases = FakeLeases::default();
        leases.rows.lock().unwrap().insert(
            "s".into(),
            FakeLeaseState { owner: None, counter: 3, checkpoint: Some("11".into()), completed: false },
        );
        let worker = Worker::new(source, leases, "w1");
        let mut proc = RecordingProcessor::default();
        worker.run(&mut proc).await.unwrap();
        assert_eq!(proc.events, vec!["init:s", "rec:s:12", "end:s"]);
    }
}
