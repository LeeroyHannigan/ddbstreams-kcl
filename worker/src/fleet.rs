//! Per-shard-task fleet runtime (concurrency model "A").
//!
//! Each coordination cycle: scan leases → the [`LeaseCoordinator`] decides which
//! to take (fair-share/steal + wall-clock expiry) → claim them → run one
//! **concurrent task per owned, eligible shard**. Each shard task delivers
//! records in order to its own [`RecordProcessor`] (from the factory),
//! checkpoints/heartbeats under the optimistic lock, and marks the shard
//! complete at SHARD_END. Parent-before-child is enforced via lease completion.
//!
//! This mirrors KCL's model (one record processor per shard, concurrent) on top
//! of the pure primitives in `core`.

use crate::{eligible, AsyncLeaseStore, AsyncShardConsumer, AsyncStreamSource, ShardConsumerFactory, WorkerError};
use ddbstreams_kcl_core::coordinator::LeaseCoordinator;
use ddbstreams_kcl_core::ShardId;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinSet;

pub struct FleetConfig {
    pub owner: String,
    pub max_leases: usize,
    pub lease_duration_ms: u64,
    /// Idle backoff between empty `GetRecords` polls inside a shard task.
    pub poll_interval_ms: u64,
}

pub struct Fleet<S, L> {
    source: Arc<S>,
    leases: Arc<L>,
    factory: Arc<dyn ShardConsumerFactory>,
    config: FleetConfig,
}

impl<S, L> Fleet<S, L>
where
    S: AsyncStreamSource + Send + Sync + 'static,
    L: AsyncLeaseStore + Send + Sync + 'static,
{
    pub fn new(source: S, leases: L, factory: Arc<dyn ShardConsumerFactory>, config: FleetConfig) -> Self {
        Self { source: Arc::new(source), leases: Arc::new(leases), factory, config }
    }

    /// Run coordination cycles until every shard's lease is complete or
    /// `max_cycles` is reached (drain model for a bounded/closing shard set; a
    /// long-running consumer loops [`Fleet::run_cycle`] forever with backoff).
    pub async fn run_until_complete(&self, max_cycles: usize) -> Result<(), WorkerError> {
        let mut coordinator =
            LeaseCoordinator::new(self.config.owner.clone(), self.config.max_leases, self.config.lease_duration_ms);
        let start = Instant::now();
        for _ in 0..max_cycles {
            let now_ms = start.elapsed().as_millis() as u64;
            if self.run_cycle(&mut coordinator, now_ms).await? {
                return Ok(());
            }
        }
        Ok(())
    }

    /// One coordination cycle. Returns `true` when all shards are complete.
    pub async fn run_cycle(
        &self,
        coordinator: &mut LeaseCoordinator,
        now_ms: u64,
    ) -> Result<bool, WorkerError> {
        // 1) Decide + claim this worker's share.
        let rows = self.leases.list().await?;
        for key in coordinator.tick(&rows, now_ms) {
            let _ = self.leases.acquire(&key, &self.config.owner).await; // best-effort
        }

        // 2) Re-read ownership + completion.
        let rows = self.leases.list().await?;
        let owner = self.config.owner.as_str();
        let has_lease: HashSet<ShardId> = rows.iter().map(|r| r.lease_key.clone()).collect();
        // shard -> (lease counter, resume checkpoint). The checkpoint lets a
        // task resume where the last owner left off instead of re-reading from
        // TRIM_HORIZON — essential for correctness across cycles and, critically,
        // across a process restart (the in-memory iterator is gone, but the
        // persisted checkpoint survives).
        let mut owned: std::collections::HashMap<ShardId, (u64, Option<String>)> = rows
            .iter()
            .filter(|r| r.owner.as_deref() == Some(owner) && !r.completed)
            .map(|r| (r.lease_key.clone(), (r.lease_counter, r.checkpoint.clone())))
            .collect();
        let completed: HashSet<ShardId> = rows
            .iter()
            .filter(|r| r.completed)
            .map(|r| r.lease_key.clone())
            .collect();

        let shards = self.source.describe_shards().await?;
        if !shards.is_empty() && shards.iter().all(|m| completed.contains(&m.id)) {
            return Ok(true);
        }

        // Shard-sync: create (acquire) leases for newly discovered eligible
        // shards (parents complete) that have no lease yet — the analog of KCL's
        // HierarchicalShardSyncer creating child leases only after SHARD_END.
        for meta in &shards {
            if completed.contains(&meta.id) || !eligible(meta, &completed) {
                continue;
            }
            if !has_lease.contains(&meta.id) && !owned.contains_key(&meta.id) {
                if let Ok(h) = self.leases.acquire(&meta.id, owner).await {
                    owned.insert(meta.id.clone(), (h.counter, h.checkpoint));
                }
            }
        }

        // 3) Run one concurrent task per owned + eligible shard.
        let mut set: JoinSet<()> = JoinSet::new();
        for meta in &shards {
            let Some((counter, checkpoint)) = owned.get(&meta.id).cloned() else { continue };
            if !eligible(meta, &completed) {
                continue;
            }
            let src = self.source.clone();
            let lease = self.leases.clone();
            let consumer = self.factory.create(&meta.id);
            let task = ShardTask {
                owner: self.config.owner.clone(),
                shard: meta.id.clone(),
                counter,
                checkpoint,
                poll_interval_ms: self.config.poll_interval_ms,
            };
            set.spawn(async move {
                let _ = process_shard(src, lease, consumer, task).await;
            });
        }
        while set.join_next().await.is_some() {}
        Ok(false)
    }

    /// Release every lease this worker currently owns (graceful shutdown), so
    /// another worker takes over immediately rather than waiting for expiry.
    /// Best-effort per lease: a lease already stolen is skipped. Returns the
    /// count released.
    pub async fn release_owned(&self) -> Result<usize, WorkerError> {
        let rows = self.leases.list().await?;
        let owner = self.config.owner.as_str();
        let mut released = 0;
        for r in rows {
            if r.owner.as_deref() == Some(owner) && !r.completed
                && self.leases.release(&r.lease_key, owner, r.lease_counter).await.is_ok()
            {
                released += 1;
            }
        }
        Ok(released)
    }
}

/// Per-shard task parameters (kept in one struct so the spawn signature stays
/// small and the fields are named at the call site).
struct ShardTask {
    owner: String,
    shard: ShardId,
    counter: u64,
    /// Resume position from the lease (`None` = TRIM_HORIZON).
    checkpoint: Option<String>,
    poll_interval_ms: u64,
}

/// Drive a single shard: deliver records in order, checkpoint/heartbeat under the
/// optimistic lock, complete at SHARD_END. Exits on lease loss or when the shard
/// yields no data (one pass, for the drain model).
async fn process_shard<S, L>(
    source: Arc<S>,
    leases: Arc<L>,
    mut consumer: Box<dyn AsyncShardConsumer + Send>,
    task: ShardTask,
) -> Result<(), WorkerError>
where
    S: AsyncStreamSource,
    L: AsyncLeaseStore,
{
    let ShardTask { owner, shard, mut counter, checkpoint, poll_interval_ms } = task;
    // Resume from the lease's persisted checkpoint (None = TRIM_HORIZON for a
    // brand-new shard). This is what makes re-processing idempotent across
    // cycles and correct across a restart. (The consumer was initialized for
    // this shard by the factory.)
    let mut after: Option<String> = checkpoint;
    loop {
        let batch = source.get_records(&shard, after.clone()).await?;
        if !batch.records.is_empty() {
            let last = batch.records.last().unwrap().seq.clone();
            // Deliver and let the consumer decide the checkpoint (its ack). A
            // sidecar returns the seq the client durably processed; the sync
            // in-process adapter returns the batch's last seq.
            match consumer.deliver(&batch.records).await {
                Ok(Some(ack)) => match leases.checkpoint(&shard, &owner, counter, &ack).await {
                    Ok(c) => counter = c,
                    Err(_) => return Ok(()), // lease lost → stop
                },
                Ok(None) => {
                    // Delivered but not acked: hold the lease without advancing
                    // the durable checkpoint (heartbeat).
                    match leases.renew(&shard, &owner, counter).await {
                        Ok(c) => counter = c,
                        Err(_) => return Ok(()),
                    }
                }
                Err(_) => return Ok(()), // delivery failed → stop; lease expires
            }
            after = Some(last);
        }
        if batch.shard_end {
            let _ = consumer.shard_ended().await;
            let _ = leases.mark_complete(&shard, &owner, counter).await;
            return Ok(());
        }
        if batch.records.is_empty() {
            // Idle: heartbeat to keep the lease (best-effort; drain model then
            // returns — a continuous consumer would keep looping with backoff).
            let _ = leases.renew(&shard, &owner, counter).await;
            let _ = poll_interval_ms;
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AsyncShardConsumer, LeaseHandle, LeaseView, ShardConsumerFactory, SyncConsumerFactory};
    use ddbstreams_kcl_core::coordinator::RawLease;
    use ddbstreams_kcl_core::{Record, RecordBatch, RecordProcessor, RecordProcessorFactory, ShardMeta};
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
    struct State {
        owner: Option<String>,
        counter: u64,
        completed: bool,
        checkpoint: Option<String>,
    }
    #[derive(Default)]
    struct FakeLeases {
        rows: Mutex<HashMap<String, State>>,
    }
    #[async_trait::async_trait]
    impl AsyncLeaseStore for FakeLeases {
        async fn get(&self, key: &str) -> Result<Option<LeaseView>, WorkerError> {
            Ok(self.rows.lock().unwrap().get(key).map(|r| LeaseView { completed: r.completed }))
        }
        async fn list(&self) -> Result<Vec<RawLease>, WorkerError> {
            Ok(self.rows.lock().unwrap().iter().map(|(k, r)| RawLease {
                lease_key: k.clone(),
                owner: r.owner.clone(),
                lease_counter: r.counter,
                completed: r.completed,
                checkpoint: r.checkpoint.clone(),
            }).collect())
        }
        async fn acquire(&self, key: &str, owner: &str) -> Result<LeaseHandle, WorkerError> {
            let mut rows = self.rows.lock().unwrap();
            let r = rows.entry(key.to_string()).or_default();
            r.owner = Some(owner.to_string());
            r.counter += 1;
            Ok(LeaseHandle { owner: owner.to_string(), counter: r.counter, checkpoint: r.checkpoint.clone() })
        }
        async fn renew(&self, key: &str, _o: &str, counter: u64) -> Result<u64, WorkerError> {
            let mut rows = self.rows.lock().unwrap();
            let r = rows.get_mut(key).ok_or("no lease")?;
            r.counter = counter + 1;
            Ok(r.counter)
        }
        async fn checkpoint(&self, key: &str, _o: &str, counter: u64, _s: &str) -> Result<u64, WorkerError> {
            let mut rows = self.rows.lock().unwrap();
            let r = rows.get_mut(key).ok_or("no lease")?;
            r.checkpoint = Some(_s.to_string());
            r.counter = counter + 1;
            Ok(r.counter)
        }
        async fn mark_complete(&self, key: &str, _o: &str, _c: u64) -> Result<(), WorkerError> {
            self.rows.lock().unwrap().get_mut(key).ok_or("no lease")?.completed = true;
            Ok(())
        }
        async fn release(&self, key: &str, _o: &str, counter: u64) -> Result<(), WorkerError> {
            let mut rows = self.rows.lock().unwrap();
            let r = rows.get_mut(key).ok_or("no lease")?;
            r.owner = None;
            r.counter = counter + 1;
            Ok(())
        }
    }

    type Sink = Arc<Mutex<HashMap<String, Vec<String>>>>;
    struct RecordingFactory { sink: Sink }
    impl RecordProcessorFactory for RecordingFactory {
        fn create(&self, _shard: &ShardId) -> Box<dyn RecordProcessor + Send> {
            Box::new(RecordingProc { shard: String::new(), sink: self.sink.clone(), inited: false })
        }
    }
    struct RecordingProc { shard: String, sink: Sink, inited: bool }
    impl RecordProcessor for RecordingProc {
        fn initialize(&mut self, s: &ShardId) { self.shard = s.clone(); self.inited = true; }
        fn process_records(&mut self, rs: &[Record]) {
            let mut m = self.sink.lock().unwrap();
            for r in rs {
                m.entry(self.shard.clone()).or_default().push(r.seq.clone());
            }
        }
        fn shard_ended(&mut self, _s: &ShardId) { assert!(self.inited); }
    }

    #[tokio::test]
    async fn fleet_processes_all_shards_concurrently_in_order() {
        let mut data = HashMap::new();
        data.insert("s0".to_string(), vec![rec("s0", "1"), rec("s0", "2")]);
        data.insert("s1".to_string(), vec![rec("s1", "3"), rec("s1", "4")]);
        data.insert("s2".to_string(), vec![rec("s2", "5")]);
        let source = FakeSource {
            metas: vec![
                ShardMeta { id: "s0".into(), parents: vec![] },
                ShardMeta { id: "s1".into(), parents: vec![] },
                ShardMeta { id: "s2".into(), parents: vec![] },
            ],
            data,
        };
        let sink: Sink = Arc::new(Mutex::new(HashMap::new()));
        let factory = Arc::new(RecordingFactory { sink: sink.clone() });
        let fleet = Fleet::new(
            source,
            FakeLeases::default(),
            Arc::new(SyncConsumerFactory::new(factory)),
            FleetConfig { owner: "w1".into(), max_leases: 100, lease_duration_ms: 1000, poll_interval_ms: 1 },
        );

        fleet.run_until_complete(10).await.unwrap();

        let m = sink.lock().unwrap();
        assert_eq!(m.get("s0").unwrap(), &vec!["1", "2"]);
        assert_eq!(m.get("s1").unwrap(), &vec!["3", "4"]);
        assert_eq!(m.get("s2").unwrap(), &vec!["5"]);
        assert_eq!(m.len(), 3, "every shard processed exactly once");
    }

    #[tokio::test]
    async fn fleet_respects_parent_before_child() {
        let mut data = HashMap::new();
        data.insert("p".to_string(), vec![rec("p", "1")]);
        data.insert("c".to_string(), vec![rec("c", "2")]);
        let source = FakeSource {
            metas: vec![
                ShardMeta { id: "c".into(), parents: vec!["p".into()] },
                ShardMeta { id: "p".into(), parents: vec![] },
            ],
            data,
        };
        let sink: Sink = Arc::new(Mutex::new(HashMap::new()));
        let factory = Arc::new(RecordingFactory { sink: sink.clone() });
        let fleet = Fleet::new(
            source,
            FakeLeases::default(),
            Arc::new(SyncConsumerFactory::new(factory)),
            FleetConfig { owner: "w1".into(), max_leases: 100, lease_duration_ms: 1000, poll_interval_ms: 1 },
        );
        fleet.run_until_complete(10).await.unwrap();
        let m = sink.lock().unwrap();
        // Both processed; child only after parent completed (separate cycles).
        assert_eq!(m.get("p").unwrap(), &vec!["1"]);
        assert_eq!(m.get("c").unwrap(), &vec!["2"]);
    }

    /// An always-open shard (never SHARD_END) that honors `after`, so it can be
    /// polled across multiple cycles like a real live shard.
    struct OpenSource {
        records: Vec<Record>,
    }
    #[async_trait::async_trait]
    impl AsyncStreamSource for OpenSource {
        async fn describe_shards(&self) -> Result<Vec<ShardMeta>, WorkerError> {
            Ok(vec![ShardMeta { id: "s0".into(), parents: vec![] }])
        }
        async fn get_records(&self, _shard: &str, after: Option<String>) -> Result<RecordBatch, WorkerError> {
            let records = match after {
                None => self.records.clone(),
                Some(tok) => match self.records.iter().position(|r| r.seq == tok) {
                    Some(i) => self.records[i + 1..].to_vec(),
                    None => self.records.clone(),
                },
            };
            Ok(RecordBatch { records, shard_end: false })
        }
    }

    #[tokio::test]
    async fn fleet_resumes_from_checkpoint_no_redelivery() {
        // A shard that never closes: across multiple cycles it must NOT re-deliver
        // records already past the persisted checkpoint. This is the correctness
        // guarantee that also holds across a process restart (the checkpoint lives
        // in the lease table, not in memory).
        let source = OpenSource { records: vec![rec("s0", "1"), rec("s0", "2"), rec("s0", "3")] };
        let sink: Sink = Arc::new(Mutex::new(HashMap::new()));
        let factory = Arc::new(RecordingFactory { sink: sink.clone() });
        let fleet = Fleet::new(
            source,
            FakeLeases::default(),
            Arc::new(SyncConsumerFactory::new(factory)),
            FleetConfig { owner: "w1".into(), max_leases: 100, lease_duration_ms: 100_000, poll_interval_ms: 1 },
        );

        // Run several cycles; the shard stays open so it's revisited each cycle.
        fleet.run_until_complete(5).await.unwrap();

        let m = sink.lock().unwrap();
        assert_eq!(
            m.get("s0").unwrap(),
            &vec!["1", "2", "3"],
            "each record delivered exactly once across cycles (resumed from checkpoint)"
        );
    }

    // A consumer that delivers but never acks (returns None) — the sidecar's
    // "client hasn't checkpointed yet" case. The fleet must hold the lease
    // (heartbeat) WITHOUT advancing the durable checkpoint.
    struct NoAckFactory {
        sink: Sink,
    }
    impl ShardConsumerFactory for NoAckFactory {
        fn create(&self, shard: &ShardId) -> Box<dyn AsyncShardConsumer + Send> {
            Box::new(NoAckConsumer { shard: shard.clone(), sink: self.sink.clone() })
        }
    }
    struct NoAckConsumer {
        shard: ShardId,
        sink: Sink,
    }
    #[async_trait::async_trait]
    impl AsyncShardConsumer for NoAckConsumer {
        async fn deliver(&mut self, records: &[Record]) -> Result<Option<String>, WorkerError> {
            let mut m = self.sink.lock().unwrap();
            for r in records {
                m.entry(self.shard.clone()).or_default().push(r.seq.clone());
            }
            Ok(None) // delivered, but not durably checkpointed
        }
        async fn shard_ended(&mut self) -> Result<(), WorkerError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn fleet_without_ack_does_not_advance_checkpoint() {
        // Because the consumer never acks, the durable checkpoint never advances,
        // so an open shard is re-read from TRIM_HORIZON every cycle. Over 3
        // cycles the same 3 records are re-delivered — proving the None path
        // holds the lease but does NOT persist progress (the safe, at-least-once
        // behavior a stuck/slow client would produce).
        let source = OpenSource { records: vec![rec("s0", "1"), rec("s0", "2"), rec("s0", "3")] };
        let sink: Sink = Arc::new(Mutex::new(HashMap::new()));
        let factory = Arc::new(NoAckFactory { sink: sink.clone() });
        let fleet = Fleet::new(
            source,
            FakeLeases::default(),
            factory,
            FleetConfig { owner: "w1".into(), max_leases: 100, lease_duration_ms: 100_000, poll_interval_ms: 1 },
        );

        fleet.run_until_complete(3).await.unwrap();

        let m = sink.lock().unwrap();
        assert_eq!(
            m.get("s0").unwrap().len(),
            9,
            "3 records re-delivered across 3 cycles (checkpoint never advanced)"
        );
    }

    #[tokio::test]
    async fn release_owned_clears_our_leases_for_fast_failover() {
        let source = FakeSource { metas: vec![], data: HashMap::new() };
        let leases = FakeLeases::default();
        {
            let mut rows = leases.rows.lock().unwrap();
            rows.insert("mine".into(), State { owner: Some("w1".into()), counter: 3, completed: false, checkpoint: None });
            rows.insert("theirs".into(), State { owner: Some("w2".into()), counter: 1, completed: false, checkpoint: None });
            rows.insert("done".into(), State { owner: Some("w1".into()), counter: 5, completed: true, checkpoint: None });
        }
        let sink: Sink = Arc::new(Mutex::new(HashMap::new()));
        let fleet = Fleet::new(
            source,
            leases,
            Arc::new(SyncConsumerFactory::new(Arc::new(RecordingFactory { sink }))),
            FleetConfig { owner: "w1".into(), max_leases: 100, lease_duration_ms: 1000, poll_interval_ms: 1 },
        );

        let released = fleet.release_owned().await.unwrap();
        assert_eq!(released, 1, "only our own, non-completed lease is released");

        let rows = fleet.leases.rows.lock().unwrap();
        assert!(rows["mine"].owner.is_none(), "our lease is now unowned");
        assert_eq!(rows["mine"].counter, 4, "counter bumped under the optimistic lock");
        assert_eq!(rows["theirs"].owner.as_deref(), Some("w2"), "another worker's lease untouched");
        assert!(rows["done"].owner.is_some(), "a completed lease is not released");
    }
}
