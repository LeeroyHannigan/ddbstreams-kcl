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

use crate::{
    eligible, AsyncLeaseStore, AsyncShardConsumer, AsyncStreamSource, ShardConsumerFactory,
    WorkerError,
};
use amazon_dynamodb_streams_consumer_core::coordinator::{LeaseCoordinator, RawLease};
use amazon_dynamodb_streams_consumer_core::leader::{shard_metas_from_leases, LEADER_LEASE_KEY};
use amazon_dynamodb_streams_consumer_core::metrics::{noop_sink, ShardMetrics, SharedMetricsSink};
use amazon_dynamodb_streams_consumer_core::{child_seed_checkpoint, InitialPosition, ShardId};
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
    /// Where freshly-seeded shards begin when they have no checkpoint yet
    /// (default TRIM_HORIZON). Applied by the shard-sync leader at seed time;
    /// reshard children inherit it only when a parent completed without ever
    /// processing a record (see core `child_seed_checkpoint`).
    pub initial_position: InitialPosition,
}

/// This worker's bid for shard-sync **leadership**, carried across coordination
/// cycles. Exactly one live worker in the fleet is the leader; only the leader
/// calls `DescribeStream` and publishes discovered shards to the lease table
/// (see [`Fleet::run_cycle`] and core `leader`). Leadership reuses the same
/// optimistic-lock + expiry machinery as shard leases: a reserved sentinel lease
/// ([`LEADER_LEASE_KEY`]) tracked by an internal single-slot [`LeaseCoordinator`]
/// for expiry detection.
pub struct Leadership {
    owner: String,
    /// Single-slot coordinator over the sentinel lease — reused purely for its
    /// tested counter-freshness/expiry logic.
    coord: LeaseCoordinator,
    /// Counter we hold while leader (for renewals).
    counter: u64,
}

impl Leadership {
    pub fn new(owner: impl Into<String>, lease_duration_ms: u64) -> Self {
        let owner = owner.into();
        Self {
            coord: LeaseCoordinator::new(owner.clone(), 1, lease_duration_ms),
            owner,
            counter: 0,
        }
    }

    /// Decide + attempt to hold leadership for this cycle, given a fresh scan of
    /// the lease table. Returns `true` iff this worker is the leader now (and is
    /// therefore the only one that should call `DescribeStream`).
    ///
    /// Race-safety: a vacant sentinel is claimed create-if-absent (one winner);
    /// an *expired* leader is stolen conditioned on the counter we observed (a
    /// revived leader that heartbeated advances the counter and defeats the
    /// steal); a *live* leader owned by someone else is left alone.
    async fn step<L: AsyncLeaseStore + ?Sized>(
        &mut self,
        leases: &L,
        rows: &[RawLease],
        now_ms: u64,
    ) -> bool {
        let leader_row = rows.iter().find(|r| r.lease_key == LEADER_LEASE_KEY);
        // Feed only the sentinel to the coordinator to maintain freshness and
        // obtain the expiry decision (empty slice if it does not exist yet).
        let sentinel: Vec<RawLease> = leader_row.cloned().into_iter().collect();
        let expired_takeable = self
            .coord
            .tick(&sentinel, now_ms)
            .iter()
            .any(|k| k == LEADER_LEASE_KEY);

        let i_own = leader_row.and_then(|r| r.owner.as_deref()) == Some(self.owner.as_str());
        if i_own {
            let base = leader_row.map(|r| r.lease_counter).unwrap_or(self.counter);
            match leases.renew(LEADER_LEASE_KEY, &self.owner, base).await {
                Ok(c) => {
                    self.counter = c;
                    true
                }
                Err(_) => false, // lost leadership; another worker will take over
            }
        } else if leader_row.is_none() {
            // Vacant → create-if-absent.
            match leases
                .try_acquire_leadership(LEADER_LEASE_KEY, &self.owner, None)
                .await
            {
                Ok(Some(c)) => {
                    self.counter = c;
                    true
                }
                _ => false,
            }
        } else if expired_takeable {
            // Expired → steal, conditioned on the counter we last observed.
            let seen = leader_row.map(|r| r.lease_counter).unwrap_or(0);
            match leases
                .try_acquire_leadership(LEADER_LEASE_KEY, &self.owner, Some(seen))
                .await
            {
                Ok(Some(c)) => {
                    self.counter = c;
                    true
                }
                _ => false,
            }
        } else {
            false // a live leader owned by another worker
        }
    }
}

pub struct Fleet<S, L> {
    source: Arc<S>,
    leases: Arc<L>,
    factory: Arc<dyn ShardConsumerFactory>,
    config: FleetConfig,
    /// Leader-only incremental-sync bookkeeping (see [`Fleet::run_cycle`]).
    sync_state: std::sync::Mutex<SyncState>,
    /// Metrics sink (default no-op). Emits per-batch lag/throughput,
    /// shard-lifecycle, and DescribeStream events. Set via [`Fleet::with_metrics`].
    metrics: SharedMetricsSink,
}

/// Shard-sync progress tracked by the leader so it can avoid full `DescribeStream`
/// re-scans: after a one-time seed, it discovers a completed parent's children via
/// the `CHILD_SHARDS` filter exactly once.
#[derive(Default)]
struct SyncState {
    /// Whether the one-time full `DescribeStream` seed (root shards) has run.
    seeded: bool,
    /// Completed parents whose children we've already fetched via `CHILD_SHARDS`
    /// (guards childless stream-tail shards from being re-queried every cycle).
    child_synced: HashSet<ShardId>,
}

impl<S, L> Fleet<S, L>
where
    S: AsyncStreamSource + Send + Sync + 'static,
    L: AsyncLeaseStore + Send + Sync + 'static,
{
    pub fn new(
        source: S,
        leases: L,
        factory: Arc<dyn ShardConsumerFactory>,
        config: FleetConfig,
    ) -> Self {
        Self {
            source: Arc::new(source),
            leases: Arc::new(leases),
            factory,
            config,
            sync_state: std::sync::Mutex::new(SyncState::default()),
            metrics: noop_sink(),
        }
    }

    /// Attach a metrics sink (OTLP/OTEL, CloudWatch EMF, or a binding callback).
    /// Defaults to a no-op sink, so metrics are opt-in and cost nothing unless set.
    pub fn with_metrics(mut self, metrics: SharedMetricsSink) -> Self {
        self.metrics = metrics;
        self
    }

    /// Run coordination cycles until every shard's lease is complete or
    /// `max_cycles` is reached (drain model for a bounded/closing shard set; a
    /// long-running consumer loops [`Fleet::run_cycle`] forever with backoff).
    pub async fn run_until_complete(&self, max_cycles: usize) -> Result<(), WorkerError> {
        let mut coordinator = LeaseCoordinator::new(
            self.config.owner.clone(),
            self.config.max_leases,
            self.config.lease_duration_ms,
        );
        let mut leadership =
            Leadership::new(self.config.owner.clone(), self.config.lease_duration_ms);
        let start = Instant::now();
        for _ in 0..max_cycles {
            let now_ms = start.elapsed().as_millis() as u64;
            if self
                .run_cycle(&mut coordinator, &mut leadership, now_ms)
                .await?
            {
                return Ok(());
            }
        }
        Ok(())
    }

    /// One coordination cycle. Returns `true` when all shards are complete.
    ///
    /// Shard discovery is **leader-gated**: only the elected leader calls
    /// `DescribeStream` and publishes newly-eligible shards as lease rows (each
    /// carrying its parents). Every worker — leader and follower alike — then
    /// reconstructs the shard graph from the lease table it already scans, so a
    /// follower issues *zero* `DescribeStream` calls. This is the KCL 3 model
    /// (one central syncer) rather than the KCLv1 model (every worker syncs),
    /// collapsing `DescribeStream` volume from (workers × cycles) to (1 × cycles).
    pub async fn run_cycle(
        &self,
        coordinator: &mut LeaseCoordinator,
        leadership: &mut Leadership,
        now_ms: u64,
    ) -> Result<bool, WorkerError> {
        // 0) Leadership + leader-only shard sync.
        let rows = self.leases.list().await?;
        if leadership.step(&*self.leases, &rows, now_ms).await {
            self.leader_shard_sync(&rows).await?;
        }

        // 1) Decide + claim this worker's share (sentinel lease excluded from
        //    shard coordination).
        let shard_rows: Vec<RawLease> = self
            .leases
            .list()
            .await?
            .into_iter()
            .filter(|r| r.lease_key != LEADER_LEASE_KEY)
            .collect();
        for key in coordinator.tick(&shard_rows, now_ms) {
            let _ = self.leases.acquire(&key, &self.config.owner).await; // best-effort
        }

        // 2) Re-read ownership + completion; rebuild the shard graph from leases
        //    (NOT DescribeStream — the leader already published it).
        let shard_rows: Vec<RawLease> = self
            .leases
            .list()
            .await?
            .into_iter()
            .filter(|r| r.lease_key != LEADER_LEASE_KEY)
            .collect();
        let owner = self.config.owner.as_str();
        // shard -> (lease counter, resume checkpoint). The checkpoint lets a
        // task resume where the last owner left off instead of re-reading from
        // TRIM_HORIZON — essential for correctness across cycles and, critically,
        // across a process restart (the in-memory iterator is gone, but the
        // persisted checkpoint survives).
        let owned: std::collections::HashMap<ShardId, (u64, Option<String>)> = shard_rows
            .iter()
            .filter(|r| r.owner.as_deref() == Some(owner) && !r.completed)
            .map(|r| (r.lease_key.clone(), (r.lease_counter, r.checkpoint.clone())))
            .collect();
        let completed: HashSet<ShardId> = shard_rows
            .iter()
            .filter(|r| r.completed)
            .map(|r| r.lease_key.clone())
            .collect();

        let shards = shard_metas_from_leases(&shard_rows);
        if !shards.is_empty() && shards.iter().all(|m| completed.contains(&m.id)) {
            return Ok(true);
        }

        // 3) Run one concurrent task per owned + eligible shard.
        let mut set: JoinSet<()> = JoinSet::new();
        for meta in &shards {
            let Some((counter, checkpoint)) = owned.get(&meta.id).cloned() else {
                continue;
            };
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
                metrics: self.metrics.clone(),
            };
            set.spawn(async move {
                let _ = process_shard(src, lease, consumer, task).await;
            });
        }
        while set.join_next().await.is_some() {}
        Ok(false)
    }

    /// Leader-only shard discovery. Publishes newly-eligible shards as leases
    /// (each carrying its parents), gating a child until its parents complete
    /// (KCL `HierarchicalShardSyncer` semantics).
    ///
    /// Efficiency: a **one-time** full `DescribeStream` seeds the root shards;
    /// thereafter children are discovered per shard-end via the `CHILD_SHARDS`
    /// `ShardFilter` ([`AsyncStreamSource::describe_child_shards`]) — so a stable
    /// topology issues ZERO `DescribeStream` calls. A completed parent that
    /// already has published children is skipped (cheap leader-failover), and a
    /// childless (stream-tail) parent is queried at most once per leader via
    /// `child_synced`. On a filtered-call error we fall back to a full describe,
    /// matching the adapter.
    async fn leader_shard_sync(&self, rows: &[RawLease]) -> Result<(), WorkerError> {
        let completed: HashSet<ShardId> = rows
            .iter()
            .filter(|r| r.completed)
            .map(|r| r.lease_key.clone())
            .collect();
        let existing: HashSet<ShardId> = rows.iter().map(|r| r.lease_key.clone()).collect();
        // Parents that already have >=1 published child lease — no need to
        // re-discover their children (survives leader failover without schema).
        let parents_with_children: HashSet<ShardId> = rows
            .iter()
            .flat_map(|r| r.parents.iter().cloned())
            .collect();

        let (seeded, already_synced) = {
            let s = self.sync_state.lock().unwrap();
            (s.seeded, s.child_synced.clone())
        };

        if !seeded {
            // One-time seed: full DescribeStream → create the root shards (those
            // whose parents are all complete; at bootstrap that's the roots).
            // Roots begin at the configured initial position.
            let seed_cp = self.config.initial_position.seed_checkpoint();
            self.metrics.on_describe_stream();
            for m in self.source.describe_shards().await? {
                if m.parents.iter().all(|p| completed.contains(p)) && !existing.contains(&m.id) {
                    let _ = self
                        .leases
                        .create_shard_lease(&m.id, &m.parents, seed_cp.as_deref())
                        .await;
                }
            }
            self.sync_state.lock().unwrap().seeded = true;
            return Ok(());
        }

        // Incremental: for each newly-completed parent, fetch ONLY its children.
        // A child inherits its parents' start mode only if every parent completed
        // without processing a record (core `child_seed_checkpoint`); otherwise it
        // begins at TRIM_HORIZON so nothing is skipped across the reshard.
        let child_cp = |parents: &[String]| -> Option<String> {
            let cps: Vec<Option<String>> = parents
                .iter()
                .map(|p| {
                    rows.iter()
                        .find(|r| &r.lease_key == p)
                        .and_then(|r| r.checkpoint.clone())
                })
                .collect();
            child_seed_checkpoint(&cps)
        };
        let mut newly_synced: Vec<ShardId> = Vec::new();
        for parent in &completed {
            if already_synced.contains(parent) || parents_with_children.contains(parent) {
                continue;
            }
            self.metrics.on_describe_stream();
            match self.source.describe_child_shards(parent).await {
                Ok(children) => {
                    for c in children {
                        if !existing.contains(&c.id) {
                            let cp = child_cp(&c.parents);
                            let _ = self
                                .leases
                                .create_shard_lease(&c.id, &c.parents, cp.as_deref())
                                .await;
                        }
                    }
                    newly_synced.push(parent.clone());
                }
                Err(_) => {
                    // Filtered call failed → fall back to a full describe this
                    // cycle (adapter behavior). Do NOT mark synced, so we retry.
                    self.metrics.on_describe_stream();
                    if let Ok(metas) = self.source.describe_shards().await {
                        for m in metas {
                            if m.parents.iter().all(|p| completed.contains(p))
                                && !existing.contains(&m.id)
                            {
                                let cp = child_cp(&m.parents);
                                let _ = self
                                    .leases
                                    .create_shard_lease(&m.id, &m.parents, cp.as_deref())
                                    .await;
                            }
                        }
                    }
                }
            }
        }
        if !newly_synced.is_empty() {
            let mut s = self.sync_state.lock().unwrap();
            for p in newly_synced {
                s.child_synced.insert(p);
            }
        }
        Ok(())
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
            if r.owner.as_deref() == Some(owner)
                && !r.completed
                && self
                    .leases
                    .release(&r.lease_key, owner, r.lease_counter)
                    .await
                    .is_ok()
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
    metrics: SharedMetricsSink,
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
    let ShardTask {
        owner,
        shard,
        mut counter,
        checkpoint,
        poll_interval_ms,
        metrics,
    } = task;
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
            // Record delivered-batch metrics: throughput + per-shard lag
            // (MillisBehindLatest). Emitted after successful delivery.
            metrics.on_batch(&ShardMetrics {
                shard_id: &shard,
                records: batch.records.len() as u64,
                bytes: batch.records.iter().map(|r| r.data.len() as u64).sum(),
                millis_behind_latest: batch.millis_behind_latest,
            });
            after = Some(last);
        }
        if batch.shard_end {
            let _ = consumer.shard_ended().await;
            let _ = leases.mark_complete(&shard, &owner, counter).await;
            metrics.on_shard_end(&shard);
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
    use crate::{
        AsyncShardConsumer, LeaseHandle, LeaseView, ShardConsumerFactory, SyncConsumerFactory,
    };
    use amazon_dynamodb_streams_consumer_core::coordinator::RawLease;
    use amazon_dynamodb_streams_consumer_core::{
        Record, RecordBatch, RecordProcessor, RecordProcessorFactory, ShardMeta,
    };
    use std::collections::HashMap;
    use std::sync::Mutex;

    fn rec(shard: &str, seq: &str) -> Record {
        Record {
            shard_id: shard.into(),
            seq: seq.into(),
            data: vec![],
        }
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
        async fn get_records(
            &self,
            shard: &str,
            after: Option<String>,
        ) -> Result<RecordBatch, WorkerError> {
            let all = self.data.get(shard).cloned().unwrap_or_default();
            let records = match after {
                None => all,
                Some(tok) => match all.iter().position(|r| r.seq == tok) {
                    Some(i) => all[i + 1..].to_vec(),
                    None => all,
                },
            };
            Ok(RecordBatch {
                records,
                shard_end: true,
                millis_behind_latest: None,
            })
        }
    }

    #[derive(Default)]
    struct State {
        owner: Option<String>,
        counter: u64,
        completed: bool,
        checkpoint: Option<String>,
        parents: Vec<String>,
    }
    #[derive(Default)]
    struct FakeLeases {
        rows: Mutex<HashMap<String, State>>,
    }
    #[async_trait::async_trait]
    impl AsyncLeaseStore for FakeLeases {
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
            _s: &str,
        ) -> Result<u64, WorkerError> {
            let mut rows = self.rows.lock().unwrap();
            let r = rows.get_mut(key).ok_or("no lease")?;
            r.checkpoint = Some(_s.to_string());
            r.counter = counter + 1;
            Ok(r.counter)
        }
        async fn mark_complete(&self, key: &str, _o: &str, _c: u64) -> Result<(), WorkerError> {
            self.rows
                .lock()
                .unwrap()
                .get_mut(key)
                .ok_or("no lease")?
                .completed = true;
            Ok(())
        }
        async fn release(&self, key: &str, _o: &str, counter: u64) -> Result<(), WorkerError> {
            let mut rows = self.rows.lock().unwrap();
            let r = rows.get_mut(key).ok_or("no lease")?;
            r.owner = None;
            r.counter = counter + 1;
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
                .or_insert_with(|| State {
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
                        State {
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

    type Sink = Arc<Mutex<HashMap<String, Vec<String>>>>;
    /// (shard_id, records, bytes, millis_behind_latest) captured per batch.
    type CapturedBatch = (String, u64, u64, Option<i64>);
    struct RecordingFactory {
        sink: Sink,
    }
    impl RecordProcessorFactory for RecordingFactory {
        fn create(&self, _shard: &ShardId) -> Box<dyn RecordProcessor + Send> {
            Box::new(RecordingProc {
                shard: String::new(),
                sink: self.sink.clone(),
                inited: false,
            })
        }
    }
    struct RecordingProc {
        shard: String,
        sink: Sink,
        inited: bool,
    }
    impl RecordProcessor for RecordingProc {
        fn initialize(&mut self, s: &ShardId) {
            self.shard = s.clone();
            self.inited = true;
        }
        fn process_records(&mut self, rs: &[Record]) {
            let mut m = self.sink.lock().unwrap();
            for r in rs {
                m.entry(self.shard.clone()).or_default().push(r.seq.clone());
            }
        }
        fn shard_ended(&mut self, _s: &ShardId) {
            assert!(self.inited);
        }
    }

    #[tokio::test]
    async fn fleet_processes_all_shards_concurrently_in_order() {
        let mut data = HashMap::new();
        data.insert("s0".to_string(), vec![rec("s0", "1"), rec("s0", "2")]);
        data.insert("s1".to_string(), vec![rec("s1", "3"), rec("s1", "4")]);
        data.insert("s2".to_string(), vec![rec("s2", "5")]);
        let source = FakeSource {
            metas: vec![
                ShardMeta {
                    id: "s0".into(),
                    parents: vec![],
                },
                ShardMeta {
                    id: "s1".into(),
                    parents: vec![],
                },
                ShardMeta {
                    id: "s2".into(),
                    parents: vec![],
                },
            ],
            data,
        };
        let sink: Sink = Arc::new(Mutex::new(HashMap::new()));
        let factory = Arc::new(RecordingFactory { sink: sink.clone() });
        let fleet = Fleet::new(
            source,
            FakeLeases::default(),
            Arc::new(SyncConsumerFactory::new(factory)),
            FleetConfig {
                owner: "w1".into(),
                max_leases: 100,
                lease_duration_ms: 1000,
                poll_interval_ms: 1,
                initial_position: InitialPosition::default(),
            },
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
                ShardMeta {
                    id: "c".into(),
                    parents: vec!["p".into()],
                },
                ShardMeta {
                    id: "p".into(),
                    parents: vec![],
                },
            ],
            data,
        };
        let sink: Sink = Arc::new(Mutex::new(HashMap::new()));
        let factory = Arc::new(RecordingFactory { sink: sink.clone() });
        let fleet = Fleet::new(
            source,
            FakeLeases::default(),
            Arc::new(SyncConsumerFactory::new(factory)),
            FleetConfig {
                owner: "w1".into(),
                max_leases: 100,
                lease_duration_ms: 1000,
                poll_interval_ms: 1,
                initial_position: InitialPosition::default(),
            },
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
            Ok(vec![ShardMeta {
                id: "s0".into(),
                parents: vec![],
            }])
        }
        async fn get_records(
            &self,
            _shard: &str,
            after: Option<String>,
        ) -> Result<RecordBatch, WorkerError> {
            let records = match after {
                None => self.records.clone(),
                Some(tok) => match self.records.iter().position(|r| r.seq == tok) {
                    Some(i) => self.records[i + 1..].to_vec(),
                    None => self.records.clone(),
                },
            };
            Ok(RecordBatch {
                records,
                shard_end: false,
                millis_behind_latest: None,
            })
        }
    }

    #[tokio::test]
    async fn fleet_resumes_from_checkpoint_no_redelivery() {
        // A shard that never closes: across multiple cycles it must NOT re-deliver
        // records already past the persisted checkpoint. This is the correctness
        // guarantee that also holds across a process restart (the checkpoint lives
        // in the lease table, not in memory).
        let source = OpenSource {
            records: vec![rec("s0", "1"), rec("s0", "2"), rec("s0", "3")],
        };
        let sink: Sink = Arc::new(Mutex::new(HashMap::new()));
        let factory = Arc::new(RecordingFactory { sink: sink.clone() });
        let fleet = Fleet::new(
            source,
            FakeLeases::default(),
            Arc::new(SyncConsumerFactory::new(factory)),
            FleetConfig {
                owner: "w1".into(),
                max_leases: 100,
                lease_duration_ms: 100_000,
                poll_interval_ms: 1,
                initial_position: InitialPosition::default(),
            },
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
            Box::new(NoAckConsumer {
                shard: shard.clone(),
                sink: self.sink.clone(),
            })
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
        let source = OpenSource {
            records: vec![rec("s0", "1"), rec("s0", "2"), rec("s0", "3")],
        };
        let sink: Sink = Arc::new(Mutex::new(HashMap::new()));
        let factory = Arc::new(NoAckFactory { sink: sink.clone() });
        let fleet = Fleet::new(
            source,
            FakeLeases::default(),
            factory,
            FleetConfig {
                owner: "w1".into(),
                max_leases: 100,
                lease_duration_ms: 100_000,
                poll_interval_ms: 1,
                initial_position: InitialPosition::default(),
            },
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
        let source = FakeSource {
            metas: vec![],
            data: HashMap::new(),
        };
        let leases = FakeLeases::default();
        {
            let mut rows = leases.rows.lock().unwrap();
            rows.insert(
                "mine".into(),
                State {
                    owner: Some("w1".into()),
                    counter: 3,
                    completed: false,
                    checkpoint: None,
                    parents: vec![],
                },
            );
            rows.insert(
                "theirs".into(),
                State {
                    owner: Some("w2".into()),
                    counter: 1,
                    completed: false,
                    checkpoint: None,
                    parents: vec![],
                },
            );
            rows.insert(
                "done".into(),
                State {
                    owner: Some("w1".into()),
                    counter: 5,
                    completed: true,
                    checkpoint: None,
                    parents: vec![],
                },
            );
        }
        let sink: Sink = Arc::new(Mutex::new(HashMap::new()));
        let fleet = Fleet::new(
            source,
            leases,
            Arc::new(SyncConsumerFactory::new(Arc::new(RecordingFactory {
                sink,
            }))),
            FleetConfig {
                owner: "w1".into(),
                max_leases: 100,
                lease_duration_ms: 1000,
                poll_interval_ms: 1,
                initial_position: InitialPosition::default(),
            },
        );

        let released = fleet.release_owned().await.unwrap();
        assert_eq!(released, 1, "only our own, non-completed lease is released");

        let rows = fleet.leases.rows.lock().unwrap();
        assert!(rows["mine"].owner.is_none(), "our lease is now unowned");
        assert_eq!(
            rows["mine"].counter, 4,
            "counter bumped under the optimistic lock"
        );
        assert_eq!(
            rows["theirs"].owner.as_deref(),
            Some("w2"),
            "another worker's lease untouched"
        );
        assert!(
            rows["done"].owner.is_some(),
            "a completed lease is not released"
        );
    }

    /// A source that records how many times `describe_shards` (DescribeStream)
    /// is called — the metric the leader-based syncer exists to minimize.
    struct CountingSource {
        metas: Vec<ShardMeta>,
        data: HashMap<ShardId, Vec<Record>>,
        describe_calls: Arc<std::sync::atomic::AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl AsyncStreamSource for CountingSource {
        async fn describe_shards(&self) -> Result<Vec<ShardMeta>, WorkerError> {
            self.describe_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(self.metas.clone())
        }
        async fn get_records(
            &self,
            shard: &str,
            after: Option<String>,
        ) -> Result<RecordBatch, WorkerError> {
            let all = self.data.get(shard).cloned().unwrap_or_default();
            let records = match after {
                None => all,
                Some(tok) => match all.iter().position(|r| r.seq == tok) {
                    Some(i) => all[i + 1..].to_vec(),
                    None => all,
                },
            };
            Ok(RecordBatch {
                records,
                shard_end: true,
                millis_behind_latest: None,
            })
        }
    }

    /// A follower (another worker already holds a live leader lease) must NOT
    /// call DescribeStream at all — it reconstructs the shard graph from the
    /// lease rows the leader published, and still works its share of shards.
    /// This is the whole point of the leader-based syncer vs KCLv1.
    #[tokio::test]
    async fn follower_does_not_call_describe_stream() {
        let describe_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut data = HashMap::new();
        data.insert("s0".to_string(), vec![rec("s0", "1")]);
        data.insert("s1".to_string(), vec![rec("s1", "2")]);
        let source = CountingSource {
            // If the follower ever (incorrectly) called describe, it'd see these.
            metas: vec![
                ShardMeta {
                    id: "s0".into(),
                    parents: vec![],
                },
                ShardMeta {
                    id: "s1".into(),
                    parents: vec![],
                },
            ],
            data,
            describe_calls: describe_calls.clone(),
        };

        let leases = FakeLeases::default();
        {
            let mut rows = leases.rows.lock().unwrap();
            // Another worker is the live leader.
            rows.insert(
                LEADER_LEASE_KEY.to_string(),
                State {
                    owner: Some("leader-x".into()),
                    counter: 5,
                    ..Default::default()
                },
            );
            // ...and it already published two shards as unowned leases.
            rows.insert("s0".into(), State::default());
            rows.insert("s1".into(), State::default());
        }

        let sink: Sink = Arc::new(Mutex::new(HashMap::new()));
        let factory = Arc::new(RecordingFactory { sink: sink.clone() });
        let fleet = Fleet::new(
            source,
            leases,
            Arc::new(SyncConsumerFactory::new(factory)),
            FleetConfig {
                owner: "w2".into(),
                max_leases: 100,
                lease_duration_ms: 1000,
                poll_interval_ms: 1,
                initial_position: InitialPosition::default(),
            },
        );

        let mut coordinator = LeaseCoordinator::new("w2".to_string(), 100, 1000);
        let mut leadership = Leadership::new("w2", 1000);
        // now_ms=0 → first sighting of the leader lease → treated as fresh/alive,
        // so w2 does not win leadership.
        fleet
            .run_cycle(&mut coordinator, &mut leadership, 0)
            .await
            .unwrap();

        assert_eq!(
            describe_calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "follower must not call DescribeStream"
        );
        let m = sink.lock().unwrap();
        assert_eq!(m.get("s0").unwrap(), &vec!["1"]);
        assert_eq!(m.get("s1").unwrap(), &vec!["2"]);
    }

    /// The elected leader calls DescribeStream and publishes shards (with
    /// parents) into the lease table, gating a child lease until its parent
    /// completes — so a single worker drives the whole graph via one syncer.
    #[tokio::test]
    async fn leader_publishes_shards_and_gates_child_on_parent() {
        let describe_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut data = HashMap::new();
        data.insert("p".to_string(), vec![rec("p", "1")]);
        data.insert("c".to_string(), vec![rec("c", "2")]);
        let source = CountingSource {
            metas: vec![
                ShardMeta {
                    id: "c".into(),
                    parents: vec!["p".into()],
                },
                ShardMeta {
                    id: "p".into(),
                    parents: vec![],
                },
            ],
            data,
            describe_calls: describe_calls.clone(),
        };
        let leases = FakeLeases::default();
        let sink: Sink = Arc::new(Mutex::new(HashMap::new()));
        let factory = Arc::new(RecordingFactory { sink: sink.clone() });
        let fleet = Fleet::new(
            source,
            leases,
            Arc::new(SyncConsumerFactory::new(factory)),
            FleetConfig {
                owner: "w1".into(),
                max_leases: 100,
                lease_duration_ms: 1000,
                poll_interval_ms: 1,
                initial_position: InitialPosition::default(),
            },
        );
        fleet.run_until_complete(10).await.unwrap();

        // The leader ran shard sync (called DescribeStream at least once).
        assert!(describe_calls.load(std::sync::atomic::Ordering::SeqCst) >= 1);
        // Both the parent and (later) the child were published + drained.
        assert!(fleet.leases.rows.lock().unwrap().contains_key("c"));
        let m = sink.lock().unwrap();
        assert_eq!(m.get("p").unwrap(), &vec!["1"]);
        assert_eq!(m.get("c").unwrap(), &vec!["2"]);
    }

    /// A source that counts full `describe_shards` (DescribeStream) calls
    /// separately from targeted `describe_child_shards` (CHILD_SHARDS) calls —
    /// so we can assert the leader avoids full re-scans.
    struct TrackingSource {
        metas: Vec<ShardMeta>,
        data: HashMap<ShardId, Vec<Record>>,
        full_describes: Arc<std::sync::atomic::AtomicUsize>,
        child_describes: Arc<std::sync::atomic::AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl AsyncStreamSource for TrackingSource {
        async fn describe_shards(&self) -> Result<Vec<ShardMeta>, WorkerError> {
            self.full_describes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(self.metas.clone())
        }
        async fn get_records(
            &self,
            shard: &str,
            after: Option<String>,
        ) -> Result<RecordBatch, WorkerError> {
            let all = self.data.get(shard).cloned().unwrap_or_default();
            let records = match after {
                None => all,
                Some(tok) => match all.iter().position(|r| r.seq == tok) {
                    Some(i) => all[i + 1..].to_vec(),
                    None => all,
                },
            };
            Ok(RecordBatch {
                records,
                shard_end: true,
                millis_behind_latest: None,
            })
        }
        // Override: the efficient CHILD_SHARDS path (counted separately, and
        // crucially does NOT do a full re-scan).
        async fn describe_child_shards(&self, parent: &str) -> Result<Vec<ShardMeta>, WorkerError> {
            self.child_describes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(self
                .metas
                .iter()
                .filter(|m| m.parents.iter().any(|p| p == parent))
                .cloned()
                .collect())
        }
    }

    /// Draining a full reshard (root → child → grandchild) must cost exactly ONE
    /// full DescribeStream (the one-time seed); every subsequent child is found
    /// via the targeted CHILD_SHARDS path. This is the DescribeStream-efficiency
    /// guarantee that KCLv1 (full scan every worker every cycle) fails.
    #[tokio::test]
    async fn leader_seeds_once_then_uses_child_shards_filter() {
        let full = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let child = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut data = HashMap::new();
        data.insert("g0".to_string(), vec![rec("g0", "1")]);
        data.insert("g1".to_string(), vec![rec("g1", "2")]);
        data.insert("g2".to_string(), vec![rec("g2", "3")]);
        let source = TrackingSource {
            metas: vec![
                ShardMeta {
                    id: "g0".into(),
                    parents: vec![],
                },
                ShardMeta {
                    id: "g1".into(),
                    parents: vec!["g0".into()],
                },
                ShardMeta {
                    id: "g2".into(),
                    parents: vec!["g1".into()],
                },
            ],
            data,
            full_describes: full.clone(),
            child_describes: child.clone(),
        };
        let sink: Sink = Arc::new(Mutex::new(HashMap::new()));
        let factory = Arc::new(RecordingFactory { sink: sink.clone() });
        let fleet = Fleet::new(
            source,
            FakeLeases::default(),
            Arc::new(SyncConsumerFactory::new(factory)),
            FleetConfig {
                owner: "w1".into(),
                max_leases: 100,
                lease_duration_ms: 1000,
                poll_interval_ms: 1,
                initial_position: InitialPosition::default(),
            },
        );
        fleet.run_until_complete(10).await.unwrap();

        // Whole 3-level lineage drained in order...
        let m = sink.lock().unwrap();
        assert_eq!(m.get("g0").unwrap(), &vec!["1"]);
        assert_eq!(m.get("g1").unwrap(), &vec!["2"]);
        assert_eq!(m.get("g2").unwrap(), &vec!["3"]);
        // ...with exactly ONE full DescribeStream (the seed)...
        assert_eq!(
            full.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "exactly one full DescribeStream (seed); no full re-scans"
        );
        // ...and children discovered via the CHILD_SHARDS filter (g0→g1, g1→g2).
        assert!(
            child.load(std::sync::atomic::Ordering::SeqCst) >= 2,
            "children found via CHILD_SHARDS, not full scans"
        );
    }

    /// A source that reports a fixed per-batch lag, to prove the fleet forwards
    /// `millis_behind_latest` (MillisBehindLatest) and throughput to the sink.
    struct LagSource;
    #[async_trait::async_trait]
    impl AsyncStreamSource for LagSource {
        async fn describe_shards(&self) -> Result<Vec<ShardMeta>, WorkerError> {
            Ok(vec![ShardMeta {
                id: "s0".into(),
                parents: vec![],
            }])
        }
        async fn get_records(
            &self,
            _shard: &str,
            after: Option<String>,
        ) -> Result<RecordBatch, WorkerError> {
            // One batch of 2 records, then SHARD_END; carry a lag of 1234ms.
            if after.is_some() {
                return Ok(RecordBatch {
                    records: vec![],
                    shard_end: true,
                    millis_behind_latest: None,
                });
            }
            Ok(RecordBatch {
                records: vec![rec("s0", "1"), rec("s0", "2")],
                shard_end: false,
                millis_behind_latest: Some(1234),
            })
        }
    }

    #[derive(Default)]
    struct CaptureSink {
        batches: Mutex<Vec<CapturedBatch>>,
        describes: std::sync::atomic::AtomicU64,
        shard_ends: std::sync::atomic::AtomicU64,
    }
    impl amazon_dynamodb_streams_consumer_core::metrics::MetricsSink for CaptureSink {
        fn on_batch(&self, m: &ShardMetrics<'_>) {
            self.batches.lock().unwrap().push((
                m.shard_id.to_string(),
                m.records,
                m.bytes,
                m.millis_behind_latest,
            ));
        }
        fn on_describe_stream(&self) {
            self.describes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        fn on_shard_end(&self, _shard_id: &str) {
            self.shard_ends
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// The fleet forwards per-batch lag/throughput, shard-end, and DescribeStream
    /// events to the attached metrics sink (Model A's data source).
    #[tokio::test]
    async fn fleet_emits_metrics_to_sink() {
        let sink = Arc::new(CaptureSink::default());
        let recording: Sink = Arc::new(Mutex::new(HashMap::new()));
        let factory = Arc::new(RecordingFactory { sink: recording });
        let fleet = Fleet::new(
            LagSource,
            FakeLeases::default(),
            Arc::new(SyncConsumerFactory::new(factory)),
            FleetConfig {
                owner: "w1".into(),
                max_leases: 100,
                lease_duration_ms: 1000,
                poll_interval_ms: 1,
                initial_position: InitialPosition::default(),
            },
        )
        .with_metrics(sink.clone());

        fleet.run_until_complete(5).await.unwrap();

        let batches = sink.batches.lock().unwrap();
        assert_eq!(batches.len(), 1, "one non-empty batch delivered");
        let (shard, records, bytes, lag) = &batches[0];
        assert_eq!(shard, "s0");
        assert_eq!(*records, 2, "2 records");
        assert_eq!(*bytes, 0, "empty payloads in this fake → 0 bytes");
        assert_eq!(*lag, Some(1234), "MillisBehindLatest forwarded to the sink");
        assert!(
            sink.describes.load(std::sync::atomic::Ordering::SeqCst) >= 1,
            "leader DescribeStream counted"
        );
        assert_eq!(
            sink.shard_ends.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "shard-end emitted once"
        );
    }
}
