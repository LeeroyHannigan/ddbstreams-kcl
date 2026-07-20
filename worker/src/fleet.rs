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
use amazon_dynamodb_streams_consumer_core::cleanup::{leases_safe_to_delete, LeaseState};
use amazon_dynamodb_streams_consumer_core::coordinator::{LeaseCoordinator, RawLease};
use amazon_dynamodb_streams_consumer_core::leader::{shard_metas_from_leases, LEADER_LEASE_KEY};
use amazon_dynamodb_streams_consumer_core::metrics::{noop_sink, ShardMetrics, SharedMetricsSink};
use amazon_dynamodb_streams_consumer_core::{
    child_seed_checkpoint, InitialPosition, ShardId, StartPosition,
};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;
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
    /// Optional cap on the number of shards whose records are **processed**
    /// (delivered to the customer consumer) concurrently, across this worker.
    /// `None` ⇒ unbounded (one processing slot per owned shard — the default,
    /// behavior-identical to prior releases). `Some(k)` ⇒ at most `k` concurrent
    /// `deliver` calls; shard *reading* (GetRecords) and lease heartbeats stay
    /// unbounded, so idle shards keep their leases while queued to process.
    /// Set via [`Fleet::with_max_processing_concurrency`]. See `docs/multiplexing-design.md`.
    processing_limit: Option<Arc<ProcessingLimit>>,
}

/// A processing-concurrency cap: a permit pool (`sem`) plus the configured
/// maximum (`max`) tracked explicitly, since [`Semaphore`] does not expose its
/// own ceiling (only currently-free permits). `max` is the source of truth for
/// [`Fleet::set_max_processing_concurrency`] resizes.
struct ProcessingLimit {
    sem: Semaphore,
    max: std::sync::atomic::AtomicUsize,
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
            processing_limit: None,
        }
    }

    /// Attach a metrics sink (OTLP/OTEL, CloudWatch EMF, or a binding callback).
    /// Defaults to a no-op sink, so metrics are opt-in and cost nothing unless set.
    pub fn with_metrics(mut self, metrics: SharedMetricsSink) -> Self {
        self.metrics = metrics;
        self
    }

    /// Bound the number of shards **processed concurrently** to `max` (opt-in).
    ///
    /// `None` (the default) keeps prior behavior: one processing slot per owned
    /// shard, so per-worker footprint grows with the stream's shard count.
    /// `Some(k)` (k ≥ 1) caps concurrent customer `deliver` calls at `k`, making
    /// footprint O(k) independent of shard count while preserving at-least-once,
    /// per-item, and per-shard ordering (a shard is never split; each shard task
    /// is sequential and holds at most one permit). `Some(0)` is treated as
    /// `None` (unbounded) rather than deadlocking.
    ///
    /// Reading (GetRecords) and lease heartbeats are intentionally *not* gated,
    /// so a shard queued for a processing slot keeps its lease and an idle shard
    /// never contends for a permit.
    pub fn with_max_processing_concurrency(mut self, max: Option<usize>) -> Self {
        self.processing_limit = match max {
            Some(k) if k >= 1 => Some(Arc::new(ProcessingLimit {
                sem: Semaphore::new(k),
                max: std::sync::atomic::AtomicUsize::new(k),
            })),
            _ => None,
        };
        self
    }

    /// Online resize of the processing-concurrency cap on a running fleet.
    ///
    /// Grow adds permits immediately; shrink removes the delta at the next batch
    /// boundary (it waits for in-flight slots to free, then forgets them), so it
    /// never interrupts an in-flight `deliver`. Only adjusts a fleet that was
    /// created bounded (`with_max_processing_concurrency(Some(_))`); switching an
    /// unbounded fleet to bounded at runtime is unsupported (returns without
    /// effect) because the slot set is fixed at construction. Call from a single
    /// controller task (concurrent resizes are not serialized against each other).
    pub async fn set_max_processing_concurrency(&self, target: usize) {
        use std::sync::atomic::Ordering;
        let Some(pl) = self.processing_limit.as_ref() else {
            return; // unbounded fleet: nothing to resize
        };
        let target = target.max(1);
        let current = pl.max.load(Ordering::SeqCst);
        if target > current {
            pl.sem.add_permits(target - current);
            pl.max.store(target, Ordering::SeqCst);
        } else if target < current {
            let take = (current - target) as u32;
            // Waits for `take` permits to be free (an in-flight deliver keeps its
            // permit until its batch completes), then permanently removes them.
            if let Ok(permits) = pl.sem.acquire_many(take).await {
                permits.forget();
                pl.max.store(target, Ordering::SeqCst);
            }
        }
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
            // Leader-only: GC completed parent leases whose children are safely
            // processing, so the lease table doesn't grow without bound on a
            // long-running resharding stream.
            self.cleanup_completed_leases(&rows).await;
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
            if self.leases.acquire(&key, &self.config.owner).await.is_ok() {
                self.metrics.on_lease_acquired(&key); // best-effort
            }
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
        self.metrics.on_leases_held(owned.len() as u64);
        if let Some(pl) = self.processing_limit.as_ref() {
            self.metrics.on_max_processing_concurrency(
                pl.max.load(std::sync::atomic::Ordering::SeqCst) as u64,
            );
        }
        let completed: HashSet<ShardId> = shard_rows
            .iter()
            .filter(|r| r.completed)
            .map(|r| r.lease_key.clone())
            .collect();

        let shards = shard_metas_from_leases(&shard_rows);
        if !shards.is_empty() && shards.iter().all(|m| completed.contains(&m.id)) {
            return Ok(true);
        }
        // Shard ids that currently have a lease row. A parent absent here has
        // been GC'd (see cleanup pass) and counts as satisfied for its children.
        let existing: HashSet<ShardId> = shards.iter().map(|m| m.id.clone()).collect();

        // 3) Run one concurrent task per owned + eligible shard.
        let mut set: JoinSet<()> = JoinSet::new();
        for meta in &shards {
            let Some((counter, checkpoint)) = owned.get(&meta.id).cloned() else {
                continue;
            };
            if !eligible(meta, &completed, &existing) {
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
                limit: self.processing_limit.clone(),
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

    /// Leader-only: delete completed parent leases that are safe to GC.
    ///
    /// A completed shard's lease is deleted only once every child lease exists
    /// and is itself processing or completed (core [`leases_safe_to_delete`]),
    /// so lineage is never rediscovered/replayed and no in-flight child is
    /// stranded ([`eligible`] treats a missing parent as satisfied). Best-effort
    /// per lease; the conditional delete no-ops if the row isn't completed.
    async fn cleanup_completed_leases(&self, rows: &[RawLease]) {
        let shards = shard_metas_from_leases(rows);
        let state: std::collections::HashMap<String, LeaseState> = rows
            .iter()
            .filter(|r| r.lease_key != LEADER_LEASE_KEY)
            .map(|r| {
                // "processing" = a real record was checkpointed (past a start
                // sentinel) or the shard completed.
                let processing = r.completed
                    || matches!(
                        StartPosition::from_checkpoint(r.checkpoint.as_deref()),
                        StartPosition::After(_)
                    );
                (
                    r.lease_key.clone(),
                    LeaseState {
                        completed: r.completed,
                        processing,
                    },
                )
            })
            .collect();
        for key in leases_safe_to_delete(&shards, &state) {
            let _ = self.leases.delete_lease(&key).await;
        }
    }

    /// Shard keys this worker currently owns (non-completed leases, excluding the
    /// leader sentinel). Used to dispatch graceful shutdown-requested
    /// notifications before releasing the leases.
    pub async fn owned_shards(&self) -> Result<Vec<ShardId>, WorkerError> {
        let owner = self.config.owner.as_str();
        Ok(self
            .leases
            .list()
            .await?
            .into_iter()
            .filter(|r| {
                r.owner.as_deref() == Some(owner) && !r.completed && r.lease_key != LEADER_LEASE_KEY
            })
            .map(|r| r.lease_key)
            .collect())
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
    /// Shared processing-concurrency cap (`None` ⇒ unbounded). A permit is held
    /// only around `deliver` (+ its checkpoint), not around GetRecords/heartbeat.
    limit: Option<Arc<ProcessingLimit>>,
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
        limit,
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
            // Bound concurrent customer processing: acquire a slot before
            // `deliver` and hold it across the checkpoint, releasing before the
            // next GetRecords. `None` ⇒ unbounded (prior behavior). Reading and
            // heartbeats are deliberately outside this permit. The semaphore is
            // never closed, so acquire cannot fail.
            let _permit = match limit.as_ref() {
                Some(pl) => {
                    let wait_start = Instant::now();
                    let p = pl
                        .sem
                        .acquire()
                        .await
                        .expect("processing semaphore is never closed");
                    metrics
                        .on_processing_slot_wait(&shard, wait_start.elapsed().as_millis() as u64);
                    Some(p)
                }
                None => None,
            };
            // Deliver and let the consumer decide the checkpoint (its ack). A
            // sidecar returns the seq the client durably processed; the sync
            // in-process adapter returns the batch's last seq.
            match consumer.deliver(&batch.records).await {
                Ok(Some(ack)) => match leases.checkpoint(&shard, &owner, counter, &ack).await {
                    Ok(c) => counter = c,
                    Err(_) => {
                        metrics.on_lease_lost(&shard);
                        let _ = consumer.lease_lost().await; // lease lost → notify + stop
                        return Ok(());
                    }
                },
                Ok(None) => {
                    // Delivered but not acked: hold the lease without advancing
                    // the durable checkpoint (heartbeat).
                    match leases.renew(&shard, &owner, counter).await {
                        Ok(c) => counter = c,
                        Err(_) => {
                            metrics.on_lease_lost(&shard);
                            let _ = consumer.lease_lost().await;
                            return Ok(());
                        }
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
    #[derive(Default, Clone)]
    struct FakeLeases {
        rows: Arc<Mutex<HashMap<String, State>>>,
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
    async fn leader_gcs_completed_parent_once_child_processing() {
        // Seed: completed parent `p` (a real record was checkpointed) + child `c`
        // that is actively processing (real checkpoint). The parent lease should
        // be deleted; the child lease must remain.
        let leases = FakeLeases::default();
        {
            let mut rows = leases.rows.lock().unwrap();
            rows.insert(
                "p".into(),
                State {
                    owner: None,
                    counter: 3,
                    completed: true,
                    checkpoint: Some("100000000000000000000000001".into()),
                    parents: vec![],
                },
            );
            rows.insert(
                "c".into(),
                State {
                    owner: Some("w1".into()),
                    counter: 1,
                    completed: false,
                    checkpoint: Some("100000000000000000000000009".into()),
                    parents: vec!["p".into()],
                },
            );
        }
        let source = FakeSource {
            metas: vec![],
            data: HashMap::new(),
        };
        let sink: Sink = Arc::new(Mutex::new(HashMap::new()));
        let fleet = Fleet::new(
            source,
            leases.clone(),
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

        let rows = fleet.leases.list().await.unwrap();
        fleet.cleanup_completed_leases(&rows).await;

        let after = leases.rows.lock().unwrap();
        assert!(
            !after.contains_key("p"),
            "completed parent lease should be GC'd"
        );
        assert!(
            after.contains_key("c"),
            "processing child lease must remain"
        );
    }

    #[tokio::test]
    async fn leader_retains_completed_parent_while_child_not_started() {
        // Child exists but has NOT processed yet (still at a start position /
        // no real checkpoint) → parent must be retained as a tombstone.
        let leases = FakeLeases::default();
        {
            let mut rows = leases.rows.lock().unwrap();
            rows.insert(
                "p".into(),
                State {
                    completed: true,
                    checkpoint: Some("100000000000000000000000001".into()),
                    ..Default::default()
                },
            );
            rows.insert(
                "c".into(),
                State {
                    checkpoint: None, // never processed a record
                    parents: vec!["p".into()],
                    ..Default::default()
                },
            );
        }
        let fleet = Fleet::new(
            FakeSource {
                metas: vec![],
                data: HashMap::new(),
            },
            leases.clone(),
            Arc::new(SyncConsumerFactory::new(Arc::new(RecordingFactory {
                sink: Arc::new(Mutex::new(HashMap::new())),
            }))),
            FleetConfig {
                owner: "w1".into(),
                max_leases: 100,
                lease_duration_ms: 1000,
                poll_interval_ms: 1,
                initial_position: InitialPosition::default(),
            },
        );
        let rows = fleet.leases.list().await.unwrap();
        fleet.cleanup_completed_leases(&rows).await;
        let after = leases.rows.lock().unwrap();
        assert!(
            after.contains_key("p"),
            "parent retained until child processes"
        );
        assert!(after.contains_key("c"));
    }

    #[tokio::test]
    async fn owned_shards_excludes_completed_other_owner_released_and_leader_sentinel() {
        // owned_shards() drives the graceful shutdown-requested notifications, so
        // it must return exactly the shards THIS worker actively owns: not
        // completed, not owned by another worker, not released, and never the
        // leader sentinel (which is coordination state, not a data shard).
        let leases = FakeLeases::default();
        {
            let mut rows = leases.rows.lock().unwrap();
            // owned + active -> included
            rows.insert(
                "s0".into(),
                State {
                    owner: Some("w1".into()),
                    completed: false,
                    ..Default::default()
                },
            );
            // owned by us but completed -> excluded (nothing to hand off)
            rows.insert(
                "s1".into(),
                State {
                    owner: Some("w1".into()),
                    completed: true,
                    ..Default::default()
                },
            );
            // owned by another worker -> excluded
            rows.insert(
                "s2".into(),
                State {
                    owner: Some("w2".into()),
                    completed: false,
                    ..Default::default()
                },
            );
            // released (no owner) -> excluded
            rows.insert(
                "s3".into(),
                State {
                    owner: None,
                    completed: false,
                    ..Default::default()
                },
            );
            // leader sentinel held by us -> excluded (never a data shard)
            rows.insert(
                LEADER_LEASE_KEY.into(),
                State {
                    owner: Some("w1".into()),
                    completed: false,
                    ..Default::default()
                },
            );
        }
        let fleet = Fleet::new(
            FakeSource {
                metas: vec![],
                data: HashMap::new(),
            },
            leases.clone(),
            Arc::new(SyncConsumerFactory::new(Arc::new(RecordingFactory {
                sink: Arc::new(Mutex::new(HashMap::new())),
            }))),
            FleetConfig {
                owner: "w1".into(),
                max_leases: 100,
                lease_duration_ms: 1000,
                poll_interval_ms: 1,
                initial_position: InitialPosition::default(),
            },
        );

        let mut owned = fleet.owned_shards().await.unwrap();
        owned.sort();
        assert_eq!(owned, vec!["s0".to_string()]);
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
        slot_waits: Mutex<Vec<(String, u64)>>,
        max_concurrency: std::sync::atomic::AtomicU64,
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
        fn on_processing_slot_wait(&self, shard_id: &str, wait_ms: u64) {
            self.slot_waits
                .lock()
                .unwrap()
                .push((shard_id.to_string(), wait_ms));
        }
        fn on_max_processing_concurrency(&self, cap: u64) {
            self.max_concurrency
                .store(cap, std::sync::atomic::Ordering::SeqCst);
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

    // ---- maxProcessingConcurrency (multiplexing) ----

    /// Consumer that records logical processing concurrency: it increments a
    /// shared counter on entry to `deliver`, yields (sleep) so sibling shard
    /// tasks interleave, tracks the observed max, then decrements. Used to prove
    /// the semaphore caps concurrent `deliver` calls.
    struct ConcProbe {
        shard: ShardId,
        cur: Arc<std::sync::atomic::AtomicUsize>,
        max: Arc<std::sync::atomic::AtomicUsize>,
        delivered: Arc<Mutex<Vec<String>>>,
    }
    #[async_trait::async_trait]
    impl AsyncShardConsumer for ConcProbe {
        async fn deliver(&mut self, records: &[Record]) -> Result<Option<String>, WorkerError> {
            use std::sync::atomic::Ordering::SeqCst;
            let now = self.cur.fetch_add(1, SeqCst) + 1;
            self.max.fetch_max(now, SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            self.cur.fetch_sub(1, SeqCst);
            self.delivered.lock().unwrap().push(self.shard.clone());
            Ok(records.last().map(|r| r.seq.clone()))
        }
        async fn shard_ended(&mut self) -> Result<(), WorkerError> {
            Ok(())
        }
    }
    struct ConcFactory {
        cur: Arc<std::sync::atomic::AtomicUsize>,
        max: Arc<std::sync::atomic::AtomicUsize>,
        delivered: Arc<Mutex<Vec<String>>>,
    }
    impl ShardConsumerFactory for ConcFactory {
        fn create(&self, shard: &ShardId) -> Box<dyn AsyncShardConsumer + Send> {
            Box::new(ConcProbe {
                shard: shard.clone(),
                cur: self.cur.clone(),
                max: self.max.clone(),
                delivered: self.delivered.clone(),
            })
        }
    }

    fn roots(n: usize) -> (Vec<ShardMeta>, HashMap<ShardId, Vec<Record>>) {
        let mut metas = Vec::new();
        let mut data = HashMap::new();
        for i in 0..n {
            let id = format!("s{i}");
            metas.push(ShardMeta {
                id: id.clone(),
                parents: vec![],
            });
            data.insert(id.clone(), vec![rec(&id, &format!("{i}"))]);
        }
        (metas, data)
    }

    /// (fleet, observed-max-concurrency counter, delivered-shard log) for the
    /// processing-concurrency tests.
    type ConcHarness = (
        Fleet<FakeSource, FakeLeases>,
        Arc<std::sync::atomic::AtomicUsize>,
        Arc<Mutex<Vec<String>>>,
    );

    fn conc_fleet(n: usize, cap: Option<usize>) -> ConcHarness {
        let (metas, data) = roots(n);
        let source = FakeSource { metas, data };
        let cur = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let max = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let delivered = Arc::new(Mutex::new(Vec::new()));
        let factory = Arc::new(ConcFactory {
            cur,
            max: max.clone(),
            delivered: delivered.clone(),
        });
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
        )
        .with_max_processing_concurrency(cap);
        (fleet, max, delivered)
    }

    #[tokio::test]
    async fn max_processing_concurrency_caps_concurrent_deliver() {
        // 6 shards, cap = 2 → never more than 2 concurrent deliveries, and every
        // shard is still processed (no starvation).
        let (fleet, max, delivered) = conc_fleet(6, Some(2));
        fleet.run_until_complete(10).await.unwrap();
        assert!(
            max.load(std::sync::atomic::Ordering::SeqCst) <= 2,
            "observed concurrency {} exceeded the cap of 2",
            max.load(std::sync::atomic::Ordering::SeqCst)
        );
        let mut got = delivered.lock().unwrap().clone();
        got.sort();
        got.dedup();
        assert_eq!(got.len(), 6, "every shard processed (no starvation)");
    }

    #[tokio::test]
    async fn max_processing_concurrency_none_is_unbounded() {
        // Default (None): all shards processed; the cap does not gate. With 6
        // sleeping deliveries and no cap, more than 2 run at once — proving None
        // imposes no artificial limit (and is behavior-identical to prior code).
        let (fleet, max, delivered) = conc_fleet(6, None);
        fleet.run_until_complete(10).await.unwrap();
        assert_eq!(delivered.lock().unwrap().len(), 6, "all shards delivered");
        assert!(
            max.load(std::sync::atomic::Ordering::SeqCst) > 2,
            "unbounded run should exceed 2 concurrent deliveries"
        );
    }

    #[tokio::test]
    async fn set_max_processing_concurrency_grows_and_shrinks() {
        // Online resize: grow adds permits immediately; shrink reclaims them
        // (no in-flight permits here, so it takes effect at once).
        use std::sync::atomic::Ordering::SeqCst;
        let (fleet, _max, _delivered) = conc_fleet(1, Some(2));
        let pl = fleet.processing_limit.clone().unwrap();
        assert_eq!(pl.sem.available_permits(), 2);
        assert_eq!(pl.max.load(SeqCst), 2);

        fleet.set_max_processing_concurrency(5).await;
        assert_eq!(pl.sem.available_permits(), 5, "grew to 5 permits");
        assert_eq!(pl.max.load(SeqCst), 5);

        fleet.set_max_processing_concurrency(1).await;
        assert_eq!(pl.sem.available_permits(), 1, "shrank to 1 permit");
        assert_eq!(pl.max.load(SeqCst), 1);
    }

    #[tokio::test]
    async fn set_max_processing_concurrency_noop_on_unbounded() {
        // Resizing an unbounded (None) fleet is a no-op, not a panic.
        let (fleet, _max, _delivered) = conc_fleet(1, None);
        assert!(fleet.processing_limit.is_none());
        fleet.set_max_processing_concurrency(4).await;
        assert!(fleet.processing_limit.is_none());
    }

    #[tokio::test]
    async fn cap_one_fully_serializes() {
        // cap = 1 → strictly one deliver at a time; all shards still processed.
        let (fleet, max, delivered) = conc_fleet(5, Some(1));
        fleet.run_until_complete(10).await.unwrap();
        assert_eq!(
            max.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "cap=1 must serialize processing"
        );
        assert_eq!(delivered.lock().unwrap().len(), 5, "all shards processed");
    }

    #[tokio::test]
    async fn cap_larger_than_shard_count_is_unbounded_in_effect() {
        // A cap above the shard count never binds: all shards run, concurrency
        // is limited only by the shard count.
        let (fleet, max, delivered) = conc_fleet(3, Some(100));
        fleet.run_until_complete(10).await.unwrap();
        assert_eq!(delivered.lock().unwrap().len(), 3);
        assert!(
            max.load(std::sync::atomic::Ordering::SeqCst) <= 3,
            "cannot exceed shard count"
        );
    }

    #[tokio::test]
    async fn cap_zero_is_treated_as_unbounded() {
        // Some(0) must NOT create a zero-permit semaphore (which would deadlock);
        // it is treated as None (unbounded).
        let (fleet, _max, _delivered) = conc_fleet(2, Some(0));
        assert!(
            fleet.processing_limit.is_none(),
            "Some(0) must map to unbounded, never a 0-permit semaphore"
        );
        fleet.run_until_complete(10).await.unwrap();
    }

    #[tokio::test]
    async fn many_shards_small_cap_bounds_and_completes() {
        // Stress: 50 shards, cap = 4 → never more than 4 concurrent, all 50 done.
        let (fleet, max, delivered) = conc_fleet(50, Some(4));
        fleet.run_until_complete(10).await.unwrap();
        assert!(
            max.load(std::sync::atomic::Ordering::SeqCst) <= 4,
            "observed {} > cap 4",
            max.load(std::sync::atomic::Ordering::SeqCst)
        );
        let mut got = delivered.lock().unwrap().clone();
        got.sort();
        got.dedup();
        assert_eq!(got.len(), 50, "every one of 50 shards processed");
    }

    #[tokio::test]
    async fn cap_no_permit_leak_on_ack_path() {
        // After a run that acks every batch, all permits are returned (no leak
        // would silently shrink effective concurrency over time).
        let (fleet, _max, _delivered) = conc_fleet(8, Some(3));
        fleet.run_until_complete(10).await.unwrap();
        let pl = fleet.processing_limit.as_ref().unwrap();
        assert_eq!(
            pl.sem.available_permits(),
            3,
            "all permits returned after acked deliveries"
        );
    }

    /// Consumer that delivers but never acks (returns `None`) — the sidecar
    /// "client hasn't checkpointed yet" path. Used to prove the permit is
    /// released regardless of the ack outcome.
    struct CapNoAckProbe;
    #[async_trait::async_trait]
    impl AsyncShardConsumer for CapNoAckProbe {
        async fn deliver(&mut self, _records: &[Record]) -> Result<Option<String>, WorkerError> {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            Ok(None)
        }
        async fn shard_ended(&mut self) -> Result<(), WorkerError> {
            Ok(())
        }
    }
    struct CapNoAckFactory;
    impl ShardConsumerFactory for CapNoAckFactory {
        fn create(&self, _shard: &ShardId) -> Box<dyn AsyncShardConsumer + Send> {
            Box::new(CapNoAckProbe)
        }
    }

    #[tokio::test]
    async fn cap_no_permit_leak_on_none_ack_path() {
        let (metas, data) = roots(6);
        let fleet = Fleet::new(
            FakeSource { metas, data },
            FakeLeases::default(),
            Arc::new(CapNoAckFactory),
            FleetConfig {
                owner: "w1".into(),
                max_leases: 100,
                lease_duration_ms: 100_000,
                poll_interval_ms: 1,
                initial_position: InitialPosition::default(),
            },
        )
        .with_max_processing_concurrency(Some(2));
        fleet.run_until_complete(10).await.unwrap();
        let pl = fleet.processing_limit.as_ref().unwrap();
        assert_eq!(
            pl.sem.available_permits(),
            2,
            "permit released even when the consumer never acks"
        );
    }

    #[tokio::test]
    async fn cap_does_not_gate_idle_shards() {
        // Idle shards (no records) never call `deliver`, so they never contend
        // for a permit: with cap=1 and one slow data shard, the empty shards
        // still complete. Proves reading/completion aren't gated by the permit.
        let metas = vec![
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
        ];
        let mut data = HashMap::new();
        data.insert("s0".to_string(), vec![rec("s0", "1")]); // s1, s2 have no data
        let leases = FakeLeases::default();
        let delivered = Arc::new(Mutex::new(Vec::new()));
        let factory = Arc::new(ConcFactory {
            cur: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            max: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            delivered: delivered.clone(),
        });
        let fleet = Fleet::new(
            FakeSource { metas, data },
            leases.clone(),
            factory,
            FleetConfig {
                owner: "w1".into(),
                max_leases: 100,
                lease_duration_ms: 100_000,
                poll_interval_ms: 1,
                initial_position: InitialPosition::default(),
            },
        )
        .with_max_processing_concurrency(Some(1));
        fleet.run_until_complete(10).await.unwrap();

        // All three shards' leases complete (empty ones weren't starved).
        let rows = leases.rows.lock().unwrap();
        for s in ["s0", "s1", "s2"] {
            assert!(
                rows.get(s).map(|r| r.completed).unwrap_or(false),
                "{s} complete"
            );
        }
        // Only the shard with data was delivered.
        assert_eq!(delivered.lock().unwrap().as_slice(), &["s0".to_string()]);
    }

    /// Records each delivered (shard, seq) in delivery order, with a small yield
    /// so shard tasks interleave under a cap.
    struct OrderProbe {
        shard: ShardId,
        log: Arc<Mutex<Vec<(String, String)>>>,
    }
    #[async_trait::async_trait]
    impl AsyncShardConsumer for OrderProbe {
        async fn deliver(&mut self, records: &[Record]) -> Result<Option<String>, WorkerError> {
            for r in records {
                self.log
                    .lock()
                    .unwrap()
                    .push((self.shard.clone(), r.seq.clone()));
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            Ok(records.last().map(|r| r.seq.clone()))
        }
        async fn shard_ended(&mut self) -> Result<(), WorkerError> {
            Ok(())
        }
    }
    struct OrderFactory {
        log: Arc<Mutex<Vec<(String, String)>>>,
    }
    impl ShardConsumerFactory for OrderFactory {
        fn create(&self, shard: &ShardId) -> Box<dyn AsyncShardConsumer + Send> {
            Box::new(OrderProbe {
                shard: shard.clone(),
                log: self.log.clone(),
            })
        }
    }

    #[tokio::test]
    async fn cap_preserves_per_shard_order() {
        // Multi-record shards under cap=2: each shard's records must arrive in
        // sequence order (the cap redistributes shards, never reorders within one).
        let mut data = HashMap::new();
        for s in ["s0", "s1", "s2"] {
            data.insert(
                s.to_string(),
                vec![rec(s, "1"), rec(s, "2"), rec(s, "3"), rec(s, "4")],
            );
        }
        let metas = ["s0", "s1", "s2"]
            .iter()
            .map(|s| ShardMeta {
                id: (*s).into(),
                parents: vec![],
            })
            .collect();
        let log = Arc::new(Mutex::new(Vec::new()));
        let fleet = Fleet::new(
            FakeSource { metas, data },
            FakeLeases::default(),
            Arc::new(OrderFactory { log: log.clone() }),
            FleetConfig {
                owner: "w1".into(),
                max_leases: 100,
                lease_duration_ms: 100_000,
                poll_interval_ms: 1,
                initial_position: InitialPosition::default(),
            },
        )
        .with_max_processing_concurrency(Some(2));
        fleet.run_until_complete(10).await.unwrap();

        let log = log.lock().unwrap();
        for s in ["s0", "s1", "s2"] {
            let seqs: Vec<&str> = log
                .iter()
                .filter(|(sh, _)| sh == s)
                .map(|(_, q)| q.as_str())
                .collect();
            assert_eq!(seqs, vec!["1", "2", "3", "4"], "{s} in-order under cap");
        }
    }

    #[tokio::test]
    async fn cap_preserves_resume_no_redelivery() {
        // cap=1 over an always-open shard across cycles: each record delivered
        // exactly once (durable checkpoint resume still holds under the cap).
        let source = OpenSource {
            records: vec![rec("s0", "1"), rec("s0", "2"), rec("s0", "3")],
        };
        let log = Arc::new(Mutex::new(Vec::new()));
        let fleet = Fleet::new(
            source,
            FakeLeases::default(),
            Arc::new(OrderFactory { log: log.clone() }),
            FleetConfig {
                owner: "w1".into(),
                max_leases: 100,
                lease_duration_ms: 100_000,
                poll_interval_ms: 1,
                initial_position: InitialPosition::default(),
            },
        )
        .with_max_processing_concurrency(Some(1));
        // Several cycles; an always-open shard never completes, so this loops
        // run_cycle up to the cap and returns after making no further progress.
        fleet.run_until_complete(4).await.unwrap();
        let seqs: Vec<String> = log.lock().unwrap().iter().map(|(_, q)| q.clone()).collect();
        assert_eq!(
            seqs,
            vec!["1", "2", "3"],
            "each record delivered exactly once across cycles under cap=1"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resize_shrink_waits_for_inflight() {
        // Shrink must not preempt an in-flight slot: it blocks until a permit is
        // free, then removes it. Hold both permits, request shrink→1, assert it
        // stays pending until we release, then completes with max=1.
        use std::sync::atomic::Ordering::SeqCst;
        let (fleet, _max, _delivered) = conc_fleet(1, Some(2));
        let fleet = Arc::new(fleet);
        let pl = fleet.processing_limit.clone().unwrap();

        let held = pl.sem.acquire_many(2).await.unwrap(); // 0 permits free
        let f2 = fleet.clone();
        let mut shrink = tokio::spawn(async move { f2.set_max_processing_concurrency(1).await });

        // With no free permit, the shrink cannot make progress.
        let pending = tokio::time::timeout(std::time::Duration::from_millis(80), &mut shrink).await;
        assert!(
            pending.is_err(),
            "shrink must wait while permits are in-flight"
        );

        drop(held); // free both permits → shrink can reclaim one
        shrink.await.unwrap();
        assert_eq!(pl.max.load(SeqCst), 1, "max updated after shrink");
        assert_eq!(
            pl.sem.available_permits(),
            1,
            "one permit reclaimed, one remains"
        );
    }

    #[tokio::test]
    async fn cap_emits_slot_wait_and_gauge_metrics() {
        // With a cap set, the fleet reports the configured cap (gauge) and a
        // slot-wait sample per delivered batch. Unbounded fleets emit neither.
        let (metas, data) = roots(6);
        let sink = Arc::new(CaptureSink::default());
        let delivered = Arc::new(Mutex::new(Vec::new()));
        let factory = Arc::new(ConcFactory {
            cur: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            max: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            delivered,
        });
        let fleet = Fleet::new(
            FakeSource { metas, data },
            FakeLeases::default(),
            factory,
            FleetConfig {
                owner: "w1".into(),
                max_leases: 100,
                lease_duration_ms: 100_000,
                poll_interval_ms: 1,
                initial_position: InitialPosition::default(),
            },
        )
        .with_metrics(sink.clone())
        .with_max_processing_concurrency(Some(2));
        fleet.run_until_complete(10).await.unwrap();

        assert_eq!(
            sink.max_concurrency
                .load(std::sync::atomic::Ordering::SeqCst),
            2,
            "configured cap reported as a gauge"
        );
        assert_eq!(
            sink.slot_waits.lock().unwrap().len(),
            6,
            "one slot-wait sample per delivered batch (6 shards)"
        );
    }
}
