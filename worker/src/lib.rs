//! Async worker for amazon-dynamodb-streams-consumer.
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

use amazon_dynamodb_streams_consumer_core::coordinator::RawLease;
use amazon_dynamodb_streams_consumer_core::{RecordBatch, RecordProcessor, ShardId, ShardMeta};
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

    /// Return the child shards of `parent` (each with its parents populated).
    ///
    /// Default: filter a full `describe_shards()` — correct but not API-efficient.
    /// The live DynamoDB Streams source OVERRIDES this with a targeted
    /// `DescribeStream` + `CHILD_SHARDS` `ShardFilter`, so the shard-sync leader
    /// pays for shard discovery only when a shard actually ends — a stable
    /// topology costs ZERO `DescribeStream` calls. Grounded in KCA
    /// `DynamoDBStreamsShutdownTask.fetchChildShardsForCompleteLineage`, which
    /// calls `listShardsWithFilter(CHILD_SHARDS, parentShardId)` (Apache-2.0).
    async fn describe_child_shards(&self, parent: &str) -> Result<Vec<ShardMeta>, WorkerError> {
        Ok(self
            .describe_shards()
            .await?
            .into_iter()
            .filter(|m| m.parents.iter().any(|p| p == parent))
            .collect())
    }
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
    /// Release a held lease (clear owner, bump counter) so another worker can
    /// take it immediately on graceful shutdown. Conditional on ownership.
    async fn release(&self, lease_key: &str, owner: &str, counter: u64) -> Result<(), WorkerError>;

    /// Delete a completed shard's lease (tombstone GC). Implementations MUST
    /// only delete a lease still marked completed. Called only by the shard-sync
    /// leader after a shard's children are safely processing (see core
    /// `leases_safe_to_delete`).
    async fn delete_lease(&self, lease_key: &str) -> Result<(), WorkerError>;

    /// Publish a shard as an **unowned** lease carrying its `parents`, if it does
    /// not already exist (idempotent create-if-absent — an existing/in-progress
    /// lease is left untouched). Called ONLY by the shard-sync leader: it is how
    /// the leader shares a discovered shard (and its lineage) with the fleet so
    /// non-leaders never call DescribeStream. See [`crate::fleet`] / core `leader`.
    /// `checkpoint` is the initial resume position to persist on the new lease
    /// (`None` = TRIM_HORIZON; a sentinel encodes another start mode — see
    /// core `StartPosition`). Applied only at create; an existing lease is
    /// left untouched.
    async fn create_shard_lease(
        &self,
        lease_key: &str,
        parents: &[ShardId],
        checkpoint: Option<&str>,
    ) -> Result<(), WorkerError>;

    /// Optimistic bid for the leader lease.
    ///   * `expected == None`  → create-if-absent: become leader only if the
    ///     sentinel is vacant (loses the race harmlessly if another worker
    ///     created it first).
    ///   * `expected == Some(c)` → steal an EXPIRED leader lease, conditioned on
    ///     the counter `c` we last observed (if the old leader revived and
    ///     heartbeated, `c` advanced, the condition fails, and the steal loses).
    ///
    /// Returns `Some(new_counter)` if this worker now holds leadership, or `None`
    /// if the bid lost the optimistic-lock race. Renewal of a held leader lease
    /// uses [`AsyncLeaseStore::renew`].
    async fn try_acquire_leadership(
        &self,
        lease_key: &str,
        owner: &str,
        expected: Option<u64>,
    ) -> Result<Option<u64>, WorkerError>;
}

/// Async, ack-gated per-shard delivery used by the [`fleet::Fleet`]. Unlike the
/// synchronous [`RecordProcessor`] (fine for the in-process [`Worker`]), a
/// consumer's [`deliver`](AsyncShardConsumer::deliver) is `async` and returns
/// the sequence number to checkpoint — so a language-binding sidecar can stream
/// a batch to the client and only checkpoint once the client acks (at-least-once).
#[async_trait::async_trait]
pub trait AsyncShardConsumer: Send {
    /// Deliver a batch (already in sequence order). Returns `Some(seq)` to
    /// checkpoint that sequence under the optimistic lock, or `None` to deliver
    /// without advancing the durable checkpoint (the lease is heartbeated instead).
    async fn deliver(
        &mut self,
        records: &[amazon_dynamodb_streams_consumer_core::Record],
    ) -> Result<Option<String>, WorkerError>;
    /// The shard reached SHARD_END.
    async fn shard_ended(&mut self) -> Result<(), WorkerError>;
}

/// Creates one [`AsyncShardConsumer`] per shard (KCL's per-shard processor model).
pub trait ShardConsumerFactory: Send + Sync {
    fn create(&self, shard: &ShardId) -> Box<dyn AsyncShardConsumer + Send>;
}

/// Adapts a synchronous [`RecordProcessorFactory`] into a [`ShardConsumerFactory`]
/// so the in-process (non-sidecar) path keeps the simple sync `RecordProcessor`
/// API. Each delivered batch is checkpointed at its last sequence number.
pub struct SyncConsumerFactory {
    inner: std::sync::Arc<dyn amazon_dynamodb_streams_consumer_core::RecordProcessorFactory>,
}

impl SyncConsumerFactory {
    pub fn new(
        inner: std::sync::Arc<dyn amazon_dynamodb_streams_consumer_core::RecordProcessorFactory>,
    ) -> Self {
        Self { inner }
    }
}

impl ShardConsumerFactory for SyncConsumerFactory {
    fn create(&self, shard: &ShardId) -> Box<dyn AsyncShardConsumer + Send> {
        let mut processor = self.inner.create(shard);
        processor.initialize(shard);
        Box::new(SyncConsumer {
            processor,
            shard: shard.clone(),
        })
    }
}

struct SyncConsumer {
    processor: Box<dyn RecordProcessor + Send>,
    shard: ShardId,
}

#[async_trait::async_trait]
impl AsyncShardConsumer for SyncConsumer {
    async fn deliver(
        &mut self,
        records: &[amazon_dynamodb_streams_consumer_core::Record],
    ) -> Result<Option<String>, WorkerError> {
        self.processor.process_records(records);
        Ok(records.last().map(|r| r.seq.clone()))
    }
    async fn shard_ended(&mut self) -> Result<(), WorkerError> {
        self.processor.shard_ended(&self.shard);
        Ok(())
    }
}

pub struct Worker<S, L> {
    source: S,
    leases: L,
    owner: String,
}

impl<S: AsyncStreamSource, L: AsyncLeaseStore> Worker<S, L> {
    pub fn new(source: S, leases: L, owner: impl Into<String>) -> Self {
        Self {
            source,
            leases,
            owner: owner.into(),
        }
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
        // Shard ids present this cycle (offline: the full source shard list, so a
        // parent is always present — the missing-parent GC case is live-only).
        let existing: HashSet<ShardId> = shards.iter().map(|m| m.id.clone()).collect();
        for meta in &shards {
            if !eligible(meta, &completed, &existing) {
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
fn eligible(meta: &ShardMeta, completed: &HashSet<ShardId>, existing: &HashSet<ShardId>) -> bool {
    // A parent gate is satisfied if the parent is complete OR its lease no longer
    // exists — a lease is only deleted (see core `leases_safe_to_delete`) once its
    // children are safely processing, so a missing parent is definitionally done.
    // Without this, GC-ing a completed parent would strand an in-flight child.
    !completed.contains(&meta.id)
        && meta
            .parents
            .iter()
            .all(|p| completed.contains(p) || !existing.contains(p))
}

#[cfg(test)]
mod tests {
    use super::*;
    use amazon_dynamodb_streams_consumer_core::Record;
    use std::collections::HashMap;
    use std::sync::Mutex;

    fn shard(id: &str, parents: &[&str]) -> ShardMeta {
        ShardMeta {
            id: id.into(),
            parents: parents.iter().map(|p| p.to_string()).collect(),
        }
    }
    fn set(items: &[&str]) -> HashSet<ShardId> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn eligible_gates_child_until_parent_complete() {
        let child = shard("c", &["p"]);
        let existing = set(&["p", "c"]);
        // Parent present but not complete → child gated.
        assert!(!eligible(&child, &set(&[]), &existing));
        // Parent complete → child eligible.
        assert!(eligible(&child, &set(&["p"]), &existing));
    }

    #[test]
    fn eligible_treats_gcd_parent_as_satisfied() {
        // Parent lease has been deleted (absent from `existing`) and is not in
        // the completed set either. A cleaned-up parent must NOT strand its
        // in-flight child — the child stays eligible.
        let child = shard("c", &["p"]);
        let existing = set(&["c"]); // "p" GC'd
        assert!(eligible(&child, &set(&[]), &existing));
    }

    #[test]
    fn eligible_merge_child_needs_all_parents_satisfied() {
        let child = shard("c", &["p1", "p2"]);
        let existing = set(&["p1", "p2", "c"]);
        // Only one parent complete → gated.
        assert!(!eligible(&child, &set(&["p1"]), &existing));
        // p1 complete, p2 GC'd (absent) → satisfied.
        assert!(eligible(&child, &set(&["p1"]), &set(&["p2_absent", "c"])));
    }

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
    struct FakeLeaseState {
        owner: Option<String>,
        counter: u64,
        checkpoint: Option<String>,
        completed: bool,
        parents: Vec<String>,
    }
    #[derive(Default)]
    struct FakeLeases {
        rows: Mutex<HashMap<String, FakeLeaseState>>,
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
            Ok(LeaseHandle {
                owner: owner.to_string(),
                counter: r.counter,
                checkpoint: r.checkpoint.clone(),
            })
        }
        async fn checkpoint(
            &self,
            key: &str,
            _owner: &str,
            counter: u64,
            seq: &str,
        ) -> Result<u64, WorkerError> {
            let mut rows = self.rows.lock().unwrap();
            let r = rows.get_mut(key).ok_or("no lease")?;
            r.checkpoint = Some(seq.to_string());
            r.counter = counter + 1;
            Ok(r.counter)
        }
        async fn mark_complete(
            &self,
            key: &str,
            _owner: &str,
            _counter: u64,
        ) -> Result<(), WorkerError> {
            self.rows
                .lock()
                .unwrap()
                .get_mut(key)
                .ok_or("no lease")?
                .completed = true;
            Ok(())
        }
        async fn release(&self, key: &str, _owner: &str, counter: u64) -> Result<(), WorkerError> {
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
            let mut rows = self.rows.lock().unwrap();
            rows.entry(key.to_string())
                .or_insert_with(|| FakeLeaseState {
                    owner: None,
                    counter: 0,
                    checkpoint: checkpoint.map(|s| s.to_string()),
                    completed: false,
                    parents: parents.to_vec(),
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
                        return Ok(None); // already created → we didn't win
                    }
                    rows.insert(
                        key.to_string(),
                        FakeLeaseState {
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
                    _ => Ok(None), // counter advanced → steal lost
                },
            }
        }
    }

    #[derive(Default)]
    struct RecordingProcessor {
        events: Vec<String>,
    }
    impl RecordProcessor for RecordingProcessor {
        fn initialize(&mut self, s: &ShardId) {
            self.events.push(format!("init:{s}"));
        }
        fn process_records(&mut self, rs: &[Record]) {
            for r in rs {
                self.events.push(format!("rec:{}:{}", r.shard_id, r.seq));
            }
        }
        fn shard_ended(&mut self, s: &ShardId) {
            self.events.push(format!("end:{s}"));
        }
    }

    #[tokio::test]
    async fn worker_preserves_parent_before_child_and_checkpoints() {
        let mut data = HashMap::new();
        data.insert(
            "parent".to_string(),
            vec![rec("parent", "1"), rec("parent", "2")],
        );
        data.insert("child".to_string(), vec![rec("child", "3")]);
        let source = FakeSource {
            metas: vec![
                ShardMeta {
                    id: "child".into(),
                    parents: vec!["parent".into()],
                },
                ShardMeta {
                    id: "parent".into(),
                    parents: vec![],
                },
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
                "init:parent",
                "rec:parent:1",
                "rec:parent:2",
                "end:parent",
                "init:child",
                "rec:child:3",
                "end:child",
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
        data.insert(
            "s".to_string(),
            vec![rec("s", "10"), rec("s", "11"), rec("s", "12")],
        );
        let source = FakeSource {
            metas: vec![ShardMeta {
                id: "s".into(),
                parents: vec![],
            }],
            data,
        };
        let leases = FakeLeases::default();
        leases.rows.lock().unwrap().insert(
            "s".into(),
            FakeLeaseState {
                owner: None,
                counter: 3,
                checkpoint: Some("11".into()),
                completed: false,
                parents: vec![],
            },
        );
        let worker = Worker::new(source, leases, "w1");
        let mut proc = RecordingProcessor::default();
        worker.run(&mut proc).await.unwrap();
        assert_eq!(proc.events, vec!["init:s", "rec:s:12", "end:s"]);
    }
}
