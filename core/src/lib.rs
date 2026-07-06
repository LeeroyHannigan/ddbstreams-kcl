//! amazon-dynamodb-streams-consumer-core — shared Rust engine for a multi-language, JVM-free
//! DynamoDB Streams KCL.
//!
//! This crate owns the correctness-critical logic that is IDENTICAL regardless
//! of how languages attach on top (daemon+IPC "Architecture A" or FFI "B"):
//!   * shard graph consumption (lineage from DescribeStream, built in
//!     `amazon-dynamodb-streams-consumer-source`)
//!   * ORDERING: single-owner-per-shard (in-sequence) + parent-before-child
//!   * checkpointing
//!
//! AWS is abstracted behind `StreamSource` + `LeaseStore` so the engine is unit
//! testable with zero network. The real DDB Streams / DynamoDB adapters are
//! added as implementors of these traits.

pub mod backoff;
pub mod cleanup;
pub mod coordinator;
pub mod leader;
pub mod metrics;
pub mod multistream;
pub mod record;
pub mod taker;

pub type ShardId = String;
/// DynamoDB Streams sequence number. This is an **opaque, monotonically
/// increasing token** (a stringified 128-bit integer) — NOT something to
/// compare or parse. The engine only ever stores the last-seen value as a
/// checkpoint and hands it back to the source, which resumes via an
/// `AFTER_SEQUENCE_NUMBER` iterator (matching KCL, which treats it as opaque).
pub type SequenceNumber = String;

// Non-numeric sentinels persisted in the lease `checkpoint` column to record the
// position a shard was seeded at, before any real record has been checkpointed.
// A real DynamoDB Streams sequence number is a long decimal string, so these
// never collide with one. `TRIM_HORIZON` seeds as the natural empty checkpoint
// (`None`); other modes persist their sentinel and are decoded by
// [`StartPosition::from_checkpoint`].
const SENTINEL_LATEST: &str = "LATEST";

/// Where a shard begins reading when its lease is first seeded and no per-record
/// checkpoint exists yet. A worker-level setting; once a shard checkpoints a real
/// record this no longer applies to it.
///
/// `#[non_exhaustive]`: additional start modes may be added, so `match`es on this
/// type must carry a wildcard arm.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum InitialPosition {
    /// Start at the oldest untrimmed record in the shard.
    #[default]
    TrimHorizon,
    /// Start at the tip — only records that arrive after the consumer starts.
    Latest,
}

impl InitialPosition {
    /// The lease `checkpoint` value to persist when first seeding a shard at this
    /// position. `None` is the natural "from the beginning" checkpoint; other
    /// positions persist a sentinel decoded by [`StartPosition::from_checkpoint`].
    pub fn seed_checkpoint(self) -> Option<String> {
        StartPosition::from(self).as_checkpoint()
    }

    /// Parse a caller-supplied name (case-insensitive) into an [`InitialPosition`].
    /// Unknown values return `None` so callers can reject or fall back.
    pub fn parse(name: &str) -> Option<Self> {
        match name.trim().to_ascii_uppercase().as_str() {
            "TRIM_HORIZON" | "TRIM-HORIZON" | "TRIMHORIZON" => Some(Self::TrimHorizon),
            "LATEST" => Some(Self::Latest),
            _ => None,
        }
    }
}

/// The resolved position from which a fresh shard iterator is derived. `After`
/// carries a durable per-record checkpoint; the other variants are start modes
/// used before any record has been checkpointed.
///
/// `#[non_exhaustive]`: `match`es must carry a wildcard arm.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum StartPosition {
    /// Oldest untrimmed record.
    TrimHorizon,
    /// Tip of the shard.
    Latest,
    /// Strictly after a specific, already-processed sequence number.
    After(SequenceNumber),
}

impl From<InitialPosition> for StartPosition {
    fn from(p: InitialPosition) -> Self {
        match p {
            InitialPosition::TrimHorizon => StartPosition::TrimHorizon,
            InitialPosition::Latest => StartPosition::Latest,
        }
    }
}

impl StartPosition {
    /// Decode a persisted lease checkpoint into a start position. `None` and any
    /// recognised sentinel map to a start mode; any other value is treated as a
    /// real sequence number (`After`).
    pub fn from_checkpoint(checkpoint: Option<&str>) -> Self {
        match checkpoint {
            None => StartPosition::TrimHorizon,
            Some(s) if s == SENTINEL_LATEST => StartPosition::Latest,
            Some(seq) => StartPosition::After(seq.to_string()),
        }
    }

    /// Encode as a persisted checkpoint value. Modes meaning "from the beginning"
    /// persist as `None`; other start modes persist their sentinel; `After`
    /// persists its sequence number.
    pub fn as_checkpoint(&self) -> Option<String> {
        match self {
            StartPosition::TrimHorizon => None,
            StartPosition::Latest => Some(SENTINEL_LATEST.to_string()),
            StartPosition::After(seq) => Some(seq.clone()),
            #[allow(unreachable_patterns)]
            _ => None,
        }
    }

    /// True if this position is a start mode (no real record processed yet),
    /// i.e. not [`StartPosition::After`].
    pub fn is_start_mode(&self) -> bool {
        !matches!(self, StartPosition::After(_))
    }
}

/// The checkpoint a freshly-discovered child shard should be seeded with, given
/// its parents' current lease checkpoints.
///
/// A child normally starts from its own beginning (`None` = TRIM_HORIZON) so no
/// records are skipped across a reshard. The one exception: if every parent
/// completed *without ever checkpointing a real record* and they all carried the
/// same start mode (e.g. a tip-only shard that produced nothing under `Latest`),
/// the child inherits that start mode — otherwise a fresh consumer would replay
/// the child's history that the parent deliberately skipped. Generic over the
/// start mode, so it holds for any non-`After` position.
pub fn child_seed_checkpoint(parent_checkpoints: &[Option<String>]) -> Option<String> {
    if parent_checkpoints.is_empty() {
        return None;
    }
    let mut inherited: Option<StartPosition> = None;
    for cp in parent_checkpoints {
        match StartPosition::from_checkpoint(cp.as_deref()) {
            // Parent means "from the beginning" or processed real records → the
            // child reads from its own beginning.
            StartPosition::TrimHorizon | StartPosition::After(_) => return None,
            mode => match &inherited {
                None => inherited = Some(mode),
                Some(cur) if *cur == mode => {}
                // Mixed start modes across parents → beginning.
                Some(_) => return None,
            },
        }
    }
    inherited.and_then(|s| s.as_checkpoint())
}

#[derive(Clone, Debug)]
pub struct Record {
    pub shard_id: ShardId,
    pub seq: SequenceNumber,
    pub data: Vec<u8>,
}

/// Shard lineage as reported by DescribeStream.
///
/// A shard may have UP TO TWO parents: one for a split child, two for a merge
/// child. Modeling this is mandatory for correctness — see KCL
/// `ShutdownTask.createLeasesForChildShardsIfNotExist` (a merge child requires
/// BOTH parents to be accounted for before its lease is created). See REFERENCES.md.
#[derive(Clone, Debug)]
pub struct ShardMeta {
    pub id: ShardId,
    pub parents: Vec<ShardId>,
}

pub struct RecordBatch {
    pub records: Vec<Record>,
    /// True when this shard is closed (SHARD_END) — no more records will arrive.
    pub shard_end: bool,
    /// Consumer lag for this batch: `now - ApproximateCreationDateTime` of the
    /// newest record, in milliseconds. `None` when unknown (empty batch or the
    /// source doesn't populate it). This is the DynamoDB-Streams analog of
    /// Kinesis `MillisBehindLatest`, computed by the source (see the DDB source).
    pub millis_behind_latest: Option<i64>,
}

/// The stream side (DynamoDB Streams in prod). Behind a trait so the engine is
/// testable in-memory and so a Kinesis source could be slotted in later.
pub trait StreamSource {
    fn describe_shards(&self) -> Vec<ShardMeta>;
    /// Return records after the opaque checkpoint `after` (exclusive); `None`
    /// means from `TRIM_HORIZON`. Implementations resume server-side via an
    /// `AFTER_SEQUENCE_NUMBER` shard iterator — they do NOT compare tokens.
    fn get_records(&self, shard: &ShardId, after: Option<SequenceNumber>) -> RecordBatch;
}

/// Lease + checkpoint state (DynamoDB lease table in prod).
pub trait LeaseStore {
    fn checkpoint(&mut self, shard: &ShardId, seq: SequenceNumber);
    fn last_checkpoint(&self, shard: &ShardId) -> Option<SequenceNumber>;
    fn mark_complete(&mut self, shard: &ShardId);
    fn is_complete(&self, shard: &ShardId) -> bool;
}

/// Customer business logic. In the real system a language binding bridges these
/// callbacks to the customer's Go/Python/etc. record processor.
pub trait RecordProcessor {
    fn initialize(&mut self, shard: &ShardId);
    fn process_records(&mut self, records: &[Record]);
    fn shard_ended(&mut self, shard: &ShardId);
}

/// Creates one [`RecordProcessor`] per shard, as KCL does (a
/// `ShardRecordProcessorFactory`). Each shard's processor owns its own state and
/// runs on its own task, so the factory must be shareable across tasks.
pub trait RecordProcessorFactory: Send + Sync {
    fn create(&self, shard: &ShardId) -> Box<dyn RecordProcessor + Send>;
}

/// Single-worker scheduler enforcing the ordering guarantees. Multi-host lease
/// stealing / balancing is a later phase; this proves the ordering core.
pub struct Scheduler<S: StreamSource, L: LeaseStore> {
    source: S,
    leases: L,
}

impl<S: StreamSource, L: LeaseStore> Scheduler<S, L> {
    pub fn new(source: S, leases: L) -> Self {
        Self { source, leases }
    }

    /// A shard is eligible only if it is not already complete AND every one of
    /// its parents has been fully processed (SHARD_END + checkpoint). This is the
    /// parent-before-child guarantee that preserves item-history order across
    /// resharding. For a merge child (two parents) BOTH must be complete.
    ///
    /// Grounded in KCL `HierarchicalShardSyncer` (child leases created only after
    /// parent SHARD_END) and `ShutdownTask.createLeasesForChildShardsIfNotExist`
    /// (merge child requires both parents). See REFERENCES.md §Ordering.
    fn eligible(&self, meta: &ShardMeta) -> bool {
        if self.leases.is_complete(&meta.id) {
            return false;
        }
        // all() over an empty parent list is true → root shards are eligible.
        meta.parents.iter().all(|p| self.leases.is_complete(p))
    }

    /// Drain all shards in dependency order. Returns when every shard is complete.
    pub fn run<P: RecordProcessor>(&mut self, processor: &mut P) {
        loop {
            let shards = self.source.describe_shards();
            if shards.iter().all(|m| self.leases.is_complete(&m.id)) {
                break;
            }

            let mut progressed = false;
            for meta in &shards {
                if !self.eligible(meta) {
                    continue;
                }
                progressed = true;
                processor.initialize(&meta.id);

                // Deliver strictly in sequence order, checkpointing as we go.
                loop {
                    let after = self.leases.last_checkpoint(&meta.id);
                    let batch = self.source.get_records(&meta.id, after);
                    if !batch.records.is_empty() {
                        // Records within a shard arrive in sequence order, so the
                        // LAST one is the newest — checkpoint it. We never compare
                        // sequence tokens (they are opaque).
                        processor.process_records(&batch.records);
                        let last = batch.records.last().unwrap().seq.clone();
                        self.leases.checkpoint(&meta.id, last);
                    }
                    if batch.shard_end {
                        processor.shard_ended(&meta.id);
                        self.leases.mark_complete(&meta.id);
                        break;
                    }
                    if batch.records.is_empty() {
                        break; // no data yet; a real impl would back off and poll
                    }
                }
            }
            if !progressed {
                break; // inconsistent shard graph; a real impl would re-sync
            }
        }
    }
}

// ---------------------------------------------------------------------------
// In-memory fakes for testing the ordering core without AWS.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod start_position_tests {
    use super::*;

    #[test]
    fn default_is_trim_horizon() {
        assert_eq!(InitialPosition::default(), InitialPosition::TrimHorizon);
        assert_eq!(InitialPosition::TrimHorizon.seed_checkpoint(), None);
    }

    #[test]
    fn latest_seeds_and_decodes_via_sentinel() {
        let cp = InitialPosition::Latest.seed_checkpoint();
        assert_eq!(cp.as_deref(), Some("LATEST"));
        assert_eq!(
            StartPosition::from_checkpoint(cp.as_deref()),
            StartPosition::Latest
        );
    }

    #[test]
    fn none_and_real_seq_decode_correctly() {
        assert_eq!(
            StartPosition::from_checkpoint(None),
            StartPosition::TrimHorizon
        );
        // A real DDB sequence number is a long decimal string.
        let seq = "100000000000000000000000001";
        assert_eq!(
            StartPosition::from_checkpoint(Some(seq)),
            StartPosition::After(seq.to_string())
        );
    }

    #[test]
    fn parse_is_case_insensitive_and_rejects_unknown() {
        assert_eq!(
            InitialPosition::parse("latest"),
            Some(InitialPosition::Latest)
        );
        assert_eq!(
            InitialPosition::parse("  TRIM_HORIZON "),
            Some(InitialPosition::TrimHorizon)
        );
        assert_eq!(InitialPosition::parse("tip"), None);
    }

    #[test]
    fn root_shard_has_no_parents_to_inherit_from() {
        assert_eq!(child_seed_checkpoint(&[]), None);
    }

    #[test]
    fn child_starts_at_beginning_when_parent_processed_records() {
        // Parent has a real checkpoint → child reads from its own beginning so
        // nothing is skipped across the reshard.
        let parent = vec![Some("100000000000000000000000009".to_string())];
        assert_eq!(child_seed_checkpoint(&parent), None);
    }

    #[test]
    fn child_starts_at_beginning_under_default_trim_horizon() {
        // Parent seeded at TRIM_HORIZON (None), completed → child TRIM_HORIZON.
        assert_eq!(child_seed_checkpoint(&[None]), None);
    }

    #[test]
    fn child_inherits_start_mode_when_parent_completed_without_records() {
        // Parent seeded at LATEST produced nothing (still the sentinel) → the
        // child inherits it, so a fresh consumer does not replay history the
        // parent deliberately skipped.
        let parent = vec![Some("LATEST".to_string())];
        assert_eq!(child_seed_checkpoint(&parent).as_deref(), Some("LATEST"));
    }

    #[test]
    fn merge_child_inherits_only_when_all_parents_agree() {
        // Both parents skipped history under the same mode → inherit.
        let both = vec![Some("LATEST".to_string()), Some("LATEST".to_string())];
        assert_eq!(child_seed_checkpoint(&both).as_deref(), Some("LATEST"));
        // One parent processed records → child from beginning (no loss).
        let mixed = vec![
            Some("LATEST".to_string()),
            Some("100000000000000000000000009".to_string()),
        ];
        assert_eq!(child_seed_checkpoint(&mixed), None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct InMemSource {
        metas: Vec<ShardMeta>,
        // shard -> records (pre-sorted by sequence order)
        data: HashMap<ShardId, Vec<Record>>,
    }

    impl StreamSource for InMemSource {
        fn describe_shards(&self) -> Vec<ShardMeta> {
            self.metas.clone()
        }
        fn get_records(&self, shard: &ShardId, after: Option<SequenceNumber>) -> RecordBatch {
            let all = self.data.get(shard).cloned().unwrap_or_default();
            // Opaque-token resume: return everything after the record whose seq
            // equals `after` (position-based, like an AFTER_SEQUENCE_NUMBER
            // iterator would server-side). No numeric comparison.
            let records = match after {
                None => all,
                Some(tok) => match all.iter().position(|r| r.seq == tok) {
                    Some(idx) => all[idx + 1..].to_vec(),
                    None => all, // token not found → from start
                },
            };
            RecordBatch {
                records,
                shard_end: true,
                millis_behind_latest: None,
            }
        }
    }

    #[derive(Default)]
    struct InMemLeases {
        checkpoints: HashMap<ShardId, SequenceNumber>,
        complete: HashMap<ShardId, bool>,
    }
    impl LeaseStore for InMemLeases {
        fn checkpoint(&mut self, shard: &ShardId, seq: SequenceNumber) {
            self.checkpoints.insert(shard.clone(), seq);
        }
        fn last_checkpoint(&self, shard: &ShardId) -> Option<SequenceNumber> {
            self.checkpoints.get(shard).cloned()
        }
        fn mark_complete(&mut self, shard: &ShardId) {
            self.complete.insert(shard.clone(), true);
        }
        fn is_complete(&self, shard: &ShardId) -> bool {
            *self.complete.get(shard).unwrap_or(&false)
        }
    }

    #[derive(Default)]
    struct RecordingProcessor {
        events: Vec<String>,
    }
    impl RecordProcessor for RecordingProcessor {
        fn initialize(&mut self, shard: &ShardId) {
            self.events.push(format!("init:{shard}"));
        }
        fn process_records(&mut self, records: &[Record]) {
            for r in records {
                self.events.push(format!("rec:{}:{}", r.shard_id, r.seq));
            }
        }
        fn shard_ended(&mut self, shard: &ShardId) {
            self.events.push(format!("end:{shard}"));
        }
    }

    fn rec(shard: &str, seq: &str) -> Record {
        Record {
            shard_id: shard.to_string(),
            seq: seq.to_string(),
            data: vec![],
        }
    }

    /// SPIKE SUCCESS CRITERION: a parent shard splits into a child; the engine
    /// MUST deliver all parent records (in order) and finish the parent before
    /// delivering any child record.
    #[test]
    fn parent_before_child_ordering() {
        let mut data = HashMap::new();
        data.insert(
            "shard-parent".to_string(),
            vec![rec("shard-parent", "1"), rec("shard-parent", "2")],
        );
        data.insert(
            "shard-child".to_string(),
            vec![rec("shard-child", "3"), rec("shard-child", "4")],
        );

        let source = InMemSource {
            metas: vec![
                // Deliberately list child first to prove ordering isn't just list order.
                ShardMeta {
                    id: "shard-child".into(),
                    parents: vec!["shard-parent".into()],
                },
                ShardMeta {
                    id: "shard-parent".into(),
                    parents: vec![],
                },
            ],
            data,
        };

        let mut proc = RecordingProcessor::default();
        let mut sched = Scheduler::new(source, InMemLeases::default());
        sched.run(&mut proc);

        assert_eq!(
            proc.events,
            vec![
                "init:shard-parent",
                "rec:shard-parent:1",
                "rec:shard-parent:2",
                "end:shard-parent",
                "init:shard-child",
                "rec:shard-child:3",
                "rec:shard-child:4",
                "end:shard-child",
            ],
            "child must not be touched until parent reaches SHARD_END + checkpoint"
        );
    }

    /// Per-shard records are delivered in strictly increasing sequence order.
    #[test]
    fn per_shard_sequence_order() {
        let mut data = HashMap::new();
        data.insert(
            "s".to_string(),
            vec![rec("s", "10"), rec("s", "11"), rec("s", "12")],
        );
        let source = InMemSource {
            metas: vec![ShardMeta {
                id: "s".into(),
                parents: vec![],
            }],
            data,
        };
        let mut proc = RecordingProcessor::default();
        let mut sched = Scheduler::new(source, InMemLeases::default());
        sched.run(&mut proc);
        assert_eq!(
            proc.events,
            vec!["init:s", "rec:s:10", "rec:s:11", "rec:s:12", "end:s"]
        );
    }

    /// MERGE case: two parents merge into one child. The child MUST NOT be
    /// touched until BOTH parents have reached SHARD_END. Sibling parents have
    /// no ordering guarantee relative to each other, so we assert positionally.
    /// Grounded in KCL ShutdownTask.createLeasesForChildShardsIfNotExist.
    #[test]
    fn merge_child_waits_for_both_parents() {
        let mut data = HashMap::new();
        data.insert("p-a".to_string(), vec![rec("p-a", "1"), rec("p-a", "2")]);
        data.insert("p-b".to_string(), vec![rec("p-b", "3"), rec("p-b", "4")]);
        data.insert("child".to_string(), vec![rec("child", "5")]);

        let source = InMemSource {
            metas: vec![
                ShardMeta {
                    id: "child".into(),
                    parents: vec!["p-a".into(), "p-b".into()],
                },
                ShardMeta {
                    id: "p-a".into(),
                    parents: vec![],
                },
                ShardMeta {
                    id: "p-b".into(),
                    parents: vec![],
                },
            ],
            data,
        };

        let mut proc = RecordingProcessor::default();
        let mut sched = Scheduler::new(source, InMemLeases::default());
        sched.run(&mut proc);

        let pos = |e: &str| proc.events.iter().position(|x| x == e).expect(e);
        let end_a = pos("end:p-a");
        let end_b = pos("end:p-b");
        let child_init = pos("init:child");
        let child_rec = pos("rec:child:5");
        assert!(
            child_init > end_a && child_init > end_b,
            "child before both parents ended: {:?}",
            proc.events
        );
        assert!(child_rec > child_init);
        assert!(pos("rec:p-a:1") < pos("rec:p-a:2") && pos("rec:p-a:2") < end_a);
        assert!(pos("rec:p-b:3") < pos("rec:p-b:4") && pos("rec:p-b:4") < end_b);
    }

    /// Deep reshard storm: g0 -> g1 -> g2 (split, then the child splits again).
    /// Strict ancestor-before-descendant ordering across multiple levels.
    #[test]
    fn reshard_storm_multilevel_ordering() {
        let mut data = HashMap::new();
        data.insert("g0".to_string(), vec![rec("g0", "1")]);
        data.insert("g1".to_string(), vec![rec("g1", "2")]);
        data.insert("g2".to_string(), vec![rec("g2", "3")]);
        let source = InMemSource {
            metas: vec![
                ShardMeta {
                    id: "g2".into(),
                    parents: vec!["g1".into()],
                },
                ShardMeta {
                    id: "g1".into(),
                    parents: vec!["g0".into()],
                },
                ShardMeta {
                    id: "g0".into(),
                    parents: vec![],
                },
            ],
            data,
        };
        let mut proc = RecordingProcessor::default();
        let mut sched = Scheduler::new(source, InMemLeases::default());
        sched.run(&mut proc);
        assert_eq!(
            proc.events,
            vec![
                "init:g0", "rec:g0:1", "end:g0", "init:g1", "rec:g1:2", "end:g1", "init:g2",
                "rec:g2:3", "end:g2",
            ]
        );
    }

    /// On restart with an existing checkpoint, only records AFTER the checkpoint
    /// are delivered (resume-from-checkpoint semantics).
    #[test]
    fn resumes_after_checkpoint() {
        let mut data = HashMap::new();
        data.insert(
            "s".to_string(),
            vec![rec("s", "10"), rec("s", "11"), rec("s", "12")],
        );
        let source = InMemSource {
            metas: vec![ShardMeta {
                id: "s".into(),
                parents: vec![],
            }],
            data,
        };
        let mut leases = InMemLeases::default();
        leases.checkpoint(&"s".to_string(), "11".to_string()); // <= 11 already processed
        let mut proc = RecordingProcessor::default();
        let mut sched = Scheduler::new(source, leases);
        sched.run(&mut proc);
        assert_eq!(proc.events, vec!["init:s", "rec:s:12", "end:s"]);
    }
}
