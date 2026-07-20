//! # amazon-dynamodb-streams-consumer (Rust client)
//!
//! A **native, JVM-free** DynamoDB Streams consumer for Rust. Unlike the
//! Python/Go/Node clients — which talk to a bundled Rust *sidecar* over stdio —
//! a Rust application depends on the engine crates **directly**, so the consumer
//! runs entirely **in-process**: no subprocess, no stdio bridge, no binary
//! download. It is the leanest possible consumer (one process, zero IPC).
//!
//! It provides the same guarantees as the other clients: per-shard ordered
//! delivery (parent-before-child across resharding), DynamoDB-lease
//! coordination with failover, and at-least-once checkpointing.
//!
//! ```no_run
//! use amazon_dynamodb_streams_consumer::{Worker, Record, RecordProcessor, RecordProcessorFactory};
//!
//! struct MyProcessor;
//! impl RecordProcessor for MyProcessor {
//!     fn process_records(&mut self, records: &[Record]) {
//!         for r in records {
//!             // Typed access (primary): r.keys / r.new_image / r.old_image are
//!             // maps of the strongly-typed AttrValue enum.
//!             println!("{:?} {:?}", r.event_name, r.keys);
//!             // Or a JSON view in the worker's configured record_format:
//!             if let Some(img) = r.new_image_json() {
//!                 println!("{img}");
//!             }
//!         }
//!     }
//! }
//!
//! struct MyFactory;
//! impl RecordProcessorFactory for MyFactory {
//!     fn create(&self, _shard_id: &str) -> Box<dyn RecordProcessor + Send> {
//!         Box::new(MyProcessor)
//!     }
//! }
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//! Worker::builder()
//!     .stream_arn("arn:aws:dynamodb:us-east-1:123456789012:table/Orders/stream/2026-...")
//!     .lease_table("my-app-leases")
//!     .processor(std::sync::Arc::new(MyFactory))
//!     .build()
//!     .await?
//!     .run()
//!     .await?;
//! # Ok(())
//! # }
//! ```

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use amazon_dynamodb_streams_consumer_core::coordinator::LeaseCoordinator;
pub use amazon_dynamodb_streams_consumer_core::record::{AttrValue, Item, StreamRecord};
pub use amazon_dynamodb_streams_consumer_core::InitialPosition;
use amazon_dynamodb_streams_consumer_core::{
    Record as CoreRecord, RecordProcessor as CoreProcessor, RecordProcessorFactory as CoreFactory,
    ShardId,
};
use amazon_dynamodb_streams_consumer_worker::fleet::{Fleet, FleetConfig, Leadership};
use amazon_dynamodb_streams_consumer_worker::SyncConsumerFactory;

use aws_sdk_dynamodbstreams as streams;
use base64::Engine as _;

/// How image attributes are surfaced by the JSON-view accessors on a [`Record`].
///
/// This is a **worker-level** setting (set once on the [`WorkerBuilder`]) that
/// applies to every record the whole processor sees, mirroring the
/// `record_format` option in the Python/Go/Node clients. The strongly-typed
/// [`AttrValue`] maps (`keys`, `new_image`, `old_image`) are **always** present
/// regardless of this setting; it only governs what the `*_json()` accessors
/// return.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum RecordFormat {
    /// Plain values: `S`/`N` → JSON string (numbers stay canonical strings to
    /// avoid float precision loss), `Bool` → bool, `Null` → null, `B` → base64
    /// string, `M` → object, `L` → array, sets → arrays. No `{"S": ...}` wrappers.
    #[default]
    Native,
    /// Canonical DynamoDB JSON (`{"S"|"N"|"BOOL"|"NULL"|"B"|"M"|"L"|"SS"|"NS"|"BS"}`),
    /// the shape the AWS SDKs and boto3's `TypeDeserializer` consume — for SDK
    /// interop or migrating from KCL.
    DdbJson,
}

/// A DynamoDB Streams change record delivered to a [`RecordProcessor`].
///
/// The item images are exposed **two** ways:
/// * strongly-typed [`AttrValue`] maps (`keys`, `new_image`, `old_image`) — the
///   primary, Rust-idiomatic shape; and
/// * `serde_json::Value` via the [`Record::keys_json`] / [`Record::new_image_json`]
///   / [`Record::old_image_json`] accessors, rendered in the worker's configured
///   [`RecordFormat`].
#[derive(Clone, Debug)]
pub struct Record {
    pub shard_id: String,
    pub sequence_number: String,
    /// INSERT / MODIFY / REMOVE.
    pub event_name: Option<String>,
    /// KEYS_ONLY / NEW_IMAGE / OLD_IMAGE / NEW_AND_OLD_IMAGES.
    pub stream_view_type: Option<String>,
    /// The key attributes of the changed item (always present).
    pub keys: Item,
    /// The item image after the change (present for INSERT/MODIFY with a
    /// NEW_IMAGE view).
    pub new_image: Option<Item>,
    /// The item image before the change (present for MODIFY/REMOVE with an
    /// OLD_IMAGE view).
    pub old_image: Option<Item>,
    format: RecordFormat,
}

impl Record {
    /// The `keys` map as a `serde_json::Value` in the worker's [`RecordFormat`].
    pub fn keys_json(&self) -> serde_json::Value {
        item_to_json(&self.keys, self.format)
    }
    /// The `new_image` as a `serde_json::Value` in the worker's [`RecordFormat`],
    /// or `None` when absent.
    pub fn new_image_json(&self) -> Option<serde_json::Value> {
        self.new_image
            .as_ref()
            .map(|i| item_to_json(i, self.format))
    }
    /// The `old_image` as a `serde_json::Value` in the worker's [`RecordFormat`],
    /// or `None` when absent.
    pub fn old_image_json(&self) -> Option<serde_json::Value> {
        self.old_image
            .as_ref()
            .map(|i| item_to_json(i, self.format))
    }
    /// The record format configured on the worker that produced this record.
    pub fn record_format(&self) -> RecordFormat {
        self.format
    }
}

/// Convert one [`AttrValue`] to a native `serde_json::Value` (no type wrappers).
/// Numbers stay strings to preserve DynamoDB's arbitrary precision; binary is
/// base64-encoded (raw bytes remain available via the typed [`AttrValue::B`]).
pub fn to_native_json(av: &AttrValue) -> serde_json::Value {
    use serde_json::Value;
    match av {
        AttrValue::S(s) => Value::String(s.clone()),
        AttrValue::N(n) => Value::String(n.clone()),
        AttrValue::Bool(b) => Value::Bool(*b),
        AttrValue::Null => Value::Null,
        AttrValue::B(b) => Value::String(b64(b)),
        AttrValue::M(m) => Value::Object(
            m.iter()
                .map(|(k, v)| (k.clone(), to_native_json(v)))
                .collect(),
        ),
        AttrValue::L(l) => Value::Array(l.iter().map(to_native_json).collect()),
        AttrValue::Ss(s) => Value::Array(s.iter().map(|x| Value::String(x.clone())).collect()),
        AttrValue::Ns(n) => Value::Array(n.iter().map(|x| Value::String(x.clone())).collect()),
        AttrValue::Bs(b) => Value::Array(b.iter().map(|x| Value::String(b64(x))).collect()),
    }
}

/// Convert one [`AttrValue`] to canonical DynamoDB JSON (the `{"S": ...}` typed
/// form the AWS SDKs consume).
pub fn to_ddb_json(av: &AttrValue) -> serde_json::Value {
    use serde_json::{Map, Value};
    let mut o = Map::new();
    match av {
        AttrValue::S(s) => {
            o.insert("S".into(), Value::String(s.clone()));
        }
        AttrValue::N(n) => {
            o.insert("N".into(), Value::String(n.clone()));
        }
        AttrValue::Bool(b) => {
            o.insert("BOOL".into(), Value::Bool(*b));
        }
        AttrValue::Null => {
            o.insert("NULL".into(), Value::Bool(true));
        }
        AttrValue::B(b) => {
            o.insert("B".into(), Value::String(b64(b)));
        }
        AttrValue::M(m) => {
            let inner: Map<String, Value> =
                m.iter().map(|(k, v)| (k.clone(), to_ddb_json(v))).collect();
            o.insert("M".into(), Value::Object(inner));
        }
        AttrValue::L(l) => {
            o.insert(
                "L".into(),
                Value::Array(l.iter().map(to_ddb_json).collect()),
            );
        }
        AttrValue::Ss(s) => {
            o.insert(
                "SS".into(),
                Value::Array(s.iter().map(|x| Value::String(x.clone())).collect()),
            );
        }
        AttrValue::Ns(n) => {
            o.insert(
                "NS".into(),
                Value::Array(n.iter().map(|x| Value::String(x.clone())).collect()),
            );
        }
        AttrValue::Bs(b) => {
            o.insert(
                "BS".into(),
                Value::Array(b.iter().map(|x| Value::String(b64(x))).collect()),
            );
        }
    }
    Value::Object(o)
}

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn item_to_json(item: &Item, fmt: RecordFormat) -> serde_json::Value {
    let conv = match fmt {
        RecordFormat::Native => to_native_json,
        RecordFormat::DdbJson => to_ddb_json,
    };
    serde_json::Value::Object(item.iter().map(|(k, v)| (k.clone(), conv(v))).collect())
}

/// Customer business logic: called with ordered batches for a single shard.
/// One instance is created per shard by a [`RecordProcessorFactory`].
pub trait RecordProcessor: Send {
    /// Deliver a batch of records, already in per-shard sequence order. Returning
    /// normally acknowledges the batch, which advances the durable checkpoint to
    /// its last record (at-least-once).
    fn process_records(&mut self, records: &[Record]);
    /// Called once before the first batch for this shard. Default: no-op.
    fn initialize(&mut self, _shard_id: &str) {}
    /// Called when the shard reaches SHARD_END (fully consumed). Default: no-op.
    fn shard_ended(&mut self, _shard_id: &str) {}
    /// Called when this worker loses the shard's lease (stolen or expired).
    /// Delivery has stopped and you must not checkpoint. Default: no-op.
    fn lease_lost(&mut self, _shard_id: &str) {}
}

/// Creates one [`RecordProcessor`] per shard (KCL's per-shard processor model).
/// Must be shareable across shard tasks.
pub trait RecordProcessorFactory: Send + Sync {
    fn create(&self, shard_id: &str) -> Box<dyn RecordProcessor + Send>;
}

// ---- Adapter: bridge the ergonomic processor to the core (byte-payload) one ----

struct Adapter {
    inner: Box<dyn RecordProcessor + Send>,
    format: RecordFormat,
    shard: String,
}

impl CoreProcessor for Adapter {
    fn initialize(&mut self, shard: &ShardId) {
        self.shard = shard.clone();
        self.inner.initialize(shard);
    }

    fn process_records(&mut self, records: &[CoreRecord]) {
        let mapped: Vec<Record> = records
            .iter()
            .map(|r| decode_record(r, self.format))
            .collect();
        self.inner.process_records(&mapped);
    }

    fn shard_ended(&mut self, shard: &ShardId) {
        self.inner.shard_ended(shard);
    }

    fn lease_lost(&mut self, shard: &ShardId) {
        self.inner.lease_lost(shard);
    }
}

/// Decode a core byte-payload record into the ergonomic [`Record`]. A record
/// whose payload fails to decode is surfaced with empty images rather than
/// dropped, so ordering/checkpointing is never silently skipped.
fn decode_record(r: &CoreRecord, format: RecordFormat) -> Record {
    let sr = StreamRecord::decode(&r.data).unwrap_or_default();
    Record {
        shard_id: r.shard_id.clone(),
        sequence_number: sr.sequence_number.clone().unwrap_or_else(|| r.seq.clone()),
        event_name: sr.event_name,
        stream_view_type: sr.stream_view_type,
        keys: sr.keys,
        new_image: sr.new_image,
        old_image: sr.old_image,
        format,
    }
}

struct FactoryAdapter {
    inner: Arc<dyn RecordProcessorFactory>,
    format: RecordFormat,
}

impl CoreFactory for FactoryAdapter {
    fn create(&self, shard: &ShardId) -> Box<dyn CoreProcessor + Send> {
        Box::new(Adapter {
            inner: self.inner.create(shard),
            format: self.format,
            shard: shard.clone(),
        })
    }
}

// ---------------------------- Worker + builder ----------------------------

/// Error type surfaced by the worker.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Builder for a [`Worker`]. Obtain via [`Worker::builder`].
pub struct WorkerBuilder {
    stream_arn: Option<String>,
    lease_table: Option<String>,
    region: Option<String>,
    owner: Option<String>,
    record_format: RecordFormat,
    max_leases: usize,
    lease_duration_ms: u64,
    poll_interval_ms: u64,
    initial_position: InitialPosition,
    processor: Option<Arc<dyn RecordProcessorFactory>>,
    max_processing_concurrency: Option<usize>,
}

impl Default for WorkerBuilder {
    fn default() -> Self {
        Self {
            stream_arn: None,
            lease_table: None,
            region: None,
            owner: None,
            record_format: RecordFormat::default(),
            max_leases: 100,
            lease_duration_ms: 60_000,
            poll_interval_ms: 1_000,
            initial_position: InitialPosition::default(),
            processor: None,
            max_processing_concurrency: None,
        }
    }
}

impl WorkerBuilder {
    /// The DynamoDB Streams ARN to consume (required).
    pub fn stream_arn(mut self, arn: impl Into<String>) -> Self {
        self.stream_arn = Some(arn.into());
        self
    }
    /// The DynamoDB table used to store shard leases + checkpoints (required).
    /// Created on first run if it does not exist.
    pub fn lease_table(mut self, name: impl Into<String>) -> Self {
        self.lease_table = Some(name.into());
        self
    }
    /// AWS region override. Defaults to the standard AWS environment resolution.
    pub fn region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }
    /// Unique worker identity used for lease ownership. Defaults to
    /// `"<hostname>-<pid>"`. Set explicitly per host when running a fleet.
    pub fn owner(mut self, owner: impl Into<String>) -> Self {
        self.owner = Some(owner.into());
        self
    }
    /// How image attributes are surfaced by the `*_json()` accessors — set once,
    /// applies to every record. Defaults to [`RecordFormat::Native`]. The typed
    /// [`AttrValue`] maps are always present regardless.
    pub fn record_format(mut self, fmt: RecordFormat) -> Self {
        self.record_format = fmt;
        self
    }
    /// Maximum number of shard leases this worker will hold at once (default 100).
    pub fn max_leases(mut self, n: usize) -> Self {
        self.max_leases = n;
        self
    }
    /// Lease duration in milliseconds before an unrenewed lease is stealable
    /// (default 60_000).
    pub fn lease_duration_ms(mut self, ms: u64) -> Self {
        self.lease_duration_ms = ms;
        self
    }
    /// Idle backoff between empty `GetRecords` polls within a shard task, in
    /// milliseconds (default 1_000).
    pub fn poll_interval_ms(mut self, ms: u64) -> Self {
        self.poll_interval_ms = ms;
        self
    }
    /// Where a shard begins when it is first consumed and has no checkpoint yet
    /// (default [`InitialPosition::TrimHorizon`]). Applies only to shards seeded
    /// while the lease table is empty; already-checkpointed shards and reshard
    /// children are unaffected.
    pub fn initial_position(mut self, position: InitialPosition) -> Self {
        self.initial_position = position;
        self
    }
    /// The per-shard processor factory (required).
    pub fn processor(mut self, factory: Arc<dyn RecordProcessorFactory>) -> Self {
        self.processor = Some(factory);
        self
    }

    /// Cap the number of shards this worker **processes concurrently** (opt-in).
    ///
    /// Unset (the default) keeps one processing slot per owned shard, so a
    /// worker's footprint grows with the stream's shard count. Setting
    /// `max` bounds concurrent record delivery to `max`, making footprint O(max)
    /// independent of shard count, while preserving at-least-once delivery,
    /// per-item ordering, and per-shard ordering (a shard is never split; each
    /// shard is processed by one slot at a time). Shard reads and lease
    /// heartbeats are not gated, so idle shards keep their leases. `0` is treated
    /// as unset (unbounded).
    pub fn max_processing_concurrency(mut self, max: usize) -> Self {
        self.max_processing_concurrency = Some(max);
        self
    }

    /// Resolve AWS clients and construct the [`Worker`].
    pub async fn build(self) -> Result<Worker, Error> {
        let stream_arn = self.stream_arn.ok_or("stream_arn is required")?;
        let lease_table = self.lease_table.ok_or("lease_table is required")?;
        let processor = self.processor.ok_or("processor is required")?;
        let owner = self.owner.unwrap_or_else(default_owner);

        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let Some(region) = self.region.clone() {
            loader = loader.region(aws_config::Region::new(region));
        }
        let cfg = loader.load().await;

        let st = streams::Client::new(&cfg);
        let ddb = aws_sdk_dynamodb::Client::new(&cfg);

        let source =
            amazon_dynamodb_streams_consumer_source::aws::DdbStreamsSource::new(st, &stream_arn);
        let leases = amazon_dynamodb_streams_consumer_lease::dynamodb::DynamoDbLeaseStore::new(
            ddb,
            &lease_table,
        );
        leases.ensure_table().await?;

        let core_factory: Arc<dyn CoreFactory> = Arc::new(FactoryAdapter {
            inner: processor,
            format: self.record_format,
        });
        let fleet = Fleet::new(
            source,
            leases,
            Arc::new(SyncConsumerFactory::new(core_factory)),
            FleetConfig {
                owner: owner.clone(),
                max_leases: self.max_leases,
                lease_duration_ms: self.lease_duration_ms,
                poll_interval_ms: self.poll_interval_ms,
                initial_position: self.initial_position,
            },
        )
        .with_max_processing_concurrency(self.max_processing_concurrency);

        Ok(Worker {
            fleet,
            owner,
            lease_duration_ms: self.lease_duration_ms,
            max_leases: self.max_leases,
            poll_interval_ms: self.poll_interval_ms,
            stop: Arc::new(AtomicBool::new(false)),
        })
    }
}

fn default_owner() -> String {
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "host".to_string());
    format!("{host}-{}", std::process::id())
}

type LiveFleet = Fleet<
    amazon_dynamodb_streams_consumer_source::aws::DdbStreamsSource,
    amazon_dynamodb_streams_consumer_lease::dynamodb::DynamoDbLeaseStore,
>;

/// A running (or ready-to-run) DynamoDB Streams consumer. Build via
/// [`Worker::builder`].
pub struct Worker {
    fleet: LiveFleet,
    owner: String,
    lease_duration_ms: u64,
    max_leases: usize,
    poll_interval_ms: u64,
    stop: Arc<AtomicBool>,
}

impl Worker {
    /// Start configuring a worker.
    pub fn builder() -> WorkerBuilder {
        WorkerBuilder::default()
    }

    /// A handle that can request a graceful shutdown from another task.
    pub fn stop_handle(&self) -> StopHandle {
        StopHandle(self.stop.clone())
    }

    /// Run coordination cycles until every shard is complete (a bounded/closing
    /// stream) or [`StopHandle::stop`] is called. On stop, owned leases are
    /// released so another worker can take over immediately.
    ///
    /// For a long-running stream that never closes, this loops indefinitely,
    /// delivering records and checkpointing, until stopped.
    pub async fn run(&self) -> Result<(), Error> {
        let mut coordinator =
            LeaseCoordinator::new(self.owner.clone(), self.max_leases, self.lease_duration_ms);
        let mut leadership = Leadership::new(self.owner.clone(), self.lease_duration_ms);
        let start = Instant::now();

        while !self.stop.load(Ordering::Relaxed) {
            let now_ms = start.elapsed().as_millis() as u64;
            let complete = self
                .fleet
                .run_cycle(&mut coordinator, &mut leadership, now_ms)
                .await?;
            if complete {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(self.poll_interval_ms)).await;
        }

        if self.stop.load(Ordering::Relaxed) {
            let _ = self.fleet.release_owned().await;
        }
        Ok(())
    }
}

/// A cloneable handle to request graceful shutdown of a [`Worker::run`] loop.
#[derive(Clone)]
pub struct StopHandle(Arc<AtomicBool>);

impl StopHandle {
    /// Request the worker to stop after the current cycle and release its leases.
    pub fn stop(&self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn sample_item() -> Item {
        let mut m = Item::new();
        m.insert("id".into(), AttrValue::N("42".into()));
        m.insert("name".into(), AttrValue::S("widget".into()));
        m.insert("active".into(), AttrValue::Bool(true));
        m.insert("deleted".into(), AttrValue::Null);
        m.insert("blob".into(), AttrValue::B(vec![1, 2, 3]));
        m.insert("tags".into(), AttrValue::Ss(vec!["a".into(), "b".into()]));
        m.insert(
            "scores".into(),
            AttrValue::Ns(vec!["1".into(), "2.5".into()]),
        );
        m.insert(
            "meta".into(),
            AttrValue::M(BTreeMap::from([(
                "k".to_string(),
                AttrValue::S("v".into()),
            )])),
        );
        m.insert(
            "list".into(),
            AttrValue::L(vec![AttrValue::N("7".into()), AttrValue::Null]),
        );
        m
    }

    #[test]
    fn native_json_has_no_type_wrappers_and_keeps_numbers_as_strings() {
        let v = item_to_json(&sample_item(), RecordFormat::Native);
        assert_eq!(v["id"], serde_json::json!("42")); // number stays a string
        assert_eq!(v["name"], serde_json::json!("widget"));
        assert_eq!(v["active"], serde_json::json!(true));
        assert_eq!(v["deleted"], serde_json::Value::Null);
        assert_eq!(v["blob"], serde_json::json!("AQID")); // base64 of [1,2,3]
        assert_eq!(v["tags"], serde_json::json!(["a", "b"]));
        assert_eq!(v["scores"], serde_json::json!(["1", "2.5"]));
        assert_eq!(v["meta"], serde_json::json!({ "k": "v" }));
        assert_eq!(v["list"], serde_json::json!(["7", null]));
    }

    #[test]
    fn ddb_json_is_canonical_typed_form() {
        let v = item_to_json(&sample_item(), RecordFormat::DdbJson);
        assert_eq!(v["id"], serde_json::json!({ "N": "42" }));
        assert_eq!(v["name"], serde_json::json!({ "S": "widget" }));
        assert_eq!(v["active"], serde_json::json!({ "BOOL": true }));
        assert_eq!(v["deleted"], serde_json::json!({ "NULL": true }));
        assert_eq!(v["blob"], serde_json::json!({ "B": "AQID" }));
        assert_eq!(v["tags"], serde_json::json!({ "SS": ["a", "b"] }));
        assert_eq!(v["scores"], serde_json::json!({ "NS": ["1", "2.5"] }));
        assert_eq!(v["meta"], serde_json::json!({ "M": { "k": { "S": "v" } } }));
        assert_eq!(
            v["list"],
            serde_json::json!({ "L": [ { "N": "7" }, { "NULL": true } ] })
        );
    }

    #[test]
    fn decode_record_populates_typed_and_json_views() {
        let mut keys = Item::new();
        keys.insert("pk".into(), AttrValue::S("k1".into()));
        let mut new_image = keys.clone();
        new_image.insert("n".into(), AttrValue::N("5".into()));
        let sr = StreamRecord {
            event_name: Some("INSERT".into()),
            sequence_number: Some("100".into()),
            stream_view_type: Some("NEW_IMAGE".into()),
            keys,
            new_image: Some(new_image),
            ..Default::default()
        };
        let core = CoreRecord {
            shard_id: "shard-1".into(),
            seq: "100".into(),
            data: sr.encode(),
        };

        let rec = decode_record(&core, RecordFormat::Native);
        assert_eq!(rec.shard_id, "shard-1");
        assert_eq!(rec.sequence_number, "100");
        assert_eq!(rec.event_name.as_deref(), Some("INSERT"));
        // typed view
        assert_eq!(rec.keys.get("pk"), Some(&AttrValue::S("k1".into())));
        // native json view
        assert_eq!(rec.new_image_json().unwrap()["n"], serde_json::json!("5"));

        // ddb_json worker view on the same record
        let rec2 = decode_record(&core, RecordFormat::DdbJson);
        assert_eq!(
            rec2.new_image_json().unwrap()["n"],
            serde_json::json!({ "N": "5" })
        );
    }

    #[test]
    fn undecodable_payload_yields_empty_images_not_panic() {
        let core = CoreRecord {
            shard_id: "s".into(),
            seq: "1".into(),
            data: b"not json".to_vec(),
        };
        let rec = decode_record(&core, RecordFormat::Native);
        assert_eq!(rec.sequence_number, "1"); // falls back to core seq
        assert!(rec.keys.is_empty());
        assert!(rec.new_image.is_none());
    }
}
