#![cfg(feature = "aws")]
//! Live integration test for **`max_processing_concurrency`** (multiplexing).
//!
//! Stands up a real DynamoDB table pre-split into many shards (via
//! `WarmThroughput`), writes records across many partition keys, then runs the
//! `Fleet` with a processing-concurrency cap and asserts:
//!   1. concurrent `deliver` calls never exceed the cap (the bound holds live),
//!   2. every written record is delivered exactly once (0 loss, 0 duplicate),
//!   3. per-shard sequence order is preserved.
//!
//! It also prints the process RSS + shard count as informational footprint data.
//!
//! Skipped unless `DDB_STREAMS_CONSUMER_IT=1`. Creates + deletes its own tables.
//!
//! Run:
//!   DDB_STREAMS_CONSUMER_IT=1 AWS_REGION=us-east-1 cargo test -p amazon-dynamodb-streams-consumer-worker \
//!     --features aws --test live_max_concurrency -- --nocapture

use amazon_dynamodb_streams_consumer_core::coordinator::LeaseCoordinator;
use amazon_dynamodb_streams_consumer_core::{Record, ShardId};
use amazon_dynamodb_streams_consumer_lease::dynamodb::DynamoDbLeaseStore;
use amazon_dynamodb_streams_consumer_source::aws::DdbStreamsSource;
use amazon_dynamodb_streams_consumer_worker::fleet::{Fleet, FleetConfig, Leadership};
use amazon_dynamodb_streams_consumer_worker::{
    AsyncShardConsumer, ShardConsumerFactory, WorkerError,
};
use aws_sdk_dynamodb as ddb;
use aws_sdk_dynamodbstreams as streams;
use ddb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType, StreamSpecification, StreamViewType, TableStatus, WarmThroughput,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const CAP: usize = 4;
const N_WRITES: usize = 200;

/// Env-overridable numeric knob (used to point the warm pre-split at the
/// account's current warm-throughput ceiling for scale runs).
fn env_u64(key: &str, default: u64) -> i64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default) as i64
}

/// Records live processing concurrency + per-shard delivery order.
struct ConcConsumer {
    shard: ShardId,
    cur: Arc<AtomicUsize>,
    max: Arc<AtomicUsize>,
    log: Arc<Mutex<Vec<(String, String)>>>, // (shard, seq) in delivery order
}
#[async_trait::async_trait]
impl AsyncShardConsumer for ConcConsumer {
    async fn deliver(&mut self, records: &[Record]) -> Result<Option<String>, WorkerError> {
        let now = self.cur.fetch_add(1, Ordering::SeqCst) + 1;
        self.max.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(40)).await; // widen the concurrency window
        {
            let mut l = self.log.lock().unwrap();
            for r in records {
                l.push((self.shard.clone(), r.seq.clone()));
            }
        }
        self.cur.fetch_sub(1, Ordering::SeqCst);
        Ok(records.last().map(|r| r.seq.clone()))
    }
    async fn shard_ended(&mut self) -> Result<(), WorkerError> {
        Ok(())
    }
}
struct ConcFactory {
    cur: Arc<AtomicUsize>,
    max: Arc<AtomicUsize>,
    log: Arc<Mutex<Vec<(String, String)>>>,
}
impl ShardConsumerFactory for ConcFactory {
    fn create(&self, shard: &ShardId) -> Box<dyn AsyncShardConsumer + Send> {
        Box::new(ConcConsumer {
            shard: shard.clone(),
            cur: self.cur.clone(),
            max: self.max.clone(),
            log: self.log.clone(),
        })
    }
}

/// DynamoDB Streams sequence numbers are numeric strings: order by (length, lexical).
fn seq_ord(a: &str, b: &str) -> std::cmp::Ordering {
    a.len().cmp(&b.len()).then_with(|| a.cmp(b))
}

fn rss_kb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmRSS:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|v| v.parse::<u64>().ok())
        })
        .unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_max_processing_concurrency_bounds_and_preserves_order() {
    if std::env::var("DDB_STREAMS_CONSUMER_IT").is_err() {
        eprintln!("skipping live max_processing_concurrency test (set DDB_STREAMS_CONSUMER_IT=1)");
        return;
    }

    let cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let db = ddb::Client::new(&cfg);
    let st = streams::Client::new(&cfg);

    let pid = std::process::id();
    let data_table = format!("ddbsc-maxconc-it-{pid}");
    let lease_table = format!("ddbsc-maxconc-leases-it-{pid}");

    // Pre-split into many shards via WarmThroughput so the cap has something to bind.
    db.create_table()
        .table_name(&data_table)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .warm_throughput(
            WarmThroughput::builder()
                .read_units_per_second(env_u64("DDB_STREAMS_CONSUMER_IT_WARM_READS", 12_000))
                .write_units_per_second(env_u64("DDB_STREAMS_CONSUMER_IT_WARM_WRITES", 20_000))
                .build(),
        )
        .stream_specification(
            StreamSpecification::builder()
                .stream_enabled(true)
                .stream_view_type(StreamViewType::NewAndOldImages)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("create data table");

    let stream_arn = loop {
        let d = db
            .describe_table()
            .table_name(&data_table)
            .send()
            .await
            .unwrap();
        let t = d.table().unwrap();
        if t.table_status() == Some(&TableStatus::Active) {
            break t.latest_stream_arn().unwrap().to_string();
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    };

    // Wait for the pre-split to surface multiple open shards.
    let shard_count = loop {
        let d = st
            .describe_stream()
            .stream_arn(&stream_arn)
            .send()
            .await
            .unwrap();
        let n = d
            .stream_description()
            .map(|s| s.shards().len())
            .unwrap_or(0);
        if n > CAP {
            break n;
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    };

    // Write across many distinct partition keys so records land on many shards.
    for i in 0..N_WRITES {
        db.put_item()
            .table_name(&data_table)
            .item("pk", AttributeValue::S(format!("k{i}")))
            .item("v", AttributeValue::N(i.to_string()))
            .send()
            .await
            .unwrap();
    }

    let cur = Arc::new(AtomicUsize::new(0));
    let max = Arc::new(AtomicUsize::new(0));
    let log = Arc::new(Mutex::new(Vec::new()));
    let result = run_capped(
        &st,
        &stream_arn,
        &lease_table,
        cur,
        max.clone(),
        log.clone(),
    )
    .await;

    let rss = rss_kb();
    let _ = db.delete_table().table_name(&data_table).send().await;
    let _ = db.delete_table().table_name(&lease_table).send().await;

    result.expect("capped fleet run");

    let observed_max = max.load(Ordering::SeqCst);
    let log = log.lock().unwrap();
    eprintln!(
        "shards={shard_count} cap={CAP} observed_max_concurrency={observed_max} \
         delivered={} rss={}MB",
        log.len(),
        rss / 1024
    );

    // 1) The cap binds live.
    assert!(
        observed_max <= CAP,
        "observed concurrency {observed_max} exceeded cap {CAP}"
    );
    // 2) Genuine concurrency occurred (not accidentally serialized).
    assert!(
        observed_max >= 2,
        "expected real concurrency, got {observed_max}"
    );

    // 3) 0 loss, 0 duplicate: every written record delivered exactly once.
    let mut seqs: Vec<String> = log.iter().map(|(_, q)| q.clone()).collect();
    let total = seqs.len();
    seqs.sort_by(|a, b| seq_ord(a, b));
    seqs.dedup();
    assert_eq!(seqs.len(), total, "duplicate delivery detected");
    assert_eq!(
        total, N_WRITES,
        "expected {N_WRITES} records, got {total} (loss/dup)"
    );

    // 4) Per-shard sequence order preserved.
    let mut by_shard: std::collections::HashMap<String, Vec<String>> = Default::default();
    for (s, q) in log.iter() {
        by_shard.entry(s.clone()).or_default().push(q.clone());
    }
    for (shard, delivered) in &by_shard {
        let mut sorted = delivered.clone();
        sorted.sort_by(|a, b| seq_ord(a, b));
        assert_eq!(
            delivered, &sorted,
            "shard {shard} delivered out of seq order"
        );
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_capped(
    st: &streams::Client,
    stream_arn: &str,
    lease_table: &str,
    cur: Arc<AtomicUsize>,
    max: Arc<AtomicUsize>,
    log: Arc<Mutex<Vec<(String, String)>>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let source = DdbStreamsSource::new(st.clone(), stream_arn);
    let leases = DynamoDbLeaseStore::from_env(lease_table).await;
    leases.ensure_table().await?;

    let factory = Arc::new(ConcFactory {
        cur,
        max,
        log: log.clone(),
    });
    let fleet = Fleet::new(
        source,
        leases,
        factory,
        FleetConfig {
            owner: "maxconc-w1".into(),
            max_leases: 1000,
            lease_duration_ms: 120_000,
            poll_interval_ms: 100,
            initial_position: Default::default(),
        },
    )
    .with_max_processing_concurrency(Some(CAP));

    let mut coordinator = LeaseCoordinator::new("maxconc-w1".to_string(), 1000, 120_000);
    let mut leadership = Leadership::new("maxconc-w1", 120_000);
    let start = std::time::Instant::now();
    // Drive cycles until we've drained all writes (open shards never SHARD_END).
    for _ in 0..30 {
        let now_ms = start.elapsed().as_millis() as u64;
        let _ = fleet
            .run_cycle(&mut coordinator, &mut leadership, now_ms)
            .await?;
        if log.lock().unwrap().len() >= N_WRITES {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}
