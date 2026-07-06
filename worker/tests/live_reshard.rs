//! Live resharding correctness test (operational, env-gated — NOT run in CI).
//!
//! Drives the real `Fleet` (leader-based single syncer + `CHILD_SHARDS`
//! incremental discovery) against a real DynamoDB stream while a writer produces
//! ordered, per-key load. A couple of minutes in, it raises the table's **warm
//! throughput**, which makes DynamoDB pre-split partitions → the stream reshards
//! (parents close, children open). It then keeps verifying correctness across
//! the split:
//!   * completeness  — every written record is observed (proves the leader
//!     discovered the NEW child shards via CHILD_SHARDS; missing post-split
//!     records would mean discovery failed)
//!   * no duplicates — no (shard, seq) delivered twice
//!   * per-shard order — seqs monotonic within a shard
//!   * per-KEY order across the split — each pk's `sk` strictly increasing even
//!     as its items move parent → child (the parent-before-child guarantee)
//!
//! Run (creates + deletes its own tables):
//!   DDB_STREAMS_RESHARD_IT=1 AWS_REGION=us-east-1 \
//!     cargo test -p amazon-dynamodb-streams-consumer-worker --features aws \
//!     --test live_reshard -- --nocapture --ignored

#![cfg(feature = "aws")]

use amazon_dynamodb_streams_consumer_core::coordinator::LeaseCoordinator;
use amazon_dynamodb_streams_consumer_core::record::{AttrValue, StreamRecord};
use amazon_dynamodb_streams_consumer_core::{
    Record, RecordProcessor, RecordProcessorFactory, ShardId,
};
use amazon_dynamodb_streams_consumer_lease::dynamodb::DynamoDbLeaseStore;
use amazon_dynamodb_streams_consumer_source::aws::DdbStreamsSource;
use amazon_dynamodb_streams_consumer_worker::fleet::{Fleet, FleetConfig, Leadership};
use amazon_dynamodb_streams_consumer_worker::SyncConsumerFactory;
use aws_sdk_dynamodb as ddb;
use aws_sdk_dynamodbstreams as streams;
use ddb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType, StreamSpecification, StreamViewType, TableStatus, WarmThroughput,
};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const NKEYS: usize = 40;

#[derive(Default)]
struct Checker {
    per_shard_last_seq: HashMap<String, String>,
    seen_shard_seq: HashSet<(String, String)>,
    per_key_last_sk: HashMap<String, i64>,
    observed: usize,
    dups: usize,
    shard_order_violations: usize,
    key_order_violations: usize,
    shards: HashSet<String>,
}

fn seq_ord(a: &str, b: &str) -> std::cmp::Ordering {
    a.len().cmp(&b.len()).then_with(|| a.cmp(b))
}

struct CheckFactory {
    checker: Arc<Mutex<Checker>>,
}
impl RecordProcessorFactory for CheckFactory {
    fn create(&self, _shard: &ShardId) -> Box<dyn RecordProcessor + Send> {
        Box::new(CheckProc {
            shard: String::new(),
            checker: self.checker.clone(),
        })
    }
}
struct CheckProc {
    shard: String,
    checker: Arc<Mutex<Checker>>,
}
impl RecordProcessor for CheckProc {
    fn initialize(&mut self, s: &ShardId) {
        self.shard = s.clone();
    }
    fn process_records(&mut self, rs: &[Record]) {
        let mut c = self.checker.lock().unwrap();
        c.shards.insert(self.shard.clone());
        for r in rs {
            // Dedup by (shard, seq).
            if !c.seen_shard_seq.insert((self.shard.clone(), r.seq.clone())) {
                c.dups += 1;
                continue;
            }
            c.observed += 1;
            // Per-shard monotonic ordering.
            if let Some(last) = c.per_shard_last_seq.get(&self.shard) {
                if seq_ord(&r.seq, last) == std::cmp::Ordering::Less {
                    c.shard_order_violations += 1;
                }
            }
            c.per_shard_last_seq
                .insert(self.shard.clone(), r.seq.clone());
            // Per-key ordering across the split (decode pk/sk from the payload).
            if let Ok(sr) = StreamRecord::decode(&r.data) {
                let pk = match sr.keys.get("pk") {
                    Some(AttrValue::S(s)) => Some(s.clone()),
                    _ => None,
                };
                let sk = match sr.keys.get("sk") {
                    Some(AttrValue::N(n)) => n.parse::<i64>().ok(),
                    _ => None,
                };
                if let (Some(pk), Some(sk)) = (pk, sk) {
                    if let Some(last) = c.per_key_last_sk.get(&pk) {
                        if sk <= *last {
                            c.key_order_violations += 1;
                        }
                    }
                    c.per_key_last_sk.insert(pk, sk);
                }
            }
        }
    }
    fn shard_ended(&mut self, _s: &ShardId) {}
}

fn secs_env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_reshard_correctness() {
    if std::env::var("DDB_STREAMS_RESHARD_IT").is_err() {
        eprintln!("skipping live reshard test (set DDB_STREAMS_RESHARD_IT=1 to run)");
        return;
    }
    let baseline_secs = secs_env("RESHARD_BASELINE_SECS", 120);
    let post_secs = secs_env("RESHARD_POST_SECS", 210);

    let cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let db = ddb::Client::new(&cfg);
    let st = streams::Client::new(&cfg);
    let pid = std::process::id();
    let data_table = format!("adsc-reshard-it-{pid}");
    let lease_table = format!("adsc-reshard-leases-it-{pid}");

    // --- provision: composite-key table (pk HASH, sk RANGE) + stream ---
    db.create_table()
        .table_name(&data_table)
        .attribute_definitions(attr_def("pk", ScalarAttributeType::S))
        .attribute_definitions(attr_def("sk", ScalarAttributeType::N))
        .key_schema(key("pk", KeyType::Hash))
        .key_schema(key("sk", KeyType::Range))
        .billing_mode(BillingMode::PayPerRequest)
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
    eprintln!("[setup] table={data_table} active; stream={stream_arn}");

    let checker = Arc::new(Mutex::new(Checker::default()));
    let written = Arc::new(AtomicUsize::new(0));
    let stop_writer = Arc::new(AtomicBool::new(false));
    let stop_consumer = Arc::new(AtomicBool::new(false));

    // --- writer: continuous, per-key ascending sk ---
    let writer = {
        let db = db.clone();
        let table = data_table.clone();
        let written = written.clone();
        let stop = stop_writer.clone();
        tokio::spawn(async move {
            let mut round: i64 = 0;
            while !stop.load(Ordering::Relaxed) {
                for k in 0..NKEYS {
                    if db
                        .put_item()
                        .table_name(&table)
                        .item("pk", AttributeValue::S(format!("k{k}")))
                        .item("sk", AttributeValue::N(round.to_string()))
                        .item("payload", AttributeValue::S("x".repeat(64)))
                        .send()
                        .await
                        .is_ok()
                    {
                        written.fetch_add(1, Ordering::Relaxed);
                    }
                }
                round += 1;
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            eprintln!("[writer] stopped after {round} rounds");
        })
    };

    // --- consumer: the Fleet (leader + CHILD_SHARDS incremental sync) ---
    let consumer = {
        let checker = checker.clone();
        let st = st.clone();
        let stream_arn = stream_arn.clone();
        let lease_table = lease_table.clone();
        let stop = stop_consumer.clone();
        tokio::spawn(async move {
            let source = DdbStreamsSource::new(st, &stream_arn);
            let leases = DynamoDbLeaseStore::from_env(&lease_table).await;
            leases.ensure_table().await.expect("lease table");
            let factory = Arc::new(SyncConsumerFactory::new(Arc::new(CheckFactory { checker })));
            let fleet = Fleet::new(
                source,
                leases,
                factory,
                FleetConfig {
                    owner: format!("reshard-w-{pid}"),
                    max_leases: 1024,
                    lease_duration_ms: 60_000,
                    poll_interval_ms: 200,
                    initial_position: Default::default(),
                },
            );
            let mut coord = LeaseCoordinator::new(format!("reshard-w-{pid}"), 1024, 60_000);
            let mut lead = Leadership::new(format!("reshard-w-{pid}"), 60_000);
            let start = Instant::now();
            while !stop.load(Ordering::Relaxed) {
                let now_ms = start.elapsed().as_millis() as u64;
                if let Err(e) = fleet.run_cycle(&mut coord, &mut lead, now_ms).await {
                    eprintln!("[consumer] cycle error: {e}");
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        })
    };

    let snap = |label: &str, checker: &Arc<Mutex<Checker>>, written: &Arc<AtomicUsize>| {
        let c = checker.lock().unwrap();
        eprintln!(
            "[{label}] written={} observed={} dups={} shard_order_viol={} key_order_viol={} shards={}",
            written.load(Ordering::Relaxed),
            c.observed,
            c.dups,
            c.shard_order_violations,
            c.key_order_violations,
            c.shards.len()
        );
    };

    // --- Phase A: baseline ---
    let start = Instant::now();
    while start.elapsed().as_secs() < baseline_secs {
        tokio::time::sleep(Duration::from_secs(15)).await;
        snap("baseline", &checker, &written);
    }
    let shards_before = checker.lock().unwrap().shards.len();

    // --- Trigger reshard: raise warm throughput → DynamoDB pre-splits ---
    eprintln!("[reshard] raising warm throughput to force partition splits...");
    match db
        .update_table()
        .table_name(&data_table)
        .warm_throughput(
            WarmThroughput::builder()
                .read_units_per_second(24_000)
                .write_units_per_second(24_000)
                .build(),
        )
        .send()
        .await
    {
        Ok(_) => eprintln!("[reshard] warm throughput update accepted"),
        Err(e) => eprintln!("[reshard] warm throughput update FAILED: {e}"),
    }

    // --- Phase B: post-split ---
    let post_start = Instant::now();
    while post_start.elapsed().as_secs() < post_secs {
        tokio::time::sleep(Duration::from_secs(15)).await;
        snap("post-split", &checker, &written);
    }

    // --- stop writer, drain to completeness ---
    stop_writer.store(true, Ordering::Relaxed);
    let _ = writer.await;
    let target = written.load(Ordering::Relaxed);
    eprintln!("[drain] writer stopped at {target}; draining...");
    let drain_start = Instant::now();
    loop {
        tokio::time::sleep(Duration::from_secs(5)).await;
        let obs = checker.lock().unwrap().observed;
        snap("drain", &checker, &written);
        if obs >= target || drain_start.elapsed().as_secs() > 120 {
            break;
        }
    }
    stop_consumer.store(true, Ordering::Relaxed);
    let _ = consumer.await;

    // --- cleanup BEFORE asserting so tables always get deleted ---
    let _ = db.delete_table().table_name(&data_table).send().await;
    let _ = db.delete_table().table_name(&lease_table).send().await;

    let c = checker.lock().unwrap();
    let shards_after = c.shards.len();
    eprintln!(
        "[VERDICT] written={target} observed={} dups={} shard_order_viol={} key_order_viol={} shards {}->{}",
        c.observed, c.dups, c.shard_order_violations, c.key_order_violations, shards_before, shards_after
    );
    let reshard_detected = shards_after > shards_before || shards_after > 1;
    eprintln!("[VERDICT] reshard_detected={reshard_detected}");

    assert_eq!(c.dups, 0, "duplicates delivered");
    assert_eq!(c.shard_order_violations, 0, "per-shard ordering violated");
    assert_eq!(
        c.key_order_violations, 0,
        "per-key ordering violated across reshard"
    );
    assert!(
        c.observed >= target,
        "incompleteness: observed {} < written {target} (child shards not fully drained?)",
        c.observed
    );
}

fn attr_def(name: &str, t: ScalarAttributeType) -> AttributeDefinition {
    AttributeDefinition::builder()
        .attribute_name(name)
        .attribute_type(t)
        .build()
        .unwrap()
}
fn key(name: &str, t: KeyType) -> KeySchemaElement {
    KeySchemaElement::builder()
        .attribute_name(name)
        .key_type(t)
        .build()
        .unwrap()
}
