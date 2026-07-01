#![cfg(feature = "aws")]
//! End-to-end live integration test for the **per-shard-task fleet runtime**
//! (`Fleet`, concurrency model "A"). It composes a real `DdbStreamsSource` +
//! `DynamoDbLeaseStore` + a `RecordProcessorFactory` and drives coordination
//! cycles against a real DynamoDB stream: each cycle scans leases, the
//! `LeaseCoordinator` decides takes, shard-sync creates leases for eligible
//! shards, and one concurrent task per owned shard delivers records in order and
//! checkpoints under the optimistic lock.
//!
//! Skipped unless `DDBSTREAMS_KCL_IT=1`. Creates + deletes its own data table
//! and lease table.
//!
//! Run:
//!   DDBSTREAMS_KCL_IT=1 AWS_REGION=us-east-1 cargo test -p ddbstreams-kcl-worker \
//!     --features aws --test live_fleet -- --nocapture

use aws_sdk_dynamodb as ddb;
use aws_sdk_dynamodbstreams as streams;
use ddb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType, StreamSpecification, StreamViewType, TableStatus,
};
use ddbstreams_kcl_core::coordinator::LeaseCoordinator;
use ddbstreams_kcl_core::{Record, RecordProcessor, RecordProcessorFactory, ShardId};
use ddbstreams_kcl_lease_dynamodb::dynamodb::DynamoDbLeaseStore;
use ddbstreams_kcl_source_ddbstreams::aws::DdbStreamsSource;
use ddbstreams_kcl_worker::fleet::{Fleet, FleetConfig};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// shard id -> (sequence, payload byte length), in delivery order.
type Sink = Arc<Mutex<HashMap<String, Vec<(String, usize)>>>>;

struct RecordingFactory {
    sink: Sink,
}
impl RecordProcessorFactory for RecordingFactory {
    fn create(&self, _shard: &ShardId) -> Box<dyn RecordProcessor + Send> {
        Box::new(RecordingProc { shard: String::new(), sink: self.sink.clone() })
    }
}
struct RecordingProc {
    shard: String,
    sink: Sink,
}
impl RecordProcessor for RecordingProc {
    fn initialize(&mut self, s: &ShardId) {
        self.shard = s.clone();
    }
    fn process_records(&mut self, rs: &[Record]) {
        let mut m = self.sink.lock().unwrap();
        for r in rs {
            m.entry(self.shard.clone()).or_default().push((r.seq.clone(), r.data.len()));
        }
    }
    fn shard_ended(&mut self, _s: &ShardId) {}
}

/// DDB Streams sequence numbers are stringified big integers → compare by
/// (length, lexical) to reflect numeric magnitude.
fn seq_ord(a: &str, b: &str) -> std::cmp::Ordering {
    a.len().cmp(&b.len()).then_with(|| a.cmp(b))
}

#[tokio::test]
async fn live_fleet_consumes_and_checkpoints() {
    if std::env::var("DDBSTREAMS_KCL_IT").is_err() {
        eprintln!("skipping live fleet integ test (set DDBSTREAMS_KCL_IT=1 to run)");
        return;
    }

    let cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let db = ddb::Client::new(&cfg);
    let st = streams::Client::new(&cfg);

    let pid = std::process::id();
    let data_table = format!("ddbstreams-kcl-fleet-it-{pid}");
    let lease_table = format!("ddbstreams-kcl-fleet-leases-it-{pid}");

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
        let d = db.describe_table().table_name(&data_table).send().await.unwrap();
        let t = d.table().unwrap();
        if t.table_status() == Some(&TableStatus::Active) {
            break t.latest_stream_arn().unwrap().to_string();
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    };

    for i in 0..5 {
        db.put_item()
            .table_name(&data_table)
            .item("pk", AttributeValue::S(format!("k{i}")))
            .send()
            .await
            .unwrap();
    }

    // Drive the fleet; collect raw results (no asserts here so cleanup always runs).
    let result = run_fleet(&cfg, &st, &stream_arn, &lease_table).await;

    let _ = db.delete_table().table_name(&data_table).send().await;
    let _ = db.delete_table().table_name(&lease_table).send().await;

    let (sink, checkpoint) = result.expect("fleet run");
    let m = sink.lock().unwrap();

    let total: usize = m.values().map(|v| v.len()).sum();
    eprintln!(
        "fleet consumed {} record(s) across {} shard(s); checkpoint = {checkpoint:?}",
        total,
        m.len()
    );

    assert!(total >= 5, "expected >= 5 records, got {total}");

    // Per-shard ordering (DynamoDB Streams guarantees order WITHIN a shard only).
    for (shard, recs) in m.iter() {
        let seqs: Vec<String> = recs.iter().map(|(s, _)| s.clone()).collect();
        let mut sorted = seqs.clone();
        sorted.sort_by(|a, b| seq_ord(a, b));
        assert_eq!(&seqs, &sorted, "shard {shard} records out of order");
    }

    // Payloads flowed through the fleet (NewImage present → non-empty data).
    let any_payload = m.values().flatten().any(|(_, len)| *len > 0);
    assert!(any_payload, "expected at least one record to carry a non-empty payload");

    // A checkpoint must be persisted in the lease table.
    assert!(checkpoint.is_some(), "expected a lease checkpoint to be persisted");
}

async fn run_fleet(
    cfg: &aws_config::SdkConfig,
    st: &streams::Client,
    stream_arn: &str,
    lease_table: &str,
) -> Result<(Sink, Option<String>), Box<dyn std::error::Error + Send + Sync>> {
    let source = DdbStreamsSource::new(st.clone(), stream_arn);
    let leases = DynamoDbLeaseStore::from_env(lease_table).await;
    leases.ensure_table().await?;
    let leases_check = DynamoDbLeaseStore::new(ddb::Client::new(cfg), lease_table);

    let sink: Sink = Arc::new(Mutex::new(HashMap::new()));
    let factory = Arc::new(RecordingFactory { sink: sink.clone() });
    let fleet = Fleet::new(
        source,
        leases,
        factory,
        FleetConfig {
            owner: "fleet-w1".into(),
            max_leases: 100,
            lease_duration_ms: 60_000,
            poll_interval_ms: 100,
        },
    );

    // A fresh on-demand table's stream shard is open (no SHARD_END), so the
    // drain never "completes"; drive bounded cycles until records appear, then
    // stop after the first record-producing cycle for a clean single-pass
    // ordering assertion (re-reading from TRIM_HORIZON each cycle would
    // otherwise re-deliver the same records).
    let mut coordinator = LeaseCoordinator::new("fleet-w1".to_string(), 100, 60_000);
    let start = std::time::Instant::now();
    for _ in 0..25 {
        let now_ms = start.elapsed().as_millis() as u64;
        let _ = fleet.run_cycle(&mut coordinator, now_ms).await?;
        if sink.lock().unwrap().values().map(|v| v.len()).sum::<usize>() >= 5 {
            break;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    // Find a persisted checkpoint on any shard that delivered records.
    let mut checkpoint = None;
    let shards: Vec<String> = sink.lock().unwrap().keys().cloned().collect();
    for shard in shards {
        if let Some(l) = leases_check.get(&shard).await? {
            if l.checkpoint.is_some() {
                checkpoint = l.checkpoint;
                break;
            }
        }
    }
    Ok((sink, checkpoint))
}
