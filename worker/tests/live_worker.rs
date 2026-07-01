#![cfg(feature = "aws")]
//! End-to-end live integration test: the `Worker` composes a real
//! `DdbStreamsSource` + `DynamoDbLeaseStore` + a recording processor and
//! consumes a real DynamoDB stream, acquiring a lease and checkpointing in
//! DynamoDB. Skipped unless `DDBSTREAMS_KCL_IT=1`. Creates + deletes its own
//! data table and lease table.
//!
//! Run:
//!   DDBSTREAMS_KCL_IT=1 cargo test -p ddbstreams-kcl-worker \
//!     --features aws --test live_worker -- --nocapture

use aws_sdk_dynamodb as ddb;
use aws_sdk_dynamodbstreams as streams;
use ddb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType, StreamSpecification, StreamViewType, TableStatus,
};
use ddbstreams_kcl_core::{Record, RecordProcessor, ShardId};
use ddbstreams_kcl_lease_dynamodb::dynamodb::DynamoDbLeaseStore;
use ddbstreams_kcl_source_ddbstreams::aws::DdbStreamsSource;
use ddbstreams_kcl_worker::Worker;
use std::collections::HashMap;
use std::time::Duration;

#[derive(Default)]
struct Recording {
    /// shard id -> sequence numbers, in delivery order.
    by_shard: HashMap<String, Vec<String>>,
    total: usize,
}
impl RecordProcessor for Recording {
    fn initialize(&mut self, _s: &ShardId) {}
    fn process_records(&mut self, rs: &[Record]) {
        for r in rs {
            self.by_shard.entry(r.shard_id.clone()).or_default().push(r.seq.clone());
            self.total += 1;
        }
    }
    fn shard_ended(&mut self, _s: &ShardId) {}
}

/// DDB Streams sequence numbers are stringified big integers → compare by
/// (length, lexical) to reflect numeric magnitude.
fn seq_lt(a: &str, b: &str) -> std::cmp::Ordering {
    a.len().cmp(&b.len()).then_with(|| a.cmp(b))
}

#[tokio::test]
async fn live_worker_consumes_and_checkpoints() {
    if std::env::var("DDBSTREAMS_KCL_IT").is_err() {
        eprintln!("skipping live worker integ test (set DDBSTREAMS_KCL_IT=1 to run)");
        return;
    }

    let cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let db = ddb::Client::new(&cfg);
    let st = streams::Client::new(&cfg);

    let pid = std::process::id();
    let data_table = format!("ddbstreams-kcl-worker-it-{pid}");
    let lease_table = format!("ddbstreams-kcl-worker-leases-it-{pid}");

    db.create_table()
        .table_name(&data_table)
        .attribute_definitions(AttributeDefinition::builder().attribute_name("pk").attribute_type(ScalarAttributeType::S).build().unwrap())
        .key_schema(KeySchemaElement::builder().attribute_name("pk").key_type(KeyType::Hash).build().unwrap())
        .billing_mode(BillingMode::PayPerRequest)
        .stream_specification(StreamSpecification::builder().stream_enabled(true).stream_view_type(StreamViewType::NewAndOldImages).build().unwrap())
        .send().await.expect("create data table");

    let stream_arn = loop {
        let d = db.describe_table().table_name(&data_table).send().await.unwrap();
        let t = d.table().unwrap();
        if t.table_status() == Some(&TableStatus::Active) {
            break t.latest_stream_arn().unwrap().to_string();
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    };

    for i in 0..5 {
        db.put_item().table_name(&data_table)
            .item("pk", AttributeValue::S(format!("k{i}")))
            .send().await.unwrap();
    }

    // Run the worker; collect raw results (no asserts here so cleanup always runs).
    let result = run_worker(&cfg, &st, &stream_arn, &lease_table).await;

    let _ = db.delete_table().table_name(&data_table).send().await;
    let _ = db.delete_table().table_name(&lease_table).send().await;

    let (rec, checkpoint) = result.expect("worker run");
    eprintln!(
        "worker consumed {} records across {} shard(s); checkpoint = {checkpoint:?}",
        rec.total,
        rec.by_shard.len()
    );

    assert!(rec.total >= 5, "expected >= 5 records, got {}", rec.total);
    // DynamoDB Streams guarantees ordering WITHIN a shard only — assert each
    // shard's delivered records are in sequence order.
    for (shard, seqs) in &rec.by_shard {
        let mut sorted = seqs.clone();
        sorted.sort_by(|a, b| seq_lt(a, b));
        assert_eq!(seqs, &sorted, "shard {shard} records out of order");
    }
    assert!(checkpoint.is_some(), "expected a lease checkpoint to be persisted");
}

async fn run_worker(
    cfg: &aws_config::SdkConfig,
    st: &streams::Client,
    stream_arn: &str,
    lease_table: &str,
) -> Result<(Recording, Option<String>), Box<dyn std::error::Error + Send + Sync>> {
    let source = DdbStreamsSource::new(st.clone(), stream_arn);
    let leases = DynamoDbLeaseStore::from_env(lease_table).await;
    leases.ensure_table().await?;
    let leases_check = DynamoDbLeaseStore::new(ddb::Client::new(cfg), lease_table);

    let worker = Worker::new(source, leases, "worker-1");
    let mut proc = Recording::default();

    // Open shards + record lag → step a bounded number of cycles until all 5 seen.
    for _ in 0..25 {
        let _ = worker.run_once(&mut proc).await?;
        if proc.total >= 5 {
            break;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    // A checkpoint must be persisted for at least one shard that delivered records.
    let mut checkpoint = None;
    for shard in proc.by_shard.keys() {
        if let Some(l) = leases_check.get(shard).await? {
            if l.checkpoint.is_some() {
                checkpoint = l.checkpoint;
                break;
            }
        }
    }
    Ok((proc, checkpoint))
}
