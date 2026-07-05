//! End-to-end live integration test for the **Rust client** (`Worker`) against a
//! real DynamoDB stream: creates its own data + lease tables, writes items, runs
//! the in-process consumer until records are delivered, and asserts per-shard
//! ordering plus both the typed and native-JSON record views. Then cleans up.
//!
//! Skipped unless `DDB_STREAMS_CONSUMER_IT=1`.
//!
//! Run:
//!   DDB_STREAMS_CONSUMER_IT=1 AWS_REGION=us-east-1 \
//!     cargo test -p amazon-dynamodb-streams-consumer --test live_consumer -- --nocapture

use std::sync::{Arc, Mutex};
use std::time::Duration;

use amazon_dynamodb_streams_consumer::{
    AttrValue, Record, RecordFormat, RecordProcessor, RecordProcessorFactory, Worker,
};
use aws_sdk_dynamodb as ddb;
use ddb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType, StreamSpecification, StreamViewType, TableStatus,
};

type Sink = Arc<Mutex<Vec<Record>>>;

struct RecordingFactory {
    sink: Sink,
}
impl RecordProcessorFactory for RecordingFactory {
    fn create(&self, _shard_id: &str) -> Box<dyn RecordProcessor + Send> {
        Box::new(RecordingProc {
            sink: self.sink.clone(),
        })
    }
}
struct RecordingProc {
    sink: Sink,
}
impl RecordProcessor for RecordingProc {
    fn process_records(&mut self, records: &[Record]) {
        self.sink.lock().unwrap().extend_from_slice(records);
    }
}

#[tokio::test]
async fn live_rust_client_consumes_and_decodes() {
    if std::env::var("DDB_STREAMS_CONSUMER_IT").is_err() {
        eprintln!("skipping live rust client integ test (set DDB_STREAMS_CONSUMER_IT=1 to run)");
        return;
    }

    let cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let db = ddb::Client::new(&cfg);

    let pid = std::process::id();
    let data_table = format!("adsc-rustclient-it-{pid}");
    let lease_table = format!("adsc-rustclient-leases-it-{pid}");

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

    for i in 0..5 {
        db.put_item()
            .table_name(&data_table)
            .item("pk", AttributeValue::S(format!("k{i}")))
            .item("n", AttributeValue::N(i.to_string()))
            .send()
            .await
            .unwrap();
    }

    let result = run_consumer(&stream_arn, &lease_table).await;

    let _ = db.delete_table().table_name(&data_table).send().await;
    let _ = db.delete_table().table_name(&lease_table).send().await;

    let sink = result.expect("consumer run");
    let recs = sink.lock().unwrap();

    assert!(recs.len() >= 5, "expected >= 5 records, got {}", recs.len());

    // Per-shard ordering (DynamoDB Streams guarantees order within a shard).
    use std::collections::HashMap;
    let mut by_shard: HashMap<String, Vec<String>> = HashMap::new();
    for r in recs.iter() {
        by_shard
            .entry(r.shard_id.clone())
            .or_default()
            .push(r.sequence_number.clone());
    }
    for (shard, seqs) in &by_shard {
        let mut sorted = seqs.clone();
        sorted.sort_by(|a, b| a.len().cmp(&b.len()).then_with(|| a.cmp(b)));
        assert_eq!(seqs, &sorted, "shard {shard} out of order");
    }

    // Typed view is populated, and the native JSON view carries no type wrappers.
    let with_image = recs
        .iter()
        .find(|r| r.new_image.is_some())
        .expect("at least one record with a new image");
    assert!(
        matches!(with_image.keys.get("pk"), Some(AttrValue::S(_))),
        "typed keys should expose AttrValue::S for pk"
    );
    let img = with_image.new_image_json().unwrap();
    assert!(
        img.get("n").map(|v| v.is_string()).unwrap_or(false),
        "native json 'n' should be a bare string (no typed wrapper), got {img}"
    );

    eprintln!(
        "rust client consumed {} record(s) across {} shard(s)",
        recs.len(),
        by_shard.len()
    );
}

async fn run_consumer(
    stream_arn: &str,
    lease_table: &str,
) -> Result<Sink, Box<dyn std::error::Error + Send + Sync>> {
    let sink: Sink = Arc::new(Mutex::new(Vec::new()));
    let worker = Worker::builder()
        .stream_arn(stream_arn)
        .lease_table(lease_table)
        .record_format(RecordFormat::Native)
        .processor(Arc::new(RecordingFactory { sink: sink.clone() }))
        .poll_interval_ms(200)
        .build()
        .await?;

    let stop = worker.stop_handle();
    let worker = Arc::new(worker);
    let run_worker = worker.clone();
    let handle = tokio::spawn(async move { run_worker.run().await });

    // Poll until records arrive (bounded), then stop gracefully.
    for _ in 0..60 {
        if sink.lock().unwrap().len() >= 5 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    stop.stop();
    let _ = handle.await;

    Ok(sink)
}
