//! Live integration test for `InitialPosition::Latest` against a real DynamoDB
//! stream. It writes a batch of records BEFORE the consumer starts, runs the
//! consumer seeded at `Latest`, then writes a second batch AFTER the consumer's
//! shard iterators are established, and asserts that only the post-start records
//! are delivered (the pre-start batch is skipped). Then cleans up.
//!
//! Skipped unless `DDB_STREAMS_CONSUMER_IT=1`.
//!
//! Run:
//!   DDB_STREAMS_CONSUMER_IT=1 AWS_REGION=us-east-1 \
//!     cargo test -p amazon-dynamodb-streams-consumer --test live_latest -- --nocapture

use std::sync::{Arc, Mutex};
use std::time::Duration;

use amazon_dynamodb_streams_consumer::{
    AttrValue, InitialPosition, Record, RecordFormat, RecordProcessor, RecordProcessorFactory,
    Worker,
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

async fn put(db: &ddb::Client, table: &str, pk: &str) {
    db.put_item()
        .table_name(table)
        .item("pk", AttributeValue::S(pk.to_string()))
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn live_latest_skips_pre_start_records() {
    if std::env::var("DDB_STREAMS_CONSUMER_IT").is_err() {
        eprintln!("skipping live LATEST integ test (set DDB_STREAMS_CONSUMER_IT=1 to run)");
        return;
    }

    let cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let db = ddb::Client::new(&cfg);

    let pid = std::process::id();
    let data_table = format!("adsc-latest-it-{pid}");
    let lease_table = format!("adsc-latest-leases-it-{pid}");

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

    // Pre-start batch: written well before the consumer establishes its LATEST
    // iterators, so it sits strictly behind the tip and must be skipped.
    for i in 0..5 {
        put(&db, &data_table, &format!("old-{i}")).await;
    }
    // Let these settle behind the tip.
    tokio::time::sleep(Duration::from_secs(8)).await;

    let sink: Sink = Arc::new(Mutex::new(Vec::new()));
    let worker = Worker::builder()
        .stream_arn(&stream_arn)
        .lease_table(&lease_table)
        .record_format(RecordFormat::Native)
        .initial_position(InitialPosition::Latest)
        .processor(Arc::new(RecordingFactory { sink: sink.clone() }))
        .poll_interval_ms(200)
        .build()
        .await
        .expect("build worker");

    let stop = worker.stop_handle();
    let worker = Arc::new(worker);
    let run_worker = worker.clone();
    let handle = tokio::spawn(async move { run_worker.run().await });

    // Give the fleet time to seed leases at LATEST and derive shard iterators
    // (leader shard-sync cycle + GetShardIterator) before writing new records.
    tokio::time::sleep(Duration::from_secs(12)).await;

    // Post-start batch: written after the LATEST iterators exist → must arrive.
    for i in 0..5 {
        put(&db, &data_table, &format!("new-{i}")).await;
    }

    // Wait until the 5 new records are delivered (bounded).
    for _ in 0..60 {
        let seen_new = sink
            .lock()
            .unwrap()
            .iter()
            .filter(|r| pk_of(r).is_some_and(|k| k.starts_with("new-")))
            .count();
        if seen_new >= 5 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    stop.stop();
    let _ = handle.await;

    let _ = db.delete_table().table_name(&data_table).send().await;
    let _ = db.delete_table().table_name(&lease_table).send().await;

    let recs = sink.lock().unwrap();
    let keys: Vec<String> = recs.iter().filter_map(pk_of).collect();
    eprintln!("LATEST consumer delivered keys: {keys:?}");

    // Core assertion: the pre-start ("old-") batch was skipped entirely.
    let leaked_old: Vec<&String> = keys.iter().filter(|k| k.starts_with("old-")).collect();
    assert!(
        leaked_old.is_empty(),
        "LATEST must skip pre-start records, but delivered: {leaked_old:?}"
    );

    // And the post-start ("new-") batch was fully delivered.
    let new_count = keys.iter().filter(|k| k.starts_with("new-")).count();
    assert_eq!(
        new_count, 5,
        "expected all 5 post-start records under LATEST, got {new_count} (keys: {keys:?})"
    );
}

fn pk_of(r: &Record) -> Option<String> {
    match r.keys.get("pk") {
        Some(AttrValue::S(s)) => Some(s.clone()),
        _ => None,
    }
}
