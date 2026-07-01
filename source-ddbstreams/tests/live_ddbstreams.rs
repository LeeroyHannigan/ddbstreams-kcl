#![cfg(feature = "aws")]
//! Live integration test against real DynamoDB Streams.
//!
//! Skipped unless `DDBSTREAMS_KCL_IT=1` so ordinary `cargo test` stays offline.
//! It creates a temporary `PAY_PER_REQUEST` table with `NEW_AND_OLD_IMAGES`
//! streams, writes ordered items, reads them back through `DdbStreamsSource`,
//! asserts the adapter surfaces the shard graph and records, then deletes the
//! table (best-effort cleanup).
//!
//! Run:
//!   DDBSTREAMS_KCL_IT=1 cargo test -p ddbstreams-kcl-source-ddbstreams \
//!     --features aws --test live_ddbstreams -- --nocapture

use aws_sdk_dynamodb as ddb;
use aws_sdk_dynamodbstreams as streams;
use ddb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType, StreamSpecification, StreamViewType, TableStatus,
};
use ddbstreams_kcl_source_ddbstreams::aws::DdbStreamsSource;
use std::time::Duration;

#[tokio::test]
async fn live_read_ordered_records() {
    if std::env::var("DDBSTREAMS_KCL_IT").is_err() {
        eprintln!("skipping live integ test (set DDBSTREAMS_KCL_IT=1 to run)");
        return;
    }

    let cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let db = ddb::Client::new(&cfg);
    let st = streams::Client::new(&cfg);

    let table = format!("ddbstreams-kcl-it-{}", std::process::id());
    eprintln!("creating temp table {table}");

    db.create_table()
        .table_name(&table)
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
        .expect("create_table");

    // Wait for ACTIVE + capture the stream ARN.
    let stream_arn = loop {
        let d = db.describe_table().table_name(&table).send().await.unwrap();
        let t = d.table().unwrap();
        if t.table_status() == Some(&TableStatus::Active) {
            break t.latest_stream_arn().unwrap().to_string();
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    };
    eprintln!("stream arn: {stream_arn}");

    // Write 5 ordered items.
    for i in 0..5 {
        db.put_item()
            .table_name(&table)
            .item("pk", AttributeValue::S(format!("k{i}")))
            .send()
            .await
            .unwrap();
    }

    let source = DdbStreamsSource::new(st, &stream_arn);

    let result = run_read(&source).await;

    // Best-effort cleanup regardless of assertion outcome.
    let _ = db.delete_table().table_name(&table).send().await;

    let (total, payload_ok) = result.expect("read from stream");
    eprintln!("read {total} records from the stream (payload_ok={payload_ok})");
    assert!(total >= 5, "expected >= 5 records, got {total}");
    assert!(payload_ok, "expected record payload to decode with the 'pk' key");
}

/// Discover shards via the adapter and drain records from the root shard(s),
/// retrying because stream records lag writes by a moment.
async fn run_read(source: &DdbStreamsSource) -> Result<(usize, bool), Box<dyn std::error::Error + Send + Sync>> {
    use ddbstreams_kcl_source_ddbstreams::record::StreamRecord;
    // Retry describe until at least one shard shows up.
    let mut shards = Vec::new();
    for _ in 0..15 {
        shards = source.describe_shards().await?;
        if !shards.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    assert!(!shards.is_empty(), "no shards discovered");

    // Read from root shards (no parents) — a fresh table has a single open shard.
    let mut total = 0usize;
    let mut payload_ok = false;
    for shard in shards.iter().filter(|s| s.parents.is_empty()) {
        let mut after: Option<String> = None;
        for _ in 0..15 {
            let batch = source.get_records(&shard.id, after.as_deref()).await?;
            if !batch.records.is_empty() {
                // Verify the typed payload decodes and carries the item key.
                if let Ok(sr) = StreamRecord::decode(&batch.records[0].data) {
                    if sr.keys.contains_key("pk") {
                        payload_ok = true;
                    }
                }
                after = Some(batch.records.last().unwrap().seq.clone());
                total += batch.records.len();
            }
            if total >= 5 || batch.shard_end {
                break;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
    Ok((total, payload_ok))
}
