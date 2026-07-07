#![cfg(feature = "aws")]
//! Live integration test proving the `GetRecords` `max_records` limit is applied.
//! Skipped unless `DDB_STREAMS_CONSUMER_IT=1`. Creates a temp table+stream, writes
//! N records, and shows a limited source never returns a batch larger than the
//! limit (yet still delivers every record across paginated batches), while an
//! unlimited source returns a larger batch — proving the knob changes behavior.
//!
//! Run:
//!   DDB_STREAMS_CONSUMER_IT=1 cargo test -p amazon-dynamodb-streams-consumer-source \
//!     --features aws --test live_max_records -- --nocapture

use amazon_dynamodb_streams_consumer_source::aws::DdbStreamsSource;
use aws_sdk_dynamodb as ddb;
use aws_sdk_dynamodbstreams as streams;
use ddb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType, StreamSpecification, StreamViewType, TableStatus,
};
use std::time::Duration;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const N: usize = 6;
const LIMIT: i32 = 2;

#[tokio::test]
async fn live_max_records_caps_batch_size() {
    if std::env::var("DDB_STREAMS_CONSUMER_IT").is_err() {
        eprintln!("skipping live max_records integ test (set DDB_STREAMS_CONSUMER_IT=1 to run)");
        return;
    }

    let cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let db = ddb::Client::new(&cfg);
    let st = streams::Client::new(&cfg);
    let table = format!(
        "amazon-dynamodb-streams-consumer-maxrec-it-{}",
        std::process::id()
    );

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

    let stream_arn = loop {
        let d = db.describe_table().table_name(&table).send().await.unwrap();
        let t = d.table().unwrap();
        if t.table_status() == Some(&TableStatus::Active) {
            break t.latest_stream_arn().unwrap().to_string();
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    };

    for i in 0..N {
        db.put_item()
            .table_name(&table)
            .item("pk", AttributeValue::S(format!("k{i}")))
            .send()
            .await
            .unwrap();
    }

    let limited = DdbStreamsSource::new(st.clone(), &stream_arn).with_max_records(LIMIT);
    let unlimited = DdbStreamsSource::new(st, &stream_arn);

    let result = run(&limited, &unlimited).await;

    // Best-effort cleanup regardless of assertion outcome.
    let _ = db.delete_table().table_name(&table).send().await;

    result.expect("max_records live check");
}

async fn run(limited: &DdbStreamsSource, unlimited: &DdbStreamsSource) -> Result<(), BoxError> {
    // Step 1 — readiness gate + contrast: drain the UNLIMITED source until all N
    // records are visible. This both (a) proves every record has propagated to
    // the stream before we test the limited path (removing fresh-stream read
    // races) and (b) shows an unlimited read returns a batch larger than LIMIT.
    let (max_batch_u, _batches_u, total_u) = drain_all(unlimited).await?;
    if total_u < N {
        return Err(format!("stream not readable: unlimited source saw {total_u}/{N}").into());
    }
    if max_batch_u <= LIMIT as usize {
        return Err(format!(
            "unlimited source never exceeded {LIMIT} (limit effect not distinguishable): \
             max batch {max_batch_u}"
        )
        .into());
    }

    // Step 2 — the cap: with every record already visible, the LIMITED source
    // must never exceed LIMIT per batch, must still deliver all N, and therefore
    // must paginate into >= ceil(N/LIMIT) batches (an unlimited read returned
    // them in fewer, larger batches above).
    let (max_batch_l, batches_l, total_l) = drain_all(limited).await?;
    if max_batch_l > LIMIT as usize {
        return Err(format!("limited batch exceeded {LIMIT}: {max_batch_l}").into());
    }
    if total_l < N {
        return Err(format!("limited source lost records: {total_l} < {N}").into());
    }
    if batches_l < N.div_ceil(LIMIT as usize) {
        return Err(format!(
            "limited returned {batches_l} batches for {N} records at limit {LIMIT}; \
             expected the limit to force >= {} batches",
            N.div_ceil(LIMIT as usize)
        )
        .into());
    }

    eprintln!(
        "max_records OK: unlimited returned up to {max_batch_u}/batch; limited capped at \
         {max_batch_l}/batch across {batches_l} batches, delivering all {total_l} records"
    );
    Ok(())
}

/// Discover shards, then drain records from ALL root shards (a fresh table may
/// expose more than one open shard). Returns (largest batch seen, non-empty
/// batch count, total records). Retries to absorb the lag between writes and
/// stream visibility. Mirrors the proven read loop in `live_ddbstreams`.
async fn drain_all(source: &DdbStreamsSource) -> Result<(usize, usize, usize), BoxError> {
    let mut shards = Vec::new();
    for _ in 0..15 {
        shards = source.describe_shards().await?;
        if !shards.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    let mut max_batch = 0usize;
    let mut batches = 0usize;
    let mut total = 0usize;
    for shard in shards.iter().filter(|s| s.parents.is_empty()) {
        let mut after: Option<String> = None;
        // Be patient: under a small limit records arrive across many polls, and
        // a fresh stream returns empty batches between records.
        for _ in 0..30 {
            let batch = source.get_records(&shard.id, after.as_deref()).await?;
            if !batch.records.is_empty() {
                max_batch = max_batch.max(batch.records.len());
                batches += 1;
                total += batch.records.len();
                after = Some(batch.records.last().unwrap().seq.clone());
            }
            if total >= N || batch.shard_end {
                break;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        if total >= N {
            break;
        }
    }
    Ok((max_batch, batches, total))
}
