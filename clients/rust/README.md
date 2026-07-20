# amazon-dynamodb-streams-consumer — Rust client

A **native, JVM-free** DynamoDB Streams consumer for Rust. Unlike the
Python/Go/Node clients — which talk to a bundled Rust **sidecar** over stdio — a
Rust application depends on the engine crates **directly**, so the consumer runs
entirely **in-process**: no subprocess, no stdio bridge, no binary download. It
is the leanest possible consumer (one process, zero IPC).

Same guarantees as the other clients: per-shard ordered delivery
(parent-before-child across resharding), DynamoDB-lease coordination with
failover, and at-least-once checkpointing.

> **Alpha (0.1.3).** API may change before 1.0.

## Install

```toml
[dependencies]
amazon-dynamodb-streams-consumer = "0.1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

AWS credentials and region are resolved from the standard AWS environment (same
as any AWS SDK); `region` can also be set explicitly on the builder.

## Usage

```rust
use std::sync::Arc;
use amazon_dynamodb_streams_consumer::{Worker, Record, RecordProcessor, RecordProcessorFactory};

struct MyProcessor;
impl RecordProcessor for MyProcessor {
    fn process_records(&mut self, records: &[Record]) {
        for r in records {
            // r.event_name is INSERT / MODIFY / REMOVE.
            // r.keys / r.new_image / r.old_image are maps of the typed AttrValue enum.
            println!("{:?} {:?}", r.event_name, r.keys);
        }
    }
    // Optional: fn initialize(&mut self, shard_id: &str) {}
    // Optional: fn shard_ended(&mut self, shard_id: &str) {}
}

struct MyFactory;
impl RecordProcessorFactory for MyFactory {
    fn create(&self, _shard_id: &str) -> Box<dyn RecordProcessor + Send> {
        Box::new(MyProcessor)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let worker = Worker::builder()
        .stream_arn("arn:aws:dynamodb:us-east-1:123456789012:table/Orders/stream/2026-...")
        .lease_table("my-app-leases")
        .processor(Arc::new(MyFactory))
        .build()
        .await?;

    // Graceful shutdown from elsewhere: let h = worker.stop_handle(); ... h.stop();
    worker.run().await?; // blocks until the stream is fully consumed or stopped
    Ok(())
}
```

`run()` drives coordination cycles until every shard is complete (a
bounded/closing stream) or `stop_handle().stop()` is called; for a long-running
stream it loops indefinitely, delivering and checkpointing. On stop it releases
its leases so another worker can take over immediately. Scale out by running the
same binary on more hosts with a distinct `owner` — leases balance shards across
workers automatically.

## Bounding footprint: `max_processing_concurrency`

By default the worker processes one shard per concurrent slot, so its footprint
grows with the stream's shard count. To keep footprint constant as a
stream accumulates shards, cap the number of shards processed concurrently:

```rust
let worker = Worker::builder()
    .stream_arn(stream_arn)
    .lease_table("my-app-leases")
    .processor(Arc::new(MyFactory))
    .max_processing_concurrency(8) // at most 8 shards processed at once
    .build()
    .await?;
```

The cap bounds concurrent record delivery only — shard reads and lease
heartbeats are unaffected, so idle shards keep their leases. It changes no
delivery semantics: at-least-once, per-item ordering, and per-shard ordering all
hold (a shard is never split; each shard is processed by one slot at a time).
Leave it unset for the prior behavior (one slot per shard).

## Record format: typed by default, native or DynamoDB JSON views

Each `Record` exposes item images **two** ways:

- **Typed (primary):** `r.keys`, `r.new_image`, `r.old_image` are
  `BTreeMap<String, AttrValue>` — the strongly-typed, exhaustively-matchable
  `AttrValue` enum (`S`/`N`/`Bool`/`Null`/`B`/`M`/`L`/`Ss`/`Ns`/`Bs`). This is
  always present.
- **JSON view:** `r.keys_json()`, `r.new_image_json()`, `r.old_image_json()`
  return `serde_json::Value` in the worker's configured `RecordFormat`.

`RecordFormat` is a **worker-level** setting (set once, applies to every record),
mirroring the `record_format` option in the other clients:

```rust
use amazon_dynamodb_streams_consumer::RecordFormat;

let worker = Worker::builder()
    .stream_arn(arn)
    .lease_table("leases")
    .record_format(RecordFormat::DdbJson) // default is RecordFormat::Native
    .processor(Arc::new(MyFactory))
    .build()
    .await?;
```

- **`Native`** (default) — plain values: `S`/`N` → JSON string (numbers stay
  canonical strings, lossless), `Bool`/`Null`/`M`/`L` natural, `B` → base64
  string, sets → arrays. No `{"S": ...}` wrappers.
- **`DdbJson`** — canonical DynamoDB JSON
  (`{"S"|"N"|"BOOL"|"NULL"|"B"|"M"|"L"|"SS"|"NS"|"BS"}`), the shape the AWS SDKs
  and boto3's `TypeDeserializer` consume — for SDK interop or KCL migration.

The free functions `to_native_json(&AttrValue)` and `to_ddb_json(&AttrValue)` are
also exported for converting typed values directly.

## Testing

```bash
cargo test -p amazon-dynamodb-streams-consumer            # unit tests (offline)

# Live integration test against a real account (creates + deletes its own tables):
DDB_STREAMS_CONSUMER_IT=1 AWS_REGION=us-east-1 \
  cargo test -p amazon-dynamodb-streams-consumer --test live_consumer -- --nocapture
```

## License

Apache-2.0.
