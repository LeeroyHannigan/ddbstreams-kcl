# amazon-dynamodb-streams-consumer — Java client

A DynamoDB Streams consumer for Java where **the coordination logic is not on the
JVM**. It embeds the shared Rust **sidecar** (which owns shard discovery,
leasing, ordering, and checkpointing) and delivers ordered, checkpointed change
records to your processor — a thin stdio bridge over the same JSON-Lines wire
protocol the Python, Go, Node, and .NET clients use.

> A Java *client* runs on your own JVM — that is expected. What is different from
> KCL is that the consumer **coordination** does not require a JVM: it runs in the
> native sidecar, so there is no MultiLangDaemon and no JVM-hosted lease/shard
> machinery.

> **Alpha (0.1.3).** API and wire protocol may change before 1.0.

## Install

```xml
<dependency>
  <groupId>com.amazon.dynamodbstreams</groupId>
  <artifactId>amazon-dynamodb-streams-consumer</artifactId>
  <version>0.1.3</version>
</dependency>
```

Maven ships a jar, not a native binary, so on the first `run()` the client
**transparently downloads the matching sidecar** from the GitHub Release,
verifies its SHA-256, and caches it under `$XDG_CACHE_HOME` (or `~/.cache`). It's
add-dependency-and-go — the download is a one-time, automatic step. Resolution
order:

1. `WorkerConfig.Builder.sidecarPath(...)` (explicit),
2. `DDB_STREAMS_CONSUMER_SIDECAR` env,
3. cached binary from a previous download,
4. auto-download from the release (checksum-verified),
5. the binary on `PATH`.

AWS credentials and region are resolved by the sidecar from the standard AWS
environment; `region(...)` can also be set on the builder.

## Usage

```java
import com.amazon.dynamodbstreams.consumer.*;
import java.util.List;

class MyProcessor implements RecordProcessor {
    public void processRecords(List<Record> records) {
        for (Record r : records) {
            // r.eventName() is INSERT / MODIFY / REMOVE.
            // r.keys() / r.newImage() / r.oldImage() are decoded to Java natives.
            System.out.println(r.eventName() + " " + r.keys());
        }
    }
    // Optional: default void shardEnded(String shardId) is a no-op.
}

Worker worker = new Worker(WorkerConfig.builder()
        .streamArn("arn:aws:dynamodb:us-east-1:123456789012:table/Orders/stream/2026-...")
        .leaseTable("my-app-leases")
        .processor(new MyProcessor())
        .region("us-east-1")
        .build());

int exitCode = worker.run(); // blocks until the sidecar shuts down
// worker.stop() from another thread for a graceful shutdown.
```

Scale out by running the same app on more hosts with a distinct `owner(...)` —
leases balance shards across workers automatically.

## Record format: native (default) vs DynamoDB JSON

By default your processor gets **plain Java values** — no `{"S": ...}` /
`{"N": ...}` type wrappers to unpack. That hand-written `AttributeValue`
unmarshalling is already done for you. Native mapping: `S`/`N` → `String`
(numbers stay canonical strings, lossless), `Bool` → `Boolean`, `Null` →
`null`, `B` → `byte[]`, `Ss`/`Ns` → `List<String>`, `Bs` → `List<byte[]>`,
`M` → `Map<String, Object>`, `L` → `List<Object>`.

To instead receive **canonical DynamoDB JSON** (the typed
`{"S"|"N"|"BOOL"|"NULL"|"B"|"M"|"L"|"SS"|"NS"|"BS"}` shape the AWS SDK consumes —
handy for KCL migration or writing items straight back with the SDK), set
`RecordFormat.DDB_JSON`. One top-level switch, applied to every record:

```java
WorkerConfig.builder()
    // ...
    .recordFormat(RecordFormat.DDB_JSON) // default is RecordFormat.NATIVE
    .build();
// native:   r.newImage().get("n") == "42"                 (String)
// ddb_json: r.newImage().get("n") == {"N": "42"}          (Map)
```

Numbers stay strings in both modes to avoid precision loss.

### SDK-native drop-in (`RecordFormat.SDK`)

For the smoothest migration off KCL/KCA, set `RecordFormat.SDK`: each image
value is an AWS SDK for Java **v2** `AttributeValue`
(`software.amazon.awssdk.services.dynamodb.model.AttributeValue`), so a record
drops straight into the SDK — no hand-conversion:

```java
import com.amazon.dynamodbstreams.consumer.SdkAttributeValues;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

WorkerConfig.builder().recordFormat(RecordFormat.SDK) /* ... */ .build();

// In the processor: write the changed item straight back with the SDK.
Map<String, AttributeValue> item = SdkAttributeValues.toItem(r.newImage());
ddb.putItem(b -> b.tableName("Orders").item(item));
```

This requires `software.amazon.awssdk:dynamodb` on the classpath. It is a
**`provided`** dependency of this client, so `native`/`ddb_json` users are not
forced to pull the AWS SDK; apps using `SDK` already have it.

## Start position

When a shard has no stored checkpoint yet, `initialPosition(...)` controls where
consumption begins:

- `TRIM_HORIZON` (default) — start at the oldest available record in the shard.
- `LATEST` — start at the newest records, skipping existing backlog.

Once a checkpoint
exists for a shard, resume always continues from the checkpoint regardless of
this setting.

```java
WorkerConfig.builder()
    // ...
    .initialPosition(InitialPosition.LATEST) // default is TRIM_HORIZON
    .build();
```

## Testing

```bash
mvn test    # unit tests + shared conformance suite (offline; needs python3 for the replay sidecar)

# Live smoke against a real account (provision a stream, then):
DDB_STREAMS_CONSUMER_IT=1 AWS_REGION=us-east-1 \
  DDB_STREAMS_CONSUMER_STREAM_ARN=<arn> DDB_STREAMS_CONSUMER_LEASE_TABLE=<table> \
  DDB_STREAMS_CONSUMER_SIDECAR=<path-to-sidecar> \
  mvn test -Dtest=LiveSmokeTest
```

The conformance suite runs the shared `../../conformance/fixtures/*.json` against
the language-agnostic `replay_sidecar.py` — no AWS, no real sidecar.

## License

Apache-2.0.

## Bounding footprint

`WorkerConfig.Builder.maxProcessingConcurrency(int)` (optional) caps the number
of shards processed concurrently, keeping footprint O(max) as the table's
shard count grows. Unset = one processing slot per shard. Bounds
concurrent record delivery only; at-least-once, per-item, and per-shard ordering
are preserved.
