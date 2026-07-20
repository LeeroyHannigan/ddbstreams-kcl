# Amazon.DynamoDBStreams.Consumer — .NET client

A JVM-free DynamoDB Streams consumer for .NET. It embeds the shared Rust
**sidecar** (which owns shard discovery, leasing, ordering, and checkpointing)
and delivers ordered, checkpointed change records to your processor — a thin
stdio bridge over the same JSON-Lines wire protocol the Python, Go, and Node
clients use.

> **Alpha (0.1.3).** API and wire protocol may change before 1.0.

## Install

```bash
dotnet add package Amazon.DynamoDBStreams.Consumer
```

NuGet ships managed assemblies, not a native binary, so on the first
`RunAsync()` the client **transparently downloads the matching sidecar** from
the GitHub Release, verifies its SHA-256, and caches it under
`$XDG_CACHE_HOME` (or `~/.cache`). It's install-and-go — the download is a
one-time, automatic step. Resolution order:

1. `WorkerConfig.SidecarPath` (explicit),
2. `DDB_STREAMS_CONSUMER_SIDECAR` env,
3. cached binary from a previous download,
4. auto-download from the release (checksum-verified),
5. the binary on `PATH`.

AWS credentials and region are resolved by the sidecar from the standard AWS
environment; `Region` can also be set on the config.

## Usage

```csharp
using Amazon.DynamoDBStreams.Consumer;

class MyProcessor : IRecordProcessor
{
    public void ProcessRecords(IReadOnlyList<Record> records)
    {
        foreach (var r in records)
        {
            // r.EventName is INSERT / MODIFY / REMOVE.
            // r.Keys / r.NewImage / r.OldImage are decoded to .NET natives.
            Console.WriteLine($"{r.EventName} {string.Join(",", r.Keys.Keys)}");
        }
    }

    // Optional: void ShardEnded(string shardId) has a default no-op implementation.
}

var worker = new Worker(new WorkerConfig
{
    StreamArn  = "arn:aws:dynamodb:us-east-1:123456789012:table/Orders/stream/2026-...",
    LeaseTable = "my-app-leases",
    Processor  = new MyProcessor(),
    Region     = "us-east-1",
});

int exitCode = await worker.RunAsync(); // completes when the sidecar shuts down
// worker.Stop() from elsewhere for a graceful shutdown.
```

Scale out by running the same app on more hosts with a distinct `Owner` — leases
balance shards across workers automatically.

## Record format: native (default) vs DynamoDB JSON

By default your processor gets **plain .NET values** — no `{"S": ...}` /
`{"N": ...}` type wrappers to unpack. That hand-written `AttributeValue`
unmarshalling is already done for you. Native mapping: `S`/`N` → `string`
(numbers stay canonical strings, lossless), `Bool` → `bool`, `Null` → `null`,
`B` → `byte[]`, `Ss`/`Ns` → `List<string>`, `Bs` → `List<byte[]>`, `M` →
`IReadOnlyDictionary<string, object?>`, `L` → `List<object?>`.

To instead receive **canonical DynamoDB JSON** (the typed
`{"S"|"N"|"BOOL"|"NULL"|"B"|"M"|"L"|"SS"|"NS"|"BS"}` shape the AWS SDK consumes —
handy for KCL migration or writing items straight back with the SDK), set
`RecordFormat.DdbJson`. One top-level switch, applied to every record:

```csharp
new WorkerConfig
{
    // ...
    RecordFormat = RecordFormat.DdbJson, // default is RecordFormat.Native
}
// native:   r.NewImage["n"] == "42"                       (string)
// ddb_json: r.NewImage["n"] == { ["N"] = "42" }           (dictionary)
```

Numbers stay strings in both modes to avoid precision loss.

### SDK-native drop-in (`RecordFormat.Sdk`)

For the smoothest migration off KCL, set `RecordFormat.Sdk`: each image value is
an AWS SDK for .NET `Amazon.DynamoDBv2.Model.AttributeValue`, so a record drops
straight into the SDK — no hand-conversion:

```csharp
using Amazon.DynamoDBv2.Model;

new WorkerConfig { /* ... */ RecordFormat = RecordFormat.Sdk };

// In the processor: write the changed item straight back with the SDK.
Dictionary<string, AttributeValue> item = SdkAttributeValues.ToItem(r.NewImage);
await ddb.PutItemAsync("Orders", item);
```

This pulls a dependency on `AWSSDK.DynamoDBv2` (a direct dependency of this
client). `Native`/`DdbJson` users don't need to reference it themselves.

## Start position

For a shard with no stored checkpoint, `InitialPosition` controls where reading
begins:

```csharp
new WorkerConfig
{
    // ...
    InitialPosition = InitialPosition.Latest, // default is InitialPosition.TrimHorizon
}
```

- `TRIM_HORIZON` (default) — start from the oldest available record in the shard.
- `LATEST` — start from records written after the worker begins.

Shards that already have a checkpoint always
resume from it, regardless of this setting.

## Testing

```bash
dotnet test    # unit tests + shared conformance suite (offline; needs python3 for the replay sidecar)

# Live smoke against a real account (provision a stream, then):
DDB_STREAMS_CONSUMER_IT=1 AWS_REGION=us-east-1 \
  DDB_STREAMS_CONSUMER_STREAM_ARN=<arn> DDB_STREAMS_CONSUMER_LEASE_TABLE=<table> \
  DDB_STREAMS_CONSUMER_SIDECAR=<path-to-sidecar> \
  dotnet test --filter FullyQualifiedName~LiveSmokeTests
```

The conformance suite runs the shared `../../conformance/fixtures/*.json` against
the language-agnostic `replay_sidecar.py` — no AWS, no real sidecar.

## License

Apache-2.0.

## Bounding footprint

`WorkerConfig.MaxProcessingConcurrency` (optional `int?`) caps the number of
shards processed concurrently, keeping footprint O(max) as the table's
shard count grows. Unset = one processing slot per shard. Bounds
concurrent record delivery only; at-least-once, per-item, and per-shard ordering
are preserved.
