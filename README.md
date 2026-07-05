# Amazon DynamoDB Streams Consumer

A client library for consuming [Amazon DynamoDB Streams](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html)
at scale from any language, without a JVM.

The AWS SDKs provide a low-level DynamoDB Streams client (`DynamoDbStreamsClient`) that exposes the raw
`DescribeStream`, `GetShardIterator`, and `GetRecords` operations. Building a production consumer on top of
that requires handling shard discovery and lineage, coordinating multiple workers, preserving per-shard
order, checkpointing progress, and rebalancing on failure. The established solution — the Amazon Kinesis
Client Library (KCL) with the DynamoDB Streams Kinesis Adapter — requires the Java Virtual Machine.

This library implements the same lease-based coordination and checkpointing model natively for DynamoDB
Streams in Rust, and makes it available to other languages through a lightweight client. Applications
implement a record processor; the library handles the rest.

[![Version](https://img.shields.io/badge/version-0.1.3-blue.svg)](CHANGELOG.md)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![codecov](https://codecov.io/gh/LeeroyHannigan/amazon-dynamodb-streams-consumer/branch/main/graph/badge.svg)](https://codecov.io/gh/LeeroyHannigan/amazon-dynamodb-streams-consumer)
[![Status](https://img.shields.io/badge/status-alpha-orange.svg)](#status)

## Status

**0.1.3 — alpha.** The API and wire protocol may change before 1.0, and the
project is not yet recommended for production use. Feedback and issues are welcome.

## Features

- Per-shard ordered delivery, including correct parent-before-child ordering across shard splits and merges.
- Multi-worker coordination through a DynamoDB lease table, with automatic shard balancing and failover.
- At-least-once processing with checkpointing; a worker resumes from the last checkpoint after failure.
- Horizontal scaling by running additional worker processes — no configuration changes required.
- Native record decoding by default — your processor receives plain language values, not DynamoDB-JSON `AttributeValue` wrappers; opt into canonical DynamoDB JSON per worker when you need SDK interop or KCL-migration parity.
- Optional metrics (consumer lag / `MillisBehindLatest`, throughput, shard lifecycle) exported over OpenTelemetry (OTLP), including directly to Amazon CloudWatch's native OTLP endpoint with no collector.
- Language clients that are thin and dependency-free; the Rust core and sidecar contain all coordination logic.

## Architecture

The coordination logic runs in a Rust process, the *sidecar*. A language client spawns the sidecar and
communicates with it over stdin/stdout using a newline-delimited JSON protocol: the sidecar sends ordered
record batches, and the client replies with checkpoint acknowledgements.

```
  Application (Python/Go/Node)          Sidecar (Rust)                     AWS
  ------------------------              --------------                     ---
  record processor    <-- records --   shard discovery & lineage
                      -- checkpoints -> DynamoDB leases                 DynamoDB Streams
                                        per-shard ordering       <-->   Lease table
                                        checkpointing
```

Because all shard, lease, and checkpoint handling lives in the sidecar, adding a language is a small client
that speaks the protocol — there is no JVM and no per-language reimplementation of the hard parts.

## Clients

Five clients ship today. The Python, Go, Node, and .NET clients speak the same
sidecar protocol and are verified against a shared [conformance suite](conformance/README.md);
the Rust client runs **in-process** (no sidecar), depending on the engine crates
directly.

| Language | Package | Reference |
|---|---|---|
| Python | `amazon-dynamodb-streams-consumer` | [`clients/python`](clients/python/README.md) |
| Go | `github.com/LeeroyHannigan/amazon-dynamodb-streams-consumer/clients/go` | [`clients/go`](clients/go/README.md) |
| Node.js | `amazon-dynamodb-streams-consumer` (npm) | [`clients/node`](clients/node/README.md) |
| Rust | `amazon-dynamodb-streams-consumer` (crates.io) | [`clients/rust`](clients/rust/README.md) |
| .NET | `Amazon.DynamoDBStreams.Consumer` (NuGet) | [`clients/dotnet`](clients/dotnet/README.md) |

Sidecar delivery differs by ecosystem: the **Python** wheel bundles the sidecar
binary, while the **Go**, **Node**, and **.NET** clients download the matching
sidecar on first use (checksum-verified, cached). The **Rust** client needs no
sidecar at all — it links the engine directly and runs in-process. Publishing
status is alpha — Python is live on
[TestPyPI](https://test.pypi.org/project/amazon-dynamodb-streams-consumer/);
Go, Node, Rust, and .NET packaging is in place but not yet published to their
registries (see [Status](#status)). A worked example in each language is in its
client README; the Python example below is representative.

## Getting started (Python)

> **Alpha — published to [TestPyPI](https://test.pypi.org/project/amazon-dynamodb-streams-consumer/), not yet on PyPI.**
> Prebuilt wheels (with the native sidecar bundled) are available for Linux
> (x86_64, aarch64), macOS (arm64, x86_64), and Windows x86_64.

Install from TestPyPI:

```
pip install -i https://test.pypi.org/simple/ amazon-dynamodb-streams-consumer
```

> The package has no runtime dependencies, so no extra index is required. If a
> wheel is not available for your platform, install fails (there is no source
> fallback — the sidecar is a prebuilt binary). You can also build a wheel
> locally (see [Building and testing](#building-and-testing)). Release to the
> real PyPI is planned once the project reaches its authoritative home.

```python
from dynamodb_streams_consumer import Worker

class OrderProcessor:
    def process_records(self, records):
        for record in records:
            # record.event_name is INSERT, MODIFY, or REMOVE.
            # record.keys, record.new_image, and record.old_image are decoded
            # DynamoDB items (native Python values).
            print(record.event_name, record.keys, record.new_image)

    def shard_ended(self, shard_id):   # optional
        pass

Worker(
    stream_arn="arn:aws:dynamodb:us-east-1:123456789012:table/Orders/stream/2026-...",
    lease_table="orders-consumer-leases",
    processor=OrderProcessor(),
    region="us-east-1",
).run()
```

`run()` blocks until the stream is fully consumed, `stop()` is called from another thread, or the process is
interrupted. Records are delivered in per-shard order, and each batch is checkpointed only after
`process_records` returns.

To scale out, run the same program on additional hosts. Workers coordinate through the `lease_table`:
shards are distributed among them, and if a worker stops, its leases are released (or expire) and another
worker resumes from the last checkpoint.

The Python client requires the `amazon-dynamodb-streams-consumer-sidecar` binary. It is located via the
`sidecar_path` argument, the `DDB_STREAMS_CONSUMER_SIDECAR` environment variable, or `PATH`. AWS credentials
and region are read from the standard AWS environment.

See [`clients/python/README.md`](clients/python/README.md) for the full client reference.

## Concepts

**Ordering.** DynamoDB Streams guarantees order only within a shard. The library preserves this by ensuring
exactly one worker owns a shard at a time (enforced by an optimistic lock on the lease's counter) and by
processing a shard's records sequentially. Across a resharding event, a child shard is not started until its
parent shards are fully consumed and marked complete, which preserves the order of a key's changes across the
split or merge.

**Leasing and balancing.** Each shard has a lease row in a DynamoDB table. Workers acquire, renew, and steal
leases using conditional writes, targeting an even share of shards. Expired leases (from a stopped worker)
are taken over automatically.

**Checkpointing.** After a batch is processed and acknowledged, the last sequence number is stored on the
lease. A worker that takes over a shard resumes immediately after that sequence number.

**Record format.** By default the processor receives item images (`keys`, `new_image`, `old_image`) as
**native language values** — plain dicts/maps, strings, booleans, etc. — so you never hand-unmarshal
DynamoDB-JSON `AttributeValue` wrappers (a common KCL pain point). Set the worker-level option
`record_format` (`RecordFormat` in Go, `recordFormat` in Node) to `ddb_json` to instead receive **canonical
DynamoDB JSON** (`{"S"|"N"|"BOOL"|"NULL"|"B"|"M"|"L"|"SS"|"NS"|"BS"}`) — the shape the AWS SDKs and boto3's
`TypeDeserializer` consume — for SDK interop or migrating from KCL. It is one switch set once for the whole
processor; numbers stay strings in both modes to avoid precision loss. See each client's README for the exact
syntax.

## Metrics

Metrics are optional and off by default — the engine emits through a pluggable sink, and with none attached
there is zero overhead. The published sidecar (the binary bundled in the Python wheel and the one the Go/Node
clients auto-download) includes OpenTelemetry (OTLP/HTTP) export, so **enabling metrics is configuration
only — no rebuild**. Set the standard `OTEL_*` environment variables where your consumer runs:

```
OTEL_METRICS_EXPORTER=otlp
OTEL_EXPORTER_OTLP_ENDPOINT=https://collector.example:4318
OTEL_SERVICE_NAME=orders-consumer
```

The same exporter targets any OTLP backend (an OpenTelemetry Collector, Prometheus, a vendor endpoint). When
the endpoint is Amazon CloudWatch's **native OTLP metrics endpoint**
(`https://monitoring.<region>.amazonaws.com/v1/metrics`), requests are AWS SigV4-signed automatically using
the ambient credentials (IAM permission `cloudwatch:PutMetricData`), so metrics land in CloudWatch with no
collector to run.

Emitted metrics (dimensioned by `shard_id` where applicable):

| Metric | Type | Meaning |
|---|---|---|
| `ddbstreams.consumer.millis_behind_latest` | gauge (ms) | Consumer lag: now − the newest record's approximate creation time |
| `ddbstreams.consumer.records_processed` | counter | Records delivered to the processor |
| `ddbstreams.consumer.bytes_processed` | counter | Payload bytes delivered |
| `ddbstreams.consumer.describe_stream.count` | counter | `DescribeStream` calls issued during shard sync |
| `ddbstreams.consumer.shard_end.count` | counter | Shards that reached `SHARD_END` |

If you build the sidecar from source, enable the exporter with the `otel` Cargo feature (see
[Building and testing](#building-and-testing)); the prebuilt/published binaries already include it.

## Project layout

| Crate / package | Description |
|---|---|
| `amazon-dynamodb-streams-consumer-core` | Coordination and ordering engine and the typed record model. No AWS dependencies. |
| `amazon-dynamodb-streams-consumer-source` | Shard-graph construction and the async DynamoDB Streams reader. |
| `amazon-dynamodb-streams-consumer-lease` | DynamoDB-backed lease store (acquire, renew, checkpoint, steal, release). |
| `amazon-dynamodb-streams-consumer-worker` | The worker runtime that composes the above and drives per-shard processing. |
| `amazon-dynamodb-streams-consumer-protocol` | The client/sidecar wire protocol. |
| `amazon-dynamodb-streams-consumer-sidecar` | The consumer process a language client runs. |
| `clients/python` | The Python client (`dynamodb_streams_consumer`). |
| `clients/go` | The Go client (`ddbstreams`). |
| `clients/node` | The Node.js client (`amazon-dynamodb-streams-consumer`). |
| `conformance` | Language-agnostic protocol fixtures + replay sidecar every client is tested against. |

## Building and testing

```
# Build, unit test, and lint (no AWS access required)
cargo test --workspace
cargo clippy --workspace --all-targets --features aws

# Build the sidecar with OpenTelemetry (OTLP) metrics export
cargo build -p amazon-dynamodb-streams-consumer-sidecar --features otel

# Integration tests against a live account
DDB_STREAMS_CONSUMER_IT=1 AWS_REGION=us-east-1 cargo test --workspace --features aws

# Python client tests
cd clients/python && python3 -m unittest discover -s tests

# Go client tests (unit + shared conformance; needs python3 for the replay sidecar)
cd clients/go && go test ./...

# Node client tests (unit + shared conformance; needs python3 for the replay sidecar)
cd clients/node && npm install && npm test

# Build a Python wheel with the sidecar bundled (installs a working consumer)
bash clients/python/build_wheel.sh
pip install clients/python/dist/*.whl
```

## License

This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

It reimplements the coordination and checkpointing algorithms of the Apache-2.0
[Amazon Kinesis Client Library](https://github.com/awslabs/amazon-kinesis-client) and
[DynamoDB Streams Kinesis Adapter](https://github.com/awslabs/dynamodb-streams-kinesis-adapter) natively for
DynamoDB Streams. See [`core/REFERENCES.md`](core/REFERENCES.md) for the behavior-to-source mapping.
