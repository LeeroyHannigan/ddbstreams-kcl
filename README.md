<h1 align="center">amazon-dynamodb-streams-consumer</h1>

<p align="center">
  <strong>A high-level, multi-language, JVM-free consumer for Amazon DynamoDB Streams.</strong><br>
  Shard discovery, leasing, ordering, checkpointing, and worker load-balancing — handled for you.
</p>

<p align="center">
  <img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-blue.svg">
  <img alt="Rust" src="https://img.shields.io/badge/core-Rust-orange.svg">
  <img alt="Python" src="https://img.shields.io/badge/client-Python%203.8%2B-3776AB.svg?logo=python&logoColor=white">
  <img alt="Tests" src="https://img.shields.io/badge/tests-72%20unit%20%2B%206%20live-brightgreen.svg">
  <img alt="Coverage" src="https://img.shields.io/badge/coverage-~94%25-brightgreen.svg">
  <img alt="Status" src="https://img.shields.io/badge/status-alpha-yellow.svg">
</p>

---

## Why

The AWS SDK gives you a **low-level** DynamoDB Streams client (`DynamoDbStreamsClient`) — raw
`DescribeStream` / `GetShardIterator` / `GetRecords`. Turning that into a correct, scalable consumer
means solving the hard parts yourself: walking the shard lineage, coordinating workers with a lease
table, preserving per-shard order, checkpointing, and rebalancing on failure.

Today the only battle-tested answer is the **Kinesis Client Library (KCL)** via the DynamoDB Streams
Kinesis Adapter — which drags in a **JVM**. There is no first-class story for Python, Go, Node, or Rust.

**`amazon-dynamodb-streams-consumer` is that story.** It reimplements the KCL lease-and-checkpoint model
**natively for DynamoDB Streams, in Rust**, and exposes it to any language through a thin client — no JVM,
no sidecar language lock-in. You write a record handler; it does the rest.

## Highlights

- 🧩 **Just write a processor** — receive ordered, decoded change records; the library owns shards, leases, and checkpoints.
- 🔒 **Correct by construction** — parent-before-child ordering across resharding, optimistic-lock leases, exactly-the-KCL-model checkpointing.
- ⚖️ **Scales horizontally** — run N copies; leases balance shards across workers automatically, with fast failover on shutdown.
- 🌍 **Multi-language, zero JVM** — a Rust core + sidecar do the work; language clients are thin stdio bridges (Python today; Go/Node next).
- ✅ **Proven** — live-verified end-to-end against real DynamoDB Streams; ~94% line coverage.
- 🪶 **Lightweight** — the Python client has **zero runtime dependencies**.

## How it works

```
┌──────────────────────────┐        stdio (JSON-Lines)        ┌───────────────────────────────┐
│  Your app (Python/Go/…)  │  ── records ──▶                  │   Rust sidecar                │
│                          │                                  │   • shard discovery + lineage │
│  class MyProcessor:      │  ◀── checkpoint acks ──          │   • DynamoDB leases (steal/    │
│    def process_records() │                                  │     expire/renew)             │
└──────────────────────────┘                                  │   • per-shard ordering        │
                                                              │   • checkpointing             │
                                                              └───────────────┬───────────────┘
                                                                              │ aws-sdk
                                                              ┌───────────────▼───────────────┐
                                                              │  DynamoDB Streams + lease table │
                                                              └────────────────────────────────┘
```

The sidecar streams ordered record batches to your process; your handler runs; the client acks, and
**only then** does the sidecar advance the checkpoint (**at-least-once**, exactly like KCL). Scale out by
running the same code on more hosts — the lease table balances shards across them, and on shutdown a
worker releases its leases so another takes over immediately.

## Quick start (Python)

```bash
pip install amazon-dynamodb-streams-consumer
```

```python
from dynamodb_streams_consumer import Worker

class OrderProcessor:
    def process_records(self, records):
        for r in records:
            # r.event_name is INSERT / MODIFY / REMOVE
            # r.keys / r.new_image / r.old_image are decoded DynamoDB items
            print(r.event_name, r.keys, "->", r.new_image)

    # optional
    def shard_ended(self, shard_id):
        print("finished shard", shard_id)

Worker(
    stream_arn="arn:aws:dynamodb:us-east-1:123456789012:table/Orders/stream/2026-...",
    lease_table="orders-consumer-leases",
    processor=OrderProcessor(),
    region="us-east-1",
).run()
```

That's the whole API. `run()` blocks until the stream is fully consumed, you call `stop()` from another
thread, or you Ctrl-C. Records arrive **in per-shard order**; each batch is checkpointed only after your
`process_records` returns.

### Scaling out

Run the same script on more hosts/containers. They coordinate through the `lease_table` in DynamoDB —
shards are distributed across workers, and if one dies its leases expire (or are released on graceful
shutdown) and another picks them up, resuming from the last checkpoint.

### Record shape

`Record` fields: `shard_id`, `sequence_number`, `event_name`, `stream_view_type`, `keys`, `new_image`,
`old_image`. Item images decode to native Python — `S`→`str`, `N`→`str` (lossless), `Bool`→`bool`,
`Null`→`None`, `B`→`bytes`, `M`→`dict`, `L`→`list`, sets→`list`.

## Components

| Component | What it is |
|---|---|
| `amazon-dynamodb-streams-consumer-core` | Pure ordering/lease/checkpoint engine + typed record model (no AWS, no network). |
| `…-source` | DynamoDB Streams shard-graph logic + async reader over `aws-sdk-dynamodbstreams`. |
| `…-lease` | Optimistic-lock lease store on DynamoDB (acquire/renew/checkpoint/steal/release). |
| `…-worker` | The `Fleet` runtime: per-shard concurrent tasks, coordination, shard-sync, checkpoint resume. |
| `…-protocol` | JSON-Lines wire protocol shared by the sidecar and language clients. |
| `…-sidecar` | The consumer binary a language client spawns and talks to over stdio. |
| `clients/python` | The reference Python client (`dynamodb_streams_consumer`). |

## Configuration

The sidecar reads standard AWS credentials/region from the environment. Tunables (all optional, set as
`Worker(...)` kwargs or `DDB_STREAMS_CONSUMER_*` env vars): `owner`, `region`, `max_leases`,
`lease_duration_ms`, `poll_interval_ms`, `cycle_interval_ms`.

## Development

```bash
# Rust: build, test, lint (offline)
cargo test --workspace
cargo clippy --workspace --all-targets --features aws

# Rust: live integration tests (needs AWS creds)
DDB_STREAMS_CONSUMER_IT=1 AWS_REGION=us-east-1 cargo test --workspace --features aws

# Python client
cd clients/python && python3 -m unittest discover -s tests
```

## Heritage & license

Licensed under **Apache-2.0**. This project reimplements the algorithms of the Apache-2.0
[Kinesis Client Library](https://github.com/awslabs/amazon-kinesis-client) and the
[DynamoDB Streams Kinesis Adapter](https://github.com/awslabs/dynamodb-streams-kinesis-adapter) —
lease coordination, parent-before-child shard ordering, and checkpointing — natively for DynamoDB Streams
and without a JVM. See [`core/REFERENCES.md`](core/REFERENCES.md) for the behavior-by-behavior source mapping.
