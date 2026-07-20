# amazon-dynamodb-streams-consumer (Python client)

A **JVM-free** DynamoDB Streams KCL consumer for Python. Your code stays pure
Python; all the hard parts — shard discovery, DynamoDB leases, ordering,
lease balancing, checkpointing — run in a bundled Rust **sidecar** process that
this library talks to over stdio. It's the KCL MultiLangDaemon model, without a
JVM.

## Usage

```python
from ddbstreams_kcl import Worker

class MyProcessor:
    def process_records(self, records):
        for r in records:
            # r.keys / r.new_image / r.old_image are decoded DynamoDB items
            # r.event_name is INSERT / MODIFY / REMOVE
            print(r.event_name, r.keys, r.new_image)

    # optional
    def shard_ended(self, shard_id):
        print("shard done:", shard_id)

Worker(
    stream_arn="arn:aws:dynamodb:us-east-1:123456789012:table/Orders/stream/2026-...",
    lease_table="my-app-leases",
    processor=MyProcessor(),
    region="us-east-1",
).run()
```

That's it. `run()` blocks until the stream is fully consumed, you call `stop()`
from another thread, or you Ctrl-C. Records are delivered **in per-shard order**,
and each batch is checkpointed only after `process_records` returns
(at-least-once). Scale out by running the same code on more hosts — the leases in
DynamoDB balance shards across workers automatically.

## Record shape

`Record` fields: `shard_id`, `sequence_number`, `event_name`,
`stream_view_type`, `keys`, `new_image`, `old_image`. Item images are decoded to
native Python (`S`→str, `N`→str to stay lossless, `Bool`→bool, `Null`→None,
`B`→bytes, `M`→dict, `L`→list, sets→list).

## Record format: native (default) vs DynamoDB JSON

By default your processor gets **plain Python values** — no `{"S": ...}` /
`{"N": ...}` type wrappers to unpack. This is the point of the library: the
DynamoDB-JSON unmarshalling that KCL/`AttributeValue` users write by hand is
already done for you.

If you'd rather receive **canonical DynamoDB JSON** (the typed
`{"S"|"N"|"BOOL"|"NULL"|"B"|"M"|"L"|"SS"|"NS"|"BS"}` shape the AWS SDKs and
`boto3`'s `TypeDeserializer` consume — useful when migrating from KCL or writing
items straight back with the SDK), set `record_format="ddb_json"` on the
`Worker`. It's a single top-level switch that applies to every record.

```python
# default — native Python values
Worker(..., record_format="native")   # r.new_image == {"id": "42", "active": True}

# opt in — canonical DynamoDB JSON
Worker(..., record_format="ddb_json")  # r.new_image == {"id": {"N": "42"}, "active": {"BOOL": True}}
```

Numbers stay strings in both modes to avoid float precision loss. This is a
client-side presentation choice — the wire protocol is unchanged.

## The sidecar binary

The library needs the `amazon-dynamodb-streams-consumer-sidecar` binary. **When
installed from a released wheel, the binary is bundled inside the package** and
used automatically — no setup required. Resolution order:
1. `sidecar_path=...` argument
2. `DDB_STREAMS_CONSUMER_SIDECAR` environment variable
3. the binary **bundled in the installed wheel** (`dynamodb_streams_consumer/_bin/`)
4. `amazon-dynamodb-streams-consumer-sidecar` on `PATH`

AWS credentials and region are picked up by the sidecar from the standard AWS
environment (same as any AWS SDK).

## Config knobs

`owner`, `region`, `max_leases`, `lease_duration_ms`, `poll_interval_ms`,
`cycle_interval_ms` — all optional keyword args on `Worker(...)`.

`max_processing_concurrency` — optional keyword arg on `Worker(...)`. Caps the
number of shards processed concurrently, so footprint stays O(max) as the
stream's shard count grows. Unset = one processing slot per shard
(prior behavior). Bounds concurrent record delivery only; at-least-once,
per-item, and per-shard ordering are preserved.

`initial_position` — optional keyword arg on `Worker(...)` controlling where a
freshly-seeded shard begins reading. Values are `TRIM_HORIZON` (the default —
start at the oldest available record) and `LATEST` (start at the newest). Input
is case-insensitive.

## Development

```bash
python3 -m unittest discover -s tests          # hermetic tests (fake sidecar)
# live smoke against a real stream + the built binary:
DDB_STREAMS_CONSUMER_SIDECAR=../../target/debug/amazon-dynamodb-streams-consumer-sidecar \
  python3 examples/live_smoke.py <stream_arn> <lease_table> <region>
```

## License

Apache-2.0.
