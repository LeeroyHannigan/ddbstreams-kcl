# ddbstreams-kcl (Python client)

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

## The sidecar binary

The library needs the `ddbstreams-kcl-sidecar` binary. Resolution order:
1. `sidecar_path=...` argument
2. `DDBSTREAMS_KCL_SIDECAR` environment variable
3. `ddbstreams-kcl-sidecar` on `PATH`

AWS credentials and region are picked up by the sidecar from the standard AWS
environment (same as any AWS SDK).

## Config knobs

`owner`, `region`, `max_leases`, `lease_duration_ms`, `poll_interval_ms`,
`cycle_interval_ms` — all optional keyword args on `Worker(...)`.

## Development

```bash
python3 -m unittest discover -s tests          # hermetic tests (fake sidecar)
# live smoke against a real stream + the built binary:
DDBSTREAMS_KCL_SIDECAR=../../target/debug/ddbstreams-kcl-sidecar \
  python3 examples/live_smoke.py <stream_arn> <lease_table> <region>
```

## License

Apache-2.0.
