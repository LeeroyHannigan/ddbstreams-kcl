# amazon-dynamodb-streams-consumer — Node.js client

A JVM-free DynamoDB Streams consumer for Node.js. It embeds the shared Rust
**sidecar** (which owns shard discovery, leasing, ordering, and checkpointing)
and delivers ordered, checkpointed change records to your processor — a thin
stdio bridge over the same JSON-Lines wire protocol the Python and Go clients
use. Zero runtime dependencies (Node stdlib only).

> **Alpha (0.1.0).** API and wire protocol may change before 1.0.

## Install

```bash
npm install amazon-dynamodb-streams-consumer
```

npm ships JavaScript, not a prebuilt binary, so on the first `worker.run()` the
client **transparently downloads the matching sidecar** from the GitHub Release,
verifies its SHA-256, and caches it under `$XDG_CACHE_HOME` (or `~/.cache`). It's
still install-and-go — the download is a one-time, automatic step. Resolution
order:

1. `config.sidecarPath` (explicit),
2. `DDB_STREAMS_CONSUMER_SIDECAR` env,
3. cached binary from a previous download,
4. auto-download from the release (checksum-verified),
5. the binary on `PATH`.

## Usage

```js
const { Worker } = require('amazon-dynamodb-streams-consumer');

const processor = {
  processRecords(records) {
    for (const r of records) {
      // r.eventName is INSERT / MODIFY / REMOVE.
      // r.keys, r.newImage, r.oldImage are decoded to JS natives.
      console.log(r.eventName, r.keys);
    }
  },
  // Optional:
  shardEnded(shardId) {},
};

const worker = new Worker({
  streamArn: 'arn:aws:dynamodb:us-east-1:123456789012:table/Orders/stream/2026-...',
  leaseTable: 'my-app-leases',
  processor,
  region: 'us-east-1',
});

worker.run().then((code) => console.log('exited', code)); // resolves on shutdown
// worker.stop() from elsewhere for a graceful shutdown.
```

## Attribute decoding

`S`/`N` → `string` (numbers stay canonical strings), `Bool` → `boolean`,
`Null` → `null`, `B` → `Buffer`, `M` → object, `L` → array, `Ss`/`Ns` →
`string[]`, `Bs` → `Buffer[]`.

## Testing

```bash
cd clients/node && node --test
```

Runs native-decoding unit tests plus the shared binding **conformance** suite
(`../../conformance/fixtures/*.json`) against the language-agnostic
`replay_sidecar.py` — no AWS, no real sidecar (needs `python3` on PATH).
