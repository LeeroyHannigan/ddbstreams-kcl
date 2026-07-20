# amazon-dynamodb-streams-consumer — Node.js client

A JVM-free DynamoDB Streams consumer for Node.js. It embeds the shared Rust
**sidecar** (which owns shard discovery, leasing, ordering, and checkpointing)
and delivers ordered, checkpointed change records to your processor — a thin
stdio bridge over the same JSON-Lines wire protocol the Python and Go clients
use. Zero runtime dependencies (Node stdlib only).

> **Alpha (0.1.3).** API and wire protocol may change before 1.0.

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

## Record format: native (default) vs DynamoDB JSON

By default your processor gets **plain JS values** — no `{"S": ...}` /
`{"N": ...}` type wrappers to unpack. That hand-written `AttributeValue`
unmarshalling is already done for you.

To instead receive **canonical DynamoDB JSON** (the typed
`{"S"|"N"|"BOOL"|"NULL"|"B"|"M"|"L"|"SS"|"NS"|"BS"}` shape the AWS SDKs consume —
handy for KCL migration or writing items straight back with the SDK), set
`recordFormat: 'ddb_json'` on the `Worker`. One top-level switch, applied to
every record.

```js
new Worker({
  // ...
  recordFormat: 'ddb_json', // default is 'native'
});
// native:   r.newImage === { id: '42', active: true }
// ddb_json: r.newImage === { id: { N: '42' }, active: { BOOL: true } }
```

Numbers stay strings in both modes to avoid precision loss. This is a
client-side presentation choice — the wire protocol is unchanged.

## Start position

For a shard with no stored checkpoint, `initialPosition` controls where reading
begins:

- `'TRIM_HORIZON'` (default) — start at the oldest available record in the shard.
- `'LATEST'` — start at the most recent records, skipping existing backlog.

The value is normalized to uppercase before being passed to the sidecar. Once a
shard has a checkpoint, the consumer always resumes from it and this setting no
longer applies.

```js
new Worker({
  // ...
  initialPosition: 'LATEST', // default is 'TRIM_HORIZON'
});
```

## TypeScript

The client is written in TypeScript; the published package ships compiled
JavaScript plus generated type declarations (`dist/index.d.ts`), so both JS and
TS consumers get full types with no `@types` package.

```ts
import { Worker, Record } from 'amazon-dynamodb-streams-consumer';
```

## Testing

```bash
cd clients/node
npm install
npm test   # tsc build + node --test (unit + shared conformance)
```

The conformance suite runs the shared `../../conformance/fixtures/*.json`
against the language-agnostic `replay_sidecar.py` — no AWS, no real sidecar
(needs `python3` on PATH).

## Bounding footprint

`maxProcessingConcurrency` (optional config field) caps the number of shards
processed concurrently, keeping footprint O(max) as the stream's shard
count grows. Unset = one processing slot per shard. Bounds concurrent record
delivery only; at-least-once, per-item, and per-shard ordering are preserved.
