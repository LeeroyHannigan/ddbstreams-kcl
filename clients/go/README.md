# amazon-dynamodb-streams-consumer — Go client

A JVM-free DynamoDB Streams consumer for Go. It embeds the shared Rust
**sidecar** (which owns shard discovery, leasing, ordering, and checkpointing)
and delivers ordered, checkpointed change records to your processor. This
package is a thin stdio bridge over the JSON-Lines wire protocol — the same
protocol the Python client uses.

> **Alpha (0.1.3).** API and wire protocol may change before 1.0.

## Install

```bash
go get github.com/LeeroyHannigan/amazon-dynamodb-streams-consumer/clients/go
```

Unlike a Python wheel, a Go module ships source, not a prebuilt binary. So on
first `Worker.Run()` the client **transparently downloads the matching sidecar**
from the GitHub Release, verifies its SHA-256, and caches it under
`os.UserCacheDir()`. From your side it is still import-and-go — the download is a
one-time, automatic step. Resolution order:

1. `Config.SidecarPath` (explicit),
2. `DDB_STREAMS_CONSUMER_SIDECAR` env,
3. cached binary from a previous download,
4. auto-download from the release (checksum-verified),
5. the binary on `PATH`.

## Usage

```go
package main

import (
	"log"

	ddbstreams "github.com/LeeroyHannigan/amazon-dynamodb-streams-consumer/clients/go"
)

type processor struct{}

func (processor) ProcessRecords(records []ddbstreams.Record) {
	for _, r := range records {
		// r.EventName is INSERT / MODIFY / REMOVE.
		// r.Keys, r.NewImage, r.OldImage are decoded to Go natives.
		log.Printf("%s %v", r.EventName, r.Keys)
	}
}

// Optional: implement ShardEnded(shardID string) to be notified at SHARD_END.

func main() {
	w, err := ddbstreams.New(ddbstreams.Config{
		StreamArn:  "arn:aws:dynamodb:us-east-1:123456789012:table/Orders/stream/2026-...",
		LeaseTable: "my-app-leases",
		Processor:  processor{},
		Region:     "us-east-1",
	})
	if err != nil {
		log.Fatal(err)
	}
	code, err := w.Run() // blocks until the sidecar shuts down
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("exited %d", code)
}
```

Call `w.Stop()` from another goroutine for a graceful shutdown.

## Attribute decoding

`S`/`N` → `string` (numbers stay canonical strings), `Bool` → `bool`,
`Null` → `nil`, `B` → `[]byte`, `M` → `map[string]any`, `L` → `[]any`,
`Ss`/`Ns` → `[]string`, `Bs` → `[][]byte`.

## Record format: native (default) vs DynamoDB JSON

By default your processor gets **plain Go values** — no `{"S": ...}` /
`{"N": ...}` type wrappers to unpack. That hand-written `AttributeValue`
unmarshalling is already done for you.

To instead receive **canonical DynamoDB JSON** (the typed
`{"S"|"N"|"BOOL"|"NULL"|"B"|"M"|"L"|"SS"|"NS"|"BS"}` shape the AWS SDKs consume —
handy for KCL migration or writing items straight back with the SDK), set
`Config.RecordFormat` to `"ddb_json"`. One top-level switch, applied to every
record.

```go
ddbstreams.New(ddbstreams.Config{
	// ...
	RecordFormat: "ddb_json", // default is "native"
})
// native:   r.NewImage == map[string]any{"id": "42", "active": true}
// ddb_json: r.NewImage == map[string]any{"id": map[string]any{"N": "42"}, ...}
```

Numbers stay strings in both modes to avoid precision loss. This is a
client-side presentation choice — the wire protocol is unchanged.

## Start position

`Config.InitialPosition` controls where a freshly-seeded shard begins reading.
It is optional and accepts `"TRIM_HORIZON"` (default) or `"LATEST"`. The value
is trimmed and upper-cased before being passed to the sidecar, so `"latest"`
and `"LATEST"` are equivalent.

```go
ddbstreams.New(ddbstreams.Config{
	// ...
	InitialPosition: "LATEST", // default is "TRIM_HORIZON"
})
```

## Testing

```bash
cd clients/go && go test ./...
```

Runs native-decoding unit tests plus the shared binding **conformance** suite
(`../../conformance/fixtures/*.json`) against the language-agnostic
`replay_sidecar.py` — no AWS, no real sidecar required (needs `python3` on PATH).

## Bounding footprint

`Config.MaxProcessingConcurrency` (optional; 0 = unbounded) caps the number of
shards processed concurrently, keeping footprint O(max) as the table's
shard count grows. Unset = one processing slot per shard. Bounds
concurrent record delivery only; at-least-once, per-item, and per-shard ordering
are preserved.
