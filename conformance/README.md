# Binding conformance suite

The hard correctness (leases, ordering, reshard, checkpointing) lives in the
Rust core + sidecar and is tested there and by the live soak. A **language
binding** is only a thin stdio bridge over the JSON-Lines wire protocol
(`protocol/src/lib.rs`), so it is tested against a shared, language-agnostic
contract rather than re-deriving cases per language.

## Pieces

- `fixtures/*.json` — the shared contract. Each fixture scripts a sidecar
  session and states what the binding must deliver. **Language-neutral.**
- `replay_sidecar.py` — a stdlib-only fake sidecar driven by a fixture. Every
  binding spawns this as its `sidecar_cmd`; no per-language fake is needed.
- `clients/<lang>/tests/` — a thin runner that, for each fixture, launches the
  binding's Worker against `replay_sidecar.py <fixture>` and asserts the
  `expect` block. `clients/python/tests/test_conformance.py` is the template.

## Fixture format

```jsonc
{
  "name": "...",
  "description": "...",
  "server_script": [            // ordered; the replay sidecar executes these
    {"emit":     <object>},     // write one JSON-Lines message to stdout
    {"emit_raw": "<string>"},   // write a raw line (inject a malformed line)
    {"await_checkpoints": [     // block on client stdin until each ack arrives,
        {"shard": "..", "seq": ".."}   // validating the acked seq matches
    ]}
  ],
  "expect": {
    "records_per_shard": {"s0": 2},        // count delivered to the processor
    "record_order":     {"s0": ["1","2"]}, // sequence_number order per shard
    "shard_ended":      ["s1"]             // shards that fired shard_ended
  }
}
```

Server message types: `records` `{shard, last_seq, records[]}`,
`shard_complete` `{shard}`, `shutdown` `{reason}`.
Client message types: `ready`, `checkpoint` `{shard, seq}`, `stop`.

## The contract a binding must satisfy (for every fixture)

1. **Delivery** — the processor receives exactly `records_per_shard` records
   per shard, in `record_order` (by `sequence_number`).
2. **Checkpointing** — after each batch, the binding acks the batch's
   `last_seq`. `replay_sidecar.py` validates this and **exits non-zero** on a
   wrong/absent ack, so the runner asserts a **0 exit code**.
3. **Lifecycle** — `shard_complete` invokes the optional `shard_ended`
   callback; `shutdown` cleanly ends the run.
4. **Robustness** — malformed (`emit_raw`) lines are ignored without crashing.

Native attribute decoding (e.g. `N` → string vs int, `B` → bytes) is a
language-idiomatic choice and is covered by each binding's **own unit tests**,
not by these shared fixtures.

## Running (Python, the template)

```bash
cd clients/python && python3 -m unittest discover -s tests
```

## Adding a language

1. Implement the binding (spawn sidecar, parse protocol, deliver, ack, shutdown).
2. Write a runner mirroring `clients/python/tests/test_conformance.py`: iterate
   `conformance/fixtures/*.json`, launch the Worker with
   `sidecar_cmd=[<python>, conformance/replay_sidecar.py, <fixture>]`, assert the
   `expect` block and a 0 exit code.
3. Add a native-decoding unit test for that language's type mapping.
