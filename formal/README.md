# Formal model — `max_processing_concurrency` (multiplexing)

A TLA+ model of the multiplexing scheduler, checked once with TLC. **Not part of
CI or the Cargo build** — it is a one-time correctness proof / design artifact.

## What it models

`Multiplexing.tla` abstracts `worker/src/fleet.rs` across the full feature:

- **Per-worker slot pool** (`cap[w]`) bounding concurrent delivery — the tokio
  `Semaphore` in `process_shard`.
- **Online resize** (`set_max_processing_concurrency`): grow freely; shrink only
  to `>=` the in-flight count (the "shrink waits for a slot to free" rule).
- **Multi-worker lease ownership**: each shard owned by `<= 1` worker; only the
  owner processes it; leases dropped on crash or handed off at a batch boundary.
- **Reshard**: a child shard is gated on its parent completing (parent-before-
  child) and is never split across slots.
- **Checkpoint / at-least-once** across crash and lease handoff.

## Properties proven

| Property | Meaning |
|---|---|
| `PerWorkerBound` | each worker runs `<= cap[w]` shards at once — always, including during/after an online resize |
| `MutualExclusion` | no shard is processed by two workers at once (no split-brain) |
| `OwnedWhileProc` | a shard in flight on `w` is owned by `w` |
| `ParentBeforeChild` | a child shard is only processed after its parent is fully checkpointed |
| `CheckpointOK` | per-shard checkpoint stays in `0..MaxSeq` and only advances by +1 (no skip) |
| `AtLeastOnce` | `delivered[s] >= checkpoint[s]` — every checkpointed record delivered ≥ once; crashes add duplicates, never loss |
| `Termination` | every shard (incl. children) is eventually fully processed — no starvation, no permanent loss — under fair scheduling with bounded crashes/handoffs |

## How to run

Requires Java + `tla2tools.jar`
(https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar).

```
java -cp tla2tools.jar tlc2.TLC -deadlock -config Multiplexing.cfg Multiplexing.tla
```

`-deadlock` is passed because a fully-drained state has no data-advancing
successor; that is a valid end state, not a deadlock.

Topology is fixed in `Multiplexing.tla` (2 workers; roots `r1`,`r2`; child `c1`
of `r1`). The `.cfg` carries only the numeric bounds: `MaxSeq = 1`, `MaxCap = 2`,
`MaxCrashes = 1`, `MaxHandoffs = 1`.

## Result (TLC 2.19, 2026-07-20)

```
Model checking completed. No error has been found.
23185 states generated, 5164 distinct states found, 0 states left on queue.
```

All seven safety invariants and the `Termination` liveness property hold across
the full 5,164-state space. Raising the bounds (more workers/shards/records,
larger caps, more crashes/handoffs) only enlarges the state space; the argument
is unchanged.

## Scope / not modelled

This model proves the **multiplexing** feature (cap, resize, per-worker bound)
together with the ownership, reshard, and crash/handoff behaviours it interacts
with — as an abstraction, not the Rust itself. A full formal model of the whole
lease-coordination protocol (fair-share, steal, expiry, leader election, counter
freshness) is a separate, larger initiative and is not covered here.
