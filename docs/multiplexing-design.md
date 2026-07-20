# Multiplexing (`maxProcessingConcurrency`) — Design Spec

**Status:** Design locked; API pending bar-raiser (Remy).
**Scope:** `amazon-dynamodb-streams-consumer` — let a customer *optionally* bound processing concurrency so footprint stays constant as the stream's shard count grows, with **no change to delivery or ordering semantics**.
**Author:** Lee Hannigan · **Reviewers:** Amrith Kumar, Watty (Streams PM), Remy (API)

---

## 0. One feature, cleanly separated

| Feature | Status | Routing unit |
|---|---|---|
| **Multiplexing** — optional cap on concurrent shard processing; footprint O(cap), not O(shards) | **This spec** | **the SHARD** (never split) |
| Per-partition-key (item-collection) cross-shard ordering | **Deferred** — not feasible client-side; server-side + formal proof; revisit later | item primary key |

This spec routes **whole shards**, never item keys. A shard is never split, so there is **no scatter/gather, no cross-shard checkpoint coordinator, and no change to ordering semantics.**

---

## 1. Problem

A DynamoDB stream for a busy or long-lived table has many shards. KCL / the KCL adapter map **one processing lane (thread + prefetch buffers + processor) per shard**, so a stream with hundreds of low-RPS shards forces hundreds of lanes → footprint grows with the **stream's shard count**, not the customer's throughput or cores.

Measured (44-shard table, same host): KCL 3.4.3 + KCA ≈ **1.2 GB RSS / 124 threads**, growing with shard count. This feature makes that curve flat and **customer-controlled**.

---

## 2. The knob

> **`maxProcessingConcurrency`** — the maximum number of shards this worker **processes concurrently**. **Opt-in.** Unset ⇒ exactly today's behavior (one processing slot per shard, unbounded).

When set to `k`:
- At most `k` `process()` calls are in flight at once, per worker.
- The worker still **leases all its shards** and **reads** them all (bounded); only *processing* concurrency is capped.
- Footprint (processing threads/contexts + in-flight buffers) is **O(k)**, independent of shard count.
- **Semantics are unchanged:** at-least-once, per-item order, per-shard order. A shard is never split across processing slots.

---

## 3. Guarantees (identical to base — only footprint changes)

1. **At-least-once delivery.** Per-shard checkpoint; crash → resume that shard from its last durable checkpoint. (Exactly-once *effect* = idempotent processor; app's job — unchanged. We do **not** claim exactly-once delivery.)
2. **Per-item ordering.** All records for one item (pk+sk) come from one shard, read in sequence order, processed one batch at a time on one slot → in order.
3. **Per-shard ordering.** A shard's records are delivered in sequence order.
4. **Static footprint under a set cap.** Processing contexts + in-flight buffers are O(`maxProcessingConcurrency`), independent of shard count.

**NOT changed / NOT claimed:** cross-shard ordering; cross-partition-key ordering (deferred). Multiplexing adds **no new ordering promise** — it is a footprint control that is semantically transparent.

---

## 4. Engine — bound the existing async per-shard fleet (no new subsystem)

The Rust core is **already async**: `worker/src/fleet.rs` ("concurrency model A") spawns **one cheap tokio task per owned shard** (a `JoinSet`), and tokio's fixed worker-thread pool is the executor. We are not the JVM — there is no thread-per-shard and no need to build an executor pool. So `maxProcessingConcurrency` is implemented by **bounding concurrency**, not by adding a scheduler.

**Implementation:** a shared `Arc<tokio::sync::Semaphore>` with `k` permits, held by the `Fleet`. Inside each shard task's loop (`process_shard`):

```
loop {
    let _permit = sem.acquire().await;      // gate the critical section (None ⇒ no semaphore)
    let batch = source.get_records(shard, iter).await?;   // fetch under permit ⇒ in-flight = O(k)
    consumer.process_records(&batch).await;               // customer processing
    lease.checkpoint(shard, batch.last_seq).await?;       // per-shard checkpoint (unchanged)
    // drop(_permit) here; heartbeat/lease-renew happens OUTSIDE the permit (see below)
}
```

- **`None` (unset):** no semaphore (unbounded) ⇒ behavior-identical to today. Asserted by test.
- **In-flight memory O(k):** acquire the permit *before* fetching the batch to be processed, so only `k` shards hold a batch at once. Idle (permit-less) shards buffer nothing.
- **Per-shard order / at-most-one-per-shard:** each shard task is sequential and holds ≤1 permit → the existing single-threaded-processor contract is unchanged.
- **Fairness:** tokio `Semaphore` is FIFO — every waiting shard eventually gets a permit → no starvation.
- **Online resize:** `Semaphore::add_permits(n)` to grow; acquire-and-`forget` `n` permits to shrink (takes effect at the next batch boundary). No shard/slot remap.

**Critical P1 detail — heartbeat must not be gated by the permit.** A shard waiting on a permit must keep renewing its lease or it will be reaped as expired. Scope the permit to the fetch+process+checkpoint section only; keep lease heartbeat/renewal on an independent path (timer or the coordinator cycle) so permit-starved shards retain ownership.

This is a surgical change to a tested fleet (`worker/src/fleet.rs` + `process_shard` in `worker/src/lib.rs`), not a rewrite. Your invariant holds: same item → same shard → same worker (lease) → sequential shard task → in order.

---

## 5. Footprint

| | Unset (today) | `maxProcessingConcurrency = k` |
|---|---|---|
| Executor contexts | O(shards) | **O(k)** |
| In-flight buffers | O(shards × prefetch) | **O(global credit budget)** |
| Reader tasks | O(shards), ~KB each | O(shards), ~KB each (negligible) |
| Grows as the stream's shard count grows? | **yes** | **no** |

---

## 6. Backpressure & fairness
- **Global in-flight credit budget** bounds total buffered records → memory independent of shard count. A reader fetches ahead only while it holds credits.
- **Fair ready-queue:** work-conserving — no executor idles while any shard is ready → automatic load balancing, no head-of-line blocking across shards.
- **Starvation-free:** a shard re-enters the ready queue only after its current batch completes; round-robin pull ensures every shard is serviced.
- **Hot shard = inherent limit:** a single hot shard occupies one slot; other shards proceed on other slots. Surface per-shard `IteratorAge` / lag.

## 7. Reshard & rebalance
- **Reshard:** parent-before-child (lease layer). Parent drains + checkpoints closed; child readers start and join the ready set. No slot affinity to preserve.
- **Lease move:** shard reassigned to another worker; new owner resumes from durable checkpoint (at-least-once). Pool state is per-worker, ephemeral.

## 8. Multi-worker
`maxProcessingConcurrency` is **per worker**. Lease layer already guarantees one shard → one worker → **same item → same worker**, globally, by construction. No cross-worker coordination or record shuffle. Fleet concurrency = Σ per-worker caps.

---

## 9. Public API (for Remy)

```
WorkerConfig {
    max_processing_concurrency: Option<usize>,   // None ⇒ unbounded (today). Some(k>=1) ⇒ ≤ k shards processed at once.
    // ... existing fields unchanged
}
```
- **Type:** plain optional unsigned integer. Cleanest across all six language bindings (no enum/generic gymnastics — see the jsii/typed-lang constraints).
- **Default:** `None` ⇒ **zero behavior change** for existing consumers. The easy back-compat yes.
- **Validation:** reject `0` and negatives at config time; a value ≥ leased-shard-count is legal and behaves as unbounded.
- **Evolvability (adaptive later, additively — does not box us in):** a future `auto_scale_processing: bool` (or a mode field) can treat `max_processing_concurrency` as the *ceiling* and size the pool dynamically up to it. Adding that field later is non-breaking, so the integer is safe to ship now.
- **No processor API change.** `RecordProcessor` + per-shard `Checkpointer` are untouched; only *which thread* runs a shard's `process()` changes. Cross-language names: `maxProcessingConcurrency` (Java/JS/.NET/Go), `max_processing_concurrency` (Rust/Python).

---

## 10. Rescaling
Because shards aren't pinned to slots, changing `maxProcessingConcurrency` = resize the executor pool, and the mechanism is trivial and safe (no key/shard remap, no drain barrier):
- **Grow** (`k → k+n`): spawn `n` executors; they start claiming ready shards immediately. The at-most-one-executor-per-shard invariant is held by the per-shard atomic claim flag, independent of pool size.
- **Shrink** (`k → k−n`): signal `n` executors to stop **at the next batch boundary** (checkpoint → release → exit); released shards re-enter the ready queue for the survivors. No reorder, no loss.

Therefore **online resize ships in v1** via a runtime setter (`set_max_processing_concurrency(k)`) — cheap, and a real operability win (retune a hot consumer without a restart). The genuinely separate, deferred piece is the **adaptive autoscaling *policy*** (deciding the size automatically from load signals with hysteresis) — that is the fast-follow, not the resize mechanism.

## 11. Metrics
- Worker: `MaxProcessingConcurrency`, `ActiveSlots`, `ReadyQueueDepth`, `InFlightCredits`, `RSS` (must be flat in shard count — the proof metric).
- Per shard: `ShardIteratorAgeMs`, `ShardCheckpointLagSeq`, `ShardStarvedMs`.

## 12. Formal model (action item #5)
Machine-check (TLA+ / Stateright):
1. **Shard integrity:** every record from a shard is processed by exactly one slot at a time, in sequence order (no split, no reorder).
2. **Bound:** concurrent `process()` calls ≤ `maxProcessingConcurrency` at all times.
3. **Footprint bound:** in-flight records ≤ global credit budget, independent of shard count.
4. **At-least-once:** under any crash/reassign interleaving, every record delivered ≥1 time.
5. **Checkpoint safety:** a shard's checkpoint never exceeds its last processed seq (trivially per-shard).
6. **Liveness:** every leased shard is serviced infinitely often (no starvation).

## 13. Non-goals
- Cross-shard / per-partition-key ordering — deferred, server-side.
- Exactly-once delivery — unchanged (idempotency is the app's job).

## 14. Rejected alternatives
- **Static shard→lane assignment** (fixed N lanes, each owns a shard subset): head-of-line blocking within a lane, load imbalance (hot shards cluster), serialized network I/O, and rescale requires remapping. The ready-queue engine dominates it on every axis.
- **Hashing the item primary key mod N:** splits a shard across slots → forces a low-water-mark scatter/gather checkpoint coordinator and is really the deferred cross-shard-key feature. Rejected for multiplexing.

## 15. Open (minor) questions
- Global credit budget: expose as a second knob or derive from `maxProcessingConcurrency × batch_size`? (Lean: derive; keep the public surface to one field.)
- Autoscaling policy (fast-follow): which load signal drives it — ready-queue depth, max shard `IteratorAge`, or executor utilization — and the hysteresis to prevent flapping?
