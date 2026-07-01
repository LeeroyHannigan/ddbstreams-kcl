# REFERENCES — authoritative sources for robustness

This engine is **not** a naive reimplementation. Every correctness-critical behavior
is grounded in the authoritative **open-source (Apache-2.0)** sources below, so this
public repo can cite and port from them cleanly. This file maps each behavior to its
source for reviewers to verify fidelity.

> Scope note: we reference only the public Apache-2.0 upstreams. We do **not** copy
> from any internal/mirror forks. Ported code must retain Apache-2.0 attribution
> (see LICENSE / NOTICE).

## Authoritative packages (all Apache-2.0)
| Package | Role | Version |
|---|---|---|
| [`awslabs/amazon-kinesis-client`](https://github.com/awslabs/amazon-kinesis-client) (KCL) | Lease coordination, shard sync, checkpointing, lifecycle | 3.5.0 |
| [`awslabs/dynamodb-streams-kinesis-adapter`](https://github.com/awslabs/dynamodb-streams-kinesis-adapter) | DDB-Streams shard detection, data fetch, sleep/catch-up, lease mgmt | 2.3.0 (KCL 3.4.3) |

## DDB adapter classes to mirror (`.../streamsadapter/`)
| Class | What we take from it |
|---|---|
| `DynamoDBStreamsShardDetector` | `DescribeStream` pagination → shard list; the `StreamSource.describe_shards` impl |
| `DynamoDBStreamsShardSyncer` | Parent-before-child lease creation; parent-open-child-open inconsistency handling; lineage-replay-safe cleanup |
| `DynamoDBStreamsDataFetcher` | `GetShardIterator`/`GetRecords`; `Trimmed`/`ExpiredIterator`/`ResourceNotFound` handling; the `StreamSource.get_records` impl |
| `DynamoDBStreamsSleepTimeController` + `polling/` | Catch-up polling rate (`catchupEnabled`, `millisBehindLatestThreshold`, `scalingFactor`); recommended `MaxRecords=1000`, `IdleTimeInMillis=500` |
| `DynamoDBStreamsLeaseManagementFactory` | Lease table wiring specifics for DDB Streams |
| `StreamsSchedulerFactory` | Config surface; single vs multi-stream tracker; requires `DynamoDBStreamsShardRecordProcessor` + `DynamoDBStreamsPollingConfig` |

## KCL classes to mirror (`software.amazon.kinesis.*`)
| Class | What we take from it |
|---|---|
| `HierarchicalShardSyncer` | Child leases created only after parent `SHARD_END`; children initialized at `TRIM_HORIZON`; one-layer lease creation |
| `ShutdownTask` (`createLeasesForChildShardsIfNotExist`) | Merge child requires BOTH parents present/complete; defer via `BlockedOnParentShardException` on partial lineage, drop-with-1-in-N to let another worker retry |
| `LeaseCleanupManager` (`cleanupLeaseForCompletedShard`) | Parent lease deleted only after child lease(s) enter PROCESSING (tombstone → prevents lineage replay) |
| `DynamoDBLeaseTaker` / `DynamoDBLeaseRenewer` / `DynamoDBLeaseCoordinator` | Lease take/steal/renew, optimistic locking on `leaseCounter`, timing model, graceful handoff |

## Robustness behaviors → source

### Ordering (the imperative requirement)
- **Parent-before-child**: child processed only after parent `SHARD_END`; children start at `TRIM_HORIZON`.
  - Source: `HierarchicalShardSyncer`; KCL lease-lifecycle doc.
  - Encoded: `Scheduler.eligible()` + `parent_before_child_ordering` test.
- **Merge child requires BOTH parents**: up to two parents; partial lineage defers.
  - Source: `ShutdownTask.createLeasesForChildShardsIfNotExist`.
  - Encoded: `ShardMeta.parents: Vec<ShardId>` + `eligible()` requires ALL complete + `merge_child_waits_for_both_parents` test. **TODO:** port the partial-lineage defer/drop policy.
- **Per-shard in-sequence delivery + mandatory SHARD_END checkpoint** (unblocks children).
  - Source: KCL checkpointer + lease lifecycle.
  - Encoded: `Scheduler.run()`.

### Lease coordination (multi-worker, phase P2+)
- Optimistic locking on `leaseCounter`; renewer at `duration/3 - epsilon`, taker at `(duration + epsilon) * 2`; expired-first then steal-from-most-loaded; very-old (> `3 * leaseDuration`) taken first; spurious-failure re-read; graceful handoff (KCL 3.x).
  - Source: `DynamoDBLeaseTaker` / `DynamoDBLeaseRenewer` / `DynamoDBLeaseCoordinator`.

### Lineage-replay safety
- Parent lease deleted only after child lease(s) enter PROCESSING.
  - Source: `LeaseCleanupManager.cleanupLeaseForCompletedShard`.
  - Encoded: `cleanup::leases_safe_to_delete()` (lease-dynamodb) — a completed parent is deletable only once all its children have present, processing/completed leases.

### Bootstrap & shard sync at scale
- Empty lease table bootstrap creates leases for a snapshot of open shards; incremental sync thereafter via `ChildShards` in `GetRecords` responses; leader-only `PeriodicShardSyncManager`.
- **ShardFilter (DDB Streams specific):** `DescribeStream` supports the **`CHILD_SHARDS`** filter type (+ `shardId`) to fetch a read-only parent's children directly, avoiding a full paginated re-scan. This differs from Kinesis (`AT_TRIM_HORIZON`/`AT_LATEST`/`AT_TIMESTAMP`). The adapter falls back to full paginated `DescribeStream` if the filtered call errors.
  - Source: `DynamoDBStreamsShardDetector.listShardsWithFilter` / `describeStreamWithFilter`; KCL 2.3.0+ CHANGELOG.
  - Encoded: `ShardFilter::child_shards()` + `merge_child_shards()` (source-ddbstreams).
  - Guard: paginated shard enumeration can yield an incomplete hash range if trim horizon advances mid-pagination — validate/retry.

### DDB Streams specifics
- ~4-hour virtual shard rollover → frequent `SHARD_END` + child creation; 24h retention; polling only (no EFO); `Trimmed`/`ExpiredIterator`/`ResourceNotFound` → restart shard at `TRIM_HORIZON`.
- **parent-open-child-open inconsistency**: DDB Streams may report a parent as open while it already has children. A parent referenced by a present shard must have ended → mark it closed, else the parent-before-child gate blocks its children forever.
  - Source: `DynamoDBStreamsShardDetector.describeStream` Phase 2 / `ShardGraphTracker.closeOpenParents`.
  - Encoded: `close_open_parents()` (source-ddbstreams).
  - Source: `DynamoDBStreamsDataFetcher`; KCL Adapter docs.

### Efficiency & runtime primitives
- **Shard-iterator reuse**: thread the `next_shard_iterator` returned by each `GetRecords` instead of calling `GetShardIterator` per poll; re-derive only on reposition (checkpoint change) or expiry.
  - Source: `DynamoDBStreamsDataFetcher` (holds the iterator; re-derives on `Trimmed`/`ExpiredIterator`).
  - Encoded: `DdbStreamsSource` per-shard `Cursor` cache (`cached_iterator`/`store_cursor`), validated against the requested `after`; self-heals expired/trimmed iterators (source-ddbstreams `aws`).
- **Adaptive catch-up polling**: respect the DDB Streams ~4 `GetRecords`/s/shard throttle (≥250 ms floor) while catching up; back off when idle at the tip.
  - Source: `DynamoDBStreamsSleepTimeController` / `polling` config.
  - Encoded: `backoff::PollBackoff` (pure, deterministic) — floor on data, exponential idle backoff to a cap. Primitive ready for the continuous run loop.
- **Multi-stream lease keys**: namespace the lease key by stream so shard ids don't collide across streams.
  - Source: `MultiStreamLease` / `StreamIdentifier.serialize()` (`<streamId>:<shardId>`).
  - Encoded: `multistream::multi_stream_lease_key`/`parse_lease_key`/`shard_of` — reversible, splits on the last colon (DDB shard ids contain none, so a stream ARN's embedded colons are preserved). Single-stream keys are the bare shard id, as the fleet uses today.

## Operational anti-patterns to avoid (industry/KCL guidance)
1. Child **processes** not threads per partition (crash isolation) — matches our daemon+IPC design.
2. Stable lease-owner id (`taskARN:pid` / `podName` / `instanceId:pid`), never hostname.
3. Lease TTL ≥ 10s (15–30s for containers); never shorter than GC/throttle pauses.
4. Always use fencing tokens; no zombie writes after lease loss.
5. SIGTERM handlers to release leases promptly.
6. Auto-scaling cooldown ≥ lease duration.

## Provenance
Sourced 2026-07-01 from the public awslabs Apache-2.0 repositories listed above.
