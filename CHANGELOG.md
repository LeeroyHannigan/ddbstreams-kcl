# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Pre-1.0 (`0.x`)
releases may include breaking changes between minor versions.

The version is shared across all crates and the language clients (lockstep); a
single `vX.Y.Z` git tag releases the whole project.

## [Unreleased]

## [0.1.4] - 2026-07-20

### Added
- `max_processing_concurrency` (opt-in) on every client and the sidecar
  (`DDB_STREAMS_CONSUMER_MAX_PROCESSING_CONCURRENCY`): caps the number of shards
  processed concurrently so per-worker footprint stays O(cap) as the stream's
  shard count grows. Unset keeps prior behavior (one processing slot per shard).
  Delivery and ordering semantics are unchanged (at-least-once, per-item and
  per-shard order preserved; a shard is never split). Supports online resize.
- Processing-concurrency metrics: per-slot-wait (`processing.slot_wait_ms`) and
  the configured cap (`processing.max_concurrency`), exported via the OTEL sink.

## [0.1.3] - 2026-07-04

### Added
- Worker-level `record_format` option (`native` default, `ddb_json` opt-in),
  set once for the whole processor across all clients. `native` delivers
  decoded native values (no DynamoDB-JSON unmarshalling burden); `ddb_json`
  delivers canonical DynamoDB JSON (the `{"S"|"N"|"BOOL"|"NULL"|"B"|"M"|"L"|
  "SS"|"NS"|"BS"}` shape the AWS SDKs / `boto3` consume) for SDK interop and
  KCL migration.

## [0.1.2] - 2026-07-04

### Fixed
- Python Linux wheels now ship as `manylinux_2_17` **and** `musllinux_1_2`
  (x86_64 + aarch64), bundling the static-musl sidecar. This lowers the glibc
  floor from 2.28 to 2.17 (installable on Amazon Linux 2, CentOS 7, and newer)
  and adds Alpine/musl support — matching the glibc-independence the Go/Node
  auto-downloaded sidecar gained in 0.1.1.

## [0.1.1] - 2026-07-04

### Fixed
- Linux sidecar release assets are now statically linked (musl), removing the
  glibc version dependency. The auto-downloaded sidecar (Go/Node clients) now
  runs on any Linux — Amazon Linux 2, older distributions, minimal/`scratch`
  containers; previously the binary required a newer glibc than some hosts
  provide (`GLIBC_x.y not found`).

## [0.1.0] - 2026-07-02

Initial alpha release.

### Added
- Rust core: shard-ordering engine (parent-before-child, split/merge lineage,
  checkpoint resume), optimistic-lock lease coordination (acquire/renew/steal/
  release), wall-clock lease expiry, lineage-safe cleanup, and the typed
  DynamoDB Streams record model.
- DynamoDB Streams source: shard-graph construction and an async reader with
  shard-iterator reuse and trimmed/expired self-healing.
- Worker runtime (`Fleet`): per-shard concurrent processing, shard-sync, and
  ack-gated checkpointing; graceful lease release on shutdown for fast failover.
- Sidecar binary and a newline-delimited JSON protocol for language clients.
- Python client (`dynamodb_streams_consumer`) with a zero-dependency stdio bridge.

[Unreleased]: https://github.com/LeeroyHannigan/amazon-dynamodb-streams-consumer/compare/v0.1.4...HEAD
[0.1.4]: https://github.com/LeeroyHannigan/amazon-dynamodb-streams-consumer/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/LeeroyHannigan/amazon-dynamodb-streams-consumer/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/LeeroyHannigan/amazon-dynamodb-streams-consumer/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/LeeroyHannigan/amazon-dynamodb-streams-consumer/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/LeeroyHannigan/amazon-dynamodb-streams-consumer/releases/tag/v0.1.0
