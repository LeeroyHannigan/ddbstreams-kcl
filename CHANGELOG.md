# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Pre-1.0 (`0.x`)
releases may include breaking changes between minor versions.

The version is shared across all crates and the language clients (lockstep); a
single `vX.Y.Z` git tag releases the whole project.

## [Unreleased]

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

[Unreleased]: https://github.com/LeeroyHannigan/amazon-dynamodb-streams-consumer/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/LeeroyHannigan/amazon-dynamodb-streams-consumer/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/LeeroyHannigan/amazon-dynamodb-streams-consumer/releases/tag/v0.1.0
