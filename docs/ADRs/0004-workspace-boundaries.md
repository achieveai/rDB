# ADR-0004: Workspace crates and dependency direction

**Status:** Accepted  
**Date:** 2026-09-17  
**Spec:** §3.1, §6

## Decision

Cargo workspace `retcd` with crates under `crates/`:

| Crate | Owns | May depend on |
|---|---|---|
| `config-core` | requests/responses/records/revisions/typed errors, `ConfigStore` trait, `Principal` + `Authorizer` hook, versioned `Command`, `MutationEvent`, deterministic `KvState` (M0 state machine), `Capabilities` | `bytes`, `serde`, `thiserror` |
| `config-log` | `tracing` JSONL init, context fields, gRPC metadata propagation helpers, test-context macro | `tracing*`, `serde_json` |
| `config-storage` | `RaftLogStorage`/`RaftStateMachine` impls: `Ephemeral` (M1) and `Rocks` (M2); identity file binding; fault injector | core, log, openraft, rocksdb |
| `config-engine` | OpenRaft lifecycle, `Engine` (linearizable reads, quorum writes), `ConfigNode` embedding, health, capabilities, gossip hint validation, Raft network client | core, log, storage, gossip, grpc-peer types, openraft |
| `config-gossip` | `memberlist` adapter behind `GossipObservationSource` | core, log, memberlist |
| `config-grpc` | `.proto`, `protox` build, client+peer tonic services, mTLS config, error mapping, `GrpcClient` | core, log, engine, tonic/prost |
| `config-client` | `ConfigStore` re-exports, `DirectClient`, `GrpcClient` facade, leader-hint retry policy | core, engine, grpc |
| `config-server` | binary: config file loading, runtime creation, supervision, signals | client, engine, grpc, gossip |
| `config-testkit` | in-process multi-node harness, test cert generation, fault injection, conformance suite | all (dev/test only) |

Rules:
- Dependencies point inward to `config-core`. `config-core` has no async runtime or network deps.
- Public API of every crate except `config-storage`/`config-gossip` internals must not expose
  OpenRaft, RocksDB, tonic, prost, or memberlist types. Enforced by review and by
  `#![deny(missing_docs)]` on public crates.
- The library never creates a global Tokio runtime; `config-server` creates its own.

## Consequences

- Some duplication of request types across core (Rust) and proto (wire); mapped in `config-grpc`.
- Tests that need multiple crates live in `config-testkit` or the top-level `tests/` crate.
