# ADR-0002: OpenRaft `=0.9.25` is the sole authority

**Status:** Accepted  
**Date:** 2026-09-17  
**Spec:** §3.2, §9.1, §13.1, §19

## Context

We need a Raft implementation in Rust that is mature enough for a 3-voter static cluster and
allows pluggable storage/network. OpenRaft 0.10 is alpha; 0.9.25 is the latest 0.9 release.

## Decision

- Pin `openraft = "=0.9.25"` in `Cargo.toml` and commit `Cargo.lock`.
- Use the v2 storage traits `RaftLogStorage` + `RaftStateMachine` (feature `storage-v2`),
  kept as separate Rust types even when sharing one RocksDB instance.
- One Raft group, three voters. Node ids are `u64` public Node IDs (see ADR-0011); `Node`
  carries the peer gRPC endpoint. Membership is set once by explicit `Raft::initialize` from
  a controlled formation harness; no auto-formation and no membership change API in this release.
- Committed Raft entries and committed membership are the only authority for state, voters,
  leadership and ordering. Nothing else (gossip, seeds, DNS, hints) may mutate them.
- Upgrading OpenRaft is a migration requiring compatibility and fault tests, not a routine bump.

## Consequences

- We implement the exact trait surface of 0.9.25, verified from crate source (research notes
  in the scratchpad) and mirrored by `config-storage`.
- Snapshot trait methods exist but are configured never to trigger (ADR-0008).

## Verification

- `cargo tree -i openraft` shows exactly 0.9.25.
- M1 tests: no empty node self-forms; one stopped voter still commits; isolated leader rejects
  strict reads/writes.
