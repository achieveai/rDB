# ADR-0009: Leader-linearizable reads and NotLeader hints

**Status:** Accepted  
**Date:** 2026-09-17  
**Spec:** §10.1, §16, §19.2

## Decision

- `Get` and `List` call `Raft::ensure_linearizable().await` on the local node first.
  Success proves current leadership by quorum and that the state machine has applied up to the
  read barrier; only then the engine reads local `KvState` and reports `read_revision`.
- `ForwardToLeader { leader_id, leader_node }` → `ConfigError::NotLeader { hint }` where the
  hint is the committed membership endpoint for that id (never a gossip endpoint). If the
  leader is unknown → `Unavailable`.
- A former leader that cannot reach quorum gets an error from `ensure_linearizable` →
  `Unavailable`. It never serves a successful strict read.
- Mutations go through `Raft::client_write`; `ForwardToLeader` maps the same way.
- `GrpcClient` follows at most N (default 3) authenticated leader hints per call, then returns
  the last error. `DirectClient` returns `NotLeader` to the embedder (it is on a follower).
- No stale/follower reads exist in this release.

## Verification

- M1 tests: follower returns `NotLeader` with correct hint; isolated leader (network blocked
  to both peers) returns `Unavailable` for Get/Put; after heal, reads succeed.
