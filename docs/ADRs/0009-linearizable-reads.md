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

## Note (2026-09-18): what `ensure_linearizable` waits on, and what a test must wait on

Three facts about the read barrier that are easy to assert past, found while stabilising the M3
suite.

- **The engine's own counters can lead OpenRaft's `last_applied` by one apply.**
  `NodeMetrics::applied_commands` and `NodeMetrics::cluster_revision` are read from the store,
  which increments them *inside* the apply; `RaftMetrics::last_applied` is published by the core
  task after the apply returns. There is therefore a real window in which a node reports
  `applied_commands = n` while `last_applied` still names entry `n-1`.
- **`ensure_linearizable` waits on `last_applied`, not on the store's counters.** It returns once
  quorum has confirmed leadership *and* the state machine has applied up to the read barrier, and
  "applied" there means the core's index. A test that waits on `applied_commands` and then reads
  is waiting on the wrong thing: it can proceed while the barrier would still block, which is why
  M3-59 was flaky in the direction of a timeout rather than a wrong answer.
- **The barrier is capped by the server-side `NodeConfig::read_timeout`, not by the client
  deadline.** `read_inner` wraps `ensure_linearizable` in `read_timeout` and reports
  `Unavailable` when it elapses. A client that asks for a 30 s deadline against a node configured
  with a 2 s `read_timeout` gets an answer in about two seconds, and the reason names the
  barrier, not the caller's budget. The two bounds are independent on purpose — the server must
  be able to stop working on a read whatever a caller claims to be willing to wait — but a test
  that sets a generous client deadline and expects it to be the binding one is testing a
  configuration it does not have.

Consequence for tests: wait on OpenRaft's `last_applied` (`ConfigNode::wait_applied`, or a
`wait_until` predicate over `NodeMetrics::last_applied`) before asserting a linearizable read,
never on `applied_commands` or `cluster_revision`. Those two remain the right thing to assert
*about* applied state; they are simply not the barrier's clock.
