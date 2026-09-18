# ADR-0008: Storage — ephemeral (M1) and RocksDB (M2) layout and sync rules

**Status:** Accepted  
**Date:** 2026-09-17  
**Spec:** §9, §21 M1–M2

## Decision

Two implementations of the same two OpenRaft v2 traits, selected at node build time:

1. `EphemeralStore` (M1): in-memory `BTreeMap` log + in-memory `KvState`. Capabilities report
   `durability=Ephemeral`. Used by fast in-process tests permanently.
2. `RocksStore` (M2): one RocksDB instance per node, column families:

| CF | Key | Value |
|---|---|---|
| `raft_log` | log index `u64` big-endian | serialized `Entry<C>` |
| `raft_meta` | `"vote"`, `"committed"`, `"last_purged"` | serialized `Vote` / `LogId` |
| `kv` | user key bytes | `Record` value bytes (value, create_rev, mod_rev) |
| `state_meta` | `"cluster_revision"`, `"last_applied"`, `"membership"`, `"identity"` | serialized |

Rules (§9.3 invariants → implementation):
- `save_vote` writes with `WriteOptions.set_sync(true)` and returns only after fsync.
- `append` writes entries in one `WriteBatch` with sync, then invokes the `LogFlushed`
  callback. Holes are impossible because we assert `index == last_index + 1` before writing.
- `apply` for a batch of entries performs **one** `WriteBatch` containing KV changes,
  `cluster_revision`, `last_applied`, and membership, written with sync. Responses are
  returned only after the batch succeeds.
- On restart, `applied_state()` returns the persisted `last_applied`; OpenRaft re-applies any
  committed entries above it. Because revision allocation is inside the batch, replay cannot
  double-allocate.
- Any RocksDB error → `StorageError` + node marked `Fatal` (health stream) + all client
  operations return `FatalStorage`; the process never continues optimistically.
- Snapshots: `SnapshotPolicy::Never`, `max_in_snapshot_log_to_keep = u64::MAX`; snapshot
  trait methods return a typed `Unsupported` storage error. No log purge.
- Identity: `state_meta/identity` = `{cluster_id, recovery_epoch, node_id}`; mismatch with the
  node config fails `open()` before Raft starts (ADR-0011).
- Blocking RocksDB calls run on `tokio::task::spawn_blocking` so Raft timers are not starved.

## Consequences

- Log grows unbounded in this release (documented).
- Fault injection: `RocksStore` accepts a `FaultInjector` hook (test-only) that can fail or
  crash at `before_vote_sync`, `after_vote_sync`, `before_log_append`, `after_log_append`,
  `before_state_batch`, `after_state_batch`.

## Verification

- M2 tests: restart survives acknowledged mutations; committed-but-unapplied replay has no
  duplicate revisions; crash injection at each boundary; identity mismatch blocks startup.
