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

## Clarifications (2026-09-18, Architect, from test-plan M2-M3 §11)

- Fault boundaries are exactly eight: `BeforeVoteSync, AfterVoteSync, BeforeLogAppend,
  AfterLogAppend, BeforeLogFlush, AfterLogFlush, BeforeStateBatch, AfterStateBatch`. Log append
  is write-then-explicit-sync (`flush_wal(true)` / `WriteOptions.sync` on a separate step) so the
  three log boundaries are distinct instants.
- `RaftLogStorage::save_committed` / `read_committed` MUST be implemented by both stores (the
  OpenRaft defaults are no-ops, which would silently disable committed-but-unapplied replay).
- `FaultAction::Crash` returns an error AND poisons the store: every later call fails until the
  store is reopened; `Drop` must not flush pending data. `FaultAction::Fail` is a plain
  recoverable I/O error.
- `RocksStore` reports `Durability::Persistent` when opened with full sync and verified identity;
  the "only after M2 gates pass" rule is enforced by CI requiring `tests/m2_*.rs` green, not by a
  cfg flag. `PersistentUnverified` is reported when opened with sync disabled (dev/bench only).

### Note (2026-09-18): Raft node id type

OpenRaft 0.9.25 requires `NodeId: Default`. `config_core::NodeId` deliberately has no
`Default` (a `NodeId(0)` footgun), so the Raft plane uses `RaftNodeId = u64`
(`config_storage::TypeConfig`) and converts at the engine boundary. Ratified by the lead.

### Note (2026-09-18, revised): `loosen-follower-log-revert` is a **test-only** feature

**Why it is needed at all.** OpenRaft's leader asserts that a follower's log never goes
backwards. When it does, `debug_assert` fires and the leader's core task aborts. An
`EphemeralStore` restart is exactly that event by construction: the node comes back under the
same id with an empty log (test plan M1-09, M1-43). Without the feature those tests cannot
express the behaviour M1 is specified to have.

**Why relaxing it is acceptable — and only here.** The assertion detects a real fault: a voter
that silently lost its log is a voter that may have lost an acknowledged write. Relaxing it in
a *shipped* binary would mean a production leader could no longer distinguish "this follower
was wiped" from "this follower is fine". Relaxing it in a *test* binary costs nothing, because
the wipe is the thing the test deliberately caused.

**How it is scoped.** The feature is **not** in the workspace `openraft` dependency. It is a
`[dev-dependencies]` feature of exactly two crates — `config-engine` and `config-testkit` —
which are the only crates whose tests restart a node onto a fresh store. Cargo's v2 resolver
does not unify dev-dependency features into a normal build, so `cargo build` compiles openraft
with the assertion intact and only `cargo test` relaxes it. Observed:

```text
$ cargo tree -e features -p config-engine --edges normal | grep -c loosen   # cargo build
0
$ cargo tree -e features -p config-engine | grep -c loosen                  # cargo test
1
$ cargo tree -e features -p config-testkit --edges normal | grep -c loosen
0
$ cargo tree -e features -p config-testkit | grep -c loosen
1
```

**Which tests guard it.** `config-engine`'s `m1_43_ephemeral_restart_loses_local_state_by_design`
(a follower is stopped, restarted under the same id on a brand new store, and must be
re-replicated to the leader's applied index) and the M1-09 cluster row in `config-testkit`. If
either crate stopped restarting nodes, the dev-dependency line should be deleted with it.

**M2 and beyond.** With `RocksStore` a restart reopens the same directory, so a log revert can
only follow a lost disk — which identity binding (ADR-0011) treats as an operator event that
must be handled explicitly, not absorbed by a relaxed assertion. Nothing in the M2+ production
path depends on the feature, and the scoping above is what keeps that true by construction
rather than by review.

### Note (2026-09-18): on-disk `format_version` (rv-schema F-030)

**What is actually stored.** Every value in the four column families is a `postcard` encoding:
`raft_log` holds `Entry<TypeConfig>`, `raft_meta` holds `Vote` / `LogId`, `kv` holds `Record`,
`state_meta` holds the identity, applied pointer and `Membership`. Four of those types belong to
OpenRaft, not to this repository. `CommandV1` (ADR-0007) is the envelope *inside* an entry's
payload, not the layout of a stored record — an earlier reading of ADR-0007 that made
`Command::encode()` the on-disk log format was wrong and has been corrected there and in
`config-core/src/command.rs`.

**Why that needs a marker.** An OpenRaft upgrade, or any serde field addition, removal, or
reorder in a persisted type, changes the byte layout with no change in this repository's own
source. `postcard` is not self-describing and tolerates trailing bytes, so the failure mode is
not a decode error but a *plausible wrong value* — a store that opens cleanly and replays
different state.

**Decision.** `state_meta/format_version` carries a bare little-endian `u32`, currently
`config_storage::FORMAT_VERSION = 1`. It is deliberately **not** `postcard`-encoded: a marker
written in the format it polices could not be read back across the change it exists to detect.

- First open of an empty directory stamps the marker in the *same* `set_sync(true)` `WriteBatch`
  that binds the identity, so a directory can never carry one without the other.
- Marker present and `== 1` → open proceeds.
- Marker present and `!= 1` → `StorageOpenError::UnsupportedFormat { found, supported }`.
- Marker absent from a directory that already holds data (identity, vote, a log entry, or the
  applied pointer) → the same refusal with `found: 0`: a pre-marker store whose format cannot be
  established. The check runs before any stored value is decoded.
- `run.rs` needs no change: `open_store` routes only `IdentityMismatch` to exit 2, and every
  other `StorageOpenError` to `Fatal::storage("storage_open_failed")` → exit 3.

**When to bump.** Raising the `openraft` dependency, or changing the serde shape of `Command`,
`CommandResponse`, `RaftNode`, `Record`, `ClusterIdentity` or anything else reachable from a
persisted value, is a **format bump**: increment `FORMAT_VERSION` in the same change. Shipping
new bytes under version 1 is the one failure this note exists to prevent.

**`EphemeralStore` is exempt.** It persists nothing — its log and state machine die with the
process — so there is no stored byte layout to version and no directory to refuse. The marker is
a `RocksStore` concept only.

**Rows.** Test plan M2-66 (first open stamps 1, reopen succeeds), M2-67 (stamped 2 → refusal),
M2-68 (marker deleted from a non-empty store → refusal with `found: 0`).

### Note (2026-09-18, M5): `dedup` CF allocated; snapshots directory under `data_dir`

The `dedup` column family named in §9.2 as reserved-but-not-created ("added through explicit later
schema migrations when... M5 optional deduplication begin[s]") is created starting at M5, per
ADR-0025, following the same format-bump template ADR-0021 established for `events`. `RocksStore`
now has five column families in the M5+ layout: `raft_log`, `raft_meta`, `kv`, `state_meta`,
`events` (ADR-0019/ADR-0021), `dedup` (ADR-0025).

Raft snapshot files (ADR-0022) and backup artifacts (ADR-0024) are **not** RocksDB column families;
they are separate files under `<data_dir>/snapshots/` — a plain OS directory sibling to the RocksDB
instance's own directory, not inside it, so RocksDB's own file management never iterates over or
interferes with snapshot files. `<data_dir>/snapshots/*.snap` holds published Raft snapshots
(retain-last-2, ADR-0022); backup artifacts are written to an operator-supplied destination
directory (`--out`/`Backup{dest_dir}`, ADR-0024), never under `<data_dir>` by default, since a
backup is meant to survive the loss of `data_dir` entirely.

### Note (2026-11-21, #186 part 2): the Raft log moves to its own database; format 4

**What changed.** Each node's store is now **two RocksDB databases** in one data directory.
Format 4 (`FORMAT_VERSION = 4`, `CURRENT_SCHEMA.format_version = 4`):

| Database | Where | Column families | Holds |
|---|---|---|---|
| log | `<data_dir>/raft-log/` | `raft_log`, `raft_meta` | entries, `vote`, `committed`, `last_purged`, and a copy of `identity` |
| state machine | `<data_dir>/` (root) | `kv`, `state_meta`, `events`, `dedup` | records, `last_applied`, membership, `identity`, `format_version`, the journal, dedup |

`snapshots/` stays where it was. Both databases are opened by one `RocksShared` and closed
together. `COLUMN_FAMILIES` names the root set and `LOG_COLUMN_FAMILIES` the log set.

**Why.** With one database, every apply batch's fsync also flushed every log entry. A second
database gives the log its own write-ahead log, so the two are synced on their own. That is
what PR-2 needs to make append asynchronous. PR-1 changes only the layout and keeps every
write synced as before.

**Write order across the two WALs (L = log database, S = state machine).**

| Operation | Order |
|---|---|
| `append` | one L batch, synced, before the flush callback |
| `save_vote`, `truncate` | one synced L write |
| `save_committed` | one unsynced L write, as before |
| `apply` | L first (the entries were appended), then the S batch |
| `purge` | S must cover the index (`last_applied` or the snapshot), then the L delete |
| install | L truncate, S install marker, S final batch, then the deferred purge as its own synced L write |
| snapshot build | a checkpoint of S only; L is not part of a snapshot |

The deferred purge used to ride in the install's final batch. It cannot share a batch across
two databases, so it now runs right after that batch and crosses `BeforePurge`/`AfterPurge` a
second time. A crash between the two leaves the snapshot installed and the old log entries in
place. That reopens cleanly with no redo, and the purge OpenRaft issues again is covered.

**Invariants.**

- **I1.** `last_applied` is never past `max(durable last log index, snapshot index)`. On
  openraft 0.9 this holds because `RaftCore` awaits the log flush before it applies. It is
  checked at open: a store that breaks it is refused as
  `Corrupt { what: "state_meta/last_applied" }`. OpenRaft would otherwise purge the log up to
  `last_applied` without a word and hide the fault. Applying entries that were never appended
  breaks I1. Even applied index zero requires a real log entry, purge pointer or covering
  snapshot with a log id. Real Raft never does that. Storage rows that used to do it now append first.
- **I2.** Purge sorts a request as covered, deferred or refused, the same way as before.
- **I3.** `committed` is never past the last log index.
- **I5.** `truncate` stays synced.
- **I6.** Both halves are present and carry the same identity, or both are fresh.
  - A state machine with state and no bound log is refused with `MissingLogStore`, naming the
    log directory.
  - A log with a vote, an entry or a pointer requires a bound state machine. An absent root
    or a root without an identity is refused with `MissingLogStore`, naming the root.
  - A log bound to another node is refused with `IdentityMismatch`, naming the log directory.
  - A first open binds the log first, with its own synced write, then writes the root batch. A
    crash between the two leaves a log holding only its identity. The next open treats that as
    fresh.
  - A fresh receiver can install a snapshot before its log records a vote or an entry. A
    covering snapshot therefore permits an identity-only log at reopen. Format 4 cannot
    distinguish that state from the same root paired with an older same-identity log copy.
    Recover both halves together.

**Old directories are refused, not migrated.** A directory with `raft_log` in its root
database was written before format 4 (formats 1 to 3). It is refused with
`PreSplitLayout { found, path }`, where `found` is its marker or 0 if the marker cannot be read.

- The check is one probe function, `refuse_pre_split_layout`. It has one call site, at the top
  of `RocksStore::open`, and it is read-only, so the directory is left exactly as it was found.
- The legacy column-family branches are deleted (`CfLayout::LegacyV1`/`LegacyV2`, the v1/v2
  family lists, and the read-only legacy log scan).
- The marker migration (`FormatAction::Migrate`) stays. A `--compat-schema 1` node writes the
  split layout stamped 1 (ADR-0030, M6-R22), so that is still a directory a current build
  upgrades, with the drained-log scan reading the log database.
- A snapshot file records the format it was built under, so a format-3 snapshot is refused by
  a format-4 restore. Offline export bypasses directory format and layout admission: it can
  read a pre-split directory and stamps the exported snapshot with format 4.

**Owner ruling, 2026-11-21: a standing rule for storage format changes.**

> From SBX's first real deploy on, every storage format change must migrate in place at
> startup and support a rolling upgrade, one node at a time.

Until that deploy, a format bump refuses old folders with a typed error and no migration.
Format 4 is such a bump. A further bump before the first deploy (for example a format 5 for
openraft 0.10) may do the same. The first bump after it must replace `refuse_pre_split_layout`
(and any refusal like it) with an in-place migration. Rolling upgrades also need cross-version snapshots: both
`validate_snapshot_file` and `restore_into_fresh_store` currently require exact format equality.

**Rows.** `crates/config-storage/tests/rocks.rs` (`i186_*`): the pre-split refusal, both
`MissingLogStore` directions, an existing unbound root beside a purged log, a stale first-open log beside
applied index zero, the foreign log, the identity-only retry, a crash after apply, and
I1 at open, together with the apply-without-append shape it refuses. M4-13, the M5 v2 row and
M5-127 became `PreSplitLayout` rows, and so did M5-71 in the testkit.
`crates/config-storage/tests/m5_snapshot.rs` covers crashes immediately before and after the deferred log purge.
