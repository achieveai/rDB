# ADR-0022: Snapshots and log purge

**Status:** Accepted
**Date:** 2026-09-18
**Spec:** §9.2, §12.1, §17, §19.7, §20 (Consensus and storage), §21 M5

## Context

M2/M3 latch `SnapshotPolicy::Never` with `max_in_snapshot_log_to_keep = u64::MAX` so that the log
never purges (ADR-0008 §4.5 of the openraft research note confirms this is a correct double latch:
`purge_end` saturates to `0`, so `calc_purge_upto` always returns `None`). M5 lifts that latch
(ADR-0001's M5 note). Disk otherwise grows forever, and `config-storage`'s snapshot trait methods
are still typed `Unsupported` stubs. Both problems are decided together here because openraft
couples them at the source: `finish_building_snapshot` schedules a purge the instant
`build_snapshot()` returns `Ok` (`openraft-research.md` §1.4), and a stubbed builder becomes
reachable on the ordinary startup path the moment anything has ever been purged (§4.3, trap T3) —
shipping purge without a real builder is not a partial feature, it is a node that cannot restart.

## Decision

### `SnapshotData` and the consistent view (research A1, A2; traps T3, T4)

- `config_storage::TypeConfig::SnapshotData = tokio::fs::File`. It satisfies `AsyncRead +
  AsyncWrite + AsyncSeek + Unpin` without enabling the `generic-snapshot-data` feature, keeps
  openraft's default chunked `install_snapshot` transport (ADR-0010's peer plane needs no new
  streaming code), and avoids trap T4 (`Cursor<Vec<u8>>` holds the whole snapshot in RAM, once per
  concurrently-replicating peer, on both leader and follower). `Cursor<Vec<u8>>` is not used even
  as an intermediate step; measuring the RAM-cost difference (research U4) is optional evidence,
  not a gate.
- The consistent point-in-time view is captured **inside `get_snapshot_builder()`**, not inside
  `build_snapshot()` (research A2, citing `$OR/src/core/sm/worker.rs:168-193`: openraft's own doc
  comment requires the builder to "hold a consistent view of the state machine that won't be
  affected by further writes," and `get_snapshot_builder()` runs on the sm worker task, serialized
  with `apply`, while the returned builder's `build_snapshot()` is spawned onto a separate task and
  races concurrent applies). `RocksStore::get_snapshot_builder` therefore takes a
  `rocksdb::Snapshot` (via the store's `Arc<DB>`) together with the `last_applied` log id and
  `StoredMembership` read at that same instant, and hands all three into the builder value. The
  builder's own `build_snapshot()` reads only through that captured `rocksdb::Snapshot`, never
  through live column-family handles — this is what makes the export race-free without a `Mutex`
  that would otherwise serialize every write for the duration of the build (trap T3, avoided by
  construction rather than by locking).

### File format: header, body, trailer

`build_snapshot()` writes a single file at `<data_dir>/snapshots/<id>.tmp`:

```text
SnapshotHeader {
    format_version: u32,      // = 2 (ADR-0021's on-disk marker, not a new value)
    command_schema: u32,      // = 2 (ADR-0007/0019 envelope version at export time)
    cluster_id: ClusterId,
    recovery_epoch: RecoveryEpoch,
    last_log_id: LogId,
    last_applied: LogId,
    membership: StoredMembership,
    cluster_revision: u64,
    compact_revision: u64,
    counts: { kv: u64, events: u64, dedup: u64 },
    bytes: u64,
}
<records: length-prefixed postcard, one per (cf, key, value) triple, cf ∈ {kv, state_meta, events,
  dedup}, in that fixed CF order and key order within each CF — deterministic layout, not required
  for correctness (a snapshot is not replayed through `apply` and is never hashed against
  `state_hash()`) but required so two builds of the same state produce byte-identical files, which
  is what makes the crash-injection rows in Verification able to compare files rather than re-parse
  them>
trailer: sha256 over header + records
```

`snapshot_id = "<last_log_index>-<term>-<unix_ms>"`. The research note's caveat governs the
`unix_ms` suffix: *"even when two snapshots are built with the same `last_log_id`, they still could
be different in bytes"* (`SnapshotMeta` doc, §1.2), so `snapshot_id` must be unique per build, not
derived from `last_log_id` alone — the timestamp component supplies that uniqueness without a
separate counter to persist.

### Publish ordering

1. Write records to `<id>.tmp`, computing the trailer hash while streaming.
2. `fsync` the file.
3. `rename(<id>.tmp, <id>.snap)` (same directory, atomic on the target filesystem).
4. `fsync` the containing `snapshots/` directory (the rename is not durable until the directory
   entry is synced).
5. One `set_sync(true)` `WriteBatch` writes `state_meta/current_snapshot = SnapshotMeta`.
6. Only after step 5 commits are older `.snap` files considered for removal, retaining the two most
   recent. This ordering — publish the new file and its pointer before touching the old ones — is
   what satisfies spec §12.1 ("the previous valid snapshot remains until publication succeeds") and
   invariant §19.7 (a purge, which depends on `current_snapshot`, can never race a publication that
   has not committed step 5).
- `build_snapshot()` returns `Ok` only after step 5 commits. Research trap T2: openraft has no
  `SnapshotFlushed` callback, so `finish_building_snapshot` schedules `Command::PurgeLog` the
  instant `build_snapshot()` returns `Ok` (research §1.4); returning early would let a purge run
  against a snapshot that is not yet durably current.
- Trap T5 / research §1.5: `build_snapshot` returning `Err` is fatal to `RaftCore` with no retry.
  Transient conditions (disk full mid-write, a cancelled task) are therefore absorbed and retried
  **inside** the builder — the same poison/guard discipline `rocks.rs` already uses (ADR-0008) — so
  only a genuinely unrecoverable condition (corruption, an invariant violated) reaches openraft as
  `Err`.

### Policy values changed together (research A3)

`SnapshotPolicy::LogsSinceLast(snapshot.logs_since_last, default 5,000)`,
`max_in_snapshot_log_to_keep = snapshot.logs_to_keep` (default 1,000), and `purge_batch_size`
(default, unchanged) move from their M2/M3 latch values in the **same change** that replaces the
snapshot stubs — not before, not after. Research §4.5 shows why: enabling the policy while leaving
`max_in_snapshot_log_to_keep = u64::MAX` builds snapshots that are never purged against (a silent
no-op); lowering `max_in_snapshot_log_to_keep` before `install_snapshot` works would let a leader
purge below what a lagging follower needs and then be unable to serve the snapshot it purged
against (trap T10: `get_current_snapshot() == None` when the leader needs one is a hard
`StorageError`, not a retry). Order of work, not just of config: build/install/get-current are
implemented and proven first, then the policy values are relaxed together.

### Startup repair path (research §4.3, trap T3)

`StorageHelper::get_initial_state()` builds a snapshot **synchronously on the startup path**
whenever `get_current_snapshot() == None && last_purged_log_id.is_some()` (research, citing
`helper.rs:128-143`). Consequence: from the moment `RocksStore::purge` is ever called, a real
`get_snapshot_builder` / `build_snapshot` must exist — a stub returning `Unsupported` turns a
restart into a permanent failure to start. This ADR replaces every snapshot stub in the same change
that enables purge (no intermediate state where purge is live and the builder is not). `RocksStore`
also implements `get_current_snapshot()` by reading `state_meta/current_snapshot` and opening the
referenced `.snap` file: after restart it returns the durably recorded snapshot without rebuilding
one, and the startup-repair path above is exercised only in the genuine gap case (purged, but the
recorded pointer is missing or unreadable).

### Install: two-phase with `install_in_progress` and redo

Follower side, driven by openraft's chunked `install_snapshot` transport (ADR-0010, unchanged):

1. `begin_receiving_snapshot()` returns a handle backed by `<data_dir>/snapshots/<id>.recv.tmp`;
   openraft writes chunks at their `offset` via `AsyncSeek` (research §2.4).
2. On the final chunk (`done`), `install_snapshot(meta, snapshot)` validates the header (see
   Validation matrix, below) against the file just received, then commits in two phases:
   - **Phase 1** — one `set_sync(true)` write of `state_meta/install_in_progress = <id>`. This is
     the durable marker that an install is underway.
   - **Phase 2** — drop and recreate the `kv`, `events`, and `dedup` column families, then stream
     the validated file's records into them via `WriteBatch`es of 4 MiB, unsynced until the final
     batch.
   - **Phase 3** — one `set_sync(true)` `WriteBatch` writes `last_applied`, `membership`,
     `cluster_revision`, `compact_revision`, `state_meta/current_snapshot`, and **deletes**
     `install_in_progress`, all together.
3. On open, if `install_in_progress` is present, the store redoes phases 2 and 3 from the durably
   retained `<id>.snap` (the file referenced by the marker is never deleted while the marker is
   present — the retain-last-2 cleanup in Publish ordering step 6 only ever removes files older
   than the *current* `current_snapshot`, and an in-progress install has not yet become the
   current snapshot). This makes phases 2–3 idempotent to re-run and closes the gap trap T1
   describes for the log side (see Purge contract, below) on the state-machine side: a crash
   between phase 1 and phase 3 always leaves a marker plus an intact source file to redo from,
   never a half-populated `kv`/`events`/`dedup` with no way back.

### Validation matrix

| Check | Failure |
|---|---|
| `header.cluster_id` == this node's bound identity | refuse, `IdentityMismatch` (ADR-0011's existing rule, applied to a received snapshot) |
| `header.recovery_epoch` == this node's bound epoch | refuse, `IdentityMismatch` |
| `header.format_version` == `FORMAT_VERSION` (ADR-0021) | refuse, `UnsupportedFormat { found, supported }` |
| `header.command_schema` <= this build's supported envelope version (ADR-0007/0019) | refuse, typed decode error |
| received byte count == `header.bytes` | refuse, `Corrupt { reason: "size_mismatch" }` |
| sha256(header + records) == trailer | refuse, `Corrupt { reason: "checksum_mismatch" }` |

Every refusal is a `StorageError` surfaced the same way any other store fault is (ADR-0008: node
marked `Fatal`, client operations return `FatalStorage`) — a snapshot that fails validation is
treated as a storage fault, not a retryable protocol error, because openraft has already committed
to this snapshot being the follower's new state (research §2.5: `install_snapshot` truncates
uncommitted logs and moves `committed` forward before the state-machine command even runs).

### Purge contract and the hard guard (traps T1, T12; research A4, U5)

- `RaftLogStorage::purge(log_id)` = `delete_range_cf(raft_log, ..=log_id)` (inclusive, per the
  trait contract) + `raft_meta/last_purged = log_id`, in one `set_sync(true)` `WriteBatch`. Kept
  O(range-delete) rather than per-key, because `Command::PurgeLog` is awaited inline on the
  RaftCore task (trap T12): a slow purge stalls elections, heartbeats, and applies cluster-wide.
- **Hard guard (trap T1, closing research open item U5):** `purge` refuses any `log_id` above the
  state machine's own durable `last_applied` — `crates/config-storage/src/rocks.rs`'s existing
  purge-upto-vs-last-applied check (ADR-0008) is promoted from a debug-only guard to an
  unconditional invariant, checked in every build. The trap it closes: `following_handler`'s
  install path pushes `Command::StateMachine` (the install) and `Command::PurgeLog` in the same
  engine output batch, but `PurgeLog` has no `Condition` (`command.rs:192`) while `StateMachine` is
  only forwarded to the sm worker's channel — so on the receiver, `purge()` can be awaited on the
  RaftCore task while `install_snapshot()` is still running on the worker task. Without the guard, a
  crash in that window leaves logs purged and no installed snapshot to fall back on, with no repair
  path (`get_initial_state`'s own repair, research §4.3, only handles the opposite order: installed
  ahead of purged). With the guard, that purge call fails closed instead — it observes
  `last_applied` has not yet advanced past the install and refuses, so the crash-injection row for
  U5 (kill the follower between `PurgeLog` completing and `install_snapshot` returning) can only
  ever observe "purge did not run yet," never "purge ran on unstable state."
- Followers build snapshots too (trap T14 — `following_handler` evaluates the same
  `SnapshotPolicy`), so purge and the same hard guard apply identically on every voter, not only the
  leader.
- `Raft::trigger_snapshot()` / `raft.trigger().purge_log()` (admin-triggered, ADR-0023's
  `TriggerSnapshot`) are the only manual entry points; trap T13 (only one build in flight per node,
  a manual trigger during an automatic build is silently a no-op) is accepted, not worked around —
  `TriggerSnapshot`'s audit line records the request, not a guaranteed new build.

### Invariant §19.7

"Snapshot/log purge cannot precede durable, validated snapshot publication" is met by construction
on both sides: leader/self-build purge is only ever scheduled after `build_snapshot()` returns
`Ok`, which itself only happens after the Publish-ordering sequence's step 5 commits (research
§1.4); follower/install purge is gated by the hard guard above rather than by ordering alone,
because openraft does not order the two operations for the receiver (research §2.5, trap T1).

## Consequences

- `tokio::fs::File` means a snapshot's cost is disk I/O and file-descriptor pressure, not RAM;
  `snapshot.max_open_files`-class tuning is future work if the default proves insufficient — no
  evidence exists yet either way (research U4 is explicitly optional).
- The retain-last-2 policy bounds `snapshots/` disk usage but means a node that falls behind by
  more than two snapshot generations cannot roll back to an older one; this is accepted because the
  purpose of a retained prior snapshot here is crash recovery during publication, not an operator
  rollback tool (that is backup/restore, ADR-0024).
- Promoting the purge-upto-last-applied guard to a hard invariant means a follower that is, for any
  reason, behind on applying an installed snapshot will reject a purge it would previously have
  silently accepted under the debug-only guard; this is the intended fail-closed behavior, not a
  regression.
- `command_schema` in the header couples snapshot compatibility to the envelope version (ADR-0007,
  ADR-0019); a full mixed-version compatibility story for snapshots across schema versions is
  ADR-0030 (M6) — this ADR only refuses a snapshot whose schema this build cannot decode, it does
  not attempt cross-version snapshot translation.

## Verification

- M5 rows for: `get_snapshot_builder` captures a `rocksdb::Snapshot` unaffected by concurrent
  `apply` (write during an in-flight build, assert the exported file matches the pre-write state);
  publish ordering survives crash injection at each of the five Publish-ordering steps, reopen
  always yields either the old or the new snapshot as current, never neither and never a torn file;
  `build_snapshot` at startup after a purge with no `current_snapshot` recorded; install validation
  matrix rejects each of the six failure rows; crash injection at each of install's three phases,
  reopen redoes from the retained `.snap` and reaches the same state as an uninterrupted install
  (byte-identical `kv`/`events`/`dedup` content); purge refuses to advance past `last_applied`
  (U5's crash-injection row, follower killed between `PurgeLog` completing and `install_snapshot`
  returning); retain-last-2 cleanup never removes a file before its `current_snapshot` batch
  commits; followers build snapshots under the same policy as the leader (T14).
- Full M5 acceptance mapping (learner catch-up via a purged leader forcing a snapshot install,
  crash during a real cross-node transfer) is covered jointly with ADR-0023; see its Verification
  section and research A11 (snapshot-vs-log replication is decided by purge position — install rows
  must first purge on the leader with tiny `logs_since_last`/`logs_to_keep`, then start a lagging
  follower).
- Test plan: `docs/testing/test-plan-m5.md`, M5 rows for snapshot build/publish, install, and purge
  (row IDs assigned when that plan is written).

## Notes

### 2026-09-18 — implementation deviations (dev-snapshot, M5)

Four deviations from the text above, all agreed with the M5 lead. The first is a behaviour
change; the rest are implementation choices that leave the contract intact.

**1. Purge classifies rather than refuses (ruling M5-R11).** The Decision's hard guard — "purge
never advances past `last_applied`, error otherwise" — kills every follower that installs a
snapshot. OpenRaft's `FollowingHandler::install_full_snapshot`
(`following_handler/mod.rs:322`) pushes `Command::PurgeLog` for the snapshot's last log id in
the *same* command batch as the install, and `RaftCore` runs them in order
(`raft_core.rs:1659`): the purge is issued while `last_applied` is still the old, lower value.
A refusal there is a fatal `StorageError`. `RocksLog::purge` therefore classifies:

1. `log_id.index <= max(last_applied.index, current_snapshot.covered_index())` — perform it.
2. otherwise, if a snapshot transfer is in flight — record it as `pending_purge` and return
   `Ok`, deferred.
3. otherwise — return `StorageError`, as the Decision intended.

"In flight" is concrete: a receive slot opened by `begin_receiving_snapshot` and not yet
resolved, or the `install_in_progress` marker. A merely stale `current_snapshot` is *not*
activity, so a genuine logic error still surfaces as an error rather than as a deferral that
never resolves. Rule 2 is always the case OpenRaft creates, because the snapshot file reaches
the node through `begin_receiving_snapshot` *before* `install_full_snapshot` is pushed, so the
slot is always open at the moment the follower purge is issued.

`pending_purge` is in-memory only. It is dropped, not executed, if the install aborts or the
process restarts, and it is executed only inside install's final synced batch, after
`current_snapshot` is durable. The next `purge` call re-evaluates from scratch. Rows:
`m5_24a_purge_during_install_is_deferred_then_executed`,
`m5_24b_aborted_install_drops_the_pending_purge`,
`m5_24c_restart_mid_defer_reports_the_lower_last_purged`, and
`m5_24_uncovered_purge_with_no_transfer_is_refused` as the rule-3 witness.

**2. No raw `state_meta` in the snapshot body.** The body carries only state-machine column
families. `last_applied`, membership, `cluster_revision`, `compact_revision` and the journal
statistics travel in the typed header and are rewritten by the installer. Copying `state_meta`
verbatim would carry the *sender's* `ClusterIdentity` into the receiver, defeating the identity
fencing ADR-0011 exists for. The header instead carries `cluster_id` and `recovery_epoch` as a
fence: a snapshot from another cluster or epoch is refused before anything is overwritten
(`m5_34_install_refuses_foreign_or_corrupt_snapshot`).

**3. `header.bytes` counts payload bytes, not file bytes.** The header cannot contain the size
of the file it is part of. `bytes` is the framed record payload written after the header, which
is what a progress estimate and the count cross-check need; file size is available from the
filesystem and is what `StorageMetrics::snapshot_size_bytes` reports.

**4. "Drop and recreate the column family" is a full-range `delete_range_cf`.** `rocksdb`'s
`drop_cf`/`create_cf` need `&mut DB`, and the database lives behind an `Arc` shared by every
`RocksLog`/`RocksSm` handle, so a real drop is not reachable without a redesign of the store's
ownership. Install instead deletes the whole key range of each data column family inside the
install batch. The observable contract is the same — the column family is empty before the
snapshot's records are written, and a crash mid-install is redone from the marker — at the cost
of leaving tombstones for compaction rather than freeing the SST files immediately.

Also: the post-rename directory fsync is best-effort on Windows, where a directory handle
cannot be opened for sync. The rename itself is atomic there, and the `current_snapshot`
metadata batch that follows is synced, so a snapshot is still never published before its file
is durable.

**Upstream property worth knowing.** OpenRaft builds snapshots in a task it spawns and never
joins (`core/sm/worker.rs:186`). `Raft::shutdown` can therefore return while a
`RaftSnapshotBuilder` — and through it the RocksDB handle — is still alive. Across a real
restart this is invisible (a new process, and the OS released the old lock); in-process
restart tests must retry the reopen rather than assume the lock is free.

### 2026-09-18 — review round 1 (critic-m5a), corrections

**5. `FORMAT_VERSION` is 3 at M5, not 2 (C5-12).** This ADR and the storage module docs were
written when the store had four data column families and `FORMAT_VERSION = 2`. M5 adds the
dedup column family, so the constant is now `3`, and a snapshot header carrying `2` is refused
by both `install_snapshot` and the offline restore as an unsupported format. The two earlier
values are kept as named constants (`FORMAT_VERSION_V1`, `FORMAT_VERSION_V2`) only so an
upgrade path can name what it is refusing; nothing in M5 accepts them. Any "format v2" wording
elsewhere in this document describes the M4 layout and should be read as historical.

**6. The receive slot is claimed for the whole install window, not taken on entry (C5-05).**
`install_received` reads the received path out of the slot but leaves the slot occupied until
the install marker is durable. Emptying it on entry would make `snapshot_activity()` false
across validate → rename → fsync → marker, and OpenRaft pushes `install_full_snapshot` and
`PurgeLog` back to back with no `Condition` between them (`following_handler/mod.rs:321-330`).
A purge arriving in that window would then take rule 3 above and be *refused* — a fatal
`StorageError` that shuts the node down — rather than deferred. Every failure path inside the
window therefore releases the slot, removes the file it currently occupies (the received name
before the rename, the published name after it) and drops any `pending_purge`, so an abandoned
transfer leaves no claim behind. Row:
`m5_24e_purge_inside_the_install_window_is_deferred`.

**7. Retention never unlinks the file an install marker points at (C5-06).** Snapshots are
retained in descending build order, and a snapshot received from a leader was necessarily built
before any snapshot this node builds afterwards — so a local build during an install can push
the incoming file past `retain_snapshots`. Pruning it would leave a marker naming a file that no
longer exists, and the next open's redo would fail as `Corrupt`: the node could not start at
all, from a situation in which nothing had actually been lost. `prune_snapshots` therefore
exempts `install_in_progress.snapshot_id` as well as the current snapshot. Row:
`m5_15a_prune_never_removes_the_in_progress_install_file`.

**8. Both kinds of partial file are swept at open (C5-13).** `.recv.tmp` is an incoming
transfer that will never be installed; `.tmp` is an export that died before its rename, so it
was never a publication. Neither is ever served and neither is visible to `list_snapshots`,
which only sees `.snap`, so leaving them accumulates disk that no retention policy counts. The
sweep runs while this process holds the RocksDB `LOCK`, so no live writer owns either file.
Row: `m5_11a_open_sweeps_partial_builds_and_receives`.

**9. The offline restore streams in bounded batches (C5-07).** A `WriteBatch` is held entirely
in memory, so one batch for a whole snapshot would put the whole state machine there — the cost
`SnapshotData = tokio::fs::File` exists to avoid. `restore_into_fresh_store` flushes every
`INSTALL_BATCH_RECORDS` records with `sync = false`, exactly as the live install does, and
writes `state_meta`/`identity` last in a single synced batch. Acceptance atomicity is preserved
by RocksDB's WAL ordering: the synced final batch makes the earlier unsynced ones durable, and a
restore that dies before it leaves a directory that no `open` will accept. Row:
`m5_82a_restore_streams_a_large_snapshot_in_bounded_batches`.

### 2026-09-19 — retired set travels in the snapshot header (finding C5B-18, ruling M5-R21)

`SnapshotHeader` gains a final field, `retired_nodes: BTreeSet<NodeId>`, and it is the one
`state_meta` value a snapshot carries. Deviation 2 above ("no raw `state_meta` in the snapshot
body") is unchanged and is the reason this had to be a *typed header field* rather than an
exception to `NON_DATA_CFS`: the body must keep carrying no `state_meta` bytes at all, because
that is what stops the sender's `ClusterIdentity` reaching the receiver.

The set had to cross because its absence was silent. A node caught up by `InstallSnapshot` gets
the membership the snapshot names, and — before this change — none of the retirements that
produced it: `install_received` re-read the *receiver's own* `state_meta/retired_nodes`, which on
a node that was down when `RetireNode` committed holds nothing. `is_retired` then answered
`false`, permanently, for an identity ADR-0023 says can never rejoin, and the peer plane and
`AddLearner` re-admitted it. A fence that is defence in depth may be redundant; it may not lapse.

**Union, never replacement.** The install reads the receiver's current set, extends it with the
header's, and writes the result — so an install can only widen the fence. A receiver that
retired an id the builder had not yet applied keeps it; a receiver missing an id the builder had
learns it. The write rides install's **final synced batch**, beside `last_applied`, `membership`,
`cluster_revision`, `current_snapshot` and the `install_in_progress` delete, so there is no
window in which the state machine is the snapshot's and the fence is not. `redo_install` shares
that function and the union is idempotent, so a crash-recovered install converges identically.

**Header compatibility.** `postcard` is positional, so the field is appended last and the struct
declaration is the format: append only, never insert or reorder. `format_version` cannot gate
the decode of the struct it lives in, so a header written before the field existed does not
report `UnsupportedFormat` — it runs out of bytes and `SnapshotReader::open` refuses it as
`Malformed`, before any column family is touched. That is a typed refusal rather than a panic,
and it costs nothing, because snapshots and `FORMAT_VERSION = 3` both first exist in M5
(correction 5 above): no build has ever written a pre-change header. `FORMAT_VERSION` is
therefore **not** bumped — it marks the RocksDB directory layout and drives ADR-0021's
migrations, and this change alters neither.

**`restore_into_fresh_store` deliberately does not carry the set forward.** A restore mints a new
`ClusterIdentity` and a new recovery epoch — a new node-id space — and writes no membership and
no `last_applied` (ADR-0024). Fencing the source cluster's ids in the cluster that replaces it
would refuse ids the new cluster may legitimately assign, which is a cost with no corresponding
safety gain: M5-R21's hazard is a node rejoining *the same* cluster.

Rows: `m5_133_retired_set_converges_through_snapshot_install` (both directions of the union,
after a purge of the entry that would otherwise teach it), `m5_103_install_restores_the_dedup_index_and_retired_set`
(the receiver-keeps-its-own half, pre-existing).
