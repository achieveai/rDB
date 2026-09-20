# dev-snapshot (M5 wave 1) — research + decisions

## Verified against the pinned openraft source (not memory)
`$OR = C:/Users/gautamb/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/openraft-0.9.25`

| Claim | Source | Verdict |
|---|---|---|
| `Command::PurgeLog` has no `Condition` | `$OR/src/engine/command.rs:193` | confirmed |
| `Command::StateMachine` is only forwarded to the sm worker channel, never awaited | `$OR/src/core/raft_core.rs:1728-1734` | confirmed |
| `run_command(PurgeLog)` awaits `log_store.purge(upto)` and `?`-propagates -> Fatal | `$OR/src/core/raft_core.rs:1659-1662` | confirmed |
| follower install emits `Command::StateMachine(install)` then `purge_log()` with `state.purge_upto = snap_last_log_id` | `$OR/src/engine/handler/following_handler/mod.rs:322-327` | confirmed |
| `LogHandler::purge_log()` pushes `PurgeLog{upto}` whenever `purge_upto > last_purged` — no clamping to applied | `$OR/src/engine/handler/log_handler/mod.rs:32-49` | confirmed |
| `calc_purge_upto` (self-build path only) clamps to the snapshot: `purge_end = snapshot.last_log_id.next_index() - max_keep` | `log_handler/mod.rs:78-113` | confirmed |

## CONTRADICTION C1 (escalated to lead) — ADR-0022 hard purge guard vs openraft 0.9.25 install path
ADR-0022 "Purge contract and the hard guard" and the m5-interfaces hard rule require
`purge` to return a `StorageError` whenever `upto.index > min(current_snapshot.last_log_id, last_applied)`.

On the **follower install path** that condition is true by construction and essentially always:
`upto = snapshot.last_log_id`, which is far above the lagging follower's `last_applied`, and the
install that would raise `last_applied` is queued on a *different* task. A `StorageError` from
`purge` is `?`-propagated out of RaftCore and makes the node **Fatal**. So the guard as literally
specified turns every routine snapshot install into a dead follower — M5-29..M5-46 could not pass.

### Resolution implemented (deviation, dated note added to ADR-0022)
`purge` classifies rather than uniformly erroring:
* `upto <= max(last_applied, current_snapshot.last_log_id)` -> **purge** (range delete + synced
  `last_purged`). This is the leader/self-build path, always provable because `build_snapshot`
  returns `Ok` only after the `current_snapshot` batch commits.
* otherwise, **and the store has snapshot activity** (a receive slot is occupied, an
  `install_in_progress` marker exists, or a `current_snapshot` older than `upto` exists) ->
  **defer**: delete nothing, persist nothing, remember `pending_purge = max(pending, upto)`,
  `warn purge_deferred{..}`, return `Ok(())`. The deferred deletion is executed by
  `install_snapshot`'s final synced batch (where durability is proven) and re-attempted on the
  next `purge` call.
* otherwise (no snapshot activity at all — a pure configuration/logic error, test-plan M5-24) ->
  **`StorageError` + `error purge_refused{upto, last_applied}`**, exactly as specified.

Why deferring is *stronger* than the specified guard, not weaker:
* nothing above provably-durable state is ever deleted (§19.7 holds in its sharpest form);
* persisted `last_purged` only ever advances to a provable value, so `get_log_state()` after a
  restart reports truth and `last_purged <= last_applied <= last_log_id` holds;
* openraft's in-memory `purged` pointer may run ahead of disk — that only makes this node *less*
  willing to serve old entries, never more, and a restart re-reads the lower (safer) value;
* T12 is respected: purge never blocks the RaftCore task waiting for the sm worker.

## Other design decisions
* **D1 consistent view (A2/T3):** captured in `get_snapshot_builder()` under the `sm` mutex —
  which is the mutex `apply` holds across its whole synced batch, so the capture is atomic w.r.t.
  apply. The view itself is a `rocksdb::checkpoint::Checkpoint` into
  `<data_dir>/snapshots/build-<id>/` (hard-linked, O(#SST)), because `rocksdb::Snapshot` borrows
  `&DB` and cannot be stored in the builder without `unsafe` (crate is `#![forbid(unsafe_code)]`).
  `build_snapshot` then exports from the checkpoint holding **no** lock -> M5-02 passes.
* **D2 CF-generic record format:** the header carries `cfs: Vec<String>` (export order) and
  `counts: BTreeMap<String,u64>`; records carry a `cf` index into `cfs`. The exported set is
  "every CF except default/raft_log/raft_meta/state_meta", discovered by `DB::list_cf` on the
  checkpoint. dev-dedup's `dedup` CF is therefore exported and installed with **no format change**.
* **D3 `state_meta` is NOT in the record stream** (deviation from D5.1/ADR-0022). Everything an
  install or a restore needs from it is in the header (`cluster_revision`, `compact_revision`,
  `last_applied`, `membership`), and streaming raw `state_meta` would carry the *source node's*
  `identity` into the file — the exact ADR-0011 fencing hazard, and something install must never
  write. Dated note added to ADR-0022.
* **D4 `header.bytes`** = total key+value payload bytes of the record section (not the framed file
  size), so it is computable in the cheap counting pre-pass that also fills `counts`. Framing and
  truncation are caught by the sha256 trailer. Dated note added to ADR-0022.
* **D5 directory fsync** is best-effort on Windows (`std::fs::File::open` on a directory fails
  without `FILE_FLAG_BACKUP_SEMANTICS`). The boundary is still crossed so the ordering rows hold;
  the limitation is documented rather than hidden.
* **D6 `Boundary::ALL` 9 -> 17** (M5-R2: purge gets `BeforePurge`/`AfterPurge` and stops crossing
  `BeforeLogFlush`, overriding test-plan OQ-41/M5-23).

## 2026-09-18 — decisions taken after ruling M5-R11, and the shipped test map

* **D7 purge classifies (M5-R11).** `covered` = `max(last_applied.index,
  current_snapshot.covered_index())`. Above it: deferred if a receive slot is open *or* the
  `install_in_progress` marker is present, refused otherwise. `pending_purge` is a
  `Mutex<Option<LogId>>` on `RocksShared`, never persisted, executed only in install's final
  synced batch. Counters: `purges`, `purge_deferrals`, `purge_refusals`, `purged_index`.
* **D8 retention lives on `RocksShared`, not `RocksOptions`.** `RocksOptions` is constructed
  exhaustively in `config-testkit/src/cluster.rs` and `config-server/src/run.rs`, neither of which
  I own, so a new field there would have broken two crates. `RocksStore::configure_snapshots`
  (called by `ConfigNode::start`) writes `retain_snapshots: AtomicUsize` instead.
* **D9 "drop the CF" is a full-range `delete_range_cf`.** `drop_cf`/`create_cf` need `&mut DB`;
  the DB lives behind an `Arc` shared by every handle. Same observable contract, tombstones
  instead of freed SSTs. Dated note in ADR-0022.
* **D10 `build_snapshot` retries up to `BUILD_ATTEMPTS = 3`.** `get_snapshot_builder` has no
  `Result`, so a failed checkpoint cannot be reported there; the builder stores
  `Option<CapturedView>` and recaptures on retry. This is also what makes M5-03 true (a transient
  failure is absorbed, not fatal).
* **D11 ephemeral forces `SnapshotConfig::DISABLED`** in `ConfigNode::start` via
  `NodeConfig::effective_snapshot`, because `EphemeralStore` cannot build and a `build_snapshot`
  `Err` is fatal.
* **Upstream trap found while testing:** openraft spawns the snapshot build in a task it never
  joins (`core/sm/worker.rs:186`), so `Raft::shutdown` can return while a builder still holds the
  RocksDB handle. In-process restart tests must retry the reopen (see
  `reopen_store` in `crates/config-engine/tests/m5_snapshot.rs`). Noted in ADR-0022.

### Shipped tests

`crates/config-storage/tests/m5_snapshot.rs` (19) and `crates/config-engine/tests/m5_snapshot.rs`
(3). Test-plan row ids filled in `docs/testing/test-plan-m5.md`; new rows M5-21a, M5-23a,
M5-24a..d and M5-48a added there for the M5-R11 behaviour and the boundary-coverage row.

### Mutation checks run (all reverted)

1. `rocks.rs` purge cover test forced false -> M5-13, M5-24, M5-24a, M5-24b, M5-24c fail (5 rows).
2. `validate_snapshot_file`'s `verify_to_end` removed -> M5-24b and M5-34 fail. The first run of
   this mutation only killed M5-24b, which exposed that M5-34 was asserting on the in-memory
   mirror (only rebuilt by the final batch) rather than on durable state; the row now reopens the
   store, and the mutation kills it.
3. Install marker never persisted -> M5-31 fails (the redo never happens).

## 2026-09-18 — fix round 1 (critic-m5a)

Six findings, five of them code. The decisions worth remembering:

* **C5-05, the install window.** `install_received` now *claims* the receive slot instead of
  taking it: `s.recv().clone()` on entry, cleared only once the marker is durable, and cleared by
  `abort` on every failure path in between. The window that mattered is validate -> rename ->
  fsync -> marker; `snapshot_activity()` was false across it, and openraft pushes
  `install_full_snapshot` and `PurgeLog` back to back with no `Condition`
  (`following_handler/mod.rs:321-330`), so a purge landing there was *refused* — fatal — instead
  of deferred. Every early return in that window had to become an explicit `abort` + return,
  because a `?` would have left a claimed slot and an orphaned published file behind.
* **C5-06, retention vs. the marker.** `prune_snapshots` now exempts
  `install_in_progress.snapshot_id` as well as `current_id`. Snapshots sort descending by
  `(created_unix_ms, index, term, id)`, and a leader's snapshot is *always* older than a build
  this node makes after receiving it, so retention reaches the incoming file first. Pruning it
  leaves a marker naming a missing file, and the next open's redo fails `Corrupt` — the node
  cannot start, from a situation where nothing was actually lost.
* **C5-07, restore batching.** `INSTALL_BATCH_RECORDS` moved out of `rocks.rs` into
  `snapshot.rs` (now `pub(crate)`) and `restore_into_fresh_store` chunks on it with
  `sync = false`, keeping `state_meta`/`identity` in the final synced batch. Acceptance
  atomicity survives because RocksDB's WAL ordering makes the earlier unsynced batches durable
  when the final one syncs.
* **C5-13 / C5-14.** `sweep_partial_receives` also removes `.tmp` exports;
  `get_snapshot_builder` logs `snapshot_view_unavailable` with a reason before returning `None`
  (the openraft-facing signature is unchanged — it cannot return an error).

### Test-construction decisions

* **`PauseAt`** (new fixture): a `sync_channel` handshake that blocks the first crossing of one
  boundary until the test releases it. Not `FaultAction::Delay`, because a duration is a sleep.
  Safe because `RocksShared::run` consults every boundary inside `spawn_blocking`, so it parks a
  blocking-pool thread, not a runtime worker; and `install_received` holds no `sm`/`recv` lock at
  `BeforeInstallMarker`, so the concurrent purge is not blocked behind it.
* **M5-15a uses a *failed* install, not a paused one.** Windows refuses to unlink a file the
  installing thread still has open, so a concurrent prune would have "passed" for the wrong
  reason. Failing at `BeforeInstallFinalBatch` leaves the marker durable and the file closed,
  which is the state the prune actually has to respect.
* **M5-15a seeds the destination to index 9** against an incoming snapshot at index 6, so the
  local build outranks it on the index tiebreak even inside the same millisecond — no sleep. A
  `sorts_newer` helper asserts the ordering rather than assuming it, so the row cannot go vacuous.
* **M5-82a uses 8500 keys** because restore only writes `kv` and `dedup` (`events` is dropped),
  so `kv` alone must exceed 2 x 4096. Appended and applied as two bulk calls, not 8500.

### Mutation checks (fix round)

4. `install_received` reverted to `s.recv().take()` -> only
   `m5_24e_purge_inside_the_install_window_is_deferred` fails.
5. `prune_snapshots`' `in_progress` exemption removed -> only
   `m5_15a_prune_never_removes_the_in_progress_install_file` fails.

Both mutation checks were run and killed exactly their own row (22 passed / 1 failed each time).

Two operational notes from this round:

* **m5_24e asserts nothing until the pause is released.** A panic between `reached()` and
  `release()` leaves the injector parked on a blocking-pool thread, and dropping a tokio runtime
  waits for blocking tasks — so the mutation would have surfaced as a *hang* instead of a
  failure. The row now captures the purge result and the three counters, releases, joins, and
  only then asserts.
* **The workspace `target/` lock is heavily contended** when several agents run cargo at once
  (one run waited >15 minutes without ever getting the lock). Mutation checks were run with a
  private `CARGO_TARGET_DIR` in the session scratchpad instead; the first build there costs a
  full rocksdb compile (~4.5 GB, ~12 min) but every later run is uncontended.
