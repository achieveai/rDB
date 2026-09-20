# dev-fence — M5-R21 (finding C5B-18) + advisories A1/A2/A3

> REMINDER: tick items in this checklist as they complete. `[x]` done, `[-]` in progress, `[ ]` not started.

## Finding
`retired_nodes` lives in `CF_STATE_META` (`KEY_RETIRED_NODES`), excluded from snapshots by
`NON_DATA_CFS` (snapshot.rs:72). Install (`rocks.rs` install_received ~3656) re-reads the
RECEIVER's own state_meta. A node that was down when the leader retired NodeId(9), then took
an InstallSnapshot, never learns the retirement → `is_retired(9)` false forever (ADR-0023 says
a retired identity cannot rejoin).

## Design decisions (evidence-backed)
- **Header versioning.** `SnapshotHeader.format_version` mirrors `crate::FORMAT_VERSION` (=3 at
  M5, ADR-0022 note 5). It is *inside* the postcard struct, so it cannot gate the decode of the
  struct that carries it. Appending `retired_nodes` at the END means an old header decodes as
  `postcard` EOF → `SnapshotFileError::Malformed` (snapshot.rs:668 maps every header decode
  error), a typed refusal, never a panic. NO FORMAT_VERSION bump: the constant governs the
  RocksDB directory layout and its migrations (rocks.rs open/upgrade, EXCLUDED scope), and
  snapshots plus FORMAT_VERSION 3 both first exist in M5 — no shipped build ever wrote a
  pre-change header, so there is no M4→M5 upgrade path to break.
- **Union site.** Inside `apply_snapshot_records`' final synced `last` batch, next to
  `last_applied`/`membership`/`cluster_revision` and the `install_in_progress` delete. That is
  the same batch M5-R11's deferred purge rides, so a crash mid-install cannot leave the fence
  lapsed. `redo_install` shares that function → redo gets it free, and union is idempotent.
- **Build site.** `CapturedView.retired_nodes` cloned from `sm.kv.retired_nodes()` under the
  same `sm` lock that already supplies `cluster_revision`/`compact_revision` and the checkpoint.
- **Offline half.** `snapshot::export_snapshot` reads `state_meta/retired_nodes` through the
  existing `offline_meta` + `offline_keys` mirror. `offline_export_matches_the_online_build`
  (tests/rocks.rs) is the cross-check that keeps the two halves equal.
- **`restore_into_fresh_store` deliberately NOT changed.** Restore mints a new `ClusterIdentity`
  and a new recovery epoch — a new node-id space — and writes no membership/last_applied.
  Carrying the source cluster's retired ids across a recovery epoch could fence an id the new
  cluster legitimately assigns. M5-R21 scopes the union to "the receiving node's" set on install.
  Recorded in the ADR-0022 note.

## Checklist
- [x] Read architecture brief rulings M5-R20/M5-R21 + ledger tail
- [x] Read snapshot.rs (header, export_snapshot, restore_into_fresh_store)
- [x] Read rocks.rs build (capture_view/export_into_file) + install (apply_snapshot_records/install_received)
- [x] Read m5_dedup.rs m5_103 (test template) + test-plan-m5 §3.4
- [x] Confirm advisories A1/A2/A3 against the real code
- [x] `SnapshotHeader.retired_nodes: BTreeSet<NodeId>` appended last + docs
- [x] snapshot.rs module doc: retired_nodes is the named exception to "no state_meta in the body"
- [x] `export_snapshot` (offline) fills it from `state_meta/retired_nodes`
- [x] `CapturedView.retired_nodes` + `export_into_file` fills the header
- [x] `apply_snapshot_records` unions header set into `KEY_RETIRED_NODES` in the final synced batch
- [x] `InstalledState.retired` returned; `install_received` uses it instead of re-reading
- [x] Test `m5_133_retired_set_converges_through_snapshot_install` beside m5_103
- [x] Plan row M5-133 in docs/testing/test-plan-m5.md §3.4
- [x] ADR-0022 dated note
- [x] ADR-0023 dated note
- [x] A1 state.rs:133 doc `global_cap` → `trim`
- [x] A2 ADR-0025 Verification "at or below" → "below"
- [x] A3 ADR-0026 `retcd_rocks_mem_bytes` labels gain `kind`
- [x] cargo test -p config-storage (private target)
- [x] clippy -p config-storage --tests
- [x] rustfmt --check on every touched file
- [x] cargo test -p config-engine --test m5_snapshot
- [x] `grep -rni mutat crates/config-storage/src` clean

## Evidence
`cargo test -p config-storage --no-fail-fast -- --test-threads=2` (private target, CARGO_INCREMENTAL=0):
lib 10 / ephemeral 10 / m4_journal 27 / m5_dedup 9 / m5_snapshot 23 / rocks 29 / doctests 0 —
every line `test result: ok. N passed; 0 failed`.
`cargo clippy -p config-storage --tests` — Finished, no diagnostics.
`rustfmt --edition 2021 --check` on snapshot.rs, rocks.rs, m5_dedup.rs, state.rs — clean.
`cargo test -p config-engine --test m5_snapshot` — 3 passed; 0 failed.
`cargo check --workspace --tests` — Finished; only a pre-existing config-server test-binary
dead-code warning (`methods state and failures are never used`), untouched by this change.

## Mutation log
- MUTATION OPEN  crates/config-storage/src/rocks.rs:3453 (union line commented out) 2026-09-19
- MUTATION CLOSED crates/config-storage/src/rocks.rs:3453 2026-09-19 — m5_133 failed as designed
  ("the install must carry the leader's retirement onto a node that never applied it: {NodeId(7)}"),
  m5_103 stayed green (proves the two rows test different halves).
- Final `grep -rni mutat crates/config-storage/src` → no hits.
