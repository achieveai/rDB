# M2 test coverage (Tester agent, config-testkit)

Owned files: `crates/config-testkit/tests/m2_durability.rs`, `m2_crash.rs`, `m2_identity.rs`,
`m2_observability.rs`, plus shared additions in `tests/support/mod.rs`. Scope: rows that exercise
the `Cluster` harness (real multi-node Rocks clusters). Rows 14/15/30-40/46/60-64 were store-level
(`RocksStore`/`RaftLogStorage`/`RaftStateMachine`, no `Cluster`) — out of scope for this harness,
belonging to `config-storage`'s own test suite. The storage-level tester picked those up: see
`crates/config-storage/tests/rocks.rs` (M2-30..35/38..40/60/61), `crates/config-engine/tests/m2_rocks.rs`
(M2-14/15), and the new `crates/config-testkit/tests/m2_store_contract.rs` (M2-36/37, which do use
`Cluster` — the plan calls them out as "cluster-workload" store rows). M2-65 was IGNORED pending a
`FaultAction::Delay` that did not exist yet; the same tester added it and un-ignored the row.

## Coverage table

| Row | Test | File | Status |
|---|---|---|---|
| M2-01 | m2_01_restart_follower_preserves_state | m2_durability.rs | PASS |
| M2-02 | m2_02_restart_leader_preserves_state | m2_durability.rs | PASS |
| M2-03 | m2_03_restart_each_node_in_turn | m2_durability.rs | PASS |
| M2-04 | m2_04_revision_monotonic_across_restart | m2_durability.rs | PASS |
| M2-05 | m2_05_acknowledged_mutation_survives_immediate_restart | m2_durability.rs | PASS |
| M2-06 | m2_06_cold_cluster_restart_does_not_reform | m2_durability.rs | PASS |
| M2-07 | m2_07_data_dir_reuse_ten_cycles | m2_durability.rs | PASS |
| M2-08 | m2_08_second_formation_after_restart_rejected | m2_durability.rs | PASS |
| M2-09 | m2_09_restart_with_stopped_peer | m2_durability.rs | PASS |
| M2-10 | m2_10_ephemeral_vs_rocks_divergence_documented | m2_durability.rs | PASS |
| M2-11 | m2_11_crash_after_commit_before_apply_replays | m2_durability.rs | PASS |
| M2-12 | m2_12_replay_allocates_no_duplicate_revision | m2_durability.rs | PASS |
| M2-13 | m2_13_committed_is_persisted | m2_durability.rs | PASS |
| M2-14 | m2_engine_02_read_committed_drives_replay_window | config-engine/tests/m2_rocks.rs | PASS |
| M2-15 | m2_engine_03_replay_chunked_over_64_entries | config-engine/tests/m2_rocks.rs | PASS |
| M2-16 | m2_16_apply_batch_is_one_atomic_crossing | m2_durability.rs | PASS |
| M2-17 | m2_17_crash_during_replay_is_idempotent | m2_durability.rs | PASS (see fix below) |
| M2-18 | m2_18_follower_committed_unapplied_replay | m2_durability.rs | PASS |
| M2-19 | m2_19_crash_before_vote_sync | m2_crash.rs | PASS |
| M2-20 | m2_20_crash_after_vote_sync | m2_crash.rs | PASS |
| M2-21 | m2_21_crash_before_log_append | m2_crash.rs | PASS |
| M2-22 | m2_22_crash_after_log_append | m2_crash.rs | PASS |
| M2-23 | m2_23_crash_before_log_flush | m2_crash.rs | PASS |
| M2-24 | m2_24_crash_after_log_flush | m2_crash.rs | PASS |
| M2-25 | m2_25_crash_before_state_batch | m2_crash.rs | PASS |
| M2-26 | m2_26_crash_after_state_batch | m2_crash.rs | PASS |
| M2-27 | m2_27_crash_boundary_table_is_exhaustive | m2_crash.rs | PASS |
| M2-28 | m2_28_repeated_crash_cycles_no_vote_regression | m2_crash.rs | PASS |
| M2-29 | m2_29_crash_matrix_loses_no_acknowledged_mutation | m2_crash.rs | PASS |
| M2-30 | m2_storage_12_append_refuses_to_leave_a_hole | config-storage/tests/rocks.rs | PASS |
| M2-31 | m2_storage_21_append_entries_readable_before_flush_callback | config-storage/tests/rocks.rs | PASS |
| M2-32 | m2_storage_22_flush_callback_mirrors_sync_result_never_ok_on_fail | config-storage/tests/rocks.rs | PASS |
| M2-33 | m2_storage_08_truncate_and_purge_persist_across_reopen | config-storage/tests/rocks.rs | PASS |
| M2-34 | m2_storage_08_truncate_and_purge_persist_across_reopen | config-storage/tests/rocks.rs | PASS (same test as M2-33: append-after-truncate is asserted in the same reopen cycle) |
| M2-35 | m2_storage_08_truncate_and_purge_persist_across_reopen | config-storage/tests/rocks.rs | PASS (same test: purge above `last_applied` asserted mid-file) |
| M2-36 | m2_36_purge_is_never_invoked | config-testkit/tests/m2_store_contract.rs | PASS — post-review fix round (audit F4): both rows now call `cluster.shutdown().await` at the end; without it the cluster was dropped rather than stopped. |
| M2-37 | m2_37_snapshot_never_built | config-testkit/tests/m2_store_contract.rs | PASS — same fix as M2-36. |
| M2-38 | m2_storage_19_snapshots_are_unsupported_but_never_panic | config-storage/tests/rocks.rs | PASS |
| M2-39 | m2_storage_23_log_state_never_under_reports_after_crash_and_reopen | config-storage/tests/rocks.rs | PASS |
| M2-40 | m2_storage_26_log_cf_keys_contiguous_after_crash_at_every_boundary | config-storage/tests/rocks.rs | PASS |
| M2-41 | m2_41_identity_written_on_first_open | m2_identity.rs | PASS |
| M2-42 | m2_42_wrong_cluster_id_blocks_startup | m2_identity.rs | PASS |
| M2-43 | m2_43_wrong_node_id_blocks_startup | m2_identity.rs | PASS |
| M2-44 | m2_44_wrong_recovery_epoch_blocks_startup | m2_identity.rs | PASS |
| M2-45 | m2_45_cloned_data_dir_rejected | m2_identity.rs | PASS |
| M2-46 | identity_mismatch_is_not_a_panic | — | Covered implicitly by M2-42..45 all asserting typed errors, not panics; no dedicated row (aggregate row per test plan, "—" setup column) |
| M2-47 | m2_47_identity_survives_crash_at_every_boundary | m2_identity.rs | PASS |
| M2-48 | m2_48_formation_identity_must_match_manifest | m2_identity.rs | PASS |
| M2-49 | m2_49_rocks_reports_persistent | m2_observability.rs | PASS |
| M2-50 | m2_50_ephemeral_never_persistent | m2_observability.rs | PASS |
| M2-51 | m2_51_no_sync_downgrades_capability | m2_observability.rs | PASS |
| M2-52 | m2_52_capabilities_identical_on_all_nodes_and_health | m2_observability.rs | PASS |
| M2-53 | m2_53_fsync_count_per_mutation | m2_observability.rs | PASS |
| M2-54 | m2_54_vote_fsync_per_term_change | m2_observability.rs | PASS |
| M2-55 | m2_55_zero_sync_is_impossible_in_default_mode | m2_observability.rs | PASS |
| M2-56 | m2_56_io_error_on_append_is_fatal_not_panic | m2_observability.rs | PASS |
| M2-57 | m2_57_enospc_on_state_batch_is_fatal | m2_observability.rs | PASS |
| M2-58 | m2_58_fatal_node_stops_acknowledging | m2_observability.rs | PASS |
| M2-59 | m2_59_fatal_node_does_not_continue_optimistically | m2_observability.rs | PASS |
| M2-60 | m2_storage_24_corrupt_log_entry_detected_on_open | config-storage/tests/rocks.rs | PASS |
| M2-61 | m2_storage_25_corrupt_state_meta_last_applied_detected_on_open | config-storage/tests/rocks.rs | PASS |
| M2-62 | m2_storage_04_missing_or_unexpected_column_family_is_typed | config-storage/tests/rocks.rs | PASS (pre-existing test, not added by this session; found while updating this table) |
| M2-63 | m2_storage_04_missing_or_unexpected_column_family_is_typed | config-storage/tests/rocks.rs | PASS (same test as M2-62: `events` CF asserted in the same file) |
| M2-64 | m2_storage_05_locked_directory_fails_fast_and_typed | config-storage/tests/rocks.rs | PASS for the same-process half; the row's "from a second process" half is not exercised — no test in this codebase spawns a second process against the same directory |
| M2-65 | m2_65_blocking_rocksdb_does_not_starve_raft | m2_observability.rs | PASS — `FaultAction::Delay(Duration)` was added to `config_storage::fault` (plus `RocksShared`/`EphemeralStore` boundary() arms and `ScriptedInjector::delay_on_nth`); row un-ignored |

**Totals (this harness's scope):** 45 tests passing, 0 ignored, 0 failing. Of the 18 rows this
harness's own files don't cover (M2-14, M2-15, M2-30..40, M2-60..64), all 18 are now PASS: 8 of
them (M2-30/33/34/35/38/62/63/64) were already covered by `config-storage`'s pre-existing
`rocks.rs` tests, unrelated to this session's additions; the other 10 (M2-14/15/31/32/36/37/39/40/
60/61) are PASS via the storage-level tester's additions in `config-storage`, `config-engine`, and
the new `m2_store_contract.rs`. M2-46 remains an intentional non-dedicated aggregate row (see its
own line above). M2-64's "from a second process" half is not exercised by any test in this
codebase — only the same-process locking half is proven.

## M2-17 root cause and fix (this session)

`m2_17_crash_during_replay_is_idempotent` failed: after the first injected crash poisoned the
store, the test's own doc comment said "arm again before the first restart" so the *replay's own*
apply would crash too — but the actual `scripts[&target].crash_on_nth(Boundary::BeforeStateBatch, 1)`
call was missing from the code between that comment and `cluster.restart(target).await`. The first
crash's one-shot arm had already self-disarmed itself (by design — `ScriptedInjector::before()`
zeroes `at[i]` the moment it fires), so the second restart's replay legitimately got `Proceed` at
`BeforeStateBatch` and completed cleanly, producing `Ok(())` instead of the expected
`Err(NodeStartError::Engine(_))`.

Confirmed via a fresh JSONL run: between `re-apply 1 log entries: [7, 8)` and
`applied command entry` (log_index 7, outcome applied) there is no crash/fault log line at all —
`RocksStore::boundary()` only logs on `Fail`/`Crash`, never on a silent `Proceed`, so the absence of
a log line does not by itself prove the boundary wasn't crossed; the missing `crash_on_nth` call is
what proves it.

Fix: added the missing `scripts[&target].crash_on_nth(Boundary::BeforeStateBatch, 1);` line right
before the `let replay_restart = cluster.restart(target).await;` call in `m2_durability.rs` (around
line 854). Not a storage or harness defect — ruled out `Boundary::index()` collisions
(`crates/config-storage/src/fault.rs:48-59`: `BeforeStateBatch=6`, `AfterStateBatch=7`, no overlap)
and the injector's Arc-persistence-across-restarts (confirmed single `Arc::clone(&slot.faults)` in
`cluster.rs`'s `try_start_node`) before finding the missing call.

## Reusable findings from this session

- `Cluster::leader_now()`/`leader()`/`wait_for_leader()` break ties by **lowest node id** among
  nodes reporting `Leader` role. After a forced election (`isolate`/`heal`), an isolated ex-leader
  keeps reporting `Leader` forever (no check-quorum/lease step-down in this openraft build) — if it
  has the lowest id, `leader_now()` returns the *stale* leader, not the real one. Use
  `Cluster::leaders_now()` (all current leaders) instead, or the new `settled_leader()` helper in
  `tests/support/mod.rs` (polls until exactly one node reports Leader).
- Isolating a node that is *currently the leader* never makes it cross a vote boundary by itself in
  this openraft build — it just sits isolated, still believing itself leader at the old term. To
  force a step-down: isolate, wait for a replacement leader via `leaders_now()`, heal, then wait for
  the old leader's `metrics(id).current_leader == Some(new_leader)` before re-isolating.
  `wait_converged()` is NOT a valid "node learned of the new leader" oracle — it can pass with no
  writes in flight without the node ever processing an RPC from the new leader.
- `RocksStore::boundary()` logs nothing on a silent `Proceed` — only `Fail`/`Crash` produce a log
  line (`"injected storage fault"` / `"injected storage crash; store is now poisoned"`).
  `after_boundary()` always logs `"storage boundary"` regardless of outcome. Do not infer "boundary
  never crossed" from the absence of a log line.
- `config_testkit::logs::lines_for_current_test()` already filters by
  `testRun == config_log::testing::test_run_id()`; no test-side change needed for that requirement.
