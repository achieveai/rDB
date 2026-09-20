# tester-m5a — M5 test rows: snapshots, purge, membership, fencing, backup/restore

## Files delivered (all new; no `src/` edits)

1. `crates/config-testkit/tests/m5_membership_cluster.rs` — M5-54, 56, 57, 58, 60, 61, 62, 63,
   64, 70, 72, 73 (12 rows). All pass individually and in the one full-file run performed.
2. `crates/config-testkit/tests/m5_snapshot_cluster.rs` — M5-05, 11, 16, 18 (4 rows). Compiles
   clean in isolation; run blocked by unrelated concurrent workspace breakage (see below) before
   a full pass/fail could be observed — re-run this before trusting it green.
3. `crates/config-testkit/tests/m5_backup_fencing_cluster.rs` — M5-88+89 combined (1 test, both
   directions). Passes.
4. `crates/config-server/tests/m5_backup_cli.rs` — M5-86, 87, 94 (3 rows). Compiles clean in
   isolation; not yet run to green (same blocker).

## Harness gaps found (no `src` touched; patches recorded in each file's module doc)

- `ClusterConfig` has no `admins` allowlist wiring → blocks M5-49..53 (AdminService RPC surface)
  entirely at the `config_testkit::Cluster` level.
- `ClusterConfig` has no `snapshot: SnapshotConfig` field → blocks policy-driven auto build/purge
  on a live `Cluster` (M5-19/20 already covered store-side; M5-17 uncovered anywhere).
- No pause-hook seam on `ScriptedInjector` (only crash/fail/delay-on-nth) → blocks M5-01, M5-02,
  M5-14.
- No `provision_reusing_dir`/`provision_v1_dir` (TA-46) → blocks M5-70's "new id, old dir" half,
  M5-71 entirely, and M5-92 (restored store as genesis member of a fresh cluster).
- `ClusterConfig` has no `promote_max_lag` override → every node runs at
  `DEFAULT_PROMOTE_MAX_LAG` (100); worked around in M5-56 by driving >120 real writes instead.

## A real bug I almost shipped in my own test, not in production code

`m5_58`'s source-grep for "no live `blocking = true` call" initially matched **doc comments**
that *describe* research trap T7 by name (config-engine/src/node.rs lines ~945, ~1302), not an
actual call. The real call (`self.raft.add_learner(node_id.0, node, false).await`, line ~1305)
is correct — `false`, as it should be. Fixed the test to strip `//` before matching. Lesson: any
future source-grep test in this codebase must skip comment lines, because this file's own prose
routinely names the anti-pattern it forbids.

## Concurrent-workspace churn — repeated external build blocker

Multiple background dev agents are writing to shared `crates/*/src/**` concurrently
(dev-snapshot, dev-admin, dev-dedup, dev-pagination, dev-evidence, dev-watch per this folder's
other notes files). `cargo check -p config-testkit -p config-server --tests` failed three
separate times over the course of this session for reasons entirely outside my 4 files:
`config-engine/src/watch.rs` (untracked, mid-write), `config-core/src/store.rs` (unresolved
`postcard` crate), `config-engine/src/{direct,pagination}.rs` (borrow-checker errors mid-edit).
Each time, isolating the check to exactly my files (`cargo check -p config-testkit --test
m5_membership_cluster --test m5_snapshot_cluster --test m5_backup_fencing_cluster`, and
similarly for config-server) succeeded — the breakage was never in anything I own. Evidence
below reflects the isolated, my-files-only runs. A workspace-wide `cargo test` should be re-run
once the other M5/M6 dev agents land, to confirm nothing in their work broke mine or vice versa.

## Mutation testing status

Baseline is now stable (11/11 membership + 4/4 snapshot + 1/1 fencing + 3/3 backup_cli, all
3x-repeated clean). Proceeding with ≥5 mutation triples, each: backup src file to $TEMP, break
the production condition, run the full sibling test file with --test-threads=1, confirm ONLY the
targeted row fails, revert (diff against backup to confirm byte-identical), re-run to confirm
green again.

1. **DONE — fencing (M5-61/62).** `crates/config-engine/src/node.rs` ~line 2303: changed
   `if self.is_retired(meta.from) {` to `if false && self.is_retired(meta.from) {`. Ran full
   `m5_membership_cluster` suite (11 tests, --test-threads=1): exactly
   `m5_61_and_62_retired_identity_is_fenced_at_readmission_and_the_peer_plane` FAILED (panic at
   `.expect_err(...)`, got `PeerResponse::vote` instead of `PeerReject::Retired`); the other 10
   passed. Reverted via Edit back to the original line; `diff` against
   `$TEMP/node.rs.orig_backup` confirmed byte-identical. Re-ran the single test:
   `test result: ok. 1 passed; 0 failed`. Triple closed.

2. **DONE — restore identity refusal (M5-94).** `crates/config-storage/src/rocks.rs` ~line 884:
   `Some(stored) if stored != identity => {` -> `Some(stored) if false && stored != identity => {`
   (falls through to the "verified" arm, disabling `StorageOpenError::IdentityMismatch`). Ran
   `m5_backup_cli` (3 tests): exactly `m5_94_restore_does_not_reuse_the_source_identity_anywhere`
   FAILED (assertion the restored store must not bind to the source identity); `m5_86`/`m5_87`
   stayed green. Reverted; confirmed the specific line matches original (unrelated diff noise
   elsewhere in the file was a concurrent dev-dedup agent's legitimate C5B-01 additions landing
   between backup and revert — verified via targeted grep, not a revert defect). Re-ran
   `m5_94` alone: `ok. 1 passed`. Triple closed.

3. **DONE — exit-code mapping for `manifest_rejected` (M5-86).**
   `crates/config-server/src/backup.rs` ~line 163: `BackupError::Refused {..} => 2` -> `=> 0`.
   Ran `m5_backup_cli` (3 tests): exactly `m5_86_restore_requires_a_new_manifest_and_new_credentials`
   FAILED (`expected exit 2 with reason manifest_rejected; got 0`); `m5_87`/`m5_94` stayed green.
   Reverted; confirmed line matches original. Triple closed (re-verify folded into the final
   green re-run below rather than a fourth isolated run, since the line is a single literal and
   the diff was trivial to eyeball).

4. **DONE — promotion lag refusal (M5-56).** `crates/config-engine/src/node.rs` ~line 1347:
   `if lag > max {` -> `if false && lag > max {` inside `promote_voter_inner`. Ran full
   `m5_membership_cluster` suite (11 tests): exactly `m5_56_promote_refused_while_learner_lags`
   FAILED (`a lagging learner must not be promotable, got Ok(Some(LogIdView{term:1,index:124}))`);
   other 10 passed. Reverted; confirmed line matches original. Triple closed.

5. **DONE — RemoveMember completeness (M5-60).** `crates/config-engine/src/node.rs` ~line 1407:
   `let last = if is_member {` -> `let last = if false && is_member {` inside
   `remove_member_inner`, skipping the `ChangeMembers::RemoveNodes` step (M5-R5 step 2) entirely.
   Ran full `m5_membership_cluster` suite: **two** tests failed, not one —
   `m5_60_removed_voter_is_also_removed_as_a_node` (direct target: "a fully removed member must
   not linger as a learner") AND `m5_70_old_identity_over_its_own_dir_is_refused_after_retirement`
   (collateral: node 4 never actually left membership, so when it restarted under its own
   identity it kept receiving replicated entries — `left: 18, right: 8` on the "must never
   receive new entries" assertion). This is a real, explainable shared blast radius (both rows
   depend on the same `RemoveNodes` step actually running), not a flaw in either test — recorded
   here as a deviation from strict one-row isolation, with the causal explanation, per the
   "never hide inconclusive/complicating evidence" rule. Other 9 tests passed. Reverted;
   confirmed line matches original.

All 3 `node.rs` mutations (fencing, lag, RemoveMember) were applied/reverted sequentially in the
same file. Final combined re-run across all 4 owned test files (19 tests total) after all
mutation cycling: 3/3 backup_cli, 1/1 backup_fencing, 11/11 membership, 4/4 snapshot — all green.
Final grep pass confirmed all 4 target lines (node.rs x3, rocks.rs, backup.rs) read back exactly
as original production text, and no `if false &&` mutation marker remains in any touched src
file. `rustfmt --check` clean on all 4 owned files; `cargo clippy -D warnings` clean for both
`-p config-testkit` (3 targets) and `-p config-server` (1 target).

≥5 mutation checks requirement: SATISFIED (5 triples: fencing, identity-mismatch,
manifest_rejected exit code, promote-lag refusal, RemoveMember completeness). All reverted and
re-verified. STATUS: COMPLETE. Ready for final HANDOFF.
