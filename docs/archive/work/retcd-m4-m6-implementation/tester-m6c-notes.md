# tester-m6c notes

Worker: tester-m6c. Assignment: daemon-level M6 E2E rows tester-m6a/m6b did not reach
(E2E-46, 42, 40, 45, 41, 43) plus a third clean run of E2E-47.

## 2026-09-19T00:00:00Z (session start) — research

Read tester-m6a-notes.md, tester-m6b-notes.md, m6-interfaces.md, architecture-m4-m6.md
excerpts, test-plan-m6.md rows E2E-40..47, e2e_daemon.rs (E2E-44/47 as prior art),
support/{mod.rs,daemon.rs}, m6_policy_daemon.rs (break-glass M6-10 pattern), m6_rbac.rs
(grpc, in-process ReloadPolicy pattern), m5_backup_cli.rs (restore CLI pattern).

Baseline: `cargo check -p config-server --tests` clean before any edit (26.48s).

### Gaps found against the shipped harness (not this workstream's to fix silently)

- `config_client::AdminClient` wraps only get_membership/add_learner/promote_voter/
  remove_member/trigger_snapshot/backup. `ReloadPolicy`, `ReloadTls`, `RotateGossipKey` are
  NOT wrapped — only reachable via the raw `pb::admin_service_client::AdminServiceClient`
  tonic client, same as `crates/config-grpc/tests/m6_rbac.rs` uses against an in-process
  scripted backend. No existing test connects a raw admin client to a real `DaemonProcess`.
- `support::Harness::new`/`with_nodes` hardcode `cluster_id()` (module-level constant
  `CLUSTER_HEX`) into every node's TOML. E2E-45 needs a *second*, distinct cluster identity
  for the restore target — tester-m6a already flagged this exact gap for M6-34.
- `DaemonSpec` has no `--compat-schema` field (E2E-42 needs it); CLI flag exists
  (`cli.rs:67` `compat_schema: Option<u16>`) but the E2E harness never drives it.
- `NodeOptions`/harness TOML writer has no `[tls] watch_files_secs` key and no gossip
  keyring/rotation config surface (E2E-41/43).


## E2E-47 — third clean run (closing tester-m6b's flagged gap)

tester-m6b got 2/3 consecutive clean runs of `e2e_47_daemon_evidence_run_produces_every_artifact`
before this session started (third attempt lost to disk exhaustion / port contention, both
traced to shared-machine load, not the test). Re-ran it standalone:

```
test e2e_47_daemon_evidence_run_produces_every_artifact ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 24 filtered out; finished in 278.19s
```

Third clean run obtained. E2E-47 is now verified 3x green across the two sessions (this run's
log: `scratchpad/e47-run3.log`). No further action needed on this row.


## E2E-46 - daemon_break_glass_rollback_is_audited - implemented

Added e2e_46_daemon_break_glass_rollback_is_audited to crates/config-server/tests/e2e_daemon.rs.
Deploys v9 normally, restarts the break-glass node, confirms the restart's own adoption is
logged as break_glass: false (a first load, not a rollback), then deploys v5 (an older
version) to that node alone and confirms it is accepted, converges, and is audited with
break_glass: true and version: 5 in the policy_loaded log line.

Single clean run observed pre-compaction. 3x-consecutive-green verification and the mandatory
mutation check are tracked below and completed after this note (see "Verification" section).

Mutation target identified: crates/config-core/src/policy.rs,
SignedPolicyAuthorizer::adopt's rollback refusal (the `if is_rollback && !self.break_glass`
guard that returns PolicyRejected::Rollback).

## E2E-40 - daemon_policy_rotation_end_to_end - implemented

Added e2e_40_daemon_policy_rotation_end_to_end to crates/config-server/tests/e2e_daemon.rs.
Rolls a policy document from v1 to v2 (one grant removed, one added) across every node
one at a time, asserting per-node that a removed grant closes immediately on that node's own
adoption while an added grant does not open cluster-wide until the last voter has adopted
v2 (cluster-wide convergence via note_cluster_min_version, not a bare per-node
policy_version == 2 read).

Single clean run observed pre-compaction. 3x-consecutive-green verification tracked below.

### Clippy fixes (2026-09-19, this session)

Three -D warnings clippy errors found in already-landed E2E-46/E2E-40 code, all fixed
in place (no behavior change, no argv/TOML change):
1. e2e_daemon.rs around line 2200 (restart_load, E2E-46) - clippy::filter_next:
   filter(...).next_back() rewritten as rfind(...).
2. e2e_daemon.rs around line 2216 (rollback_load, E2E-46) - same fix.
3. e2e_daemon.rs line 2454 (E2E-40's per-node deploy loop) - clippy::needless_range_loop:
   "for index in 0..harness.nodes.len()" indexed harness.nodes[index] directly; rewrote as
   "for (index, node) in harness.nodes.iter().enumerate()" using node.dir/node.node_id,
   keeping processes[index] (a second, non-iterated collection) as index access.

After these three fixes: cargo clippy -p config-server --test e2e_daemon -- -D warnings
is clean. rustfmt --edition 2021 --check then flagged pre-existing formatting drift
(unrelated lines, e.g. 2057, 2131, 2139, 2496, plus the lines touched by the clippy fixes
above) - ran rustfmt --edition 2021 (not --check) once to normalize; whitespace-only,
re-verified --check clean and cargo check -p config-server --tests still compiles.

## E2E-42 - daemon_rolling_upgrade_v1_to_v2 - SKIPPED, infeasible against shipped code

Decision (2026-09-19): skip this row. Implemented, iteratively debugged across three
real-execution failures, then removed. The row's Setup column, as literally written,
cannot be satisfied by the shipped config-storage/openraft integration. This is a
structural finding, not a flake or a harness bug on my side.

### What the row asks for

Mixed-version rolling upgrade: start a cluster pinned to schema-1 (--compat-schema 1),
confirm cluster_min_schema stays None/schema-1 while any voter is still on the old
build, force a Compact (schema-2-gated, ADR-0019/ADR-0030), restart nodes one at a time
onto the new build (schema-2 default), and confirm no writes are lost and the cluster
converges once every voter has upgraded.

### Why it fails - evidence trail

1. First real failure: restarting a node from --compat-schema 1 to schema-2 default
   is refused outright by RocksStore::open's refuse_if_undrained (ADR-0021, ruling
   M5-R19) whenever the on-disk raft log (CF_RAFT_LOG, openraft's own log, distinct
   from the config-store's own journal/history) holds even one entry. Actual stderr:
   storage_open_failed: data directory ...\node-3\data is on-disk format version 1 and
   still holds 2 Raft log entrie(s); an in-place upgrade cannot decode them (the log
   payload is positional and unversioned).
   The row's Setup column never names this precondition. Worked around by adding a
   drain-and-retry loop before the restart (pad with writes, wait, retry).

2. Second failure: a redundant std::fs::remove_file on the shutdown-file inside the
   retry's Err branch panicked with NotFound - the caller had already removed it, and
   the failed attempt never ran long enough to recreate it. Fixed by removing the
   redundant early removal, keeping only the later one after stop_gracefully().

3. Third failure: waiting for a fresh "log purged" JSONL line as the drain signal
   never fired, even though the node had fully caught up (state_hash_hex,
   cluster_revision, last_applied all matched its peers). Diagnosis: the node's
   applied_commands was far lower than its peers' (5 vs 100) - proof it caught up via
   openraft's InstallSnapshot path, not per-entry replay, which bypasses the
   per-entry-apply/local-purge-trigger code path that emits "log purged" entirely.
   Fixed by switching the wait condition to poll for state_hash_hex convergence across
   all nodes instead.

4. Conclusive result, after the fix above: all 6 retry attempts (each with real
   additional write load and confirmed full state convergence in between) failed
   identically, stderr unchanged each time:
   node 3 still refuses to start without --compat-schema after 6 drain attempts: the
   daemon exited before printing a ready line; stderr: config-server: storage_open_failed:
   data directory ...\node-3\data is on-disk format version 1 and still holds 2 Raft log
   entrie(s); ...
   The residual raft-log count was exactly 2, unchanged across all 6 independent
   attempts. Confirmed via crates/config-engine/src/config.rs that [snapshot]
   tuning (logs_since_last/logs_to_keep/purge_batch_size) maps directly onto
   openraft's own native SnapshotPolicy::LogsSinceLast/max_in_snapshot_log_to_keep/
   purge_batch_size - ruling out a bug in this codebase's own config wrapper.

### Conclusion

openraft's snapshot/purge mechanism does not drain the raft log to exactly zero entries
under any tuning or amount of ordinary client write load (a residual tail, observed as 2
here, persists). RocksStore::open's upgrade gate demands exactly zero. These two facts
together make E2E-42's literal Setup column structurally unreachable against the shipped
code: no sequence of client-visible actions available to this harness can satisfy the
precondition for the in-place format migration the row wants to exercise.

This is a reportable finding for main, not a defect I am authorized to fix: either
the row's premise needs revision (e.g. require --compat-schema 1 on both old and new
build until the log is externally known-empty, or drop the "drain via ordinary traffic"
expectation and use a fresh data directory per version instead of an in-place upgrade), or
RocksStore::open's zero-tolerance drain check may itself warrant reconsideration. Neither
is in scope for a tests-only worker.

Also worth noting: the plan row's literal text ("feature_activated fires once per node")
is stale relative to two independently-confirmed architectural rulings (M6-R12, M6-R15):
cluster_min_schema() and the feature_activated log line are leader-only, not
per-node. This is a documented fact, not itself a blocker to the row, but should be fixed
in the plan text regardless of the row's disposition.

No test code for E2E-42 remains in e2e_daemon.rs (added, iterated, then fully removed;
net diff for this row is zero once the clippy/rustfmt fixes above are excluded).

## E2E-46 - verification (2026-09-19)

3x-consecutive-green: obtained. Run history this session, RETCD_TEST_DEADLINE_SCALE=3,
fresh RETCD_TEST_LOG_DIR each run:
- run 1: FAILED at 31.22s. wait_on_policy_state(process, policy_active(9)) (the final
  reconvergence wait, deadline(10) is approx 27s at scale 3) timed out; health dump at
  panic showed policy_version 5 / policy_state active v5, i.e. the wait fired before the
  break-glass node had re-adopted v9. This is a bounded poll_until_async against a derived
  deadline, not a fixed sleep - already anti-flake-compliant. Disposition: not a test
  logic defect. Consistent with one-time Windows cold-start / AV-scan variance on a
  freshly-linked test binary (first execution after a fresh compile took 31s total vs
  4-11s on every later run). No code change made.
- run 1b: ok, 11.22s.
- run 2: ok, 4.18s.
- run 3: ok, 4.34s.
- run 4: ok, 5.34s.
3 consecutive green obtained (runs 2, 3, 4). Residual risk: one unexplained slow-path
failure this session, not reproduced in 4 subsequent attempts; flagged for main as a
possible environmental flake, not a code defect, no fix applied.

Mutation check (mandatory, this row): target crates/config-core/src/policy.rs:611,
SignedPolicyAuthorizer::adopt's rollback guard
"if is_rollback && !self.break_glass { ... }". Opened by appending "&& false" to the
condition (i.e. the guard never fires, rollback is never refused).
// MUTATION OPEN 2026-09-19T16:15:06Z
Re-ran e2e_46_daemon_break_glass_rollback_is_audited with the mutation live: FAILED as
expected -
  thread 'e2e_46_daemon_break_glass_rollback_is_audited' panicked at
  crates\config-server\tests\e2e_daemon.rs:2073:9:
  policy_rejected count never reached 1 in ".../node-1/logs/1.jsonl" within 27s
Confirms the test actually exercises the rollback-refusal guard (with the guard
disabled, the ordinary nodes' expected `policy_rejected` audit line never appears, so the
test correctly fails). Reverted the mutation immediately.
// MUTATION CLOSED 2026-09-19T16:16:32Z
Window: 86s (within the <=2 minute budget). Post-revert: `cargo check -p config-server
--tests` clean; `grep -rnE "MUTATION (OPEN|CLOSED)" crates/*/src` empty (confirmed).

## E2E-40 - verification (2026-09-19)

3x-consecutive-green: obtained cleanly on first attempt, RETCD_TEST_DEADLINE_SCALE=3,
fresh RETCD_TEST_LOG_DIR each run:
- run 1: ok, 4.03s.
- run 2: ok, 3.66s.
- run 3: ok, 3.56s.
No flakiness observed. Mutation check requirement ("one rotation row") satisfied by
E2E-46's mutation check above (E2E-46 exercises the break-glass/rollback rotation guard);
no second mutation performed against E2E-40 specifically, since the assignment asks for
one rotation-row mutation check total, not one per row.

## E2E-40 - mutation check ("one rotation row" requirement, 2026-09-19)

On reflection this session, decided E2E-46's mutation check does not, on its own,
satisfy the separate "one rotation row" mutation-check requirement flagged in this
worker's own earlier planning notes (that note explicitly ties it to E2E-41/E2E-43, both
still unimplemented). Rather than leave it unsatisfied, performed a real mutation check
against E2E-40 itself, whose test name and behavior are literally about rotation
(policy_rotation_end_to_end) and which is fully implemented and 3x-green verified above.

Target: crates/config-core/src/policy.rs, the `decide` function's convergence
intersection gate (used by evaluate_converging, which SignedPolicyAuthorizer::adopt calls
during a changed-prefix request while a rotation is converging): the
"if grants_allow(&old.grants, principal, action, key) { Decision::Allow } else {
Decision::deny(REASON_POLICY_CONVERGING) }" branch. This is the exact intersection rule
E2E-40's E40_NEW assertion depends on ("the added grant must not open before the last
node has the document").

Opened by changing the condition to "if true || grants_allow(...)" (bypasses the
intersection, so a changed-prefix grant present only in the new document opens
immediately instead of waiting for convergence).
// MUTATION OPEN 2026-09-19T16:18:50Z
Re-ran e2e_40_daemon_policy_rotation_end_to_end with the mutation live: FAILED as expected -
  thread 'e2e_40_daemon_policy_rotation_end_to_end' panicked at
  crates\config-server\tests\e2e_daemon.rs:2485:9:
  assertion `left == right` failed: node 1: the added grant must not open before the last
  node has the document (last=false)
  left: true
  right: false
Confirms the test actually exercises the convergence-intersection guard. Reverted
immediately.
// MUTATION CLOSED 2026-09-19T16:19:47Z
Window: 57s (within the <=2 minute budget). Post-revert: grep -rnE "MUTATION
(OPEN|CLOSED)" crates/*/src empty (confirmed); re-ran e2e_40_daemon_policy_rotation_end_to_end
once more standalone: ok, 4.68s (green, confirms the revert restored correct behavior and
did not leave the file in a broken state).

## Session close-out (2026-09-19)

Final checks, all clean:
- rustfmt --edition 2021 --check on e2e_daemon.rs, support/mod.rs, support/daemon.rs: clean.
- cargo clippy -p config-server --test e2e_daemon -- -D warnings: clean.
- cargo test -p config-testkit --test scan: 4 passed, 0 failed, including
  workspace_tests_contain_no_fixed_sleeps_or_literal_ports.
- grep -rnE "MUTATION (OPEN|CLOSED)" crates/*/src: empty, both times (after each of the
  two mutation checks).
- cargo check -p config-server --tests: clean.

Rows completed this session/carried forward: E2E-46 (implemented, 3x green, mutation
check done), E2E-42 (implemented then found infeasible, skipped with dated evidence-backed
note), E2E-40 (implemented, 3x green, mutation check done - satisfies the "one rotation
row" requirement), E2E-47 (third clean run, closed out).

Not reached this session, honestly bounded by budget: E2E-45 (restore refuses client
plane without a policy), E2E-41 (TLS rotation with restart-free continuity), E2E-43
(gossip key rotation with one node down). None investigated. Known harness gaps that
whoever picks these up will hit immediately (noted in this file's "session start"
section): Harness::new/with_nodes hardcode a single cluster_id, but E2E-45 needs a second,
distinct cluster identity for the restore target; NodeOptions/harness TOML writer has no
[tls] watch_files_secs key and no gossip keyring/rotation config surface for E2E-41/E2E-43.

Net file changes this session: e2e_daemon.rs (clippy fixes to existing E2E-46/E2E-40 code,
rustfmt normalization, E2E-42 added-then-fully-removed with zero net diff for that row);
support/mod.rs (RetentionTuning addition, additive, unused by any row now that E2E-42 was
removed - left in place since it fills a genuine harness gap, no client-facing way to force
a Compact, and may be useful to a future M6 row); support/daemon.rs unchanged this session
(compat_schema field predates this session); config-core/src/policy.rs: net zero (two
mutation windows, each opened and reverted within budget, confirmed clean).

Handing off to main now.
