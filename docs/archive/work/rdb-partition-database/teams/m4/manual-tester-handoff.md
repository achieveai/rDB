# M4 Manual Tester Handoff — TAKEN OVER MID-RUN

Basis: `d8873a3`, export `/c/m4` (left in place, untouched, per instruction).
Status: coordinator took over mutation work directly. This handoff reports what actually
ran, not a completed pass.

## Process-safety answer (asked first by coordinator)

All `taskkill` calls were PID-scoped, never image-name-wide. No `/IM cargo.exe` kill was
ever issued.
- `taskkill //PID 149320 //F` — succeeded. Verified as my own process: PID matched the
  `m4_watch-bb5a906d8a96de91.exe` binary named in my own `mut1_run.txt`, under `/c/m4/.t`.
- `taskkill //PID 65928 //F` — "process not found," no effect.
- `taskkill //PID 107404 //F` — "process not found," no effect.
Both no-effect PIDs were selected from an unscoped `tasklist /FI IMAGENAME eq cargo.exe`
query, which was sloppy, but since both returned "not found" nothing was actually
terminated by either call. Host recheck just now: `tasklist /FI "IMAGENAME eq cargo.exe"`
is empty and no process has `C:\m4` in its command line. No evidence any other tester's
process was affected.

## 1. What the plan says M4 is

Per `docs/testing/test-plan-m4.md` (892 lines, read in full — matched the brief, no
mismatch): "resumable watches." Retained deterministic event journal (one synced state
batch with KV), replicated compaction as a `Compact` command, leader-served prefix `Watch`
with a serialized gate (`WatchGate`: `AfterRegister` → `BeforeReplay` → `BeforeLiveDrain`),
bounded stream queues with overload termination, gRPC `Watch` RPC, format v1→v2 migration
(`events` CF). Rows M4-01..M4-121, conformance W-01..W-12, E2E-20..E2E-27, TA-28..TA-40
production/harness seam requirements, anti-flake rules 21-26.

## 2. Baseline

`scripts/gate.sh test -p config-storage -p config-engine -p config-testkit -p config-grpc
-p config-client -p config-core -p config-server` → `run1_baseline.txt`, ends `gate: test
OK` / **EXIT=0**. All M4 binaries green:

| binary | pass count |
|---|---|
| m4_watch_client.rs | 4 |
| m4_core.rs | 16 |
| m4_watch.rs (config-engine) | 22 |
| m4_watch_transport.rs | 7 |
| m4_watch_wire.rs | 7 |
| e2e_daemon.rs | 31 (492.93s, mixed M0-M3+M4) |
| m4_e2e_daemon.rs | 5 |
| m4_journal.rs (config-storage) | 27 |
| m4_capabilities.rs | 3 |
| m4_journal_cluster.rs | 12 |
| m4_observability.rs | 3 |
| m4_watch_cluster.rs | 20 |
| m4_watch_conformance.rs | 3 |
| m4_watch_faults_cluster.rs | 22 |
| m4_watch_progress.rs | 3 |

Note: `m4_e2e_daemon.rs` has only 5 test functions against the plan's 8 E2E rows
(E2E-20..E2E-27) — not investigated further, flagged in Not Covered.

## 3. TASK 1 — defect-shape sweep (from 4 background sweep agents, already reported to
   coordinator directly; repeated here for the handoff record)

| # | Location | Shape | Reason | Proof status |
|---|---|---|---|---|
| 1 | `m4_watch_faults_cluster.rs:890-987`, `m4_67_overload_termination_is_resumable_and_says_so` | (c) generous condition, not proven | Doc comment (890-892) claims gRPC+direct resume "loses nothing still retained." Direct half (line 946, with its own damning comment: "Just needs to register and accept the cursor; draining it fully is not this row's claim") registers the resumed watch and drops it without draining. gRPC half (948-984) never calls `watch_grpc` a second time at all — only checks the `resumable:true` error property on the first call. The gRPC side of the claimed property is asserted nowhere. | **Not run — handed to lead.** No mutation executed (planned: break `AdmissionGuard::drop` release-on-drop semantics, corroborate against `m4_74` which does exercise the guard via direct client). |
| 2 | `m4_watch_faults_cluster.rs:335`, `m4_28_follower_has_no_age_map` | (c) | `tokio::time::sleep(200ms)` proves a negative by elapsed time. Disclosed in-file (`// testkit:allow-sleep: ...`). Ranked below #1. | Not run — handed to lead. |
| 3 | `m4_watch_faults_cluster.rs:407`, `m4_29_new_leader_rebuilds_age_map_empty` | (c) | Same pattern, `sleep(300ms)`, disclosed. Ranked below #1. | Not run — handed to lead. |

No shapes (a), (b), or (d) candidates were found strong enough to rank above these three;
the sweep agents' full raw output was reported to the coordinator directly and is not
reproduced here.

## 4. TASK 2 — six invariant mutations

| # | File:line | Invariant | Edit | Test target | Status |
|---|---|---|---|---|---|
| 1 | `config-engine/src/watch.rs:819`, `register_locked` | OQ-27: `compact_revision==0` must not reject `start_after=0` (fresh-cluster guard) | Removed the `compact_revision > 0` guard so the comparison fires even when compaction has never run | `config-engine/tests/m4_watch.rs::m4_57_fresh_cluster_start_after_zero` | **CAUGHT** (partial run — see below) |
| 2 | `config-storage/src/rocks.rs:2935-2943` | TA-28: `AfterStateBatchBeforePublish` boundary must actually cross | Replaced the `s.after_boundary(Boundary::AfterStateBatchBeforePublish, ...)?` call with a comment, suppressing the crossing | `config-storage/tests/m4_journal.rs::m4_91_crash_after_state_batch_before_publish` | **Not run — handed to lead** (build failure before test execution, see below) |
| 3 | `config-core/src/state.rs:883`, `apply_compact` | OQ-31: compaction strictly-greater-target-only monotonicity | Planned: invert/remove the `if clamped <= self.compact_revision` guard | `config-storage/tests/m4_journal.rs::m4_25_compact_is_monotonic`, `config-testkit/tests/m4_journal_cluster.rs::m4_36_compaction_proposal_is_not_deduplicated_in_m4` | **Not run — handed to lead** (never applied) |
| 4 | `config-storage/src/rocks.rs:4316`, `read_events` | Half-open journal range bound | Planned: change `if revision > to_inclusive { break; }` to an off-by-one (`>=`) | `config-storage/tests/m4_journal.rs::m4_10_journal_range_is_ordered_and_half_open` | **Not run — handed to lead** (never applied) |
| 5 | `config-engine/src/watch.rs:1486`, `send_event` | Per-event authorization check on live delivery | Planned: bypass `if !self.authorized(&event.key)` | `config-testkit/tests/m4_watch_faults_cluster.rs::m4_47_authorization_checked_per_event`, `m4_48_authorization_checked_on_replay_too` | **Not run — handed to lead** (never applied) |
| 6 | `config-engine/src/node.rs:2200-2209`, `evaluate_retention` | Leader-only retention/age-map gate | Planned: remove the non-leader early-return so followers also run retention/receipts-clear | `config-testkit/tests/m4_watch_faults_cluster.rs::m4_28_follower_has_no_age_map`, `m4_29_new_leader_rebuilds_age_map_empty` | **Not run — handed to lead** (never applied) |

### Mutation 1 detail (CAUGHT)

Edit applied at `config-engine/src/watch.rs:819` inside `register_locked`. Ran
`scripts/gate.sh test -p config-engine --test m4_watch`. Target row failed clearly:
`m4_57_fresh_cluster_start_after_zero ... FAILED`, plus cascading failures in related
rows. **Caveat: I stopped this run before it reached a final `EXIT=` line.** Three
unrelated tests (`c4_01_progress_never_claims_a_revision_this_stream_has_not_drained`,
`c4_08_a_batch_between_subscribe_and_high_water_is_delivered_once`,
`m4_39_live_handoff_delivers_above_high_water`) hung past 60s under the mutated build and
never resolved in a 10-minute window — plausibly a genuine liveness side-effect of this
specific mutation (breaking the R=0 guard can put registration into an unexpected wait
state), not a harness bug, but not diagnosed further. I stopped the task rather than wait
indefinitely. The verdict CAUGHT rests on the explicit `FAILED` line for the targeted row,
observed before the stop — not on a full-suite `EXIT` code.

### Mutation 2 detail (not run — handed to lead)

Edit applied at `config-storage/src/rocks.rs:2935-2943`:
```
Old:
                // TA-28: the durable-but-unpublished window, named so a test can crash inside
                // it and prove the events survive to be replayed from disk.
                s.after_boundary(
                    Boundary::AfterStateBatchBeforePublish,
                    ErrorSubject::StateMachine,
                    ErrorVerb::Write,
                    last_index,
                    s.sync_writes,
                )?;
New:
                // TA-28: the durable-but-unpublished window, named so a test can crash inside
                // it and prove the events survive to be replayed from disk.
                // MUTATION TARGET (M4 manual-test mutation #2): crossing suppressed.
```
Ran `scripts/gate.sh test -p config-storage --test m4_journal > /c/m4/mut2_run.txt 2>&1`
in the background. **This run never reached test execution.** It was still in dependency
compilation (`librocksdb-sys`) when I stopped it per the coordinator's takeover
instruction. Checked the file directly just now (`tail`/`grep "^EXIT="`, both after
stopping the monitor that was polling it): no `EXIT=` line anywhere in 14,911 lines. The
file ends on a **build failure**, not a test result:
```
error occurred in cc-rs: command did not execute successfully (status code exit code:
0xc0000142): "...\lib.exe" "-out:C:/m4/.t\debug\build\librocksdb-sys-.../librocksdb.a" ...
```
`0xc0000142` is `STATUS_DLL_INIT_FAILED` — consistent with the target directory being
disturbed mid-link by a stopped/killed process on this shared host, not a defect in the
mutation or in rEtcd. **No test evidence exists for mutation 2 in either direction.**
Status is "not run — handed to lead," not INCONCLUSIVE — the distinction the coordinator
asked me to preserve is that INCONCLUSIVE would imply a test ran and produced an
ambiguous result; here, zero test rows executed.

### Mutations 3-6

Never applied to any file. Targets and test rows located and listed above so the
coordinator does not need to re-locate them.

## 5. TASK 3 — flakiness

**Not run — handed to lead.** No 5x reruns were started for any row. Recommended
candidates, both already disclosed in-file as intentional sleeps (see TASK 1 #2/#3 above):
`m4_watch_faults_cluster.rs:335` (`m4_28_follower_has_no_age_map`, 200ms) and `:407`
(`m4_29_new_leader_rebuilds_age_map_empty`, 300ms).

## 6. Guards

None written. Guard-writing is scoped to a confirmed MISSED finding; no mutation reached a
MISSED verdict (mutation 1 was CAUGHT; mutations 2-6 never produced evidence either way).

## 7. Revert verification

`config-engine/src/watch.rs` (mutation 1) — reverted via `cp watch.rs.orig watch.rs`,
verified `diff watch.rs.orig watch.rs` → empty, then `rm watch.rs.orig`.

`config-storage/src/rocks.rs` (mutation 2) — `diff rocks.rs.orig rocks.rs` run **before**
revert confirmed the exact mutation shown above (non-empty, matched expected diff); then
`cp rocks.rs.orig rocks.rs`; then `diff rocks.rs.orig rocks.rs` again →
**"DIFF EMPTY AFTER REVERT"**; then `rm rocks.rs.orig`.

Final sweep: `find /c/m4/crates -name "*.orig"` → **empty result**. No `.orig` file
remains anywhere under `/c/m4/crates`. Re-ran this check again just now as part of this
handoff — still empty.

Process state: no `cargo.exe` running host-wide, no process with `C:\m4` in its command
line, as of the final check before writing this file.

## 8. Not covered

- Mutations 3, 4, 5, 6 — targets located, never applied or run.
- TASK 3 flakiness reruns — not started.
- Guards — none written (contingent on findings not reached).
- `m4_e2e_daemon.rs` coverage gap (5 functions vs. 8 plan rows E2E-20..E2E-27) — noted,
  not investigated.
- The three TASK-1 candidates (m4_67, and the two disclosed sleeps) were analyzed and
  ranked but never proven or disproved by mutation.
- Mutation 1's full-suite completion (EXIT code) was never obtained — only the targeted
  row's explicit FAILED result, plus 3 unexplained hangs in unrelated tests under the
  mutated build.

## Verdict: THUMBS DOWN

- Basis: d8873a3
- Scope tested: baseline (all M4 binaries, EXIT=0); mutation 1 partial run (target row
  `m4_57_fresh_cluster_start_after_zero` observed FAILED = CAUGHT, full suite not
  completed); mutation 2 attempted, zero test evidence (build failure before execution).
- Blocking: mutations 3-6 not run; TASK 3 not run; mutation 1's full-suite result and the
  3 unexplained test hangs under that mutation not resolved; mutation 2 produced no
  evidence in either direction; none of the three TASK 1 candidates proven by mutation.
- Not covered: see section 8.
