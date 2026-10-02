# Manual mutation-test pass — kernel-a authority.rs (M7A-28/29/32/33)

Workspace: `C:\hc1` (export of HEAD `fe5b824`). Target: `crates/rdb-core/src/authority.rs`
(plus `crates/rdb-core/src/contracts/control.rs` for K3's `is_gap`). Tests:
`crates/rdb-sim/tests/authority.rs`.

Environment for every run:
```
cd /c/hc1
export CARGO_TARGET_DIR=/c/hc1/.t CARGO_INCREMENTAL=0 RETCD_TEST_DEADLINE_SCALE=3 RETCD_TEST_LOG_DIR=/c/hc1/logs
cargo test -p rdb-sim --test authority
```

## Baseline

Run: `run1_baseline.txt`. All green.

| Test | Result |
|---|---|
| m7a_28_gap_termination_reloads_then_rewatches | ok |
| m7a_29_watched_run_reads_each_change_and_never_reloads | ok |
| m7a_32_no_read_family_without_a_termination | ok |
| m7a_33_admission_refused_backs_off_and_never_reloads | ok |

EXIT=0, "4 passed; 0 failed".

## Mutations

Revert method for every mutation: `cp FILE.orig FILE` then `diff FILE FILE.orig` (empty every
time — confirmed after every mutation, logs in run outputs / this table).

| # | Edit | File:line | Command / run file | EXIT | Failing tests + first assertion message | Verdict |
|---|---|---|---|---|---|---|
| K1 | `on_watched` also pushes `Reload{prefix}` after the `Get`s for a healthy delivery | authority.rs:167-171 | run2_k1.txt | 101 | m7a_29 (line 259: "a contiguous watch run is not a gap and must not provoke a reload"); m7a_32 (line 322: "arm 1: 200 contiguous watch runs and 50 progress watermarks declare no gap, so the kernel must not reload once") | **CAUGHT** |
| K2 | `on_terminated`: gap branch returns `Vec::new()` instead of `Reload{prefix}` | authority.rs:189-190 | run3_k2.txt | 101 | m7a_28 (line 427: "a resumable-exhaustion termination is a gap: one reload per gapped family, no more"); m7a_32 (line 342: "...if this is empty the arm-1 counter was never live and arm 1 proved nothing") | **CAUGHT** (not the dangerous case — m7a_32 did fail, so the arm-2 positive control is proven live) |
| K3 | `WatchTermination::is_gap` also returns `true` for `ResourceExhaustedFatal` | contracts/control.rs:286-291 | run4_k3.txt | 101 | m7a_33 only (line 368: "`ResourceExhaustedFatal` answers false to `is_gap` — it is a capacity error, not a gap"). m7a_28 unaffected, still `ok`. | **CAUGHT** |
| K4a | `WATCH_ADMISSION_ATTEMPT_CAP` = 2 | authority.rs:69 | run5_k4_cap2.txt | 0 | none — all 4 pass | **MISSED** |
| K4b | `WATCH_ADMISSION_ATTEMPT_CAP` = 4 | authority.rs:69 | run6_k4_cap4.txt | 0 | none — all 4 pass | **MISSED** |
| K5 | `on_terminated` gap branch reloads `ControlPrefix::ClusterSchema` (fixed) instead of the gapped `prefix` | authority.rs:189-195 | run7_k5.txt | 101 | m7a_28 (line 427, same message as K2); m7a_32 (line 342: "arm 2: exactly one reload per gapped family, naming that family — ...") | **CAUGHT** |
| K6 | `on_control`'s `Watched` arm discards `on_watched`'s effects and returns `Vec::new()` | authority.rs:284-291 | run8_k6.txt | 101 | m7a_29 only (line 264: "each change is followed by a linearizable read of that record"). m7a_28/32/33 unaffected. | **CAUGHT** |

### K4 — MISSED, root cause

`m7a_33`'s tail loop is:
```rust
while driver.kernel.watch_refused_attempts() < WATCH_ADMISSION_ATTEMPT_CAP {
```
It re-derives the expected cap from the same production constant it's meant to be checking, so
changing the constant's value moves both sides of the comparison together — the loop just runs a
different number of iterations and the final assertions (`>= WATCH_ADMISSION_ATTEMPT_CAP`, "never
a reload") stay true regardless of what the cap actually is. Confirmed both directions (2 and 4)
pass clean. This is the row `MISSED` a numeric-value mutation, per the task's flag for that case.

## Test written for K4 (MISSED mutation only)

File: `crates/rdb-sim/tests/authority.rs`, appended after `m7a_28_...` (end of file), function
`manual_k4_admission_cap_is_exactly_three`. Diff (added lines only, nothing else in the file
changed — confirmed via `diff` against the pre-mutation original):

```rust
/// manual_K4 — `WATCH_ADMISSION_ATTEMPT_CAP` is exactly 3, not merely "some cap".
///
/// `m7a_33` drives its own tail loop with `while ... < WATCH_ADMISSION_ATTEMPT_CAP`, so it
/// re-derives the cap from the same constant it is meant to check and cannot notice the constant
/// itself moving to 2 or to 4 (manual mutation-test finding K4). This row hardcodes the expected
/// counts instead of reading them back from the constant, so a changed cap value fails it.
///
/// `become_held` leaves both `Grants` and `Partitions` watched, and `watch_refused_attempts` is
/// one counter shared by every family (see `Driver::terminations`'s doc comment: one `terminate`
/// ends every open watch). So each `terminate` call advances the shared counter by 2, once per
/// family, and the cap is checked separately for each family's own increment within that call.
#[retcd_test]
fn manual_k4_admission_cap_is_exactly_three() {
    support::preamble();
    let mut driver = Driver::new();
    driver.become_held();
    let _ = driver.take_reloads();

    // Call 1 carries the counter through 1, then 2 (one increment per watched family). Both are
    // below the cap of 3, so both families re-arm.
    let reopened_1 = driver.terminate(WatchTermination::ResourceExhaustedFatal);
    assert_eq!(
        driver.kernel.watch_refused_attempts(),
        2,
        "manual_K4: one call terminates both watched families, advancing the shared counter by 2"
    );
    assert_eq!(
        reopened_1.len(),
        2,
        "manual_K4: attempts 1 and 2 are both below the cap of 3, so both families re-arm"
    );
    driver.reopen(&reopened_1);

    // Call 2 carries the counter through 3, then 4. The cap is exactly 3: both increments on
    // this call land at or past it, so neither family re-arms.
    let reopened_2 = driver.terminate(WatchTermination::ResourceExhaustedFatal);
    assert_eq!(
        driver.kernel.watch_refused_attempts(),
        4,
        "manual_K4: the counter keeps advancing regardless of the cap"
    );
    assert!(
        reopened_2.is_empty(),
        "manual_K4: the cap is exactly 3 -- attempts 3 and 4 on this call are both at or past \
         it, so neither family re-arms"
    );
    assert_eq!(
        driver.take_reloads(),
        Vec::<ControlPrefix>::new(),
        "manual_K4: reaching the cap never provokes a reload"
    );
}
```

Design note: my first draft assumed one `terminate()` call = one attempt and hardcoded 1/2/3. It
failed on *clean* code (`run9_new_test_clean.txt`, EXIT 101, "left: 2 right: 1") because
`become_held` leaves two families watched (`Grants`, `Partitions`) and `ControlOp::TerminateWatch`
ends every open watch the node holds — so one `terminate()` call delivers two
`WatchTerminated` events and the shared `watch_refused_attempts` counter advances by 2 per call,
not 1. Rewrote to assert the actual per-call counts (2, then 4) and the per-call re-arm counts
(2 families, then 0) instead. This is a real trap in the harness worth flagging to the row's
owner, not just to this handoff.

### Proof — three runs, same test function, three code states

| Code state | Run file | Result |
|---|---|---|
| Clean (cap=3) | `run10_new_test_clean2.txt` | EXIT=0, "5 passed; 0 failed" — new test passes |
| K4a mutant (cap=2) | `run11_new_test_cap2.txt` | EXIT=101, `manual_k4_admission_cap_is_exactly_three` FAILED at line 481: "attempts 1 and 2 are both below the cap of 3, so both families re-arm" (got `reopened_1.len() == 1`, expected 2) |
| K4b mutant (cap=4) | `run12_new_test_cap4.txt` | EXIT=101, `manual_k4_admission_cap_is_exactly_three` FAILED at line 496 (`reopened_2.is_empty()` false — one family still re-armed at cap=4) |

Final run on reverted, clean production code with the new test present: `run13_final.txt`,
EXIT=0, "5 passed; 0 failed; ... finished in 0.01s". `diff` of the test file against the
pre-session original shows only the appended `manual_k4_...` function (see run13_final.txt
tail) — nothing else in the test file changed.

## Revert verification

Every mutation was applied via `Edit`, run, then reverted with
`cp FILE.orig FILE && diff FILE FILE.orig` (exit 0 / empty diff) before the next mutation.
Confirmed clean after: K1, K2, K3 (plus `control.rs`), K4a, K4b, K5, K6, and the final cap=4
proof run for the K4 test. Backup `.orig` files were deleted at the end of the session; the
working tree in `C:\hc1` now differs from the session-start export only by the added
`manual_k4_admission_cap_is_exactly_three` test in
`crates/rdb-sim/tests/authority.rs`. No production code (`authority.rs`, `control.rs`) was
touched — both are back to the session-start baseline.

## Not done / out of scope

- No production code was fixed (per instructions — this pass only tests the tests).
- No test was written for K1, K2, K3, K5, K6 — all five were CAUGHT, per instructions ("do not
  write tests for CAUGHT mutations").
- Did not investigate whether `WATCH_ADMISSION_ATTEMPT_CAP`'s *per-family* shared-counter
  semantics (one counter across both watched families, not one per family) is itself a design
  question worth raising — flagging it here since it was the reason my first K4 test draft was
  wrong, but it is outside this task's mandate to judge production behavior, only to test the
  tests.

## Round 2 — re-verify at HEAD f22aa44

The K4 guard landed committed, renamed to `m7a_33_admission_cap_is_exactly_three` (same body).
Re-exported `/c/hc1` from `git archive f22aa44` (confirmed via `/c/hc1/EXPORT_BASIS` and
`git rev-parse HEAD`).

**Mid-round incident:** the coordinator re-exported `/c/hc1` (`rm -rf` + fresh `git archive`)
while I was mid-mutation on K4b, wiping my `.orig` backups and run-log files for baseline, K4a
and the aborted K4b attempt. Confirmed by an `EXPORT_BASIS` marker file appearing that I never
created, and by `Cargo.toml` briefly not being found. Per the coordinator's instruction, I did
not rely on any evidence that lived only in a deleted run file — baseline, K4a and K4b were
re-run from scratch in the stable re-export below; nothing in this section rests on the deleted
files.

### Baseline at f22aa44

Run: `r2b_run1_baseline.txt`. EXIT=0, "5 passed; 0 failed" (`m7a_28`, `m7a_29`, `m7a_32`,
`m7a_33_admission_refused_backs_off_and_never_reloads`,
`m7a_33_admission_cap_is_exactly_three`).

### Re-run K4a / K4b against the committed guard

| # | Edit | Run file | EXIT | Failing test + message | Verdict |
|---|---|---|---|---|---|
| K4a | `WATCH_ADMISSION_ATTEMPT_CAP` = 2 | `r2b_run2_k4a_cap2.txt` | 101 | `m7a_33_admission_cap_is_exactly_three`, line 481: "M7A-33: attempts 1 and 2 are both below the cap of 3, so both families re-arm" | **CAUGHT** |
| K4b | `WATCH_ADMISSION_ATTEMPT_CAP` = 4 | `r2b_run3_k4b_cap4.txt` | 101 | `m7a_33_admission_cap_is_exactly_three`, line 496 (`reopened_2.is_empty()` false) | **CAUGHT** |

Both reverted, `diff` against `.orig` clean immediately after.

### K2 re-run — counter still live at HEAD

Same edit as round 1 (`on_terminated`'s gap branch returns `Vec::new()` instead of
`Reload{prefix}`). Run: `r2b_run4_k2.txt`. EXIT=101. `m7a_28` fails (line 427) and `m7a_32`
fails (line 342, "...arm-1 counter was never live and arm 1 proved nothing"). **CAUGHT**,
unchanged from round 1. Reverted, clean.

### K7 — `on_family_snapshot` resumes from the old cursor instead of the snapshot revision

Edit: `authority.rs`, `on_family_snapshot` — capture `old_cursor` before the insert, then build
the returned `Watch{from}` from `old_cursor` instead of `snapshot_revision` (the `cursors` map
itself is still updated correctly, so only the *emitted effect* is wrong).

First run against the mutant, full suite: `r2b_run5_k7.txt`. EXIT=0, **all 5 committed tests
pass, including `m7a_28`** — the row the task expected to catch this. **MISSED.**

Root cause: `ControlStore`'s revision counter only advances on a committed CAS
(`crates/rdb-sim/src/sim/control.rs:509`), and `m7a_28`'s scenario (`become_held` then
immediately `terminate(ResourceExhaustedResumable)`) never commits anything in between. So the
snapshot revision `on_family_snapshot` receives on the gap-triggered reload is numerically
identical to the cursor `become_held` already recorded. `old_cursor == snapshot_revision` in
this exact fixture, so a kernel that resumes from `old_cursor` is indistinguishable, on this
row alone, from one that correctly resumes from `snapshot_revision`. Not a directionality
mistake in the mutation — verified in `sim/control.rs` that no other write happens on this path.

**Guard test written** (K7 was MISSED): `manual_k7_resumed_watch_uses_snapshot_revision_not_stale_cursor`,
appended to `crates/rdb-sim/tests/authority.rs` after `m7a_33_admission_cap_is_exactly_three`.
Forces the two values apart with an unrelated committed write (`driver.create` +
`discard_completions`, the same idiom `m7a_29` uses) before triggering the gap, then asserts the
resumed watch's `from` is not the stale pre-write cursor and does equal the post-write cursor.

Two proofs:
- Against the K7 mutant (still applied): `r2b_run6_k7_guard_vs_mutant.txt`. EXIT=101. My new test
  fails at line 555 ("manual_K7: the resumed watch starts after the snapshot revision, not a
  stale cursor"); **`m7a_28` itself still passes**, confirming it is blind to this mutation and
  the guard is the one doing the catching.
- Reverted to clean HEAD: `r2b_run7_k7_guard_clean.txt`. EXIT=0, "6 passed; 0 failed" — the new
  test passes on unmutated code.

### K8 — reset `watch_refused_attempts` to 0 on every termination, not only on success

Edit: in the `ResourceExhaustedFatal` arm of `on_terminated`, reset `watch_refused_attempts = 0`
immediately before the `saturating_add(1)` (so every refused termination resets-then-increments
to 1, instead of accumulating).

Risk noted before running: `m7a_33_admission_refused_backs_off_and_never_reloads`'s tail loop
(`while driver.kernel.watch_refused_attempts() < WATCH_ADMISSION_ATTEMPT_CAP { ... }`) is a real
Rust loop with no simulated-clock bound, so if the counter could never reach the cap this would
busy-spin forever. Ran under a hard 30s wall-clock `timeout` as a precaution.

Run: `r2b_run8_k8.txt`. Completed in 0.08s, no hang. EXIT=101, "4 passed; 2 failed":
- `m7a_33_admission_refused_backs_off_and_never_reloads` fails at line 379 *before* reaching the
  loop: "every refused termination is counted, so the re-arm can be bounded" — `left: 1, right: 2`
  (one `terminate()` call ends both watched families; the mutation makes the second family's
  reset-then-increment overwrite the first family's count within the same call, so the counter
  never accumulates past 1).
- `m7a_33_admission_cap_is_exactly_three` fails at line 476 with the equivalent message.
- `m7a_28`, `m7a_32` unaffected, still `ok`.

**CAUGHT.** No guard test needed. Reverted, `diff` clean.

### Final state

Run `r2b_run9_final.txt`: EXIT=0, "6 passed; 0 failed" on clean HEAD with the new `manual_k7_...`
test present. `diff` of the test file against the committed original shows only the appended
`manual_k7_resumed_watch_uses_snapshot_revision_not_stale_cursor` function — nothing else
changed. `authority.rs` and `control.rs` are both byte-identical to the `f22aa44` originals
(confirmed via `diff` against `.orig` backups, then backups deleted).

## Verdict: THUMBS UP
- Basis: f22aa44
- Scope tested: `crates/rdb-core/src/authority.rs` (rows M7A-28, M7A-29, M7A-32,
  `m7a_33_admission_refused_backs_off_and_never_reloads`, `m7a_33_admission_cap_is_exactly_three`)
  and `crates/rdb-core/src/contracts/control.rs` (`WatchTermination::is_gap`), against
  `crates/rdb-sim/tests/authority.rs`. 8 mutations total across both rounds (K1–K8): 6 CAUGHT
  outright (K1, K2 ×2 rounds, K3, K5, K6, K8); K4 was MISSED in round 1 and is now CAUGHT by the
  committed guard `m7a_33_admission_cap_is_exactly_three`, re-proven in round 2; K7 was MISSED by
  the existing suite and is now guarded by `manual_k7_resumed_watch_uses_snapshot_revision_not_stale_cursor`,
  proven both ways (fails on the mutant, passes clean).
- Not covered: the §3 authority gates, `Fence` and `PublishAuthorityView` are not implemented in
  this build (`authority.rs` module header, `CapabilityState::Unavailable`) and have no rows —
  nothing to mutate. The `watch_refused_attempts` counter being shared across all watched
  families rather than tracked per-family (surfaced by the K4/K8 work) is a design question
  logged for the architect, not a defect: every row's own claim about the counter's behavior
  still holds under mutation testing, including the newly-guarded K7 and K8 cases.
