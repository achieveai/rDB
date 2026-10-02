# Kernel-a A1 reach — manual tester handoff

Date 2026-09-22. Tree basis HEAD `395d535`, branch `feature/rdb-m7`.
`crates/rdb-core/src/authority.rs` was **clean at HEAD** throughout (unmodified in `git status`).

## VERDICT

**SPLIT. NOT YET for the package A1 is advertised as; THUMBS UP for a narrow, named list.**

- **NOT YET** — every fence, epoch, generation, config-version, gate and clock row.
  Not because the harness bypasses `Dispatcher::ctx_for`. Because **`StepCtx` is not an input
  to `Authority::step` at all**. Measured, not inferred (probe 1 below).
- **THUMBS UP** — rows whose whole claim is a function of the `ControlEvent` sequence and the
  three accessors `state()`, `cursor(prefix)`, `watch_refused_attempts()`. That harness has real
  discriminating power: five separate source mutations turned it red. List in §5.

## 1. The brief's claims, checked

| # | Brief said | Verdict |
|---|---|---|
| 1 | `authority.rs` has a real `Module::step` for the §2.4 watch/resync slice; gates, fence, pushed view not wired | **Confirmed.** `authority.rs:318-339`. Module header `:7-27` says so. |
| 2 | `tests/authority.rs:105` calls `.step(&support::ctx(), &event)`, bypassing `Dispatcher::ctx_for` (`dispatch.rs:185`) | **Confirmed**, exact line. |
| 3a | `support::ctx()` (`support/mod.rs:122`) freezes `control_time` | **Confirmed**, `:125-130`. |
| 3b | It freezes the authority triple, so no fence/epoch row is writable through that path | **Conclusion right, diagnosis wrong — and the correction matters.** See §2. |
| 4 | 6 `m7a_` functions on disk, dashboard says 4 | **Confirmed**, and the truth is worse than a count mismatch. See §4. |

## 2. The binding constraint is in `src/`, not in the test harness

`crates/rdb-core/src/authority.rs:331`

```rust
fn step(&mut self, _ctx: &StepCtx<'_>, event: &Event) -> Result<Vec<Effect>, RdbError> {
```

`_ctx` is underscore-prefixed and there is **no other occurrence of `ctx` in the file**
(`grep -n "ctx" crates/rdb-core/src/authority.rs` returns that one line).

Three consequences the brief's framing would have missed:

1. **The landed rows are not vacuous-by-constant. They are silent.** None of the six asserts on
   `generation`, `owner_epoch`, `config_version` or clock staleness — because the code under test
   cannot read them. So no row reports coverage it lacks *on this axis*. (There is a different,
   real mislabelling problem — §4.)
2. **Routing the rows through `Dispatcher::ctx_for` buys nothing.** Measured: probe 1 drove the
   whole wired slice twice — once with `support::ctx()`, once through `Dispatcher::step` with
   `generation 9999 / owner_epoch 4242 / config_version 777` and a clock skewed 86 400 000 ms with
   `bound_established = false` — and asserted the two `Vec<Vec<Effect>>` equal. **They are equal.**
   The probe first asserts the perturbation actually reached `ctx_for`, so it is not a no-op test.
3. **It would in fact be a regression to switch.** `support::ctx()` hands A1 the triple `(1,1,1)`;
   `ctx_for` hands it `Adopted::default()` = `(0,0,0)`, because `adopted` is populated only from
   `EffectKind::AdoptAuthority` in `Dispatcher::deliver` (`dispatch.rs:250-262`) and **A1 never
   emits `AdoptAuthority`** — it emits only `EffectKind::Control` (`authority.rs:148`; header
   `:10`; `authority.rs:310-313` says the adopt path "is not in this slice"). Either way it is a
   constant.

**The harness side is not the gap.** `Dispatcher` already exposes everything a clock/epoch row
needs — `clock_mut()` (`dispatch.rs:169`), `ctx_for` (`:185`), `adopted()` (`:149`) — and
foundation row `m7f_09_the_dispatcher_fills_the_authority_triple_from_the_last_adoption`
(`crates/rdb-sim/tests/dispatch.rs:94`) already proves a test can seed the triple by handing
`deliver` a hand-built `AdoptAuthority` effect. It is on disk and green. The consumer is missing,
not the producer.

## 3. Deletion probes — what has no test

Method: mutate `crates/rdb-core/src/authority.rs`, run `cargo test -p rdb-sim --test authority`,
restore from a backup, repeat. Every mutation was verified to have actually applied (`cmp` against
the backup) before the run — a silently-failed `perl -pi` would otherwise read as GREEN.

**GREEN = the suite passed with the guard broken = no test covers it.**

| # | Mutation | Result |
|---|---|---|
| M1 | `on_terminated` `if termination.is_gap()` → `if false` | RED (3 rows) |
| M2 | cap test `>=` → `>` (`authority.rs:197`) | RED (`m7a_33_admission_cap_is_exactly_three`) |
| M3 | adoption drops `Self::family_of(*key) != ControlPrefix::Grants` (`:259`) | **GREEN — NO TEST** |
| M4 | adoption drops `self.state != AuthorityState::Unheld` (`:258`) | **GREEN — NO TEST** |
| M5 | remove the Held gate `_ if self.state != AuthorityState::Held` (`:282`) | **GREEN — NO TEST** |
| M6 | `WatchProgress` no longer moves the cursor (`:294`) | **GREEN — NO TEST** |
| M7 | `on_watched` no longer moves the cursor (`:166`) | **GREEN — NO TEST** |
| M8 | `NotLeader`/`Unavailable` no longer re-arm the watch (`:211-219`) | **GREEN — NO TEST** |
| M9 | resumed watch uses `snapshot_revision + 1` (TD-18) (`:241`) | RED (2 rows) |
| M10 | `on_watched` reads only the **first** change of a run (`:167-170`) | **GREEN — NO TEST** |
| M11 | gap reload names the wrong family (`Reload{Routes}`) (`:190`) | RED (2 rows) |
| M12 | `capability()` → `Wired` (`:328`) | RED, but **not by an A1 row** — caught by `m7f_01` and `m7v_82` |
| M13 | non-control event returns `Ok(vec![])` instead of `Unavailable` (`:334`) | RED, foundation rows again |
| M14 | `on_watched` no longer resets `watch_refused_attempts` (`:165`) | **GREEN — NO TEST** (this is planned row **M7A-164**, not on disk) |
| M15 | `on_family_snapshot` no longer resets it (`:236`) | **GREEN — NO TEST** (M7A-164) |
| M16 | refusal counter never increments (`:196`) | RED (2 rows) |

**Dead code, reachable by nothing.** `AuthorityState::Fenced` (`authority.rs:83`) is never
constructed anywhere in `crates/` — `grep -rn Fenced --include=*.rs crates/` returns the enum
variant, two doc lines, and unrelated `AuthorityOutcome::Fenced` / `DenyReason::SelfFenced`
occurrences. Probe 3 drove the entire accepted event vocabulary and the state never left
`Unheld` / `Held`. **Fencing is the core of A1 and its terminal state has no producer.**

## 4. The real vacuity problem: four row ids credited, two claims tested

Not a clock problem — a labelling problem. The plan (`docs/testing/test-plan-m7-kernel-a.md`
lines 366-371) and the file disagree about which row each function is.

| On disk (`crates/rdb-sim/tests/authority.rs`) | Claims | Plan row of that id | Actually matches |
|---|---|---|---|
| `:412 m7a_28_gap_termination_reloads_then_rewatches` | M7A-28 | `watch_gap_revision_compacted_read_family_and_rewatch`, input **`RevisionCompacted{minimum_available_revision}`** | **M7A-29.** Line 418 sends `WatchTermination::ResourceExhaustedResumable` |
| `:524 m7a_28_resumed_watch_uses_the_snapshot_revision_not_the_stale_cursor` | M7A-28 | as above | M7A-29 again (`:548`, same termination) |
| `:242 m7a_29_watched_run_reads_each_change_and_never_reloads` | M7A-29 | `watch_gap_lagged_resumable_read_family_and_rewatch` — a **gap** row | Neither. It is a healthy-watch row; §2.4 property 3 / M7A-32 arm 1 territory |
| `:294 m7a_32_no_read_family_without_a_termination` | M7A-32 | matches, both arms | **M7A-32 — the one clean match** |
| `:355 m7a_33_admission_refused_backs_off_and_never_reloads` | M7A-33 | `watch_resync_state_equals_uninterrupted_watch` — **two kernels**, equal `served` / `revoked_epochs` / `partitions_revision` | **M7A-31** (backoff) + **M7A-129** (zero reloads) |
| `:467 m7a_33_admission_cap_is_exactly_three` | M7A-33 | as above | **M7A-31** (the cap) |

Consequences:

- **M7A-28's own claim is untested.** Plan line 366 requires `RevisionCompacted` *and* a fixture
  change placed at exactly `r + 1`, so a `from: r+1` spelling is red. No landed row sends
  `RevisionCompacted` as the subject of the two-step assertion (`m7a_32` arm 2 sends it but
  asserts only a count), and no landed row places a change at `r + 1`. M9 did go red, so the
  off-by-one is caught incidentally through the cursor-equality assertion — but the seam claim
  (a change at `r+1` is not dropped) is not tested.
- **M7A-33's claim is untested entirely.** It is a two-kernel resync-equivalence row over
  `served`, `revoked_epochs`, `partitions_revision`. **None of those three exist on `Authority`.**
  Two functions carry the id; neither goes near it.
- **M7A-29's claim is tested, under the wrong name.**
- §12 coverage counting and `docs/progress/src/parts.json:54` ("4 of 193 rows on disk") both read
  these ids at face value. 6 functions, 4 distinct ids credited, **2 claims actually tested**
  (M7A-32, and M7A-29 mislabelled).
- **M7A-31 is not writable as specified.** Plan line 369 asserts effects
  `[Fact(AdmissionRefused), Timer(backoff_n)]` with `backoff_n` non-decreasing and
  `backoff_20 == cap`. The module emits a `ControlEffect::Watch` re-arm and no `Fact` and no
  `Timer` (`authority.rs:195-208`), there is no `AdmissionRefused` in `AuthorityIgnoreReason`
  (`contracts/ignore.rs:111-143`), and `Dispatcher::deliver` refuses `EffectKind::Kernel` and
  `EffectKind::Timer` outright (`dispatch.rs:272-286`). This is a plan-vs-code divergence for the
  plan owner; I did not edit `docs/testing/*`.

## 5. THUMBS UP — rows that may be written now

Reachable, and the deletion probes prove the harness can falsify them.

1. **M7A-28, as the plan actually specifies it** — `RevisionCompacted`, two steps, `from: r` not
   `r+1`, with a committed change placed at exactly `r+1` so the `+1` spelling is red.
   Rename the two existing `m7a_28_*` functions to `m7a_29_*` first; they are M7A-29.
2. **M7A-29** — the existing `m7a_28_gap_termination_reloads_then_rewatches`, renamed. No code change.
3. **M7A-32** — already landed and genuine. Leave it.
4. **M7A-129** — zero reloads across N `ResourceExhaustedFatal`, then exactly one for a
   `ResourceExhaustedResumable` control. Covered in substance by `m7a_33_admission_refused_*`;
   split it out under its own id so §12 credits the right row.
5. **M7A-164** — the refusal-counter reset row. M14 and M15 prove nothing tests it today.
6. **New rows the probes justify** (no plan id yet; ask the planner):
   - a committed CAS on a **non-grant** family must not adopt (M3);
   - a CAS committing while already `Held` must not re-adopt (M4);
   - an **`Unheld`** kernel must emit nothing for any watch event (M5) — the documented rule at
     `authority.rs:72-74`, and nothing tests it;
   - `WatchProgress` moves the cursor and nothing else (M6);
   - a `Watched` run moves the cursor (M7);
   - `NotLeader` / `Unavailable` re-arm from the cursor and never reload (M8);
   - **one** `Get` per change, not one per run (M10) — `m7a_29` uses `.contains`, which a
     read-only-the-first-change kernel passes.

**Rename before adding.** Adding rows on top of the current ids buries the mislabelling.

## 6. NOT YET — what cannot be written, and the entry point each needs

| Blocked | Missing entry point | Unblocks |
|---|---|---|
| Every clock-bound / sample-staleness row | `Authority::step` must **read** `ctx.control_time` and act on `ControlTime::compare` / `ClockVerdict::Uncertain`. The producer already exists (`Dispatcher::clock_mut`, `ctx_for`) | the ~14 rows `risks.json:46` counts, and the §3 gate rows |
| Every epoch / generation / config-version / fence row | (a) `Authority::step` must read `ctx.{generation, owner_epoch, config_version}`; (b) A1 must **emit `EffectKind::AdoptAuthority`** so the triple can change across steps under the dispatcher. `Dispatcher::deliver` already absorbs it — no harness work needed | §11's "every `Fence` row", `M7A-51..57`, `M7A-149..151` |
| `AuthorityState::Fenced` rows | a transition that sets it. Nothing does | the A1 state-machine rows M7A-01..27 |
| **M7A-33** as specified | `served`, `revoked_epochs`, `partitions_revision` accessors on `Authority`. None exist | M7A-33 |
| **M7A-31** as specified | a `Fact` carrier (`KernelEffect::Ignored` plus an `AdmissionRefused` reason) and a `TimerEffect` back-off — **or** a plan rewrite onto the landed `Watch` re-arm shape | M7A-31 |
| M7A-123, M7A-126 | foundation **H1** (the control-fake conformance suite); M7A-126 also needs `ControlKey::Operation` staging in the fake. §11 lists M7A-130 on H1 but names M7A-123/126 as runnable — not re-derived here | M7A-123, M7A-126 |

Note that §11 line 974 names **M7A-31** as having a subject to run against. Per §5 it does not,
and **M7A-33** is named there too and has no subject either. Two of the seven ids §11 cleared are
not actually clear.

## 7. Commands and observed results

```sh
export CARGO_TARGET_DIR=$PWD/.rtargets/ka-test CARGO_INCREMENTAL=0
export RETCD_TEST_DEADLINE_SCALE=3 RETCD_TEST_LOG_DIR=$PWD/.rtargets/ka-test/test-logs
cargo test -p rdb-core -p rdb-sim --no-fail-fast > /tmp/ka.log 2>&1; echo $? > /tmp/ka.rc
```

- Baseline before any change: `/tmp/ka.rc` = **0**. 16 test binaries, all `test result: ok`.
  `tests/authority.rs` = 6 passed.
- Probe run: `cargo test -p rdb-sim --test zz_ka_reach_probe` → **5 passed**, including
  `probe_ctx_is_not_an_input_to_a1`.
- 16 mutations, each restored immediately; results in §3.
- Final: probe file deleted, `sha256sum crates/rdb-core/src/authority.rs` =
  `65dad40bcb6a23ddd4d8595d01d06f71e194188024a167739d9215800f79df44`, **identical** to the
  pre-probe value. `git status --short crates/rdb-core/src/authority.rs` → empty.
  Full re-run after cleanup: `/tmp/ka_final.rc` = **0**, 16 binaries ok.

## 8. What I changed and restored

- Created and then **deleted** `crates/rdb-sim/tests/zz_ka_reach_probe.rs` (5 probes; not plan rows).
- `crates/rdb-core/src/authority.rs`: 16 temporary mutations, each reverted from a backup within
  the same command. **Net change: zero**, verified by sha256 and `git status`.
- No file under `docs/` was edited. `sim.rs`, `replay.rs`, `seams.rs`, `storage.rs`,
  `dispatch.rs` and `support/mod.rs` were **read only**. No git write of any kind.

## 9. Assumptions and risks

- The row-id mapping in §4 is read from the plan's row table at lines 366-371 of
  `docs/testing/test-plan-m7-kernel-a.md`. That file is `M` in `git status` — a third agent is in
  it. Re-check the table before acting; the mapping may move.
- Mutation testing shows a guard has **no** test. It does not show the guard is correct.
- M12 and M13 went red via foundation rows only. If those rows move, A1 loses that cover silently.
- I did not re-derive foundation H1's state for M7A-123/126; §11's claim there is unchecked.

## 10. Recommended status

**BLOCKED for package A1 as advertised** (gates, fence, epochs, clock — `capability()` at
`authority.rs:328` still correctly reports `Unavailable`).
**IN PROGRESS, rows may be written, for the watch and coherent-resync slice** — the six in §5,
after the rename in §4. Do the rename first.
