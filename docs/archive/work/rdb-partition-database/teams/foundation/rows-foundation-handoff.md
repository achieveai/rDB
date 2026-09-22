# Rows handoff — M7 foundation

Author: `rows-foundation`. 2026-09-22. Branch `feature/rdb-m7`. Basis `395d535` (working tree).

> **OUTCOME: COMPLETED.**
> **9 rows written of 9 attempted.** Nine more were considered and **declined**; §4 says which and
> why, and that list is the part of this handoff I would read first.
> `cargo test -p rdb-core -p rdb-sim --no-fail-fast` → **`CARGO_EXIT=0`**, 16 binaries, 169 tests,
> 0 failed, 0 warnings. `m7f_26` is green and its seven-string set has not moved.
>
> **The plan says 23 rows remain, not 72.** §6, finding F-B.

---

## 1. The rows

| Row | Function | File | Class |
|---|---|---|---|
| M7F-23 | `m7f_23_network_send_is_unavailable_and_names_itself` | `crates/rdb-sim/tests/sim.rs` **(new)** | unit |
| M7F-24 | `m7f_24_cluster_suspend_is_unavailable_and_names_itself` | `crates/rdb-sim/tests/sim.rs` | unit |
| M7F-43 | `m7f_43_a_stale_timer_version_never_fires` | `crates/rdb-sim/tests/sim.rs` | unit |
| M7F-47 | `m7f_47_the_scheduler_order_is_total_over_tick_and_event_id` | `crates/rdb-sim/tests/sim.rs` | unit |
| M7F-25 | `m7f_25_harness_replay_is_unavailable_and_names_itself` | `crates/rdb-sim/tests/replay.rs` **(new)** | unit |
| M7F-36 | `m7f_36_authority_seq_is_on_the_decision_the_view_and_the_trace` | `crates/rdb-core/tests/seams.rs` | unit |
| M7F-37 | `m7f_37_external_fence_verified_carries_its_six_binding_fields` | `crates/rdb-core/tests/seams.rs` | unit |
| M7F-40 | `m7f_40_append_reject_names_every_ladder_row_once` | `crates/rdb-core/tests/seams.rs` | unit |
| M7F-41 | `m7f_41_the_header_carries_no_bare_seed` | `crates/rdb-core/tests/seams.rs` | unit |

Both new files are the ones the plan's file-mapping table names as **new** and assigns to
foundation. Nothing under `crates/config-*` was touched, no `drift-basis` marker was moved, nothing
was committed or pushed, and no `Unavailable` seam was opened.

## 2. What turns each row red

Every row carries this line in its own doc comment. Summarised so the lead does not have to open
four files:

| Row | The production change that turns it red |
|---|---|
| M7F-23 | a `send` that records the frame before refusing — pushes onto `in_flight`, consumes the `PlanNext`, or burns a `MessageId`; or the seam string changing |
| M7F-24 | a `suspend` that marks the node stopped or rolls its boot before refusing; a `start` that reuses a `BootId`; or the seam string changing |
| M7F-43 | making `Clock::cancel` unconditional on the version; `next_deadline` answering the most recently armed tick rather than the least; `due` reporting the superseded version |
| M7F-47 | a queue ordered by insertion or by `at` alone; a `now` that does not follow the popped tick; dropping the `at < now` guard; dropping the duplicate-key guard; a repeating `next_event_id` |
| M7F-25 | `replay` answering **any** `ReplayOutcome` without a runner behind it; the seam string changing; a fourth outcome added (the `match` has no `_` arm, so it stops compiling) |
| M7F-36 | `authority_seq` dropped from the decision, the view or the trace event; `past_horizon` retyped off `DenyReason`; an `impl From<..>` appearing in `ids.rs` between the three generation-shaped newtypes |
| M7F-37 | any of the six binding fields dropped or renamed; the variant moved off `EventKind`; a **ninth** `EventKind` variant (`event_kind_name` has no `_` arm) |
| M7F-40 | a **seventeenth** `AppendReject` variant (no `_` arm); one of the sixteen renamed or removed; a second spelling of a ladder row |
| M7F-41 | a convenience `seed`, `config_digest` or top-level `budgets` added to `TraceHeader`; `#[serde(deny_unknown_fields)]` dropped |

**Two of these are not hypothetical.** M7F-40 failed twice while I wrote it (§5), and the second
failure was a wrong claim of mine that the row refused to let through.

## 3. Criterion → evidence

| Criterion | Evidence |
|---|---|
| Each row asserts observed behaviour | M7F-23/24/25 are the tester's §3 door table and C5, with the *state* half added. M7F-43 and M7F-47 are `Clock` and `Scheduler`, which the tester drove through the composed loop (§2a) and which the landed `m7f_47_*` pair partly covers. M7F-36/37/40/41 are `rdb-core` values built and read back, the same shape as the tester's A1/A3/A5. |
| No row is vacuous | §2. **No row asserts on a module's effects, on a trace, or on `support::ctx()`** — the tester's §2b measurement and condition 1 are honoured by construction: none of the nine touches the clock-to-`StepCtx` path at all. |
| Rows are green | `cargo test -p rdb-core -p rdb-sim --no-fail-fast` → `CARGO_EXIT=0` read from a file, 16/16 binaries `ok`, **169 passed, 0 failed**. The tester's pre-existing baseline was 14 binaries / 160 tests; +2 binaries and +9 tests is exactly this work. |
| Nothing else moved | `git status crates/` lists `M crates/rdb-core/tests/seams.rs` plus `?? crates/rdb-sim/tests/{sim,replay}.rs` as the only entries that are mine. `crates/rdb-sim/tests/dispatch.rs` is untouched, so `m7f_26`'s seven-string assertion is byte-identical; it ran and passed. dev-reach's six mechanical edits inside `seams.rs` are intact (verified by grep for all three of their changed lines). |
| No new warnings | `cargo clippy -p rdb-core -p rdb-sim --all-targets -- -D warnings` → exit 0, zero warnings. `rustfmt --edition 2021 --check` on the three files → exit 0. I ran `rustfmt` on **only my three files**, never `cargo fmt --all`, because `--all` would reformat other agents' uncommitted work in this tree. |

## 4. Rows I declined, and why — read this list

### 4a. Declined because the row would be vacuous

**M7F-05** `m7f_05_one_recorded_stream_replays_byte_identically_twice`. Everything it can assert
today is `replay(&trace)` → `Unavailable`, which is **M7F-25's** assertion. I could not name a
production change that turns M7F-05 red without also turning M7F-25 red: a `Trace` from `Recorder`
and a hand-built `Trace` are the same type, and `replay` takes `&Trace`. I considered giving it a
`Recorder` → `write_jsonl` → `read_jsonl` → `compare_traces` round trip to earn its keep, and did
not, for two reasons: `M7F-11` already owns that round trip, and the tester's condition 3 forbids a
row citing `compare_traces` as replay or determinism evidence. **Write M7F-05 with the runner, as
one piece of work.** Its gate-map line is unmet either way — a row asserting `Unavailable` was never
going to meet an H1 acceptance claim.

### 4b. Declined because writing them means building production code

**M7F-30 … M7F-35**, six rows, the I1 trace validator. `harness::trace::validate(&Trace) ->
Result<(), TraceDefect>` does not exist — `harness::trace` has `Recorder`, `write_jsonl` and
`read_jsonl` and nothing that checks a trace's shape. Writing these rows means designing and landing
a new public surface in `rdb-sim`, which is package I1 and outside a row-writing brief. Note for
whoever funds it: `M7V-88` is partly vacuous until they exist, and §18 Q-4 records the lead
authorising the surface on 2026-09-20, so this is a queued package and not a dead row.

### 4c. Declined because it is not a cargo row

**M7F-42** `m7f_42_rdb_core_has_no_live_io_and_no_unordered_iteration`. `script` class: a gate stage
and two greps run by `scripts/gate.{sh,ps1}`, on both scripts because the repository is
agent-neutral. Editing the gate scripts in a tree three other agents are building in is a different
kind of change from writing a row, and a broken stage stops everyone. Recommend it be dispatched as
a tooling task with its own brief.

### 4d. Not written for budget — all four are buildable and none is vacuous

Ranked by what I would write next:

1. **M7F-29** `m7f_29_the_record_preimage_has_exactly_eleven_parts` (`contracts.rs`). Fourteen
   assertions, no golden: eleven mutations move the digest, three leave it unchanged. It is the
   completeness check the four landed `M7F-02` vectors cannot make — **a twelfth field silently
   added to the preimage passes every existing vector.** That is a real, nameable red-making
   change, and it is the highest-value row left on the board.
2. **M7F-45** `m7f_45_every_crash_boundary_yields_a_whole_batch_or_none` (`storage.rs`). Enumerates
   both crash kinds in the body, so a third `StorageFault` crash kind added without a row fails the
   match.
3. **M7F-46** `m7f_46_sync_wal_through_answers_under_the_write_order` (`storage.rs`). The twin of
   the landed `m7f_18(a)`: there the engine is short by injection, here by ordering.
4. **M7F-44** `m7f_44_a_snapshot_never_sees_a_partial_batch` (`storage.rs`). Partly shadowed by the
   landed `m7f_06_a_failed_commit_keeps_none_of_a_multi_write_batch`; check the overlap before
   writing.
5. **M7F-38** arm 1 only (`seams.rs`). Arm 2 is held on CB-5 and `BlockReason` still has one
   variant, so arm 2 is not spellable. Arm 1's positive control — a `Blocked` whose `reason` key is
   removed on the wire must be **refused**, naming `reason` — is the part worth having.

### 4e. Not touched on purpose

**M7F-26**. Landed as `m7f_26_every_unbuilt_seam_refuses_by_its_own_name`. Green, unmoved, and the
new rows are deliberately not a second copy of it (§6, F-D).

## 5. Exact commands and what I observed

Private target directory throughout, `CARGO_TARGET_DIR=$PWD/.rtargets/rows-foundation`, with
`CARGO_INCREMENTAL=0` and `RETCD_TEST_DEADLINE_SCALE=3`. **Every exit code was written to a file by
`echo "CARGO_EXIT=$?" > file` as the last statement**, never read from a pipeline.

```
cargo test -p rdb-sim --test sim --test replay --no-fail-fast    CARGO_EXIT=0   5 passed
cargo test -p rdb-core --test seams --no-fail-fast               CARGO_EXIT=101 E0505 (fixed)
cargo test -p rdb-core --test seams --no-fail-fast               CARGO_EXIT=101 m7f_40 FAILED
cargo test -p rdb-core -p rdb-sim --no-fail-fast                 CARGO_EXIT=101 m7f_40 FAILED
cargo test -p rdb-core -p rdb-sim --no-fail-fast                 CARGO_EXIT=0   16 binaries, 169 passed
cargo clippy -p rdb-core -p rdb-sim --all-targets -- -D warnings CARGO_EXIT=0   0 warnings
rustfmt --edition 2021 --check <the three files>                 RUSTFMT_EXIT=0
cargo test -p rdb-core -p rdb-sim --no-fail-fast  (after fmt)    CARGO_EXIT=0   16 binaries, 169 passed
```

I did **not** run the full workspace gate, as instructed: the `config-*` evidence rows are red on
this host for the documented Windows UDP port-exclusion reason, which is not M7's.

### The two red runs, because a failed check is the useful part

1. **E0505, a borrow error of mine** in `m7f_41` — `keys` borrowed the JSON object I then moved.
   Fixed by taking the count before the move. No behaviour involved.
2. **`m7f_40` failed twice, and the second failure was the row doing its job.** My literal list had
   `UnknownEpoch` before `Unauthenticated`, which is not sorted order — a typo, caught immediately.
   Then it failed on an assertion I had written as a *claim about the design*:

   ```
   assertion `left != right` failed
     left: String("NotAMember")   right: String("NotAMember")
   ```

   I had asserted `AppendReject::NotAMember` and `AckRejectReason::NotAMember` serialise
   differently. **They do not — the bare wire tags are byte-identical.** That is not a defect; it is
   the fact CB-7's carrier exists for. What separates them on the wire is the arm, so
   `{"AppendRejected":"NotAMember"}` against `{"AckRejected":"NotAMember"}`. The row now asserts
   both halves: the bare tags **collide** (so uniqueness is per enum, never global) and the
   `KernelIgnoredReason` forms **differ**. This is the tester's A3/A5 result reached from the other
   side, and I would have shipped the wrong claim if the row had been weaker.

## 6. Findings

### F-A (MATERIAL) — two landed functions carry the `m7f_47_` prefix, and one of them is M7F-43's claim

`crates/rdb-sim/tests/dispatch.rs:580` `m7f_47_two_events_at_one_tick_pop_in_ascending_event_id_order`
and `:618` `m7f_47_a_timer_rearmed_at_the_same_or_lower_version_is_refused`.

The second asserts `Clock::arm`'s version guard. That is **plan row M7F-43**'s third clause, not
M7F-47's — M7F-47 is the scheduler row. Both landed on 2026-09-21 from the manual tester's findings
F2 and F3, **after** the plan was written (2026-09-20), so the plan still marks M7F-43 and M7F-47
**owed** and §17's "60 landed" is two low.

- **Not anyone's mistake to fix in a hurry.** §18 Q-2 already records the cost of renaming a landed
  test: it breaks the `@m` values already written into the JSONL logs, which §12's DuckDB queries
  then miss **silently**. I did not rename either function.
- **What I did instead:** wrote only the clauses neither landed function covers, and said so in
  each row's doc comment and in each file's module header. M7F-47 here owns the cross-tick order,
  `now`'s monotonicity and the two `SimError::Config` refusals; M7F-43 here owns cancel-then-rearm,
  the no-op cancel and `next_deadline`'s minimum.
- **Closure condition:** the plan's §17 counts and its **owed** markers for M7F-43 and M7F-47 are
  re-read against `dispatch.rs`, and §18 grows a Q on whether the timer function keeps its
  `m7f_47_` name. Plan edit, not a code edit. **This is the lead's file, not mine.**

### F-B (ADVISORY) — "72 foundation rows remain" is not the plan's number

The brief says 72. `docs/testing/test-plan-m7-foundation.md` §17 counts **56 rows / 83 test
functions, 60 landed, 23 owed**. I enumerated the 23 by grepping `**owed**` and got the same set.
Two of those 23 are partly covered by F-A's functions. I wrote 9 and declined 9 of the remaining
14, so what is genuinely left after this pass is **5 rows** (§4d) plus the six-row validator
package and one gate stage.

### F-C (ADVISORY) — `Network::next_message` has no accessor

M7F-23's plan text asks the row to assert that "the next `MessageId` the network would hand out is
the same as before the call (no id was burned)". `Network::next_message` is a private field with no
getter, so no named method answers it. I asserted it through the derived `Debug` rendering of the
whole `Network` value, compared before and after — which also catches any other field moving, and
catches a field this row does not know about. The cost is that renaming the field changes the
string on both sides, so the row stays green through a rename; it is only a burn that turns it red,
which is what the clause is for. If a named accessor is preferred it is one line
(`pub const fn next_message_id(&self) -> MessageId`), and that is a production change I did not
make.

### F-D (ADVISORY) — three of the nine rows sit next to `m7f_26`, and the line between them is the state assertion

`m7f_26` already drives `Network::send`, `Cluster::suspend` and `replay`. It asserts **only the seam
string**. M7F-23, M7F-24 and M7F-25 add the half that has no other row: nothing moved, the near-miss
twin works, and the refusal is not conditional on an empty input (`m7f_26` hands `replay` an
**empty** `Trace`; M7F-25 hands it a populated one). Each file's module header states this in as
many words, so a later reader does not read them as a copy. If the lead disagrees and wants them
folded into `m7f_26`, that is a merge, not a rewrite — but it would put five distinct claims in one
function.

## 7. Assumptions

1. **Creating `crates/rdb-sim/tests/sim.rs` and `tests/replay.rs` is mine.** The plan's file-mapping
   table names both as **new** and assigns them to foundation. Two new test binaries is the cost;
   `m7f_26` clause 3 greps `crates/rdb-sim/src`, not `tests/`, so it is unaffected — checked, and
   it passed.
2. **Appending to `crates/rdb-core/tests/seams.rs` is mine.** The same table maps M7F-36/37/40/41
   there. That file carries dev-reach's uncommitted mechanical edits; I appended and widened the
   import block, and verified all three of their changed lines survive.
3. **M7F-36's "no conversion" clause reads `ids.rs` from the test** rather than shelling out to
   `grep`, so the row runs the same way on every host. It is the same device `M7F-26` clause 3 uses
   and §18 Q-6 rules acceptable.

## 8. Risks

1. **F-A's naming collision will confuse the next reader of the plan** before it confuses a build.
   Three functions now begin `m7f_47_` and two of them test different subjects.
2. **M7F-23's `Debug`-string comparison is the weakest assertion in the nine.** It is whole-value
   and therefore broad, but it is also a string, so a field rename passes it silently. F-C names the
   one-line alternative.
3. **The tree is shared and these results are point-in-time.** `CARGO_EXIT=0` describes the working
   tree at the moment of the run, not HEAD. Two other agents have uncommitted `rdb-*` changes in it
   (dev-reach's, and whoever owns `config-storage/src/rocks.rs`).
4. **Nothing here exercises the clock.** That is deliberate — the tester's §2b showed that a row
   perturbing skew and asserting on anything downstream of a module passes with the clock wired and
   unwired — but it means the CB-9b wiring still has no **row**, only the three scaffolding tests in
   `src/`. The first real clock row must go through `Dispatcher::ctx_for`, and finding F-1 in the
   tester's handoff (`authority.rs:105` bypassing it) has to be routed before kernel-a writes one.
