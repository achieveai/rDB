# dev-verification-r2 — correction round 1 handoff

Agent: `dev-verification-r2`. Date: 2026-09-21. Branch `feature/rdb-m7`, shared working tree.
Scope: close review findings F1 and F2, disclose F4. **F3 ignored** — withdrawn by the reviewer
(another agent's uncommitted work-in-progress, misattributed). No git operations performed.

## Outcome

**COMPLETED.** F1 closed with a falsification demonstration. F2 closed by keeping both clauses and
adding trip + near-miss fixtures, after checking the shadowing question. F4 disclosed in three
places. Five advisories also closed; one left open with a reason.

Row count went 80 → **83** (the three new INV-LIN rows). All green, exit 0.

---

## F1 — vacuous assertion in M7V-82(a) — CLOSED

`Wired` and `Defaulted` in `crates/rdb-sim/tests/campaign.rs` now
`impl rdb_core::contracts::event::Module`. `Wired` overrides `capability()`; `Defaulted`
deliberately does not, so its assertion is answered by the trait's own default in `rdb-core`.
The call sites use UFCS (`Module::capability(&Wired)`) so a future inherent method cannot silently
shadow the trait method and re-introduce the defect.

### The demonstration, in three runs

All three: `CARGO_TARGET_DIR=.rtargets/verification-r2 CARGO_INCREMENTAL=0 cargo test -p rdb-sim --test campaign`.

**(1) Default flipped in `rdb-core` (`Unavailable` → `Wired`) — row RED, exit 101:**

```
thread '...m7v_82_capability_state_is_derived...' panicked at crates\rdb-sim\tests\campaign.rs:239:5:
no kernel package is wired in M7, so a Wired row here is a misreported cause: [Unavailable, Wired, Wired, Wired, Wired, Wired]
test result: FAILED. 0 passed; 1 failed;
EXIT=101
```

This is honest but **not sufficient**: it panics at clause 1 (the dispatcher report, line 239),
which the flip also turns red, so it says nothing about F1's clause. Run 2 isolates it.

**(2) Default still flipped, clause 1 temporarily relaxed so execution reaches line 249 — RED at
F1's own clause, exit 101:**

```
thread '...m7v_82_capability_state_is_derived...' panicked at crates\rdb-sim\tests\campaign.rs:249:5:
assertion `left == right` failed
  left: Wired
 right: Unavailable
EXIT=101
```

Line 249 is `assert_eq!(Module::capability(&Defaulted), CapabilityState::Unavailable)`. This is the
exact clause the reviewer proved unfalsifiable. Pre-fix it was an inherent `const fn` returning the
literal `Unavailable`, so the flip could not reach it. It now reads the trait default and goes red.

**(3) Both temporary edits reverted — GREEN, exit 0:**

```
test m7v_82_capability_state_is_derived_from_the_modules_own_report_never_a_literal ... ok
test result: ok. 6 passed; 0 failed;
EXIT=0
```

**Both temporary edits are reverted and verified.** `git diff crates/rdb-core/src/contracts/event.rs`
contains no `CapabilityState` change line (the only match is an unchanged context line), and the
live default at `event.rs:451-453` reads `CapabilityState::Unavailable`. The remaining diff in that
file is kernel-a's work, not mine.

---

## F2 — two untripped INV-LIN clauses — CLOSED by keeping both, not deleting

### The ordering check first, because it decided the disposition

`cutoff_above_selected_source` (`lineage.rs:164`) fires when the **selected** source's
`reported_seq < selected_cutoff_seq`. `cutoff_below_an_available_recorded_prefix` fires when a
reachable source's `reported_seq > selected_cutoff_seq` with a matching recorded digest. On the same
source the two are mutually exclusive; across sources they can both hold, and the first returns
early, so it wins.

**M7V-18 is not shadowed today.** Its fixture selects N2 with `reported_seq = 9` against
`selected_cutoff_seq = 6`. `9 < 6` is false, so the first clause stands aside and M7V-18 reaches the
clause it names. Verified by reading the fixture at `oracle.rs:1508` and confirmed by the
falsification run below, where neutering the first clause left M7V-16..M7V-19 green.

But the shadow is **real for shapes M7V-18 does not cover**, and `Oracle::judge` keeps only the
first violation per invariant, so which rule a seed's signature carries is a fact about ordering.
That makes deletion the wrong call: the clause is correct, reachable, and load-bearing on ordering.
**Kept, and exercised.** The reviewer's counter-argument (extra safety the plan never asked for,
so deleting closes the finding) is fair on cost but trades a weaker oracle for a tidier table.

### What was added — `crates/rdb-sim/tests/oracle.rs`

Three rows, deliberately **without** M7V ids, because the plan does not own these clauses:

| Row | Covers |
|---|---|
| `lin_a_recovery_root_that_cites_no_predecessor_violates` | 3 trips (both fields missing, and each `\|\|` disjunct alone) + 2 near-misses: a recovery root citing both fields, and an *initial* root, which legitimately has neither — that one pins the `LineageSource::Recovery` scope |
| `lin_a_cutoff_above_the_selected_sources_prefix_violates` | trip at `reported 3 < cutoff 6`; near-miss at the **boundary** `reported == cutoff` (a `<=` in place of `<` fails it); near-miss for a source that reported nothing |
| `lin_the_two_cutoff_clauses_do_not_shadow_each_other` | both conditions true → asserts the first clause wins; selected source at the boundary → asserts the loop reaches the other source and M7V-18's rule fires |

`cutoff_above_selected_source` was also **added to `lineage.rs`'s module rule table** (it was
missing), the count corrected "Four clauses" → "Five", and the ordering dependency documented there
with a pointer to the shadow row.

### Falsification — the new rows are load-bearing

A row that passes first try proves nothing, so both clauses were temporarily neutered
(`if false && …` on each condition) and the rows re-run:

```
test lin_a_recovery_root_that_cites_no_predecessor_violates ... FAILED
  INV-LIN expected Violated{recovery_root_without_predecessor}, got Proven
test lin_a_cutoff_above_the_selected_sources_prefix_violates ... FAILED
  INV-LIN expected Violated{cutoff_above_selected_source}, got Proven
test lin_the_two_cutoff_clauses_do_not_shadow_each_other ... FAILED
  INV-LIN fired cutoff_below_an_available_recorded_prefix — detail: node 3 is reachable and
  reported generation 7 seq 9 with the digest already recorded there, above the selected cutoff 6
  left: "cutoff_below_an_available_recorded_prefix"  right: "cutoff_above_selected_source"
test result: FAILED. 5 passed; 3 failed;   EXIT=101
```

All three new rows go red; **M7V-16, M7V-16b, M7V-17, M7V-18 and M7V-19 stayed green**, which is
the independent evidence that these clauses had no prior fixture — exactly the gap F2 named. The
third failure message is also the direct evidence for the shadow claim: with the first clause
neutered, the contested trace falls through to node 3 and the other rule. `lineage.rs` restored and
verified (`grep -c "false &&"` → 0).

---

## F4 (review F4) — the eleven tier-2 runner lines — DISCLOSED, not built

Not built, per assignment: they cannot emit until **foundation's item 1**, the tier-1 `TraceEvent`
serialiser, lands. That work is in flight in this tree right now
(`crates/rdb-sim/src/harness/trace.rs` +209, `tests/harness.rs` +183 — foundation's, untouched by me).

**Measured, not grepped.** Fresh log root from my own completed run, writer exited, DuckDB with
`map_inference_threshold=-1`, 86 JSONL files:

```
lines 415 | methods 83 | modules 3        (campaign 6, oracle 58, scenarios 19)
@m vocabulary:  capability 249 | test finished 83 | test started 83
```

All eleven of `invariant_status`, `violation`, `capability_seen`, `coverage_cell`,
`coverage_shortfall`, `coverage_unavailable`, `shrink_step`, `shrink_result`, `campaign_run`,
`mutation_caught`, `trace_header` return **zero lines**. Verification emits no log line of its own.

Disclosed in three places, each naming foundation item 1 and Q-34/Q-38/Q-39/Q-40:

1. **`teams/verification/developer-handoff.md` §4 "Not touched (held)"** — a new paragraph stating
   the debt, the measurement, the blocker, and the two consequences: `invariant_status.seeds_armed`
   is the contract's own named defence against a vacuous pass and has no producer, and a zero-row
   Q-row result is indistinguishable from a clean run.
2. **`developer-handoff.md` §7** — was "3. No other asks". Now item 3 is the tier-1 serialiser
   named as a **dependency**, and "no other asks" renumbered to 4. The silence F4 was really
   about is gone.
3. **`docs/testing/test-plan-m7-verification.md`** — a held block in **VA-7** (the log-line
   contract, verification's own section) above the eleven-row table, and a warning at the head of
   **§10** telling a reader who runs Q-34 or Q-38..Q-40 to read zero rows as *unavailable*, never
   as *passing*. §10's note also covers Q-35..Q-37, dark for the same reason.

`scripts/gate.sh drift` exits **0** after the plan edit — I did **not** move the `drift-basis`
marker, because I did not re-read the contract table and AGENTS.md is explicit that the marker is
set after the re-read, never to clear a build.

---

## Advisories

| # | Item | Disposition |
|---|---|---|
| F7 | `std::env::set_var` in M7V-43 (anti-flake rule 6) | **Fixed.** Both `set_var` calls removed. Replaced with a serialisation-stability assertion on `second`, which the D4 fixture actually depends on. The environment claim is carried by the source-level grep already in the row — and that is the *stronger* statement: the behavioural version could only falsify the two names it happened to set; the grep falsifies any `std::env` read. Reasoning left in the row as a comment so it is not re-added. |
| F8 | `payload` field name in `grammar.rs` | **Fixed.** `payload` → `digest_id` in `grammar.rs` (2 fields) and `gen.rs` (4 sites). This mattered more than advisory: foundation's own field-discipline check at `tests/harness.rs:310` forbids a line key named exactly `payload`, and `Scenario` derives `Serialize`, so the first tier-2 line carrying a `ScenarioOp` would have failed that row. No JSON fixture referenced the old name. |
| F6 | M7V-85's name contradicts its `== 0` assertion | **Fixed.** Renamed `m7v_85_without_rule_has_exactly_one_call_site` → `m7v_85_without_rule_has_no_call_site_until_m7v_23`. No other reference in the tree. |
| F9 | Two `#[allow(dead_code)]` prose-only items | **Fixed.** `_partition_is_local` (`lag.rs`) and `type Holder` (`loss.rs`) deleted, prose moved into the module doc comments. Now-unused imports removed (`PartitionId`; `BootId`, `NodeId`). The `allow` no longer masks genuinely dead code in either module. |
| F5 | `ShrinkBudget::total` is per-call, not the aggregate critic F11 asked for | **Documented, deliberately not fixed.** Adding the accumulator would put cross-failure state inside `ddmin`, which the plan defines as per-signature, and nothing in `reduce.rs` sees more than one failure. The doc comment now states plainly that `BudgetSpent::Total` is unreachable from a single call at the defaults, that the decrement is the **I1 runner's** obligation, and that critic F11's "N failing seeds, uncapped" stays **open** until I1 carries it. This is a stated residual risk, not a closure — the lead should pin it in I1's contract. |

---

## Commands and real exit codes

Every run: `CARGO_TARGET_DIR=.rtargets/verification-r2 CARGO_INCREMENTAL=0`, a private target
directory, never two cargo invocations at once. Output written to a file and the file read, so
these are cargo's own codes, not a pipeline's.

| Command | Exit |
|---|---|
| `cargo test -p rdb-sim --test oracle --test scenarios --test campaign` (baseline, before any edit) | **0** — 80 rows |
| `cargo test -p rdb-sim --test campaign` (F1 fix) | **0** |
| `cargo test -p rdb-sim --test campaign m7v_82` (default flipped) | **101** |
| `cargo test -p rdb-sim --test campaign m7v_82` (flipped + clause 1 relaxed) | **101**, at `campaign.rs:249` |
| `cargo test -p rdb-sim --test campaign` (both reverted) | **0** |
| `cargo test -p rdb-sim --test oracle lin_` (both clauses neutered) | **101** — 3 new rows red, M7V-16..19 green |
| `cargo test -p rdb-sim --test oracle --test scenarios --test campaign` (final) | **0** — **6 + 58 + 19 = 83**, 0 failed, no warnings |
| `cargo clippy -p rdb-sim --test oracle --test scenarios --test campaign -- -D warnings` | **0** |
| `rustfmt --edition 2021 --check` over all nine touched `.rs` files | **0** |
| `bash scripts/gate.sh drift` | **0** — all four plans OK at `ec610f4` |
| DuckDB over 86 JSONL files, `map_inference_threshold=-1` | 0 — 415 lines, 83 methods, 3 modules |

**Clippy is scoped to my three test targets on purpose.** A `--all-targets` run in this shared tree
compiles `tests/harness.rs`, which foundation is mid-edit; attributing that to my change is exactly
the mistake that produced the withdrawn F3.

**Field discipline: 0 offending rows, measured.** `DESCRIBE` over the relation returns the same 14
columns the reviewer saw; a pattern match for `payload`, `key_byte`, `value_byte` and bare
`key`/`value` over the column names returns **0**. My changes add no log line, so the vocabulary is
unchanged.

---

## Files I changed

- `crates/rdb-sim/tests/campaign.rs` — F1
- `crates/rdb-sim/tests/oracle.rs` — F2 rows, M7V-85 rename
- `crates/rdb-sim/tests/scenarios.rs` — F7
- `crates/rdb-sim/tests/support/oracle/checks/lineage.rs` — rule table, ordering doc
- `crates/rdb-sim/tests/support/oracle/checks/{lag,loss}.rs` — F9
- `crates/rdb-sim/tests/support/scenarios/{grammar,gen}.rs` — F8
- `crates/rdb-sim/tests/support/scenarios/reduce.rs` — F5 doc
- `docs/testing/test-plan-m7-verification.md` — F4 (VA-7, §10)
- `teams/verification/developer-handoff.md` — F4 (§4, §7)

**Not touched:** `crates/rdb-sim/src/harness/trace.rs`, `crates/rdb-sim/tests/harness.rs`,
`crates/rdb-core/src/contracts/` (beyond the reverted, verified F1 flip), and every other team's
plan and handoff. No git operation of any kind — the tree is left dirty for the lead.

---

## Left open, deliberately

1. **F5's aggregate shrink bound** — see the table above. Needs the I1 runner; the obligation is
   now written where the implementer will read it, but it is a **residual risk the lead should
   accept explicitly**, not a closure.
2. **The eleven tier-2 lines themselves** — disclosed, not built, per assignment. Blocked on
   foundation item 1. Q-34 and Q-38..Q-40 remain dark.
3. **The oracle has still never judged a real kernel trace.** Every row here folds a hand-built
   `Trace`. Unchanged by this round, and it stays the package's largest risk (handoff §8).

## Recommended status

**COMPLETED_WITH_RISKS.** F1 and F2 are closed with falsification evidence in both directions. F4
is disclosed in three places. The residual risks are F5's uncapped aggregate and the four dark
Q-rows, both of which need I1 or foundation item 1 and neither of which this round could close.
