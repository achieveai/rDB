# Code review — verification O1 oracle, G1 grammar / generator / reducer

Reviewer: `review-verification`. Date: 2026-09-21. Branch `feature/rdb-m7`, tree at HEAD `0c304fd`.
Reviewing commit `6175fff` `test(rdb-sim): O1 oracle and G1 grammar, generator and reducer`.
Read-only on code. No git operations performed.

## Verdict

**PASS_WITH_RISKS.** Recommended status: **COMPLETED_WITH_RISKS** — the developer's own outcome,
with F1, F2 and F4 as named corrections.

No BLOCKER. **Three** MATERIAL findings stand: F1, F2, F4. **F3 is withdrawn** — I attributed a
build failure to a defect at HEAD when it was another agent's uncommitted in-flight work; re-run
against exported HEAD content, the developer's clippy-clean and workspace-compiles claims are both
**true**. Correction rounds used: 1 (coordinator), 1 finding withdrawn, F1 independently confirmed.

The package is unusually well defended against the vacuous-assertion class; the three surviving
MATERIAL findings are bounded and each has a cheap closure.

---

## What I ran, with real exit codes

| Command | Exit | Observed |
|---|---|---|
| `CARGO_TARGET_DIR=.rtargets/review-verification CARGO_INCREMENTAL=0 cargo test -p rdb-sim --test oracle --test scenarios --test campaign` | **0** | campaign 6, oracle 55, scenarios 19 — 80 passed, 0 failed |
| `… cargo clippy -p rdb-core -p rdb-sim --all-targets -- -D warnings` (shared tree) | 101 | 5 errors in another agent's dirty `harness.rs` — **not a finding**, see F3 |
| `… cargo test -p rdb-sim --no-run` (shared tree) | 101 | same dirty file |
| `duckdb` CLI over `.rtargets/rvl/**/*.jsonl` (83 files, 368 KB, writer exited) | 0 | see below |
| **on exported HEAD**: `cargo clippy -p rdb-core -p rdb-sim --all-targets -- -D warnings` | **0** | clean, no warnings |
| **on exported HEAD**: `cargo test -p rdb-sim --test oracle --test scenarios --test campaign` | **0** | 6 / 55 / 19 passed |
| **on exported HEAD**: `cargo test -p rdb-sim --no-run` (all targets) | **0** | every target builds |

Cold target directory, never shared with another running invocation.

**Read the two blocks differently.** The first three rows ran against the *shared working tree*,
which several agents are editing concurrently — at the time of writing `crates/rdb-core/src/`,
`crates/rdb-sim/src/harness/` and `crates/rdb-sim/tests/harness.rs` were all dirty with other
teams' work. A cargo result from that tree is a statement about the tree at that instant, not about
HEAD. The last three rows ran against `git archive HEAD | tar -x` into an isolated path with its own
target directory — no stash, reset, checkout or clean, and the shared tree was not touched. Those
are the rows that speak to the commit under review.

**Scope is immune to tree churn.** `git show --name-only 6175fff` is **entirely** under
`crates/rdb-sim/tests/` — no `rdb-core` file, no `src/` file. That, not a point-in-time
`git status`, is what establishes the purity result below.

### Measured from the logs, not from the handoff

Log root produced by my own completed run, `RETCD_TEST_LOG_DIR=.rtargets/rvl`, 83 files / 368 KB,
cargo exited before any query (no open writer). `map_inference_threshold=-1` on every call.

```sql
SELECT count(*), count(DISTINCT testMethod), count(DISTINCT testModule)
FROM read_json_auto('.rtargets/rvl/**/*.jsonl', union_by_name=true, map_inference_threshold=-1);
-- 400 lines, 80 methods, 3 modules
SELECT testModule, count(DISTINCT testMethod) GROUP BY 1;
-- campaign 6, oracle 55, scenarios 19
```

"80 rows across three test binaries" is **confirmed against the logs**, not only the cargo summary.

The package's entire `@m` vocabulary, queried:

| `@m` | lines | owner |
|---|---|---|
| `capability` | 240 | foundation's `support::preamble()` (80 rows x 3 environment packages) |
| `test started` | 80 | `config_log` harness |
| `test finished` | 80 | `config_log` harness |

**Verification emits no log line of its own.** `DESCRIBE` returns 14 columns: 12 `config_log`
envelope columns plus `package` and `state` from `capability`. No verification-owned field exists.

Field discipline (Q-60 shape) over every emitted line: **0 offending rows**. Weak evidence — the
vocabulary is three messages wide — but it is measured, not assumed.

Coordinator's point 4 (a row that passes only because the reader is slow) is **structurally
inapplicable** here, and I checked rather than assumed: `grep` for `test_logs_relation`,
`relation_for_current_test`, `lines_for_current_test`, `config_testkit`, `thread::spawn`,
`Duration::from`, `sleep` across all six verification source trees returns exactly one hit, a
comment in `campaign.rs:417`. Every row is a synchronous in-process fold over a hand-built
`Trace`. There is no live producer to race and no row asserts on log output.

---

## Findings, most severe first

### F1 — MATERIAL — M7V-82(a)'s "other direction" is a vacuous assertion

**Criterion.** Charter priority 1: an assertion no production edit can turn red is a defect of the
same severity as a wrong one. The row's own comment states the clause's purpose.

**Location.** `crates/rdb-sim/tests/campaign.rs:245-248` and `:300-314`.

**Evidence.**

```rust
// campaign.rs:245-248
// The other direction: a module that overrides `capability()` reports Wired. Without this
// half, a report hardwired to `Unavailable` would pass the clause above.
assert_eq!(Wired.capability(), CapabilityState::Wired);
assert_eq!(Defaulted.capability(), CapabilityState::Unavailable);

// campaign.rs:300-314
/// A module that claims to be wired, and one that takes the trait's default.
struct Wired;
struct Defaulted;
impl Wired     { fn capability(&self) -> CapabilityState { CapabilityState::Wired } }
impl Defaulted { const fn capability(&self) -> CapabilityState { CapabilityState::Unavailable } }
```

Neither struct implements `rdb_core::contracts::event::Module`. These are inherent methods whose
bodies are the literals being asserted, so the two lines assert `Wired == Wired` and
`Unavailable == Unavailable`. Flipping `Module::capability`'s default in `rdb-core` leaves both
green. The doc "one that takes the trait's default" is false: `Defaulted` hardcodes the value.

**False-positive check performed.** `grep -n "Module\|contracts::event" crates/rdb-sim/tests/campaign.rs`
returns **no matches** — the trait is never named in the file. I confirmed `Module` is a real trait
at `crates/rdb-core/src/contracts/event.rs:371` whose only members are `name()`, `step()` and
`capability()` with `CapabilityState::Unavailable` as its default, so implementing it in a test is
a few lines and the correct form is available.

**Consequence.** The half of M7V-82(a) that exists specifically to stop a hardwired-`Unavailable`
report from passing does not do that. The first clause still has force, so the row is not wholly
vacuous — but the stated defence is absent.

**Closure.** Make both structs `impl rdb_core::contracts::event::Module`, with `Defaulted` taking
the trait default and `Wired` overriding it, so changing the default in `rdb-core` turns
`Defaulted`'s assertion red. Then re-run `--test campaign`.

**Closure met in the working tree (uncommitted) — verified by inspection.** After this review was
filed, `campaign.rs` was corrected in the shared tree (+36/-8). `grep -c Module` is now 12, not 0.
Both doubles implement the real trait; `Defaulted` carries the comment
"`capability()` is deliberately not overridden. That absence is the assertion.", and the row now
reads `assert_eq!(Module::capability(&Defaulted), CapabilityState::Unavailable)`. Flipping
`Module::capability`'s default in `rdb-core` turns that red. This is exactly the closure condition.

Not yet committed, and I did **not** re-run `--test campaign` against it: the shared tree also holds
several other teams' in-flight edits (`rdb-core/src/`, `rdb-sim/src/harness/`, a new
`tests/authority.rs`), so a build there would again describe the tree at an instant rather than the
change. The lead should confirm it compiles green once the tree settles — see F3 for why that
distinction matters.

### F2 — MATERIAL — two INV-LIN clauses have no trip fixture; one is absent from its own rule table

**Criterion.** Charter O1: each checker gets a bad trace that trips it and a valid trace that does
not. Handoff §2 presents a per-invariant trip/near-miss table as evidence of this.

**Location.** `crates/rdb-sim/tests/support/oracle/checks/lineage.rs:66`
(`recovery_root_without_predecessor`) and `:164` (`cutoff_above_selected_source`).

**Evidence.** I extracted all 32 rule strings from `checks/*.rs` and counted literal occurrences
across `oracle.rs`, `scenarios.rs` and `campaign.rs`. Thirty have at least one exercising row.
These two have **zero**. A repo-wide grep for both strings (tests tree, the M7 verification plan,
and the team's own notes) returns only the three definition sites in `lineage.rs` itself.

`cutoff_above_selected_source` additionally does not appear in `lineage.rs`'s own module rule table
at lines 7-12, which documents four rules. Rule strings are load-bearing: the reducer's acceptance
predicate compares `CoreTuple.rule`.

**False-positive check performed.** Confirmed against the plan that neither clause is one the plan
asked for — the plan's INV-LIN rows M7V-16..M7V-19 name `predecessor_digest_mismatch`,
`digest_conflict_without_quarantine` and `cutoff_below_an_available_recorded_prefix`, all three of
which are covered. These two are developer additions beyond the plan. The handoff's §2 table does
not claim them, so this is an undisclosed gap rather than a false claim.

**Consequence.** An inverted or over-broad condition in either is invisible today.
`cutoff_above_selected_source` runs *before* the `cutoff_below_an_available_recorded_prefix` loop in
the same `RecoveryDecision` arm, so a misfire there would mask M7V-18's clause on shapes M7V-18
does not cover.

**Closure.** Add a trip fixture and a near-miss for each, **or** delete
`cutoff_above_selected_source` and record the other as deliberately unexercised. Either way, add
`cutoff_above_selected_source` to the module rule table if it stays.

### F3 — WITHDRAWN — the handoff's clippy and "workspace compiles" claims are TRUE at HEAD

I raised this as MATERIAL and it was wrong. Withdrawn in full, and the developer's claim is now
positively **verified**, not merely un-disproved.

**What I originally reported.** `cargo clippy -p rdb-core -p rdb-sim --all-targets -- -D warnings`
and `cargo test -p rdb-sim --no-run` both exit 101 on `crates/rdb-sim/tests/harness.rs:40`, an
E0432 on four `rdb_sim::harness::trace` symbols that do not exist.

**Why it was wrong.** That failure is real but it is **not at HEAD**. `harness.rs` is *dirty* in the
shared working tree:

```
$ git status --porcelain crates/rdb-sim/tests/harness.rs
 M crates/rdb-sim/tests/harness.rs
$ git show HEAD:crates/rdb-sim/tests/harness.rs | grep -c "harness::trace"
0
```

The breaking `use` line was never committed. Those four symbols are the tier-1 serialiser API that
**dev-foundation-r2 is writing right now**, test-imports-first. I compiled another agent's in-flight
red-before-green and read it as a defect.

**My method error, recorded so it is not repeated.** I attributed with
`git log -3 -- crates/rdb-sim/tests/harness.rs`, which reports the last commit to *touch* the path
(`6893442`). A line that was never committed cannot appear there, so `git log` on a path can never
falsify "this is committed state". `git status` or `git show HEAD:<path>` answers that question;
`git log` does not. I also cited "`git status --porcelain crates/rdb-sim` is empty" as proof of
committed state — true when I ran it, false 30 minutes later.

**The standing lesson.** Several agents edit this one working tree concurrently. **Any `cargo`
result is a statement about the tree at that instant, not about HEAD.** Before attributing a build
failure to another team, diff the file against HEAD.

**Re-run against HEAD** — via `git archive HEAD | tar -x` into an isolated short path with its own
`CARGO_TARGET_DIR`. No stash, reset, checkout or clean; the shared tree was not touched.

| Command, on exported HEAD content | Exit |
|---|---|
| `cargo clippy -p rdb-core -p rdb-sim --all-targets -- -D warnings` | **0** — clean, no warnings |
| `cargo test -p rdb-sim --test oracle --test scenarios --test campaign` | **0** — 6 / 55 / 19 passed |
| `cargo test -p rdb-sim --no-run` (every target, including `harness`) | **0** |

Handoff §2 "clean, no warnings" and §9 "the workspace compiles at this stopping point" are both
**true as written**. Nothing is owed by verification here, and `gate.sh test -p rdb-sim` does go
green at HEAD.

### F4 — MATERIAL — the log contract's item 2 is entirely absent and undisclosed

**Criterion.** `docs/testing/m7-log-fields.md` "What is owed": item 2, "The eleven tier-2 runner
lines", owner **verification**, unblocks Q-34, Q-38, Q-39, Q-40.

**Evidence — queried, not grepped.** Against my own 83-file log root:

```sql
SELECT m, (SELECT count(*) FROM read_json_auto('.rtargets/rvl/**/*.jsonl',
       union_by_name=true, map_inference_threshold=-1) t WHERE t."@m" = m)
FROM (SELECT unnest(['invariant_status','violation','capability_seen','coverage_cell',
  'coverage_shortfall','coverage_unavailable','shrink_step','shrink_result',
  'campaign_run','mutation_caught','trace_header']) AS m);
```

All eleven return **0 lines**. `invariant_status` specifically: 0. The full emitted vocabulary is
`capability` / `test started` / `test finished`, none of them verification's.

**False-positive check performed.** I first grepped and found zero, then re-verified by query per
the coordinator's instruction — the query agrees. I also confirmed no verification source declares
these names at all, so there is no "emitted but filtered out" explanation.

**Consequence.** `invariant_status.seeds_armed` is the log-field contract's own named defence
against the vacuous pass ("a `proven` row with `seeds_armed = 0` is the vacuous pass, and Q-34's
second statement fails the run on it"). It has no producer. `coverage_shortfall` likewise — the
document warns that without it "Q-38 reports a clean sheet on a run that covered nothing". Four
Q-rows are dark and a zero-row Q-row is indistinguishable from a clean run.

**Mitigation, stated fairly.** Most of the eleven are the campaign runner's, and the runner is I1.
But `invariant_status`, `capability_seen` and `trace_header` are derivable from `Oracle::judge`'s
`Report` today, which `campaign.rs` already holds.

**Undisclosed.** Handoff §4's "Not touched (held)" list does not mention item 2, and §7 "Contract
requests" says "No other asks". This is the finding's real weight: the gap is defensible, the
silence is not.

**Closure.** Either emit the lines the oracle can already produce, or add item 2 to the handoff's
held list with its I1 dependency named, so the lead can see Q-34 and Q-38..Q-40 are still dark.

**Coordinator's note, recorded.** Nobody was assigned to build the eleven tier-2 lines — a reviewer
was dispatched, not a developer. So the *absence* is not a delivery failure by this developer. The
sustained part of this finding is narrower and it is the part that matters: the obligation is
verification's per the log-field contract, it is not built, and the handoff does not say so in
either its held list or its contract requests. Scope the correction to the disclosure.

### F5 — ADVISORY — `ShrinkBudget::total` is not the aggregate bound it documents

`reduce.rs:44` documents `total` as "Aggregate re-runs per run, across every failure", answering
critic F11's "with `--no-fail-fast` and N failing seeds it is N times that, uncapped". But `ddmin`
starts `steps = 0` per call and compares `steps >= budget.total` inside that one call. With the
defaults (`steps: 2_000`, `total: 20_000`) `BudgetSpent::Total` is unreachable, and no
cross-failure accounting exists anywhere.

False-positive check: M7V-48's `total` clause (`scenarios.rs:419-429`) does bind — with
`steps: u32::MAX, total: 10` the bound fires — so the clause is not vacuous. It is only per-call.
A caller *could* implement the aggregate by decrementing `total` between failures; nothing says so.

Closure: document that the caller decrements `total` per failure and pin it in the I1 runner's
contract, or add the accumulator.

### F6 — ADVISORY — M7V-85's function name contradicts its assertion

`oracle.rs:2508` `m7v_85_without_rule_has_exactly_one_call_site` asserts `total == 0`. Disclosed in
handoff §5.4, but the name is what a reader greps. Rename to `..._has_no_call_site_until_m7v_23`.
Related: `Report::without_rule` (`support/oracle.rs:293`) has zero callers and is therefore entirely
unexercised — acceptable and disclosed, but it will be used for the first time when M7V-23 lands.

### F7 — ADVISORY — `std::env::set_var` inside M7V-43

`scenarios.rs:149-151` sets `SPIKE_SEEDS` and `SPIKE_SHRINK_STEPS` and never restores them. Cargo
runs test fns on parallel threads in one process; `set_var` racing another thread's env read is UB
and is `unsafe` in edition 2024. Nothing else reads these two names today, so the risk is latent.
Closure: pass the values as arguments, or keep only the source-level half already in the row.

### F8 — ADVISORY — `payload` as a grammar field name

`grammar.rs:133,171` declare `payload: u64` — an abstract identity, not bytes, so **no discipline
breach today** (Q-60 shape returned 0 offending lines). But `Scenario` derives `Serialize` for the
D4 fixture, and tier-2's `trace_header` / `shrink_*` lines are meant to carry scenario provenance.
Q-45, Q-48 and Q-60 fail the run on any log line carrying `payload`. Closure: rename before the
runner serialises a `ScenarioOp` into a log line, or pin in VA-7 that no tier-2 line does.

### F9 — ADVISORY — two `#[allow(dead_code)]` items carrying only prose

`lag.rs:262` `_partition_is_local` and `loss.rs:158` `type Holder` exist to state a comment. The
`allow` suppresses the signal that would catch genuinely dead code later. Move the prose to a doc
comment and delete the items.

---

## What I inspected

Full read: `support/oracle.rs`, `support/oracle/checks.rs`, `checks/{dedup,publication,lag,liveness,
loss,version,lineage}.rs`, `support/scenarios/{builder,reduce}.rs`, `campaign.rs`,
`support/oracle/model.rs` (head), `scenarios.rs` (rows 1-360, 360-490, 735-773), `oracle.rs`
(rows 1-850, M7V-85), `docs/testing/m7-log-fields.md`, the developer handoff, `AGENTS.md`
DuckDB section (re-read after the coordinator's correction).

Cross-checks: all 32 checker rule strings mapped to exercising rows; `Module` trait shape in
`rdb-core`; `environment_capabilities()`; `Dispatcher::capability_report()`; field-discipline grep
across all six verification source trees; `git log`/`git status` attribution for `harness.rs` and
for the verification commit.

**F-V1 confirmed real.** The developer's self-reported INV-DEDUP defect is genuine and the fixture
genuinely trips the old bug. `TraceBuilder::ack_from` (`builder.rs:314-347`) emits the secondary's
own `BatchApply` **on the unchanged correlation and partition** before its `ReplicationAck`. The
golden trace's RF3 publish therefore produces three applies under one `(partition, identity,
generation)` key. The pre-fix `duplicate_effect` clause, which did not filter on `role`, fired on
the second. The fix at `dedup.rs:75` (`if *role != ReplicaRole::Primary { return Ok(()); }`) is the
correct and minimal one. Confirmed structurally; I did not edit code to reproduce it, per my
read-only constraint.

**Purity (ADR-rdb-0002 §58, ADR-rdb-0003 §44): clean.** Established from the commit, not from a
point-in-time `git status`: `git show --name-only 6175fff` lists only files under
`crates/rdb-sim/tests/`. The work is test-only; no clock, I/O, randomness or async is introduced
into `rdb-core`. The generator's PRNG and the builder's ticks are `rdb-sim` test code, and M7V-43's
source half already forbids `std::env` inside `gen.rs`. (My first pass cited
`git status --porcelain crates/rdb-core` being empty — true when run, dirty with other teams' work
half an hour later. The commit-scoped check is the one that holds.)

**Field discipline: clean.** No key byte, value byte or payload reaches a trace or a log. Keys are
`KeyId`, values are `(KeyId, Version)` pairs, digests are `Digest` compared only for equality and
built content-free by `digest_at`. Q-60 shape over the real log: 0 offending rows. See F8 for the
one forward-looking naming risk.

---

## Strongest evidence against my own verdict

This package is better defended against the class I was sent to hunt than anything else in the
wave, and a reviewer looking for findings could easily overweight the ones I found.

Concretely: M7V-01, M7V-82(b), M7V-77 and `checked_in_fixtures()` each carry an explicit
"an empty set must fail this row, not pass it" guard. The parked rows read the **live** capability
table through `environment_capabilities()` rather than hardcoding `Unavailable`, so all nine of
them go red the day I1 lands instead of reporting forever — that is the opposite of the M7A-32
failure mode. M7V-85 builds its own needle at run time so the row cannot match its own source.
M7V-01 is an allowlist rather than a blocklist. Thirty of 32 checker clauses have a trip row, and
every checker has a documented near-miss. The developer found, disclosed and correctly fixed a real
defect in their own reviewed code, and disclosed a row whose expectation they had written wrong.

Against F4 specifically: the campaign runner is genuinely I1-dependent, so nine of the eleven
tier-2 lines could not be emitted today under any plan. The finding rests on the undisclosed
silence and on the two or three lines that *are* derivable now, not on the absence itself.

Against F2: both unexercised clauses are *extra* safety the plan never asked for. Removing them
would close the finding while making the oracle weaker. That is a real argument for accepting the
risk rather than correcting it, and the lead may reasonably take it.

None of this changes the verdict — F1 is a genuine vacuous assertion and F4 is a genuine
undisclosed obligation — but it should temper how the finding count is read.

And one finding of mine did not survive contact with the evidence: **F3 was wrong**, withdrawn
above. I reported a developer's evidence claim as false without establishing it was false at the
commit they claimed it for, on a tree four other agents were editing. That is the same error class
I was sent to hunt — an assertion whose antecedent I never checked — committed by the reviewer.
The re-run against exported HEAD says their claim was true all along. A reader weighing F1, F2 and
F4 should know the reviewer's base rate here is three from four.

F1 has since been independently confirmed by the coordinator (`grep -c Module
crates/rdb-sim/tests/campaign.rs` = 0; inherent `impl` blocks at lines 304 and 310) and is recorded
as the fifth confirmed member of the vacuous class in this wave.
