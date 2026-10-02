# Manual test plan — M7 foundation contracts freeze

Author: manual tester (foundation), 2026-09-21. Written **before** implementation, against HEAD
`aa0b6bf` on `feature/rdb-m7`.

Input: `teams/foundation/design-contracts-freeze.md` (arch-foundation-freeze, basis `26cd632`),
`docs/testing/test-plan-m7-foundation.md` §14, my own round-1/round-2 handoffs.

**Status: COMPLETED_WITH_RISKS.** Three of the four changes have a hand-test I can run and an
observable that distinguishes working from broken. **CB-8 does not**, and **G-13's step 5 does
not** — both are defect shape (a-variant), and both are findings about the design, not about
reach. Details below. I also found that a §14 premise about CB-8 is stale at this HEAD.

**Amended after the lead's mid-task correction (same day).** The lead independently withdrew the
§14 "asserted by no row in the workspace" claim and raised **CB-9** (the simulator's clock-skew
injection path is dead). I had reached both conclusions independently while drafting — they are in
§2.1 and were in §2.3 as demand 3 — but CB-9 was filed as a supporting observation, not as an item.
It is now **§2b, first-class**, and it is larger than either of us stated: not only do `set_skew`
and `control_time` have no callers, **nothing in any `src/` owns a `Clock` at all**. Method note
taken: every "not covered anywhere" claim below now states the paths searched.

Everything below is a plan. I wrote no test row, edited nothing under `crates/` or `docs/`, and
ran no git command that mutates anything.

---

## 0. Standing rules for every run below

Same discipline as round 2, which worked:

- Work in **my own** `git archive` export, not the shared tree. `echo <sha> > EXPORT_BASIS`.
  Delete only my own path, only when I say I am done.
- `CARGO_TARGET_DIR=<export>/.t CARGO_INCREMENTAL=0 RETCD_TEST_DEADLINE_SCALE=3
  RETCD_TEST_LOG_DIR=<export>/logs`.
- One cargo invocation at a time. Output to a file; read `EXIT=` from the file's tail, never the
  pipeline's status (AGENTS.md: a run ending `error: 8 targets failed` showed exit 0 through
  `| grep | tail`).
- Any file I mutate gets `cp FILE FILE.orig` first, `diff` verified empty after revert, backup
  deleted.
- Baseline first: full workspace test profile green before I trust any red as mine.

---

## 1. CB-7 — `KernelIgnoredReason`

### 1.1 How I will drive this by hand

Four probes. Two are compile-only, which is the right oracle here: §10 of the foundation plan
makes a compile error a *contract* verdict, not a test verdict.

**P1 — the widened vocabulary compiles (the point of CB-7).** In a scratch file inside my export,
not a test row:

```rust
// probe.rs, compiled with `cargo build -p rdb-core` after pasting into a scratch bin target
let e = KernelEffect::Ignored {
    reason: KernelIgnoredReason::AppendRejected(AppendReject::WrongPartition),
};
```

Observable: it compiles, and `format!("{e:?}")` reads
`Ignored { reason: AppendRejected(WrongPartition) }`. I will print it and read the string, not
just assert compilation — the `Debug` text is the thing kernel-b's ladder rows will eventually
match on.

**P2 — `Copy` is really gone, and the blocked variant really unblocks.** Add, in my export only:

```rust
// event.rs, inside KernelEffect
BlockPartition { reason: BlockReason },
```

`BlockReason::DivergenceRequiresOperator` holds `Vec<CopyId>`
(`crates/rdb-core/src/contracts/authority.rs:198-206`).
- Against the **pre-CB-7** tree: expect `E0204`, "the trait `Copy` cannot be implemented for this
  type", pointing at the `Vec`.
- Against the **post-CB-7** tree: expect a clean `cargo clippy -p rdb-core --all-targets -- -D
  warnings`.
Then revert. This is the only probe that proves the `Copy` drop was load-bearing rather than
cosmetic. Command: `scripts/gate.sh lint` for the clean half.

**P3 — nothing else lost `Copy` by accident.** The design asserts `EffectKind` is already
non-`Copy` (`event.rs:293`, `:312-313`). I verified that claim at HEAD by reading the derives —
it holds. After the change I will re-run `scripts/gate.sh lint` across the **workspace**, not just
`rdb-core`, because a `KernelEffect` moved out of a match arm in `rdb-sim` would now be a borrow
error and clippy is where that surfaces. Round 1's M5 measured the derive as load-bearing nowhere
in `crates/`; this re-measures it after the shape actually changes, which is a different claim.

**P4 — serde survives the split.** `KernelIgnoredReason` is `Serialize`/`Deserialize`, and
`KernelEffect` is carried into the JSONL trace. Probe: round-trip
`KernelEffect::Ignored { reason: KernelIgnoredReason::Error(ErrorKind::NotPrimary) }` and
`...::AppendRejected(AppendReject::WrongPartition)` through `serde_json` and compare. The failure
I am hunting is an untagged/flattened representation in which `Error(NotPrimary)` and
`AppendRejected(<something>)` collide on the wire — the trace validator (`M7F-30…35`) will read
these, and two reasons that serialise the same are two reasons a trace row cannot tell apart.

### 1.2 What I would see if it were broken

| Break | What I observe |
|---|---|
| `Ignored` still carries `ErrorKind` | P1 fails to compile, naming `WrongPartition` as not an `ErrorKind` variant. Loud. |
| `Copy` not actually dropped | P2 still reports `E0204` after the change. Loud. |
| `Copy` dropped but a caller moved out of a match | P3's workspace clippy reports E0507/E0382. Loud. |
| `KernelIgnoredReason` given a lossy serde representation | P4's round-trip returns a different variant, or two distinct inputs produce identical JSON. **This is the one that is silent without P4** — nothing else in the freeze reads the wire form. |
| A third variant is needed and silently omitted | Nothing observable. `#[non_exhaustive]` means a later add is additive. Not a defect I can test for; §4 open question 2 already flags it. Accepted. |

### 1.3 Entry points I need that do not exist

**None.** CB-7 is plain data in `rdb-core` with public constructors. This is the only one of the
four I can drive with what exists. I need one thing that is not an entry point: permission to
compile a throwaway bin target inside my own export (I will not add it to the committed tree).

### 1.4 Where I expect a defect shape

- **(a-variant), at the serde boundary.** If `KernelIgnoredReason` is written with
  `#[serde(untagged)]` or the two variants' payloads serialise to the same JSON shape, a trace row
  that reads "the reason" takes the same value on both sides of the `Error`/`AppendRejected`
  boundary. The variant split then exists in Rust and not on the wire, which is where
  `M7F-30…35` will read it. **P4 exists specifically for this.** I rate this the most likely real
  defect in CB-7.
- **(d), weakly.** Whoever writes the eventual rows will reach for the first `AppendReject`
  variant in the enum every time. That is a fixture-degeneracy risk for the *rows*, not for this
  change, and it is not mine to fix before the rows exist. Noting it for the Test Planner.

---

## 2. CB-8 — `ctx_with_control_time`

**This is my main finding. Read §2.2 before §2.1.**

### 2.1 How I will drive this by hand

**Entry point 1 (the design's "pure, no fixture") — works, and is already partly done.**
Build a `ControlTime` directly (all fields `pub`, `contracts/time.rs:83-101`) and call
`compare`/`is_stale`. I will drive all six branches from a scratch bin and print the verdicts.

**But the design's premise here is stale at this HEAD.** §14 says `compare`/`is_stale` are
"asserted by **no row in the workspace**". That was true of `crates/rdb-sim/tests`. It is **not**
true of the workspace. (The lead reached this independently and withdrew the claim mid-task, with
the same diagnosis: an `rdb-sim`-scoped grep generalised to a workspace-scoped conclusion.)

Paths searched: `grep -rn "ClockVerdict\|ControlTime\|control_time\|is_stale\|bound_established"
crates/` — the whole `crates/` tree, all crates, `src/` and `tests/` alike, plus a separate
`grep -rn "DefinitelyAfter" crates/`.

`crates/rdb-core/tests/seams.rs:50-95`, row `m7f_14`, already asserts

- `bound_established: false` → `Uncertain` (`seams.rs:84-90`),
- stale by `>` on age → `Uncertain` (`:69-76`),
- a sample stamped after `now` → `Uncertain` (`:77-81`),
- `DefinitelyBefore` (`:60-65`).

Four of the six named branches are covered at `aa0b6bf`. I verified by reading the file and by
grepping the whole workspace for `ClockVerdict`, `DefinitelyAfter`, `bound_established`.

**The two that are genuinely uncovered are `DefinitelyAfter` and the overlap `Uncertain`.**
*Paths searched:* `grep -rn "DefinitelyAfter\|DefinitelyBefore" crates/` — every crate, `src/` and
`tests/`. `DefinitelyAfter` returns **only** `contracts/time.rs:112` and `:152`: the declaration
and the arm that produces it. No test anywhere in the workspace names it. (`DefinitelyBefore`
additionally returns `seams.rs:64`, which is how I know the search would have found a row if one
existed.)

§14 says "two of them are the sign", meaning the `DefinitelyBefore`/`DefinitelyAfter` pair. Exactly
one half of that pair is asserted. So: **the sign can be flipped in one direction and the workspace
stays green.** That is the concrete, mutation-provable gap, and it is *not* the gap CB-8 fixes.

Planned mutation (my export, reverted): swap the two arms at `time.rs:150` / `:152`, then
`scripts/gate.sh test --workspace`. Prediction: `m7f_14`'s `DefinitelyBefore` assertion catches
the swap — so this particular mutation is CAUGHT. The sharper mutation is
`self.estimate.0 > instant.0.saturating_add(slack)` → `>=`, which changes only the
`DefinitelyAfter`/overlap boundary. Prediction: **MISSED, green workspace.** That is the mutation I
will actually run, and it is what I would route.

**Entry point 2 (the design's "through the shared fixture") — I cannot drive it, because there is
nothing to observe.**

### 2.2 What I would see if it were broken: *nothing*. This is a design finding.

The design says:

> 2. **Through the shared fixture**, after the change:
>    `support::ctx_with_control_time(ControlTime { bound_established: false, .. })`, then step a
>    kernel through it and observe `Uncertain`.

**No kernel reads `StepCtx::control_time`, and none is planned to.** Evidence, all checked at
`aa0b6bf`:

- Every `Module::step` in the workspace takes the context as `_ctx`, underscore-prefixed and
  unread: `authority.rs:331`, `protection.rs:35`, `publication.rs`, `recovery.rs`,
  `replication.rs`, `transaction.rs` (all `impl Module` sites from
  `grep -rn "impl Module" crates/`).
- `grep -rn "ControlTime\|control_time\|is_stale\|ClockVerdict" crates/` returns, outside
  `contracts/time.rs`: a doc line in `authority.rs:3`, the field declaration
  (`event.rs:412`), one mechanical copy (`harness/dispatch.rs:152`, `control_time:
  base.control_time`), the sim clock, the fixture, and `seams.rs`. **Zero reads by any kernel.**
- `ControlTime::compare` and `ControlTime::is_stale` have **zero production callers**.
- And they are not going to get one. Kernel-a's design states it outright at
  `teams/kernel-a/design.md:875-877`: "`ControlTime::compare` and `is_stale` stay where they are
  and are used by whoever wants that comparison; **A1 does not call them**." A1 decides the same
  question in `effective_epsilon`/`utc_ok` instead.

So after `ctx_with_control_time` lands, a row that passes `bound_established: false` produces
**byte-identical kernel effects** to a row that passes `true`. The observable takes the same value
on both sides of the boundary. That is **defect shape (a-variant), verbatim**, and it is in the
design's own proposed hand-test — the exact thing I was told to test the proposals for.

Worse: a row written against entry point 2 would be *worse than no row*. It would appear in the
gate map as coverage of CB-8's branch while asserting a tautology, and the next sweep would find it
the way the last one found eight.

I also checked whether a different seam already varies the flag honestly. It does not, and that is
now **CB-9 — see §2b**. In short: the environment→context→kernel chain CB-8's fixture sits in the
middle of is broken in **three** places, and CB-8 repairs one of them.

**My position.** CB-8 as designed is *harmless and additive* — I am not asking for it to be
dropped. But it does not close the gap §14 describes, and it must not be counted as closing it.
The honest statement of CB-8 is: "a builder that will be needed once a kernel reads
`StepCtx::control_time`; today no such kernel exists or is planned."

### 2.3 Entry points I need that do not exist

Listed in the order I would accept them. **(1) is what I actually need; (2)/(3) are what I need if
the team wants CB-8's own hand-test to mean anything.**

1. **A `DefinitelyAfter` / overlap row in `crates/rdb-core/tests/seams.rs`'s `m7f_14`, or a
   sibling row.** Not a new entry point — the reach already exists. This is a coverage demand, and
   it is the *real* CB-8 gap. It is the Test Planner's to place after my thumbs up; I name it here
   so it is not lost.
2. **One production reader of `StepCtx::control_time`, or an explicit written decision that there
   will be none.** If foundation's answer is "none, by design — A1 owns the clock rule", then
   `StepCtx::control_time` is a dead field and CB-8 is a builder for a dead field. Either answer
   is fine; I need it in writing, because it changes whether entry point 2 is ever testable.
3. **If (2) says a reader is coming: the dispatcher must fill `StepCtx::control_time` from
   `Clock::control_time(node)` rather than from a literal.** Right now `dispatch.rs:152` copies
   whatever the caller passed, and the only caller is the test fixture. Wiring the sim clock in is
   what would make `set_skew(node, _, false)` observable end to end — and would make `Clock::set_skew`
   and `Clock::control_time` stop being dead code.

I do **not** need `ctx_with_control_time` itself to do any of my work. I will not block on it.

### 2.4 Where I expect a defect shape

- **(a-variant), in the design's own hand-test #2**, located at `design-contracts-freeze.md` §2
  "How a Manual Tester drives this by hand", item 2. Named above. **This is a BLOCKER on the
  *hand-test*, not on the code change.**
- **(d-strong) is real but mislocated.** The unreachable branch is real. But the fixture is not
  what makes it unreachable — `ctx()` hardcoding `bound_established: true` is downstream of the
  fact that nothing reads the field. Fixing the fixture does not make the branch reachable.
  CB-8 is filed as (d-strong) and is actually a dead-field problem wearing (d-strong)'s clothes.
- **(a-variant) again, at the boundary that matters.** `estimate > instant + slack` versus `>=`
  is the one-line change that separates `DefinitelyAfter` from the overlap `Uncertain`, and no
  observable in the workspace distinguishes them. Mutation planned in §2.1.

---

## 2b. CB-9 — the simulator's clock is not wired into the simulator

Raised by the lead 2026-09-21 while I was drafting. I had the same two facts in hand; the lead is
right that it deserves its own item rather than a supporting sentence under CB-8. Having now run
the ownership search the lead asked for, **it is larger than either of us stated.**

### 2b.1 What I searched, and what I found

| Search | Command | Result |
|---|---|---|
| Callers of the skew API | `grep -rn "set_skew" crates/ docs/ .claude/scratchpad/` | 4 hits, **all in `clock.rs` itself**: the definition at `:111` and three doc-comment references at `:13`, `:74`, `:86`. Zero callers. |
| Callers of the converter | `grep -rn "control_time" crates/` | 4 hits: the `StepCtx` field decl (`rdb-core/src/contracts/event.rs:412`), one mechanical copy (`rdb-sim/src/harness/dispatch.rs:152`), the definition (`clock.rs:91`), the test fixture literal (`tests/support/mod.rs:118`). **Zero callers of the method.** |
| **Owners of a `Clock`** | `grep -rn --include=*.rs "Clock" crates/*/src` minus `clock.rs` | **Nothing in any crate's `src/` refers to `rdb_sim::sim::clock::Clock`.** The only `Clock` hits across every `src/` are `config-engine`'s unrelated `LeaderClock`/`SystemClock`/`ManualClock`. |
| Constructors | `grep -rn "Clock::new" crates/` | One caller in the entire workspace: `crates/rdb-sim/tests/dispatch.rs:619`, the guard row I added in round 1 (F3). |
| `Dispatcher`'s fields | read `harness/dispatch.rs:75-88` | six modules, `adopted`, `boots`, `replies`. **No clock.** |

So the finding is not "an injection API with no caller". It is: **`Clock` is a component the crate
declares itself to own and never composes.** `crates/rdb-sim/src/lib.rs`'s own layout table says
module `sim` (package H1) owns "scheduler, **clock and timers**, network, fake control store,
cluster". Nothing in `src/` holds one. The only `Clock` that exists at runtime anywhere in this
workspace is the one my own round-1 guard row constructs.

That also means the chain CB-8 sits in is broken at three links, not one:

```
Clock::set_skew(n, _, false)     [link 1] no caller; no owner holds a Clock
  → Clock::control_time(n)       [link 2] no caller; nothing converts sim clock → ControlTime
    → StepCtx.control_time       [link 3] CB-8 fixes this one — the fixture hardcodes true
      → Module::step reads it    [link 4] every impl takes `_ctx`; kernel-a says A1 never will
```

**CB-8 repairs link 3.** Links 1, 2 and 4 stay broken, and link 4 is broken by design decision
(`teams/kernel-a/design.md:875-877`). A row written against the repaired link 3 still asserts
nothing, which is the §2.2 finding restated from the other end.

### 2b.2 Can I drive skew by hand today? **Partly — and the split is the useful part.**

**CB-9a — yes, at the unit level, with no new entry point.** `Clock` is `pub` in a `pub mod`, and
`Clock::new`, `set_skew` and `control_time` are all `pub`. My round-1 guard row already proves the
type is constructible from outside the crate (`dispatch.rs:619`). So from a scratch bin in my
export I can drive, and assert, the contract `clock.rs:13-15` states in words:

- `set_skew(n, +k, true)` then `control_time(n)` → `estimate == now.plus_millis(k)`.
- `set_skew(n, -k, true)` → `estimate == now - k`, **saturating at zero** (`clock.rs:99`), which
  is the branch a negative skew larger than `now` reaches and which nothing exercises.
- `set_skew(n, _, false)` → `control_time(n).bound_established == false`, and therefore
  `.compare(..)` → `Uncertain` for every argument. This is `clock.rs:15`'s "every comparison must
  fail" made observable, and it is currently a documented promise with no subject.
- a node **never** skewed → `bound_established: true` (the `unwrap_or` default at `:92-95`).
- `sampled_at == clock.now()`, not the delivery tick — the field K-F-15 exists for.

I will run these and record the outputs. **Planned mutation, my export, reverted:** flip
`skew.bound_established` to a literal `true` at `clock.rs:104`. Prediction: **MISSED**, whole
workspace green. That converts CB-9a from a caller-count observation into a proven gap, which is
what makes it routable rather than arguable.

**CB-9b — no. Not reachable by any means, and this is the first-class half.** There is no run-level
path from `set_skew` to anything, because nothing owns a `Clock`. I cannot drive skew "through a
scenario" because no scenario has a clock to skew. This is not a fixture gap and no fixture fixes
it; it is a missing composition in `src/`.

### 2b.3 What I would see if it were broken

- **CB-9a broken:** my probes read the wrong `estimate`, or `bound_established` comes back `true`
  after `set_skew(.., false)`. Loud, once the probes exist. **Silent today** — that is the point.
- **CB-9b broken:** *nothing, from any direction.* This is the lead's own warning and it is exactly
  right: an unused injection API reads as coverage from every angle except a caller search. The
  module doc asserts the behaviour, the type is `pub`, `missing_docs` is denied so it is fully
  documented, clippy is clean, and `#[deny(missing_docs)]` plus a `pub` API is indistinguishable
  from a wired one. **Dead-code lints do not fire on `pub` items in a library.** There is no
  observable that distinguishes "the simulator can inject clock skew" from "the simulator declares
  a type that can, and never does".

### 2b.4 Entry points I need that do not exist

- **CB-9a: none.** I can drive it today. I am not asking for anything.
- **CB-9b: D8 below.** Something in `crates/rdb-sim/src` must own a `Clock` and fill
  `StepCtx::control_time` from `Clock::control_time(node)`, or the crate's own layout table must
  stop claiming `sim` owns the clock. **Either answer closes it; ambiguity does not.** This is the
  same demand as §2.3's item 3, promoted, and it is now unblocked from item 2: even if no kernel
  ever reads `control_time`, an environment that declares a fault it cannot inject is a defect on
  its own terms.

### 2b.5 Where I expect a defect shape

- **(d-strong), and this is its purest form yet.** CB-8 is a fixture field frozen to a constant.
  CB-9 is an entire capability frozen to "absent" — not by a constant, but by never being
  constructed. Every row in the workspace runs against an environment with no clock, and no row
  can tell, because the absence is in the composition rather than in any value a row can read.
- **The sixth shape, pre-emptively.** If CB-9b is closed by wiring a `Clock` in, the claim "no row
  needs skew" is retired silently — kernel-a's 189 authority rows are the ones that will need it
  (`ClockUnbounded`, `ClockSampleStale`, K-A-50 retraction). Decide before those rows are written,
  not after. The lead's framing is the right one and I endorse it: either a scenario drives
  `set_skew` and the authority rows inherit real skew, or the API is admitted unused and the rows
  that need skew say so in their own plan.

**Severity: I rate CB-9b MATERIAL and ahead of CB-8.** CB-8 is an additive convenience for a field
nothing reads. CB-9b is a declared fault-injection capability that does not exist, in the crate
whose entire purpose is deterministic fault injection.

---

## 3. kernel-a ask 9 — judged not a reshape

### 3.1 How I will drive this by hand

Ask 9 produces **no foundation artefact**. `ClockSample` does not exist in `crates/` at this HEAD
(`grep -rn "ClockSample" crates/` returns one unrelated hit: `authority.rs:68`,
`DenyReason::ClockSampleStale`). There is no behaviour of foundation's for me to drive.

What is testable is the *negative* claim the design makes, and it is worth testing because the
lead's brief originally asserted the opposite:

**N1 — `contracts/time.rs` is untouched by the freeze.** After the four changes land:
`git diff <basis>..HEAD -- crates/rdb-core/src/contracts/time.rs` must be **empty**. I will run
this through my export (`git show <sha>:<path>` comparison), not against the shared tree.

**N2 — CB-8 and ask 9 do not collide.** Once both land, `scripts/gate.sh lint` and
`scripts/gate.sh test -p rdb-sim` are clean with `ctx_with_control_time` present and whatever I1
built present. Sequencing claim confirmed by both orders compiling; I can only observe whichever
order actually happens.

**N3 — the drift basis moved.** CB-7 edits `crates/rdb-core/src/contracts/`, which per AGENTS.md
moves the drift basis and red-builds all four §15 tables. `scripts/gate.sh drift` must go **red**
after CB-7 and before the four plans' section 15 markers are re-read. If it stays green, the drift
stage did not fire on a `contracts/` edit, which is the stage's whole job.

### 3.2 What I would see if it were broken

- N1 broken (someone did reshape `ControlTime`): a non-empty diff, and kernel-a's I1 conversion
  table (`kernel-a/design.md:850-857`, four field mappings) silently means something else.
- N2 broken: a compile error in `rdb-sim`. Loud.
- N3 broken: `drift` green after a `contracts/` edit. **Silent, and it over-holds** — plans report
  `Unavailable` on types that have landed. AGENTS.md records exactly this happening on 2026-09-20
  with all four teams stale simultaneously.

There is a fourth thing I would like to see and **cannot**: whether I1's conversion copies
`bound_established` into `ClockSample.valid` "and nothing else", as
`kernel-a/design.md:855` requires. Until I1 exists there is nothing to observe. I flag it below.

### 3.3 Entry points I need that do not exist

- **`scripts/gate.sh drift` must actually be run after CB-7 lands and before the markers move.**
  Not a new entry point; a sequencing demand. If the markers are moved in the same commit as
  CB-7, N3 is unobservable for ever. AGENTS.md is explicit that the stage "catches the plan that
  forgot; it cannot catch the author who skipped."

### 3.4 Where I expect a defect shape

- **(d-strong), one layer up, in kernel-a's I1 fixture.** The design's §4 open question 1 already
  raises this and it is correct: if I1's scaffolding hardcodes `bound_established: true` on the
  `ControlTime` it converts, `ClockSample.valid` is frozen `true`, and A1's `ClockUnbounded` fence
  and its K-A-50 retraction rule — both of which kernel-a's design says are "already tested here"
  — become unreachable from I1. That is the identical shape as CB-8, reproduced in a crate where
  it *would* matter, because A1 genuinely reads `valid`. **This is the highest-value thing in
  ask 9 and it is out of foundation's file ownership.** Route to kernel-a's test planner before
  they write I1's fixture, not after.

  *Disagreement with the lead's reading, narrowly.* The lead's CB-9 note says this open question
  "does not repeat there, because the conversion has no caller to repeat it in." True **today** —
  `ClockSample` does not exist (`grep -rn "ClockSample" crates/` returns one unrelated hit,
  `authority.rs:68`). But the question is about the fixture kernel-a is *about to* write, and the
  risk is that it is written with the same frozen constant. "No caller yet" is a reason it cannot
  be observed now, not a reason it will not happen. I would keep the item open and route it, not
  close it. This is the kind of claim that quietly carries its scope forward — the sixth shape.
- **The equivalence-argument shape (the sixth).** "Ask 9 does not touch `contracts/time.rs`" is
  proved under today's kernel-a design. If kernel-a later decides A1 *should* call
  `ControlTime::compare` after all, nothing in this freeze turns red. N1 is the guard, and N1 must
  be re-run at each later milestone, not once. Recommend the claim be written down with its scope
  attached: "true as of kernel-a design at `<sha>`".

---

## 4. G-13 — seed the policy-version floor on restore

### 4.1 How I will drive this by hand

The design's §3 steps 1-5 are a daemon-level sequence. **Step 3 and step 4 need a fixture that
`docs/testing/test-plan-m6.md:505` records as not existing** (M6-34: "Needs a real daemon started
against a *restored* directory under a second, distinct cluster identity. `support::Harness`
hardcodes its cluster id via the free function `cluster_id()`"). I confirmed the gap rather than
taking the plan's word. *Paths searched:* `grep -rn "restore_into_fresh_store\|RocksStore::open"
crates/config-server/tests/` across all twelve test files, and a read of each hit. Two hits, both
in `m5_admin.rs` (`:905`, `:1039`), and both open the restored directory as a **bare
`RocksStore`** — no process, no client plane. No file under `crates/config-server/tests/` starts a
daemon against a restore target.

So I will drive G-13 in **two layers**, and the lower one is the one I can actually run today.

**Layer A — storage, no daemon. Runnable now, and it is the layer where the defect actually is.**

1. `cargo run -p config-server -- backup --data-dir <stopped dir> --out <art>` (offline path,
   `backup.rs:280-285`) and, separately, take an RPC-path backup so I have one manifest with
   `policy_version_ref: Some(N)` and one with `None`.
2. Read the manifest TOML by hand (`cat <art>/<name>.manifest.toml`) and confirm the field. This
   is a text file; no tooling needed.
3. `cargo run -p config-server -- restore --from <art> --data-dir <fresh> --cluster-id <new>
   --recovery-epoch <n+1> --node-id 1 --manifest <new bootstrap> --trust-key <key>`.
4. **Read the floor cell out of the restored directory directly.** This is the observable that
   matters and it is the one I do not have — see §4.3 demand D1.
5. Read the `restore_completed` audit line from stdout (`main.rs:240-250`) and check the new
   field.

**Layer B — daemon, needs M6-34's fixture (does not exist).** Steps 3/4 of the design's sequence:
start a signed-policy daemon against the restored directory with an older document, without and
then with `--break-glass-policy-rollback`; observe `policy_rejected` with
`reason: "rollback_floor"`, then `policy_loaded` with `break_glass: true` and the rollback counter
incrementing. I will drive this by hand from the shell if the fixture exists; if it does not, I
will drive the *authorizer* half only, which is Layer A plus reading
`SignedPolicyAuthorizer::adopt` outcomes from a scratch bin.

### 4.2 What I would see if it were broken

**The honest answer for the seeded path (steps 1-4): I can see it, provided D1.**

- Floor not written at all: step 4 reads `0`; step 3's daemon accepts the old document. Observable.
- Floor written under the wrong key bytes: reads `0`. **Indistinguishable from "not written".**
  Still *detectable* (both are "broken"), so this is adequate — but see the shape note below.
- Floor written but read too late: `PolicyLoader::new` seeds before the first `adopt`
  (`config-server/src/policy.rs:158-160`), verified. If someone moves the read, step 3 silently
  accepts. Only step 3 catches it, so **Layer B is not optional** for the full claim.

**The honest answer for step 5 (the `None` / offline path): I cannot see it. This is a finding.**

The brief asked precisely this, and the answer is no.

`RocksStore::policy_version_floor()` is `read_meta(...).unwrap_or_default()`
(`crates/config-storage/src/rocks.rs:1325-1330`). It returns **`0` for an absent cell**. The gate
is `below_floor = floor > 0 && to < floor` (`config-core/src/policy.rs:767-772`) — **inert at 0**.
Therefore:

| State | `policy_version_floor()` | `adopt` behaviour | Distinguishable? |
|---|---|---|---|
| cell absent | `0` | accepts anything | — |
| cell present, value `0` | `0` | accepts anything | **No** |

There is no observable, at any layer, that separates them. **Defect shape (a-variant), exactly as
the brief predicted.**

*Paths searched before claiming "no observable":* every reader of the cell —
`grep -rn "policy_version_floor\|KEY_POLICY_VERSION_FLOOR" crates/` (four sites: the const at
`rocks.rs:197`, the reader at `:1325`, the writer at `:1349`, the trait use at
`config-server/src/policy.rs:158-160`) — plus the whole `Option`-returning surface of
`read_meta`, which `policy_version_floor()` collapses with `.unwrap_or_default()`. The `Option`
exists one call further in; it is discarded at exactly the point where I need it.

And it is not merely academic. The design proposes
`RestoreReport.policy_version_floor: Option<u64>` plus a `restore_completed` audit field as the
auditability story. **That field is sourced from `manifest.policy_version_ref` — the same input the
write is sourced from.** An implementation that computes the report field, logs it correctly, and
*omits the `batch.put_cf`* passes every assertion on the audit line. That is **defect shape (a) in
its pure form: the oracle reads its expected value out of the same place as the thing under
test.** The audit line is a report of intent, not of effect.

There is a second edge with the same signature: a manifest carrying
`policy_version_ref: Some(0)` — legal, because `config-core/src/policy.rs:745-747` says in terms
that "a fresh node must still accept its first document, including the `version: 0` a hand-built
document can carry". `Some(0)` seeds a cell that reads identically to unseeded. So G-13 provably
does nothing for a version-0 lineage, and no row can tell.

### 4.3 Entry points I need that do not exist — **my demand list for G-13**

**D1 (required — I cannot verify G-13 at all without it). A read-only way to print
`state_meta/policy_version_floor` from a stopped data directory.** Either:
- a `config-server inspect --data-dir <dir>` subcommand that prints the state-meta cells,
  including `present: true/false` for the floor **separately from its value**; or
- at minimum, `policy_version_floor()` gaining a sibling that returns `Option<u64>`
  (`None` for absent) and being reachable from the CLI.

The `Option` is not a nicety. Without it, D1 still cannot distinguish absent from zero, and step 5
stays untestable. **`Option<u64>` is the specific thing I am asking for.**

**D2 (required for the seeded path's end-to-end claim). M6-34's fixture**, as the design's §3
"What M6-34 needs" already describes it: a harness that takes an explicit `ClusterIdentity`
instead of the hardcoded `cluster_id()` free function, restores into a fresh directory, and starts
a real daemon against it exposing the client plane. This is a **known, already-reported gap**
(`docs/testing/test-plan-m6.md:505`, flagged by tester-m6a 2026-09-19). Naming it again is not a
failure to reach; it is the same gap blocking the same claim a second time.

**D3 (required). A compile-time or test-time link between the two copies of the key bytes.**
The design adds `restore_keys::POLICY_VERSION_FLOOR = b"policy_version_floor"` in
`config-storage/src/snapshot.rs`, mirroring the private `KEY_POLICY_VERSION_FLOOR` at
`rocks.rs:197`, with a comment saying "must equal". Nothing enforces it.

This mirror is **not** like the existing ones. `restore_keys::IDENTITY` has a reader that fails
loudly — `RocksStore::open` refuses a directory with no identity, and `m5_admin.rs:905` proves it.
The floor's reader fails **silently**: a one-byte typo yields `0`, which is exactly the
pre-G-13 behaviour. A typo and "not implemented" are the same observable. Both constants are in
**the same crate** (`config-storage`), so `pub(crate)` on `KEY_POLICY_VERSION_FLOOR` removes the
mirror entirely at zero cost. I am asking for that, or for a `const _: () = assert!(...)`.

**D4 (I need an answer, not code). Two documented contracts say restore must not do this. Which
one wins?**

1. `crates/config-server/src/backup.rs:98-101`, on `policy_version_ref` itself:
   > "**Nothing checks it at restore**, because the independently supplied policy may legitimately
   > be older, newer or unrelated; it exists so a recovering operator can tell which document the
   > data was authorized under."
2. `crates/config-server/src/cli.rs`, on `--active-policy-version`:
   > "the manifest's reference is a **breadcrumb for a human and not a validation input**:
   > verifying a signed document here would buy nothing the daemon does not already do at startup,
   > and would pull policy verification into a recovery path that deliberately depends on as little
   > as possible — restore reads no configuration file at all."

G-13 turns the breadcrumb into a validation input. The design's §3(d) cites the *first* doc — but
only its `null` sentence, not the sentence that says nothing checks it and **why**. The "why"
is exactly the failure mode I describe in §4.4: the supplied policy may legitimately be
*unrelated*. I am not saying G-13 is wrong. I am saying two shipped doc comments will be false the
moment it lands, and nobody has written down which claim is being retired. Per my round-2
handoff's sixth shape, an argument carries the scope it was proved under.

### 4.4 The question that matters: **is there a sequence where an operator does everything right and still cannot recover?**

**Yes. One, and it is the ordinary disaster-recovery sequence.**

Restore mints a **new cluster identity** — that is the point of it (`cli.rs`: "The new cluster id
and the advanced recovery epoch are not conveniences: they are what makes spec §19.11 hold
structurally"). The signed policy document is **bound to a cluster id**: `verify_policy(&document,
&signature, &trust_keys, self.expected_cluster)` (`config-server/src/policy.rs:230-235`), and
`signed.document.cluster_id` may be `None` (unscoped) or must match.

So an operator recovering a cluster scoped at old-cluster-id **must issue a new signed document
scoped to the new cluster id**. It is a new document in a new lineage. Whatever version their
issuing tooling stamps on it — `1` is the obvious choice for a brand-new cluster — is compared
against a floor seeded from the **old** cluster's `policy_version_ref`.

```
old cluster policy at version 412 → backup manifest policy_version_ref = 412
restore → new cluster id, floor seeded to 412
operator issues correct, correctly-signed, correctly-scoped document for the new cluster, v1
daemon boots → below_floor = 412 > 0 && 1 < 412 → PolicyRejected::RollbackFloor
```

The operator did everything right. Every input is correct. And:

- The daemon **does not exit**; it comes up `AuthzKind::NoValidPolicy`
  (`config-server/src/run.rs:1136-1138`).
- `SignedPolicyAuthorizer::authorize` with no active document returns
  `Decision::deny(REASON_NO_VALID_POLICY)` for **everything** (`config-core/src/policy.rs:960-963`).
- The admin-plane `ReloadPolicy` RPC — the in-band repair route — is not available: its own doc
  says "The admin plane has already checked the caller against the **currently active** document's
  `admins`" (`config-server/src/run.rs:468-470`), and with no active document `admin_set()`
  returns `None`.

So the cluster is up, refusing every call, with **no in-band route to install a policy**. The only
exits are a process restart with `--break-glass-policy-rollback`, or re-issuing the document at
version > 412.

The design's claim is that this is fine because "restore inherits break-glass for free" and the
operator would "already need the same flag to force the identical downgrade on a plain restart."
**That equivalence does not hold in this sequence**, for three reasons:

1. It is **not a downgrade.** Version 1 of the new cluster's lineage is not older than version 412
   of the old cluster's. Two lineages' version numbers are not comparable, and `below_floor` is a
   bare integer compare that assumes they are. The design never addresses cross-lineage seeding —
   it reasons throughout as though the restored node continues the same policy lineage.
2. The operator has **no reason to reach for a break-glass flag**, because from where they stand
   nothing is being rolled back. `RollbackFloor { floor: 412, incoming: 1 }` at least prints both
   numbers, which is the one mercy here — but the flag is named
   `--break-glass-policy-rollback` and the operator is not performing a rollback.
3. The comparison case ("the identical downgrade on a plain restart") **cannot arise**, because a
   plain restart never changes cluster id, so a plain restart never faces a foreign lineage.

**This is a MATERIAL finding on the design, and it is the exact thing the brief asked me to
hunt.** It is not a reason to abandon G-13. Narrower fixes exist that I would accept:

- Seed only when the manifest's `cluster_id` and the restore's `--cluster-id` describe the same
  policy lineage — which, for a restore, they never do by construction. That collapses to "do not
  seed on restore", which defeats the change.
- Seed, and make the refusal message say the restore-specific thing: name the source cluster, the
  seeded floor's provenance, and `--break-glass-policy-rollback` by name. The gate stays; the
  operator is not left guessing. **This is what I would recommend.**
- Bind the seeded floor to the source cluster id, and ignore it when the active document's
  `cluster_id` differs. More machinery; closes the hole properly.
- Require `--active-policy-version` (already a flag on `restore`, already optional) to be present
  whenever the manifest names one, and seed from *that* — the operator's stated intent — rather
  than from the manifest. Then the floor reflects the lineage the operator is actually installing.

I am not choosing between these. I am reporting that **step 3 of the design's own hand-test, run
with correct inputs on the most ordinary recovery, refuses the operator**, and that the design's
break-glass-is-inherited argument does not cover it.

Note for the lead's verification: the two facts the lead checked are both true and I re-verified
them (`policy.rs:158-160` seeds on every boot; `config-core/src/policy.rs:767-772` plus
`break_glass: below_floor` at `:788` is the only gate). **The facts are right; the inference from
them is what does not survive the cluster-identity change.**

### 4.5 Where I expect a defect shape

| Shape | Where |
|---|---|
| **(a)** — the oracle reads its expected value out of the thing under test | `RestoreReport.policy_version_floor` + the `restore_completed` audit field. Both sourced from `manifest.policy_version_ref`; an implementation that skips the `put_cf` passes every assertion on them. **Any row asserting the audit line and not the cell proves nothing.** |
| **(a-variant)** — the observable takes the same value on both sides | `policy_version_floor()` returns `0` for absent and for seeded-zero. Step 5 of the design's hand-test cannot distinguish them. Also covers `policy_version_ref: Some(0)`. Fixed by D1's `Option<u64>`. |
| **(d)** — a fixture that can only build the degenerate case | M6-34's absent fixture. Every existing restore row opens a bare `RocksStore` (`m5_admin.rs:905`, `:1039`) and never starts a daemon, so every restore row that exists can only exercise the storage half. The behaviour half has no fixture at all. |
| **(c)** — passes because a timeout was generous | Layer B, if it is ever built. A daemon that comes up `NoValidPolicy` and one that is merely slow to adopt look the same to a poll loop. Any Layer-B row must assert the `policy_rejected` line with `reason: "rollback_floor"` by field, not assert "client plane did not open within N". |
| **The sixth shape** — an equivalence argument carrying its scope | "Restore inherits break-glass for free." Proved under same-lineage assumptions; retired by the cluster-identity change that restore itself performs. §4.4. |

---

## 5. What I cannot reach at all

Stated plainly, so nobody records these as covered.

1. **CB-8 through a kernel.** Not "hard" — impossible. Nothing reads `StepCtx::control_time`, and
   kernel-a's design says A1 never will (`kernel-a/design.md:875-877`). §2.2. Needs demand 2/3 in
   §2.3, or a written decision that the field is dead.
   *Paths searched:* `grep -rn "impl Module" crates/` for every `step` signature (all six take
   `_ctx`), plus `grep -rn "control_time\|ControlTime" crates/` across every crate's `src/` and
   `tests/`.

1b. **CB-9b — skew through a scenario.** No scenario has a clock. Nothing in any `src/` owns a
   `Clock`; the type's only runtime instance in the workspace is in my own round-1 guard row.
   §2b. Needs D8. **CB-9a (skew at the unit level) I *can* reach today and will drive** — do not
   record CB-9 as wholly unreachable.
   *Paths searched:* `grep -rn "set_skew" crates/ docs/ .claude/scratchpad/`;
   `grep -rn "Clock::new" crates/`; `grep -rn --include=*.rs "Clock" crates/*/src`; and a read of
   `Dispatcher`'s field list at `harness/dispatch.rs:75-88`.
2. **"Floor absent" versus "floor seeded to 0".** No observable anywhere. Needs D1's `Option<u64>`.
   Until then, the design's §3 step 5 is a step I will run and cannot judge. §4.2.
3. **G-13 steps 3 and 4 end to end.** No fixture starts a daemon against a restored directory under
   a second identity. Known gap, `test-plan-m6.md:505`, open since 2026-09-19. Needs D2. I can
   drive the authorizer half from a scratch bin; that is strictly less than the claim.
4. **Whether I1 copies `bound_established` into `ClockSample.valid` and nothing else.** `ClockSample`
   does not exist. Out of foundation's ownership; routed to kernel-a's test planner in §3.4.
5. **Whether the `restore_keys` mirror matches the `rocks` constant.** Detectable only as "floor
   reads 0", which is the same observable as "not implemented". Needs D3.

---

## 6. Summary of demands, one line each

| # | Demand | Blocks |
|---|---|---|
| D1 | A read-only surface that prints `state_meta/policy_version_floor` as **`Option<u64>`** — absent distinguished from zero | All of G-13. **Hard blocker.** |
| D2 | M6-34's fixture: explicit `ClusterIdentity`, real restore, real daemon against the restored directory | G-13 steps 3-4 (the behaviour half) |
| D3 | `pub(crate)` on `rocks::KEY_POLICY_VERSION_FLOOR` (same crate) instead of a second copy in `restore_keys` | G-13 silent-typo mode |
| D4 | A written ruling on which of the two shipped doc comments (`backup.rs:98-101`, `cli.rs` `--active-policy-version`) is being retired | G-13 acceptance |
| D5 | A `DefinitelyAfter` / overlap-`Uncertain` assertion in `rdb-core/tests/seams.rs` — the reach exists; the coverage does not | The real CB-8 gap |
| D6 | A written decision: does any kernel ever read `StepCtx::control_time`? If no, say so; if yes, wire `dispatch.rs:152` from `Clock::control_time` | CB-8's hand-test #2 |
| D7 | Run `scripts/gate.sh drift` after CB-7 and **before** the §15 markers move | N3 |
| **D8** | **Something in `crates/rdb-sim/src` owns a `Clock` and fills `StepCtx::control_time` from `Clock::control_time(node)` — or `lib.rs`'s layout table stops claiming `sim` owns the clock** | **CB-9b. Either answer closes it.** |
| — | Permission to compile a scratch bin target inside my own export (not committed) | CB-7 P1/P4, CB-9a |

Entry points needed for CB-7: **none**. For CB-9a: **none** — I can drive it today from a scratch
bin, and I will. Those are the two of five items I can reach without anyone building anything.

### Priority, if the Developer can only act on some of this before starting

1. **D1** — without it G-13 cannot be verified at all, and G-13 is the restore path.
2. **D8** — CB-9b is a declared capability that does not exist; deciding it after kernel-a writes
   189 authority rows is far more expensive than deciding it now.
3. **D4** — a ruling, not code; blocks G-13 acceptance rather than G-13 work.
4. **D3** — one `pub(crate)`, same crate, removes a silent-failure mode for free.
5. **D2** — the largest build; needed for G-13's behaviour half, not its storage half.
6. **D6**, **D5**, **D7** — a written decision, a coverage placement, and a sequencing rule.
