# Critic — M7 Foundation Contracts Freeze

Reviewer: `critic-foundation-freeze`, 2026-09-21. Basis `aa0b6bf`, branch `feature/rdb-m7`.
Reviewed: `teams/foundation/design-contracts-freeze.md`.

**Verdict: FAIL.** One BLOCKER (CB-7's variant set), five MATERIAL, two ADVISORY, plus one
escalation (CB-9).
G-13 (a), (c), (d) and the same-batch placement survive scrutiny. CB-8 is real but a quarter
the stated size, and its proposed fix is at the wrong layer.

The lead's six pre-verified claims were not re-derived. One was spot-checked and **holds**:
`KernelEffect` is named in exactly two `.rs` files outside `event.rs` — `rdb-core/tests/seams.rs`
and `rdb-sim/tests/dispatch.rs` (`rdb-core/src/authority.rs:23` is a doc link, not a use).
`dispatch.rs:483` moves the value out of an array literal, so dropping `Copy` does not break it.

---

## F-1 — BLOCKER. `KernelIgnoredReason`'s two variants cannot spell ~30 reason names the M7 plans already assert.

**Criterion violated.** L-R62, the ruling CB-7 exists to close, is scoped to "kernel-b's
**fifteen** ladder drop reasons" (`ledger.md`, L-R62). The design's evidence is kernel-b
`design.md:983-984` — a four-name table — and it generalises from there.

**Location.** `design-contracts-freeze.md:73-78` ("needs no new leaf names") and `:405-408`
(Q2: a third variant is "a possibility, not built").

**Evidence — kernel-a, which the design never examined.** Landed source,
`crates/rdb-core/src/authority.rs:23`:

    //! - `Fact(..)` is [`crate::contracts::event::KernelEffect::Ignored`] (ruling A-R24).

`docs/testing/test-plan-m7-kernel-a.md` contains **45** `Fact(..)` assertions over **20 distinct
names**: `AdmissionRefused`, `AdmissionSuspended`, `AlreadyBlocked`, `CandidateUnreachable`,
`DispatchDroppedByFreeze`, `DispatchRefusedFrozen`, `FamilyRejected`, `FenceWhileBlocked`,
`LateRenewalIgnored`, `PublishDeferred`, `PublishRefusedBlocked`, `PublishedWhileFrozen`,
`QualificationLostAfterPublish`, `Quarantined`, `ReplySuppressedAfterTimeout`, `ReplyWithheld`,
`StaleAuthorityAnswer`, `StaleAuthorityView`, `StaleTimer`, `TakeoverDeferred`.
**None is an `ErrorKind` variant** (`errors.rs:79-116`, eighteen names, all spec §5.4 client
errors). **None is an `AppendReject` variant** (`envelope.rs:524-597`, sixteen names, all
append-ladder refusals). Named rows: M7A-18, M7A-22, M7A-36, M7A-124, and 41 more.

**Evidence — kernel-b, beyond §3.6.** `docs/testing/test-plan-m7-kernel-b.md:501` (committed;
`git show HEAD:docs/testing/test-plan-m7-kernel-b.md` carries it) enumerates the fifteen and
rules on each. Nine have no counterpart in **either** enum the design reuses:
`NOTHING_OUTSTANDING` (M7B-64), `NO_QUALIFYING_SECONDARY` (M7B-76), `BARRIER_NOT_DURABLE`
(M7B-74), `NOT_A_CURSOR_EVENT` (M7B-61), `OUTSTANDING` (M7B-55), `NOT_FENCED` (M7B-84),
`ALREADY_DIVERGED` (M7B-140), `ALREADY_BLOCKED` (M7B-142), `NOT_REQUIRED` (M7B-128).
`FORGED_ACK` (M7B-31) is a tenth — its landed name is `AckRejectReason::ForgedIdentity`
(`trace.rs:315`), a third enum the design does not reuse. §13 at `:409` lists thirteen rows held
on this ask by id.

So the lead's test — "if a reason in that table maps to neither, the design is wrong now" — is
met, and met an order of magnitude wider than the table.

**Consequence.** CB-7 ships, the drift basis moves, all four §15 tables are re-read
(`docs/testing/test-plan-m7-foundation.md:536` states exactly this cost), and kernel-a's 45
assertions plus kernel-b's thirteen rows are **still** `Unavailable`. Closing them then needs a
second `contracts/event.rs` edit: a second basis move and a second round of four §15 re-reads.
That is the expensive undo the freeze exists to prevent, paid twice.

**False-positive check — what would make me wrong.** Any of:
(i) A-R24 is superseded and `Fact(..)` now has its own `EffectKind` variant. I checked: `EffectKind`
has seven variants (`event.rs:250`-`:346`) and none is `Fact`; `authority.rs:23` is current source
at `aa0b6bf`, not a plan.
(ii) The M7 rows are meant to assert `Fact`/`Ignored` **presence and arity** rather than the reason
value. Falsified by `test-plan-m7-kernel-b.md:55`: "the reason is now a typed C0 value and **a row
pins it by value** — there is no spelling left to leave open."
(iii) The lead intends kernel-a's vocabulary to be out of CB-7's scope. That is a decision, not a
refutation — but it must be **recorded**, because today `authority.rs:23` routes it here.

**Closure condition.** `KernelIgnoredReason` carries a third variant for kernel-internal facts
(e.g. `Kernel(IgnoreReason)`), and a table in the design maps every one of kernel-a's 20 `Fact`
names and kernel-b's 15 `Ignored` names to a spelled variant, with no name left unmapped.
Observable: `grep -o "Fact([A-Za-z]*)" docs/testing/test-plan-m7-kernel-a.md | sort -u` and
BA-11's list at `test-plan-m7-kernel-b.md:501` both resolve against the frozen enum with zero
residue.

**Not an escalation, but a scope decision the lead owns.** CB-7 was recorded against kernel-b only
(L-R62). Covering kernel-a widens a recorded ask; it does not require a fifth change. The open
question is *who owns the variant set* — `test-plan-m7-kernel-b.md:409` says "The lead's call, not
the developer's", and kernel-a's 20 names were never routed to anyone.

---

## F-2 — MATERIAL. Two of the names that *do* exist in `AppendReject` are ones the plans forbid using.

**Criterion.** `test-plan-m7-kernel-b.md:497`: "**No row may conflate them.** M7B-44's
`NotAMember` is the tracker dropping an ACK (`AckRejectReason`); §3's row-13 `NotAMember` is the
receiver refusing an append (`AppendReject`). ... a row that asserts one where the other belongs
**passes for the wrong reason**." Same ruling at `:491`.

**Location.** `design-contracts-freeze.md:73-78`, which counts `AppendReject` coverage by name.

**Evidence.** `AppendReject::NotAMember` (`envelope.rs:568`) is documented as "the authenticated
peer is not the primary of the pinned configuration". M7B-44 and M7B-145 assert
`Ignored{NOT_A_MEMBER}` for an ACK from a copy outside the pinned config — a tracker drop, not an
append refusal. Same shape for `Quarantined`: `AppendReject::Quarantined` is "row 0: the receiver
is quarantined" (`envelope.rs:526`), while kernel-b's `QUARANTINED_TERMINAL` (M7B-119) is F1's
terminal phase and kernel-a's `Fact(Quarantined)` is an authority fact. Three different facts,
one word.

**Consequence.** Name-level coverage overstates real coverage by three, and the two that look
covered are the dangerous kind: a row compiles, passes, and proves something else.

**False-positive check.** I am wrong if the lead rules that `KernelIgnoredReason::AppendRejected`
is understood to mean "the append ladder answered this", in which case these rows simply cannot
use it and fall under F-1's count instead of being a separate defect. Either way they are not
covered; only the bookkeeping changes.

**Closure.** The mapping table demanded by F-1 assigns `NOT_A_MEMBER`, `QUARANTINED_TERMINAL` and
`Fact(Quarantined)` to a variant that is **not** `AppendRejected(..)`, and says so in one line each.

---

## F-3 — MATERIAL. `restore_into_fresh_store` has five call sites; the design names one.

**Criterion.** A signature change to a `pub` item re-exported from `config_storage`
(`crates/config-storage/src/lib.rs:39`) must account for every caller.

**Location.** `design-contracts-freeze.md:352-355`: "Call site,
`crates/config-server/src/backup.rs:977-983`, one new argument" — singular.

**Evidence.** `grep -rn "restore_into_fresh_store" crates/` returns five callers:

- `crates/config-server/src/backup.rs:978` — the one named
- `crates/config-storage/tests/m5_snapshot.rs:1214`
- `crates/config-testkit/tests/m5_backup_fencing_cluster.rs:218`
- `crates/config-testkit/tests/m6_compat_cluster.rs:604`
- `crates/config-testkit/tests/m6_evidence.rs:590`

Three of the four unnamed sites are in crates the foundation scope does not own
(`config-storage`, `config-testkit`), and `m6_evidence.rs` is the binary that produces
`docs/evidence/*.json`, which is modified in the working tree right now.

**Consequence.** As designed, the change is red-before-green across three crates. Per
`AGENTS.md` ("Several agents, one working tree"), a `config-testkit` compile error another team
did not cause is exactly the failure that gets misattributed — this repository has a recorded
incident of it on 2026-09-21.

**False-positive check.** I am wrong if the four are already scheduled in a handoff I did not
read, or if the implementer takes a route that leaves the 4-arg form intact (a second function, or
a parameter struct). The design proposes neither.

**Closure.** The design's §3 names all five sites and says what each passes (`None` for the four
test sites), and the implementer's handoff carries the same list.

---

## F-4 — MATERIAL. G-13(b) turns a documented universal limit into a silent exception, and the audit line is the only thing that stops it.

**Criterion.** A security control whose coverage is partial must make the uncovered case
observable at the moment it applies.

**Location.** `design-contracts-freeze.md:267-276` (the (b) decision) together with `:352-355`
(the audit extension, which the design defers to the lead as an open question at `:409-412`).

**Evidence.** Today the hole is universal and documented: `crates/config-storage/src/rocks.rs:1316`
— "a directory restored from a backup starts here at `0`, and the downgrade this cell exists to
refuse succeeds once across that restore". An operator reading the floor's own doc learns this
about **every** restore. After G-13, the RPC path seeds a floor and the offline path does not, so
absence stops being the rule and becomes a per-artifact property — decided at backup time by
`crates/config-server/src/backup.rs:285`'s `None`, invisible in the artifact's filename, and
consumed at restore time by a different operator on a different day.

**Consequence.** The change strictly improves coverage and strictly worsens *legibility* of what
is left. An operator who has internalised "restore seeds the floor" will not notice the one
restore where it did not.

**False-positive check.** I am wrong if the `restore_completed` audit line renders
`policy_version_floor: None` as a positive statement ("no floor seeded from this artifact") rather
than as an omitted or empty field, and if the known-limit doc at `rocks.rs:1306-1324` is rewritten
to name the surviving case. Both are in the design's gift; neither is stated.

**Closure.** The audit line distinguishes seeded-`N` from seeded-nothing in words, and the
`rocks.rs` known-limit paragraph is narrowed from "a restore" to "a restore from an artifact whose
`policy_version_ref` is null". Manual-test step 5 (`:389-391`) then has something to assert.

**This inverts the lead's question 4.** The audit extension is not the discretionary piece — it is
the mitigation the whole (b) decision rests on. Accepting it was right; leaving it as an open
question is not.

---

## F-5 — ADVISORY. `RestoreReport.policy_version_floor` echoes the caller's own argument.

**Location.** `design-contracts-freeze.md:341-350`.

**Evidence.** The field is written from the `policy_version_floor: Option<u64>` parameter the
caller just supplied, and the only production caller (`backup.rs:978`) still holds
`manifest.policy_version_ref` in scope at the point it would read the report back. `RestoreReport`
is not `#[non_exhaustive]` (`snapshot.rs:1119-1121`), so the field is a breaking addition for any
struct-literal construction.

**Consequence.** Small. A pub field that is definitionally equal to an input is one that can
silently diverge from what was actually written if a later branch skips the `put_cf`.

**False-positive check.** I am wrong if the field is set from the write branch rather than the
parameter — then it is a genuine write-confirmation and worth its surface. The design's sketch
sets it from the parameter.

**Closure.** Either the field is populated inside the `if let Some(v)` arm that performs the write,
or it is dropped and the audit line reads `manifest.policy_version_ref` directly.

---

## F-6 — MATERIAL. CB-8's builder is new API surface at the wrong layer; the two genuinely missing branches need no fixture at all.

**Criterion.** The smallest change that is still honest. Also the lead's correction of
2026-09-21, which I verified rather than accepted.

**The corrected picture — checked, and the lead is right.**
`crates/rdb-core/tests/seams.rs:50-92`,
`m7f_14_a_stale_sample_is_uncertain_even_when_the_bound_is_confident`, already asserts **four** of
the six branches by constructing a `ControlTime` literal and calling `compare` directly:
`DefinitelyBefore` (`:62-65`), stale-by-`>` (`:68-74`), future-stamped sample (`:77-81`), and
`bound_established: false` (`:84-91`). So `design-contracts-freeze.md:126-130`'s "no file under
`crates/rdb-sim/tests` mentions `ClockVerdict` or `bound_established`" is true and irrelevant —
the assertions live in `crates/rdb-core/tests`.

**What is actually missing.** Exactly two arms of `ControlTime::compare`
(`crates/rdb-core/src/contracts/time.rs:138-155`):

    } else if self.estimate.0 > instant.0.saturating_add(slack) {
        ClockVerdict::DefinitelyAfter      // :150 — unasserted
    } else {
        ClockVerdict::Uncertain            // :152 — the overlap; unasserted
    }

Both are in the **arithmetic** half, below the early return. Both are `const fn`, pure, and take
four scalars plus a `ControlTime` literal. Neither needs a `StepCtx`, a module, or a fixture.

**Consequence.** As proposed, CB-8 adds a public helper to
`crates/rdb-sim/tests/support/mod.rs` to reach two branches that `m7f_14` could reach with two
more `assert_eq!` calls, in the file where the other four already live and next to the sign they
protect. The design's own §2 says as much at `:180` — "All six branches are reachable today, with
or without this change" — and then proposes the change anyway.

**The surviving argument for the builder, and why it does not land here.** It is real that no
`rdb-sim` row can reach those branches *through the shared `StepCtx` fixture*, because
`support::ctx()` hardcodes `bound_established: true`, `error_millis: 0` and `sampled_at:
Tick::ZERO` (`support/mod.rs:116-123`). But that is a different defect from the one CB-8 names,
it lives one layer down, and F-8 shows the builder is the wrong instrument for it: a builder lets
a row pin a **constant** `ControlTime`, while the thing an `rdb-sim` row needs is a clock that
**moves** — a sample that ages as the scheduler advances. A frozen literal injected once cannot
express "the bound was lost at tick 5000" or "the sample aged past the threshold", which is
precisely what kernel-a's clock rows assert.

**False-positive check.** I am wrong if `m7f_14` is scheduled for deletion, or if the freeze
intends the six-branch coverage to live in `rdb-sim` rather than `rdb-core` for a reason I did not
find. I searched and found no such statement. I am also wrong if `ctx_with_control_time` has a
named consumer already written — I found none; `StepCtx` is built in exactly one place in the sim
test suite (`support/mod.rs:115`).

**Closure.** Two `assert_eq!` calls appended to `m7f_14` in `crates/rdb-core/tests/seams.rs`
covering `DefinitelyAfter` and the overlap-`Uncertain`, and CB-8 is closed. The `rdb-sim` fixture
question is re-filed against F-8, not fixed with a builder.

---

## F-7 — ADVISORY. The blind spot the design cannot see about itself (lead's question 5).

**The pattern.** Each of the four changes is sized against exactly one artifact, and in each case
it is the artifact the author happened to be reading. CB-7 against kernel-b `design.md` §3.6.
G-13's blast radius against `backup.rs`. CB-8 against a grep scoped to `crates/rdb-sim/tests`.
F-1, F-3 and F-6 are **the same mistake three times**: a sufficient local check presented as a
global one. The wording gives it away — `:75` "needs no new leaf names", `:352` "Call site", `:126`
"no file under `crates/rdb-sim/tests`" — the last of which is the only one that names its scope,
and the conclusion drawn from it silently drops the scope.

**What a reader who sees only one change gets wrong.**

- Only CB-7: reads "two variants cover every named case" (`:405-406`) as a repository-wide result.
  It is a result about four table rows.
- Only G-13: reads §3's heading, "a restore resets the durable policy-version floor", as closed. It
  is closed for one of the two backup paths (F-4).
- Only CB-8: believes six branches are unreachable. Four are asserted today (F-6).

**A second-order case.** L-R66 justifies dropping `Copy` by measuring current `crates/`. CB-7's
entire purpose is to admit consumers that do not exist yet, so a measurement of today's tree
cannot speak for them. The conclusion still holds — `#[non_exhaustive]` plus `Clone` is right for
this type — but the argument offered for it does not reach the case it is defending.

**The deepest instance is F-8.** The design treats CB-8 as a fixture problem. It is downstream of
an injection path with no callers, which no amount of fixture work reaches.

**Consequence.** The record is honest in its details and misleading in its summaries, and §0's
"Decisions at a glance" table is the part most likely to be read alone.

**False-positive check.** I am wrong if §0 is understood as an index rather than a claim. The
`CB-7` row asserts a shape and the `G-13` row asserts a mechanism; both read as conclusions.

**Closure.** Every coverage claim in the record carries the command that established it, the way §5
already does for the lead's six verified claims.

---

## F-8 — MATERIAL here, and a BLOCKER for kernel-a's sim rows. CB-9: the clock cannot be perturbed, and the harness never asks it the question.

**The lead's CB-9 reading is right, and understates it by one link.** I verified the two he named
and found a third.

**Evidence.** `grep -rn "set_skew" crates/` returns four hits, all inside
`crates/rdb-sim/src/sim/clock.rs` — the definition at `:111` and three doc comments at `:13`,
`:74`, `:86`. **Zero callers.** `grep -rn "control_time" crates/` returns four hits:

- `crates/rdb-core/src/contracts/event.rs:412` — the `StepCtx` field
- `crates/rdb-sim/src/sim/clock.rs:91` — `Clock::control_time`, **zero callers**
- `crates/rdb-sim/tests/support/mod.rs:118` — `ctx()`'s hardcoded literal
- `crates/rdb-sim/src/harness/dispatch.rs:152` — and this is the third link:

      pub fn ctx_for<'a>(&self, base: &StepCtx<'a>) -> StepCtx<'a> {
          let adopted = self.adopted(base.node, base.partition);
          StepCtx { now: base.now, control_time: base.control_time, ... }

`Dispatcher::ctx_for` fills the authority triple from the last adoption and **passes
`control_time` and `now` through verbatim from whatever the caller handed in**. So the chain runs
`Clock` → (nothing), and `StepCtx.control_time` → the caller's literal. The simulator's clock and
the kernel's view of time are not connected at either end.

**Why this outranks the fixture question.** `clock.rs:13-16` states the intent —
"`Clock::set_skew` moves one node's authority-clock estimate ... Outside the bound, the estimate
is reported with `ControlTime::bound_established` false and every comparison must fail closed."
Nothing drives it. And kernel-a's plan depends on exactly that mechanism:
`docs/testing/test-plan-m7-kernel-a.md:1078` (Q-12, accepted by the lead) requires samples
"delivered **even when the bound is not established** (so A1 sees the invalid sample and fences,
rather than seeing nothing)", and `:1286` names the rows that hang on it —
**M7A-38..M7A-46, M7A-143, M7A-146, M7A-148, M7A-165**. `:1288` pins two of them to sample *age*:
M7A-46 at exactly 2000 (not stale) and M7A-43 at 2001 (stale).

A sample age is `now - sampled_at`. With `ctx_for` copying both from `base`, a row can only
produce an age by hand-writing a `ControlTime` whose `sampled_at` it keeps consistent with the
scheduler's tick itself. A unit row can do that. **A sim row cannot** — the dispatcher builds the
ctx, the scheduler owns the tick, and neither consults the clock.

**Consequence.** Kernel-a is about to write 189 rows, ~14 of them keyed to a clock the simulator
cannot perturb, over a seam (ask 9's `ControlTime` → `ClockSample` conversion) whose only varying
input is dead. Worse, the failure is silent in the direction that passes: every sim row sees
`bound_established: true` and age zero, so every fencing comparison succeeds and nothing fails
closed. That is the same defect shape the design names for CB-8 at `:185-187` — "every existing
row calling bare `ctx()` still passes ... That silent pass is the defect shape" — one layer
lower, where it was not looked for.

**Why it bears on the freeze being decided now.** If CB-9 is closed by wiring `Clock::control_time`
into the harness, an `rdb-sim` row perturbs time with `set_skew` and the ctx follows, and CB-8's
builder has no remaining consumer. If CB-8's builder lands first, rows will pin frozen literals,
that becomes the established idiom, and the dead path stays dead with a fixture standing in front
of it. **Order matters: decide CB-9 before implementing CB-8.**

**False-positive check — what would make me wrong.**
(i) A caller of `set_skew` or `control_time` outside `crates/` — a doctest, an example, a
    generated harness. I searched `crates/` only, which is where the workspace's code lives; a
    doctest would still not be a scenario.
(ii) The harness is meant to be driven with a caller-built `base` per step, with I1 (unwritten)
     responsible for calling `Clock::control_time` and building `base`. **This is the strongest
     counter and I cannot rule it out**: `ctx_for`'s doc at `dispatch.rs:145-146` says it replaces
     "the authority triple" and nothing more, so passing `control_time` through may be deliberate
     and I1 may be the intended caller. If so, CB-9 is not a defect but an **unwritten
     requirement on I1**, and it should be recorded as one — because ask 9 assigns I1 the
     `ControlTime` → `ClockSample` conversion (`kernel-a/design.md:842-848`) and says nothing
     about I1 sourcing the `ControlTime` from the `Clock`.
(iii) Kernel-a's clock rows are all unit rows. `:1286` does not say; M7A-143/146/148/165 read as
      sim ids. If every one is a unit row, F-8 drops to ADVISORY.

**Closure.** Either (a) the harness sources `StepCtx.control_time` from `Clock::control_time(node)`
and a test drives `set_skew(node, _, false)` end to end, or (b) a recorded requirement on I1 names
it as I1's job, with `grep -rn "set_skew" crates/` returning a caller by the time M7A-38..46 are
written. Observable in either case: one row exists in which a kernel sees
`bound_established: false` without a test having written that literal itself.

---

## The offline-CLI downgrade question (lead's question 2) — **not a BLOCKER**

**Can someone who can run the offline backup CLI manufacture an artifact that launders a downgrade
past the floor? No — the capability the exploit needs is strictly weaker than the capability the
attacker must already hold.**

1. **The field is signed, so it cannot be flipped without a key.** `backup.rs:877` binds the
   manifest from `verify_backup(req.from, req.name, keys)?`, and every later read — including
   `manifest.policy_version_ref` at `:997` — is off `verified.manifest`. So the `None` cannot be
   forged onto an RPC-taken artifact by editing a file; it requires the backup signing key, which
   `backup_offline` already loads at `backup.rs:268` (`load_signing_key(keys.signing_key)?`).
2. **The floor is written into a directory that did not exist.** Restore refuses a non-empty
   destination twice (`backup.rs:867`, `snapshot.rs:1169-1177`), so the "bypassed" floor is not an
   existing floor being lowered. Nothing is laundered; a fresh store starts where every restored
   store starts today (`rocks.rs:1316`).
3. **Exploiting it requires starting the restored daemon, and anyone who can do that already has
   `--break-glass-policy-rollback`**, which permits `below_floor` unconditionally
   (`config-core/src/policy.rs:767-772`, `:788`). The floor's value is irrelevant to an attacker
   who controls the invocation — which is exactly the attacker who could choose the offline path.
4. **The design's own reasoning says this**, at `:243-245`: break-glass already overrides any floor
   value it finds, regardless of provenance. I checked it and it is correct.

**The residual, and it is not nothing.** The confused-deputy case is real: an attacker with the
backup signing key and read access to a stopped data directory can hand an honest operator an
artifact that restores with no floor, where an RPC-taken artifact of the same data would have
restored with one. It does **not** give the attacker the old document — `policy_version_ref` is "a
reference, never a copy" (`backup.rs:95-97`) and the operator supplies the document independently
— so the attack needs a second, unrelated capability to land. That is why it is F-4 (make the
absence loud) and not a BLOCKER.

**False-positive check on this answer.** I am wrong if `--break-glass-policy-rollback` is gated by
something I did not read — an operator confirmation, an audit refusal, a config the attacker
cannot reach. I read `policy.rs:767-772` and `:788` and found the flag consulted directly with no
further gate. I am also wrong if a future path restores into a directory that already holds a
floor; today both emptiness guards forbid it.

---

## What I checked and found clean

- **G-13 same-batch write (lead question 3) — PASSES.** `snapshot.rs:1273-1304` is the batch that
  carries `restore_keys::IDENTITY`, committed at `:1310` with `WriteOptions::set_sync(true)`, and
  the all-or-nothing note at `:1305-1309` is attached to that write. Adding a key cannot break the
  acceptance rule, which turns on the **presence** of `state_meta/identity`, not on the absence of
  other keys — and `policy_version_floor` is already a key `RocksStore::open` tolerates, since any
  node that has adopted a document holds it (`rocks.rs:115`, `:197`).
- **Encoding matches.** The design's `postcard::to_stdvec(&v)` for `v: u64`
  (`design-contracts-freeze.md:328-334`) is byte-identical to
  `RocksStore::set_policy_version_floor`'s `postcard::to_stdvec(&version)` (`rocks.rs:1345`), which
  `policy_version_floor()` reads back at `:1325-1330`. No codec mismatch, no versioned wrapper.
- **G-13 (c) and (d) — PASS.** The reasoning against gating on `policy_divergence` is correct and
  the second reason (`:283-287`) is the stronger one: `policy_divergence` is direction-blind. `(d)`
  is satisfied — `policy_version_ref` is an existing signed field (`backup.rs:107`).
- **Dropping `Copy` from `KernelEffect` — PASS.** Two `.rs` uses outside `event.rs`;
  `seams.rs:297` needs `KernelIgnoredReason: Copy`, which the design keeps (`:64`);
  `dispatch.rs:480-486` moves out of an array literal and never copies.
- **Ask 9 is not a `ControlTime` reshape — PASS, and the design's correction of the lead's brief
  stands.** Not re-derived; but F-8 shows ask 9 has an unstated input requirement that neither the
  brief nor the correction names.
- **`Alert{reason: ErrorKind}` left unchanged — flagged, not filed.** Kernel-b asserts
  `Alert{RebuildStalled}` (M7B-128), and `RebuildStalled` is not an `ErrorKind` variant either.
  Not filed as a finding because the design explicitly scopes `Alert` out as "spec-bound,
  operator-facing" (`:59`) and that may be right. But if F-1's mapping table is built, `Alert`
  belongs in it — cheaper to answer now than after the basis moves.

## What I looked for and did **not** find

- No golden file, oracle checkpoint or recorded JSONL holds a serialised `KernelEffect`, so
  retyping `Ignored.reason` breaks no stored artifact. `grep -rn "KernelEffect" crates/` returns
  four files, all source or tests.
- No validation in `RocksStore::open` that rejects an unexpected `state_meta` key.
- No second writer of `state_meta/policy_version_floor` that a restore-time seed could race.
- No `restore_into_fresh_store` path that writes into a non-empty directory, so the
  replace-vs-max question in `(a)` is moot at restore time.
- No caller of `Clock::set_skew` or `Clock::control_time` anywhere in `crates/` (F-8).
- **No fifth unrecorded contract change is required by the four as scoped.** F-1 widens CB-7; it
  does not create a new ask. CB-9/F-8 **is** new, but it is the lead's own finding and already
  numbered, and it is a simulator-plumbing decision rather than a `contracts/` change — unless it
  is closed by giving `Dispatcher` a `Clock`, which is `rdb-sim`, still not `contracts/`.
