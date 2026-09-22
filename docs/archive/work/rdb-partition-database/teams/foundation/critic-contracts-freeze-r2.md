# Critique — M7 Foundation Contracts Freeze, **round 2**

Critic: `critic-foundation-freeze-r2`. 2026-09-22. Under review:
`teams/foundation/design-contracts-freeze.md` (round 2, basis `395d535`).

**Verdict: FAIL.** Two BLOCKERs, both in Shape E — the design's own recommended G-13 shape, and the
part it says ships under any charter ruling. Five MATERIAL, five ADVISORY.

**Tree state.** `git rev-parse HEAD` → `395d535`. Every citation below was opened this round. For
`crates/config-storage/src/rocks.rs` I read **`git show HEAD:crates/config-storage/src/rocks.rs`**,
never the working copy, and line numbers for that file are HEAD's. Every other cited path was read
from the working tree; `git status --porcelain` at the start of this round listed no `crates/` file
other than `rocks.rs` as modified, plus the untracked `crates/config-server/tests/g13_scratch_repro.rs`,
which I did not open and which nothing below rests on. I ran no `cargo`, no gate, and wrote no file
but this one.

**Round 1's finding is the frame.** F-7: *a sufficient local check presented as a global claim.*
The lead added Rule 2: *before changing what a field means, read what the field says it means.* The
architect adopted both and tabulated five instances. **I found four more, and three of them are in
the sections revision 2 rewrote to close F-7.** They are R2-1, R2-2, R2-3 and R2-6.

---

## Findings by severity

| # | Severity | One line | Where |
|---|---|---|---|
| R2-1 | **BLOCKER** | E2's premise is false at HEAD, and E2 as specified turns two landed green rows red — rows that exist to refuse exactly what E2 asks for | `main.rs:228-236`, `m6_backup_policy.rs:367,396` |
| R2-2 | **BLOCKER** | E1 contradicts three shipped rationale comments, misstates the cause, and names no value to write | `backup.rs:103-107,280-284`, `run.rs:388-393` |
| R2-3 | MATERIAL | "No composition of existing parts can produce" the age boundary is false. CB-9b alone makes M7A-43 and M7A-46 reachable. This inverts the brief's question 3 | `clock.rs:91-107`, `dispatch.rs:144-158`, `time.rs:123-125` |
| R2-4 | MATERIAL | Both new leaves are declared C-like and `Copy`. Ten of kernel-a's 27 `Fact(..)` names are asserted **with payloads**, and one of them carries a non-`Copy` `BlockReason` | `test-plan-m7-kernel-a.md:631`, `contracts/authority.rs:197-206` |
| R2-5 | MATERIAL | `#[non_exhaustive]` has no effect inside the defining crate. The six kernel modules are inside `rdb-core`, so the "an append cannot break a consumer's `match`" guarantee does not hold for the intended consumers | `lib.rs:39-45`, `contracts.rs:25-38` |
| R2-6 | MATERIAL | §1.5's "all 42 names … zero residue. Full table." omits `TOO_LARGE`, named as an `Ignored` reason in the same cited row | `test-plan-m7-kernel-b.md:501` |
| R2-7 | MATERIAL | The layering that rules out kernel-module leaves is a convention with no enforcement, and kernel-b's plan already commits kernel-b to editing `event.rs` for six effect variants | `lib.rs:20-24`, `test-plan-m7-kernel-b.md:501` |
| R2-8 | ADVISORY | E2/§5.5's audit-line change has a specified log contract with plan rows asserting its field set; not costed | `test-plan-m5.md:514,656`, `ADR-0024:272-276` |
| R2-9 | ADVISORY | The `Copy`/retype edit is six sites, not four; the two `use` lines are unnamed | `seams.rs:20,299`, `dispatch.rs:20-22` |
| R2-10 | ADVISORY | D1's status is stated without citing the shipped comment that says the absent/zero collapse is deliberate | `rocks.rs:191-196` (HEAD) |
| R2-11 | ADVISORY | §1.7's contract argument indicts `Ord`, `Hash` and `Serialize` as much as `Copy`, and stops at `Copy` without saying why | `event.rs:211,232` |
| R2-12 | ADVISORY | After CB-9b, `base.control_time` is ignored **only** on the `ctx_for` path; a direct `Module::step` caller still reads the frozen literal | `dispatch.rs:144-158`, `support/mod.rs:115-131` |

---

## R2-1 — BLOCKER. E2's premise is false, and E2 as specified turns two landed green rows red

**Criterion.** A design may not propose a change whose stated justification is a behaviour the
shipped code does not have, and may not specify a change that breaks a landed green row without
naming the row.

**Location.** Design §0 row 14, §5.4 "E2", §5.5, §6 (the lead-verification row
"`policy_divergence` computed but not enforced"). Code: `crates/config-server/src/main.rs:221-236`;
`crates/config-server/src/backup.rs:797-802` and `:836-839`; tests
`crates/config-server/tests/m6_backup_policy.rs:367` and `:396`.

**Evidence.**

The design's E2 is *"restore **reports divergence loudly instead of swallowing it**"*, and the
lead's brief states the premise as *"the comparison at `backup.rs:998` is computed and dropped"*.

It is neither swallowed nor dropped. `crates/config-server/src/main.rs:221-236`:

```rust
// M6-35. Emitted **before** `restore_completed`, so the divergence is read in the
// order it was decided rather than as a footnote to a success. ADR-0027 specifies
// `warn`; these subcommands install no tracing subscriber, so the severity travels
// as a field on the record rather than as a log level that would not exist.
if let Some(divergence) = outcome.policy_divergence {
    audit_line(serde_json::json!({
        "msg": "restore_policy_mismatch",
        "level": "warn",
        "source": "cli",
        "manifest_version": divergence.manifest_version,
        "active_version": divergence.active_version,
    }));
}
```

`RestoreOutcome::policy_divergence`'s own doc at `backup.rs:836-839` says why it is returned rather
than logged in that module — "the offline subcommands install no tracing subscriber, so a
`tracing::warn!` in this module would compile, read like an audit trail, and emit nothing. The
caller writes it to the same JSONL stderr channel `restore_completed` already uses." The delivery
mechanism is documented, implemented and tested.

Two landed green rows assert it. `crates/config-server/tests/m6_backup_policy.rs:367`,
`m6_35_restore_records_a_policy_divergence_without_blocking`, asserts the line, both numbers, and
`level == "warn"`, and its doc says: "The row asserts the *line*, not the decision behind it …
so a test that reached into `RestoreOutcome` would pass with the emission deleted." That is
precisely the failure E2 says is present.

**The second row is the one that makes this a blocker rather than a correction.**
`m6_backup_policy.rs:396`, `m6_35_restore_says_nothing_when_the_policy_versions_agree`:

```rust
assert!(
    run.events("restore_policy_mismatch").is_empty(),
    "matching versions are not a divergence: {:?}", run.stderr
);
```

E2 demands the line "state … **in words** which of the three cases holds — **agreed**, diverged
with both numbers, or *not compared because the operator supplied nothing*". A line that reports
"agreed" fires on every ordinary restore, which this row asserts it must not. The row's doc gives
the reason, and so does the production function's doc at `backup.rs:799-802`:

> Silent unless **both** versions are known and they differ. An unknown version on either side is
> not a divergence but an absence … Reporting those as a mismatch would teach an operator to ignore
> the line on every ordinary recovery, which is how a real divergence goes unread.

So the shipped code **already made E2's exact design decision, in the opposite direction, with a
stated rationale**, and the design cites none of it. This is Rule 2 — the architect's own second
rule — broken in the section that introduced it.

**Consequence.** Under the design's recommendation ("I recommend E1 + E2"), E2 is the control that
Shape E rests on. With E2's premise false, Shape E's second deliverable is either already shipped
(the divergence line) or is a regression against a green row and two rationale comments (the
agreed/not-compared cases). Combined with R2-2, Shape E delivers approximately nothing, which is a
materially different decision for the product owner than "G-13 delivers three things."

**False-positive check.** Could E2 mean "make the *existing* line richer" rather than "start
emitting one"? Three things say no. (a) The verb is "instead of swallowing it". (b) The design
routes the content onto `restore_completed`, not onto `restore_policy_mismatch` — §5.5 names
`main.rs:240-250`, the `restore_completed` block, and never names `restore_policy_mismatch` or
M6-35 anywhere in 1545 lines (`grep -c 'restore_policy_mismatch\|M6-35' design-contracts-freeze.md`
→ 0 for both). (c) The three-case wording explicitly includes "agreed", which no reading of
"enrich the existing line" reaches, because the existing line does not exist in the agreed case.
Could `restore_policy_mismatch` be CLI-only while an RPC restore path swallows it? `grep -rn
"restore_into_fresh_store" crates/` returns five calls; the only production one is
`backup.rs:978`, reached from `backup::restore`, whose sole caller is `main.rs:220`. There is no
second restore path to swallow anything.

**Closure condition.** §5.4's E2 is rewritten against `main.rs:221-236`, `backup.rs:797-802` and
both M6-35 rows: state what the line already carries, state which of the three cases the shipped
design deliberately declines to report and why, and either withdraw the "agreed" and
"not-compared" cases or argue against `backup.rs:799-802` and name
`m6_35_restore_says_nothing_when_the_policy_versions_agree` as a row the change turns red.

---

## R2-2 — BLOCKER. E1 contradicts three shipped rationale comments and names no value to write

**Criterion.** Rule 2, as the design states it: *before changing what a field means, read what the
field says it means.* And: a change the design commits to shipping under every ruling must specify
what it writes.

**Location.** Design §0 row 12, §5.4 "E1", §6 (lead-verification row "One backup path passes
`None` … **Stands, and is promoted.** Round 2 makes it **E1**"). Code:
`crates/config-server/src/backup.rs:95-107` and `:280-285`; `crates/config-server/src/run.rs:388-393`.

**Evidence.** The design says:

> `crates/config-server/src/backup.rs:285` passes `None` … where `crates/config-server/src/run.rs:422`
> passes a real version. So an artifact's provenance record is present or absent depending on
> **which command took the backup**, which is invisible in the artifact and **is not a property
> anyone chose**.

Three shipped comments say it was chosen, and two of them are within six lines of a line the design
cites.

1. `backup.rs:280-284`, immediately above the cited `:285`:

> `None`, and not a guess: this process opened a **stopped** data directory and runs no policy
> loader, so there is no active document for it to name. Recording a version it cannot observe
> would be worse than recording none — the field is read by an operator mid-recovery, and a wrong
> breadcrumb is followed. The durable policy version floor (gap G-09) is what would let this path
> answer honestly.

2. `backup.rs:103-107`, the second paragraph of the field doc whose *first* paragraph (`:96-100`)
   the design quotes four separate times as its Rule-2 citation:

> `null` when the exporting process had no active policy to name: a static-mode node, a signed-mode
> node holding no valid document, and — until the policy version floor is durable (gap G-09) —
> every backup taken by the offline CLI …

3. `run.rs:388-392`, above the "real version" the design contrasts against:

> `None` under static mode and on a signed-mode node holding no valid document — a manifest that
> named a version this node was not enforcing would be a breadcrumb an operator follows to the
> wrong document.

And `run.rs:393` is `self.policy.as_ref().and_then(|l| l.state_and_version().1)` — an
`Option<u64>` that is `None` on two of three documented paths.

**Two defects, not one.**

(a) **The cause is misstated.** `None` does not depend on "which command took the backup". It
depends on whether the exporting process had an active signed document, which the RPC path also
often lacks. The design compared two call sites and generalised to a property of the two commands.
That is F-7's shape, in the round organised around F-7, in the item the design says it would ship
regardless.

(b) **E1 has no specified value.** "The offline path stops passing `None`" does not say what it
passes. The offline path structurally cannot observe an active document — `backup_offline`
(`backup.rs:255`) opens a stopped directory through `export_snapshot`, which "opens RocksDB
read-only and therefore neither takes the directory lock" (`backup.rs:251-253`), and runs no policy
loader. The only observable substitute is `state_meta/policy_version_floor`, which the shipped
comment at `:284` names as the enabler. **But that is a different quantity.** The floor is a
high-water mark that break-glass can move down (`rocks.rs:1338-1340` at HEAD: "The value replaces
rather than maximises … A break-glass rollback has to be able to move this down"). The field is
documented as "The signed policy document that was **in force when the backup was taken**"
(`backup.rs:95`). Writing the floor into it would make one field mean two things depending on which
command produced the artifact — the exact confusion E1 claims to remove, inverted, and now
invisible because the field would be populated on both paths.

**Consequence.** E1 is the one item the design commits to unconditionally ("**This is the one item
I would ship regardless of how the charter question is ruled**"). §5 otherwise STOPs. A developer
picking up the freeze will pick up E1, find no value specified, and either pick the floor — landing
(b) — or read `:280-284` and stop, costing a round.

**False-positive check.** Does §8 item 9's caveat cover this? No. That caveat is "E1 and E2 are
recommendations I have argued but not **costed**" and its scope is `RestoreOutcome` consumers and
`docs/evidence/` JSONs. It does not disclaim the factual premise, and an uncosted implementation
caveat cannot excuse a false statement about current behaviour. Second check: is `:280-284` perhaps
stale, describing a constraint that no longer holds? Its stated blocker is "until the policy version
floor is durable (gap G-09)", and G-09 *is* now durable (`config-server/src/policy.rs:348` writes it
through `set_policy_version_floor`; the durable row is `config-server/src/policy.rs:1014`). So the
comment is not stale — it is a **specification of what E1 should do**, which strengthens rather
than weakens the finding: the design missed a shipped comment that had already answered its own
open question.

**Closure condition.** §5.4's E1 states (i) the three documented causes of `None`, not one;
(ii) which value the offline path writes, with an argument that it is the same quantity the field's
doc at `backup.rs:95-100` names, or a statement that the field's meaning is being widened and that
`:95-107` is amended in the same change; (iii) whether `backup.rs:280-284`'s rationale is agreed
with or overruled, in the form §0.1's Rule 2 demands.

---

## R2-3 — MATERIAL. CB-9b alone *does* make M7A-43 and M7A-46 reachable

This inverts the premise of the brief's question 3.

**Criterion.** F-7: an impossibility proved under one condition may not be restated as an
impossibility.

**Location.** Design §0 row 9, §4.4, §4.5 (the sizing table and the dependency table), §4.6
(R-CB9). Code: `crates/rdb-sim/src/sim/clock.rs:91-107`;
`crates/rdb-sim/src/harness/dispatch.rs:144-158`; `crates/rdb-core/src/contracts/time.rs:123-125`;
`crates/rdb-core/src/contracts/event.rs:412-430`.

**Evidence.** §4.4 argues correctly and **conditionally**: "*If* the harness advances the clock in
lockstep with the scheduler and samples on every step, then `clock.now() == ctx.now` always, age is
always zero". §4.5 and §4.6 then drop the condition:

> two of them (M7A-43, M7A-46) are pinned to an age boundary that **no composition of existing
> parts can produce**, because every existing part samples at the tick it is asked.

`Clock::control_time` does **not** sample "at the tick it is asked". Its signature is
`pub fn control_time(&self, node: NodeId) -> ControlTime` (`clock.rs:91`) — it takes **no tick**.
It stamps `sampled_at: self.now` (`clock.rs:105`), the clock's own tick, moved only by
`Clock::advance` (`clock.rs:75-81`). The judging tick is a *separate* value:
`ctx.now` comes from `base.now` (`dispatch.rs:147`), which the caller owns, and `StepCtx`'s fields
are all `pub` (`event.rs:413-430`). `ControlTime::is_stale(now, max)` is
`sampled_at.0 > now.0 || now.0 - sampled_at.0 > max` (`time.rs:123-125`).

So after CB-9b's twelve lines, a row does this and nothing else:

- `Dispatcher::default()` → `Clock::default()` → `Clock::new(100)` → `now = Tick::ZERO`
  (`clock.rs:44-48`, `:54-61`).
- Build `base` with `now: Tick(2_001)` (a `StepCtx` literal, all fields public).
- `dispatcher.ctx_for(&base)` → `sampled_at = Tick(0)`, `ctx.now = Tick(2_001)`.
- `is_stale(Tick(2_001), 2_000)` → `2_001 - 0 > 2_000` → **true**. That is M7A-43.
- `base.now = Tick(2_000)` → `2_000 - 0 > 2_000` → **false**. That is M7A-46.

Not one line of I1 is involved, and `Clock::advance` need not be called at all. The age is
`ctx.now − clock.now()`, exactly as §4.6's R-CB9 says, and **both terms are already under a row's
control**.

**Two consequences for what the design says about itself.**

(a) §0 row 9 — "it does **not** make kernel-a's sample-*age* rows writable on its own" — and the
second row of §4.5's dependency table ("A sampling cadence under which `ctx.now − clock.now()` can
exceed `max_sample_age_millis` | **I1** | M7A-43 is unreachable") are both wrong. There is one
dependency outside foundation, not two: kernel-a's consumer. The I1 obligation is real but is the
weaker one R-CB9 already states — *do not* advance in lockstep on every step — and it is a note for
I1, not a gate on CB-9b.

(b) **The `clock.rs` self-contradiction the design reports is not one.** §4.4 says `:3-5` and
`:87-89` "cannot both govern a per-step fill" and that `:3-5` "forbids" a separate sampling cadence.
`:3-5` says time moves only through `Clock::advance`, called by the harness with the scheduler's
tick. `:87-89` says a caller that holds a sample and asks later is what `is_stale` exists for.
These are consistent: the second describes a caller holding a sample across advances, which the
first permits. The incompatibility appears only under CB-9b's *own* proposed per-step re-fill, and
then only under lockstep. The design attributes to shipped source an inconsistency created by its
own proposal — and does so in §4, without applying Rule 2's "cite the comment and say whether you
agree or overrule it".

**(c) The finding is not new, and its source is a file the design cites three times.**
§4.4 opens: "it is not in the lead's brief, the critic's report or the Manual Tester's plan."
It is in `docs/testing/test-plan-m7-kernel-a.md:1286`, §15 drift row 7 — the row the design cites in
§3, §4.1 and §4.5 for the thirteen-row list — in the same table cell:

> The `at = ct.sampled_at` clause is the load-bearing one: **a seam that stamped the sample at its
> delivery tick would make every sample age zero and M7A-43's stale sample unreachable.**

Kernel-a has already stated the mechanism and already ruled the fix (`at = ct.sampled_at`, not
`now`). The design re-derived half of it and reported the whole as novel.

**Consequence.** The lead's question 3 asked whether "a 12-line change that cannot unblock two of
its thirteen rows needs saying plainly." It can unblock them. Stopping CB-9b on an I1 cadence
decision that does not exist would hold two rows and one design conversation for no reason, and
would leave in place the frozen-literal idiom §4.5's own argument 2 says is the expensive thing to
retrofit.

**False-positive check.** Does the kernel-a consumer dependency make this moot — i.e. are M7A-43/46
unwritable anyway until A1 reads `ctx.control_time`? No, and the distinction matters. M7A-43/46
need (i) a stale sample delivered and (ii) A1 reading it. The design names (ii) correctly as
kernel-a's. It names (i) as I1's, and (i) is satisfied by CB-9b itself. Second check: would the
harness, once I1 exists, force lockstep and take the reachability away? Only if I1 advances the
clock to `ctx.now` before every step; R-CB9 already forbids that, and a scenario can use
`clock_mut()` — the accessor the design itself proposes — to put the clock behind the scheduler at
any time. Third check: is `Clock::advance`'s monotonicity (`clock.rs:76-78`, refuses a backward
`now`) an obstacle? No — the row never moves the clock backwards; it moves `base.now` forwards.

**Closure condition.** §0 row 9, §4.4's closing paragraph, §4.5's sizing and dependency tables and
§4.6 are restated with the condition attached: *under a harness that advances the clock to the
scheduler's tick before every step*, age is zero; the composition itself leaves both terms free, so
M7A-43 and M7A-46 are reachable after CB-9b, and the I1 obligation is to not remove that freedom.
§4.4's "not in any input" sentence is withdrawn and `test-plan-m7-kernel-a.md:1286` is cited.
The `clock.rs:3-5` / `:87-89` "contradiction" is withdrawn or restated as a property of the
proposed per-step fill.

---

## R2-4 — MATERIAL. The two new leaves cannot be C-like `Copy` enums; the plans already say so

**Criterion.** Rule 1 as the design states it for counts: *a table of N rows does not speak for a
plan of M*. A name census is not a variant specification.

**Location.** Design §1.3 (both new leaves carry `#[derive(Debug, Clone, Copy, …)]`), §1.5 (the
27-name and 12-name tables, "Residue: **zero**"), §1.7's fourth table row ("The two new leaves are
C-like and start `Copy`; if kernel-a later needs a payload, it drops its own derive"). Code and
plans: `docs/testing/test-plan-m7-kernel-a.md:631` and `:388,485,547,606,613,656,660,663`;
`crates/rdb-core/src/contracts/authority.rs:196-206`.

**Evidence.** Command and scope — one file, `docs/testing/test-plan-m7-kernel-a.md`:

```sh
grep -o 'Fact([A-Za-z_][A-Za-z_]*{' docs/testing/test-plan-m7-kernel-a.md | sort | uniq -c
```

**Ten of the 27 names carry a payload brace, over 22 occurrences**: `SampleRejected` (4),
`ReplyWithheld` (4), `Quarantined` (4), `PublishPredicateFalse` (3), `AcquireWithheld` (2),
`RenewalWithheld`, `ExternalFenceRejected`, `CandidateWhileNotServing`, `CandidateUnreachable`,
`Blocked` (1 each). These are assertions, not prose — `Fact(ReplyWithheld{reason})` is in M7A-104's
**Expected** column (`:495`), `Fact(AcquireWithheld{reason})` in M7A-148's (`:606`).

**One of them is not `Copy`.** `test-plan-m7-kernel-a.md:631` is M7A-158,
`blocked_then_freeze_then_recovered_mode_sequence`, whose setup is
`BlockPartition{DivergenceRequiresOperator…}` and whose expectation includes `Fact(Blocked{reason})`.
That `reason` is a `BlockReason`, and `BlockReason` is
`DivergenceRequiresOperator { diverged: Vec<CopyId> }` — `crates/rdb-core/src/contracts/authority.rs:197-206`,
deriving `Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize` and **not
`Copy`**. So `AuthorityIgnoreReason::Blocked { reason: BlockReason }` does not compile under the
derive §1.3 writes for it. E0204, pointing at the `Vec` — the identical failure §1.9's P2 probe
uses to demonstrate the *carrier's* `Copy` problem.

**Why this is the round-1 mistake, not a typo.** §1.7 argues at length that a derive must not be
justified from today's tree, because the type exists to admit shapes that do not exist yet — and
then puts `Copy` on the two leaves on exactly that reasoning, calling the payload case hypothetical
("`FamilyRejected { keys: Vec<ControlKey> }` is the shape kernel-a's `Fact(FamilyRejected)`
**suggests**"). It is not hypothetical and it is not `FamilyRejected`: ten names have payloads
today, in the plan §1.5 censused, and one of them is already non-`Copy`.

**Consequence.** §1.5 is a table of 42 *names*; §0 row 2 presents it as "map to a spelled variant
with **zero residue**. Full table." Ten of the 42 need fields the table does not show, and the shape
of those fields is the thing a reviewer would want to see, because it is where `Copy`, `Ord`, `Hash`
and `Serialize` are decided. Kernel-b's plan calls this exact defect against its own round-5 rows
(`test-plan-m7-kernel-b.md:501`: "**The literals are wrong.** … neither field name exists"); the
design reproduces it one level up.

**False-positive check.** Could the plan's braces be shorthand that the rows will drop? Two reasons
no. (a) `test-plan-m7-kernel-b.md:501` records the lead's standing position on exactly this — a
plan's literals are corrected to the landed shape *or* the type is widened; braces are not dropped
silently. (b) M7A-158 and M7A-171 need the reason: `Fact(Blocked{reason})` with no payload cannot
tell which block a row is asserting, and M7B-142 (cited at `test-plan-m7-kernel-b.md:501`) asserts
the `diverged` payload by value. Second check: does this break the five-arm shape? **No.** The arms
are unaffected; the defect is in the leaf specification and the completeness claim. That is why this
is MATERIAL and not a BLOCKER.

**Closure condition.** §1.3 drops `Copy` from both new leaves, or states per-leaf which variants
carry payloads. §1.5 gains a payload column for the ten names, or §0 row 2 stops saying "Full table"
and says "42 names; the ten payload shapes are the owning kernel's to spell." `Blocked` is named
explicitly, with `contracts/authority.rs:197` as the reason `AuthorityIgnoreReason` cannot be `Copy`.

---

## R2-5 — MATERIAL. `#[non_exhaustive]` buys nothing inside `rdb-core`, where the consumers live

**Criterion.** A guarantee stated as a property of the type must hold for the consumers the design
names.

**Location.** Design §1.3 (the doc comments on both new leaves), §1.4 ("The two new leaves are
`#[non_exhaustive]`, so an append cannot break a consumer's `match`"), §1.7 (the contract argument:
"`#[non_exhaustive]` — *kernel-b may add a variant without foundation's involvement*"). Code:
`crates/rdb-core/src/lib.rs:39-45`; `crates/rdb-core/src/contracts.rs:25-38`.

**Evidence.** `#[non_exhaustive]` is a *cross-crate* attribute: the Rust language gives it no effect
within the crate that defines the type, so an in-crate `match` may be exhaustive with no catch-all
and breaks on every added variant. This is a language rule, not something I measured.

The types and the consumers are in one crate. `lib.rs:39-45` declares `authority`, `contracts`,
`protection`, `publication`, `recovery`, `replication`, `transaction` — the six kernel modules and
the contracts, all `rdb-core`. `contracts.rs:25-38` puts `KernelIgnoredReason`'s prospective home
alongside them. So when kernel-a's `crates/rdb-core/src/authority.rs` matches a
`KernelIgnoredReason` — which is the whole point of `Ignored{reason}` being "a kernel talking to
itself" (§1.8) — kernel-b appending a `ReplicaIgnoreReason` variant *can* break it, and foundation
appending an arm certainly can.

The guarantee does hold where the consumer is a different crate: `crates/rdb-core/tests/seams.rs`
and `crates/rdb-sim/` are separate compilation units, and `seams.rs:296-299` already writes the
open `match` with a `_` arm that `#[non_exhaustive]` requires.

**Consequence.** §1.4's rule — "*it never edits `event.rs`, never edits `KernelIgnoredReason`'s arm
set, and **never waits on foundation or on the other kernel***" — is true for the *append*, false
for the *consequence of the append*: an in-crate cross-kernel `match` breaks, and fixing it is the
other kernel's edit in the other kernel's file. The cross-team wait is reduced, not removed. This is
the same over-claim shape F-7 names, applied to a language feature instead of a search.

**False-positive check.** Are there any such matches today? No — `grep -rn "KernelEffect\|KernelEvent"
crates/ --include=*.rs` returns 13 hits in 4 files, and the only `crates/rdb-core/src/` hit is
`authority.rs:23`, a doc link. So nothing breaks now. The finding is about the guarantee the design
states for the consumers it is building the shape for, and those consumers are the six modules.
Second check: could the leaves live in a different crate to recover the guarantee? Not without
breaking the layering §1.4 relies on; the honest fix is to state the limit. Third check: does
`#[non_exhaustive]` still buy something? Yes — for `rdb-sim`, the tests and verification, which is
real and is most of today's consumers.

**Closure condition.** §1.4 and §1.7 state the boundary: `#[non_exhaustive]` protects consumers
outside `rdb-core`; inside it, a kernel module's total `match` on another kernel's leaf still breaks
on an append, and the mitigation is a `_` arm by convention (which `seams.rs:259-260` deliberately
refuses for the carrier enums, so the convention is not uniform and needs stating).

---

## R2-6 — MATERIAL. "All 42 names … zero residue. Full table." omits `TOO_LARGE`

**Criterion.** The design's own closure condition: *every coverage claim carries the command that
established it and the scope that command covered.* The scope here is one row, and the row names a
code the table does not.

**Location.** Design §0 row 2, §1.5 (the kernel-b table and "Residue: **zero**"). Plan:
`docs/testing/test-plan-m7-kernel-b.md:501`.

**Evidence.** §1.5's kernel-b table is sourced "`test-plan-m7-kernel-b.md:501`, scope one row" and
lists fifteen codes. The same row says:

> M7B-59 alone needs `Ignored{TOO_LARGE}` and `Ignored{RECOVERY_ONLY}` to stay apart in **one**
> vector.

`TOO_LARGE` is a sixteenth `Ignored` reason code the row asserts. It is outside the row's "fifteen"
only because that count is of names measured against `ErrorKind`, and `TOO_LARGE` maps cleanly to
`AppendReject::TooLarge` (`crates/rdb-core/src/contracts/envelope.rs:527`). §1.5's `RECOVERY_ONLY`
note mentions the pair in passing ("keeps M7B-59's `TOO_LARGE`/`RECOVERY_ONLY` pair distinct in one
vector") and then gives `TOO_LARGE` no row and no arm. So the table the design offers as its proof
of completeness is not complete, and §0 row 2's "All **42** names the two M7 plans assert … with
**zero residue**. Full table." over-claims by at least one.

**The irony is worth recording because it changes the Q1 answer.** `Ignored{TOO_LARGE}` is the
*only* demonstrated consumer of the `AppendRejected(..)` arm in either plan — none of the 42 names
maps to it. The design left out the one row that justifies its fifth arm.

**Consequence.** Low for the code — the shape handles `TOO_LARGE` correctly as
`AppendRejected(AppendReject::TooLarge)`. High for the claim, which is the thing round 1 failed the
design for. An implementer building `ReplicaIgnoreReason` from the table gets the right twelve; a
reviewer checking "zero residue" gets a claim that does not survive re-reading the cited row.

**False-positive check.** Is `TOO_LARGE` prose rather than an assertion? The row says "M7B-59 alone
**needs** `Ignored{TOO_LARGE}` … to stay apart in one vector", and M7B-59 is named in §13 of that
plan as a held row. It is an assertion. Second check: is 42 defined as 27 + 15, making the table
internally consistent? Yes — but §0 row 2 states it as "all 42 names the two M7 plans assert", which
is a claim about the plans, not about the arithmetic.

**Closure condition.** §1.5 gains a `TOO_LARGE → AppendRejected(AppendReject::TooLarge)` row, and
§0 row 2's count moves to 43 or is restated with its scope ("the 27 `Fact(..)` names and the fifteen
codes `:501` counts against `ErrorKind`").

---

## R2-7 — MATERIAL. The layering is a convention, and the carrier edit is not removed

Answering the brief's question 2 directly.

**Criterion.** A constraint a design treats as ruling out an alternative must be enforced, or must
be named as a convention.

**Location.** Design §1.4 placement item 1 ("**rejected, layering inversion**"), §1.3's file header
("FOUNDATION owns this file. Edited once by CB-7 and, for a reason name, never again"), §0 row 1.

**Evidence, two halves.**

(a) **The layering is real as a fact and unenforced as a rule.** I re-ran the architect's grep with
a wider net — `grep -rn "^use " crates/rdb-core/src/contracts/*.rs`, all 14 files, every import, not
only `crate::`/`super::` — and its result holds exactly: **every** in-crate import is
`use crate::contracts::…`; no contracts file names a kernel module. `lib.rs:20-24` declares the
layering in prose. But nothing enforces it: Rust permits module cycles freely within one crate, and
the repository has no mechanism that would catch the inversion — `find . -maxdepth 2 -name
clippy.toml -o -maxdepth 2 -name deny.toml` returns nothing, and `grep -n "lints\|disallowed"
Cargo.toml crates/rdb-core/Cargo.toml` returns nothing. `lib.rs:36-37` carries only
`#![deny(missing_docs)]` and `#![forbid(unsafe_code)]`. So `contracts/ignore.rs` importing
`crate::authority` would compile, and the gate's fmt/deps/drift/clippy stages would not object.

**This does not make the design's conclusion wrong** — a 14-for-14 convention with a declared
rationale in `lib.rs` is a good reason, and the *subject* argument in placement item 2 is
independent and stronger. It makes the design's framing wrong: §1.4 presents an unenforced
convention as a structural constraint, in a document whose §1.6 is careful to distinguish "structural
and not a convention" for `NotAMember`. The same distinction is owed here and is not drawn.

(b) **"One carrier edit, never again" is narrower than §0 row 1 reads.**
`test-plan-m7-kernel-b.md:501` — the row §1.5 cites — contains a second **NEW (round 6)** entry:

> **The effect half is empty.** … today only `Ignored` and `Alert` can be emitted, so every row
> asserting one of these six **in an effect vector** has no landed spelling. `#[non_exhaustive]`
> plus kernel-b's ownership of the variants is exactly what makes this a kernel-b edit and **not** a
> foundation ask

The six are `PeerProgress`, `CopyLost`, `DivergenceDetected`, `QualificationChanged`,
`BlockPartition`, `CopyQuarantined`, across M7B-30, 41, 51, 54, 112, 128, 131, 134, 140, 142, 144.
Kernel-b will edit `crates/rdb-core/src/contracts/event.rs` — foundation's file, the file §1.3's
header says is edited once and never again — six more times, and each edit moves the drift basis.
The design's sentence is scoped to *reason names* and is true as written; §0 row 1's summary and the
header comment are not, and a reader of §0 will take away a benefit the design does not deliver.

**Consequence.** The freeze's headline benefit is "removes the carrier edit and the cross-team
wait". Against R2-5 (in-crate `match` breakage), §1.4's own admission (leaf additions still move the
drift basis), and (b) here, what remains is: a kernel adds a *reason name* without waiting on
foundation, and gets a type error instead of a wrong-sibling green row. That is still worth the
shape — see the Q1 answer — but it is a smaller claim than §0 row 1 makes.

**False-positive check.** Is the six-variant `KernelEffect` work in CB-7's scope, so that the design
is right to exclude it? No — it is explicitly kernel-b's own edit per `:501`, and the design is right
to exclude it. The finding is about what §0 row 1 and §1.3's header promise, not about scope. Second
check: could a `[lints]` table or a clippy config exist elsewhere? I searched the repo root and
`crates/rdb-core/Cargo.toml`; I did **not** search every crate's manifest or `.cargo/config.toml`,
and a per-crate lint table would not enforce module layering in any case, since no Rust lint does.

**Closure condition.** §1.4 item 1 says "convention, unenforced, and the subject argument in item 2
is the one that decides it". §0 row 1 and §1.3's header scope the "never again" to reason names and
name kernel-b's six pending `KernelEffect` variants as edits to the same file that are out of CB-7's
scope.

---

## R2-8 — ADVISORY. The audit line has a specified contract with rows asserting its field set

**Criterion.** A change to an audited record costs whatever the plans assert about that record.

**Location.** Design §5.5 (the `policy_version_floor_seeded` field), §8 item 9. Plans:
`docs/testing/test-plan-m5.md:514` (M5-81) and `:656` (M5-123); `docs/ADRs/0024-backup-and-fenced-restore.md:272-276`.

**Evidence.** M5-123, `log_restore_completed`, pins the field list: `source_cluster_id`,
`source_epoch`, `source_revision`, `new_cluster_id`, `new_epoch`, `revision`, `compact_revision`.
M5-81 asserts the line "exists exactly once" and that "**neither contains a key or a value**".
`ADR-0024:272-276` describes the line as "the only record in which both identities appear".
`test-plan-m5.md:812` shows a DuckDB query enumerating the message names. Adding
`policy_version_floor_seeded: "none — --active-policy-version not supplied; the first document this
node adopts will be accepted at any version"` puts a sentence of operator prose into a record two
plan rows describe as a fixed identity-correlation field set.

**Consequence.** Not large, and the design's §8 item 9 already names "I did not trace what else
consumes `RestoreOutcome`". But M5-123 and M5-81 are specific, findable, and the natural home for
the seeded/not-seeded fact is `restore_policy_mismatch` (which R2-1 shows already exists and which
M6-35 owns), not `restore_completed`.

**False-positive check.** Does §8 item 9 already disclose this? It discloses the `docs/evidence/`
JSONs and unnamed `RestoreOutcome` consumers. It does not name M5-81 or M5-123, and those are
assertions that would go red, not costs that would go uncounted.

**Closure condition.** §5.5 names M5-123's field list and M5-81, and says whether the new field goes
on `restore_completed` (and both rows are amended) or on `restore_policy_mismatch` (and M6-35's two
rows are amended, subject to R2-1).

---

## R2-9 — ADVISORY. The `Copy` / retype edit is six sites, not four

**Criterion.** A blast-radius table is a coverage claim.

**Location.** Design §1.7 (two sites named), §4.5 sizing table ("2 test files (3 literals, 1
`*reason`)").

**Evidence.** Scope: `grep -rn "KernelEffect\|KernelEvent" crates/ --include=*.rs` — the whole
`crates/` tree, 13 hits in 4 files; I opened every hit in the two test files.

| # | Site | Why it changes |
|---|---|---|
| 1 | `crates/rdb-core/tests/seams.rs:20` | `use …event::{EffectKind, EventKind, KernelEffect, KernelEvent}` — needs `KernelIgnoredReason` |
| 2 | `seams.rs:267` | `reason: ErrorKind::Unavailable` literal |
| 3 | `seams.rs:297` | `Some(*reason)` → `Some(reason.clone())` — the `Copy` drop |
| 4 | `seams.rs:299` | `assert_eq!(reason, Some(ErrorKind::Unavailable))` — the **expected** value must be re-wrapped, or the row fails to compile on type mismatch |
| 5 | `crates/rdb-sim/tests/dispatch.rs:20-22` | same import gap |
| 6 | `dispatch.rs:483` | literal |

The design names 2, 3, 6 explicitly and arguably counts 4 in its "3 literals". The two `use` lines
are unnamed. Nothing else breaks: `EffectKind` (`event.rs:312`) and `EventKind` (`event.rs:152`) are
already non-`Copy`, and no site copies a `KernelEvent` out of a reference.

**One correction to the design's own derive statement.** §1.7 says "`EffectKind` … and `EventKind` …
both derive `Debug, Clone, PartialEq, Eq`". `EventKind` at `event.rs:152` derives
`Debug, Clone, PartialEq, Eq, Serialize, Deserialize`. The `Copy` conclusion is unaffected; the
statement is incomplete.

**A negative result worth recording, because the design did not check it.** The five-arm derive set
closes. `KernelIgnoredReason` is written with `PartialOrd, Ord, Hash, Serialize, Deserialize`, so
every leaf must have them. Verified by opening all three landed leaves: `ErrorKind`
(`errors.rs:78`), `AppendReject` (`envelope.rs:524`) and `AckRejectReason` (`trace.rs:314`) each
derive `Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize`. No
missing bound. Subject to R2-4, which is about the *new* leaves' payloads, not the landed ones.

**Closure condition.** The §4.5 sizing row reads "2 test files, 6 edit sites (2 imports, 3 literals
including one expected value, 1 deref)".

---

## R2-10 — ADVISORY. D1's status is stated without the comment that says the collapse is deliberate

**Location.** Design §5.5 ("cell absent and cell present, value 0 are indistinguishable at every
layer"), §8 Q-3. Code, **read via `git show HEAD:crates/config-storage/src/rocks.rs`**: `:191-196`
and `:1306-1310`.

**Evidence.** The design cites the method doc (`:1306-1310`, "`0` means the cell has never been
written"). It does not cite the constant's doc at `:191-196`:

> **Absent means nothing is known**, which is what a directory written by any earlier build says —
> so no format bump and no migration, exactly as `KEY_MAX_COMMAND_SCHEMA` was added at M6.

The collapse is a stated design decision with a stated cost avoided (no format bump, no migration).
The Manual Tester's D1 asks to undo it. Under the design's own Rule 2, D1's status is owed a
sentence saying whether it agrees with `:191-196` or overrules it. The design's §5.5 says only that
D1 "is unmet at HEAD".

**Consequence.** Small — the design routes D1 to Q-3 rather than absorbing it, and correctly refuses
to cite the uncommitted `policy_version_floor_cell` as landed. But the routing note tells the lead
D1 is a gap, when the shipped source says it is a choice.

**False-positive check.** Is `:191-196` about a different key? No: it is the doc on
`const KEY_POLICY_VERSION_FLOOR: &[u8] = b"policy_version_floor"` at `:197`, which the design itself
cites in §5.7. The design read `:197` and not the six lines above it.

**Closure condition.** §5.5/§8 Q-3 cites `rocks.rs:191-196` at HEAD and says D1 asks to reverse a
documented decision, at the cost the comment names.

---

## R2-11 — ADVISORY. The contract argument indicts `Ord`, `Hash` and `Serialize` too

**Location.** Design §1.7.

**Evidence.** The argument is: `#[non_exhaustive]` promises another team may add a variant;
`#[derive(Copy)]` promises every future payload is `Copy`; these contradict. The same sentence holds
word for word for `Ord`, `Hash`, `PartialOrd`, `Serialize` and `Deserialize`, all of which
`KernelEffect` and `KernelEvent` carry at `event.rs:211` and `:232` and all of which constrain a
future payload at least as hard as `Copy` does — `Ord` and `Hash` exclude a `HashMap` or an `f64`
payload outright, where `Copy` merely excludes an owning one. §1.7 disposes of `Clone` explicitly
("constrains the payload only to `Clone`, which every payload in a `Serialize`/`Deserialize`
contract has anyway") and does not mention the other five.

**Consequence.** The argument either proves the carrier should derive almost nothing, or it needs a
line saying why `Copy` is the one that goes: that `Copy` is the only one a kernel-b payload has
already hit (`test-plan-m7-kernel-b.md:501`, the `BlockPartition` row: "**Blocked on a derive, not
on a missing name** … this variant needs foundation to drop `Copy`"). That line is the honest
version, it is a census of today's tree, and §1.7 rejected census arguments on principle. The
conclusion is right; the stated reason over-reaches.

**False-positive check.** Does `Ord`/`Hash` matter in practice? Yes, for exactly the payload §1.7
names: `BlockReason` does derive `Ord` and `Hash` (`contracts/authority.rs:197`), so today's first
member of the class passes. The next one may not.

**Closure condition.** §1.7 adds a sentence: `Copy` goes because it is the constraint a named
kernel-b payload has already hit; the others stay because no plan has hit them, and that is a claim
about today's plans, offered as one.

---

## R2-12 — ADVISORY. CB-9b gives one way to get skew only on one path

**Location.** Design §4.5, recommendation reason 3 ("a row that wants skew has **one way** to get
it"). Code: `crates/rdb-sim/src/harness/dispatch.rs:144-158`;
`crates/rdb-sim/tests/support/mod.rs:115-131`.

**Evidence.** `ctx_for` is the only place CB-9b changes. `support::ctx()` still returns a frozen
literal (`estimate: Tick::ZERO, error_millis: 0, bound_established: true, sampled_at: Tick::ZERO`),
and a row may call `Module::step(&ctx, &event)` directly with it — which is what the two
`Dispatcher::step` callers' sibling paths do today. So after CB-9b there are two spellings, and the
frozen one is still the shorter.

**Consequence.** The (d-strong) trap §4.5 argument 2 describes is reduced, not closed. Cheap fix:
`support::ctx()`'s doc says the sample is a placeholder and points at `Dispatcher::clock_mut`.

**False-positive check.** Is `support::ctx()` reachable outside `rdb-sim`'s own tests? It is a test
support module in `crates/rdb-sim/tests/support/mod.rs`, so no — the exposure is to `rdb-sim`'s test
authors, who are the audience for the idiom argument.

**Closure condition.** §4.3 or §4.5 names the residual path and either accepts it or adds the doc
line.

---

## Answer to the brief's question 1 — the five-arm shape

**Recommendation: keep the five arms. The shape is right; its justification in the design is not
the strongest one available, and its leaf specification is wrong (R2-4).**

**1. The "one flat enum with 42 names" alternative is not available, and the design's own strongest
counter-argument rests on it.** Three of the five arms carry landed enums whose variants have
**fields**: `AppendReject::NeedPrefix { have: Seq, head_digest: Digest }`,
`StaleGeneration { current: Generation }`, `CorruptHistory { at: Seq }` and five more
(`envelope.rs:524-596`). You cannot flatten a fielded variant into a name. So the real choice was
never 5-vs-1; it was 5-vs-4 (merge the two kernel leaves) or 5-vs-3 (merge them into the landed
arms). The architect's own doubt is answered by the enums it cites.

**2. 5-vs-4 is decided by ownership and by a collision the merge would create.** A single merged
kernel leaf puts both kernels in one enum in one file. Kernel-a's 27 and kernel-b's 12 both contain
`AlreadyBlocked` (design §1.5 notes it), so the merge forces a rename on one team. And
`AGENTS.md`'s "Several agents, one working tree" records two same-day incidents from exactly this
kind of shared-file contention. Two leaves, two files, two owners.

**3. Which mistake is more likely — wrong ladder or wrong rung — is answerable from the plans, not
from intuition, and the answer is wrong ladder.** The homograph families across the five leaves:
`NotAMember` (`AppendReject` + `AckRejectReason`), `StaleGeneration` (same two),
`StaleEpoch`/`StaleConfig` (same two), `IncompatibleVersion` (`ErrorKind` + `AppendReject` +
`AckRejectReason`), `Quarantined` (`AppendReject` + kernel-a's `Fact`) versus `QuarantinedTerminal`
(kernel-b), `AlreadyBlocked` (kernel-a + kernel-b). **Seven families, spanning every one of the five
arms.** Every one of them is a pair a row can write with the right word and the wrong ladder, and the
arms make each one an E0308 at the row's own line.

The rung mistake has no comparable evidence. I looked for it: `test-plan-m7-kernel-b.md:497` rules
on the ladder mistake by name ("a row that asserts one where the other belongs **passes for the
wrong reason**"), and `contracts/trace.rs:307-313` records the collision as deliberate in shipped
source. **Scope of the absence claim:** `grep -n` for `StaleEpoch`, `StaleConfig` and the phrase
"wrong reason" across `docs/testing/test-plan-m7-kernel-a.md` and `test-plan-m7-kernel-b.md` — the
two M7 kernel plans, the only documents that assert these names — turns up no ruling, no correction
and no held row about confusing two variants *within* one leaf. I did not search verification's
plans or the ADRs.

So the compile-time separation buys protection against a mistake that has a ruling, a shipped doc
comment and seven realised instances, at the cost of one enum indirection. It is not ceremony.

**4. The `AppendRejected` arm is justified — by a row the design did not cite.** None of §1.5's 42
names maps to it, which is what makes it look like the spare degree of freedom. Its justification is
`test-plan-m7-kernel-b.md:501`'s `Ignored{TOO_LARGE}` / `Ignored{RECOVERY_ONLY}` pair for M7B-59
(R2-6), which needs `AppendRejected(AppendReject::TooLarge)` to stay distinct from
`Replica(RecoveryOnly)` in one vector. Put that row in §1.5 and the fifth arm stops looking spare.

**5. Two things the shape does **not** buy, and both should be in §0 rather than only in §1.4.**
In-crate consumers get no `#[non_exhaustive]` protection (R2-5), and kernel-b still edits `event.rs`
six more times for the effect variants (R2-7b). Neither changes the recommendation.

**What to fix before it lands.** R2-4 (drop `Copy` from both new leaves; give the ten payload names
their shapes) and R2-6 (`TOO_LARGE`). Both are edits to §1.3 and §1.5; neither moves an arm.

---

## What I could not verify, named as unverified

1. **Nothing was compiled.** The brief forbids `cargo`. R2-4's E0204 claim is read from
   `contracts/authority.rs:197-206`'s derive list and the language rule, not from a build. R2-9's
   "nothing else breaks" is a reading of 13 grep hits in 4 files.
2. **R2-5 rests on a language rule**, that `#[non_exhaustive]` has no effect within the defining
   crate. I did not demonstrate it in this tree, because no in-crate `match` on these types exists
   yet.
3. **The `crates/config-storage/src/rocks.rs` working-tree change.** I read only HEAD for that file.
   I did not read the uncommitted `policy_version_floor_cell` and make no claim about it. I also did
   not open `crates/config-server/tests/g13_scratch_repro.rs`.
4. **`docs/evidence/*.json`.** Eight are modified in the working tree by another agent. I did not
   read them, so R2-8 does not say how many the audit-line change would move.
5. **Kernel-b's `design.md`.** R2-6 and the Q1 answer rest on `test-plan-m7-kernel-b.md:501`, one
   row of one plan, which that plan's own history shows was rewritten in round 6. I did not
   cross-check it against kernel-b's design, and the design's §8 item 3 already names this.
6. **Whether `AuthorityIgnoreReason` payload shapes are settled.** R2-4 establishes that ten names
   carry braces and that one carries a non-`Copy` type. It does not establish what the other nine
   payloads should be; that is kernel-a's.
7. **Lint configuration outside the repo root and `crates/rdb-core/Cargo.toml`.** R2-7(a)'s
   "unenforced" rests on those two locations plus the absence of `clippy.toml`/`deny.toml` at depth
   ≤ 2. I did not read every crate manifest or `.cargo/config.toml`.
8. **Verification's plans.** The Q1 absence claim about the rung mistake covered the two M7 kernel
   plans only.

---

## Verified and standing — things I checked and did not find wrong

These matter because a FAIL is only worth its accuracy elsewhere.

- **CB-8's replacement assertions are arithmetically correct and kill the mutations claimed.**
  Against `crates/rdb-core/tests/seams.rs:50-93`, where `sample` is
  `{estimate: 1_000, error_millis: 10, bound_established: true, sampled_at: 1_000}` and `margin` is
  `10`, so `slack` is 20: `compare(1_100, 500, 979, 10)` → `1_000 > 999` → `DefinitelyAfter` ✓;
  `compare(1_100, 500, 980, 10)` → `1_000 > 1_000` false → `Uncertain` ✓, and the `>` → `>=` mutant
  answers `DefinitelyAfter` → red ✓; the recommended third, `compare(1_100, 500, 1_020, 10)` →
  `1_020 < 1_020` false → `Uncertain` ✓, and the `<` → `<=` mutant answers `DefinitelyBefore` →
  red ✓. The insertion point (`:83-91` block, before the `tracing::info!` at `:92`) and the in-scope
  bindings both check out. **CB-8's withdrawal and its replacement are the strongest section of the
  document.**
- **F-3's five call sites are exactly five and exactly those.** `grep -rn "restore_into_fresh_store"
  crates/` — whole tree, 14 hits: `backup.rs:978`, `m5_snapshot.rs:1214`,
  `m5_backup_fencing_cluster.rs:218`, `m6_compat_cluster.rs:604`, `m6_evidence.rs:590`; the rest are
  the definition, the `lib.rs:39` re-export and doc comments.
- **§1.2's `#[non_exhaustive]` census holds.** `grep -rn "non_exhaustive"
  crates/rdb-core/src/contracts/` → four hits, all `event.rs` (`:203`, `:212`, `:230`, `:233`).
- **§1.4's import grep holds under a wider net** (R2-7a).
- **§4.4's structural point about `Clock::advance` holds.** `self.now` is read only by `now()`
  (`clock.rs:65`) and `control_time()` (`:105`); `due` takes the tick as a parameter (`:170`),
  `next_deadline` reads the timer table (`:160`), `arm` reads its `at` (`:129`). `advance` moves
  nothing but the sample time. That fact is correct — it is the conclusion drawn from it (R2-3) that
  is not.
- **§1.6's "no `From` between the two enums" holds**, and the five-arm derive set closes (R2-9).
- **§5.2 argument 4 holds.** `backup.rs:880-889` refuses a same-identity restore with
  `cluster_id_reused`. The charter contradiction §5.1 names is real, the position §5.2 takes is
  argued from primary sources, and the decision to STOP on it rather than design past it is the
  right call. **Nothing in R2-1 or R2-2 touches the charter argument** — they are about what Shape E
  *delivers*, not about which shape is right.

---

## My strongest argument against my own most severe finding

Against **R2-1** (E2's premise is false).

The argument is this: §5's whole purpose is a STOP. The design says "the amendment is a
product-owner decision and is why this section stops", and §8's Q-6 makes the charter ruling "the
one that gates the rest". §5.4's two shapes are sketched *so that the decision is one choice, not a
new round* — they are decision inputs, not specifications. Under that reading, E2 is a one-paragraph
sketch of a direction, and objecting that its three-case wording breaks
`m6_35_restore_says_nothing_when_the_policy_versions_agree` is reviewing a sketch as if it were a
patch. Had the design gone on to specify E2, it would have opened `main.rs`, found the line, and
written the paragraph differently — and the design does disclose, in §8 item 9, that E1 and E2 are
"recommendations I have argued but not costed". A critic who fails a design for the precision of a
deliberately imprecise section is enforcing a standard the section did not claim.

**It is a real argument and it moves the severity of R2-1's second half — the M6-35 collision —
from BLOCKER to MATERIAL. It does not move the first half, and here is why.**

The two halves are different kinds of statement. "The three-case line breaks a green row" is a
consequence of an unspecified proposal, and the counter-argument lands on it. "`policy_divergence`
is **swallowed**" is not a proposal — it is a **factual claim about the shipped code**, and it is
the entire reason E2 is offered. Take it away and E2 has no subject: there is nothing to make loud
that is not already loud, at `warn`, in its own record, before `restore_completed`, asserted by two
rows. A STOP does not license a false premise, because the premise is what the product owner is
being asked to weigh. The design's §6 shows the slide happening in one table: the lead-verification
row reads "`policy_divergence` computed but **not enforced** — **Yes** — `backup.rs:997-1000`.
**Stands.**" — which is true — and §5.4 then spends it as "instead of **swallowing** it", which is
not. That is F-7's mechanism exactly, in the round organised around F-7, and it is the mechanism
rather than the wording that has to be reported.

The same test applied to R2-2 gives the same split, and R2-2 survives it more cleanly: "is not a
property anyone chose" is a factual claim about three shipped comments, one of which sits five lines
above the line cited, and the design's own Rule 2 exists to require reading it. §8 item 9's caveat
is about costing consumers, not about whether the behaviour was deliberate.

So: R2-1 and R2-2 stay BLOCKER on their factual halves, and I record that R2-1's specification half
would be MATERIAL on its own.
