# M7 Foundation Contracts Freeze — Design Record

> ## STATUS: **FROZEN**, 2026-09-22, by the lead.
>
> Round-3 critic verdict **FREEZE**, no BLOCKER, all four corrections independently re-verified
> (`teams/foundation/critic-contracts-freeze-r3.md`). Three MATERIAL non-blocking items were
> raised and all three are applied in place, above: §1.5's heading (42 → 43), §1.5's kernel-b
> sub-header (15 codes / one row → 16 codes / whole-plan census), §1.5's "none is a landed
> variant" claim (`Quarantined` is shared, deliberately), and the citation-audit row 7 that
> turned a correct `seams.rs:297` into a wrong `:296` — withdrawn, verified by the lead against
> the file.
>
> **The contract names are now fixed.** A change to them from here is a change to a frozen
> interface, with the review that implies — not an edit.
>
> **Carried forward as its own review item, not a blocker:** a `drift-check.sh`-style gate stage
> over the six kernel modules, landed **before the first in-crate consumer**. The critic chased
> and rejected the opaque-newtype route for a reason worth keeping: Rust has no per-variant
> visibility, so private variants lock kernel-b's own siblings out of their vocabulary and
> `pub(crate)` re-opens it to kernel-a identically. `token()` concedes it cannot protect the
> neighbour; this repo's own answer to exactly that problem is a red build, and its header says
> so. The ordering matters more than the mechanism.

Author: `arch-foundation-freeze`. **Round 3**, 2026-09-22. Basis **`395d535`** (rounds 1 and 2 were
`26cd632` and `395d535`).

Inputs: `teams/foundation/critic-contracts-freeze.md` (FAIL, F-1..F-8);
`teams/foundation/critic-contracts-freeze-r2.md` (FAIL, R2-1..R2-12);
`teams/foundation/manual-test-plan-contracts-freeze.md` (COMPLETED_WITH_RISKS, D1..D8); the lead's
rulings and corrections of 2026-09-21 and 2026-09-22; the lead's amendment draft
`adr-0027-g13-amendment.md` (read, not edited).

**Round 3 is a correction round, not a redesign.** The shape is settled: five arms, on the
lead's acceptance of the critic's Q1 answer. What changed is listed in §9, and every change is
either a correction the critic proved or an admission the critic forced. **Two of round 2's
findings were wrong and are withdrawn here in their own words** — §4.4's "no composition of
existing parts can produce" (it can, §4.4) and §5.2b step 8's "the in-band repair route does not
exist" (it exists, §5.2b).

**Tree state when every citation below was taken.** `git rev-parse HEAD` → `395d535`. At the start
of this round `git status --porcelain -- crates/ docs/testing/` was **empty**, and most of this
document's citations were taken then. **It is no longer empty, and the correction matters.** Re-run
near the end of the round:

```
 M crates/config-storage/src/rocks.rs
?? crates/config-server/tests/g13_scratch_repro.rs
```

Another agent is editing `rocks.rs` **right now** (+13/−4). So for that one file the working tree
is **not** HEAD, and every `rocks.rs` citation below was re-derived with
`git show HEAD:crates/config-storage/src/rocks.rs | grep -n …` rather than read from the working
tree. What changed is recorded in §8, item 7 — it is relevant, because it partly answers the
Manual Tester's D1. For every other cited path under `crates/` and `docs/`, the working tree and
`395d535` are the same bytes. Round-1 citations were re-opened, not carried.

---

## 0. Decisions at a glance

Each row is a claim. The section named is where it is proved, and no row states more than its
section proves.

| # | Change | Decision | Proved in |
|---|---|---|---|
| 0 | CB-7 shape, **settled** | Five arms. The lead accepted the critic's Q1 answer: the flat-enum alternative is **not available** (nine `AppendReject` variants carry fields), so the real choice was five versus four, and four merges two owners into one file on a name both hold. **Not reopened in round 3.** | §1.3, and the critic's Q1 |
| 1 | CB-7 shape | `Ignored{reason}` retypes to a **five-arm namespaced carrier**, `KernelIgnoredReason`. Three arms reuse landed closed vocabularies; two are new `#[non_exhaustive]` leaves, one per kernel team. **Two things the shape does not buy, both here rather than buried in §1.4:** `#[non_exhaustive]` protects no consumer inside `rdb-core`, which is where all six kernels live (§1.4b); and "one carrier edit, never again" is scoped to *reason names* — kernel-b still edits `event.rs` six more times for its effect variants (§1.4c). | §1.3, §1.4b, §1.4c |
| 2 | CB-7 coverage | **43** names, re-derived across **both whole plans** rather than one row each: kernel-a's 27 `Fact(..)` names and the **16** distinct `Ignored{CODE}` codes kernel-b's plan spells anywhere. Zero residue. Round 2 said 42 and had one residue (`TOO_LARGE`) because it read one row; the table is re-derived, not patched. | §1.5 |
| 3 | CB-7 / F-2 | `AppendReject::NotAMember`, `AckRejectReason::NotAMember` and `AppendReject::Quarantined` vs kernel-b's `QUARANTINED_TERMINAL` vs kernel-a's `Fact(Quarantined)` land in **different arms**. The conflation stops being expressible in the type. | §1.6 |
| 4 | CB-7 / L-R63 `Copy` | Drop `Copy` from **`KernelEffect`, `KernelEvent`, `KernelIgnoredReason` and both new leaves** — five types, not three. Round 2 put `Copy` on the two leaves and called the payload case hypothetical; it is not. Ten of kernel-a's 27 names are asserted with payload braces and one of them carries a non-`Copy` `BlockReason`, so the round-2 derive was **E0204 on day one**. | §1.7 |
| 5 | CB-8 | **Withdrawn.** No new API. Closed by assertions added to `m7f_14` in `crates/rdb-core/tests/seams.rs`. Exact assertions written out. | §2 |
| 6 | `StepCtx::control_time` | **No kernel reads it, at `395d535`.** All six `impl Module::step` take `_ctx`. Stated as a finding, with the search. | §2.3 |
| 7 | kernel-a ask 9 | Unchanged from round 1: not a reshape of `ControlTime`; it is I1 code in `rdb-sim`. Now carries its scope. | §3 |
| 8 | CB-9a | Unit-level skew against `Clock` is reachable today with no new entry point. | §4.2 |
| 9 | CB-9b | **Designed, and it is small — ~12 added lines in one file, zero call-site breakage.** **Corrected in round 3:** CB-9b **does** make M7A-43 and M7A-46 reachable on its own, with no I1. Round 2 proved age-zero under a lockstep harness and then restated it as an impossibility — F-7's own mistake. There is **one** dependency outside foundation, kernel-a's consumer, not two; I1 carries an obligation not to remove the freedom, which is a note, not a gate. | §4.3–§4.6 |
| 10 | **G-13 charter** | **The freeze cannot settle G-13.** ADR-0027 (**Accepted**) says the fix is to seed from `manifest.policy_version_ref`; two shipped doc comments say that value is deliberately never a validation input. **I take the position that the doc comments are right and the ADR clause is wrong**, on evidence from the ADR's own body and from the restore path. Amending an Accepted ADR is not mine. **STOP, with a recommendation.** | §5.1–§5.3 |
| 11 | G-13 (a) | **Round-1 decision reversed.** Do **not** seed the floor from `manifest.policy_version_ref`. The restore path **refuses** a same-identity restore (`backup.rs:880`), so that value is *always* from a foreign lineage. | §5.3 |
| 12 | G-13 shape under "evidence" | **E2 is void and withdrawn** — restore already reports divergence at `warn`, with two green rows, one asserting **silence** when the versions agree. **E1 is live**, and its own withdrawal was itself wrong: the `None` is conditioned on gap G-09, and G-09 closed in `096bbfa`. **The delivered scope:** (1) the rollback refusal reports the two numbers it already holds; (2) the durable floor gets a reading that tells absent from zero, plus an operator surface, still open; (3) **E1** — correct two doc comments that assert a closed gap, and let the offline path record the floor it can now observe. Optional fourth: seed from `req.active_policy_version`. | §5.4 |
| 13 | G-13 (b) | The audit line is **required**, not an open question (F-4). Specified either way. | §5.5 |
| 14 | G-13 (c) | Unchanged: do not touch `policy_divergence` as a **gate**; do change what it **reports**. | §5.6 |
| 15 | G-13 (d) | Unchanged in conclusion (`Option<u64>`, no schema change), different source field. | §5.7 |
| 16 | G-13 signature | Written against **all five** `restore_into_fresh_store` call sites, **conditional on the charter ruling** — under "evidence, minimal shape" the signature does not change at all. | §5.8 |
| 17 | F-5 | `RestoreReport.policy_version_floor` **kept** only if a floor is written at all; populated from inside the write branch, never from the parameter. | §5.9 |
| 18 | D4 | **Neither doc comment is retired.** The lead's D4 ruling is satisfied by changing G-13's shape rather than by earning a retirement. | §5.4 |
| 19 | Not absorbed | One tooling observation (the drift stage's watched path) is **named and routed, not built**. | §7 |

---

## 0.1 Method — what round 1 got wrong, and the rule that replaces it

F-7 is the finding this round is organised around, and the lead has since produced a sixth
instance of the same shape in his own G-13 ruling. Stated once:

> **Evidence has a scope. A conclusion that widens the scope is a new claim and needs new
> evidence.** This holds for searches (a grep over one directory does not speak for the
> workspace), for counts (a table of four rows does not speak for a plan of forty-five), and for
> **equivalence arguments** (an equivalence proved under same-lineage version comparison does not
> survive a cluster-identity change).

Five recorded instances, all in this milestone:

| Instance | The sufficient local check | The claim it was presented as | Who |
|---|---|---|---|
| F-1 | kernel-b `design.md` §3.6, four table rows | "needs no new leaf names" | round-1 architect |
| F-3 | `backup.rs:978` | "Call site" (singular) | round-1 architect |
| F-6 | a grep scoped to `crates/rdb-sim/tests` | "six branches unreachable" | round-1 architect |
| L-R62 | kernel-b had asked; kernel-a had not | "fifteen ladder drop reasons" | lead |
| **G-13 (a)** | break-glass overrides a floor **within one lineage** | "restore inherits break-glass for free" | lead, withdrawn §5.2 |

**Round 3 adds four more, and three of them share a narrower mechanism than F-7 names.** F-7 is
"a sufficient local check presented as a global claim". These three are the **sub-species**: *part
of one source read, and cited as the whole source.*

| Instance | The part read | The claim attached to it | Who | Where corrected |
|---|---|---|---|---|
| R2-6 | `test-plan-m7-kernel-b.md:501`'s "fifteen" list | "all 42 names … zero residue. Full table." | round-2 architect (me) | §1.5, re-derived over both whole plans |
| R2-2 | `backup.rs:96-100`, quoted four times | "`None` … is not a property anyone chose" — the next six lines say it was | lead, withdrawn | §5.4 E1 |
| R2-1 | §6's own verification row, "computed but **not enforced**" | spent one section later as "instead of **swallowing** it" | lead + me | §5.4 E2 |
| R2-3 | §4.4's own conditional proof, "*if* the harness advances in lockstep" | restated in §4.5/§4.6 as "no composition of existing parts can produce" | round-2 architect (me) | §4.4, §4.5, §4.6 |

**Rule 3, which is the one this round earns.** *A citation is a promise that you read enough of
the source to support the claim.* `file:line` is not a unit of evidence; the smallest honest unit
is the doc comment, the enum, the table row **and its neighbours**, or the paragraph. F-6 (my
CB-8 grep), R2-2 and R2-1 are all the same failure: the line was read, the thing next to it was
not. R2-3 is the same failure applied to **one's own text** — the condition was written in §4.4
and dropped by §4.5.

Applied as a check: §10 of this document is a citation audit — every `file:line` re-opened with
its neighbourhood, with a count of how many changed.

**Closure condition, applied throughout this document:** every coverage claim carries the command
that established it and the scope that command covered, in the same sentence as the conclusion.
Where a claim is about a directory, it says the directory. Where it is about the workspace, the
command searched the workspace. Where I could not establish it, §8 says so.

### A second rule, because the first does not catch the sixth instance

The lead's third correction found a defect the closure condition above **cannot** catch, and he is
right that it is a different mechanism. Round 1 proposed to make
`manifest.policy_version_ref` a validation input. The field's own doc comment
(`crates/config-server/src/backup.rs:96-100`) says, in the shipped source, that *nothing checks it
at restore* and gives the reason — "the independently supplied policy may legitimately be older,
newer or **unrelated**". That is the exact mechanism the Manual Tester later rediscovered by
experiment. **No search was skipped. What was skipped was reading the comment on the field being
changed.**

> **Rule 2: before changing what a field means, read what the field says it means.** A doc comment
> that gives a *rationale* is a prior design decision with an argument attached. Contradicting it
> is allowed; contradicting it without noticing is how a defect gets a charter.

Applied as a check: a design that changes a field's role cites the field's own doc comment and
says whether it agrees or overrules it. §5 does this for `policy_version_ref` (`backup.rs:96-100`),
`--active-policy-version` (`cli.rs:202-210`), and the floor cell (`rocks.rs:1312-1324`). §1 does it
for `Alert`'s `ErrorKind` (`event.rs:246-250`) and for the deliberate `NotAMember` collision
(`trace.rs:307-313`) — in both cases the shipped comment **won** and this design moved.

**Six instances now, and the sixth is structural rather than personal.** Five of the six were made
by people who had read the surrounding material carefully. That is the argument for making both
rules mechanical checks in the review checklist rather than advice.

---

## 1. CB-7 — `KernelEffect::Ignored{reason}` cannot carry either kernel's vocabulary

### 1.1 What round 1 proposed, and why it fails

Round 1: `KernelIgnoredReason { Error(ErrorKind), AppendRejected(AppendReject) }`, sized against
kernel-b `design.md` §3.6 — a four-name table.

F-1 is correct and the BLOCKER stands. Two enums cannot hold forty-two names, and widening
`ErrorKind` is not available: `errors.rs:73-77` documents it as "**one name per spec §5.4 error**,
plus `Unavailable`", it carries no `#[non_exhaustive]` (verified: `grep -rn "non_exhaustive"
crates/rdb-core/src/contracts/` returns exactly four hits, all in `event.rs` — `:203` and `:230`
are prose, `:212` is on `KernelEvent`, `:233` is on `KernelEffect`; **no other contracts type is
`#[non_exhaustive]` at `395d535`**), and putting kernel-internal names in it would put kernel
vocabulary in the client error set.

### 1.2 Scope, ruled by the lead and re-counted here

Ruling 1 places kernel-a's `Fact(..)` vocabulary inside CB-7, on the authority of A-R24 at
`crates/rdb-core/src/authority.rs:23`:

```
//! - `Fact(..)` is [`crate::contracts::event::KernelEffect::Ignored`] (ruling A-R24).
```

**I re-ran the census rather than taking the lead's number.** Command and scope — one file,
`docs/testing/test-plan-m7-kernel-a.md`:

```sh
grep -o 'Fact([A-Za-z_]*' docs/testing/test-plan-m7-kernel-a.md | sort | uniq -c | sort -rn
grep -o 'Fact(' docs/testing/test-plan-m7-kernel-a.md | wc -l          # 64
grep -o 'Fact([A-Za-z_][A-Za-z_]*' … | sort -u | wc -l                  # 27
```

Result: **64 occurrences of `Fact(`** over **27 distinct names**, plus 6 bare `Fact(`. The lead's
count was 45/27; the distinct-name count agrees exactly, the occurrence count does not — **64, not
45**. The critic's count was 45/20. The 27 names are listed in §1.5. The difference does not change
any decision; it is recorded because a number nobody re-derives is how L-R62 got to "fifteen".

**None of the 27 is an `ErrorKind` variant.** Scope: I read the whole enum,
`crates/rdb-core/src/contracts/errors.rs:79-116`, all **18** variants — `NotPrimary`,
`RouteChanged`, `LeaseExpired`, `RecoveryReadOnly`, `ProtectionPaused`, `ConditionFailed`,
`CrossAffinity`, `InvalidArgument`, `Overloaded`, `DeadlineBeforeAdmission`, `UnknownOutcome`,
`GenerationChanged`, `RequestIdReuse`, `StaleContinuation`, `CorruptHistory`,
`IncompatibleVersion`, `StatusExpired`, `Unavailable` — and compared it name by name against the
27. Zero intersection.

**Kernel-b's side: twelve unmappable, not nine.** The critic listed nine and called `FORGED_ACK` a
tenth. The plan itself, `docs/testing/test-plan-m7-kernel-b.md:501` (the "NEW (round 6)" row), says
**twelve**, and names them: `NOTHING_OUTSTANDING`, `NO_QUALIFYING_SECONDARY`,
`BARRIER_NOT_DURABLE`, `NOT_A_CURSOR_EVENT`, `OUTSTANDING`, `NOT_FENCED`, `QUARANTINED_TERMINAL`,
`ALREADY_DIVERGED`, `ALREADY_BLOCKED`, `NOT_REQUIRED`, `NOT_A_MEMBER`, `FORGED_ACK`. The critic's
list omits `QUARANTINED_TERMINAL` and `NOT_A_MEMBER`. Plus one that maps (`NOT_PRIMARY`) and two
the plan says map loosely and lose their meaning (`INVALID_CONFIG`, `RECOVERY_ONLY`). Fifteen.

**Round 2 stopped there, and "one row of one file" was exactly the defect (R2-6).** The row's
fifteen is a count *against `ErrorKind`*, not a count of the codes the plan asserts — the same row,
four sentences later, writes `Ignored{TOO_LARGE}` for M7B-59. A number a row states about itself is
not a census. **Re-derived over the whole file**, which is what the census should have been from
the start:

```sh
grep -o 'Ignored{[A-Za-z_][A-Za-z_]*' docs/testing/test-plan-m7-kernel-b.md | sort | uniq -c | sort -rn
```

**Sixteen distinct codes, over 20 occurrences** (plus 11 `Ignored{reason`, which is prose and names
nothing): `NOT_A_MEMBER` (3), `TOO_LARGE` (2), `RECOVERY_ONLY` (2), and one each of
`QUARANTINED_TERMINAL`, `OUTSTANDING`, `NO_QUALIFYING_SECONDARY`, `NOT_REQUIRED`, `NOT_PRIMARY`,
`NOT_FENCED`, `NOT_A_CURSOR_EVENT`, `NOTHING_OUTSTANDING`, `INVALID_CONFIG`, `FORGED_ACK`,
`BARRIER_NOT_DURABLE`, `ALREADY_DIVERGED`, `ALREADY_BLOCKED`. The same command over kernel-a's plan
returns **only** `Ignored{reason` (3), so kernel-a asserts no `Ignored{CODE}` spelling and its
vocabulary reaches `Ignored` through `Fact(..)` alone — which the A-R24 link already says, and
which is now measured rather than assumed.

So CB-7's real scope is **43 names**, not 4, not 15 and not 42.

### 1.3 The shape — CB-1's precedent, applied one level down

The precedent is landed and I opened it. `crates/rdb-core/src/contracts/event.rs:195-210`:

> Foundation owns the **carrier**; team kernel-b owns the **variants** (ask CB-1: "carrier pair in
> C0, variants owned by kernel-b"). The shape is the one `AppendReject` already uses — one variant
> holding an enum the consuming team fills — rather than a flat variant per kernel fact, which
> would put roughly 130 rows' worth of names in this file and make every addition a foundation
> edit.

Applied to the reason: **one arm per owning vocabulary**, each arm's leaf owned by whoever names
its variants.

```rust
// ── crates/rdb-core/src/contracts/event.rs ──────────────────────  FOUNDATION owns this file.
//    Edited once by CB-7 and, FOR A REASON NAME, never again. Kernel-b still edits this file six
//    more times for its own effect variants (PeerProgress, CopyLost, DivergenceDetected,
//    QualificationChanged, BlockPartition, CopyQuarantined) — its own edits, not CB-7's. §1.4c.

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)] // Copy dropped (§1.7)
#[non_exhaustive]
pub enum KernelEvent { /* unchanged variants; derive line changes only */ }

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)] // Copy dropped (§1.7)
#[non_exhaustive]
pub enum KernelEffect {
    Ignored { reason: KernelIgnoredReason },  // was `ErrorKind`
    Alert   { reason: ErrorKind },            // unchanged: spec-bound, operator-facing (§1.8)
}

// ── crates/rdb-core/src/contracts/ignore.rs ─────────────────────  NEW FILE.
//    Registered in crates/rdb-core/src/contracts.rs (the `pub mod` list at :23-36).

/// Why a kernel module produced `KernelEffect::Ignored` (CB-7).
///
/// FOUNDATION owns this enum's ARM SET. An arm is added only when a new *owner* appears, which
/// is a foundation-scale event. No team adds an arm to spell a fact; it adds a variant to its
/// own leaf below.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)] // not Copy (§1.7)
#[non_exhaustive]
pub enum KernelIgnoredReason {
    /// A client-facing spec §5.4 condition. Leaf: `contracts::errors::ErrorKind` (18, closed).
    Error(ErrorKind),
    /// The append ladder refused the record. Leaf: `contracts::envelope::AppendReject` (16).
    AppendRejected(AppendReject),
    /// The acknowledgement did not count. Leaf: `contracts::trace::AckRejectReason` (14).
    AckRejected(AckRejectReason),
    /// A fact kernel-a's authority module states about itself.
    Authority(AuthorityIgnoreReason),
    /// A fact one of kernel-b's five modules states about itself.
    Replica(ReplicaIgnoreReason),
}

/// KERNEL-B owns every variant of this enum. Add one by editing this enum and nothing else.
///
/// Not `Copy` — see `AuthorityIgnoreReason` below for why, and §1.7 for the general rule. A
/// variant with an owning payload is a variant kernel-b adds without asking anyone.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ReplicaIgnoreReason { /* twelve at §1.5; kernel-b's list, kernel-b's edit */ }

// ── crates/rdb-core/src/contracts/authority.rs ──────────────────  KERNEL-A's vocabulary file.
//    Already holds DenyReason (15), Checkpoint, AuthorityView, BlockReason, PartitionMode.

/// KERNEL-A owns every variant of this enum. Add one by editing this enum and nothing else.
///
/// **Not `Copy`, and it cannot be.** Ten of the twenty-seven names are asserted with a payload
/// (§1.5), and `Blocked { reason: BlockReason }` (M7A-158, `test-plan-m7-kernel-a.md:631`) carries
/// `BlockReason::DivergenceRequiresOperator { diverged: Vec<CopyId> }`
/// (`contracts/authority.rs:197-206`), which is not `Copy`. A `Copy` derive here is E0204 on the
/// first variant kernel-a writes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum AuthorityIgnoreReason { /* twenty-seven at §1.5; kernel-a's list, kernel-a's edit */ }
```

**Round 2 wrote `Copy` on both leaves and called the payload case hypothetical. It was not, and
the correction is R2-4's.** §1.7 argues at length that a derive must not be justified from today's
tree — and then justified two derives from today's tree, by not looking. The ten payload names are
in the very file §1.5 censused, and one of them was already non-`Copy` at `395d535`.

**The derive set, re-checked against the payloads that exist rather than against symmetry.** The
carrier's derive set is a standing constraint on every leaf — `KernelIgnoredReason: Ord` requires
`AuthorityIgnoreReason: Ord` — so this is the one place a foundation derive can still block a
kernel, and it is worth stating rather than deriving by habit:

| Derive | Survives today's named payload (`BlockReason`)? | Kept? |
|---|---|---|
| `Debug`, `Clone` | yes; `Clone` constrains nothing a `Serialize` payload does not already have | **keep** |
| `PartialEq`, `Eq` | yes (`authority.rs:197`). A future `f64` payload would break `Eq` | **keep**, and named as a claim about today's plans |
| `PartialOrd`, `Ord`, `Hash` | yes — `BlockReason` derives all three, and `Vec<CopyId>` has them because `CopyId` does (`contracts/membership.rs:33`) | **keep**, same caveat |
| `Serialize`, `Deserialize` | yes | **keep** — required by §1.9 P4 |
| **`Copy`** | **no** | **drop, on all five types** |

**Why `Copy` is the one that goes, when R2-11 is right that the argument indicts the others
equally.** Because `Copy` is the only one a **named** payload has already hit. Kernel-b's plan says
so in its own words (`test-plan-m7-kernel-b.md:501`, the `BlockPartition` row): "**Blocked on a
derive, not on a missing name** … this variant needs foundation to drop `Copy`". That is a census
of today's plans, and §1.7 rejected census arguments on principle — so it is offered as a census,
with its scope attached, rather than dressed up as a contract argument. The contract argument
(§1.7) says the carrier should promise as little as possible; the census says which promise to
break **first**. The others stay because no plan has hit them, and when one does, the same
reasoning applies again.

### 1.4 Owner and file for every leaf — and the rule for adding a name

| Arm | Leaf type | File (at `395d535`) | Status | Who edits the variants |
|---|---|---|---|---|
| `Error(..)` | `ErrorKind` | `contracts/errors.rs:79` | landed, 18, closed | **foundation** — and only when spec §5.4 changes |
| `AppendRejected(..)` | `AppendReject` | `contracts/envelope.rs:524` | landed, 16 | **kernel-b** (its ladder; CB-2 widened a payload here) |
| `AckRejected(..)` | `AckRejectReason` | `contracts/trace.rs:315` | landed, 14 | **kernel-b** (its ack tracker; CB-3 took it 7 → 14) |
| `Authority(..)` | `AuthorityIgnoreReason` | `contracts/authority.rs` | **NEW** | **kernel-a** |
| `Replica(..)` | `ReplicaIgnoreReason` | `contracts/ignore.rs` | **NEW** | **kernel-b** |

**The rule, stated so it can be checked:** *a kernel adds a reason name by appending one variant to
its own leaf enum. It never edits `event.rs` **for a reason name**, never edits
`KernelIgnoredReason`'s arm set, and never waits on foundation or on the other kernel **for the
append itself**.* Both qualifications are load-bearing and both are new in round 3: see §1.4b for
what `#[non_exhaustive]` does not do, and §1.4c for the six `event.rs` edits kernel-b will make
anyway.

~~The two new leaves are `#[non_exhaustive]`, so an append cannot break a consumer's `match`.~~
**Withdrawn — false for exactly the six consumers this shape is built for. §1.4b.**

**Placement, and why not elsewhere.** Two homes were considered and rejected on evidence:

1. *Leaves in the kernel modules* (`crates/rdb-core/src/authority.rs`, `replication.rs`, …) —
   **rejected on the subject argument in item 2. The layering is a convention, not a constraint,
   and round 2 presented it as a constraint (R2-7a).** Nothing enforces it: there is no
   `clippy.toml` and no `deny.toml` anywhere in the repository, no `[lints]` table or
   `disallowed-*` entry in the workspace manifest or `crates/rdb-core/Cargo.toml`, the only
   `.cargo/config.toml` is four lines setting `LIBCLANG_PATH` for RocksDB's bindgen (ADR-0017),
   and `lib.rs:36-37` carries only `#![deny(missing_docs)]` and `#![forbid(unsafe_code)]`. Rust
   permits module cycles freely inside one crate, and **no Rust lint enforces module layering at
   all**, so no configuration could have. `contracts/ignore.rs` importing `crate::authority`
   would compile and the gate's fmt/deps/drift/clippy stages would not object. **This document is
   careful in §1.6 to say "structural and not a convention"; the same distinction is owed here,
   and round 2 did not draw it.** The convention is still worth honouring — it is 14-for-14 with
   a rationale declared in `lib.rs:20-24` — and it is a *reason*, not a *rule*. The evidence for
   it, which stands:
   Scope: `grep -rn "^use crate::\|^use super::"
   crates/rdb-core/src/contracts/*.rs` over all fourteen contracts files returns **only**
   `use crate::contracts::…` imports. No contracts file names a kernel module, in either direction.
   `lib.rs:21-24` states the layering: `contracts` are the seams, the six modules sit on top.
   Putting a leaf in `crate::authority` would make the shared vocabulary depend on kernel-a's
   implementation file, which was rewritten +302/−16 at the last basis
   (`test-plan-m7-kernel-a.md` §15 row 16).
2. *Both leaves in `contracts/authority.rs` / `contracts/envelope.rs`* — **rejected, subject
   mismatch.** This repository files contracts by *subject*, not by team. Kernel-a's 27 names are
   all authority facts, so `contracts/authority.rs` **is** the right subject home for its leaf and
   that is where it goes. Kernel-b's twelve span four subjects — replication
   (`NOTHING_OUTSTANDING`, `OUTSTANDING`, `NOT_A_CURSOR_EVENT`), publication
   (`NO_QUALIFYING_SECONDARY`, `BARRIER_NOT_DURABLE`), protection (`NOT_FENCED`,
   `ALREADY_BLOCKED`, `QUARANTINED_TERMINAL`) and recovery (`NOT_REQUIRED`, `ALREADY_DIVERGED`) —
   so no landed subject file holds them all. They get the new file.

**What this does not buy, said plainly (F-7).** A leaf addition still lands under
`crates/rdb-core/src/contracts`, so it still moves the drift basis and still red-builds all four
§15 tables (`AGENTS.md`, "Running the gate"). The shape removes the **carrier** edit and the
**cross-team wait**; it does not remove the drift cost. Anyone reading row 1 of §0 as "leaf
additions are free" is reading more than this section proves. A way to remove the drift cost too
exists and is **named but not built** — §7.

### 1.4b `#[non_exhaustive]` does not do what round 2 claimed, and the shape survives it

**The admission first, plainly.** `#[non_exhaustive]` is a **cross-crate** attribute. The Rust
language gives it no effect inside the crate that defines the type: an in-crate `match` may be
exhaustive with no catch-all, and it breaks on every added variant. `crates/rdb-core/src/lib.rs:39-45`
declares `pub mod authority; pub mod contracts; pub mod protection; pub mod publication;
pub mod recovery; pub mod replication; pub mod transaction;` — **the six kernel modules and the
contracts are one crate.** So the sentence round 2 wrote in §1.4, "an append cannot break a
consumer's `match`", is **false for exactly the six consumers this shape exists to serve**, and it
was one of two benefits §0 row 1 advertised. It is withdrawn, not softened.

The guarantee does hold where the consumer is another crate — `crates/rdb-core/tests/seams.rs` and
everything in `crates/rdb-sim/` — which is most of today's consumers and none of tomorrow's.

**Does the shape still earn its keep with that benefit removed? Yes, and I will say exactly why,
because "yes" is the cheap answer here.**

*First, the benefit is not differential.* Every alternative loses it identically. One flat
39-name enum in `event.rs`, one merged kernel leaf, leaves in the kernel modules — all of them are
in `rdb-core`, all of them are matched by the six modules, and none of them gets `#[non_exhaustive]`
protection either. Removing a benefit that no candidate has changes no comparison. The three
grounds that **are** differential are untouched by R2-5: ownership (each kernel's names live in a
file only that kernel edits), homograph separation (seven families become E0308 at the row's own
line, §1.6), and one carrier edit for reason names.

*Second — and this is the part that is not merely "the alternatives are no better" — the arms
change where the breakage lands, and that is worth more without `#[non_exhaustive]` than with it.*
Rust's exhaustiveness check is per-`match`, against the variants the pattern actually names. So:

| In-crate event | Under one flat 39-name enum | Under the five arms |
|---|---|---|
| kernel-b adds a **name** (frequent: 16 today, ~130 rows' worth foreseen) | every total `match` in all six modules breaks | breaks **only** a `match` that destructures `Replica(..)` into its variants. A consumer that writes `Replica(_) => …` is unaffected |
| foundation adds an **arm** (rare: one per new owner, "a foundation-scale event") | n/a | every in-crate total `match` breaks |

The frequent event is the one the shape puts behind a boundary a consumer can stand outside of;
the rare event is the one that still breaks everybody. A flat enum has only the frequent event and
puts it in the breaking position. **`#[non_exhaustive]`'s loss costs less under this shape than
under any alternative, and the arm split is the reason.** That is a claim about Rust's
exhaustiveness rule, not a measurement; there is no in-crate `match` on these types at `395d535`
to measure (`grep -rn "KernelEffect\|KernelEvent" crates/ --include=*.rs` → 13 hits in 4 files, and
the only `crates/rdb-core/src/` hit is a doc link at `authority.rs:23`).

**So: no design change. The mechanism that recovers the protection is a convention, and it costs
nothing.** Stated so it can be checked, and written into both leaves' doc comments:

> **A kernel module never destructures another kernel's leaf.** It matches the arm — `Replica(_)`,
> `Authority(_)` — or it does not match the reason at all. Destructuring is for the owner.

Three alternatives were considered and are costed rather than waved past:

1. **A `_` arm on every in-crate `match`.** Cost: it is the failure `seams.rs:258-259` deliberately
   refuses for the carrier enums — "Both matches are exhaustive with **no `_` arm**. A wildcard
   would compile forever and stop catching the next variant silently." A blanket `_` convention
   would make a newly added kernel fact fall into a default branch with no compile error, which is
   the (d-strong) shape. **Rejected as a blanket rule**; note that the convention above is
   narrower — it wildcards *the other kernel's* leaf, where silently ignoring a fact you do not own
   is the correct behaviour, and leaves your own leaf exhaustive. The distinction is real and the
   repository is already inconsistent about it: the same row that refuses `_` on the carriers
   writes `_ => None` on the reason match at `seams.rs:297`.
2. **A test that enumerates the variants.** There is a landed precedent —
   `PolicyRejected::reason()` (`crates/config-core/src/policy.rs:342-362`) is a total `match`
   returning a `&'static str` token, beside an "every reason token, in declaration order" list. A
   `const fn token(&self)` on each leaf, **in the leaf's own file**, makes an append break exactly
   one place: the owner's own file, one line below the variant they just added, in the same edit.
   Cost: ~27 lines in `contracts/authority.rs` and ~12 in `contracts/ignore.rs`, both owner-owned.
   **Recommended as a cheap improvement, not required by this freeze**, and it does not substitute
   for the convention — it protects the owner from forgetting, not the other kernel from being
   broken.
3. **Nothing.** Honest, and what the design falls back to if the convention is not adopted: a
   cross-kernel in-crate `match` breaks on an append, and fixing it is the other kernel's edit in
   the other kernel's file. **The cross-team wait is reduced, not removed.** §0 row 1 now says so.

### 1.4c "One carrier edit, never again" is scoped to reason names (R2-7b)

`test-plan-m7-kernel-b.md:501` carries a second **NEW (round 6)** entry that round 2 read past
while citing the same row three times: "**The effect half is empty.** … today only `Ignored` and
`Alert` can be emitted, so every row asserting one of these six **in an effect vector** has no
landed spelling. `#[non_exhaustive]` plus kernel-b's ownership of the variants is exactly what
makes this a kernel-b edit and **not** a foundation ask."

The six are `PeerProgress`, `CopyLost`, `DivergenceDetected`, `QualificationChanged`,
`BlockPartition`, `CopyQuarantined`, across M7B-30, 41, 51, 54, 112, 128, 131, 134, 140, 142, 144.
**Kernel-b will edit `crates/rdb-core/src/contracts/event.rs` six more times**, and each edit moves
the drift basis. That work is correctly **outside** CB-7's scope — it is kernel-b's own edit by
`:501`'s own ruling — but §1.3's file header ("Edited once by CB-7 and, for a reason name, never
again") and §0 row 1 both read as a promise about the file, and a reader takes away a benefit this
design does not deliver. The header is corrected to say *for a reason name*; §0 row 1 names the six.

One consequence worth pairing with §1.7: `BlockPartition{reason: BlockReason}` is the variant
kernel-b is blocked on, and it is blocked on the `Copy` derive, not on a missing name. Dropping
`Copy` from `KernelEffect` (§1.7) unblocks M7B-142 as a side effect of CB-7, and that is the one
row where this freeze's derive decision and kernel-b's six edits touch.

### 1.5 The complete mapping — all 43 names, zero residue

**Kernel-a — 27 names, all to `Authority(AuthorityIgnoreReason::…)`.** Source: the census command
in §1.2, scope `docs/testing/test-plan-m7-kernel-a.md`. Checked name by name against the three
landed enums, read in full at `errors.rs:79`, `envelope.rs:524`, `trace.rs:315`.

**One name is shared, and that is deliberate.** An earlier revision said "none is an `ErrorKind`,
an `AppendReject` or an `AckRejectReason` variant". `comm -12` over the 41 landed variant names
and these 27 returns **`Quarantined`** (found by the round-3 critic, 2026-09-22). It does not move
the table: §1.6 at line 588 of this document already rules the three-way `Quarantined` split
deliberate, and all five `Fact(Quarantined)` rows — M7A-99, 132, 171, 174 — are authority facts.
Residue stays zero, the total stays 43, `ReplicaIgnoreReason` stays twelve.

| # | `Fact(..)` name | Occurrences | # | `Fact(..)` name | Occurrences |
|---|---|---|---|---|---|
| 1 | `ReplyWithheld` | 6 | 15 | `TakeoverDeferred` | 1 |
| 2 | `StaleAuthorityAnswer` | 5 | 16 | `StaleTimer` | 1 |
| 3 | `Quarantined` | 5 | 17 | `RenewalWithheld` | 1 |
| 4 | `SampleRejected` | 4 | 18 | `QualificationLostAfterPublish` | 1 |
| 5 | `PublishRefusedBlocked` | 4 | 19 | `PublishedWhileFrozen` | 1 |
| 6 | `LateRenewalIgnored` | 4 | 20 | `FenceWhileBlocked` | 1 |
| 7 | `StaleAuthorityView` | 3 | 21 | `FamilyRejected` | 1 |
| 8 | `ReplySuppressedAfterTimeout` | 3 | 22 | `ExternalFenceRejected` | 1 |
| 9 | `PublishPredicateFalse` | 3 | 23 | `DispatchRefusedFrozen` | 1 |
| 10 | `PublishDeferred` | 2 | 24 | `DispatchDroppedByFreeze` | 1 |
| 11 | `CandidateUnreachable` | 2 | 25 | `CandidateWhileNotServing` | 1 |
| 12 | `AcquireWithheld` | 2 | 26 | `Blocked` | 1 |
| 13 | — | | 27 | `AlreadyBlocked` | 1 |
| 14 | `AdmissionSuspended` | 1 | — | `AdmissionRefused` | 1 |

(Rows 13/14 are the table's own numbering gap, not a missing name; 27 names are listed. The 6 bare
`Fact(` occurrences are prose, not assertions, and name nothing.)

**Kernel-b — 16 codes, across four arms.** Source: a census re-derived over the **whole**
kernel-b plan, not the single row at `test-plan-m7-kernel-b.md:501`. That row's own list reads
"fifteen" and is 1+2+12; it names `TOO_LARGE` outside its own count, which is how the round-2
table lost it. The re-derived figures: 18 raw strings over 35 occurrences, less `Ignored{reason`
(11) and bare `Ignored{` (4), leaves **16 codes over 20 occurrences**. Re-run independently by
the round-3 critic on 2026-09-22 and found set-identical to this table.

| Code | Arm | Leaf variant | Note |
|---|---|---|---|
| `NOT_PRIMARY` | `Error` | `ErrorKind::NotPrimary` | the one that already mapped (L-R62) |
| `NOT_A_MEMBER` (M7B-44, 145) | `AckRejected` | `AckRejectReason::NotAMember` | **F-2** — the tracker's drop, never the receiver's |
| `FORGED_ACK` (M7B-31, 32) | `AckRejected` | `AckRejectReason::ForgedIdentity` | landed name, per `trace.rs:315` |
| `QUARANTINED_TERMINAL` (M7B-119) | `Replica` | `QuarantinedTerminal` | **F-2** — F1's terminal phase, not `AppendReject::Quarantined` |
| `INVALID_CONFIG` | `Replica` | `InvalidConfig` | gains its own name; the plan says `InvalidArgument` loses its meaning |
| `RECOVERY_ONLY` | `Replica` | `RecoveryOnly` | same; keeps M7B-59's `TOO_LARGE`/`RECOVERY_ONLY` pair distinct in one vector |
| `NOTHING_OUTSTANDING` (M7B-64) | `Replica` | `NothingOutstanding` | |
| `NO_QUALIFYING_SECONDARY` (M7B-76) | `Replica` | `NoQualifyingSecondary` | |
| `BARRIER_NOT_DURABLE` (M7B-74) | `Replica` | `BarrierNotDurable` | |
| `NOT_A_CURSOR_EVENT` (M7B-61) | `Replica` | `NotACursorEvent` | |
| `OUTSTANDING` (M7B-55) | `Replica` | `Outstanding` | |
| `NOT_FENCED` (M7B-84) | `Replica` | `NotFenced` | |
| `ALREADY_DIVERGED` (M7B-140) | `Replica` | `AlreadyDiverged` | |
| `ALREADY_BLOCKED` (M7B-142) | `Replica` | `AlreadyBlocked` | distinct from kernel-a's `Fact(AlreadyBlocked)`, which is `Authority(..)` |
| `NOT_REQUIRED` (M7B-128) | `Replica` | `NotRequired` | |
| **`TOO_LARGE` (M7B-59)** | **`AppendRejected`** | **`AppendReject::TooLarge`** (`contracts/envelope.rs:530`) | **Added in round 3 (R2-6).** The row is `TOO_LARGE` → `Ignored{TOO_LARGE}` at `test-plan-m7-kernel-b.md:176`, and it needs to stay apart from `Ignored{RECOVERY_ONLY}` in **one** vector |

`ReplicaIgnoreReason` therefore starts at **twelve** variants. `AuthorityIgnoreReason` starts at
**twenty-seven**. Residue over both whole plans: **zero**.

**Why `TOO_LARGE`'s omission was not cosmetic, and what it changes about the fifth arm.** It is the
**only demonstrated consumer of the `AppendRejected(..)` arm in either plan** — none of the other
42 names reaches it. Round 2 offered a "full table" that omitted the single row justifying one of
its five arms, and then argued for that arm from precedent rather than from a consumer. M7B-59
needs `AppendRejected(AppendReject::TooLarge)` to stay distinct from `Replica(RecoveryOnly)` in one
vector; with the row present, the fifth arm stops looking like a spare degree of freedom. The
critic found this, and its observation that the omission "changes the Q1 answer" is correct.

**How the table was re-derived rather than patched.** §0.1's closure condition says a coverage
claim carries the command and the scope. Round 2's scope was *one row* and its conclusion was
*both plans*. The corrected census is the two `grep -o` commands in §1.2 over the two whole files,
and it found exactly one missing name — which is the result, not the reason: a table that claimed
zero residue and had one is a table whose method failed. The method is now the file.

**The ten payload names, which this table does not spell and must not pretend to (R2-4).** §1.5 is
a table of **names**. Ten of kernel-a's 27 are asserted **with a payload brace**, over 22
occurrences — scope, one file, `grep -o 'Fact([A-Za-z_][A-Za-z_]*{' docs/testing/test-plan-m7-kernel-a.md`:

`SampleRejected` (4), `ReplyWithheld` (4), `Quarantined` (4), `PublishPredicateFalse` (3),
`AcquireWithheld` (2), and one each of `RenewalWithheld`, `ExternalFenceRejected`,
`CandidateWhileNotServing`, `CandidateUnreachable`, `Blocked`.

These are assertions, not prose: `Fact(ReplyWithheld{reason})` is in M7A-104's **Expected** column,
`Fact(AcquireWithheld{reason})` in M7A-148's. **One payload type is already known and is already
not `Copy`:** M7A-158 (`test-plan-m7-kernel-a.md:631`,
`blocked_then_freeze_then_recovered_mode_sequence`) sets up
`BlockPartition{DivergenceRequiresOperator{diverged}}` and expects `Fact(Blocked{reason})`, whose
`reason` is a `BlockReason` — `DivergenceRequiresOperator { diverged: Vec<CopyId> }`,
`contracts/authority.rs:197-206`. **The other nine payload shapes are kernel-a's to spell, and this
document does not spell them.** §0 row 2's claim is therefore about **names**, and says so.

The same file's own standing ruling applies to the braces, which is why they cannot be read as
shorthand: `test-plan-m7-kernel-b.md:501` records that a plan's literals are corrected to the
landed shape *or* the type is widened ("**The literals are wrong.** … neither field name exists").
Braces are not dropped silently.

Kernel-b's §3 ladder rows (M7B-02..07, 14, 18, 20, 28, 120..122) are unaffected: they assert
`AppendReject` variants directly, and reach `Ignored` through `AppendRejected(..)` when they assert
the effect rather than the outcome.

### 1.6 How the shape structurally closes F-2

`test-plan-m7-kernel-b.md:497` rules that a row asserting one `NotAMember` where the other belongs
"passes for the wrong reason". Foundation's own doc comment already records the collision as
deliberate — `crates/rdb-core/src/contracts/trace.rs:307-313`:

> `StaleGeneration` and `NotAMember` are also `AppendReject` variants. Same words, different enum,
> different meaning: there, why a replica refused an append; here, why an acknowledgement does not
> count. Kept deliberately rather than by accident.

Under round 1's two arms, both facts could only be spelled `AppendRejected(AppendReject::NotAMember)`
— the conflation was not merely possible, it was the **only** spelling available, so every M7B-44
row would have been wrong and green.

Under the five arms the two facts have different *types*, one enum apart:

- `AckRejected(AckRejectReason::NotAMember)` — the tracker dropped an ACK.
- `AppendRejected(AppendReject::NotAMember)` — the receiver refused an append.

**Why that is structural and not a convention.** `AckRejectReason` and `AppendReject` are distinct
Rust types with no `From` between them (checked: `grep -rn "impl From" crates/rdb-core/src/contracts/`
finds no conversion between the two). A row that writes the wrong one does not compile — it is an
E0308 naming both enums, at the row's own line. The three `Quarantined` facts split the same way:
`Authority(Quarantined)` (kernel-a), `Replica(QuarantinedTerminal)` (kernel-b F1),
`AppendRejected(AppendReject::Quarantined)` (row 0 of the ladder).

**What it does not close.** Two variants *within* one leaf can still be confused — nothing stops a
row writing `AckRejectReason::StaleEpoch` where `StaleConfig` belonged. The arms separate the
*ladders*, which is what `:497` rules on; they do not separate rungs.

### 1.7 L-R63 and the `Copy` derive — re-argued on the contract

F-7's second-order finding is correct: round 1 justified dropping `Copy` by measuring today's
`crates/`, and CB-7 exists precisely to admit consumers that do not exist yet. The census cannot
speak for them. Here is the argument that can.

**The contract argument.** `KernelEffect` carries two promises at `event.rs:232-233`:

- `#[non_exhaustive]` — *kernel-b may add a variant without foundation's involvement*. The file's
  own doc says so at `:203-206`: it "is the machine-readable form of that ownership".
- `#[derive(Copy)]` — *every variant that will ever exist has a `Copy` payload*.

**These contradict.** The second silently conditions the first: kernel-b may add a variant
*provided foundation approves its payload's traits*. That is the wait CB-1 was asked to remove.
`BlockReason::DivergenceRequiresOperator { diverged: Vec<CopyId> }`
(`crates/rdb-core/src/contracts/authority.rs:198-206`, read at `395d535`) is not the reason to drop
`Copy`; it is the first member of the class the derive forbids, and it arrived within one milestone
of the derive being written. A carrier whose variants another team owns cannot also promise a trait
that constrains those variants' payloads. `Clone` makes the same value available and constrains the
payload only to `Clone`, which every payload in a `Serialize`/`Deserialize` contract has anyway.

**The argument applies to three types, not one.** Round 1 dropped `Copy` from `KernelEffect` and
kept it on the reason. That is inconsistent under the contract argument:

| Type | `#[non_exhaustive]`? | Variants owned by | Verdict |
|---|---|---|---|
| `KernelEffect` (`event.rs:233-234`) | yes | kernel-b | **drop `Copy`** |
| `KernelEvent` (`event.rs:212-213`) | yes | kernel-b | **drop `Copy`** — same contradiction. Kernel-b's plan asks for it ("and `KernelEvent`'s, for symmetry", `test-plan-m7-kernel-b.md:501`) and names the likely first payload: `CopyLost` needs a reason, because M7B-41 and M7B-134 must tell divergence from lag |
| `KernelIgnoredReason` (new) | yes | foundation (arms), two kernels (leaves) | **do not derive `Copy`** — the same promise one level down. A leaf variant with a non-`Copy` payload (`FamilyRejected { keys: Vec<ControlKey> }` is the shape kernel-a's `Fact(FamilyRejected)` suggests) would otherwise need a foundation edit |
| the five leaves | `ErrorKind`/`AppendReject`/`AckRejectReason`: no; the two new: yes | each owner | **each owner's call, and neither new leaf derives `Copy`.** ~~The two new leaves are C-like and start `Copy`; if kernel-a later needs a payload, it drops its own derive and nothing else moves.~~ **WITHDRAWN (R2-4).** Ten of kernel-a's 27 names carry payloads today and `Blocked{reason: BlockReason}` is already non-`Copy`, so the round-2 derive was E0204 on the first variant written. "If kernel-a later needs a payload" was the hypothetical this very section forbids arguing from — and it was wrong in the direction that matters, because the payload is not later, it is in the plan §1.5 censused. See §1.3's derive table. **The half that survives is real and is the arm split's own benefit:** if kernel-a later needs a payload that breaks `Ord` or `Hash`, it drops *its own* derive — subject to the carrier's derive set, which is the one foundation promise that still constrains a leaf (§1.3) |

**Blast radius, re-measured at `395d535`.** Command and scope — the whole `crates/` tree:
`grep -rn "KernelEffect\|KernelEvent" crates/ --include=*.rs` returns **13 hits in 4 files**:
`contracts/event.rs` (8, the definitions and carriers), `src/authority.rs:23` (a doc link, not a
use), `rdb-core/tests/seams.rs` (4), `rdb-sim/tests/dispatch.rs` (2).

**Six edit sites, not four (R2-9).** Round 2 named the sites that *mention* the types and missed
the ones that *import* them — the same class of omission as the rest of this round:

| # | Site | Why it changes |
|---|---|---|
| 1 | `crates/rdb-core/tests/seams.rs:20` | `use …event::{EffectKind, EventKind, KernelEffect, KernelEvent}` — needs `KernelIgnoredReason` |
| 2 | `seams.rs:267` | `reason: ErrorKind::Unavailable` literal → `KernelIgnoredReason::Error(ErrorKind::Unavailable)` |
| 3 | `seams.rs:297` | `EffectKind::Kernel(KernelEffect::Ignored { reason }) => Some(*reason)`. `*reason` needs `KernelIgnoredReason: Copy`, which this design does not give it → `Some(reason.clone())`. **Corrected 2026-09-22: round 2 and the critic both cited `:297` and both were right. A round-3 "re-open" moved it to `:296` and was wrong — `:296` is the `match` head, `:297` is the `Some(*reason)` arm, `:298` is `_ => None`. Verified by the lead against the file** |
| 4 | `seams.rs:299` | `assert_eq!(reason, Some(ErrorKind::Unavailable))` — the **expected** value must be re-wrapped too, or the row fails to compile on a type mismatch |
| 5 | `crates/rdb-sim/tests/dispatch.rs:20-22` | same import gap |
| 6 | `dispatch.rs:483` | literal, inside a `for effect in [ … ]` array. The loop **moves** out of the array and `EffectKind` is already non-`Copy`, so the array already moves — the derive change does not touch it; the **field type** change does |

`EffectKind` (`event.rs:312`) derives `Debug, Clone, PartialEq, Eq`. **`EventKind` is at
`event.rs:152`, not `:151`, and derives `Debug, Clone, PartialEq, Eq, Serialize, Deserialize`** —
round 2 gave the wrong line and an incomplete derive list, and the `Copy` conclusion survives both.
**Neither wrapper is `Copy` today**, so dropping `Copy` from the two inner enums changes no
wrapper's trait set. That is a fact about today's tree and is offered as one: it says nothing about
future wrappers, and there is no reason it should.

**The five-arm derive set closes — a negative result the critic established and round 2 never
checked.** `KernelIgnoredReason` carries `PartialOrd, Ord, Hash, Serialize, Deserialize`, so every
leaf needs them. All three landed leaves have them: `ErrorKind` (`errors.rs:78`), `AppendReject`
(`envelope.rs:523`) and `AckRejectReason` (`trace.rs:314`) each derive
`Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize`. No missing
bound. The landed leaves keep their own `Copy`, and that is the point rather than an inconsistency:
**`Copy` on a leaf is the owner's business; `Copy` on the carrier is a promise about payloads the
carrier does not own.**

### 1.8 `Alert{reason: ErrorKind}` — left unchanged, and the critic's flag answered

The critic flagged (not filed) that kernel-b asserts `Alert{RebuildStalled}` (M7B-128) and
`RebuildStalled` is not an `ErrorKind` variant either — verified against the 18 names in §1.2.

**Decision: `Alert` keeps `ErrorKind`, and that is a narrowing of what `Alert` means, stated
rather than assumed.** `Alert` is documented at `event.rs:246-250` as "an operator-visible
condition the kernel wants surfaced". An operator-visible condition *is* client-facing vocabulary;
that is what makes `ErrorKind` the right type for it and what makes it different from `Ignored`,
which is a kernel talking to itself.

**So M7B-128's `Alert{RebuildStalled}` is not spellable after CB-7, and this design does not make
it spellable.** That is a live consequence, not a closed one. The two available answers, neither
of which foundation should pick alone:

1. `RebuildStalled` is genuinely operator-visible → it wants an `ErrorKind` variant, which is a
   **spec §5.4 question**, not a contracts question.
2. It is a kernel fact that also deserves an alert → the row asserts
   `Ignored{Replica(RebuildStalled)}` and a separate `Alert` at whatever §5.4 name fits.

**Routed to the lead as an open question (§8, Q-2).** It is named here because the critic is right
that it is cheaper to answer before the basis moves than after.

### 1.9 How a Manual Tester drives this by hand

Unchanged in substance from round 1, with two additions the Manual Tester's P4 demands.

1. **P1 — the widened vocabulary compiles.**
   `KernelEffect::Ignored { reason: KernelIgnoredReason::AppendRejected(AppendReject::WrongPartition) }`.
   Before CB-7 the equivalent line does not compile; that failure *is* the defect. After, `Debug`
   reads `Ignored { reason: AppendRejected(WrongPartition) }`.
2. **P1b — F-2's conflation is unspellable.** Write
   `KernelIgnoredReason::AckRejected(AppendReject::NotAMember)`. Expect **E0308**, naming both
   enums. This is the probe that proves §1.6 structural rather than conventional, and it did not
   exist in round 1.
3. **P2 — `Copy` is really gone.** Add `BlockPartition { reason: BlockReason }` to `KernelEffect`
   in the tester's own export. Before: `E0204` pointing at the `Vec`. After: clean.
4. **P4 — serde round-trip.** The Manual Tester rates this CB-7's most likely real defect
   (`manual-test-plan-contracts-freeze.md` §1.4) and it is right: with five arms and two of them
   new, an untagged or flattened representation would let `Error(NotPrimary)` and
   `Replica(NotRequired)` collide on the wire, where M7F-30..35 read them. **Design commitment:
   `KernelIgnoredReason` and both new leaves take serde's default externally-tagged
   representation. No `#[serde(untagged)]`, no `#[serde(flatten)]`, no `#[serde(rename)]` on any
   arm or variant.** That is a statement the round-trip probe can falsify.

---

## 2. CB-8 — **WITHDRAWN**

### 2.1 The withdrawal

Round 1 proposed `support::ctx_with_control_time(control_time) -> StepCtx<'static>` in
`crates/rdb-sim/tests/support/mod.rs`. **It is withdrawn. No new API.**

F-6 is correct, and I re-derived it rather than accepting it. `ControlTime::compare` at
`crates/rdb-core/src/contracts/time.rs:138-155` is a `const fn` taking `self` plus four scalars:

```rust
pub const fn compare(self, now: Tick, max_sample_age_millis: u64, instant: Tick, margin_millis: u64) -> ClockVerdict {
    if !self.bound_established || self.is_stale(now, max_sample_age_millis) {
        return ClockVerdict::Uncertain;                      // :146
    }
    let slack = self.error_millis.saturating_add(margin_millis);
    if self.estimate.0.saturating_add(slack) < instant.0 {
        ClockVerdict::DefinitelyBefore                       // :150
    } else if self.estimate.0 > instant.0.saturating_add(slack) {
        ClockVerdict::DefinitelyAfter                        // :152 — unasserted
    } else {
        ClockVerdict::Uncertain                              // :154 — the overlap; unasserted
    }
}
```

It touches no `StepCtx`. Four of the six branches are already asserted by
`m7f_14_a_stale_sample_is_uncertain_even_when_the_bound_is_confident`,
`crates/rdb-core/tests/seams.rs:50-93`, by constructing `ControlTime` literals and calling
`compare` directly — `DefinitelyBefore` (`:61-65`), stale-by-`>` (`:68-74`), future-stamped sample
(`:76-81`), `bound_established: false` (`:83-91`).

**Round 1's grep said "no file under `crates/rdb-sim/tests` mentions `ClockVerdict` or
`bound_established`". That was true and it is the F-7 mistake in miniature: the conclusion drawn
from it — "six branches unreachable" — silently dropped the directory.** The assertions live in
`crates/rdb-core/tests`.

**Why the builder is worse than nothing, not merely unnecessary.** Two independent reasons, and
the second is the one that decides it:

1. It buys public API surface to reach pure arithmetic that a `ControlTime` literal already
   reaches.
2. **It would entrench the wrong idiom.** A builder lets a row pin a *constant* `ControlTime`. The
   thing kernel-a's clock rows need is a sample that **ages** as the scheduler advances (§4). If
   the builder lands first, rows pin frozen literals, that becomes the established spelling, and
   the dead injection path stays dead with a fixture standing in front of it. The critic's ordering
   argument (F-8, "decide CB-9 before implementing CB-8") is correct, and withdrawing CB-8 settles
   the ordering by removing one side of it.

### 2.2 What replaces it — the exact assertions

Appended to `m7f_14` in `crates/rdb-core/tests/seams.rs`, after the existing `unbounded` block at
`:83-91` and before the `tracing::info!` at `:92`. They reuse the row's existing bindings
(`sample`, `margin`), which are in scope:

```rust
    // `sample` is estimate 1_000, error_millis 10, bound_established true, sampled_at 1_000.
    // `margin` is 10, so `slack` is 20 and the overlap window is [980, 1_020] around 1_000.
    // Judged at tick 1_100 with a 500 ms maximum age: fresh, so the arithmetic half runs.

    // The `DefinitelyAfter` arm (time.rs:152), which no row in the workspace reached.
    // 1_000 > 979 + 20 — one millisecond past the boundary.
    assert_eq!(
        sample.compare(Tick(1_100), 500, Tick(979), margin),
        ClockVerdict::DefinitelyAfter
    );

    // The overlap `Uncertain` arm (time.rs:154), at the exact boundary the `>` at :151 decides.
    // 1_000 > 980 + 20 is FALSE because the comparison is strict; one millisecond earlier it is
    // true. This pair is what turns `>` into `>=` from a silent mutation into a red row.
    assert_eq!(
        sample.compare(Tick(1_100), 500, Tick(980), margin),
        ClockVerdict::Uncertain,
        "the estimate sits exactly on the far edge of the error bound; equality is overlap, \
         not proof (time.rs:151's strict `>`)"
    );
```

**Why these two values and not two arbitrary ones.** The Manual Tester planned a mutation and
predicted its outcome (`manual-test-plan-contracts-freeze.md` §2.1): change
`self.estimate.0 > instant.0.saturating_add(slack)` to `>=`. Prediction: **MISSED, green
workspace.** Against the pair above: at `Tick(979)` the mutant still answers `DefinitelyAfter`
(unchanged); at `Tick(980)` it answers `DefinitelyAfter` where the row demands `Uncertain` —
**red**. Arbitrary values inside the arms would not kill it. The boundary pair does.

**One line more closes the symmetric mutation, and I recommend it.** The lead's ruling names two
assertions; this is a third, and it is offered rather than assumed:

```rust
    // The near edge, deciding the strict `<` at time.rs:149 the same way.
    // 1_000 + 20 < 1_020 is FALSE; at 1_021 it is true and the answer is DefinitelyBefore.
    assert_eq!(
        sample.compare(Tick(1_100), 500, Tick(1_020), margin),
        ClockVerdict::Uncertain
    );
```

It kills `<` → `<=` at `:149`, which is otherwise as silent as the `>` mutation was. Cost: one
`assert_eq!` in a row that is being edited anyway.

**CB-8 is then closed.** `crates/rdb-sim/tests/support/mod.rs` is not touched, `contracts/time.rs`
is not touched, and no public surface is added anywhere.

### 2.3 The finding underneath CB-8 — nothing reads `StepCtx::control_time`

The lead asked for this to be verified and stated. **It holds.**

*Command and scope — the whole `crates/rdb-core/src` tree:* `grep -rn "fn step" crates/rdb-core/src --include=*.rs`.
Result: seven hits. One is the trait declaration, `contracts/event.rs:469`, which names `ctx`. The
other six are every `impl Module` in the workspace, and **all six take `_ctx`**:

| Module | Site | Signature |
|---|---|---|
| authority | `src/authority.rs:331` | `fn step(&mut self, _ctx: &StepCtx<'_>, event: &Event)` |
| protection | `src/protection.rs:35` | `fn step(&mut self, _ctx: &StepCtx<'_>, _event: &Event)` |
| publication | `src/publication.rs:35` | same |
| recovery | `src/recovery.rs:35` | same |
| replication | `src/replication.rs:35` | same |
| transaction | `src/transaction.rs:35` | same |

`authority.rs` is the one module that is **not** a stub — `impl Module` is real there
(`test-plan-m7-kernel-a.md` §15 row 16: `capability()` and `step()` returning effects, +302/−16 at
the last basis) — and it still underscores the context.

*Command and scope — the whole `crates/` tree:* `grep -rn "control_time" crates/` returns **four**
hits, and I opened all four: the field declaration (`rdb-core/src/contracts/event.rs:412`),
`Clock::control_time`'s definition (`rdb-sim/src/sim/clock.rs:91`), one mechanical copy
(`rdb-sim/src/harness/dispatch.rs:152`), one test-fixture literal
(`rdb-sim/tests/support/mod.rs:118`). **Zero reads by any kernel.** `ControlTime::compare` and
`is_stale` have zero production callers.

And kernel-a's design says A1 never will:
`teams/kernel-a/design.md:875-877` — "`ControlTime::compare` and `is_stale` stay where they are
and are used by whoever wants that comparison; **A1 does not call them**."

**This sharpens Ruling 3 exactly as the lead expected.** Round 1's hand-test #2 — "step a kernel
through `bound_established: false` and observe `Uncertain`" — is unrunnable, and a row written
against it would produce byte-identical effects on both sides of the boundary. That is defect shape
(a-variant) *inside the design's own proposed test*. The builder would have been a fixture for a
field nothing reads.

**Scope of the claim, attached:** true at `395d535`, and true of kernel-a's design as of the
revision this plan cites. It is not a claim about M8, and it is not a claim that the field should
be deleted — §4.6 says why it should not.

---

## 3. kernel-a ask 9 — unchanged, with its scope now attached

Round 1's conclusion stands and the lead has withdrawn the contrary premise in his own brief.
Restated with the sources re-opened at `395d535`:

Ask 9 is **not** a reshape of `ControlTime`. `teams/kernel-a/design.md:842-848` assigns the
`ControlTime` → `ClockSample` conversion to **I1, the replay runner in `rdb-sim`**, and `:875-877`
says `compare`/`is_stale` stay put. `docs/testing/test-plan-m7-kernel-a.md` §15 drift row 7 and
Q-12 (`:1078`) say the same thing from the plan side, restated against the landed four fields:
`at = ct.sampled_at`, `utc_ms` from `ct.estimate`, `epsilon_ms = ct.error_millis`,
`valid = ct.bound_established`, and nothing else.

`contracts/time.rs` is untouched by this freeze. The Manual Tester's N1 is the guard:
`git diff <basis>..HEAD -- crates/rdb-core/src/contracts/time.rs` must be empty after all changes
land.

**Scope attached, per the Manual Tester's §3.4 correction, which I accept.** This claim is proved
*under kernel-a's design as it reads at `395d535`*. If kernel-a later decides A1 should call
`ControlTime::compare` after all, nothing in this freeze turns red. Write it down with its scope,
and re-run N1 at each milestone rather than once.

**Re-checked against CB-9, as the brief asked.** CB-9 changes what fills `StepCtx.control_time`; it
changes nothing about what I1 does with a `ControlTime` once it has one. The conversion is still
four field copies. **But CB-9 changes where I1 gets the `ControlTime` from**, and that is an
unstated requirement on I1 that neither the ask nor round 1 named — §4.6 states it.

---

## 4. CB-9 — the simulator's clock is not wired into the simulator

The lead's mid-round correction is right on every link and I re-derived all of them. Splitting it
the way the Manual Tester did (CB-9a / CB-9b) is the right split and I adopt it.

### 4.1 The four links, each verified at `395d535`

| Link | Claim | Command and scope | Result |
|---|---|---|---|
| 0 | Nothing in any `src/` composes a `Clock` | `grep -rn "Clock::new" crates/` — whole tree | 11 hits. **Ten are `config-engine`'s unrelated `ManualClock`.** The only `rdb_sim::sim::clock::Clock::new` is `crates/rdb-sim/tests/dispatch.rs:619`, a test |
| 0b | No `src/` file names the type | `grep -rn "Clock" crates/*/src --include=*.rs` — every crate's `src/` | Outside `rdb-sim/src/sim/clock.rs` itself: only `config-engine`'s `LeaderClock`/`SystemClock`/`ManualClock`, `config-server`'s `ClockUnavailable`, and `rdb-core`'s `ClockVerdict`/`ClockUnbounded`/`ClockSampleStale`/`ClockError`. **Nothing refers to `rdb_sim`'s `Clock`** |
| 1 | `set_skew` has no caller | `grep -rn "set_skew" crates/` — whole tree | 4 hits, **all inside `clock.rs`**: the definition at `:111` and doc references at `:13`, `:74`, `:86` |
| 2 | `control_time` has no caller | `grep -rn "control_time" crates/` — whole tree | 4 hits, none a call: `event.rs:412` (field), `clock.rs:91` (definition), `dispatch.rs:152` (copy), `support/mod.rs:118` (literal) |
| 3 | `ctx_for` copies the caller's literal through | read `crates/rdb-sim/src/harness/dispatch.rs:148-161` | `StepCtx { now: base.now, control_time: base.control_time, … }` — verbatim |
| 4 | No kernel reads the field | §2.3 | six `_ctx` |

I also read `Dispatcher`'s fields at `crates/rdb-sim/src/harness/dispatch.rs:74-88`: six modules,
`adopted`, `boots`, `replies`. **No clock.** And `crates/rdb-sim/src/lib.rs:22` claims module
`sim` (package H1) owns "scheduler, **clock and timers**, network, fake control store, cluster".
The crate declares a component it never composes.

**Verifying the ~14-row list myself, as instructed.** `docs/testing/test-plan-m7-kernel-a.md:1286`
(§15 drift row 7) names **M7A-38..M7A-46, M7A-143, M7A-146, M7A-148, M7A-165**. `:1078` (Q-12)
names the identical list. M7A-38..46 is nine rows, plus four = **thirteen rows**, not fourteen.
`:1288` pins two of them to sample *age*: **M7A-46 at exactly 2000 (not stale) and M7A-43 at 2001
(stale)**, and §15 drift row 9 confirms the landed `>` is strict, so that boundary is the row's
whole content. The count is thirteen; the lead's and the critic's "~14" is one over. Scope: two
lines of one file.

### 4.2 CB-9a — reachable today, no design needed

`Clock` is `pub` in a `pub mod`; `new`, `set_skew` and `control_time` are all `pub`;
`dispatch.rs:619` proves the type is constructible from outside the crate. Every assertion the
Manual Tester lists in §2b.2 is writable today with no new entry point. **CB-9a needs no design
and no decision. It needs a row.**

The one that matters most is the Manual Tester's planned mutation: flip
`bound_established: skew.bound_established` to a literal `true` at `clock.rs:104`. Predicted
**MISSED, whole workspace green**. A CB-9a row makes it red. That converts the caller-count
observation into a proven gap.

### 4.3 CB-9b — the designed path

**The composition.** `Dispatcher` gains a `Clock`, and `ctx_for` asks it instead of copying.

```rust
// crates/rdb-sim/src/harness/dispatch.rs

use crate::sim::clock::Clock;

#[derive(Debug, Default)]
pub struct Dispatcher {
    authority: Authority,
    /* … the other five modules, adopted, boots, replies … */
    /// The authority-clock estimate every `StepCtx` this dispatcher builds is filled from.
    ///
    /// Held here rather than passed per step because the sample must be able to **age**: the
    /// gap between `Clock::now` and the scheduler's tick is what `ControlTime::is_stale`
    /// measures, and a per-step argument would let a caller pass a fresh literal and erase it.
    clock: Clock,
}

impl Dispatcher {
    /// The clock this dispatcher samples. A scenario drives skew through `clock_mut`.
    #[must_use]
    pub const fn clock(&self) -> &Clock { &self.clock }

    /// Mutable access, for `Clock::set_skew` and `Clock::advance`.
    pub fn clock_mut(&mut self) -> &mut Clock { &mut self.clock }

    #[must_use]
    pub fn ctx_for<'a>(&self, base: &StepCtx<'a>) -> StepCtx<'a> {
        let adopted = self.adopted(base.node, base.partition);
        StepCtx {
            now: base.now,
            // CB-9: sourced from the environment's own clock, not copied from the caller.
            // `base.control_time` is ignored; `ctx_for`'s doc is corrected to say so.
            control_time: self.clock.control_time(base.node),
            node: base.node,
            /* … unchanged … */
        }
    }
}
```

**Call-site cost: zero.** `Dispatcher` derives `Default` (`dispatch.rs:74`) and `Clock` has a
`Default` impl (`clock.rs:44-48`, `Self::new(100)`), so `#[derive(Default)]` still compiles and
`Dispatcher::new()` keeps its signature. All **seven** `Dispatcher::new()` call sites
(`grep -rn "Dispatcher::new" crates/` — whole tree: `campaign.rs:248`, `dispatch.rs:96/344/411/487`,
`harness.rs:45/63`) are untouched. `Dispatcher` also derives `Debug`, and `Clock` derives `Debug`
(`clock.rs:35`), so that holds too.

**Test cost: I believe zero, and I state the limit of that belief.** The four direct `ctx_for`
callers are `crates/rdb-sim/tests/dispatch.rs:102, 118, 136, 155`; I opened all four and each
asserts only `.generation` or `.owner_epoch`. None reads `control_time`. Both `Dispatcher::step`
callers (`authority.rs:105`, `harness.rs:67`) pass `support::ctx()` and assert on returned effects.
So no landed assertion changes value. **I did not compile this** — the brief forbids `cargo` — so
this is a reading of six call sites, not a build result.

### 4.4 The half CB-9b does **not** fix, and it is the half the thirteen rows need

This is the finding that makes the sizing honest, and it is not in the lead's brief, the critic's
report or the Manual Tester's plan.

`Clock::control_time` stamps the sample at the clock's own current tick —
`crates/rdb-sim/src/sim/clock.rs:101-106`:

```rust
ControlTime { estimate, error_millis: self.error_millis, bound_established: skew.bound_established, sampled_at: self.now }
```

`ControlTime::is_stale(now, max)` is `sampled_at > now || now - sampled_at > max`
(`time.rs:123-125`). So a sample's **age is `ctx.now − clock.now()`**.

Now read the module's own doc against that. `clock.rs:3-5`:

> Time moves only when the scheduler takes an event and the harness tells the clock so through
> `Clock::advance`.

and `clock.rs:87-89`:

> `sampled_at` is the current tick, so a caller that holds the sample and asks later is what
> `ControlTime::is_stale` exists for.

> **Round-3 correction, and it inverts this subsection's conclusion (R2-3).** What follows was
> argued **conditionally** and then restated in §4.5 and §4.6 as an impossibility. It is not one.
> **CB-9b alone makes M7A-43 and M7A-46 reachable, with no line of I1.** The corrected reading is
> below the rule; the original is kept above it because a withdrawn claim stated is worth more
> than one quietly deleted.

**These two cannot both govern a per-step fill *under a lockstep harness*.** If the harness
advances the clock in lockstep with the scheduler and samples on every step, then
`clock.now() == ctx.now` always, age is always **zero**, and `is_stale` can never be true — so
M7A-43 (age 2001) would stay unreachable *after* CB-9b lands, and M7A-46 (age 2000, not stale)
would pass for the wrong reason. **The condition is "under a lockstep harness", and there is no
harness: `crates/rdb-sim/src/harness/replay.rs:12` says "Signatures only; package I1 lands
replay".** An impossibility proved under a condition that does not hold is not an impossibility.

---

**What is actually true: the composition leaves both terms free, and a row owns both.**

`Clock::control_time` does **not** sample at the tick it is asked. Its signature is
`pub fn control_time(&self, node: NodeId) -> ControlTime` (`clock.rs:91`) — **it takes no tick.**
It stamps `sampled_at: self.now` (`clock.rs:105`), the clock's own tick, moved only by
`Clock::advance` (`clock.rs:76-81`). The judging tick is a *separate* value: `ctx.now` is
`base.now` (`dispatch.rs:151`), which the caller owns, and every `StepCtx` field is `pub`
(`event.rs:408-430`). So after CB-9b's twelve lines a row does this and nothing else:

- `Dispatcher::default()` → `Clock::default()` → `Clock::new(100)` → `now = Tick::ZERO`
  (`clock.rs:44-48`, `:54-61`).
- Build `base` with `now: Tick(2_001)`, a `StepCtx` literal.
- `dispatcher.ctx_for(&base)` → `sampled_at = Tick(0)`, `ctx.now = Tick(2_001)`.
- `is_stale(Tick(2_001), 2_000)` → `2_001 - 0 > 2_000` → **true**. That is **M7A-43**.
- `base.now = Tick(2_000)` → `2_000 - 0 > 2_000` → **false**. That is **M7A-46**.

`Clock::advance` need not be called at all, and its monotonicity guard (`clock.rs:76-78`, refuses a
backward `now`) is no obstacle: the row moves `base.now` forwards, never the clock backwards.

**The `clock.rs:3-5` / `:87-89` contradiction is withdrawn.** `:3-5` says time moves only through
`Clock::advance`, called by the harness with the scheduler's tick. `:87-89` says a caller that
holds a sample and asks later is what `is_stale` exists for. These are consistent: the second
describes a caller holding a sample **across** advances, which the first permits. The
incompatibility appears only under this design's *own* proposed per-step re-fill, and then only
under lockstep. **Round 2 attributed to shipped source an inconsistency created by its own
proposal — and did so without applying Rule 2's "cite the comment and say whether you agree or
overrule it".** I agree with both comments; neither is overruled.

**And the finding was not new. It was in a file this document cites three times.** §4.4 opened
"it is not in the lead's brief, the critic's report or the Manual Tester's plan". It is in
`docs/testing/test-plan-m7-kernel-a.md:1286`, §15 drift row 7 — the row cited in §3, §4.1 and §4.5
for the thirteen-row list — in the same table cell: "The `at = ct.sampled_at` clause is the
load-bearing one: **a seam that stamped the sample at its delivery tick would make every sample
age zero and M7A-43's stale sample unreachable.**" Kernel-a had already stated the mechanism and
already ruled the fix. **That sentence is withdrawn**, and this is Rule 3's sub-species again: the
cell was read for its row list and not for its prose.

Structurally, `Clock::advance` is already a *sampling*-clock operation, not a scheduler operation.
I read every use of the `now` field: `now()` (`:65`) and `control_time()` (`:105`) are the only
readers. `due(&mut self, now: Tick)` (`:170`) takes the tick as a **parameter** and never consults
`self.now`; `next_deadline` (`:160`) reads the timer table; `arm` (`:129`) reads its `at` argument.
**So `Clock::advance` moves nothing but the sample time.** The lockstep sentence at `:3-5`
describes the timer half of the module and has been read as governing the sample half.

**Consequence.** CB-9b's composition is necessary and is not sufficient. The remaining decision —
*on what cadence is the authority clock re-sampled, and who calls `advance`* — is a step-loop
policy, and there is no step loop: `crates/rdb-sim/src/harness/replay.rs:12` says "Signatures only;
**package I1 lands replay**", and `lib.rs:24` assigns the whole `harness` module to I1.

### 4.5 Sizing — the number, and where I stop

Measured as files touched and call sites broken, at `395d535`:

| Change | Files edited | Call sites broken | Behaviour |
|---|---|---|---|
| CB-7 | 4 source (`contracts.rs`, `contracts/event.rs`, `contracts/authority.rs`, new `contracts/ignore.rs`) + `lib.rs` re-export + **2 test files, 6 edit sites (2 imports, 3 literals including one expected value, 1 deref)** — §1.7, corrected from "3 literals, 1 `*reason`" | 0 (additive; all six are mechanical) | none |
| G-13 | 4 source (`snapshot.rs`, `backup.rs`, `main.rs`, `rocks.rs` doc) + **4 landed green test files** | **5** — §5.8 | yes |
| CB-8 | 1 test file, 3 `assert_eq!` | 0 | none |
| **CB-9b (composition)** | **1 source file** (`harness/dispatch.rs`): 1 `use`, 1 field, 2 accessors, 1 changed line, 1 corrected doc paragraph — **~12 added lines** | **0** | yes, and only for a field no kernel reads |
| **CB-9b (sampling cadence)** | package **I1**, which does not exist | — | — |

**CB-9b's composition is the smallest of the four changes, not the largest.** It is smaller than
CB-7 and materially smaller than G-13. **The brief's stop condition — "if CB-9b sizes larger than
CB-7 and G-13 combined" — is not triggered, and by a wide margin.**

**But small is not the same as ready, and the lead's fourth correction is right.** The lead ran the
narrower search I did not:

```
grep -rn "\.control_time" crates/ --include=*.rs     # one hit: dispatch.rs:152, the pass-through
grep -rn "\.compare(\|\.is_stale(" crates/ --include=*.rs   # zero production callers
```

So the whole apparatus — the `StepCtx` field, `compare`, `is_stale`, `ClockVerdict`, `set_skew` —
has **no production consumer at all**. `seams.rs` is the only thing holding it up. Wiring `ctx_for`
to a real `Clock` would build a delivery path to an empty room: a correct value would arrive, and
nothing would read it. **A test that perturbs skew and asserts an effect would then pass
identically with the clock wired and unwired — defect shape (a-variant), in the row written to
prove the wiring.**

**So CB-9b is a joint item with a named dependency, not a foundation deliverable that stands
alone.** Three parts, two owners:

| Part | Owner | Ready? |
|---|---|---|
| **CB-9a** — unit skew against `Clock` directly | **foundation** | **yes, today.** No dependency on anything below. §4.2 |
| **A consumer** — an authority gate that reads `ctx.control_time` and denies on `ClockVerdict::Uncertain` | **kernel-a** | not started. This is what `crates/rdb-core/src/authority.rs:3` and ADR-rdb-0008 say the gates are for, and `DenyReason::ClockUnbounded` / `ClockSampleStale` (`contracts/authority.rs:65`, `:68`) are the names it would produce |
| **CB-9b** — the structural wiring | **foundation**, ~12 lines | ready to build; **value lands only when the consumer exists** |

**My recommendation, as a stated choice rather than an assumption: foundation builds CB-9b now
anyway, and does not wait for kernel-a.** Three reasons, and the third is the deciding one:

1. **It is 12 lines with zero call-site breakage** (§4.3). The cost of building it early is close
   to the cost of scheduling a conversation about building it later.
2. **It removes a wrong default that kernel-a would otherwise inherit.** Today the only way to get
   a `StepCtx` is `support::ctx()`, which hardcodes a constant `ControlTime`
   (`crates/rdb-sim/tests/support/mod.rs:118-123`). If kernel-a starts writing rows before the
   wiring exists, the frozen literal *is* the idiom, and every clock row is written against it.
   That is exactly the (d-strong) trap at scale, and it is much cheaper to prevent than to
   retrofit across ~189 rows.
3. **It converts a design question into a compile-time fact for kernel-a.** With `Dispatcher`
   owning the clock, `base.control_time` is ignored **on the `ctx_for` path** and a row that wants
   skew has one way to get it there. Kernel-a then designs its gate against a real sample instead
   of against a decision that has not been made.

   **Scope correction (R2-12): "one way" is true of `ctx_for`, not of the crate.** `ctx_for` is
   the only place CB-9b changes. `support::ctx()` (`crates/rdb-sim/tests/support/mod.rs:115-131`)
   still returns a frozen literal — `estimate: Tick::ZERO, error_millis: 0,
   bound_established: true, sampled_at: Tick::ZERO` — and a row may call
   `Module::step(&ctx, &event)` directly with it, which is what the two `Dispatcher::step`
   callers' sibling paths do today. **So after CB-9b there are two spellings and the frozen one is
   still the shorter**, and the (d-strong) trap argument 2 describes is *reduced*, not closed.
   Cheap fix, and I recommend it as part of CB-9b rather than after: `support::ctx()`'s doc says
   the sample is a placeholder and points at `Dispatcher::clock_mut`. Exposure is `rdb-sim`'s own
   test authors, who are exactly the audience the idiom argument is about.

**What I am not recommending: building the consumer.** That is kernel-a's gate, kernel-a's
`DenyReason` choice, and kernel-a's `effective_epsilon` policy. Foundation designing it would
repeat the error `event.rs:207-210` refuses for `SetAdmission` and `Recovered`.

**I do invoke the stop condition on the sampling cadence.** It is not a foundation change at all;
it is the first policy decision of package I1, and designing it here would be foundation deciding
I1's shape — the same error `event.rs:207-210` refuses for `SetAdmission` and `Recovered`.
**Sizing and stop, as the brief asks for a number:** thirteen kernel-a rows (M7A-38..46, 143, 146,
148, 165) depend on the clock being real. ~~Two of them (M7A-43, M7A-46) are pinned to an age
boundary that **no composition of existing parts can produce**, because every existing part samples
at the tick it is asked.~~ **WITHDRAWN (R2-3).** `Clock::control_time` takes no tick and stamps
`self.now`; `ctx.now` is the caller's `base.now`; both terms are free after CB-9b, so **all
thirteen rows are unblocked by the composition**, not eleven. The sentence restated the conditional
proof of §4.4 as an impossibility, which is the mistake this document is organised around. The I1
question that remains is not a gate and is smaller than it was: **do not remove the freedom** —
§4.6's R-CB9.

**So CB-9 has *one* dependency outside foundation, not two.** Round 2 said two; the second was
R2-3's error. The lead's original concern is right and is still the reason CB-9 matters: if
kernel-a writes ~189 authority rows under clock uncertainty while the clock cannot be perturbed and
the field is never read, those rows assert against a frozen literal and prove nothing.

| Dependency | Owner | Without it | Gate? |
|---|---|---|---|
| A gate that reads `ctx.control_time` | **kernel-a** | the wiring delivers a correct value nothing consumes; rows pass identically wired and unwired | **yes** — this is the real one |
| ~~A sampling cadence under which `ctx.now − clock.now()` can exceed `max_sample_age_millis`~~ | ~~I1~~ | **WITHDRAWN.** `Dispatcher::default()` puts the clock at `Tick::ZERO` and the row owns `base.now`, so the age is free with no cadence at all | **no** — an I1 obligation not to *remove* the freedom (§4.6), which is a note, not a gate |

**Consequence for the lead's question 3.** It asked whether "a 12-line change that cannot unblock
two of its thirteen rows needs saying plainly". It can unblock them. Holding CB-9b on an I1 cadence
decision that does not exist would stall two rows and one design conversation for nothing, and
would leave in place the frozen-literal idiom that §4.5's own argument 2 calls the expensive thing
to retrofit. **The recommendation to build CB-9b now is unchanged and is now stronger, on a
premise one item shorter.**

**Third-check, so this correction does not become the next over-claim.** Do M7A-43/46 become
*writable* today? No — they need (i) a stale sample delivered and (ii) A1 reading it. CB-9b
satisfies (i) on its own; (ii) is kernel-a's and is the one gate above. The corrected claim is
about **reachability of the input**, not about the rows going green.

**This has to be decided before kernel-a starts, not discovered afterwards** — which is the lead's
point, and it is the one scheduling claim in this document that I would defend hardest.

### 4.6 What this makes required of I1, recorded rather than assumed

The critic's strongest counter to F-8 was that `ctx_for` passing `control_time` through may be
deliberate, with I1 as the intended caller — and that it "cannot rule it out". CB-9b's composition
answers it in the direction of wiring, and creates one requirement that must be written down or it
will be rediscovered:

> **R-CB9.** The environment, not a test literal, is the source of `StepCtx.control_time`. Once
> `Dispatcher` owns the clock, a caller's `base.control_time` is ignored, so a row perturbs time
> with `Clock::set_skew` and ages a sample by advancing the scheduler past `Clock::now`. I1 owns
> the re-sampling cadence. **The freedom exists by default** — `Dispatcher::default()` leaves the
> clock at `Tick::ZERO` while a row chooses `base.now` — so the obligation is negative:
> **I1 must not advance the clock to `ctx.now` before every step.** If it does, every sample ages
> zero and M7A-43 becomes unreachable again. A scenario can always put the clock back behind the
> scheduler through `clock_mut()`, so even a lockstep I1 is recoverable per-row; the obligation is
> about the default, not about possibility.
>
> *Corrected in round 3.* R-CB9 previously read as a **precondition on I1** — "whatever cadence it
> picks, it must be one under which `ctx.now − clock.now()` can exceed `max_sample_age_millis`, or
> M7A-43 is unreachable". That inverted it: the inequality holds by construction and I1 can only
> destroy it. A requirement written as "you must create X" when X already exists is a requirement
> that makes work.

And the answer to the Manual Tester's D6, which asked for a written decision:

> **D6 answered.** No kernel reads `StepCtx::control_time` today and kernel-a's design says A1
> never will (§2.3). The lead's narrower searches make it stronger than "no kernel":
> `grep -rn "\.control_time" crates/ --include=*.rs` returns **one hit in the whole workspace**,
> `dispatch.rs:152`, the pass-through itself; `grep -rn "\.compare(\|\.is_stale(" crates/
> --include=*.rs` returns **zero production callers**, every hit being `seams.rs` plus the one
> internal call at `contracts/time.rs:145`. **The entire `ControlTime` apparatus is a contract with
> no user.** **The field is not deleted, and the reason is not "someone might need it".**
> It is the only channel by which spec §7.2's bounded clock reaches a kernel at all, and
> kernel-a's own fence vocabulary already names two consumers of it — `DenyReason::ClockUnbounded`
> and `DenyReason::ClockSampleStale`, landed at `contracts/authority.rs:65` and `:68`. A1 decides
> the same question in its own `effective_epsilon`/`utc_ok`; it still needs the sample to decide
> it from. CB-9b makes that sample real instead of constant.

---

## 5. G-13 — a restore and the durable policy-version floor

### 5.1 The charter contradiction — and why this section stops short of settling it

**G-13 is not an open design question. It is chartered by an Accepted ADR, and the shipped code
contradicts the charter.** Round 1 did not notice, and neither did round 1's critic.

**The charter.** `docs/ADRs/0027-signed-policy-documents-and-rbac.md:477-482`, status **Accepted**:

> **Known limit, carried rather than closed: a restore resets the floor (G-13).** `state_meta` is
> not carried in a snapshot body, so a directory restored from a backup starts at floor `0` and an
> old signed document adopts. G-09 ships with that bypass. It is recorded as a separate gap, with
> the fix being to seed the floor from the backup manifest's `policy_version_ref` at restore …

**A third statement of the charter, which the lead's message did not list and which I found while
checking it.** `crates/config-storage/src/rocks.rs:1322-1324` (HEAD, via
`git show HEAD:… | grep -n`, because that file is dirty in the working tree):

> Closing it needs the restore to seed this cell from the backup manifest's `policy_version_ref`,
> which is tracked separately and is not what this cell does today.

**The contradiction.** Two shipped doc comments, both deliberate, both saying the opposite.
`crates/config-server/src/backup.rs:96-100`:

> A **reference**, never a copy … **Nothing checks it at restore, because the independently
> supplied policy may legitimately be older, newer or unrelated**; it exists so a recovering
> operator can tell which document the data was authorized under.

`crates/config-server/src/cli.rs:202-210`:

> A number rather than a policy file, because **the manifest's reference is a breadcrumb for a
> human and not a validation input** … **Silence here is the absence of a check, not a statement
> that the versions agree.**

So the score is **two statements for "gate"** (ADR:480, rocks.rs:1322) and **two for "evidence"**
(backup.rs:98, cli.rs:203). They cannot all be right, and round 1's design silently picked a side.

**What I am doing about it.** The lead ruled that the design must name the contradiction and take a
position, and that amending an Accepted ADR on a security control is a product-owner decision.
**Position: "evidence" is right; the ADR's G-13 clause and the `rocks.rs` sentence that repeats it
are both wrong and should be amended.** The argument is §5.2–§5.3. **Because that requires amending
an Accepted ADR, I stop there and hand off with the recommendation rather than designing the
amended G-13 as if the ruling had been made.** §5.4 states both shapes so the decision is one
choice, not a new round.

### 5.2 Why "evidence" wins — four arguments, strongest last

**Argument 1 — the two doc comments give a reason; the ADR clause gives an intention.**
`backup.rs:98` does not merely assert that nothing checks the reference. It says *why*: "the
independently supplied policy may legitimately be older, newer or **unrelated**." That is the exact
mechanism the Manual Tester rediscovered by experiment in §4.4 of its plan, written down in the
shipped source before the experiment was run. The ADR clause is a subordinate phrase — "with the
fix being to …" — inside a paragraph whose own heading is "carried rather than **closed**".

**Argument 2 — by the ADR's own standard of argument, the G-13 clause is the least worked sentence
in its section.** I read the surrounding limits at `ADR-0027:455-500`. The `(version, hash)` window
gets a stated exact fix, **two grounds** for not taking it, and a deferral argument about migration
cost. The unreadable-floor exposure gets an alternative considered, **two reasons** for rejecting
it, and a distinction between independent and correlated failure modes. The `Ephemeral` case gets
its own paragraph. G-13's fix gets **one clause and no consequences section** — no discussion of
what the restored node compares against, no alternative, no worked case.

**Argument 3 — the same ADR, fifty lines earlier, builds the thing that breaks the clause.**
`ADR-0027:407-418` is G-06: `PolicyDocument` gains `cluster_id`, and `verify_policy` refuses a
document naming a different cluster with `cluster_mismatch`. `:426-427` states the direction of
travel: "Re-issuing every document with a `cluster_id` is owed before the unscoped path can be
removed." **G-06 and G-13 are in the same document, and the G-13 clause does not reason about
G-06.** Under G-06's intended end state — every document scoped — a restored node's documents are
in a different lineage from the manifest's reference *by construction*.

**Argument 4, the decisive one — the restore path structurally guarantees the lineage change, by an
explicit refusal.** `crates/config-server/src/backup.rs:880-889`:

```rust
if manifest.cluster_id == cluster_id.to_string() {
    return Err(BackupError::refused(
        "cluster_id_reused",
        format!("--cluster-id {} is the cluster this backup was taken from; a restore must \
                 mint a new one, because reusing it is what leaves two writable authorities \
                 for one logical service", manifest.cluster_id),
    ));
}
```

**A restore that reuses the source cluster id is refused.** So on this code path the restored
cluster's identity is *never* the backup's, and `manifest.policy_version_ref` is therefore *always*
a number from a lineage no scoped document the restored node can adopt belongs to. The ADR's
proposed gate would compare a value that the same codebase guarantees is foreign. That is not a
bug in the proposal's implementation; there is no implementation of it that is not comparing
incomparable values.

**This also answers the lead's question 4** — *does the answer change if the restored identity
matches the backup's?* It would be the honest boundary for a gate, and it is the case I went
looking for. **It cannot occur.** `backup.rs:880` refuses it by name, with a stated reason that has
nothing to do with policy (two writable authorities for one logical service). There is no
same-identity restore for the gate to be scoped to.

### 5.2b The lead's withdrawn ruling, and the shape it belongs to

The lead's ruling was: *"Restore therefore inherits break-glass for free"* — an operator restoring
to force an older document needs `--break-glass-policy-rollback`, **the same flag** they would need
to force the identical downgrade on a plain restart.

**Both facts in it are true. The inference is not.** Verified at `395d535`:

- `crates/config-server/src/policy.rs:158-160` — `PolicyLoader::new` seeds the floor on every boot.
  **True.**
- `crates/config-core/src/policy.rs:767-772` — `below_floor = floor > 0 && to < floor`, refused
  unless `break_glass`; and `break_glass: below_floor` on the `Adopted` result at `:788`. **True.**

The inference fails because **a restore mints a new cluster, and policy documents are
cluster-bound**. Chain, every link opened:

1. `crates/config-storage/src/snapshot.rs:1163-1168` — `restore_into_fresh_store(data_dir,
   new_identity: &ClusterIdentity, snapshot, restored_from)`. The doc at `:1156-1158` states the
   intent: `identity` is "the new one … which is exactly what lets `--form` treat it as the genesis
   member of **the new cluster**".
2. `crates/config-server/src/policy.rs:230-235` — `config_core::verify_policy(&document,
   &signature, &self.cfg.trust_keys, self.expected_cluster)`.
3. `crates/config-core/src/policy.rs:453-459` — `if let Some(document_cluster) = document.cluster_id
   { if document_cluster != expected_cluster { return Err(PolicyRejected::ClusterMismatch { .. }) } }`.
4. So the old cluster's **scoped** documents do not verify against the new cluster. The operator
   must issue a new document scoped to the new cluster — **a new lineage**, whose version number it
   chooses independently, naturally `1`.
5. The floor was seeded from the **old** cluster's `policy_version_ref`, say 412.
   `below_floor = 412 > 0 && 1 < 412` → `PolicyRejected::RollbackFloor { floor: 412, incoming: 1 }`.
6. `crates/config-server/src/run.rs:1136-1138` — a failed startup reload sets
   `AuthzKind::NoValidPolicy`.
7. `crates/config-core/src/policy.rs:960-963` — `SignedPolicyAuthorizer::authorize` returns
   `Decision::deny(REASON_NO_VALID_POLICY)` when there is no active document. **Deny-all.**
8. ~~The in-band repair route does not exist.~~ **WITHDRAWN in round 3, and this is my error, not
   the critic's.** I read `crates/config-server/src/run.rs:467-470` — the `ReloadPolicy` RPC's own
   doc, "The admin plane has already checked the caller against the **currently active**
   document's `admins`" — concluded that with no active document there is no admins list, and then
   generalised one closed route to "the route does not exist". That is F-7 again, in my own §5.2b,
   in a chain I said I had opened every link of. **The lead disproved it by running it**: on a real
   daemon on a real restored directory under a second cluster identity, writing a document at a
   higher version onto the live deny-all node is adopted by the policy poller within one tick — no
   restart, no break-glass flag, no admin RPC. `PolicyLoader::spawn_poller`
   (`crates/config-server/src/policy.rs:466-493`) re-reads the files every `authz.poll_interval`
   and calls `reload("poll")` regardless of the node's authz state, and
   `m6_27_policy_arrival_restores_readiness_without_restart`
   (`crates/config-server/tests/m6_rbac.rs:124`) already proves that shape for a node holding no
   document. **The poller is the primary reload route and it is never closed; the admin RPC is the
   expedited path, not the only one.** I have not re-run the scenario (the brief forbids `cargo`);
   I am relying on the lead's run and on the two cited sources, and I name that as second-hand.

   **What this does and does not change.** The severity of the chartered fix's failure drops from
   "the cluster is bricked" to "the operator is not told which number to issue" — a maze with the
   exit unmarked rather than a wall. **The conclusion of §5.2 does not move at all**, because it
   never rested on severity: it rests on `backup.rs:880` making every such comparison meaningless
   by enforcement, and on G-06 already ruling out what G-13 asks for. Both are code. **That a
   withdrawn premise leaves the conclusion standing is not luck** — it is what argument 4 being
   "the decisive one" was supposed to mean, and this is the test of it.

**The deeper defect the lead named, and I agree with it:** `below_floor` compares two integers from
two independent numbering lineages. Cluster A's 412 and cluster B's 413 have no ordering
relationship. A restore that happens to pass the gate passes by luck. **Seeding a floor across a
cluster-identity change is comparing incomparable values**, and the number that comes out is not
evidence of anything.

This is F-7's sixth instance, and it is the lead's: an equivalence proved under same-lineage
version comparison, applied across a cluster-identity change. It is recorded here rather than
dropped because a withdrawn ruling stated is worth more than one quietly abandoned.

### 5.3 What is actually left of G-13's threat — and it is narrower than the known-limit doc says

Before choosing a replacement I asked what the floor protects against **after** a restore, since
step 3 above removes most of it. The answer changes the change.

The known limit, `crates/config-storage/src/rocks.rs:1312-1320`, states the threat:

> a directory restored from a backup starts here at `0`, and the downgrade this cell exists to
> refuse succeeds once across that restore: an operator who restores an old backup and supplies an
> old signed document gets it adopted with no refusal.

**That threat is already closed for scoped documents, by a control that is not the floor.** An old
document scoped to the source cluster is refused by `verify_policy` step 7 (`policy.rs:453-459`)
before the floor is ever consulted. The floor's value is irrelevant to it.

**It survives for exactly one case: unscoped documents.** `cluster_id` is
`Option<ClusterId>`, `#[serde(default)]` (`crates/config-core/src/policy.rs:127-128`), and `None`
is accepted — `verify_policy`'s doc at `:412-415`: "A document carrying no cluster at all predates
the field and is accepted." The doc at `:105-117` explains it: `None` means legacy-unscoped.

So the surviving hole is: **an old, validly signed, unscoped document replayed into a restored
directory.** And note what is true of it that is not true of the cross-lineage case — an unscoped
document is by construction *not* bound to a cluster, so its version numbers **are** comparable
across a restore. Where the floor still has a job, the lineages are shared; where the lineages are
unrelated, the floor has no job. That is not a coincidence, and it is the load-bearing observation
of this section.

**Width of the hole:** one adoption. On the first successful adoption the no-active-document branch
does `self.floor.store(to, …)` (`config-core/src/policy.rs:781`) and
`crates/config-server/src/policy.rs:348` writes it durably via `set_policy_version_floor`. So
`rocks.rs:1319`'s "**succeeds once** across that restore" is exact.

**And G-09's own landed scope is same-lineage by construction, which is worth stating because it is
where the floor's evidence actually comes from.** `crates/config-core/tests/m6_rbac.rs:482` heads
its section "Gap G-09 — the version floor outlives the **process**", and every row under it is a
**restart**: `g09_a_seeded_floor_refuses_an_older_document_at_the_first_adoption` (`:498`),
`g09_a_restart_against_an_equal_floor_is_an_ordinary_load` (`:554`),
`g09_an_unseeded_floor_changes_nothing` (`:585`),
`g09_break_glass_crosses_the_floor_and_resets_it` (`:606`), and the durable half at
`crates/config-server/src/policy.rs:1014`,
`a_restart_refuses_a_document_below_the_persisted_floor`. Same node, same cluster, same lineage —
which is exactly where a version comparison has meaning. **G-13 proposes to carry that control
across a cluster-identity boundary that `backup.rs:880` makes mandatory**, and none of G-09's
evidence speaks to that. This is Rule 1 again, one level up: the floor's proof of correctness has a
scope, and G-13 extends it past that scope without new evidence.

*Scope of this whole subsection:* six files read at `395d535` — `config-core/src/policy.rs`,
`config-server/src/policy.rs`, `config-server/src/run.rs`, `config-server/src/backup.rs`,
`config-core/tests/m6_rbac.rs`, and `config-storage/src/rocks.rs` **via `git show HEAD:` because
that file is dirty**. It is a claim about the code paths a restored node's first boot takes, not
about operator practice.

### 5.4 What G-13 delivers under each ruling — the decision, laid out as one choice

The lead asked what G-13 is left delivering if "evidence" wins (question 2), and what must be
solved if "gate" wins (question 3). Both, so the product owner chooses once.

#### Shape E — "evidence" wins (my recommendation)

The ADR's G-13 clause at `:480` and the repeat at `rocks.rs:1322-1324` are amended to say the
opposite: that the manifest's reference is **not** the fix, with §5.2's argument 4 as the reason.
G-13 then delivers the list at the end of this subsection. **Round 2 wrote "three things, in
descending order of how well they are established" and both of the first two were wrong in
opposite directions** — one void, one wrongly withdrawn — so the ordering is dropped along with
the items, and each is now stated with what was checked for it.

> **Round 3 rewrote this whole subsection. Round 2's E1 and E2 were specified against a gap list
> eighteen hours stale; the lead wrote them and has withdrawn both. One of the two withdrawals was
> then itself wrong.** What follows is the corrected scope, with every claim re-verified by me at
> `395d535` — including the claims used to *reject* things, which is the round's own lesson.

**E0 — the two claims round 2 made about current behaviour, and what is actually true.**

| Round 2's claim | True at `395d535`? | Evidence I opened |
|---|---|---|
| "an artifact's provenance record is present or absent depending on **which command took the backup** … is not a property anyone chose" | **No, twice over.** The cause is not the command: `run.rs:393` is `self.policy.as_ref().and_then(\|l\| l.state_and_version().1)`, `None` under static mode and on a signed node holding no valid document, so the RPC path is also often `None`. And it was chosen, in three places. | `backup.rs:103-107`, `backup.rs:280-284`, `run.rs:388-393` |
| "restore … **swallowing** it" | **No.** It emits `restore_policy_mismatch` with both numbers and `level:"warn"`, **before** `restore_completed`, and the delivery mechanism is documented because these subcommands install no tracing subscriber. | `main.rs:221-236`; two green rows at `m6_backup_policy.rs:367` and `:397` |

The second claim is the more instructive failure. §6's verification row says "`policy_divergence`
computed but **not enforced**" — which is true — and §5.4 then spent it one section later as
"instead of **swallowing** it", which is not. Nothing was searched wrongly; a true sentence was
paraphrased into a false one across two sections of one document.

**E1 — corrected twice, and it is live. Two parts.**

*Part 1, unconditional, no decision attached: two shipped doc comments assert a gap that closed.*
Both conditioned the `None` on gap G-09, and **round 2 and its withdrawal each quoted the passage
up to the condition and stopped**:

- `crates/config-server/src/backup.rs:103-107` — "`null` when the exporting process had no active
  policy to name: a static-mode node, a signed-mode node holding no valid document, and — **until
  the policy version floor is durable (gap G-09)** — every backup taken by the offline CLI".
- `crates/config-server/src/backup.rs:280-284` — "**The durable policy version floor (gap G-09) is
  what would let this path answer honestly.**"

**G-09 is durable at HEAD**, in the same commit `096bbfa` that voided E2:
`const KEY_POLICY_VERSION_FLOOR` (`git show HEAD:crates/config-storage/src/rocks.rs`, `:197`),
`policy_version_floor()` (`:1325`), `set_policy_version_floor()` (`:1341`). So both comments now
describe a blocker that no longer exists, and they say so in the subordinate clause each reading
stopped short of. Correcting them is owed regardless of every other decision in this section.

**This is L-R85's stale-gap-list defect a second time, in source comments rather than in a risk
list, and that is worth one line of its own.** A gap list going stale is a tracking problem. A
*doc comment* going stale is a trap, because §0.1's Rule 2 tells the next designer to treat the
comment as a prior decision with an argument attached — and this one's argument expired. Rule 2
needs a rider: **a rationale that names a gap is only as current as the gap. Check the gap.**

*Part 2, the seeding itself.* `backup_offline` can now answer honestly, and the machinery is
already there. `export_snapshot` reads `state_meta` from its read-only handle **seven** times, not
four — `IDENTITY`, `LAST_APPLIED`, `MEMBERSHIP`, `CLUSTER_REVISION`, `COMPACT_REVISION`,
`RETIRED_NODES`, `MAX_COMMAND_SCHEMA` (`crates/config-storage/src/snapshot.rs:938-961`) — through
`offline_meta` (`:874-896`), which returns `Option<T>`, so **absent-versus-zero falls out for
free**, which is the same distinction item 2 below is buying elsewhere. `offline_keys` (`:863-871`)
is a seven-constant module, and `MAX_COMMAND_SCHEMA` in it is the cell `policy_version_floor` was
explicitly modelled on — `rocks.rs:191-196` at HEAD says so: "Absent means nothing is known … so no
format bump and no migration, exactly as `KEY_MAX_COMMAND_SCHEMA` was added at M6."

**One thing in the lead's sizing I have to correct, and it is the only place I disagree.** "One
`offline_keys` constant plus one `offline_meta` call" is right about the *read* and silent about
the *carry*. `backup_offline` never touches a store; it receives a `SnapshotHeader` from
`export_snapshot` and hands `finish_artifact(&header, …, None)` the policy version
(`backup.rs:277-285`, `:312-319`). `SnapshotHeader` has **no field for this**, and it is not a
plain struct: `snapshot.rs:210-221` says "`postcard` is positional and not self-describing, so this
declaration *is* the on-disk layout: a field may be appended at the end, never inserted or
reordered", and a header written before an appended field "runs out of bytes and is refused as
`Malformed`". So the carry is a real, small choice, and it should be made rather than discovered:

| Route | Cost | Verdict |
|---|---|---|
| Append `policy_version_floor` to `SnapshotHeader` | a **snapshot format change**. Precedented — `retired_nodes` (M5) and `max_applied_command_schema` (M6) were both appended this way, both read via `offline_meta`, both documented "appended last" — but it puts the value **in the snapshot body**, which is what you want only if a *restore* is going to read it | **rejected under Shape E.** Under "evidence" nothing reads it at restore, so paying a format change to carry it there is paying for Shape G |
| `export_snapshot` returns `(SnapshotHeader, Option<u64>)` | one signature, its callers | workable, noisy |
| **A separate `snapshot::offline_policy_version_floor(data_dir) -> Result<Option<u64>, SnapshotFileError>`** | one constant, one `offline_meta` call, one small `pub fn`, one extra read-only open — which takes no directory lock, as `backup.rs:251-253` documents | **recommended.** No format change, no existing signature moved, and the value travels to the **manifest**, which is where "evidence" belongs |

**And the semantic caution, stated rather than papered over.** `policy_version_floor` and
`policy_version_ref` are **not definitionally the same quantity**. The manifest field is "The
signed policy document that was **in force when the backup was taken**" (`backup.rs:95`); the cell
is "The signed policy version this node **last had in force**" (`rocks.rs:191`, HEAD). For a
cleanly stopped node they coincide, and I traced the paths where they might not: the setter
"replaces rather than maximises" so a break-glass rollback moves it **down**
(`rocks.rs:1338-1340`), which keeps it *last in force* rather than *max ever* — which is closer to
the ref's meaning, not further; and on a static-mode or no-valid-document directory the cell is
absent and the daemon path would write `None` too, so they agree there as well. I found no path
where they disagree. **That is a trace, not a proof, and the doc comment E1 writes must say which
quantity it wrote** — "the version this node last had in force, read from the stopped directory"
— not "the document in force when the backup was taken". Under Shape E the field is evidence and
no enforcement rides on the difference; under Shape G it would, which is one more reason the two
shapes must not share an implementation by accident.

*Route for a shipped inconsistency found while checking this, not absorbed:*
`PolicyRejected::RollbackFloor`'s `floor` field is documented as "The highest version this node is
known to have served" (`config-core/src/policy.rs:315-316`), which is a *max-ever* reading of a
cell whose setter replaces. Nothing depends on it today. `config-core` text — **Q-4**.

**E2 — WITHDRAWN. The premise is false and the specification would have turned a green row red.**

Round 2 wrote "restore reports divergence loudly instead of swallowing it". It already does.
`crates/config-server/src/main.rs:221-236` emits
`restore_policy_mismatch{manifest_version, active_version, level:"warn", source:"cli"}` before
`restore_completed`, and `main.rs:224-227`'s comment explains the channel: "these subcommands
install no tracing subscriber, so the severity travels as a field on the record rather than as a
log level that would not exist."

Two landed green rows assert it. `m6_backup_policy.rs:367`,
`m6_35_restore_records_a_policy_divergence_without_blocking`, pins both numbers and `level ==
"warn"`. **`m6_backup_policy.rs:397`, `m6_35_restore_says_nothing_when_the_policy_versions_agree`,
is the one that makes the withdrawal mandatory rather than tidy:** it asserts
`run.events("restore_policy_mismatch").is_empty()` when the versions agree, and its own doc gives
the reason — "a divergence line that fired on every restore would be noise, and an operator who
learns to skip it has lost the diagnostic the line exists to be." E2 demanded a line stating **in
words** which of three cases holds, *including* "agreed". **That row goes red.** The production
function said the same thing first (`backup.rs:799-802`: "An unknown version on either side is not
a divergence but an absence … Reporting those as a mismatch would teach an operator to ignore the
line on every ordinary recovery"). So the shipped code had already made E2's exact decision, in
the opposite direction, with a stated rationale — Rule 2 broken in the section that introduced it.

**The delivered G-13 scope under Shape E, as one list.**

1. **The rollback refusal reports the numbers it already holds.** `PolicyRejected::RollbackFloor`
   carries `floor` and `incoming` (`config-core/src/policy.rs:314-319`) and is emitted as the bare
   token `"rollback_floor"` — `#[error("rollback_floor")]` at `:313` and
   `Self::RollbackFloor { .. } => "rollback_floor"` in `reason()` at `:358`. Neither number reaches
   the operator through `policy_rejected` or `/health`. A refusal that will not say which version
   it would accept is the whole of the difficulty the gap actually causes. Separately, the refusal
   an operator meets first is `config-grpc/src/admin_plane.rs:430-434` — "this principal is not
   listed in `[authz] admins`" — and `config-server/src/run.rs:849-850` states that under signed
   mode "`[authz] admins` is not consulted at all, and startup said so". **Verified: the string and
   the contradiction are both in shipped source.**
2. **The durable floor gets a reading that distinguishes absent from zero.**
   `policy_version_floor()` is `read_meta(…)?.unwrap_or_default()` (HEAD `rocks.rs:1325-1330`), so
   "never seeded" and "seeded to 0" are one value, and G-13's founding premise was unmeasurable for
   as long as it has been written down. A developer has landed
   `policy_version_floor_cell() -> Result<Option<u64>, String>` in the working tree
   (`crates/config-storage/src/rocks.rs:1337`; `git show HEAD:… | grep -n policy_version_floor_cell`
   returns nothing, so it is **not committed** and I do not cite it as landed).

   **The surface is decided, not open — L-R88: one new field on `/health`.** Not an
   `inspect-store` subcommand. The reasoning is the part worth keeping: the lead had been leaning
   toward an offline subcommand because a restore lives in a stopped directory, which reasons from
   where the **data** sits; the reproduction put the operator at a **booted daemon in deny-all with
   the admin plane refusing them**, which is where the **operator** sits. `/health` is the surface
   that still answers in the state actually observed. The subcommand is not ruled out and is **not
   funded**: it would fit the offline family `Backup` / `VerifyBackup` / `Restore` at
   `crates/config-server/src/cli.rs:117-161` behind `run_offline`, with its JSONL-on-stderr
   convention, so it is cheap whenever an observation funds it — the same discipline the developer
   applied to D3. **Surface follows an observation, not a plausible story about one.**

   **One implementation constraint, which I am attaching rather than deciding, because getting it
   wrong ships a field that looks like the fix and is not.** The ruling names the block at
   `crates/config-server/src/health.rs:141-144`, whose anti-tearing rationale (`:136-140`: "Both
   fields come from one read … the torn payload M6-20 catches") is right and applies unchanged.
   But that block reads the **authorizer**, and the authorizer cannot represent absent:
   `SignedPolicyAuthorizer::floor` is an `AtomicU64` initialised to `0`
   (`crates/config-core/src/policy.rs:677`, `:702`), documented as "The version floor starts at
   zero — *nothing durable is known*" (`:696-697`), read out by
   `version_floor(&self) -> u64` (`:720-721`), and `below_floor = floor > 0 && …` (`:769`) uses `0`
   as the sentinel. **A `/health` field filled from `version_floor()` reports `0` for both "never
   seeded" and "seeded to 0" — the exact collapse item 2 exists to remove.** Two routes:

   | Route | Cost | |
   |---|---|---|
   | Field is `Option<u64>` read from `policy_version_floor_cell()` on the store | a **second** read in the handler; M6-20's one-read property covers `policy_state`/`policy_version`, which must agree with each other, and the floor is a third fact that need not | **recommended** — it is the only reading that carries the distinction, and `config-server` owns both sides |
   | Teach the authorizer to distinguish: `seed_version_floor` is already called only when a durable record exists (`policy.rs:665-672`), so an `Option` or a seeded flag is available | a **`config-core`** change, outside this freeze's file ownership, and it changes a type two crates read | fits the one-read property; **routed, not taken** |

   Either way the field's type is `Option<u64>` and never `u64`. **This is independent of item 3**
   — item 3 is about what the *manifest records* at backup time, this is about what a *running
   daemon reports*. They read the same cell and must not be merged.
3. **E1**, both parts above.
4. *Optional:* `--active-policy-version` may also seed the floor. §5.4b holds the mechanics.

**E3 — optional: seed the floor from `req.active_policy_version`.** If a floor is wanted at all
across a restore, this is the only cluster-correct source, and it is already landed.
`crates/config-server/src/backup.rs:771-780` documents it as "The signed policy version **the
restored node will run under**, when the operator knows it. Supplied rather than discovered:
restore deliberately reads no configuration file." That is a statement about the **new** lineage,
made by the operator, at restore time. `--active-policy-version` (`crates/config-server/src/cli.rs:200-212`).

**E3 does not contradict either doc comment**, and that is the test it was chosen to pass:
`backup.rs:98` says nothing checks *the manifest's reference*, which stays true; `cli.rs:203` says
*the manifest's reference* is not a validation input, which also stays true. The value being used
is the operator's own, and `cli.rs`'s claim about it is only that its absence means no comparison
is made — which E3 preserves, because absent means no floor is seeded.

**And E3 cannot produce the Manual Tester's §4.4 brick.** The gate is `to < floor`, strictly
(`config-core/src/policy.rs:750-757`: "`to == floor` is the ordinary restart"). The operator names
the version they are installing; the floor equals it; `1 < 1` is false; the document adopts with no
flag. The false refusal that round 1's design produced is arithmetically impossible here.

**I recommend items 1–3 (E2 is withdrawn), and E3 only if the product owner wants a restore-path
floor at all.** My own view is that E3 is worth little: §5.3 shows the surviving threat is the unscoped-legacy replay, and
`ADR-0027:426-427` says that path is on its way out. **E3 is a control whose entire remaining domain
is a deprecated case.** It is cheap and it is correct, so I would not argue against it; I would not
argue for it either, and it should not be what G-13 is *for*.

#### Shape G — "gate" wins

Then both doc comments are wrong and **must be retired in the same change**, not left standing —
that is the lead's D4 ruling read the other way, and it is not optional: leaving
`backup.rs:96-100` in place while making the field a validation input reproduces the exact defect
this round is about, one layer down.

And the new-lineage brick must be *solved*, not messaged. I do not have a solution I believe in,
and I will say so rather than produce one. The obstacle is §5.2 argument 4: `backup.rs:880` refuses
a same-identity restore, so there is no case in which the manifest's reference and the incoming
document are in one lineage — except the unscoped-legacy residue of §5.3, which does not need the
manifest's reference because a floor seeded from **anything** the old lineage used would do. A gate
therefore needs a rule for "the incoming document is in a new lineage, admit it", and the only
honest inputs for that rule are `document.cluster_id` and the node's identity — at which point the
gate has become "refuse a foreign-lineage document", which is **`verify_policy` step 7, already
shipped** (`config-core/src/policy.rs:453-459`). The Manual Tester's recommended restore-specific
refusal message is a real improvement to Shape G and it is not a solution: a better message makes a
wrong comparison legible rather than making it right.

**If Shape G is ruled, the design work is a new round, and I am naming that rather than absorbing
it.**

#### The parts that are the same under both

§5.5 (the audit line), §5.6 (`policy_divergence` stays report-only as a *gate*), §5.7 (`Option<u64>`,
no schema change), §5.8 (the five call sites, **only if a floor is written**) and §5.9 (the report
field is a write confirmation) hold under either ruling. Under Shape E without E3, §5.8's signature
change **does not happen at all** and F-3's five-call-site cost goes to zero.

### 5.4b If E3 or Shape G is ruled: the seeding mechanics

Called **(a′)** below and in §5.5–§5.10, to distinguish it from round 1's **(a)**, which sourced
the floor from the manifest and is withdrawn.

**Seed `state_meta/policy_version_floor` from `req.active_policy_version`, the restore's own
existing parameter. Do not seed from `manifest.policy_version_ref`.**

The parameter is already landed, already optional, and already documented as exactly the right
quantity. `crates/config-server/src/backup.rs:771-780`:

> The signed policy version **the restored node will run under**, when the operator knows it.
> Supplied rather than discovered: restore deliberately reads no configuration file — a recovery
> must not depend on a document that may have been lost with the cluster.

It reaches the code as `RestoreRequest::active_policy_version: Option<u64>` (`backup.rs:780`), from
the CLI flag `--active-policy-version` (`crates/config-server/src/cli.rs:200-212`), and is consumed
today only by `policy_divergence` at `backup.rs:997-1000`.

Answering the lead's three questions in order:

**1. Should the floor be seeded at all when the restore mints a new identity?** Yes — but only from
a value that belongs to the new lineage. `manifest.policy_version_ref` belongs to the old one
(§5.2); `active_policy_version` is the operator's statement about the new one (`backup.rs:771`).
Seeding from the second is a same-lineage comparison, which is the only kind `below_floor` can make
sense of.

**2. What makes a legitimate new-lineage document admissible without guessing a flag?** Arithmetic,
not a flag. The operator names the version they are installing; the floor equals it; the gate is
`to < floor`, **strict** — and the strictness is deliberate and documented at
`config-core/src/policy.rs:750-757` ("`to == floor` is the ordinary restart"). So the document the
operator just declared is adopted, with no flag, on the first boot. The Manual Tester's §4.4
sequence — correct operator, correct document, deny-all cluster — **cannot occur**: the floor is
1, the document is 1, `1 < 1` is false.

**3. If it is not seeded, what does `policy_version_ref` buy?** It stays exactly what two shipped
doc comments already say it is: evidence for an auditor. `backup.rs:98-101` — "**Nothing checks it
at restore**, because the independently supplied policy may legitimately be older, newer or
unrelated; it exists so a recovering operator can tell which document the data was authorized
under." `cli.rs:201-207` — "the manifest's reference is a **breadcrumb for a human and not a
validation input**".

**D4, answered under the lead's ruling that neither comment is retired by default.** D4 asked which
of the two shipped doc comments G-13 retires. Under round 1's design, both — silently, which is the
defect. **Under Shape E, neither**, and the retirement is not *earned* but *avoided*:
`policy_version_ref` is still never checked at restore and is still a breadcrumb; the floor, if
there is one, comes from a different input entirely. That is the lead's stated preference — "leave
both standing and change G-13's shape instead" — and it is available because the operator's own
declared version exists as a landed parameter. **What Shape E does retire is the ADR clause and its
repeat in `rocks.rs`**, which is a product-owner decision and is why this section stops.

Of the Manual Tester's four narrower fixes (§4.4), this is its fourth — "Require
`--active-policy-version` … and seed from **that**, the operator's stated intent". I checked its
citations and they hold. I prefer it to its own recommendation (the restore-specific refusal
message), because a better message makes a wrong comparison legible where this makes the comparison
right. **The message should still be improved, and that is named in §8 as Q-4 rather than absorbed
here** — it is a `config-core` refusal-text change, outside this freeze's file ownership.

**What (a′) does and does not close, stated to its scope.** It closes the surviving unscoped-replay
hole (§5.3) **whenever the operator passes the flag**, and it never produces a false refusal. It
does **not** close it when the flag is omitted — §5.5. And it does not seed a floor that is higher
than the document being installed, so it cannot refuse a legitimate first boot in any lineage,
scoped or not.

### 5.5 (b) The `None` path — no longer a silent per-artifact exception

F-4's criterion is right: a security control whose coverage is partial must make the uncovered case
observable at the moment it applies. Round 1 failed it. **(a′) satisfies it structurally**, and
this is the second reason to prefer (a′) over round 1's (a):

| | Round 1 (a): source `manifest.policy_version_ref` | (a′): source `req.active_policy_version` |
|---|---|---|
| Who decides `None` | the **exporting** process, days earlier — `backup.rs:285` passes `None` on the offline CLI path | the **restoring** operator, now, by omitting a flag they are typing |
| Where it is visible | nowhere: not in the artifact's filename, not in the restore command | in the restore command, which is the thing being run |
| What F-4 called it | "a silent per-artifact exception" to a universal documented limit | the operator declining to state a version |

So the uncovered case is uncovered *at the moment and by the person that chooses it*, which is
exactly F-4's closure condition. The residual confused-deputy case F-4 raised — an attacker with
the backup signing key handing an honest operator an artifact that restores with no floor —
**disappears**, because the artifact no longer decides the floor.

**The audit line is required, not an open question.** F-4 is right that leaving it as question 3
inverted its importance. Specified:

- `RestoreOutcome` carries the seeded value forward and the existing `restore_completed` audit line
  (`crates/config-server/src/main.rs:240-250`) reports it.
- It must **distinguish seeded-`N` from seeded-nothing in words**, not by an omitted or empty
  field. `policy_version_floor_seeded: "none — --active-policy-version not supplied; the first
  document this node adopts will be accepted at any version"` versus
  `policy_version_floor_seeded: "412"`.
- The known-limit paragraph at `crates/config-storage/src/rocks.rs:1312-1324` is **narrowed**, not
  deleted, from "a restore" to: a restore performed without `--active-policy-version`, and — from
  §5.3 — only for an unscoped document, since a scoped one from the source cluster is refused by
  `verify_policy` before the floor is consulted.

**Cost of the audit-line change, which round 2 asserted and did not count (R2-8).**
`restore_completed` is not a free-form record: `docs/testing/test-plan-m5.md:656` (M5-123,
`log_restore_completed`) pins its field list — `source_cluster_id`, `source_epoch`,
`source_revision`, `new_cluster_id`, `new_epoch`, `revision`, `compact_revision` — and
`:514` (M5-81) asserts the line "exists exactly once" and that neither it nor its sibling
"contains a key or a value"; `ADR-0024:272-276` describes it as the only record in which both
identities appear. **Adding a sentence of operator prose to that record moves two named plan
rows.** The natural home for a seeded/not-seeded fact is `restore_policy_mismatch`, which already
exists and which M6-35 owns — but that record is asserted **silent** when the versions agree
(`m6_backup_policy.rs:397`), so a "seeded nothing" line cannot ride on it either without moving
M6-35. **Decision: the audit line is required under E3/Shape G and its home is an open cost, not a
free one.** Whichever record it lands on, the change names the rows it moves — M5-81 and M5-123, or
both M6-35 rows — in the same commit. Round 2's §8 item 9 disclaimed `RestoreOutcome` consumers and
evidence JSONs; it did not name these, and these are assertions that go red rather than costs that
go uncounted.

**The Manual Tester's D1 is unmet at HEAD and is being met right now by somebody else.** At
`395d535`, `RocksStore::policy_version_floor()` is
`read_meta(&self.shared.db, CF_STATE_META, KEY_POLICY_VERSION_FLOOR)?.unwrap_or_default()`
(`rocks.rs:1325-1327` at HEAD) and returns `0` for an absent cell, while
`below_floor = floor > 0 && …` is inert at `0` — so *cell absent* and *cell present, value 0* are
indistinguishable at every layer.

**And that collapse is a stated decision, not an oversight — round 2 cited the method doc and not
the constant's (R2-10).** `git show HEAD:crates/config-storage/src/rocks.rs`, `:191-196`, the doc
on `const KEY_POLICY_VERSION_FLOOR` at `:197` which §5.7 already cites for its byte encoding:

> The signed policy version this node **last had in force** … **Absent means nothing is known**,
> which is what a directory written by any earlier build says — so **no format bump and no
> migration**, exactly as `KEY_MAX_COMMAND_SCHEMA` was added at M6.

So D1 asks to **reverse a documented decision**, at a stated cost the comment names, and round 2's
"D1 is unmet at HEAD" told the lead it was a gap. Under §0.1's Rule 2 that sentence was owed and is
now given. **The decision is still the right one to reverse**, and the reason is in the comment
itself: "absent means nothing is known" is a claim the *storage layer* makes and then throws away
at `policy_version_floor()`'s `.unwrap_or_default()`. The cell already distinguishes; only the
accessor collapses. **So D1 costs no format bump and no migration either** — it is a second
accessor, which is exactly the shape of the uncommitted `policy_version_floor_cell`. The comment's
stated cost does not apply to the fix actually being asked for, and that is why the reversal is
cheap rather than why it is unnecessary. **In the working tree, they are not:** an uncommitted +13/−4 edit
to `rocks.rs` adds `pub fn policy_version_floor_cell(&self) -> Result<Option<u64>, String>` (line
1337 in the working tree; `git show HEAD:… | grep -n policy_version_floor_cell` returns **nothing**).
That is D1's surface, arriving from another team while this document was being written.

**This changes D1's status, not this design.** I record it because §8's "unverified" list would
otherwise carry a claim that is about to be false, and because `AGENTS.md`'s "Several agents, one
working tree" section says a working-tree reading is about an instant, not about HEAD. **I have not
read that change, have not verified it does what D1 asks, and must not cite it as landed.** Named
in §8 as Q-3 and item 7, not absorbed.

### 5.6 (c) Report vs enforce — unchanged

**Do not touch `policy_divergence`.** It stays report-only. Both round-1 reasons survive and the
critic passed them; the second is the stronger: `policy_divergence`
(`crates/config-server/src/backup.rs:797-812`) fires on *any* difference between the manifest's
version and the restored node's, including the routine case where policy has legitimately moved on.
Gating on it would refuse ordinary restores in the common case.

One consequence of (a′) worth naming: `req.active_policy_version` now feeds **two** things — the
report at `backup.rs:997-1000` and the floor. That is a feature, not a coupling risk: the operator
states the version once and both the report and the gate use the same statement, so they cannot
disagree about what the operator said.

### 5.7 (d) L-R31 — fits inside existing shapes, different source

Conclusion unchanged, source changed. `req.active_policy_version` is `Option<u64>`
(`backup.rs:780`), already a parameter; no manifest schema change, no new signed field, and now not
even a new read of a signed field. Destination is the existing cell,
`crates/config-storage/src/rocks.rs:197` — `const KEY_POLICY_VERSION_FLOOR: &[u8] = b"policy_version_floor"`.

Encoding matches, re-verified: `postcard::to_stdvec(&version)` for `version: u64` at
`rocks.rs:1345`, read back by `policy_version_floor()` at `:1326-1330`. Byte-identical; no codec
mismatch and no versioned wrapper.

### 5.8 The signature change, against all five call sites (F-3)

**Conditional on the charter ruling.** Under Shape E without E3, no floor is written at restore and
**this subsection does not happen** — `restore_into_fresh_store` keeps its four arguments and F-3's
cost is zero. It is written out because E3 and Shape G both need it, and because F-3's finding —
that round 1 said "call site" where there were five — has to be closed with the table whether or
not the change ships.

*Command and scope — whole tree:* `grep -rn "restore_into_fresh_store" crates/` returns 14 hits;
five are calls, the rest are the definition, the `lib.rs:39` re-export, and doc comments. I opened
all five.

```rust
pub fn restore_into_fresh_store(
    data_dir: &Path,
    new_identity: &config_core::ClusterIdentity,
    snapshot: &Path,
    restored_from: &config_core::RestoredFrom,
    policy_version_floor: Option<u64>,   // NEW — from req.active_policy_version (§5.4)
) -> Result<RestoreReport, SnapshotFileError>
```

| # | Call site | Crate | What it passes | Why |
|---|---|---|---|---|
| 1 | `crates/config-server/src/backup.rs:978` | `config-server` | `req.active_policy_version` | the production path; the only one that has an operator |
| 2 | `crates/config-storage/tests/m5_snapshot.rs:1214` | `config-storage` | `None` | a storage-level restore row; no policy plane exists in it |
| 3 | `crates/config-testkit/tests/m5_backup_fencing_cluster.rs:218` | `config-testkit` | `None` | restores into a fenced identity then forms a cluster; asserts fencing, not policy |
| 4 | `crates/config-testkit/tests/m6_compat_cluster.rs:604` | `config-testkit` | `None` | schema-compat restore; asserts version skew |
| 5 | `crates/config-testkit/tests/m6_evidence.rs:590` | `config-testkit` | `None` | **produces `docs/evidence/*.json`**, and those files are modified in the working tree right now (`git status` at session start lists eight of them) |

**Four of the five are landed green tests in two crates foundation does not own**, so the change is
red-before-green across `config-storage` and `config-testkit` unless all five land in one commit.
`AGENTS.md` ("Several agents, one working tree") records an incident of exactly this
misattribution on 2026-09-21. **Implementation requirement: the five edits ship together, and the
handoff carries this table.**

**The alternative I considered and rejected.** A second function, or a parameter struct, leaving the
4-argument form intact — the route the critic said the design proposed neither of. Rejected: a
4-arg form that silently seeds nothing is the same silent-omission hazard the Manual Tester
describes for the audit line (§4.5 of its plan, shape (a)), and it would let a future production
caller forget the floor with no compile error. A required parameter makes the decision explicit at
every site, which is why four sites saying `None` out loud is the *point* rather than the cost.

**D3, the Manual Tester's key-mirror demand, is accepted and is cheaper than round 1's answer.**
Round 1 proposed `restore_keys::POLICY_VERSION_FLOOR = b"policy_version_floor"` in `snapshot.rs`
with a "must equal" comment and nothing enforcing it. The Manual Tester is right that this mirror
is unlike `restore_keys::IDENTITY`: identity's reader fails loudly (`RocksStore::open` refuses a
directory with no identity), the floor's reader fails **silently** (a one-byte typo yields `0`,
which is exactly the pre-G-13 behaviour, indistinguishable from "not implemented"). Both constants
are in the **same crate**, `config-storage`. **Decision: widen `rocks::KEY_POLICY_VERSION_FLOOR`
from private to `pub(crate)` and use it directly from `snapshot.rs`. No second copy, no comment, no
`const _: () = assert!(…)` needed.**

### 5.9 (F-5) `RestoreReport.policy_version_floor` — kept, and populated from the write

F-5 is right as written and its closure condition is the right one. Round 1's field echoed the
caller's own argument, so an implementation that computed the field, logged it correctly and
**omitted the `batch.put_cf`** would pass every assertion on it — the Manual Tester's shape (a) in
pure form: the oracle reads its expected value from the same place as the thing under test.

**Decision: keep the field, and populate it inside the write branch, never from the parameter.**

```rust
// in restore_into_fresh_store's final synced batch (snapshot.rs:1273-1304), immediately after
// the existing RESTORED_FROM write and inside the same batch that carries IDENTITY.
let mut floor_written = None;
if let Some(v) = policy_version_floor {
    batch.put_cf(
        meta,
        rocks::KEY_POLICY_VERSION_FLOOR,                 // pub(crate), §5.8 — not a mirror
        encode("policy_version_floor", postcard::to_stdvec(&v))?,
    );
    floor_written = Some(v);                             // set by the write, not by the argument
}
```

`RestoreReport.policy_version_floor` is then `floor_written`. It is a **write confirmation**, which
is the value the caller does not already hold, and it is what earns the field its surface. An
implementation that skips the `put_cf` now reports `None` and the audit line says "none", which
step 5 of the hand-test can read.

Same-batch placement is unchanged and the critic passed it: `snapshot.rs:1273-1304` is the batch
carrying `restore_keys::IDENTITY`, committed at `:1310` with `WriteOptions::set_sync(true)`, and
the all-or-nothing note at `:1305-1309` attaches to that write. No crash window between "restore
committed" and "floor recorded".

`RestoreReport` is not `#[non_exhaustive]` (`snapshot.rs:1119-1121`), so the field is a breaking
addition for struct-literal construction. *Scope of what I checked:* `grep -rn "RestoreReport"
crates/` — the type is constructed only inside `snapshot.rs`; the four test call sites bind the
returned value and read `.revision` / `.written`. No external struct literal exists at `395d535`.

### 5.10 How a Manual Tester drives this by hand

**This sequence tests E3 / Shape G — a restore that seeds a floor.** Under Shape E without E3 there
is no floor to test and the sequence collapses to steps 1 and 2, which test **E1**: that a
provenance version is recorded on **both** backup paths — the offline one now reading the durable
floor from the stopped directory. **The "audit line says in words which of agreed / diverged /
not-compared holds" step is deleted**, because that was E2 and E2 is withdrawn: the line already
exists, it already carries both numbers at `warn`, and a line that also fired on "agreed" would
turn `m6_35_restore_says_nothing_when_the_policy_versions_agree` red. The existing behaviour is
worth *observing* in step 1a below, not changing.

**Step 1a (new, E1's hand-test, runnable today).** Take the **same** stopped directory twice: once
with `config-server backup` (offline) and once by the daemon's RPC path, with a signed document at
version *N* active before the stop. **Expected after E1: both manifests read
`policy_version_ref: N`.** Before E1 the offline one reads `null`. **Expected unchanged on a
static-mode directory: both read `null`**, because there is no active document and the floor cell
is absent — which is the case that proves E1 did not just write a number to have one.

Revised for the operator-declared source. Layer A is runnable today except where D1 bites; Layer B still needs M6-34's
fixture (`docs/testing/test-plan-m6.md:505`, open since 2026-09-19).

1. Take an RPC-path backup with a signed document at version *N* active. Confirm
   `manifest.policy_version_ref` reads *N*. **Note: under (a′) this value is no longer an input to
   anything; step 1 confirms the breadcrumb is intact, not that the floor will be seeded.**
2. Restore to a fresh directory under a new cluster id/epoch, passing
   `--active-policy-version M`, where *M* is the version of the **new** document the operator has
   issued for the new cluster. Confirm the `restore_completed` audit line names `M` in words.
3. Start a signed-policy daemon against the restored directory with that new document at version
   *M*, and **no** `--break-glass-policy-rollback`. **Expected: adopted.** This is the step that
   round 1's design failed and (a′) is built to pass.
4. Same directory, restart with a *different* validly signed unscoped document at version < *M*,
   no flag. **Expected: refused, `policy_rejected` with `reason: "rollback_floor"`,** naming both
   numbers. This is the surviving threat from §5.3, now closed.
5. Same, **with** the flag. Expected: adopted; `policy_loaded` carries `break_glass: true`; the
   rollback counter increments.
6. Repeat 2 **omitting** `--active-policy-version`. Expected: audit line says "none — …"; floor
   cell unseeded; first document at any version accepted. The deliberate, legible gap from §5.5.

**If broken:** step 3 refuses (the floor was seeded from the wrong lineage — round 1's defect), or
step 4 accepts (the floor was not seeded or not enforced — G-13's original defect). Both are loud.
Step 6 is the one that stays partly unjudgeable until **D1** lands, because absent and zero read
alike.

---

## 6. Lead verification (2026-09-21)

**This table is the lead's, not the architect's.** Rows whose *inference* this round changes are
marked; no row's underlying fact was found false.

| Claim | Verified | Status after round 2 |
|---|---|---|
| ask 9 is `rdb-sim` conversion code, not a `ControlTime` reshape | **Yes** — `kernel-a/design.md:842-848` and `:875-877`, and the ask-9 row in foundation §14. This **contradicted the lead's own brief**, which asserted a reshape. Brief was wrong; architect withdrew it with two primary sources. | **Stands.** §3 carries it, now with its scope attached and re-checked against CB-9. |
| `PolicyLoader::new` seeds the floor on every boot | **Yes** — `config-server/src/policy.rs:158-160`. | **Stands.** Re-opened at `395d535`. |
| `break_glass` unconditionally permits `below_floor` | **Yes** — `config-core/src/policy.rs:767-772`, and `break_glass: below_floor` on the `Adopted` result at `:788`. | **Fact stands; the inference built on it is WITHDRAWN.** "Restore inherits break-glass for free" was proved under same-lineage version comparison and does not survive the cluster-identity change a restore performs. §5.2. |
| `state_meta` excluded from snapshot body | **Yes** — `config-storage/src/snapshot.rs:78`. | **Stands.** |
| One backup path passes `None` | **Yes** — `backup.rs:285` vs `run.rs:393`. | **The fact stands; round 2's account of it was wrong twice, and so was its withdrawal.** (i) The cause is not "which command": `run.rs:393` is `None` under static mode and on a signed node holding no valid document. (ii) It *was* chosen, three times. (iii) But each of those three conditions the `None` on **gap G-09**, and G-09 closed in `096bbfa` — so the withdrawal was itself made by reading up to the condition and stopping. **E1 is live**, in two parts. §5.4. |
| `policy_divergence` computed but not enforced | **Yes** — `backup.rs:997-1000`. | **The fact stands and was spent as something else.** This row says "not **enforced**", which is true; §5.4 spent it as "instead of **swallowing** it", which is false — it is emitted at `warn`, before `restore_completed`, with two green rows. **E2 withdrawn.** §5.6 keeps `policy_divergence` report-only as a gate, which was never in doubt. The slide from a true row to a false paraphrase, inside one document, is the clearest single instance of this milestone's mechanism. |

**Two rows the lead added in the third correction, verified independently by me:**

| Claim | Verified | Status |
|---|---|---|
| ADR-0027 charters G-13's fix as seeding from `manifest.policy_version_ref` | **Yes** — `docs/ADRs/0027-…md:477-482`, status Accepted. **And a third statement the correction did not list:** `crates/config-storage/src/rocks.rs:1322-1324` repeats it. | **Stands, and the charter is contradicted.** §5.1. My position is that the clause and its repeat are both wrong; the amendment is a product-owner decision. **STOP.** |
| G-09's scope is same-lineage by construction | **Yes** — `config-core/tests/m6_rbac.rs:482` heads the section "outlives the **process**"; all four rows plus the durable half at `config-server/src/policy.rs:1014` are restarts. | **Stands, and strengthens.** §5.3. Found additionally: `config-server/src/backup.rs:880-889` **refuses** a same-identity restore with `cluster_id_reused`, so the lineage change is not merely typical — it is mandatory. That is §5.2's decisive argument and it was not in any input to this round. |

**Doc defect found while verifying (round 1):** the CB-8 row in `test-plan-m7-foundation.md` §14
argues its urgency from "kernel-a's ask 9 above reshapes `ControlTime`". The ask-9 row two lines
above says the opposite, and kernel-a's design agrees with the ask-9 row. **Round-2 update: the §14
row needs a second correction as well.** Its coverage premise — that `compare`/`is_stale` are
asserted by no row in the workspace — is also false: four of the six branches are asserted by
`m7f_14` at `crates/rdb-core/tests/seams.rs:50-93`. Both corrections are one edit to a file this
team does not own; routed, not made here.

---

## 7. Named and not absorbed

The brief's stop rule: *if you find a fifth change the freeze needs, name it and stop rather than
absorbing it.* I found none the freeze **needs**. I found one the freeze would **benefit** from,
and it is recorded here as an observation, not built and not required:

> **The drift stage watches `crates/rdb-core/src/contracts` as a whole.** (`AGENTS.md`, "Running
> the gate": the stage fails when a plan's `<!-- drift-basis: <sha> -->` is no longer the newest
> commit to touch that path.) CB-7's shape puts each kernel's leaf enum in a file only that kernel
> edits, but every leaf still sits under `contracts/`, so a leaf addition still red-builds all four
> §15 tables. If the stage watched a path list that excluded the two leaf files — or watched
> per-file bases — a kernel could add a reason name at no cross-team cost, which is what the
> ownership split is trying to achieve. **This is a tooling change in `scripts/`, not a contract
> change. It is not required for the freeze, no decision here depends on it, and I have not
> designed it.** Routed to the lead.

---

## 8. Open questions, and what I could not verify

**Open questions for the lead** (each is a decision, not a gap in this design):

- **Q-1. `ReplicaIgnoreReason`'s name and file.** Ruling 2 ruled the principle, not the spelling.
  The principle here is fixed — one leaf per owner, `#[non_exhaustive]`, in a file that owner
  edits alone. The *name* `ReplicaIgnoreReason` and the *file* `contracts/ignore.rs` are my
  choices, and kernel-b may prefer others. Nothing in §1.5's mapping moves if they change.
- **Q-2. `Alert{RebuildStalled}` (M7B-128).** `Alert` keeps `ErrorKind` (§1.8), so this row is not
  spellable after CB-7 either. Two answers, both outside foundation's gift: widen `ErrorKind`
  (a spec §5.4 question) or re-route the row to `Ignored{Replica(RebuildStalled)}`. The critic is
  right that it is cheaper to answer before the basis moves.
- **Q-3. CLOSED, not open — the Manual Tester's D1.** The storage half is landing: an uncommitted
  working-tree edit to `rocks.rs` adds `policy_version_floor_cell() -> Result<Option<u64>, String>`
  (§5.5; not committed, not cited as landed). The **operator surface is ruled, L-R88: one new field
  on `/health`**, not an `inspect-store` subcommand — the reproduction put the operator at a booted
  deny-all daemon, not at a stopped directory, and the surface follows the observation. One
  implementation constraint is attached in §5.4 item 2 and it is not cosmetic: the field must be
  `Option<u64>` and cannot be filled from `SignedPolicyAuthorizer::version_floor()`, which is an
  `AtomicU64` using `0` as the "nothing known" sentinel (`config-core/src/policy.rs:677`, `:696-702`,
  `:720-721`) — filling it from there reproduces the collapse the field exists to remove. Still
  required to verify §5.10 step 6.
- **Q-4. The `RollbackFloor` refusal text.** Under Shape E the comparison is either absent or
  correct, so the Manual Tester's recommended restore-specific message is not load-bearing — but
  `PolicyRejected::RollbackFloor { floor, incoming }` still prints two bare integers, and naming
  the flag would still help. **Under Shape G it becomes load-bearing and is still not sufficient**
  (§5.4). `config-core` text, outside this freeze.
- **Q-6 (the one that gates the rest). The charter ruling: is `manifest.policy_version_ref`
  evidence or a gate?** §5.1–§5.3 argue evidence and recommend amending `ADR-0027:477-482` and
  `rocks.rs:1322-1324`. **Amending an Accepted ADR on a security control is not mine, and G-13's
  deliverable has two different shapes behind this one answer (§5.4). This is the stop.**
- **Q-5. Sequencing (the Manual Tester's D7).** Run `scripts/gate.sh drift` after CB-7 lands and
  **before** the four §15 markers move. If the markers move in the same commit, the stage's fire is
  unobservable for ever.

**Unverified, named as unverified rather than assumed:**

1. **Nothing in this design was compiled.** The brief forbids `cargo`. Every "this compiles" /
   "this breaks nothing" statement is a reading of the cited lines, not a build result. The two
   most exposed are §4.3's "call-site cost: zero" (six sites read) and §5.9's "no external
   `RestoreReport` struct literal" (one grep).
2. **The serde representation of the five-arm enum** (§1.9 P4) is a *commitment*, not an
   observation. Externally-tagged is serde's default and I have asserted it; I have not round-tripped
   it. The Manual Tester rates this CB-7's most likely real defect and I agree.
3. **Whether `ReplicaIgnoreReason`'s twelve names are the right twelve** rests on
   `test-plan-m7-kernel-b.md:501`, one row of one plan, re-read at `395d535`. I did not re-derive
   them from kernel-b's `design.md`, and the plan's own history shows this row was rewritten in
   round 6 ("BA-11's reading is falsified and BA-11 is rewritten"). If kernel-b has moved since,
   the leaf's contents move and the shape does not.
4. **I1's sampling cadence** (§4.4). ~~I established that a per-step fill yields age zero
   forever.~~ **Corrected: a per-step fill yields age zero only under a harness that advances the
   clock to `ctx.now` first, and no harness exists.** What still holds and was checked directly is
   the structural half: `Clock::advance` moves only the sample time — `self.now` is read by
   `now()` (`clock.rs:65`) and `control_time()` (`:105`) and nothing else; `due` takes the tick as
   a parameter (`:170`), `next_deadline` reads the timer table (`:160`), `arm` reads its `at`
   (`:129`). I did **not** establish what cadence I1 should use, and after the correction that is
   no longer a stop for CB-9b — it is a note, §4.6.
5. **`config-testkit` and `config-storage` handoff scheduling.** F-3's false-positive check allows
   that the four unnamed sites are already scheduled in a handoff I did not read. I did not read
   those teams' handoffs; §5.8 assumes they are not scheduled and requires the five to ship
   together, which is safe either way.
6. **The occurrence-count discrepancy in §1.2** (64 `Fact(` vs the lead's 45 and the critic's 45).
   The distinct-name count agrees at 27 across all three counts, so nothing depends on it, and I
   did not chase which files or filters produced 45.
7. **`crates/config-storage/src/rocks.rs` is dirty in the working tree** (+13/−4, another agent,
   uncommitted). Every `rocks.rs` citation in this document was taken with
   `git show HEAD:crates/config-storage/src/rocks.rs | grep -n …`, and the HEAD line numbers
   (`:197`, `:1312`, `:1325`, `:1341`, `:1345`) were re-confirmed that way. I have **not** read the
   uncommitted change beyond noting that it adds `policy_version_floor_cell`, and nothing in this
   design depends on it. Per `AGENTS.md`, I did not touch, stash or revert it.
8. **`crates/config-server/tests/g13_scratch_repro.rs` is untracked and is not mine.** Its own
   header calls it scratch, `#[ignore]`d, and "not evidence for" a plan row, and states its own
   boundary: it plants the floor by the product's own mechanism and runs under **one** cluster
   identity throughout, so it does **not** reproduce the identity mint. §5.2 argument 4 makes that
   boundary sharper than the file's header does — `backup.rs:880` refuses a same-identity restore,
   so the half that file omits is the half the whole question turns on. **Nothing in §5 rests on
   it, and I did not run it** (the brief forbids `cargo`). Its result, when it lands, is worth
   reading against §5.2 rather than instead of it.
9. **Shape E's E1 and E2 are recommendations I have argued but not costed.** I read
   `backup.rs:285` and `run.rs:422` and the `policy_divergence`/audit-line sites, and I did not
   trace what else consumes `RestoreOutcome` or how many evidence JSONs in `docs/evidence/` change
   when the audit line gains fields. Eight of those files are already modified in the working tree
   by someone else. **Round 3 largely closes this.** E2 is withdrawn, so its cost is zero. E1 is
   costed in §5.4 — one `offline_keys` constant, one `offline_meta` call, one small `pub fn` and
   one extra read-only open, with the two rejected carry routes priced beside it — and its
   correctness rests on a *trace* of where floor and ref could diverge, which found no such path
   and is named as a trace rather than a proof. The audit line's cost under E3/Shape G is now
   counted against M5-81, M5-123 and both M6-35 rows (§5.5). **Still uncounted:** the
   `docs/evidence/` JSONs, and what else consumes `RestoreOutcome`.
10. **The lead's amendment draft** (`adr-0027-g13-amendment.md`). Read for context, not edited.
    **It contradicts one thing in this document, and this document is the wrong one:** §5.2b step 8
    said the in-band repair route does not exist; the amendment says the policy poller repairs it
    in band, disproved by a run. Withdrawn at §5.2b. Nothing else in the draft contradicts anything
    I found — I re-checked its evidence table and every row I could check at `395d535` holds
    (`backup.rs:880-889`, `policy.rs:230-235`, `policy.rs:767-772`, `run.rs:1136-1138`,
    `policy.rs:960-963`, `policy.rs:314-319` vs `:358`, `run.rs:849-850`, `ADR-0027:407-427`). Two
    rows I could **not** check and am naming: "Floor after a real restore is `None`" and the
    reproduction itself, which rest on the lead's run of a real daemon — the brief forbids
    `cargo`, so both are second-hand here. **One caution on the draft's own text, not a
    contradiction:** its evidence row "Charter stated three times" cites
    `config-server/src/backup.rs:105` as the third statement. `:103-107` is the field's `null`
    paragraph; it states the **gap**, not the charter — and it is the passage whose G-09 condition
    has now expired (§5.4 E1). Citing it as a statement of the charter would carry a stale clause
    into the amendment that exists to remove stale clauses.
11. **Nothing in round 3 was compiled either**, and two new claims are the exposed ones: §1.4b's
    account of which `match` shapes break on an append (a reading of Rust's exhaustiveness rule,
    with no in-crate `match` on these types existing to test), and §4.4's corrected arithmetic for
    M7A-43/46 (a reading of `clock.rs:91-107`, `dispatch.rs:148-162` and `time.rs:123-125`, not a
    run).

---

## 9. What round 3 changed, as one list

Every entry is a correction the critic or the lead proved, or an admission one of them forced.
Nothing here is a new design.

| # | Section | Change | Status |
|---|---|---|---|
| A | §1.3, §1.7, §0 row 4 | Both new leaves **drop `Copy`**; derive set re-checked payload by payload; `Copy` justified as a census with its scope attached (R2-4, R2-11) | done |
| B | §1.4, new §1.4b, §0 row 1 | `#[non_exhaustive]`'s in-crate guarantee **withdrawn**; the shape re-argued without it and it survives; convention plus two costed alternatives (R2-5) | done, **no design change** |
| C | §1.2, §1.5, §0 row 2 | Residue table **re-derived over both whole plans**, not patched: 43 names, `TOO_LARGE` added, ten payload names named as kernel-a's (R2-6) | done |
| D | §0 row 9, §4.4, §4.5, §4.6, §8.4 | CB-9b **does** unblock all thirteen rows; the `clock.rs` "contradiction" withdrawn; the "not in any input" sentence withdrawn and `test-plan-m7-kernel-a.md:1286` cited; R-CB9 inverted from a precondition to a negative obligation (R2-3) | done |
| E | §1.4 placement item 1, new §1.4c | Layering **restated as an unenforced convention**; the subject argument promoted to the deciding one; kernel-b's six pending `event.rs` edits named (R2-7) | done |
| F | §5.2b, §5.4, §5.5, §6, §8 Q-3 | G-13's delivered scope rewritten: **E2 void**, **E1 live and corrected twice**, refusal-reports-its-numbers and floor-reading added, `/health` surface ruled (L-R88), in-band-repair claim withdrawn | done-with-modification — see below |
| — | §1.7, §4.5 sizing | Six CB-7 edit sites, not four; `EventKind`'s line and derive list corrected (R2-9) | done |
| — | §4.5 argument 3, §5.5 | R2-12 (`support::ctx()` residual path), R2-8 (audit-line field set), R2-10 (D1's Rule-2 sentence) | done |
| — | §0.1 | **Rule 3** added, with four new instances tabulated — three of them mine or the lead's | done |
| — | §10 | Citation audit | done |

**The one place I did not simply execute the brief.** Correction F's item 3 sizing. The lead sized
E1's seeding as "one `offline_keys` constant plus one `offline_meta` call". The **read** is that
cheap; the **carry** is not free, because `backup_offline` receives only a `SnapshotHeader`, and
that struct is the postcard-positional on-disk snapshot format (`snapshot.rs:210-221`, "this
declaration *is* the on-disk layout"). §5.4 costs the three routes and recommends the one that
changes no format and no existing signature. **A sizing correction inside the item, not a rejection
of it.**

**What did not change and was not reopened:** the five arms; CB-8's withdrawal and its three
replacement assertions; §5.1–§5.3's charter argument and its STOP; the five
`restore_into_fresh_store` call sites, untouched under Shape E without E3.

---

## 10. Citation audit

The brief asked: for every `file:line` in this document, confirm enough of the surrounding source
was opened to support the claim attached to it. The lead's mid-round correction extended it —
**a retraction is a claim and needs the same evidence** — and that extension found the most
valuable item in the audit.

**Method.** Each citation re-opened with its *neighbourhood* rather than its line: the whole doc
comment, the whole enum, the whole table cell, or the function and the lines above it. Counting a
citation as a distinct `file:line` or `file:a-b` span rather than an occurrence, the document
carries **118**; I re-opened **61**. The 57 I did not re-open are ones whose neighbourhood I had
already opened this round for a different claim, or `rocks.rs` spans taken by
`git show HEAD:` (that file is dirty; §8 item 7). Every number below is a re-opened one.

**Nine changed. Four of them changed a claim, not a line number.**

| # | Citation | What was wrong | Consequence |
|---|---|---|---|
| 1 | `backup.rs:96-100` | the field doc **continues at `:103-107`** with a condition on gap G-09 | **A claim.** The passage round 2 quoted four times and the lead quoted to withdraw E1. Six lines further reverses the withdrawal. §5.4 |
| 2 | `backup.rs:285` | the rationale is at `:280-284`, immediately above, and also conditions on G-09 | **A claim.** Same reversal. §5.4 |
| 3 | `test-plan-m7-kernel-b.md:501` | the row's "fifteen" is a count *against `ErrorKind`*; the same row names `TOO_LARGE`, and the file names sixteen codes | **A claim.** "Zero residue" was false by one. §1.2, §1.5 |
| 4 | `test-plan-m7-kernel-a.md:1286` | the cell was read for its row list; its prose already states the sample-age mechanism and rules the fix | **A claim.** §4.4's "not in any input" is false. §4.4 |
| 5 | `rocks.rs:197` (HEAD) | the constant's doc at `:191-196` states the absent/zero collapse as a **decision**, with the cost it avoids | **A claim's framing.** §5.5 owed D1 a Rule-2 sentence. §5.5 |
| 6 | `run.rs:422` | the `policy_version` read is `run.rs:393`, under the comment at `:388-392` | line — and the comment is the third documented `None` |
| 7 | ~~`seams.rs:297`~~ | **WITHDRAWN 2026-09-22.** This row was itself the error. `:296` is the `match` head, `:297` is `Some(*reason)`, `:298` is `_ => None` — so the original `:297` was correct and this row "corrected" it into a wrong one, and "corrected" the critic's correct R2-9 citation with it | An audit row that manufactures the defect it audits for. Same mechanism as L-R90: the line was quoted, the span around it was not read |
| 8 | `event.rs:151` | `EventKind` is at `:152` and also derives `Serialize, Deserialize` | line, plus an incomplete derive list |
| 9 | `envelope.rs:527` (the critic's, for `TooLarge`) | `AppendReject::TooLarge` is at `:530`; `:523` is the derive, `:524` the enum | line |

**Two counts I could not reconcile, named rather than smoothed.** §1.2 reports 64 `Fact(`
occurrences where the lead and the critic each report 45; the distinct-name count agrees at 27
across all three, so nothing depends on it, and I did not chase which filter produced 45. And the
brief says eight `AppendReject` variants carry fields; I count **nine** — `StaleGeneration`,
`NeedLineage`, `StaleEpoch`, `UnknownEpoch`, `StaleConfig`, `NeedConfig`, `CorruptHistory`,
`DivergentHistory`, `NeedPrefix`. Nine strengthens the argument it was made for.

**The finding, stated as the brief asked for it.**

> **Four of the nine changed a claim, and all four are one mechanism: the cited line was read, and
> the thing immediately next to it was not.** Not one is a wrong search, a missed directory or an
> unopened file. In every case the correct source was already open. `backup.rs:96-100` and
> `:280-284` are the same file, six lines and five lines from what was quoted.
> `test-plan-m7-kernel-b.md:501` and `test-plan-m7-kernel-a.md:1286` are single table cells, each
> read for one column and cited for the cell.
>
> **So `file:line` is the wrong unit of evidence, and that is the round's finding.** A line number
> is precise enough to look authoritative and narrow enough to have dropped the sentence that
> governs it. F-7 named the scope failure in *searches*; this is the same failure in *reading*, and
> it is harder to see — a search's scope is visible in the command, a read's scope is visible
> nowhere. Rule 3 (§0.1) is the check, and the only mechanical form of it I can offer is the one
> this audit used: **cite the span you read, not the line you quote.**
>
> **The most valuable item is the one the lead's extension caught, and it would not have been
> caught otherwise.** Items 1 and 2 were found only because the audit was turned on a
> *retraction*. E1 was withdrawn on evidence that was real, quoted accurately, and cut one clause
> short — so the withdrawal read as better-sourced than the claim it withdrew, and nothing in the
> ordinary review path questions a withdrawal. **A retraction audited less carefully than an
> assertion is how a correct item is deleted by a correct-looking process.** It is the first time
> this milestone that the mechanism produced a **false negative** rather than a false positive, and
> a false negative leaves no artifact to find later.
