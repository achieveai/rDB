# Critic — kernel-a M7 test plan, round 3 (of the critic series; reviewing planner round 4)

**Target:** `docs/testing/test-plan-m7-kernel-a.md`, 955 lines, 174 rows, drift basis `ec610f4`
**Basis I read against:** `crates/rdb-core/src/contracts/*` and `crates/rdb-sim/src/sim/control.rs`; `git log -1 -- crates/rdb-core/src/contracts` ⇒ `ec610f4` (confirmed newest; basis marker is fresh)
**Method:** source first, claim second. No cargo run. Read-only on every file the planner owns.

## Verdict: **FAIL**

Not because the plan is unsound — §15 was genuinely re-derived this round and the three drifts the
planner self-reported are all correct against source. It fails because the same method gap that
produced them survives in a second place the round-4 re-read did not sweep: **the control-seam
effect vocabulary**. One row (M7A-32) asserts a count of an effect shape the landed contract
cannot construct, so it passes trivially and the ADR 0008 §7 item 4 row §12 claims covered is not
covered. Four sibling rows name the same unconstructable shape. Separately, §11 still over-holds
two rows on a type neither asserts — the exact failure mode this round existed to remove.

Eight findings, TD-12..TD-19, plus one advisory TD-20. TD-12 is the blocker.

---

## TD-12 — `Control(Get{family})` does not exist and cannot; M7A-32 is a vacuous assertion — **BLOCKER**

**Criterion:** a row asserts against a shape the landed contract can produce (KA-1; hard rule 5
"never lower an assertion" — an assertion that cannot fail is lower than none).

**Rows:** M7A-28, M7A-31, M7A-32, M7A-123, M7A-126. (The `Get{grant}` cells of M7A-30 and M7A-57
are *correct* — see below.)

**Source that settles it** — `crates/rdb-core/src/contracts/control.rs:329-363`:

```rust
pub enum ControlEffect {
    Cas { key: ControlKey, expected: Option<Revision>, value: Option<Bytes> },
    Get { key: ControlKey },
    Watch { prefix: ControlPrefix, from: Revision },
    Reload { prefix: ControlPrefix },
}
```

and, at `control.rs:46-52`, the reason it is two types:

> "A separate type from [`ControlKey`] on purpose (finding K-F-19). A key names one record; a
> prefix names every record of one family. Typing the family position with the record type let a
> single-record key be passed where a family was meant... With this type the compiler refuses the
> first."

`ControlKey` (`control.rs:25-44`) has no family member: `ClusterSchema, Node(NodeId),
Grant(NodeId), Partition(PartitionId), Route(RangeId), Operation(OperationId), PlannerGrant`.
The coherent family read is `Reload { prefix }`, and `control.rs:354-358` says so explicitly:

> "Read one whole family coherently at one revision: the only sanctioned answer to a gap
> (spec §7.1)... Completes as [`ControlEvent::FamilySnapshot`]... **Team kernel-a's
> `ReadFamily { prefix }` binds to this.**"

So design §2.4's `ReadFamily` binds to `Reload`, not to `Get`. `Control(Get{family})` is refused by
the compiler by deliberate construction.

**Why it is a blocker and not a typo.** M7A-32's whole assertion is
`count of Control(Get{family}) == 0`. Against the landed contract that count is zero in every run,
including a run where the kernel reloads on every `Watched` — which is the bug ADR 0008 §7 item 4
exists to catch. §12 maps that ADR item to M7A-32 and reports it covered. It is not. The same
reading makes M7A-31's `zero Get{family}; zero Reload` two assertions of which one is vacuous, and
M7A-129 ("= M7A-31 with `Reload` counted") redundant with the half of M7A-31 that is real.

M7A-28's effect list `[Control(Get{family}), Control(Watch{..})]` and M7A-123/M7A-126's
`Get{family}` inputs are compile errors, which a developer catches at the keyboard. M7A-32 is not;
it goes green.

**Closure:** every `Get{family}` in the plan becomes `Reload{prefix: <family>}`; M7A-32's count is
over `Control(Reload{..})`; M7A-31 and M7A-129 are reconciled so the plan does not assert the same
count twice; §15 gains a row recording that design's `ReadFamily` binds to the landed `Reload`, so
the next re-read does not have to rediscover it. `Get{grant}` (M7A-30, M7A-57) needs no change —
`Grant(NodeId)` is a real `ControlKey`.

---

## TD-13 — §14 contradiction 1 still reports `ReplyEffect::Read` absent — **MATERIAL**

**Criterion:** no stale absence claim survives the round-4 re-read (the stated purpose of TD-07).

**Section:** §14, "Source state", contradiction 1.

**The text:** "**Contradiction 1 — `ReplyEffect::Read`.** F-R7 says it exists;
`crates/rdb-core/src/contracts/event.rs:166` shows `Transaction, Status, Failed` at the time of the
grep. §13 Q-9."

**Source:** `event.rs:213-226` has the variant. `event.rs:166` is now inside `EventKind`'s doc
comment for `ExternalFenceVerified` ("Spec §7.2's fallback: an operator or platform mechanism
verified..."), not `ReplyEffect` at all. The file is 402 lines.

The planner updated contradiction 4 in the same section to "**settled by landed code**" and left
contradiction 1 as written. §15 row 4 and §13 Q-9 both record the correct fact; §14 contradicts
both. A developer who reads §14 first — it is the section titled "source state" — concludes the
variant is missing and holds a row that can be written today.

**Closure:** contradiction 1 is rewritten in contradiction 4's form: settled, the variant landed at
`event.rs:218`, the residual is the *shape*, see §15 row 4 and Q-16. The `event.rs:166` citation
goes.

---

## TD-14 — §11 holds M7A-111 and M7A-118 on a type neither asserts; M7A-105 asserts it and is not held — **MATERIAL**

**Criterion:** §11 holds a row only on a dependency the row actually needs — the over-hold rule the
round-4 §11 rewrite states in its own preamble ("holding them was the defect, not the rows").

**Section:** §11, row "M7A-107..M7A-109, M7A-111, M7A-118 | **C0** `SnapshotId::at` ...". The
absence claim itself is correct: `ids.rs:96` has only `SnapshotHandle(u64)` and `grep -rn SnapshotId
crates/` returns zero hits. The *membership* of the held set is wrong, in both directions.

- **M7A-118** asserts `ReplyEffect::Status{identity, status}` and a log line. No snapshot anywhere.
  `ReplyEffect::Status{identity, status: TxnStatus}` is landed (`event.rs:198-204`); `TxnStatus` is
  landed (`txn.rs`, four variants as §15 row 1 says). §15 row 2 says in terms: "M7A-118 asserts the
  landed shape and reads `status` through KA-9's mapping." §11 holds the same row as unavailable.
  The plan contradicts itself about one row in two sections.
- **M7A-111** asserts the waiter cap ⇒ `OVERLOADED` and one `Read`-failure reply per waiter on
  freeze. `ReplyEffect::Failed{identity, error}` is landed (`event.rs:205-212`). No snapshot
  identity is asserted.
- **M7A-105**, which *does* assert `snapshot SnapshotId::at(g, 5)`, is **not** in §11 at all
  (checked with `awk` bounded to §11: it returns M7A-107, M7A-111, M7A-118 and no M7A-105).

Two rows that can be written today report `unavailable` and are counted missing by §12; one row
that cannot be written today is counted runnable.

**Closure:** M7A-111 and M7A-118 leave the `SnapshotId::at` line (M7A-111 to `none`, M7A-118 to
`KA-9`); M7A-105 joins it; §12's missing count moves with them.

---

## TD-15 — KA-2's `ControlOp::PlanCas{outcome}` omits the `node` field that exists to make two-node races expressible — **MATERIAL**

**Criterion:** where §1 names a landed type's fields it names them correctly. This is the
`ReplyEffect::Read` class of miss: the type exists, so a presence check passes.

**Section:** KA-2, which claims "**Eight ops, verified against the crate at `ec610f4`**".

**Plan text:** "`ControlOp::{PlanCas{outcome}, PlanReadUnavailable, EmitWatch, EmitProgress,
TerminateWatch, Compact, DelayCompletion{node, by_millis}, DropCompletion{node}}`".

**Source** — `crates/rdb-sim/src/sim/control.rs:61-66`:

```rust
    PlanCas {
        /// Whose next CAS.
        node: NodeId,
        /// What it will report.
        outcome: CasOutcome,
    },
```

with the doc comment above it: "Per node (finding K-F-14): one racer's report can be forced while
the other's proceeds."

The count of eight is right; the one variant KA-2 spells with fields is the one it gets wrong, and
it drops the field K-F-14 was raised to add. It matters for content, not tidiness: M7A-127
(`route_cutover_one_winner` — "two cutover `Cas` on the same `Route` key at the same expected
revision; exactly one `Committed`; the loser `Conflict`") and §3.5's takeover rows are two-node
races that cannot be scripted without naming whose CAS is being forced. Row cells inherit the
node-less spelling (`PlanCas{Conflict}`, `PlanCas{Unknown}` in M7A-119/M7A-120 — harmless there,
single node; not harmless in M7A-127).

**Closure:** KA-2 spells `PlanCas{node, outcome}`; M7A-127's Input names the node per CAS.

---

## TD-16 — KA-1 still lists `ControlTime` with three fields; §15 row 7 lists four — **MATERIAL**

**Criterion:** §1 and §15 agree about the landed surface at one basis.

**Section:** KA-1, third paragraph.

**Plan text:** "...`rdb-core`'s `EventKind`, whose time is `StepCtx.now` plus `ControlTime {
estimate, error_millis, bound_established }` (T-A-11)."

**Source** — `crates/rdb-core/src/contracts/time.rs`, `ControlTime` has four public fields:
`estimate: Tick`, `error_millis: u64`, `bound_established: bool`, **`sampled_at: Tick`**, the last
doc-commented "An estimate established long ago is not an estimate; without this field a bound
stayed `established` for ever."

§15 row 7 corrects this and calls out the three-field listing as the round-3 error: "**four**
fields, not the three the round-3 table listed". KA-1 *is* that round-3 three-field listing, left
in place. Q-12's own text says the `at = ct.sampled_at` clause is load-bearing and that a seam
stamping `at` at delivery makes M7A-43 unreachable. A developer reading KA-1 — the section that
says "A row that reaches for `Instant`, `SystemTime` or a sleep is a defect", i.e. the one read to
learn how time arrives — does not see the field the clause turns on.

**Closure:** KA-1's parenthetical becomes `ControlTime { estimate, error_millis, bound_established,
sampled_at }`, with a pointer to Q-12 for the conversion rule.

---

## TD-17 — M7A-05's Input hands the kernel a watch change carrying a value; the landed watch stream carries no body — **MATERIAL**

**Criterion:** a row's Input is constructable from landed types.

**Row:** M7A-05, `lineage_watch_event_reads_never_mutates`. Input: `Watched{key: Partition p1,
value: owner=other}`.

**Source** — `control.rs:296-307`:

```rust
/// Structural on purpose (finding K-F-13, ADR-rdb-0008 §4). A watch invalidates a cache; it
/// never delivers the record, because a record delivered on a stream that is allowed to gap and
/// to be delivered late is a record the kernel would act on without a linearizable read.
pub struct ControlChange {
    pub key: ControlKey,
    pub revision: Revision,
}
```

and `ControlEvent::Watched { prefix, cursor, changes: Vec<ControlChange> }` (`control.rs:384-391`).
There is no value on the watch path, encoded or otherwise, at any level.

The row's *assertion* is right and is the important half — `effects = [Control(Get)]`, state
byte-identical — and the landed contract makes it stronger, not weaker: the kernel cannot see the
new owner even if it wanted to. But the Input cannot be built, and `value: owner=other` is doing
rhetorical work ("it saw another owner and still only read") that the seam forbids.

This is not the §15 row 8 translation pattern. There, `ControlRecord.value: Bytes` exists and A1
decodes it. Here there is no body at any layer.

**Closure:** Input becomes a `Watched` carrying `ControlChange{key: Partition(p1), revision: r}`
where the record *behind* `r` names another owner; the assertion is unchanged.

---

## TD-18 — the re-watch resumes one revision too late, and the landed doc says so — **MATERIAL**

**Criterion:** a resumed watch loses no change (ADR 0008 §4 coherent resync — the rule M7A-28
exists to prove).

**Rows:** M7A-28 (assertion `[Control(Get{family}), Control(Watch{from: snapshot_revision+1})]`),
M7A-123 (restates "M7A-28's re-watch starts at `snapshot_revision + 1`").

**Source** — `ControlEffect::Watch` (`control.rs:344-353`): "Start or resume a watch on one family,
delivering changes **after** `from`." And `ControlEvent::FamilySnapshot.snapshot_revision`
(`control.rs:413-419`): "The `snapshot_revision` is what the resumed watch **starts after**, which
is what makes reload and re-watch a closed loop rather than a race."

`from` is exclusive. `from = snapshot_revision` delivers every change strictly after the snapshot —
the closed loop. `from = snapshot_revision + 1` delivers changes after `snapshot_revision + 1`,
silently dropping any change at exactly `snapshot_revision + 1`. A one-revision gap in the resync
path, introduced by the row that certifies the resync path has no gap, and it goes green: the
fixture would have to place a change at exactly that revision for the row to notice.

**Where I cannot settle it.** The exclusivity reading of the doc is unambiguous and the intent
("starts after") corroborates it. What I cannot confirm from reading is whether foundation's watch
*implementation* honours it — there is no implementation of the watch seam in `crates/rdb-sim` to
check. If foundation later implements `from` inclusively, this row is right and the contract doc is
wrong. That is a seam question, not a plan question. Raising it either way.

**Closure:** M7A-28 asserts `Watch{prefix, from: snapshot_revision}`, M7A-123 follows, and §15 or
§13 records the exclusivity as the foundation seam assumption the row rests on.

---

## TD-19 — M7A-169's `Err`-completion twin is stated as settled fact; nothing in the plan marks it open — **MATERIAL**

**Criterion:** a question routed to the architect is marked open in the plan, not answered by
assumption (§13's own stated contract: "each has a default that is what you implement if the lead
does not answer first").

**Row:** M7A-169, near-miss twin: "a batch that completes `Err` while frozen ⇒ **no**
`RetainDedup`, and the retry re-executes (one fact: the completion result)."

**What I checked:** `grep -n "M7A-169"` over the plan returns four hits — the §file-mapping row, the
§2 class list, the row itself, and the §12 gate map. §13 has no entry for it; Q-15 and Q-16 are the
two round-4 additions and neither is this. The planner's handoff says, under "Not mine this round":
"M7A-169's `Err`-completion ambiguity. Routed to the architect by the coordinator. Untouched." The
routing is recorded in the handoff and nowhere in the deliverable.

I am not settling the ambiguity — per instruction. I note only why the marking matters: M7A-82
asserts `BatchCompleted{Err}` ⇒ `Reply(UNKNOWN_OUTCOME)` + `Frozen{LocalStorageFenced}`, and
`RdbError::UnknownOutcome`'s landed retry rule is `RetryRule::QueryStatus` — "Query status with the
*same* identity. Never generate a fresh request id." (`errors.rs:45-66`). "The retry re-executes"
is at least in tension with that, which is presumably why it was routed. A developer implementing
M7A-169 today has no signal that the twin is contested.

**Closure:** §13 gains a Q-17 stating the question and the default the twin is written against, and
M7A-169's cell points at it — the treatment Q-15 and Q-16 got.

---

## TD-20 — M7A-121's `TerminateWatch{k}` / `WatchTerminated{k}` spelling — **ADVISORY**

`ControlOp::TerminateWatch{node, termination}` ends *all* of a node's watches; the completion is
`ControlEvent::WatchTerminated{prefix, from, termination}` (`control.rs:400-409`). The row's `{k}`
reads naturally as an index over the five terminations, in which case it is shorthand and fine; it
also reads as a key, in which case it is wrong in both places. The five-termination count is
correct (`RevisionCompacted{minimum_available_revision}, ResourceExhaustedResumable,
ResourceExhaustedFatal, NotLeader, Unavailable`). One word of disambiguation, not a defect.

---

## R15 — settled: M7A-172's `else`-arm sub-case stays

The planner's risk was "if the landed type makes it unconstructable, that sub-case must be deleted
with a stated reason rather than left to fail silently."

**It is not unconstructable, and the answer is an absence.** `grep -rn "RetainedStatusMap" crates/`
returns **zero hits** at `ec610f4`. No landed type constrains it, so nothing landed can make
`RetainedStatusMap{retained_through: 10, discarded_from: None, uncertain: false}` with a query at
seq 11 unbuildable. The row builds the map by hand — its Input says so ("direct table test of
`fold_recovered`") — and §11 holds it on kernel-b F1's recovery event, correctly.

The risk converts into a standing condition rather than closing outright: **when `RetainedStatusMap`
lands, re-derive this sub-case.** If F1 declares `discarded_from: Revision` rather than
`Option<Revision>`, the `None` arm disappears and the sub-case must go with a stated reason. Worth a
line in §15 so the next re-read finds it.

Secondary check on the same row, since TD-05 rewrote it: M7A-172 asserts `StatusExpired` on the
state side. `TxnStatus` has `Expired`, not `StatusExpired`; `ErrorKind` has `StatusExpired`
(`errors.rs:113`); design §1.4's five-member `Outcome` has `StatusExpired` per §15 row 1. The row is
a direct test of `fold_recovered`, which is state-side, so `StatusExpired` is the right spelling and
KA-9's table maps it to `TxnStatus::Expired` at the wire. **Correct as written.**

## R14 — the §15 table was re-derived, not just re-hashed

I checked this by re-deriving it myself rather than trusting the paragraph that says it was done.
**All eleven rows verified correct against source; none stale.**

| §15 row | Claim | Verified |
|---|---|---|
| 1 | `txn::Outcome` two unit variants; `TxnStatus` four | `txn.rs` — `Published`, `RecoveredApplied`; `Resolved(TxnResult)`, `Unresolved{seq}`, `Unknown`, `Expired` |
| 2 | `ReplyEffect::Status{identity, status}` | `event.rs:198-204` |
| 3 | `NodeLifecycle::{Resumed{suspended_millis}, Rebooted{boot}}` at `event.rs:135` | exact, enum opens at line 135 |
| 4 | `Read{identity, outcome: ReadServiceOutcome, value: Option<(Version, Digest)>}` at `event.rs:218`; `ReadServiceOutcome` at `trace.rs:362`; `ids.rs:96` only `SnapshotHandle(u64)` | all three line numbers exact |
| 5 | `ControlOp` eight variants, last two `DelayCompletion`/`DropCompletion`; `EvidenceRef([u8;32])`; no `FencingProof` anywhere in `crates/` | counted eight in `sim/control.rs`; `grep -rn FencingProof crates/` ⇒ zero hits |
| 6 | `authority::Checkpoint` five, `trace::AuthorityGate` four, no `OutboxDispatch` on the latter | `authority.rs:22-33`, `trace.rs:244-253` |
| 7 | `ControlTime` four fields | `time.rs` — but §1 was not updated with it, see TD-16 |
| 8 | `ReadOutcome::Found{revision, value}` at `control.rs:230`; no `GrantRecord` | enum opens at 230, `Found {` at 232; `grep GrantRecord` ⇒ zero |
| 9 | `is_stale` returns `sampled_at.0 > now.0 \|\| now.0 - sampled_at.0 > max_sample_age_millis` — strict `>` | `time.rs`, verbatim |
| 10 | `DenyReason` 15 variants, no `ConfigVersionChanged` | counted 15 in `authority.rs`; names match the plan's list exactly, in order |
| 11 | `AuthorityView.past_horizon: DenyReason`, bare | `authority.rs` |

The basis itself is honest: `git log -1 -- crates/rdb-core/src/contracts` ⇒ `ec610f4`, which is also
`git log -1 -- crates/`. The marker is a single line and §15's prose refers to it without repeating
the literal, so the drift stage's occurrence count is satisfied.

**The gap R14 predicts is real but is not in §15.** It is in the sections §15 does not cover: §1's
architecture requirements (TD-15, TD-16), §14's contradiction list (TD-13), and the row tables
themselves (TD-12, TD-17, TD-18). The drift stage checks the basis; §15's re-read checked §15. The
five findings above live outside both. That is the concrete shape of R14 and it is worth recording
as such: **a re-read scoped to §15 leaves §1, §11, §14 and the row cells stale, and those are where
the compile errors are.**

---

## What I checked and found correct

**Absence claims — every one in §11 and §15, opened and confirmed.** `grep -rn` over `crates/`
returns zero hits for: `FencingProof`, `RetainedStatusMap`, `QualificationChanged`,
`ReplicationView`/`qualifies_now` (one doc-comment mention at `membership.rs:70`, no type),
`BlockPartition`, `RecoveryResult`, `AdmissionState`, `SnapshotId`, `GrantRecord`,
`ConfigVersionChanged`. `ErrorKind` has 18 variants and `DivergenceRequiresOperator` is not among
them — the §11 cell for M7A-69(b)/(c) is right, and calling it a kernel-b dependency (B-R31 item 18)
rather than a finding against this plan is the right disposition. **No stale absence claim found in
§11.** The 39 releases are all justified.

**The three drifts the planner self-reported are correct.** `ReplyEffect::Read` is a shape drift;
`FencingProof` is A1's own (and `EvidenceRef` is the input handle — `authority.rs` doc: "Opaque
handle to external fence evidence... The kernel cannot verify the external fact"); `is_stale`'s
strict `>` settles §14 contradiction 4 in favour of M7A-46 (`age == 2000` not stale) and M7A-43
(2001 stale). No row moves. `TICK_MILLIS = 1` confirms the millis/ticks residue is harmless.

**Shape claims checked one at a time and found right:** `CasOutcome::Conflict{exists, current}`
carries no value (M7A-119); `CasOutcome` has four variants so M7A-120's three-way distinction is
real; five `WatchTermination`s (M7A-121 count); `ReadOutcome::Unavailable` exists (M7A-122);
`FamilySnapshot{prefix, snapshot_revision, records}` carries `snapshot_revision` (M7A-123's
assertion half — its Input half is TD-12); `EffectKind::AdoptAuthority{partition, generation,
owner_epoch, config_version}` matches M7A-04/M7A-162 field for field;
`EventKind::ExternalFenceVerified` carries all six K-A-37 binding fields, matching M7A-149;
`AuthorityDecision` carries `authority_seq`, `checkpoint`, `correlation`, `decided_at` and
`same_lineage_as`/`same_lineage_as_view`, matching M7A-60 and M7A-174's conjunct;
`AuthorityView.{authority_seq, valid_through_tick, past_horizon, config_version}` all present,
matching M7A-166 and §15 row 10's "a config-version change reaches the rows through
`AuthorityView.config_version`"; `Checkpoint::OutboxDispatch` is landed and doc-commented "Declared
for spec §11; unused in M7", so §14 contradiction 3 and M7A-61 are right; `PartitionMode` four
members with `Blocked{reason}` and `BlockReason::DivergenceRequiresOperator{diverged: Vec<CopyId>}`,
matching KA-7 and M7A-158/160; `Budgets` has ten fields and `BudgetName::ALL` has exactly ten
members in field order.

**Ownership claims checked against design rather than against §11's own table:** `FencingProof` as
A1's — confirmed, and the reasoning ("a type the package under test declares is not an external
dependency") is sound. `RetainedStatusMap` as kernel-b F1's, `AdmissionState` as kernel-b L1's,
`QualificationChanged` as kernel-b's, `ControlOp` and the conformance suite as foundation H1's,
namespace inventory as M1's + O1's, `ReplicationView` fake as **kernel-a's own** (KA-8, correctly
listed under "no external dependency"). **No second wrong-owner hold found.** The `Budgets` doc
comment independently corroborates §2's placement of `max_sample_age_ticks` with kernel-a: "The
clock sample age is deliberately not here: it is the caller's budget... and kernel-a's A1 passes its
own."

**TD-01..TD-11 spot-verified** (not exhaustively — see below): TD-03's nine-trigger list contains no
`ConfigVersionChanged` and maps each trigger to a landed `DenyReason`; TD-04's bare `past_horizon`;
TD-05's `StatusExpired` arm is present and the narrowing is stated; TD-08's §15 row 8 is rewritten
and its claim that M7A-107..111 never assert `{revision, value}` is true; TD-09's §15 row 6 names
both landed enums and correctly identifies which one Q-42 filters; TD-10's KA-9 paragraph plus
M7A-115's wire half exist and the `Unresolved` unreachability argument holds against landed
`TxnStatus`; TD-11's §12 uses row titles. TD-02, TD-06 and TD-07's §12/§14 edits: text present and
internally consistent, not independently re-derived against the ADRs.

**M7A-169's open question:** confirmed the plan does not assume an answer beyond the twin's wording,
and confirmed the twin is the only place the ambiguity appears. I did not attempt to settle it. The
only issue is that it is unmarked (TD-19).

---

## What I did not reach

1. **The ADRs and `design.md` were not read.** I checked the plan against `crates/` and against the
   design *as the plan quotes it*. Every finding above is settled by a source file in `crates/`.
   Claims that turn on ADR 0007 §3's nine-row trigger table, ADR 0004's row titles, design §3.4's
   mapping, `design.md` line 1889, or K-A-52's three-way rule — TD-01, TD-02, TD-03's
   trigger-to-scope split, TD-06, and the §12 gate map — are **unverified by me**. Round 4's
   evidence for them looks internally coherent; I did not confirm it.
2. **§12's gate checklist was not swept.** I checked only the criterion TD-12 breaks (ADR 0008 §7
   item 4 → M7A-32) and TD-11's use of row titles. The rest of the criterion-to-row mapping is
   unaudited, and the "0 missing" line moves if TD-14 is accepted.
3. **§9's DuckDB queries (Q-41..Q-45) were not checked** beyond §15 row 6's finding that
   `checkpoint='StorageDispatch'` matches `authority::Checkpoint`. The SQL, the field list against
   KA-4's closed set, and Q-45's redaction predicate are unread.
4. **§8.1–§8.6 (M7A-138..M7A-164) were read for landed-type mentions only**, not row by row. They
   were cleared by critic round 3 and I took that. §8.7 I read in full.
5. **§3.1–§3.4 (M7A-01..M7A-50) coverage is partial.** I read §3.5, §3.6, §4, §5, §6, §7 and §8.7 in
   full and grepped §3.1–§3.4 for landed-type mentions. TD-17 and TD-18 came out of that grep; a
   full row-by-row read might find siblings, and given TD-12's spread across five rows I would
   expect one or two.
6. **No build.** Every finding is a reading of source, as instructed. TD-12 and TD-17 predict
   compile failures I did not observe. TD-18 is the one place where reading cannot finish the job —
   it needs a foundation watch implementation that does not exist yet.

## Challenges to lead rulings

None. Nothing I found requires re-litigating A-R10..A-R26, B-R20/21/27/29/31 or F-R3/F-R7/F-R8/F-R10.
