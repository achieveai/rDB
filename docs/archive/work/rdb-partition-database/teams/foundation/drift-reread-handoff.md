# M7 contract-drift re-read — handoff

Date 2026-09-22. Branch `feature/rdb-m7`. Scope: §15 of the four M7 test plans, re-read against
`crates/rdb-core/src/contracts/`.

## Headline

**No marker moved, and none could.** `git log -1 --format=%H -- crates/rdb-core/src/contracts`
is `f616ddf449bdaf6b690010909675648f5d436dca`. All four plans already declared `f616ddf`. The
`drift` stage is green today and was green before this pass.

**The thing the stage cannot see is real and is in the tree right now.** Ask **CB-7** is being
implemented, **uncommitted**, in the contracts directory:

```
 crates/rdb-core/src/contracts.rs           |   2 +      (pub mod ignore; + the module table row)
 crates/rdb-core/src/contracts/authority.rs | 111 +      (AuthorityIgnoreReason, 27 variants)
 crates/rdb-core/src/contracts/event.rs     |  30 +/4 -  (Copy dropped; Ignored retyped)
 crates/rdb-core/src/contracts/ignore.rs    | 213        (untracked: KernelIgnoredReason, ReplicaIgnoreReason)
```

Because it is uncommitted, the marker stays at `f616ddf` and the tables stay licensed. When
dev-foundation-r3 commits, the basis moves and the four §15 tables must move with it — the
re-read is already written, so that becomes one marker edit per plan, not four re-derivations.

## Banked finding — verified, both halves true, one addition

Checked at `crates/rdb-core/src/contracts/event.rs`.

| Claim | Verdict | Evidence |
|---|---|---|
| `KernelEffect::Ignored{reason: ErrorKind}` → `KernelIgnoredReason` | **TRUE** | working tree `event.rs:260`; HEAD `event.rs:242` still `ErrorKind` |
| `KernelEffect` is `Copy` → now `Clone`, not `Copy` | **TRUE** | derive at working-tree `event.rs:246`; HEAD `event.rs:232` has `Copy` |
| *(not banked)* `KernelEvent` **also** lost `Copy` | **TRUE, addition** | derive at working-tree `event.rs:215`; HEAD `event.rs:211` has `Copy` |

Both are **working-tree only**. `git show HEAD:...event.rs` still has `Copy` on both and
`reason: ErrorKind`. So the plans were **not** wrong at their declared basis — they are about to
be, which is a different disposition and is how it is recorded.

`Alert{reason: ErrorKind}` is deliberately unchanged: an operator-visible condition *is*
client-facing vocabulary.

## What CB-7 actually is (it is neither shape kernel-b predicted)

New `contracts/ignore.rs:77` — `KernelIgnoredReason`, five arms, `#[non_exhaustive]`, externally
tagged serde: `Error(ErrorKind)`, `AppendRejected(AppendReject)`, `AckRejected(AckRejectReason)`,
`Authority(AuthorityIgnoreReason)`, `Replica(ReplicaIgnoreReason)`. Foundation owns the arm set
only. `ReplicaIgnoreReason` (`ignore.rs:106`, 12 variants) is kernel-b's; `AuthorityIgnoreReason`
(`contracts/authority.rs:257`, 27 variants) is kernel-a's.

Kernel-b listed twelve `Ignored` names with no `ErrorKind` counterpart. **All twelve now have
one** — ten as `ReplicaIgnoreReason` variants, `NOT_A_MEMBER` and `FORGED_ACK` through the
`AckRejected` arm. `INVALID_CONFIG` and `RECOVERY_ONLY` get their own names back instead of the
lossy `ErrorKind` mapping. M7B-59's requirement (`TOO_LARGE` and `RECOVERY_ONLY` distinct in one
vector) is met by construction: different arms, different Rust types.

## Per plan

### `docs/testing/test-plan-m7-foundation.md` (§15, §15.1, §15.2)

Checked: `Scheduler::pop`; `Completion`; `OpSkipped.scenario_op_index: u32`; `AdoptAuthority`
carrying `partition` (`event.rs:331`); `ProtectionState` 7 fields, no `quorum_rule`
(`trace.rs:1063`); `EventKind` 8 / `EffectKind` 7; `AppendReject` 16 (`envelope.rs:524`);
`NeedPrefix{have, head_digest}` (`:581`); `AppendOutcome` 5 (`:613`); `AckRejectReason` 14
(`trace.rs:315`); `BlockReason` **1** variant (`contracts/authority.rs:198`); `PartitionMode` 4
(`:212`); `DenyReason::ControlUnavailable` (`:80`); `PartitionConfig` `serde(try_from)`
(`membership.rs:61`); `TraceHeader.provenance` (`trace.rs:212`); `ErrorKind` 18, closed
(`errors.rs:79`).

**Every one holds. Every cited span resolves at HEAD exactly.** This table is the most carefully
re-read of the four; its `contracts/` prefix convention (two files named `authority.rs`) is doing
real work.

Stale: nothing at `f616ddf`. One edit made — a dated **CB-7 (working tree)** note appended to the
§15.2 CB-1 row, recording the retype, the dropped derives, the +21 span shift, and that the marker
does not move for an uncommitted change. §14's CB-7 row already predicted this landing would move
the basis; the note closes that loop.

CB-5 re-confirmed **still open**: `BlockReason` has exactly one variant in the working tree as
well as at HEAD. `M7F-38` arm 2 stays `Unavailable` — correctly.

Not touched: §5, §8, §10, §14, §17. §17's withdrawn column sums left withdrawn, as instructed.

### `docs/testing/test-plan-m7-kernel-a.md` (§15, 16 rows)

Checked at HEAD: row 1 `txn::Outcome` 2 unit variants; row 2 `ReplyEffect::Status{identity,
status}` (`event.rs:262`); row 3 `NodeLifecycle` (`:136`); row 4 `Read` (`:281`),
`ReadServiceOutcome` (`trace.rs:399`), `SnapshotHandle(u64)` (`ids.rs:96`), no `SnapshotId::at`;
row 5 `EvidenceRef` (`authority.rs:184`), `grep -rn FencingProof crates/` ⇒ **0** (re-run
2026-09-22); row 6 `Checkpoint` 5 incl. `OutboxDispatch` (`:23-34`), `AuthorityGate` 4
(`trace.rs:244`); row 7 `ControlTime` 4 fields (`time.rs:84-101`); row 8 `ReadOutcome::Found`
(`control.rs:230`); row 9 `is_stale` verbatim, millis, strict `>` (`time.rs:123`); row 10
`DenyReason` **15**, no `ConfigVersionChanged`; row 11 `past_horizon: DenyReason` bare (`:176`);
row 12 `ControlEffect` 4 incl. `Reload` (`control.rs:329`); row 14 `RetainedStatusMap` grep ⇒ 0;
row 15 `TraceKind::AuthorityDecision` (`trace.rs:790`).

**All sixteen hold at `f616ddf`. Every cited line resolves.**

Stale: the **CB-1 bullet** in the basis preamble, on two clauses. Edits made:

1. CB-1 bullet — dated note: `Ignored`'s reason is retyped; *"their variants are kernel-b's"* is
   now wrong for the reason vocabulary because **kernel-a owns `AuthorityIgnoreReason` outright**
   (27 variants, kernel-a edits it alone); neither inner enum is `Copy`.
2. Row 15(c) — dated note. The L-R60 mapping is **not** reopened and no new `EffectKind` variant
   is owed; what changes is that the owed §3 / §8.1–§8.6 rewrite can now pin an `Ignored` reason
   **by value in kernel-a's own vocabulary**, instead of leaving it open or forcing it onto a
   client-facing `ErrorKind`. That was the one thing that would have made the rewrite weaker than
   the rows it replaces. The rewrite is still owed; those rows still report `unavailable`.

Carried into the note: `ignore.rs`'s own rule — **a kernel never destructures another kernel's
leaf**. `#[non_exhaustive]` is cross-crate and every kernel module is in `rdb-core`, so it buys
nothing in-crate; the convention is what recovers it.

### `docs/testing/test-plan-m7-kernel-b.md` (§15)

Checked at HEAD: `AppendAck` (`envelope.rs:495`); `RegularSecondary` + `regular_secondaries()`
(`membership.rs:202`); `Event.at` / `StepCtx.now`; `ControlEffect::Cas` / `ControlEvent::CasResult`;
`CasOutcome` **4**, no `QuorumLost` (`control.rs:196`); `AppendReject` **16** (`envelope.rs:524`);
`NeedPrefix{have, head_digest}` (`:581`); `AppendOutcome` **5** and `Copy` (`:612-613`);
`PartitionMode::Blocked` (`authority.rs:212`); `BlockReason` **1** (`:198`); `TraceKind`
(`trace.rs:748`); `AckRejectReason` **14** (`:315`); `EventKind` 8 (`event.rs:190`), `EffectKind`
7 (`:346`), `KernelEvent` (`:213`), `KernelEffect` (`:234`); `min_regular_acks` on decode
(`membership.rs:61`/`:147`); `ErrorKind` 18 closed (`errors.rs:79`); `DurableProof` absent.

**Every line number cited resolves at `f616ddf` exactly.** The round-6 re-read was honest work.

Stale — four cells, all from uncommitted CB-7. Edits made, each a dated **CB-7 (working tree,
2026-09-22)** note that replaces the claim without deleting it:

1. **Preamble** — "`crates/` clean in the working tree" is no longer true. Records what is
   uncommitted, that all cited spans still resolve at the commit, and that working-tree `event.rs`
   is +21 against them.
2. **CB-1 carrier row** — `Copy` gone from both inner enums (`event.rs:246`, `:215`); `Ignored`'s
   reason is `KernelIgnoredReason` (`:260`); `Alert` unchanged.
3. **`Ignored` reason-codes row (NEW round 6)** — the unnumbered ask is **answered**, by a third
   shape neither candidate predicted. Full mapping of all fifteen names written into the cell.
   Notes that the homograph hazard becomes an `E0308` rather than a convention, and that a
   **fourth** `Quarantined` homograph now exists (`AuthorityIgnoreReason::Quarantined`).
4. **`BlockPartition` / `Copy` row (NEW round 6)** — the derive is dropped under L-R66, so the
   blocker is discharged and `BlockPartition{reason: BlockReason}` compiles as a kernel-b edit.
   **M7B-142 released on the commit, not on the note.** CB-5 explicitly untouched.
5. **`AckRejectReason` row** — its round-6 clause "`Ignored{reason}` is now typed `ErrorKind`" is
   corrected.

**Holds are released on the commit, not on these notes.** Every `Ignored` row and M7B-142 stay
`Unavailable` until CB-7 is committed — stated that way in each cell so nobody reads a note as a
release. M7B-109/110/146 stay `Unavailable` on CB-5 regardless.

### `docs/testing/test-plan-m7-verification.md` (§15 drift table, rows 1–27)

**This is where the re-read found drift the plan's own prose denied.** The 2026-09-21 note claims
rows 1–8 and 10–22 "still hold verbatim" because `f616ddf` touched only `AckRejectReason` inside
`trace.rs`. True of the field names. **False of the line numbers**: widening that enum at
`trace.rs:315` added **37 lines**, so every citation below it moved while its field stayed put.

| Row | Cited | Actual at `f616ddf` | Checked |
|---|---|---|---|
| 6 | `AckEvidence` `trace.rs:606` | **`:643`** | `git show ec610f4:…` has it at 606 — the pre-CB-3 value was carried |
| 21 | `SkipReason` `trace.rs:634` | **`:671`** | same, +37 |
| 21 | `OpSkipped` `trace.rs:1115` | **`:1152`** | same, +37; foundation's §15 cites `:1152-1158` and is right |
| 8 | `partitions: u8` `trace.rs:214` | **`:216`** | wrong at `ec610f4` too; `:214` is `pub config: RunManifest` |

All four corrected in place, each keeping the old value with the date it was superseded. A dated
correction paragraph added above the table explaining the mechanism.

**Why this one matters beyond four numbers.** It is the drift stage's blind spot pointing the
*opposite* way from the failure AGENTS.md describes. Nothing over-held; verification's open-ask
count really is zero; no row was gated on a landed contract. So the usual tell was absent. What
was re-read was the set of field names — which `grep` confirms without ever opening the file at a
line — while the prose licensing the marker said the whole table was re-read. Rule written into
the plan: **cite a span you opened; a field name surviving a commit is not evidence its line did.**

Also verified at HEAD and holding: `Provenance` (`trace.rs:85`), `TraceHeader.provenance` (`:212`),
`RunManifest` (`:187`), `config` (`:214`), `ProtectionPhase`, `ReplicaRole`, `ScenarioId(u64)`
(`ids.rs:116`), `AppendReject` 16 with `StaleGeneration` (`envelope.rs:534`) / `NotAMember`
(`:568`), `AppendOutcome` (`:613`), `EventKind`/`EffectKind` (`event.rs:153`/`:313`),
`AckRejectReason` 14 (`trace.rs:315`).

Row 26 edit: dated note. **Disposition unchanged — adopt.** Grepped 2026-09-22: no row, VA, Q-row
or literal in this plan names any CB-7 type or reads `Ignored`'s reason. `ErrorKind` itself is
untouched at 18 variants, so **drift row 9's `admission_decision.reason: Option<ErrorKind>` and
the `ADMISSION_REASONS` subset are unaffected** — the one place a retype *could* have reached this
plan, checked rather than assumed.

**New standing hazard recorded in row 26.** CB-7 adds three more `#[non_exhaustive]` enums, two of
which (`ReplicaIgnoreReason`, `AuthorityIgnoreReason`) are *designed* to be appended to by a kernel
team without a foundation edit. Unlike `AckRejectReason` at drift row 23, a widening will arrive
with **no contract commit that moves this marker and no ask to notice**. A future row wanting
M7V-56's set-equality treatment for them has no supported way to get it.

## Not done / deliberately left

- **No marker moved.** All four already at `f616ddf` = newest contract commit. Nothing was earned
  by moving one and nothing was withheld.
- Verification's §15 prose says "four new rows (23–26)"; the table carries **five** (23–27).
  Recorded in the plan, not patched — a drift re-read that did not re-derive the count should not
  silently fix a count.
- Foundation §17's withdrawn column sums left withdrawn, per the brief.
- No `src/`, test, or non-§15 section touched. No commit, no push, no gate run.
- The CB-7 note text was **not** shown to dev-foundation-r3; if the implementation changes shape
  before it commits, these five cells need one more pass.

## Assumptions

1. Correcting §15 against **uncommitted** contract source is in scope, because the brief names
   `event.rs:234-268` in the working tree and the banked finding only exists there. Every such
   correction is labelled `CB-7 (working tree, 2026-09-22)`, states that the marker does not move,
   and states that holds release on the commit rather than on the note. A reader who wants only
   committed truth can ignore every cell so labelled and the tables remain correct at `f616ddf`.
2. `git diff` against HEAD in `docs/testing/` includes other agents' uncommitted work (the lead's
   foundation edits to §5/§8/§10/§14/§15/§17, and `test-plan-m6.md`). Only the hunks listed above
   are mine. Verified by hunk position: kernel-a `@@ +1249,19` and `@@ +1313`; kernel-b `@@ +482,13`
   and four single-line cells in 504–516; verification `@@ +1098,31` and four cells in 1136–1153;
   foundation one line, 628. All inside §15.
3. Markdown table integrity checked by pipe count on every added row; no inserted text contains a
   literal `|`. Multi-paragraph cell content uses `<br>`, matching the tables' existing style.

## Recommended status

**COMPLETED_WITH_RISKS.**

Delivered: four §15 tables re-read claim by claim against the contract files; ~60 cited spans
opened at HEAD; eight cells corrected with dated notes across four plans; four stale line
citations fixed in verification.

Residual risks, in order:

1. **CB-7 is uncommitted and could change shape.** Five cells describe it. If dev-foundation-r3
   lands something different, those cells are wrong in the same way the banked finding was.
   Re-check at commit time — it is a read, not a re-derivation.
2. **Whoever commits CB-7 must move all four markers** and shift kernel-a's, kernel-b's and
   foundation's `event.rs` spans by +21. The tables are already written for it; this is the one
   step nothing in the repo forces.
3. **Verification's line drift will recur.** The +37 shift was invisible to a name-based re-read
   and will be invisible to the next one. The durable fix is citing a symbol rather than a line,
   or a check that resolves cited spans; neither exists and neither is in this brief's scope.
