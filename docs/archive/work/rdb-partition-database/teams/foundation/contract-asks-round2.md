# Foundation contract asks — round 2, consolidated

One authoritative list of every open ask the three consuming teams have on team foundation.
Written for the second foundation round. Grouped **by the file foundation has to open**, not by
the team that asked, because that is the order the work happens in. Inside a group, asks with
**no start-now workaround come first**.

**Landed state was read, not inferred.** Every entry cites a file and a line I opened. Code read
at `HEAD = 3051c5d`; `git status` shows `crates/rdb-core` clean and only
`crates/rdb-sim/tests/support/oracle*` dirty (another agent's), so the contracts below are the
committed ones, not a half-edited tree.

Sources, in the order team-rules.md gives them: `ledger.md` rulings (A-R, B-R, V-R, F-R);
`teams/kernel-a/architect-handoff.md` §3 (nine asks, round 4, supersedes round 3 §7);
`teams/kernel-b/architect-handoff.md` §15 (CB-1..CB-4) and `teams/kernel-b/test-planner-handoff.md`
(CB-5, and how each ask is used); `teams/verification/trace-requirements.md` §8 drift table and
§8.1; `teams/*/critic-tests.md`.

## Counts

| | |
|---|---|
| Asks tracked | 18 |
| Already satisfied by landed code | 4 (KA-5, KA-8, VER-TR8-1, VER-TR8-21) |
| Partially landed | 2 (KB-CB-2, KB-CB-4) |
| Absent | 12 |
| Asks with **no** workaround | 1 (KB-CB-5) |
| Conflicts for the lead | 3 |
| Asks that are **code**, not a shape | 2 (KA-9 in `rdb-sim`, VER-CR-3 in `rdb-sim`) |

Ids keep each team's own numbering, prefixed by team: `KA-n` = kernel-a architect-handoff §3 item
n; `KB-CB-n` = kernel-b contract ask n; `VER-TR8-n` = verification trace-requirements §8 drift row
n. Nothing is renumbered and no ask appears twice.

---

## Group 1 — `crates/rdb-core/src/contracts/authority.rs`

### KB-CB-5 — widen `BlockReason` (no workaround; do this first)

- **Shape asked**, verbatim from the kernel-b planner handoff: "`BlockReason` widened by those
  three variants" — `NoEligibleRegular`, `ControlUnavailable`, `ControlUnknown`. "Collapsing them
  into one reason is not an option — design.md 1436–1441 keeps `Unavailable` and `Unknown` apart
  precisely because they are different operator stories, and asserting a bare `Blocked` would
  discard the whole content of the rows."
- **Lands in**: type in `rdb-core`, `contracts/authority.rs`, the `BlockReason` enum.
- **Blocks**: rows `M7B-109`, `M7B-110`, `M7B-146`. Also leaves the four-arm CAS coverage the
  kernel-b §14 gate checklist asks for incomplete — two of the four arms are the blocking ones.
- **Workaround**: **none.** The planner's words: "unlike CB-1 it has no start-now workaround: a
  row cannot assert a reason variant that does not compile." The three rows report `Unavailable`
  until it lands.
- **Landed state**: **absent.** `crates/rdb-core/src/contracts/authority.rs:198` — `BlockReason`
  has exactly one variant, `DivergenceRequiresOperator { diverged: Vec<CopyId> }` (line 202).
  `PartitionMode::Blocked { reason: BlockReason }` is at lines 220–222. The three asked names
  appear nowhere in `crates/`.
- **Ruling**: **B-R34** (adopts CB-5 keeping the planner's number; lead read the enum and refused
  the collapse).
- **Same type as**: **KA-5**, which asks for the opposite width. See Conflict 1.

### KA-4 — `AdmissionState` with `reason: Option<ErrorCode>`

- **Shape asked**: "`AdmissionState` per kernel-b §4.5 with `reason: Option<ErrorCode>`; T1
  replies it verbatim."
- **Lands in**: type in `rdb-core`. File not settled by any source — `authority.rs` is the
  natural neighbour of `PartitionMode`, but foundation picks. Say which, once.
- **Blocks**: T1's reply path; kernel-b's `AdmissionState.reason` reporting a distinct
  `DIVERGENCE_REQUIRES_OPERATOR` rather than `PROTECTION_PAUSED` (B-R31 item 18).
- **Workaround**: not named in any source. Kernel-a has no code yet, so this is a shape it needs
  before its first module lands, not a blocked row today.
- **Landed state**: **absent.** No `AdmissionState` anywhere in `crates/` (grep over
  `--include=*.rs`).
- **Ruling**: none dedicated. B-R31 item 18 constrains its `reason` value.
- **Two teams, one shape**: defined by kernel-b §4.5, consumed verbatim by kernel-a. Settle it
  with kernel-b's spelling.

### KA-3 — `RecoveryResult`, `SelectedLineage`, `RetainedStatusMap`

- **Shape asked**, verbatim: "`RecoveryResult { selected: SelectedLineage { root, cutoff_seq,
  cutoff_digest, source }, mode: PartitionMode, committed: CommittedRoot, retained_status_map:
  RetainedStatusMap }` and `RetainedStatusMap { predecessor_generation, predecessor_cutoff,
  retained_through, discarded_from: Option<Seq>, uncertain: bool }` per kernel-b §5.8 — T1 and P1
  read both."
- **Lands in**: types in `rdb-core`. Carries `PartitionMode`, so `authority.rs` or a new recovery
  contracts module; foundation picks.
- **Blocks**: kernel-a's P1 `Recovered` row (the three-way `RetainedStatusMap` rule, A-R25 Q2) and
  kernel-b's F1 re-emission of `Recovered(RecoveryResult { mode: Active, .. })` at the rebuild
  barrier (B-R33 Q-B-2, row `M7B-137`, `M7B-148`).
- **Workaround**: none named; both teams' `Recovered` arms are written against this struct.
- **Landed state**: **absent.** No `RecoveryResult`, `SelectedLineage`, `RetainedStatusMap` or
  `CommittedRoot` in `crates/`. Nearest landed thing is the *trace* side:
  `contracts/trace.rs:986` `TraceKind::RecoveryDecision`, which carries `predecessor_cutoff`
  (line 980) — a trace field, not the kernel struct.
- **Ruling**: A-R25 Q2 (the three-way rule is verbatim kernel-b §5.8); B-R31 item 17 (`Recovered`
  checks `lookup(cutoff_seq)` before adopting).
- **Two teams, one shape**: kernel-b owns the definition (§5.8); kernel-a reads it.

### KA-6 — `From<&LineageRoot> for Lineage`

- **Shape asked**, verbatim: "`Lineage` projection from kernel-b's `LineageRoot` (`partition,
  generation, owner_epoch`); a `From<&LineageRoot> for Lineage` would let `may_publish` compare
  without a helper."
- **Lands in**: `rdb-core`, `contracts/authority.rs` (`Lineage` lives there) — an impl, not a new
  type. Needs kernel-b's `LineageRoot` **as a struct** to exist first.
- **Blocks**: nothing hard. It removes a helper from `may_publish`.
- **Workaround**: yes — kernel-a writes the three-field comparison by hand.
- **Landed state**: **absent, and its precondition is absent.** `Lineage { partition, generation,
  owner_epoch }` is landed at `contracts/authority.rs:38`. `LineageRoot` exists **only** as a
  trace variant, `contracts/trace.rs:967` `TraceKind::LineageRoot`, not as a kernel struct, so
  there is nothing to `From`-convert yet.
- **Ruling**: none. Convenience, lowest priority in this group.

### KA-5 — `BlockReason` stays single-variant — **ALREADY SATISFIED**

- **Shape asked**, verbatim: "`BlockReason` stays single-variant; `PartitionMode::Blocked
  { reason }` is the carrier. Kernel-a has removed its private `RecoveryBlocked`."
- **Lands in**: `rdb-core`, `contracts/authority.rs`.
- **Blocks**: nothing — it is a request to *not* change something.
- **Landed state**: **already landed exactly as asked.** `contracts/authority.rs:198` one variant;
  `PartitionMode::Blocked { reason: BlockReason }` at 220–222. Nothing to do for kernel-a.
- **Ruling**: A-R23 (`BlockReason` next to `PartitionMode`); ledger 19:56 PDT
  (`BlockReason::RecoveryBlocked` deleted, `PartitionMode::Blocked { reason }` carries it).
- **Same type as**: **KB-CB-5**. Satisfying one un-satisfies the other. See Conflict 1.

---

## Group 2 — `crates/rdb-core/src/contracts/envelope.rs`

### KB-CB-2 — `AppendReject::NeedPrefix { have, head_digest }`

- **Shape asked**, verbatim: "`AppendReject::NeedPrefix { have, head_digest }` — amends the B-R30
  Q2 ask, currently `{ have }`."
- **Lands in**: type in `rdb-core`, `contracts/envelope.rs`, one field on one variant.
- **Blocks**: rows `M7B-21/55/56/57/63/124/140`. Without the digest "the cursor cannot prove
  divergence and the one-writer path (K-B-52) loses one of its two inputs."
- **Workaround**: none named. The variant compiles, so rows that read only `have` can run; the
  divergence-proof half cannot.
- **Landed state**: **partially landed.** `contracts/envelope.rs:581` — `NeedPrefix { have: Seq }`
  (field at 583). The variant exists; `head_digest` does not. Additive one-field change.
- **Ruling**: B-R30 Q2 as amended by B-R33 Q-B-4.

### KB-CB-4 — non-reject `AppendOutcome` variants

- **Shape asked**, verbatim: "Non-reject `AppendOutcome` variants: `Busy { accepted_through }`,
  `AlreadyHave`, `ProbeDigestAt { seq }`; and the ask should state whether `AppendOutcome` is
  `Result<Accepted, AppendReject>`-shaped or one enum."
- **Lands in**: type in `rdb-core`, `contracts/envelope.rs`.
- **Blocks**: rows `M7B-16, 17, 19, 59, 60` — the three outcomes that drive the cursor (re-send,
  advance, answer a probe).
- **Workaround**: none named.
- **Landed state**: **partially landed — and the shape question is half-answered by what landed.**
  No `AppendOutcome`, `Busy`, `AlreadyHave` or `ProbeDigestAt` anywhere in `crates/`. What landed
  is a *pair*: `AppendAck` (struct, `contracts/envelope.rs:495`) and `AppendReject` (enum, line
  524, 16 variants in ladder order). That is `Result`-shaped in substance, so the open half is
  whether the three non-reject outcomes join `AppendAck`, become a third type, or the pair
  collapses into one enum.
- **Ruling**: B-R30 Q2 / B-R33 foundation-ask list; closes K-F-34.
- **Note**: CB-1 and CB-4 are **one decision**, per kernel-b §15: "CB-1 and CB-4 are shape
  decisions foundation should settle together; CB-2 and CB-3 are additive."

### KA-1 + KA-2 — `digest_at` and `DigestLookup`

Two numbered asks, one landing: the enum and the method that returns it. Kept under both ids
because kernel-a numbered them separately; neither is a duplicate of the other.

- **Shapes asked**, verbatim: (1) "`ReplicationView::digest_at(&self, seq: Seq, expected: Digest)
  -> DigestLookup` — or kernel-b's `digest_at(seq) -> DigestLookup` with `Match(Digest)`. Either;
  one spelling." (2) "`DigestLookup { Match, Differs { stored: Digest }, NotRetained }` in
  contracts (kernel-b §3.2 owner C0), so P1 and R1 match on one enum."
- **Lands in**: types in `rdb-core`. `DigestLookup` is a contracts enum; `ReplicationView` is the
  view trait kernel-a reads — file not settled, `envelope.rs` is the replication neighbour.
- **Blocks**: kernel-a's publish guard (`may_publish`'s digest conjunct, A-R25 Q1, rows behind
  `Fact(PublishPredicateFalse{which})`); kernel-b's `lookup(cutoff_seq)` three-arm rule
  (`M7B-27/124/139`).
- **Workaround**: none named.
- **Landed state**: **absent.** No `DigestLookup` and no `ReplicationView` in `crates/`. The only
  `digest_at` in the tree is `rdb-sim/tests/support/oracle/model.rs:372` — the verification
  oracle's own model helper, a different thing, and owned by another agent.
- **Ruling**: A-R25 Q1 ("YES, P1 evaluates the digest conjunct; `ReplicationView` gains
  `digest_at(seq)` matching kernel-b §3.5 lookup result; contract shape goes to foundation");
  ledger 19:56 PDT ("`digest_at(expected: Digest)` shape left to foundation").
- **Two teams, one shape**: kernel-a and kernel-b want the same enum with different signatures.
  See Conflict 3 — it is a spelling choice foundation may take, not a dispute.

---

## Group 3 — `crates/rdb-core/src/contracts/event.rs`

### KB-CB-1 — `EventKind::Kernel` / `EffectKind::Kernel` carrier pair

- **Shape asked**, verbatim: "`EventKind::Kernel(KernelEvent)` / `EffectKind::Kernel(KernelEffect)`
  carrier pair in C0, variants owned by kernel-b (same shape foundation used for `AppendReject`)."
- **Lands in**: types in `rdb-core`, `contracts/event.rs` — two enum variants plus the two carried
  enums.
- **Blocks**: "every row asserting a kernel-internal event or effect" — roughly 130 kernel-b rows,
  listed once in its §13 rather than per row (Q-R4-5). `Ignored { reason }`, `Alert`,
  `SetAdmission`, `QualificationChanged`, `PeerProgress`, `CopyLost`, `BlockPartition`,
  `DivergenceDetected`, `CopyQuarantined`, `Recovered` and F1's set "have nowhere to live."
- **Workaround**: **yes** — B-R33: "Developer may start on a kernel-b-private pair + `From` shim."
  Residual risk the architect states: the private pair and the eventual C0 pair "could diverge in
  variant names; the `From` shim is the intended seam and should be one file."
- **Landed state**: **absent.** `contracts/event.rs:152` — `EventKind` has seven variants
  (`Client`, `Node`, `Transport`, `Storage`, `Control`, `Timer`, `ExternalFenceVerified`), no
  `Kernel`. `contracts/event.rs:250` — `EffectKind` has six (`Send`, `Store`, `Control`, `Timer`,
  `Reply`, `AdoptAuthority`), no `Kernel`. The doc comment there says "The first five match
  `EventKind` one for one", which a `Kernel` pair would extend, not break.
- **Ruling**: B-R33 Q-B-1.
- **Note**: one decision with **KB-CB-4**. Also the ask that "can move the rows": if foundation
  lands a differently shaped carrier — e.g. one flat variant per kernel event — BA-10's substance
  holds but BA-5's fixture helpers change and the private pair has to be unwound, not swapped.

---

## Group 4 — `crates/rdb-core/src/contracts/errors.rs`

### KB-B-R31-18 — `ErrorKind::DivergenceRequiresOperator`

- **Shape asked**: an `ErrorKind` variant so `AdmissionState.reason` reports a distinct
  `DIVERGENCE_REQUIRES_OPERATOR` "not `PROTECTION_PAUSED`" (B-R31 item 18).
- **Lands in**: type in `rdb-core`, `contracts/errors.rs`, `ErrorKind`.
- **Blocks**: kernel-a row `M7A-69`'s §11 dependency; kernel-b's blocked-admission reporting.
- **Workaround**: none named; a row asserting the distinct reason cannot compile without it (same
  shape of problem as CB-5, on a different enum).
- **Landed state**: **absent.** `contracts/errors.rs:79` — `ErrorKind` has 18 variants
  (`NotPrimary` … `Unavailable`, lines 81–115); no divergence variant. The only
  `DivergenceRequiresOperator` in the tree is `BlockReason`'s, at `contracts/authority.rs:202`.
- **Ruling**: B-R31 item 18; **A-R27 Q-14** ("belongs to kernel-b … goes to foundation in the
  second foundation round").
- **Two teams, one shape**: raised by kernel-b, depended on by kernel-a. One ask, one id.

---

## Group 5 — `crates/rdb-core/src/contracts/trace.rs`

### KB-CB-3 — widen `AckRejectReason` by seven variants

- **Shape asked**, verbatim: "`AckRejectReason` widened by seven variants: `StaleGeneration`,
  `RoleMismatch`, `InconsistentProgress`, `RegressedProgress`, `Unverifiable`, `Diverged`,
  `NotAMember` (kernel-b's names; `ForgedIdentity` already covers `FORGED_ACK`)."
- **Lands in**: type in `rdb-core`, `contracts/trace.rs`, the trace-side `AckRejectReason`.
- **Blocks**: `Q-46`, `Q-47`, `M7B-32`, `M7B-62`, and BA-4's mapping. "§3.4's ladder has eleven
  drop reasons and the landed enum carries four of them."
- **Workaround**: none named; rows can still assert the landed seven, but cannot distinguish
  "dropped because diverged" from "dropped because stale" — "which is the whole content of rows 1d
  and 9."
- **Landed state**: **absent (as asked), and the premise has moved.** `contracts/trace.rs:292` —
  `AckRejectReason` now carries **seven** variants: `Gap`, `DigestMismatch`, `StaleEpoch`,
  `StaleBoot`, `StaleConfig`, `ForgedIdentity`, `IncompatibleVersion`. The ask says "the landed
  enum carries four of them", which was true at `8a23b1d`; at `3051c5d` it is seven. **None of the
  seven asked names is among them**, so the ask stands, but its arithmetic should be re-read before
  the widening is sized.
- **Ruling**: B-R33 Q-B-8.
- **Two teams, one type**: verification's `trace-requirements.md` §3.5 declares this enum a
  **closed set** of exactly the seven landed names and `M7V-56` asserts set equality against the
  coverage lists. See Conflict 2.
- **Name check foundation should do once**: two of the asked names, `StaleGeneration` and
  `NotAMember`, already exist as `AppendReject` variants (`contracts/envelope.rs:524` block). Same
  words, different enum, different meaning — decide deliberately rather than by accident.

### VER-TR8-1 — header `provenance: Provenance` — **ALREADY SATISFIED**

- **Shape asked**: trace-requirements §8.1 — "Replace `TraceHeader.seed: u64` with `pub
  provenance: Provenance`", `Generated { seed }` | `Reduced { parent: ScenarioId }` |
  `Authored { case: String }`.
- **Lands in**: type in `rdb-core`, `contracts/trace.rs`.
- **Blocks**: row `M7V-46`'s header half, parked in the plan's §12 under "C0 + `provenance`".
- **Landed state**: **already landed, matching §8.1 arm for arm.** `contracts/trace.rs:85` — `enum
  Provenance { Generated { seed: u64 }, Reduced { parent: ScenarioId }, Authored { case: String } }`;
  `TraceHeader.provenance` at `contracts/trace.rs:212`; `ScenarioId(u64)` at
  `contracts/ids.rs:116`. There is no `TraceHeader.seed` left.
- **Ruling**: V-R20 (2) (F18/T-23 routed to foundation).
- **Why this matters**: verification's §8 drift table and its critic both still call this "the one
  open contract ask" and treat the landed enum as "observed but not relied on", because they read
  `8a23b1d`. It landed in foundation round 1 (`6893442`). **`M7V-46` can be un-parked.**

### VER-TR8-21 — `op_skipped` kind — **ALREADY SATISFIED**

- **Shape asked**: "`op_skipped { scenario_op_index, reason }` (§3.16a)" — reducer diagnostics, no
  checker reads it.
- **Lands in**: type in `rdb-core`, `contracts/trace.rs`, a `TraceKind` variant.
- **Blocks**: the `op_skipped` clause of `M7V-88` and `M7V-22`; design §4.5's "replays without
  `op_skipped{ReferentGone}`" row.
- **Landed state**: **already landed.** `contracts/trace.rs:1115` — `OpSkipped { scenario_op_index:
  u32, reason: SkipReason }`, with the rustdoc citing finding K-F-08 and verification §3.16a.
- **Ruling**: standing ask from verification critic round 1 (F17) / K-F-08.
- **Why this matters**: same staleness as VER-TR8-1. The plan's §12 parks two asks; **both are
  landed.** Verification has **no open shape ask left** — only the code ask below.

---

## Group 6 — `crates/rdb-sim/src/harness/dispatch.rs` (code, not a shape)

### KA-9 — I1 converts `ControlTime` to `ClockSample`

The kernel-a handoff flags this itself: "Request 9 is the first item on the list that is *code*
rather than a shape, and it is in `rdb-sim`, not `rdb-core`. If foundation's queue is ordered by
crate it will be missed."

- **Shape asked**, verbatim: "I1 converts `ControlTime` to `ClockSample` on the way into
  `AuthorityEvent::Clock`, per the table in design §2.2: `at = sampled_at`, `utc_ms = estimate.0 as
  i64`, `epsilon_ms = error_millis` saturating to `u32`, `valid = bound_established`. It delivers
  `valid = false` rather than withholding the event, and it applies **no** staleness, jump or
  ceiling test … roughly six lines and one doc comment pointing here."
- **Lands in**: **code in `rdb-sim`**, the dispatch path, "next to the other event constructions"
  — `crates/rdb-sim/src/harness/dispatch.rs`. Nothing in `rdb-core`.
- **Blocks**: every kernel-a clock row's sim half; `M7A-43` is the load-bearing one (a stale sample
  denies admission *without* fencing).
- **Workaround**: yes, and it is already in force — "every clock row was fixture-only" until the
  seam exists. Unit rows build a `ClockSample` themselves; sim rows wait.
- **Landed state**: **absent.** `ClockSample` does not exist anywhere in `crates/` (the only hit
  for the string is `DenyReason::ClockSampleStale`, `contracts/authority.rs:68`). No conversion in
  `harness/dispatch.rs` (284 lines, no `ControlTime` construction). The **inputs** are all landed:
  `ControlTime { estimate, error_millis, bound_established, sampled_at }` at
  `contracts/time.rs:84`, produced per node by `sim/clock.rs:91` `control_time(node)`.
- **Ruling**: **A-R26 Q-3** and **A-R27 Q-12** — "I1 builds it, `valid = ct.bound_established`,
  delivered even when unbound, NO staleness filtering at the seam."
- **See Conflict 3a**: `StepCtx` already carries the raw `ControlTime` ambiently.

---

## Group 7 — `crates/rdb-sim/src/harness/` trace or replay (code, not a shape)

### VER-CR-3 — I1 exposes a trace validator

- **Shape asked**: expose I1's well-formedness checks so fixtures can be checked for realizability
  — "strictly increasing `event_id`, the `capability` block first, `schedule_phase` before any
  liveness arming, a `replication_ack.contiguous_seq` never above the emitting node's last
  `batch_apply.seq`" (verification design §4.5).
- **Lands in**: **code in `rdb-sim`**, I1's harness — `harness/trace.rs` or `harness/replay.rs`.
  Nothing in `rdb-core`.
- **Blocks**: `M7V-88` (a sim row that replays every fixture and owns a < 10 s budget).
- **Workaround**: yes, written into the row: "the row degrades to envelope checks and reports
  `Unavailable{Capability(I1)}` for the rest."
- **Landed state**: **absent.** No validator or well-formedness entry point in
  `crates/rdb-sim/src/harness/` (`dispatch.rs` 284, `manifest.rs` 63, `replay.rs` 46, `trace.rs`
  191 lines; the only `validate` in the crate is `sim/cluster.rs:66`, a topology check).
- **Ruling**: no ruling id. Recorded by the lead in `ledger.md` at 2026-09-20 20:35 PDT:
  "Foundation ask recorded for the I1 developer: expose the trace validator (ordering + envelope)
  so fixtures can be checked for realizability (M7V-88, design §4.5)", answering planner CR-3
  ("Yes, add as an ask").
- **Same shape of hazard as KA-9**: it is code in `rdb-sim` and will be missed by a crate-ordered
  queue.

---

## Already satisfied — nothing for foundation to do

Two more asks are **not** open, and are listed so nobody re-opens them:

| Id | Ask | Landed at |
|---|---|---|
| KA-7 | `admission_horizon(h, clock, now, cfg)` takes `now`; `AuthorityView.valid_through_tick` on a fence is `now − 1` saturating — kernel-a states "no contract change" | `AuthorityView.valid_through_tick` `contracts/authority.rs:173`, `past_horizon: DenyReason` at 176. The seed fixture move is kernel-a's, not foundation's |
| KA-8 | "`TxnEvent::Resolved` removed; `NotifyTxn { seq }` is the only P1→T1 notification" | Satisfied vacuously: no `TxnEvent` type ever landed (`contracts/txn.rs` carries `TxnRequest`, `Condition`, `Mutation`, `ConditionOutcome`, `Durability`, `Outcome`, `TxnResult`, `TxnStatus` only). `NotifyTxn` does not exist either, but it is an inter-module message, not a contract foundation has been asked to add — confirm with kernel-a before adding anything |

Also confirmed landed while checking, all from kernel-a's superseded round-3 list under A-R23, so
no entry is owed: `AuthorityDecision.authority_seq` (`authority.rs:118`), `AuthorityView
.authority_seq` (171) and `past_horizon` (176), trace `AuthorityDecision.authority_seq`
(`trace.rs:775`), `EffectKind::AdoptAuthority { partition, .. }` (`event.rs:268`),
`EventKind::ExternalFenceVerified` with its six binding fields (`event.rs:172`). Verification's
§5 asks 1–7 are landed too (`TraceHeader.topology`, `TraceKind::TopologyChange` `trace.rs:1084`,
`Capability` 1102, `ReplicationAckDelivered` 864, `RecoveryDecision.predecessor_cutoff` 980), as
are V-R9's two sim hooks (`sim/network.rs:102` `NetworkOp::ForgeAck`, `storage/memory.rs:51`
`false_claims`).

---

## Conflicts for the lead to rule on

Described, not decided.

### Conflict 1 — how wide `BlockReason` is (KA-5 vs KB-CB-5)

- **Kernel-a's position** (KA-5): "`BlockReason` stays single-variant; `PartitionMode::Blocked
  { reason }` is the carrier. Kernel-a has removed its private `RecoveryBlocked`." Artifact it
  defends: kernel-a design §4.1/§4.2's mode rows and ADR-0007's "Blocked is sticky under a
  publication deny" — a total match over a one-variant reason, written after it deleted its own
  competing variant on the lead's instruction.
- **Kernel-b's position** (KB-CB-5): widen by `NoEligibleRegular`, `ControlUnavailable`,
  `ControlUnknown`; collapsing `Unavailable` and `Unknown` is refused. Artifacts it defends: rows
  `M7B-109/110/146`, and design.md 1436–1441 keeping the two CAS arms apart because they are
  different operator stories.
- **A third position exists inside kernel-b itself**, and the lead should see it before ruling: the
  kernel-b architect's §15 residual risks say F1's internal `Blocked` has four reasons
  (`OvertakenByPeer`, `CasContention`, `ControlUnavailable`, `ControlUnknown`) and that "the F1
  reasons may need a second enum or a widening — flagged, not asked, because F1's `Blocked` is
  internal to the phase machine and never crosses a seam in M7." So the architect's own instinct is
  a second enum; the planner's ask is a widening. They are not the same request.
- **Why it is the F-R13 shape**: one type is being asked to serve two consumers with different
  totality needs. F-R13's test — who owns the decision, and is one fact being stored twice — does
  not obviously settle it, because here both teams *consume* the enum.

### Conflict 2 — whether `AckRejectReason` is a closed set (KB-CB-3 vs verification §3.5)

- **Kernel-b's position** (KB-CB-3): widen by seven. Artifact: the §3.4 ladder's eleven drop
  reasons and rows 1d and 9, whose entire content is *which* reason fired.
- **Verification's position**: `trace-requirements.md` §3.5 writes the field as
  "`reject_reason: Option<AckRejectReason>` (closed set: `Gap`, `DigestMismatch`, `StaleEpoch`,
  `StaleBoot`, `StaleConfig`, `ForgedIdentity`, `IncompatibleVersion`)", and `M7V-56` asserts **set
  equality** between the coverage required-lists and the enum — the plan states plainly that
  "adding an `AckRejectReason` variant … without a cell fails a row". Artifact it defends: the
  coverage matrix and `AckRejectReason::ForgedIdentity`'s required cell.
- **Not necessarily a real fight**: `M7V-56` is *designed* to fail on an un-celled widening, so the
  cost may be seven coverage cells rather than a contract dispute. But it is verification's cost,
  on verification's artifact, and verification has not been asked. That is the F-R13 pattern — the
  consuming team owns the decision — with two consuming teams on opposite sides.

### Conflict 3 — two paths for one clock fact (KA-9 vs the landed `StepCtx`)

- **Kernel-a's position** (KA-9): I1 converts `ControlTime` into a `ClockSample` and delivers it as
  an `AuthorityEvent::Clock`, including when `bound_established == false`, because A1 fences in
  `Held` and *retracts* the held sample in `Unheld`/`Fenced` (K-A-50). Artifacts: `M7A-43` and
  K-A-50's retraction rule; the handoff calls the two prohibitions "the load-bearing half".
- **The landed contract's position**: `StepCtx.control_time: ControlTime`
  (`contracts/event.rs:344`) already hands every module the raw sample on every step, and
  `contracts/time.rs` puts the staleness rule in the kernel ("the rule lives in the kernel and not
  in whichever environment filled the sample in"). On that path A1 reads the sample directly and
  no conversion exists.
- **Why it is the F-R13 shape**: after KA-9 lands, the same fact reaches A1 twice — ambiently
  through `ctx.control_time` and as an event — on two schedules. F-R13's words were "a value that
  is both stored and derived is two sources of truth". No source I read states which path A1 is
  meant to read, or that `ctx.control_time` is off-limits to A1.
- **3a, the narrower version if the lead would rather not re-open the seam**: rule only which path
  A1 reads, and say so in one line at the seam. That leaves A-R26 Q-3 and A-R27 Q-12 untouched.

### Not a conflict, recorded so it is not mistaken for one

`digest_at`'s signature (KA-1). Kernel-a wants `digest_at(seq, expected) -> DigestLookup`,
kernel-b's §3.5 spells it `digest_at(seq) -> DigestLookup` with `Match(Digest)`. Kernel-a states
"Either; one spelling", and the ledger (19:56 PDT) records the shape as left to foundation. It is
foundation's choice, not a dispute needing a lead ruling.

---

## Checks run before handing off

- **Every landed-state line cites a file and line I opened**: `contracts/authority.rs`,
  `contracts/envelope.rs`, `contracts/event.rs`, `contracts/errors.rs`, `contracts/time.rs`,
  `contracts/trace.rs`, `contracts/txn.rs`, `contracts/ids.rs`, `rdb-sim/src/harness/*.rs`,
  `rdb-sim/src/sim/clock.rs`, `sim/network.rs`, `storage/memory.rs`.
- **Absence claims are greps over `crates/` with `--include=*.rs`**, not over one file:
  `DigestLookup`, `ReplicationView`, `RecoveryResult`, `SelectedLineage`, `RetainedStatusMap`,
  `CommittedRoot`, `AdmissionState`, `AppendOutcome`, `Busy`, `AlreadyHave`, `ProbeDigestAt`,
  `ClockSample`, `NotifyTxn`, `TxnEvent` — no hits outside the ones noted.
- **No ask appears under two ids.** KA-1 and KA-2 are the method and the enum it returns, kept
  separate because kernel-a numbered them separately and either could land without the other.
- **Shapes wanted by two teams are marked on every entry involved**: KA-5 ↔ KB-CB-5 (`BlockReason`);
  KB-CB-3 ↔ verification §3.5 (`AckRejectReason`); KA-4 and KA-3 (kernel-b defines, kernel-a
  consumes); KB-B-R31-18 (kernel-b raises, kernel-a depends); KA-1/KA-2 (both kernels).
- **Staleness caught**: verification's §8 drift table and its critic both still name `provenance`
  as "the only" open ask and `op_skipped` as unlanded. Both landed in foundation round 1
  (`6893442`). That is the same failure mode B-R34 recorded — a drift table is only as fresh as
  its last re-read. The two rows parked in the verification plan's §12 can be un-parked; the plan
  file is verification's to edit, not mine.
- **Not verified by compiling anything.** No cargo was run, per the assignment. Every claim above
  is a read of source text at `3051c5d`.
