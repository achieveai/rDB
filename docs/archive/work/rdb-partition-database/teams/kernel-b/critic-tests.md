# Kernel-b critic: round-4 diff check (K-B-51/52) and test-plan review

Critic kernel-b-2, 2026-09-20. One pass, read-only.

Inputs: architect handoff §14 (lines 631–689); `git diff ac9956d..1248357 -- docs/ADRs/rdb/`
(0005/0006/0009); `design.md` round 4 (scratchpad); `docs/testing/test-plan-m7-kernel-b.md` at
`6ff5a3d` (145 rows); `test-planner-handoff.md` "## Round 3"; ruling B-R32 (binding); landed C0 at
`8a23b1d` via `git show 8a23b1d:crates/rdb-core/src/...`; the in-flight working tree read only to
route drifts (it is not the developer's basis); verification critic T-23 as the drift pattern;
`test-plan-m7-verification.md` VA-7 (the log-line contract kernel-b's BA-4 must sit under).

Authority: charter > rulings (B-R3..B-R32, V-R14) > ADRs 0005/0006/0009 > design.md > this note.

---

## A. K-B-51 / K-B-52 against round 4

| Finding | Disposition | Evidence |
|---|---|---|
| K-B-51 resume arm reachable while blocked (`ConfigChanged` adding a regular copy) | **CLOSED** | design §4.4 arm now `Paused, all_durable_through(..) AND qualifies_now_at_head AND blocked.is_none() -> Reprotecting` (line 1230); the `on BlockPartition` bracket note is a pointer to the conjunct, not an unreachability claim (1219–1221); paragraph 1244–1252 states the `ConfigChanged` counterexample and the P1-stays-`Blocked` consequence; §7 row 1896. ADR-0006 §4 arm `AND not blocked`, note "[the resume arm requires 'not blocked'; no exit inside this instance]", sentence after "A block is not a pause", verification row "A membership change does not unblock". `rg unreachable` in §4.4 → none (remaining hits at 606/679/1399/1515 are other sections, other meanings) |
| K-B-52 `CopyQuarantined` had no consumer; a `required` copy quarantined at `Recovered` stalled `Rebuilding` silently | **CLOSED** (closure (a), one writer kept) | design §3.4 lines 710–713: "`CopyQuarantined { copy }` — the cursor's other proof — is consumed by the same arm with the same effects… Two routed events, one arm, one writer"; §3.6 row 963 names the consumer and the `Rebuilding` consequence; §3.3 `Differs` row (617) now "evidence, never a holder, and **not a target of this `Rebuilding`**… a valid catch-up target only after operator action and a later `Recovered`"; §5.6a 1736; §7 row 1897. ADR-0005 §2 `Differs` sentence, §5 one-writer paragraph + row "A quarantined copy is marked without an ACK"; ADR-0009 §7 sentence + row "A quarantined required copy stalls the same way". `rg "rebuild target"` in §3.3 → none (remaining hits 410, 1702, 1730, 1752, 1757, 1855 are §3.2/§5.6a/§6 and mean the placement-supplied target — correct usage) |
| K-B-46 citation caveat (lead note) | closed | ADR-0005 §5 line 289 now cites "kernel-a's `design.md` §4.1/§4.2, scratchpad, uncommitted"; `rg 785e41b docs/ADRs/rdb/0005*` → none. Residual, already the architect's: an ADR pointing at an uncommitted scratchpad is a temporary pointer until kernel-a's P1 ADR lands |

Two-place sweep confirmed: `CopyQuarantined` has one emitter (§3.6) and one consumer (§3.4);
`diverged` one writer; `blocked.is_none()` in the arm and the paragraph and ADR-0006.

**Provisional rows against the round-4 text.**

- **M7B-143** ≡ design §7 row 1896 and ADR-0006 row "A membership change does not unblock":
  fixture (`Paused`, `blocked Some`, `ConfigChanged` adds D, D ACKs at head → `Gained`,
  `DurableAdvanced` satisfies every predicate, `HealthEval`) and assertions (stays `Paused`, no
  `Allow`, `reason == DIVERGENCE_REQUIRES_OPERATOR`, twin `blocked None` → `Reprotecting`) match the
  landed text. **Marker may lift.**
- **M7B-144** ≡ design §7 row 1897, ADR-0005 row, ADR-0009 row: tracker step on
  `CopyQuarantined{B}` sets `diverged` and emits the B-R26 vector once; `Rebuilding` gets `CopyLost`
  → one `RebuildStalled`, `required` unchanged, no `ActivationProposed`. **Marker may lift.** One
  nit, not a hold: the idempotence reason `Ignored{ALREADY_DIVERGED}` is the planner's name (R3-3);
  the design says "a copy already marked emits nothing more", which under the plan's own BA-2 must
  still be one `Ignored`. See T-B-05.

---

## B. Test plan: what was checked

- **Charter acceptance** (§10 map): spike §5 rows R1/L1/F1 verbatim in the `Proves` column of
  M7B-62/68/92; all unequal secondary prefix pairings (M7B-92, six ordered pairs over {10,20,30});
  all three lone-survivor choices (M7B-111); divergent digest at the same position quarantines and
  blocks promotion (M7B-97, plus `LengthSpy == 0`); buffered entries fsynced before the barrier
  (M7B-104, with the `FalseDurable` hook). V8 ladder (V-R14) carried as M7B-66/67/68/80/132. Every
  row is a named test. Present and correctly aimed, with one exception (T-B-03).
- **Counts**: 145 rows = 134 unit + 11 sim + 0 campaign; §15 per-section sums check
  (29+16+9+9+20+36+26). The planner's mechanical proof in the handoff is consistent with the file.
- **Round-3 re-alignments in place**: M7B-45 (peers zeroed, `diverged == false`, no
  `retained_status_map` read — matches §3.4 `Recovered`), M7B-57 (`[DivergenceDetected(B)]` only —
  matches §3.6 line 912), M7B-82 (post-`Recovered` instance starts `Paused`, initial-state
  assertions delegated to M7B-141 — matches §4.1). Correct.
- **Design §7 / ADR rows of rounds 2–4** spot-checked against rows: 1871→M7B-139, 1872→M7B-140,
  1892→M7B-128, 1896→M7B-143, 1897→M7B-144; ADR-0009 rows "Holder ≠ leader" →M7B-138, "A
  credential names its sender" →M7B-121. Present.
- **Drift against landed C0 at `8a23b1d`**: every row literal that names a type, field or variant
  was checked against `git show 8a23b1d:crates/rdb-core/src/contracts/*.rs`. Results in T-B-01,
  T-B-02, T-B-04. Pending foundation asks (A-R23, B-R30, B-R31) are listed as dependencies, not
  findings; where an ask as ruled does not cover what a row needs, that gap is named.

---

## C. Findings

Severity: BLOCKER (unsafe or core criterion unmet) / MATERIAL (bounded defect; correct or lead
accepts the risk) / ADVISORY.

### T-B-01 — BA-4's log-line vocabulary has no emitter: nine `@m` names are not `TraceKind` variants, and the value enums the JSONL rows read do not carry the design's outcomes

- **Severity:** MATERIAL.
- **Criterion:** BA-1 (modules do no I/O; every decision is in the effect vector) together with
  verification VA-7 (`test-plan-m7-verification.md` lines 185–204: JSONL under `RETCD_TEST_LOG_DIR`
  carries `#[retcd_test]` lines and one line per `TraceEvent` with `@m` = the `TraceKind` variant in
  snake_case; nothing else). T-23's criterion, one team over.
- **Location:** plan §1 BA-4; Q-46, Q-47 (§11); the JSONL clauses of M7B-26, 32, 62, 78, 96, 137.
- **Evidence** (`crates/rdb-core/src/contracts/trace.rs` at `8a23b1d`):

  | BA-4 / rows write | Landed C0 has | Rows hit |
  |---|---|---|
  | `@m ∈ {append_decision, ack_decision, qualification, catchup_step, protection_transition, recovery_phase, selection, barrier_check, source_unavailable}` | `TraceKind` variants `ReplicationSend`, `ReplicationAck`, `ReplicationAckDelivered`, `DurabilityAdvance`, `LineageRoot`, `RecoveryDecision`, `Quarantine`, `ProtectionState` (+ client/publish/read/dedup/version/fault/topology/schedule/capability). None of the nine names exists; the in-flight `trace.rs` adds none of them either | BA-4, Q-46, Q-47, M7B-26, 32, 62, 78, 96, 137 |
  | `ack_decision.outcome` ∈ the eleven §3.4 drop reasons (`FORGED_ACK`, `STALE_GENERATION`, `STALE_EPOCH`, `STALE_CONFIG`, `ROLE_MISMATCH`, `STALE_BOOT`, `INCONSISTENT_PROGRESS`, `REGRESSED_PROGRESS`, `UNVERIFIABLE_ACK`, `DIVERGED_COPY`, `NOT_A_MEMBER`) | `ReplicationAck { accepted: bool, reject_reason: Option<AckRejectReason> }`, `AckRejectReason = Gap \| DigestMismatch \| StaleEpoch \| StaleBoot \| StaleConfig \| ForgedIdentity \| IncompatibleVersion` — seven variants; **no** `StaleGeneration`, `RoleMismatch`, `InconsistentProgress`, `RegressedProgress`, `Unverifiable`, `Diverged`, `NotAMember` | M7B-32 ("all `outcome == FORGED_ACK`" — `ForgedIdentity` exists, so this one is a rename), M7B-62 |
  | `protection_transition` with state `Reprotecting` | `ProtectionState { phase: ProtectionPhase }`, `ProtectionPhase = Healthy \| Warn \| Paused \| Resuming` | **M7B-78 is vacuous as written**: "JSONL `protection_transition` has no `Reprotecting`" is true of every run, including one that resumed, because the landed name is `Resuming` and the landed `@m` is `protection_state` |
  | `append_decision.outcome` ∈ the receiver ladder codes | no trace variant for a receiver decision at all; `Quarantine { reason: QuarantineReason = DigestConflict \| CorruptHistory \| IncompatibleVersion }` covers only the two quarantine rows, under different names (`DigestConflict` ≠ `DIVERGENT_HISTORY`) | Q-46 (enumerates ladder outcomes over `append_decision`), Q-47, M7B-26, 62 |
  | `recovery_phase`, `selection`, `barrier_check`, `source_unavailable` | `RecoveryDecision { .., mode: RecoveryMode = TwoSurvivor \| LoneSurvivorReadOnly \| Quarantine, .. }` is one line per recovery, not per phase; `LineageRoot`; nothing for the barrier or a stalled source | M7B-96 ("`RecordSourceUnavailable{C, Stalled}` logged before `selection` line"), M7B-137 ("JSONL `recovery_phase` sequence recorded") |

- **Consequence.** Kernel modules cannot write these lines (BA-1), the trace cannot carry them
  (no variants), and the harness writes only what VA-7 says. So either the developer adds a second
  log channel from inside the kernel — a BA-1 violation and a foundation-charter violation — or the
  JSONL assertions in six sim rows and both Q-rows are untestable, and M7B-78 passes by absence.
  Q-46 is the row that proves "no `Ignored{}` with an unknown reason" across the whole R1 end-to-end
  run; without a carrier it proves nothing.
- **False-positive check.** Could the sim harness log every *effect* it dispatches, so
  `append_decision` is derivable from `AppendReply(outcome)` effects? Technically yes, and that
  would keep the kernel pure — but it is not what BA-4 says, VA-7 does not list it, and the
  dispatcher's in-flight code logs trace events, not effects. Could BA-4 be read as "these are the
  trace variants kernel-b will *ask for*"? Then §13 must list them as an ask, and it does not.
  Either way the plan as written has no emitter.
- **Closure.** (1) BA-4 becomes a table: each `@m` → a landed `TraceKind` variant and field, or
  "foundation ask <id>". Concretely: `ack_decision` → `replication_ack{accepted, reject_reason}`
  with an ask to widen `AckRejectReason` by the seven missing variants (or replace it with
  `Option<ErrorKind>` as kernel-a's admission line does); `protection_transition` →
  `protection_state{phase}`; `recovery_phase`/`selection` → `recovery_decision` + `lineage_root`;
  `append_decision`, `catchup_step`, `qualification`, `barrier_check`, `source_unavailable` →
  either new variants (ask) or the rows assert on the recorded effect vector in the sim instead of
  JSONL. (2) M7B-78 asserts on the landed field (`phase != Resuming` for the run) **and** adds a
  positive control (a run that does resume shows one `Resuming` line), so absence is evidence.
  (3) Q-46/Q-47 re-keyed to whatever (1) lands. (4) One sentence in design §4/ADR-0006: the
  internal state `Reprotecting` is traced as `ProtectionPhase::Resuming` (verification's M7V-02/41
  already use `Resuming`; renaming the enum would break their rows). Q-B-6.

### T-B-02 — the plan's event and effect vocabulary has no carrier in landed `Event`/`Effect`, and §13 does not say so

- **Severity:** MATERIAL.
- **Criterion:** BA-1 (the signature the rows call) and BA-2 (`Ignored{reason}` is an effect,
  never silence); §13 "Unavailable until" completeness; T-23's criterion.
- **Location:** BA-1, BA-2, BA-5; every row that feeds an event other than a transport, storage,
  control or timer event, and every row that asserts an effect other than send/store/control/timer/
  reply — which is most of §4–§9.
- **Evidence** (`contracts/event.rs` at `8a23b1d`): BA-1's
  `step(&mut self, ctx: &StepCtx, ev: &Event) -> Result<Vec<Effect>, RdbError>` is exactly
  `Module::step` — good. But `EventKind = Client | Node | Transport | Storage | Control | Timer` and
  `EffectKind = Send | Store | Control | Timer | Reply`. The following have **no variant**:
  - events the rows feed: `HealthEval{now}`, `QualificationChanged`, `PeerProgress`, `CopyLost`,
    `BlockPartition`, `DivergenceDetected`, `CopyQuarantined`, `LocalApplied`, `DurableAdvanced`,
    `ConfigChanged`, `TransitionBarrierConfirmed`, `PinnedConfig`, `ControlBoot`, `Recovered`,
    `FenceProven`, `InventoryReported`, `DiscoveryDeadline`, `DurableAt`, `CopyCaughtUp`,
    `StaleOwnerReturned`, `ProbeDigestReply`;
  - effects the rows assert by index: `Ignored{reason}`, `Alert{..}`, `SetAdmission`,
    `ProtectionWarn`, `ProtectionCleared`, `ApplyBatch`, `QualificationChanged`, `PeerProgress`,
    `CopyLost`, `BlockPartition`, `DivergenceDetected`, `CopyQuarantined`,
    `SnapshotCatchupRequired`, `QueryInventory`, `SyncWalThrough`, `CatchUp`, `CatchUpBeforeGrant`,
    `ProposeOwnership`, `ActivationProposed`, `QuarantineSuffix`, `RebuildFromAuthoritative`,
    `RetainQuarantinedSuffix`, `RecordSourceUnavailable`, `CloseWindow`, `Recovered`.
  The in-flight tree adds `EventKind::ExternalFenceVerified` and `EffectKind::AdoptAuthority` only.
  Of the pending asks the coordinator named, `BlockPartition`, `CopyLost`, `PeerProgress` are
  covered (dependencies). `Ignored`, `Alert`, `SetAdmission`, `QualificationChanged`,
  `HealthEval`, `Recovered` and the F1 set are on no ask I can find (`rg -i "Ignored|Alert" ledger.md`
  → B-R26's "with an alert" only). ADR-0003 §9 and the in-flight `dispatch.rs` describe the
  effect→event hop but name no variant that rides it.
  Also: `Effect` is a struct `{ correlation, from: ModuleName, partition, kind }`, so A3's
  `assert_eq!(effects, vec![..])` must build whole `Effect`s with the event's `correlation`; BA-5's
  builder list has no `effect(kind)` helper.
- **Consequence.** BA-1 and BA-2 cannot both hold against landed C0: a `Module` that returns
  `Vec<Effect>` has nowhere to put `Ignored`. The developer's first decision — add
  `EventKind::Kernel(..)`/`EffectKind::Kernel(..)` to C0, or make kernel-b's modules not `Module`
  implementors with private event/effect types — is a contract decision, and the plan hands it to
  the developer by omission.
- **False-positive check.** Is `HealthEval{now}` just `EventKind::Timer(TimerFired)`? Yes, with
  `now = ctx.now` (`TimerFired{id, version, scheduled_at}` has no `now`; `StepCtx.now` does) — a
  plan edit, not an ask. Are the F1 events really kernel-internal? `FenceProven` is kernel-a's
  A1 seam (an effect of theirs, an event of ours); `DurableAt` is derivable from
  `StorageEvent::Flushed`; `ControlCasResult` is `ControlEvent::CasResult` (T-B-04). The rest are
  intra-kernel. None of that rescues `Ignored`/`Alert`/`SetAdmission`/`QualificationChanged`.
- **Closure.** (1) §1 gains BA-10 naming the carrier, after the lead rules (Q-B-1); (2) §13 lists
  every row that feeds or asserts a kernel-internal event/effect under "C0 + kernel event/effect
  carrier"; (3) `HealthEval{now}` rows say `TimerFired` + `ctx.now`; (4) BA-5 adds the `Effect`
  builder. Until (1) lands the developer can proceed with a kernel-b-private enum pair and one
  `From` shim (§D); the rows' substance does not change.

### T-B-03 — M7B-111 and M7B-137 assert that L1 withholds `SetAdmission(Allow)` until the three-copy barrier; nothing in the design makes L1 do that, and the half that does hold writes in `ReadOnly` has no row

- **Severity:** MATERIAL (the §14 V3 gate counts M7B-111/137 as proving spec §8.4 "writes wait for
  three validated durable copies, then CAS `ACTIVE`").
- **Criterion:** charter "all three lone-survivor choices" and "three-copy rebuild barrier";
  spec §8.4; BA-1 "rows assert what the module decides".
- **Location:** M7B-111 assertion "`recovery_mode true`; no `SetAdmission(Allow)` until three-copy
  barrier (M7B-126, 137)"; M7B-137 "L1 constructed `Paused` at `Recovered` and `SetAdmission(Allow)`
  only after that commit plus the M7B-129 hold".
- **Evidence.** L1's inputs (design §4.4, ADR-0006 "four inputs"): `QualificationChanged`,
  `BlockPartition`, `HealthEval`, plus the state-writers `PeerProgress`, `CopyLost`,
  `DurableAdvanced`, `ConfigChanged`, `TransitionBarrierConfirmed`. `PartitionMode` is not among
  them (`rg "recovery_mode|ReadOnly" design.md` → §5.6 only). The refusal of writes in `ReadOnly`
  is kernel-a's: T1 row 1448 and P1 row 1680 map `Recovered(r).mode == ReadOnly ⇒
  Frozen{RecoveryReadOnly}`. In M7B-137's own scenario, placement supplies C′/D′ as data
  (`ConfigChanged`), they catch up through §3.6, ACK at head → `Gained`; the barrier goes durable
  (the same `DurableAt`s F1 is collecting) → `Reprotecting` → 5 s hold → `Healthy` +
  `SetAdmission(Allow)`, with `blocked == None`. That can precede `ActivationProposed → Committed
  {Active}`; nothing in L1 waits for it. So the assertion fails against the design — or passes
  vacuously if the fixture's C′/D′ never ACK at head, which M7B-137 needs them to do for the
  rebuild.
  The other half: design §5.6a (lines 1747–1750) says activation "commits by the same single CAS…
  flipping the record to `ACTIVE`" and names no event to T1/P1/L1; kernel-a's rows leave
  `Frozen{RecoveryReadOnly}` only on `Recovered(r)`. No M7B row asserts that the activation reaches
  T1/P1 — the assertion that would catch a partition stuck read-only after a successful rebuild.
- **Consequence.** Two V3 rows assert the wrong module and the right behaviour has no row. If the
  design's seam is missing (not this review's scope — it is a design question, Q-B-2), the plan is
  where it would have surfaced, and it did not.
- **False-positive check.** Could the fixture's pinned `ReadOnly` config list only the survivor,
  so no secondary can qualify until re-pinned and L1 stays `Paused` "until the barrier" by
  accident? Yes for M7B-111 (unit, no rebuild) — but then the clause proves nothing about §8.4,
  and M7B-137 explicitly re-pins. Is `recovery_mode` an L1 flag? It is `RecoveryResult`'s (design
  §5.6 row 1), read by T1/P1.
- **Closure.** (1) M7B-111 drops the `SetAdmission` clause and cross-references kernel-a's
  `Frozen{RecoveryReadOnly}` rows for the write refusal. (2) M7B-137 asserts what L1 does (resumes
  on `Gained` + barrier + hold, mode-blind) **and** that the activation commit reaches T1/P1 as a
  mode change — which needs the architect to name the event (default in Q-B-2: F1 re-emits
  `Recovered(RecoveryResult{mode: Active, ..})` on `Committed{Active}`; kernel-a's rows already
  handle `Recovered`). (3) §14 V3 line names the corrected rows.

### T-B-04 — row literals that name fields or variants not in landed C0 (T-23 pattern): plan edits, and three gaps in the asks

- **Severity:** MATERIAL-low (mostly renames; three items are not renames).
- **Criterion:** BA-3/BA-8 "landed C0 is the basis"; §13 completeness.
- **Evidence** (`git show 8a23b1d:crates/rdb-core/src/contracts/{envelope,event,control,ids,membership,storage,transport}.rs`):

  | Plan writes | Landed `8a23b1d` | Rows | Disposition |
  |---|---|---|---|
  | `ProgressAck{..}` (design's name) | `AppendAck { partition, generation, owner_epoch, config_version, from: NodeId, boot, role: ReplicaRole, progress: ReplicaProgress, digest_at_buffered }` | golden_ack, M7B-17, 22, 30..45 | plan edit (rename) |
  | `role Regular` | `ReplicaRole = Primary \| RegularSecondary \| Shadow`; no `Regular` | M7B-30 golden, 32, 35, 44 | plan edit — T-23's exact row |
  | `ev.tick`, `{.., tick}` | `Event.at: Tick` | BA-1, M7B-30, 69, 129, 141 | plan edit (cosmetic) |
  | `HealthEval{now}` | `EventKind::Timer(TimerFired { id, version, scheduled_at })`; `now` is `StepCtx.now` | M7B-61, 64..83, 141..143 | plan edit; see T-B-02 |
  | `ControlCas{key: partitions/{id}, expected_revision r}` | `ControlEffect::Cas { key: ControlKey::Partition(id), expected: Option<Revision>, value: Option<Bytes> }` | M7B-105, 126 | plan edit |
  | `ControlCasResult::Committed{revision} \| Conflict \| QuorumLost` | `ControlEvent::CasResult { key, outcome: CasOutcome }`, `CasOutcome = Committed(Revision) \| Conflict { exists, current } \| Unknown \| Unavailable`; **no `QuorumLost`**; two outcomes design §5.1 (line 1408) does not name | M7B-106..109 | plan edit **and** design §5.1 needs arms for `Unknown` and `Unavailable` — "never retry blind: the CAS may have landed" is literally `Unknown`. Q-B-3 |
  | `NeedPrefix { from, head_digest }` | `AppendReject::NeedPrefix { have: Seq }`; in-flight still `{ have }` — **no `head_digest`** | M7B-21, 55, 56, 57, 63, 124, 140 | **gap in the ask**: B-R30 Q2 is "additive", and `head_digest` on `NeedPrefix` is what the cursor's step 1 (§3.6) and K-B-45's catch-up-side divergence proof rest on. Route to foundation; until then these rows are `Unavailable`. Q-B-4 |
  | `Busy{accepted_through}`, `AlreadyHave`, `ProbeDigestAt{seq}` as `AppendOutcome` variants | not in `AppendReject`; the in-flight extension adds `Quarantined, IncompatibleVersion, TooLarge, WrongPartition, StaleGeneration, NeedLineage, UnknownEpoch, StaleConfig, NeedConfig, NotAMember, CorruptHistory{at}, DivergentHistory{at}, StaleFence` — rejects only; the three non-reject outcomes have no carrier | M7B-16, 17, 19, 59, 60 | dependency (B-R30 Q2) — but §13 lists only M7B-52, 59 under it; every §3/§6 row that names a ladder outcome belongs there, and the ask must say whether `AppendOutcome` is `Result<Accepted, AppendReject>`-shaped or one enum |
  | `Alert{CopyQuarantined}` on `CORRUPT_HISTORY` | design names no such alert (its alert kinds: `CopyDiverged`, `RebuildStalled`; §3.2 row 7 lists no `Alert`); and `CopyQuarantined { copy }` is now the routed cursor→tracker **event** (K-B-52) | M7B-14; Q-56 | plan edit: drop it or name it distinctly after the architect states row 7's effect. As written it collides with a routed event, and Q-56's `rg CopyQuarantined` would count the alert as a "consumer" |
  | `unavailable!("<seam>")` (A5) | no such macro anywhere; verification's mechanism is a checker verdict `Unavailable{NotArmed \| Capability(p)}` and `#[retcd_test]` from `config_log` | every dependent row | plan edit: name the owner (foundation `support/`, handoff Q4) or define it in-file; A5 currently cites a thing that does not exist |
  | `DurableProof` "plain public struct" in C0 (BA-3) | not in C0; B-R30 Q10: kernel-b builds it from `DurablePrefix` + its own digest lookup | M7B-22, 24, 99..103 | plan edit: BA-3 says kernel-b-owned |
  | `Budgets{warn_ms, ..}` | `Budgets { warn_age_millis, pause_age_millis, resume_lag_millis, resume_hold_millis, grant_millis }` | §7 fixture prose | cosmetic |
  | `PartitionConfig.min_regular_acks` | absent; in-flight `min_regular_acks: u8`, `with_min_regular_acks(0) → Err` | M7B-49, 51, 52 | dependency (B-R30 Q3), listed |
  | `PartitionMode`, `BlockReason` (BA-8) | absent; in-flight `authority.rs`: `PartitionMode = Active \| DegradedRf2 \| ReadOnly \| Blocked { reason: BlockReason }` | M7B-110, 117, 142 | dependency (A-R23), listed; note `Blocked` carries a reason, so M7B-110's "0 eligible → `Blocked`" needs a reason value and design line 1818 lists a bare `Blocked` |
  | `FenceCredential { .., sender }` | absent; in-flight `EventKind::ExternalFenceVerified{..}` is kernel-a's proof event, not the wire credential | M7B-84, 120..122, 136, 138 | dependency (B-R31), listed |
  | `ReplicaProgress{received, buffered_applied, durable}` (three newtypes), `DurablePrefix{partition, generation, through: DurableSeq}`, `StorageEvent::{Committed{batch, applied}, CommitFailed{batch, fault}, Flushed{ticket, durable}, FlushFailed{ticket, fault}}`, `StorageFault` (5 variants), `PeerLabel{node, boot, authenticated}`, `PartitionConfig::copy_of`, `CopyId(u8)`, `AppendReject::Unauthenticated` | **match** | M7B-13, 22..25, 31 | none — buildable today (T-B-06) |

- **Closure.** §1 gains the table above as "C0 drift"; §13 gains the three gap items as asks
  (`NeedPrefix.head_digest`; non-reject `AppendOutcome` carrier; `CasOutcome` four arms) and moves
  the ladder-outcome rows under B-R30 Q2; literals updated to landed names.

### T-B-05 — BA-2 is contradicted by three rows and the design's "no effect" wording

- **Severity:** ADVISORY.
- **Location:** M7B-128 twin (`CopyLost{D}`, D ∉ required → `effects == []`); M7B-142 ("a second
  `BlockPartition` is idempotent" — vector unstated); M7B-140/144 (`Ignored{ALREADY_DIVERGED}` vs
  design §3.4 "emits nothing more" and §5.6a "else no effect").
- **Evidence:** BA-2: "An event with no other effect still yields one `Ignored`." M7B-128's twin
  asserts the empty vector.
- **Closure:** BA-2 wins (it is the plan's own rule and the anti-silence guard): M7B-128 twin →
  `[Ignored{NOT_REQUIRED}]`, M7B-142 → `[Ignored{ALREADY_BLOCKED}]`, M7B-140/144 as written. One
  table in §2 listing the planner-named reason codes (`NOTHING_OUTSTANDING`,
  `NO_QUALIFYING_SECONDARY`, `BARRIER_NOT_DURABLE`, `NOT_A_CURSOR_EVENT`, `OUTSTANDING`,
  `NOT_PRIMARY`, `NOT_FENCED`, `QUARANTINED_TERMINAL`, `ALREADY_DIVERGED`, `INVALID_CONFIG`,
  `RECOVERY_ONLY`, `NOT_REQUIRED`, `ALREADY_BLOCKED`) as developer-defined — the row pins presence
  and distinctness, not spelling — so the developer does not hunt the design for them.

### T-B-06 — §13 "Unavailable until" over-holds four buildable rows and pins one open choice that landed C0 already answers

- **Severity:** ADVISORY.
- **Evidence:** M7B-22, 23, 24, 25 are unit rows over `StorageEvent`/`StorageFault`/
  `DurablePrefix`, all landed and matching (T-B-04 last row); only M7B-26 needs M1's `FalseDurable`.
  M7B-13's "handoff Q6: `NOT_A_MEMBER` or `Unauthenticated`" — `AppendReject::Unauthenticated`
  is landed, so the row can pin it now.
- **Closure:** move 22–25 out of §13; M7B-13 asserts `Unauthenticated`.

### T-B-07 — M7B-27 omits the `cutoff_digest` its `Match` arm depends on

- **Severity:** ADVISORY.
- **Evidence:** since round 3, `Recovered` does `lookup(cutoff_seq)` first (§3.3 three arms).
  M7B-27's fixture (receiver at `(15, d15)`, quarantine set, `Recovered{cutoff 15, ..}`) asserts
  `quarantine == None` — true only on the `Match` arm, i.e. only if `cutoff_digest == d15`. The row
  does not say so; M7B-139 is the twin.
- **Closure:** fixture states `cutoff_digest d15`; row cites M7B-139 as the digest twin.

### T-B-08 — M7B-41 and M7B-140 read design §3.4 item 1 two ways

- **Severity:** ADVISORY.
- **Evidence:** §3.4 (lines 716–718): the step that sets `diverged` — "from rule 9 or from the
  routed event" — emits "1. `DivergenceDetected(copy)` and `Alert{CopyDiverged}` — always". M7B-41
  (rule 9) asserts `DivergenceDetected(B)` at index 0; M7B-140 (routed `DivergenceDetected` event)
  asserts a vector starting at `Alert`. If the tracker re-emits `DivergenceDetected` on the routed
  path, I1 routes it back, the second step is the idempotent `Ignored` — harmless, one hop, but
  M7B-140's exact vector (A3) is then wrong; if it does not, M7B-41's is right and the sentence
  needs "on the rule-9 path".
- **Closure:** architect states it (recommend: not re-emitted on the routed path — the routed
  event *is* the proof's trace); M7B-140/144 say so. Q-B-5.

---

## D. Developer start list

**May start now** — rows whose inputs are landed `EventKind` variants or direct calls on kernel-b's
own types, whose assertions are on kernel-b state or landed effects, with kernel-internal
events/effects behind a **kernel-b-private enum pair and one `From` shim** until Q-B-1 lands
(reversible; the rows' substance does not move):

- §3: M7B-01..12, 15, 16..25 (outcome enum literals against the in-flight `AppendReject` names, `Unavailable` until Q2 lands), 27 (with T-B-07), 28, 29.
- §4: M7B-30, 31, 33..45 (`AppendAck`, `RegularSecondary`).
- §5: M7B-46, 48, 49, 50, 53, 54.
- §6: M7B-58, 59, 60, 61 (M7B-55/56/57/63 need `head_digest` — Q-B-4).
- §7: M7B-64, 65, 66, 69..77, 79..83 (`TimerFired` + `ctx.now`), 141, 142 (with T-B-05).
- §8: M7B-85..95, 97..103, 105 (landed `Cas` shape), 110, 113..119.
- §9: M7B-123, 125..135, 139, 140 (with T-B-08 default), 143, 144, 145.

**Hold** (named condition each):

- T-B-01: Q-46, Q-47; the JSONL clauses of M7B-26, 32, 62, 78, 96, 137 (the non-JSONL halves of 26/32/62/96 also wait on F:H1/F:M1/F:T1, already in §13).
- T-B-03: M7B-111 (the `SetAdmission` clause only), M7B-137.
- T-B-04: M7B-14 (alert name), M7B-106..109 (four `CasOutcome` arms, Q-B-3), M7B-21, 55, 56, 57, 63, 124 (`NeedPrefix.head_digest`, Q-B-4).
- Already in §13 and correct: M7B-13, 26, 32, 47, 62, 67, 68, 78, 96, 104, 136 (seams); M7B-84, 120..122, 138 (`FenceCredential`); M7B-52 (`min_regular_acks`, in flight — near).
- M7B-143/144: provisional marker lifts on the lead's word (§A); no other hold.

---

## E. Verdict

**PASS_WITH_RISKS.**

- K-B-51, K-B-52: CLOSED against round 4; M7B-143/144 match the landed text.
- The plan covers every charter acceptance item with a named row, carries V8, keeps ids stable,
  and its counts are proven. The round-3 rows are aligned to the round-3/4 design.
- Three MATERIAL findings, all the same shape — the plan's vocabulary is the design's, not the
  crate's, and §13 does not say where they differ: T-B-01 (JSONL `@m`/enum vocabulary has no
  emitter; M7B-78 vacuous), T-B-02 (no carrier for kernel-internal events/effects; BA-1 and BA-2
  cannot both hold on landed C0), T-B-03 (M7B-111/137 assert the wrong module for §8.4 and the
  right half has no row). T-B-04 is the T-23 table. None is a BLOCKER: no charter acceptance row
  is unsafe or absent; the developer can start on §D's list today.
- Ranked: **T-B-03** (V3 gate coverage claim) > **T-B-02** (day-one contract decision) >
  **T-B-01** (six sim rows and both Q-rows untestable as written) > T-B-04 > T-B-05..08.

---

## F. Questions for the lead (defaults)

| # | Question | Default |
|---|---|---|
| Q-B-1 | Carrier for kernel-internal events and effects (`Ignored`, `Alert`, `SetAdmission`, `QualificationChanged`, `HealthEval`→`TimerFired`, `Recovered`, F1's set): C0 `EventKind::Kernel(KernelEvent)` / `EffectKind::Kernel(KernelEffect)` with kernel-b's variants (as foundation did for `AppendReject`), or kernel-b-private types? | C0 pair, foundation ask; developer starts on a private pair + `From` shim now (T-B-02) |
| Q-B-2 | How does `Committed{Active}` (rebuild activation) reach T1/P1/L1? Design §5.6a names only the CAS. | F1 re-emits `Recovered(RecoveryResult{mode: Active, ..})`; architect one paragraph; M7B-137 asserts it (T-B-03) |
| Q-B-3 | Design §5.1 has `Committed \| Conflict \| QuorumLost`; landed `CasOutcome` has `Committed \| Conflict \| Unknown \| Unavailable`. | `Unavailable` → `Blocked{ControlUnavailable}`, `Unknown` → `Blocked{ControlUnknown}`, both "never retry blind"; M7B-109 enumerates both (T-B-04) |
| Q-B-4 | `AppendReject::NeedPrefix{have}` in flight lacks `head_digest`, which §3.6 step 1 / K-B-45 need. Amend the B-R30 Q2 ask? | Yes: `NeedPrefix { have, head_digest }`; M7B-21/55/56/57/63/124/140 `Unavailable` until then |
| Q-B-5 | Is `DivergenceDetected` re-emitted by the tracker on the routed path (§3.4 item 1)? | No; M7B-140/144 stand, M7B-41 stands, one clause in §3.4 (T-B-08) |
| Q-B-6 | Trace name for L1's `Reprotecting`: rename `ProtectionPhase::Resuming` or trace `Reprotecting` as `Resuming`? | Trace as `Resuming` (verification's M7V-02/41 already use it); one sentence in design §4/ADR-0006; M7B-78 rewritten (T-B-01) |
| Q-B-7 | Lift M7B-143/144's provisional marker? | Yes (§A evidence) |
| Q-B-8 | `AckRejectReason` widened by the seven missing variants, or `ReplicationAck.reject_reason: Option<ErrorKind>`? | Widen the enum (seven variants, kernel-b's names in Rust, as `AppendReject` was done); Q-46/47 keyed to it (T-B-01) |

---

# Round 5: diff pass over the test plan (9c9b1f9 -> 3051c5d)

Scope: commits `6c5a929` (round 4, 148 rows), `ab85c14` (CB-5), `3051c5d` (CB-5 wording + the
M7B-116 pipe fix). Diff only. Rows closed in rounds 1-4 are not re-litigated. Rulings B-R33 and
**B-R34** taken as binding: M7B-109/110/146 are correctly `Unavailable` (no start-now path exists
for a variant that does not compile), and `CasOutcome::Unavailable` / `Unknown` are never merged.

## A. Standing check 1 — the verbatim-mirroring table

Each listed pair diffed against the line it mirrors. Design is kernel-b `design.md`; ADRs at
`9c9b1f9`; `authority.rs` read in the working tree.

| Pair | Cited line | Verified | Result |
|---|---|---|---|
| M7B-146 <-> design 1437 | `Unknown` -> `Blocked { reason: ControlUnknown }` … "the CAS is *more* likely to have landed than in the `Unavailable` case, which is exactly why a blind retry is worse here, not better" | yes | **matches**. Row elides "than in the `Unavailable` case" and "not better" mid-quote without ellipsis; meaning unchanged, no finding |
| M7B-146 <-> ADR 0009:326 | "An earlier draft of this ADR named the last two together as `QuorumLost`" | yes | **matches** |
| M7B-148 <-> design 1752 | `ActivationProposed --CasResult{outcome}--> Committed { mode: Active }` … 1753-1754 "and, on `Committed(revision)`, re-emit / `Recovered(RecoveryResult { mode: Active, .. })`" | yes | **matches** |
| M7B-148 <-> ADR 0009:265 | "**Activation is announced, not just written.**" | yes | **matches** |
| M7B-147 <-> design 1320 | "**`Reprotecting` is traced as `ProtectionPhase::Resuming`**" | yes | **matches** |
| M7B-147 <-> ADR 0006:198 | "`ProtectionPhase::Resuming`, and a test reading a `protection_state` line sees `Resuming`." | yes | **matches** |
| M7B-140 <-> design 729 / ADR 0005:304 | "`DivergenceDetected` **only on the rule-9 path**"; "on the routed path the tracker does not re-emit the detection event it was just handed" | yes | **matches**. The row keeps `iff` on items 3 and 4, so its "four-wide here" is a shape statement, not a length assertion |
| M7B-41 <-> design 729 | "the vector is five-wide from rule 9 and four-wide from the routed path" | yes | **DRIFTED — T-B-09** |
| M7B-139 <-> design 625-629 | `lookup(cutoff_seq)` table: `Match` / `Differs` / `NotRetained` | yes | **matches**, and exactly **three** arms — see section D |
| M7B-128 <-> design 1746-1751 | `Rebuilding --CopyLost-->` drop proof, `Alert{RebuildStalled}`, "`required` is NEVER shrunk", else no effect | yes | **matches**. The twin's `== [Ignored{NOT_REQUIRED}]` is the correct BA-2 reading of "no effect" |
| BA-4 <-> design section 7 preamble, design 1320, ADR 0006:198 | two assertion surfaces; `Reprotecting` traced as `Resuming` | yes | text matches; **row list drifted — T-B-13** |
| section 15 drift table <-> crate | `BlockReason` `authority.rs:198` (derive at 197), one variant `DivergenceRequiresOperator{diverged: Vec<CopyId>}`; `PartitionMode` `authority.rs:212` | yes | line numbers and payload **correct**; but the table's **basis commit is wrong — T-B-10** |

## B. Standing check 2 — pipe counts

Every table row counted for unescaped pipes against its own header width (7-column tables: 8; the
section 9 eight-column *Was* table: 9; the section 11 four-column Q table: 5). All section 9 rows
are at 9, so the `3051c5d` M7B-116 fix holds. Two rows are over:

| Line | Row | Bare pipe | Origin |
|---|---|---|---|
| 370 | Q-51 | inside `` `rg -n -i "backstop...re-read.*flag" …` `` — 6 pipes, expect 5 | pre-existing (`bd2b458`) |
| 376 | Q-57 | inside the `rg` regex `…vec!\[\s*\]...effects\.is_empty\(\)"` — 6 pipes, expect 5 | **new in `6c5a929`** |

Both render one phantom column in section 11. Reported as **T-B-15**.

## C. Findings

### T-B-09 — M7B-41 labels a three-effect literal "five-wide" (MATERIAL-low)

- **Criterion.** Section 2's landed-literal rule and BA-1: a row asserts effect-vector contents
  *and order by index*, so its stated width must be the width its fixture produces.
- **Row / location.** Section 4, M7B-41, assertion cell (plan line 141). Introduced by `6c5a929`.
- **Evidence.** The cell asserts `[DivergenceDetected(B), Alert{CopyDiverged}, CopyLost{B,
  Diverged, tick}]` — three effects — then says "**five-wide from rule 9, with `DivergenceDetected`
  at index 0**". The same cell then says "No `QualificationChanged` (C still qualifies); no
  `BlockPartition`". Design 728-740 makes items 3 and 4 conditional ("only if"), so five is the
  *maximum* width, not this fixture's. M7B-140 got this right by keeping `iff` on both items.
- **Consequence.** A developer who writes the stated width fails a correct kernel; one who writes
  the literal passes. The cell contains both instructions.
- **False-positive check.** Could "five-wide" mean the rule-9 *shape* rather than this vector?
  Possible, and that is the defect — the design's own sentence is loose in the same way, and the
  row copied the looseness into a place where a length is asserted.
- **Closure.** Either (a) reword to "the rule-9 shape, `DivergenceDetected` at index 0; this
  fixture fires neither conditional effect, so the literal is three-wide", or (b) add a twin whose
  fixture drops the floor so the full five-wide vector is asserted once. (a) is enough.

### T-B-10 — section 15's declared basis commit predates the contracts it cites (MATERIAL)

- **Criterion.** Section 15: "Every row in this plan is written against **landed C0 at `8a23b1d`**
  … read on 2026-09-20" and "Checked against the crate at `authority.rs:197` on 2026-09-20, not
  inferred from the design."
- **Evidence.** `git show 8a23b1d:crates/rdb-core/src/contracts/authority.rs` ->
  `fatal: path … exists on disk, but not in '8a23b1d'`. The file landed in **`6893442`**
  ("foundation correction round 1"), a descendant of `8a23b1d`. That commit rewrote **every**
  contracts file section 15 is written against: `authority.rs +224`, `trace.rs +277`,
  `control.rs +148`, `envelope.rs`, `event.rs +100`, `membership.rs +104`, `storage.rs`, `time.rs`,
  `ids.rs`, `digest.rs`, `errors.rs` — 943 insertions.
- **Consequence.** Three concrete drifts, all in the direction of over-holding:
  1. **`PartitionConfig.min_regular_acks` has landed** (`membership.rs:74`, with
     `DEFAULT_MIN_REGULAR_ACKS` and a `validate` that rejects 0 — B-R3). Section 13 still holds
     **M7B-52** on "foundation lands the contract item". That row can start now.
  2. **`AppendReject` went from 5 variants to 16.** At `8a23b1d`: `NeedPrefix`, `DigestMismatch`,
     `StaleEpoch`, `IncompatibleConfig`, `Unauthenticated`. At HEAD it also has `Quarantined`,
     `DivergentHistory`, `CorruptHistory`, `StaleFence`, `NotAMember`, `StaleGeneration`,
     `NeedLineage`, `NeedConfig`, `UnknownEpoch`, `TooLarge`, `WrongPartition`, `StaleConfig`.
     Five ladder literals the rows spell as developer strings — `QUARANTINED`,
     `DIVERGENT_HISTORY`, `CORRUPT_HISTORY`, `STALE_FENCE`, `NOT_A_MEMBER` (M7B-02..07, 14, 18, 20,
     28, 120..122) — now have landed variants, and section 15's rule says the crate wins in a row's
     literals. Section 15 does not mention this at all.
  3. **`EventKind::ExternalFenceVerified` and `EffectKind::AdoptAuthority` have landed.** BA-10
     enumerates the landed sets as six and five variants; both lists are now short by one.
- **What survives.** All five asks re-checked at HEAD and **all five are still correctly open**:
  CB-1 (no `Kernel` variant in either enum), CB-2 (`NeedPrefix { have }` only, `envelope.rs:581`),
  CB-3 (`AckRejectReason` still seven variants, now at `trace.rs:292`), CB-4 (no `AppendOutcome`
  type anywhere), CB-5 (`BlockReason` still one variant). B-R34 is unaffected, and so is the
  `Unavailable` status of M7B-109/110/146.
- **False-positive check.** Is `8a23b1d` a deliberate frozen basis rather than a stale one? No:
  section 15 cites `authority.rs:198` and `:212` as *landed* in the same table, and those lines
  exist only after `6893442`. The document already mixes the two commits; it just does not say so.
- **Closure.** Re-read the contracts at `6893442` (or HEAD), restate section 15's basis as that
  commit, add an `AppendReject` row to the drift table, correct BA-10's two enum lists, and release
  M7B-52 from section 13. This is the check the lead warned is not self-refreshing.

### T-B-11 — M7B-110's dependency cell says `none` (MATERIAL-low)

- **Criterion.** Section 14: "Every row whose dependency column reads CB-1..CB-5 reports
  `Unavailable` with that id as the reason, never a pass". The gate item is keyed on the **column**.
- **Row / location.** Section 8.3, M7B-110, Dependency column (plan line 261).
- **Evidence.** The row's own assertion text: "Landed `BlockReason` does **not** have
  `NoEligibleRegular` … so this row is `Unavailable` on CB-5". Section 13: "M7B-109, 110, 146 |
  **CB-5**". B-R34: all three are `Unavailable`. The cell reads `none`. M7B-109 and M7B-146 read
  `CB-5`.
- **Consequence.** The row looks start-now-able in the one place a developer scans, and the
  section 14 item that would catch it reads the column, so it passes over this row silently.
- **False-positive check.** Could `none` mean "no seam beyond BA-10's carrier" (section 2's
  convention)? No — M7B-109 and M7B-146 share that carrier and still read `CB-5`.
- **Closure.** Dependency cell for M7B-110 -> `CB-5 (landed BlockReason has no NoEligibleRegular)`.

### T-B-12 — section 14's CAS line reads as "covered" on its own (MATERIAL-low) — *the lead's specific ask*

- **Criterion.** Section 14 is the gate. A reader who reads only section 14 must not conclude that
  work which cannot run has run.
- **Location.** Section 14, the CAS line.
- **Evidence.** It reads, in full: "CAS: M7B-106, 107, 108, 109, 146 cover all four landed
  `CasOutcome` arms, and no test names `QuorumLost` (`rg -n "QuorumLost" crates/rdb-sim/tests`
  returns nothing)." Two of the five named rows — M7B-109 (`Unavailable`) and M7B-146 (`Unknown`)
  — cannot compile until CB-5 lands. The sentence asserts coverage flatly and names no exception.
- **Consequence.** The box ticks green while the two arms the design most cares about keeping apart
  have never been exercised. **Answering the lead's question directly: no, section 14 does not
  state it plainly enough.**
- **Why the mitigation is not enough.** The generic line "Every row whose dependency column reads
  CB-1..CB-5 reports `Unavailable`…" is four lines away, names no row, and requires the reader to
  leave section 14 for section 13 and then read two dependency cells — one of which is wrong
  (T-B-11). A reader who stops at the CAS line never learns there is an exception, and a reader who
  follows the generic line lands on M7B-110's `none`.
- **False-positive check.** Could the CAS line be read as a claim about the *plan's* coverage
  rather than the *run's*? Yes — and every other line in section 14 is a claim about the run
  ("pass", "runs", "returns nothing"), so that reading is not available in context.
- **Closure.** Make the exception local to the line, e.g. "CAS: M7B-106, 107, 108 pass; M7B-109 and
  M7B-146 are `Unavailable` on CB-5 and this box cannot be ticked green until the enum widens —
  the `Unavailable` and `Unknown` arms are **not** covered today; and no test names `QuorumLost`."

### T-B-13 — BA-4's row list names M7B-146 and omits M7B-147 (ADVISORY)

- **Criterion.** BA-4's trailing column is the traceability list of rows that depend on the
  log-line contract.
- **Evidence.** The list ends "… M7B-26, 32, 62, 78, 96, 137, 146". M7B-146 is a unit row asserting
  `Blocked{reason: ControlUnknown}` and makes no trace assertion at all. M7B-147 is the sim row
  whose whole content is `protection_state` JSONL lines with `phase == Resuming` — and BA-4's own
  prose names it: "paired with a positive control (M7B-78 / M7B-147)". Q-57's list has it right
  ("… 137, 142, 147").
- **Consequence.** Traceability only; no row's assertion moves. Looks like a transposition.
- **False-positive check.** Does M7B-146 touch a trace surface? No — its assertions are all on the
  effect vector and the `Blocked` value.
- **Closure.** BA-4 row list: `146` -> `147`.

### T-B-14 — section 9's ordering note contradicts M7B-146's dependency (ADVISORY)

- **Evidence.** Section 9 closes: "M7B-146..148 depend on nothing beyond BA-10's carrier."
  M7B-146's own Dependency cell reads `CB-5`, section 13 lists it under CB-5, and B-R34 rules it
  `Unavailable`.
- **Consequence.** Same class as T-B-11 — a second place telling a developer M7B-146 is startable.
- **Closure.** "M7B-147 and M7B-148 depend on nothing beyond BA-10's carrier; M7B-146 is
  `Unavailable` on CB-5."

### T-B-15 — two unescaped pipes in section 11 (ADVISORY)

- **Evidence.** Section B above. Q-57 (new in `6c5a929`) and Q-51 (pre-existing) each carry one
  bare pipe inside a code span; the section 11 table is four columns and both rows render five.
- **Closure.** Escape the alternation pipe in both `rg` patterns.

## D. Checked and clean

- **`Unavailable` vs `Unknown` are not merged anywhere.** design 1436-1441 keeps them apart;
  section 15's `CasOutcome` row, M7B-109, M7B-146 and the section 14 CAS line all assert the two
  `Blocked` reasons **by value**. Nothing in the diff moves toward a collapse. B-R34 holds as
  written.
- **`authority.rs` line numbers are right.** `#[derive]` at 197, `pub enum BlockReason` at 198,
  one variant `DivergenceRequiresOperator { diverged: Vec<CopyId> }`, `pub enum PartitionMode` at
  212 with `Blocked { reason }`. The plan's mixed 197/198 citations both point at the same enum.
  M7B-142's `diverged` payload assertion matches the landed field.
- **M7B-116's pipe fix is correct** and no other section 9 row is over width.
- **Section 16 counts reconcile**: section 9 is 26 unit + 3 sim = 29; 136 unit + 12 sim = 148
  active; Q-46..Q-57 = 12.
- **The "three arms vs four arms" disagreement recorded in section 15 resolves in the plan's
  favour.** `history_digests.lookup(cutoff_seq)` has exactly three arms at design 625-629 (`Match`,
  `Differs`, `NotRetained`); the four-arm item is `CasOutcome` at design 1432-1437. The plan's
  section 9 preamble should stay "three arms". The architect's round-5 note is wrong on this point;
  no row moves.

## E. Verdict

**PASS_WITH_RISKS.** The diff is a real improvement: BA-8 now states the landed `PartitionMode`
and the one-variant `BlockReason`, CB-5 is carried consistently through sections 1/13/15, the CAS
split is asserted by value, and M7B-147 closes T-B-01's absence gap. Six findings, ranked:

1. **T-B-10** (MATERIAL) — section 15's basis commit `8a23b1d` predates `authority.rs`; the whole
   drift table needs re-reading at `6893442`. One row (M7B-52) is held on an item that has landed
   and one contract (`AppendReject`, 5 -> 16 variants) is absent from the table. All five CB asks
   survive the re-read.
2. **T-B-12** (MATERIAL-low) — section 14's CAS line reads as covered. This is the lead's question
   and the answer is no.
3. **T-B-11** (MATERIAL-low) — M7B-110's dependency cell says `none`.
4. **T-B-09** (MATERIAL-low) — M7B-41 labels a three-effect literal five-wide.
5. **T-B-13**, **T-B-14**, **T-B-15** (ADVISORY) — BA-4 row list, section 9 ordering note, two
   bare pipes.

No finding disturbs a row closed in rounds 1-4, and none argues for merging `Unavailable` with
`Unknown`.

## F. Questions for the lead

| # | Question | Default if no answer |
|---|---|---|
| Q-B-9 | Re-read section 15 at `6893442` now, or defer to the next round? | Now — T-B-10's three consequences all point at over-holding, and the drift grows with every foundation commit |
| Q-B-10 | Should the five ladder literals (`QUARANTINED`, `DIVERGENT_HISTORY`, `CORRUPT_HISTORY`, `STALE_FENCE`, `NOT_A_MEMBER`) be respelled as the landed `AppendReject` variants? | Yes — section 15's own rule is "the crate wins in a row's literals" |
| Q-B-11 | Release M7B-52 from section 13 on the landed `min_regular_acks`? | Yes (`membership.rs:74`) |
| Q-B-12 | Is `8a23b1d` a deliberately frozen basis the plan should keep citing? | No — section 15 already cites post-`6893442` lines |
