# Trace requirements — team verification to team foundation (via the lead)

**From:** verification architect, 2026-09-20
**To:** team foundation (C0 trace vocabulary, I1 trace/replay) — routed by the lead
**Why:** spike §4's trace seam names the *shape* ("ordered choices/events, oracle checkpoint
digest") but not the fields. The oracle checks client-visible logical state and declared lineage
only (spike §6). Every field below is one a checker reads, **except the six named in §7** — listed
there rather than left to look load-bearing. Nothing here asks for kernel internals.

Charter BUDGET/STOP says to report BLOCKED if the vocabulary cannot express an invariant. **Not
blocked** — this is the request that prevents that.

**Revised after critic round 1 (rulings V-R9, V-R10).** Three changes are not new fields but
*emission rules*, and each one closes an invariant that was weaker than its own statement: §3.5
(where an ACK is emitted), §3.14 (when protection state is emitted), §3.8 (the outcome set). See §5
items 4–6. Two fields were **withdrawn** (§6).

**Revised again after critic round 2 (ruling V-R12).** One new event kind, §3.19 `topology_change`,
and the header `topology` is now explicitly the *initial* membership only. See §5 ask 7.

**Revised after critic round 3 (ruling V-R20, critic T-23).** C0 **landed at `8a23b1d`**
(`crates/rdb-core/src/contracts/trace.rs`, `ids.rs`). Where the landed shape differs from the ask
below, **the landed shape wins and this document adopts it** — §8 is the drift table (field →
landed shape → resolution), and the developer types the landed names. Two entries are not
adoptions: `quorum_rule` is **withdrawn** from §3.14 (the oracle derives it from
`required_copy_set.len()`, design §2.3), and header `provenance` is the **one open contract ask**,
with its exact shape in §8.1. The §3 subsections keep their original field lists as the record of
what was asked; a landed-name note is added only where a checker rule would otherwise read wrong.

## 0. Ground rules we are assuming

1. **No key or value bytes, ever.** team-rules: "Never log key or value bytes." Everywhere below,
   a key is a `key_id` (a stable small integer assigned by the scenario generator) and a value is a
   `value_version` plus a `digest`. If C0 wants opaque hashes instead of generator ids, that works
   too — we need identity and ordering, not content.
2. **One total order.** `event_id: u64`, strictly increasing within a trace, assigned by I1. The
   oracle is a single left-to-right fold; it never sorts.
3. **Every field is declared by the emitting component.** The oracle compares declarations against
   each other. It does not recompute a digest or re-derive a decision.
4. **Closed sets.** Every `outcome` / `reason` / `mode` / `state` field must be a Rust enum, not a
   string. rEtcd M6 rows M6-118 and M6-122 exist because open reason sets make assertions
   impossible.

## 1. Trace header

Spike §4 already requires most of this; listing it so nothing is dropped.

| Field | Type | Used by |
|---|---|---|
| `schema_version` | `u16` | every checker; a bump invalidates checked-in fixtures on purpose |
| `provenance` | `Provenance` = `Generated{seed: u64}` \| `Reduced{parent: ScenarioId}` \| `Authored{case: String}` — **landed at `6893442`, `trace.rs:85`, arm for arm** (§8 row 1; the ask is closed and §8.1 is kept only as its record). The header's field is at `trace.rs:212`; there is **no** `seed` field | report, reproducer (F18). Not a bare `seed`: a reduced or authored scenario is not in the generator's image, so replaying its seed reproduces nothing and the checked-in fixture is the reproducer. Matches `design.md` §3 |
| `generator_version` | `u16` | spike §4: seed alone is insufficient |
| `config` | landed at `6893442` as `config: RunManifest{budgets, overridden: Vec<BudgetName>, nodes: u8, event_cap: u32}` (`trace.rs:187`, `:213`); `config_digest` and the bare `budgets` are **gone** (§8 row 10) | report; unknown fields must error (spike §7). `overridden` says which budgets were not the spec defaults, so an overridden run is never mistaken for a default one |
| `partitions` | `u8`, landed at `6893442` (`trace.rs:214`) — **state it, do not derive it** (§8 row 8) | INV-ISO, coverage (V-R8's two-partition topology) |
| `topology` | landed as a flat `Vec<TopologyEntry{partition, node, role: ReplicaRole, config_version}>` in ascending `(partition, node)` order, which is also its field order and its derived `Ord` (K-F-38) — the **initial** membership only; there is no `config_version_0` field (each entry carries its own; §8 row 8). Later membership arrives as `topology_change` events (§3.19, F19) | **INV-PUB (role grounding — see §3.5), INV-LOSS, INV-ISO, coverage.** This is the one place a node's role is a *declared input* rather than a kernel-computed label, which is why INV-PUB resolves roles here and treats `replication_ack.peer_role` as a claim to cross-check |
| `oracle_checkpoint_digest` | `[u8; 32]` | I1 replay equality; not read by a checker (§7) |

## 2. Event envelope — on every event

| Field | Type | Notes |
|---|---|---|
| `event_id` | `u64` | total order, strictly increasing |
| `logical_tick` | `u64` | simulated time. Never a wall clock. |
| `kind` | enum (§3) | |
| `partition_id` | `PartitionId` | |
| `node_id` | `NodeId` | the node the event happened on |
| `boot_id` | `BootId` | distinguishes a restarted node from itself |
| `correlation_id` | `CorrelationId` | ties a request to its applies, acks, publish and outcome. **Load-bearing for INV-ATOM, INV-DEDUP and INV-VER** — without it the oracle cannot tell which apply belongs to which submit. |

## 3. Event kinds and their fields

Ordered roughly along the write path. The right-hand column is the checker that would break without
that event kind.

### 3.1 `client_submit`
`request_id`, `tenant`, `client_id`, `affinity_id`, `expected_generation`, `request_digest`,
`deadline_remaining_ms`, `mutation_keys: Vec<KeyId>`, `condition_keys: Vec<KeyId>`
→ INV-DEDUP, INV-ATOM

### 3.2 `admission_decision`
`outcome: Admitted | Rejected`, `reason: AdmissionReason` (closed set = spec §5.4 error list;
landed as `reason: Option<ErrorKind>`, `None` when admitted — one enum for every §5.4 error, no
second `AdmissionReason` type; §8 row 9),
`admitted_seq: Option<Seq>`, `paused: bool`, `oldest_unsafe_age_ms: u64`,
`required_copies: Vec<NodeId>`, `config_version`
→ INV-LAG, INV-DEDUP (the `GENERATION_CHANGED`-before-mutation rule), coverage

### 3.3 `authority_decision`
`gate: Admission | Dispatch | Publication | Reply`, `owner_node`, `owner_epoch`, `grant_id`,
`grant_boot_id`, `generation`, `valid_from_tick`, `expiry_tick`, `decision_tick`,
`outcome: Valid | Expired | Fenced | Uncertain`
→ **INV-AUTH**. All four gates must emit; spec §5.2 revalidates at each, and INV-AUTH's
overlap check needs `valid_from_tick` and `expiry_tick`, not just "valid now".

### 3.4 `batch_apply`
`role: Primary | RegularSecondary | Shadow`, `generation`, `seq`, `predecessor_seq`,
`predecessor_digest`, `entry_digest`, `key_versions: Vec<(KeyId, Version)>`,
`outcome: Applied | Failed | CrashedBeforeCommit | CrashedAfterCommit`
→ **INV-ATOM, INV-LIN**. `key_versions` is the after-image identity the oracle folds into its
published map; it must list every key the batch touched, including deletes (as a tombstone version).

### 3.5 `replication_send` / `replication_ack`
`from_node`, `to_node`, `peer_role: Regular | Shadow` (landed: `peer_role: ReplicaRole` =
`Primary | RegularSecondary | Shadow`, no `PeerRole` type and no `Regular` variant; §8 row 5),
`peer_boot_id` (landed: `peer_boot: BootId`; §8 row 4), `config_version`,
`generation`, `owner_epoch`, `contiguous_seq`, `contiguous_digest`,
`durability_class: Buffered | Durable`, `accepted: bool`,
`reject_reason: Option<AckRejectReason>` (closed set of **fourteen**: `Gap`, `DigestMismatch`,
`StaleEpoch`, `StaleBoot`, `StaleConfig`, `ForgedIdentity`, `IncompatibleVersion`,
`StaleGeneration`, `RoleMismatch`, `InconsistentProgress`, `RegressedProgress`, `Unverifiable`,
`Diverged`, `NotAMember`)
→ **INV-PUB**, **INV-LOSS**, **INV-LAG**

**The tripwire fired, and it fired correctly (lead ruling F-R20, 2026-09-20 23:08 PDT).**
Kernel-b's ask **CB-3** (ruling B-R33 Q-B-8) landed at `f616ddf`: `AckRejectReason` went from
seven variants to **fourteen** (`crates/rdb-core/src/contracts/trace.rs:315`, the seven added at
`:331`, `:333`, `:335`, `:337`, `:339`, `:341`, `:343`). `M7V-56` asserts **set equality** between
the enum and the coverage lists, so it went red the moment they landed. That is the row working,
not the row breaking. The lead ruled **explicitly** that `M7V-56` is **not** to be weakened to a
subset assertion: a set-equality assertion that degrades to subset the first time it fires was
never an assertion. The response is the seven coverage cells, written out in the verification
plan's **§15.1**; three of them are real gaps and are recorded as gaps with owners rather than
closed with an empty cell.

**Two of the fourteen are also `AppendReject` variants, and the list must stay qualified.**
`StaleGeneration` (`envelope.rs:534`) and `NotAMember` (`envelope.rs:568`) exist on
`crate::contracts::envelope::AppendReject` as well. Same words, two enums, two meanings: there,
why a **replica refused an append**; here, why the **primary would not count an acknowledgement**.
Foundation kept both deliberately (kernel-b's §15 CB-3 row: "one concept, two paths"), so nothing
here renames either. The discipline that follows: every list, table and row literal in
verification's files spells these two **enum-qualified** — `AckRejectReason::StaleGeneration`,
never a bare `StaleGeneration`. `coverage.rs`'s `ACK_REJECT_REASONS` is already enum-qualified by
Rust's own syntax and cannot conflate them. The **JSONL** `reject_reason` field serialises a bare
variant name, which would conflate them if a query ever unioned it with an `AppendReject`-valued
column; no Q-row in the verification plan does (checked at `f616ddf` — `reject_reason` appears
only in M7V-55, M7V-56, M7V-69 and design §6's guard axis, never in a union), and any future one
must project the source column name alongside it.

**Emission point — ruling V-R10, and it inverts the invariant's strength.** `replication_ack` is
emitted **where the ACK is generated: at the secondary**, with a **separate delivery record at the
primary** (`replication_ack_delivered { ack_event_id, accepted }`; landed as
`ReplicationAckDelivered { ack: EventRef, from_node, peer_role, config_version, counted }` —
`counted` is the delivery-half fact INV-PUB reads; §8 row 20).

If only *delivered* ACKs were traced, a secondary that applied and whose ACK was then dropped by
`NetworkOp::Drop` would be invisible as a holder. INV-LOSS would under-count holders and **permit
loss it should forbid** — the checker would be silently weaker than its own statement. With
generation-point emission the holder map is right, and the delivery record still gives INV-PUB the
"did the primary actually have the ACK when it published" half.

**`peer_role` is a cross-checked field, not a trusted one.** The oracle resolves a node's role from
the environment's topology **in force at that `config_version`** — the header declaration plus
`topology_change` events (§3.19); a mismatch between the ACK's claim and that topology is itself a
violation. `AckRejectReason::ForgedIdentity` is produced by `NetworkOp::ForgeAck` and has a required
coverage cell.

### 3.6 `durability_advance`
`generation`, `durable_seq`, `durable_digest`, `flush_ticket`,
`sync_wal_through_prefixes: Vec<(PartitionId, Seq)>`, `outcome: Synced | Failed | Partial`
→ INV-LOSS (MUT-5), INV-LAG (`resume_barrier_seq` must be met exactly)

### 3.7 `publish`
`generation`, `seq`, `published_digest`,
`ack_evidence: Vec<(NodeId, BootId, PeerRole, DurabilityClass)>` (landed at `6893442` as
`Vec<AckEvidence { node, boot: BootId, role: ReplicaRole, durability }>` — **four** fields; the
boot asked for here is present, added under foundation's K-F-22 citing this paragraph. The
round-3 workaround of pairing `node` with the boot on that node's `replication_ack` is
**withdrawn**: read the field, and treat a disagreement with the paired ack at the same `seq` as
a trace defect; §8 row 6),
`authority_recheck: EventId` (points at the `authority_decision{gate=Publication}`)
→ **INV-PUB**. `authority_recheck` as an explicit back-reference is what lets the checker be a
single forward fold instead of a search.

### 3.8 `client_outcome`
`request_id`, `partition`, `outcome: ClientOutcome`, `generation`, `seq: Option<Seq>`,
`result_digest`, `delivered: bool`

**`ClientOutcome = Success | RecoveredApplied | <every §5.4 error>`** (ruling V-R10). The earlier
"`Success` + every §5.4 error" could not express `RECOVERED_APPLIED`, which is a *status* result and
not an error — and spec §8.1 plus spike §6's mandatory F1/T1/P1 case both require it. INV-DEDUP's
"absence is never proof of nonexecution" clause is the checker that needs it.
→ INV-ATOM, INV-DEDUP, INV-LIVE. `delivered: false` is how the oracle models a lost reply and
checks that publication is not retracted (§5.3).

### 3.9 `read` / `status`
`request_kind: Read | Status | Export | ActorRead`, `barrier: BarrierRef`, `generation`,
`observed_seq`, `observed_key_versions: Vec<(KeyId, Version)>`, `recovery_mode: bool`,
`outcome: ReadOutcome`
→ **INV-PUB** (nothing above the published prefix), **INV-LOSS** (this is where loss is *observed*)

### 3.10 `dedup_record`
`tenant`, `client_id`, `request_id`, `request_digest`, `result_digest`, `generation`,
`retained_until_tick`, `action: Store | Hit | ReuseReject | Expire`
→ **INV-DEDUP**

### 3.11 `lineage_root`
`partition`, `generation`, `owner_epoch`, `base_seq`, `base_digest`, `predecessor_generation`,
`predecessor_cutoff`, `source: Initial | Recovery`
→ **INV-LIN, INV-LOSS**. `predecessor_cutoff` is the single field that makes restricted loss
checkable rather than an impossible global no-loss claim (spike §6).

### 3.12 `recovery_decision`
`fenced_epoch`, `discovery_window_ticks`,
`queried_sources: Vec<{ node, boot, role, reachable: bool, reported_generation,
reported_seq, reported_digest }>`, `selected_source: Option<NodeId>`, `selected_cutoff_seq`,
`selected_digest`, `mode: TwoSurvivor | LoneSurvivorReadOnly | Quarantine`,
`loss_uncertainty: bool`, `new_generation`
→ **INV-LIN, INV-LOSS**. `queried_sources` with `reachable` is the precondition INV-LOSS checks
before allowing a suffix to vanish. Without it the checker cannot distinguish permitted loss from a
bug.

### 3.13 `quarantine`
`reason: DigestConflict | CorruptHistory | IncompatibleVersion`, `generation`, `seq`,
`sources: Vec<NodeId>`
→ INV-LIN

### 3.14 `protection_state`
`phase: Healthy | Warn | Paused | Resuming` (landed name is `phase: ProtectionPhase`, not
`state`; §8 row 3), `oldest_unsafe_age_ms`, `required_copy_set: Vec<NodeId>`, `config_version`,
`paused_prefix_seq`, `resume_barrier_seq`, `healthy_since_tick: Option<u64>`
→ **INV-LAG, INV-PUB**

**`quorum_rule` is withdrawn from this ask (ruling V-R20, critic T-23).** It was listed here as
`quorum_rule: Rf3 | DegradedRf2`; the landed C0 has no such field and no `QuorumRule` enum, and it
is not needed: the oracle derives the rule from `required_copy_set.len()` on this same event — two
nodes is `DEGRADED_RF2`, three is RF3, any other length is a violation (design §2.3). A length is
a declared fact, so the derivation is not a second implementation. The coverage cell is keyed on
the derived value (design §6, `derived_quorum_rule`).

**Emission cadence — ruling V-R10.** Emitted on every protection-state transition **and on every
`config_version` change**, even when `phase` is unchanged (landed field name; §8 row 3 — critic
T-41).

This is what INV-PUB's degraded-RF2 rule stands on. Spec §8.3: "While running with two copies, both
are required for every successful transaction. There is no one-copy fallback." The oracle must know
the *currently pinned* `required_copy_set` (and the quorum rule its length implies) to check a
publish, and a
`StorageOp::Crash` on a secondary plus a `RecoveryOp::Synchronize` reaches `DEGRADED_RF2` in two ops
— so this is a state the campaign will pass through routinely. Without the cadence rule the oracle
would apply the healthy RF3 one-ACK rule while degraded and pass the exact bug spec §8.3 spends a
paragraph forbidding.

`config_version` on the same event is also what catches "unsafe age reset via membership renaming"
(spec §6.2: "No timer reset merely because a replica was renamed/replaced").

### 3.15 `version_check`
`surface: Message | Record`, `declared_protocol_version`, `declared_config_version`,
`declared_schema_version`, `known_max`, `mandatory_unknown_fields: Vec<FieldId>`,
`outcome: Accept | RefuseBeforeApply`
→ **INV-VER** (V12 subset)

### 3.16 `fault_injected`
`fault_kind: FaultKind` (mirrors the six scenario groups), `target`, `boundary: BoundaryId`
(closed set = the "required boundary cases" column of spike §6's table, **and nothing else**),
`scenario_op_index: usize`
→ coverage matrix (all three axes), and `Signature.faults`. Without `boundary` as a closed enum, the
fault-boundary axis is not countable and the "every required cell ≥ 1" rule cannot be written.

`BoundaryId` gains exactly two members for the two new sim-provider faults (V-R9): a forged-identity
ACK (spike §4 transport) and a false durable watermark (spike §6 storage). Both were already
required boundary cases in the spike; neither had an id.

### 3.16a `op_skipped`
`scenario_op_index: usize`, `reason: ReferentGone | OutOfBudget`
→ reducer diagnostics only; **not** read by a checker, **not** a coverage cell.

**Landed at `6893442`** as `TraceKind::OpSkipped { scenario_op_index: u32, reason: SkipReason }`
(`trace.rs:1115`) with `SkipReason { ReferentGone, OutOfBudget }` (`trace.rs:634`), its own kind
exactly as the argument below asked, rustdoc citing K-F-08 and this section. The index is `u32`,
not `usize` — the same adoption as §8 row 18. §8 row 21 is closed; it was carried as open for
four rounds after it shipped.

Its own event kind on purpose. The reducer's skip rule (a deleted op leaves a later op with no
referent) must not be expressed as `fault_injected{boundary="op_skipped"}`, because that puts a
reducer artifact into the enum the fault-boundary coverage axis counts, producing a cell in
`rdb-m7-coverage.json` that nobody can interpret and that has no required count. `BoundaryId` stays
exactly equal to spike §6's column; that identity is what makes the coverage rule writable.

### 3.17 `schedule_phase`
`phase: Chaotic | Healed`, `fair_delivery: bool`, `remaining_event_budget: u32`
→ **INV-LIVE and INV-ISO** (F22). This is the only thing that arms either checker — INV-ISO is
armed exactly like INV-LIVE, off this same event. Spike §6 forbids calling an unhealed partition a
liveness failure, so the oracle must be told when the schedule healed; it must not infer it.

### 3.18 `capability`
`package: PackageId` (`C0 H1 M1 I1 A1 T1 R1 P1 L1 F1`), `state: CapabilityState = Wired |
Unavailable`, emitted once **per package** at trace start (foundation's landed shape,
`TraceKind::Capability { package, state }`; the earlier `capability_id` name is superseded)
→ **the campaign's `Unavailable(Capability(package))` report.** Charter Q1 requires the runner to
report `Unavailable` for unwired capabilities and never a pass. Without this event the campaign
cannot tell "no violation" from "nothing ran", which is the single most dangerous false green in
this milestone.

**Two things this event is not (ruling V-R16, `design.md` §2.4).** It is not the only source of an
`Unavailable` verdict: the oracle's second reason, `Unavailable(NotArmed)`, is its own end-of-fold
conclusion when every needed package is `Wired` and the checker never saw its arming situation —
that needs no trace field, and no event kind is requested for it. And its `state` is not a
literal: the dispatcher derives it from `Module::capability(&self)` over every module (K-F-10) and
emits the block from that report, so a hand-maintained table cannot mark a landed package
`Unavailable` or an unlanded one `Wired`. Every required coverage cell keys on this event's entry
for the package whose provider emits its `fault_injected`, per fault family (V-R20; design §3.1
carries the provisional family map, foundation's handoff the authoritative one) — the two hook
cells (`ForgedIdentity` via H1's `ForgeAck`, `FalseDurableWatermark` via M1's `FalseDurable`) are
the V-R19 special case of that rule, not a separate mechanism: while the emitting package reports
`Unavailable`, the cell is reported as unavailable by capability, not deleted from the required
list.

### 3.19 `topology_change`
`config_version: u64`, `nodes: Vec<(NodeId, Role)>` (landed: `Vec<(NodeId, ReplicaRole)>` in
ascending `NodeId` order — a tuple, serialised as a two-element JSON array, so DuckDB reads
`nodes[1]` and `nodes[2]`; §8 row 7)
→ **INV-PUB role grounding** (and, through it, MUT-2). Emitted by the **environment** — H1 or the
control provider — on every membership change, never by the kernel. Role grounding only works if
the role is a declared input; a role the kernel computed is not independent evidence (§3.5).

Why this event exists (F19, ruling V-R12). The header's `topology` is one snapshot at
`config_version_0`, but membership changes inside a run: spec §8.3's RF3 → `DEGRADED_RF2` → "build
and fsync replacement third regular copy" → CAS back to three copies is reachable from
`RecoveryOp::Rebuild` plus `ControlOp::Cas`, and the coverage matrix *requires* a `DEGRADED_RF2`
cell. Without this event the replacement copy has no declared role, and INV-PUB's "a claimed role
the topology does not grant is a violation" fires on a **correct** kernel — in exactly the scenarios
the gate exists to exercise. With it, the oracle resolves a role from
`topology[config_version in force at that ack]`, symmetric with `required_copy_set` being pinned by
`config_version` in `protection_state`.

Cost note: if foundation pre-declares every node id with its eventual role and a membership change
only flips which nodes are *active*, this event is a formality — emit it anyway, so the oracle never
has to assume which of the two models is in force.

## 4. Invariant → required fields (the compact version)

| Invariant | Cannot be checked without |
|---|---|
| INV-ATOM | `correlation_id`; `batch_apply.{key_versions, outcome}`; `publish.seq`; `read.observed_key_versions` |
| INV-PUB | header `topology` plus `topology_change.*` (config-versioned role grounding, §3.19); `publish.{ack_evidence, authority_recheck}`; `replication_ack.{peer_role, contiguous_seq, durability_class}` **emitted at the secondary**; `durability_advance.{node, outcome, durable_seq}`; **`protection_state.{required_copy_set, config_version}` on every config change** (the quorum rule is derived from `required_copy_set.len()`, V-R20); `admission_decision.required_copies`; `read.{barrier, observed_seq}`; `client_outcome.delivered` |
| INV-AUTH | `authority_decision.{gate, generation, valid_from_tick, expiry_tick, decision_tick, outcome}` |
| INV-LIN | `batch_apply.{seq, predecessor_digest, entry_digest}`; `lineage_root.*`; `recovery_decision.{queried_sources, selected_cutoff_seq, mode}` |
| INV-DEDUP | `dedup_record.*`; `client_submit.{affinity_id, request_digest, expected_generation}`; `client_outcome.outcome` **including `RecoveredApplied`** |
| INV-LOSS | `lineage_root.predecessor_cutoff`; `recovery_decision.queried_sources[].{reachable, boot}`; `replication_ack.{peer_role, durability_class}` **at the secondary**; `durability_advance`; `read.observed_key_versions` |
| INV-LIVE | `schedule_phase.*` (§3.17 — the arming event); `client_outcome` terminal-ness; `protection_state.phase` |
| INV-ISO | envelope `partition_id`; `schedule_phase.*` (§3.17 — armed exactly like INV-LIVE, F22); `client_outcome.partition`; `publish.seq` per partition |
| INV-VER | `version_check.{mandatory_unknown_fields, outcome}` + `correlation_id` linkage to `batch_apply` |
| INV-LAG | `protection_state.*` with `logical_tick`; `durability_advance.durable_seq` |
| coverage | `fault_injected.boundary` as a closed enum (and **only** spike §6's cases); every `outcome`/`reason`/`mode`/`phase` as a closed enum; the quorum-rule cell is derived from `required_copy_set.len()`, not read from a field (V-R20) |
| reducer | `fault_injected.boundary` (feeds `Signature.faults`, reported not gated); `op_skipped.scenario_op_index` |

## 5. Seven asks that are easy to miss

The first three were in the original request. Asks 4–6 are rulings V-R9 and V-R10, added after
critic round 1. Ask 7 is ruling V-R12, added after critic round 2. Each closes an invariant that
was **weaker than its own statement**.

1. **Roles declared by the environment.** INV-PUB grounds a peer's role in the header `topology`
   plus `topology_change` (ask 7), not in `replication_ack.peer_role`. A kernel that resolves role
   from the ACK message produces a self-consistent wrong trace that no cross-check of kernel
   declarations can catch.
2. **`predecessor_cutoff` on every recovery root.** Without it INV-LOSS degenerates into a global
   no-loss oracle, which spike §6 explicitly says is the wrong oracle.
3. **The `capability` event.** The difference between an honest `Unavailable` and a false pass while
   the kernel packages are still landing.
4. **`replication_ack` emitted at the secondary** (§3.5), with a separate delivery record at the
   primary. Delivery-point-only emission makes a dropped ACK's holder invisible and INV-LOSS
   **permits loss it should forbid**.
5. **`protection_state` on every `config_version` change** (§3.14), not only on a state transition.
   Without it INV-PUB applies the healthy RF3 one-ACK rule while `DEGRADED_RF2`, passing the exact
   bug spec §8.3 forbids.
6. **`durability_advance{node, outcome=Synced, durable_seq}` as the only thing that makes `Durable`
   true** (§3.6). Otherwise "no false durable watermark" — a named V1 clause — has no checker at all.
7. **`topology_change{config_version, nodes}` from the environment** (§3.19). The header topology
   is one snapshot; membership changes inside a run, and the `DEGRADED_RF2` coverage cell forces
   the campaign through spec §8.3's rebuild path. Without this, INV-PUB's role-mismatch clause
   fires on a **correct** kernel exactly where V3 is being gated. Emit it even if node roles are
   pre-declared and only activation changes.

Two sim-provider hooks go with them (V-R9, not trace fields but the same seam freeze): H1's network
path must be able to deliver an ACK claiming a role or identity the topology does not grant (spike
§4: "forged identity is injectable and rejected"), and M1's flush path must be able to report a
completion it never performed (spike §6: "no false durable watermark").

## 6. What we are *not* asking for

No internal tree shapes, no batch internals, no queue depths, no memory figures, no per-node
algorithm state. If a field's only reader would be a second implementation of the protocol, we do
not want it.

Two fields were withdrawn from the original ask under exactly that rule: `batch_apply
.state_digest_after` and `publish.published_state_digest`. The only way to *use* a whole-state digest
is to compute your own and compare — which is a second implementation. `key_versions` and
`entry_digest` give the oracle everything it needs. Both landed anyway at `8a23b1d` (§8 row 12);
the withdrawal stands as a rule — no checker reads them — and foundation may drop them at will.

## 7. Requested, but not read by any checker

The preamble says every field is one a checker reads. These are the exceptions, listed so the claim
stays true rather than quietly false:

| Field | Real reader |
|---|---|
| `oracle_checkpoint_digest` | foundation's I1 replay-equality row |
| `batch_apply.batch_id` (landed `batch: u64`) | DuckDB debugging (the M6 §11 Q-row pattern) |
| `durability_advance.{flush_ticket, sync_wal_through_prefixes}` (landed `captured`) | M1's own rows; DuckDB debugging |
| `client_submit.deadline_remaining_ms` | kernel-a's admission rows; DuckDB debugging |
| `replication_send` (whole kind) | pairing with `replication_ack` for DuckDB debugging, and INV-LOSS's delivery-vs-generation distinction (§3.5) |
| `op_skipped` (whole kind) | reducer diagnostics |

If foundation would rather not carry one of these, say so and we will drop the ask — none of them
blocks an invariant.

## 8. Drift against the landed C0 at `ec610f4` (critic T-23, ruling V-R20; re-read in full round 5)

Read from `git show ec610f4:crates/rdb-core/src/contracts/{trace,ids,errors}.rs`, not from the
working tree. **Resolution** is one of: *adopt* (the landed shape is fine and this document,
`design.md` and the planner's rows use it), *plan edit* (a rule changes so the field is not
needed), or *foundation ask* (a contract change still requested). Rows 1–9 are the critic's list;
10–23 were found by the same diff.

**Basis history, and why the whole table was re-read rather than two rows.** This table was
written against `8a23b1d`. The contract surface has moved twice since — `6893442` (foundation's
first code round) and `ec610f4` (K-F-39) — and the table was not re-read at either. Six rows had
moved: **1** and **21** landed and are no longer asks; **6**, **8** and **10** gained or changed
fields; **12** landed the withdrawal it had recorded as refused. Every other row was re-read
against `ec610f4` and still holds. The planner's `docs/testing/test-plan-m7-verification.md`
carries the matching basis marker `<!-- drift-basis: ec610f4 -->`, checked by
`scripts/drift-check.sh`.

**Open foundation asks from this document: none.** The shape side is closed. What is still owed
is code — **VER-CR-3**, I1's trace validator (`harness/trace.rs` or `harness/replay.rs`;
recorded by the lead in `ledger.md`, 2026-09-20 20:35 PDT, no ruling id). It has the hazard the
lead named for KA-9: it is code in `rdb-sim`, not a shape in `rdb-core`, so a crate-ordered queue
skips it, and its workaround is already in force, so **nothing goes red when it slips**.

| # | Asked (§) | Landed at `ec610f4` | Resolution |
|---|---|---|---|
| 1 | header `provenance: Provenance` (§1) | **landed at `6893442`, matching §8.1 arm for arm and derive for derive**: `Provenance { Generated { seed: u64 }, Reduced { parent: ScenarioId }, Authored { case: String } }` (`trace.rs:85`), `TraceHeader.provenance` (`trace.rs:212`), `ScenarioId(u64)` via `dense_id!` (`ids.rs:116`), and **no `TraceHeader.seed`** | **ask CLOSED.** It landed in the same foundation round that received it. This row said "the only one" for four rounds after that, because it was read at `8a23b1d` and never re-read. The planner's `M7V-46` is un-parked and both its halves run |
| 2 | `protection_state.quorum_rule: Rf3 \| DegradedRf2` (§3.14) | no field; no `QuorumRule` enum in `rdb-core` | **plan edit**: withdrawn. The oracle derives the rule from `required_copy_set.len()` (design §2.3, §6 `derived_quorum_rule`); the derived value is authoritative (V-R21). **Settled for good by F-R13:** K-F-07 was decided against V-R20, so no `quorum_rule` field will ever land, and the conditional cross-check with its `quorum_rule_mismatch` signature is withdrawn along with row M7V-90 |
| 3 | `protection_state.state` (§3.14) | `phase: ProtectionPhase { Healthy, Warn, Paused, Resuming }` | adopt `phase` |
| 4 | `replication_ack.peer_boot_id` (§3.5) | `peer_boot: BootId` | adopt |
| 5 | `peer_role: PeerRole = Regular \| Shadow` (§3.5, §3.7, `replication_send`) | `peer_role: ReplicaRole { Primary, RegularSecondary, Shadow }`; no `PeerRole`, no `Regular` | adopt; every `Regular` literal becomes `RegularSecondary`. An ack claiming `Primary` is a role the topology does not grant to a secondary and is the same mismatch violation as any other |
| 6 | `publish.ack_evidence: Vec<(NodeId, BootId, PeerRole, DurabilityClass)>` (§3.7) | **moved at `6893442`**: `Vec<AckEvidence { node, boot: BootId, role: ReplicaRole, durability }>` (`trace.rs:606`) — the boot is **back**, added under foundation's finding K-F-22, whose rustdoc cites this document's §3.7 four-tuple | adopt, and **withdraw the workaround**: the oracle no longer has to pair `node` with the boot on that node's `replication_ack` — the evidence entry states it. It must still agree with the paired ack at the same `seq`, and a disagreement is a trace defect. The point of the field is K-F-22's: two acks from one node across a restart are two boots and the checker counts **one** copy. Planner row literals are four-field; **no row asserts the boot yet** — flagged to the lead as candidate `M7V-91` |
| 7 | `topology_change.nodes: Vec<(NodeId, Role)>` (§3.19) | `Vec<(NodeId, ReplicaRole)>`, a tuple serialised as a two-element array | adopt; DuckDB reads `nodes[1]`, `nodes[2]` (planner T-27) |
| 8 | header `topology { nodes, config_version_0, partitions }` (§1) | flat `Vec<TopologyEntry { partition, node, role, config_version }>` (`trace.rs:68`; field order is `(partition, node)`, the derived `Ord` order, K-F-38), per-entry `config_version` — **and `TraceHeader.partitions: u8` now exists** (`trace.rs:214`), added at `6893442` | adopt the flat vector as before, but **stop deriving `partitions`**: the header states it, so read it. A stated count that disagrees with the distinct `partition` values is a fixture defect and should be visible, not papered over by a derivation that can never disagree. V-R8's two-partition topology reads the field |
| 9 | `admission_decision.reason: AdmissionReason` (§3.2) | `reason: Option<ErrorKind>`, `None` when admitted; no `AdmissionReason` type | adopt; the enumeration row names `ErrorKind`'s admission subset |
| 10 | header `config` (§1) | **moved at `6893442`**: `config_digest` and `budgets` are **gone** from the header; it carries `config: RunManifest { budgets, overridden: Vec<BudgetName>, nodes: u8, event_cap: u32 }` (`trace.rs:187`, `:213`) | adopt the manifest by the name `config`. `overridden` is the field this team should care about: it is the trace's own record of which budgets a run did **not** take from `Budgets::SPEC_DEFAULTS`, so a row that fails under an override cannot be mistaken for one that fails under defaults — the `RETCD_TEST_DEADLINE_SCALE` hazard AGENTS.md warns about, written into the artifact instead of remembered. **No row asserts it yet**; flagged with row 6 |
| 11 | `durability_advance.sync_wal_through_prefixes` (§3.6) | `captured: Vec<(PartitionId, Seq)>` | adopt (no checker reads it, §7) |
| 12 | `batch_apply.state_digest_after`, `publish.published_state_digest` withdrawn (§6) | **moved at `6893442`**: both fields are **deleted** (lead ruling **F-R9**). The table had recorded them as present | adopt: the withdrawal this document asked for is now the contract. Nothing changes for any checker — none read them — but the line is corrected so the next reader does not go looking for a field the table claims exists |
| 13 | `batch_apply.batch_id` (§7) | `batch: u64` | adopt |
| 14 | `client_submit.affinity_id` (§3.1) | `affinity: u64` — the generator's group index, not an `AffinityId` | adopt; the dedup key in design §2.1 uses this value |
| 15 | `client_outcome { request_id, partition, … }` (§3.8) | kind `ClientOutcomeReported { request, outcome, generation, seq, result_digest, delivered }`; partition is the envelope's | adopt |
| 16 | `read` / `status` as two kinds (§3.9) | one kind `Read { request_kind: ReadRequestKind, barrier: EventRef, generation, observed_seq, observed_key_versions, recovery_mode, outcome: ReadServiceOutcome }` | adopt; `Status`, `Export`, `ActorRead` are `request_kind` values |
| 17 | `schedule_phase` (§3.17) | `SchedulePhaseChanged { phase, fair_delivery, remaining_event_budget }` | adopt |
| 18 | `fault_injected.scenario_op_index: usize` (§3.16) | `u32` | adopt |
| 19 | `version_check.mandatory_unknown_fields: Vec<FieldId>` (§3.15) | `Vec<u16>` | adopt |
| 20 | `replication_ack_delivered { ack_event_id, accepted }` (§3.5) | `ReplicationAckDelivered { ack: EventRef, from_node, peer_role, config_version, counted }` | adopt; `counted` is the delivery-half fact INV-PUB reads |
| 21 | `op_skipped { scenario_op_index, reason }` (§3.16a) | **landed at `6893442`**: `TraceKind::OpSkipped { scenario_op_index: u32, reason: SkipReason }` (`trace.rs:1115`) with `SkipReason { ReferentGone, OutOfBudget }` (`trace.rs:634`), given its own kind rather than a `BoundaryId` member exactly as §3.16a argued, rustdoc citing K-F-08 and §3.16a. The index is `u32` where §3.16a wrote `usize` — the same adoption as row 18 | **ask CLOSED** (it was F17, standing from round 1). Design §4.5's "replays without `op_skipped{ReferentGone}`" row is **no longer vacuous**; it still waits on **I1**, which is a runner dependency and not a contract one |
| 22 | envelope `partition_id`, `node_id`, `boot_id`, `correlation_id` (§2) | `partition`, `node`, `boot`, `correlation` | adopt |
| 23 | `key_id`, `value_version` (§0) | `KeyId(u32)` and `type Version = u64` defined in `trace.rs` | adopt |

**The "observed but not relied on" note is withdrawn, and it is worth saying why it was wrong.**
It read: the uncommitted working tree at round 3 carried a `Provenance` enum matching §8.1, a
`quorum_rule` field and an `OpSkipped` kind, and "the other two remain unlanded asks, not facts."
Refusing to rely on an uncommitted tree was correct. **Failing to re-read after it committed was
not.** All three were committed at `6893442`: `Provenance` and `OpSkipped` as asked, and
`quorum_rule` never — ruling F-R13 decided K-F-07 against V-R20, so the oracle derives the rule
and has no cross-check to perform (row 2). Two asks stayed "open" for four rounds on the strength
of a note that was only ever true of one working tree on one afternoon. Rows 1 and 21 are now
read from the committed contract at `ec610f4`, cited by file and line.

### 8.1 Contract request to foundation: header `provenance` (F18, T-23) — **SATISFIED at `6893442`**

**Kept as written, marked satisfied rather than deleted**, so a reader arriving from an older
reference sees the ask and its answer together. The draft below and the landed
`crates/rdb-core/src/contracts/trace.rs:85` agree arm for arm, field for field and derive for
derive; `TraceHeader.provenance` is at `trace.rs:212` and there is no `TraceHeader.seed` left.
Nothing here is still requested.

Replace `TraceHeader.seed: u64` with `pub provenance: Provenance`, where:

```rust
/// Where a scenario came from (F18). Never a bare seed: a reduced or authored scenario is not
/// in the generator's image, so replaying its seed reproduces nothing; the checked-in event
/// stream is the reproducer, and this says which of the three ways it was made.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Provenance {
    /// In the generator's image: `scenario(seed, budget)` at `generator_version` reproduces it.
    Generated { seed: u64 },
    /// Shrunk by the reducer from `parent`. Not in the generator's image.
    Reduced { parent: ScenarioId },
    /// Written by hand as a Rust constructor. Not in the generator's image.
    Authored { case: String },
}
```

`ScenarioId(u64)` joins `ids.rs` through `dense_id!`; the reducer assigns it and no checker reads
it. `case` is a test name, never key or value bytes. `design.md` §3's `Scenario.provenance` is
this same type re-used, not a second definition.
