# Team foundation — critic, round 2 (design + code, one pass)

**Date:** 2026-09-20 · **Role:** critic (independent) · **Against:** `architect-handoff.md` §10
(document side) and commit `6893442` (code side), ADR sources at `4997c87` · **Base:** `d8b3877`,
branch `feature/rdb-m7` · **Scope:** packages C0, H1, M1, I1

**Verdict: PASS_WITH_RISKS.** Every BLOCKER and MATERIAL from round 1 is closed in the committed
code, and I reproduced the load-bearing ones myself rather than reading the handoff. The surviving
risk is entirely on the **document** side: `design.md` — the file team-rules names as the
architect's output and the kernel teams' input — now instructs three shapes the code deliberately
does not have, and omits a 224-line public module two teams consume.

**Rulings I did not re-litigate** (given as binding): F-R13 settles K-F-07 toward V-R20, so there
is no `quorum_rule` on `ProtectionState` and the value derives from `required_copy_set.len()`;
K-F-38's `ClientOutcomeReported` bullet is withdrawn; `ResourceExhaustedFatal` is not a gap; the
four name divergences (`OpSkipped.scenario_op_index: u32`, a named `Completion`, `submit(&Effect)`,
`Scheduler::pop`) are accepted.

---

## 0. What I verified myself, and how

Read-only. `CARGO_TARGET_DIR=.rtargets/critic-foundation`, `CARGO_INCREMENTAL=0`, one cargo
invocation at a time. No file outside this one was written; no git write was run.

| # | Check | Command / method | Observed |
|---|---|---|---|
| 1 | the record-digest preimage is F-R6's eleven parts | read `crates/rdb-core/src/contracts/envelope.rs` `compute_record_digest` | parts in order `prev_digest, partition(u32 LE), generation, owner_epoch, seq, config_version, identity(16), request_digest, conditions, mutations, result` — 11, `protocol_version` and `lease_id` absent |
| 2 | the goldens are the digest of *that* preimage, not of the code | re-implemented the preimage from `design.md` §4.8 in perl (`Digest::SHA`) and hashed it independently of the crate | `entry1 = 0d31b22c883fd531d0b037044aebbf9f577d2b55a6633eee612fbfe75ee429f0`, `entry2 = cf81114353a593748af1e9d5eba6a445636912c2fe4f4de2828e4d9a761c3358` — **byte-identical to the two re-pinned goldens** |
| 3 | the re-pin was scoped | `git show 6893442 -- crates/rdb-core/tests/contracts.rs` | only the two `record_digest` goldens changed. The request golden `be896f14c9b2711f…` and the 188-byte envelope golden are **not in the diff**, so the F-R6 change touched only what it meant to |
| 4 | tests | `cargo test -p rdb-core -p rdb-sim` | `contracts` 21, `seams` 4, `control` 7, `dispatch` 6, `harness` 5, `storage` 6 — **49 passed, 0 failed** |
| 5 | `deps` gate, positive | `CARGO_TARGET_DIR=.rtargets/critic-foundation scripts/gate.sh deps` | `== deps` / `gate: deps OK`, **exit 0** |
| 6 | `deps` gate, negative × 3 | throwaway workspace in the session scratchpad (`config-bad` → `rdb-oops`), one kind at a time | dev: `deps: config-bad depends on rdb-oops (dev)` exit 1; **normal and renamed** (`totallyfine = { package = "rdb-oops" }`): `… (normal)` exit 1; **build**: `… (build)` exit 1. All three print the ADR line and exit 1 |
| 7 | the three ADR verification greps (K-F-31) | ran all three verbatim | each printed nothing, **exit 1** |
| 8 | no panic path | `grep -rn "\.unwrap()\|\.expect(\|panic!\|todo!\|unimplemented!\|unreachable!" crates/rdb-core/src crates/rdb-sim/src` | **zero hits in code**; the five matches are doc comments that forbid the construct |
| 9 | JSONL per row, three `Capability` lines | listed `.rtargets/critic-foundation/test-logs/**/*.jsonl` and read one | 88 files, one per row under `<suite>/<test>.jsonl`; a harness row's file holds exactly **3** `"@m":"capability"` lines; fields are `package`/`state`, no key or value bytes |
| 10 | trace vocabulary counts | `awk` over `contracts/trace.rs` | `TraceKind` **24** kinds, `BoundaryId` **29** members in §10.5's exact order, `BudgetName` **10** against `Budgets`' 10 fields, `ErrorKind` **18** and `RdbError` **18** |

---

## 1. K-F-01..38 disposition

Severity column is round 1's (B = BLOCKER, M = MATERIAL, A = ADVISORY), as restated in
`architect-handoff.md` §10.1. "Closed" means I found the artifact and, where a row exists, saw it
green — not that the handoff says so.

| # | Sev | Disposition | Evidence location |
|---|---|---|---|
| K-F-01 | B | **closed** | `contracts/envelope.rs` `compute_record_digest` — eleven parts, one preimage. `design.md` §4.8 is the single statement; the rustdoc cites it and does not restate. Check 1 |
| K-F-02 | B | **closed** | `protocol_version` and `lease_id` excluded; `m7f_02_record_digest_is_invariant_under_protocol_version`, `…_under_lease_id`, `…_binds_partition` green; goldens re-pinned and **independently reproduced** (check 2); request + envelope goldens unmoved (check 3) |
| K-F-03 | B | **closed** | `storage/crash_image.rs` `CrashImage::of` — `ProcessCrash` keeps `lineage.applied`, `HostCrash` writes `AppliedSeq(lineage.durable.0)` with the relabel spelled once and commented. `m7f_06_process_crash_keeps_applied_and_host_crash_truncates_to_durable` green |
| K-F-04 | B | **closed** | `contracts/storage.rs` `SnapshotRead::version(ns, key) -> Option<Version>`; `m7f_08_snapshot_version_answers_the_writing_sequence` green |
| K-F-05 | B | **closed** | `EffectKind::AdoptAuthority { partition, generation, owner_epoch, config_version }`; `harness/dispatch.rs` `deliver` records it and `ctx_for` copies it into `StepCtx`; `adopted()` falls back to `Adopted::default()` (the zero triple). `m7f_09_the_dispatcher_fills_the_authority_triple_from_the_last_adoption` green. No authority rule anywhere in `rdb-sim` |
| K-F-06 | B | **closed** | `TraceKind::ControlInteraction` and `TraceKind::FamilyReload { …, after_termination }` present; `ControlOutcomeKind::Terminated { termination, gap }`; `m7f_10_every_completion_records_a_control_interaction` green |
| K-F-07 | M | **revised → withdrawn** under F-R13/V-R20. `grep -rn quorum_rule crates/` → no hit; `ProtectionState` carries `required_copy_set` + `config_version` instead. **But the documents still demand it — see K-F-40** |
| K-F-08 | M | **closed (shape)** | `TraceKind::OpSkipped { scenario_op_index: u32, reason: SkipReason }`, `SkipReason { ReferentGone, OutOfBudget }`; absent from `BoundaryId` as required. No row here by design — verification's G1/Q1 produce it |
| K-F-09 | M | **closed** | `TraceHeader { schema_version, generator_version, provenance, config: RunManifest, partitions, topology, oracle_checkpoint_digest }`, `#[serde(deny_unknown_fields)]` on header and manifest; no bare `seed`. `m7f_11_trace_header_round_trips_through_jsonl_for_every_provenance` and `m7f_11_an_unknown_header_field_and_a_foreign_schema_are_refused` green |
| K-F-10 | B | **closed** | `Module::capability(&self)` with default `CapabilityState::Unavailable`; `Dispatcher::capability_report(&self)` takes `&self` and calls `module(name).capability()` — no step, no effect discarded. `m7f_01_every_kernel_package_reports_unavailable_without_being_stepped` green |
| K-F-11 | B | **closed** | `ReplyEffect::Read { identity, outcome: ReadServiceOutcome, value: Option<(Version, Digest)> }` — versions and digests, never bytes |
| K-F-12 | M | **closed** | `ControlStore::snapshot_family(prefix) -> Result<(Revision, Vec<ControlRecord>), SimError>`; `FamilySnapshot` carries `records`. `m7f_12_family_snapshots_at_one_revision_are_identical_across_a_cas` green |
| K-F-13 | M | **closed** | `ControlChange { key, revision }` — no `value` field. A value reaches the kernel only via `ControlEvent::Value` or `FamilySnapshot`'s separate `ControlRecord` |
| K-F-14 | M | **closed** | `submit(node, &Effect)` / `complete(now) -> Vec<Completion>`; `ControlOp::PlanCas { node, outcome }`, `DelayCompletion`, `DropCompletion` act on the held queue. `m7f_13_two_nodes_race_a_create_and_the_late_completion_tells_the_truth` and `m7f_20_a_dropped_completion_leaves_no_trace_of_completing` green |
| K-F-15 | M | **closed** | `ControlTime.sampled_at`, `is_stale`, `compare(now, max_sample_age_millis, instant, margin)`; `m7f_14_a_stale_sample_is_uncertain_even_when_the_bound_is_confident` green |
| K-F-16 | A | **open, as designed**. `estimate` is still `Tick`. Developer's call; no consumer impact |
| K-F-17 | M | **closed** (was already closed at the seed). `m7f_02_request_digest_*` green; the request golden did not move this round (check 3) |
| K-F-18 | B | **closed** | `contracts/ids.rs` `AuthorityGeneration(pub u64)`, no conversions to `Generation` or `OwnerEpoch` |
| K-F-19 | M | **closed** | `ControlPrefix` with exactly the 7 §7.1 families, `encode()`, `const fn contains()`, `ControlKey::prefix()`; `Watch { prefix, from }`, `Reload { prefix }` |
| K-F-20 | M | **closed** | `ReadOutcome::Absent { as_of }`; `m7f_15_a_create_keyed_on_a_stale_absent_conflicts` green, and the row is a real race (b commits between a's read and a's CAS), not a shape assertion |
| K-F-21 | M | **closed** | `Member.boot: BootId` (not `Option`); `copy_of` returns `None` on unauthenticated, unknown node **or** boot mismatch, as one answer. `m7f_16_a_member_node_at_another_boot_is_not_a_copy` green |
| K-F-22 | M | **closed** | `AckEvidence { node, boot: BootId, role, durability }` |
| K-F-23 | M | **closed** | `required_regular()` excludes primary and shadow; `primary()` added. `m7f_17` asserts **2** on RF3, **0** on a lone survivor, and `None` primary on a fenced two-secondary config |
| K-F-24 | M | **closed** | `grep -rn "state_digest_after\|published_state_digest" crates/` → no hit anywhere |
| K-F-25 | M | **closed** | `StorageOp::ShortFlush { node, through }`; `MemoryEngine::sync_wal_through` answers `min(captured, applied, short)` and the crossing to `DurableSeq` is spelled once with the B-R13 comment. `m7f_18_short_flush_reports_and_keeps_the_shorter_prefix` green. `FalseDurable` returns `Ok(vec![])` — no `DurablePrefix`, so no watermark can move; `m7f_07` green |
| K-F-26 | M | **closed** | `RdbError::Unavailable` → `RetryRule::NotWired`; `proves_no_mutation()` matches only `Definitive \| BoundedJitter`. `m7f_01_unwired_is_definitive_and_proves_no_mutation_claim` asserts the negative |
| K-F-27 | M | **closed** | `RunManifest { budgets, overridden, nodes, event_cap }` in `rdb-core`; `BudgetName` 10 members against `Budgets`' 10 fields; `harness::manifest::resolve`. `m7f_19_the_manifest_lists_exactly_the_overridden_budgets` green |
| K-F-28 | M | **closed** | `Dispatcher::module` is an infallible `match` on `ModuleName`; no registry. Check 8: **zero** panic paths in either crate's `src` |
| K-F-29 | M | **closed** | No provider struct derives `Copy`: `Scheduler`, `Clock`, `Network`, `ControlStore`, `Cluster` are all `#[derive(Debug)]` / `#[derive(Debug, Default)]` and hold their fields. (`const fn` survives on real accessors — harmless, but `design.md` §5's flat "neither `Copy` nor `const fn`" is now false: K-F-43) |
| K-F-30 | M | **closed** | 24 `#[retcd_test]` across `control.rs`/`dispatch.rs`/`harness.rs`/`storage.rs`, **0** bare `#[test]`. Check 9 confirms one JSONL per row and three `Capability` lines each |
| K-F-31 | M | **closed** | I ran all three greps verbatim (check 7): each prints nothing and exits 1 |
| K-F-32 | M | **closed** | `deps` stage in both scripts, reading `cargo metadata --format-version 1 --no-deps`; included in `all`. Checks 5–6 |
| K-F-33 | M | **closed (document)** | `architect-handoff.md` §5.1 carries the three-row deferral table |
| K-F-34 | A | **closed, beyond what was asked**, under B-R30: `AppendReject` now has 16 ladder variants matching kernel-b §3.2 rows 0–8 and §3.2a 5R/6R/6R′, and `PartitionConfig.min_regular_acks` defaults to 1 with zero refused. See K-F-39 (the zero-refusal is not structural) and K-F-43 (`design.md` §4.8 still shows the seed's five variants) |
| K-F-35 | A | **closed** | The only `WatchGap` in `src` is the `BoundaryId` member; no `ControlEvent::WatchGap` reference remains |
| K-F-36 | A | **open, routed to kernel-a**, as designed. `WatchTermination::NotLeader` is a unit variant |
| K-F-37 | A | **closed as the architect ruled**: `Digest::of` does a plain `part.len() as u64` widening with a comment saying why a clamp would be the collision it exists to prevent. The **developer's handoff row is wrong** about this — see K-F-45 |
| K-F-38 | A | **closed**, bullet by bullet: `expect()` gone (check 8); ADR-rdb-0002 decision 2 names both crates' dev-deps and `hex`, matching both `Cargo.toml`s; `TopologyEntry` field order is `(partition, node, role, config_version)` with derived `Ord`; `Resuming`/"Reprotecting" mapping stated; `ErrorKind` reworded; `crash_image.rs` doc matches `reopen()`. `ClientOutcomeReported` bullet withdrawn (binding). `CasOutcome::Conflict { exists, current }` left as a struct variant, open to kernel-a |

**Score:** 9 BLOCKERs closed, 0 sustained. 22 MATERIALs closed, 1 (K-F-07) withdrawn on a later
ruling. ADVISORIES: 4 closed, 2 open by design, 1 (K-F-37) closed with a wrong handoff row.

---

## 2. New findings, K-F-39+

### K-F-39 — `min_regular_acks` cannot be zero "by any path", but the type admits one — MATERIAL

**Violated criterion.** Round 1 closed K-F-13 on the principle the architect wrote down: a safety
property stated in a doc comment is a convention; deleting the field that allows the violation is
what makes it structural. `PartitionConfig` restates the doc-comment form of the same mistake.

**Artifact.** `crates/rdb-core/src/contracts/membership.rs:53–113`.

**Direct evidence.** `PartitionConfig` is `#[derive(Debug, Clone, PartialEq, Eq, Serialize,
Deserialize)]` with `pub min_regular_acks: u8` and no `#[serde(try_from = …)]`,
`#[serde(deserialize_with = …)]` or any other hook. `validate()` is a separate method nothing
calls on the decode path. The rustdoc on `validate()` nevertheless says, verbatim:

> A configuration deserialised from a control record goes through this too, so a zero cannot
> arrive by any path.

There is no such path in the workspace today (`grep` finds no `PartitionConfig` deserialisation
site), so the sentence describes a caller that has not been written — and kernel-a and kernel-b
will write it while reading that sentence as a guarantee already in force. B-R30's own rationale is
that "no acknowledgement required" is spec §5.2 with the safety taken out; a control record with
`"min_regular_acks": 0` deserialises into exactly that today.

**False-positive check.** Three ways this could be a non-finding, all checked: (a) a custom
`Deserialize` impl elsewhere — none, the derive is the only one; (b) a decode wrapper in
`rdb-sim` that validates — `sim/control.rs` stores control records as opaque `Bytes` and never
decodes a `PartitionConfig`; (c) the invariant being enforced at use rather than at construction —
`required_regular()` and `copy_of()` do not consult `min_regular_acks` at all. The test
`b_r30_min_regular_acks_defaults_to_one_and_refuses_zero` covers `new()`, `with_min_regular_acks(0)`
and a hand-mutated literal passed to `validate()` — every path **except** the one the doc names.

**Closure condition.** Either `#[serde(try_from = "…")]` on `PartitionConfig` routing every decode
through `validate()` — about four lines, and then the sentence is true — or reword the rustdoc to
name the discipline ("the decoder must call `validate()`") plus a row asserting the one decode site
does. A row that deserialises `{"min_regular_acks": 0, …}` and expects `InvalidArgument` is the
observable.

### K-F-40 — `design.md` still orders kernel-b to emit the field F-R13 deleted — MATERIAL

**Violated criterion.** team-rules' role table makes `teams/<team>/design.md` the architect's
output and the downstream teams' input. It currently contradicts a binding ruling and the code.

**Artifact.** `design.md:687–690`; `architect-handoff.md:117` and `:298`.

**Direct evidence.** `design.md` §4.10: "**(R1, K-F-07)** `ProtectionState` gains `quorum_rule:
QuorumRule` … Emitted by kernel-b's L1; the field is closed here." `architect-handoff.md` §10.1 row
K-F-07 repeats the instruction, and §5.1's deferral table lists it as landing this round. The code
has no such field (`grep -rn quorum_rule crates/` → nothing; `ProtectionState` carries
`required_copy_set: Vec<NodeId>` and `config_version`). The developer's §R1.5 flags the conflict
and resolved it toward V-R20; F-R13 confirms that. Nothing propagated the ruling back into the
design.

**Consequence.** Kernel-b's L1 developer is the named emitter. Following `design.md` produces code
that does not compile, which is the cheap outcome; the expensive one is the field being re-added as
a second source of truth for a value the oracle derives, which is the exact reconciliation V-R20
exists to prevent.

**False-positive check.** Is the handoff superseding the design? No — §10.1's K-F-07 row carries
the same instruction, and §10.9 lists K-F-07 among the MATERIALs "closed", not withdrawn. Nothing
in either file records F-R13.

**Closure condition.** `design.md` §4.10's `quorum_rule` bullet replaced by the F-R13 ruling and the
derivation from `required_copy_set.len()`; `architect-handoff.md` §5.1 and §10.1 rows for K-F-07
marked withdrawn. Observable: `grep -rn quorum_rule teams/foundation/` returns only withdrawal text.

### K-F-41 — a public contracts module two teams consume has no design section — MATERIAL

**Violated criterion.** Charter DELIVERABLE and design §7 ("who consumes what") make the design the
statement of every seam a kernel team writes against.

**Artifact.** `crates/rdb-core/src/contracts/authority.rs` (224 lines, new in `6893442`);
`crates/rdb-core/src/contracts/event.rs:167–186`; `design.md:126–133` and §7.

**Direct evidence.** `grep -n "ExternalFenceVerified\|contracts::authority\|AuthorityView\|
PartitionMode" design.md` returns nothing. The module is public and exported from `lib.rs:48–49`
(`AuthorityDecision, AuthorityView, BlockReason, Checkpoint, DenyReason, EvidenceRef, Lineage,
PartitionMode, Verdict`). `EventKind` now has **seven** variants — the seventh,
`ExternalFenceVerified { partition, prior_generation, prior_owner_epoch, prior_boot_id,
control_revision, evidence }`, is spec §7.2's fallback and A1's takeover guard — while `design.md`
§4.1 lists six and annotates `Node(NodeLifecycle)` with "six sources, not five". `PartitionMode` is
the enum every kernel is supposed to match on totally (K-B-19).

**Consequence.** The A-R23 shapes are correct and usable — I checked each by name — but a kernel-a
or kernel-b developer working from `design.md` will not know `contracts::authority` exists, and
§7's kernel-a row does not list it. A total `match` on `EventKind` written from §4.1 will not
compile.

**False-positive check.** Could the shapes belong to kernel-a's design rather than foundation's?
The types live in `rdb-core`, which is foundation's exclusive artifact, and A-R23 routed them here;
the architect's §10.7 name-reconciliation table does not mention them either, so nobody recorded
them on the foundation side.

**Closure condition.** A `design.md` §4.11 listing the `contracts::authority` types, `EventKind`
in §4.1 restated at seven variants, and §7's kernel-a row naming `contracts::authority`.
Observable: `grep -n "ExternalFenceVerified" design.md` returns a hit in §4.1.

### K-F-42 — ADR-rdb-0003 asserts replay evidence that does not exist — MATERIAL

**Violated criterion.** ADR-0000/ADR-0031's Verification sections are evidence claims; team-rules
says "'tests pass' is not evidence — give the exact command and the observed output".

**Artifact.** `docs/ADRs/rdb/0003-deterministic-simulation-kernel.md:178–179`.

**Direct evidence.** The bullet reads: "`harness::replay::replay` returns `ReplayOutcome::Identical`
for a recorded trace, and `Unreplayable` for one whose schema or generator version differs."
`crates/rdb-sim/src/harness/replay.rs:45` is `Err(SimError::unavailable("harness::replay::replay"))`
unconditionally. Every other unbuilt row in the same section carries an explicit marker — "Row
M7F-05 (owed by package H1)", "(owed by package I1)" ×3 — so a reader takes the unmarked bullet as
a verified fact. This is the one place in the three ADRs where that happens; K-F-31's corrections
otherwise hold up (check 7).

**False-positive check.** Is `replay` wired behind a feature or a second entry point? No — it is
the only function in the module and it is `const fn` returning `Err`. Does the capability report
cover it? `m7f_22_environment_capabilities_name_what_is_owed` names the owed seams, which is the
honest statement; the ADR bullet is the dishonest one.

**Closure condition.** The bullet marked "(owed by package I1)" like its neighbours, or deleted.
Observable: no unmarked Verification bullet in ADR-rdb-0003 names a function that returns
`SimError::Unavailable`.

### K-F-43 — three more stale statements in `design.md` §4.6, §4.8 and §5 — ADVISORY

**Artifact / evidence.**

| Statement | Where | What landed |
|---|---|---|
| `AppendReject { NeedPrefix, DigestMismatch, StaleEpoch, IncompatibleConfig, Unauthenticated }` | `design.md:526` | 16 variants under B-R30 (`Quarantined … Unauthenticated`), and there is no `DigestMismatch` or `IncompatibleConfig` at all — they are `CorruptHistory { at }` and `StaleConfig`/`NeedConfig` |
| `PartitionConfig { partition, config_version, members }` | `design.md` §4.6 | plus `min_regular_acks: u8` (B-R30) |
| "neither `Copy` nor `const fn` appears on any of them" | `design.md` §5 | `Copy` is genuinely gone from every provider (K-F-29 closed); `const fn` survives on real accessors (`Scheduler::now`, `Clock::now`, `ControlStore::revision`, `Cluster::config`), which is harmless |
| `PowerLoss`/`HostCrash` truncates `applied` | `design.md` §5 | `StorageFault` has no `PowerLoss` variant; it is `WriteFailed, FlushFailed, ProcessCrash, HostCrash, Corrupt` |

**False-positive check.** None of these is a code defect — in each case the code follows a later
ruling or the design's own intent. They are wrong statements in the file the kernel teams read.

**Closure condition.** §4.6, §4.8 and §5 reconciled against `6893442`. Observable:
`grep -n "DigestMismatch\|PowerLoss" design.md` returns nothing.

### K-F-44 — M7F-05's assertable half is not asserted, and `Scheduler` has no direct row — ADVISORY

**Artifact.** `crates/rdb-sim/src/sim/scheduler.rs`; `design.md` §8 row M7F-05.

**Direct evidence.** `Scheduler` is fully implemented — `schedule` refuses an event before `now`
(`SimError::Config { field: "at" }`) and refuses a duplicate `(tick, id)` (`field: "event_id"`),
`pop` takes `pop_first` off a `BTreeMap<(Tick, EventId), Event>` and advances `now`. None of that is
`Unavailable`, and `grep -rn "m7f_05" crates/` finds nothing. The only coverage is incidental:
`tests/dispatch.rs` constructs a `Scheduler` three times as a dispatcher argument. So the landed
half of M7F-05 — "`(tick, event_id)` order is total" — is untested, while it is bundled with the
half that genuinely needs H1 ("two runs of one recorded stream give byte-identical traces").

**False-positive check.** Could the ordering be covered transitively by `m7f_21`? `m7f_21` asserts
tick arithmetic on the dispatcher hop, not queue order among equal ticks, and does not exercise
either `schedule` rejection. The duplicate-id refusal is the one that silently loses an event if it
regresses.

**Closure condition.** Split the row: M7F-05a (ordering totality + both `schedule` refusals,
writable today) and M7F-05b (byte-identical replay, owed to the replay seam). Observable:
`m7f_05a_*` green in `crates/rdb-sim/tests/`.

### K-F-45 — the developer's K-F-37 row describes behaviour the code does not have — ADVISORY

**Artifact.** `developer-handoff.md` §R1.3, row K-F-37: "`Digest::of` refuses an over-long part".

**Direct evidence.** `crates/rdb-core/src/contracts/digest.rs:65` is
`hasher.update((part.len() as u64).to_le_bytes())` — a plain widening with a comment explaining
that a clamp would give two lengths one prefix. `Digest::of` returns `Self`, not a `Result`; it
cannot refuse anything. This matches the architect's §10.1 K-F-37 closure and design §4.7 exactly,
so **the code is right and the handoff row is wrong.** Flagged because the handoff is the record a
later reader audits the closure against.

**False-positive check.** `compute_record_digest` does return `Result` and does refuse over-long
counts and key/value lengths via `fits_u32` — that is a different function and a different check.

**Closure condition.** The row reworded to the architect's form ("length prefix stays `u64` LE; the
dead clamp removed").

### K-F-46 — the charter's `scripts/gate.sh all` acceptance line has no evidence — ADVISORY

**Artifact.** `charter.md` ACCEPTANCE: "`scripts/gate.sh all` green for the workspace at handoff
(cold build)". `developer-handoff.md` §R1.4 lines 1–9.

**Direct evidence.** No line in §R1.4 is `scripts/gate.sh all`. Line 2 is
`cargo clippy -p rdb-core -p rdb-sim --all-targets -- -D warnings` — **not** `--workspace`, which is
what the `lint` stage runs. Line 6 is `scripts/gate.sh test -p rdb-core -p rdb-sim`. So the
workspace-wide clippy stage the charter names was never observed. I ran it myself; the result is in
§4 below.

**False-positive check.** The risk is low by construction — `config-*` cannot depend on `rdb-*`
(now gate-enforced), so a workspace clippy can only newly fail inside the two rdb crates, which
line 2 did cover. This is an evidence gap against a stated acceptance criterion, not a suspected
break. I tried to close it myself and could not: the workspace clippy fails today on
`dev-verification-1`'s uncommitted, half-written `tests/support/oracle/` subtree (§4a), not on
anything in `6893442`.

**Closure condition.** One `scripts/gate.sh all` run recorded with its observed output at the
foundation gate — **taken after `dev-verification-1`'s subtree compiles**, or the run will blame
foundation for verification's in-flight files.

### K-F-47 — the two statements of Q-F-1 disagree about their scope — ADVISORY

**Artifact.** `design.md` §8 last paragraph vs `architect-handoff.md` §10.6 last row.

**Direct evidence.** Design §8: `read_json_auto('<target>/test-logs/*/harness/*.jsonl')` "returns
one line per M7F row in `crates/rdb-sim/tests/harness.rs`". Handoff §10.6: "every `rdb-sim` test
file has exactly three `Capability` events at trace start". The glob covers only the `harness`
suite (5 of the 24 rdb-sim rows); the second claim needs `*/*/*.jsonl`. Check 9 confirms the
directory layout is `test-logs/<run>/<suite>/<test>.jsonl`, so the wider glob works.

**Closure condition.** One statement of Q-F-1, with the glob that matches it. The test planner owns
which.

---

## 3. What I looked for and did **not** find

Recorded so this is a review and not a list.

- **A stub that fakes success.** All four owed capabilities refuse by name and none mutates first:
  `sim::network::Network::send`, `sim::cluster::Cluster::suspend` (returns before touching
  `self`), `harness::replay::replay`, and the dispatcher's three unwired effect kinds
  (`harness::dispatch::deliver::{send,store,timer}`) — which `return Err` rather than skipping the
  effect, so an unwired provider cannot be mistaken for a delivered one. `m7f_21_an_unwired_
  provider_is_refused_by_name_after_earlier_effects_land` pins that ordering. M7F-05 is an owed
  **row**, not a stub; see K-F-44. No `Ok(())` stub anywhere in either crate.
- **A golden pinned to the implementation.** Check 2 re-derived both from the design text in a
  different language. They match.
- **An unscoped re-pin.** Check 3: the request and envelope goldens are absent from the diff.
- **`HashMap` on a trace path, `todo!()`, a clock in `rdb-core`.** Checks 7–8, all clean.
- **A `deps` stage that only catches normal dependencies.** Check 6 covers dev, build and a
  renamed normal dependency; `cargo metadata`'s `dependencies[].name` is the real package name, so
  the rename alias does not evade it. Residual, and the developer says so: `--no-deps` reads
  manifests, not the built graph.
- **Kernel stubs touched under cover of the correction.** `git show --stat 6893442 --
  crates/rdb-core/src/{authority,transaction,replication,publication,protection,recovery}.rs` is
  empty.
- **A test that tests the mock.** `m7f_15` and `m7f_13` are the two that could have been; both
  drive a real race through the store's own state machine, which *is* the H1 artifact under test.

---

## 4. Commands and observed results

| # | Command | Observed |
|---|---|---|
| 1 | `CARGO_TARGET_DIR=.rtargets/critic-foundation CARGO_INCREMENTAL=0 cargo test -p rdb-core -p rdb-sim` | contracts 21, seams 4, control 7, dispatch 6, harness 5, storage 6 — **49 passed, 0 failed**, exit 0 |
| 2 | `CARGO_TARGET_DIR=.rtargets/critic-foundation scripts/gate.sh deps` | `gate: deps OK`, **exit 0** |
| 3 | `bash scripts/gate.sh deps` in a scratchpad fixture, dev-dependency edge | `deps: config-bad depends on rdb-oops (dev)` + ADR line, **exit 1** |
| 4 | same fixture, normal dependency **renamed** `totallyfine = { package = "rdb-oops" }` | `deps: config-bad depends on rdb-oops (normal)`, **exit 1** |
| 5 | same fixture, build-dependency edge | `deps: config-bad depends on rdb-oops (build)`, **exit 1** |
| 6 | `grep -rn "HashMap" crates/rdb-core/src crates/rdb-sim/src \| grep -v -E ':[[:space:]]*//'` | no output, **exit 1** |
| 7 | `grep -rn "todo!" …` / `grep -rn -E "SystemTime\|Instant::now\|rand::" crates/rdb-core/src` (same filter) | no output, **exit 1** each |
| 8 | perl `Digest::SHA` re-derivation of the F-R6 preimage | `0d31b22c88…` / `cf81114353…` — equal to the re-pinned goldens |
| 9 | `CARGO_TARGET_DIR=.rtargets/critic-foundation cargo clippy --workspace --all-targets -- -D warnings` | **exit 101** — `error[E0583]: file not found for module 'builder'` at `crates/rdb-sim/tests/support/oracle.rs:24`, plus two more and four `clippy::duplicate_mod`. **Not foundation's** — see §4a |
| 10 | `git show 6893442:crates/rdb-sim/tests/support/oracle.rs \| grep "^pub mod"` | **no output**: at the reviewed commit, `oracle.rs` declared no submodules |
| 11 | `git status --porcelain crates/` | ` M crates/rdb-sim/tests/support/oracle.rs` and `?? crates/rdb-sim/tests/support/oracle/` |

The fixture for rows 3–5 is a throwaway workspace in the session scratchpad. Nothing was added to
the repository working tree by me; `git status --porcelain scripts` is empty and the only `crates/`
entries are row 11's, which are not mine.

### 4a. The workspace gate is red right now, and foundation did not make it red

Row 9 looks alarming and is a false positive against this review. The chain: `tests/*.rs` declare
`mod support;` → `support/mod.rs:22` declares `pub mod oracle;` → the **working-copy**
`support/oracle.rs` (modified, +418 lines, uncommitted) declares `pub mod builder; pub mod checks;
pub mod model;`, and `support/oracle/checks.rs` declares `pub mod lag; … pub mod version;`. Three of
those files do not exist yet.

`ls --time-style=full-iso crates/rdb-sim/tests/support/oracle/` shows the directory being written
between **22:14:49 and 22:17:48** today — `dev-verification-1`'s O1 work, in flight while I was
reviewing. Row 10 proves foundation's own tree was self-consistent: at `6893442`, `oracle.rs` had no
`pub mod` at all, which is why my row 1 (`cargo test -p rdb-core -p rdb-sim`, 22:09) compiled and
passed all 49.

**Three consequences for the lead, none of them a foundation finding.**

1. Any `scripts/gate.sh all` or `cargo test -p rdb-sim` run in this working tree **right now** fails,
   and the failure text names `crates/rdb-sim/tests/` — foundation's directory. Whoever runs the
   next gate will read it as foundation's regression. It is not.
2. This is the shared-file risk team-rules' exclusive-ownership rule exists to prevent, arriving by
   a path the rule did not name: foundation owns `tests/support/mod.rs` "(registry only)" and
   registered `pub mod oracle;` there on day one, so an incomplete `oracle` subtree breaks every
   `rdb-sim` test binary, including foundation's four. The registry seam couples the two teams'
   compile status.
3. K-F-46 stands and cannot be cleared until `dev-verification-1` lands a compiling subtree. The
   charter's `scripts/gate.sh all` evidence has to be taken after that, not before.

I did **not** touch the files, did not stash and did not revert. Verified at `6893442`, which is
what this review is against.

---

## 5. Questions for the lead, each with my default

1. **Who reconciles `design.md` (K-F-40, K-F-41, K-F-43)?** The code is right in all three; the
   design is wrong. *Default:* one scoped architect round 2 over §4.1, §4.6, §4.8, §4.10, §5 and
   §7 only — reconciliation, no new design — before kernel-b's L1 and kernel-a's A1 developers
   start. If you would rather not spend the round, the alternative is declaring
   `architect-handoff.md` §10.7 plus the code authoritative for those six sections and saying so at
   the top of `design.md`; I do not recommend it, because §10.7 does not cover
   `contracts::authority` at all.
2. **K-F-39: structural or documented?** *Default:* structural — `#[serde(try_from)]` through
   `validate()`. It is four lines, and "structural, not a convention" is the standard this team set
   itself when it closed K-F-13.
3. **`AppendReject` has no `Busy { accepted_through }` and no `AlreadyHave`,** which kernel-b's
   §3.2 step-8 table names as outcomes. *Default:* no change now — those are non-reject outcomes in
   kernel-b's own table, and B-R30 covered the reject ladder. Kernel-b names the variant when R1
   lands, and it is an additive enum change.
4. **Is `replay` owed to I1 or H1?** ADR-rdb-0003 and `design.md` §5 imply I1; `developer-handoff`
   §R1.6 says H1. *Default:* I1, matching ADR-rdb-0003's three other owed rows. It matters only for
   K-F-42's wording.
5. **`tests/support/mod.rs`'s `pub mod oracle;` couples foundation's compile status to
   verification's in-flight work** (§4a). Today every `rdb-sim` test binary fails to build because
   three files under `tests/support/oracle/` are declared and not yet written. *Default:* leave the
   registry as it is and treat it as a sequencing rule — nobody runs a workspace gate while another
   team's subtree is half-written — because a `#[cfg]` or a feature flag on the registry would make
   "the oracle seam exists on day one" untrue. If you would rather it not be possible, the
   alternative is moving `oracle` and `scenarios` behind their own test target, which is a
   charter-ownership change and mine to recommend, not to make.

---

## 6. Verdict and what may proceed

**PASS_WITH_RISKS.**

- Round 1's nine BLOCKERs and twenty-two MATERIALs are closed in `6893442`, with the four
  highest-blast-radius ones (digest preimage, crash image, capability probe, authority adoption)
  verified by reproduction rather than by reading the handoff.
- The one genuinely new code-side MATERIAL, K-F-39, is a doc-vs-type mismatch with no live
  exploit today, because no `PartitionConfig` decode path exists yet. It should be fixed before
  one does.
- Three of the four new MATERIALs are document defects. They do not break anything that is built;
  they will mislead the people about to build on top.

**Foundation's test planner: proceed.** `design.md` §8's row table is accurate — I checked every
row that has landed against its test, and the 22 rows plus the B-R30 vector map cleanly. Two
cautions for the planner: do **not** take `ProtectionState`, `AppendReject` or `PartitionConfig`
from §4.6/§4.8/§4.10 (K-F-40, K-F-43) — read them from `crates/rdb-core/src/contracts/`; and pick
one statement of Q-F-1 (K-F-47). M7F-05 should be split per K-F-44.

**H1 / M1 / I1 developer work: proceed.** Every seam those packages build on is landed, typed and
exercised: the scheduler, the clock, the memory engine with crash images and short flushes, the
control store's submit/complete split with all six `ControlOp` faults, the dispatcher's hop and
authority fill, the trace header and manifest. The owed capabilities refuse by name, so a package
that reaches one gets a message with the seam in it rather than a silent pass.

**Kernel-a and kernel-b developers: hold on `design.md` §4.1/§4.6/§4.8/§4.10 only** until K-F-40
and K-F-41 are closed, or until the lead names the code as authority for those sections. The rest
of the contract surface is safe to write against today.

**One operational warning that is not a finding (§4a):** the workspace does not build at this
moment. `dev-verification-1` is mid-write in `crates/rdb-sim/tests/support/oracle/`, three declared
module files do not exist yet, and because `tests/support/mod.rs` registers `pub mod oracle;` every
`rdb-sim` test binary fails to compile — foundation's four included. Nothing in `6893442` causes it;
`git show 6893442:…/support/oracle.rs` has no `pub mod` line at all, and my own test run at 22:09
was green across all 49 rows. Do not read the next red gate as a foundation regression, and take
K-F-46's evidence after verification's subtree compiles.
