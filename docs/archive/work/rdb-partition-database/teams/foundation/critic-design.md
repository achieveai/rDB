# Critic round 1 — team `foundation`

**Role:** CRITIC (round 1). **Scope:** the architect's `design.md`, `architect-handoff.md`,
`research.md`, ADR-rdb-0000/0002/0003, and the seed committed at HEAD under
`crates/rdb-core/src/**` and `crates/rdb-sim/src/**`, `crates/rdb-sim/tests/**`.
**Budget:** one complete pass, read-only. No code, ADR or architect file was edited. No git
operation was run.

**Motto applied:** no code is best code. 4,989 lines of contract landed before one line of
protocol. The short answer is that the *volume* is defensible — most of it is enum vocabulary
the other three teams have already written designs against — but the *content* has three kinds
of defect: two frozen-and-contradictory digest preimages, several seams a named consumer cannot
reach, and a small amount of vocabulary that verification explicitly withdrew and is still
being carried.

---

## 0. What I verified clean

Stated up front so the finding list is not mistaken for a verdict on everything.

| Check | Result | Evidence |
|---|---|---|
| (b) determinism — `HashMap`/`HashSet` | clean in real code | `rg -n "HashMap\|HashSet" crates/rdb-core/src crates/rdb-sim/src` → 3 hits, all in doc comments (see K-F-33) |
| (b) determinism — clock | clean | `rg -n "SystemTime\|Instant::now\|Instant\b" crates/rdb-core/src crates/rdb-sim/src` → doc mentions only; `Tick` is a plain newtype |
| (b) determinism — randomness | clean | no `rand` in either `Cargo.toml`; `rg -n "rand::"` → nothing in code |
| (b) iteration order | clean | every ordered collection in `trace.rs`, `membership.rs`, `control.rs` is `Vec` or `BTreeMap` |
| §5.4 retry mapping | correct | `errors.rs::retry_rule()` matched line-by-line against `design-specification.md` §5.4, including the four Quarantine rows |
| `ControlTime::compare` | correct | both branches strict and symmetric; saturating arithmetic fails closed to `Uncertain` |
| `Digest::of` framing | correct | `DIGEST_MAGIC` + domain byte + per-part `u64` LE length prefix — genuinely domain-separated and unambiguous, which is what K-B-08 asked for |
| `BoundaryId` | complete | 29 members, cross-checked against spike §6 |
| (i) `config-* -> rdb-*` | no such edge today | no `config-*/Cargo.toml` names an `rdb-*` package |
| ADR-0002 decision 5 | held | `rdb-core` `[dependencies]` = `bytes, serde, thiserror, tracing, sha2`, exactly |

Two hypotheses I formed and then **withdrew** on evidence are recorded in §4, because a critic
who only reports confirmed suspicions is not reporting honestly.

---

## 1. Findings

### K-F-01 — two frozen `record_digest` preimages, and they differ

- **severity:** BLOCKER
- **criterion:** `design.md` §4.8 ("frozen"); kernel-b `design.md` §1.1; spec §6.1
- **location:** `.../teams/foundation/design.md` §4.8 vs
  `crates/rdb-core/src/contracts/envelope.rs:99-112` (rustdoc on `compute_record_digest`)
- **evidence:** §4.8 freezes seven parts: `prev_digest`; `generation`/`owner_epoch`/`seq`;
  `config_version`; `request_identity`; `request_digest`; `conditions_result`; encoded mutations
  and result. The rustdoc on the function that must implement it lists a different seven:
  `prev_digest`; **header fields in declaration order** — `protocol_version`, `partition`,
  `generation`, `config_version`, `owner_epoch`, `seq`; **`lease_id`**; `request_identity` then
  `request_digest`; `conditions_result`; mutations; `result`. The two disagree on four fields
  (`protocol_version`, `partition`, `lease_id`, and the relative order of `config_version` and
  `owner_epoch`).
- **consequence:** the C0 developer implements one of them; kernel-b implements the other on the
  validation side; the known-answer vector is written against whichever the author read last. A
  digest mismatch here is not a test failure, it is `CorruptHistory` on a live replica, and F1's
  compatible-prefix selection is built on "equal digest at equal seq implies equal prefix".
  Two frozen answers is worse than none, because both look authoritative.
- **false-positive check:** I considered that the rustdoc might be describing the *encoding*
  while §4.8 describes the *semantic* set. It is not: the rustdoc is a numbered preimage part
  list in the same shape as §4.8, and `lease_id` and `protocol_version` are not derivable from
  §4.8's parts.
- **closure:** one preimage, stated once, in `design.md` §4.8; the rustdoc cites §4.8 rather than
  restating it; the C0 vector file is generated from that single statement.

### K-F-02 — the code's preimage binds `protocol_version` and `lease_id`, which kernel-b's design forbids

- **severity:** BLOCKER
- **criterion:** kernel-b `design.md` §1.1; ADR-rdb-0019 gate V12
- **location:** `crates/rdb-core/src/contracts/envelope.rs:99-112`, parts 2 and 3
- **evidence:** kernel-b's binding contract states the record digest must **include
  `partition_id`**, must **exclude `protocol_version`** ("a version bump must not rewrite
  history … gate V12"), and must **exclude `lease_id`**. The code's preimage includes both
  excluded fields. `design.md` §4.8, separately, omits `partition_id`, which kernel-b requires.
  So neither of the two frozen answers matches the consumer's stated requirement: §4.8 is missing
  a required field, the rustdoc has two forbidden ones.
- **consequence:** with `protocol_version` in the preimage, bumping the envelope version — the
  exact mechanism ADR-rdb-0002 consequence 3 names as the escape hatch for changing the digest
  algorithm — invalidates the digest of every historical record, so a rolling upgrade turns
  every existing chain into `CorruptHistory`. With `lease_id` in, a record replayed under a
  reissued lease cannot be shown identical to the original. Without `partition_id`, two
  partitions at the same `(generation, owner_epoch, seq)` with identical bodies collide, which is
  precisely the case F1 lineage selection must distinguish.
- **false-positive check:** I checked whether `partition` and `protocol_version` are already
  pinned outside the digest by the envelope header being covered elsewhere. They are not — the
  envelope carries no second digest over its own header.
- **closure:** preimage includes `partition_id`, excludes `protocol_version` and `lease_id`; a
  C0 vector row asserts "same record, two `protocol_version` values, same digest" and "same
  record, two `lease_id` values, same digest"; a second row asserts two partitions at equal
  `(generation, owner_epoch, seq)` and equal body produce **different** digests.

### K-F-03 — `CrashImage.surviving` is typed `DurableSeq`, so a process crash cannot be modelled

- **severity:** BLOCKER
- **criterion:** spec §5.2, §6.1 (buffered/durable split); charter H1/M1; ADR-rdb-0003 reference
  to "the buffered/durable split the trace has to make observable"
- **location:** `crates/rdb-sim/src/storage/crash_image.rs` — `pub surviving: Vec<(PartitionId,
  Generation, DurableSeq)>`
- **evidence:** the module doc says `ProcessCrash` "keeps everything applied, buffered or
  synced", while `reopen()`'s doc says reopen yields "a `durable` watermark equal to what
  survived and a `buffered_applied` equal to it". Those two sentences describe different
  machines. The type forces the second: the only thing a crash image can carry is a
  `DurableSeq`, and B-R13 deliberately gives the three watermarks no conversions, so buffered
  state that survives a process crash has no representation except by being relabelled durable.
- **consequence:** the single most valuable class of history in this milestone — "acknowledged at
  buffered, process dies, does the write survive?" — becomes unrepresentable. Worse, it is
  unrepresentable in the *safe* direction: every process crash silently promotes buffered to
  durable, so a kernel that acknowledges too early passes. That is a false-green on the headline
  property "no lost acknowledged write". B-R13's whole point was to stop exactly this conversion,
  and the sim performs it in a struct field.
- **false-positive check:** I checked whether a separate field carries the buffered tail. There
  is none; `CrashImage` is 58 lines and `surviving` is the only per-partition datum.
- **closure:** `surviving` carries both watermarks per partition — `(PartitionId, Generation,
  DurableSeq, AppliedSeq)` or a named struct — `ProcessCrash` preserves the buffered tail and
  `PowerLoss` truncates to durable, `reopen()` restores each to its own watermark, and an M7F row
  asserts the two crash kinds produce different reopened states from one pre-crash image.

### K-F-04 — the read seam exposes no version, so no condition in the spec can be evaluated

- **severity:** BLOCKER
- **criterion:** spec §5.1 (conditions), §5.2; ADR-rdb-0003 decision 2
- **location:** `crates/rdb-core/src/contracts/storage.rs` — `trait SnapshotRead { handle, at,
  generation, get, scan }`; compare `crates/rdb-sim/src/storage/memory.rs` —
  `version_of(partition, generation, key) -> Result<Option<u64>>`
- **evidence:** `Condition::VersionEquals` and `Mutation::expected_version` are both in the
  contract. The only read surface the kernel is given returns a value, not a version.
  `MemoryEngine` already implements `version_of` — the data exists and the kernel cannot reach
  it, because ADR-rdb-0003 decision 2 makes `&dyn SnapshotRead` the one read surface.
- **consequence:** the first module to implement condition evaluation cannot, and its only
  options are all bad: widen the trait mid-milestone (a shared-interface change, lead-only),
  smuggle a version into the value bytes, or route reads through the effect queue — the
  alternative ADR-rdb-0003 decision 2 explicitly rejected. Any of the three costs the milestone
  more than adding one method now.
- **false-positive check:** I considered that `get` might return a versioned type. It returns the
  value only; no accessor on the returned type exposes a version.
- **closure:** `SnapshotRead` gains a version accessor (`version(&self, ns, key) -> Option<u64>`
  or `get` returns value-and-version), `EmptySnapshot` and `MemoryEngine` implement it, and a C0
  row evaluates `VersionEquals` against a populated snapshot.

### K-F-05 — the kernel is handed `generation` and `owner_epoch` as ambient context with no effect that changes them

- **severity:** BLOCKER
- **criterion:** charter DO-NOT ("No kernel decisions in the sim"); ADR-rdb-0007; ADR-rdb-0003
  decision 5 (environment-owned events are an enumerated exception, not the default)
- **location:** `crates/rdb-core/src/contracts/event.rs` — `StepCtx { …, generation, owner_epoch,
  config_version, … }`; `EffectKind::{Send, Store, Control, Timer, Reply}`
- **evidence:** the partition's generation, owner epoch and config version arrive as fields the
  environment fills in before every step. No `Effect` variant lets a module declare that it has
  adopted a new epoch after a successful fenced CAS, and `TopologyChange` (V-R12) covers topology,
  not epoch. So the value in `StepCtx` is decided by whoever builds the context — `rdb-sim`.
- **consequence:** either `rdb-sim` implements the epoch-advance rule from ADR-rdb-0007 (the
  charter's explicit DO-NOT, and it makes the oracle agree with the simulator by construction),
  or the kernel silently keeps running under a stale epoch while its own control CAS has already
  advanced it. Fencing is the property; the seam cannot express it.
- **false-positive check:** I looked for a `ControlEffect` whose completion could carry the new
  epoch back and be applied by the environment without a protocol decision. `Cas` completes with
  a `CasOutcome`; mapping an outcome to "the epoch is now N" is the protocol rule, not a
  mechanical translation.
- **closure:** either the kernel owns its authority state (`&mut self` holds generation/epoch and
  `StepCtx` stops carrying them) or a new effect — `Effect::AdoptAuthority { generation,
  owner_epoch, config_version }` — is what changes them, and an M7F row asserts a step under a
  stale epoch is refused.

### K-F-06 — no trace vocabulary for the control plane, so ADR-0008 §7 item 4 (as amended by A-R15) cannot be asserted

- **severity:** BLOCKER
- **criterion:** ADR-rdb-0008 §7 item 4; ruling A-R15; ADR-rdb-0003 decision 5 (the oracle reads
  the trace and nothing else)
- **location:** `crates/rdb-core/src/contracts/trace.rs` — the 21 `TraceKind` variants
- **evidence:** A-R15 restated ADR-0008 §7 item 4 as a **kernel assertion**, on the reasoning
  that "the kernel-side consequence … is observable in the trace". It is not. There is no
  `TraceKind` for a control CAS attempt or outcome, none for a watch termination and its kind,
  and none for a coherent family reload. The assertion "no family reload occurs unless a
  termination was delivered first" has no two events to relate.
- **consequence:** the amended ADR requirement is unassertable, so the hostile fake control store
  — the most protocol-relevant fault source in this milestone — produces no checkable evidence.
  The gate that A-R15 moved from the store to the kernel now exists in neither place.
- **false-positive check:** I re-read all 21 kinds and the `BoundaryId` list for a
  control-flavoured member that could stand in. `BoundaryId` names boundaries for fault
  injection; it is not a trace kind, and the oracle folds `TraceEvent`s.
- **closure:** two trace kinds land — one declaring a control-store interaction and its outcome
  (including the termination kind and whether it was resumable), one declaring a family reload
  with its `snapshot_revision` — and an M7F row folds a trace in which a reload without a prior
  termination is rejected by the checker.

### K-F-07 — `TraceKind::ProtectionState` has no `quorum_rule`

- **severity:** BLOCKER
- **criterion:** verification `trace-requirements.md` §3.14; ruling V-R10
- **location:** `crates/rdb-core/src/contracts/trace.rs`, `TraceKind::ProtectionState`
- **evidence:** §3.14 requires `quorum_rule: Rf3 | DegradedRf2` on every `protection_state`
  event. The variant carries phase and watermarks and no quorum rule.
- **consequence:** the checker cannot tell which acknowledgement rule was in force at the moment
  a write was acknowledged, so the degraded-RF2 invariant — the whole point of V-R10's "emit on
  every `config_version` change" — is unwritable. This is a named consumer ask against an owned
  artifact, stated before the seed was written.
- **false-positive check:** I checked whether the rule is derivable from `TraceHeader.topology`
  plus the config version. It is not: degraded mode is a protection decision, not a topology
  fact, and deriving it in the checker re-implements the rule under test.
- **closure:** the field exists as a closed enum and an M7F row asserts it changes across a
  simulated degrade.

### K-F-08 — `TraceKind::op_skipped` is missing

- **severity:** BLOCKER
- **criterion:** verification `trace-requirements.md` §3.16a
- **location:** `crates/rdb-core/src/contracts/trace.rs` (absent)
- **evidence:** §3.16a specifies `op_skipped` as a required event kind. None of the 21 variants
  covers it. `architect-handoff.md` §5.1 states "**Nothing was pushed back.**" — that sentence is
  false for this item and for K-F-07.
- **consequence:** "no duplicate effect from a retry" is one of the three headline properties. A
  deduplicated retry that produces no observable event is indistinguishable in the trace from a
  request that was never submitted, so the property is checkable only in the direction that
  cannot fail.
- **false-positive check:** I searched `trace.rs` for `skip`, `Dedup`, `Suppressed` and
  `AlreadyApplied`. Nothing.
- **closure:** the kind exists with the fields §3.16a names, and the handoff's "nothing was
  pushed back" is corrected.

### K-F-09 — `TraceHeader` carries `seed`, which verification explicitly refused

- **severity:** MATERIAL
- **criterion:** `trace-requirements.md` §1 ("Not a bare `seed`"); ADR-rdb-0003 decision 6
- **location:** `crates/rdb-core/src/contracts/trace.rs`, `TraceHeader`
- **evidence:** §1 requires a `provenance` structure and says in terms that a bare `seed` is not
  it. The header field is `seed: u64`. §1 also requires `config` and `partitions: u8`; neither is
  present. ADR-rdb-0003 decision 6 agrees with verification ("the reproducer is the recorded
  event stream, not the seed") and the header then contradicts the ADR that froze it.
- **consequence:** a failing campaign row reports a seed that, by the ADR's own reasoning, does
  not reproduce it. The operator-facing reproducer command is wrong from the first row.
- **false-positive check:** `generator_version` and `schema_version` are present, which is most of
  what provenance needs — this is a shape and naming gap, not a total absence, hence MATERIAL
  rather than BLOCKER.
- **closure:** `provenance { seed, generator_version, schema_version, … }` per §1, plus `config`
  and `partitions`; an M7F row asserts a header round-trips through JSONL with all §1 fields.

### K-F-10 — `Dispatcher::capability_report` steps every module with a phantom event and discards the effects

- **severity:** BLOCKER
- **criterion:** spike §8; ADR-rdb-0003 decision 7; charter I1
- **location:** `crates/rdb-sim/src/harness/dispatch.rs` — `capability_report(&mut self, ctx,
  probe) -> [CapabilityState; 6]`
- **evidence:** the probe calls `self.step(module, ctx, probe)` on all six modules through `&mut
  self`, keeps only whether the error kind was `Unavailable`, and drops the returned
  `Vec<Effect>`. It also classifies **any non-`Unavailable` outcome** as `Wired` — including
  `Ok(effects)` and including every other error.
- **consequence:** today all six modules are stubs and this is harmless. The day the first module
  lands — this milestone — the trace-start capability probe mutates that module's state with an
  event the protocol never sent and silently swallows any effects it produced. Every campaign run
  then begins from a corrupted state, deterministically, which is the worst kind: it reproduces,
  so it will be mistaken for a protocol bug. Separately, a module that returns `Ok` for an
  unrecognised probe is reported `Wired` while being empty, which is the "fake success" spike §8
  forbids.
- **false-positive check:** I checked whether `probe_event()` in `tests/support/mod.rs` is a
  designated no-op event modules are required to ignore. It is an ordinary `Event`; nothing in
  the contract obliges a module to treat it as inert.
- **closure:** capability is reported by a non-mutating method on `Module` (`fn capability(&self)
  -> CapabilityState`), or the probe runs against a clone; `Wired` is asserted positively rather
  than inferred from "not `Unavailable`"; M7F-01 is re-stated against the new method.

### K-F-11 — `ReplyEffect` has no read answer

- **severity:** MATERIAL
- **criterion:** spec §5.1; kernel-a `design.md` §1
- **location:** `crates/rdb-core/src/contracts/event.rs` — `ReplyEffect::{Transaction, Status,
  Failed}`
- **evidence:** a client read has no reply variant. Reads are pure lookups (ADR-rdb-0003 decision
  2), but the *answer* still has to leave the kernel as an effect, and the trace's
  `ReadServiceOutcome` (F-R4) implies a served read exists.
- **consequence:** the first read-serving row has nowhere to put its answer, and the obvious
  workaround — returning it inside `Transaction` — makes read and write outcomes
  indistinguishable in the trace.
- **false-positive check:** I checked whether M7 serves reads at all. F-R3 adds a `ControlOp` to
  inject `ReadOutcome::Unavailable` and F-R4 renames `trace::ReadOutcome`, so the milestone does
  model read service.
- **closure:** `ReplyEffect::Read { outcome: ReadServiceOutcome, … }` exists, or the lead rules
  that M7 serves no client reads and F-R3/F-R4 are re-scoped accordingly.

### K-F-12 — `ControlStore::snapshot_family` returns a `Revision` but `ControlEvent::FamilySnapshot` needs records

- **severity:** MATERIAL
- **criterion:** ADR-rdb-0008 §7 item 6
- **location:** `crates/rdb-sim/src/sim/control.rs` — `snapshot_family(&self, family: ControlKey)
  -> Result<Revision>`; `crates/rdb-core/src/contracts/control.rs` —
  `ControlEvent::FamilySnapshot { family, snapshot_revision, records }`
- **evidence:** item 6 requires a coherent family read with a resumable `snapshot_revision`. The
  event carries `records`; the store method that must produce it returns a revision and nothing
  else.
- **consequence:** the fake store cannot actually serve the coherent read, so item 6's row is
  unimplementable without changing a signature the architect froze.
- **false-positive check:** I looked for a paired `read_family` that returns records at a given
  revision. There is none.
- **closure:** the method returns records and revision together, and an M7F row asserts two reads
  at one `snapshot_revision` are identical across an interleaved write.

### K-F-13 — `ControlChange` delivers the record value on the watch stream, which makes ADR-0008's "structural" claim untrue

- **severity:** MATERIAL
- **criterion:** ADR-rdb-0008 §4 ("A watch event causes a read, never a state change. This is
  structural, not a convention.")
- **location:** `crates/rdb-core/src/contracts/control.rs` — `ControlEvent::Watched { changes:
  Vec<ControlChange> }`, where `ControlChange` carries `value: Option<Bytes>`
- **evidence:** the value bytes arrive on the watch. A kernel can therefore widen a right
  directly from a watch event, which the ADR says has no representation.
- **consequence:** the ADR's strongest safety claim is downgraded to a review convention without
  anyone deciding to downgrade it, and the field that enables the shortcut is one nothing in M7
  needs. This is the clearest **delete-code** finding in the seed: removing `value` makes the
  property structural for free.
- **false-positive check:** I checked whether any consumer design asks for the value on the
  watch. Kernel-a §1.2 asks for `ReadOk { record, read_revision }` from an explicit read — the
  opposite.
- **closure:** `ControlChange` carries `(key, revision)` only; or ADR-0008 §4 drops the word
  "structural". Prefer the first.

### K-F-14 — `ControlOp::PlanCas` has no node, and `ControlStore::cas` is synchronous

- **severity:** MATERIAL
- **criterion:** ADR-rdb-0008 §7 items 7 and 8; ADR-rdb-0003 decision 3
- **location:** `crates/rdb-sim/src/sim/control.rs` — `ControlOp::PlanCas { outcome }`;
  `ControlStore::cas(...)` returning a result directly
- **evidence:** items 7 and 8 require a late completion after expiry and an effect that never
  completes; `ControlOp::{DelayCompletion, DropCompletion}` exist for exactly that. A synchronous
  `cas` cannot be delayed or dropped, and with no `node` on `PlanCas` a plan cannot target one
  node's CAS while another node's succeeds — the split-brain case fencing exists for.
- **consequence:** items 7 and 8 have enum variants and no mechanism; the two-nodes-race row
  cannot be written.
- **false-positive check:** the zero-sized stub signatures are placeholders, so this could be
  called premature. It is not: `ControlOp` is an enumerated fault vocabulary the coverage matrix
  counts, and a variant that no signature can honour will be counted as covered.
- **closure:** `cas` issues a `ControlEffect` and completes as a `ControlEvent`; `PlanCas` carries
  a `node`; an M7F row shows two nodes' CAS against one key with one delayed past expiry.

### K-F-15 — `ControlTime` has no sample age

- **severity:** MATERIAL
- **criterion:** spec §7.2; ruling A-R12
- **location:** `crates/rdb-core/src/contracts/time.rs` — `ControlTime { estimate, error_millis,
  bound_established }`
- **evidence:** the struct records the estimate, its error and whether a bound was established,
  but not *when* the sample was taken. A-R12 requires a stale bound to stop being trusted.
- **consequence:** an estimate established long ago stays `bound_established: true` forever, so
  `ClockVerdict` is confident precisely when it should not be. The fail-closed design is
  defeated by the missing field rather than by any logic error.
- **false-positive check:** `error_millis` could in principle be widened by the environment as
  time passes. Nothing in the sim does that, and making the environment widen it puts the
  staleness rule in `rdb-sim`, which the charter forbids.
- **closure:** a `sampled_at: Tick` field (or `age_ticks`) exists, `compare` takes staleness into
  account or the caller can, and a C0 row asserts a stale bound yields `Uncertain`.

### K-F-16 — the authority estimate is typed `Tick`

- **severity:** ADVISORY
- **criterion:** `time.rs` module doc, which says the two notions "must not be confused"
- **location:** `crates/rdb-core/src/contracts/time.rs` — `ControlTime { estimate: Tick, … }`
- **evidence:** the module doc distinguishes simulator ticks from an authority clock estimate and
  then gives the estimate the simulator's type.
- **consequence:** the confusion the doc warns about compiles silently.
- **false-positive check:** it is one newtype away from harmless; nothing is wrong today.
- **closure:** a distinct newtype, or the doc stops claiming a separation the types do not make.

### K-F-17 — no `compute_request_digest`, and no A-R18 vector

- **severity:** MATERIAL
- **criterion:** ruling A-R18
- **location:** `crates/rdb-core/src/contracts/envelope.rs` (absent); `design.md` §8 acceptance
  rows
- **evidence:** A-R18 froze the request-digest preimage and required a C0 vector, "same request,
  two deadlines, same digest". `Domain::Request` exists in `digest.rs`; no function computes it
  and no row asserts it.
- **consequence:** dedup is a headline property and its digest has no implementation and no
  known-answer test. Deadline-sensitivity is the specific bug A-R18 anticipated.
- **false-positive check:** the four in-flight Codec stubs are `compute_record_digest`, `encode`,
  `decode_header`, `decode` — request digest is not among them, so this is not "in flight".
- **closure:** the function exists with the A-R18 preimage and the two-deadline vector row passes.

### K-F-18 — no `AuthorityGeneration` newtype, which kernel-a assumes is present

- **severity:** MATERIAL
- **criterion:** kernel-a `design.md` §1.1
- **location:** `crates/rdb-core/src/contracts/` (absent)
- **evidence:** kernel-a's design is written against `AuthorityGeneration`. The seed has
  `Generation` (partition data generation) and `OwnerEpoch`, and kernel-a uses the name for
  neither.
- **consequence:** kernel-a's developer either invents the type in `rdb-core` (a shared-artifact
  write they do not own) or silently substitutes `Generation`, conflating data generation with
  authority generation — the exact conflation ADR-rdb-0007 separates.
- **false-positive check:** I checked whether `OwnerEpoch` is the same thing under another name.
  Kernel-a §1.1 uses both in one sentence, so it is not.
- **closure:** the lead rules whether `AuthorityGeneration` is a third newtype or kernel-a's
  design is corrected to use `OwnerEpoch`; whichever, one name appears in both documents.

### K-F-19 — `ControlEffect::Watch` has no prefix, and `Reload` keys a family by an instance key

- **severity:** MATERIAL
- **criterion:** kernel-a `design.md` §1.2; ADR-rdb-0008 §2 (key families)
- **location:** `crates/rdb-core/src/contracts/control.rs` — `ControlEffect::Watch { from:
  Revision }`, `ControlEffect::Reload { key: ControlKey }`, `ControlEvent::FamilySnapshot {
  family: ControlKey, … }`
- **evidence:** kernel-a asks for `ControlPrefix`-scoped `ReadFamily` and `Watch`. `Watch` is
  scoped by revision only — it watches everything. `Reload` and `FamilySnapshot` use a
  `ControlKey` (one record's key) in a `family` position.
- **consequence:** every kernel sees every other family's changes and must filter, which is both
  a correctness hazard and a per-event cost in a 10,000-history budget; and "family" being typed
  as an instance key means the compiler cannot stop a single-record key being passed where a
  family is meant.
- **false-positive check:** `ControlKey` might itself be a prefix-capable enum. It is the record
  key type used by `Cas` and `Get`, so it is not.
- **closure:** a `ControlPrefix` type exists; `Watch` and the family operations take it; kernel-a
  §1.2's `ReadFamily` has a signature to bind to.

### K-F-20 — `ReadOutcome::Absent` carries no revision

- **severity:** MATERIAL
- **criterion:** ADR-rdb-0008 §7 item 2 (`Unknown` ≠ `Unavailable` ≠ `Conflict`); kernel-a §1.2
- **location:** `crates/rdb-core/src/contracts/control.rs` — `ReadOutcome::Absent`
- **evidence:** a present read returns a revision; an absent one returns nothing. Kernel-a asks
  for `ReadOk { record, read_revision }` and correlates by revision.
- **consequence:** "this key did not exist as of revision R" cannot be expressed, so a CAS-on-
  absent has no revision to fence against and a stale absent read is indistinguishable from a
  fresh one.
- **false-positive check:** the store revision is global in etcd semantics, so the caller could
  read it separately. It cannot here — nothing returns the store revision alongside an absent
  read.
- **closure:** `Absent { as_of: Revision }`; a row asserts a CAS keyed on an absent read at a
  stale revision is rejected.

### K-F-21 — `Member.boot` has no writer and fails open

- **severity:** MATERIAL
- **criterion:** `trace-requirements.md` §3.7; ADR-rdb-0007 (a reincarnated node is a new peer)
- **location:** `crates/rdb-core/src/contracts/membership.rs` — `copy_of`, which matches with
  `member.boot.is_none_or(|boot| boot == peer.boot)`
- **evidence:** when `Member.boot` is `None` the predicate is true for **any** peer boot id.
  Nothing in `rdb-core` or `rdb-sim` ever sets `Member.boot` to `Some`, so today every membership
  lookup ignores boot entirely.
- **consequence:** a node that crashed and restarted is accepted as the same copy it was before
  the crash, so its pre-crash acknowledgements are credited to the new incarnation. That is a
  direct path to counting a lost write as acknowledged, and it is currently the default
  behaviour, not an edge case. `is_none_or` fails open where fencing must fail closed.
- **false-positive check:** `None` could legitimately mean "not yet observed", with the caller
  expected to refuse. No caller refuses; `copy_of` is the seam.
- **closure:** either `Member.boot` is non-optional, or `copy_of` refuses a `None` member against
  a peer that names a boot id; an M7F row asserts a reincarnated peer is not matched.

### K-F-22 — `AckEvidence` carries no `BootId`

- **severity:** MATERIAL
- **criterion:** `trace-requirements.md` §3.7 — `ack_evidence: Vec<(NodeId, BootId, PeerRole,
  DurabilityClass)>`
- **location:** `crates/rdb-core/src/contracts/trace.rs` — `AckEvidence { node, role, durability }`
- **evidence:** three of the four required fields are present; `BootId` is missing.
- **consequence:** the checker cannot tell two acknowledgements from the same node across a
  restart apart, which is the same hole as K-F-21 but on the oracle's side, where it cannot be
  patched by the kernel.
- **false-positive check:** none needed — this is a named field in a named consumer requirement.
- **closure:** the field exists; a row folds a trace with a restart between two acks and the
  checker counts one copy, not two.

### K-F-23 — `required_regular()` includes the primary

- **severity:** MATERIAL
- **criterion:** spec §5.2 (acknowledgement rule)
- **location:** `crates/rdb-core/src/contracts/membership.rs` — `required_regular` filters on
  `role.may_qualify_ack()`, which is true for `Primary`
- **evidence:** the method name says "regular" — the set of non-primary copies whose acks the
  primary is waiting for — and the predicate admits the primary itself.
- **consequence:** an RF3 rule implemented as "two of `required_regular()`" is satisfied by the
  primary plus one secondary where the spec requires two secondaries. That is off-by-one in the
  safety direction that loses writes, and it reads as correct.
- **false-positive check:** `may_qualify_ack()` may be intended as "counts toward durability",
  with the primary legitimately counting. If so the method name is wrong, which is the same
  finding at ADVISORY.
- **closure:** the method excludes the primary or is renamed to say what it returns, and a C0 row
  asserts its cardinality on an RF3 membership.

### K-F-24 — `state_digest_after` and `published_state_digest` are carried after verification withdrew them

- **severity:** MATERIAL
- **criterion:** `trace-requirements.md` §6 (explicit withdrawal)
- **location:** `crates/rdb-core/src/contracts/trace.rs` — `TraceKind::BatchApply
  { state_digest_after, … }`, `TraceKind::Publish { published_state_digest, … }`
- **evidence:** §6 withdraws both fields by name. Both are in the seed.
- **consequence:** two whole-state digests are computed per apply and per publish, over the full
  state, in a campaign budgeted at 10,000 histories in ten minutes — the only super-linear cost
  in the trace path, kept for a consumer that said it does not read them. This is the clearest
  over-engineering finding by cost, and the only one that threatens check (h) directly. It also
  invites a checker to compare kernel-derived state digests, which is the second implementation
  ADR-rdb-0003 decision 5 forbids.
- **false-positive check:** a digest could be cheap if the state is small. It is O(state) per
  event by construction, and M7's whole point is long histories.
- **closure:** both fields are deleted, or verification re-states a use for them in writing.

### K-F-25 — `StoreEffect::Flush` makes the kernel capture the prefix

- **severity:** MATERIAL
- **criterion:** spec §5.2, §6.1; ADR-rdb-0003 decision 1
- **location:** `crates/rdb-core/src/contracts/storage.rs` — `StoreEffect::Flush { ticket,
  captured: Vec<CapturedPrefix> }`
- **evidence:** the kernel hands the storage layer the prefix it believes is being made durable.
- **consequence:** the kernel's belief about what was flushed becomes the definition of what was
  flushed, so `sync_wal_through` cannot disagree with the kernel and the fault "the flush covered
  less than the kernel thought" is unrepresentable. The buffered/durable split is then only ever
  tested in the direction where the kernel is right.
- **false-positive check:** the ticket could let the environment answer with its own prefix at
  completion. Nothing in the completion event carries a prefix, so the captured set is the only
  answer.
- **closure:** the storage completion event reports the durable prefix the environment actually
  achieved, and a `StorageOp` can make it shorter than requested; an M7F row asserts a short
  flush is observed as short.

### K-F-26 — `proves_no_mutation()` is true for `Unavailable`

- **severity:** MATERIAL
- **criterion:** spec §5.4; spike §8
- **location:** `crates/rdb-core/src/contracts/errors.rs` — `proves_no_mutation()`, true for
  `RetryRule::NotWired`
- **evidence:** an unwired capability returns `Unavailable`/`NotWired`, and the seed asserts from
  that that no mutation occurred.
- **consequence:** correct today, when nothing is wired. It becomes a lie as soon as a partially
  wired module can fail after emitting a store effect, and it is asserted in M7F-01, so the false
  claim is load-bearing in a passing row. An error that means "not built" should prove nothing.
- **false-positive check:** the M7F-01 assertion is what makes this worth raising now rather than
  in M8 — the claim is already being tested as true.
- **closure:** `proves_no_mutation()` returns false for `NotWired`, or M7F-01 asserts the
  narrower fact ("no effect was returned") directly.

### K-F-27 — no run manifest type, so resolved budgets are not recorded

- **severity:** MATERIAL
- **criterion:** charter I1 ("manifest records resolved budgets"); spike §7
- **location:** `crates/rdb-core/src/contracts/` and `crates/rdb-sim/src/harness/` (absent)
- **evidence:** `Budgets::SPEC_DEFAULTS` exists and `TraceHeader` carries `budgets`, but there is
  no manifest type and no record of which budget values a run actually resolved to after
  environment overrides.
- **consequence:** a campaign row that fails under overridden budgets cannot be distinguished
  from one that fails under defaults, which is the same class of problem `RETCD_TEST_DEADLINE_SCALE`
  was introduced to solve for the existing gate.
- **false-positive check:** `TraceHeader.budgets` covers most of it. What is missing is the
  provenance of the values (default vs override) and the rest of the run configuration — related
  to K-F-09 and closable with it.
- **closure:** the manifest exists, names resolved budgets and their source, and an M7F row
  asserts an override is visible in it.

### K-F-28 — "an unregistered handler fails explicitly" is vacuous as built

- **severity:** MATERIAL
- **criterion:** charter I1; ADR-rdb-0003 decision 7
- **location:** `crates/rdb-sim/src/harness/dispatch.rs` — `step()` resolves the module by
  `ModuleName::ALL.iter().position(...).expect("ModuleName::ALL is exhaustive")`
- **evidence:** there is no registry. Dispatch indexes a fixed array of six, so "unregistered"
  cannot occur and the requirement is satisfied by construction rather than by behaviour. The
  `.expect` is unreachable today and becomes a panic path the moment `ModuleName` and the array
  drift.
- **consequence:** the acceptance row for I1 tests nothing, and the failure mode it was written
  to prevent (a campaign runner panicking on an unbuilt module — ADR-rdb-0003 decision 7's stated
  reason) is reintroduced by the `.expect`.
- **false-positive check:** the fixed array is arguably the right design and the requirement is
  the thing that should change. Either way the row as written does not test what it says.
- **closure:** the array is indexed by `ModuleName` without a fallible lookup (making the row
  unnecessary and deleting it), or a real registry exists and the row exercises a missing entry.

### K-F-29 — the `rdb-sim` provider stubs are `Copy` unit structs with `const fn` methods

- **severity:** MATERIAL
- **criterion:** charter H1; spike §4
- **location:** `crates/rdb-sim/src/sim/{scheduler,clock,network,control,cluster}.rs` — e.g. `pub
  struct Scheduler;` deriving `Clone, Copy`, with `const fn` methods including `const fn` taking
  `&mut self`
- **evidence:** every provider is zero-sized and every method is `const`, returning
  `SimError::Unavailable("<path>")`.
- **consequence:** none of these signatures survives implementation — a scheduler holds a queue, a
  clock holds a tick, a network holds in-flight messages — so `const` and `Copy` come off
  everywhere and every call site churns. `Copy` on a stateful simulator component is also an
  active hazard: a silent copy of a scheduler would duplicate its state rather than alias it, and
  that bug is invisible at the call site. The shape says "this is a value"; it is about to become
  the least value-like thing in the crate.
- **false-positive check:** `Unavailable` stubs are correct per spike §8 — I am not objecting to
  the stubs, only to the traits and `const` that advertise properties the real thing cannot have.
- **closure:** the provider types carry their state fields (even if unused and `Unavailable`),
  and neither `Copy` nor `const fn` appears on them.

### K-F-30 — `crates/rdb-sim/tests/harness.rs` uses plain `#[test]`

- **severity:** MATERIAL
- **criterion:** team-rules (JSONL logging via `#[retcd_test]`); ADR-0014
- **location:** `crates/rdb-sim/tests/harness.rs` — three `#[test]` functions
- **evidence:** `rdb-sim` dev-depends on `config-log` and `config-log-macros` precisely for this
  (ADR-rdb-0002 decision 2), and the row does not use them.
- **consequence:** M7F-01 — the one passing row and the evidence cited in ADR-rdb-0003's
  Verification section — produces no JSONL, so the DuckDB Q-rows have nothing to query and the
  campaign reporting path is untested at the moment it is cheapest to test.
- **false-positive check:** `#[retcd_test]` might be unsuitable for a non-async unit row. It is
  used for synchronous rows elsewhere in the workspace.
- **closure:** the three rows use `#[retcd_test]` and a Q-row reads them.

### K-F-31 — ADR-rdb-0002 and ADR-rdb-0003 cite verification greps that do not reproduce

- **severity:** MATERIAL
- **criterion:** team-rules evidence standard ("give the exact command and the observed output")
- **location:** `docs/ADRs/rdb/0002-...md:96`; `docs/ADRs/rdb/0003-...md:114-115`
- **evidence:** ADR-0002 claims `grep -rn "HashMap" crates/rdb-core/src crates/rdb-sim/src` finds
  nothing; it finds three doc-comment hits. ADR-0003 claims the same for `todo!` and for
  `SystemTime|Instant::now|rand::`; both find doc-comment hits. The *properties* hold — no code
  uses any of them (§0) — but the stated commands do not produce the stated output.
- **consequence:** three Verification bullets in two ADRs fail if anyone runs them, which trains
  the next reader to skip the Verification section. That is a slow, expensive failure.
- **false-positive check:** I ran each command as written; the hits are real and are all in
  comments.
- **closure:** the commands exclude comments (or the ADRs state the observed hit count and why it
  is acceptable).

### K-F-32 — ADR-rdb-0002's forbidden direction is unenforced, and `scripts/gate.sh` has no rdb wiring

- **severity:** MATERIAL
- **criterion:** ADR-rdb-0002 decision 2 and Consequences; check (i)
- **location:** `docs/ADRs/rdb/0002-...md:84-85`; `scripts/gate.sh:42-44`
- **evidence:** the ADR admits "the forbidden direction is a convention, not a compiler error …
  checked by review and by the crate list in this ADR". The gate runs fmt, clippy and tests, and
  nothing inspects the dependency graph. Four teams are about to write in parallel.
- **consequence:** the one architectural invariant that is cheap to check mechanically and
  expensive to undo later is the one left to review. A `config-* -> rdb-*` edge added under
  deadline compiles, passes the gate and is discovered at M8.
- **false-positive check:** no such edge exists today (§0), so this is prevention, not a defect
  in the current graph — hence MATERIAL, not BLOCKER. `cargo metadata` makes the check a few
  lines and needs no new dependency.
- **closure:** a gate stage parses `cargo metadata` and fails if any `config-*` package depends
  on an `rdb-*` package, and the ADR's Consequences paragraph is corrected.

### K-F-33 — `architect-handoff.md` §5.1 states "Nothing was pushed back"

- **severity:** MATERIAL
- **criterion:** team-rules handoff format; check (j)
- **location:** `.../teams/foundation/architect-handoff.md` §5.1
- **evidence:** `quorum_rule` (K-F-07), `op_skipped` (K-F-08) and `provenance` (K-F-09) are all
  named consumer asks that the seed does not satisfy.
- **consequence:** the test planner reads the handoff and plans rows against a vocabulary that
  does not exist. A false completeness claim costs more than the three missing fields.
- **false-positive check:** §5.1 might mean "nothing was pushed back *to me*" rather than "I
  delivered everything asked". Either reading misleads the next reader.
- **closure:** §5.1 lists the three deferrals with their dispositions.

### K-F-34 — `AppendReject` has no generation or quarantine variant

- **severity:** ADVISORY
- **criterion:** spec §5.4; ADR-rdb-0005
- **location:** `crates/rdb-core/src/contracts/envelope.rs` — `AppendReject`
- **evidence:** the reject enum has no variant for "this envelope belongs to a different
  generation" or for a digest-chain break requiring quarantine.
- **consequence:** kernel-b maps those cases onto a neighbouring variant, and the trace loses the
  distinction between a benign stale append and a corrupt-history event.
- **false-positive check:** `RdbError` has `CorruptHistory`, so the error path exists; this is
  about the structured reject the replication seam returns.
- **closure:** kernel-b confirms the variant set, or one is added.

### K-F-35 — `control.rs` module doc references a nonexistent `ControlEvent::WatchGap`

- **severity:** ADVISORY
- **criterion:** check (j); `#![deny(missing_docs)]` intent
- **location:** `crates/rdb-core/src/contracts/control.rs` module doc
- **evidence:** the doc names `ControlEvent::WatchGap`; no such variant exists. Gap-ness is
  expressed by `WatchTermination::is_gap()`.
- **consequence:** a broken intra-doc link, and a reader looks for a variant that was renamed.
- **false-positive check:** `cargo doc --no-deps` would flag it; I did not run it (read-only
  budget) and the absence is confirmed by reading the enum.
- **closure:** the doc names `WatchTermination::is_gap()`.

### K-F-36 — `WatchTermination::NotLeader` drops the ADR's `validated_hint`

- **severity:** ADVISORY
- **criterion:** ADR-rdb-0008 §4 termination table
- **location:** `crates/rdb-core/src/contracts/control.rs` — `WatchTermination::NotLeader`
- **evidence:** the ADR's table pairs `NotLeader` with a hint the client must validate before
  believing. The variant is a bare unit.
- **consequence:** the "never believe the hint without a read" behaviour has nothing to test
  against, so item 3's coverage is one case thinner than the table.
- **false-positive check:** dropping the hint is arguably safer than carrying it — if so, say so
  in the ADR rather than diverging silently.
- **closure:** the variant carries the hint, or ADR-0008 §4 records the deliberate omission.

### K-F-37 — `Digest::of` silently clamps a part length

- **severity:** ADVISORY
- **criterion:** spec §6.1 (unambiguous framing)
- **location:** `crates/rdb-core/src/contracts/digest.rs` —
  `u64::try_from(part.len()).unwrap_or(u64::MAX)`
- **evidence:** on a 64-bit target `usize` is `u64`, so the fallback is unreachable; on a 32-bit
  target it is a widening conversion and equally unreachable. The clamp can never fire.
- **consequence:** dead code that reads as a deliberate safety measure, and if it ever did fire it
  would produce a length prefix that does not match the part — silently breaking the one property
  the framing exists to provide.
- **false-positive check:** clippy does not flag it because `try_from` is technically fallible for
  the generic case.
- **closure:** `part.len() as u64` with a comment, or a debug assertion instead of a clamp.

### K-F-38 — smaller doc/code divergences

- **severity:** ADVISORY
- **criterion:** check (j)
- **location:** various
- **evidence and consequence, one line each:**
  - `crates/rdb-sim/src/harness/dispatch.rs` — `.expect("ModuleName::ALL is exhaustive")` is the
    only panic path in the crate (see K-F-28).
  - `rdb-core` dev-depends on `config-*` while ADR-rdb-0002 decision 2 says only `rdb-sim` does.
  - `hex` appears as a dev-dependency although `design.md` §4.7 argues against needing it.
  - `TopologyEntry`'s derived `Ord` field order does not match the `(partition, node)` order the
    doc states; equal today, divergent the moment a field is added.
  - `CasOutcome::Conflict { exists, current }` leaves `current` meaningless when `exists` is
    false; an enum would make it unrepresentable.
  - `ProtectionPhase::Resuming` is spelled "Reprotecting" in the spec.
  - `ClientOutcomeReported` carries only a `RequestId`, so V-R10's three-way client outcome has
    no field to carry the outcome.
  - `ErrorKind` is documented as "the closed set of spec §5.4 errors" and contains `Unavailable`,
    which §5.4 does not define.
  - `crates/rdb-sim/src/storage/crash_image.rs` module doc and `reopen()` doc contradict each
    other (the prose half of K-F-03).

---

## 2. Verdict

**FAIL** — as the basis for the test planner and for the other three teams' developers.

Not as a judgement on the volume of work, which is large and mostly sound, and not on
determinism, which is clean. The verdict follows from six specific facts:

1. The digest preimage is frozen in two contradictory places and **neither** matches the
   consumer's stated contract (K-F-01, K-F-02). Every team downstream builds on it.
2. Two of the three headline properties cannot fail as the seed stands: a process crash promotes
   buffered to durable (K-F-03), and a deduplicated retry emits nothing (K-F-08). A test planner
   writing rows today would write rows that pass for the wrong reason.
3. A seam the spec requires has no method (K-F-04) and an authority change has no effect
   (K-F-05), so two of the four teams hit a shared-interface change in their first week.
4. The control-plane gate the lead explicitly relocated to the kernel is unassertable (K-F-06).
5. The trace is missing two fields a named consumer specified in writing, and the handoff says
   nothing was pushed back (K-F-07, K-F-33).
6. The capability probe mutates module state and discards effects (K-F-10), which is harmless
   today and poisons every run the day the first module lands — this milestone.

These are defects against stated criteria, not preferences. Preferences are marked ADVISORY and
should not gate anything.

**Cost to clear:** small. Most of the BLOCKERs are one field, one method or one enum variant.
K-F-01/02 need a decision rather than work. This is a FAIL that should take under a day, not a
redesign.

**On over-engineering:** the 4,989 lines are mostly justified — the enum vocabulary is what three
other teams already wrote designs against, and closed enums over reason strings is the right call
(ADR-rdb-0003 decision 5, and rEtcd's M6-118/M6-122 earn it). The genuine excess is small and
specific: the two withdrawn whole-state digests (K-F-24, also the only budget risk under check
(h)), the value bytes on the watch stream (K-F-13, whose removal *buys* a safety property), the
dead length clamp (K-F-37), and the `Copy`/`const fn` decoration on five provider stubs (K-F-29).
Deleting those four things makes the seed both smaller and stronger.

---

## 3. Top five, ranked

1. **K-F-01 + K-F-02 — the record digest.** Two frozen answers, neither matching kernel-b's
   contract, one of them making a version bump corrupt all history. Highest blast radius, lowest
   fix cost, and it blocks the C0 developer who is writing the vectors right now.
2. **K-F-03 — the crash image cannot hold buffered state.** Turns "no lost acknowledged write"
   into a test that cannot fail. False green on the headline property.
3. **K-F-10 — the capability probe mutates modules.** Harmless today, deterministic state
   corruption from the first real module onward, and it will look like a protocol bug.
4. **K-F-04 + K-F-05 — the read seam has no version, the authority has no effect.** Two teams
   hit a lead-only interface change in their first week if these are not settled first.
5. **K-F-06 + K-F-07 + K-F-08 — the trace vocabulary gaps.** Three named invariants are
   unwritable, and the handoff says there are none. The test planner cannot plan around a gap it
   has not been told about.

---

## 4. Skeptical check — objections I withdrew

Recorded because a finding list without them is not evidence of a real pass.

- **`StatusExpired -> Quarantine` looked wrong.** It is right. Spec §5.4 groups `CORRUPT_BLOB`,
  `CORRUPT_HISTORY`, `INCOMPATIBLE_VERSION` and `STATUS_EXPIRED` under "Quarantine/reject;
  operator or rollout action". `retry_rule()` matches the table exactly. **Withdrawn.**
- **`ControlTime::compare` looked asymmetric.** It is not. `DefinitelyBefore` requires
  `estimate + slack < instant` and `DefinitelyAfter` requires `estimate > instant + slack`;
  both are strict, the shapes are mirror images, and saturation fails closed to `Uncertain`.
  **Withdrawn.** K-F-15 is a separate, surviving concern about a missing input, not about this
  logic.
- **"`record_digest` binds a specific transmission" looked like a resend hazard.** Mostly not:
  the digest is computed once by the primary and replicated verbatim, so a resend re-presents the
  same bytes. The residual risk narrows to an F1 rebuild that re-replicates under a new epoch,
  and K-F-02's exclusions cover it. **Downgraded and folded into K-F-02.**
- **4,989 lines looked like over-engineering on its face.** Reading the three consumer designs
  disproved the general form of that objection. Only the four specific items named in §2 survive.
  **Withdrawn as a general claim.**

---

## 5. Questions for the lead, each with a default

1. **Which record-digest preimage is canonical?** (K-F-01, K-F-02)
   *Default if no answer:* kernel-b's — `prev_digest`, `partition_id`, `generation`,
   `owner_epoch`, `seq`, `config_version`, `request_identity`, `request_digest`,
   `conditions_result`, mutations, result; **excluding** `protocol_version` and `lease_id`. It is
   the only one of the three stated with a safety reason attached (gate V12), and the consumer
   that validates the digest should own its content.

2. **Does M7 serve client reads?** (K-F-11, and F-R3/F-R4 depend on it)
   *Default:* yes — add `ReplyEffect::Read`. F-R3 and F-R4 already legislate read service, so
   assuming no would strand two rulings.

3. **Is `AuthorityGeneration` a third newtype, or is kernel-a's design using the wrong name for
   `OwnerEpoch`?** (K-F-18)
   *Default:* kernel-a's design is corrected to `OwnerEpoch`. Adding a third authority-ish
   newtype to a seed already carrying `Generation` and `OwnerEpoch` needs a reason the designs do
   not currently give.

4. **Do `batch_apply.state_digest_after` and `publish.published_state_digest` stay?** (K-F-24)
   *Default:* delete both. Verification withdrew them in writing and they are the only
   O(state)-per-event cost on the trace path.

5. **Who owns the fix for `StepCtx.generation`/`owner_epoch`?** (K-F-05) It touches a contract
   `foundation` owns and a rule `kernel-a` owns.
   *Default:* `foundation` adds `Effect::AdoptAuthority` and `kernel-a` decides when to emit it.
   Leaving the fields in `StepCtx` puts an ADR-rdb-0007 rule in `rdb-sim`, which the charter
   forbids outright.

6. **Should the gate enforce the dependency direction mechanically?** (K-F-32)
   *Default:* yes — a `cargo metadata` check as a new gate stage, before the four teams' parallel
   work lands. It needs no new dependency and the ADR already admits the invariant is otherwise
   unenforced.
