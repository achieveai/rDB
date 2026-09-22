# Team foundation — architect handoff

**Date:** 2026-09-20 · **Role:** architect · **Packages:** C0, H1, M1, I1 (seed only)

---

## 1. Outcome

**COMPLETED.** The seed compiles, is clippy-clean at `-D warnings`, is fmt-clean, and row M7F-01
passes with 3 assertions. Three other teams are unblocked: every seam they consume exists with a
frozen signature, and every unwired body returns an explicit `Unavailable` rather than panicking or
faking success.

43 files, 4,989 lines, all of it contract and documentation. No protocol logic was written — that
belongs to the kernel teams.

## 2. Artifacts

### Code (mine, exclusive)

| Path | What it is |
|---|---|
| `Cargo.toml` | added `crates/rdb-core` and `crates/rdb-sim` as members and as `[workspace.dependencies]` |
| `crates/rdb-core/` | 22 files: `Cargo.toml`, `lib.rs`, `contracts.rs`, 13 contract modules, 6 kernel stubs |
| `crates/rdb-sim/` | 21 files: `Cargo.toml`, `lib.rs`, `error.rs`, `sim/` (5), `storage/` (3 + root), `harness/` (3 + root), `tests/` (4) |

### Documents

| Path | What it is |
|---|---|
| `docs/ADRs/rdb/0000-rdb-adr-series-and-naming.md` | the series, the `ADR-rdb-NNNN` convention, the two names |
| `docs/ADRs/rdb/0002-rdb-crates-and-dependency-direction.md` | crates, the one-way arrow, the fixed dependency set, SHA-256 over BLAKE3 |
| `docs/ADRs/rdb/0003-deterministic-simulation-kernel.md` | the fold, reads-are-pure, the two vocabularies, the reproducer, explicit `Unavailable` |
| `docs/ADRs/rdb/README.md` | index: 0001 Accepted; 0000, 0002–0009, 0019 Proposed |
| `docs/rdb/README.md` | what each doc is; the evidence packets are absent; rEtcd = control plane, rDB = data plane |
| `teams/foundation/design.md` | the committed signatures, what is not built, who consumes what |
| `teams/foundation/research.md` | sources, the BLAKE3 evaluation, rulings folded in, one mistake corrected |

## 3. Criterion → evidence

| Criterion | Evidence |
|---|---|
| C0 — seam contracts exist and compile | `crates/rdb-core/src/contracts/` (13 modules); build succeeds |
| C0 — every seam type documented | `#![deny(missing_docs)]` is on and the crate compiles; no `#[allow(missing_docs)]` anywhere |
| C0 — kernel modules are explicit stubs | the six `src/*.rs` each return `RdbError::unavailable(Capability::X, "package Y is not wired yet")` |
| H1/M1/I1 — signatures frozen | `crates/rdb-sim/src/{sim,storage,harness}` compile; every unimplemented body returns `SimError::Unavailable("<path>")` naming itself |
| I1 — the dispatcher is real | `harness::dispatch::Dispatcher::{new, step, capability_report}` have bodies, not stubs |
| M1 — a real `SnapshotRead` exists | `storage::snapshot::EmptySnapshot` — genuinely empty, not a fake success |
| the registry is frozen | `tests/support/mod.rs` declares `oracle` and `scenarios` and exports `ctx()`, `probe_event()`, `BUDGETS`, `SNAPSHOT`, `ROOT_DIGEST` |
| row M7F-01 | 3 tests pass, output below |
| naming | the banned contraction grep prints nothing, exit 1 |
| no panicking stub | `grep -rn "todo!" crates/rdb-*/src` finds only doc comments explaining why there are none |
| no clock in the kernel | `grep -rn "SystemTime\|Instant::now\|rand::" crates/rdb-core/src` finds only a doc comment |
| no hash iteration order | `grep -rn "HashMap" crates/rdb-*/src` finds only doc comments forbidding it |

## 4. Commands and observed output

```
$ CARGO_TARGET_DIR=.rtargets/foundation CARGO_INCREMENTAL=0 cargo build -p rdb-core -p rdb-sim
   Compiling rdb-core v0.1.0 (C:\Users\gautamb\source\repos\rEtcd\crates\rdb-core)
   Compiling rdb-sim v0.1.0 (C:\Users\gautamb\source\repos\rEtcd\crates\rdb-sim)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 4.27s

$ CARGO_TARGET_DIR=.rtargets/foundation CARGO_INCREMENTAL=0 \
      cargo clippy -p rdb-core -p rdb-sim --all-targets -- -D warnings
    Checking rdb-core v0.1.0 (C:\Users\gautamb\source\repos\rEtcd\crates\rdb-core)
    Checking rdb-sim v0.1.0 (C:\Users\gautamb\source\repos\rEtcd\crates\rdb-sim)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 4.93s

$ cargo fmt --all --check
(no output, exit 0)

$ grep -rIi partdb Cargo.toml crates docs/ADRs/rdb docs/rdb
(no output)
grep exit: 1
```

Row M7F-01. I ran the single test target, not `cargo test --workspace`, with my own target
directory:

```
$ CARGO_TARGET_DIR=.rtargets/foundation CARGO_INCREMENTAL=0 cargo test -p rdb-sim --test harness
     Running tests\harness.rs (.rtargets/foundation\debug\deps\harness-*.exe)

running 3 tests
test m7f_01_unwired_is_definitive_and_not_retryable ... ok
test m7f_01_unavailable_names_the_capability_that_is_missing ... ok
test m7f_01_every_kernel_package_reports_unavailable ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

## 5. Routed items, answered

### 5.1 Verification (trace requirements, V-R9/V-R10/V-R12)

| Ask | Answer |
|---|---|
| 18 event kinds, closed enums everywhere | Done, and it is **21** kinds. All 18 are present; the extras are `Capability`, `ReplicationAckDelivered` and `TopologyChange`. Every outcome, reason, mode and state is a closed Rust enum — 22 of them |
| `peer_role` on every ack | `TraceKind::ReplicationAck.peer_role`, and again on `ReplicationAckDelivered` as the *receiver* resolved it. The disagreement between the two is how a forged role is caught |
| `predecessor_cutoff` on every recovery root | On `TraceKind::LineageRoot` |
| a capability event `Wired \| Unavailable` at trace start | `TraceKind::Capability { package: PackageId, state: CapabilityState }`, one per package |
| V-R9 — `NetworkOp::ForgeAck { claimed_node, claimed_role }` | Added, plus an `authenticated: bool` so a scenario can also model a stolen-but-real credential. No `cfg` branch in kernel code: rejection comes from `PartitionConfig::copy_of` returning `None` for an unauthenticated peer, and from the receiver resolving role from its own pinned config |
| V-R9 — `StorageOp::FalseDurable { node, through }` | Added. `through` is an `AppliedSeq`, because that is what a false flush claims. It produces no `DurableSeq`, so no durable watermark can move |
| V-R10 — `protection_state` on every `config_version` change | Documented on the variant: emitted on every phase transition **and** every `config_version` change, transition or not |
| V-R10 — ack at the secondary, delivery at the primary | Two records: `ReplicationAck` (envelope `node` = the acknowledging secondary) and `ReplicationAckDelivered { ack: EventRef, ... counted: bool }` (envelope `node` = the primary) |
| V-R10 — `ClientOutcome` = Success \| RecoveredApplied \| every §5.4 error | `Success \| RecoveredApplied \| Error(ErrorKind)`. `ErrorKind` *is* the closed §5.4 set — one enum, not a second copy that can drift |
| V-R12 — `topology_change` from the environment | `TraceKind::TopologyChange { config_version, nodes: Vec<(NodeId, ReplicaRole)> }`, documented as emitted by the H1 control provider and never by a kernel module. `TraceHeader::topology` is documented as the initial snapshot only |
| register `oracle` and `scenarios` | Done — see question Q2 for the handshake |
| list ADR-rdb-0019 as Proposed | Done |

**Three asks were deferred at the seed, not delivered.** An earlier revision of this line said
"Nothing was pushed back", which was untrue (K-F-33). The deferrals and their disposition:

| Deferred ask | Where verification asked | Why it was deferred at the seed | Disposition |
|---|---|---|---|
| `ProtectionState.quorum_rule: Rf3 \| DegradedRf2` | trace-requirements §3.14 | I read the rule as derivable from `qualified_copies` and the phase; it is not — a lone-survivor row needs the rule the kernel *applied*, not what an oracle can infer | **WITHDRAWN (R2, ruling F-R13).** It did not land and must not. Verification's own later ruling V-R20 had the oracle derive the rule from `required_copy_set.len()`; F-R13 adjudicated K-F-07 that way and closed it **by derivation, not by a field**. Kernel-b's L1 emits no `quorum_rule`. See design §4.10 and `trace.rs:620–630`, `:1026–1042` |
| `op_skipped { scenario_op_index, reason }` | trace-requirements §3.16a | I treated a reducer skip as a harness log line, not a trace kind; verification needs it in the stream so a reduced trace is self-describing | **Lands in correction round 1** (K-F-08), `TraceKind::OpSkipped` |
| `provenance: Generated \| Reduced \| Authored` on the header | trace-requirements §1 | I carried a bare `seed`, which verification explicitly refused because a reduced or authored trace has no generating seed | **Lands in correction round 1** (K-F-09), `TraceHeader.provenance` |

No requested field forces kernel internals into the trace.

### 5.2 kernel-b (5 items)

1. **`record_digest` chains `prev_digest`.** Confirmed and frozen. The commitment is
   under `Domain::Record`, `prev_digest` **first**; design.md §4.8 has the exact order. Equal
   digest at equal seq implies equal prefix. *Seed text said "seven parts" and the seed code bound
   thirteen; both are superseded by ruling F-R6 in correction round 1 — eleven parts, kernel-b
   §1.1's list, `protocol_version` and `lease_id` excluded. See §10 below.* The known-answer
   vectors landed at `8a23b1d` (M7F-02) and are re-pinned in round 1.
2. **Buffered and durable are separate values.** Confirmed, and stronger than asked: they are
   separate *types*. `MemoryEngine::buffered_applied -> AppliedSeq`, `durable -> DurableSeq`, plus
   `received -> ReceivedSeq`.
3. **`authenticated_peer -> copy_id` lives in `rdb-core`.** Confirmed:
   `contracts::membership::PartitionConfig::copy_of(&PeerLabel) -> Option<&Member>`, in the pinned
   configuration type. Nothing in `rdb-sim` maps identity to a copy.
4. **`DurableProof`.** **Superseded by ruling B-R13, which I implemented.** `DurableProof` is
   deleted. `ReceivedSeq`, `AppliedSeq` and `DurableSeq` are plain public `u64` newtypes in
   `contracts::ids` with **no conversion between them** — no `From`, no `into_seq`. The M1 storage
   seam returns `DurableSeq` for the durable prefix and `AppliedSeq` for the buffered-applied one;
   `CapturedPrefix.through` is an `AppliedSeq`, the new `DurablePrefix.through` is a `DurableSeq`,
   and `StorageEvent::Flushed` carries `Vec<DurablePrefix>`. That is the whole mechanism. No sealed
   trait, no doc-hidden constructor, no proof object.
5. **README lists 0004–0009 and 0019 as Proposed.** Done, with an owner table.

### 5.3 kernel-a — ADR-rdb-0008 §7, as amended by A-R15

| Item | Coverage |
|---|---|
| 1. conflict hides the value | `CasOutcome::Conflict { exists, current }` — no value field exists, so a loser must follow with a linearizable read |
| 2. `Unknown` distinct from `Unavailable` | Three separate variants; `ControlOp::PlanCas` forces any of them. `Unknown` cannot be derived from the store's state, which is what makes it unknown |
| 3. the five typed watch terminations | All five on `WatchTermination`, plus `ControlEvent::WatchProgress`. `is_gap()` answers "did I miss something", and `ResourceExhaustedFatal` deliberately returns false — reloading on it turns a capacity error into an outage |
| 4. no silent gap | **Now a kernel-side assertion (A-R15), not a fake property.** `ControlOp::EmitWatch` delivers contiguously and a gap is only ever a termination, so "the kernel reloaded without being told to" is something the oracle asserts against the trace. The fake does not try to enforce it |
| 5. `Unavailable` inside a generous deadline | **Deleted (A-R15).** `ReadOutcome::Unavailable` remains in the contract because rEtcd really returns it (ADR-0009's server-side read timeout), but there is no longer an injection method for it. See question Q3 |
| 6. resumable `snapshot_revision` | `ControlStore::snapshot_family` and `ControlEvent::FamilySnapshot { family, snapshot_revision, records }` |
| 7. **new** — a completion delivered after the grant expired | `ControlOp::DelayCompletion { node, by_millis }` |
| 8. **new** — a control effect that never completes | `ControlOp::DropCompletion { node }`, deliberately distinct from `Unknown`: there the caller is *told* nothing is known; here it is told nothing at all and must still make progress off its own deadline |

**`ProcessResumed` confirmed.** `EventKind::Node(NodeLifecycle::Resumed { suspended_millis })`,
delivered by `Cluster::suspend`. It is a sixth event source, added for exactly this: a module is
not allowed to read a clock and notice a jump, so this event is the only way it learns it was
stopped.

### 5.4 Lead rulings, confirmed

| Ruling | Confirmation |
|---|---|
| **A-R17** — step returns `Vec<Effect>`, no wrapper | Done. `fn step(&mut self, ctx: &StepCtx<'_>, event: &Event) -> Result<Vec<Effect>, RdbError>`. The `Effects` newtype is deleted, and so is its re-export |
| **A-R15** — §7 items 4 and 5 amended, 7 and 8 added | Done; see 5.3 |
| **B-R13** — three watermark newtypes | Done; see 5.2 item 4 |
| **V-R12** — topology is an environment-owned event | Done; see 5.1 |
| **V-R1** — no proptest | Neither crate depends on it |
| **V-R7** — `ADR-rdb-NNNN` in prose | Used throughout |
| **USER DECISION** — the name is rDB | Done; grep clean |

### 5.5 Topology breadth (coordinator item 3)

`ClusterConfig` holds `partitions: Vec<PartitionSpec>` and fixes nothing — a core set may own many
partitions, and the envelope already carries `partition`. So "two partitions in M7" is a generator
and topology decision, **not** a contract change, and nothing in `rdb-core` needs to move. Team
verification's `scenarios` is where a `partitions: u8` breadth knob belongs; the doc comment on
`tests/support/scenarios.rs` says so.

## 6. Assumptions

1. **Row ids `M7F-NN` are mine** and map one-to-one to acceptance bullets, per ADR-0014. M7F-01 is
   taken; M7F-02 onwards are listed in design.md §8 with their owners.
2. **`Budgets::SPEC_DEFAULTS` are the spec's stated numbers**, not measured ones. Spike §7 is
   explicit that its targets are proposed, not observed.
3. **`Tick` is 1 ms** (`TICK_MILLIS = 1`). Every spec threshold is stated in milliseconds, so this
   makes a budget and a tick count the same number.
4. **All four mandatory versions start at 1.** A bump is a deliberate act that invalidates fixtures.
5. **`bytes::Bytes` in the public API is acceptable**, following the control plane's existing
   exception under ADR-0004.
6. **`rdb-sim` takes `config-log` + `config-log-macros` as dev-dependencies only**, for JSONL test
   logging via `#[retcd_test]`. `rdb-core` takes them as dev-dependencies too. No `config-*` crate
   was touched.

## 7. Questions, with the default I took

**Q1 — `config-testkit` as a dev-dependency of `rdb-sim` (ruling V-R5).**
*Default taken: deferred.* `config-testkit` pulls `config-storage` (RocksDB), `config-engine`
(OpenRaft), `config-grpc` (tonic/protox), `rcgen` and `tokio-rustls`. All cargo caches were purged
before this work, so adding it turns every `cargo test -p rdb-sim` into a long cold build for four
teams simultaneously, and risks the ADR-0017 `LIBCLANG_PATH` failure blocking seed verification
outright. Nothing in the seed calls `write_evidence` yet. **Recommendation:** add it the day
package Q1 actually writes evidence, as a one-line change, and pay the build cost once at that
point rather than now for four teams. Override me and I will add it.

**Q2 — the `oracle` / `scenarios` registration handshake.**
A live `pub mod oracle;` cannot compile before the file exists, so I created
`tests/support/oracle.rs` and `tests/support/scenarios.rs` at spike §3's exact paths as documented
but otherwise **empty module roots**. *From now on their contents belong to team verification.* I
will not touch them again. If verification would rather own the `pub mod` lines too, say so and I
will remove them — but then the registry does not compile until O1 lands.

**Q3 — `ReadOutcome::Unavailable` has no injection method any more.**
A-R15 deleted §7 item 5, so I removed `ControlStore::plan_read_outcome`. The *contract* variant
stays, because rEtcd genuinely returns it (ADR-0009 bounds the read barrier by the server's own
timeout, independent of the caller's deadline). *Default taken: no injection method.* If
verification wants a scenario that produces a read-unavailable, it is one variant on `ControlOp`.

**Q4 — `trace::ReadOutcome` and `control::ReadOutcome` are two different types with one name.**
They are genuinely different things: one is what the control store said, the other is how a data
read was served (`Served | WaitedAtBarrier | Rejected`). *Default taken: leave both.* Rust's module
paths keep them apart and neither name is wrong in its own module. If a kernel team finds the
collision confusing in practice, renaming the trace one to `ReadServiceOutcome` is cheap now and
expensive after O1 is written. *Ruling F-R5 took the rename: the trace type is
`ReadServiceOutcome` from correction round 1 on (design §4.10).*

## 8. Risks

**R1 — the `record_digest` known-answer vector is owed, not delivered.** `compute_record_digest`,
`encode`, `decode` and `decode_header` are the four `Capability::Codec` stubs. The chaining *order*
is frozen in design.md §4.8 and ADR-rdb-0002, so kernel-b can write against it today, but nothing
proves the implementation matches the commitment until C0 lands the body and the two-entry
flip-a-byte vector. *Mitigation:* row M7F-02 exists and is the next thing C0 should do.
*Closed at `8a23b1d`: the four codec bodies and rows M7F-02..04 pass. The preimage they pinned is
itself superseded by F-R6 in round 1; the goldens move once more.*

**R2 — purity is a convention, not a compiler guarantee.** Nothing stops a kernel module from
calling `SystemTime::now()`. It is caught by review, by the fixed dependency set, and — reliably —
by replay equality, which a wall clock breaks immediately. But replay is package I1 and is not
wired yet, so for now the only guard is review.

**R3 — the trace schema will move.** 21 kinds and 22 closed enums were frozen before any protocol
code exists. Some will be wrong. `TRACE_SCHEMA_VERSION` exists so a bump deliberately invalidates
fixtures; the cost of a mid-milestone change is real and lands on team verification.

**R4 — the forbidden dependency direction is not mechanically enforced.** Cargo would accept a
`config-* -> rdb-*` edge. It is checked by review and by ADR-rdb-0002's crate list. If the lead
wants a guarantee, a small `cargo metadata` check in the gate would give one. *Taken: ruling
F-R11 adds the `deps` gate stage in correction round 1 (K-F-32); ADR-rdb-0002 decision 2 and its
Consequences now say so.*

**R5 — 4,989 lines of contract before one line of protocol.** That is the deliberate trade: four
teams working in parallel need frozen seams more than they need a small surface. If a seam turns
out wrong, six teams pay for the change rather than one. The rulings folded in during this session
(A-R15, A-R17, B-R13, V-R9/10/12) are evidence the process is catching those early.

## 9. Recommended next role

**Implementer on C0**, immediately, for the four `Capability::Codec` stubs and rows M7F-02 to
M7F-04: `compute_record_digest` with the two-entry chain vector, `ControlKey::encode` vectors, and
envelope encode/decode with unknown-version refusal. These are pure functions with known answers,
they unblock kernel-b's ancestry work, and they are the only part of the seed where a commitment
currently outruns an implementation.

After that, **implementer on H1** (scheduler first — the `(tick, event_id)` total order is what
every other row rests on), then M1, then I1.

---

## 10. Correction round 1 — document side

**Date:** 2026-09-20 · **Against:** `critic-design.md` (K-F-01..38, verdict FAIL) · **Under:** rulings
F-R5 (leftovers), F-R6..F-R12, B-R23/QC-14, V-R19 · **Seed reviewed:** `8a23b1d`

**Split.** I closed the document side: `design.md`, ADR-rdb-0000, ADR-rdb-0002, ADR-rdb-0003 and
this handoff. `dev-foundation-r1` closes the code side (`crates/**`, `scripts/gate.*`) in parallel
under the same rulings. I did not edit `crates/**` or `scripts/**`. Where I chose a concrete
shape the critic left open (a type name, a method name, a gate stage name), it is listed in
§10.6 so the lead can reconcile it against what the developer built. The critic verifies design
against code; a divergence between the two is a naming reconciliation, not a reopened finding.

### 10.1 Finding -> change -> where -> how the critic verifies

Severity in the critic's words: B = BLOCKER, M = MATERIAL, A = ADVISORY.

| # | Sev | Change (document side) | Where | Critic verifies by |
|---|---|---|---|---|
| K-F-01 | B | one preimage, stated once: F-R6's 11 parts (`prev_digest, partition, generation, owner_epoch, seq, config_version, request_identity, request_digest, conditions_result, mutations, result`); exclusion table for `protocol_version`, `lease_id`, `body_len`, `record_digest`; rustdoc is to cite §4.8 and not restate | design §4.8 | grep `prev_digest` in design.md: exactly one preimage table; ADR-rdb-0002 no longer states a part count |
| K-F-02 | B | `protocol_version` and `lease_id` excluded, with the reason each (V12 says a version bump must not fork history; B-R20 says the lease is authority, not content); three new vectors in M7F-02 (same digest across `protocol_version`, across `lease_id`; different `partition` => different digest); goldens re-pinned, `ENVELOPE_VERSION` stays 1 | design §4.8, §8 row M7F-02 | design §4.8 exclusion table; developer's `contracts.rs` vectors |
| K-F-03 | B | `SurvivingPrefix { partition, generation, durable: DurableSeq, applied: AppliedSeq }`, `CrashImage { surviving: Vec<SurvivingPrefix> }`; process crash keeps `applied`, host crash sets `applied == durable`; row M7F-06 restated | design §5 (M1), §8; ADR-rdb-0003 Consequences | design §5 `CrashImage` block; ADR 0003 last Consequences bullet |
| K-F-04 | B | `SnapshotRead::version(&self, ns, key) -> Option<Version>`; row M7F-08 | design §4.3, §8; ADR-rdb-0003 decision 2 | design §4.3 trait listing |
| K-F-05 | B | F-R10: `EffectKind::AdoptAuthority { generation, owner_epoch, config_version }` (sixth variant, no completion event); dispatcher stores last adopted per partition and fills `StepCtx` mechanically; zero triple before first adoption; no authority rule in `rdb-sim`; row M7F-09 | design §4.1, §5 (I1), §8; ADR-rdb-0003 **new decision 8** | design §4.1 `EffectKind` listing and the "authority is adopted by effect" paragraph |
| K-F-06 | B | `TraceKind::ControlInteraction { op: ControlOpKind, key, prefix, outcome: ControlOutcomeKind }` (environment-owned, emitted by H1) and `TraceKind::FamilyReload { prefix, snapshot_revision, after_termination: Option<EventRef> }` (kernel-emitted); `ControlOutcomeKind::Terminated { termination, gap }`; the A-R15 assertion is now two trace events; row M7F-10 | design §4.10, §8; ADR-rdb-0003 decision 5 | design §4.10 kind list (24 kinds) |
| K-F-07 | M | ~~`ProtectionState.quorum_rule: QuorumRule { Rf3, DegradedRf2 }`~~ — **WITHDRAWN (R2, ruling F-R13)**, closed by derivation instead: no field, oracle derives from `required_copy_set.len()`. `QuorumRule` stays as the oracle's vocabulary. Kernel-b's L1 emits nothing here | design §4.10 (withdrawal recorded); this handoff §5.1 | `grep -rn quorum_rule crates/` returns nothing; design §4.10's bullet reads as a withdrawal |
| K-F-08 | M | `TraceKind::OpSkipped { scenario_op_index: u32, reason: SkipReason { ReferentGone, OutOfBudget } }`; never a fault boundary | design §4.10; ADR-rdb-0003 decision 6; §5.1 | design §4.10 |
| K-F-09 | M | `TraceHeader { schema_version, generator_version, provenance: Provenance, config: RunManifest, partitions: u8, topology, oracle_checkpoint_digest }`; `Provenance { Generated { seed }, Reduced { parent: ScenarioId }, Authored { case } }`; bare `seed`, `config_digest`, `budgets` removed; `deny_unknown_fields`; row M7F-11 | design §4.10, §8; ADR-rdb-0003 decision 6; §5.1 | design §4.10 header block |
| K-F-10 | B | `Module::capability(&self) -> CapabilityState` non-mutating, answered from a declared constant; `Dispatcher::capability_report(&self)`; "a question, not a probe"; M7F-01 restated | design §4.1, §5 (I1), §8; ADR-rdb-0003 decisions 1 and 7 | design §4.1 trait listing |
| K-F-11 | B | F-R7: `ReplyEffect::Read { identity, outcome: ReadServiceOutcome, value: Option<(Version, Digest)> }`; reads are served in M7 | design §4.1 | design §4.1 `ReplyEffect` listing |
| K-F-12 | M | `ControlStore::snapshot_family(prefix) -> Result<(Revision, Vec<ControlRecord>), SimError>`; `FamilySnapshot { prefix, snapshot_revision, records }`; row M7F-12 | design §4.4, §5 (H1), §8 | design §5 `ControlStore` listing |
| K-F-13 | M | `ControlChange { key, revision }` — no value on the watch stream; new `ControlRecord { key, revision, value }` for snapshots/reloads only | design §4.4 | design §4.4 `ControlChange` and `ControlRecord` |
| K-F-14 | M | `ControlStore::submit(node, ControlEffect)` + `complete(now) -> Vec<(NodeId, Tick, ControlEvent)>` (async); `ControlOp::PlanCas { node, outcome }`; row M7F-13 two-node race | design §5 (H1), §8 | design §5 `ControlStore` and `ControlOp` listings |
| K-F-15 | M | `ControlTime.sampled_at: Tick`; `is_stale(now, max_age_millis)`; `compare(now, max_age_millis, instant, margin_millis)` returns `Uncertain` when stale; row M7F-14 | design §4.2, §8 | design §4.2 |
| K-F-16 | A | **left open**, stated as open in the design: `estimate` stays `Tick` this round; the developer may retype without a design change | design §4.2 | design §4.2 last paragraph |
| K-F-17 | M | already closed at `8a23b1d` (`compute_request_digest`, A-R18 vectors, M7F-02); design §4.8 states request digest unchanged by F-R6 | design §4.8 | `8a23b1d` |
| K-F-18 | B | F-R8: `AuthorityGeneration(pub u64)` third newtype in `contracts::ids`, no conversions | design §4.3 | design §4.3 newtype list |
| K-F-19 | M | `ControlPrefix { ClusterSchema, Nodes, Grants, Partitions, Routes, Operations, PlannerGrant }` with `encode()`/`contains(key)`; `ControlEffect::Watch { prefix, from }`, `Reload { prefix }`; `Watched`/`WatchProgress`/`WatchTerminated` carry `prefix` | design §4.4 | design §4.4 `ControlPrefix` block |
| K-F-20 | M | `ReadOutcome::Absent { as_of: Revision }`; row M7F-15 | design §4.4, §8 | design §4.4 |
| K-F-21 | M | `Member.boot: BootId` non-optional, sourced from `nodes/{id}` boot UUID / `TopologyChange`; `copy_of` matches node **and** boot; fails closed; row M7F-16 | design §4.6, §8 | design §4.6 `Member` |
| K-F-22 | M | `AckEvidence { node, boot: BootId, role, durability }` | design §4.10 | design §4.10 |
| K-F-23 | M | `required_regular()` = regular secondaries only; new `primary()`; row M7F-17 (count 2 on RF3, 0 on lone survivor) | design §4.6, §8 | design §4.6 |
| K-F-24 | M | F-R9: `BatchApply.state_digest_after` and `Publish.published_state_digest` deleted; closed-set list updated | design §4.10; ADR-rdb-0003 decision 5 | grep `state_digest_after` in design.md: only the deletion note |
| K-F-25 | M | **partly disputed, closed anyway** — see §10.2. Rule stated: the achieved prefix in `StorageEvent::Flushed { durable }` is the truth, `Flush.captured` is the request; new `StorageOp::ShortFlush { node, through: AppliedSeq }`; row M7F-18 | design §4.3, §5 (M1), §8 | design §4.3 "achieved prefix is the truth" paragraph |
| K-F-26 | M | `proves_no_mutation()` false for `NotWired`; M7F-01 asserts "no effect returned" instead | design §4.7, §8; ADR-rdb-0003 decision 7 | design §4.7 |
| K-F-27 | M | `RunManifest { budgets, overridden: Vec<BudgetName>, nodes, event_cap }` in `rdb-core` as plain data; `BudgetName` 10 members; `harness::manifest::resolve(config, overrides) -> RunManifest`; header carries it; row M7F-19 | design §4.10, §5 (I1), §8; ADR-rdb-0003 decision 6 | design §4.10 `RunManifest` |
| K-F-28 | M | dispatcher is an infallible `match` on `ModuleName`; the vacuous "unregistered handler fails explicitly" claim is deleted and the I1 row restated as M7F-01's "no effect returned, never panics" | design §5 (I1), §8 | design §5 dispatcher paragraph |
| K-F-29 | M | provider stubs are stateful structs, no `Copy`, no `const fn` | design §5; ADR-rdb-0003 Consequences | design §5 provider listings |
| K-F-30 | M | `tests/harness.rs` uses `#[retcd_test]`; one JSONL file per test; row M7F-22 + DuckDB Q-F-1 | design §1, §8; ADR-rdb-0003 Verification | design §1 |
| K-F-31 | M | all three greps replaced with the comment-excluding form and the observed result (exit 1; unfiltered hits are doc comments) | ADR-rdb-0002 Verification; ADR-rdb-0003 Verification | run the three commands in the ADRs; each prints nothing, exit 1 (observed 2026-09-20) |
| K-F-32 | M | F-R11: `deps` gate stage over `cargo metadata` (`config-*` must not depend on `rdb-*`, any kind); ADR decision 2 states it, Consequences corrected from "convention, checked by review"; R4 marked taken | ADR-rdb-0002 decision 2, Consequences, Verification; this handoff §8 R4 | ADR text; script is the developer's |
| K-F-33 | M | §5.1 "Nothing was pushed back" replaced with the three-row deferral table; all three land this round | this handoff §5.1 | read §5.1 |
| K-F-34 | A | **left open**: `AppendReject` generation/quarantine variants are kernel-b's to confirm; not added on speculation | — | listed in §10.4 |
| K-F-35 | A | already closed at `8a23b1d` (stale `WatchGap` doc reference) | — | `8a23b1d` |
| K-F-36 | A | **left open, routed**: `NotLeader` carries no hint, deliberately; the hint is ADR-rdb-0008 §4's and belongs to kernel-a's authority view, stated in design | design §4.4 | design §4.4 K-F-36 paragraph |
| K-F-37 | A | length prefix stays `u64` LE; the clamp is dead code and is noted as such for the developer to remove | design §4.7 | design §4.7 |
| K-F-38 | A | see §10.3 bullet by bullet | various | — |

### 10.2 Disputes (with counterevidence), for the lead

**K-F-25 — "`StoreEffect::Flush` makes the kernel capture the prefix" — partly disputed.**
The critic's false-positive check says the flush completion carries no achieved prefix. That is
not what the seed had: `StorageEvent::Flushed { durable: Vec<DurablePrefix> }` already carried
the achieved `DurableSeq` per partition, distinct from the kernel's `Flush.captured` (an
`AppliedSeq`), since ruling B-R13 (this handoff §5.2 item 4). So the *type* side of the finding
was already true. What was genuinely missing, and what I closed: (a) the stated rule that the
achieved prefix is the truth and `captured` is only the request, so a kernel that trusts its
own `captured` is wrong by contract; and (b) an injection op that makes the two differ
(`StorageOp::ShortFlush`), without which no row could show a kernel reading the wrong one.
Disposition I recommend: **sustained in part, closed**; the severity stands because (a) and (b)
were real.

**K-F-38, bullet "`ClientOutcomeReported` carries only a `RequestId`" — withdrawn on evidence.**
`contracts/trace.rs` at `8a23b1d` has `ClientOutcomeReported { identity, outcome: ClientOutcome,
generation, seq, result_digest, delivered }`; `ClientOutcome` is `Success | RecoveredApplied |
Error(ErrorKind)` (this handoff §5.1, V-R10 row). The three-way outcome has its field. No change
made for this bullet. Disposition I recommend: **withdrawn**.

Everything else closed as the critic wrote (F-R12).

### 10.3 K-F-38 bullets, one line each

| Bullet | Disposition |
|---|---|
| `.expect("ModuleName::ALL is exhaustive")` only panic path | closed with K-F-28: infallible `match`, no `expect` |
| `rdb-core` dev-depends on `config-*` while ADR 0002 says only `rdb-sim` | ADR-rdb-0002 decision 2 corrected: both crates, dev-only |
| `hex` dev-dependency vs design §4.7 | not a contradiction: design §4.7's `Digest::to_hex` is the production-side encoder; the `hex` dev-dependency *decodes* golden strings in `contracts.rs`, which `to_hex` cannot. ADR-rdb-0002 decision 2 now names it, dev-only, so the ADR and `Cargo.toml` agree |
| `TopologyEntry` derived `Ord` field order | developer's; design states `(partition, node)` as the order and the developer aligns the field order |
| `CasOutcome::Conflict { exists, current }` | left as is this round; an enum is a kernel-a-facing signature change and kernel-a already consumes the struct form; listed in §10.4 |
| `Resuming` vs spec "Reprotecting" | design §4.10 states the equivalence; the trace name stays (a rename bumps `TRACE_SCHEMA_VERSION` for a synonym) |
| `ClientOutcomeReported` only a `RequestId` | **withdrawn** (§10.2) |
| `ErrorKind` "closed §5.4 set" contains `Unavailable` | design §4.7 reworded: "the 17 §5.4 names plus `Unavailable`"; nowhere else claims "exactly §5.4" |
| `crash_image.rs` module doc vs `reopen()` doc | design §5 states one rule; the developer's doc edit follows it (K-F-03) |

### 10.4 Advisories left open, and who owns them

| Finding | Owner | Why open |
|---|---|---|
| K-F-16 `estimate: Tick` | developer | typing choice with no consumer impact; may change without a design edit |
| K-F-34 `AppendReject` variants | kernel-b | speculative without kernel-b's ladder; add when they name the variant |
| K-F-36 `NotLeader` hint | kernel-a | ADR-rdb-0008 §4's hint belongs to the authority view, not the termination |
| K-F-38 `CasOutcome::Conflict` enum | kernel-a + developer | signature kernel-a already consumes; change together or not at all |
| K-F-38 `TopologyEntry` field order | developer | code-only |

### 10.5 For verification — the closed `BoundaryId` set (V-R19)

29 members, in the order `contracts::trace` declares them. The planner asserts set equality
against this list; a member added or removed is a `TRACE_SCHEMA_VERSION` bump.

```
ChangedDigest, OldGeneration, LostSuccessReply, RetainedDedupHit, ExpiredDedup,
StaleBoot, StaleEpoch, StaleConfig, MissingPredecessor, AckAfterRevocation,
ForgedIdentity, SameTickOrder, GrantSkewWithinBound, GrantSkewOutsideBound,
DedupWindowJump, BeforeAtomicCommit, AfterAtomicCommit, BeforeFlush, AfterFlush,
FalseDurableWatermark, StaleSnapshot, LostControlQuorum, InvalidGrant,
PartialStagedMetadata, WatchGap, UnequalSecondaryPrefix, LoneSurvivorChoice,
Divergence, ReturningStaleOwner
```

`OpSkipped` is a trace kind, not a boundary (K-F-08); it is deliberately absent from this set.

### 10.6 For the test planner — rows this round names

Full statements are in design §8. Package in brackets; **(R1)** = new or restated this round.

| Row | One line | Status at `8a23b1d` |
|---|---|---|
| M7F-01 (R1) [I1] | every package `Unavailable` from `capability(&self)`; stepping returns `Unavailable`, no effect, no panic | passes; restated |
| M7F-02 (R1) [C0] | record digest vectors incl. `protocol_version`/`lease_id` invariance and partition binding; request digest A-R18 | passes; three vectors flip, goldens re-pinned |
| M7F-03 [C0] | `ControlKey` encode/decode vectors | passes |
| M7F-04 [C0] | envelope round trip, unknown version refused | passes |
| M7F-05 [H1] | `(tick, event_id)` total; byte-identical replay | owed |
| M7F-06 (R1) [M1] | process vs host crash on one image, `SurvivingPrefix` | owed |
| M7F-07 [M1] | `FalseDurable` moves no durable watermark | owed |
| M7F-08 (R1) [M1] | `SnapshotRead::version` | owed |
| M7F-09 (R1) [I1] | `AdoptAuthority` fills `StepCtx`; zero triple before | owed |
| M7F-10 (R1) [H1] | `ControlInteraction` per completion; `FamilyReload.after_termination` | owed |
| M7F-11 (R1) [I1] | header with each `Provenance` + manifest round-trips; unknown field refused | owed |
| M7F-12 (R1) [H1] | `snapshot_family` stable at one revision across a CAS | owed |
| M7F-13 (R1) [H1] | two-node CAS race with `PlanCas { node }` + `DelayCompletion` past grant | owed |
| M7F-14 (R1) [C0] | stale `ControlTime` sample => `Uncertain` | owed |
| M7F-15 (R1) [H1] | CAS on a stale `Absent { as_of }` is `Conflict` | owed |
| M7F-16 (R1) [C0] | `copy_of` refuses a reincarnated peer (boot differs) | owed |
| M7F-17 (R1) [C0] | `required_regular()` cardinality 2 / 0; `primary()` | owed |
| M7F-18 (R1) [M1] | `ShortFlush` lands `durable` short of `captured` | owed |
| M7F-19 (R1) [I1] | `manifest::resolve` records exactly the overridden budgets | owed |
| M7F-20 (R1) [H1] | `PlanReadUnavailable` affects one `Get` | owed |
| M7F-21 (R1) [I1] | effect-to-event hop is zero ticks; `DelayCompletion { 50 }` lands at *t + 50* | owed |
| M7F-22 (R1) [I1] | one JSONL file per test, DuckDB-readable | owed |
| Q-F-1 (R1) | DuckDB: every `rdb-sim` test file has exactly three `Capability` events at trace start | owed |

### 10.7 Names I chose that the developer may have chosen differently

The critic's closures name the fix, not the identifier. These are mine; if the developer's
differ, the lead picks one and the loser edits. None changes a consumer-facing seam another team
has already written against, except where marked.

| Where | My name | Consumer impact |
|---|---|---|
| gate stage | `deps` (`scripts/gate.sh deps`, `gate.ps1 deps`) | none |
| control store async pair | `ControlStore::submit(node, effect)` / `complete(now)` | H1-internal |
| control prefix enum | `ControlPrefix` (7 members) + `encode()`/`contains()` | **kernel-a uses `ControlPrefix` by that name already** — keep it |
| reload/snapshot record | `ControlRecord { key, revision, value }` | kernel-a's `ReadOk { record, .. }` — keep it |
| trace enums | `ControlOpKind`, `ControlOutcomeKind`, `QuorumRule`, `SkipReason`, `BudgetName` | verification's oracle; names are mine, member sets are verification's |
| manifest | `RunManifest` in `rdb-core::contracts::trace`; `harness::manifest::resolve` | I1-internal |
| short flush | `StorageOp::ShortFlush { node, through }` | M1/H1 scenario op |
| surviving prefix | `SurvivingPrefix` | M1-internal |

### 10.8 Residual risks after this round

1. **Parallel edits, one truth.** Design and code were corrected by two people at once against
   one finding list. The critic's re-check is the reconciliation; expect naming diffs (§10.7),
   not semantic ones.
2. **Goldens move twice.** M7F-02's hex goldens were pinned at `8a23b1d` against the 13-part
   preimage and are re-pinned against F-R6's 11 parts. Kernel-b's ancestry vectors, if any were
   written against the seed goldens, must be re-derived.
3. **ADR-rdb-0003 grew from 7 decisions to 9** (`AdoptAuthority`, hop budget). Both are lead
   rulings, not new architecture, but a reader of the Proposed ADR sees a larger surface than the
   seed's.
4. **ADR-rdb-0000 unchanged.** Re-read; nothing in the round touches it. Its Verification still
   points at this handoff for the banned-contraction command, which is spelled only in §4 above,
   outside the grep paths.
5. **R2 (purity is a convention) stands**; replay (M7F-05) is still owed.

### 10.9 Recommended status

**REVIEW** — document side closed for all 10 BLOCKERs and all MATERIALs (one partial dispute,
one withdrawn bullet, both argued in §10.2), ADVISORY K-F-16/34/36 and two K-F-38 bullets left
open by design and listed. Ready for the critic's re-check once `dev-foundation-r1` hands off
the code side.

---

## Round 2 reconciliation (architect, 2026-09-20)

**Scope, as assigned:** `design.md` §4.1, §4.6, §4.8, §4.10, §5, §7 only, plus the two named
fixes. Reconciliation against the code at `6893442` — no new decision, no new shape, no new rule.
Where document and code disagreed, the code won and the document changed. Files touched: this one,
`design.md`, `docs/ADRs/rdb/0003-deterministic-simulation-kernel.md`. `crates/` was read for
evidence only; nothing there was written.

### R2.1 What changed, and the line it was read from

| # | Finding | Change | Code evidence |
|---|---|---|---|
| 1 | K-F-41 | **New `design.md` §4.11, `contracts::authority`** — the whole module, written from the code: `Checkpoint`, `Lineage`, `DenyReason` (15), `Verdict`, `AuthorityDecision` + its three methods, `AuthorityView`, `EvidenceRef`, `BlockReason`, `PartitionMode`. Four rules restated, each from a rustdoc, not from me | `crates/rdb-core/src/contracts/authority.rs:1-224`; exported at `lib.rs:47-50` |
| 2 | K-F-41 | **§4.1 `EventKind` is seven variants**, `ExternalFenceVerified { partition, prior_generation, prior_owner_epoch, prior_boot_id, control_revision, evidence }` added with the "never synthesised from unreachability" rule, and the `Node(NodeLifecycle)` "six sources" annotation corrected. A total `match` written from the old six does not compile | `contracts/event.rs:152-186` |
| 3 | A-R23 | **§4.1 `EffectKind::AdoptAuthority` gains `partition`**, and the "authority is adopted by effect" paragraph names it | `contracts/event.rs:268-278` |
| 4 | A-R23 | **§4.10** notes `TraceKind::AuthorityDecision.authority_seq` and that freshness is that counter, not `decision_tick` | `contracts/trace.rs:753-775` |
| 5 | **K-F-40** | **§4.10's `quorum_rule` bullet is now a withdrawal**, citing F-R13, naming the seven fields `ProtectionState` actually carries, and saying the oracle derives the rule from `required_copy_set.len()`. Kept as a withdrawal, not deleted, so a developer arriving from an older reference sees why. §4.10's "three fields moved" lead-in corrected to "two of the three landed" | `contracts/trace.rs:620-630` (the `QuorumRule` rustdoc says why it is not a trace field), `:1026-1042` (the landed field list) |
| 6 | **K-F-40** | **This handoff §5.1 and §10.1 K-F-07 rows marked WITHDRAWN** with the ruling and the observable | `grep -rn quorum_rule crates/` returns nothing |
| 7 | K-F-43 | **§4.8 `AppendReject` restated at 16 variants** in ladder order, with the row numbers. `DigestMismatch` and `IncompatibleConfig` did not exist and are gone from the document; they are `CorruptHistory { at }` and `StaleConfig`/`NeedConfig` | `contracts/envelope.rs:524-590` |
| 8 | Lead ruling, R2 | **§4.8 states that `Busy` and `AlreadyHave` are deliberately absent** — non-reject outcomes in kernel-b's step-8 table, to be added additively when R1 lands (with `ProbeDigestAt`, B-R33). No variant was added | — |
| 9 | K-F-43 | **§4.6 `PartitionConfig` gains `min_regular_acks: u8`**, `DEFAULT_MIN_REGULAR_ACKS`, `new`, `with_min_regular_acks`, `validate`, and a paragraph on why zero is refused | `contracts/membership.rs:53-113` |
| 10 | K-F-43 | **§5 provider sentence corrected**: `Copy` is gone from all five providers; `const fn` survives on four single-field accessors and is harmless. The seed's flat "neither `Copy` nor `const fn`" was false | `sim/scheduler.rs:51`, `sim/clock.rs:65`, `sim/control.rs:194`, `sim/cluster.rs:166` |
| 11 | K-F-43 | **§5 `PowerLoss` removed.** `StorageFault` is `WriteFailed, FlushFailed, ProcessCrash, HostCrash, Corrupt`; `HostCrash` is the whole-host case the sentence named twice | `contracts/storage.rs:142-154` |
| 12 | K-F-43, same class | **§5 method inventory** matches the code: `Scheduler::pop` (not `next`), plus `next_tick`; `ControlStore::submit(node, &Effect)`; `complete(now) -> Vec<Completion>`. These are F-R13's accepted name divergences — the code does not move, the document does | `sim/scheduler.rs:83, 97`, `sim/control.rs:318, 402` |
| 13 | K-F-41 | **§7 rows name `contracts::authority`**: A1 all of it plus `ExternalFenceVerified`; T1 and P1 the decision/view types; R1 and F1 `PartitionMode`/`BlockReason`; L1 and R1 `min_regular_acks`. One note under the table says the module is shared on purpose | `authority.rs:203-208` ("every kernel matches on"), `lib.rs:47-50` |
| 14 | **K-F-42** | **ADR-rdb-0003's replay bullet marked "(owed by package I1)"**, matching its three owed neighbours, and now states what the function does today. **Lead ruling: I1, not H1** — `developer-handoff.md` §R1.6 says H1 and is wrong; I do not own that file, so the correction is routed, not made | `crates/rdb-sim/src/harness/replay.rs:44-46` — `Err(SimError::unavailable("harness::replay::replay"))`, unconditional, `const fn`, the only function in the module |

### R2.2 Observables the critic named

- `grep -n "ExternalFenceVerified" design.md` gives hits in §4.1 (the enum and the paragraph),
  §4.11 and §7. **Closes K-F-41's observable.**
- `grep -n "DigestMismatch\|PowerLoss" design.md` gives one hit: the §5 sentence that now says
  there is no `PowerLoss` fault. **Closes K-F-43's observable** in the direction asked for.
- `grep -rn quorum_rule teams/foundation/` gives, in the files I own, only withdrawal text
  (`design.md` §4.10, this handoff §5.1 and §10.1). The remaining hits are in `critic-design.md`,
  `critic-round2.md` and `developer-handoff.md`, which I do not own; all three are historical
  records of the finding, not instructions. **Closes K-F-40's observable.**
- ADR-rdb-0003: no unmarked Verification bullet now names a function returning
  `SimError::Unavailable`. **Closes K-F-42's observable.**

### R2.3 One change slightly outside the named list, disclosed

ADR-rdb-0003's *Consequences* section carried the same false sentence as `design.md` §5 — "the
simulator's providers ... are neither `Copy` nor `const`". I corrected it in the same minimal
form. Reason: the assignment told me to fix that exact statement in `design.md`, and leaving the
ADR asserting it would put the reconciled design in conflict with an Accepted ADR I own. It is a
correction of fact, not a decision. The lead may revert the clause without affecting anything else.

### R2.4 Declined, and why

- **`AppendReject` gained no variant.** The ruling is explicit; I state the absence in §4.8 rather
  than fixing it.
- **`min_regular_acks` validation wording left alone.** K-F-39 is being fixed in `crates/` by a
  developer right now. §4.6 describes `validate()` as it stands and does **not** repeat the
  rustdoc's "cannot arrive by any path" claim, which is the sentence under repair. If the fix
  lands `#[serde(try_from)]`, §4.6 needs one clause added and nothing rewritten.
- **`design.md` §4.9 untouched.** It calls `NodeLifecycle` "a sixth event source", which is loose
  now that `EventKind` has seven variants. §4.9 is outside the assigned scope; the §4.1 annotation
  the critic named is corrected. One sentence for a later round.
- **§8's row table untouched.** Out of scope, and K-F-44's M7F-05 split is a test-plan decision.
- **§10.1's K-F-05 row untouched.** It records what round 1 ordered and is true as history;
  A-R23's `partition` addition is recorded in R2.1 row 3 rather than by rewriting a history row.
- **No `crates/` file written, no test-plan or other team's file touched, no git state change.**

### R2.5 Where I think the code, not the document, may be wrong — for the lead

None found in the reconciled sections. Every disagreement resolved the way the assignment
predicted: the code follows a later ruling and the document had not caught up. Two things worth a
line, neither a code defect:

1. **`PartitionConfig::validate`'s rustdoc** (`membership.rs:105-107`) is the K-F-39 sentence.
   I did not touch it (code), and §4.6 deliberately does not restate it.
2. **`developer-handoff.md` §R1.6 says replay is owed to H1.** The lead ruled I1. The file is not
   mine; it needs the one-word correction, or a later reader finds ADR-rdb-0003 and the developer
   handoff disagreeing.

### R2.6 Recommended status

**COMPLETED.** `design.md` §4.1, §4.6, §4.8, §4.10, §4.11, §5 and §7 now agree with `6893442`.
Kernel-a's A1 and kernel-b's L1 developers may start against §4.1, §4.10, §4.11 and §7 — K-F-40
and K-F-41 are closed. Recommended next: the lead commits, and routes the two one-line corrections
in R2.5 to whoever holds `developer-handoff.md`.
