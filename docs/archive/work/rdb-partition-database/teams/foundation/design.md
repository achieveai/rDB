# Team foundation — design (architect, 2026-09-20)

How `rdb-core` and `rdb-sim` are organised so tests are fast and the code stays small.

Motto: **no code is best code.** Every type below earns its place by making a specific wrong
program fail to compile, or by removing a state machine that would otherwise be written six times.

**Status: correction round 1 (critic findings K-F-01..38, rulings F-R6..F-R12, B-R23, V-R19).**
The seed at `8a23b1d` compiles, is clippy-clean, is fmt-clean, and rows M7F-01..04 pass. The
signatures below are the corrected contract: `dev-foundation-r1` is landing them in
`crates/rdb-core` and `crates/rdb-sim` in parallel with this revision. Where a signature changed
in this round it is marked **(R1)** and named in `architect-handoff.md` "Correction round 1", so
the critic can check the document and the code against each other line by line. The code is the
seed; this document says what the code must say.

---

## 1. Shape

```
crates/rdb-core/          pure kernel. no clock, no I/O, no randomness, no async.
  src/lib.rs              module list + flat `pub use`. #![deny(missing_docs)] #![forbid(unsafe_code)]
  src/contracts.rs        the seam modules, and a seam -> spec-section table
  src/contracts/
    ids.rs                every dense identity newtype, incl. the three watermarks
    digest.rs             Domain, Digest, domain-separated hashing
    errors.rs             Capability, ErrorKind, RetryRule, RdbError
    version.rs            the four mandatory versions and one check
    time.rs               Tick, Deadline, ControlTime, ClockVerdict, timer effect/event
    storage.rs            Namespace, Batch, prefixes, StoreEffect/StorageEvent, SnapshotRead
    control.rs            ControlKey, CasOutcome, ReadOutcome, WatchTermination, effect/event
    transport.rs          PeerLabel, Frame, SendEffect, TransportEvent
    membership.rs         CopyId, Member, PartitionConfig
    txn.rs                TxnRequest/TxnResult and everything they carry
    envelope.rs           ReplicationEnvelope, ReplicaProgress, AppendAck, AppendReject
    event.rs              Event, EventKind, Effect, EffectKind, Budgets, StepCtx, Module
    trace.rs              the second vocabulary: what the system declares it did
  src/authority.rs        A1 | kernel-a
  src/transaction.rs      T1 | kernel-a
  src/publication.rs      P1 | kernel-a
  src/replication.rs      R1 | kernel-b
  src/protection.rs       L1 | kernel-b
  src/recovery.rs         F1 | kernel-b

crates/rdb-sim/           the replaceable environment. owns all the nondeterminism there is.
  src/lib.rs              determinism rules + module table
  src/error.rs            SimError
  src/sim.rs              scheduler, clock, network, control, cluster        | H1
  src/storage.rs          StorageOp + memory / crash_image / snapshot        | M1
  src/harness.rs          dispatch / trace / replay                          | I1
  tests/support/mod.rs    the frozen registry (ctx, probe_event, budgets)
  tests/support/oracle.rs        module root, owned by verification (O1)
  tests/support/scenarios.rs     module root, owned by verification (G1, Q1)
  tests/harness.rs        row M7F-01 (uses #[retcd_test]; every row in this crate does — K-F-30)
crates/rdb-core/tests/contracts.rs   rows M7F-02..04 and the C0 rows below (C0 developer)
```

Dependency arrow, one way only (ADR-rdb-0002): `rdb-sim -> rdb-core`, and `rdb-* -> config-*`
never the reverse. `rdb-core` depends on no `config-*` crate at all.

## 2. Why tests are fast

Four choices, in the order they matter.

1. **The kernel has no runtime.** `Module::step` is a synchronous function. Every kernel test is a
   plain `#[test]` that builds a `StepCtx`, calls `step`, and inspects the returned
   `Vec<Effect>`. No Tokio, no harness, no sleep. `rdb-core` depends on bytes, serde, thiserror,
   tracing and sha2 — all already in `Cargo.lock`, none with a build script.
2. **Time is a `u64`.** `Tick` is logical. Nothing in the simulator ever waits; a scenario that
   exercises a 24-hour dedup window advances a counter.
3. **No proptest, no second shrinker** (ruling V-R1). Shrinking replays the recorded event stream,
   which is the only reproducer that survives a schema bump.
4. **No heavy dev-dependency in the hot path.** `config-testkit` pulls RocksDB, OpenRaft, tonic and
   rustls; the seed does not take it (handoff question Q1). Today's `cargo test -p rdb-sim` builds
   nine crates and the row runs in 0.00s.

## 3. Why the code stays small

| Instead of | The seed has | Saved |
|---|---|---|
| four typed read families x get/scan | one `Namespace` parameter | six methods |
| a read effect + a read event + a resume state per condition | `StepCtx.snapshot: &dyn SnapshotRead` | a three-event state machine in six modules |
| provider traits in the kernel (clock, net, store) | none — the kernel's only trait is `Module` | three traits, and the need for a harness to unit-test |
| a sealed `DurableProof` with a hidden constructor | three watermark newtypes with no conversion (B-R13) | a type, its impl, and a rule nobody could check |
| an `Effects` wrapper with push/take | `Vec<Effect>` (A-R17) | a newtype and six methods, and a signature that differed from `KvState::apply_with_effects` for no gain |
| a string control key | `ControlKey` enum + `encode()` | a naming convention and the typos it allows |
| a broadcast send effect | `SendEffect::Unicast` only | fan-out logic in the kernel; the scenario decides delivery |
| a method per injectable fault | `NetworkOp` / `StorageOp` / `ControlOp` enums | an uncountable fault surface. Enums are enumerable, so the coverage matrix can count them |

## 4. The seams, with the signatures I commit to

### 4.1 Event / effect — `contracts::event`

The whole kernel interface is one trait:

```rust
pub trait Module {
    fn name(&self) -> ModuleName;
    fn capability(&self) -> CapabilityState;     // (R1, K-F-10) non-mutating; never steps
    fn step(&mut self, ctx: &StepCtx<'_>, event: &Event) -> Result<Vec<Effect>, RdbError>;
}

pub struct StepCtx<'a> {
    pub now: Tick,
    pub control_time: ControlTime,
    pub node: NodeId,
    pub boot: BootId,
    pub partition: PartitionId,
    pub generation: Generation,          // (R1, F-R10) filled mechanically from the last
    pub owner_epoch: OwnerEpoch,         //   Effect::AdoptAuthority this partition emitted;
    pub config_version: ConfigVersion,   //   the dispatcher decides nothing — see below
    pub snapshot: &'a dyn SnapshotRead,
    pub budgets: &'a Budgets,
}

pub struct Event {
    pub id: EventId,             // the total order; assigned by the scheduler, never by a module
    pub at: Tick,
    pub node: NodeId,
    pub boot: BootId,
    pub partition: PartitionId,
    pub correlation: CorrelationId,
    pub kind: EventKind,
}

pub enum EventKind {                     // (R2, A-R23) seven variants — `event.rs:152–186`
    Client(ClientEvent),
    Node(NodeLifecycle),         // the process-lifecycle source — see 4.9
    Transport(TransportEvent),
    Storage(StorageEvent),
    Control(ControlEvent),
    Timer(TimerFired),
    ExternalFenceVerified {              // (R2, A-R23) spec §7.2's fallback; A1's takeover guard
        partition: PartitionId,
        prior_generation: Generation,
        prior_owner_epoch: OwnerEpoch,
        prior_boot_id: BootId,
        control_revision: Revision,
        evidence: EvidenceRef,           // `contracts::authority` — see 4.11
    },
}

pub struct Effect {
    pub correlation: CorrelationId,
    pub from: ModuleName,
    pub partition: PartitionId,
    pub kind: EffectKind,
}

pub enum EffectKind {
    Send(SendEffect),
    Store(StoreEffect),
    Control(ControlEffect),
    Timer(TimerEffect),
    Reply(ReplyEffect),
    AdoptAuthority {                     // (R1, F-R10) the sixth kind; no completion event
        partition: PartitionId,          // (R2, A-R23) per partition, not per dispatcher
        generation: Generation,
        owner_epoch: OwnerEpoch,
        config_version: ConfigVersion,
    },
}

pub enum ReplyEffect {
    Transaction { identity: RequestIdentity, result: TxnResult },
    Status      { identity: RequestIdentity, status: TxnStatus },
    Read        { identity: RequestIdentity, outcome: ReadServiceOutcome,   // (R1, F-R7)
                  value: Option<(Version, Digest)> },                        // never key/value bytes
    Failed      { identity: RequestIdentity, error: RdbError },
}

pub enum ModuleName { Authority, Transaction, Replication, Publication, Protection, Recovery }
impl ModuleName {
    pub const ALL: [Self; 6];
    pub const fn capability(self) -> Capability;
}
pub struct Budgets { /* 10 spec thresholds */ }
impl Budgets { pub const SPEC_DEFAULTS: Self; }
```

`step` returns a plain `Vec<Effect>` (ruling A-R17), matching `KvState::apply_with_effects` in the
control plane. `&mut self` is the only mutable state a module has, and it is per-module — there is
no shared kernel state object. A step that returns an error returns no effects.

**Authority is adopted by effect, never assumed by the environment (R1, K-F-05, ruling F-R10).**
`StepCtx.generation`, `owner_epoch` and `config_version` are not decided by `rdb-sim`. Kernel-a's
A1 emits `EffectKind::AdoptAuthority { partition, generation, owner_epoch, config_version }` —
**(R2, A-R23)** the effect names the partition it adopts for, because A1 serves many partitions
from one grant (`event.rs:268–278`) — when its fenced CAS has committed (the *when* is ADR-rdb-0007's rule and stays in kernel-a). The dispatcher stores
the last adopted triple per `(node, partition)` and copies it into every later `StepCtx` for that
partition. Before the first adoption the triple is `Generation(0)`, `OwnerEpoch(0)`,
`ConfigVersion(0)` — a value no grant ever carries, so a module that acts on it is refused by its
own ladder (kernel-b row 5), not by the simulator. `AdoptAuthority` has no completion event: it is
a declaration, and the trace records it as the `AuthorityDecision` the module already emits. No
authority rule lives in `rdb-sim` (charter DO-NOT).

**Capability is a question, not a probe (R1, K-F-10).** `Module::capability(&self)` takes `&self`
and returns `Wired` or `Unavailable` from a constant the module declares. The dispatcher never
steps a module to find out whether it is built; the old probe stepped all six with an event the
protocol never sent and dropped the effects, which would have corrupted the first real module's
state at the start of every run, deterministically. `Wired` is asserted positively by the module,
never inferred from "not `Unavailable`".

**Reads are served (R1, K-F-11, ruling F-R7).** M7 serves client reads. `ReplyEffect::Read` carries
`ReadServiceOutcome` — the same closed set the trace's `Read` kind carries — and the value as a
`(Version, Digest)` pair, never bytes, so a read reply and a transaction reply are different
things in the trace and the oracle can tell them apart.

**`EventKind` has seven variants, not six (R2, ruling A-R23).** `ExternalFenceVerified` is spec
§7.2's fallback — the operator or platform mechanism that verified the prior machine is fenced —
and it is an event source because the kernel cannot observe the external fact for itself. Its
rustdoc (`crates/rdb-core/src/contracts/event.rs:165–185`) states the rule the shape exists for:
it is *never* synthesised from unreachability; it arrives from outside, from operator tooling at
M9 and from the scenario at M7. It carries the six binding fields so A1's takeover guard compares
them against its own takeover state rather than against a value filled in from that state (finding
K-A-37). A `match` on `EventKind` written from the six-variant list in the seed's design does not
compile; this list is the one to write against.

### 4.2 Time — `contracts::time`

```rust
pub const TICK_MILLIS: u64 = 1;
pub struct Tick(pub u64);
impl Tick { pub const ZERO: Self;
            pub const fn plus_millis(self, millis: u64) -> Self;
            pub const fn millis_until(self, later: Self) -> u64; }
pub struct Deadline { pub at: Tick }
impl Deadline { pub const fn remaining_millis(self, now: Tick) -> u64;
                pub const fn elapsed(self, now: Tick) -> bool; }

pub struct ControlTime { pub estimate: Tick, pub error_millis: u64, pub bound_established: bool,
                         pub sampled_at: Tick }                                  // (R1, K-F-15)
impl ControlTime {
    pub const fn is_stale(self, now: Tick, max_age_millis: u64) -> bool;        // (R1, K-F-15)
    pub const fn compare(self, now: Tick, max_age_millis: u64,
                         instant: Tick, margin_millis: u64) -> ClockVerdict;    // (R1) stale => Uncertain
}
pub enum ClockVerdict { DefinitelyBefore, DefinitelyAfter, Uncertain }

pub enum TimerEffect { Arm { id: TimerId, version: TimerVersion, at: Tick },
                       Cancel { id: TimerId, version: TimerVersion } }
pub struct TimerFired { pub id: TimerId, pub version: TimerVersion, pub scheduled_at: Tick }
```

`ClockVerdict::Uncertain` is a third value, not an error and not a `bool`. Spec §7.2's fail-closed
rule is unwritable as a two-valued comparison. `TimerVersion` makes a fire that was cancelled and
rearmed ignorable without a cancellation map.

**A bound has an age (R1, K-F-15, ruling A-R12).** `sampled_at` is the tick the estimate was taken
at. `compare` returns `Uncertain` when `now.millis_until` from `sampled_at` exceeds
`max_age_millis`, before it looks at the error bound, so an estimate established long ago cannot
stay `bound_established: true` and confident forever. The staleness rule is in `rdb-core`, in the
one comparison every module shares; the environment does not widen `error_millis` as time passes,
because that would put the rule in `rdb-sim`. `max_age_millis` is the caller's budget
(kernel-a's A1 passes `renew_millis`). Row M7F-14 asserts a stale bound yields `Uncertain`.

`estimate` stays typed `Tick` in this round (K-F-16, ADVISORY, open): the module doc says the two
notions must not be confused and the types do not enforce it. A distinct newtype is one line and
is the developer's call; until then the doc claims a separation the types make only by name.

### 4.3 Storage — `contracts::storage`

```rust
pub enum Namespace { User, History, Dedup, Progress, Meta }
pub struct Write { pub ns: Namespace, pub key: Bytes, pub value: Option<Bytes> }
pub struct Batch { pub id: BatchId, pub partition: PartitionId, pub generation: Generation,
                   pub seq: Seq, pub writes: Vec<Write> }

pub struct CapturedPrefix { pub partition: PartitionId, pub generation: Generation,
                            pub through: AppliedSeq }
pub struct DurablePrefix  { pub partition: PartitionId, pub generation: Generation,
                            pub through: DurableSeq }

pub enum StoreEffect {
    Commit(Batch),
    Flush { ticket: FlushTicket, captured: Vec<CapturedPrefix> },
    Snapshot { handle: SnapshotHandle, partition: PartitionId },
    Release { handle: SnapshotHandle },
}

pub enum StorageEvent {
    Committed { batch: BatchId, applied: AppliedSeq },
    CommitFailed { batch: BatchId, fault: StorageFault },
    Flushed { ticket: FlushTicket, durable: Vec<DurablePrefix> },
    FlushFailed { ticket: FlushTicket, fault: StorageFault },
    SnapshotReady { handle: SnapshotHandle, at: Seq },
}

pub enum StorageFault { WriteFailed, FlushFailed, ProcessCrash, HostCrash, Corrupt }

pub trait SnapshotRead {
    fn handle(&self) -> SnapshotHandle;
    fn at(&self) -> Seq;
    fn generation(&self) -> Generation;
    fn get(&self, ns: Namespace, key: &[u8]) -> Option<Bytes>;
    fn version(&self, ns: Namespace, key: &[u8]) -> Option<Version>;   // (R1, K-F-04)
    fn scan(&self, ns: Namespace, from: &[u8], limit: usize) -> Vec<(Bytes, Bytes)>;
}
```

**The read seam exposes versions (R1, K-F-04).** `Condition::VersionEquals` and
`Mutation::expected_version` are in the contract, so the one read surface the kernel has must
answer "what version is this key at". `version` is the accessor; `get` keeps returning the value
only, because most reads want one or the other and a tuple would make every caller destructure.
`EmptySnapshot` returns `None`; `MemoryEngine`'s snapshot returns the same number its
`version_of` already tracked. `Version` is the `u64` alias `contracts::trace` already defines, so
the trace and the seam agree on what a version is. Row M7F-08 evaluates `VersionEquals` against a
populated snapshot.

**The achieved prefix is the truth, not the captured one (R1, K-F-25).** `StoreEffect::Flush`
carries what the kernel captured under the write-order mutex; `StorageEvent::Flushed` carries what
the environment actually made durable, and only the second may advance `durable_seq`. `durable`
may be shorter than `captured` — a real `sync_wal_through` can cover less than was asked — and a
kernel that advances to `captured` on a `Flushed` has advanced on its own belief. To make that
observable rather than a review convention, `StorageOp::ShortFlush { node, through: AppliedSeq }`
(R1) makes the next flush on `node` report `Flushed` with every `DurablePrefix` truncated to
`through`; row M7F-18 asserts the durable watermark lands at `through` and not at the capture.

The three watermarks (`contracts::ids`) are the whole durability mechanism, per ruling B-R13:

```rust
pub struct ReceivedSeq(pub u64);   // diagnostic only; qualifies nothing
pub struct AppliedSeq(pub u64);    // BufferedOnTwo rests on this; a flush may capture up to it
pub struct DurableSeq(pub u64);    // DurableOnRequiredCopies and protection resume rest on this
```

No `From`, no `into_seq`, no conversion of any kind. Marking buffered data durable has to be
written as `DurableSeq(applied.0)` — a line a reviewer sees and `grep` finds — instead of a field
assignment that reads correctly.

`contracts::ids` also gains a third authority-flavoured newtype this round (R1, K-F-18, ruling
F-R8):

```rust
pub struct Generation(pub u64);            // a partition's lineage (data generation)
pub struct OwnerEpoch(pub u64);            // one owner's tenure within a lineage
pub struct AuthorityGeneration(pub u64);   // the cluster authority generation, spec §7.2 (R1)
```

Kernel-a's `AuthorityView` keeps `authority_generation` (ruling B-R20), and A1's deny reasons
distinguish `GenerationChanged` (partition lineage) from `AuthorityGenerationChanged` (cluster),
so the two cannot share a type without the conflation ADR-rdb-0007 separates. No conversion
between the three, for the same reason as the watermarks.

### 4.4 Control — `contracts::control`

```rust
pub enum ControlKey { ClusterSchema, Node(NodeId), Grant(NodeId), Partition(PartitionId),
                      Route(RangeId), Operation(OperationId), PlannerGrant }
impl ControlKey { pub fn encode(self) -> String;                 // "cluster/schema", "nodes/{id}", ...
                  pub fn decode(key: &str) -> Result<Self, RdbError>;   // strict inverse (C0)
                  pub const fn prefix(self) -> ControlPrefix; }         // (R1, K-F-19)

pub enum ControlPrefix { ClusterSchema, Nodes, Grants, Partitions, Routes, Operations,
                         PlannerGrant }                           // (R1, K-F-19) one per §7.1 family
impl ControlPrefix { pub fn encode(self) -> String;              // "nodes/", "grants/", ...
                     pub const fn contains(self, key: ControlKey) -> bool; }

pub enum CasOutcome {
    Committed(Revision),
    Conflict { exists: bool, current: Revision },   // no value, ever
    Unknown,
    Unavailable,
}
pub enum ReadOutcome { Found { revision: Revision, value: Bytes },
                       Absent { as_of: Revision },                // (R1, K-F-20)
                       Unavailable }
pub enum WatchTermination {
    RevisionCompacted { minimum_available_revision: Revision },
    ResourceExhaustedResumable, ResourceExhaustedFatal, NotLeader, Unavailable,
}
impl WatchTermination { pub const fn is_gap(self) -> bool; }

pub struct ControlChange { pub key: ControlKey, pub revision: Revision }   // (R1, K-F-13) no value
pub struct ControlRecord { pub key: ControlKey, pub revision: Revision, pub value: Bytes }  // (R1)

pub enum ControlEffect { Cas { key, expected: Option<Revision>, value: Option<Bytes> },
                         Get { key },
                         Watch { prefix: ControlPrefix, from: Revision },   // (R1, K-F-19)
                         Reload { prefix: ControlPrefix } }                 // (R1, K-F-19)
pub enum ControlEvent {
    CasResult { key, outcome: CasOutcome },
    Value { key, outcome: ReadOutcome },
    Watched { prefix: ControlPrefix, cursor: WatchCursor, changes: Vec<ControlChange> },
    WatchProgress { prefix: ControlPrefix, revision: Revision },
    WatchTerminated { prefix: ControlPrefix, from: Revision, termination: WatchTermination },
    FamilySnapshot { prefix: ControlPrefix, snapshot_revision: Revision,
                     records: Vec<ControlRecord> },                         // (R1, K-F-12/19)
}
```

There is no multi-key write and no way to express one (spec §7.1). Staged records plus one pointer
CAS is the only atomic multi-record path.

**A watch carries no value (R1, K-F-13).** `ControlChange` is `(key, revision)` and nothing else.
ADR-rdb-0008 §4 says "a watch event causes a read, never a state change — structural, not a
convention", and with value bytes on the watch stream that sentence was a convention: a kernel
could widen a right straight from a `Watched`. Deleting the field makes it structural for free.
The only ways a value reaches the kernel are `ControlEvent::Value` (a linearizable read the kernel
asked for) and `ControlEvent::FamilySnapshot` (the coherent reload), whose records are the
separate `ControlRecord` type because a reload *is* a read.

**A family is a prefix, not a representative key (R1, K-F-19).** `ControlPrefix` has one member per
spec §7.1 family. `Watch` and `Reload` take a prefix, so a kernel sees only the families it asked
for instead of everything and filtering; `FamilySnapshot` and every watch event name the prefix
they answer. Kernel-a's `ReadFamily { prefix }` binds to `Reload`; its "watch grants and
partitions" is two `Watch` effects. Passing a single-record key where a family is meant no longer
compiles.

**Absence has a revision (R1, K-F-20).** `ReadOutcome::Absent { as_of }` is "this key did not exist
as of store revision `as_of`", so a create-only CAS has something to fence against and a stale
absent read is distinguishable from a fresh one. Row M7F-15 asserts a CAS keyed on an absent read
at a stale revision is a `Conflict`, not a `Committed`.

`WatchTermination::NotLeader` carries no hint (K-F-36, ADVISORY). ADR-rdb-0008 §4's table pairs it
with a `validated_hint` the client must not believe without a read. The kernel rule is "read
before believing anything" whether a hint arrives or not, so the hint has no consumer; the
omission is deliberate and is routed to kernel-a to record in ADR-rdb-0008 §4.

### 4.5 Transport — `contracts::transport`

```rust
pub struct PeerLabel { pub node: NodeId, pub boot: BootId, pub authenticated: bool }
pub struct Frame { pub id: MessageId, pub protocol: u16, pub config: ConfigVersion, pub body: Bytes }
pub enum SendEffect { Unicast { to: NodeId, frame: Frame } }     // no broadcast, deliberately
pub enum LinkFault { Partitioned, Unreachable, TooLarge }
pub enum TransportEvent { Delivered { from: PeerLabel, frame: Frame },
                          SendFailed { id: MessageId, fault: LinkFault } }
```

`authenticated: bool` on the label is what makes forged identity rejectable without a test-only
branch, because `PartitionConfig::copy_of` returns `None` when it is false.

### 4.6 Membership — `contracts::membership`

```rust
pub struct CopyId(pub u8);
pub struct Member { pub copy: CopyId, pub node: NodeId, pub boot: BootId,   // (R1, K-F-21) not Option
                    pub role: ReplicaRole }
pub struct PartitionConfig { pub partition: PartitionId, pub config_version: ConfigVersion,
                             pub members: Vec<Member>,
                             pub min_regular_acks: u8 }   // (R2, B-R30) default 1; zero refused
impl PartitionConfig {
    pub const DEFAULT_MIN_REGULAR_ACKS: u8 = 1;
    pub fn new(partition, config_version, members) -> Self;            // at the default threshold
    pub fn with_min_regular_acks(self, n: u8) -> Result<Self, RdbError>;
    pub fn validate(&self) -> Result<(), RdbError>;                    // InvalidArgument on zero
    pub fn copy_of(&self, peer: &PeerLabel) -> Option<&Member>;   // node AND boot must match
    pub fn primary(&self) -> Option<&Member>;
    pub fn required_regular(&self) -> impl Iterator<Item = &Member>;   // (R1, K-F-23) secondaries only
    pub fn member(&self, copy: CopyId) -> Option<&Member>;
}
```

This is the `authenticated_peer -> copy_id` mapping, and it lives in `rdb-core`, in the pinned
configuration type — not in `rdb-sim` (kernel-b item 3).

**A member has exactly one boot (R1, K-F-21).** `Member.boot` was `Option<BootId>` and `copy_of`
matched any peer boot when it was `None`; nothing ever set it, so every lookup ignored boot and a
restarted node was accepted as the copy it used to be — its pre-crash acknowledgements credited to
the new incarnation. The field is now a plain `BootId`. The value comes from where it exists in
the real system: the `nodes/{id}` control record carries the boot UUID (spec §7.1), and the
membership the H1 control provider activates (`TopologyChange`) names it. `copy_of` matches node
**and** boot, and a `Rebooted { boot }` node is not a copy until a new configuration names its new
boot. Fail closed, no `is_none_or`. Row M7F-16 asserts a reincarnated peer is not matched.

**`required_regular` is the secondaries (R1, K-F-23).** It returns the members whose
acknowledgement the primary is *waiting for*: regular secondaries, never the primary itself and
never a shadow. The old predicate admitted the primary, so "two of `required_regular()`" was
satisfied by primary plus one secondary where spec §5.2 wants two secondaries — off by one in the
direction that loses writes. `primary()` is the separate accessor. Kernel-b's own
`required_copies()` (their design §3.5) includes self by design and is derived from this; the two
names mean different sets and both say which. Row M7F-17 asserts `required_regular().count() == 2`
on an RF3 configuration and `0` on a lone-survivor one.

**The acknowledgement threshold lives on the pinned configuration (R2, ruling B-R30).**
`min_regular_acks: u8` (`crates/rdb-core/src/contracts/membership.rs:66`) is how many regular-
secondary acknowledgements qualify a write. R1 reads it from the pinned configuration and P1
consumes it, so there is no second, independently maintained threshold.
`DEFAULT_MIN_REGULAR_ACKS` is 1 — spec §5.2's `BufferedOnTwo` — and `validate()` refuses zero with
`RdbError::InvalidArgument { field: "min_regular_acks" }` (`membership.rs:107–113`), because "no
acknowledgement required" is the spec §5.2 rule with the safety taken out. `new()` sets the
default; `with_min_regular_acks(n)` validates. Row `b_r30_min_regular_acks_defaults_to_one_and_
refuses_zero` pins both directions.

### 4.7 Digest, errors, versions

```rust
pub const DIGEST_MAGIC: &[u8; 4] = b"RDBH";
pub enum Domain { Record = 1, Request = 2, Lineage = 3, Config = 4, Checkpoint = 5 }
pub struct Digest(pub [u8; 32]);
impl Digest { pub const ROOT: Self;
              pub fn of(domain: Domain, parts: &[&[u8]]) -> Self;
              pub fn to_hex(self) -> String; }

pub enum Capability { Authority, Transaction, Replication, Publication, Protection, Recovery,
                      Codec, Environment }
pub enum ErrorKind { /* 18 payload-free names; Serialize + Deserialize */ }
pub enum RetryRule { RefreshRoute, RetryAfterRecovery, Definitive, BoundedJitter, QueryStatus,
                     Reconcile, Quarantine, NotWired }
pub enum RdbError { /* 18 variants; thiserror; deliberately NOT serde */ }
impl RdbError {
    pub const fn kind(&self) -> ErrorKind;
    pub const fn retry_rule(&self) -> RetryRule;
    pub const fn proves_no_mutation(&self) -> bool;
    pub const fn capability(&self) -> Option<Capability>;
    pub const fn unavailable(capability: Capability, reason: &'static str) -> Self;
}

pub const API_VERSION: u16 = 1;
pub const ENVELOPE_VERSION: u16 = 1;
pub const CONTROL_RECORD_VERSION: u16 = 1;
pub const TRACE_SCHEMA_VERSION: u16 = 1;
pub enum VersionedArtifact { Api, Envelope, ControlRecord, Trace }
pub const fn check_mandatory(artifact: VersionedArtifact, found: u16) -> Result<(), RdbError>;
```

`RdbError` carries `&'static str` reasons, so it cannot derive `Deserialize`. `ErrorKind` is the
serde-capable projection used in traces and logs — the same split `config_core::ConfigError` uses.
`ErrorKind` is the 17 spec §5.4 names **plus `Unavailable`**, which §5.4 does not define: it is
the build-time "not wired" answer spike §8 permits, and it is in the enum so a trace can carry it
(K-F-38). Nowhere else does this document call `ErrorKind` "exactly the §5.4 set".

**`NotWired` proves nothing (R1, K-F-26).** `proves_no_mutation()` returns `false` for
`RetryRule::NotWired`. An unwired capability has not mutated anything *today*, but "not built"
is not a pre-admission rejection, and the day a partially wired module fails after emitting a
store effect the old `true` would be a lie asserted by a passing row. M7F-01 asserts the narrower
fact directly: the step returned an error and **no effect**.

`Digest::of` keeps the `u64` LE length prefix per part (K-F-37, ADVISORY): the `try_from` clamp
can never fire on any supported target and is replaced by a plain widening with a comment, so the
dead branch stops reading as a safety measure. Developer's file; no contract change.

### 4.8 The replication envelope and `record_digest` (kernel-b item 1)

```rust
pub struct ReplicationEnvelope {
    pub header: EnvelopeHeader,        // protocol_version, partition, generation, config_version,
                                       // owner_epoch, seq, body_len
    pub lease_id: LeaseId,
    pub prev_digest: Digest,
    pub request_identity: RequestIdentity,
    pub request_digest: Digest,
    pub conditions_result: ConditionOutcome,
    pub mutations: Vec<Write>,
    pub result: Outcome,
    pub record_digest: Digest,
}
impl ReplicationEnvelope {
    pub fn compute_record_digest(&self) -> Result<Digest, RdbError>;   // C0 owes the body
    pub fn encode(&self) -> Result<Bytes, RdbError>;
    pub fn decode_header(bytes: &[u8]) -> Result<EnvelopeHeader, RdbError>;
    pub fn decode(bytes: &[u8]) -> Result<Self, RdbError>;
}

pub struct ReplicaProgress { pub received: ReceivedSeq,
                             pub buffered_applied: AppliedSeq,
                             pub durable: DurableSeq }
pub struct AppendAck { /* partition, generation, owner_epoch, config_version, from, boot,
                          role, progress, digest_at_buffered */ }
pub enum AppendReject {                  // (R2, B-R30) 16 variants — `envelope.rs:524–590`
    Quarantined,                                       // row 0
    IncompatibleVersion,                               // row 1
    TooLarge,                                          // row 2
    WrongPartition,                                    // row 3
    StaleGeneration { current: Generation },           // row 4, `<`
    NeedLineage    { current: Generation },            // row 4, `>`
    StaleEpoch     { current: OwnerEpoch },            // row 5, `<`
    UnknownEpoch   { current: OwnerEpoch },            // row 5, `>`
    StaleConfig    { current: ConfigVersion },         // row 6, `<`
    NeedConfig     { current: ConfigVersion },         // row 6, `>`
    NotAMember,                                        // row 6, and §3.2a row 6R′
    CorruptHistory { at: Seq },                        // row 7 — quarantines
    DivergentHistory { at: Seq },                      // row 8 — quarantines
    NeedPrefix { have: Seq },                          // row 8
    StaleFence,                                        // §3.2a rows 5R/6R
    Unauthenticated,
}
```

**The reject ladder is kernel-b's §3.2 table, one variant per rung (R2, ruling B-R30).** The seed
listed five; the landed enum is the sixteen above, in ladder order, and the two names the seed used
are not among them: a digest that does not recompute is `CorruptHistory { at }` and a configuration
mismatch splits by direction into `StaleConfig` and `NeedConfig`. The split matters — row 4 and row
6 each mean two different things depending on which side is ahead, and a secondary never learns a
generation, an epoch or a configuration from the data path, so the `>` arms exist to say "refused,
and control is where this arrives from" rather than to accept. `Quarantined` is row 0 because a
quarantined receiver changes no state at all, ever.

**Two outcomes kernel-b's step-8 table names are not rejects and are not here (R2, lead ruling,
round 2).** `Busy { accepted_through }` and `AlreadyHave` are non-reject `AppendOutcome` arms in
kernel-b's own table. The ladder gains no variant for them this round; kernel-b names them when R1
lands, as an additive enum change. The same holds for `ProbeDigestAt` (B-R33).

**The `record_digest` preimage — stated here, once (ruling F-R6, closing K-F-01 and K-F-02).**
This paragraph is the contract. The rustdoc on `compute_record_digest` cites this section and
does not restate the list; kernel-b's design §1.1 is the consumer statement this was ruled to
match. `Digest::of(Domain::Record, parts)` hashes `"RDBH" ‖ 1u8 ‖ (len_u64_LE ‖ part)*` with
SHA-256, every part its own length-prefixed field (K-B-08), in this order:

| # | Part | Bytes |
|---|---|---|
| 1 | `prev_digest` | 32 — **first**, so no later field can displace the chain link (B-R9) |
| 2 | `header.partition` (`partition_id`) | 4, LE |
| 3 | `header.generation` | 8, LE |
| 4 | `header.owner_epoch` | 8, LE |
| 5 | `header.seq` | 8, LE |
| 6 | `header.config_version` | 8, LE |
| 7 | `request_identity` | 16: tenant u32 LE, client u32 LE, request u64 LE |
| 8 | `request_digest` | 32 |
| 9 | `conditions_result` | `count u32 LE`, then one u8 each (1 Met, 2 NotMet) |
| 10 | `mutations` | `count u32 LE`, then per write: `ns u8` (1 User … 5 Meta), `key_len u32 LE`, key, `has_value u8`, and when 1 `value_len u32 LE` and value |
| 11 | `result` | 1 (1 Published, 2 RecoveredApplied) |

**Excluded, each for a stated reason:**

| Field | Why it is not in the preimage |
|---|---|
| `header.protocol_version` | a version bump must not rewrite history: the same transaction keeps its digest across a compatible upgrade (validation gate V12). Including it would make the envelope-version bump that ADR-rdb-0002 names as the escape hatch turn every historical chain into `CorruptHistory` |
| `lease_id` | not authority-bearing (kernel-b §2.4, ladder row 5a dropped under B-R20); a record replayed under a reissued lease must be provably the same record |
| `header.body_len` | framing, not content |
| `record_digest` | it is the output |

`partition_id` **is** in: two partitions at equal `(generation, owner_epoch, seq)` with identical
bodies must not collide, because F1's compatible-prefix selection compares `(seq, digest)` pairs
that never pass the ladder's partition check.

Equal digest at equal seq therefore implies equal prefix within a partition. The C0 vectors
(row M7F-02, `crates/rdb-core/tests/contracts.rs`) that pin this, all known-answer:

- chaining — two entries, flip one byte in the first, the **second**'s digest changes (B-R9);
- field boundaries — `key="ab" value="c"` and `key="a" value="bc"` differ (K-B-08);
- **(R1)** same record, two `protocol_version` values, **same** digest;
- **(R1)** same record, two `lease_id` values, **same** digest;
- **(R1)** two partitions, equal `(generation, owner_epoch, seq)` and body, **different** digests;
- `record_digest` and `body_len` excluded;
- the golden hex, re-pinned after the R1 preimage change (the seed's goldens included
  `protocol_version` and `lease_id` and are superseded; `ENVELOPE_VERSION` stays 1 because no
  envelope has been persisted outside a test).

The `request_digest` preimage (ruling A-R18) is unchanged by this round: `Domain::Request` over
`identity.tenant`, `affinity`, `conditions`, `mutations`, `api_version`; `remaining_millis`,
`identity.client`, `identity.request` and `expected_generation` excluded. `dev-notes.md` §3 has
the bytes; the vector "same request, two deadlines, same digest" passes at `8a23b1d`.

### 4.9 `NodeLifecycle` (kernel-a)

```rust
pub enum NodeLifecycle {
    Resumed { suspended_millis: u64 },
    Rebooted { boot: BootId },
}
```

A sixth event source. The only way a module learns it was stopped: it is not allowed to read a
clock and notice a jump. `Cluster::suspend` delivers `Resumed` on resumption, which is what
kernel-a's monotonic admission rule needs.

### 4.10 Trace — `contracts::trace`

The second vocabulary: what the system *declares it did*, as opposed to `event::Event`, which is
what the kernel is *fed*. Collapsing them would make the oracle re-derive decisions from inputs —
the second implementation of the protocol that spike §6 forbids.

Three properties, kept: no key or value bytes ever (a key is a `KeyId`, a value is a version plus a
`Digest`); every outcome, reason, mode and state is a closed Rust enum; `TraceEvent::event_id` is a
single strictly-increasing total order, so the oracle is a left-to-right fold that never sorts.

```rust
pub struct TraceHeader {                              // (R1, K-F-09, K-F-27)
    schema_version: u16,
    generator_version: u16,
    provenance: Provenance,                           // never a bare seed
    config: RunManifest,                              // the resolved run, budgets and their source
    partitions: u8,
    topology: Vec<TopologyEntry>,                     // initial snapshot only (V-R12)
    oracle_checkpoint_digest: Digest,
}
pub enum Provenance { Generated { seed: u64 },
                      Reduced { parent: ScenarioId },
                      Authored { case: String } }     // verification trace-requirements §1
pub struct RunManifest {                              // (R1, K-F-27) plain data, no sim type
    budgets: Budgets,                                 // the values the run resolved to
    overridden: Vec<BudgetName>,                      // which of them differ from SPEC_DEFAULTS
    nodes: u8,
    event_cap: u32,
}
pub enum BudgetName { WarnAge, PauseAge, ResumeLag, ResumeHold, Grant, Renew, ClockError,
                      DispatchMargin, DedupRetention, DiscoveryWindow }   // one per Budgets field
pub struct TraceEvent { event_id: EventId, logical_tick: u64, partition, node, boot,
                        correlation, kind: TraceKind }
pub enum TraceKind { /* 24 variants */ }
pub struct Trace { header: TraceHeader, events: Vec<TraceEvent> }
```

**The header (R1).** `seed: u64`, `config_digest` and the top-level `budgets` are gone.
Verification's §1 refused a bare seed in writing: a reduced or authored scenario is not in the
generator's image, so replaying its seed reproduces nothing and the checked-in event stream is the
reproducer (ADR-rdb-0003 decision 6 says the same). `Provenance` is the shape they asked for.
`config: RunManifest` is the charter's I1 "manifest records resolved budgets": the values, and
**which of them were overridden** from `Budgets::SPEC_DEFAULTS`, so a campaign row that fails
under an override cannot be mistaken for one that fails under defaults — the
`RETCD_TEST_DEADLINE_SCALE` lesson from the rEtcd gate. It is plain data in `rdb-core` because the
header carries it and the header is in `rdb-core`; no simulator type crosses the crate boundary.
`#[serde(deny_unknown_fields)]` on the header and the manifest, as verification's §1 requires.
Row M7F-11 round-trips a header with every §1 field through JSONL; row M7F-19 asserts an override
is visible in `overridden`.

24 kinds. The three additions over verification's 18 from the seed, and three more this round:

- `Capability { package, state }` — verification's third easy-to-miss ask, emitted once per package
  at trace start.
- `ReplicationAckDelivered { ack, from_node, peer_role, config_version, counted }` (V-R10) — the
  ack is recorded **at the secondary where it is generated**, and again **at the primary that
  counts it**. Two claims, so a dropped, delayed or stale-config ack is visible as the gap between
  them, and a forged `peer_role` shows up as a disagreement between the two records.
- `TopologyChange { config_version, nodes: Vec<(NodeId, ReplicaRole)> }` (V-R12) — **emitted by the
  H1 control provider, never by a kernel module.** `TraceHeader::topology` is the initial snapshot
  only. A kernel-emitted topology would let the oracle resolve roles from the belief under test.

- **(R1, K-F-06)** `ControlInteraction { op: ControlOpKind, key: Option<ControlKey>,
  prefix: Option<ControlPrefix>, outcome: ControlOutcomeKind }` — every control-store interaction
  and how it came out, including a watch termination with its `WatchTermination` and `gap: bool`.
  Emitted by the H1 control provider as it completes the effect, so it is environment-owned like
  `TopologyChange` and the kernel cannot lie about what the store said.
- **(R1, K-F-06)** `FamilyReload { prefix: ControlPrefix, snapshot_revision: Revision,
  after_termination: Option<EventRef> }` — a coherent reload, with an explicit back-reference to
  the `ControlInteraction` whose termination justified it. `None` is the bug: ADR-rdb-0008 §7
  item 4 as amended by A-R15 ("no reload unless a termination was delivered first") is now two
  events the oracle relates by a back-reference, not a search. Emitted by the kernel module that
  asked for the reload, because the *decision* to reload is the thing under test.
- **(R1, K-F-08)** `OpSkipped { scenario_op_index: u32, reason: SkipReason }` with
  `SkipReason { ReferentGone, OutOfBudget }` — verification's §3.16a. Its own kind on purpose: a
  reducer artifact must not be a `BoundaryId`, or the coverage matrix gains a cell nobody can
  interpret. Not read by a checker; read by the reducer. Without it a deduplicated retry that
  produces nothing is indistinguishable from a request never submitted, and "no duplicate effect
  from a retry" is checkable only in the direction that cannot fail.

```rust
pub enum ControlOpKind { Cas, Get, Watch, Reload }
pub enum ControlOutcomeKind {
    Committed, Conflict, Unknown, Unavailable,          // a CAS
    Found, Absent,                                      // a read
    Progress,                                           // a watch tick
    Terminated { termination: WatchTermination, gap: bool },
}
```

Three fields were ordered this round on named consumer asks the seed had not honoured (K-F-33).
**(R2)** Only two of the three landed; the first was withdrawn by ruling F-R13 before any code
was written, and is kept below as a withdrawal so nobody re-adds it:

- **(R1, K-F-07 — superseded in R2 by ruling F-R13.)** The R1 instruction was that
  `ProtectionState` gain `quorum_rule: QuorumRule`, emitted by kernel-b's L1. **It does not, and
  kernel-b must not emit one.** F-R13 adjudicated K-F-07 against V-R20 and closed it *by
  derivation*: the consuming team owns what its oracle needs, and a stored value beside a derived
  one is two sources of truth for one fact. `TraceKind::ProtectionState` carries `phase`,
  `oldest_unsafe_age_ms`, `required_copy_set: Vec<NodeId>`, `config_version`, `paused_prefix_seq`,
  `resume_barrier_seq` and `healthy_since_tick` (`crates/rdb-core/src/contracts/trace.rs:
  1026–1042`) — no `quorum_rule`. The oracle derives the rule from `required_copy_set.len()` and
  the membership in force. The `QuorumRule { Rf3, DegradedRf2 }` enum stays as the oracle's
  vocabulary for that derivation and for the report, and its own rustdoc says why it is not a
  trace field (`trace.rs:620–630`). Verification row M7V-90, the cross-check row, was removed
  under the same ruling. The instruction is recorded here as withdrawn rather than deleted so a
  developer arriving from an older reference sees why the field is gone.
- **(R1, K-F-22)** `AckEvidence { node, boot: BootId, role, durability }` — verification §3.7's
  four-tuple. Two acknowledgements from one node across a restart are two boots, and the checker
  counts one copy.
- **(R1, K-F-24, ruling F-R9)** `BatchApply.state_digest_after` and `Publish.published_state_digest`
  are **deleted**. Verification §6 withdrew both by name: the only way to use a whole-state digest
  is to compute your own and compare, which is the second implementation ADR-rdb-0003 decision 5
  forbids. They were also the only O(state)-per-event cost on the trace path, in a campaign
  budgeted at 10,000 histories in ten minutes. `key_versions` and `entry_digest` are what the
  oracle folds.

Closed sets folded in from verification's requirements: `AdmissionOutcome`, `AuthorityGate`,
`AuthorityOutcome`, `ApplyOutcome`, `DurabilityClass`, `AckRejectReason`, `SyncOutcome`,
`ClientOutcome`, `ReadRequestKind`, `ReadServiceOutcome` (ruling F-R4; was `ReadOutcome`, renamed
so it cannot be confused with `control::ReadOutcome` in one function), `DedupAction`,
`LineageSource`, `RecoveryMode`, `QuarantineReason`, `ProtectionPhase`, `VersionSurface`,
`VersionOutcome`, `FaultKind`, `BoundaryId` (29 — the full member list is in
`architect-handoff.md` "For verification", per ruling V-R19), `SchedulePhase`, `PackageId`,
`CapabilityState`, and this round `QuorumRule`, `SkipReason`, `ControlOpKind`,
`ControlOutcomeKind`, `Provenance`, `BudgetName`.

Two of these moved on rulings:

- `ClientOutcome` is `Success | RecoveredApplied | Error(ErrorKind)`. Three-way, so "a recovered
  transaction reported as an ordinary publication" is observable. `ErrorKind` is the closed set
  of spec §5.4 errors plus `Unavailable` (§4.7) — one enum, not a second copy that can drift.
- `ProtectionState` is emitted on every phase transition **and on every `config_version` change**,
  transition or not. Spec §6.2's trap is a membership edit that renames the required-copy set and
  resets the unsafe age; on transitions only, that edit leaves no record.

`ProtectionPhase::Resuming` is what spec §6.2 calls "Reprotecting" (K-F-38). The trace name is
kept — verification's §3.14 asked for `Resuming` — and this sentence is the mapping.

**(R2, ruling A-R23)** `TraceKind::AuthorityDecision` also carries `authority_seq: u64`
(`crates/rdb-core/src/contracts/trace.rs:772–775`): A1's per-node monotonic counter at the moment
of decision. It is what a consumer compares to tell a stale decision from a later one —
`decision_tick` is trace data, not freshness. The same counter is on the contract types in §4.11.

### 4.11 Authority — `contracts::authority` (R2, ruling A-R23)

**This section is reconciliation, not new design.** The module landed at `6893442`, 224 lines at
`crates/rdb-core/src/contracts/authority.rs`, exported from `lib.rs:47–50`. It had no design
section; this is it, written from the code. Nothing here is a decision of mine.

Contract types only. No rule lives here: *when* a grant is held, *when* a fence fires and how
`valid_through_tick` is computed are package A1's (kernel-a design §2); *when* a partition is
`Blocked` is R1's and F1's (kernel-b design §3.5, §5.8). The shapes are in `rdb-core` because
three kernels match on them, and two private enums with the same variants are two enums that
drift (`authority.rs:1–10`). `contracts::authority` is distinct from `rdb_core::authority`, which
is the A1 kernel module stub.

```rust
pub enum Checkpoint { Admission, StorageDispatch, Publication, Reply, OutboxDispatch }
pub struct Lineage { pub partition: PartitionId, pub generation: Generation,
                     pub owner_epoch: OwnerEpoch }

pub enum DenyReason {          // 15 names; spec §5.4's error mapping is total over it
    NoGrant, Frozen, Revoked, EpochRevoked, Expired, ExpiryUnproven,
    ClockUnbounded, ClockSampleStale, ProcessSuspended, BootMismatch,
    AuthorityGenerationChanged, GenerationChanged, SelfFenced,
    ControlUnavailable, LocalStorageFenced,
}
pub enum Verdict { Admit, Deny(DenyReason) }

pub struct AuthorityDecision {         // one check at one checkpoint: A1 -> T1/P1/F1
    pub owner: NodeId, pub boot: BootId, pub grant: GrantId,
    pub authority_generation: AuthorityGeneration,
    pub lineage: Lineage,
    pub expiry_utc_ms: i64,            // `E` from the grant record; evidence, never a local timer
    pub decided_at: Tick,              // trace data only
    pub authority_seq: u64,            // (A-R23) what a consumer compares
    pub checkpoint: Checkpoint, pub correlation: CorrelationId, pub verdict: Verdict,
}
impl AuthorityDecision {
    pub fn same_lineage_as(&self, earlier: &Self) -> bool;
    pub const fn admitted(&self) -> bool;
    pub fn same_lineage_as_view(&self, view: &AuthorityView) -> bool;
}

pub struct AuthorityView {             // what A1 pushes to R1, T1 and P1
    pub lineage: Lineage, pub grant_id: GrantId, pub boot_id: BootId,
    pub authority_generation: AuthorityGeneration, pub config_version: ConfigVersion,
    pub authority_seq: u64,            // (A-R23)
    pub valid_through_tick: Tick,      // hard deny boundary, not a hint
    pub past_horizon: DenyReason,      // (A-R23) the reason whose horizon bound first
}

pub struct EvidenceRef(pub [u8; 32]);  // opaque handle to external fence evidence

pub enum BlockReason {                 // one landed variant
    DivergenceRequiresOperator { diverged: Vec<CopyId> },
}
pub enum PartitionMode { Active, DegradedRf2, ReadOnly, Blocked { reason: BlockReason } }
```

Four things a kernel developer needs from this, each stated in the code:

- **A decision is a snapshot, valid only for the tick it names.** The caller carries it forward so
  a later checkpoint can prove the lineage did not move; `same_lineage_as` is that comparison
  (grant, boot, authority generation, lineage — four fields, not a tick), and
  `same_lineage_as_view` is the same comparison against the pushed view the consumer admitted
  under (`authority.rs:126–150`).
- **Freshness is `authority_seq`, never `decided_at`.** A consumer keeps the view with the highest
  `authority_seq` it has seen and rejects any answer below it. A1 bumps it on every fence and every
  grant, epoch or generation change (`authority.rs:113–121`, `:154–160`).
- **`past_horizon` is a `DenyReason`, not a bool.** Past `valid_through_tick` the view is
  worthless, and the field names the reason whose horizon bound first, so the consumer reports the
  same reason A1 would (`authority.rs:172–175`).
- **`EvidenceRef` is opaque and the kernel does not verify the external fact.** It verifies that
  the evidence names this partition, this prior lineage, this prior boot and a frozen grant record
  at a revision — the six fields `EventKind::ExternalFenceVerified` carries (§4.1). `Debug` prints
  two bytes, never the handle (`authority.rs:179–194`).

`PartitionMode` is the one enum every kernel matches on totally (finding K-B-19), which is why it
is here rather than private to one team: a mode one kernel adds is a mode every other kernel's
total `match` refuses to compile without (`authority.rs:203–224`). `BlockReason` has exactly one
variant today, `DivergenceRequiresOperator { diverged }` (ruling B-R26): R1 found a divergence with
no durable floor under the pinned configuration, qualification can never return, and the only exit
is an operator removing the diverged copies and fencing. The `diverged: Vec<CopyId>` is there so
the alert names them.

## 5. What `rdb-sim` owns

```rust
// sim::scheduler   — Scheduler { now, next_id, queue: BTreeMap<(Tick, EventId), Event> }
//                    now / next_event_id / schedule / pop / queued / next_tick   (R2: `pop`)
// sim::clock       — Clock { now, skew_millis, sample: ControlTime, timers: BTreeMap<..> }
//                    control_time(now) / set_skew / arm / cancel / next_deadline
// sim::network     — Network { links: BTreeMap<(NodeId, NodeId), LinkState>, in_flight: Vec<..>,
//                              planned: Vec<..> }
//                    inject(NetworkOp) / send
// sim::control     — ControlStore { revision, records: BTreeMap<ControlKey, (Revision, Bytes)>,
//                                   watches: BTreeMap<NodeId, ..>, pending: Vec<..>,
//                                   planned: Vec<ControlOp> }
//                    inject(ControlOp)
//                    submit(node, &Effect) -> Result<(), SimError>           (R1, K-F-14) async
//                    complete(now) -> Vec<Completion>                        (R1) drained by H1
//                                       (R2: a named struct, not the tuple)
//                    snapshot_family(prefix) -> Result<(Revision, Vec<ControlRecord>), SimError>
//                                                                             (R1, K-F-12)
// sim::cluster     — ClusterConfig { nodes: Vec<NodeSpec { node, boot }>, partitions: Vec<PartitionSpec>,
//                                    initial_config_version }
//                    Cluster: new / stop / start / suspend
// storage          — StorageOp; memory::MemoryEngine, crash_image::CrashImage,
//                    snapshot::EmptySnapshot
// harness::dispatch — Dispatcher: new / step / capability_report(&self) / adopted / deliver
//                                                                             <-- REAL today
// harness::trace    — Recorder, write_jsonl, read_jsonl
// harness::replay   — replay -> ReplayOutcome
```

**Provider stubs carry their state (R1, K-F-29).** The seed's `Scheduler`, `Clock`, `Network`,
`ControlStore` and `Cluster` were zero-sized `Copy` unit structs with `const fn` methods. None of
that survives implementation, and `Copy` on a scheduler is an active hazard: a silent copy
duplicates the queue rather than aliasing it, invisibly at the call site. Each now declares the
fields above (unused until its package lands; the bodies still return `SimError::Unavailable`),
and **`Copy` is gone from every one of them** — `Scheduler`, `Clock`, `Network`, `ControlStore`
and `Cluster` are plain `#[derive(Debug)]` / `#[derive(Debug, Default)]` structs holding their
fields. **(R2, correcting this sentence:** `const fn` did survive, on four real accessors that
read one field and decide nothing — `Scheduler::now`, `Clock::now`, `ControlStore::revision`,
`Cluster::config`. That is harmless; `Copy` was the hazard, and the seed's version of this
sentence banned both.**)** Every map is a `BTreeMap`.

**The control store is asynchronous (R1, K-F-14).** `cas` and `get` as direct calls could not be
delayed or dropped, so ADR-rdb-0008 §7 items 7 and 8 had variants and no mechanism. The kernel
submits a `ControlEffect`; the store answers with a `ControlEvent` when H1 drains it, and
`DelayCompletion` and `DropCompletion` act on that queue. `ControlOp::PlanCas` gains `node`, so a
plan can force one node's CAS while another's proceeds — the two-nodes-race row fencing exists
for (M7F-13).

**`snapshot_family` returns the records (R1, K-F-12).** `ControlEvent::FamilySnapshot` carries
`records`; the store method that produces it returned a revision and nothing else, so item 6's
coherent read was unimplementable without a signature change. It now returns both. Row M7F-12
asserts two reads at one `snapshot_revision` are identical across an interleaved write.

The injectable fault vocabularies, as enums so the coverage matrix can count them:

```rust
pub enum NetworkOp {
    SetLink { a, b, state: LinkState },
    PlanNext { from, to, delivery: Delivery },
    ForgeAck { from, to, claimed_node: NodeId, claimed_role: ReplicaRole, authenticated: bool },
    ForgeNext { from, to, label: PeerLabel },
}
pub enum StorageOp {
    Fail { node, fault: StorageFault },
    Crash { node, fault: StorageFault },
    FalseDurable { node, through: AppliedSeq },
    ShortFlush { node, through: AppliedSeq },   // (R1, K-F-25) next Flushed is truncated to `through`
}
pub enum ControlOp {
    PlanCas { node, outcome: CasOutcome },      // (R1, K-F-14) `node` added
    PlanReadUnavailable,                        // ruling F-R3
    EmitWatch { node }, EmitProgress { node },
    TerminateWatch { node, termination }, Compact { up_to },
    DelayCompletion { node, by_millis },        // ADR-rdb-0008 §7 item 7 (A-R15)
    DropCompletion { node },                    // ADR-rdb-0008 §7 item 8 (A-R15)
}
```

**A crash image holds both watermarks (R1, K-F-03).**

```rust
pub struct SurvivingPrefix { pub partition: PartitionId, pub generation: Generation,
                             pub durable: DurableSeq, pub applied: AppliedSeq }
pub struct CrashImage { pub surviving: Vec<SurvivingPrefix> }   // ascending (partition, generation)
```

The seed typed `surviving` as `(PartitionId, Generation, DurableSeq)`, and B-R13 deliberately
gives the watermarks no conversions, so buffered state that survives a `ProcessCrash` had no
representation except by being relabelled durable. Every process crash then silently promoted
buffered to durable and a kernel that acknowledged too early passed — a false green on "no lost
acknowledged write", in a struct field. Now: `ProcessCrash` preserves `applied` (the page cache is
the OS's, not the process's) and `HostCrash` truncates `applied` to `durable`;
`reopen()` restores each watermark to its own value, never above it. Row M7F-06 asserts the two
crash kinds produce different reopened engines from one pre-crash image. The module doc and the
`reopen()` doc say the same thing now (K-F-38). **(R2)** There is no `PowerLoss` fault and never
was one: `StorageFault` is `WriteFailed, FlushFailed, ProcessCrash, HostCrash, Corrupt`
(`crates/rdb-core/src/contracts/storage.rs:142–154`), and `HostCrash` is the whole-host case this
sentence used to name twice.

**The dispatcher is a fixed table, and the I1 row says so (R1, K-F-28).** `Dispatcher::step` used
`ModuleName::ALL.iter().position(..).expect(..)` — the only panic path in the crate, unreachable
today and a real one the moment `ALL` and the struct drift. The six modules are now indexed by an
infallible `match` on `ModuleName`; there is no registry and nothing to be unregistered. The
charter's I1 row "an unregistered handler fails explicitly" is therefore restated as what the
seed can actually promise: **an event routed to an unwired module yields `RdbError::Unavailable`
through the dispatcher, never a panic, and the capability report says `Unavailable` for it** —
which is M7F-01. The critic offered either a real registry or this; a registry would be code
that exists to make one row non-vacuous.

**The effect-to-event hop is bounded (R1, ruling B-R23, QC-14).** Kernel-b's 2,100 ms pause budget
is `pause_ms (2,000) + eval_cadence (≤ 50 ms) + admission_propagation (≤ 50 ms)`, and
`admission_propagation` — the interval between L1 emitting an admission effect and T1 refusing
the next transaction — is I1's term, not L1's. The dispatcher drains every effect a step returned
**in the same tick**, in vector order, before the scheduler advances: an effect's completion is
scheduled at `now` plus whatever the environment's fault plan adds, never later by the harness's
own doing. So the hop is zero ticks by construction and `admission_propagation ≤ 50 ms` holds
with 50 ms to spare; a `NetworkOp`/`ControlOp` delay is a scenario decision the trace records, not
a harness cost. The dispatcher never drops an effect (kernel-b B-R28 relies on this); a dropped
completion is only ever `ControlOp::DropCompletion`, which is a fault the trace shows. Row M7F-21
asserts the hop: an `AdoptAuthority` and a `Control` effect emitted at tick *t* are visible to the
next step at tick *t*, and with a `DelayCompletion { by_millis: 50 }` the completion lands at
exactly *t + 50* and not one tick later.

**The run manifest is written (R1, K-F-27).** `harness::manifest::resolve(config, overrides) ->
RunManifest` builds `TraceHeader.config` and records which budgets an override touched. The
campaign's `rdb-m7-campaign.json` (ADR-rdb-0019) embeds the same manifest, so a report and its
trace agree on what was run.

`ForgeAck` carries two independent lies because they are refused by two different rules:
`claimed_node != from` fails the membership lookup, and `claimed_role` above the sender's real role
is refused because the receiver resolves the role from **its own** pinned configuration. The
disagreement is visible in the trace as `ReplicationAck.peer_role` against
`ReplicationAckDelivered.peer_role`. `authenticated: true` models a stolen-but-real credential,
which epoch and membership checks must still refuse.

`FalseDurable` reports a flush as successful without syncing. No `DurableSeq` is produced, so no
durable watermark can move; a later `HostCrash` discards what it claimed.

`DelayCompletion` and `DropCompletion` catch a kernel that treats "I asked" as "I have it": a grant
renewal whose CAS lands after expiry must not revive the grant, and an effect with no completion
must not leave a partition waiting forever.

## 6. What is deliberately NOT built

| Not built | Why |
|---|---|
| async anywhere | the kernel is a synchronous fold; the simulator is single-threaded by construction |
| a real storage engine | M8 / package D1. `SnapshotRead` is the seam; RocksDB arrives behind it |
| provider traits in `rdb-core` (Clock, Network, ControlStore) | the kernel never *calls* an environment. It receives events and emits effects. Adding them would invert the design |
| a broadcast send effect | fan-out is the scenario's decision, not the kernel's |
| multi-key control-plane transactions | spec §7.1 forbids them; no type could express one |
| `From`/`Into` between the three watermarks | that conversion is the bug, not a convenience |
| an `Effects` wrapper type | ruling A-R17: `Vec<Effect>`, matching `KvState::apply_with_effects` |
| proptest, quickcheck, or any second shrinker | ruling V-R1; the recorded event stream is the reproducer |
| `config-testkit` as a dev-dependency | deferred; handoff question Q1 |
| a `Serialize` impl on `RdbError` | it carries `&'static str`; `ErrorKind` is the serde projection |
| routing (which module sees which event) | package I1, not a contract |
| any `HashMap` on a trace path | `BTreeMap` / ordered `Vec` only; iteration order is part of the trace |
| `todo!()` in any stub | it panics, which would abort the campaign runner. Explicit `Unavailable` instead (spike §8) |
| oracle and scenario bodies | registered as module roots, owned by team verification from now on |
| a fake "no silent gap" property in the control store | ruling A-R15: that is a kernel-side assertion the oracle makes against the trace, not something the fake can enforce — and since R1 it has the two trace events (`ControlInteraction`, `FamilyReload`) to be asserted over |
| value bytes on the watch stream | (R1, K-F-13) deleting `ControlChange.value` is what makes ADR-rdb-0008 §4's "structural" claim true |
| whole-state digests in the trace | (R1, K-F-24, F-R9) verification withdrew them; the only super-linear cost on the trace path |
| an authority rule in `rdb-sim` | (R1, F-R10) `StepCtx`'s authority triple is copied from the last `AdoptAuthority`; kernel-a decides when |
| a capability probe that steps modules | (R1, K-F-10) `Module::capability(&self)` answers without stepping |
| a module registry | (R1, K-F-28) six fields indexed by an infallible `match`; nothing can be unregistered |
| `Copy` or `const fn` on a provider | (R1, K-F-29) the providers hold state; a silent copy would duplicate it |

## 7. Who consumes what

| Team | Package | Consumes |
|---|---|---|
| kernel-a | A1 authority | **`authority` (all of it — §4.11)**, `event::EventKind::ExternalFenceVerified`, `time` (all of it, `ClockVerdict` especially), `control` (grant CAS, the five terminations), `event::NodeLifecycle`, `errors`, `membership` |
| kernel-a | T1 transaction | `authority::{AuthorityDecision, AuthorityView, Checkpoint, Verdict}`, `txn`, `storage` (`Batch`, `Namespace::{User,History,Dedup}`, `SnapshotRead`), `digest`, `errors` |
| kernel-a | P1 publication | `authority::{AuthorityView, Checkpoint}`, `storage` (`SnapshotRead`, `SnapshotReady`), `envelope::ReplicaProgress`, `txn::{Durability,Outcome}` |
| kernel-b | R1 replication | `envelope` (all), `authority::{AuthorityView, PartitionMode, BlockReason}`, `transport`, `membership` (incl. `min_regular_acks`), `storage` (`AppliedSeq`, `DurableSeq`, `DurablePrefix`) |
| kernel-b | L1 protection | `envelope::ReplicaProgress`, `membership::{required_regular, min_regular_acks}`, `time`, `Budgets` |
| kernel-b | F1 recovery | `authority::{PartitionMode, Lineage}`, `digest`, `envelope`, `ids::{Generation,OwnerEpoch}`, `errors::{CorruptHistory,...}` |
| verification | O1 oracle | `trace` **only**. Never simulator state, never a kernel internal |
| verification | G1 scenarios | `sim::cluster::ClusterConfig`, `NetworkOp`, `StorageOp`, `ControlOp`, `trace::BoundaryId` |
| verification | Q1 campaign | `harness::{dispatch,trace,replay}`, `trace::{PackageId,CapabilityState,SchedulePhase}` |
| all four | — | `tests/support/mod.rs`: `ctx()`, `probe_event()`, `BUDGETS`, `SNAPSHOT`, `ROOT_DIGEST` |

**(R2)** `contracts::authority` is shared on purpose and the §4.11 rows above say who reads what.
`PartitionMode` in particular is matched totally by kernel-a *and* kernel-b — that is the reason it
is in `rdb-core` and not private to either (`crates/rdb-core/src/contracts/authority.rs:203–208`).
The per-package column is which types a package reads, not a permission: the module is public and
exported from `lib.rs:47–50`.

## 8. Acceptance rows

One row = one test; the row id prefixes the test name (`m7f_08_...`). Rows marked **(R1)** were
added or restated in correction round 1 and are the ones the test planner must carry. Package =
who lands the code the row exercises. Every `rdb-sim` row uses `#[retcd_test]` (K-F-30) and a
Q-row reads its JSONL.

| Row | Claim | Package | State |
|---|---|---|---|
| M7F-01 | every kernel package reports `Unavailable` from `Module::capability(&self)` without being stepped; stepping an unwired module through the dispatcher returns `Unavailable` with **no effect** and never panics | I1 | **passes** at `8a23b1d`; **(R1)** restated against `capability(&self)` and "no effect returned" (K-F-10, K-F-26, K-F-28) |
| M7F-02 | `record_digest` known-answer vectors: chain flip, field boundaries, `protocol_version` invariance **(R1)**, `lease_id` invariance **(R1)**, partition binds **(R1)**, `body_len`/self excluded, golden hex; `request_digest` A-R18 vectors | C0 | passes at `8a23b1d` for the seed preimage; **(R1)** three vectors flip and the goldens are re-pinned (F-R6) |
| M7F-03 | `ControlKey::encode`/`decode` known-answer vectors, families distinct, bad keys refused | C0 | **passes** |
| M7F-04 | envelope encode/decode round trip, golden bytes, unknown mandatory version refused before any body decode | C0 | **passes** |
| M7F-05 | scheduler `(tick, event_id)` order is total; two runs of one recorded stream give byte-identical traces | H1 | owed |
| M7F-06 | one pre-crash image: `ProcessCrash` reopens with `applied` intact, `HostCrash` reopens with `applied == durable`; the two reopened engines differ | M1 | owed; **(R1)** restated on `SurvivingPrefix` (K-F-03) |
| M7F-07 | `FalseDurable` advances no durable watermark | M1 | owed, asserted by kernel-b |
| M7F-08 **(R1)** | `SnapshotRead::version` on a populated `MemoryEngine` snapshot answers the version `Condition::VersionEquals` needs; `EmptySnapshot` answers `None` | M1 | K-F-04 |
| M7F-09 **(R1)** | before any `AdoptAuthority` the dispatcher fills `StepCtx` with the zero triple; after a module emits `AdoptAuthority { g, e, c }` the next `StepCtx` for that partition carries exactly `(g, e, c)`, and another partition's is unchanged | I1 | K-F-05, F-R10. The kernel-side row ("a step under a stale epoch is refused") is kernel-b's ladder row 5 |
| M7F-10 **(R1)** | the H1 control provider records a `ControlInteraction` for every completed effect, with `Terminated { termination, gap }` on a `TerminateWatch`; a `FamilyReload` records `after_termination` as the back-reference to that event | H1 | K-F-06. The oracle row ("reject a reload with `after_termination: None`") is verification's |
| M7F-11 **(R1)** | a `TraceHeader` with `Provenance::{Generated, Reduced, Authored}`, `config: RunManifest`, `partitions`, `topology` round-trips through `write_jsonl`/`read_jsonl` byte-identically; an unknown header field is refused | I1 | K-F-09 |
| M7F-12 **(R1)** | two `snapshot_family(prefix)` reads at one `snapshot_revision` return identical records across an interleaved CAS on that family | H1 | K-F-12 |
| M7F-13 **(R1)** | two nodes submit a create-only CAS on one key; `PlanCas { node: b, .. }` plus `DelayCompletion { node: a, by_millis }` past the grant duration: `b` sees `Committed`, `a`'s completion arrives after expiry and reports what really happened | H1 | K-F-14 |
| M7F-14 **(R1)** | `ControlTime::compare` with `sampled_at` older than `max_age_millis` returns `Uncertain` even when the error bound alone would say `DefinitelyBefore` | C0 | K-F-15 |
| M7F-15 **(R1)** | a CAS with `expected: None` keyed on an `Absent { as_of }` older than the store's current revision after a create is `Conflict`, not `Committed` | H1 | K-F-20 |
| M7F-16 **(R1)** | `copy_of` returns `None` for an authenticated peer whose `node` is a member but whose `boot` differs from the member's | C0 | K-F-21 |
| M7F-17 **(R1)** | `required_regular().count() == 2` on an RF3 configuration (primary + 2 regular + 1 shadow), `== 0` on a lone survivor; `primary()` is the one primary | C0 | K-F-23 |
| M7F-18 **(R1)** | `ShortFlush { through }` makes `Flushed.durable` land at `through`, shorter than `Flush.captured`; the engine's durable watermark is `through` | M1 | K-F-25 |
| M7F-19 **(R1)** | `harness::manifest::resolve` with one budget overridden lists exactly that `BudgetName` in `overridden` and the override's value in `budgets`; with none, `overridden` is empty and `budgets == SPEC_DEFAULTS` | I1 | K-F-27 |
| M7F-20 **(R1)** | `ControlOp::PlanReadUnavailable` makes the next `Get` complete as `Value { outcome: Unavailable }` and the following one as the store's real answer | H1 | ruling F-R3/F-R5 |
| M7F-21 **(R1)** | effect-to-event hop: an effect emitted at tick *t* is delivered to the next step at tick *t*; with `DelayCompletion { by_millis: 50 }` the completion lands at exactly *t + 50* | I1 | B-R23, QC-14 |
| M7F-22 **(R1)** | the three `Capability` trace events and every `rdb-sim` row produce one JSONL file per test under the test log root, readable by DuckDB (Q-F-1) | I1 | K-F-30 |

DuckDB Q-rows the planner can start from: `Q-C0-1` and `Q-C0-2` in `dev-notes.md` §6 (digest
vectors; "did any row log a key or value"), and **Q-F-1 (R1)**: `SELECT testMethod, count(*) FROM
read_json_auto('<target>/test-logs/*/harness/*.jsonl', union_by_name = true) GROUP BY 1` returns
one line per M7F row in `crates/rdb-sim/tests/harness.rs`, which is the K-F-30 evidence.
