//! The event/effect seam: `step(state, event) -> effects`, and nothing else.
//!
//! This is the shape the whole spike rests on (spike §4, §6). A kernel module is a synchronous
//! function from an event to a list of effects. It has no clock to read, no socket to write, no
//! file to open and no thread to wait on. Everything that would be I/O leaves as an [`Effect`]
//! and comes back later as an [`Event`].
//!
//! What that buys: feed the same event log twice and you get the same effects twice, byte for
//! byte. That equality is the only reason a ten-thousand-history campaign can find a fencing
//! bug and hand back a reproducer instead of a shrug.
//!
//! ## What is **not** an effect
//!
//! Reads. [`StepCtx::snapshot`] is a read-only view of an already-published prefix: total,
//! ordered, side-effect free, and therefore not something replay has to reproduce. Making
//! condition evaluation round-trip through the effect queue would buy no determinism and cost
//! six modules a three-state machine each (rdb ADR-0003).
//!
//! ## Serialisation asymmetry
//!
//! [`Event`] is `Serialize`/`Deserialize` because replay reconstructs events from a recorded
//! trace. [`Effect`] is not: it carries [`RdbError`], whose explanatory fields are
//! `&'static str` so no caller bytes can reach them. Effects are observed through
//! [`crate::contracts::trace`], never round-tripped.

use serde::{Deserialize, Serialize};

use crate::contracts::authority::{
    AuthorityEffect, AuthorityEvent, BlockReason, Checkpoint, EvidenceRef, Lineage,
};
use crate::contracts::control::{ControlEffect, ControlEvent};
use crate::contracts::digest::Digest;
use crate::contracts::errors::{Capability, ErrorKind, RdbError};
use crate::contracts::ids::{
    BootId, ConfigVersion, CorrelationId, DurableSeq, EventId, Generation, NodeId, OwnerEpoch,
    PartitionId, RequestIdentity, Revision, Seq,
};
use crate::contracts::ignore::KernelIgnoredReason;
use crate::contracts::membership::{CopyId, PartitionConfig};
use crate::contracts::protection::AdmissionState;
use crate::contracts::publication::{AppliedCandidate, PublicationEffect, PublicationEvent};
use crate::contracts::qualification::QualificationChanged;
use crate::contracts::recovery::{RecoveryEffect, RecoveryEvent, RecoveryResult};
use crate::contracts::storage::{SnapshotRead, StorageEvent, StoreEffect};
use crate::contracts::time::{ControlTime, Tick, TimerEffect, TimerFired};
use crate::contracts::trace::{CapabilityState, ReadServiceOutcome, Version};
use crate::contracts::transport::{SendEffect, TransportEvent};
use crate::contracts::txn::{TxnRequest, TxnResult, TxnStatus};

/// Which kernel module an event is dispatched to, and which one produced an effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ModuleName {
    /// Grants and fencing (package A1).
    Authority,
    /// Conditions, mutations and dedup (package T1).
    Transaction,
    /// Append, ancestry and progress (package R1).
    Replication,
    /// Publication barrier, reads and status (package P1).
    Publication,
    /// Unsafe-age admission and resume (package L1).
    Protection,
    /// Survivor inventory, lineage selection and rebuild (package F1).
    Recovery,
}

impl ModuleName {
    /// The capability this module provides, for reporting it unwired.
    #[must_use]
    pub const fn capability(self) -> Capability {
        match self {
            Self::Authority => Capability::Authority,
            Self::Transaction => Capability::Transaction,
            Self::Replication => Capability::Replication,
            Self::Publication => Capability::Publication,
            Self::Protection => Capability::Protection,
            Self::Recovery => Capability::Recovery,
        }
    }

    /// Every module, in dispatch order. A registry that iterates this cannot forget one.
    pub const ALL: [Self; 6] = [
        Self::Authority,
        Self::Transaction,
        Self::Replication,
        Self::Publication,
        Self::Protection,
        Self::Recovery,
    ];
}

/// Something a client asked for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientEvent {
    /// A transaction.
    Submit(TxnRequest),
    /// A read at the publication barrier.
    Read {
        /// Who is asking.
        identity: RequestIdentity,
        /// The key to read, already encoded.
        key: bytes::Bytes,
    },
    /// A status query for a previously submitted identity.
    Status {
        /// The request being asked about.
        identity: RequestIdentity,
        /// The generation the caller believes the request ran in, if it knows one: the
        /// `generation` of a reply it received, or the `expected_generation` it sent.
        ///
        /// Without it a retired generation cannot be told from one that never held the request,
        /// so the answer is `Unknown` rather than `Expired` (M7A-135; lead ruling A-R63).
        generation: Option<Generation>,
    },
}

/// What happened, addressed to one node and one partition.
///
/// `id` is the total-order tiebreak for events landing on the same [`Tick`]. Spike §6: equal-time
/// events have stable ids, and generated schedules vary their order explicitly rather than
/// leaving it to whatever a hash map felt like.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    /// Strictly increasing within a run.
    pub id: EventId,
    /// When it happens.
    pub at: Tick,
    /// The node it happens on.
    pub node: NodeId,
    /// That node's process lifetime, so a restarted node is not confused with its former self.
    pub boot: BootId,
    /// The partition it concerns.
    pub partition: PartitionId,
    /// Ties this event back to the request that ultimately caused it.
    pub correlation: CorrelationId,
    /// What it is.
    pub kind: EventKind,
}

/// Something happened to the process itself.
///
/// These are ambient facts in a real deployment — the scheduler stopped us, the machine rebooted
/// — and the whole design of this crate is that an ambient fact must arrive as an event or it
/// does not exist. Team kernel-a's monotonic admission rule depends on
/// [`NodeLifecycle::Resumed`] specifically: after a suspension a node's cached grant is invalid
/// whatever its clock now says (spec §7.2), and it cannot notice a suspension by looking at a
/// clock it is not allowed to read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum NodeLifecycle {
    /// The process was suspended and has resumed. Cached grants are invalid; the clock bound is
    /// no longer established until it is re-established.
    Resumed {
        /// How long the process was stopped, in milliseconds of logical time.
        suspended_millis: u64,
    },
    /// The process restarted under a new boot identity. Nothing from the old boot carries over.
    Rebooted {
        /// The new process lifetime.
        boot: BootId,
    },
}

/// The sources an event can come from. There is no other: anything else would be a hidden
/// input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventKind {
    /// A client request.
    Client(ClientEvent),
    /// The process was suspended, resumed or restarted.
    Node(NodeLifecycle),
    /// A frame arrived, or a send failed.
    Transport(TransportEvent),
    /// A batch, flush or snapshot completed or failed.
    Storage(StorageEvent),
    /// A control CAS, read or watch reported back.
    Control(ControlEvent),
    /// A timer fired. May be stale; the kernel checks the version.
    Timer(TimerFired),
    /// Spec §7.2's fallback: an operator or platform mechanism verified that the prior
    /// machine is fenced (lead ruling A-R23; team kernel-a `design.md` §2.2).
    ///
    /// Never synthesised from unreachability; it only ever arrives from outside — operator
    /// tooling at M9, the scenario at M7. It carries the six binding fields so the A1 guard
    /// compares them against its own takeover state rather than against a value filled in
    /// from that state (finding K-A-37).
    ExternalFenceVerified {
        /// The partition being taken over.
        partition: PartitionId,
        /// The lineage the prior owner served.
        prior_generation: Generation,
        /// The epoch the prior owner held.
        prior_owner_epoch: OwnerEpoch,
        /// The prior owner's process lifetime.
        prior_boot_id: BootId,
        /// The control revision at which a linearizable read found the grant frozen.
        control_revision: Revision,
        /// Opaque handle to the external evidence.
        evidence: EvidenceRef,
    },
    /// A fact one kernel module hands another (ask CB-1; lead ruling B-R33 Q-B-1).
    ///
    /// The carrier half of the pair with [`EffectKind::Kernel`]. See [`KernelEvent`].
    Kernel(KernelEvent),
}

/// Kernel-internal events, carried by [`EventKind::Kernel`].
///
/// # Whose variants these are
///
/// Foundation owns the **carrier**; team kernel-b owns the **variants** (ask CB-1: "carrier pair
/// in C0, variants owned by kernel-b"). The shape is the one [`crate::contracts::envelope::AppendReject`]
/// already uses — one variant holding an enum the consuming team fills — rather than a flat
/// variant per kernel fact, which would put roughly 130 rows' worth of names in this file and
/// make every addition a foundation edit.
///
/// `#[non_exhaustive]` is the machine-readable form of that ownership: a consumer's `match` must
/// keep a catch-all until the list settles, so kernel-b can land a variant without breaking
/// every reader at once. It is also what makes the `From` shim in the ask's workaround a seam
/// rather than a rewrite.
///
/// Not `Copy`: see [`KernelEffect`] for the argument, which applies here identically. Kernel-b's
/// plan names the likely first payload — `CopyLost` needs a reason, because a row must be able to
/// tell divergence from lag.
///
/// # Both halves of a carrier, or neither (lead ruling R-S6)
///
/// [`Self::SetAdmission`], [`Self::Recovered`] and [`Self::QualificationChanged`] each have a
/// twin on [`KernelEffect`], because each is *emitted* by one kernel and *delivered* to another:
/// under the dispatcher's model a fact leaves as an [`EffectKind::Kernel`] and arrives as an
/// [`EventKind::Kernel`]. An earlier round named only the event half of the first two, which is
/// a carrier-completeness gap rather than a shape question — the emitting kernel had no way to
/// say the thing the receiving kernel could hear.
///
/// Kernel-b's L1 inputs were closed on 2026-09-22 (lead ruling B-R34): `PeerProgress` and
/// `CopyLost` gained their [`KernelEffect`] twins, and `LocalApplied`, `DurableAdvanced` and
/// `BlockPartition` arrived with both halves. `ConfigChanged` and `TransitionBarrierConfirmed`
/// have the event half only: the membership transition is the control plane's, not a kernel's,
/// so no kernel emits them and the environment delivers them.
///
/// R1's divergence carriers followed (lead ruling B-R36): `DivergenceDetected` and
/// `CopyQuarantined` have both halves, because the catch-up cursor emits them and the tracker
/// receives them. `SnapshotCatchupRequired` is effect-only.
/// The catch-up cursor's `SendEnvelopes`, `CopyAheadOnControl` and `CopyCaughtUp` followed as
/// effects only (lead ruling B-R40); `CopyCaughtUp` reaches F1 through `KernelEvent::Recovery`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum KernelEvent {
    /// A peer reported how far it has taken this lineage.
    PeerProgress {
        /// The reporting peer.
        peer: NodeId,
        /// The highest contiguous sequence it holds.
        contiguous_seq: Seq,
    },
    /// A copy is no longer eligible to serve this partition.
    CopyLost {
        /// Which copy.
        copy: CopyId,
    },
    /// L1 published a new admission state (team kernel-b `design.md` §4.5).
    ///
    /// Delivered to T1, which reads `allow` and `reason` and passes the rest to telemetry. The
    /// payload is the whole eleven-field [`AdmissionState`], not a two-variant verdict: the
    /// exposure and liveness fields are the operator's view of *why* admission is where it is,
    /// and a reply that carried only the verdict would leave them with no way to ask.
    SetAdmission(AdmissionState),
    /// F1 finished a recovery (team kernel-b `design.md` §5.8).
    ///
    /// The only event that rewrites a receiver wholesale. Also the only announcement that a
    /// rebuilt partition has returned to [`crate::contracts::authority::PartitionMode::Active`],
    /// which is why it is emitted a second time at the end of a rebuild rather than only at the
    /// commit.
    ///
    /// **Boxed**, and not as a style choice. [`RecoveryResult`] is 592 bytes — it carries a
    /// [`crate::contracts::authority::FencingProof`], a
    /// [`crate::contracts::membership::PartitionConfig`], an
    /// [`crate::contracts::authority::AuthorityView`] and four `Vec`s — where the next largest
    /// variant of this enum is 112. Unboxed it sets the size of [`KernelEvent`], hence of
    /// [`EventKind`], hence of every [`Event`] in the run queue, including the overwhelming
    /// majority that are a timer firing. A recovery happens once per fencing; a `Timer` happens
    /// every 50 ms.
    Recovered(Box<RecoveryResult>),
    /// R1's publish predicate changed value (team kernel-b `design.md` §4.1).
    ///
    /// Delivered to L1, whose `no_qualifying_secondary` arm fires on this and on nothing else,
    /// and to P1, which uses it as a wake-up in front of its own live recheck.
    QualificationChanged(QualificationChanged),
    /// A fact one of A1's peers hands the authority module (lead ruling A-R25).
    ///
    /// Kernel-a's arm, and the same shape as [`KernelIgnoredReason::Authority`] one level down:
    /// foundation owns the arm, kernel-a owns the variants, and adding one is an edit to
    /// [`AuthorityEvent`] alone. The twin is [`KernelEffect::Authority`], so this is a carrier
    /// with both halves (lead ruling R-S6).
    Authority(AuthorityEvent),
    /// The primary applied a record locally; it is not yet durable on every required copy
    /// (team kernel-b `design.md` §4.1, §4.3). Creates L1's unsafe entry, stamped at the
    /// event's tick. Delivered to L1 and to R1. The twin is [`KernelEffect::LocalApplied`].
    ///
    /// It is also how R1 learns the primary's own history (lead ruling B-R47, closing
    /// B-R36-Q1): the tracker grows its own `received`, `applied` and digest ladder from it,
    /// and bounds every ACK by them. That bound is sound only because of an ordering rule:
    /// **the primary emits this once per sequence, in sequence order, and before that record
    /// is shipped to any copy.** No copy can then acknowledge a record the tracker has not
    /// heard of, so an ACK past the primary's own head is a copy holding a tail the primary
    /// never had, not a race.
    LocalApplied {
        /// The record's sequence.
        seq: Seq,
        /// Its encoded size, summed into [`AdmissionState::outstanding_unsafe_bytes`] (K-B-22).
        bytes: u64,
        /// The record's [`crate::contracts::envelope::ReplicationEnvelope::record_digest`]:
        /// the value R1's ladder holds at `seq`. L1 does not read it.
        record_digest: Digest,
    },
    /// R1's durable views moved (team kernel-b `design.md` §4.3, §4.4): for each active
    /// protection predicate, the highest sequence durable on every copy of it. Delivered to L1.
    DurableAdvanced {
        /// `(config_version, all_durable_through)` for each active predicate.
        per_predicate: Vec<(ConfigVersion, DurableSeq)>,
    },
    /// A new membership configuration was pinned (team kernel-b `design.md` §4.3). Delivered to
    /// L1, which pushes a predicate and never resets an age, and to R1's tracker.
    ///
    /// Event half only: the control plane changes membership, not a kernel.
    ConfigChanged(PartitionConfig),
    /// A membership transition's durable barrier and lineage checkpoint are confirmed, so the
    /// predicate pinned at `config_version` may retire (team kernel-b `design.md` §4.3).
    ///
    /// Event half only, for the same reason as [`Self::ConfigChanged`].
    TransitionBarrierConfirmed {
        /// The retiring predicate's configuration.
        config_version: ConfigVersion,
        /// The barrier the transition was confirmed through.
        through_seq: Seq,
    },
    /// R1 found no durable floor left under the pinned configuration (team kernel-b
    /// `design.md` §3.4 effect 4, §4.4; K-B-46). Delivered to L1 and P1.
    BlockPartition(BlockReason),
    /// Package F1's inputs (team kernel-b `design.md` §5; lead ruling B-R35). Kernel-b owns the
    /// leaf, the same shape as [`Self::Authority`].
    Recovery(RecoveryEvent),
    /// The catch-up cursor proved divergence; the tracker marks it (team kernel-b `design.md`
    /// §3.4, K-B-45; lead ruling B-R36).
    DivergenceDetected {
        /// The copy that disagrees.
        copy: CopyId,
    },
    /// The catch-up cursor saw a `Quarantined` answer (§3.6, K-B-52; lead ruling B-R36). The
    /// tracker consumes it exactly as [`Self::DivergenceDetected`].
    CopyQuarantined {
        /// The quarantined copy.
        copy: CopyId,
    },
    /// T1's applied candidate (team kernel-a `design.md` §1.3). Delivered to P1 only. The
    /// twin is [`KernelEffect::AppliedCandidate`].
    ///
    /// Not to R1, although the kernel-a design names it (lead ruling A-R65). R1 learns the
    /// primary's history from [`Self::LocalApplied`], whose once-per-seq, in-order,
    /// before-shipping rule is what bounds every ACK (B-R47). A second carrier for the same
    /// fact would give R1 two sources that can disagree. T1 emits both.
    ///
    /// **Boxed**, for the reason [`Self::Recovered`] is: the payload carries an
    /// [`crate::contracts::authority::AuthorityDecision`] and a [`TxnResult`], and unboxed it
    /// would set the size of every [`Event`] in the run queue.
    AppliedCandidate(Box<AppliedCandidate>),
    /// P1 published `seq` (team kernel-a `design.md` §4.2 step 4, finding K-A-54). Delivered
    /// to T1. The twin is [`KernelEffect::Published`].
    Published {
        /// The lineage it was published in.
        lineage: Lineage,
        /// The published position.
        seq: Seq,
        /// Its record digest.
        record_digest: Digest,
        /// Whose transaction it was.
        request: RequestIdentity,
    },
    /// P1's environment inputs that no other carrier holds. Kernel-a owns the leaf, the same
    /// shape as [`Self::Authority`].
    Publication(PublicationEvent),
    /// Dedup retention below `below` in `generation` may be dropped (team kernel-a `design.md`
    /// §4.4; ADR-rdb-0004 §4). Computed outside the kernel; delivered to T1.
    ///
    /// Event half only: a watermark is the environment's decision, not a kernel's.
    DedupTrim {
        /// The generation it applies to.
        generation: Generation,
        /// Entries strictly below this may go.
        below: Seq,
    },
    /// Status-index retention below `below` in `generation` may be dropped (team kernel-a
    /// `design.md` §4.4). Delivered to P1. Event half only, as [`Self::DedupTrim`].
    StatusTrim {
        /// The generation it applies to.
        generation: Generation,
        /// Entries strictly below this may go.
        below: Seq,
    },
    /// `generation` is retired: its status answers become `StatusExpired` (team kernel-a
    /// `design.md` §4.4; ADR-rdb-0004 retention boundary). Delivered to T1 and P1. Event half
    /// only, as [`Self::DedupTrim`].
    RetireGeneration {
        /// The retired generation.
        generation: Generation,
    },
}

/// Kernel-internal effects, carried by [`EffectKind::Kernel`].
///
/// The effect half of CB-1's pair. Ownership and the `#[non_exhaustive]` reasoning are the same
/// as [`KernelEvent`]'s.
///
/// # Not `Copy` (ask CB-7)
///
/// This enum carries two promises that contradict each other if both are kept. `#[non_exhaustive]`
/// says *kernel-b may add a variant without foundation's involvement*; `#[derive(Copy)]` says
/// *every variant that will ever exist has a `Copy` payload*. The second silently conditions the
/// first — kernel-b may add a variant provided foundation approves its payload's traits, which is
/// the wait CB-1 was asked to remove. `Copy` is the one that goes, and the cost is already
/// measured: `BlockPartition { reason: BlockReason }` is a variant kernel-b is blocked on today,
/// and it is blocked on this derive rather than on a missing name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum KernelEffect {
    /// The module handled the event and deliberately did nothing.
    ///
    /// Required, not decorative (lead rulings A-R24 and B-R33): an empty effect vector is
    /// indistinguishable from an unhandled event, so "nothing happened" has to be something a
    /// row can assert rather than an absence it has to trust.
    Ignored {
        /// Why nothing was done, in the vocabulary of whichever kernel is speaking.
        ///
        /// Not an [`ErrorKind`] (ask CB-7): that is spec §5.4's client-facing set, and between
        /// them the two kernel teams name 43 distinct reasons, none of which is a client answer.
        /// See [`KernelIgnoredReason`] for whose names are whose.
        reason: KernelIgnoredReason,
    },
    /// An operator-visible condition the kernel wants surfaced.
    Alert {
        /// What the condition is.
        ///
        /// Stays an [`ErrorKind`], and that is a narrowing of what `Alert` means rather than an
        /// oversight: an operator-visible condition *is* client-facing vocabulary, which is what
        /// makes it different from [`Self::Ignored`], where a kernel is talking to itself.
        reason: ErrorKind,
    },
    /// L1 publishes a new admission state (team kernel-b `design.md` §4.5).
    ///
    /// The emitted half of [`KernelEvent::SetAdmission`] (lead ruling R-S6). L1 emits one at
    /// construction — it starts `Paused` — and on every edge thereafter.
    SetAdmission(AdmissionState),
    /// F1 publishes a finished recovery (team kernel-b `design.md` §5.8).
    ///
    /// The emitted half of [`KernelEvent::Recovered`] (lead ruling R-S6), and boxed for the
    /// same reason — see that variant.
    Recovered(Box<RecoveryResult>),
    /// R1 publishes a change in the publish predicate (team kernel-b `design.md` §4.1).
    ///
    /// The emitted half of [`KernelEvent::QualificationChanged`] (lead ruling R-S6). Emitted
    /// from `step(ProgressTracker, ..)` when and only when the predicate changes value, so the
    /// edge stays synchronous with the acknowledgement that caused it.
    QualificationChanged(QualificationChanged),
    /// A fact the authority module hands its peers (lead ruling A-R25).
    ///
    /// The emitted half of [`KernelEvent::Authority`] (lead ruling R-S6), and kernel-a's leaf in
    /// the same sense [`KernelIgnoredReason::Authority`] is one level down. Note that
    /// [`AuthorityEffect::Fact`] — not [`Self::Ignored`] — carries what A1 *did*: the two
    /// vocabularies are separate enums on separate arms, because "deliberately did nothing" and
    /// "installed a lineage" are not the same kind of statement (lead ruling A-R25b).
    Authority(AuthorityEffect),
    /// R1 publishes a peer's progress. The emitted half of [`KernelEvent::PeerProgress`].
    PeerProgress {
        /// The reporting peer.
        peer: NodeId,
        /// The highest contiguous sequence it holds.
        contiguous_seq: Seq,
    },
    /// R1 publishes a lost copy. The emitted half of [`KernelEvent::CopyLost`].
    CopyLost {
        /// Which copy.
        copy: CopyId,
    },
    /// A record was applied locally on the primary. The emitted half of
    /// [`KernelEvent::LocalApplied`].
    LocalApplied {
        /// The record's sequence.
        seq: Seq,
        /// Its encoded size.
        bytes: u64,
        /// The record's digest, as R1's ladder holds it.
        record_digest: Digest,
    },
    /// R1 publishes its durable views. The emitted half of [`KernelEvent::DurableAdvanced`].
    DurableAdvanced {
        /// `(config_version, all_durable_through)` for each active predicate.
        per_predicate: Vec<(ConfigVersion, DurableSeq)>,
    },
    /// R1 blocks the partition. The emitted half of [`KernelEvent::BlockPartition`].
    BlockPartition(BlockReason),
    /// L1 crossed the warn threshold (team kernel-b `design.md` §4.4 `Healthy -> Warn`; plan
    /// row M7B-65).
    ///
    /// Not a [`Self::SetAdmission`]: admission does not change at warn, and `SetAdmission` is
    /// emitted on admission edges only. Operator-facing, so no kernel receives it and it has
    /// no event twin.
    ProtectionWarn {
        /// The oldest record not yet durable on every required copy.
        oldest_unsafe_seq: Seq,
        /// Its age in milliseconds at the evaluation that crossed the threshold.
        age_ms: u64,
    },
    /// Package F1's outputs (team kernel-b `design.md` §5; lead ruling B-R35). The emitted half
    /// of [`KernelEvent::Recovery`] in the sense that both are F1's vocabulary; most variants are
    /// requests to the environment rather than facts for another kernel.
    Recovery(RecoveryEffect),
    /// R1 proved `copy` holds a different history (team kernel-b `design.md` §3.4 rule 9,
    /// §3.6 `Differs`; lead ruling B-R36). From the tracker it opens the divergence vector; from
    /// the catch-up cursor it is the whole effect, routed back to the tracker as its event twin.
    DivergenceDetected {
        /// The copy that disagrees.
        copy: CopyId,
    },
    /// The catch-up cursor saw `copy` answer `Quarantined` (§3.6, K-B-52; lead ruling B-R36).
    /// Routed to the tracker, which consumes it exactly as `DivergenceDetected`.
    CopyQuarantined {
        /// The quarantined copy.
        copy: CopyId,
    },
    /// Re-sending records cannot bring `copy` up to date (§3.4 rule 9 `NotRetained`, §3.6
    /// steps 1 and 1a, the probe cap; lead ruling B-R36). M7 emits the signal only. No kernel
    /// receives it, so it has no event twin.
    SnapshotCatchupRequired {
        /// The copy.
        copy: CopyId,
        /// The primary's head when the need was found.
        barrier: Seq,
    },
    /// Catch-up (team kernel-b `design.md` §3.6 step 2; lead ruling B-R40): the host reads the
    /// canonical envelopes `from..=through` from this primary's log and unicasts them to `copy`,
    /// unchanged. R1 holds no record bytes. `from == through` in M7, because one record is in
    /// flight at a time.
    ///
    /// Also the answer to a `ProbeDigestAt { seq }` (`from == through == seq`): the record carries
    /// its digest, and the receiver's own ladder does the comparison, so no digest frame exists.
    SendEnvelopes {
        /// The copy being caught up.
        copy: CopyId,
        /// The first sequence to send.
        from: Seq,
        /// The last sequence to send, inclusive.
        through: Seq,
    },
    /// The copy answered `NeedLineage`, `NeedConfig` or `UnknownEpoch`: it has seen control this
    /// primary has not, so the cursor stopped (§3.6; lead ruling B-R40).
    CopyAheadOnControl {
        /// The copy that is ahead.
        copy: CopyId,
    },
    /// The ACK that closed the gap: `copy` holds the primary's head (§3.6; lead ruling B-R40).
    /// Emitted once per catch-up. F1's `Rebuilding` consumes it (§5.6a) as
    /// [`crate::contracts::recovery::RecoveryEvent::CopyCaughtUp`], whose fields these mirror
    /// name for name; that routing lands with F1's sim wiring.
    CopyCaughtUp {
        /// The copy that caught up.
        copy: CopyId,
        /// The head it reached.
        head: Seq,
        /// The primary's digest at `head`.
        digest: Digest,
    },
    /// T1's applied candidate, for P1 (team kernel-a `design.md` §1.3). The emitted half
    /// of [`KernelEvent::AppliedCandidate`] (lead ruling R-S6); boxed for the same reason.
    AppliedCandidate(Box<AppliedCandidate>),
    /// P1 published `seq`, for T1. The emitted half of [`KernelEvent::Published`].
    Published {
        /// The lineage it was published in.
        lineage: Lineage,
        /// The published position.
        seq: Seq,
        /// Its record digest.
        record_digest: Digest,
        /// Whose transaction it was.
        request: RequestIdentity,
    },
    /// P1's outputs that no other carrier holds. Kernel-a owns the leaf.
    Publication(PublicationEffect),
    /// T1 or P1 asks A1 for a recheck (team kernel-a `design.md` §3.3, §4.2; lead ruling
    /// A-R63). Routed to A1 as [`AuthorityEvent::Check`] with the same three fields.
    ///
    /// A separate arm, not a variant of [`AuthorityEffect`]: that enum is what A1 emits, and a
    /// request *to* A1 in A1's own effect vocabulary would read as A1 asking itself.
    AuthorityCheck {
        /// Where the recheck happens.
        checkpoint: Checkpoint,
        /// The lineage the caller believes it is operating under.
        lineage: Lineage,
        /// The request the recheck is for.
        correlation: CorrelationId,
    },
}

/// What a module hands back to a client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplyEffect {
    /// A transaction succeeded.
    Transaction {
        /// Who asked.
        identity: RequestIdentity,
        /// What happened.
        result: TxnResult,
    },
    /// A status query was answered.
    Status {
        /// Who asked.
        identity: RequestIdentity,
        /// The answer.
        status: TxnStatus,
    },
    /// A request failed, or its outcome is unknown.
    Failed {
        /// Who asked.
        identity: RequestIdentity,
        /// Why. An `UNKNOWN_OUTCOME` here is not a failure — see
        /// [`RdbError::proves_no_mutation`].
        error: RdbError,
    },
    /// A read was answered (lead ruling F-R7, 2026-09-20).
    ///
    /// Reads are pure lookups against [`StepCtx::snapshot`], but the *answer* still leaves the
    /// kernel as an effect, and it leaves as its own variant so a served read and a published
    /// write are never the same shape in the trace.
    Read {
        /// Who asked.
        identity: RequestIdentity,
        /// How the read was served, in the trace's own vocabulary.
        outcome: ReadServiceOutcome,
        /// The value found, as its version and digest — never bytes, so the effect can be
        /// recorded as it is. `None` when the key is absent or the read was rejected.
        value: Option<(Version, Digest)>,
    },
}

/// One thing the environment must do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Effect {
    /// The request this effect serves, carried through so its completion event can be tied back.
    pub correlation: CorrelationId,
    /// Which module emitted it.
    pub from: ModuleName,
    /// The partition it concerns.
    pub partition: PartitionId,
    /// What to do.
    pub kind: EffectKind,
}

/// The things a kernel module may ask for.
///
/// The first five match [`EventKind`] one for one, and that is deliberate: every request has
/// exactly one completion channel, and none of them is "return a value". The sixth,
/// [`Self::AdoptAuthority`], has no completion event because it asks the environment for nothing.
/// It is the kernel *declaring* which lineage it now serves, and the dispatcher consumes it
/// mechanically (lead ruling F-R10, 2026-09-20).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EffectKind {
    /// Send a frame.
    Send(SendEffect),
    /// Commit, flush, snapshot or release.
    Store(StoreEffect),
    /// CAS, read, watch or reload a control record.
    Control(ControlEffect),
    /// Arm or cancel a timer.
    Timer(TimerEffect),
    /// Answer a client.
    Reply(ReplyEffect),
    /// The kernel has adopted a new lineage, epoch or membership pin for this partition.
    ///
    /// The **only** thing that changes [`StepCtx::generation`], [`StepCtx::owner_epoch`] and
    /// [`StepCtx::config_version`]. The dispatcher stores the last adopted triple per partition
    /// and fills the next context from it — a lookup, never a rule. Which CAS outcome means "the
    /// epoch is now N" is a protocol decision, and it stays in the module that emits this
    /// (finding K-F-05: the alternative was `rdb-sim` deciding it, which the charter forbids).
    AdoptAuthority {
        /// The partition adopted for (lead ruling A-R23: per partition, not per dispatcher).
        /// A1 serves many partitions from one grant and names each one it adopts.
        partition: PartitionId,
        /// The lineage now served.
        generation: Generation,
        /// The epoch now believed current.
        owner_epoch: OwnerEpoch,
        /// The membership pin now in force.
        config_version: ConfigVersion,
    },
    /// A fact this module hands another kernel module (ask CB-1; lead ruling B-R33 Q-B-1).
    ///
    /// The carrier half of the pair with [`EventKind::Kernel`]. See [`KernelEffect`]. Like
    /// [`Self::AdoptAuthority`] it has no completion event: it asks the environment for nothing.
    Kernel(KernelEffect),
}

/// The spec's timing and retention numbers, resolved once.
///
/// Every value here is a threshold the spec states in milliseconds. They live in one struct
/// because a scenario may legitimately shrink them to keep a history short, and a module that
/// hard-coded `2000` would silently ignore that — and because a run's resolved budgets are part
/// of the result manifest (spike §7; [`crate::contracts::trace::RunManifest`] names each field
/// through [`crate::contracts::trace::BudgetName`], one member per field). The clock sample age
/// is deliberately not here: it is the caller's budget to
/// [`crate::contracts::time::ControlTime::compare`], and kernel-a's A1 passes its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Budgets {
    /// Unsafe-age warning threshold (spec §6.2: 1,000 ms).
    pub warn_age_millis: u64,
    /// Unsafe-age pause threshold (spec §6.2: 2,000 ms, rejecting within a further 100 ms).
    pub pause_age_millis: u64,
    /// Lag that counts as healthy while resuming (spec §6.2: 250 ms).
    pub resume_lag_millis: u64,
    /// How long healthy lag must hold before resuming (spec §6.2: 5,000 ms).
    pub resume_hold_millis: u64,
    /// Grant duration (spec §7.2: 3,000 ms).
    pub grant_millis: u64,
    /// Grant renewal interval (spec §7.2: 500 ms).
    pub renew_millis: u64,
    /// The longest scheduler suspension that does **not** end a grant (lead ruling A-R32;
    /// team kernel-a `design.md` §2.4, the `Held | ProcessResumed` row). 500 ms.
    ///
    /// Compared against [`NodeLifecycle::Resumed::suspended_millis`], which is why it is spelled
    /// `_millis` rather than `_ticks`: the event and its own threshold are in the same unit and
    /// no conversion sits between them.
    ///
    /// A **separate** field rather than a reuse of [`Self::renew_millis`], although the spec
    /// value is the same number. The two have no reason to move together, and sharing one would
    /// make an operator's renewal tuning silently retune suspend detection.
    pub resume_gap_tolerance_millis: u64,
    /// How often A1 expects a fresh bounded-clock sample (team kernel-a `design.md` §2.1). 500 ms.
    ///
    /// Its own threshold and **not** a reuse of [`Self::renew_millis`] (finding K-A-07), although
    /// the spec gives both the same number: sampling the clock and renewing a grant are
    /// independent rates, and an operator who retunes one has not asked to retune the other.
    pub clock_sample_period_millis: u64,
    /// The oldest clock sample A1 will admit on. Four sample periods, so one late sample is not
    /// an event.
    ///
    /// A sample older than this **denies** and never fences (lead ruling A-R12): the next fresh
    /// sample restores admission. At two periods every ordinary scheduling hiccup would suspend
    /// admission for no safety gain.
    pub max_sample_age_millis: u64,
    /// Assumed bound on the local tick rate's error against UTC, in parts per million. 500.
    ///
    /// Assumption 3 of rDB ADR-rdb-0007 §4. It is what lets an *aged* sample still be usable: the
    /// sample's own error covers the instant it was taken, and this covers the interval since.
    ///
    /// Not a duration, and the only member of this struct that is not milliseconds — which is why
    /// it is spelled `_ppm`. [`crate::contracts::trace::BudgetName`] reads every member as a
    /// `u64`, so the unit lives in the name or nowhere.
    pub clock_rate_ppm: u64,
    /// Verified maximum clock error, spec §7.2's `epsilon` (100 ms).
    pub clock_error_millis: u64,
    /// Dispatch margin, spec §7.2's `delta` (100 ms).
    pub dispatch_margin_millis: u64,
    /// Minimum dedup retention (spec §5.3: 24 h).
    pub dedup_retention_millis: u64,
    /// Survivor discovery window after fencing (spec §8.1: 2,000 ms).
    pub discovery_window_millis: u64,
}

impl Budgets {
    /// The values the specification states. A scenario may override any of them; a module may
    /// not assume these.
    pub const SPEC_DEFAULTS: Self = Self {
        warn_age_millis: 1_000,
        pause_age_millis: 2_000,
        resume_lag_millis: 250,
        resume_hold_millis: 5_000,
        grant_millis: 3_000,
        renew_millis: 500,
        resume_gap_tolerance_millis: 500,
        clock_sample_period_millis: 500,
        max_sample_age_millis: 2_000,
        clock_rate_ppm: 500,
        clock_error_millis: 100,
        dispatch_margin_millis: 100,
        dedup_retention_millis: 24 * 60 * 60 * 1_000,
        discovery_window_millis: 2_000,
    };
}

/// Everything a module is allowed to know that did not arrive in the event.
///
/// Deliberately small. Anything added here is an ambient input, and every ambient input is a way
/// for two runs of the same event log to diverge.
///
/// Three of these fields are not ambient at all: `generation`, `owner_epoch` and
/// `config_version` are whatever the kernel last declared through
/// [`EffectKind::AdoptAuthority`] for this partition, copied back by the dispatcher. Before the
/// first adoption they are zero, which no grant ever names.
pub struct StepCtx<'a> {
    /// Current logical time.
    pub now: Tick,
    /// The bounded estimate of the authority clock (spec §7.2).
    pub control_time: ControlTime,
    /// The node stepping.
    pub node: NodeId,
    /// Its process lifetime.
    pub boot: BootId,
    /// The partition being stepped.
    pub partition: PartitionId,
    /// The lineage it is serving — the last [`EffectKind::AdoptAuthority`] for this partition.
    pub generation: Generation,
    /// The owner epoch it believes is current — the last [`EffectKind::AdoptAuthority`].
    pub owner_epoch: OwnerEpoch,
    /// The membership configuration the required-copy predicate is pinned to — the last
    /// [`EffectKind::AdoptAuthority`].
    pub config_version: ConfigVersion,
    /// A read-only view at the published prefix.
    pub snapshot: &'a dyn SnapshotRead,
    /// The resolved thresholds for this run.
    pub budgets: &'a Budgets,
}

/// A kernel module: synchronous, deterministic, and the only place protocol decisions are made.
///
/// Implementations must not read a clock, call an allocator-order-dependent iterator, consult
/// the environment, or panic. An unimplemented path returns
/// [`RdbError::Unavailable`] — spike §8 permits an explicit unavailable result and forbids a
/// fake success, and a `todo!()` would abort the campaign runner instead of letting it report
/// the gap.
pub trait Module {
    /// Which module this is.
    fn name(&self) -> ModuleName;

    /// Whether this module is wired in this build.
    ///
    /// Non-mutating, and answered positively: the default is
    /// [`CapabilityState::Unavailable`], and a module says `Wired` by overriding this, never by
    /// happening not to return `Unavailable` from a probe step (finding K-F-10 — the probe
    /// stepped every module with an event the protocol never sent, mutated whatever state it had
    /// and discarded the effects). The harness emits the answer as
    /// [`crate::contracts::trace::TraceKind::Capability`] at trace start.
    fn capability(&self) -> CapabilityState {
        CapabilityState::Unavailable
    }

    /// Handle one event and return everything the environment must do, in order.
    ///
    /// A plain `Vec<Effect>` (lead ruling A-R17, 2026-09-20), matching `KvState::apply_with_effects`
    /// in the control plane. An `Effects` wrapper was considered and dropped: it bought a
    /// `push`/`take` API and an implied ordering rule over a type that already has both, and it
    /// made every module signature differ from the one pattern this workspace already uses.
    /// Order is still part of the contract — the environment executes the vector front to back,
    /// and a module that reorders on a replay has broken determinism.
    ///
    /// # Errors
    ///
    /// Any [`RdbError`]. An error is a protocol decision like any other: the dispatcher records
    /// it in the trace and the oracle checks it, so returning one is never a shortcut around
    /// emitting the right effects. A step that returns an error returns no effects.
    fn step(&mut self, ctx: &StepCtx<'_>, event: &Event) -> Result<Vec<Effect>, RdbError>;
}
