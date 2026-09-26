//! The authority seam: what A1 decides, what it publishes, and the partition mode every kernel
//! matches on.
//!
//! Contract types only (lead ruling A-R23, 2026-09-20). No rule lives here: when a grant is
//! held, when a fence fires and how `valid_through_tick` is computed are package A1's
//! (team kernel-a `design.md` §2), and when a partition is `Blocked` is package R1's and F1's
//! (team kernel-b `design.md` §3.5, §5.8). The shapes are here because three kernels match on
//! them, and two private enums with the same variants are two enums that drift.
//!
//! Distinct from [`crate::authority`], which is the A1 kernel module itself.

use serde::{Deserialize, Serialize};

use crate::contracts::errors::ErrorKind;
use crate::contracts::event::EventKind;
use crate::contracts::ids::{
    AuthorityGeneration, BootId, ConfigVersion, CorrelationId, Generation, GrantId, NodeId,
    OwnerEpoch, PartitionId, Revision,
};
use crate::contracts::membership::CopyId;
use crate::contracts::time::Tick;

/// Where a recheck happens (spec §7.3 step 6; team kernel-a `design.md` §1.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Checkpoint {
    /// Before the request enters the partition queue.
    Admission,
    /// Immediately before the storage batch is dispatched.
    StorageDispatch,
    /// Before the applied prefix is published.
    Publication,
    /// Before the client reply is sent.
    Reply,
    /// Declared for spec §11; unused in M7.
    OutboxDispatch,
}

/// The lineage a caller believes it is operating under (team kernel-a `design.md` §1.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Lineage {
    /// The partition.
    pub partition: PartitionId,
    /// Its history incarnation.
    pub generation: Generation,
    /// The owner epoch inside that incarnation.
    pub owner_epoch: OwnerEpoch,
}

/// Why A1 denied (team kernel-a `design.md` §1.2). A closed set; spec §5.4's error mapping is
/// total over it.
///
/// **Sixteen variants, of which ten are reachable as a fence and six are deny-only** (lead
/// rulings A-R33b and A-R42). Deny-only: [`Self::NoGrant`], [`Self::ExpiryUnproven`],
/// [`Self::ClockModeUnbounded`], [`Self::ClockSampleStale`], [`Self::SelfFenced`],
/// [`Self::ControlUnavailable`]. The split is a claim a row asserts by value, and it can come
/// out wrong in both directions — a reason that silently becomes fenceable, or one that stops
/// being — so move a name across it only with a ruling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum DenyReason {
    /// No grant is held.
    NoGrant,
    /// The planner froze the exact revision (spec §7.3 step 1).
    Frozen,
    /// A durable drain proof was recorded.
    Revoked,
    /// This partition's epoch specifically was revoked (spec §7.3 step 4).
    EpochRevoked,
    /// Our own conservative expiry crossed.
    Expired,
    /// A renewal's outcome is unknown; never extend on hope (rEtcd ADR-0015).
    ExpiryUnproven,
    /// The clock bound is not established, invalid, above the configured bound, or jumped
    /// backwards. **Fences** — every one of these is an ADR-rdb-0007 §3 trigger.
    ClockUnbounded,
    /// The node is **configured** with [`crate::authority::clock::ClockMode::Unbounded`], so no
    /// error bound can be established at all. Denies, never fences (lead ruling A-R42).
    ///
    /// Split out of [`Self::ClockUnbounded`] rather than folded into it, and deliberately spelled
    /// next to it so the difference is read rather than assumed. `ClockUnbounded` is an
    /// *observation about a sample*; this is a *configuration input*, and spec §7.2 says a node
    /// that cannot establish a bound "stops accepting requests" — it does not fence, because
    /// there is no grant-ending event. [`crate::authority::clock::ClockMode`]'s own doc has said
    /// that since it landed, while [`crate::authority::clock::utc_ok`] returned one value for
    /// both and `Authority::revalidate` fenced on it, so a configuration change terminally fenced
    /// a healthy primary.
    ///
    /// That collapse is the one [`crate::authority::clock::ClockFault`] exists to prevent one
    /// function over — *"collapsing them terminally fenced a healthy primary on one late
    /// sample"* — and it is the same reading the kernel-a architect rejected once already, for
    /// the **renewal** guard, where the fix was to guard on `e_new is None`: the *sample*, never
    /// the *mode*. The fence guard kept the rejected reading until A-R42.
    ///
    /// Nothing is lost by not fencing. With the mode unbounded
    /// [`crate::authority::clock::e_new`] is `None`, so no renewal CAS is dispatched, `E` stands
    /// still, and the grant ends at its own expiry — ADR-rdb-0007 §3's natural-expiry path,
    /// already relied on for [`Self::ClockSampleStale`].
    ClockModeUnbounded,
    /// The last clock sample is older than the maximum sample age. Denies, never fences
    /// (lead ruling A-R12).
    ClockSampleStale,
    /// The scheduler reported a resume gap.
    ProcessSuspended,
    /// The grant was issued to another boot of this node.
    BootMismatch,
    /// The cluster authority generation moved.
    AuthorityGenerationChanged,
    /// The partition lineage moved under us.
    GenerationChanged,
    /// The kernel fenced itself.
    SelfFenced,
    /// Control quorum was lost (spec §7.2, last paragraph).
    ControlUnavailable,
    /// Local storage failed and the partition is fenced (spec §5.2 step 3).
    LocalStorageFenced,
}

/// How an authority check came out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Verdict {
    /// Proceed under the named lineage.
    Admit,
    /// Do not proceed, for this reason. Never retried inside the kernel.
    Deny(DenyReason),
}

/// One authority check at one checkpoint — `A1 -> T1/P1/F1` (team kernel-a `design.md` §1.2).
///
/// A snapshot, valid only for the tick it names, carried forward by the caller so a later
/// checkpoint can prove the lineage did not move.
///
/// `PartialOrd`, `Ord` and `Hash` are derived for the carrier's sake, exactly as
/// [`AuthorityView`]'s are and for the same reason: this decision rides inside
/// [`AuthorityEvent::Answer`] and [`AuthorityEffect::Answer`], hence inside
/// [`crate::contracts::event::KernelEvent`] and [`crate::contracts::event::KernelEffect`], which
/// derive all three. **Nothing reads the ordinal.** Freshness is [`Self::authority_seq`],
/// compared by hand; sameness is [`Self::same_lineage_as`]. Ordering two decisions
/// whole-struct would compare `owner` first and mean nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AuthorityDecision {
    /// The node holding the grant.
    pub owner: NodeId,
    /// Its process lifetime.
    pub boot: BootId,
    /// The grant.
    pub grant: GrantId,
    /// The cluster authority generation the grant was issued under.
    pub authority_generation: AuthorityGeneration,
    /// The partition lineage decided for.
    pub lineage: Lineage,
    /// `E` from the grant record, as a control-time estimate in milliseconds. Evidence only;
    /// never a local timer.
    pub expiry_utc_ms: i64,
    /// When the decision was taken. Trace data only; freshness is [`Self::authority_seq`].
    pub decided_at: Tick,
    /// A1's monotone authority counter at the moment of decision (lead ruling A-R23;
    /// team kernel-a `design.md` §2.1). Bumped on every fence and every grant, epoch or
    /// generation change. What a consumer compares, not `decided_at`.
    pub authority_seq: u64,
    /// The checkpoint this answers.
    pub checkpoint: Checkpoint,
    /// The request it answers.
    pub correlation: CorrelationId,
    /// The answer.
    pub verdict: Verdict,
}

impl AuthorityDecision {
    /// Whether this decision and `earlier` describe the same accepted lineage.
    #[must_use]
    pub fn same_lineage_as(&self, earlier: &Self) -> bool {
        self.grant == earlier.grant
            && self.boot == earlier.boot
            && self.authority_generation == earlier.authority_generation
            && self.lineage == earlier.lineage
    }

    /// Whether the verdict is [`Verdict::Admit`].
    #[must_use]
    pub const fn admitted(&self) -> bool {
        matches!(self.verdict, Verdict::Admit)
    }

    /// The same comparison against the pushed view the consumer admitted under.
    #[must_use]
    pub fn same_lineage_as_view(&self, view: &AuthorityView) -> bool {
        self.grant == view.grant_id
            && self.boot == view.boot_id
            && self.authority_generation == view.authority_generation
            && self.lineage == view.lineage
    }
}

/// The authority state A1 pushes to R1, T1 and P1 (team kernel-a `design.md` §1.7).
///
/// For R1 it is the secondary's epoch gate; for T1 the synchronous admission checkpoint; for
/// P1 the `authority_seq` reference. A consumer keeps the view with the highest
/// [`Self::authority_seq`] it has seen and rejects any answer below it.
///
/// `PartialOrd`, `Ord` and `Hash` are derived for the carrier's sake, not because anything
/// reads the ordinal: this view rides inside [`crate::contracts::recovery::CommittedRoot`],
/// hence inside [`crate::contracts::recovery::RecoveryResult`], hence inside
/// [`crate::contracts::event::KernelEvent`], which derives all three. Freshness is still
/// [`Self::authority_seq`], compared by hand; never the whole-struct ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AuthorityView {
    /// The lineage served.
    pub lineage: Lineage,
    /// The grant held.
    pub grant_id: GrantId,
    /// The boot it was issued to.
    pub boot_id: BootId,
    /// The cluster authority generation, so R1 can refuse a stale-cluster append.
    pub authority_generation: AuthorityGeneration,
    /// The membership pin in force.
    pub config_version: ConfigVersion,
    /// A1's monotone counter at publication (lead ruling A-R23).
    pub authority_seq: u64,
    /// Hard deny boundary, not a hint. Past this tick the view is worthless.
    pub valid_through_tick: Tick,
    /// The deny reason that applies once `valid_through_tick` is passed — the reason whose
    /// horizon bound first (lead ruling A-R23; team kernel-a `design.md` §1.7).
    pub past_horizon: DenyReason,
}

/// Opaque handle to external fence evidence (team kernel-a `design.md` §1.1, §2.6).
///
/// The kernel cannot verify the external fact; it verifies that the evidence names this
/// partition, this prior lineage, this prior boot and a frozen grant record at a revision.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EvidenceRef(
    /// The handle bytes.
    pub [u8; 32],
);

impl core::fmt::Debug for EvidenceRef {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "EvidenceRef({:02x}{:02x}..)", self.0[0], self.0[1])
    }
}

/// `A1 -> F1`: the only door into recovery (team kernel-a `design.md` §1.7).
///
/// Team kernel-b makes a proof the **only** transition out of F1's `Idle`, so "reachability does
/// not elect a primary" (spec §7.3) becomes unrepresentable rather than reviewed.
///
/// # Honesty rule (ADR-rdb-0007 §1, closes K-A-17)
///
/// This names an *authorization to take over*, not evidence that the prior node cannot write.
/// [`Revocation::ExpiryProven`] holds only under ADR-rdb-0007 §4's assumptions, which nothing in
/// this repository verifies at runtime. The names are kept because the seam is settled with team
/// kernel-b (lead ruling A-R8), so the discipline carries the weight instead: **log fields, trace
/// facts and test names say `takeover_authorized`, never `fenced` or `proven`.**
///
/// # No `sender`, and no `Copy`
///
/// There is deliberately no `sender` field (lead ruling B-R31): naming a source is
/// [`FenceCredential`]'s whole reason to exist, and F1 mints one credential per transfer
/// *source* from one proof — see [`Self::credential_for`]. Every field here is `Copy`, but the
/// derive is withheld for the reason ask CB-7 gives on
/// [`crate::contracts::event::KernelEffect`]: [`Revocation`] is kernel-a's to widen, and a
/// `Copy` derive would silently condition that freedom on the payload traits of a variant
/// nobody has written yet.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct FencingProof {
    /// The partition being taken over.
    pub partition: PartitionId,
    /// The lineage the prior owner served.
    pub prior_generation: Generation,
    /// The epoch the prior owner held.
    pub prior_owner_epoch: OwnerEpoch,
    /// The grant the prior owner held.
    pub prior_grant_id: GrantId,
    /// The prior owner's process lifetime.
    pub prior_boot_id: BootId,
    /// Which of the three revocation routes completed.
    pub revocation: Revocation,
    /// The rEtcd revision of the linearizable read this proof was derived from (spec §7.2).
    pub control_revision: Revision,
    /// When kernel-a took the fencing decision.
    ///
    /// Audit and spec §7.2's inequality only. **Never a deadline base** (finding K-B-14): F1
    /// anchors its discovery window to the tick at which it *received* the proof, because this
    /// stamp can be arbitrarily older than its arrival — control propagation, a queued effect,
    /// a replayed proof in a test — and anchoring to it can close discovery before a single
    /// inventory is collected.
    pub decision_tick: Tick,
}

impl FencingProof {
    /// The wire-carryable credential for one transfer out of this proof (team kernel-b
    /// `design.md` §5.4, §5.6; findings K-B-36, K-B-42; lead ruling B-R31).
    ///
    /// F1 mints **one credential per transfer source**, filling `sender` with the `from` of each
    /// `CatchUp` or `CatchUpBeforeGrant`, and ships it to that copy inside the effect. The
    /// constructor exists so the four copied bindings are copied rather than re-derived at each
    /// call site; `sender` is the only field a caller supplies, which is also the only field
    /// this proof does not hold.
    #[must_use]
    pub const fn credential_for(&self, sender: CopyId) -> FenceCredential {
        FenceCredential {
            partition: self.partition,
            prior_generation: self.prior_generation,
            prior_owner_epoch: self.prior_owner_epoch,
            control_revision: self.control_revision,
            sender,
        }
    }
}

/// Which of spec §7.3's three routes revoked the prior owner (team kernel-a `design.md` §1.7).
///
/// Kernel-a's to widen: a fourth route is an edit to this enum and to nothing else.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Revocation {
    /// Spec §7.3 step 2. Counts only because a restart cannot restore that epoch.
    DurableDrain {
        /// The revision at which the drain was acknowledged.
        ack_revision: Revision,
    },
    /// Spec §7.3 step 3, under the bounded-clock contract.
    ExpiryProven {
        /// `E` from the frozen grant record, read linearizably at
        /// [`FencingProof::control_revision`].
        frozen_expiry_utc_ms: i64,
        /// The authority's own control-time estimate, in the same units as
        /// `frozen_expiry_utc_ms`. Without it `C_auth > E + epsilon + delta` cannot be
        /// re-derived by a reviewer or by the oracle — `authority_tick` is monotonic, not UTC.
        authority_utc_ms: i64,
        /// The local tick the estimate was taken at.
        authority_tick: Tick,
        /// Spec §7.2's `epsilon`, the verified maximum clock error, in milliseconds.
        epsilon_ms: u32,
        /// Spec §7.2's `delta`, the dispatch margin, in milliseconds.
        delta_ms: u32,
    },
    /// Spec §7.2's fallback. Never inferred from unreachability.
    ///
    /// Every field except `evidence_ref` is a **binding**: the kernel cannot verify the external
    /// fact, but it can and does verify that the evidence names this partition, this prior
    /// lineage, this prior boot, and a grant record a linearizable read found **frozen** at
    /// `control_revision` (closes K-A-16).
    ///
    /// Field for field the same six, in the same order and at the same types, as the landed
    /// [`EventKind::ExternalFenceVerified`]. That is not a coincidence to be maintained by hand
    /// — see [`Self::from_external_fence_verified`].
    ExternalFence {
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
        /// Opaque handle to the external evidence. The one field that is not a binding.
        evidence_ref: EvidenceRef,
    },
}

impl Revocation {
    /// Build [`Self::ExternalFence`] from the event that carries the claim, or `None` for any
    /// other event (finding K-A-37).
    ///
    /// The A1 guard compares the six binding fields against its own takeover state rather than
    /// against values filled in *from* that state. This constructor is how that stays true: the
    /// six fields are copied across in one place, so a reader checking the binding reads one
    /// function instead of auditing every call site for a field somebody re-derived. The only
    /// difference between the two spellings is the last field's name — `evidence` on the event,
    /// `evidence_ref` here — and it is bridged here and nowhere else.
    #[must_use]
    pub fn from_external_fence_verified(event: &EventKind) -> Option<Self> {
        match event {
            EventKind::ExternalFenceVerified {
                partition,
                prior_generation,
                prior_owner_epoch,
                prior_boot_id,
                control_revision,
                evidence,
            } => Some(Self::ExternalFence {
                partition: *partition,
                prior_generation: *prior_generation,
                prior_owner_epoch: *prior_owner_epoch,
                prior_boot_id: *prior_boot_id,
                control_revision: *control_revision,
                evidence_ref: *evidence,
            }),
            _ => None,
        }
    }
}

/// The wire-carryable part of a [`FencingProof`], plus the source of one transfer (team kernel-b
/// `design.md` §1.3).
///
/// Carried on `RecoveryAppend` and minted by [`FencingProof::credential_for`].
///
/// # Its smallness is the security property
///
/// > "Keeping the credential smaller than the proof means a receiver cannot start re-deriving
/// > authority decisions from it." — team kernel-b `design.md` §1.3
///
/// So the three fields it does **not** carry are as load-bearing as the five it does, and none
/// of them may be added back as a convenience:
///
/// * **No `decision_tick` and no [`Revocation`].** Those are kernel-a's evidence for *granting*
///   the proof, not a receiver's evidence for *accepting* a record. A receiver holding them can
///   re-run the authority decision, and a second, weaker copy of that decision is exactly what
///   this seam exists to prevent.
/// * **No `prior_grant_id`** (finding K-B-35, lead ruling B-R24). A receiver's
///   [`AuthorityView`] is derived from `partitions/{id}`, which holds no grant id, so a
///   receiver has nothing to compare one against without the two-key join spec §7.1 forbids.
///   `prior_owner_epoch` plus the monotone `control_revision` is the whole "your fence is at
///   least as new as anything I have seen" check.
///
/// `Copy` is derived here and withheld on [`FencingProof`], and the asymmetry is the point:
/// this shape is closed at five fields by a stated security property, so promising `Copy` costs
/// no future freedom.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct FenceCredential {
    /// The partition the transfer belongs to.
    pub partition: PartitionId,
    /// The lineage the prior owner served.
    pub prior_generation: Generation,
    /// The epoch the prior owner held.
    pub prior_owner_epoch: OwnerEpoch,
    /// The control revision the fence was read at. Monotone, so a receiver can refuse a fence
    /// older than anything it has already seen.
    pub control_revision: Revision,
    /// The copy F1 designated as the source of **this** transfer (findings K-B-36, K-B-42; lead
    /// ruling B-R31).
    ///
    /// Row 6R' binds the transport's `authenticated_peer` to this field, so a third member
    /// replaying a captured credential fails the equality while the designated source is
    /// admitted. It is not the recovering node: spec §8.3 says the records that fill a lagging
    /// holder come from the *selected* holder, which need not be the node F1 runs on, and round
    /// 2's `recoverer` spelling killed every legitimate `CatchUpBeforeGrant` with
    /// `NOT_A_MEMBER` for exactly that reason.
    pub sender: CopyId,
}

/// Why a partition is [`PartitionMode::Blocked`] (lead ruling A-R23; team kernel-b
/// `design.md` §3.5, §5.1, team kernel-a `design.md` §4.1).
///
/// # Six reasons, and why none of them merge
///
/// [`PartitionMode::Blocked`] is terminal until an operator acts or a fresh fence arrives
/// (team kernel-b `design.md` §5.1, "`Blocked` staying terminal-until-operator-or-refence is
/// deliberate"). Nothing on the data path clears it, so the reason is not a hint alongside a
/// retry — **it is the entire content of the alert an operator reads**, and two reasons that
/// lead to different operator actions may not be spelled as one.
///
/// The design makes that argument explicitly for the `Unavailable`/`Unknown` pair (team
/// kernel-b `design.md` §5.1, the paragraph under the CAS table: "they are different operator
/// stories"). It applies identically to [`Self::OvertakenByPeer`] and [`Self::CasContention`],
/// which arrive on the same `CasOutcome::Conflict` arm: one says *someone else is the owner
/// now*, the other says *the control plane is contended and you are not the owner yet*. The
/// first is answered by looking at who won; the second by looking at what else is proposing.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum BlockReason {
    /// R1 found a divergence that leaves no durable floor under the pinned configuration
    /// (lead ruling B-R26): qualification can never return under this configuration, and the
    /// only exit is an operator removing the diverged copies from membership and fencing.
    ///
    /// The one reason on this list with a client answer, because it is the one that reaches
    /// admission — see [`Self::client_error_kind`].
    DivergenceRequiresOperator {
        /// The copies whose history diverged, so the alert names them.
        diverged: Vec<CopyId>,
    },
    /// Somebody else recovered this partition first. Two sources state it: the recovery or
    /// activation CAS came back `Conflict` and the re-read found a record with a **newer
    /// epoch, or byte-identical to our own proposal** (team kernel-b `design.md` §5.1, CAS
    /// table `Conflict` row; lead ruling B-R45 F-g), or discovery found a survivor whose root
    /// is newer than the plan's anchor, so the plan itself is stale (lead ruling B-R41 F-b).
    ///
    /// Identical bytes are a peer, never our own landing: `Conflict` means our comparison
    /// failed, and exactly one racer commits (see [`crate::contracts::control::CasOutcome`]).
    /// Two recoverers on one fence that chose the same leader write the same record, and the
    /// loser must not report a second `Recovered` for the winner's revision.
    ///
    /// Distinct from [`Self::CasContention`], which is the same `Conflict` arm with the record
    /// *unchanged*. The operator story is different in the part that matters: here the
    /// partition has an owner and it is not us, so the question is whether that owner is the
    /// one the operator wanted; there the partition has no new owner at all. This node
    /// re-enters recovery only through a fresh [`FencingProof`], never by retrying the CAS.
    OvertakenByPeer,
    /// The recovery or activation CAS came back `Conflict`, and the re-read found neither a
    /// record with a newer epoch nor one identical to our proposal: an older or equal epoch
    /// with other content, `Absent`, or bytes that do not decode (team kernel-b `design.md`
    /// §5.1, CAS table `Conflict` row; lead rulings B-R41 F-f, B-R45 F-g). There is no
    /// re-propose: a record F1 cannot account for is not one it writes over.
    ///
    /// Distinct from [`Self::OvertakenByPeer`]: nobody has taken the partition, so an operator
    /// is looking for whatever else is proposing against `partitions/{id}` rather than for a
    /// new owner. Also distinct from [`Self::ControlUnavailable`] — the control plane answered,
    /// and its answer was "no".
    CasContention,
    /// The activation CAS came back `Unavailable`: the control plane could not be reached, so
    /// the CAS may or may not have landed (team kernel-b `design.md` §5.1, CAS table
    /// `Unavailable` row). **Never retried blind.**
    ///
    /// Distinct from [`Self::ControlUnknown`], and deliberately not merged with it: this one
    /// says *the control plane is down and the partition is waiting on it*. An operator seeing
    /// it looks at the control plane's health first, because until it returns there is nothing
    /// to read.
    ///
    /// Distinct in turn from [`DenyReason::ControlUnavailable`], which is A1 refusing one
    /// authority check while control quorum is lost. That is a deny on the data path and it
    /// clears by itself; this is a partition mode and it does not.
    ControlUnavailable,
    /// The activation CAS came back `Unknown`: the request was sent and the response was lost
    /// (team kernel-b `design.md` §5.1, CAS table `Unknown` row). **Never retried blind** — the
    /// CAS is *more* likely to have landed here than under [`Self::ControlUnavailable`], which
    /// is exactly what makes a blind retry worse rather than better.
    ///
    /// The operator story that separates it: this one says *the control plane may already have
    /// a new owner recorded*. The first thing to do is read `partitions/{id}`, not restart
    /// anything. Both reasons resolve the same way in the end — a re-read once the control
    /// plane returns, which needs a fresh fence — but an operator has to know which of the two
    /// they are looking at before they can decide that.
    ControlUnknown,
    /// The recovery committed with **zero** eligible regular copies holding the barrier (team
    /// kernel-b `design.md` §5.6 mode table, the `0` row: "operator restore; outside automatic
    /// recovery").
    ///
    /// Distinct from every control-plane reason above: the control plane worked and said what
    /// it was asked. There is simply no copy left that may serve, so the operator action is a
    /// restore, not an investigation of `partitions/{id}`.
    NoEligibleRegular,
    /// Recovery could not form its durability barrier before commit (team kernel-b `design.md`
    /// §5.6; lead ruling B-R41 F-a): a required copy was lost in `Synchronizing` or `Barrier`,
    /// or the barrier deadline passed with copies still owing a catch-up or a `DurableAt`
    /// proof. Nothing was committed; the operator re-fences.
    ///
    /// Distinct from [`Self::NoEligibleRegular`], which is a recovery that *committed* with no
    /// copy left to serve. Here nothing committed, and the alert names who is missing.
    BarrierIncomplete {
        /// The copies lost, or still owing their catch-up or proof when the deadline passed.
        /// Sorted by copy id.
        missing: Vec<CopyId>,
    },
}

impl BlockReason {
    /// The client-facing error for a partition blocked for this reason.
    ///
    /// # Two layers of one fact
    ///
    /// [`Self::DivergenceRequiresOperator`] and [`ErrorKind::DivergenceRequiresOperator`] are
    /// not two facts that happen to agree — they are the same fact seen from the two sides of
    /// the admission seam. The kernel layer is the partition's mode; the client layer is the
    /// answer T1 puts in a reply. L1 copies the second into
    /// [`crate::contracts::protection::AdmissionState::reason`] when `blocked` is set, and
    /// check 8 of team kernel-b `design.md` §3.2 replies **that value**, never a hard-coded
    /// code (team kernel-a `design.md` §1.6): a blocked partition has to answer *"nothing on
    /// the data path will change this"* where a paused one answers *"retry later"*
    /// ([`ErrorKind::ProtectionPaused`]).
    ///
    /// The mapping is written **here and nowhere else** (lead ruling R-S2). Three spellings of
    /// one fact without a stated mapping is how the next reader invents a fourth.
    ///
    /// Every reason answers [`ErrorKind::DivergenceRequiresOperator`] today (lead ruling B-R42,
    /// after B-R38 F1 and team kernel-b `design.md` §4.5): whatever blocked the partition,
    /// nothing on the data path will unblock it, and that is the whole of what a client needs
    /// to hear. The reason itself is the operator's story and travels in the alert, not the
    /// reply. The function stays, and stays total, so that a reason which ever needs a
    /// different client answer is decided here — L1 calls it rather than hard-coding the code.
    #[must_use]
    pub const fn client_error_kind(&self) -> ErrorKind {
        match self {
            Self::DivergenceRequiresOperator { .. }
            | Self::OvertakenByPeer
            | Self::CasContention
            | Self::ControlUnavailable
            | Self::ControlUnknown
            | Self::NoEligibleRegular
            | Self::BarrierIncomplete { .. } => ErrorKind::DivergenceRequiresOperator,
        }
    }
}

/// The shared partition mode every kernel matches on (finding K-B-19; team kernel-b
/// `design.md` §5.8). One enum in the contracts crate, so a mode one kernel adds is a mode
/// every other kernel's total `match` refuses to compile without.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum PartitionMode {
    /// Serving reads and writes under the RF3 rule.
    Active,
    /// Serving under spec §6.3's degraded two-copy rule; losing either copy stops writes.
    DegradedRf2,
    /// A lone survivor: whole prefix readable, no writes until the three-copy durable barrier.
    ReadOnly,
    /// No data-path exit (lead rulings B-R26, B-R29).
    Blocked {
        /// Why.
        reason: BlockReason,
    },
}

/// A fact kernel-a's authority module states about why it did nothing (ask CB-7).
///
/// The [`crate::contracts::ignore::KernelIgnoredReason::Authority`] leaf. KERNEL-A owns every
/// variant: add one by editing this enum and nothing else — not the carrier, not `event.rs`,
/// and without waiting on foundation or on kernel-b.
///
/// The leaf lives here rather than in a file of its own because this repository files contracts
/// by **subject**, not by team, and every variant is an authority fact. It is not in
/// [`crate::authority`] — the A1 kernel module — because the shared vocabulary does not depend
/// on one kernel's implementation file.
///
/// **Not `Copy`, and it cannot be.** [`Self::Blocked`] carries a [`BlockReason`], whose
/// `DivergenceRequiresOperator` holds a `Vec<CopyId>`; a `Copy` derive here is `E0204` on that
/// variant. That is the general rule as well as this instance: a leaf whose variants one team
/// owns must not promise a trait that constrains payloads that team has not written yet.
///
/// **Kernel-b never destructures this enum.** It matches `Authority(_)` or it does not match the
/// reason at all.
///
/// # Payload shapes are kernel-a's to spell
///
/// Ten of these names are asserted with a payload brace in kernel-a's plan. Only
/// [`Self::Blocked`]'s payload type is settled, so it is spelled; the other nine land as unit
/// variants and kernel-a widens each one in this file when it spells the shape. Widening a unit
/// variant is kernel-a's edit in kernel-a's own leaf, which is exactly the freedom the arm split
/// exists to give.
///
/// Variants are in alphabetical order so the set can be diffed name by name against the design
/// record's table. Nothing reads the ordinal; `Ord` is derived for the carrier's sake.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum AuthorityIgnoreReason {
    /// A grant acquisition was withheld rather than attempted.
    AcquireWithheld,
    /// Admission refused the request.
    AdmissionRefused,
    /// Admission is suspended, so the request was not considered.
    AdmissionSuspended,
    /// The partition is already blocked, so blocking it again does nothing.
    ///
    /// Distinct from kernel-b's `ReplicaIgnoreReason::AlreadyBlocked`, which is a replica fact
    /// about the same word. The two are one arm apart on purpose.
    AlreadyBlocked,
    /// The partition is blocked, so the step did not run.
    ///
    /// The one payload shape this freeze spells, because its type is already settled: the
    /// `reason` is the same [`BlockReason`] that [`PartitionMode::Blocked`] carries, so a row
    /// that sets up a block and asserts the ignore compares one value against itself rather
    /// than against a restatement of it.
    Blocked {
        /// Why the partition is blocked.
        reason: BlockReason,
    },
    /// A [`crate::contracts::event::NodeLifecycle::Rebooted`] announced the boot the held grant
    /// was already issued to, so nothing restarted under it (lead ruling A-R43).
    ///
    /// Named for what it is — *this lifecycle event is not a discontinuity* — and not for the
    /// nearest landed word. It stood in as [`Self::StaleTimer`], which is about a timer version
    /// and has nothing to do with a boot id, and it carried **no marker at the site**, so a row
    /// written against it would have looked correct and been wrong.
    ///
    /// The discontinuity half is [`DenyReason::BootMismatch`], which fences. This is the
    /// near-miss twin of that trigger and the two are one enum apart, not one variant apart.
    BootUnchanged,
    /// The takeover candidate could not be reached.
    CandidateUnreachable,
    /// A takeover candidate was considered while this node is not serving the partition.
    CandidateWhileNotServing,
    /// A dispatch was dropped because the partition froze under it.
    DispatchDroppedByFreeze,
    /// A dispatch was refused because the partition is frozen.
    DispatchRefusedFrozen,
    /// An external fence claim was refused: spec §7.2's fallback is never synthesised from
    /// unreachability, and a claim that does not bind all six fields is not evidence.
    ///
    /// Widened by kernel-a in the §3.4 build (team kernel-a `design.md` §2.6a; lead ruling A-R51)
    /// to say **which** binding failed. The checks run in [`ExternalFenceMismatch`]'s declaration
    /// order and the first failure wins, so one claim always gets one answer.
    ExternalFenceRejected {
        /// The first binding that did not hold.
        mismatch: ExternalFenceMismatch,
    },
    /// A control key family was refused.
    FamilyRejected,
    /// A fence was requested while the partition is already blocked.
    FenceWhileBlocked,
    /// A read-back of this node's own `grants/{node}` found the record already held, at a
    /// revision no newer than the one held, so nothing was adopted (team kernel-a `design.md`
    /// §2.6a decision e3; lead ruling A-R51).
    ///
    /// Appended by kernel-a under lead ruling A-R41. It replaces [`Self::StaleAuthorityView`] at
    /// that one site, where it was a homograph: that name is about a view this node holds having
    /// been superseded, and this is a grant record read that is not newer. Same precedent as
    /// [`Self::PartitionReadSuperseded`].
    GrantRecordNotNewer,
    /// A renewal arrived after it could still matter and was ignored.
    ///
    /// Distinct from [`Self::RenewalWithheld`], which is this node declining to renew: one is
    /// an input arriving too late, the other is an output never sent.
    LateRenewalIgnored,
    /// A coherent partitions snapshot, or a single-record read of one `partitions/{id}`,
    /// installed exactly what was already installed, so no write of `served` happened and
    /// `authority_seq` did not move (team kernel-a `design.md` §2.4, the family row and the
    /// single-record unchanged row; lead ruling A-R41).
    ///
    /// # The trap this variant is placed to avoid, spelled out
    ///
    /// [`AuthorityFact::LineageChanged`] is the write; this is the absence of one. Had both
    /// landed in the **same** enum they would be alphabetically adjacent unit variants differing
    /// by a negation prefix — the single easiest pair in this vocabulary to assert as each
    /// other, in an enum whose stated convention is alphabetical order. One arm apart they are
    /// distinct **types** with no `From` between them, so writing the wrong one is an `E0308`
    /// naming both enums at the row's own line. That is what [`crate::contracts::ignore`]'s
    /// homograph rule is for, the same way [`Self::Quarantined`] and [`Self::AlreadyBlocked`]
    /// spell theirs.
    ///
    /// The taxonomic reason agrees and is the weaker one: all three of
    /// [`AuthorityFact::LineageChanged`], [`AuthorityFact::LineageInstalled`] and
    /// [`AuthorityFact::LineageLoaded`] record a **write** of `served`, and this records that
    /// there was none.
    LineageUnchanged,
    /// A `partitions/{id}` read answered about a partition this node was not serving, so there
    /// was no right to end and no view to supersede (team kernel-a `design.md` §2.4; lead ruling
    /// A-R41).
    ///
    /// Deliberate inaction, not an act: fencing on it would emit a `Fence` per read of a
    /// partition this node never had, which is exactly the "exactly one `Fence` per trigger"
    /// claim lead ruling A-R37 asks a row to be able to make.
    NotOurs,
    /// A single-record read of `partitions/{id}` answered with a lineage of ours that differs
    /// from the one served, at a revision **no newer** than the one that partition's entry was
    /// installed at — or, for a partition not served, than the last coherent snapshot (lead
    /// ruling A-R48). It is older than what is held, so it is not installed: doing so would roll
    /// `served` back and publish a lower `owner_epoch` after a higher one.
    ///
    /// Appended by kernel-a in the A1 phase-2 build (§3.2), under the authority of lead ruling
    /// A-R41. `design.md` §2.4 has no row for this case — its changed row requires
    /// `part.revision > partitions_revision` and its unchanged row requires equality — and an
    /// empty effect vector is not an answer.
    ///
    /// # Why not the nearest landed word
    ///
    /// [`Self::StaleAuthorityAnswer`] reads as a fit and is not one: it is T1's and P1's reason
    /// for an `AuthorityAnswer` whose `authority_seq` no longer matches (`design.md` §3.3,
    /// §4.2). Reusing it here would make one name mean two claims in two kernels — a homograph a
    /// row could assert against the wrong module and pass.
    PartitionReadSuperseded,
    /// Publication was deferred rather than refused.
    PublishDeferred,
    /// The publication predicate was false, so nothing was published.
    PublishPredicateFalse,
    /// Publication was refused because the partition is blocked.
    PublishRefusedBlocked,
    /// A publication happened while the partition was frozen.
    PublishedWhileFrozen,
    /// Qualification was lost after the publish decision was taken.
    QualificationLostAfterPublish,
    /// The copy is quarantined.
    ///
    /// One of three `Quarantined` facts, in three different arms: this authority fact,
    /// kernel-b's `ReplicaIgnoreReason::QuarantinedTerminal`, and
    /// [`crate::contracts::envelope::AppendReject::Quarantined`] on the append ladder. Same
    /// word, three meanings, and the arms keep them from being spelled as each other.
    Quarantined,
    /// A renewal was withheld rather than sent; never extend a grant on hope.
    RenewalWithheld,
    /// A reply was suppressed because its deadline had already passed.
    ReplySuppressedAfterTimeout,
    /// A reply was withheld rather than sent.
    ReplyWithheld,
    /// A [`crate::contracts::event::NodeLifecycle::Resumed`] reported a suspension inside
    /// [`crate::contracts::event::Budgets::resume_gap_tolerance_millis`], so the scheduler gap
    /// was not a discontinuity (lead ruling A-R43).
    ///
    /// Named for what it is, not for the nearest landed word. It stood in as [`Self::StaleTimer`]
    /// — which is about a timer version, and a resume gap is not a timer — with **no marker at
    /// the site**, so a row written against it would have looked correct and been wrong.
    ///
    /// The discontinuity half is [`DenyReason::ProcessSuspended`], which fences. One enum apart,
    /// for the same reason as [`Self::BootUnchanged`].
    ResumeGapWithinTolerance,
    /// A clock sample was rejected.
    SampleRejected,
    /// An answer arrived carrying an authority view older than the one held.
    ///
    /// Distinct from [`Self::StaleAuthorityView`]: this is a stale *answer* received, that is a
    /// stale *view* held.
    StaleAuthorityAnswer,
    /// The authority view in hand is stale.
    StaleAuthorityView,
    /// A timer fired carrying a version that is no longer the armed one.
    StaleTimer,
    /// A takeover of this partition is already authorized: A1 emitted its one
    /// [`AuthorityEffect::FenceProven`] for this `(partition, prior_owner_epoch)`, and a second
    /// route completing does not emit another (team kernel-a `design.md` §2.6 rule 4, §2.6a T12;
    /// finding K-A-30).
    ///
    /// Appended by kernel-a under lead ruling A-R41. Named for the authorization, not the fence,
    /// by [`FencingProof`]'s honesty rule.
    TakeoverAlreadyAuthorized,
    /// A takeover was deferred rather than attempted.
    TakeoverDeferred,
    /// A `grants/{node}` CAS completion arrived that matches no CAS this node has outstanding:
    /// its correlation is not the in-flight acquisition's (lead ruling A-R47).
    ///
    /// Appended by kernel-a in the A1 phase-2 build (§3.1), under the authority of lead ruling
    /// A-R41. It is what the retired one-event shortcut now answers: a `Committed` this kernel
    /// did not issue used to move it to `Held`, and a commit nobody asked for is not a grant.
    /// `design.md` §2.4 matches every completion row on `op == acquire.op` and has no row for a
    /// completion that matches nothing, and an empty effect vector is not an answer (A-R24).
    ///
    /// # Why not the nearest landed words
    ///
    /// [`Self::LateRenewalIgnored`] is `design.md`'s `Fenced | CasApplied` row: a completion of a
    /// renewal this node **did** issue, arriving after the fence. This is a completion of nothing
    /// it issued. [`Self::StaleAuthorityView`] is a view held, not a completion received.
    UnmatchedCompletion,
}

/// Which binding of an [`crate::contracts::event::EventKind::ExternalFenceVerified`] claim did
/// not hold (team kernel-a `design.md` §2.6a T11; lead ruling A-R51).
///
/// Declared in the order A1 checks them, and the first failure wins. A1 cannot verify the
/// external fact; it verifies that the claim names what its own takeover state holds (finding
/// K-A-16).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ExternalFenceMismatch {
    /// A1 holds no takeover entry for the claim's partition. One condition, two causes: no
    /// linearizable read of the prior grant has been made, or the claim names the wrong
    /// partition.
    Partition,
    /// A1 holds an entry, but no linearizable read has found the prior grant **frozen**.
    NotFrozen,
    /// The claim names a different prior generation.
    PriorGeneration,
    /// The claim names a different prior owner epoch.
    PriorOwnerEpoch,
    /// The claim names a different prior boot.
    PriorBootId,
    /// The claim names a different revision than the read that found the grant frozen.
    ControlRevision,
}

/// What a fence covers (team kernel-a `design.md` §2.1; lead ruling A-R28).
///
/// Lives on [`AuthorityEffect::Fence`] and nowhere else, because that is the only place it is
/// real. A1's own `Fenced` state carries **no** scope field (`design.md:709`, finding K-A-29):
/// the two partition-scoped fences — a local storage failure and a persisted epoch revocation —
/// leave the node in `Held`, so a state accessor can never show a partition-scoped fence at all.
/// That is why the fence rows assert the effect rather than reading the state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum FenceScope {
    /// Every partition this node serves. The grant itself is gone.
    Node,
    /// One partition. The grant is still held, and the other partitions keep serving.
    Partition(
        /// The partition fenced.
        PartitionId,
    ),
}

/// A fact kernel-a's authority module states about something it **did** (lead ruling A-R25b).
///
/// The positive vocabulary, carried by [`AuthorityEffect::Fact`]. A different enum from
/// [`AuthorityIgnoreReason`], and deliberately so: that one answers *why nothing happened*, and
/// [`crate::contracts::event::KernelEffect::Ignored`]'s own doc calls its subject a module that
/// "deliberately did nothing". Spelling `LineageInstalled` as an ignore reason would contradict
/// that sentence and collapse the homograph separation [`crate::contracts::ignore`] exists to
/// provide — the two enums are one arm apart for the same reason `Quarantined` appears in three.
///
/// KERNEL-A owns every variant. Add one by editing this enum and nothing else.
///
/// Not `Copy`, for the reason ask CB-7 gives on [`crate::contracts::event::KernelEffect`]: the
/// variants are unit today, but they are kernel-a's to widen, and `Copy` would condition that
/// freedom on foundation approving a payload nobody has written yet.
///
/// Variants are in alphabetical order, matching [`AuthorityIgnoreReason`], so the set can be
/// diffed name by name against team kernel-a `design.md` §2.4. Nothing reads the ordinal.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum AuthorityFact {
    /// The create-only acquisition CAS lost (`design.md` §2.4, the `Unheld | CasConflict` row).
    ///
    /// The loser learns nothing else, and that is the control plane's rule rather than a gap
    /// here: rEtcd ADR-0006 says a conflict "exposes only `exists` and `current_mod_revision`,
    /// never the value". So the fact is the whole content of the observation.
    AcquireLost,
    /// A grant record with the same grant id and a higher revision was adopted (`design.md`
    /// §2.4, the `Held | ReadOk` row). No lineage moved, so `authority_seq` does not bump.
    Adopted,
    /// A drain proof was recorded: an epoch revocation is durable and this partition's epoch
    /// will not be served again (spec §7.3 step 2; `design.md` §2.4, the
    /// `Held | EpochRevocationPersisted` row).
    DrainProof,
    /// A partition record moved under a held grant and the new lineage was installed
    /// (`design.md` §2.4, the generic changed row).
    ///
    /// Distinct from [`Self::LineageInstalled`], which is the install that follows a recovery,
    /// and from [`Self::LineageLoaded`], which is the whole-family load after a grant adoption.
    /// Three different reads write the served lineage, and a row must be able to say which one
    /// it saw.
    LineageChanged,
    /// The lineage a committed recovery selected was read back from `partitions/{id}` and
    /// installed (`design.md` §2.4, the post-`Recovered` install row).
    LineageInstalled,
    /// A coherent read of the partitions family replaced the served set wholesale after a grant
    /// was adopted (`design.md` §2.4, the `Held | FamilyOk` row).
    LineageLoaded,
    /// F1 announced a recovery naming a lineage this node does not currently serve
    /// (`design.md` §2.4, the `Recovered` row).
    ///
    /// **No rights change with it.** The authority to serve the recovered lineage still comes
    /// from the partition record, read linearizably; this fact records only that A1 saw the
    /// announcement and issued that read.
    RecoveryObserved,
    /// A renewal CAS came back `Conflict` (`design.md` §2.4, the `Held | CasConflict` row). The
    /// expiry is unchanged — never extend a grant on hope (rEtcd ADR-0015).
    RenewLost,
    /// A renewal CAS came back `Unknown` (`design.md` §2.4, the `Held | Unknown` row).
    ///
    /// Distinct from [`Self::RenewLost`]: a lost renewal proves the CAS did not land, an unknown
    /// one proves nothing at all, and the operator stories differ. Both leave the expiry alone.
    RenewUnknown,
    /// The watch has been refused for capacity
    /// [`crate::authority::WATCH_ADMISSION_ATTEMPT_CAP`] consecutive times, and A1 has **stopped
    /// re-arming** until an operator or scenario event resets it (team kernel-a `design.md` §2.4,
    /// the at-cap row; lead ruling A-R41).
    ///
    /// # Why this is a fact and not an ignore reason
    ///
    /// The at-cap row does not merely decline once, it **latches**, and latching is an act.
    /// [`AuthorityIgnoreReason::AdmissionRefused`] is "did nothing this time, will retry"; this
    /// is "have given up and will not retry". `Authority::watch_refused_attempts` already lets a
    /// test read the counter, but the counter is *state* and the latch is *behaviour* — a row
    /// reading `== 3` still cannot say whether A1 re-armed. In a trace, which is the operator's
    /// artifact rather than the test's, three `Ignored(AdmissionRefused)` lines followed by
    /// silence is indistinguishable from A1 having crashed. This is the line that says the
    /// silence was deliberate.
    ///
    /// # The twinning is the reason for the arm, not an argument against it
    ///
    /// Being `AdmissionRefused`'s at-cap twin is exactly what makes co-location dangerous: as
    /// adjacent unit variants in one enum, a row asserting the cap passes on the under-cap
    /// reason. That is not hypothetical — it is the precise mechanism that cost kernel-a half its
    /// credited work, where two functions named for one row asserted a neighbouring row's input
    /// and the census could not tell.
    WatchAdmissionExhausted,
}

/// A fact one of A1's peers hands the authority module (lead ruling A-R25).
///
/// The [`crate::contracts::event::KernelEvent::Authority`] leaf. FOUNDATION owns the arm;
/// KERNEL-A owns every variant. This is the rule [`crate::contracts::ignore`] already states one
/// level down — *"a kernel adds a reason name by appending one variant to its own leaf enum. It
/// never edits `event.rs`"* — applied one level up, where the fact is positive rather than a
/// reason for inaction. Foundation conceded that ownership at the `Ignored` level; one level up
/// is the same concession, and it costs `event.rs` two lines once.
///
/// **Kernel-b never destructures this enum.** It matches `Authority(_)` or it does not match.
///
/// # Why four variants where the design names thirteen
///
/// Team kernel-a `design.md` §2.2 lists thirteen; this leaf keeps **three** of them — [`Self::Check`],
/// [`Self::RevokeEpochRequested`] and [`Self::EpochRevocationPersisted`] — and adds [`Self::Answer`],
/// which A-R29 pairs with `Check`. The other **ten** already arrive in foundation's vocabulary, and a
/// second spelling of an arriving fact is a second copy that drifts:
///
/// | `design.md` §2.2 name | Already arrives as | Ruling |
/// |---|---|---|
/// | `Tick`, `Clock` | [`crate::contracts::event::StepCtx::now`] and [`crate::contracts::event::StepCtx::control_time`], read on every step | A-R27 |
/// | `AcquireDue`, `RenewDue` | [`crate::contracts::event::EventKind::Timer`], with [`crate::contracts::time::TimerFired::version`] as the stale-timer discriminator | A-R25 |
/// | `Control` | [`crate::contracts::event::EventKind::Control`] | — |
/// | `LocalStorageFailure` | [`crate::contracts::storage::StorageEvent::CommitFailed`] carrying [`crate::contracts::storage::StorageFault::WriteFailed`], whose doc already says the partition fences | A-R25 |
/// | `ProcessResumed`, `BootObserved` | [`crate::contracts::event::NodeLifecycle::Resumed`] and [`crate::contracts::event::NodeLifecycle::Rebooted`] | A-R25 |
/// | `ExternalFenceVerified` | [`crate::contracts::event::EventKind::ExternalFenceVerified`], landed whole with all six binding fields | A-R25 |
/// | `Recovered` | [`crate::contracts::event::KernelEvent::Recovered`] | — |
///
/// A-R27 is the one that reverses an earlier decision rather than mapping a name: there is no
/// clock event because [`crate::contracts::time::ControlTime`] is field for field the design's
/// `ClockSample`, and it is already in [`crate::contracts::event::StepCtx`] on every step. A1
/// holds the previous sample as its own state — the retraction rule and the backward-jump
/// comparison both need it — and arms its own timer for the periodic wake.
///
/// # Size: nothing is boxed, and that was measured
///
/// [`crate::contracts::event::KernelEvent::Recovered`] is boxed because its payload would set the
/// size of every [`crate::contracts::event::Event`] in the run queue. The same check was run here
/// and comes out the other way: this enum is **88 bytes**, set by [`Self::Answer`]'s
/// [`AuthorityDecision`], against a `KernelEvent` that is already 112 for `SetAdmission`. So
/// `KernelEvent`, `EventKind` and `Event` are **unchanged at 112, 112 and 152** — the arm is
/// free. A variant added here stays free only while it is under 112; check before adding one.
///
/// Not `Copy`: same argument as [`AuthorityFact`]'s.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum AuthorityEvent {
    /// T1, P1 or F1 asks for a recheck at a checkpoint (spec §7.3 step 6; lead ruling A-R29).
    ///
    /// The request half of the asynchronous pair whose answer is [`Self::Answer`]. Three of the
    /// five checkpoints cross a module boundary — A1, T1 and P1 are separate
    /// [`crate::contracts::event::Module`] implementations, stepped independently — so there is
    /// no function call to make instead. [`Checkpoint::Admission`] is the exception and stays
    /// synchronous against the last pushed [`AuthorityView`], which is what keeps two events per
    /// transaction out of the budget.
    Check {
        /// Where the recheck happens.
        checkpoint: Checkpoint,
        /// The lineage the caller believes it is operating under.
        lineage: Lineage,
        /// The request the recheck is for, carried through so the answer can be matched to it.
        correlation: CorrelationId,
    },
    /// A1's answer, delivered to the module that asked (lead ruling A-R29).
    ///
    /// The event half of [`AuthorityEffect::Answer`] — the R-S6 rule, both halves of a carrier or
    /// neither, applied inside the leaf: A1 *emits* the decision and T1, P1 or F1 *receives* it,
    /// so the same shape exists on both sides of the dispatcher.
    ///
    /// Not a [`crate::contracts::trace::TraceKind`] variant, which was the alternative A-R29
    /// refused: [`Verdict::Deny`] carries one of sixteen [`DenyReason`]s, and a four-variant
    /// trace outcome cannot tell `Deny(ControlUnavailable)` from `Deny(ClockSampleStale)`.
    Answer(AuthorityDecision),
    /// A peer asks that this partition's epoch never be served again (spec §7.3 step 2).
    ///
    /// A request, not a fact: the revocation counts only once it is durable, which is what
    /// [`Self::EpochRevocationPersisted`] reports. A1 answers this one with
    /// [`crate::contracts::storage::StoreEffect::PersistEpochRevocation`] and nothing else.
    RevokeEpochRequested {
        /// The partition whose epoch is being revoked.
        partition: PartitionId,
        /// The epoch that will never be served again.
        epoch: OwnerEpoch,
    },
    /// The epoch revocation is on local disk (spec §7.3 step 2: an acknowledgement counts only
    /// if a restart cannot restore that epoch).
    ///
    /// The completion of [`Self::RevokeEpochRequested`], and the point at which the fence becomes
    /// real: A1 emits [`AuthorityEffect::Fence`] scoped to this partition with
    /// [`DenyReason::EpochRevoked`], records the drain proof, and **stays held** for every other
    /// partition.
    EpochRevocationPersisted {
        /// The partition whose epoch is now revoked.
        partition: PartitionId,
        /// The epoch that is now unserveable.
        epoch: OwnerEpoch,
    },
}

/// A fact the authority module hands its peers (lead ruling A-R25).
///
/// The [`crate::contracts::event::KernelEffect::Authority`] leaf. Ownership is
/// [`AuthorityEvent`]'s, identically: foundation owns the arm, kernel-a owns the variants.
///
/// # Why five variants where the design names ten
///
/// Five are kept, four of them under the design's own names; the design's `Decide` is spelled
/// [`Self::Answer`] here, so the emitted half and the delivered half of one pair read as one pair
/// (lead ruling A-R29).
///
/// The other five are already foundation effects and route through them: `Control` is
/// [`crate::contracts::event::EffectKind::Control`], `ArmTimer`/`CancelTimer` are
/// [`crate::contracts::event::EffectKind::Timer`], `AdoptAuthority` is
/// [`crate::contracts::event::EffectKind::AdoptAuthority`], and `PersistEpochRevocation` is a
/// [`crate::contracts::storage::StoreEffect`] variant rather than an arm variant (lead ruling
/// A-R26) — on this leaf it would make `rdb-sim` route a kernel fact to storage, which the
/// charter forbids.
///
/// Also not here: an `Alert { reason: ErrorKind }`. Team kernel-a `design.md` §3.4 maps **13 of
/// 15** [`DenyReason`]s onto `LEASE_EXPIRED`, so an [`ErrorKind`] alert cannot distinguish two
/// denials that the plan writes as explicit one-fact twins. (A-R42's
/// [`DenyReason::ClockModeUnbounded`] makes that **14 of 16** in the crate: it is a clock denial
/// and maps where [`DenyReason::ClockUnbounded`] does. `design.md` §3.4 still shows the table of
/// 15 — the derivation is stated here, not read off that table.)
///
/// # Size: [`Self::FenceProven`] is **not** boxed, and that is a measurement, not an oversight
///
/// [`crate::contracts::event::KernelEffect::Recovered`] is boxed because
/// [`crate::contracts::recovery::RecoveryResult`] is 592 bytes against a next-largest variant of
/// 112 — 5.3x, on the type that sets the size of every [`crate::contracts::event::Event`] in the
/// run queue. Both halves of that argument fail here. Measured on this host, `std::mem::size_of`,
/// before and after this arm landed:
///
/// | | bytes |
/// |---|---|
/// | [`FencingProof`], the largest payload on this enum | 128 |
/// | [`AuthorityDecision`], the next largest | 88 |
/// | this enum | 128 |
/// | [`crate::contracts::event::KernelEffect`] and `EffectKind`, before → after | 112 → 128 |
/// | [`crate::contracts::event::Event`], before → after | 152 → 152 |
///
/// 1.45x rather than 5.3x, and the sixteen bytes land on [`crate::contracts::event::Effect`] — a
/// per-step `Vec` that is built and dropped — never on the run queue, which did not move.
/// Boxing would buy that back at the price of an allocation on the fencing path and of spelling
/// this variant differently from the `FenceProven` the design, the plan rows and kernel-b's F1
/// door all name. Re-measure if a payload here passes 128.
///
/// Not `Copy`: same argument as [`AuthorityFact`]'s.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum AuthorityEffect {
    /// A1 answers an [`AuthorityEvent::Check`] (lead ruling A-R29).
    ///
    /// The emitted half of [`AuthorityEvent::Answer`]; see that variant for the pairing.
    Answer(AuthorityDecision),
    /// Broadcast to T1 and P1: stop serving the named scope (lead ruling A-R28).
    ///
    /// An effect rather than only a state change, because the state cannot carry the questions a
    /// row asks. A partition-scoped fence leaves A1 held (see [`FenceScope`]), so no accessor can
    /// see it; and no accessor can count *exactly one* fence, or assert the ordered pairing with
    /// [`Self::PublishAuthorityView`] that finding K-A-49 requires.
    ///
    /// Always paired with a superseding [`Self::PublishAuthorityView`] whose `valid_through_tick`
    /// is the fence tick less one, saturating, and whose `past_horizon` is this `reason` — so a
    /// consumer that missed the fence still denies from the view alone.
    Fence {
        /// What the fence covers.
        scope: FenceScope,
        /// Why, in the same vocabulary the superseding view's `past_horizon` carries.
        reason: DenyReason,
    },
    /// A1 pushes its authority state to R1, T1 and P1 (team kernel-a `design.md` §1.7).
    ///
    /// Emitted wherever `valid_through_tick` or `authority_seq` moves (finding K-A-35): every
    /// grant adoption, every committed renewal, every accepted clock sample, every write of the
    /// served lineage, and every fence. It does not scale with transactions, which is what lets
    /// [`Checkpoint::Admission`] be answered against the last one pushed.
    PublishAuthorityView(AuthorityView),
    /// A1 → F1: the only door into recovery (team kernel-a `design.md` §1.7, §2.6).
    ///
    /// Emitted at most once per (partition, prior owner epoch), and only when one of spec §7.3's
    /// three revocation routes is complete. Kernel-b makes this the only transition out of F1's
    /// `Idle`, so "reachability does not elect a primary" is unrepresentable rather than
    /// reviewed. Read [`FencingProof`]'s honesty rule before naming this in a log field.
    FenceProven(FencingProof),
    /// Something A1 did, for the trace and the oracle (lead ruling A-R25b).
    ///
    /// The positive counterpart of [`crate::contracts::event::KernelEffect::Ignored`]. The two
    /// are separate enums on separate arms; see [`AuthorityFact`].
    Fact(AuthorityFact),
}
