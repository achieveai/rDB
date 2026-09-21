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

use crate::contracts::ids::{
    AuthorityGeneration, BootId, ConfigVersion, CorrelationId, Generation, GrantId, NodeId,
    OwnerEpoch, PartitionId,
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
    /// backwards.
    ClockUnbounded,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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

/// Why a partition is [`PartitionMode::Blocked`] (lead ruling A-R23; team kernel-b
/// `design.md` §3.5, team kernel-a `design.md` §4.1).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum BlockReason {
    /// R1 found a divergence that leaves no durable floor under the pinned configuration
    /// (lead ruling B-R26): qualification can never return under this configuration, and the
    /// only exit is an operator removing the diverged copies from membership and fencing.
    DivergenceRequiresOperator {
        /// The copies whose history diverged, so the alert names them.
        diverged: Vec<CopyId>,
    },
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
