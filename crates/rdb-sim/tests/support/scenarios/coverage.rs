//! The coverage matrix (design §6, VA-6).
//!
//! Every required list here is **enumerated from its enum**, not hand-listed, so a variant added
//! upstream fails row **M7V-56** instead of passing quietly. That is the M6-107 / TA-63 pattern:
//! a missing case must be a red, never a silent gap.
//!
//! Two tables key the required cells:
//!
//! * [`family_of`] — `BoundaryId -> FaultKind`. C0 has no `impl BoundaryId` and no
//!   `fault_kind()`, so this assignment has **no ground truth in the contract**. An enumeration
//!   row that reads this const and tests this const would pass with a member filed under the
//!   wrong family, and that member would then inherit the wrong gating package. So the table is
//!   checked **against the trace**: every observed `fault_injected` must carry the `fault_kind`
//!   this table assigns its `boundary` (critic T-40; M7V-42's static half, M7V-55's behavioural
//!   half).
//! * [`gated_by`] — `BoundaryId -> PackageId`, keyed **per family** (V-R20 (4)). A cell whose
//!   emitter reports `capability{state=Unavailable}` is recorded as `unavailable(package)`,
//!   **excluded from `required_missing[]` by that capability entry and never by editing the
//!   required list**. When the package reports `Wired`, the cell is required again with no code
//!   change.
//!
//! The gate itself applies only when the corpus is at least [`REQUIRED`]`.len()` seeds
//! (V-R20 (7)): a smaller corpus cannot attempt every member by construction, so it records the
//! shortfall, writes `coverage_gated: false` and does not fail.

use std::collections::{BTreeMap, BTreeSet};

use serde::de::value::{Error as ValueError, U32Deserializer};
use serde::de::DeserializeOwned;

use rdb_core::contracts::errors::ErrorKind;
use rdb_core::contracts::ids::ReplicaRole;
use rdb_core::contracts::trace::{
    AckRejectReason, BoundaryId, CapabilityState, FaultKind, PackageId, ProtectionPhase,
    RecoveryMode,
};

pub use crate::support::oracle::model::DerivedQuorumRule;

/// The required boundary cases, in `BoundaryId` declaration order.
///
/// Its member set **equals** `BoundaryId`'s variant set exactly — no more, no fewer (critic F17,
/// V-R19). Row **M7V-56** asserts the equality, which is what stops a member being quietly
/// dropped when it proves hard to reach.
pub const REQUIRED: [BoundaryId; 29] = [
    BoundaryId::ChangedDigest,
    BoundaryId::OldGeneration,
    BoundaryId::LostSuccessReply,
    BoundaryId::RetainedDedupHit,
    BoundaryId::ExpiredDedup,
    BoundaryId::StaleBoot,
    BoundaryId::StaleEpoch,
    BoundaryId::StaleConfig,
    BoundaryId::MissingPredecessor,
    BoundaryId::AckAfterRevocation,
    BoundaryId::ForgedIdentity,
    BoundaryId::SameTickOrder,
    BoundaryId::GrantSkewWithinBound,
    BoundaryId::GrantSkewOutsideBound,
    BoundaryId::DedupWindowJump,
    BoundaryId::BeforeAtomicCommit,
    BoundaryId::AfterAtomicCommit,
    BoundaryId::BeforeFlush,
    BoundaryId::AfterFlush,
    BoundaryId::FalseDurableWatermark,
    BoundaryId::StaleSnapshot,
    BoundaryId::LostControlQuorum,
    BoundaryId::InvalidGrant,
    BoundaryId::PartialStagedMetadata,
    BoundaryId::WatchGap,
    BoundaryId::UnequalSecondaryPrefix,
    BoundaryId::LoneSurvivorChoice,
    BoundaryId::Divergence,
    BoundaryId::ReturningStaleOwner,
];

/// The admission-rejection axis.
///
/// `ErrorKind` is much wider than admission control: it carries read, status and continuation
/// errors that no `admission_decision` can produce. Requiring a cell per `ErrorKind` variant
/// would demand coverage of the unreachable, and the reflex fix for that is deleting cells. So
/// the axis is this subset, asserted by row **M7V-56** to contain the four variants other rows
/// pin. A `client_outcome` axis over the full `ErrorKind` is **reported, not required**.
pub const ADMISSION_REASONS: &[ErrorKind] = &[
    ErrorKind::ProtectionPaused,
    ErrorKind::RequestIdReuse,
    ErrorKind::CrossAffinity,
    ErrorKind::GenerationChanged,
    ErrorKind::Overloaded,
    ErrorKind::DeadlineBeforeAdmission,
    ErrorKind::NotPrimary,
];

/// Every `AckRejectReason`, enumerated.
///
/// Widened 7 → 14 with the enum itself (ask CB-3, lead ruling B-R33 Q-B-8). Five of the seven
/// added have no scenario that produces them yet — `StaleGeneration`, `RoleMismatch`,
/// `RegressedProgress`, `Unverifiable`, `NotAMember`; `InconsistentProgress` is `M7V-70` and
/// `Diverged` is `M7V-80`. Listing all fourteen anyway is the rule this module states at the
/// top: a cell leaves `required_missing[]` **through its package's capability entry, never by
/// being dropped from the enumeration.** All fourteen are `unavailable(R1)` in M7, so none is
/// red today and all five gaps surface the round R1 is wired. Shortening this list to the
/// reachable seven would close `M7V-56`'s equality by hiding the question.
///
/// Widened 14 → 15 with `InFlightUnverified` (lead ruling B-R58c; ruling V-R23). The enum grew
/// and this list did not, and `M7V-56` stayed green because its arity guard was the literal 14.
/// The guard now reads the enum ([`variants`]) and [`ack_reject_cell`]'s exhaustive `match`.
pub const ACK_REJECT_REASONS: [AckRejectReason; 15] = [
    AckRejectReason::Gap,
    AckRejectReason::DigestMismatch,
    AckRejectReason::StaleEpoch,
    AckRejectReason::StaleBoot,
    AckRejectReason::StaleConfig,
    AckRejectReason::ForgedIdentity,
    AckRejectReason::IncompatibleVersion,
    AckRejectReason::StaleGeneration,
    AckRejectReason::RoleMismatch,
    AckRejectReason::InconsistentProgress,
    AckRejectReason::RegressedProgress,
    AckRejectReason::Unverifiable,
    AckRejectReason::InFlightUnverified,
    AckRejectReason::Diverged,
    AckRejectReason::NotAMember,
];

/// Every `RecoveryMode`, enumerated.
pub const RECOVERY_MODES: [RecoveryMode; 3] = [
    RecoveryMode::TwoSurvivor,
    RecoveryMode::LoneSurvivorReadOnly,
    RecoveryMode::Quarantine,
];

/// Every `ProtectionPhase`, enumerated.
pub const PROTECTION_PHASES: [ProtectionPhase; 4] = [
    ProtectionPhase::Healthy,
    ProtectionPhase::Warn,
    ProtectionPhase::Paused,
    ProtectionPhase::Resuming,
];

/// Every `ReplicaRole`, enumerated.
pub const REPLICA_ROLES: [ReplicaRole; 3] = [
    ReplicaRole::Primary,
    ReplicaRole::RegularSecondary,
    ReplicaRole::Shadow,
];

/// `BoundaryId` -> its cell: the variant's index in [`REQUIRED`].
///
/// One `match` with no `_` arm, so a new `BoundaryId` fails to compile here until it is given a
/// cell (ruling V-R23). The `cells_agree!` block checks at compile time that the index is its position.
#[must_use]
pub const fn boundary_cell(value: BoundaryId) -> usize {
    match value {
        BoundaryId::ChangedDigest => 0,
        BoundaryId::OldGeneration => 1,
        BoundaryId::LostSuccessReply => 2,
        BoundaryId::RetainedDedupHit => 3,
        BoundaryId::ExpiredDedup => 4,
        BoundaryId::StaleBoot => 5,
        BoundaryId::StaleEpoch => 6,
        BoundaryId::StaleConfig => 7,
        BoundaryId::MissingPredecessor => 8,
        BoundaryId::AckAfterRevocation => 9,
        BoundaryId::ForgedIdentity => 10,
        BoundaryId::SameTickOrder => 11,
        BoundaryId::GrantSkewWithinBound => 12,
        BoundaryId::GrantSkewOutsideBound => 13,
        BoundaryId::DedupWindowJump => 14,
        BoundaryId::BeforeAtomicCommit => 15,
        BoundaryId::AfterAtomicCommit => 16,
        BoundaryId::BeforeFlush => 17,
        BoundaryId::AfterFlush => 18,
        BoundaryId::FalseDurableWatermark => 19,
        BoundaryId::StaleSnapshot => 20,
        BoundaryId::LostControlQuorum => 21,
        BoundaryId::InvalidGrant => 22,
        BoundaryId::PartialStagedMetadata => 23,
        BoundaryId::WatchGap => 24,
        BoundaryId::UnequalSecondaryPrefix => 25,
        BoundaryId::LoneSurvivorChoice => 26,
        BoundaryId::Divergence => 27,
        BoundaryId::ReturningStaleOwner => 28,
    }
}

/// `AckRejectReason` -> its cell: the variant's index in [`ACK_REJECT_REASONS`].
///
/// One `match` with no `_` arm, so a new `AckRejectReason` fails to compile here until it is given a
/// cell (ruling V-R23). The `cells_agree!` block checks at compile time that the index is its position.
#[must_use]
pub const fn ack_reject_cell(value: AckRejectReason) -> usize {
    match value {
        AckRejectReason::Gap => 0,
        AckRejectReason::DigestMismatch => 1,
        AckRejectReason::StaleEpoch => 2,
        AckRejectReason::StaleBoot => 3,
        AckRejectReason::StaleConfig => 4,
        AckRejectReason::ForgedIdentity => 5,
        AckRejectReason::IncompatibleVersion => 6,
        AckRejectReason::StaleGeneration => 7,
        AckRejectReason::RoleMismatch => 8,
        AckRejectReason::InconsistentProgress => 9,
        AckRejectReason::RegressedProgress => 10,
        AckRejectReason::Unverifiable => 11,
        AckRejectReason::InFlightUnverified => 12,
        AckRejectReason::Diverged => 13,
        AckRejectReason::NotAMember => 14,
    }
}

/// `RecoveryMode` -> its cell: the variant's index in [`RECOVERY_MODES`].
///
/// One `match` with no `_` arm, so a new `RecoveryMode` fails to compile here until it is given a
/// cell (ruling V-R23). The `cells_agree!` block checks at compile time that the index is its position.
#[must_use]
pub const fn recovery_mode_cell(value: RecoveryMode) -> usize {
    match value {
        RecoveryMode::TwoSurvivor => 0,
        RecoveryMode::LoneSurvivorReadOnly => 1,
        RecoveryMode::Quarantine => 2,
    }
}

/// `ProtectionPhase` -> its cell: the variant's index in [`PROTECTION_PHASES`].
///
/// One `match` with no `_` arm, so a new `ProtectionPhase` fails to compile here until it is given a
/// cell (ruling V-R23). The `cells_agree!` block checks at compile time that the index is its position.
#[must_use]
pub const fn protection_phase_cell(value: ProtectionPhase) -> usize {
    match value {
        ProtectionPhase::Healthy => 0,
        ProtectionPhase::Warn => 1,
        ProtectionPhase::Paused => 2,
        ProtectionPhase::Resuming => 3,
    }
}

/// `ReplicaRole` -> its cell: the variant's index in [`REPLICA_ROLES`].
///
/// One `match` with no `_` arm, so a new `ReplicaRole` fails to compile here until it is given a
/// cell (ruling V-R23). The `cells_agree!` block checks at compile time that the index is its position.
#[must_use]
pub const fn replica_role_cell(value: ReplicaRole) -> usize {
    match value {
        ReplicaRole::Primary => 0,
        ReplicaRole::RegularSecondary => 1,
        ReplicaRole::Shadow => 2,
    }
}

/// `list[i]`'s cell is `i` for every `i`: the list holds each cell once, in the match's order.
macro_rules! cells_agree {
    ($list:ident, $cell:ident) => {{
        let mut index = 0;
        while index < $list.len() {
            assert!(
                $cell($list[index]) == index,
                concat!(stringify!($list), " disagrees with ", stringify!($cell))
            );
            index += 1;
        }
    }};
}

/// Compile-time half of the arity guard (ruling V-R23). With the exhaustive matches above, a new
/// variant is a compile error until it has an arm, and an arm whose index is not its position in
/// the list fails here. The runtime half is [`variants`], which row **M7V-56** compares with
/// each list, so an arm added without a list entry is a red too.
const _: () = {
    cells_agree!(REQUIRED, boundary_cell);
    cells_agree!(ACK_REJECT_REASONS, ack_reject_cell);
    cells_agree!(RECOVERY_MODES, recovery_mode_cell);
    cells_agree!(PROTECTION_PHASES, protection_phase_cell);
    cells_agree!(REPLICA_ROLES, replica_role_cell);
};

/// Every variant of a fieldless enum, in declaration order, read from the enum itself.
///
/// Serde's derive numbers the variants from 0 in declaration order and accepts the index as the
/// variant; the walk stops at the first index the enum refuses. So the count is the enum's arity,
/// not a literal (ruling V-R23). A variant marked `#[serde(skip)]` would be missed; none of the
/// enumerated enums has one.
#[must_use]
pub fn variants<T: DeserializeOwned>() -> Vec<T> {
    (0_u32..)
        .map_while(|index| T::deserialize(U32Deserializer::<ValueError>::new(index)).ok())
        .collect()
}

/// The family a boundary belongs to, as spike §6 groups them.
///
/// One `match` with no `_` arm, so a new `BoundaryId` fails to compile here rather than
/// defaulting into somebody's family.
#[must_use]
pub const fn family_of(boundary: BoundaryId) -> FaultKind {
    match boundary {
        BoundaryId::ChangedDigest
        | BoundaryId::OldGeneration
        | BoundaryId::LostSuccessReply
        | BoundaryId::RetainedDedupHit
        | BoundaryId::ExpiredDedup => FaultKind::Client,
        BoundaryId::StaleBoot
        | BoundaryId::StaleEpoch
        | BoundaryId::StaleConfig
        | BoundaryId::MissingPredecessor
        | BoundaryId::AckAfterRevocation
        | BoundaryId::ForgedIdentity => FaultKind::Network,
        BoundaryId::SameTickOrder
        | BoundaryId::GrantSkewWithinBound
        | BoundaryId::GrantSkewOutsideBound
        | BoundaryId::DedupWindowJump => FaultKind::Time,
        BoundaryId::BeforeAtomicCommit
        | BoundaryId::AfterAtomicCommit
        | BoundaryId::BeforeFlush
        | BoundaryId::AfterFlush
        | BoundaryId::FalseDurableWatermark => FaultKind::Storage,
        BoundaryId::StaleSnapshot
        | BoundaryId::LostControlQuorum
        | BoundaryId::InvalidGrant
        | BoundaryId::PartialStagedMetadata
        | BoundaryId::WatchGap => FaultKind::Control,
        BoundaryId::UnequalSecondaryPrefix
        | BoundaryId::LoneSurvivorChoice
        | BoundaryId::Divergence
        | BoundaryId::ReturningStaleOwner => FaultKind::Recovery,
    }
}

/// The package whose provider emits a boundary's `fault_injected`.
///
/// Keyed **per family**, so every member of one family gates on the same package (V-R20 (4)).
/// Without that, I1 landing before H1 or M1 would fail every `Network` and `Storage` cell as
/// `missing` — a red that contradicts "green with invariants unavailable, by design", and whose
/// reflex fix is deleting cells.
#[must_use]
pub const fn gated_by(boundary: BoundaryId) -> PackageId {
    match family_of(boundary) {
        // The scheduler, the manual clock, the controlled network and the fake control store are
        // all H1's providers.
        FaultKind::Network | FaultKind::Time | FaultKind::Control => PackageId::H1,
        // The memory engine's flush and crash paths.
        FaultKind::Storage => PackageId::M1,
        // The dispatcher applies client and recovery ops.
        FaultKind::Client | FaultKind::Recovery => PackageId::I1,
    }
}

/// Which axis a cell belongs to. The artifact names it beside the cell so a shortfall says what
/// kind of thing is missing, not only which name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Axis {
    /// [`REQUIRED`].
    Boundary,
    /// [`ACK_REJECT_REASONS`].
    AckReject,
    /// [`RECOVERY_MODES`].
    Recovery,
    /// [`PROTECTION_PHASES`].
    Protection,
    /// [`REPLICA_ROLES`].
    Role,
    /// [`ADMISSION_REASONS`].
    Admission,
    /// The derived quorum rule, from `required_copy_set.len()` (V-R20 (1)). There is no
    /// `QuorumRule` field in any trace event, and none is asked for.
    QuorumRule,
}

impl Axis {
    /// Every axis, so the report cannot omit one.
    pub const ALL: [Self; 7] = [
        Self::Boundary,
        Self::AckReject,
        Self::Recovery,
        Self::Protection,
        Self::Role,
        Self::Admission,
        Self::QuorumRule,
    ];

    /// The name written into `rdb-m7-coverage.json`.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Boundary => "boundary",
            Self::AckReject => "ack_reject_reason",
            Self::Recovery => "recovery_mode",
            Self::Protection => "protection_phase",
            Self::Role => "replica_role",
            Self::Admission => "admission_reason",
            Self::QuorumRule => "quorum_rule",
        }
    }
}

/// The required cells of every axis, as `(axis, cell name)`.
#[must_use]
pub fn required_cells() -> Vec<(Axis, String)> {
    let mut cells = Vec::new();
    for boundary in REQUIRED {
        cells.push((Axis::Boundary, format!("{boundary:?}")));
    }
    for reason in ACK_REJECT_REASONS {
        cells.push((Axis::AckReject, format!("{reason:?}")));
    }
    for mode in RECOVERY_MODES {
        cells.push((Axis::Recovery, format!("{mode:?}")));
    }
    for phase in PROTECTION_PHASES {
        cells.push((Axis::Protection, format!("{phase:?}")));
    }
    for role in REPLICA_ROLES {
        cells.push((Axis::Role, format!("{role:?}")));
    }
    for reason in ADMISSION_REASONS {
        cells.push((Axis::Admission, format!("{reason:?}")));
    }
    for rule in DerivedQuorumRule::ALL {
        cells.push((Axis::QuorumRule, rule.cell().to_owned()));
    }
    cells
}

/// One cell the run could not require, and why.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct UnavailableCell {
    /// Which axis.
    pub axis: Axis,
    /// Which cell.
    pub cell: String,
    /// The package whose provider would have emitted it.
    pub package: PackageId,
}

/// One cell that was required, reachable, and never hit.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Shortfall {
    /// Which axis.
    pub axis: Axis,
    /// Which cell.
    pub cell: String,
}

/// What a campaign observed, and what it concludes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoverageReport {
    /// How many seeds the corpus ran.
    pub seeds: usize,
    /// Whether the required-cell gate applies at all (V-R20 (7)).
    pub coverage_gated: bool,
    /// Cells that were required, reachable and never hit. Non-empty **and** gated is the only
    /// failing combination.
    pub required_missing: Vec<Shortfall>,
    /// Cells excluded by an `Unavailable` capability, named with their package.
    pub unavailable: Vec<UnavailableCell>,
    /// Every cell that was hit, with its count.
    pub hits: BTreeMap<(Axis, String), u32>,
}

impl CoverageReport {
    /// Whether the run fails on coverage.
    #[must_use]
    pub const fn fails(&self) -> bool {
        self.coverage_gated && !self.required_missing.is_empty()
    }
}

/// Judge an observed cell multiset against the required lists.
///
/// `capabilities` is the trace's own capability block. A cell whose gating package reports
/// [`CapabilityState::Unavailable`] is recorded as unavailable rather than missing — the
/// exclusion lives here, in the capability lookup, and never in [`REQUIRED`].
#[must_use]
pub fn evaluate(
    seeds: usize,
    hits: &BTreeMap<(Axis, String), u32>,
    capabilities: &BTreeMap<PackageId, CapabilityState>,
) -> CoverageReport {
    let unavailable_packages: BTreeSet<PackageId> = capabilities
        .iter()
        .filter(|(_, state)| **state == CapabilityState::Unavailable)
        .map(|(package, _)| *package)
        .collect();

    let mut required_missing = Vec::new();
    let mut unavailable = Vec::new();
    for (axis, cell) in required_cells() {
        if hits.contains_key(&(axis, cell.clone())) {
            continue;
        }
        match package_for(axis, &cell) {
            Some(package) if unavailable_packages.contains(&package) => {
                unavailable.push(UnavailableCell {
                    axis,
                    cell,
                    package,
                });
            }
            _ => required_missing.push(Shortfall { axis, cell }),
        }
    }

    CoverageReport {
        seeds,
        coverage_gated: seeds >= REQUIRED.len(),
        required_missing,
        unavailable,
        hits: hits.clone(),
    }
}

/// The gating package for one cell, when its axis has one. Only the boundary axis is gated on a
/// provider: the other axes are properties of events any wired package emits.
#[must_use]
pub fn package_for(axis: Axis, cell: &str) -> Option<PackageId> {
    if axis != Axis::Boundary {
        return None;
    }
    REQUIRED
        .iter()
        .find(|boundary| format!("{boundary:?}") == cell)
        .map(|boundary| gated_by(*boundary))
}
