//! The trace seam: what the harness records, and the only thing the oracle is allowed to read.
//!
//! This vocabulary is not a log. It is the *declaration* interface between the kernel and an
//! independent judge. Team verification specified every field (their
//! `teams/verification/trace-requirements.md`, 2026-09-20) as "one a checker reads", and this
//! module is that request folded into the contract, with three properties kept deliberately:
//!
//! 1. **No key or value bytes, ever** (team rules). A key is a [`KeyId`]; a value is a version
//!    plus a [`Digest`]. Identity and ordering are what a checker needs; content is not.
//! 2. **Closed sets.** Every outcome, reason, mode and state is a Rust enum. rEtcd's M6 rows
//!    M6-118 and M6-122 exist because open reason strings make assertions impossible.
//! 3. **One total order.** [`TraceEvent::event_id`] is strictly increasing, assigned by the
//!    harness. The oracle is a single left-to-right fold; it never sorts and never searches.
//!
//! ## Why this is a second vocabulary
//!
//! [`crate::contracts::event::Event`] is what the kernel is *fed*. A trace event is what the
//! system *declares it did*, and most of the interesting declarations — an authority decision at
//! four gates, the acknowledgement evidence a publication rested on, a recovery's queried
//! sources — are not scheduler events at all. Collapsing the two would force the oracle to
//! re-derive decisions from inputs, which is the second implementation of the protocol that
//! spike §6 forbids.
//!
//! ## Replay
//!
//! A seed is not enough (spike §4). Replay needs [`TraceHeader::schema_version`] **and**
//! [`TraceHeader::generator_version`] to match, and then replays the explicit event stream.

use serde::{Deserialize, Serialize};

use crate::contracts::control::{ControlKey, ControlPrefix, WatchTermination};
use crate::contracts::digest::Digest;
use crate::contracts::errors::ErrorKind;
use crate::contracts::event::Budgets;
use crate::contracts::ids::{
    BootId, ClientId, ConfigVersion, CorrelationId, EventId, Generation, GrantId, NodeId,
    OwnerEpoch, PartitionId, ReplicaRole, RequestId, Revision, ScenarioId, Seq, TenantId,
};

/// A key, as a stable small integer assigned by the scenario generator.
///
/// The reason there is no key type carrying bytes anywhere in this module.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct KeyId(
    /// The generator-assigned index.
    pub u32,
);

/// A key's version, as the oracle folds it into its published map. A delete is a tombstone
/// version, not an absence.
pub type Version = u64;

/// A back-reference to another event in the same trace.
///
/// Used where a checker would otherwise have to search backwards — the authority recheck a
/// publication rested on, or the barrier a read acquired.
pub type EventRef = EventId;

// ---------------------------------------------------------------------------------------------
// Header
// ---------------------------------------------------------------------------------------------

/// One node's place in the topology, recorded once in the header.
///
/// Field order is the sort order of [`TraceHeader::topology`] — `(partition, node)` — so the
/// derived [`Ord`] and the documented order are one thing (finding K-F-38).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TopologyEntry {
    /// The partition this entry is about.
    pub partition: PartitionId,
    /// The node.
    pub node: NodeId,
    /// What it is allowed to do for the protection predicate.
    pub role: ReplicaRole,
    /// The membership configuration this placement belongs to.
    pub config_version: ConfigVersion,
}

/// Where a scenario came from (finding K-F-09; team verification `trace-requirements.md` §1).
///
/// Never a bare seed. A reduced or authored scenario is not in the generator's image, so
/// replaying its seed reproduces nothing; the checked-in event stream is the reproducer
/// (ADR-rdb-0003 decision 6), and this says which of the three ways the stream was made.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Provenance {
    /// The generator produced it from a seed.
    Generated {
        /// The seed. For the report; never sufficient for replay on its own.
        seed: u64,
    },
    /// The reducer shrank it from another scenario.
    Reduced {
        /// The scenario it was shrunk from.
        parent: ScenarioId,
    },
    /// A person wrote it.
    Authored {
        /// The case name, as the author gave it. A test name, never key or value bytes.
        case: String,
    },
}

/// Which field of [`Budgets`] a name refers to. One member per field, in field order, so the
/// manifest can say which budgets a run overrode without a string that can be misspelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum BudgetName {
    /// [`Budgets::warn_age_millis`].
    WarnAge,
    /// [`Budgets::pause_age_millis`].
    PauseAge,
    /// [`Budgets::resume_lag_millis`].
    ResumeLag,
    /// [`Budgets::resume_hold_millis`].
    ResumeHold,
    /// [`Budgets::grant_millis`].
    Grant,
    /// [`Budgets::renew_millis`].
    Renew,
    /// [`Budgets::clock_error_millis`].
    ClockError,
    /// [`Budgets::dispatch_margin_millis`].
    DispatchMargin,
    /// [`Budgets::dedup_retention_millis`].
    DedupRetention,
    /// [`Budgets::discovery_window_millis`].
    DiscoveryWindow,
}

impl BudgetName {
    /// Every budget, in [`Budgets`] field order. A resolver that walks this cannot skip one.
    pub const ALL: [Self; 10] = [
        Self::WarnAge,
        Self::PauseAge,
        Self::ResumeLag,
        Self::ResumeHold,
        Self::Grant,
        Self::Renew,
        Self::ClockError,
        Self::DispatchMargin,
        Self::DedupRetention,
        Self::DiscoveryWindow,
    ];

    /// The value this name selects in `budgets`.
    #[must_use]
    pub const fn get(self, budgets: &Budgets) -> u64 {
        match self {
            Self::WarnAge => budgets.warn_age_millis,
            Self::PauseAge => budgets.pause_age_millis,
            Self::ResumeLag => budgets.resume_lag_millis,
            Self::ResumeHold => budgets.resume_hold_millis,
            Self::Grant => budgets.grant_millis,
            Self::Renew => budgets.renew_millis,
            Self::ClockError => budgets.clock_error_millis,
            Self::DispatchMargin => budgets.dispatch_margin_millis,
            Self::DedupRetention => budgets.dedup_retention_millis,
            Self::DiscoveryWindow => budgets.discovery_window_millis,
        }
    }

    /// Set the value this name selects in `budgets`.
    pub const fn set(self, budgets: &mut Budgets, millis: u64) {
        match self {
            Self::WarnAge => budgets.warn_age_millis = millis,
            Self::PauseAge => budgets.pause_age_millis = millis,
            Self::ResumeLag => budgets.resume_lag_millis = millis,
            Self::ResumeHold => budgets.resume_hold_millis = millis,
            Self::Grant => budgets.grant_millis = millis,
            Self::Renew => budgets.renew_millis = millis,
            Self::ClockError => budgets.clock_error_millis = millis,
            Self::DispatchMargin => budgets.dispatch_margin_millis = millis,
            Self::DedupRetention => budgets.dedup_retention_millis = millis,
            Self::DiscoveryWindow => budgets.discovery_window_millis = millis,
        }
    }
}

/// The resolved run: what a scenario actually ran under (finding K-F-27; spike §7 "the
/// manifest records resolved budgets").
///
/// Plain data in the contract crate, because the header carries it and the header is here; no
/// simulator type crosses the crate boundary. `overridden` is the field that matters: a
/// campaign row that fails under an override must not be mistaken for one that fails under
/// defaults — the `RETCD_TEST_DEADLINE_SCALE` lesson from the rEtcd gate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunManifest {
    /// The thresholds the run resolved to.
    pub budgets: Budgets,
    /// Which of them differ from [`Budgets::SPEC_DEFAULTS`], in [`BudgetName::ALL`] order.
    pub overridden: Vec<BudgetName>,
    /// How many nodes the topology has.
    pub nodes: u8,
    /// The event budget the run was bounded by.
    pub event_cap: u32,
}

/// Everything a replay or a report needs before the first event.
///
/// `deny_unknown_fields` on purpose (team verification `trace-requirements.md` §1): a header
/// field this build does not know is a header from another build, and reading it as if it were
/// this one is how a fixture passes for the wrong reason.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TraceHeader {
    /// [`crate::contracts::version::TRACE_SCHEMA_VERSION`] at record time. A bump invalidates
    /// checked-in fixtures on purpose.
    pub schema_version: u16,
    /// The scenario generator's version. Two generators at one seed are two different runs.
    pub generator_version: u16,
    /// Where the scenario came from. Never a bare seed (finding K-F-09).
    pub provenance: Provenance,
    /// The resolved run: budgets, which were overridden, node count and event cap.
    pub config: RunManifest,
    /// How many partitions the topology has.
    pub partitions: u8,
    /// Node roles and configuration versions, in ascending `(partition, node)` order.
    ///
    /// The **initial snapshot only** (lead ruling V-R12, 2026-09-20). Every later membership
    /// change — a partition degraded to two copies, a rebuilt third copy, a CAS of normal
    /// membership — arrives as a [`TraceKind::TopologyChange`] event. The oracle resolves a
    /// peer's role from the topology in force at the `config_version` an acknowledgement
    /// carried, never from this field, because this field is only ever true at tick zero.
    pub topology: Vec<TopologyEntry>,
    /// Digest over the oracle checkpoints, for replay equality. Not read by any checker.
    pub oracle_checkpoint_digest: Digest,
}

// ---------------------------------------------------------------------------------------------
// Closed sets
// ---------------------------------------------------------------------------------------------

/// Whether admission let a request through.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum AdmissionOutcome {
    /// Admitted to the partition queue.
    Admitted,
    /// Rejected before any mutation.
    Rejected,
}

/// Which of spec §5.2's four revalidation points an authority decision was made at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum AuthorityGate {
    /// Before the request enters the queue.
    Admission,
    /// Immediately before the storage batch is dispatched.
    Dispatch,
    /// Before the applied prefix is published.
    Publication,
    /// Before the client reply is sent.
    Reply,
}

/// How an authority check came out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum AuthorityOutcome {
    /// The grant is valid for this node, boot and epoch.
    Valid,
    /// The grant's expiry has provably passed.
    Expired,
    /// The grant was frozen or revoked by the planner.
    Fenced,
    /// The clock bound spans the decision. Must fail closed (spec §7.2).
    Uncertain,
}

/// How a storage batch ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ApplyOutcome {
    /// The whole batch is applied.
    Applied,
    /// The batch failed. Nothing in it is visible.
    Failed,
    /// A crash was injected before the atomic commit point. None of the batch survives.
    CrashedBeforeCommit,
    /// A crash was injected after the atomic commit point. All of the batch survives.
    CrashedAfterCommit,
}

/// How strongly a replica holds a prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum DurabilityClass {
    /// The engine batch completed. Not on disk.
    Buffered,
    /// An fsync boundary confirmed it.
    Durable,
}

/// Why a replica refused an append, as the oracle sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum AckRejectReason {
    /// The predecessor is missing.
    Gap,
    /// Same sequence, different digest.
    DigestMismatch,
    /// The sender's epoch is not current.
    StaleEpoch,
    /// The sender's boot is not the live one.
    StaleBoot,
    /// The sender's membership configuration is not the pinned one.
    StaleConfig,
    /// The sender was not authenticated.
    ForgedIdentity,
    /// An unknown mandatory version, refused before the body was decoded.
    IncompatibleVersion,
}

/// How a `sync_wal_through` ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum SyncOutcome {
    /// Unambiguous success. Only this publishes the captured prefixes.
    Synced,
    /// Errored. No watermark moves.
    Failed,
    /// Completed partially. Also no watermark moves (spec §6.1).
    Partial,
}

/// What a client was actually told.
///
/// Closed, and deliberately three-way rather than two. `RecoveredApplied` is a success the client
/// is told about differently (spec §5.2): the transaction survived recovery rather than being
/// published by its own primary. Folding it into [`Self::Success`] would make "a recovered
/// transaction was reported as an ordinary publication" unobservable, and that is precisely the
/// confusion the oracle exists to catch. The error arm carries [`ErrorKind`], which is itself the
/// closed set of spec §5.4 errors — one enum, not a second copy that can drift.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ClientOutcome {
    /// Published by the owning primary, with a generation and a sequence.
    Success,
    /// Applied through recovery, with a generation and a sequence. Distinct from
    /// [`Self::Success`] on the wire and in the trace.
    RecoveredApplied,
    /// One of the spec §5.4 errors.
    Error(ErrorKind),
}

/// What kind of reader acquired the publication barrier.
///
/// All four are listed in spec §5.3 and all four must go through the barrier; a maintenance
/// export that read the raw prefix would be the bug.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ReadRequestKind {
    /// An API read.
    Read,
    /// A status query.
    Status,
    /// A snapshot or export worker.
    Export,
    /// An actor-local reader.
    ActorRead,
}

/// How a read was *served*.
///
/// Named apart from [`crate::contracts::control::ReadOutcome`] on purpose (lead ruling F-R4,
/// 2026-09-20). That one is what the control store said about a record; this one is how the data
/// plane served a reader. Module paths kept them apart, but a trace field and a control field in
/// the same function would not have, and the oracle folds both.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ReadServiceOutcome {
    /// Served from the published prefix.
    Served,
    /// Waited at the barrier for the in-flight transaction, then served.
    WaitedAtBarrier,
    /// Refused.
    Rejected(ErrorKind),
}

/// What happened to a dedup record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum DedupAction {
    /// Stored atomically with the transaction it describes.
    Store,
    /// A retry matched a retained identity and digest; no second effect.
    Hit,
    /// A retry matched an identity with a different digest: `REQUEST_ID_REUSE`.
    ReuseReject,
    /// The retention window passed. Absence is not proof of nonexecution.
    Expire,
}

/// Where a lineage root came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum LineageSource {
    /// The partition's first generation.
    Initial,
    /// A recovery decision created it.
    Recovery,
}

/// What a recovery decided to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum RecoveryMode {
    /// Two survivors synchronised; both required for every subsequent ACK (spec §8.3).
    TwoSurvivor,
    /// One survivor; serve the declared prefix read-only until three copies return (spec §8.4).
    LoneSurvivorReadOnly,
    /// Divergence or corruption. Automatic promotion is blocked (spec §8.1).
    Quarantine,
}

/// Why something was quarantined.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum QuarantineReason {
    /// Different digests at one lineage position.
    DigestConflict,
    /// A record failed its own digest check, or required history is missing.
    CorruptHistory,
    /// An unknown mandatory version on a stored record.
    IncompatibleVersion,
}

/// The lag-protection state machine of spec §6.2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ProtectionPhase {
    /// Within budget.
    Healthy,
    /// Oldest unsafe age at or above the warning threshold.
    Warn,
    /// At or above the pause threshold, or no secondary can ACK. Admission stops.
    Paused,
    /// Copies are catching up; the exact barrier and the hysteresis are not yet met.
    Resuming,
}

/// Which surface carried a version that was checked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum VersionSurface {
    /// A transport frame.
    Message,
    /// A stored or control record.
    Record,
}

/// How a version check ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum VersionOutcome {
    /// Understood; the body was decoded.
    Accept,
    /// Refused **before** the body was decoded (validation-plan V12).
    RefuseBeforeApply,
}

/// The six scenario groups of spike §6.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum FaultKind {
    /// submit, read, status, retry.
    Client,
    /// deliver, drop, duplicate, reorder, partition, heal.
    Network,
    /// advance, fire, cancel, expire, pause, resume.
    Time,
    /// complete batch, fail batch, flush, crash, reopen.
    Storage,
    /// CAS, emit watch, gap, compact, reload.
    Control,
    /// inspect survivors, select prefix, synchronize, rebuild.
    Recovery,
}

/// The "required boundary cases" column of spike §6, as a closed set.
///
/// Closed so the coverage matrix is *countable*: "every required cell at least once" is not a
/// rule that can be written against a free-text boundary name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum BoundaryId {
    /// Client: a retry with the same identity and a different payload digest.
    ChangedDigest,
    /// Client: a retry carrying a stale expected generation.
    OldGeneration,
    /// Client: the success reply is lost after publication.
    LostSuccessReply,
    /// Client: a retry inside the retention window.
    RetainedDedupHit,
    /// Client: a retry after retention expired.
    ExpiredDedup,
    /// Network: a frame from a boot that is no longer live.
    StaleBoot,
    /// Network: a frame from an epoch that is no longer current.
    StaleEpoch,
    /// Network: a frame pinned to an old membership configuration.
    StaleConfig,
    /// Network: an append whose predecessor the replica does not hold.
    MissingPredecessor,
    /// Network: an acknowledgement arriving after the grant was revoked.
    AckAfterRevocation,
    /// Network: a frame delivered under an identity its sender did not earn — another node's
    /// name, or a claimed role above its own. Rejection is the only correct outcome, and it must
    /// come from the kernel's own authentication and membership check, never from a test-only
    /// branch.
    ForgedIdentity,
    /// Time: two events on one tick, in both orders.
    SameTickOrder,
    /// Time: grant skew inside the +/-100 ms assumption.
    GrantSkewWithinBound,
    /// Time: grant skew outside it, which must fail closed.
    GrantSkewOutsideBound,
    /// Time: a jump across the 24 h dedup window.
    DedupWindowJump,
    /// Storage: a crash immediately before the atomic commit point.
    BeforeAtomicCommit,
    /// Storage: a crash immediately after it.
    AfterAtomicCommit,
    /// Storage: a crash before the flush boundary.
    BeforeFlush,
    /// Storage: a crash after it.
    AfterFlush,
    /// Storage: an attempt to advance a durable watermark without a successful flush.
    FalseDurableWatermark,
    /// Control: a read served from a stale snapshot.
    StaleSnapshot,
    /// Control: the control quorum is lost.
    LostControlQuorum,
    /// Control: a grant record that does not validate.
    InvalidGrant,
    /// Control: staged metadata that was never activated by the pointer CAS.
    PartialStagedMetadata,
    /// Control: a watch gap forcing a coherent reload.
    WatchGap,
    /// Recovery: survivors ending at different sequences.
    UnequalSecondaryPrefix,
    /// Recovery: each of the lone-survivor choices.
    LoneSurvivorChoice,
    /// Recovery: different digests at one position.
    Divergence,
    /// Recovery: an old owner returning with a longer suffix.
    ReturningStaleOwner,
}

/// Whether the schedule is still adversarial or has healed.
///
/// The only thing that arms the liveness checker. Spike §6 forbids calling an unhealed partition
/// a liveness failure, so the oracle must be *told*, never left to infer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum SchedulePhase {
    /// Faults are still being injected. Safety only.
    Chaotic,
    /// Delivery is fair and authority is valid. Liveness may be checked from here.
    Healed,
}

/// A spike work package, as spike §5 names them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum PackageId {
    /// Contracts and crate boundary.
    C0,
    /// Deterministic environment.
    H1,
    /// Memory storage and crash images.
    M1,
    /// Dispatch, replay and CI.
    I1,
    /// Authority and fencing.
    A1,
    /// Transactional KV and dedup.
    T1,
    /// Replication and progress.
    R1,
    /// Publication and outcomes.
    P1,
    /// Lag protection.
    L1,
    /// Recovery and rebuild.
    F1,
}

/// Whether a package is wired in this build.
///
/// Emitted once per package at trace start. Without it a campaign cannot tell "no violation"
/// from "nothing ran", which is the most dangerous false green available in this milestone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum CapabilityState {
    /// Its handler is registered and executes.
    Wired,
    /// Its handler returns [`crate::contracts::errors::RdbError::Unavailable`].
    Unavailable,
}

/// One source a recovery queried, and what it said.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct QueriedSource {
    /// The node asked.
    pub node: NodeId,
    /// Its process lifetime.
    pub boot: BootId,
    /// Its role. A shadow may be a validated recovery source but never a primary.
    pub role: ReplicaRole,
    /// Whether it answered within the discovery window. Recording an unreachable source
    /// *before* choosing a shorter prefix is what separates permitted loss from a bug.
    pub reachable: bool,
    /// The generation it reported, when it answered.
    pub reported_generation: Option<Generation>,
    /// The highest contiguous sequence it reported.
    pub reported_seq: Option<Seq>,
    /// The digest at that sequence.
    pub reported_digest: Option<Digest>,
}

/// One acknowledgement a publication rested on.
///
/// Team verification's §3.7 four-tuple (finding K-F-22): two acknowledgements from one node
/// across a restart are two boots, and the checker counts one copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AckEvidence {
    /// Who acknowledged.
    pub node: NodeId,
    /// Its process lifetime when it did.
    pub boot: BootId,
    /// In what role. A shadow entry here must never qualify the publication.
    pub role: ReplicaRole,
    /// How strongly.
    pub durability: DurabilityClass,
}

/// The acknowledgement rule in force when a write was acknowledged (finding K-F-07; team
/// verification §3.14).
///
/// Not carried on [`TraceKind::ProtectionState`]: lead ruling V-R20 (2026-09-20) has the
/// oracle derive it from `required_copy_set` and the membership in force, so the kernel cannot
/// declare a rule its acknowledgements did not follow. The enum is the oracle's vocabulary for
/// that derivation and for the report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum QuorumRule {
    /// Three copies: durable on the primary and acknowledged by two secondaries (spec §5.2).
    Rf3,
    /// Two survivors: both required for every acknowledgement (spec §8.3).
    DegradedRf2,
}

/// Why a scenario operation was not applied (finding K-F-08; team verification §3.16a).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum SkipReason {
    /// The thing the operation referred to no longer exists — a reducer removed the request it
    /// was a retry of, or the node it targeted.
    ReferentGone,
    /// The run's bound was reached before the operation's turn.
    OutOfBudget,
}

/// Which control-store operation a [`TraceKind::ControlInteraction`] is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ControlOpKind {
    /// [`crate::contracts::control::ControlEffect::Cas`].
    Cas,
    /// [`crate::contracts::control::ControlEffect::Get`].
    Get,
    /// [`crate::contracts::control::ControlEffect::Watch`], or an event on the stream it opened.
    Watch,
    /// [`crate::contracts::control::ControlEffect::Reload`].
    Reload,
}

/// How a control-store operation came out, as the environment declares it.
///
/// One closed set over the three control outcome types, so the oracle folds one field. A
/// termination carries its [`WatchTermination`] and, spelled out, whether it was a gap — the
/// oracle must not have to know which terminations gap (ADR-rdb-0008 §4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ControlOutcomeKind {
    /// A CAS committed.
    Committed,
    /// A CAS lost.
    Conflict,
    /// A CAS may or may not have landed.
    Unknown,
    /// The store could not be reached.
    Unavailable,
    /// A read found the record.
    Found,
    /// A read found no record.
    Absent,
    /// A watch progress tick, or a contiguous run of changes.
    Progress,
    /// A watch ended.
    Terminated {
        /// Why.
        termination: WatchTermination,
        /// [`WatchTermination::is_gap`] of it, so the oracle reads a field rather than a rule.
        gap: bool,
    },
}

// ---------------------------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------------------------

/// One recorded declaration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceEvent {
    /// Strictly increasing within the trace. The oracle folds in this order.
    pub event_id: EventId,
    /// Simulated time. Never a wall clock.
    pub logical_tick: u64,
    /// The partition.
    pub partition: PartitionId,
    /// The node it happened on.
    pub node: NodeId,
    /// That node's process lifetime.
    pub boot: BootId,
    /// Ties a request to its applies, acknowledgements, publication and outcome. Load-bearing
    /// for the atomicity, dedup and version invariants.
    pub correlation: CorrelationId,
    /// What was declared.
    pub kind: TraceKind,
}

/// Everything the harness can declare. Ordered roughly along the write path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TraceKind {
    /// A client sent a transaction.
    ClientSubmit {
        /// The request id.
        request: RequestId,
        /// The tenant.
        tenant: TenantId,
        /// The client.
        client: ClientId,
        /// The affinity group. Recorded as the generator's group index.
        affinity: u64,
        /// The generation the caller expected, if any.
        expected_generation: Option<Generation>,
        /// Digest of the request payload.
        request_digest: Digest,
        /// Remaining deadline in milliseconds at send time.
        deadline_remaining_ms: u64,
        /// Keys the mutations touch.
        mutation_keys: Vec<KeyId>,
        /// Keys the conditions read.
        condition_keys: Vec<KeyId>,
    },

    /// Admission control decided.
    AdmissionDecision {
        /// Admitted or rejected.
        outcome: AdmissionOutcome,
        /// Why, when rejected. `None` when admitted.
        reason: Option<ErrorKind>,
        /// The sequence it was admitted at.
        admitted_seq: Option<Seq>,
        /// Whether the partition was paused at this moment.
        paused: bool,
        /// Age of the oldest transaction not durably on the required copies.
        oldest_unsafe_age_ms: u64,
        /// The required-copy set, as pinned by `config_version`.
        required_copies: Vec<NodeId>,
        /// The configuration that set pins to.
        config_version: ConfigVersion,
    },

    /// An authority check at one of the four gates.
    AuthorityDecision {
        /// Which gate.
        gate: AuthorityGate,
        /// The node claiming ownership.
        owner_node: NodeId,
        /// Its epoch.
        owner_epoch: OwnerEpoch,
        /// The grant it holds.
        grant: GrantId,
        /// The boot the grant was issued to.
        grant_boot: BootId,
        /// The lineage.
        generation: Generation,
        /// Earliest tick the grant is valid from.
        valid_from_tick: u64,
        /// The grant's expiry tick.
        expiry_tick: u64,
        /// When the decision was taken.
        decision_tick: u64,
        /// The decision's position in A1's per-node monotonic sequence (lead ruling A-R23;
        /// team kernel-a `design.md` §1.2). Strictly increasing per node, so the oracle can
        /// tell a stale decision from a later one without comparing ticks.
        authority_seq: u64,
        /// How it came out.
        outcome: AuthorityOutcome,
    },

    /// A storage batch reached a boundary.
    BatchApply {
        /// The role of the node applying it.
        role: ReplicaRole,
        /// The lineage.
        generation: Generation,
        /// This transaction's position.
        seq: Seq,
        /// The position it descends from.
        predecessor_seq: Seq,
        /// The digest it descends from.
        predecessor_digest: Digest,
        /// This record's digest.
        entry_digest: Digest,
        /// The batch identity.
        batch: u64,
        /// The after-image identity for every key the batch touched, deletes included as a
        /// tombstone version.
        key_versions: Vec<(KeyId, Version)>,
        /// How it ended.
        outcome: ApplyOutcome,
    },

    /// An append was sent to a peer.
    ReplicationSend {
        /// Sender.
        from_node: NodeId,
        /// Intended recipient.
        to_node: NodeId,
        /// The recipient's role.
        peer_role: ReplicaRole,
        /// The lineage.
        generation: Generation,
        /// The sender's epoch.
        owner_epoch: OwnerEpoch,
        /// The configuration the sender was pinned to.
        config_version: ConfigVersion,
        /// The position sent.
        seq: Seq,
        /// Its digest.
        digest: Digest,
    },

    /// A peer answered an append.
    ///
    /// Emitted **at the secondary, where the acknowledgement is generated**, and the envelope's
    /// `node` is therefore the acknowledging node. What the primary later counts is a separate
    /// [`Self::ReplicationAckDelivered`]. Two records rather than one because they are two
    /// different claims: what a copy holds, and what the primary believed when it counted. An ack
    /// that is generated and then dropped, delayed past a decision, or delivered to a primary that
    /// has moved on is only visible as the gap between them.
    ReplicationAck {
        /// The acknowledging node.
        from_node: NodeId,
        /// The node that had sent the append.
        to_node: NodeId,
        /// The acknowledging node's role. Without this, "count a shadow ACK" is uncatchable.
        peer_role: ReplicaRole,
        /// Its process lifetime.
        peer_boot: BootId,
        /// The configuration it was pinned to.
        config_version: ConfigVersion,
        /// The lineage.
        generation: Generation,
        /// The epoch it believed current.
        owner_epoch: OwnerEpoch,
        /// The highest contiguous sequence it holds.
        contiguous_seq: Seq,
        /// The digest at that sequence.
        contiguous_digest: Digest,
        /// How strongly it holds it.
        durability_class: DurabilityClass,
        /// Whether the append was accepted.
        accepted: bool,
        /// Why not, when refused.
        reject_reason: Option<AckRejectReason>,
    },

    /// The primary received an acknowledgement and was willing to count it.
    ///
    /// Emitted **at the primary**, and the envelope's `node` is therefore the primary. Carries a
    /// back-reference to the [`Self::ReplicationAck`] it corresponds to, so the checker can pair
    /// them without re-deriving a matching rule. A durability advance that rests on an ack with no
    /// delivery record, or with one at a stale `config_version`, is the bug this pairing finds.
    ReplicationAckDelivered {
        /// Which acknowledgement, by its `event_id`.
        ack: EventRef,
        /// The node that produced it.
        from_node: NodeId,
        /// Its role **as the receiving primary resolved it**, from its own pinned configuration.
        /// Not copied from the sender: a shadow that claims to be a regular copy is caught here,
        /// as a disagreement between this field and the ack's own `peer_role`.
        peer_role: ReplicaRole,
        /// The configuration the receiver was pinned to when it counted the ack.
        config_version: ConfigVersion,
        /// Whether it was counted towards durability at all. `false` with a reason on the ack is
        /// a normal rejection; `false` with an accepted ack means the receiver discarded it.
        counted: bool,
    },

    /// An fsync boundary completed.
    DurabilityAdvance {
        /// The lineage.
        generation: Generation,
        /// The new durable position, when it advanced.
        durable_seq: Seq,
        /// The digest at that position.
        durable_digest: Digest,
        /// The flush identity.
        flush_ticket: u64,
        /// The prefixes captured under the write-order mutex.
        captured: Vec<(PartitionId, Seq)>,
        /// How it ended. Only [`SyncOutcome::Synced`] may advance anything.
        outcome: SyncOutcome,
    },

    /// A prefix became client-visible.
    Publish {
        /// The lineage.
        generation: Generation,
        /// The position published.
        seq: Seq,
        /// Digest of the record at that position.
        published_digest: Digest,
        /// The acknowledgements this publication rested on.
        ack_evidence: Vec<AckEvidence>,
        /// The `AuthorityDecision` at the publication gate that authorised it. An explicit
        /// back-reference, so the checker stays a forward fold.
        authority_recheck: EventRef,
    },

    /// A client was told something.
    ClientOutcomeReported {
        /// Which request.
        request: RequestId,
        /// What it was told.
        outcome: ClientOutcome,
        /// The lineage reported.
        generation: Generation,
        /// The position, on success.
        seq: Option<Seq>,
        /// Digest of the returned result.
        result_digest: Digest,
        /// Whether the reply actually reached the client. `false` models a lost reply, which
        /// must never retract a publication (spec §5.3).
        delivered: bool,
    },

    /// A reader observed state through the publication barrier.
    Read {
        /// What kind of reader.
        request_kind: ReadRequestKind,
        /// The barrier it acquired.
        barrier: EventRef,
        /// The lineage it read in.
        generation: Generation,
        /// The published position it saw.
        observed_seq: Seq,
        /// What it saw.
        observed_key_versions: Vec<(KeyId, Version)>,
        /// Whether the partition is in read-only recovery.
        recovery_mode: bool,
        /// How it was served.
        outcome: ReadServiceOutcome,
    },

    /// A dedup record moved.
    DedupRecord {
        /// The tenant.
        tenant: TenantId,
        /// The client.
        client: ClientId,
        /// The request.
        request: RequestId,
        /// Digest of the payload that was retained.
        request_digest: Digest,
        /// Digest of the result that was retained.
        result_digest: Digest,
        /// The lineage it belongs to.
        generation: Generation,
        /// When retention ends.
        retained_until_tick: u64,
        /// What happened.
        action: DedupAction,
    },

    /// A lineage root was established.
    LineageRoot {
        /// The lineage it starts.
        generation: Generation,
        /// The epoch that established it.
        owner_epoch: OwnerEpoch,
        /// Its base position.
        base_seq: Seq,
        /// The digest at that position.
        base_digest: Digest,
        /// The lineage it descends from, when it is not the first.
        predecessor_generation: Option<Generation>,
        /// The predecessor position it cuts off at. The single field that keeps restricted loss
        /// checkable instead of collapsing into an impossible global no-loss oracle.
        predecessor_cutoff: Option<Seq>,
        /// Where it came from.
        source: LineageSource,
    },

    /// A recovery chose.
    RecoveryDecision {
        /// The epoch that was fenced first.
        fenced_epoch: OwnerEpoch,
        /// How long the discovery window ran, in ticks.
        discovery_window_ticks: u64,
        /// Every source asked, and whether it answered.
        queried_sources: Vec<QueriedSource>,
        /// The source whose prefix was selected.
        selected_source: Option<NodeId>,
        /// The position selected.
        selected_cutoff_seq: Seq,
        /// The digest at that position.
        selected_digest: Digest,
        /// What was decided.
        mode: RecoveryMode,
        /// Whether a suffix may have been lost without proof either way.
        loss_uncertainty: bool,
        /// The lineage created.
        new_generation: Generation,
    },

    /// Something was quarantined.
    Quarantine {
        /// Why.
        reason: QuarantineReason,
        /// The lineage.
        generation: Generation,
        /// Where.
        seq: Seq,
        /// The nodes involved.
        sources: Vec<NodeId>,
    },

    /// The lag-protection state machine moved, or restated itself.
    ///
    /// Emitted on every [`ProtectionPhase`] transition **and on every `config_version` change,
    /// whether or not the phase moved**. Spec §6.2's trap is a membership edit that renames the
    /// required-copy set and thereby resets the unsafe age without anything catching up. If this
    /// event only fired on phase transitions, that edit would leave no record and the oracle would
    /// see a legitimately healthy run.
    ProtectionState {
        /// The phase.
        phase: ProtectionPhase,
        /// Age of the oldest transaction not durably on the required copies.
        oldest_unsafe_age_ms: u64,
        /// The required-copy set.
        required_copy_set: Vec<NodeId>,
        /// The configuration that set is pinned to. On the same event so that "unsafe age reset
        /// via membership renaming" is catchable (spec §6.2).
        config_version: ConfigVersion,
        /// The prefix admission paused after.
        paused_prefix_seq: Seq,
        /// The exact durable barrier required to resume.
        resume_barrier_seq: Seq,
        /// When lag first became healthy, for the hysteresis.
        healthy_since_tick: Option<u64>,
    },

    /// A mandatory version was checked.
    VersionCheck {
        /// Which surface carried it.
        surface: VersionSurface,
        /// The protocol version declared.
        declared_protocol_version: u16,
        /// The membership configuration declared.
        declared_config_version: ConfigVersion,
        /// The schema version declared.
        declared_schema_version: u16,
        /// The highest this build understands.
        known_max: u16,
        /// Mandatory fields the decoder did not recognise, by field number.
        mandatory_unknown_fields: Vec<u16>,
        /// How it ended.
        outcome: VersionOutcome,
    },

    /// The scenario injected a fault.
    FaultInjected {
        /// Which scenario group.
        fault_kind: FaultKind,
        /// The node it was aimed at.
        target: NodeId,
        /// The required boundary case it exercises.
        boundary: BoundaryId,
        /// Its index in the scenario's operation list, for shrinking.
        scenario_op_index: u32,
    },

    /// Membership changed.
    ///
    /// **Environment-owned.** Emitted by the H1 control provider when it activates a new
    /// membership, never by a kernel module (lead ruling V-R12, 2026-09-20). A kernel module
    /// emitting this would mean the oracle resolves roles from what the kernel believed, which
    /// is exactly the belief under test — a shadow counted as a regular copy would then be
    /// self-consistent and invisible.
    ///
    /// Together with the header's initial snapshot, these events are the complete topology
    /// history, and every `config_version` an acknowledgement can carry has one.
    TopologyChange {
        /// The membership version being activated. Strictly increasing per partition.
        config_version: ConfigVersion,
        /// The new placement, in ascending [`NodeId`] order.
        nodes: Vec<(NodeId, ReplicaRole)>,
    },

    /// The schedule changed character.
    SchedulePhaseChanged {
        /// The new phase.
        phase: SchedulePhase,
        /// Whether delivery is now fair.
        fair_delivery: bool,
        /// How many events the liveness check may still consume.
        remaining_event_budget: u32,
    },

    /// A package reported whether it is wired. Emitted once per package at trace start.
    Capability {
        /// Which package.
        package: PackageId,
        /// Wired or unavailable.
        state: CapabilityState,
    },

    /// A scenario operation was not applied (finding K-F-08; team verification §3.16a).
    ///
    /// Its own kind on purpose: a reducer artifact is not a [`BoundaryId`], or the coverage
    /// matrix gains a cell nobody can interpret. Not read by a checker; read by the reducer.
    /// Without it a deduplicated retry that produces nothing is indistinguishable from a
    /// request never submitted.
    OpSkipped {
        /// Its index in the scenario's operation list, the same index
        /// [`Self::FaultInjected`] carries.
        scenario_op_index: u32,
        /// Why.
        reason: SkipReason,
    },

    /// A control-store interaction completed, and how (finding K-F-06).
    ///
    /// **Environment-owned**, like [`Self::TopologyChange`]: emitted by the H1 control
    /// provider as it completes the effect, so the kernel cannot lie about what the store said.
    /// A watch termination arrives here with its `gap`, which is what
    /// [`Self::FamilyReload`] refers back to.
    ControlInteraction {
        /// Which operation.
        op: ControlOpKind,
        /// The record, for a CAS or a read.
        key: Option<ControlKey>,
        /// The family, for a watch or a reload.
        prefix: Option<ControlPrefix>,
        /// How it came out.
        outcome: ControlOutcomeKind,
    },

    /// A coherent reload of one family (finding K-F-06).
    ///
    /// Emitted by the kernel module that asked for it, because the *decision* to reload is the
    /// thing under test. `after_termination` is the explicit back-reference to the
    /// [`Self::ControlInteraction`] whose termination justified it, and `None` is the bug:
    /// ADR-rdb-0008 §7 item 4 as amended by lead ruling A-R15 — no reload unless a termination
    /// was delivered first — is two events the oracle relates by a back-reference, not a search.
    FamilyReload {
        /// The family reloaded.
        prefix: ControlPrefix,
        /// The revision the snapshot was coherent at, and the resumed watch starts after.
        snapshot_revision: Revision,
        /// The termination that justified it, by its `event_id`.
        after_termination: Option<EventRef>,
    },
}

/// A whole recorded run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Trace {
    /// What the run was.
    pub header: TraceHeader,
    /// What it declared, in `event_id` order.
    pub events: Vec<TraceEvent>,
}
