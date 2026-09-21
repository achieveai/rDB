//! The public error set and what a caller is allowed to do about each one.
//!
//! Spec §5.4 is a table of errors and retry rules. That table is a safety contract, not
//! documentation: the difference between "definitive rejection" and "unknown outcome" is the
//! difference between a caller safely reissuing and a caller silently duplicating a mutation.
//! [`RdbError::retry_rule`] puts the mapping in one place so six kernel modules cannot each
//! get it slightly wrong.
//!
//! [`RdbError::Unavailable`] is the spike's own variant, not a spec error. Spike §8 permits
//! "a temporary explicit unavailable result ... for an unimplemented capability; a fake success
//! is not". Every unwired seam returns it, and no stub in this crate ever panics: a `todo!()`
//! would abort the campaign runner instead of letting it report an unwired capability.

use serde::{Deserialize, Serialize};

use crate::contracts::ids::{
    AffinityId, Generation, GrantId, NodeId, PartitionId, RequestIdentity, Seq,
};
use crate::contracts::version::VersionedArtifact;

/// A capability that a build may not have wired yet.
///
/// Named rather than a string so the harness can assert *which* seam was missing, and so a
/// campaign run can report unwired capabilities as a list instead of a pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Capability {
    /// Grants, fencing and coherent watch resync (package A1).
    Authority,
    /// Conditions, mutations, atomic batches and dedup (package T1).
    Transaction,
    /// Ordered append, ancestry validation and per-copy progress (package R1).
    Replication,
    /// Publication barrier, reads, status and uncertain outcomes (package P1).
    Publication,
    /// Unsafe-age admission and durable resume (package L1).
    Protection,
    /// Survivor inventory, lineage selection and rebuild (package F1).
    Recovery,
    /// Canonical encode/decode of a contract artifact (package C0).
    Codec,
    /// An environment seam in `rdb-sim` that this build does not provide.
    Environment,
}

/// What a caller may do after an error. One rule per spec §5.4 row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum RetryRule {
    /// Refresh the route, then reissue with the same request identity and expected generation.
    RefreshRoute,
    /// Wait for health or authority to recover, then reissue. The already-admitted request must
    /// not be assumed to have failed.
    RetryAfterRecovery,
    /// Nothing was mutated. The caller may issue a deliberately different request.
    Definitive,
    /// Nothing was admitted. Bounded jittered retry is safe.
    BoundedJitter,
    /// Query status with the *same* identity. Never generate a fresh request id.
    QueryStatus,
    /// Reconcile the generation or the identity explicitly. Never replay transparently.
    Reconcile,
    /// Quarantine and escalate. Operator or rollout action, not a retry.
    Quarantine,
    /// The capability is not wired in this build. Not a runtime condition and not retryable.
    NotWired,
}

/// The serialisable name of an error, without its payload.
///
/// [`RdbError`] itself is deliberately **not** `Deserialize`: its explanatory fields are
/// `&'static str` so that no caller key or value can ever reach an error message, and a
/// `&'static str` cannot be deserialised. Traces, oracle checkpoints and log fields therefore
/// carry the kind, which is stable, ordered and round-trippable. Same shape as
/// `config_core::ConfigError::kind`.
///
/// One name per spec §5.4 error, plus [`Self::Unavailable`], which §5.4 does not define: it is
/// the spike's own "not wired in this build" answer (spike §8), and it is in this set because a
/// trace has to be able to say it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ErrorKind {
    /// [`RdbError::NotPrimary`].
    NotPrimary,
    /// [`RdbError::RouteChanged`].
    RouteChanged,
    /// [`RdbError::LeaseExpired`].
    LeaseExpired,
    /// [`RdbError::RecoveryReadOnly`].
    RecoveryReadOnly,
    /// [`RdbError::ProtectionPaused`].
    ProtectionPaused,
    /// [`RdbError::ConditionFailed`].
    ConditionFailed,
    /// [`RdbError::CrossAffinity`].
    CrossAffinity,
    /// [`RdbError::InvalidArgument`].
    InvalidArgument,
    /// [`RdbError::Overloaded`].
    Overloaded,
    /// [`RdbError::DeadlineBeforeAdmission`].
    DeadlineBeforeAdmission,
    /// [`RdbError::UnknownOutcome`].
    UnknownOutcome,
    /// [`RdbError::GenerationChanged`].
    GenerationChanged,
    /// [`RdbError::RequestIdReuse`].
    RequestIdReuse,
    /// [`RdbError::StaleContinuation`].
    StaleContinuation,
    /// [`RdbError::CorruptHistory`].
    CorruptHistory,
    /// [`RdbError::IncompatibleVersion`].
    IncompatibleVersion,
    /// [`RdbError::StatusExpired`].
    StatusExpired,
    /// [`RdbError::Unavailable`].
    Unavailable,
}

/// Every error the rDB kernel may return to a caller or to the harness.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RdbError {
    /// This node does not hold the grant for the partition.
    #[error("not primary for partition {partition:?}")]
    NotPrimary {
        /// The partition that was addressed.
        partition: PartitionId,
        /// The node the router should try instead, when one is known.
        hint: Option<NodeId>,
    },

    /// The route this request was sent on is no longer the committed one.
    #[error("route changed for partition {partition:?}")]
    RouteChanged {
        /// The partition that was addressed.
        partition: PartitionId,
    },

    /// The grant backing this partition expired, was frozen, or was revoked.
    #[error("lease expired for partition {partition:?} grant {grant:?}")]
    LeaseExpired {
        /// The partition that was addressed.
        partition: PartitionId,
        /// The grant that is no longer valid.
        grant: GrantId,
    },

    /// The partition is serving a declared prefix read-only after majority loss (spec §8.4).
    #[error("partition {partition:?} is read-only in generation {generation:?}")]
    RecoveryReadOnly {
        /// The partition that was addressed.
        partition: PartitionId,
        /// The recovery generation currently serving.
        generation: Generation,
    },

    /// Admission is paused because the oldest unsafe transaction exceeded the pause
    /// threshold (spec §6.2).
    #[error("partition {partition:?} paused at prefix {paused_after:?}")]
    ProtectionPaused {
        /// The partition that was addressed.
        partition: PartitionId,
        /// The prefix admission was paused after.
        paused_after: Seq,
    },

    /// A declared condition did not hold. Definitive: nothing was mutated.
    #[error("condition {index} failed")]
    ConditionFailed {
        /// Zero-based position of the failing condition in the request.
        index: u32,
    },

    /// The request touches more than one affinity group (spec §5.1).
    #[error("cross-affinity request: expected {expected:?}, found {found:?}")]
    CrossAffinity {
        /// The affinity group the request declared.
        expected: AffinityId,
        /// The affinity group a key actually resolved to.
        found: AffinityId,
    },

    /// A required field was missing, malformed or out of range.
    #[error("invalid argument: {field}")]
    InvalidArgument {
        /// Name of the offending field. A static name, never caller bytes: no key or value
        /// content may reach an error string or a log field (team rules).
        field: &'static str,
    },

    /// Admission control rejected the request before any mutation.
    #[error("partition {partition:?} overloaded")]
    Overloaded {
        /// The partition that was addressed.
        partition: PartitionId,
    },

    /// The remaining deadline had already elapsed when the request reached admission.
    #[error("deadline elapsed before admission for partition {partition:?}")]
    DeadlineBeforeAdmission {
        /// The partition that was addressed.
        partition: PartitionId,
    },

    /// The transaction was applied locally but its outcome could not be resolved. Never a
    /// definitive failure (spec §5.3, rEtcd ADR-0015).
    #[error("unknown outcome for {identity:?} on partition {partition:?}")]
    UnknownOutcome {
        /// The partition that was addressed.
        partition: PartitionId,
        /// The request whose outcome is unresolved.
        identity: RequestIdentity,
    },

    /// The caller's expected generation is not the current one; it must reconcile (spec §5.3).
    #[error("generation changed: expected {expected:?}, current {current:?}")]
    GenerationChanged {
        /// The generation the caller expected.
        expected: Generation,
        /// The generation now serving the partition.
        current: Generation,
    },

    /// A retained request identity was reused with a different payload digest (spec §5.3).
    #[error("request id reused with a different payload: {identity:?}")]
    RequestIdReuse {
        /// The reused identity.
        identity: RequestIdentity,
    },

    /// A scan continuation no longer matches the snapshot it was issued against.
    #[error("stale continuation")]
    StaleContinuation,

    /// Two histories carry different digests at one lineage position, or a record failed its
    /// digest check. Quarantine; never a normal tie (spec §8.1).
    #[error("corrupt history on partition {partition:?} at {at:?}")]
    CorruptHistory {
        /// The partition whose history is suspect.
        partition: PartitionId,
        /// The lineage position where validation failed.
        at: Seq,
    },

    /// An artifact carried a mandatory version this build does not support. Raised *before* the
    /// body is decoded (spec §5.4, validation-plan V12).
    #[error("incompatible {artifact:?} version {found}, supported {min}..={max}")]
    IncompatibleVersion {
        /// Which artifact carried the version.
        artifact: VersionedArtifact,
        /// The version found on the wire or on the record.
        found: u16,
        /// Lowest version this build accepts.
        min: u16,
        /// Highest version this build accepts.
        max: u16,
    },

    /// The retention window for this request's status has passed. Absence is not proof of
    /// nonexecution (spec §5.3, §8.1).
    #[error("status expired for {identity:?}")]
    StatusExpired {
        /// The request whose status is no longer retained.
        identity: RequestIdentity,
    },

    /// The capability is not wired in this build. Explicit by design (spike §8).
    #[error("{capability:?} unavailable: {reason}")]
    Unavailable {
        /// Which seam is missing.
        capability: Capability,
        /// A static explanation. Never caller data.
        reason: &'static str,
    },
}

impl RdbError {
    /// The payload-free name of this error, for traces, oracle checkpoints and log fields.
    #[must_use]
    pub const fn kind(&self) -> ErrorKind {
        match self {
            Self::NotPrimary { .. } => ErrorKind::NotPrimary,
            Self::RouteChanged { .. } => ErrorKind::RouteChanged,
            Self::LeaseExpired { .. } => ErrorKind::LeaseExpired,
            Self::RecoveryReadOnly { .. } => ErrorKind::RecoveryReadOnly,
            Self::ProtectionPaused { .. } => ErrorKind::ProtectionPaused,
            Self::ConditionFailed { .. } => ErrorKind::ConditionFailed,
            Self::CrossAffinity { .. } => ErrorKind::CrossAffinity,
            Self::InvalidArgument { .. } => ErrorKind::InvalidArgument,
            Self::Overloaded { .. } => ErrorKind::Overloaded,
            Self::DeadlineBeforeAdmission { .. } => ErrorKind::DeadlineBeforeAdmission,
            Self::UnknownOutcome { .. } => ErrorKind::UnknownOutcome,
            Self::GenerationChanged { .. } => ErrorKind::GenerationChanged,
            Self::RequestIdReuse { .. } => ErrorKind::RequestIdReuse,
            Self::StaleContinuation => ErrorKind::StaleContinuation,
            Self::CorruptHistory { .. } => ErrorKind::CorruptHistory,
            Self::IncompatibleVersion { .. } => ErrorKind::IncompatibleVersion,
            Self::StatusExpired { .. } => ErrorKind::StatusExpired,
            Self::Unavailable { .. } => ErrorKind::Unavailable,
        }
    }

    /// The retry rule for this error, per the spec §5.4 table.
    ///
    /// Total on purpose: adding a variant without deciding its retry rule fails to compile.
    #[must_use]
    pub const fn retry_rule(&self) -> RetryRule {
        match self {
            Self::NotPrimary { .. } | Self::RouteChanged { .. } => RetryRule::RefreshRoute,
            Self::LeaseExpired { .. }
            | Self::RecoveryReadOnly { .. }
            | Self::ProtectionPaused { .. } => RetryRule::RetryAfterRecovery,
            Self::ConditionFailed { .. }
            | Self::CrossAffinity { .. }
            | Self::InvalidArgument { .. } => RetryRule::Definitive,
            Self::Overloaded { .. } | Self::DeadlineBeforeAdmission { .. } => {
                RetryRule::BoundedJitter
            }
            Self::UnknownOutcome { .. } => RetryRule::QueryStatus,
            Self::GenerationChanged { .. }
            | Self::RequestIdReuse { .. }
            | Self::StaleContinuation => RetryRule::Reconcile,
            Self::CorruptHistory { .. }
            | Self::IncompatibleVersion { .. }
            | Self::StatusExpired { .. } => RetryRule::Quarantine,
            Self::Unavailable { .. } => RetryRule::NotWired,
        }
    }

    /// Whether this error proves no mutation was applied.
    ///
    /// Only pre-admission rejection proves it (spec §5.4 takeaway). An oracle that needs to know
    /// "could this request have had an effect?" asks here rather than matching variants itself.
    ///
    /// [`RetryRule::NotWired`] is **not** proof (finding K-F-26). An unwired module has not
    /// mutated anything, but the module that returned `Unavailable` may not be the only one the
    /// event reached, and the answer a caller acts on must not depend on which package landed
    /// first. `Unavailable` is a build-state report, not a protocol decision, and it proves
    /// nothing about the request.
    #[must_use]
    pub const fn proves_no_mutation(&self) -> bool {
        matches!(
            self.retry_rule(),
            RetryRule::Definitive | RetryRule::BoundedJitter
        )
    }

    /// Which capability an [`Self::Unavailable`] names, or `None` for every other error.
    ///
    /// Exists so a caller can say *which* seam is missing without matching on the variant and
    /// without parsing the message. The harness uses it to build its capability report.
    #[must_use]
    pub const fn capability(&self) -> Option<Capability> {
        match self {
            Self::Unavailable { capability, .. } => Some(*capability),
            _ => None,
        }
    }

    /// Shorthand for the stub bodies every unwired seam returns.
    #[must_use]
    pub const fn unavailable(capability: Capability, reason: &'static str) -> Self {
        Self::Unavailable { capability, reason }
    }
}
