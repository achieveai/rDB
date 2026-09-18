//! Typed semantic errors and their transport classification (spec §16, §6.2).
//!
//! `config-core` never names a transport type. [`ConfigError::kind`] returns a
//! [`StatusClass`], and `config-grpc` is the only crate that turns a [`StatusClass`] into a
//! `tonic::Status`. That keeps the error taxonomy testable without a network stack and keeps
//! the mapping in exactly one place.

use serde::{Deserialize, Serialize};

use crate::identity::NodeId;

/// A validated pointer to the node a caller should retry against (spec §16 `NotLeader`).
///
/// A hint is only ever constructed from *authenticated* information — committed membership
/// plus the peer's mTLS identity. Gossip observations and request fields never produce one
/// (ADR-0003, ADR-0012).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaderHint {
    /// Stable id of the node believed to be leader.
    pub node_id: NodeId,
    /// Client-plane endpoint (`host:port`) of that node.
    pub endpoint: String,
}

/// The gRPC status class a [`ConfigError`] maps to (spec §6.2 normative table).
///
/// This is a semantic classification, not a transport type: it exists so the mapping can be
/// asserted in a unit test and so no crate below `config-grpc` needs a transport dependency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StatusClass {
    /// `INVALID_ARGUMENT` — a *structural* violation: empty key, over-long key,
    /// `Delete` with `expected_mod_revision == 0`, or an undecodable replicated command.
    InvalidArgument,
    /// `RESOURCE_EXHAUSTED` — a *budget* violation: value, request, or list size cap.
    ResourceExhausted,
    /// `UNAVAILABLE` — retryable; the request was rejected before entering the Raft log.
    Unavailable,
    /// `FAILED_PRECONDITION` — the addressed node is not the leader, or a CAS precondition
    /// failed on a path that surfaces conflict as an error rather than as an outcome.
    FailedPrecondition,
    /// `NOT_FOUND` — the addressed key does not exist.
    NotFound,
    /// `DEADLINE_EXCEEDED` — the mutation outcome is **unknown**, not failed (ADR-0015).
    DeadlineExceeded,
    /// `PERMISSION_DENIED` — an authenticated principal without a matching grant.
    PermissionDenied,
    /// `UNAUTHENTICATED` — no usable transport identity.
    Unauthenticated,
    /// `INTERNAL` — fatal local storage; the node also becomes unready.
    Internal,
}

/// The first-release semantic error set (spec §16).
///
/// `M4` adds `RevisionCompacted` and `M6` pagination adds `PageTokenExpired`; neither is
/// present here, because a milestone does not silently pull later scope forward.
///
/// Note what is **not** an error: a `CONFLICT` or `NOT_FOUND` *mutation outcome* is an
/// application result carried in [`crate::MutationResponse`] with transport status `OK`
/// (spec §7.3). [`ConfigError::Conflict`] and [`ConfigError::NotFound`] exist for the read
/// and helper paths that surface those conditions as failures.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    /// This node is not the leader. The hint, when present, is validated and safe to follow;
    /// when absent, the node does not currently know a leader.
    #[error("not leader{}", match .hint { Some(h) => format!(" (try node {} at {})", h.node_id, h.endpoint), None => String::new() })]
    NotLeader {
        /// Validated leader hint, if this node knows one.
        hint: Option<LeaderHint>,
    },

    /// The node cannot serve the request right now and the request never entered the log.
    /// Safe to retry: it is the one condition under which a client may resubmit a mutation.
    #[error("unavailable: {reason}")]
    Unavailable {
        /// Operator-facing explanation; never contains a key or value.
        reason: String,
    },

    /// The deadline expired after submission. The mutation may still commit (ADR-0015).
    ///
    /// A client must **not** replay the mutation. Recovery is: read the key, compare
    /// `mod_revision`/value, then issue a CAS against the observed revision.
    #[error("deadline exceeded; mutation outcome is unknown and must not be replayed")]
    DeadlineExceededUnknownOutcome,

    /// A CAS precondition failed. Carries only `exists` and `current_mod_revision`; returning
    /// the current value would require independent read permission (spec §7.3).
    #[error("conflict: exists={exists} current_mod_revision={current_mod_revision}")]
    Conflict {
        /// Whether the key exists at the linearization point.
        exists: bool,
        /// The key's current `mod_revision`, or `0` when it does not exist.
        current_mod_revision: u64,
    },

    /// The addressed key does not exist.
    #[error("not found")]
    NotFound,

    /// A size budget was exceeded (value, request, or list caps).
    #[error("resource exhausted: {detail}")]
    ResourceExhausted {
        /// Which budget and by how much; never contains a key or value.
        detail: String,
    },

    /// No usable transport identity was presented.
    #[error("unauthenticated: {detail}")]
    Unauthenticated {
        /// Why the identity was unusable; never contains a credential.
        detail: String,
    },

    /// The principal is authenticated but has no grant covering this key or prefix.
    #[error("permission denied: {detail}")]
    PermissionDenied {
        /// Principal name, action, and truncated key hex — never the value.
        detail: String,
    },

    /// A structural violation: empty key, over-long key, `Delete` with `expected == 0`, or a
    /// replicated command that failed apply-time validation.
    #[error("invalid argument: {detail}")]
    InvalidArgument {
        /// Which rule was violated; never contains a value.
        detail: String,
    },

    /// Local storage failed fatally. The node becomes unready; this is not retryable here.
    #[error("fatal storage error: {detail}")]
    FatalStorage {
        /// Operator-facing explanation.
        detail: String,
    },
}

impl ConfigError {
    /// Classify this error for the transport layer (spec §6.2).
    ///
    /// The classification is total and pure, so `config-grpc`'s mapping is a table lookup
    /// rather than a chain of `if let`s that can drift from the spec.
    pub fn kind(&self) -> StatusClass {
        match self {
            Self::NotLeader { .. } | Self::Conflict { .. } => StatusClass::FailedPrecondition,
            Self::Unavailable { .. } => StatusClass::Unavailable,
            Self::DeadlineExceededUnknownOutcome => StatusClass::DeadlineExceeded,
            Self::NotFound => StatusClass::NotFound,
            Self::ResourceExhausted { .. } => StatusClass::ResourceExhausted,
            Self::Unauthenticated { .. } => StatusClass::Unauthenticated,
            Self::PermissionDenied { .. } => StatusClass::PermissionDenied,
            Self::InvalidArgument { .. } => StatusClass::InvalidArgument,
            Self::FatalStorage { .. } => StatusClass::Internal,
        }
    }

    /// Whether a client may safely resubmit the same mutation after this error.
    ///
    /// Only `Unavailable` and `NotLeader` qualify: both are rejections that happened *before*
    /// the command could enter the Raft log, so a resubmission cannot duplicate an effect.
    /// [`ConfigError::DeadlineExceededUnknownOutcome`] deliberately returns `false`
    /// (ADR-0015): there is no request deduplication in the first release, so an automatic
    /// replay could apply a second time.
    pub fn is_safe_to_resubmit(&self) -> bool {
        matches!(self, Self::Unavailable { .. } | Self::NotLeader { .. })
    }

    /// Build an [`ConfigError::InvalidArgument`] from any displayable detail.
    pub fn invalid_argument(detail: impl Into<String>) -> Self {
        Self::InvalidArgument {
            detail: detail.into(),
        }
    }

    /// Build a [`ConfigError::ResourceExhausted`] from any displayable detail.
    pub fn resource_exhausted(detail: impl Into<String>) -> Self {
        Self::ResourceExhausted {
            detail: detail.into(),
        }
    }
}
