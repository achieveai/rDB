//! Typed semantic errors and their transport classification (spec §16, §6.2).
//!
//! `config-core` never names a transport type. [`ConfigError::kind`] returns a
//! [`StatusClass`], and `config-grpc` is the only crate that turns a [`StatusClass`] into a
//! `tonic::Status`. That keeps the error taxonomy testable without a network stack and keeps
//! the mapping in exactly one place.

use serde::{Deserialize, Serialize};

use crate::identity::NodeId;

/// The reserved [`ConfigError::Unavailable`] reason for a request that names a feature this
/// cluster has not activated yet (M5 reserves the string, M6/ADR-0030 gates on it).
///
/// It is a named constant rather than a literal for the same reason the termination reasons
/// are: an operator's alert rule and a client's branch both match on the exact string, so a
/// typo in either would silently stop matching. A reason, not a new variant: the request never
/// entered the log and retrying after the feature is activated is exactly the right recovery,
/// which is what [`ConfigError::Unavailable`] already means.
pub const UNAVAILABLE_FEATURE_NOT_ACTIVATED: &str = "feature_not_activated";

/// The [`ConfigError::InvalidArgument`] detail for a continuation token whose bound prefix is
/// not the prefix the caller just asked for (M6, ADR-0029, OQ-62).
///
/// A caller error, not an expiry: re-running the walk with the same mutated prefix would fail
/// identically, so a client that retried on this would loop forever.
pub const REASON_PREFIX_MISMATCH: &str = "prefix_mismatch";

/// The [`ConfigError::PermissionDenied`] detail for a continuation token presented by a
/// principal other than the one it was issued to (M6, ADR-0029, OQ-62).
///
/// Deliberately a security-class refusal rather than an expiry: a transferable cursor is a
/// lateral-movement primitive, and it has to be visible as one. The detail names neither
/// principal — the presenter already knows its own, and the issuer's identity is not the
/// presenter's to learn.
pub const REASON_TOKEN_PRINCIPAL: &str = "token_principal";

/// The [`ConfigError::PermissionDenied`] detail for a watch stream terminated because the grants
/// covering its prefix changed under a new policy document (M6, ADR-0027, M6-28).
///
/// A detail constant rather than a field on the variant, for the same reason
/// [`REASON_TOKEN_PRINCIPAL`] is one: the wire already carries the discriminator in the
/// `retcd-reason` trailer, a client branches on the exact string, and a new struct field would
/// break every existing construction site for no behaviour a caller can observe. Lead ruling M6-R8
/// of 2026-09-19; ADR-0027 records the deviation.
///
/// It is a *terminal* condition, not a retryable one at the same cursor: the client's recovery is
/// to re-`List` under the new grants and restart the watch, exactly as it would after a
/// revocation.
pub const REASON_POLICY_CHANGED: &str = "policy_changed";

/// The [`ConfigError::PermissionDenied`] detail for a request the **new** policy document grants
/// but the old one does not, while the cluster is still converging (M6, ADR-0027, M6-17, M6-30).
///
/// Distinct from an ordinary denial on purpose: it clears on its own once every voter reports the
/// new version, so a caller that could not tell the two apart could not decide whether to retry.
/// Equal to [`crate::policy::REASON_POLICY_CONVERGING`] by construction — the same string names
/// the [`crate::Decision::Deny`] reason and this error's detail, so the audit line and the wire
/// cannot drift.
pub const REASON_POLICY_CONVERGING: &str = crate::policy::REASON_POLICY_CONVERGING;

/// Why a continuation token was refused as no longer usable (M6, ADR-0029).
///
/// Every variant is *transient*: the caller's correct response to all of them is to re-`List`
/// from the start. The two non-transient refusals — a mutated prefix and a borrowed token —
/// are deliberately **not** here; they are [`ConfigError::InvalidArgument`] and
/// [`ConfigError::PermissionDenied`], because telling a caller to retry a call that can never
/// succeed is how a client ends up in a loop.
///
/// `Display` is the exact `retcd-reason` trailer value, so the wire string, the log field and
/// the pin-registry counter key cannot drift apart (test plan Q-30, M6-122).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PageTokenExpiredReason {
    /// HMAC verification failed: the token was corrupted, truncated, forged, or minted under a
    /// `list.token_key` this node no longer holds (M6-67, M6-79).
    Mac,
    /// The token is past `list.ttl_seconds`, measured against the injectable clock (M6-68).
    Expired,
    /// The pinned snapshot was dropped under LRU pressure before the walk finished (M6-69).
    Evicted,
    /// The pin cannot exist on this node: another node issued the token, or this process
    /// started after the token was issued, so its pin table never held the entry (M6-70,
    /// M6-83). One reason, not a race between two.
    Node,
    /// The active policy document changed since the token was issued, so the grants the walk
    /// started under are no longer the grants in force (M6-32, M6-71; ADR-0027).
    PolicyVersion,
    /// The token declares a `token_version` this build does not implement (M6-78).
    ///
    /// Not in ADR-0029's list of five; it is here because a token whose envelope version is
    /// unknown is exactly as unusable as an expired one, and the test plan requires the reason
    /// to be distinguishable from `mac` — a forged version field that was re-MACed with the
    /// real key passes the MAC check and must not be reported as a MAC failure.
    TokenVersion,
}

impl PageTokenExpiredReason {
    /// The `retcd-reason` trailer value, also the log field and the counter key.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Mac => "mac",
            Self::Expired => "expired",
            Self::Evicted => "evicted",
            Self::Node => "node",
            Self::PolicyVersion => "policy_version",
            Self::TokenVersion => "token_version",
        }
    }

    /// Every reason, in declaration order.
    ///
    /// The closed set a registry seeds its counters from, so a reason that is never hit still
    /// reports `0` rather than being absent (test plan TA-59.2).
    pub const ALL: [Self; 6] = [
        Self::Mac,
        Self::Expired,
        Self::Evicted,
        Self::Node,
        Self::PolicyVersion,
        Self::TokenVersion,
    ];

    /// Parse a trailer value back into a reason.
    ///
    /// Named for the trailer rather than `from_str`: this is a lookup over a closed set that
    /// cannot fail with a described error, which is not what [`std::str::FromStr`] promises.
    pub fn from_trailer(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|r| r.as_str() == s)
    }
}

impl std::fmt::Display for PageTokenExpiredReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

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
    /// `OUT_OF_RANGE` — the requested revision is outside retained history (M4, spec §16).
    ///
    /// Distinct from [`StatusClass::NotFound`] on purpose: the data was *deliberately* dropped
    /// by compaction, the client's cursor is permanently unusable, and the recovery is to
    /// re-`List` and restart the watch at the listed revision — not to retry the same request.
    OutOfRange,
    /// `UNAUTHENTICATED` — no usable transport identity.
    Unauthenticated,
    /// `INTERNAL` — fatal local storage; the node also becomes unready.
    Internal,
}

/// The first-release semantic error set (spec §16).
///
/// `M4` added [`ConfigError::RevisionCompacted`] and `M6` pagination added
/// [`ConfigError::PageTokenExpired`] — each when its milestone landed, because a milestone
/// does not silently pull later scope forward.
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
    ///
    /// See [`UNAVAILABLE_FEATURE_NOT_ACTIVATED`] for the one reserved `reason` string.
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

    /// A size budget was exceeded (value, request, list, or — since M4 — watch caps).
    #[error("resource exhausted: {detail}{}", if *.resumable { " (resumable)" } else { "" })]
    ResourceExhausted {
        /// Which budget and by how much; never contains a key or value.
        detail: String,
        /// Whether the caller can make progress by *resuming* rather than by shrinking the
        /// request (M4, lead ruling R9; surfaced on the wire as the `retcd-resumable` trailer).
        ///
        /// `true` for a watch stream terminated because its own queue overflowed: the client
        /// still holds `last_delivered_revision`, and reconnecting at that cursor is a
        /// well-defined, gap-free continuation. `false` for a request-size or admission cap,
        /// where resuming the identical request would exhaust the identical budget.
        ///
        /// This is **not** [`ConfigError::is_safe_to_resubmit`]: that asks whether replaying a
        /// *mutation* can duplicate an effect. A watch carries no effect to duplicate.
        resumable: bool,
    },

    /// The requested start revision is no longer retained: compaction dropped it (M4, §11.2).
    ///
    /// Not retryable and not resumable at the same cursor. The client's recovery is spec
    /// §11.2's list-to-watch flow: `List` the prefix, then watch from the revision that
    /// response reported.
    #[error("revision compacted; the oldest resumable revision is {minimum_available_revision}")]
    RevisionCompacted {
        /// The lowest revision a watch may still start after — `compact_revision + 1`. A
        /// cursor at or below `compact_revision` is permanently gone (OQ-27).
        minimum_available_revision: u64,
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

    /// A continuation token can no longer be used (M6, ADR-0029).
    ///
    /// Always transient, and always recovered the same way: re-`List` the prefix from the
    /// start. It is `FAILED_PRECONDITION` rather than `OUT_OF_RANGE` because nothing was
    /// dropped from history — the *pin* is gone, not the data — and the walk can be restarted
    /// immediately against the same live state.
    #[error("page token expired: {reason}")]
    PageTokenExpired {
        /// Which of the transient causes fired; the `retcd-reason` trailer value.
        reason: PageTokenExpiredReason,
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
            Self::NotLeader { .. } | Self::Conflict { .. } | Self::PageTokenExpired { .. } => {
                StatusClass::FailedPrecondition
            }
            Self::Unavailable { .. } => StatusClass::Unavailable,
            Self::DeadlineExceededUnknownOutcome => StatusClass::DeadlineExceeded,
            Self::NotFound => StatusClass::NotFound,
            Self::ResourceExhausted { .. } => StatusClass::ResourceExhausted,
            Self::RevisionCompacted { .. } => StatusClass::OutOfRange,
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

    /// Build a non-resumable [`ConfigError::ResourceExhausted`] — the budget violations that
    /// predate M4 (value, request, and list caps), none of which a resume can get past.
    pub fn resource_exhausted(detail: impl Into<String>) -> Self {
        Self::ResourceExhausted {
            detail: detail.into(),
            resumable: false,
        }
    }

    /// Build a **resumable** [`ConfigError::ResourceExhausted`]: the caller may reconnect at
    /// its last delivered revision and continue without a gap (M4 watch overload).
    pub fn resource_exhausted_resumable(detail: impl Into<String>) -> Self {
        Self::ResourceExhausted {
            detail: detail.into(),
            resumable: true,
        }
    }

    /// Build a [`ConfigError::PageTokenExpired`] (M6, ADR-0029).
    pub fn page_token_expired(reason: PageTokenExpiredReason) -> Self {
        Self::PageTokenExpired { reason }
    }

    /// The refusal for a continuation token bound to a different prefix (M6, OQ-62).
    ///
    /// A constructor rather than a literal so the detail is exactly
    /// [`REASON_PREFIX_MISMATCH`] at every call site: a client branches on that string.
    pub fn prefix_mismatch() -> Self {
        Self::InvalidArgument {
            detail: REASON_PREFIX_MISMATCH.to_string(),
        }
    }

    /// The refusal for a continuation token presented by another principal (M6, OQ-62).
    pub fn token_principal() -> Self {
        Self::PermissionDenied {
            detail: REASON_TOKEN_PRINCIPAL.to_string(),
        }
    }

    /// The terminal refusal for a watch whose prefix grants changed under a new policy document
    /// (M6, ADR-0027, M6-28).
    pub fn policy_changed() -> Self {
        Self::PermissionDenied {
            detail: REASON_POLICY_CHANGED.to_string(),
        }
    }

    /// The refusal for a request only the new policy document grants, while converging (M6,
    /// ADR-0027, M6-17, M6-30).
    pub fn policy_converging() -> Self {
        Self::PermissionDenied {
            detail: REASON_POLICY_CONVERGING.to_string(),
        }
    }

    /// The machine-readable reason of a [`ConfigError::PermissionDenied`], if it has one.
    ///
    /// Two shapes reach this: the bare constant that [`ConfigError::policy_changed`] and its
    /// siblings build, and the operator-facing sentence the node wraps an authorizer's verdict
    /// in — `principal "app" may not Read key_hex=… (policy_converging)`. The second exists
    /// because a denial has to name *who* was denied *what* in the log, and the reason is the
    /// trailing parenthesised group of exactly that sentence.
    ///
    /// The extraction is deliberately shallow: it is only ever compared against a known
    /// constant, so a detail that merely happens to end in `)` yields a token that matches
    /// nothing rather than a wrong classification.
    pub fn permission_denied_reason(&self) -> Option<&str> {
        let Self::PermissionDenied { detail } = self else {
            return None;
        };
        Some(
            detail
                .strip_suffix(')')
                .and_then(|head| head.rfind('(').map(|at| &head[at + 1..]))
                .unwrap_or(detail),
        )
    }

    /// Whether this is a [`ConfigError::PermissionDenied`] carrying `reason`.
    ///
    /// The one place the detail-as-discriminator convention is read, so a caller never
    /// re-implements the string comparison and the trailer mapping has a single source.
    pub fn is_permission_denied_reason(&self, reason: &str) -> bool {
        self.permission_denied_reason() == Some(reason)
    }

    /// Build a [`ConfigError::RevisionCompacted`] from the watermark that refused the cursor.
    ///
    /// Takes `compact_revision` rather than the minimum so no caller can get the `+ 1` wrong:
    /// a cursor is refused when `R <= compact_revision`, so the lowest usable one is the next.
    pub fn revision_compacted(compact_revision: u64) -> Self {
        Self::RevisionCompacted {
            minimum_available_revision: compact_revision.saturating_add(1),
        }
    }
}
