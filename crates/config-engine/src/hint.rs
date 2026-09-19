//! Gossip hint validation (ADR-0003, test plan TA-7).
//!
//! [`validate_hint`] is a pure function so the whole rejection matrix is table-testable
//! without sockets, and so there is exactly one place that decides whether an advisory
//! observation is coherent with committed reality.
//!
//! **An accepted hint still confers nothing.** It is telemetry. Raft membership comes from
//! committed membership entries, and the transport dials the committed endpoint. A hint that
//! is "accepted" merely means it agreed with what we already knew.

use config_core::{ClusterIdentity, ObservedPeerHint};

use crate::metrics::MembershipView;

/// The outcome of validating one [`ObservedPeerHint`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HintVerdict {
    /// The hint agrees with committed membership. Recorded for telemetry only.
    Accepted,
    /// The hint disagrees with committed membership and is discarded.
    Rejected {
        /// Stable snake_case reason, used as the `reason` log field and as a metric label.
        reason: &'static str,
    },
}

impl HintVerdict {
    /// Whether the hint was accepted.
    pub fn is_accepted(self) -> bool {
        matches!(self, HintVerdict::Accepted)
    }

    /// The rejection reason, if any.
    pub fn reason(self) -> Option<&'static str> {
        match self {
            HintVerdict::Accepted => None,
            HintVerdict::Rejected { reason } => Some(reason),
        }
    }
}

/// The hint claims a different cluster.
pub const REASON_CLUSTER_MISMATCH: &str = "cluster_mismatch";
/// The hint claims our cluster at a different recovery epoch (ADR-0011).
pub const REASON_EPOCH_MISMATCH: &str = "recovery_epoch mismatch";
/// The hint claims a node id that is not a committed voter.
pub const REASON_UNKNOWN_NODE: &str = "unknown_node";
/// The hint advertises a peer endpoint other than the committed one.
pub const REASON_ENDPOINT_MISMATCH: &str = "endpoint_mismatch";
/// The hint claims to be us.
pub const REASON_SELF_CLAIM: &str = "self_claim";
/// Membership has not been committed yet, so nothing can be validated against it.
pub const REASON_NOT_FORMED: &str = "not_formed";

/// Decide whether an advisory peer observation is coherent with committed membership.
///
/// Pure: same inputs, same verdict, no I/O and no clock. The checks are ordered from the
/// cheapest and most damning to the most specific, so the reported reason names the first
/// thing that was actually wrong: wrong cluster, then wrong epoch of the right cluster
/// (ADR-0011's identity is the *pair*), then self-claim, then membership questions.
pub fn validate_hint(
    hint: &ObservedPeerHint,
    membership: &MembershipView,
    identity: &ClusterIdentity,
) -> HintVerdict {
    if hint.cluster_id != identity.cluster_id {
        return HintVerdict::Rejected {
            reason: REASON_CLUSTER_MISMATCH,
        };
    }
    // Checked before `self_claim`: a peer stranded on the pre-recovery epoch may well be
    // advertising *our* node id (it still believes the old membership), and "wrong epoch" is
    // the truthful diagnosis of that, not "someone is impersonating me".
    if hint.recovery_epoch != identity.recovery_epoch {
        return HintVerdict::Rejected {
            reason: REASON_EPOCH_MISMATCH,
        };
    }
    if hint.node_id == identity.node_id {
        return HintVerdict::Rejected {
            reason: REASON_SELF_CLAIM,
        };
    }
    if !membership.is_formed() {
        return HintVerdict::Rejected {
            reason: REASON_NOT_FORMED,
        };
    }
    if !membership.voters.contains(&hint.node_id) {
        return HintVerdict::Rejected {
            reason: REASON_UNKNOWN_NODE,
        };
    }
    match membership.endpoint_of(hint.node_id) {
        Some(committed) if committed == hint.peer_endpoint => HintVerdict::Accepted,
        // A voter with no committed address cannot be corroborated, and a disagreeing
        // address is exactly the hijack ADR-0003 exists to refuse. Either way the committed
        // endpoint stays authoritative; the hint is simply not used.
        _ => HintVerdict::Rejected {
            reason: REASON_ENDPOINT_MISMATCH,
        },
    }
}
