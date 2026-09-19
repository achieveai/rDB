//! The admin plane's engine-side types: membership reporting, the catch-up oracle, and the
//! typed refusals the learner lifecycle produces (spec §13.2, §19.8; ADR-0023).
//!
//! The operations themselves live on [`crate::ConfigNode`] in `node.rs`, because they need
//! `NodeInner`'s private handles. Only the vocabulary is here.

use std::collections::{BTreeMap, BTreeSet};

use config_core::{ConfigError, LeaderHint, NodeId};

use crate::metrics::{LogIdView, MembershipView};

/// The default catch-up threshold: how many log entries a learner may still be missing and
/// still be promotable (`[membership] promote_max_lag`, architecture A5, OQ-50).
///
/// It is a *lag* rather than a wait, because the only honest catch-up oracle openraft offers
/// is `RaftMetrics.replication[id]` compared against the leader's own last log index
/// (research §3.3). A busy cluster never reaches lag zero, so demanding equality would make
/// promotion impossible exactly when it matters.
pub const DEFAULT_PROMOTE_MAX_LAG: u64 = 100;

/// Why an administrative operation was refused.
///
/// Every variant is a decision this node reached, never an unknown outcome: a membership
/// change either entered the log or it did not, and `change_membership` reports which.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AdminError {
    /// This node is not the leader. Membership changes are proposed only by a leader.
    #[error("not leader{}", match .hint {
        Some(h) => format!(" (try node {} at {})", h.node_id, h.endpoint),
        None => String::new(),
    })]
    NotLeader {
        /// The current leader's *client* endpoint, when committed membership names one.
        hint: Option<LeaderHint>,
    },
    /// The node id has been retired by a committed `RetireNode` and can never be re-added
    /// (spec §21 M5, ADR-0023). This is the readmission half of the fence; the peer plane is
    /// the network half.
    #[error("node {node_id} is retired: node_retired")]
    Retired {
        /// The fenced id.
        node_id: NodeId,
    },
    /// The learner is too far behind to be promoted safely (A5).
    #[error("node {node_id} lags the leader by {lag} entries, more than promote_max_lag {max}")]
    Lagging {
        /// The learner asked about.
        node_id: NodeId,
        /// How many entries it is missing right now.
        lag: u64,
        /// The configured threshold.
        max: u64,
    },
    /// The id is not in the membership this operation needs it to be in — promoting a node
    /// that was never added as a learner, or removing one that is not a member.
    #[error("node {node_id} is not a member of this cluster")]
    NotAMember {
        /// The id asked about.
        node_id: NodeId,
    },
    /// A snapshot build is already in flight; openraft runs at most one (research trap T13).
    #[error("a snapshot build is already in progress")]
    AlreadyInProgress,
    /// The request was malformed or contradicted this node's bound identity.
    #[error("invalid argument: {detail}")]
    InvalidArgument {
        /// A stable, machine-greppable reason first, prose after.
        detail: String,
    },
    /// The operation could not be attempted; retrying later is safe.
    #[error("unavailable: {reason}")]
    Unavailable {
        /// Why.
        reason: String,
    },
    /// OpenRaft refused the change. Retryable: `change_membership` reports a refusal only
    /// when nothing was committed.
    #[error("raft refused the membership change: {0}")]
    Raft(String),
    /// The Raft core is gone. Not retryable against this node.
    #[error("raft core is not running: {0}")]
    Fatal(String),
}

impl From<AdminError> for ConfigError {
    /// Map onto the shared client error vocabulary, so the admin plane reuses
    /// `config-grpc`'s one status mapping (ADR-0010) instead of inventing a second.
    ///
    /// `Retired` becomes `InvalidArgument{node_retired}` rather than `PermissionDenied`: the
    /// caller's credentials were fine, the id it named is the thing that is gone (TA-51).
    fn from(e: AdminError) -> Self {
        match e {
            AdminError::NotLeader { hint } => ConfigError::NotLeader { hint },
            AdminError::Retired { node_id } => ConfigError::InvalidArgument {
                detail: format!("node_retired: node {node_id} was retired and cannot rejoin"),
            },
            AdminError::Lagging { node_id, lag, max } => ConfigError::Unavailable {
                reason: format!("learner_lagging: node {node_id} lag {lag} exceeds {max}"),
            },
            AdminError::NotAMember { node_id } => ConfigError::InvalidArgument {
                detail: format!("not_a_member: node {node_id} is not in this cluster"),
            },
            AdminError::AlreadyInProgress => ConfigError::Unavailable {
                reason: "snapshot_build_in_progress".to_string(),
            },
            AdminError::InvalidArgument { detail } => ConfigError::InvalidArgument { detail },
            AdminError::Unavailable { reason } => ConfigError::Unavailable { reason },
            AdminError::Raft(detail) => ConfigError::Unavailable { reason: detail },
            AdminError::Fatal(detail) => ConfigError::FatalStorage { detail },
        }
    }
}

impl AdminError {
    /// The stable reason token an audit line and a refusal message both carry.
    ///
    /// Sub-causes are distinguished here and nowhere else: callers match on this string, not
    /// on prose, so the prose stays free to improve.
    pub fn reason(&self) -> &'static str {
        match self {
            Self::NotLeader { .. } => "not_leader",
            Self::Retired { .. } => "node_retired",
            Self::Lagging { .. } => "learner_lagging",
            Self::NotAMember { .. } => "not_a_member",
            Self::AlreadyInProgress => "snapshot_build_in_progress",
            Self::InvalidArgument { .. } => "invalid_argument",
            Self::Unavailable { .. } => "unavailable",
            Self::Raft(_) => "raft_refused",
            Self::Fatal(_) => "raft_fatal",
        }
    }
}

/// How far behind one peer is, as the leader sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicationProgress {
    /// Highest log index this peer has acknowledged, or `None` when the leader has not heard
    /// from it since it became leader.
    pub matched_index: Option<u64>,
    /// `leader_last_log_index - matched_index`, saturating. A peer with no acknowledgement at
    /// all lags by the leader's whole log.
    pub lag: u64,
}

/// What one node believes about membership right now (ADR-0023, test plan TA-45).
///
/// Everything here is an id, an index or a count: an admin report carries no keys and no
/// values, so it is safe to log in full.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembershipReport {
    /// Committed membership: voters and their two committed endpoints.
    pub membership: MembershipView,
    /// Members that are not voters — learners, in the *effective* membership, because a
    /// learner is only interesting before its promotion is committed.
    pub learners: BTreeSet<NodeId>,
    /// Configs in the effective membership: `1` uniform, `2` joint. Anything above 1 means a
    /// `change_membership` was interrupted between its two round trips (research trap T8);
    /// the repair is to re-issue the same call, which is idempotent.
    pub joint_config_len: usize,
    /// Ids a committed `RetireNode` has fenced out for good.
    pub retired: BTreeSet<NodeId>,
    /// Per-peer replication progress. Empty off the leader, because openraft populates the
    /// replication map only on a leader — which is why [`MembershipReport::authoritative`]
    /// exists rather than leaving a caller to read an empty map as "nothing is replicating".
    pub replication: BTreeMap<NodeId, ReplicationProgress>,
    /// The leader's own last log index; the other half of the catch-up predicate. `0` off the
    /// leader.
    pub leader_last_log_index: u64,
    /// Who this node currently believes the leader is.
    pub current_leader: Option<NodeId>,
    /// Whether this node was the leader at the instant it answered.
    pub authoritative: bool,
    /// The `promote_max_lag` this node will actually apply, so a polling caller measures
    /// against the server's threshold instead of guessing one.
    pub promote_max_lag: u64,
    /// Effective (not necessarily committed) peer endpoints, which is where a freshly added
    /// learner's address shows up before its membership entry commits.
    pub effective_endpoints: BTreeMap<NodeId, (String, String)>,
    /// `(term, index)` of the effective membership entry.
    pub effective_membership_log_id: Option<LogIdView>,
}

impl MembershipReport {
    /// Whether a `change_membership` was interrupted mid-flight (trap T8).
    pub fn is_joint(&self) -> bool {
        self.joint_config_len > 1
    }

    /// The lag of `node_id` as the leader sees it, or `None` off the leader or for a peer the
    /// leader has never heard from.
    pub fn lag_of(&self, node_id: NodeId) -> Option<u64> {
        self.replication.get(&node_id).map(|p| p.lag)
    }
}

/// The outcome of an admin `TriggerSnapshot` (OQ-44, test plan M5-28).
///
/// A typed outcome rather than an error, because "a build was already running" is a normal
/// answer — but it must never be reported as a plain `Ok`, which is exactly the silence
/// `Raft::trigger().snapshot()` inherits from openraft's internal `false` return.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotTriggered {
    /// A build was requested. `published` is the snapshot this node had published by the time
    /// the call returned, when the build completed within the write timeout.
    Started {
        /// Id of the published snapshot, once there is one.
        snapshot_id: Option<String>,
        /// The log id that snapshot covers.
        last_log_id: Option<LogIdView>,
    },
    /// A build was already in flight, so this request did nothing (trap T13).
    AlreadyInProgress,
}
