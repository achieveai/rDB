//! Engine lifecycle and formation errors (ADR-0011, ADR-0015).

use std::collections::BTreeMap;
use std::time::Duration;

use config_core::{ClusterId, ClusterIdentity, IdentityMismatch, NodeId, RecoveryEpoch};

use crate::metrics::NodeMetrics;

/// Why a node could not be started or stopped.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EngineError {
    /// [`crate::ConfigNode::start`] was called outside a Tokio runtime. The library never
    /// creates one (spec §6.3): OpenRaft spawns its tick, core, and state-machine tasks on
    /// the caller's runtime.
    #[error("no current Tokio runtime; ConfigNode::start must be called from inside one")]
    NoRuntime,
    /// OpenRaft refused to start or stop.
    #[error("raft error: {0}")]
    Raft(String),
    /// The store refused to open or answer.
    #[error("storage error: {0}")]
    Storage(String),
    /// This node has already been started.
    #[error("node already started")]
    AlreadyStarted,
    /// The node has been stopped; it will not serve anything further.
    #[error("node stopped")]
    Stopped,
}

/// The explicit act of creating a cluster (spec §13.1, ADR-0011).
///
/// There is no implicit formation: a node with an empty store that is never handed a plan
/// stays idle and answers `Unavailable` forever, which is what stops a restarted-but-wiped
/// node from electing itself a one-node cluster and overwriting reality.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FormationPlan {
    /// The cluster being created; must equal the node's configured cluster id.
    pub cluster_id: ClusterId,
    /// The recovery epoch being created at; must equal the node's configured epoch.
    pub recovery_epoch: RecoveryEpoch,
    /// The initial voters and their **peer** endpoints (`host:port`). These become
    /// `BasicNode::addr` in committed membership, and are thereafter the only addresses the
    /// transport dials and the only endpoints a leader hint may name (ADR-0003).
    pub voters: BTreeMap<NodeId, String>,
}

impl FormationPlan {
    /// Build a plan for `identity`'s cluster from `(node id, peer endpoint)` pairs.
    pub fn new(
        identity: &ClusterIdentity,
        voters: impl IntoIterator<Item = (NodeId, String)>,
    ) -> Self {
        Self {
            cluster_id: identity.cluster_id,
            recovery_epoch: identity.recovery_epoch,
            voters: voters.into_iter().collect(),
        }
    }
}

/// Why [`crate::ConfigNode::form_cluster`] refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FormationError {
    /// The plan names a different cluster or epoch than this node is bound to.
    #[error(transparent)]
    IdentityMismatch(#[from] IdentityMismatch),
    /// The local store already holds a vote, a log entry, or applied state. Formation is
    /// only ever legitimate on a genuinely fresh store.
    #[error("local store is not fresh; formation is only allowed on an empty store")]
    StoreNotFresh,
    /// The cluster already has committed membership.
    #[error("cluster is already formed")]
    AlreadyFormed,
    /// The plan does not list this node among its voters.
    #[error("this node is not a voter in the supplied formation plan")]
    NotAVoter,
    /// OpenRaft refused `initialize`.
    #[error("raft error: {0}")]
    Raft(String),
}

/// A bounded wait expired (test plan §6 rule 2: a timeout must be diagnosable).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("timed out after {waited:?} waiting for {what}; last observed metrics: {last:?}")]
pub struct Timeout {
    /// What was being waited for, e.g. `"applied index >= 5"`.
    pub what: String,
    /// How long the wait lasted.
    pub waited: Duration,
    /// The node's metrics at the moment the deadline expired.
    pub last: Box<NodeMetrics>,
}
