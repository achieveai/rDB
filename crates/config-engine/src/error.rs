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
    /// [`config_storage::RaftNode::peer`] in committed membership and are thereafter the only
    /// addresses the Raft transport dials (ADR-0003).
    pub voters: BTreeMap<NodeId, String>,
    /// The initial voters' **client** endpoints (`host:port`), which become
    /// [`config_storage::RaftNode::client`] and are the only endpoints a leader hint may name
    /// (ADR-0009).
    ///
    /// A voter absent from this map advertises its peer endpoint as its client endpoint,
    /// which is the single-listener in-process profile.
    pub client_endpoints: BTreeMap<NodeId, String>,
}

impl FormationPlan {
    /// Build a plan for `identity`'s cluster from `(node id, peer endpoint)` pairs.
    ///
    /// Every voter's client endpoint defaults to its peer endpoint. Use
    /// [`FormationPlan::with_client_endpoints`] when the two planes have separate listeners.
    pub fn new(
        identity: &ClusterIdentity,
        voters: impl IntoIterator<Item = (NodeId, String)>,
    ) -> Self {
        Self {
            cluster_id: identity.cluster_id,
            recovery_epoch: identity.recovery_epoch,
            voters: voters.into_iter().collect(),
            client_endpoints: BTreeMap::new(),
        }
    }

    /// Build a plan from `(node id, peer endpoint, client endpoint)` triples.
    pub fn with_client_endpoints(
        identity: &ClusterIdentity,
        voters: impl IntoIterator<Item = (NodeId, String, String)>,
    ) -> Self {
        let mut peers = BTreeMap::new();
        let mut clients = BTreeMap::new();
        for (id, peer, client) in voters {
            peers.insert(id, peer);
            clients.insert(id, client);
        }
        Self {
            cluster_id: identity.cluster_id,
            recovery_epoch: identity.recovery_epoch,
            voters: peers,
            client_endpoints: clients,
        }
    }

    /// The client endpoint this plan gives `node_id`: its explicit one, or its peer endpoint.
    pub fn client_endpoint_of(&self, node_id: NodeId) -> Option<&str> {
        self.client_endpoints
            .get(&node_id)
            .or_else(|| self.voters.get(&node_id))
            .map(String::as_str)
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
    /// The plan's entry for this node names an endpoint this node does not serve.
    ///
    /// Committing it would publish an address nobody answers on, and the disagreement would
    /// only surface later as an unreachable peer or an unusable leader hint.
    #[error("formation plan gives this node {plane} endpoint {planned:?}, but it is configured as {configured:?}")]
    EndpointMismatch {
        /// `"peer"` or `"client"`.
        plane: &'static str,
        /// What the plan said.
        planned: String,
        /// What this node is configured to advertise.
        configured: String,
    },
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
