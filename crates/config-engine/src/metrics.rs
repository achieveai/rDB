//! Node-level observable state: metrics, health, and the committed membership view.
//!
//! None of these types name an OpenRaft type (ADR-0004), because they cross into the test
//! harness, the health endpoint, and eventually the gRPC surface.

use std::collections::{BTreeMap, BTreeSet};

use config_core::{
    Authz, ClusterIdentity, Durability, LeaderHint, NodeId, RecoveryEpoch, TransportSecurity,
};
use serde::Serialize;

use crate::config::AuthzKind;

/// A Raft log id, flattened for callers that must not depend on OpenRaft.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct LogIdView {
    /// Term of the leader that proposed the entry.
    pub term: u64,
    /// Index of the entry.
    pub index: u64,
}

impl LogIdView {
    /// Build a view from its parts.
    pub const fn new(term: u64, index: u64) -> Self {
        Self { term, index }
    }
}

impl From<LogIdView> for (u64, u64) {
    fn from(v: LogIdView) -> Self {
        (v.term, v.index)
    }
}

/// A node's Raft role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeRole {
    /// Replicating, but neither voting nor timing out — including a node that has never been
    /// formed.
    Learner,
    /// Replicating from a leader.
    Follower,
    /// Campaigning.
    Candidate,
    /// Leading.
    Leader,
    /// Shutting down or shut down.
    Shutdown,
}

impl NodeRole {
    /// Stable snake_case name for log and metric fields.
    pub const fn as_str(self) -> &'static str {
        match self {
            NodeRole::Learner => "learner",
            NodeRole::Follower => "follower",
            NodeRole::Candidate => "candidate",
            NodeRole::Leader => "leader",
            NodeRole::Shutdown => "shutdown",
        }
    }
}

impl std::fmt::Display for NodeRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A cheap synchronous snapshot of one node's Raft and storage state (test plan TA-6).
///
/// `raft_log_len` and `membership_voter_ids` are load-bearing, not decorative: they are how a
/// test proves a direct write really went through Raft (the log grew on every node) and how
/// it proves gossip cannot change membership (the voter set is identical before and after).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeMetrics {
    /// This node.
    pub node_id: NodeId,
    /// Its current role.
    pub role: NodeRole,
    /// Its current Raft term.
    pub current_term: u64,
    /// The leader it currently believes in, if any. Metrics-derived and therefore stale by
    /// design — a routing hint, never a read guard.
    pub current_leader: Option<NodeId>,
    /// Last index appended to this node's log.
    pub last_log_index: Option<u64>,
    /// Last log id applied to this node's state machine.
    pub last_applied: Option<LogIdView>,
    /// Entries currently held in this node's log.
    pub raft_log_len: u64,
    /// Voter ids of the **effective** membership, ascending.
    ///
    /// Effective, not committed: this is OpenRaft's `membership_config`, which moves as soon
    /// as a membership entry is appended. It is the right thing for a harness to poll while
    /// waiting for a cluster to form. It is the wrong thing to derive a client-followable
    /// hint from — [`crate::ConfigNode::committed_membership`] is that (ADR-0009).
    pub membership_voter_ids: Vec<NodeId>,
    /// Log id of the effective membership entry, if any.
    pub membership_log_id: Option<LogIdView>,
    /// The public cluster revision this node has applied (not the log index; ADR-0005).
    pub cluster_revision: u64,
    /// Command-carrying entries applied so far, excluding blank and membership entries.
    pub applied_commands: u64,
    /// Whether OpenRaft's `running_state` is `Ok`. `false` means the core stopped, normally
    /// because storage failed fatally (spec §9.3.7).
    pub running_state_ok: bool,
    /// For a leader, milliseconds since a quorum last acknowledged it. The signal that a
    /// leader may be partitioned (spec §18.2).
    pub millis_since_quorum_ack: Option<u64>,
    /// Authorization decisions this node refused, since start (M3-81).
    ///
    /// Counted in the single authorize seam, so it covers every client entry point and
    /// includes the fail-closed denials a node with a missing or invalid policy issues. One
    /// `retcd.audit` `deny` line exists for each of these; the counter is what makes "a spike
    /// of refusals" assertable without parsing a log.
    pub authz_denied: u64,
    /// Client connections whose transport identity could not be established, since start.
    ///
    /// *Authentication*, not authorization: the caller never became a principal, so no
    /// authorization decision was reached and no audit line was written. Recorded by the
    /// transport through [`crate::ConfigNode::record_authn_rejection`], because the engine
    /// never sees a certificate.
    pub authn_rejected: u64,
}

/// What authorization policy a node is actually holding (M3-42).
///
/// Three facts, none of them a policy body. Together they answer the question a cross-process
/// test and an operator both need answered — *are these nodes enforcing the same policy?* —
/// without putting a grant, a principal name, or a key prefix on an unauthenticated health
/// endpoint.
///
/// `policy_hash_hex` is the digest of the **document bytes the embedder loaded**, not of the
/// parsed value: two nodes given byte-identical documents print the same string, and a node
/// given an edited copy prints a different one even when the edit happened to parse to the
/// same grants. That is the property a fleet check wants. It is `None` when there is no
/// document at all — an `AllowAll` node, or one whose policy was missing.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct PolicySummary {
    /// The model in force, mirroring [`crate::ConfigNode::capabilities`]'s `authz`.
    pub kind: Authz,
    /// How many grant rules the node's allowlist holds. `0` for `AllowAll`, and for a policy
    /// that was missing or unparsable — in those two cases the node enforces no grants at all
    /// and is unready besides.
    pub grants: u64,
    /// Lowercase hex SHA-256 of the policy document bytes, when the embedder supplied them.
    pub policy_hash_hex: Option<String>,
}

/// What a node will do with client traffic right now (spec §18.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Health {
    /// Leader, with a working quorum; strict reads and writes are served here.
    Ready,
    /// A follower. The hint, when present, names the committed endpoint of the leader.
    NotLeader {
        /// Where to retry.
        hint: Option<LeaderHint>,
    },
    /// Not formed, no leader known, or storage failed fatally.
    Unavailable {
        /// Operator-facing explanation; never contains a key or value.
        reason: String,
    },
    /// [`crate::ConfigNode::stop`] has been called.
    Stopped,
}

/// The committed membership: the only authoritative statement of who the voters are and
/// where they live (ADR-0003, ADR-0011).
///
/// Gossip observations are validated *against* this and never merged into it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MembershipView {
    /// Committed voter ids.
    pub voters: BTreeSet<NodeId>,
    /// Committed **peer-plane** endpoints, by voter id. What the Raft transport dials.
    pub endpoints: BTreeMap<NodeId, String>,
    /// Committed **client-plane** endpoints, by voter id. What a leader hint names.
    ///
    /// Both planes come out of the same membership entry, so a hint can never name an
    /// endpoint the cluster did not commit to (ADR-0009).
    pub client_endpoints: BTreeMap<NodeId, String>,
    /// `(term, index)` of the membership log entry, if membership has ever been committed.
    pub membership_log_id: Option<(u64, u64)>,
}

impl MembershipView {
    /// Whether any membership has been committed yet — i.e. whether the cluster is formed.
    pub fn is_formed(&self) -> bool {
        !self.voters.is_empty()
    }

    /// The committed peer endpoint of `node_id`, if it is a known member.
    pub fn endpoint_of(&self, node_id: NodeId) -> Option<&str> {
        self.endpoints.get(&node_id).map(String::as_str)
    }

    /// The committed client endpoint of `node_id`, if it is a known member.
    pub fn client_endpoint_of(&self, node_id: NodeId) -> Option<&str> {
        self.client_endpoints.get(&node_id).map(String::as_str)
    }
}

/// The serializable cross-process state oracle (spec §18.1, ADR-0016, test plan TA-17).
///
/// This is what a health endpoint serves and what an end-to-end test reads instead of calling
/// `state_hash()` in a process it does not share. It carries **no keys and no values**: every
/// field is an id, a count, a revision, an enum, or the [`state_hash_hex`] digest, so serving
/// it on an unauthenticated loopback listener leaks nothing (§15.2, OQ-16).
///
/// [`state_hash_hex`]: HealthPayload::state_hash_hex
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HealthPayload {
    /// This node's id.
    pub node_id: NodeId,
    /// Its cluster id, as 32 lowercase hex characters.
    pub cluster_id: String,
    /// Its recovery epoch.
    pub recovery_epoch: RecoveryEpoch,
    /// Its current Raft role.
    pub role: NodeRole,
    /// The leader it currently believes in, if any.
    pub current_leader: Option<NodeId>,
    /// Its current Raft term.
    pub term: u64,
    /// Last log index applied to the state machine.
    pub last_applied: Option<u64>,
    /// Last log index known to be committed.
    pub committed: Option<u64>,
    /// Committed voter ids, ascending.
    pub membership_voter_ids: Vec<NodeId>,
    /// Log id of the committed membership entry.
    pub membership_log_id: Option<LogIdView>,
    /// The public cluster revision applied here (ADR-0005), not a log index.
    pub cluster_revision: u64,
    /// The deterministic applied-state digest as 64 lowercase hex characters (TA-2). Two
    /// nodes on the same applied prefix print the same string, which is what makes a
    /// cross-process convergence check one assertion.
    pub state_hash_hex: String,
    /// Command-carrying entries applied so far.
    pub applied_commands: u64,
    /// What this node's store guarantees survives a restart.
    pub durability: Durability,
    /// Whether this node will serve client traffic: membership known, storage not poisoned,
    /// and an authorization model actually in force.
    pub ready: bool,
    /// Which authorization model is in force — including `missing` and `invalid`, which
    /// [`config_core::Capabilities::authz`] cannot express.
    pub authz_kind: AuthzKind,
    /// How the client plane is protected.
    pub transport_security: TransportSecurity,
    /// The policy this node holds, identically on every node given the same document (M3-42).
    pub policy: PolicySummary,
    /// Authorization decisions refused since start. See [`NodeMetrics::authz_denied`].
    pub authz_denied: u64,
    /// Client connections whose identity could not be established since start. See
    /// [`NodeMetrics::authn_rejected`].
    pub authn_rejected: u64,
}

impl HealthPayload {
    /// Render an identity into the payload's id fields.
    pub(crate) fn identity_fields(identity: &ClusterIdentity) -> (NodeId, String, RecoveryEpoch) {
        (
            identity.node_id,
            identity.cluster_id.to_string(),
            identity.recovery_epoch,
        )
    }
}
