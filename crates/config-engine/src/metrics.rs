//! Node-level observable state: metrics, health, and the committed membership view.
//!
//! None of these types name an OpenRaft type (ADR-0004), because they cross into the test
//! harness, the health endpoint, and eventually the gRPC surface.

use std::collections::{BTreeMap, BTreeSet};

use config_core::{LeaderHint, NodeId};

/// A Raft log id, flattened for callers that must not depend on OpenRaft.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
    /// Committed voter ids, ascending.
    pub membership_voter_ids: Vec<NodeId>,
    /// Log id of the committed membership entry, if any.
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
    /// Committed peer endpoints, by voter id.
    pub endpoints: BTreeMap<NodeId, String>,
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
}
