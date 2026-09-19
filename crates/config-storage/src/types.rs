//! OpenRaft type configuration shared by every store, the engine, and the transport.

use std::io::Cursor;

use config_core::{Command, CommandResponse};
use serde::{Deserialize, Serialize};

/// OpenRaft's node id type. `config_core::NodeId` is a newtype over this; convert with
/// `NodeId::from(raft_id)` / `raft_id = node_id.0`.
pub type RaftNodeId = u64;

/// What committed membership records about one voter (ADR-0009, ADR-0011).
///
/// `BasicNode` carries a single `addr`, which forced the engine to choose between the address
/// peers dial and the address clients dial. It chose the peer endpoint, so every leader hint
/// named a port no client could reach. Both endpoints therefore live in the Raft `Node` type:
/// they are proposed by one membership entry and committed by the same quorum, so a hint can
/// never name an endpoint the cluster did not agree on.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RaftNode {
    /// Peer-plane endpoint (`host:port`). The only address the Raft transport ever dials.
    pub peer: String,
    /// Client-plane endpoint (`host:port`). The only address a leader hint ever names.
    pub client: String,
}

impl RaftNode {
    /// A node reachable at `peer` for replication and at `client` for client traffic.
    pub fn new(peer: impl Into<String>, client: impl Into<String>) -> Self {
        Self {
            peer: peer.into(),
            client: client.into(),
        }
    }

    /// A node whose client plane is served on the same endpoint as its peer plane.
    ///
    /// The M1 in-process profile: there is one synthetic endpoint per node and no second
    /// listener to name.
    pub fn same(endpoint: impl Into<String>) -> Self {
        let endpoint = endpoint.into();
        Self {
            peer: endpoint.clone(),
            client: endpoint,
        }
    }
}

impl std::fmt::Display for RaftNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "peer={} client={}", self.peer, self.client)
    }
}

openraft::declare_raft_types!(
    /// rEtcd's OpenRaft type configuration (ADR-0002, ADR-0007).
    ///
    /// * `D = Command` — the only payload that enters the log; canonical bytes come from
    ///   `Command::encode`, serde is used only for OpenRaft's in-memory/wire plumbing.
    /// * `R = CommandResponse` — exactly one per applied entry, including `Noop` for blank and
    ///   membership entries.
    /// * `Node = RaftNode` — the committed peer *and* client endpoints of a voter.
    /// * Snapshots are never built or installed in this release (`SnapshotPolicy::Never`).
    pub TypeConfig:
        D = Command,
        R = CommandResponse,
        NodeId = RaftNodeId,
        Node = RaftNode,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        Responder = openraft::impls::OneshotResponder<TypeConfig>,
        AsyncRuntime = openraft::TokioRuntime,
);
