//! OpenRaft type configuration shared by every store, the engine, and the transport.

use std::io::Cursor;

use config_core::{Command, CommandResponse};

/// OpenRaft's node id type. `config_core::NodeId` is a newtype over this; convert with
/// `NodeId::from(raft_id)` / `raft_id = node_id.0`.
pub type RaftNodeId = u64;

openraft::declare_raft_types!(
    /// rEtcd's OpenRaft type configuration (ADR-0002, ADR-0007).
    ///
    /// * `D = Command` — the only payload that enters the log; canonical bytes come from
    ///   `Command::encode`, serde is used only for OpenRaft's in-memory/wire plumbing.
    /// * `R = CommandResponse` — exactly one per applied entry, including `Noop` for blank and
    ///   membership entries.
    /// * `Node = BasicNode` — `addr` holds the committed peer endpoint (`host:port`).
    /// * Snapshots are never built or installed in this release (`SnapshotPolicy::Never`).
    pub TypeConfig:
        D = Command,
        R = CommandResponse,
        NodeId = RaftNodeId,
        Node = openraft::BasicNode,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        Responder = openraft::impls::OneshotResponder<TypeConfig>,
        AsyncRuntime = openraft::TokioRuntime,
);
