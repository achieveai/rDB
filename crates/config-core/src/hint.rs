//! Advisory peer observations (ADR-0003, spec §5.2).
//!
//! These are *hints*. They never confer authority; the engine validates a hint's identity
//! against committed membership and mTLS identity before any use.

use serde::{Deserialize, Serialize};

use crate::identity::{ClusterId, NodeId};

/// Local liveness observation of a peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Liveness {
    /// Responding to probes.
    Alive,
    /// Missed probes; unconfirmed.
    Suspect,
    /// Declared dead by the failure detector.
    Dead,
    /// Left gracefully.
    Left,
}

/// Non-authoritative metadata a node may advertise over gossip.
///
/// Must serialize to ≤ 512 bytes (memberlist meta cap); keep strings short.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservedPeerHint {
    /// Cluster the peer claims to belong to.
    pub cluster_id: ClusterId,
    /// Node id the peer claims.
    pub node_id: NodeId,
    /// Candidate Raft peer endpoint (`host:port`).
    pub peer_endpoint: String,
    /// Candidate client endpoint (`host:port`), if it serves clients.
    pub client_endpoint: Option<String>,
    /// Software version string.
    pub software_version: String,
    /// Wire/protocol version.
    pub protocol_version: u32,
    /// Zone / failure-domain label.
    pub zone: Option<String>,
    /// Local liveness observation.
    pub liveness: Liveness,
}

/// Source of advisory peer hints. The engine polls this; it is never awaited on the Raft path.
pub trait GossipObservationSource: Send + Sync {
    /// Current snapshot of observed peers (excluding self). Cheap and non-blocking.
    fn peers(&self) -> Vec<ObservedPeerHint>;
}

/// A source that never observes anything (gossip disabled).
#[derive(Debug, Default, Clone, Copy)]
pub struct NoGossip;

impl GossipObservationSource for NoGossip {
    fn peers(&self) -> Vec<ObservedPeerHint> {
        Vec::new()
    }
}
