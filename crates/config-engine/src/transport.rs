//! Transport-agnostic peer plane (ADR-0010).
//!
//! OpenRaft RPCs are wrapped in [`PeerRequest`] / [`PeerResponse`] and bound to a cluster
//! identity by [`PeerEnvelopeMeta`]. The engine sends through a [`PeerTransport`] (implemented
//! by `config-grpc` over tonic) and receives through a [`PeerHandler`] (served by
//! `config-grpc`'s `PeerService`). Identity checks happen in the engine, before any OpenRaft
//! decoding of the payload is trusted.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use config_core::{
    ClusterId, NodeId, RecoveryEpoch, SchemaTriple, COMPAT_SCHEMA_1, CURRENT_SCHEMA,
};
use config_log::TraceContext;
use config_storage::{RaftNodeId, TypeConfig};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use serde::{Deserialize, Serialize};

/// Payload encoding tag carried in `PeerEnvelope.payload_encoding`.
///
/// One encoding per protocol version, never a negotiation: a receiver accepts exactly this
/// tag and refuses everything else with `INVALID_ARGUMENT`, so a peer built against a
/// different encoding is turned away by a typed refusal instead of handing bytes to a decoder
/// that will misread them. Tag `1` was serde JSON and is retired — JSON rendered
/// `bytes::Bytes` as an array of decimal numbers, expanding a payload up to 4x on the wire
/// (ADR-0010, fix-round note).
pub const PAYLOAD_ENCODING_POSTCARD: u32 = 2;

/// An OpenRaft request addressed to a peer.
#[derive(Serialize, Deserialize)]
pub enum PeerRequest {
    /// Log replication / heartbeat.
    AppendEntries(AppendEntriesRequest<TypeConfig>),
    /// Election vote.
    Vote(VoteRequest<RaftNodeId>),
    /// Required by the OpenRaft network trait; never issued in this release (ADR-0008).
    InstallSnapshot(InstallSnapshotRequest<TypeConfig>),
}

impl PeerRequest {
    /// Stable name for log fields (`rpc = "append_entries"`).
    pub const fn kind(&self) -> &'static str {
        match self {
            PeerRequest::AppendEntries(_) => "append_entries",
            PeerRequest::Vote(_) => "vote",
            PeerRequest::InstallSnapshot(_) => "install_snapshot",
        }
    }
}

impl std::fmt::Debug for PeerRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PeerRequest::{}", self.kind())
    }
}

/// The peer's answer to a [`PeerRequest`], same variant.
#[derive(Serialize, Deserialize)]
pub enum PeerResponse {
    /// Answer to `AppendEntries`.
    AppendEntries(AppendEntriesResponse<RaftNodeId>),
    /// Answer to `Vote`.
    Vote(VoteResponse<RaftNodeId>),
    /// Answer to `InstallSnapshot` (never produced in this release).
    InstallSnapshot(InstallSnapshotResponse<RaftNodeId>),
}

impl PeerResponse {
    /// Stable name for log fields.
    pub const fn kind(&self) -> &'static str {
        match self {
            PeerResponse::AppendEntries(_) => "append_entries",
            PeerResponse::Vote(_) => "vote",
            PeerResponse::InstallSnapshot(_) => "install_snapshot",
        }
    }
}

impl std::fmt::Debug for PeerResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PeerResponse::{}", self.kind())
    }
}

/// Identity binding for one peer call (ADR-0011). Carried in the `PeerEnvelope` fields and
/// checked by the receiving engine before the payload is handed to OpenRaft.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerEnvelopeMeta {
    /// Cluster the sender believes it is in.
    pub cluster_id: ClusterId,
    /// Sender's recovery epoch.
    pub recovery_epoch: RecoveryEpoch,
    /// Sending node.
    pub from: NodeId,
    /// Intended receiver.
    pub to: NodeId,
    /// Cross-wire trace context (ADR-0013); becomes gRPC metadata on the wire.
    pub trace: TraceContext,
}

/// Why a [`PeerTransport::send`] failed. Maps to OpenRaft `RPCError` in the engine:
/// `Unreachable` → backoff, `Network` → immediate retry, `Remote` → remote error,
/// `IdentityRejected` → logged at warn and treated as `Unreachable`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TransportError {
    /// Could not connect, or the destination is administratively blocked (NetFault).
    #[error("peer unreachable: {0}")]
    Unreachable(String),
    /// Connected but the call failed in flight (reset, timeout, dropped response).
    #[error("network error: {0}")]
    Network(String),
    /// The peer answered with an error status.
    #[error("remote error: {0}")]
    Remote(String),
    /// The peer rejected our identity (wrong cluster/epoch/destination or TLS identity).
    #[error("identity rejected by peer: {0}")]
    IdentityRejected(String),
}

/// Client side of the peer plane. One instance is shared by all of a node's outgoing
/// connections; implementations connect lazily per endpoint and must honor `deadline`.
#[async_trait]
pub trait PeerTransport: Send + Sync {
    /// Send `req` to the node at `endpoint` (the **committed** membership endpoint; never a
    /// gossip hint) and wait for its answer.
    async fn send(
        &self,
        meta: PeerEnvelopeMeta,
        endpoint: &str,
        req: PeerRequest,
        deadline: Duration,
    ) -> Result<PeerResponse, TransportError>;

    /// The same call, carrying this node's schema and returning the peer's (ADR-0030, M6-86).
    ///
    /// A defaulted method rather than a field on [`PeerEnvelopeMeta`]: that struct is
    /// constructed literally in a dozen places across four crates, most of them owned by other
    /// work, and a new field would be a mechanical edit in every one of them for a value only
    /// the peer plane reads.
    ///
    /// The default answers `None`, which every caller must read as "schema 1" rather than as an
    /// error — that is exactly how a genuinely older peer behaves, and the safe direction
    /// (M6-86, M6-89).
    async fn send_with_schema(
        &self,
        meta: PeerEnvelopeMeta,
        endpoint: &str,
        req: PeerRequest,
        deadline: Duration,
        schema: SchemaTriple,
    ) -> Result<(PeerResponse, Option<SchemaTriple>), TransportError> {
        let _ = schema;
        self.send(meta, endpoint, req, deadline)
            .await
            .map(|response| (response, None))
    }
}

/// The schema each peer was last observed to advertise, on the peer plane only.
///
/// Leader-local and never replicated (OQ-63, M6-R4). An entry is never removed: a voter the
/// leader can no longer reach keeps its last-known value, so an unreachable old voter holds the
/// minimum *down* rather than dropping out of it. Divergence is therefore only ever in the safe
/// direction — the leader under-reports what the cluster supports and refuses a feature it
/// might have been allowed to use, which costs a compaction, where the other direction costs
/// the cluster an undecodable committed entry (M6-89).
#[derive(Debug, Default)]
pub struct PeerSchemas {
    seen: std::sync::Mutex<std::collections::BTreeMap<NodeId, SchemaTriple>>,
}

impl PeerSchemas {
    /// Record what `node` advertised on its last answer.
    pub fn record(&self, node: NodeId, schema: SchemaTriple) {
        self.seen
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(node, schema);
    }

    /// What `node` last advertised, or [`COMPAT_SCHEMA_1`] if it never has.
    ///
    /// Never `Option`: a voter that has not answered, or that answered without a schema field,
    /// is indistinguishable from a build too old to have one, and both must gate the same way.
    #[must_use]
    pub fn get(&self, node: NodeId) -> SchemaTriple {
        self.seen
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&node)
            .copied()
            .unwrap_or(COMPAT_SCHEMA_1)
    }
}

/// Why the receiving engine refused a peer call before handing it to OpenRaft.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PeerReject {
    /// Sender is in a different cluster.
    #[error("wrong cluster: expected {expected} got {got}")]
    WrongCluster {
        /// Our cluster id.
        expected: ClusterId,
        /// Sender's cluster id.
        got: ClusterId,
    },
    /// Sender's recovery epoch differs from ours.
    #[error("wrong recovery epoch: expected {expected} got {got}")]
    WrongEpoch {
        /// Our epoch.
        expected: RecoveryEpoch,
        /// Sender's epoch.
        got: RecoveryEpoch,
    },
    /// The envelope was addressed to a different node id.
    #[error("wrong destination: this is node {expected}, envelope addressed to {got}")]
    WrongDestination {
        /// Our node id.
        expected: NodeId,
        /// `to` in the envelope.
        got: NodeId,
    },
    /// The transport identity (mTLS SAN) does not match `from` (M3).
    #[error("transport identity mismatch: {0}")]
    IdentityMismatch(String),
    /// The sender's node id was retired by a committed `RetireNode` (M5, ADR-0023,
    /// spec §21 "stale identities cannot rejoin").
    ///
    /// Distinct from [`PeerReject::IdentityMismatch`] on purpose: the certificate is
    /// genuine and the envelope is consistent — M5 fences the *identity*, not the key
    /// material, and certificate revocation is M6 (ADR-0028). An operator reading
    /// `identity_mismatch` here would go looking for a PKI fault that does not exist.
    #[error("node {node_id} is retired: identity_retired")]
    Retired {
        /// The fenced sender.
        node_id: NodeId,
    },
    /// The node is not running (stopped or not yet started).
    #[error("node not running")]
    NotRunning,
    /// OpenRaft returned an error for the call.
    #[error("raft error: {0}")]
    Raft(String),
}

/// Server side of the peer plane, implemented by the engine.
#[async_trait]
pub trait PeerSink: Send + Sync {
    /// Validate identity, then hand the request to OpenRaft and return its answer.
    async fn handle(
        &self,
        meta: PeerEnvelopeMeta,
        req: PeerRequest,
    ) -> Result<PeerResponse, PeerReject>;

    /// Whether `node_id` has been fenced out by a committed `RetireNode` (M5, ADR-0023).
    ///
    /// Exposed separately from [`PeerSink::handle`] so the transport can refuse a retired
    /// sender **before** deserializing its payload: the point of the fence is that a retired
    /// node never gets to hand this process bytes that OpenRaft will interpret. `handle`
    /// re-checks it, because the in-process transport does not go through a codec at all.
    ///
    /// Synchronous and cheap: it reads applied state under the store's lock.
    fn is_retired(&self, node_id: NodeId) -> bool {
        let _ = node_id;
        false
    }

    /// The schema this node stamps on its peer-plane answers (ADR-0030, M6-86).
    ///
    /// Defaulted for the same reason as [`PeerTransport::send_with_schema`]: a sink that does
    /// not override it is simply a node of the current build.
    fn local_schema(&self) -> SchemaTriple {
        CURRENT_SCHEMA
    }
}

/// Cheap, cloneable handle to a node's [`PeerSink`]; what `config-grpc`'s `PeerService`
/// and the in-process test router call.
#[derive(Clone)]
pub struct PeerHandler(Arc<dyn PeerSink>);

impl PeerHandler {
    /// Wrap a sink.
    pub fn new(sink: Arc<dyn PeerSink>) -> Self {
        Self(sink)
    }

    /// Deliver one peer call.
    pub async fn handle(
        &self,
        meta: PeerEnvelopeMeta,
        req: PeerRequest,
    ) -> Result<PeerResponse, PeerReject> {
        self.0.handle(meta, req).await
    }

    /// Whether the node this handle belongs to has fenced `node_id` out (M5, ADR-0023).
    pub fn is_retired(&self, node_id: NodeId) -> bool {
        self.0.is_retired(node_id)
    }

    /// The schema the node behind this handle advertises (M6, ADR-0030).
    #[must_use]
    pub fn local_schema(&self) -> SchemaTriple {
        self.0.local_schema()
    }
}

impl std::fmt::Debug for PeerHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PeerHandler")
    }
}
