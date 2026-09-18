//! The OpenRaft `RaftNetwork` adapter over rEtcd's [`PeerTransport`] (ADR-0010).
//!
//! OpenRaft asks for a connection per target; we answer with a value that carries the
//! target's **committed** endpoint and the shared transport. Nothing here ever consults
//! gossip: `new_client` is handed the `BasicNode` from committed membership, and that
//! address is the only one dialed.
//!
//! Error mapping is the load-bearing part (research §4):
//!
//! | [`TransportError`] | `RPCError` | OpenRaft's reaction |
//! |---|---|---|
//! | `Unreachable` | `Unreachable` | backoff before retrying |
//! | `IdentityRejected` | `Unreachable` (+ `warn`) | backoff; a mismatched peer is not a flaky link |
//! | `Network` | `Network` | retry immediately |
//! | `Remote` | `RemoteError(Fatal)` | decomposed by OpenRaft into `Unreachable` |

use std::time::Duration;

use config_core::{ClusterIdentity, NodeId};
use config_log::TraceContext;
use config_storage::{RaftNodeId, TypeConfig};
use openraft::error::{Fatal, NetworkError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use tracing::Instrument;

use crate::transport::{
    PeerEnvelopeMeta, PeerRequest, PeerResponse, PeerTransport, TransportError,
};
use std::sync::Arc;

/// Creates one [`EngineNetwork`] per replication target.
pub(crate) struct EngineNetworkFactory {
    pub(crate) identity: ClusterIdentity,
    pub(crate) transport: Arc<dyn PeerTransport>,
    pub(crate) span: tracing::Span,
}

impl RaftNetworkFactory<TypeConfig> for EngineNetworkFactory {
    type Network = EngineNetwork;

    async fn new_client(&mut self, target: RaftNodeId, node: &BasicNode) -> Self::Network {
        EngineNetwork {
            identity: self.identity,
            target: NodeId(target),
            endpoint: node.addr.clone(),
            transport: Arc::clone(&self.transport),
            span: self.span.clone(),
        }
    }
}

/// One node's outgoing peer channel, as OpenRaft sees it.
pub(crate) struct EngineNetwork {
    identity: ClusterIdentity,
    target: NodeId,
    endpoint: String,
    transport: Arc<dyn PeerTransport>,
    span: tracing::Span,
}

impl EngineNetwork {
    fn meta(&self) -> PeerEnvelopeMeta {
        PeerEnvelopeMeta {
            cluster_id: self.identity.cluster_id,
            recovery_epoch: self.identity.recovery_epoch,
            from: self.identity.node_id,
            to: self.target,
            trace: TraceContext::current_or_root().child(),
        }
    }

    async fn call(
        &self,
        req: PeerRequest,
        deadline: Duration,
    ) -> Result<PeerResponse, TransportError> {
        let rpc = req.kind();
        let meta = self.meta();
        self.transport
            .send(meta, &self.endpoint, req, deadline)
            .instrument(tracing::debug_span!(
                "peer_rpc",
                rpc,
                target = self.target.0,
                endpoint = %self.endpoint,
            ))
            .instrument(self.span.clone())
            .await
    }
}

/// Map a transport failure onto the `RPCError` variant whose retry behavior matches it.
fn to_rpc_error<E>(
    target: NodeId,
    err: TransportError,
) -> RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId, E>>
where
    E: std::error::Error,
{
    match err {
        TransportError::Unreachable(_) => RPCError::Unreachable(Unreachable::new(&err)),
        TransportError::IdentityRejected(ref detail) => {
            tracing::warn!(
                target_node_id = target.0,
                detail = %detail,
                "peer rejected our identity; backing off"
            );
            RPCError::Unreachable(Unreachable::new(&err))
        }
        TransportError::Network(_) => RPCError::Network(NetworkError::new(&err)),
        // OpenRaft decomposes `RemoteError(Fatal)` into `Unreachable`, which is the right
        // reaction to a peer that answered but could not serve us.
        TransportError::Remote(_) => {
            RPCError::RemoteError(RemoteError::new(target.0, RaftError::Fatal(Fatal::Stopped)))
        }
    }
}

fn wrong_variant<E>(
    target: NodeId,
    expected: &'static str,
    got: &'static str,
) -> RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId, E>>
where
    E: std::error::Error,
{
    RPCError::Network(NetworkError::new(&TransportError::Network(format!(
        "peer {target} answered a {expected} call with a {got} response"
    ))))
}

impl RaftNetwork<TypeConfig> for EngineNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<RaftNodeId>,
        RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId>>,
    > {
        let resp = self
            .call(PeerRequest::AppendEntries(rpc), option.hard_ttl())
            .await
            .map_err(|e| to_rpc_error(self.target, e))?;
        match resp {
            PeerResponse::AppendEntries(r) => Ok(r),
            other => Err(wrong_variant(self.target, "append_entries", other.kind())),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<RaftNodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<RaftNodeId>, RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId>>>
    {
        let resp = self
            .call(PeerRequest::Vote(rpc), option.hard_ttl())
            .await
            .map_err(|e| to_rpc_error(self.target, e))?;
        match resp {
            PeerResponse::Vote(r) => Ok(r),
            other => Err(wrong_variant(self.target, "vote", other.kind())),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<RaftNodeId>,
        RPCError<
            RaftNodeId,
            BasicNode,
            RaftError<RaftNodeId, openraft::error::InstallSnapshotError>,
        >,
    > {
        // Required by the trait; never reached, because the engine runs with
        // `SnapshotPolicy::Never` and never purges a log (ADR-0008). Returning a typed error
        // rather than `todo!()` keeps a contract violation debuggable instead of fatal.
        let resp = self
            .call(PeerRequest::InstallSnapshot(rpc), option.hard_ttl())
            .await
            .map_err(|e| to_rpc_error(self.target, e))?;
        match resp {
            PeerResponse::InstallSnapshot(r) => Ok(r),
            other => Err(wrong_variant(self.target, "install_snapshot", other.kind())),
        }
    }
}
