//! The OpenRaft `RaftNetwork` adapter over rEtcd's [`PeerTransport`] (ADR-0010).
//!
//! OpenRaft asks for a connection per target; we answer with a value that carries the
//! target's **committed** endpoint and the shared transport. Nothing here ever consults
//! gossip: `new_client` is handed the [`RaftNode`] from committed membership, and its
//! `peer` address is the only one dialed.
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

use config_core::{ClusterIdentity, NodeId, SchemaTriple, COMPAT_SCHEMA_1};
use config_log::TraceContext;
use config_storage::{RaftNode, RaftNodeId, TraceRegistry, TypeConfig};
use openraft::error::{Fatal, NetworkError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use tracing::Instrument;

use crate::transport::{
    PeerEnvelopeMeta, PeerRequest, PeerResponse, PeerSchemas, PeerTransport, TransportError,
};
use std::sync::Arc;

/// Creates one [`EngineNetwork`] per replication target.
pub(crate) struct EngineNetworkFactory {
    pub(crate) identity: ClusterIdentity,
    pub(crate) transport: Arc<dyn PeerTransport>,
    pub(crate) span: tracing::Span,
    pub(crate) traces: Arc<TraceRegistry>,
    /// What this node advertises to its peers (ADR-0030).
    pub(crate) schema: SchemaTriple,
    /// Where each peer's answer is recorded, shared with the node that computes the minimum.
    pub(crate) peer_schemas: Arc<PeerSchemas>,
}

impl RaftNetworkFactory<TypeConfig> for EngineNetworkFactory {
    type Network = EngineNetwork;

    async fn new_client(&mut self, target: RaftNodeId, node: &RaftNode) -> Self::Network {
        EngineNetwork {
            identity: self.identity,
            target: NodeId(target),
            endpoint: node.peer.clone(),
            transport: Arc::clone(&self.transport),
            span: self.span.clone(),
            traces: Arc::clone(&self.traces),
            schema: self.schema,
            peer_schemas: Arc::clone(&self.peer_schemas),
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
    traces: Arc<TraceRegistry>,
    schema: SchemaTriple,
    peer_schemas: Arc<PeerSchemas>,
}

impl EngineNetwork {
    /// The client trace this RPC is replicating, if it is unambiguous.
    ///
    /// Only when *every* command entry in the request maps to the same recorded client trace:
    /// the receiving node cannot tell which of a batch's entries an envelope trace belongs to,
    /// so a mixed batch propagates nothing rather than mislabelling entries. A heartbeat (no
    /// command entries) also propagates nothing (ADR-0013; `config_storage::trace`).
    fn client_trace_id(&self, req: &PeerRequest) -> Option<String> {
        let PeerRequest::AppendEntries(rpc) = req else {
            return None;
        };
        let mut found: Option<String> = None;
        for entry in &rpc.entries {
            let openraft::EntryPayload::Normal(cmd) = &entry.payload else {
                continue;
            };
            let trace_id = self.traces.lookup(cmd)?;
            match &found {
                None => found = Some(trace_id),
                Some(seen) if *seen == trace_id => {}
                Some(_) => return None,
            }
        }
        found
    }

    fn meta(&self, req: &PeerRequest) -> PeerEnvelopeMeta {
        // A replicated client write travels under the client's trace; anything else opens its
        // own hop, which is still a truthful statement about why the entry moved.
        let trace = match self.client_trace_id(req) {
            Some(trace_id) => TraceContext::from_headers(Some(&trace_id), None, None),
            None => TraceContext::current_or_root().child(),
        };
        PeerEnvelopeMeta {
            cluster_id: self.identity.cluster_id,
            recovery_epoch: self.identity.recovery_epoch,
            from: self.identity.node_id,
            to: self.target,
            trace,
        }
    }

    async fn call(
        &self,
        req: PeerRequest,
        deadline: Duration,
    ) -> Result<PeerResponse, TransportError> {
        let rpc = req.kind();
        let meta = self.meta(&req);
        let (response, peer_schema) = self
            .transport
            .send_with_schema(meta, &self.endpoint, req, deadline, self.schema)
            .instrument(tracing::debug_span!(
                "peer_rpc",
                rpc,
                target = self.target.0,
                endpoint = %self.endpoint,
            ))
            .instrument(self.span.clone())
            .await?;
        // Only an answer counts, and every answer counts. A peer that did not reply tells us
        // nothing new, and its last known schema — or the schema-1 default — is what the gate
        // must keep using (M6-89); the `?` above has already returned in that case.
        //
        // An answer that carried no schema field is recorded as [`COMPAT_SCHEMA_1`] rather
        // than left absent, because it is the strongest evidence the peer plane can produce
        // about a genuinely pre-M6 build: it replied, and it has no triple to name. Leaving it
        // absent would make it indistinguishable from a voter the leader has never heard from,
        // and the gate's steady-state clause treats those two oppositely on purpose (F-014) —
        // an unreachable voter must not re-gate a running cluster, an answering old voter
        // must. Absence therefore means strictly "no answer" (`PeerSchemas::observed`).
        self.peer_schemas
            .record(self.target, peer_schema.unwrap_or(COMPAT_SCHEMA_1));
        Ok(response)
    }
}

/// Map a transport failure onto the `RPCError` variant whose retry behavior matches it.
fn to_rpc_error<E>(
    target: NodeId,
    err: TransportError,
) -> RPCError<RaftNodeId, RaftNode, RaftError<RaftNodeId, E>>
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
) -> RPCError<RaftNodeId, RaftNode, RaftError<RaftNodeId, E>>
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
        RPCError<RaftNodeId, RaftNode, RaftError<RaftNodeId>>,
    > {
        let resp = self
            .call(PeerRequest::AppendEntries(rpc), option.hard_ttl())
            .await
            .map_err(|e| self.span.in_scope(|| to_rpc_error(self.target, e)))?;
        match resp {
            PeerResponse::AppendEntries(r) => Ok(r),
            other => Err(wrong_variant(self.target, "append_entries", other.kind())),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<RaftNodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<RaftNodeId>, RPCError<RaftNodeId, RaftNode, RaftError<RaftNodeId>>>
    {
        let resp = self
            .call(PeerRequest::Vote(rpc), option.hard_ttl())
            .await
            .map_err(|e| self.span.in_scope(|| to_rpc_error(self.target, e)))?;
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
            RaftNode,
            RaftError<RaftNodeId, openraft::error::InstallSnapshotError>,
        >,
    > {
        // Required by the trait; never reached, because the engine runs with
        // `SnapshotPolicy::Never` and never purges a log (ADR-0008). Returning a typed error
        // rather than `todo!()` keeps a contract violation debuggable instead of fatal.
        let resp = self
            .call(PeerRequest::InstallSnapshot(rpc), option.hard_ttl())
            .await
            .map_err(|e| self.span.in_scope(|| to_rpc_error(self.target, e)))?;
        match resp {
            PeerResponse::InstallSnapshot(r) => Ok(r),
            other => Err(wrong_variant(self.target, "install_snapshot", other.kind())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use config_core::{ClusterId, RecoveryEpoch, CURRENT_SCHEMA};
    use openraft::Vote;

    /// A peer that answers every call and never names a schema — the shape of a build older
    /// than ADR-0030, reproduced by simply *not* overriding `send_with_schema`.
    struct SchemalessPeer;

    #[async_trait::async_trait]
    impl PeerTransport for SchemalessPeer {
        async fn send(
            &self,
            _meta: PeerEnvelopeMeta,
            _endpoint: &str,
            req: PeerRequest,
            _deadline: Duration,
        ) -> Result<PeerResponse, TransportError> {
            let PeerRequest::Vote(v) = req else {
                unreachable!("this stub is only ever asked to vote")
            };
            Ok(PeerResponse::Vote(VoteResponse {
                vote: v.vote,
                vote_granted: true,
                last_log_id: None,
            }))
        }
    }

    fn network(peer_schemas: Arc<PeerSchemas>) -> EngineNetwork {
        EngineNetwork {
            identity: ClusterIdentity {
                cluster_id: ClusterId::from_bytes([1u8; 16]),
                recovery_epoch: RecoveryEpoch(0),
                node_id: NodeId(1),
            },
            target: NodeId(2),
            endpoint: "inproc://2".to_string(),
            transport: Arc::new(SchemalessPeer),
            span: tracing::Span::none(),
            traces: Arc::new(TraceRegistry::new()),
            schema: CURRENT_SCHEMA,
            peer_schemas,
        }
    }

    /// F-014 third case: a reachable voter that answers *without* a schema field is recorded,
    /// as schema 1, rather than left absent.
    ///
    /// This is the case the gate's steady-state clause would otherwise get exactly backwards.
    /// Absence means "never answered" and deliberately does not block a cluster that has
    /// already activated the feature (ruling M6-R15); a genuinely pre-M6 voter is the opposite
    /// — it is present, it is replicating, and it cannot decode the generation. Leaving it
    /// absent would make the real old-build case the *only* one the gate ignores, which is
    /// worse than the bug F-014 closes, because the `--compat-schema` case is a rehearsal and
    /// this one is production.
    #[tokio::test]
    async fn an_answer_without_a_schema_field_is_recorded_as_schema_1() {
        let seen = Arc::new(PeerSchemas::default());
        let net = network(Arc::clone(&seen));
        assert_eq!(
            seen.observed(NodeId(2)),
            None,
            "nothing is known before the first answer"
        );

        net.call(
            PeerRequest::Vote(VoteRequest::new(Vote::new(1, 1), None)),
            Duration::from_secs(1),
        )
        .await
        .expect("the stub always answers");

        assert_eq!(
            seen.observed(NodeId(2)),
            Some(COMPAT_SCHEMA_1),
            "an answer always records something, so absence can mean only `no answer`"
        );
    }
}
