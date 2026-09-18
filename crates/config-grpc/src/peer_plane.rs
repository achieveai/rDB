//! The Raft peer plane: `PeerService` over tonic (ADR-0010, ADR-0011).
//!
//! OpenRaft payloads travel as opaque serde-JSON bytes inside a [`pb::PeerEnvelope`] whose
//! header fields bind the call to a cluster identity. The order of checks here is the
//! security property: encoding tag, cluster id shape, then — under mutual TLS — the
//! certificate's claimed node identity, and only then is the payload deserialized. A node
//! from a foreign cluster never gets to hand us bytes that OpenRaft will interpret.
//!
//! Identity *checks* live in the engine ([`PeerHandler`] returns [`PeerReject`]); this layer
//! only adds the one check the engine cannot make, because only the transport can see the
//! certificate.

use std::time::Instant;

use config_core::{ClusterId, NodeId, RecoveryEpoch};
use config_engine::transport::{
    PeerEnvelopeMeta, PeerHandler, PeerReject, PeerRequest, PeerResponse, PAYLOAD_ENCODING_JSON,
};
use config_log::TraceContext;
use tokio::net::TcpListener;
use tonic::{Request, Response, Status};
use tracing::Instrument;

use crate::error::GrpcError;
use crate::pb;
use crate::pb::peer_service_server::{PeerService, PeerServiceServer};
use crate::server::{spawn, ServerHandle};
use crate::tls::{node_identity_from_certs, TlsMode};

/// Who this peer plane answers as (ADR-0011).
///
/// Every response envelope is stamped with these fields rather than echoing the request's.
/// Echoing makes the identity fields self-confirming: an impostor that answers a call gets to
/// claim whatever the caller already believed. Stamping means the caller can check that the
/// node that answered is the node it addressed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerIdentity {
    /// Cluster this node belongs to.
    pub cluster_id: ClusterId,
    /// This node's recovery epoch.
    pub recovery_epoch: RecoveryEpoch,
    /// This node's id.
    pub node_id: NodeId,
}

struct PeerSvc {
    handler: PeerHandler,
    tls: TlsMode,
    identity: PeerIdentity,
    /// The span in effect when the plane was started; see the note on the client plane's
    /// equivalent field — connection tasks do not inherit it by themselves.
    server_span: tracing::Span,
}

/// Map an engine-side refusal onto the wire.
///
/// Every identity failure is `UNAUTHENTICATED` and says nothing beyond what the caller
/// already knows about itself, so a prober learns no cluster topology from the answer.
pub fn status_from_reject(reject: &PeerReject) -> Status {
    match reject {
        PeerReject::WrongCluster { .. }
        | PeerReject::WrongEpoch { .. }
        | PeerReject::WrongDestination { .. }
        | PeerReject::IdentityMismatch(_) => Status::unauthenticated(reject.to_string()),
        PeerReject::NotRunning => Status::unavailable(reject.to_string()),
        PeerReject::Raft(detail) => Status::internal(detail.clone()),
    }
}

impl PeerSvc {
    /// Verify that the TLS certificate's `retcd://<cluster_id>/node/<node_id>` SAN agrees with
    /// what the envelope claims (M3). Under [`TlsMode::Insecure`] there is nothing to check.
    fn check_transport_identity<T>(
        &self,
        request: &Request<T>,
        cluster_id: ClusterId,
        from: NodeId,
    ) -> Result<(), Status> {
        if matches!(self.tls, TlsMode::Insecure) {
            return Ok(());
        }
        let certs = request.peer_certs().ok_or_else(|| {
            status_from_reject(&PeerReject::IdentityMismatch(
                "no peer certificate presented".into(),
            ))
        })?;
        let der: Vec<&[u8]> = certs.iter().map(|c| c.as_ref()).collect();
        match node_identity_from_certs(&der) {
            Some((cert_cluster, cert_node)) if cert_cluster == cluster_id && cert_node == from => {
                Ok(())
            }
            Some((cert_cluster, cert_node)) => {
                Err(status_from_reject(&PeerReject::IdentityMismatch(format!(
                    "certificate claims cluster {cert_cluster} node {cert_node}, \
                     envelope claims cluster {cluster_id} node {from}"
                ))))
            }
            None => Err(status_from_reject(&PeerReject::IdentityMismatch(
                "peer certificate carries no retcd node SAN URI".into(),
            ))),
        }
    }

    async fn call(
        &self,
        expected: &'static str,
        request: Request<pb::PeerEnvelope>,
    ) -> Result<Response<pb::PeerEnvelope>, Status> {
        let started = Instant::now();
        let meta_headers = request.metadata();
        let header = |k: &str| meta_headers.get(k).and_then(|v| v.to_str().ok());
        let trace = TraceContext::from_headers(
            header(config_log::HEADER_TRACE_ID),
            header(config_log::HEADER_PARENT_SPAN),
            header(config_log::HEADER_REQUEST_ID),
        );
        // Opened before the first check so that a refusal is a line inside the caller's trace,
        // not an orphan (ADR-0013). A rejected peer call is the one a operator most wants to
        // find by trace id.
        let span = self.server_span.in_scope(|| trace.span("peer_rpc"));

        let reject = |reason: &'static str, status: Status| -> Status {
            span.in_scope(|| {
                tracing::warn!(
                    rpc = expected,
                    reason,
                    latency_ms = started.elapsed().as_millis() as u64,
                    detail = status.message(),
                    "peer rpc rejected"
                )
            });
            status
        };

        let env = request.get_ref();
        if env.payload_encoding != PAYLOAD_ENCODING_JSON {
            return Err(reject(
                "bad_payload_encoding",
                Status::invalid_argument(format!(
                    "unsupported payload_encoding {}; this release speaks {PAYLOAD_ENCODING_JSON}",
                    env.payload_encoding
                )),
            ));
        }
        let cluster_id: ClusterId = match env.cluster_id.parse() {
            Ok(id) => id,
            Err(e) => {
                return Err(reject(
                    "bad_cluster_id",
                    Status::invalid_argument(format!("{e}")),
                ))
            }
        };
        let from = NodeId(env.from_node_id);
        let to = NodeId(env.to_node_id);

        if let Err(status) = self.check_transport_identity(&request, cluster_id, from) {
            return Err(reject("identity_mismatch", status));
        }

        let env = request.into_inner();
        let req: PeerRequest = match serde_json::from_slice(&env.payload) {
            Ok(req) => req,
            Err(e) => {
                return Err(reject(
                    "bad_payload_encoding",
                    Status::invalid_argument(format!("undecodable peer payload: {e}")),
                ))
            }
        };
        if req.kind() != expected {
            return Err(reject(
                "bad_payload_encoding",
                Status::invalid_argument(format!(
                    "payload is a {} request but arrived on the {expected} rpc",
                    req.kind()
                )),
            ));
        }

        let meta = PeerEnvelopeMeta {
            cluster_id,
            recovery_epoch: RecoveryEpoch(env.recovery_epoch),
            from,
            to,
            trace,
        };

        let result = self
            .handler
            .handle(meta, req)
            .instrument(span.clone())
            .await;

        span.in_scope(|| match &result {
            Ok(resp) => {
                tracing::debug!(rpc = expected, %from, %to, response = resp.kind(), "peer rpc")
            }
            Err(reject) => {
                tracing::warn!(rpc = expected, %from, %to, reason = %reject, "peer rpc rejected")
            }
        });

        let response = result.map_err(|r| status_from_reject(&r))?;
        let payload = serde_json::to_vec(&response)
            .map_err(|e| Status::internal(format!("peer response encode failed: {e}")))?;

        Ok(Response::new(pb::PeerEnvelope {
            // Our identity, not the caller's: see [`PeerIdentity`]. The destination is the
            // only field taken from the request, because that is the one fact the answer is
            // about — who asked.
            cluster_id: self.identity.cluster_id.to_string(),
            recovery_epoch: self.identity.recovery_epoch.0,
            from_node_id: self.identity.node_id.0,
            to_node_id: from.0,
            payload_encoding: PAYLOAD_ENCODING_JSON,
            payload: payload.into(),
        }))
    }
}

#[tonic::async_trait]
impl PeerService for PeerSvc {
    async fn append_entries(
        &self,
        request: Request<pb::PeerEnvelope>,
    ) -> Result<Response<pb::PeerEnvelope>, Status> {
        self.call("append_entries", request).await
    }

    async fn vote(
        &self,
        request: Request<pb::PeerEnvelope>,
    ) -> Result<Response<pb::PeerEnvelope>, Status> {
        self.call("vote", request).await
    }

    /// The OpenRaft network trait requires this RPC; this release never triggers a snapshot
    /// (ADR-0008), so the server answers `UNIMPLEMENTED` without decoding the payload — the
    /// answer documented in `peer.proto`.
    async fn install_snapshot(
        &self,
        _request: Request<pb::PeerEnvelope>,
    ) -> Result<Response<pb::PeerEnvelope>, Status> {
        Err(Status::unimplemented(
            "InstallSnapshot is not served in this release (ADR-0008)",
        ))
    }
}

/// Serve `PeerService` on an already-bound listener.
///
/// `identity` is stamped onto every response envelope so a caller can verify that the node it
/// addressed is the node that answered (ADR-0011).
pub fn serve_peer_plane(
    handler: PeerHandler,
    listener: TcpListener,
    tls: TlsMode,
    identity: PeerIdentity,
) -> Result<ServerHandle, GrpcError> {
    let svc = PeerSvc {
        handler,
        tls: tls.clone(),
        identity,
        server_span: tracing::Span::current(),
    };
    let router = tls
        .apply_server(tonic::transport::Server::builder())?
        .add_service(PeerServiceServer::new(svc));
    spawn("peer", router, listener)
}

/// Decode a [`PeerResponse`] out of an answering envelope (used by the transport client).
pub(crate) fn decode_response(env: &pb::PeerEnvelope) -> Result<PeerResponse, String> {
    if env.payload_encoding != PAYLOAD_ENCODING_JSON {
        return Err(format!(
            "peer answered with payload_encoding {}",
            env.payload_encoding
        ));
    }
    serde_json::from_slice(&env.payload).map_err(|e| format!("undecodable peer response: {e}"))
}
