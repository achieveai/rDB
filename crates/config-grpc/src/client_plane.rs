//! The client plane: `ConfigService` over tonic (spec §6.2, §15.1).
//!
//! This layer owns exactly three things and delegates everything else:
//!
//! 1. **Identity.** The principal comes from the transport ([`crate::tls`]) and is used to
//!    pick the backing store. A request field can never influence it (ADR-0012).
//! 2. **Trace continuity.** The caller's `retcd-trace-id` / `retcd-parent-span` /
//!    `retcd-request-id` become this hop's span, so one operation is greppable across
//!    processes (ADR-0013).
//! 3. **Status mapping.** [`crate::error::status_from_error`] is the only translation, and a
//!    `CONFLICT` / `NOT_FOUND` mutation outcome is never translated at all — it is an `OK`
//!    response carrying [`pb::MutationResponse`] (spec §7.3).
//!
//! The service never touches consensus itself; it calls an `Arc<dyn ConfigStore>` supplied by
//! a [`ClientBackend`]. That is what lets the transport be tested against a scripted store,
//! and what lets `config-server` hand it a `DirectClient` over a real `ConfigNode`.

use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use config_core::{ClusterId, ConfigError, ConfigStore, Principal};
use config_log::TraceContext;
use tokio::net::TcpListener;
use tonic::{Request, Response, Status};
use tracing::Instrument;

use crate::error::{mark_rejected, status_from_error, GrpcError};
use crate::pb;
use crate::pb::config_service_server::{ConfigService, ConfigServiceServer};
use crate::server::{spawn, ServerHandle};
use crate::tls::{principal_from_certs, TlsMode};

/// Supplies the store a given authenticated principal should be served by.
///
/// The one required method exists because the only per-connection decision this plane makes
/// is *whose* store to use. An implementation over a `ConfigNode` typically returns a
/// `DirectClient` bound to the principal; a test implementation returns a scripted store and
/// ignores the principal.
pub trait ClientBackend: Send + Sync {
    /// The store that serves `principal`.
    ///
    /// Called once per request, so it must be cheap — typically an `Arc` clone or a small
    /// wrapper construction, never I/O.
    fn store_for(&self, principal: Principal) -> Arc<dyn ConfigStore>;

    /// Called when a caller's transport identity could not be established (M3-81).
    ///
    /// This plane is the only place that sees a certificate, and the engine is the only place
    /// that keeps counters, so the fact has to cross the boundary here. An implementation over
    /// a `ConfigNode` forwards to `ConfigNode::record_authn_rejection`.
    ///
    /// It is *not* an authorization denial: no principal was derived, so no `Authorizer` was
    /// consulted and no audit line was written. The default does nothing, which is right for a
    /// backend with nothing to count.
    fn record_authn_rejection(&self) {}
}

impl<F> ClientBackend for F
where
    F: Fn(Principal) -> Arc<dyn ConfigStore> + Send + Sync,
{
    fn store_for(&self, principal: Principal) -> Arc<dyn ConfigStore> {
        self(principal)
    }
}

struct ConfigSvc {
    backend: Arc<dyn ClientBackend>,
    tls: TlsMode,
    /// The cluster this listener serves; a client certificate minted for another one is
    /// refused even when the CA is shared (ADR-0011).
    cluster_id: ClusterId,
    /// The span in effect when the plane was started — the node span in production, the test
    /// span under `#[retcd_test]`.
    ///
    /// hyper serves each connection on its own `tokio::spawn`ed task, and `tokio::spawn` does
    /// not carry the caller's span. Without re-parenting, every RPC line would be an orphan
    /// with no `node_id` and no `testMethod`, which is exactly the correlation ADR-0013 is
    /// for.
    server_span: tracing::Span,
}

impl ConfigSvc {
    /// Derive the caller's principal from the transport, never from the message.
    fn principal<T>(&self, request: &Request<T>) -> Result<Principal, Status> {
        match self.tls {
            TlsMode::Insecure => Ok(Principal::development()),
            TlsMode::MutualTls(_) => match request.peer_certs() {
                Some(certs) if !certs.is_empty() => {
                    let der: Vec<&[u8]> = certs.iter().map(|c| c.as_ref()).collect();
                    principal_from_certs(&der, self.cluster_id)
                }
                _ => Err(Status::unauthenticated(
                    "mutual TLS is required on this listener but no client certificate was presented",
                )),
            },
        }
    }

    /// Everything every RPC does: identity, trace span, the call, and one info log line.
    async fn dispatch<Req, Res, Call, Fut>(
        &self,
        op: &'static str,
        request: Request<Req>,
        call: Call,
    ) -> Result<Response<Res>, Status>
    where
        Call: FnOnce(Arc<dyn ConfigStore>, Req) -> Fut,
        Fut: Future<Output = Result<Res, ConfigError>>,
    {
        let started = Instant::now();

        let meta = request.metadata();
        let header = |k: &str| meta.get(k).and_then(|v| v.to_str().ok());
        let ctx = TraceContext::from_headers(
            header(config_log::HEADER_TRACE_ID),
            header(config_log::HEADER_PARENT_SPAN),
            header(config_log::HEADER_REQUEST_ID),
        );
        // Built before the identity check, so a refusal is logged inside the caller's trace
        // rather than as an orphan line nothing can be correlated with (ADR-0013).
        let span = self.server_span.in_scope(|| ctx.span(op));

        let principal = match self.principal(&request) {
            Ok(principal) => principal,
            Err(status) => {
                self.backend.record_authn_rejection();
                span.in_scope(|| {
                    tracing::warn!(
                        rpc = op,
                        reason = "unauthenticated",
                        latency_ms = started.elapsed().as_millis() as u64,
                        detail = status.message(),
                        "rpc rejected"
                    )
                });
                return Err(mark_rejected(status));
            }
        };

        let store = self.backend.store_for(principal.clone());
        let result = call(store, request.into_inner())
            .instrument(span.clone())
            .await;

        let latency_ms = started.elapsed().as_millis() as u64;
        let outcome = result.map_err(|e| status_from_error(&e));
        span.in_scope(|| match &outcome {
            Ok(_) => tracing::info!(
                rpc = op,
                principal = %principal.name,
                status = "ok",
                latency_ms,
                "rpc"
            ),
            Err(status) => tracing::info!(
                rpc = op,
                principal = %principal.name,
                status = %status.code(),
                latency_ms,
                "rpc"
            ),
        });
        outcome.map(Response::new)
    }
}

#[tonic::async_trait]
impl ConfigService for ConfigSvc {
    async fn get(
        &self,
        request: Request<pb::GetRequest>,
    ) -> Result<Response<pb::GetResponse>, Status> {
        self.dispatch("get", request, |store, req| async move {
            store.get(req.into()).await.map(Into::into)
        })
        .await
    }

    async fn list(
        &self,
        request: Request<pb::ListRequest>,
    ) -> Result<Response<pb::ListResponse>, Status> {
        self.dispatch("list", request, |store, req| async move {
            store.list(req.into()).await.map(Into::into)
        })
        .await
    }

    async fn put(
        &self,
        request: Request<pb::PutRequest>,
    ) -> Result<Response<pb::MutationResponse>, Status> {
        self.dispatch("put", request, |store, req| async move {
            store.put(req.into()).await.map(Into::into)
        })
        .await
    }

    async fn delete(
        &self,
        request: Request<pb::DeleteRequest>,
    ) -> Result<Response<pb::MutationResponse>, Status> {
        self.dispatch("delete", request, |store, req| async move {
            store.delete(req.into()).await.map(Into::into)
        })
        .await
    }
}

/// Serve `ConfigService` on an already-bound listener.
///
/// `cluster_id` is the cluster this node belongs to; under [`TlsMode::MutualTls`] a client
/// certificate must carry a `retcd://<cluster_id>/client/<name>` SAN for *that* cluster
/// (ADR-0011). Under [`TlsMode::Insecure`] it is unused — there is no certificate to check.
///
/// Requires a current Tokio runtime; the library never creates one (spec §6.3).
pub fn serve_client_plane(
    backend: Arc<dyn ClientBackend>,
    listener: TcpListener,
    tls: TlsMode,
    cluster_id: ClusterId,
) -> Result<ServerHandle, GrpcError> {
    let svc = ConfigSvc {
        backend,
        tls: tls.clone(),
        cluster_id,
        server_span: tracing::Span::current(),
    };
    let router = tls
        .apply_server(tonic::transport::Server::builder())?
        .add_service(ConfigServiceServer::new(svc));
    spawn("client", router, listener)
}
