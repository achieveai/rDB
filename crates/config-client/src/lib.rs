//! rEtcd remote client (spec §6.1, ADR-0009, ADR-0015).
//!
//! [`GrpcClient`] implements [`config_core::ConfigStore`] over the client plane, so the
//! conformance suite that runs against an embedded store runs unchanged against a remote one.
//! The crate exists separately from `config-grpc` because a client-only embedder should not
//! have to link a server.
//!
//! # The two behaviours that are easy to get wrong
//!
//! **Leader hints are followed, but boundedly.** A `FAILED_PRECONDITION` carrying
//! `retcd-leader-node-id` / `retcd-leader-endpoint` is a pointer, not an instruction: the
//! client re-sends only to an endpoint it was already configured with, at most
//! [`GrpcClientOptions::max_hint_follows`] times, reusing the same `request_id` so the whole
//! sequence is one operation in the log (ADR-0009). An unbounded chase would turn a
//! flapping election into a request storm.
//!
//! **An unknown outcome is never replayed.** `DEADLINE_EXCEEDED` on a `put` or `delete`
//! surfaces as [`config_core::ConfigError::DeadlineExceededUnknownOutcome`] and the client
//! stops there. There is no request deduplication in this release, so an automatic retry
//! could apply the mutation twice (ADR-0015). [`ClientStats::sends`] exists to make that
//! provable: a test asserts the client sent exactly once.
#![deny(missing_docs)]
#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use config_core::{
    Authz, Capabilities, ConfigError, ConfigStore, Dedup, DeleteRequest, Durability, GetRequest,
    GetResponse, ListRequest, ListResponse, MutationResponse, Pagination, PutRequest,
    WatchResumption,
};
use config_grpc::pb::config_service_client::ConfigServiceClient;
use config_grpc::{error_from_status, pb, TlsMode};
use config_log::TraceContext;
use tonic::transport::{Channel, Endpoint};
use tonic::Status;

/// Why a [`GrpcClient`] could not be constructed.
///
/// Note what is *not* here: "could not connect". Channels are lazy, so a peer that happens to
/// be down at construction time is not a configuration error.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// No endpoints were supplied.
    #[error("no endpoints configured")]
    NoEndpoints,
    /// An endpoint string was not a usable authority.
    #[error("invalid endpoint {endpoint:?}: {detail}")]
    InvalidEndpoint {
        /// The rejected endpoint.
        endpoint: String,
        /// Why it was rejected.
        detail: String,
    },
    /// A pin was requested for an endpoint outside the configured set.
    #[error("endpoint {endpoint:?} is not in the configured endpoint set")]
    UnknownEndpoint {
        /// The rejected endpoint.
        endpoint: String,
    },
    /// The TLS profile was refused.
    #[error("tls configuration error: {0}")]
    Tls(String),
}

/// How a [`GrpcClient`] behaves.
#[derive(Debug, Clone)]
pub struct GrpcClientOptions {
    /// How many times a `NotLeader` hint may be followed for one operation (ADR-0009).
    pub max_hint_follows: usize,
    /// Per-attempt deadline. Each hint follow gets its own deadline.
    pub request_deadline: Duration,
    /// Transport security profile.
    pub tls: TlsMode,
    /// What [`ConfigStore::capabilities`] should report.
    ///
    /// The normative schema has no capabilities RPC (spec §6.2), so a remote client cannot
    /// discover a server's guarantees over the wire. An embedder that knows what it deployed
    /// sets this; otherwise the client reports the conservative profile described on
    /// [`GrpcClient::capabilities`].
    pub expected_capabilities: Option<Capabilities>,
}

impl Default for GrpcClientOptions {
    fn default() -> Self {
        Self {
            max_hint_follows: 3,
            request_deadline: Duration::from_secs(5),
            tls: TlsMode::Insecure,
            expected_capabilities: None,
        }
    }
}

/// Counters a test or operator can read to prove retry behaviour.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ClientStats {
    /// Requests actually put on the wire, including hint follows.
    pub sends: u64,
    /// How many of those were hint follows.
    pub hint_follows: u64,
}

#[derive(Debug, Default)]
struct Counters {
    sends: AtomicU64,
    hint_follows: AtomicU64,
}

/// A remote [`ConfigStore`] over the rEtcd client plane.
#[derive(Debug, Clone)]
pub struct GrpcClient {
    endpoints: Vec<String>,
    pinned: String,
    channels: Arc<HashMap<String, Channel>>,
    opts: GrpcClientOptions,
    capabilities: Capabilities,
    counters: Arc<Counters>,
}

impl GrpcClient {
    /// Build a client over `endpoints` (each `host:port`).
    ///
    /// Channels are lazy: this does not dial, so a cluster that is still starting does not
    /// make construction fail. Requests are pinned to the first endpoint until a leader hint
    /// moves them.
    pub fn connect(
        endpoints: Vec<String>,
        opts: GrpcClientOptions,
    ) -> Result<GrpcClient, ClientError> {
        if endpoints.is_empty() {
            return Err(ClientError::NoEndpoints);
        }
        let mut channels = HashMap::with_capacity(endpoints.len());
        for endpoint in &endpoints {
            channels.insert(endpoint.clone(), build_channel(endpoint, &opts.tls)?);
        }
        let capabilities = opts
            .expected_capabilities
            .unwrap_or_else(|| conservative_capabilities(&opts.tls));

        Ok(Self {
            pinned: endpoints[0].clone(),
            endpoints,
            channels: Arc::new(channels),
            opts,
            capabilities,
            counters: Arc::new(Counters::default()),
        })
    }

    /// Send to `endpoint` first instead of the first configured one.
    ///
    /// Used by tests that deliberately address a follower, and by an embedder that prefers a
    /// local node. `endpoint` must be one of the configured endpoints.
    pub fn pinned(mut self, endpoint: &str) -> Result<Self, ClientError> {
        if !self.endpoints.iter().any(|e| e == endpoint) {
            return Err(ClientError::UnknownEndpoint {
                endpoint: endpoint.to_string(),
            });
        }
        self.pinned = endpoint.to_string();
        Ok(self)
    }

    /// The endpoint requests start at.
    pub fn pinned_endpoint(&self) -> &str {
        &self.pinned
    }

    /// The configured endpoint set.
    pub fn endpoints(&self) -> &[String] {
        &self.endpoints
    }

    /// Send and hint-follow counters since construction.
    pub fn stats(&self) -> ClientStats {
        ClientStats {
            sends: self.counters.sends.load(Ordering::Relaxed),
            hint_follows: self.counters.hint_follows.load(Ordering::Relaxed),
        }
    }

    fn client_for(&self, endpoint: &str) -> Result<ConfigServiceClient<Channel>, ConfigError> {
        let channel = self
            .channels
            .get(endpoint)
            .ok_or_else(|| ConfigError::Unavailable {
                reason: format!("no channel for endpoint {endpoint:?}"),
            })?
            .clone();
        Ok(ConfigServiceClient::new(channel))
    }

    /// One logical operation: send, and follow a bounded number of leader hints.
    ///
    /// Every attempt reuses the same [`TraceContext`], so all of them carry one `request_id`
    /// and a log query for the operation returns the whole chase rather than its last hop.
    async fn execute<Req, Res, Call, Fut>(
        &self,
        op: &'static str,
        is_mutation: bool,
        request: Req,
        call: Call,
    ) -> Result<Res, ConfigError>
    where
        Req: Clone,
        Call: Fn(ConfigServiceClient<Channel>, tonic::Request<Req>) -> Fut,
        Fut: Future<Output = Result<tonic::Response<Res>, Status>>,
    {
        let ctx = TraceContext::current_or_root().child();
        let span = ctx.span(op);
        let _guard = span.enter();

        let mut endpoint = self.pinned.clone();
        let mut follows = 0usize;

        loop {
            let mut wire = tonic::Request::new(request.clone());
            for (key, value) in ctx.to_headers() {
                if let Ok(value) = value.parse() {
                    wire.metadata_mut().insert(key, value);
                }
            }

            self.counters.sends.fetch_add(1, Ordering::Relaxed);
            let client = self.client_for(&endpoint)?;
            let attempt = follows + 1;
            let outcome =
                tokio::time::timeout(self.opts.request_deadline, call(client, wire)).await;

            let status = match outcome {
                Ok(Ok(response)) => {
                    tracing::debug!(%endpoint, attempt, status = "ok", "client attempt");
                    return Ok(response.into_inner());
                }
                Ok(Err(status)) => status,
                Err(_) => Status::deadline_exceeded(format!(
                    "no answer from {endpoint} within {} ms",
                    self.opts.request_deadline.as_millis()
                )),
            };
            tracing::debug!(%endpoint, attempt, status = %status.code(), "client attempt");

            let error = self.classify(&status, is_mutation);
            match &error {
                // A hint is a pointer to an endpoint we already trust, followed a bounded
                // number of times. Anything else — including an unknown outcome — stops here.
                ConfigError::NotLeader { hint: Some(hint) }
                    if follows < self.opts.max_hint_follows
                        && self.endpoints.contains(&hint.endpoint) =>
                {
                    follows += 1;
                    self.counters.hint_follows.fetch_add(1, Ordering::Relaxed);
                    tracing::debug!(
                        from = %endpoint,
                        to = %hint.endpoint,
                        leader_node_id = %hint.node_id,
                        follows,
                        "following leader hint"
                    );
                    endpoint = hint.endpoint.clone();
                }
                _ => return Err(error),
            }
        }
    }

    /// Inverse of the server's status table, with one read-path adjustment.
    ///
    /// A read that timed out has no outcome to be uncertain about, so it is `Unavailable`
    /// rather than [`ConfigError::DeadlineExceededUnknownOutcome`]; conflating the two would
    /// tell a caller to run the mutation-recovery procedure for a `get`.
    fn classify(&self, status: &Status, is_mutation: bool) -> ConfigError {
        let error = error_from_status(status);
        match error {
            ConfigError::DeadlineExceededUnknownOutcome if !is_mutation => {
                ConfigError::Unavailable {
                    reason: status.message().to_string(),
                }
            }
            other => other,
        }
    }
}

/// What a client reports when the embedder did not tell it what was deployed.
///
/// Deliberately pessimistic on every axis except the one the client can actually observe —
/// its own transport security. The normative schema has no capability RPC, so a remote client
/// cannot know a server's durability; claiming anything stronger would be the exact failure
/// ADR-0016 exists to prevent.
fn conservative_capabilities(tls: &TlsMode) -> Capabilities {
    Capabilities {
        durability: Durability::Ephemeral,
        watch_resumption: WatchResumption::Unsupported,
        authz: Authz::Development,
        transport_security: tls.transport_security(),
        pagination: Pagination::Unsupported,
        dedup: Dedup::Unsupported,
    }
}

fn build_channel(endpoint: &str, tls: &TlsMode) -> Result<Channel, ClientError> {
    let uri = format!("{}://{endpoint}", tls.scheme());
    let mut ep = Endpoint::from_shared(uri).map_err(|e| ClientError::InvalidEndpoint {
        endpoint: endpoint.to_string(),
        detail: e.to_string(),
    })?;
    if let TlsMode::MutualTls(cfg) = tls {
        ep = ep
            .tls_config(cfg.client_tls_config())
            .map_err(|e| ClientError::Tls(e.to_string()))?;
    }
    Ok(ep.connect_lazy())
}

#[async_trait]
impl ConfigStore for GrpcClient {
    async fn get(&self, request: GetRequest) -> Result<GetResponse, ConfigError> {
        let response: pb::GetResponse = self
            .execute(
                "get",
                false,
                pb::GetRequest::from(request),
                |mut c, r| async move { c.get(r).await },
            )
            .await?;
        Ok(response.into())
    }

    async fn list(&self, request: ListRequest) -> Result<ListResponse, ConfigError> {
        let response: pb::ListResponse = self
            .execute(
                "list",
                false,
                pb::ListRequest::from(request),
                |mut c, r| async move { c.list(r).await },
            )
            .await?;
        Ok(response.into())
    }

    async fn put(&self, request: PutRequest) -> Result<MutationResponse, ConfigError> {
        let response: pb::MutationResponse = self
            .execute(
                "put",
                true,
                pb::PutRequest::from(request),
                |mut c, r| async move { c.put(r).await },
            )
            .await?;
        response.try_into()
    }

    async fn delete(&self, request: DeleteRequest) -> Result<MutationResponse, ConfigError> {
        let response: pb::MutationResponse = self
            .execute(
                "delete",
                true,
                pb::DeleteRequest::from(request),
                |mut c, r| async move { c.delete(r).await },
            )
            .await?;
        response.try_into()
    }

    /// See [`GrpcClientOptions::expected_capabilities`].
    ///
    /// **Limitation.** This is configuration, not discovery. Without
    /// `expected_capabilities`, only `transport_security` reflects reality; every other field
    /// is the weakest legal value. Do not read a `Durability::Ephemeral` here as evidence
    /// that the server is ephemeral.
    fn capabilities(&self) -> Capabilities {
        self.capabilities
    }
}
