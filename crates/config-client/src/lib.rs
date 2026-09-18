//! rEtcd remote client (spec §6.1, ADR-0009, ADR-0015).
//!
//! [`GrpcClient`] implements [`config_core::ConfigStore`] over the client plane, so the
//! conformance suite that runs against an embedded store runs unchanged against a remote one.
//! The crate exists separately from `config-grpc` because a client-only embedder should not
//! have to link a server.
//!
//! # The two behaviours that are easy to get wrong
//!
//! **Leader hints are followed, but boundedly — and only to a node that proves who it is.**
//! A `FAILED_PRECONDITION` carrying `retcd-leader-node-id` / `retcd-leader-endpoint` is a
//! pointer, not an instruction: the client re-sends only to an endpoint it was already
//! configured with, at most [`GrpcClientOptions::max_hint_follows`] times, reusing the same
//! `request_id` so the whole sequence is one operation in the log (ADR-0009). An unbounded
//! chase would turn a flapping election into a request storm. Under
//! [`TlsMode::MutualTls`] the hinted *node id* is verified too: see
//! [`GrpcClient::with_cluster_id`].
//!
//! **An unknown outcome is never replayed.** A `put` or `delete` that ends without a typed
//! answer from a server — its deadline expired, or the connection dropped after the request
//! was written — surfaces as [`config_core::ConfigError::DeadlineExceededUnknownOutcome`] and
//! the client stops there. There is no request deduplication in this release, so an automatic
//! retry could apply the mutation twice (ADR-0015). [`ClientStats::sends`] exists to make that
//! provable: a test asserts the client sent exactly once.
//!
//! The distinction the second rule rests on is *who produced the error*. A server stamps every
//! status it generates with `retcd-outcome` ([`config_grpc::HEADER_OUTCOME`]); an error status
//! arriving without it came from the transport and cannot say whether a mutation was applied.
//! A gRPC code alone is not enough: a reset stream and a node's own "I am overloaded" both
//! arrive as `UNAVAILABLE`, and treating the first as resubmittable is how a duplicate write
//! happens.
//!
//! **A failure to connect is not an unknown outcome.** The rule above applies to a request
//! that reached a socket. Getting a usable connection is a separate, earlier phase: the client
//! establishes each channel with an explicit [`Endpoint::connect`], bounded by whatever is
//! left of the request budget, and a refusal there — no route, connection refused, TLS
//! handshake rejected, wrong certificate — happened before a single request byte was written.
//! It is [`config_core::ConfigError::Unavailable`] for mutations exactly as for reads, and the
//! caller may resubmit freely. Connect failures are retried across the remaining budget, at
//! most [`GrpcClientOptions::max_hint_follows`] times, because nothing has been submitted yet
//! and there is nothing to duplicate (ADR-0015, note of 2026-09-18).
//!
//! Under [`TlsMode::MutualTls`] the connect phase does not end at `connect()`. TLS 1.3 lets
//! the client finish its half of the handshake before the server has looked at the client
//! certificate, so a server that *refuses* that certificate still yields `Ok` from
//! `connect()` and fails the first RPC instead — as an unmarked status, which the mutation
//! table would read as an unknown outcome for a request no store ever saw. So a fresh mutual-
//! TLS channel is proved first, by calling a method that does not exist: tonic's router
//! answers `UNIMPLEMENTED` without entering a handler, which proves the server accepted the
//! handshake while touching no store, no authorizer and no audit trail. Anything else means
//! the channel is unusable, and that is [`ConfigError::Unavailable`], not an unknown outcome.
//! One round trip per channel, never per request.
#![deny(missing_docs)]
#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use config_core::{
    Authz, Capabilities, ClusterId, ConfigError, ConfigStore, Dedup, DeleteRequest, Durability,
    GetRequest, GetResponse, ListRequest, ListResponse, MutationResponse, NodeId, Pagination,
    PutRequest, WatchResumption,
};
use config_grpc::pb::config_service_client::ConfigServiceClient;
use config_grpc::{error_from_status, is_server_rejection, pb, peer_server_domain};
use config_log::TraceContext;
use tonic::transport::{Channel, Endpoint};
use tonic::Status;
use tracing::Instrument;

/// Re-exported so a client-only embedder can configure transport security without depending on
/// `config-grpc` directly.
pub use config_grpc::{MtlsConfig, TlsMode};

/// The gRPC path [`GrpcClient::probe`] calls to prove a fresh mutual-TLS channel is real.
///
/// It names a method that deliberately does not exist, so tonic's router answers it without
/// entering any service handler. The service name is the real one only so the request looks
/// unremarkable in a server-side access log.
const PROBE_PATH: &str = "/retcd.v1.ConfigService/ConnectProbe";

/// One cached connection per address *and* per node identity it was verified as.
///
/// `None` is the ordinary dial: the server is verified against the endpoint's own host (or
/// against [`MtlsConfig::server_domain`] when the embedder set one). `Some(node)` is a
/// hint dial pinned to that node's certificate name — a different connection to the same
/// address, deliberately, because the two verify different things.
type ChannelKey = (String, Option<NodeId>);

/// Why a [`GrpcClient`] could not be constructed.
///
/// Note what is *not* here: "could not connect". Construction validates endpoint syntax and
/// the TLS profile and dials nothing, so a peer that happens to be down at construction time
/// is not a configuration error — it is a [`ConfigError::Unavailable`] on the first request
/// that needs it.
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
    /// The **total** budget for one operation, hint follows included.
    ///
    /// Not a per-attempt deadline: with `max_hint_follows = 3` a per-attempt budget would let
    /// a single `put` block for four times as long as its caller asked for, and a caller that
    /// set five seconds because five seconds is what it has cannot act on twenty.
    ///
    /// Each attempt is bounded by whatever is left: the remaining budget is sent as the gRPC
    /// `grpc-timeout` header (so the server abandons the work too, rather than finishing a
    /// call nobody is waiting for) and enforced locally as well. When the budget runs out
    /// before a hint can be followed, the error from the last attempt is returned.
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
    ///
    /// Deliberately *not* incremented for an attempt that never got a connection: this is the
    /// counter a caller reasons with when it asks whether a mutation could have been applied,
    /// and counting a refused handshake here would make `sends == 1` mean nothing.
    pub sends: u64,
    /// How many of those were hint follows.
    pub hint_follows: u64,
    /// Attempts that ended in the connect phase and were retried.
    ///
    /// Bounded by [`GrpcClientOptions::max_hint_follows`] per operation, and the evidence that
    /// "reconnect on `Unavailable` before submission" stays bounded (ADR-0015, M3-64). Nothing
    /// was submitted on any of these, so none of them appears in `sends`.
    pub reconnects: u64,
}

#[derive(Debug, Default)]
struct Counters {
    sends: AtomicU64,
    hint_follows: AtomicU64,
    reconnects: AtomicU64,
}

/// A remote [`ConfigStore`] over the rEtcd client plane.
#[derive(Debug, Clone)]
pub struct GrpcClient {
    endpoints: Vec<String>,
    pinned: String,
    cluster_id: Option<ClusterId>,
    channels: Arc<Mutex<HashMap<ChannelKey, Channel>>>,
    opts: GrpcClientOptions,
    capabilities: Capabilities,
    counters: Arc<Counters>,
    /// `hint_identity_unverified` is a configuration mistake, not an event: one line per
    /// client says it, and a per-request line would bury the log it is meant to warn in.
    warned_unverified_hint: Arc<AtomicBool>,
}

impl GrpcClient {
    /// Build a client over `endpoints` (each `host:port`).
    ///
    /// This does not dial. Each endpoint and the TLS profile are validated here — a bad
    /// authority or an unusable certificate is a configuration error and should not wait for
    /// the first request — but the connection itself is established on first use, so a
    /// cluster that is still starting does not make construction fail. Requests are pinned to
    /// the first endpoint until a leader hint moves them.
    pub fn connect(
        endpoints: Vec<String>,
        opts: GrpcClientOptions,
    ) -> Result<GrpcClient, ClientError> {
        if endpoints.is_empty() {
            return Err(ClientError::NoEndpoints);
        }
        for endpoint in &endpoints {
            // Built and dropped: the point is the validation, not the value.
            build_endpoint(endpoint, &opts.tls, None)?;
        }
        let capabilities = opts
            .expected_capabilities
            .unwrap_or_else(|| conservative_capabilities(&opts.tls));

        Ok(Self {
            pinned: endpoints[0].clone(),
            endpoints,
            cluster_id: None,
            channels: Arc::new(Mutex::new(HashMap::new())),
            opts,
            capabilities,
            counters: Arc::new(Counters::default()),
            warned_unverified_hint: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Tell the client which cluster it is talking to, which is what makes a leader hint
    /// *authenticated* (OQ-21, ADR-0010).
    ///
    /// A hint names a node id and an endpoint. Under [`TlsMode::MutualTls`] the endpoint is
    /// already restricted to the configured set, but that only proves the operator trusts the
    /// address — not that the node answering there is the node the hint named. With a cluster
    /// id the client can say so in TLS terms: the hint dial is pinned to
    /// [`config_grpc::peer_server_domain`]`(cluster_id, hinted_node_id)`, so a member holding
    /// a perfectly valid certificate for *itself* cannot accept a mutation redirected to a
    /// different member. The handshake fails and nothing is sent.
    ///
    /// Without it, under mutual TLS, hints are **not followed at all** and one
    /// `hint_identity_unverified` warning is logged per client. Fail-closed is the only safe
    /// reading: a client that has been given certificates plainly cares who it talks to, and
    /// following an unverifiable redirect would quietly undo that. Under
    /// [`TlsMode::Insecure`] nothing is verifiable anyway, so hints behave as before.
    pub fn with_cluster_id(mut self, cluster_id: ClusterId) -> Self {
        self.cluster_id = Some(cluster_id);
        self
    }

    /// The cluster this client verifies leader hints against, if it was given one.
    pub fn cluster_id(&self) -> Option<ClusterId> {
        self.cluster_id
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

    /// Send, hint-follow and reconnect counters since construction.
    pub fn stats(&self) -> ClientStats {
        ClientStats {
            sends: self.counters.sends.load(Ordering::Relaxed),
            hint_follows: self.counters.hint_follows.load(Ordering::Relaxed),
            reconnects: self.counters.reconnects.load(Ordering::Relaxed),
        }
    }

    /// A connected channel for `endpoint`, verified as `verify_as` when that is set.
    ///
    /// The connect is explicit and bounded by `budget`, so a refusal is observed *here*, in a
    /// phase where nothing has been submitted, instead of surfacing later as an anonymous
    /// transport status that a mutation has to treat as an unknown outcome (ADR-0015).
    ///
    /// Under [`TlsMode::MutualTls`] a successful `connect()` is not enough to call the channel
    /// established, so the connect phase ends with [`GrpcClient::probe`]. See that method for
    /// why.
    async fn channel(
        &self,
        endpoint: &str,
        verify_as: Option<NodeId>,
        budget: Duration,
    ) -> Result<Channel, ConfigError> {
        let key: ChannelKey = (endpoint.to_string(), verify_as);
        if let Some(channel) = self.cache().get(&key) {
            return Ok(channel.clone());
        }
        let started = Instant::now();

        let domain = verify_as.map(|node| {
            let cluster = self
                .cluster_id
                .expect("a pinned hint dial is only built when a cluster id is configured");
            peer_server_domain(&cluster, node)
        });
        let ep = build_endpoint(endpoint, &self.opts.tls, domain.as_deref()).map_err(|e| {
            ConfigError::Unavailable {
                reason: e.to_string(),
            }
        })?;

        let channel = match tokio::time::timeout(budget, ep.connect()).await {
            Ok(Ok(channel)) => channel,
            Ok(Err(e)) => {
                return Err(ConfigError::Unavailable {
                    reason: format!("cannot connect to {endpoint}{}: {e}", verified_as(&domain)),
                })
            }
            Err(_) => {
                return Err(ConfigError::Unavailable {
                    reason: format!(
                        "connecting to {endpoint}{} did not finish within the remaining {} ms \
                         of the request budget",
                        verified_as(&domain),
                        budget.as_millis()
                    ),
                })
            }
        };
        if matches!(self.opts.tls, TlsMode::MutualTls(_)) {
            self.probe(
                &channel,
                endpoint,
                &domain,
                budget.saturating_sub(started.elapsed()),
            )
            .await?;
        }
        self.cache().insert(key, channel.clone());
        Ok(channel)
    }

    /// Prove a freshly connected mutual-TLS channel is actually usable, before it counts as
    /// established.
    ///
    /// `Endpoint::connect().await` returning `Ok` does **not** mean the server accepted us.
    /// Under TLS 1.3 the client finishes its half of the handshake and considers the
    /// connection open before the server has even looked at the client certificate; a server
    /// that then refuses it sends an alert that arrives later, as the first RPC dying with an
    /// unmarked `CANCELLED`. That status reaches [`classify`] on an "established" channel, so
    /// a `put` was reported as [`ConfigError::DeadlineExceededUnknownOutcome`] although no
    /// request had reached any store — the caller was sent to run ADR-0015's read-back-then-CAS
    /// recovery for a mutation that provably never happened.
    ///
    /// The probe makes the refusal observable while it is still the connect phase, and it asks
    /// exactly one question: *did this server's gRPC layer answer us at all?* It can only do
    /// that once the server has accepted the handshake, so an answer — any answer — settles
    /// the question the connect phase left open.
    ///
    /// It asks by calling a **method that does not exist**,
    /// `/retcd.v1.ConfigService/ConnectProbe`. tonic 0.12 registers a service on a wildcard
    /// route (`/<service>/*rest`), so the path is routed *into* the generated
    /// `ConfigServiceServer`, whose catch-all match arm answers `UNIMPLEMENTED` before any
    /// handler runs. That holds only while the server adds the plain service with no
    /// interceptor or tower layer in front of it, which is how `config_grpc::client_plane`
    /// builds it today; an interceptor would see the probe. So the call stops short of every
    /// handler:
    /// no principal is derived, no `Authorizer` is consulted, no audit line is written, and no
    /// `ConfigStore` is touched. That last property is the reason for the choice. A probe that
    /// called a real method would be a request like any other to a backend that does not
    /// validate ahead of its store — a test double, a future embedder — and would show up in
    /// its call count as a phantom `Get` nobody made.
    ///
    /// So: `UNIMPLEMENTED` (or, absurdly, a response) means the channel is established.
    /// Anything else — a status the transport minted while the server's alert was arriving,
    /// or silence until the budget runs out — means the channel is unusable and is reported as
    /// [`ConfigError::Unavailable`]. The refused client certificate lands there, which is the
    /// point of the exercise.
    ///
    /// Note what the probe deliberately does *not* decide. A certificate minted for another
    /// cluster passes it, because the cluster check lives in the service handler the probe
    /// never reaches; the caller's real request then comes back as a marked `UNAUTHENTICATED`
    /// on an established channel, which is both accurate and exactly what [`classify`] is for.
    /// The probe's job is to tell a connection apart from a rejection, not to pre-judge
    /// requests.
    ///
    /// Nothing here is counted in [`ClientStats::sends`], and a channel is cached only once
    /// established. The cost is one round trip per **fresh** channel, never per request. The
    /// probe also carries no trace headers: it is not part of the caller's operation, and
    /// stamping it with that `request_id` would put a synthetic call into the log query for
    /// every `put` that had to dial.
    async fn probe(
        &self,
        channel: &Channel,
        endpoint: &str,
        domain: &Option<String>,
        budget: Duration,
    ) -> Result<(), ConfigError> {
        let unusable = |detail: String| ConfigError::Unavailable {
            reason: format!(
                "connected to {endpoint}{} but the channel is not usable: {detail}",
                verified_as(domain)
            ),
        };

        let mut grpc = tonic::client::Grpc::new(channel.clone());
        // `ready()` is a bare `poll_fn` in tonic; on a fresh, exclusively owned channel it
        // cannot block, but the probe exists to bound every await of the connect phase, so
        // this one is bounded too.
        match tokio::time::timeout(budget, grpc.ready()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                return Err(unusable(format!("the channel never became ready: {error}")));
            }
            Err(_) => {
                return Err(unusable(format!(
                    "the channel did not become ready within the remaining {} ms of the request budget",
                    budget.as_millis()
                )));
            }
        }
        let mut wire = tonic::Request::new(pb::GetRequest::default());
        wire.set_timeout(budget);
        let path = tonic::codegen::http::uri::PathAndQuery::from_static(PROBE_PATH);
        let codec: tonic::codec::ProstCodec<pb::GetRequest, pb::GetResponse> =
            tonic::codec::ProstCodec::default();

        match tokio::time::timeout(budget, grpc.unary(wire, path, codec)).await {
            Ok(Err(status)) if status.code() == tonic::Code::Unimplemented => Ok(()),
            Ok(Ok(_)) => Ok(()),
            Ok(Err(status)) => Err(unusable(format!(
                "the probe was answered with {}: {}",
                status.code(),
                status.message()
            ))),
            Err(_) => Err(unusable(format!(
                "the probe went unanswered for the remaining {} ms of the request budget",
                budget.as_millis()
            ))),
        }
    }

    /// Drop a cached connection whose peer produced a transport-level failure.
    ///
    /// The next operation then goes through the connect phase again, where a peer that is
    /// simply gone is reported honestly as [`ConfigError::Unavailable`] instead of as another
    /// anonymous mid-request error.
    fn forget_channel(&self, endpoint: &str, verify_as: Option<NodeId>) {
        self.cache().remove(&(endpoint.to_string(), verify_as));
    }

    fn cache(&self) -> std::sync::MutexGuard<'_, HashMap<ChannelKey, Channel>> {
        self.channels.lock().expect("channel cache poisoned")
    }

    /// Whether a hint to `node_id` may be followed, and as whom the target must authenticate.
    ///
    /// `None` refuses the follow; `Some(verify_as)` allows it.
    fn hint_dial(&self, node_id: NodeId) -> Option<Option<NodeId>> {
        match (&self.opts.tls, self.cluster_id) {
            // Nothing to verify against, and nothing claimed: the M1 behaviour, unchanged.
            (TlsMode::Insecure, _) => Some(None),
            (TlsMode::MutualTls(_), Some(_)) => Some(Some(node_id)),
            (TlsMode::MutualTls(_), None) => None,
        }
    }

    /// One warning per client for the fail-closed case, not one per request.
    fn warn_unverified_hint_once(&self) {
        if self
            .warned_unverified_hint
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            tracing::warn!(
                msg = "hint_identity_unverified",
                "leader hints are not followed under mutual TLS without a cluster id; call \
                 GrpcClient::with_cluster_id so the hinted node's certificate can be checked \
                 (OQ-21)"
            );
        }
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
        // `.instrument`, never `span.enter()`: this future yields at every await, and a guard
        // held across a yield attributes whatever the runtime schedules next to this
        // operation's span (ADR-0013).
        self.attempts(op, is_mutation, request, call, &ctx)
            .instrument(span)
            .await
    }

    async fn attempts<Req, Res, Call, Fut>(
        &self,
        op: &'static str,
        is_mutation: bool,
        request: Req,
        call: Call,
        ctx: &TraceContext,
    ) -> Result<Res, ConfigError>
    where
        Req: Clone,
        Call: Fn(ConfigServiceClient<Channel>, tonic::Request<Req>) -> Fut,
        Fut: Future<Output = Result<tonic::Response<Res>, Status>>,
    {
        let started = Instant::now();
        let budget = self.opts.request_deadline;
        let left = || budget.saturating_sub(started.elapsed());
        let exhausted = |last: Option<ConfigError>| {
            last.unwrap_or(ConfigError::Unavailable {
                reason: "request deadline budget was exhausted before any attempt".into(),
            })
        };

        let mut endpoint = self.pinned.clone();
        let mut verify_as: Option<NodeId> = None;
        let mut follows = 0usize;
        let mut reconnects = 0usize;
        let mut attempt = 0usize;
        let mut last_error: Option<ConfigError> = None;

        loop {
            if left().is_zero() {
                // The budget covers the whole chase, so running out mid-chase returns what the
                // last node actually said rather than inventing a timeout it never caused.
                tracing::debug!(
                    op,
                    follows,
                    reconnects,
                    "request budget exhausted before the next attempt"
                );
                return Err(exhausted(last_error));
            }
            attempt += 1;

            // Phase one: get a connection. A failure here wrote nothing, so it is plainly
            // `Unavailable` — for a mutation just as much as for a read — and retrying it
            // duplicates nothing (ADR-0015, note of 2026-09-18).
            let channel = match self.channel(&endpoint, verify_as, left()).await {
                Ok(channel) => channel,
                Err(error) => {
                    tracing::debug!(%endpoint, attempt, "client connect attempt failed");
                    last_error = Some(error);
                    if reconnects < self.opts.max_hint_follows {
                        reconnects += 1;
                        self.counters.reconnects.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    return Err(exhausted(last_error));
                }
            };

            // Phase two: submit. The remaining budget is re-read, because connecting spent
            // some of it and the server must not be told it has more time than we will wait.
            let remaining = left();
            if remaining.is_zero() {
                return Err(exhausted(last_error));
            }
            let mut wire = tonic::Request::new(request.clone());
            for (key, value) in ctx.to_headers() {
                if let Ok(value) = value.parse() {
                    wire.metadata_mut().insert(key, value);
                }
            }
            // The server gets the same budget we are holding ourselves to, so it can stop
            // working on a call whose answer can no longer be delivered (ADR-0015).
            wire.set_timeout(remaining);

            let client = ConfigServiceClient::new(channel);
            // Counted here, on a connection that exists: `sends` is what a caller reasons
            // about when it asks whether a mutation could have been applied, and a connect
            // attempt that never produced a request must not answer that question yes.
            self.counters.sends.fetch_add(1, Ordering::Relaxed);
            let outcome = tokio::time::timeout(remaining, call(client, wire)).await;

            let status = match outcome {
                Ok(Ok(response)) => {
                    tracing::debug!(%endpoint, attempt, status = "ok", "client attempt");
                    return Ok(response.into_inner());
                }
                Ok(Err(status)) => status,
                Err(_) => Status::deadline_exceeded(format!(
                    "no answer from {endpoint} within the remaining {} ms of the request budget",
                    remaining.as_millis()
                )),
            };
            tracing::debug!(%endpoint, attempt, status = %status.code(), "client attempt");
            if transport_minted(&status) {
                // This connection produced something the server did not decide. Whatever it
                // was, the connection is suspect; the next operation re-runs the connect phase
                // rather than inheriting it.
                self.forget_channel(&endpoint, verify_as);
            }

            let error = classify(&status, is_mutation);
            match &error {
                // A hint is a pointer to an endpoint we already trust, followed a bounded
                // number of times. Anything else — including an unknown outcome — stops here.
                ConfigError::NotLeader { hint: Some(hint) }
                    if follows < self.opts.max_hint_follows
                        && self.endpoints.contains(&hint.endpoint) =>
                {
                    let Some(next_verify_as) = self.hint_dial(hint.node_id) else {
                        // Mutual TLS without a cluster id: the hint cannot be checked, so it
                        // is not followed (OQ-21). The caller gets the hint and can act on it
                        // with knowledge this client does not have.
                        self.warn_unverified_hint_once();
                        return Err(error);
                    };
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
                    verify_as = next_verify_as;
                    last_error = Some(error);
                }
                _ => return Err(error),
            }
        }
    }
}

/// A short suffix naming the identity a dial was pinned to, for an error message.
fn verified_as(domain: &Option<String>) -> String {
    match domain {
        Some(d) => format!(" as {d}"),
        None => String::new(),
    }
}

/// Whether this status was produced by the transport rather than decided by a server.
///
/// `DEADLINE_EXCEEDED` is excluded because our own expired budget says nothing about the
/// connection: the peer may be answering other calls perfectly well.
fn transport_minted(status: &Status) -> bool {
    !is_server_rejection(status) && status.code() != tonic::Code::DeadlineExceeded
}

/// Turn a failed attempt into a semantic error.
///
/// Three cases, and the order matters:
///
/// 1. A status a server generated (it carries `retcd-outcome`) is read with the server's own
///    table — `FAILED_PRECONDITION` is a leader hint or a CAS conflict, `INTERNAL` is a
///    storage failure, and so on.
/// 2. `DEADLINE_EXCEEDED` keeps that reading whether or not it is marked, because our own
///    expired budget means the same thing as the server's.
/// 3. Anything else unmarked came from the transport. For a mutation the request may have been
///    written to a socket and applied, so it is
///    [`ConfigError::DeadlineExceededUnknownOutcome`] and must never be replayed; for a read
///    there is no outcome to be unsure about, so it is [`ConfigError::Unavailable`].
///
/// Case 3 is narrower than it used to be. It is reached only for a status produced on a
/// connection that was already established, because failing to *get* a connection is handled
/// before any request exists: [`GrpcClient::channel`] returns
/// [`ConfigError::Unavailable`] for it, reads and mutations alike, and never arrives here
/// (ADR-0015, note of 2026-09-18). What remains in case 3 is the genuinely ambiguous shape —
/// the connection accepted our bytes and then died — where "unknown" is the truth.
///
/// The detail string is the code and message only. Neither ever contains request bytes.
fn classify(status: &Status, is_mutation: bool) -> ConfigError {
    if transport_minted(status) {
        let detail = format!("{}: {}", status.code(), status.message());
        return if is_mutation {
            tracing::warn!(
                status = %status.code(),
                "the connection failed after the mutation was submitted; outcome is unknown"
            );
            ConfigError::DeadlineExceededUnknownOutcome
        } else {
            ConfigError::Unavailable { reason: detail }
        };
    }
    match error_from_status(status) {
        // A read that timed out has no outcome to be uncertain about; conflating the two would
        // tell a caller to run the mutation-recovery procedure for a `get`.
        ConfigError::DeadlineExceededUnknownOutcome if !is_mutation => ConfigError::Unavailable {
            reason: status.message().to_string(),
        },
        other => other,
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

/// A validated, not-yet-dialled endpoint.
///
/// `domain` pins the name the server certificate must carry; `None` keeps whatever the TLS
/// profile says (the endpoint's own host, unless [`MtlsConfig::server_domain`] overrides it).
fn build_endpoint(
    endpoint: &str,
    tls: &TlsMode,
    domain: Option<&str>,
) -> Result<Endpoint, ClientError> {
    let uri = format!("{}://{endpoint}", tls.scheme());
    let mut ep = Endpoint::from_shared(uri).map_err(|e| ClientError::InvalidEndpoint {
        endpoint: endpoint.to_string(),
        detail: e.to_string(),
    })?;
    if let TlsMode::MutualTls(cfg) = tls {
        let profile = match domain {
            Some(d) => cfg.client_tls_config_for(d),
            None => cfg.client_tls_config(),
        };
        ep = ep
            .tls_config(profile)
            .map_err(|e| ClientError::Tls(e.to_string()))?;
    }
    Ok(ep)
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
