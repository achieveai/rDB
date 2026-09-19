//! `PeerTransport` over tonic (ADR-0010, test-plan TA-5).
//!
//! One instance per node, shared by every outgoing peer connection. Channels are created
//! lazily per target and cached: `connect_lazy` means constructing one cannot fail on a
//! peer that is currently down, so a transient outage never poisons the cache.
//!
//! # Which name a peer is verified against
//!
//! Under [`TlsMode::MutualTls`] the channel for an endpoint is pinned to
//! [`peer_server_domain`]`(envelope.cluster_id, envelope.to)` — the DNS SAN the addressed node's
//! certificate carries. A dial is therefore verified against *the node the envelope names*, not
//! against whatever happens to answer at that address, and a member's own certificate cannot
//! satisfy a dial addressed to a different member. Consequently the cache is keyed by
//! `(endpoint, to)`, and an [`MtlsConfig::server_domain`](crate::MtlsConfig::server_domain)
//! configured on the profile is ignored here: it describes one name, and this transport needs
//! one per peer.
//!
//! Every send runs inside [`NetFault::guard`], *before* a socket is touched. A blocked pair
//! therefore fails without dialing — which is what makes an in-process partition behave like
//! a cut cable rather than like a slow link — and a block applied mid-call cancels it. The
//! per-call deadline wraps the guard rather than the socket call, so an injected latency is
//! spent *inside* the caller's budget: a 200 ms delay under a 100 ms deadline must look like a
//! slow peer, not like a peer that answered on time.
//!
//! The answer's identity fields are checked before its payload is decoded: see [`PeerTransport`]
//! below and `PeerIdentity` on the serving side.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use config_core::{Limits, NodeId, SchemaTriple, CURRENT_SCHEMA};
use config_engine::netfault::NetFault;
use config_engine::transport::{
    PeerEnvelopeMeta, PeerRequest, PeerResponse, PeerTransport, TransportError,
    PAYLOAD_ENCODING_POSTCARD,
};
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Status};

use crate::credentials::Credentials;
use crate::error::GrpcError;
use crate::limits::peer_plane_message_limit;
use crate::pb;
use crate::pb::peer_service_client::PeerServiceClient;
use crate::peer_plane::{decode_response, schema_from_pb, schema_to_pb};
use crate::tls::{peer_server_domain, MtlsConfig, TlsMode};

/// One cached channel per address *and* per node identity verified at that address.
type ChannelKey = (String, NodeId);

/// The pooled channels, and the credential generation every one of them was dialled under.
///
/// The generation is held *with* the map rather than beside it so the two can never be read
/// apart: a channel authenticated under material the operator has since withdrawn must not be
/// handed out, and a cache whose generation could lag its contents would do exactly that.
struct ChannelCache {
    generation: u64,
    channels: HashMap<ChannelKey, Channel>,
}

/// tonic-backed peer transport with per-target lazy channels and fault injection.
pub struct GrpcPeerTransport {
    tls: TlsMode,
    /// The peer-dial material in force right now, and how many times it has been replaced.
    ///
    /// Deliberately *not* a [`crate::credentials::CredentialSource`]. That type compiles a
    /// rustls **server** profile eagerly, which a dialler never uses, and its constructor is
    /// fallible — which would make [`GrpcPeerTransport::new`] fallible for every caller,
    /// including the many that pass [`TlsMode::Insecure`] and cannot fail. What is shared with
    /// the listener side is the part that matters: [`GrpcPeerTransport::reload`] validates
    /// through the same compiler, so one definition decides what "serveable material" means.
    dial: Mutex<Arc<MtlsConfig>>,
    generation: AtomicU64,
    faults: NetFault,
    channels: Mutex<ChannelCache>,
    /// Codec cap applied to every peer stub, derived once from the node's [`Limits`].
    ///
    /// Held as the resolved byte count rather than as the `Limits` it came from: the
    /// derivation belongs to [`peer_plane_message_limit`], and doing it per call would only
    /// invite the two ends to drift.
    message_limit: usize,
}

impl std::fmt::Debug for GrpcPeerTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcPeerTransport")
            .field("tls", &self.tls)
            .field("generation", &self.generation())
            .field(
                "endpoints",
                &self.channels.lock().map(|c| c.channels.len()).unwrap_or(0),
            )
            .finish()
    }
}

impl GrpcPeerTransport {
    /// Build a transport. `faults` is [`NetFault::default`] in production (permanently
    /// transparent) and the harness's shared switchboard in tests.
    ///
    /// `limits` must be the caps this cluster enforces: they size the stub's codec so an
    /// `AppendEntries` the leader may legally build is one the follower answers instead of
    /// rejecting as over-size — a rejection OpenRaft would retry forever.
    pub fn new(tls: TlsMode, faults: NetFault, limits: Limits) -> Arc<Self> {
        // Under `Insecure` there is no material to dial with; the placeholder is never read,
        // because `channel` consults it only on the `MutualTls` branch.
        let dial = match &tls {
            TlsMode::Insecure => MtlsConfig::new(Vec::new(), Vec::new(), Vec::new()),
            TlsMode::MutualTls(cfg) => cfg.clone(),
        };
        Arc::new(Self {
            tls,
            dial: Mutex::new(Arc::new(dial)),
            generation: AtomicU64::new(0),
            faults,
            channels: Mutex::new(ChannelCache {
                generation: 0,
                channels: HashMap::new(),
            }),
            message_limit: peer_plane_message_limit(&limits),
        })
    }

    /// How many `(endpoint, target node)` pairs currently have a cached channel (diagnostics
    /// and tests).
    pub fn cached_endpoints(&self) -> usize {
        self.channels
            .lock()
            .expect("channel cache poisoned")
            .channels
            .len()
    }

    /// How many times the peer-dial material has been replaced since start.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Dial with `mtls` from now on, dropping every pooled channel, and return the new
    /// generation.
    ///
    /// Dropping the pool is the point. A pooled channel completed its handshake under the
    /// material in force when it was opened, so leaving it in place after a rotation would
    /// keep talking to a peer with a certificate the operator has withdrawn — for as long as
    /// the connection happened to stay up, which is indefinitely on a healthy cluster (M6-49).
    /// The channels are lazy, so dropping them costs one reconnect on next use and nothing
    /// otherwise.
    ///
    /// # Errors
    ///
    /// [`GrpcError::Tls`] if the material is not serveable, in which case nothing changes:
    /// neither the material nor the pool. The validation is the listener's, deliberately —
    /// this is the same node identity that serves the peer plane, so material this node could
    /// not serve is material it must not dial with either.
    pub fn reload(&self, mtls: MtlsConfig) -> Result<u64, GrpcError> {
        if matches!(self.tls, TlsMode::Insecure) {
            return Err(GrpcError::Tls(
                "peer transport is insecure; there is no credential to rotate".to_string(),
            ));
        }
        Credentials::compile(mtls.clone())?;

        let generation = {
            let mut dial = self.dial.lock().expect("dial credentials poisoned");
            *dial = Arc::new(mtls);
            // Inside the lock, so no dial can pair the new material with the old generation.
            self.generation.fetch_add(1, Ordering::AcqRel) + 1
        };
        tracing::info!(generation, "peer dial credentials reloaded");
        Ok(generation)
    }

    /// The channel for `endpoint`, verified against the node `meta` addresses.
    ///
    /// A reload since the pool was filled empties it first, so no call is ever served by a
    /// connection authenticated under withdrawn material.
    fn channel(&self, meta: &PeerEnvelopeMeta, endpoint: &str) -> Result<Channel, TransportError> {
        let key: ChannelKey = (endpoint.to_string(), meta.to);
        let generation = self.generation();
        {
            let mut cache = self.channels.lock().expect("channel cache poisoned");
            if cache.generation != generation {
                cache.channels.clear();
                cache.generation = generation;
            } else if let Some(channel) = cache.channels.get(&key) {
                return Ok(channel.clone());
            }
        }

        let uri = format!("{}://{endpoint}", self.tls.scheme());
        let mut ep = Endpoint::from_shared(uri.clone()).map_err(|e| {
            TransportError::Unreachable(format!("invalid peer endpoint {endpoint:?}: {e}"))
        })?;
        if matches!(self.tls, TlsMode::MutualTls(_)) {
            // Read from the live material, not from the mode: the mode holds what this node
            // started with, which a rotation has since replaced.
            let dial = Arc::clone(&self.dial.lock().expect("dial credentials poisoned"));
            // The envelope names the node we mean to reach, so that is the name TLS verifies.
            let domain = peer_server_domain(&meta.cluster_id, meta.to);
            ep = ep
                .tls_config(dial.client_tls_config_for(&domain))
                .map_err(|e| {
                    TransportError::IdentityRejected(format!("client tls config rejected: {e}"))
                })?;
        }
        let channel = ep.connect_lazy();
        let mut cache = self.channels.lock().expect("channel cache poisoned");
        // Another dial may have reloaded while this one was building its endpoint. Caching a
        // channel from a superseded generation would defeat the invalidation above, so it is
        // simply not cached — the call still proceeds on the channel it built.
        if cache.generation == self.generation() {
            cache.channels.insert(key, channel.clone());
        }
        Ok(channel)
    }

    async fn send_inner(
        &self,
        meta: &PeerEnvelopeMeta,
        endpoint: &str,
        req: PeerRequest,
        schema: SchemaTriple,
    ) -> Result<(PeerResponse, Option<SchemaTriple>), TransportError> {
        let kind = req.kind();
        let payload = postcard::to_allocvec(&req)
            .map_err(|e| TransportError::Remote(format!("peer request encode failed: {e}")))?;

        let envelope = pb::PeerEnvelope {
            cluster_id: meta.cluster_id.to_string(),
            recovery_epoch: meta.recovery_epoch.0,
            from_node_id: meta.from.0,
            to_node_id: meta.to.0,
            payload_encoding: PAYLOAD_ENCODING_POSTCARD,
            payload: payload.into(),
            schema: Some(schema_to_pb(schema)),
        };

        let mut request = tonic::Request::new(envelope);
        for (key, value) in meta.trace.to_headers() {
            if let Ok(value) = value.parse() {
                request.metadata_mut().insert(key, value);
            }
        }

        let mut client = PeerServiceClient::new(self.channel(meta, endpoint)?)
            .max_decoding_message_size(self.message_limit)
            .max_encoding_message_size(self.message_limit);
        let call = async move {
            match kind {
                "append_entries" => client.append_entries(request).await,
                "vote" => client.vote(request).await,
                _ => client.install_snapshot(request).await,
            }
        };

        let response = call.await.map_err(|status| map_status(endpoint, &status))?;

        let env = response.get_ref();
        check_response_identity(endpoint, meta, env)?;
        // Read after the identity check, never before: a triple from a node we did not address
        // is not evidence about the node we did, and feeding it to the gate would let an
        // impostor raise the cluster minimum (ADR-0030 M6-86).
        let peer_schema = schema_from_pb(env.schema.as_ref());
        decode_response(env)
            .map_err(TransportError::Remote)
            .map(|response| (response, peer_schema))
    }
}

/// Refuse an answer that does not come from the node we addressed (ADR-0011).
///
/// Checked before the payload is deserialized: a foreign responder must not get to hand
/// OpenRaft bytes to interpret, and `from`/`to` swapped is the cheapest possible proof that
/// the answer belongs to this exchange.
fn check_response_identity(
    endpoint: &str,
    meta: &PeerEnvelopeMeta,
    env: &pb::PeerEnvelope,
) -> Result<(), TransportError> {
    let expected_cluster = meta.cluster_id.to_string();
    let mismatch = if env.cluster_id != expected_cluster {
        Some(format!(
            "answer claims cluster {:?}, we addressed {expected_cluster:?}",
            env.cluster_id
        ))
    } else if env.from_node_id != meta.to.0 {
        Some(format!(
            "answer claims to come from node {}, we addressed node {}",
            env.from_node_id, meta.to
        ))
    } else if env.to_node_id != meta.from.0 {
        Some(format!(
            "answer is addressed to node {}, we are node {}",
            env.to_node_id, meta.from
        ))
    } else {
        None
    };
    match mismatch {
        None => Ok(()),
        Some(detail) => Err(TransportError::IdentityRejected(format!(
            "{endpoint}: {detail}"
        ))),
    }
}

/// Classify a peer-call status for OpenRaft's retry policy.
///
/// The distinction that matters: `Unreachable` means "do not expect this peer to answer soon,
/// back off", while `Network` means "the call itself failed, retry is reasonable".
fn map_status(endpoint: &str, status: &Status) -> TransportError {
    let detail = format!("{endpoint}: {}", status.message());
    match status.code() {
        Code::Unavailable => TransportError::Unreachable(detail),
        Code::DeadlineExceeded | Code::Cancelled | Code::Aborted | Code::Unknown => {
            TransportError::Network(detail)
        }
        Code::Unauthenticated => TransportError::IdentityRejected(detail),
        _ => TransportError::Remote(format!("{}: {detail}", status.code())),
    }
}

#[async_trait]
impl PeerTransport for GrpcPeerTransport {
    async fn send(
        &self,
        meta: PeerEnvelopeMeta,
        endpoint: &str,
        req: PeerRequest,
        deadline: Duration,
    ) -> Result<PeerResponse, TransportError> {
        self.send_with_schema(meta, endpoint, req, deadline, CURRENT_SCHEMA)
            .await
            .map(|(response, _)| response)
    }

    async fn send_with_schema(
        &self,
        meta: PeerEnvelopeMeta,
        endpoint: &str,
        req: PeerRequest,
        deadline: Duration,
        schema: SchemaTriple,
    ) -> Result<(PeerResponse, Option<SchemaTriple>), TransportError> {
        let (from, to) = (meta.from, meta.to);
        // The deadline covers the guard, so an injected delay is charged to the caller's
        // budget instead of being added on top of it.
        let guarded = self
            .faults
            .guard(from, to, self.send_inner(&meta, endpoint, req, schema));
        match tokio::time::timeout(deadline, guarded).await {
            Ok(result) => result,
            Err(_) => Err(TransportError::Network(format!(
                "peer call to {endpoint} exceeded {} ms",
                deadline.as_millis()
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use config_core::{ClusterId, RecoveryEpoch};
    use config_log::TraceContext;

    use crate::testing::TlsFixture;

    /// Addressed at node 2 of a fixed cluster. Nothing is dialled: `connect_lazy` builds a
    /// channel without touching the network, which is what lets the pool be asserted without a
    /// listener. The endpoints below are `.invalid` names for the same reason — there is no
    /// socket behind them, and none is wanted.
    fn meta() -> PeerEnvelopeMeta {
        PeerEnvelopeMeta {
            cluster_id: ClusterId::from_bytes([0x11; 16]),
            recovery_epoch: RecoveryEpoch(1),
            from: NodeId(1),
            to: NodeId(2),
            trace: TraceContext::new_root(),
        }
    }

    /// A rotation must not leave a peer link authenticated under withdrawn material in the
    /// pool. Channels are lazy, so the cost of dropping them is one reconnect on next use —
    /// and the cost of *not* dropping them is talking to a peer with a certificate the
    /// operator has retired, for as long as the connection stays up (M6-49).
    #[tokio::test]
    async fn a_reload_empties_the_pool_and_bumps_the_generation() {
        let fixture = TlsFixture::new();
        let transport = GrpcPeerTransport::new(
            TlsMode::MutualTls(fixture.server_material()),
            NetFault::new(),
            Limits::DEFAULT,
        );

        transport
            .channel(&meta(), "peer-2.invalid:1")
            .expect("lazy dial");
        assert_eq!(transport.cached_endpoints(), 1);
        assert_eq!(transport.generation(), 0);

        assert_eq!(
            transport.reload(fixture.server_material()).expect("reload"),
            1
        );
        assert_eq!(transport.generation(), 1);
        assert_eq!(
            transport.cached_endpoints(),
            1,
            "the pool is emptied lazily, on the next dial, not eagerly on reload"
        );

        // The next dial is what observes the new generation: it clears what the old one left
        // and re-dials.
        transport
            .channel(&meta(), "peer-3.invalid:1")
            .expect("lazy dial");
        assert_eq!(
            transport.cached_endpoints(),
            1,
            "the pre-reload channel must be gone, not merely joined by a second one"
        );
    }

    /// A reload that cannot be served changes nothing — not the material, not the generation,
    /// not the pool. The operator is told at the reload rather than by the next peer to fail.
    #[tokio::test]
    async fn a_refused_reload_changes_nothing() {
        let fixture = TlsFixture::new();
        let transport = GrpcPeerTransport::new(
            TlsMode::MutualTls(fixture.server_material()),
            NetFault::new(),
            Limits::DEFAULT,
        );
        transport
            .channel(&meta(), "peer-2.invalid:1")
            .expect("lazy dial");

        let mut broken = fixture.server_material();
        broken.ca_pem = Vec::new();
        let error = transport
            .reload(broken)
            .expect_err("a bundle with no trust anchor is not serveable");

        assert!(
            matches!(error, GrpcError::Tls(_)),
            "expected a typed TLS error, got {error:?}"
        );
        assert_eq!(transport.generation(), 0);
        assert_eq!(transport.cached_endpoints(), 1);
    }

    /// There is nothing to rotate on a listener that never authenticated anyone. Saying so is
    /// better than reporting a successful rotation that changed nothing.
    #[tokio::test]
    async fn reloading_an_insecure_transport_is_refused() {
        let transport = GrpcPeerTransport::new(TlsMode::Insecure, NetFault::new(), Limits::DEFAULT);
        let fixture = TlsFixture::new();

        let error = transport
            .reload(fixture.server_material())
            .expect_err("an insecure transport has no credential");
        assert!(
            matches!(error, GrpcError::Tls(ref detail) if detail.contains("insecure")),
            "expected the insecure refusal, got {error:?}"
        );
    }

    /// The classification table OpenRaft's retry policy reads, asserted without a socket.
    #[test]
    fn status_codes_map_to_the_right_transport_error() {
        let cases = [
            (Code::Unavailable, "unreachable"),
            (Code::DeadlineExceeded, "network"),
            (Code::Cancelled, "network"),
            (Code::Aborted, "network"),
            (Code::Unknown, "network"),
            (Code::Unauthenticated, "identity"),
            (Code::Internal, "remote"),
            (Code::InvalidArgument, "remote"),
            (Code::Unimplemented, "remote"),
            (Code::PermissionDenied, "remote"),
        ];
        for (code, expected) in cases {
            let got = match map_status("peer:1", &Status::new(code, "boom")) {
                TransportError::Unreachable(_) => "unreachable",
                TransportError::Network(_) => "network",
                TransportError::IdentityRejected(_) => "identity",
                TransportError::Remote(_) => "remote",
            };
            assert_eq!(got, expected, "classification for {code:?}");
        }
    }
}
