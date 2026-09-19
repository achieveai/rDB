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
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use config_core::{Limits, NodeId};
use config_engine::netfault::NetFault;
use config_engine::transport::{
    PeerEnvelopeMeta, PeerRequest, PeerResponse, PeerTransport, TransportError,
    PAYLOAD_ENCODING_POSTCARD,
};
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Status};

use crate::limits::peer_plane_message_limit;
use crate::pb;
use crate::pb::peer_service_client::PeerServiceClient;
use crate::peer_plane::decode_response;
use crate::tls::{peer_server_domain, TlsMode};

/// One cached channel per address *and* per node identity verified at that address.
type ChannelKey = (String, NodeId);

/// tonic-backed peer transport with per-target lazy channels and fault injection.
pub struct GrpcPeerTransport {
    tls: TlsMode,
    faults: NetFault,
    channels: Mutex<HashMap<ChannelKey, Channel>>,
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
            .field(
                "endpoints",
                &self.channels.lock().map(|c| c.len()).unwrap_or(0),
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
        Arc::new(Self {
            tls,
            faults,
            channels: Mutex::new(HashMap::new()),
            message_limit: peer_plane_message_limit(&limits),
        })
    }

    /// How many `(endpoint, target node)` pairs currently have a cached channel (diagnostics
    /// and tests).
    pub fn cached_endpoints(&self) -> usize {
        self.channels.lock().expect("channel cache poisoned").len()
    }

    /// The channel for `endpoint`, verified against the node `meta` addresses.
    fn channel(&self, meta: &PeerEnvelopeMeta, endpoint: &str) -> Result<Channel, TransportError> {
        let key: ChannelKey = (endpoint.to_string(), meta.to);
        if let Some(channel) = self
            .channels
            .lock()
            .expect("channel cache poisoned")
            .get(&key)
        {
            return Ok(channel.clone());
        }

        let uri = format!("{}://{endpoint}", self.tls.scheme());
        let mut ep = Endpoint::from_shared(uri.clone()).map_err(|e| {
            TransportError::Unreachable(format!("invalid peer endpoint {endpoint:?}: {e}"))
        })?;
        if let TlsMode::MutualTls(cfg) = &self.tls {
            // The envelope names the node we mean to reach, so that is the name TLS verifies.
            let domain = peer_server_domain(&meta.cluster_id, meta.to);
            ep = ep
                .tls_config(cfg.client_tls_config_for(&domain))
                .map_err(|e| {
                    TransportError::IdentityRejected(format!("client tls config rejected: {e}"))
                })?;
        }
        let channel = ep.connect_lazy();
        self.channels
            .lock()
            .expect("channel cache poisoned")
            .insert(key, channel.clone());
        Ok(channel)
    }

    async fn send_inner(
        &self,
        meta: &PeerEnvelopeMeta,
        endpoint: &str,
        req: PeerRequest,
    ) -> Result<PeerResponse, TransportError> {
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
        decode_response(env).map_err(TransportError::Remote)
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
        let (from, to) = (meta.from, meta.to);
        // The deadline covers the guard, so an injected delay is charged to the caller's
        // budget instead of being added on top of it.
        let guarded = self
            .faults
            .guard(from, to, self.send_inner(&meta, endpoint, req));
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
