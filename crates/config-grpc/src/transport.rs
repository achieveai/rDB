//! `PeerTransport` over tonic (ADR-0010, test-plan TA-5).
//!
//! One instance per node, shared by every outgoing peer connection. Channels are created
//! lazily per endpoint and cached: `connect_lazy` means constructing one cannot fail on a
//! peer that is currently down, so a transient outage never poisons the cache.
//!
//! Every send runs inside [`NetFault::guard`], *before* a socket is touched. A blocked pair
//! therefore fails without dialing — which is what makes an in-process partition behave like
//! a cut cable rather than like a slow link — and a block applied mid-call cancels it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use config_engine::netfault::NetFault;
use config_engine::transport::{
    PeerEnvelopeMeta, PeerRequest, PeerResponse, PeerTransport, TransportError,
    PAYLOAD_ENCODING_JSON,
};
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Status};

use crate::pb;
use crate::pb::peer_service_client::PeerServiceClient;
use crate::peer_plane::decode_response;
use crate::tls::TlsMode;

/// tonic-backed peer transport with per-endpoint lazy channels and fault injection.
pub struct GrpcPeerTransport {
    tls: TlsMode,
    faults: NetFault,
    channels: Mutex<HashMap<String, Channel>>,
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
    pub fn new(tls: TlsMode, faults: NetFault) -> Arc<Self> {
        Arc::new(Self {
            tls,
            faults,
            channels: Mutex::new(HashMap::new()),
        })
    }

    /// How many endpoints currently have a cached channel (diagnostics and tests).
    pub fn cached_endpoints(&self) -> usize {
        self.channels.lock().expect("channel cache poisoned").len()
    }

    fn channel(&self, endpoint: &str) -> Result<Channel, TransportError> {
        if let Some(channel) = self
            .channels
            .lock()
            .expect("channel cache poisoned")
            .get(endpoint)
        {
            return Ok(channel.clone());
        }

        let uri = format!("{}://{endpoint}", self.tls.scheme());
        let mut ep = Endpoint::from_shared(uri.clone()).map_err(|e| {
            TransportError::Unreachable(format!("invalid peer endpoint {endpoint:?}: {e}"))
        })?;
        if let TlsMode::MutualTls(cfg) = &self.tls {
            ep = ep.tls_config(cfg.client_tls_config()).map_err(|e| {
                TransportError::IdentityRejected(format!("client tls config rejected: {e}"))
            })?;
        }
        let channel = ep.connect_lazy();
        self.channels
            .lock()
            .expect("channel cache poisoned")
            .insert(endpoint.to_string(), channel.clone());
        Ok(channel)
    }

    async fn send_inner(
        &self,
        meta: &PeerEnvelopeMeta,
        endpoint: &str,
        req: PeerRequest,
        deadline: Duration,
    ) -> Result<PeerResponse, TransportError> {
        let kind = req.kind();
        let payload = serde_json::to_vec(&req)
            .map_err(|e| TransportError::Remote(format!("peer request encode failed: {e}")))?;

        let envelope = pb::PeerEnvelope {
            cluster_id: meta.cluster_id.to_string(),
            recovery_epoch: meta.recovery_epoch.0,
            from_node_id: meta.from.0,
            to_node_id: meta.to.0,
            payload_encoding: PAYLOAD_ENCODING_JSON,
            payload: payload.into(),
        };

        let mut request = tonic::Request::new(envelope);
        for (key, value) in meta.trace.to_headers() {
            if let Ok(value) = value.parse() {
                request.metadata_mut().insert(key, value);
            }
        }

        let mut client = PeerServiceClient::new(self.channel(endpoint)?);
        let call = async move {
            match kind {
                "append_entries" => client.append_entries(request).await,
                "vote" => client.vote(request).await,
                _ => client.install_snapshot(request).await,
            }
        };

        let response = tokio::time::timeout(deadline, call)
            .await
            .map_err(|_| {
                TransportError::Network(format!(
                    "peer call to {endpoint} exceeded {} ms",
                    deadline.as_millis()
                ))
            })?
            .map_err(|status| map_status(endpoint, &status))?;

        decode_response(response.get_ref()).map_err(TransportError::Remote)
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
        self.faults
            .guard(from, to, self.send_inner(&meta, endpoint, req, deadline))
            .await
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
