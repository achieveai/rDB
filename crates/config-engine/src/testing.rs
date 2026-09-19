//! In-process cluster plumbing (test plan TA-4).
//!
//! Public, not `#[cfg(test)]`: `config-testkit` and the integration tests of other crates
//! build 3-node clusters with it, and a `cfg(test)` module is invisible to them.
//!
//! [`InProcTransport`] is a real [`PeerTransport`]: the same envelope, the same identity
//! checks on the receiving side, the same deadline handling, the same [`NetFault`] rules. It
//! replaces the wire, not the protocol — so a partition test proves something about the
//! engine rather than about a mock.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use config_core::NodeId;

use crate::netfault::NetFault;
use crate::transport::{
    PeerEnvelopeMeta, PeerHandler, PeerReject, PeerRequest, PeerResponse, PeerTransport,
    TransportError,
};

/// Routes peer calls between nodes in one process, through a [`NetFault`] switchboard.
///
/// One instance is shared by every node of a cluster, so a single `NetFault` handle controls
/// every direction at once.
pub struct InProcTransport {
    faults: NetFault,
    routes: Mutex<BTreeMap<String, PeerHandler>>,
}

impl std::fmt::Debug for InProcTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let routes = self.routes.lock().unwrap_or_else(|e| e.into_inner());
        f.debug_struct("InProcTransport")
            .field("endpoints", &routes.keys().collect::<Vec<_>>())
            .field("faults", &self.faults)
            .finish()
    }
}

impl InProcTransport {
    /// A transport whose calls obey `faults`.
    pub fn new(faults: NetFault) -> Self {
        Self {
            faults,
            routes: Mutex::new(BTreeMap::new()),
        }
    }

    /// The synthetic peer endpoint of `node_id`.
    ///
    /// A formation plan must use exactly this string, because committed membership is the
    /// only address the engine will ever dial (ADR-0003).
    pub fn endpoint(node_id: NodeId) -> String {
        format!("inproc://{}", node_id.0)
    }

    /// Make `node_id` reachable at [`InProcTransport::endpoint`].
    pub fn register(&self, node_id: NodeId, handler: PeerHandler) {
        self.routes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(Self::endpoint(node_id), handler);
    }

    /// Remove `node_id`'s route; later calls to it fail as `Unreachable`.
    ///
    /// Models a process that is gone, as opposed to [`NetFault::block`], which models a
    /// network that is down while the process still runs.
    pub fn deregister(&self, node_id: NodeId) {
        self.routes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&Self::endpoint(node_id));
    }

    /// The shared fault switchboard.
    pub fn faults(&self) -> &NetFault {
        &self.faults
    }

    fn route(&self, endpoint: &str) -> Option<PeerHandler> {
        self.routes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(endpoint)
            .cloned()
    }
}

/// A receiver's refusal, as the sender's transport sees it.
fn reject_to_transport_error(reject: PeerReject) -> TransportError {
    match reject {
        PeerReject::WrongCluster { .. }
        | PeerReject::WrongEpoch { .. }
        | PeerReject::WrongDestination { .. }
        | PeerReject::IdentityMismatch(_)
        // A retired sender is fenced out, not flaky: the sender backs off exactly as it does
        // for a cluster mismatch, and its log says which (M5, ADR-0023).
        | PeerReject::Retired { .. } => TransportError::IdentityRejected(reject.to_string()),
        // The peer exists but is not serving: backoff, exactly as for a closed port.
        PeerReject::NotRunning => TransportError::Unreachable(reject.to_string()),
        PeerReject::Raft(_) => TransportError::Remote(reject.to_string()),
    }
}

#[async_trait]
impl PeerTransport for InProcTransport {
    async fn send(
        &self,
        meta: PeerEnvelopeMeta,
        endpoint: &str,
        req: PeerRequest,
        deadline: Duration,
    ) -> Result<PeerResponse, TransportError> {
        let Some(handler) = self.route(endpoint) else {
            return Err(TransportError::Unreachable(format!(
                "no in-process node registered at {endpoint}"
            )));
        };
        let (from, to) = (meta.from, meta.to);
        self.faults
            .guard(from, to, async move {
                match tokio::time::timeout(deadline, handler.handle(meta, req)).await {
                    Err(_) => Err(TransportError::Network(format!(
                        "in-process call to {endpoint} exceeded {deadline:?}"
                    ))),
                    Ok(Ok(resp)) => Ok(resp),
                    Ok(Err(reject)) => Err(reject_to_transport_error(reject)),
                }
            })
            .await
    }
}
