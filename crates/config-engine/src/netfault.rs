//! Network fault injection for the peer plane (test plan TA-5).
//!
//! A [`NetFault`] handle is shared by every node of an in-process cluster and consulted by the
//! `PeerTransport` implementation on every send. Blocking a pair fails new calls immediately
//! with [`TransportError::Unreachable`] **and** cancels calls already in flight, so a partition
//! applied mid-request behaves like a cut cable, not like a slow link.
//!
//! Production nodes use `NetFault::default()`, which is permanently transparent.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use config_core::NodeId;
use tokio::sync::Notify;

use crate::transport::TransportError;

#[derive(Default)]
struct State {
    /// One-way blocks `(from, to)`.
    blocked: HashSet<(NodeId, NodeId)>,
    /// Added latency before a call is sent.
    delays: HashMap<(NodeId, NodeId), Duration>,
    /// Calls are sent but their responses are discarded (`Network` error to the caller).
    drop_response: HashSet<(NodeId, NodeId)>,
    /// Bumped on every change; lets waiters detect changes without holding the lock.
    generation: u64,
}

/// Shared, cloneable fault switchboard for one cluster.
#[derive(Clone, Default)]
pub struct NetFault {
    state: Arc<Mutex<State>>,
    changed: Arc<Notify>,
}

impl std::fmt::Debug for NetFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.state.lock().expect("netfault poisoned");
        f.debug_struct("NetFault")
            .field("blocked", &s.blocked)
            .field("delays", &s.delays)
            .field("drop_response", &s.drop_response)
            .finish()
    }
}

impl NetFault {
    /// A transparent switchboard.
    pub fn new() -> Self {
        Self::default()
    }

    fn mutate(&self, f: impl FnOnce(&mut State)) {
        {
            let mut s = self.state.lock().expect("netfault poisoned");
            f(&mut s);
            s.generation += 1;
        }
        self.changed.notify_waiters();
    }

    /// Block `from → to` (one way). Idempotent.
    pub fn block(&self, from: NodeId, to: NodeId) {
        tracing::info!(from = from.0, to = to.0, "netfault block");
        self.mutate(|s| {
            s.blocked.insert((from, to));
        });
    }

    /// Block both directions between `a` and `b`.
    pub fn block_pair(&self, a: NodeId, b: NodeId) {
        tracing::info!(a = a.0, b = b.0, "netfault block_pair");
        self.mutate(|s| {
            s.blocked.insert((a, b));
            s.blocked.insert((b, a));
        });
    }

    /// Block `id` from every node in `peers` and every node in `peers` from `id`.
    pub fn isolate(&self, id: NodeId, peers: &[NodeId]) {
        tracing::info!(node_id = id.0, ?peers, "netfault isolate");
        self.mutate(|s| {
            for &p in peers {
                if p != id {
                    s.blocked.insert((id, p));
                    s.blocked.insert((p, id));
                }
            }
        });
    }

    /// Remove one one-way block.
    pub fn unblock(&self, from: NodeId, to: NodeId) {
        self.mutate(|s| {
            s.blocked.remove(&(from, to));
        });
    }

    /// Clear every block, delay, and drop rule (the harness's `heal`).
    pub fn unblock_all(&self) {
        tracing::info!("netfault unblock_all");
        self.mutate(|s| {
            s.blocked.clear();
            s.delays.clear();
            s.drop_response.clear();
        });
    }

    /// Add latency before each `from → to` call is sent.
    pub fn delay(&self, from: NodeId, to: NodeId, d: Duration) {
        self.mutate(|s| {
            s.delays.insert((from, to), d);
        });
    }

    /// Send `from → to` calls but discard their responses; the caller sees a `Network` error.
    /// Used to prove `DeadlineExceededUnknownOutcome` handling (ADR-0015).
    pub fn drop_response(&self, from: NodeId, to: NodeId) {
        self.mutate(|s| {
            s.drop_response.insert((from, to));
        });
    }

    /// Stop dropping responses for `from → to`.
    pub fn undrop_response(&self, from: NodeId, to: NodeId) {
        self.mutate(|s| {
            s.drop_response.remove(&(from, to));
        });
    }

    /// Is `from → to` currently blocked?
    pub fn is_blocked(&self, from: NodeId, to: NodeId) -> bool {
        self.state
            .lock()
            .expect("netfault poisoned")
            .blocked
            .contains(&(from, to))
    }

    /// Configured latency for `from → to`, if any.
    pub fn delay_for(&self, from: NodeId, to: NodeId) -> Option<Duration> {
        self.state
            .lock()
            .expect("netfault poisoned")
            .delays
            .get(&(from, to))
            .copied()
    }

    /// Are responses for `from → to` being discarded?
    pub fn drops_response(&self, from: NodeId, to: NodeId) -> bool {
        self.state
            .lock()
            .expect("netfault poisoned")
            .drop_response
            .contains(&(from, to))
    }

    /// Resolves as soon as `from → to` is blocked (immediately if it already is).
    pub async fn wait_blocked(&self, from: NodeId, to: NodeId) {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_blocked(from, to) {
                return;
            }
            notified.await;
        }
    }

    /// Run one peer call under the fault rules: apply the delay, refuse if blocked, cancel if
    /// blocked while in flight, and discard the response if a drop rule is active.
    ///
    /// `PeerTransport` implementations wrap their actual send in this.
    pub async fn guard<F, T>(&self, from: NodeId, to: NodeId, send: F) -> Result<T, TransportError>
    where
        F: Future<Output = Result<T, TransportError>>,
    {
        if self.is_blocked(from, to) {
            return Err(TransportError::Unreachable(format!(
                "netfault: {from} -> {to} blocked"
            )));
        }
        if let Some(d) = self.delay_for(from, to) {
            tokio::time::sleep(d).await;
        }
        let result = tokio::select! {
            biased;
            _ = self.wait_blocked(from, to) => {
                return Err(TransportError::Unreachable(format!(
                    "netfault: {from} -> {to} blocked while in flight"
                )));
            }
            r = send => r,
        };
        match result {
            Ok(_) if self.drops_response(from, to) => Err(TransportError::Network(format!(
                "netfault: response {to} -> {from} dropped"
            ))),
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(i: u64) -> NodeId {
        NodeId(i)
    }

    #[tokio::test]
    async fn block_refuses_new_calls_and_cancels_in_flight() {
        let nf = NetFault::new();
        nf.block(n(1), n(2));
        let r = nf
            .guard(n(1), n(2), async { Ok::<_, TransportError>(1u8) })
            .await;
        assert!(matches!(r, Err(TransportError::Unreachable(_))));
        // unaffected direction
        assert_eq!(
            nf.guard(n(2), n(1), async { Ok::<_, TransportError>(2u8) })
                .await
                .unwrap(),
            2
        );

        nf.unblock_all();
        let nf2 = nf.clone();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let call = tokio::spawn(async move {
            nf2.guard(n(1), n(3), async move {
                let _ = rx.await; // never completes; must be cancelled by the block
                Ok::<_, TransportError>(9u8)
            })
            .await
        });
        tokio::task::yield_now().await;
        nf.block_pair(n(1), n(3));
        let r = call.await.unwrap();
        assert!(matches!(r, Err(TransportError::Unreachable(_))), "{r:?}");
        drop(tx);
    }

    #[tokio::test]
    async fn drop_response_turns_success_into_network_error() {
        let nf = NetFault::new();
        nf.drop_response(n(1), n(2));
        let r = nf
            .guard(n(1), n(2), async { Ok::<_, TransportError>(1u8) })
            .await;
        assert!(matches!(r, Err(TransportError::Network(_))));
        nf.undrop_response(n(1), n(2));
        assert!(nf
            .guard(n(1), n(2), async { Ok::<_, TransportError>(1u8) })
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn isolate_blocks_both_directions_with_all_peers() {
        let nf = NetFault::new();
        nf.isolate(n(2), &[n(1), n(2), n(3)]);
        assert!(nf.is_blocked(n(2), n(1)) && nf.is_blocked(n(1), n(2)));
        assert!(nf.is_blocked(n(2), n(3)) && nf.is_blocked(n(3), n(2)));
        assert!(!nf.is_blocked(n(1), n(3)));
        nf.unblock_all();
        assert!(!nf.is_blocked(n(2), n(1)));
    }
}
