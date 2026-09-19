//! A network-free [`GossipObservationSource`] for tests and fault injection.

use config_core::hint::{GossipObservationSource, ObservedPeerHint};

/// Returns a fixed list of hints, unchanged, forever.
///
/// This is how the test harness injects *poisoned* observations — a hint carrying the wrong
/// `cluster_id` or `node_id` — without standing up a network. The engine's `validate_hint`
/// must reject them, proving that a hostile gossip source cannot influence membership
/// (ADR-0003 "Verification").
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StaticObservationSource(
    /// The hints handed back by every call to `peers()`.
    pub Vec<ObservedPeerHint>,
);

impl StaticObservationSource {
    /// Source that always reports `hints`.
    pub fn new(hints: Vec<ObservedPeerHint>) -> Self {
        Self(hints)
    }

    /// Source that never observes anything, distinct from
    /// [`config_core::NoGossip`] only in that it can later be replaced by a populated one.
    pub fn empty() -> Self {
        Self(Vec::new())
    }

    /// The configured hints.
    pub fn hints(&self) -> &[ObservedPeerHint] {
        &self.0
    }
}

impl From<Vec<ObservedPeerHint>> for StaticObservationSource {
    fn from(hints: Vec<ObservedPeerHint>) -> Self {
        Self(hints)
    }
}

impl GossipObservationSource for StaticObservationSource {
    fn peers(&self) -> Vec<ObservedPeerHint> {
        self.0.clone()
    }
}
