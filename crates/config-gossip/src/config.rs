//! Gossip node configuration.

use std::fmt;
use std::net::SocketAddr;
use std::time::Duration;

use config_core::identity::{ClusterId, NodeId};

/// Configuration for a [`crate::GossipNode`].
///
/// Gossip runs on its own port and its own key, separate from Raft peer and client traffic
/// (spec §15.1, ADR-0003). Fields are public so a daemon can map its config file onto them
/// directly; [`GossipConfig::new`] supplies defaults tuned for fast local convergence.
#[derive(Clone)]
pub struct GossipConfig {
    /// Cluster this node claims. Also used as the gossip label, which is the AES-GCM
    /// additional authenticated data when encryption is on — so it must be identical on
    /// every node of the cluster.
    pub cluster_id: ClusterId,

    /// This node's stable id. Used as the `memberlist` node name.
    pub node_id: NodeId,

    /// Address to bind the gossip TCP listener and UDP socket to.
    ///
    /// Port `0` picks an ephemeral port; read the real one back with
    /// [`crate::GossipNode::advertise_addr`].
    pub bind_addr: SocketAddr,

    /// Address advertised to peers, when it differs from `bind_addr` (NAT, container port
    /// mapping). `None` advertises the resolved bind address.
    pub advertise_addr: Option<SocketAddr>,

    /// Seed gossip addresses contacted at startup. Unreachable seeds are warnings, not
    /// errors: static Raft seeds remain the only mandatory bootstrap path (ADR-0003).
    pub seeds: Vec<SocketAddr>,

    /// AES-256 gossip key this node *signs* with. `None` disables encryption and is intended
    /// for single-host tests only; production deployments must set it (spec §15.1).
    pub secret_key: Option<[u8; 32]>,

    /// Further AES-256 keys this node accepts on receive without ever signing with them
    /// (M6, ADR-0028).
    ///
    /// Only meaningful alongside [`GossipConfig::secret_key`]: with no primary key there is no
    /// keyring to install them on and nothing is encrypted at all. They are the first half of a
    /// rotation — every node accepts the new key before any node starts signing with it.
    pub accepted_keys: Vec<[u8; 32]>,

    /// Failure-detector probe interval.
    pub probe_interval: Duration,

    /// Per-probe response timeout. Should stay at or below `probe_interval`.
    pub probe_timeout: Duration,

    /// Interval between gossip (rumor dissemination) rounds.
    pub gossip_interval: Duration,

    /// Upper bound between full rebuilds of the [`config_core::GossipObservationSource`]
    /// snapshot. Membership events refresh it sooner; this bounds staleness when no event
    /// arrives (for example a silent state change).
    pub refresh_interval: Duration,

    /// How many times [`crate::GossipNode::join`] retries an incomplete seed join.
    pub join_attempts: u32,

    /// Delay between join attempts.
    pub join_retry_delay: Duration,

    /// Timeout for the leave and update broadcasts issued by
    /// [`crate::GossipNode::shutdown`] and [`crate::GossipNode::update_hint`].
    pub broadcast_timeout: Duration,

    /// Advisory fields appended after the hint body (ADR-0030).
    ///
    /// `None` advertises exactly the bytes every earlier build advertised.
    pub extras: Option<crate::HintExtras>,
}

impl GossipConfig {
    /// Configuration with fast, test-friendly timers: 200 ms probes, 100 ms gossip rounds,
    /// a 250 ms snapshot refresh, three join attempts and no encryption key.
    ///
    /// Production deployments should set [`GossipConfig::secret_key`] and normally relax the
    /// timers toward `memberlist`'s LAN profile.
    pub fn new(cluster_id: ClusterId, node_id: NodeId, bind_addr: SocketAddr) -> Self {
        Self {
            cluster_id,
            node_id,
            bind_addr,
            advertise_addr: None,
            seeds: Vec::new(),
            secret_key: None,
            accepted_keys: Vec::new(),
            probe_interval: Duration::from_millis(200),
            probe_timeout: Duration::from_millis(200),
            gossip_interval: Duration::from_millis(100),
            refresh_interval: Duration::from_millis(250),
            join_attempts: 3,
            join_retry_delay: Duration::from_millis(250),
            broadcast_timeout: Duration::from_secs(2),
            extras: None,
        }
    }
}

/// Redacts [`GossipConfig::secret_key`]; gossip keys must never reach a log (ADR-0013).
impl fmt::Debug for GossipConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GossipConfig")
            .field("cluster_id", &self.cluster_id)
            .field("node_id", &self.node_id)
            .field("bind_addr", &self.bind_addr)
            .field("advertise_addr", &self.advertise_addr)
            .field("seeds", &self.seeds)
            .field(
                "secret_key",
                &if self.secret_key.is_some() {
                    "<redacted>"
                } else {
                    "<none>"
                },
            )
            .field("accepted_keys", &self.accepted_keys.len())
            .field("probe_interval", &self.probe_interval)
            .field("probe_timeout", &self.probe_timeout)
            .field("gossip_interval", &self.gossip_interval)
            .field("refresh_interval", &self.refresh_interval)
            .field("join_attempts", &self.join_attempts)
            .field("join_retry_delay", &self.join_retry_delay)
            .field("broadcast_timeout", &self.broadcast_timeout)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_redacts_the_key() {
        let mut cfg = GossipConfig::new(
            ClusterId::from_bytes([1; 16]),
            NodeId(1),
            "127.0.0.1:0".parse().expect("addr"),
        );
        cfg.secret_key = Some([0xab; 32]);
        let rendered = format!("{cfg:?}");
        assert!(rendered.contains("<redacted>"));
        assert!(!rendered.contains("171"));
        assert!(!rendered.contains("ab, ab"));
    }
}
