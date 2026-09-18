//! Advisory gossip for rEtcd: a thin [`memberlist`] adapter (ADR-0003, spec §5).
//!
//! This crate advertises one node's [`ObservedPeerHint`](config_core::ObservedPeerHint) as
//! encrypted gossip metadata and
//! exposes what it hears from other nodes through [`config_core::GossipObservationSource`].
//!
//! # Advisory only
//!
//! Nothing here is authoritative. Hints are *candidate* endpoints and *local* liveness
//! observations. The engine validates every hint against committed Raft membership and mTLS
//! identity before use, and a `Dead` observation is telemetry — it never removes a voter.
//! Gossip failures are alerts, not outages: a failed join is logged at `warn` and the node
//! keeps running (spec §5.3, ADR-0003 "Consequences").
//!
//! # Boundary
//!
//! No `memberlist` type appears in this crate's public API (ADR-0004). Peer state, meta
//! decoding and transport errors are all mapped to
//! [`ObservedPeerHint`](config_core::ObservedPeerHint), [`GossipError`] and
//! [`HintDecodeError`].
//!
//! # Logs emitted by `memberlist` itself
//!
//! `memberlist` logs through `tracing` on its own `memberlist_*` targets. Those lines are
//! **outside** rEtcd's ADR-0013 contract: they carry no `node_id`/`cluster_id` span fields,
//! and 0.8.5 emits `Error`-level lines during an ordinary clean shutdown (a peer's socket
//! closing mid-probe). Alerting and the ADR-0013 §5 log queries must therefore exclude them
//! with `"@logger" NOT LIKE 'memberlist%'`. A level-remap layer in `config-log` is the
//! tracked follow-up; until then the filter is the contract.
//!
//! # Example
//!
//! ```no_run
//! use config_core::{ClusterId, GossipObservationSource, Liveness, NodeId, ObservedPeerHint};
//! use config_gossip::{GossipConfig, GossipNode};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let cluster_id = ClusterId::from_bytes([1u8; 16]);
//! let node_id = NodeId(1);
//! let mut cfg = GossipConfig::new(cluster_id, node_id, "127.0.0.1:7946".parse()?);
//! cfg.secret_key = Some([7u8; 32]);
//! cfg.seeds = vec!["127.0.0.1:7947".parse()?];
//!
//! let hint = ObservedPeerHint {
//!     cluster_id,
//!     node_id,
//!     peer_endpoint: "127.0.0.1:2380".into(),
//!     client_endpoint: Some("127.0.0.1:2379".into()),
//!     software_version: "0.1.0".into(),
//!     protocol_version: 1,
//!     zone: None,
//!     liveness: Liveness::Alive,
//! };
//!
//! let node = GossipNode::start(cfg, hint).await?;
//! let observed = node.peers(); // sync, non-blocking snapshot
//! node.shutdown().await;
//! # let _ = observed;
//! # Ok(())
//! # }
//! ```

#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod config;
mod error;
mod meta;
mod node;
mod static_source;

pub use config::GossipConfig;
pub use error::{GossipError, HintDecodeError};
pub use meta::{decode_hint, encode_hint, HINT_WIRE_VERSION, MAX_HINT_BYTES};
pub use node::GossipNode;
pub use static_source::StaticObservationSource;
