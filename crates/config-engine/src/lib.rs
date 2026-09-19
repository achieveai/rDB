//! rEtcd node engine (spec §6.3, §8, §10, §13; ADR-0009, ADR-0011).
//!
//! Owns the OpenRaft lifecycle for one node: formation, client writes through `client_write`,
//! leader-linearizable reads gated by `ensure_linearizable`, health/metrics/capabilities, and
//! the transport-agnostic peer-plane contract ([`transport`]) that `config-grpc` implements.
//!
//! The engine never creates a Tokio runtime, and no OpenRaft or tonic type appears on the
//! **client-facing** API — `put`/`get`/`list`/`delete`, [`DirectClient`], [`NodeMetrics`],
//! [`Health`] and [`MembershipView`] are all OpenRaft-free. The peer plane is a different
//! matter: [`transport::PeerRequest`] and [`transport::PeerResponse`] wrap OpenRaft RPC types
//! by design, because that is what a Raft transport transports (ADR-0010).
//!
//! # Shape
//!
//! ```text
//!   client  ──▶ DirectClient ──▶ ConfigNode ──▶ Raft ──▶ StorageHandle::Ephemeral | ::Rocks
//!                                    │            │
//!                                    │            └─▶ EngineNetwork ─▶ PeerTransport ─▶ peer
//!                                    └─▶ peer_handler() ◀── PeerTransport ◀── peer
//! ```
//!
//! # The three rules this crate exists to keep
//!
//! 1. **No implicit formation.** [`ConfigNode::start`] never calls `initialize`. A wiped node
//!    that is never handed a [`FormationPlan`] stays idle and answers `Unavailable` forever,
//!    instead of electing itself a one-voter cluster and overwriting the real one (ADR-0011).
//! 2. **No stale reads.** Every `Get` and `List` passes `ensure_linearizable` first, so an
//!    isolated former leader refuses rather than serving what it last saw (ADR-0009).
//! 3. **Gossip decides nothing.** Observations are validated against committed membership by
//!    [`validate_hint`] and then used only for telemetry; the transport dials the committed
//!    endpoint and nothing else (ADR-0003).
#![deny(missing_docs)]
#![forbid(unsafe_code)]

pub mod admin;
pub mod config;
pub mod direct;
pub mod error;
pub mod hint;
pub mod metrics;
pub mod netfault;
mod network;
pub mod node;
pub mod pagination;
pub mod testing;
pub mod transport;
pub mod watch;

pub use admin::{
    AdminError, MembershipReport, ReplicationProgress, SnapshotTriggered, DEFAULT_PROMOTE_MAX_LAG,
};
pub use config::{AuthzKind, NodeConfig, RaftTimers, StorageHandle, MAX_PAYLOAD_ENTRIES};
pub use direct::DirectClient;
pub use error::{EngineError, FormationError, FormationPlan, Timeout};
pub use hint::{
    validate_hint, HintVerdict, REASON_CLUSTER_MISMATCH, REASON_ENDPOINT_MISMATCH,
    REASON_EPOCH_MISMATCH, REASON_NOT_FORMED, REASON_SELF_CLAIM, REASON_UNKNOWN_NODE,
};
pub use metrics::{
    AuthnRejectReason, Health, HealthPayload, HistogramSnapshot, LatencyHistogram, LogIdView,
    MembershipView, MetricsReport, NodeMetrics, NodeRole, OpLatencies, PolicyMetrics,
    PolicySummary, TlsMetrics, LATENCY_BUCKETS_SECONDS,
};
pub use netfault::NetFault;
pub use node::ConfigNode;
pub use pagination::{PaginationConfig, Paginator, PinStats, PinTable};
pub use testing::InProcTransport;
pub use transport::{
    PeerEnvelopeMeta, PeerHandler, PeerReject, PeerRequest, PeerResponse, PeerSink, PeerTransport,
    TransportError,
};
pub use watch::{
    JournalView, LeaderClock, ManualClock, PolicyChange, RetentionReason, StreamId, SystemClock,
    TerminationReason, TrackedWatch, WatchHub, WatchStats, DEFAULT_PROGRESS_INTERVAL,
    MAX_PROGRESS_INTERVAL, MIN_PROGRESS_INTERVAL,
};
