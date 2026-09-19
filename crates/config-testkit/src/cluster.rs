//! The `Cluster` harness (test plan §4.1): an N-node rEtcd cluster over the **real** gRPC
//! peer and client planes.
//!
//! # What makes this the real thing
//!
//! Every node here is what `config-server` would run: an [`EphemeralStore`], a
//! [`ConfigNode`], a peer-plane gRPC server, and a client-plane gRPC server. Raft messages
//! travel over tonic between loopback sockets, not through an in-process router, so a test
//! that passes here has exercised encoding, connection management, and status mapping as well
//! as consensus. The one thing that is not real is *where* faults come from: partitions are
//! applied by [`NetFault`] before the socket is touched, because a test cannot unplug a cable.
//!
//! # The three invariants the harness itself must keep
//!
//! 1. **Ephemeral ports, bound before formation.** Every listener binds `127.0.0.1:0`, and all
//!    peer listeners are bound *before* the [`FormationPlan`] is built — the plan's endpoint
//!    strings become committed membership, which is both what peers dial and what a
//!    `NotLeader` hint names (ADR-0003, ADR-0009). A port discovered later would be a
//!    different cluster.
//! 2. **No sleeping.** Every wait is [`crate::poll::poll_until`] against observable state with
//!    a deadline derived from [`TestTimers`], and a timeout reports every node's
//!    [`NodeMetrics`] (anti-flake rules 1-3, 10).
//! 3. **Spans are entered, not assumed.** `ConfigNode::start` and both `serve_*` calls happen
//!    inside a per-node span that is a child of the test span, because both capture
//!    `Span::current()` at call time. Without that, RPC and Raft lines would be orphans and
//!    the §5 log queries would return nothing (ADR-0013).
//!
//! # Example
//!
//! ```no_run
//! # use config_testkit::cluster::{Cluster, StorageKind};
//! # async fn run() {
//! let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
//! let leader = cluster.leader().await;
//! let store = cluster.client(leader);
//! // ... assertions ...
//! cluster.shutdown().await;
//! # }
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use config_core::{
    AllowAll, Authorizer, Capabilities, ClusterId, ClusterIdentity, ConfigStore, Durability,
    GossipObservationSource, Limits, Liveness, NodeId, ObservedPeerHint, Principal, RecoveryEpoch,
    StaticAllowlist, WatchRequest, WatchRetention,
};
use config_engine::watch::testing::GateHandle;
use config_engine::{
    ConfigNode, FormationPlan, HealthPayload, JournalView, ManualClock, NetFault, NodeConfig,
    NodeMetrics, NodeRole, RaftTimers, StorageHandle, TrackedWatch, WatchHub, WatchStats,
};
use config_gossip::{GossipConfig, GossipNode};
use config_grpc::peer_plane::PeerIdentity;
use config_grpc::{
    admin_service, serve_client_plane, serve_peer_plane, AdminAllowlist, AdminBackend,
    BackupArtifact, ClientBackend, GrpcPeerTransport, ServerHandle, TlsMode,
};
use config_storage::{
    EphemeralStore, FaultCounters, FaultInjector, NoFaults, RocksOptions, RocksStore,
    SnapshotConfig, StorageOpenError,
};
use tokio::net::TcpListener;

use crate::poll::{poll_until, TestTimers, Timeout};
use crate::ports::ephemeral_listener;
use crate::tls::{CertOverrides, CertProfile, TlsFixture};

/// Which storage backend the cluster's nodes run on (test plan TA-16).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StorageKind {
    /// In-memory storage. A restarted node comes back empty, by design (ADR-0008, ADR-0016).
    #[default]
    Ephemeral,
    /// Persistent RocksDB storage, one directory per node under the cluster's own `TempDir`.
    ///
    /// A restart reopens the *same* directory, which is what makes the M2 rows about
    /// acknowledged mutations surviving a restart mean anything.
    Rocks(RocksSpec),
}

impl StorageKind {
    /// Rocks with the production profile (full fsync).
    pub const ROCKS: Self = Self::Rocks(RocksSpec::DEFAULT);

    /// Whether this cluster keeps its data on disk across a restart.
    pub const fn is_persistent(self) -> bool {
        matches!(self, StorageKind::Rocks(_))
    }

    fn rocks_options(self, create_if_missing: bool) -> Option<RocksOptions> {
        match self {
            StorageKind::Ephemeral => None,
            StorageKind::Rocks(spec) => Some(RocksOptions {
                sync_writes: spec.sync_writes,
                create_if_missing,
            }),
        }
    }
}

/// How a [`StorageKind::Rocks`] cluster opens its directories.
///
/// The directory itself is not a field: the harness owns it, under a per-cluster `TempDir`, so
/// that a restart reopens the same path and `shutdown` can still delete it on Windows (TA-16.3).
/// [`Cluster::data_dir`] reports where it landed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RocksSpec {
    /// Whether vote, log flush, and state batches are fsynced.
    ///
    /// `false` is the only way to reach [`Durability::PersistentUnverified`], which exists so
    /// the capability-downgrade row (M2-44) has something honest to assert against. It is not
    /// a speed knob for ordinary tests.
    pub sync_writes: bool,
}

impl RocksSpec {
    /// Full fsync — what a real deployment runs.
    pub const DEFAULT: Self = Self { sync_writes: true };

    /// No fsync. Only for the capability-downgrade row (M2-44 / OQ-15).
    pub const NO_SYNC: Self = Self { sync_writes: false };
}

impl Default for RocksSpec {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// A deliberately incoherent gossip observation (test plan M1-30..M1-32).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoisonSpec {
    /// The hint claims a different cluster.
    WrongClusterId,
    /// The hint claims a node id that is not a committed voter.
    WrongNodeId,
    /// The hint claims a real voter but advertises somebody else's peer endpoint.
    HijackedEndpoint,
    /// The hint claims the right cluster at the wrong recovery epoch (ADR-0011): a peer that
    /// was fenced off during an unsafe recovery and never learned the epoch moved.
    WrongEpoch,
}

/// How the cluster's nodes observe their peers.
///
/// Extends the plan's `GossipKind` with `Static`, which is how a test injects an arbitrary
/// hint list without standing up sockets. Whatever the mode, hints can also be injected at
/// runtime through [`Cluster::gossip`].
#[derive(Debug, Clone, Default)]
pub enum GossipKind {
    /// A real [`GossipNode`] per node on an ephemeral port, all seeded to node 1.
    Real,
    /// No observations at all. The M1 default: gossip must not be needed for Raft to work
    /// (ADR-0003).
    #[default]
    Disabled,
    /// Every node observes exactly these hints.
    Static(Vec<ObservedPeerHint>),
    /// Every node observes one hint built to violate `spec`, aimed at a real voter.
    Poisoned(PoisonSpec),
    /// Observations stop flowing while Raft is untouched (test plan M1-29).
    ///
    /// Implemented as "the observation source reports nothing", not as a blocked gossip
    /// socket: M1 has no `NetFault` seam on the gossip transport.
    Partitioned,
}

/// How the cluster's two gRPC planes are protected (test plan §4, TA-18).
///
/// One [`TlsFixture`] is shared by every node of a cluster, because a cluster is exactly the
/// set of identities one CA vouches for. A node minted by a *different* fixture is the negative
/// case, and [`ClusterConfig::node_cert_overrides`] is how a row asks for one.
#[derive(Clone, Default)]
pub enum ClusterTls {
    /// Plain TCP. The M1/M2 default; a node serving it reports
    /// [`config_core::TransportSecurity::Insecure`] and derives the development principal.
    #[default]
    Insecure,
    /// Mutual TLS from `fixture`: every node serves both planes with its own node certificate,
    /// and dials its peers with the same one.
    MutualTls(Arc<TlsFixture>),
}

impl std::fmt::Debug for ClusterTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterTls::Insecure => f.write_str("Insecure"),
            ClusterTls::MutualTls(fixture) => f
                .debug_tuple("MutualTls")
                .field(&fixture.cluster_id())
                .finish(),
        }
    }
}

impl ClusterTls {
    /// Mutual TLS from a fresh fixture for `cluster_id`, seeded deterministically.
    pub fn mutual(cluster_id: ClusterId, seed: u64) -> Self {
        ClusterTls::MutualTls(Arc::new(TlsFixture::new(cluster_id, seed)))
    }

    /// The fixture backing this mode, if any.
    pub fn fixture(&self) -> Option<&Arc<TlsFixture>> {
        match self {
            ClusterTls::Insecure => None,
            ClusterTls::MutualTls(f) => Some(f),
        }
    }

    /// Whether client identities are derived from certificates rather than assumed.
    pub fn is_mutual(&self) -> bool {
        matches!(self, ClusterTls::MutualTls(_))
    }

    /// What a node serving this mode must advertise (ADR-0016).
    pub fn transport_security(&self) -> config_core::TransportSecurity {
        match self {
            ClusterTls::Insecure => config_core::TransportSecurity::Insecure,
            ClusterTls::MutualTls(_) => config_core::TransportSecurity::MutualTls,
        }
    }

    /// The `TlsMode` node `id` serves both of its planes with.
    fn serving_mode(&self, id: NodeId, overrides: &CertOverrides) -> TlsMode {
        match self {
            ClusterTls::Insecure => TlsMode::Insecure,
            ClusterTls::MutualTls(fixture) => TlsMode::MutualTls(
                fixture
                    .issue_with(CertProfile::node(id), overrides.clone())
                    .mtls()
                    // The harness serves the CN-fallback profile because two rows are *about*
                    // that fallback — M3-16 and the §4.2 smoke row both present a certificate
                    // with no SAN URI — and a harness that refused it could not express them.
                    // That the fallback is off unless a listener asks for it is M3-88's row,
                    // against a listener the row builds itself (F-015).
                    .with_common_name_principals(true),
            ),
        }
    }
}

/// Which authorization model the cluster's nodes enforce (test plan §4.1).
///
/// [`AuthzKind::Missing`] and [`AuthzKind::Invalid`] are not error handling — they are the
/// states an operator can actually put a node into, and the engine denies every client call and
/// reports itself unready in both (engine delta §2, §4.3).
#[derive(Debug, Clone, Default)]
pub enum AuthzKind {
    /// [`AllowAll`] — the M1 development profile.
    #[default]
    AllowAll,
    /// A deployment-managed static allowlist, already parsed.
    StaticAllowlist(config_core::AllowlistPolicy),
    /// A static allowlist given as TOML text, parsed by the node at startup.
    ///
    /// Reserved for M3, where the policy arrives as a file the daemon reads. Constructing a
    /// cluster with it panics rather than silently parsing it here, because the whole point of
    /// the M3 rows is that the *daemon* does the parsing.
    Static(String),
    /// A policy was required and none was supplied: every client call is denied and the node
    /// is unready.
    Missing,
    /// A policy was supplied but could not be parsed or validated: same denial, different
    /// reported kind.
    Invalid,
}

/// Everything a [`Cluster`] needs to start (test plan §4.1).
///
/// Built with [`Cluster::builder`]; every field has a default so a test names only what it
/// cares about.
#[derive(Clone)]
pub struct ClusterConfig {
    /// Number of nodes, with ids `1..=nodes`.
    pub nodes: u64,
    /// Storage backend.
    pub storage: StorageKind,
    /// Gossip mode.
    pub gossip: GossipKind,
    /// Whether [`Cluster::start_with`] calls [`Cluster::form`] and waits for a leader.
    pub form: bool,
    /// Replicated apply-time caps, identical on every voter.
    pub limits: Limits,
    /// Transport security for both planes. `Insecure` in M1; mutual TLS arrives in M3, at
    /// which point a `TlsFixture` fills this in rather than the harness changing shape.
    pub tls: ClusterTls,
    /// Per-node deviations from a correct node certificate (test plan §4.1 negatives).
    ///
    /// A node absent from the map gets a correct certificate. One entry is how a row starts a
    /// single node with a wrong-cluster, wrong-node, foreign-CA, expired, or SAN-less
    /// certificate and then asserts that the other two keep quorum (M3-13).
    ///
    /// Only meaningful under [`ClusterTls::MutualTls`].
    pub node_cert_overrides: BTreeMap<NodeId, CertOverrides>,
    /// Timers the Raft nodes run with **and** every harness deadline is derived from, so the
    /// two can never drift apart (anti-flake rule 3).
    pub timers: TestTimers,
    /// Authorization model.
    pub authz: AuthzKind,
    /// Cluster id. Fixed rather than random: the tests that matter are about *mismatched*
    /// ids, and those build their own identity.
    pub cluster_id: ClusterId,
    /// Recovery epoch.
    pub recovery_epoch: RecoveryEpoch,
    /// Per-node storage fault injectors. A node absent from the map gets [`NoFaults`].
    pub faults: BTreeMap<NodeId, Arc<dyn FaultInjector>>,
    /// Wraps `ensure_linearizable` on every read.
    pub read_timeout: Duration,
    /// Wraps `client_write` on every mutation.
    pub write_timeout: Duration,
    /// How often each node polls its gossip source.
    pub gossip_poll: Duration,
    /// Journal retention ceilings the leader compacts against (M4, ADR-0019).
    ///
    /// The default is *no* automatic compaction: a row that wants one sets a ceiling or calls
    /// [`Cluster::compact_now`], and every other row is spared a background task that could
    /// delete the history it is asserting on.
    pub retention: WatchRetention,
    /// Default progress-frame interval for streams that do not ask for one.
    pub watch_progress_interval: Duration,
    /// The clock the leader's retention task reads (M4, test plan TA-36).
    ///
    /// A [`ManualClock`] by default, so an age-based row advances time explicitly instead of
    /// sleeping — and so no row can compact because a test machine was slow (anti-flake rule
    /// 3). Reachable as [`Cluster::leader_clock`].
    pub clock: Arc<ManualClock>,
    /// Principal names permitted on the admin plane (`[authz] admins`, M5, ADR-0023, OQ-43).
    ///
    /// Empty by default, which is exactly what an absent configuration key means to
    /// [`AdminAllowlist`]: **no** principal may call the admin surface. The service itself is
    /// always mounted, as `config-server` mounts it, so a row can assert the closed default
    /// rather than an absent endpoint.
    pub admins: BTreeSet<String>,
    /// When a snapshot is built, how much snapshot-covered log survives it, and how many
    /// snapshot files are kept (M5, ADR-0022).
    ///
    /// [`SnapshotConfig::DISABLED`] by default, so no row pays for a background build it did
    /// not ask for, and so every existing caller keeps the `SnapshotPolicy::Never` behaviour
    /// it was written against. `NodeConfig::effective_snapshot` forces `DISABLED` on an
    /// ephemeral store regardless, so setting this on a non-Rocks cluster is inert, not fatal.
    pub snapshot: SnapshotConfig,
    /// How far behind the leader a learner may still be and still be promotable (M5,
    /// ADR-0023, `[membership] promote_max_lag`).
    pub promote_max_lag: u64,
    /// Data directories supplied by the caller, overriding `<data_root>/node-{id}`.
    ///
    /// The seam a row needs when a node must open a directory that already holds a store
    /// somebody else wrote — above all a directory produced by
    /// `config_storage::restore_into_fresh_store` standing up as a genesis member (M5-92,
    /// OQ-45). The directory's lifetime belongs to the caller, so it is **not** deleted with
    /// the cluster; use a `tempfile::TempDir` held for the length of the test.
    ///
    /// Only meaningful for a persistent [`StorageKind`].
    pub data_dirs: BTreeMap<NodeId, PathBuf>,
}

impl std::fmt::Debug for ClusterConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterConfig")
            .field("nodes", &self.nodes)
            .field("storage", &self.storage)
            .field("gossip", &self.gossip)
            .field("form", &self.form)
            .field("tls", &self.tls)
            .field("timers", &self.timers)
            .field("authz", &self.authz)
            .field("cluster_id", &self.cluster_id)
            .field("faulty_nodes", &self.faults.keys().collect::<Vec<_>>())
            .field("retention", &self.retention)
            .finish()
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            nodes: 3,
            storage: StorageKind::Ephemeral,
            gossip: GossipKind::Disabled,
            form: true,
            limits: Limits::DEFAULT,
            tls: ClusterTls::Insecure,
            node_cert_overrides: BTreeMap::new(),
            timers: TestTimers::DEFAULT,
            authz: AuthzKind::AllowAll,
            cluster_id: ClusterId::from_bytes([7u8; 16]),
            recovery_epoch: RecoveryEpoch(1),
            faults: BTreeMap::new(),
            // Short enough that a call against an unreachable quorum finishes well inside the
            // 10 s per-test budget, long enough that a healthy 3-node write never races it.
            read_timeout: Duration::from_secs(2),
            write_timeout: Duration::from_secs(2),
            gossip_poll: Duration::from_millis(100),
            // Every ceiling off: the journal grows for the length of a test and nothing
            // deletes an event a row has not finished asserting on.
            retention: WatchRetention {
                max_age: Duration::ZERO,
                max_revisions: 0,
                max_bytes: 0,
                check_interval: Duration::from_millis(50),
            },
            watch_progress_interval: config_engine::DEFAULT_PROGRESS_INTERVAL,
            clock: Arc::new(ManualClock::new()),
            admins: BTreeSet::new(),
            snapshot: SnapshotConfig::DISABLED,
            promote_max_lag: config_engine::DEFAULT_PROMOTE_MAX_LAG,
            data_dirs: BTreeMap::new(),
        }
    }
}

impl ClusterConfig {
    /// The Raft timers derived from [`ClusterConfig::timers`].
    pub fn raft_timers(&self) -> RaftTimers {
        RaftTimers {
            heartbeat_ms: self.timers.heartbeat.as_millis() as u64,
            election_min_ms: self.timers.election_timeout_min.as_millis() as u64,
            election_max_ms: self.timers.election_timeout_max.as_millis() as u64,
        }
    }

    /// The identity of node `id` in this cluster.
    pub fn identity(&self, id: NodeId) -> ClusterIdentity {
        ClusterIdentity {
            cluster_id: self.cluster_id,
            recovery_epoch: self.recovery_epoch,
            node_id: id,
        }
    }
}

/// Fluent builder for a [`ClusterConfig`].
#[derive(Debug, Clone, Default)]
pub struct ClusterBuilder {
    cfg: ClusterConfig,
}

impl ClusterBuilder {
    /// Number of nodes (default 3).
    pub fn nodes(mut self, n: u64) -> Self {
        self.cfg.nodes = n;
        self
    }

    /// Storage backend (default [`StorageKind::Ephemeral`]).
    pub fn storage(mut self, storage: StorageKind) -> Self {
        self.cfg.storage = storage;
        self
    }

    /// Gossip mode (default [`GossipKind::Disabled`]).
    pub fn gossip(mut self, gossip: GossipKind) -> Self {
        self.cfg.gossip = gossip;
        self
    }

    /// Whether to form the cluster and wait for a leader (default `true`).
    pub fn form(mut self, form: bool) -> Self {
        self.cfg.form = form;
        self
    }

    /// Replicated caps (default [`Limits::DEFAULT`]).
    pub fn limits(mut self, limits: Limits) -> Self {
        self.cfg.limits = limits;
        self
    }

    /// Transport security for both planes (default [`ClusterTls::Insecure`]).
    pub fn tls(mut self, tls: ClusterTls) -> Self {
        self.cfg.tls = tls;
        self
    }

    /// Mutual TLS from a fresh fixture for the cluster id configured *so far*.
    ///
    /// Call after [`ClusterBuilder::cluster_id`], not before: the fixture mints SANs for the
    /// cluster it is given, and a node whose SAN names another cluster is rejected.
    pub fn mutual_tls(mut self, seed: u64) -> Self {
        self.cfg.tls = ClusterTls::mutual(self.cfg.cluster_id, seed);
        self
    }

    /// Give node `id` a deliberately wrong certificate (test plan §4.1 negatives).
    pub fn node_cert_override(mut self, id: NodeId, overrides: CertOverrides) -> Self {
        self.cfg.node_cert_overrides.insert(id, overrides);
        self
    }

    /// Raft timers and the deadlines derived from them (default [`TestTimers::DEFAULT`]).
    pub fn timers(mut self, timers: TestTimers) -> Self {
        self.cfg.timers = timers;
        self
    }

    /// Authorization model (default [`AuthzKind::AllowAll`]).
    pub fn authz(mut self, authz: AuthzKind) -> Self {
        self.cfg.authz = authz;
        self
    }

    /// Cluster id every node is bound to.
    pub fn cluster_id(mut self, cluster_id: ClusterId) -> Self {
        self.cfg.cluster_id = cluster_id;
        self
    }

    /// Install a storage fault injector on one node (default [`NoFaults`] everywhere).
    pub fn faults(mut self, id: NodeId, injector: Arc<dyn FaultInjector>) -> Self {
        self.cfg.faults.insert(id, injector);
        self
    }

    /// Per-read and per-write engine timeouts.
    pub fn timeouts(mut self, read: Duration, write: Duration) -> Self {
        self.cfg.read_timeout = read;
        self.cfg.write_timeout = write;
        self
    }

    /// Journal retention ceilings the leader's compaction task compacts against (M4, TA-32).
    ///
    /// The default is every ceiling off (see [`ClusterConfig::default`]'s doc comment), so a
    /// row only needs this when it is specifically testing age/count/byte-driven compaction
    /// (M4-33..M4-36) — every other row is spared a background task that could delete history
    /// it has not finished asserting on.
    pub fn retention(mut self, retention: WatchRetention) -> Self {
        self.cfg.retention = retention;
        self
    }

    /// The clock the leader's retention task reads for age-based compaction (M4, TA-33).
    ///
    /// Defaults to a fresh [`ManualClock`] reading zero. A row that wants to *share* a clock
    /// across two separately-built clusters (uncommon) passes its own; the ordinary case is
    /// just reading it back with [`Cluster::leader_clock`] after `start()`.
    pub fn leader_clock(mut self, clock: ManualClock) -> Self {
        self.cfg.clock = Arc::new(clock);
        self
    }

    /// The principals permitted on the admin plane (M5-49..M5-53, ADR-0023).
    ///
    /// Replaces the set rather than adding to it, so a row states the whole allowlist in one
    /// place. Leaving it unset keeps the closed default: every admin call is denied.
    pub fn admins<S: Into<String>>(mut self, names: impl IntoIterator<Item = S>) -> Self {
        self.cfg.admins = names.into_iter().map(Into::into).collect();
        self
    }

    /// Enable snapshot building and log purge with an explicit policy (M5, ADR-0022).
    ///
    /// Only meaningful on [`StorageKind::ROCKS`]; an ephemeral node forces
    /// [`SnapshotConfig::DISABLED`] because a failed `build_snapshot` is fatal to OpenRaft.
    pub fn snapshot(mut self, snapshot: SnapshotConfig) -> Self {
        self.cfg.snapshot = snapshot;
        self
    }

    /// The promotion lag ceiling every node runs with (M5, ADR-0023, A5).
    pub fn promote_max_lag(mut self, max: u64) -> Self {
        self.cfg.promote_max_lag = max;
        self
    }

    /// Open node `id`'s store in `dir` instead of the harness-allocated `<root>/node-{id}`.
    ///
    /// `dir` is the caller's to create, populate and delete — see [`ClusterConfig::data_dirs`].
    pub fn data_dir(mut self, id: NodeId, dir: impl Into<PathBuf>) -> Self {
        self.cfg.data_dirs.insert(id, dir.into());
        self
    }

    /// The assembled configuration.
    pub fn config(self) -> ClusterConfig {
        self.cfg
    }

    /// Start the cluster.
    pub async fn start(self) -> Cluster {
        Cluster::start_with(self.cfg).await
    }
}

// ---------------------------------------------------------------------------------------
// Gossip plumbing
// ---------------------------------------------------------------------------------------

/// One node's observation source: an optional real gossip node plus a mutable injected list.
///
/// Both halves exist at once so that [`GossipControl`] behaves identically whether the
/// cluster was built with real gossip or without it — a poison test should not have to know.
#[derive(Default)]
struct SharedGossipSource {
    real: RwLock<Option<Arc<dyn GossipObservationSource>>>,
    injected: RwLock<Vec<ObservedPeerHint>>,
    enabled: AtomicBool,
}

impl SharedGossipSource {
    fn new() -> Self {
        Self {
            real: RwLock::new(None),
            injected: RwLock::new(Vec::new()),
            enabled: AtomicBool::new(true),
        }
    }
}

impl GossipObservationSource for SharedGossipSource {
    fn peers(&self) -> Vec<ObservedPeerHint> {
        if !self.enabled.load(Ordering::SeqCst) {
            return Vec::new();
        }
        let mut out = match self.real.read() {
            Ok(guard) => guard.as_ref().map(|s| s.peers()).unwrap_or_default(),
            Err(poisoned) => poisoned
                .into_inner()
                .as_ref()
                .map(|s| s.peers())
                .unwrap_or_default(),
        };
        let injected = self
            .injected
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        out.extend(injected.iter().cloned());
        out
    }
}

/// Runtime control over what each node observes (test plan §4.1 `gossip()`).
///
/// Injection is the seam M1-30..M1-35 need: a hostile observation must be *delivered* and
/// then rejected, because a hint that never arrived proves nothing (anti-flake rule 11).
#[derive(Clone)]
pub struct GossipControl {
    sources: BTreeMap<NodeId, Arc<SharedGossipSource>>,
    cluster_id: ClusterId,
    recovery_epoch: RecoveryEpoch,
    peer_endpoints: BTreeMap<NodeId, String>,
    client_endpoints: BTreeMap<NodeId, String>,
}

impl std::fmt::Debug for GossipControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GossipControl")
            .field("nodes", &self.sources.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl GossipControl {
    /// A well-formed hint describing node `about`, as its peers would legitimately observe it.
    pub fn truthful_hint(&self, about: NodeId) -> ObservedPeerHint {
        ObservedPeerHint {
            cluster_id: self.cluster_id,
            recovery_epoch: self.recovery_epoch,
            node_id: about,
            peer_endpoint: self.peer_endpoints.get(&about).cloned().unwrap_or_default(),
            client_endpoint: self.client_endpoints.get(&about).cloned(),
            software_version: env!("CARGO_PKG_VERSION").to_string(),
            protocol_version: 1,
            zone: None,
            liveness: Liveness::Alive,
        }
    }

    /// A hint about `about` that violates `spec`.
    ///
    /// `HijackedEndpoint` advertises some *other* node's peer endpoint, which is the realistic
    /// attack: a syntactically valid address that would redirect replication if anyone trusted
    /// gossip (ADR-0003).
    pub fn poisoned_hint(&self, about: NodeId, spec: PoisonSpec) -> ObservedPeerHint {
        let mut hint = self.truthful_hint(about);
        match spec {
            PoisonSpec::WrongClusterId => hint.cluster_id = ClusterId::from_bytes([0xAB; 16]),
            PoisonSpec::WrongNodeId => hint.node_id = NodeId(99),
            PoisonSpec::WrongEpoch => {
                hint.recovery_epoch = RecoveryEpoch(self.recovery_epoch.0.wrapping_add(1))
            }
            PoisonSpec::HijackedEndpoint => {
                let other = self
                    .peer_endpoints
                    .iter()
                    .find(|(id, _)| **id != about)
                    .map(|(_, ep)| ep.clone())
                    .unwrap_or_else(|| "127.0.0.1:0".to_string());
                hint.peer_endpoint = other;
            }
        }
        hint
    }

    /// Make node `observer` see `hint` on its next poll, in addition to whatever else it sees.
    pub fn inject(&self, observer: NodeId, hint: ObservedPeerHint) {
        if let Some(src) = self.sources.get(&observer) {
            src.injected
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(hint);
        }
    }

    /// Inject `hint` into every node's observations.
    pub fn inject_all(&self, hint: ObservedPeerHint) {
        for id in self.sources.keys().copied().collect::<Vec<_>>() {
            self.inject(id, hint.clone());
        }
    }

    /// Inject a `spec`-violating hint about `about` into every node.
    pub fn poison_all(&self, about: NodeId, spec: PoisonSpec) {
        self.inject_all(self.poisoned_hint(about, spec));
    }

    /// Drop every injected hint. Real gossip, if any, keeps flowing.
    pub fn clear(&self) {
        for src in self.sources.values() {
            src.injected
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clear();
        }
    }

    /// Stop node `id` observing anything at all (test plan `gossip: Partitioned`).
    pub fn stop(&self, id: NodeId) {
        if let Some(src) = self.sources.get(&id) {
            src.enabled.store(false, Ordering::SeqCst);
        }
    }

    /// Resume observations on node `id`.
    pub fn resume(&self, id: NodeId) {
        if let Some(src) = self.sources.get(&id) {
            src.enabled.store(true, Ordering::SeqCst);
        }
    }

    /// Stop every node observing anything.
    pub fn stop_all(&self) {
        for id in self.sources.keys().copied().collect::<Vec<_>>() {
            self.stop(id);
        }
    }

    /// What node `id` currently observes.
    pub fn peers(&self, id: NodeId) -> Vec<ObservedPeerHint> {
        self.sources.get(&id).map(|s| s.peers()).unwrap_or_default()
    }
}

// ---------------------------------------------------------------------------------------
// Peer transport
// ---------------------------------------------------------------------------------------

/// Why a node did not come up.
///
/// Starting a node is two fallible steps, and the M2 rows assert on both: the store has to
/// open (identity, an exclusive RocksDB `LOCK`) and the engine has to replay it. A crash
/// injected at an apply boundary fails the *second* step — OpenRaft reports it as
/// [`config_engine::EngineError::Raft`] carrying `"...storage is poisoned"`, not as a
/// [`StorageOpenError`] — so collapsing the two into one variant would make M2-17
/// unassertable.
#[derive(Debug, thiserror::Error)]
pub enum NodeStartError {
    /// The store would not open: a foreign identity, a held lock, or I/O.
    #[error("store did not open: {0}")]
    Storage(#[from] StorageOpenError),
    /// The store opened but the engine would not start on it — above all a replay that
    /// crashed at an injected boundary.
    #[error("engine did not start: {0}")]
    Engine(#[from] config_engine::EngineError),
}

// ---------------------------------------------------------------------------------------
// Node bookkeeping
// ---------------------------------------------------------------------------------------

/// The parts of a node that exist only while it is running.
///
/// Dropping this is what releases RocksDB's exclusive `LOCK` file, so nothing here may be
/// cloned out and kept past a stop (TA-16.1).
struct RunningNode {
    node: ConfigNode,
    store: StorageHandle,
    peer_server: ServerHandle,
    client_server: ServerHandle,
    gossip: Option<Arc<GossipNode>>,
}

/// One cluster member, running or stopped, plus the identity that survives a restart.
struct NodeSlot {
    identity: ClusterIdentity,
    peer_addr: SocketAddr,
    client_addr: SocketAddr,
    span: tracing::Span,
    faults: Arc<dyn FaultInjector>,
    gossip_source: Arc<SharedGossipSource>,
    /// Where this node's persistent data lives, for `StorageKind::Rocks`. Stable across a
    /// restart — that is the whole point.
    data_dir: Option<PathBuf>,
    running: Option<RunningNode>,
}

/// Serves each authenticated principal a [`config_engine::DirectClient`] over one node.
///
/// This is the same wiring `config-server` uses: the client plane holds no state of its own,
/// it just binds the transport-derived principal to the local node.
struct NodeBackend {
    node: ConfigNode,
}

impl ClientBackend for NodeBackend {
    fn store_for(&self, principal: Principal) -> Arc<dyn ConfigStore> {
        Arc::new(self.node.direct_client(principal))
    }

    /// Forward the plane's authentication refusals to the node's counter (M3-81).
    ///
    /// The default is a no-op, which would leave `HealthPayload::authn_rejected` reading zero
    /// however many certificates the listener turned away — an oracle that can only ever say
    /// "fine".
    fn record_authn_rejection(&self) {
        self.node.record_authn_rejection();
    }
}

/// The same value behind the admin plane (M5, ADR-0023, OQ-43): every method forwards to the
/// node, so a harness admin call and a `config-server` admin call reach identical code.
///
/// The one deliberate difference from the daemon's backend is [`AdminBackend::backup`], which
/// reports `Unavailable`. Writing a signed backup triple is `config-server`'s
/// `backup::finish_artifact` — signing keys, manifests, encryption — none of which a
/// `config-testkit` cluster is configured with, and duplicating it here would give the rows
/// that matter (`crates/config-server/tests/m5_backup_cli.rs`) a second, weaker oracle. The
/// refusal is still a *reachable* method, which is what M5-50's "every admin RPC" needs: the
/// allowlist is consulted before the backend, so a denied call never gets this far.
#[async_trait::async_trait]
impl AdminBackend for NodeBackend {
    fn cluster_id(&self) -> ClusterId {
        self.node.identity().cluster_id
    }

    fn membership_report(&self) -> config_engine::MembershipReport {
        self.node.membership_report()
    }

    async fn add_learner(
        &self,
        node_id: NodeId,
        peer_endpoint: String,
        client_endpoint: String,
    ) -> Result<Option<config_engine::LogIdView>, config_engine::AdminError> {
        self.node
            .add_learner(node_id, peer_endpoint, client_endpoint)
            .await
    }

    async fn promote_voter(
        &self,
        node_id: NodeId,
    ) -> Result<Option<config_engine::LogIdView>, config_engine::AdminError> {
        self.node.promote_voter(node_id).await
    }

    async fn remove_member(
        &self,
        node_id: NodeId,
    ) -> Result<Option<config_engine::LogIdView>, config_engine::AdminError> {
        self.node.remove_member(node_id).await
    }

    async fn trigger_snapshot(
        &self,
    ) -> Result<config_engine::SnapshotTriggered, config_engine::AdminError> {
        self.node.trigger_snapshot().await
    }

    async fn backup(
        &self,
        _dest_dir: PathBuf,
        _name: Option<String>,
    ) -> Result<BackupArtifact, config_engine::AdminError> {
        Err(config_engine::AdminError::Unavailable {
            reason: "this harness node has no backup configuration; the backup artifact is \
                     covered by config-server's own rows"
                .to_string(),
        })
    }
}

// ---------------------------------------------------------------------------------------
// Cluster
// ---------------------------------------------------------------------------------------

/// An N-node rEtcd cluster over the real gRPC planes (test plan §4.1).
pub struct Cluster {
    cfg: ClusterConfig,
    slots: Mutex<BTreeMap<NodeId, NodeSlot>>,
    netfault: NetFault,
    gossip: GossipControl,
    /// Owns every node's data directory. Dropped last, after `shutdown` has closed the stores,
    /// because Windows will not delete a directory RocksDB still has open (TA-16.3).
    data_root: Option<tempfile::TempDir>,
    /// Last `compact_revision` observed per node by [`Cluster::assert_journal_invariants`]
    /// (test plan §3.8): the monotonicity half of that helper's contract needs a baseline from
    /// a *previous* call, since a single snapshot cannot tell "never regresses" from "just
    /// compacted for the first time".
    journal_baseline: Mutex<BTreeMap<NodeId, u64>>,
}

impl std::fmt::Debug for Cluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cluster")
            .field("nodes", &self.ids())
            .field("running", &self.running_ids())
            .field("netfault", &self.netfault)
            .finish()
    }
}

/// A registered, authorized watch stream whose consumer never polls it (test plan TA-35.3).
///
/// Produced by [`Cluster::stalled_stream`]. A real stream through the real queue — opened
/// exactly like any other watch — just never driven, so it fills its queue/byte budget under
/// concurrent writes precisely as a genuinely slow client would. Dropping it early is itself a
/// meaningful action (M4-63's "disconnected consumer"); holding it is what M4-62/M4-64's "apply
/// never blocks" rows need while they drive a workload.
pub struct StalledStream {
    _watch: TrackedWatch,
}

impl std::fmt::Debug for StalledStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StalledStream").finish_non_exhaustive()
    }
}

impl Cluster {
    /// A builder over [`ClusterConfig`].
    pub fn builder() -> ClusterBuilder {
        ClusterBuilder::default()
    }

    /// Start `n` nodes, form the cluster, and return once a leader exists and every node
    /// agrees on the voter set (test plan §4.1: "otherwise every test re-implements formation
    /// waiting").
    pub async fn start(n: u64, storage: StorageKind) -> Cluster {
        Cluster::start_with(ClusterConfig {
            nodes: n,
            storage,
            ..ClusterConfig::default()
        })
        .await
    }

    /// Start a cluster from an explicit configuration.
    ///
    /// Forms and waits for a leader unless [`ClusterConfig::form`] is `false`.
    pub async fn start_with(cfg: ClusterConfig) -> Cluster {
        assert!(cfg.nodes >= 1, "a cluster needs at least one node");
        if let Some(fixture) = cfg.tls.fixture() {
            assert_eq!(
                fixture.cluster_id(),
                cfg.cluster_id,
                "the TLS fixture mints SANs for another cluster than the nodes are bound to; build it with ClusterBuilder::mutual_tls or ClusterTls::mutual(cfg.cluster_id, _)"
            );
        }

        // One TempDir for the whole cluster, with a directory per node inside it. Per-node
        // TempDirs would be dropped independently and a restart could race the deletion.
        let data_root = cfg.storage.is_persistent().then(crate::fs::temp_dir);

        let netfault = NetFault::new();

        // Bind every listener first. The peer addresses go into the FormationPlan, and the
        // plan's strings are what peers dial and what a NotLeader hint names forever after.
        let mut pending = Vec::new();
        let mut peer_endpoints = BTreeMap::new();
        let mut client_endpoints = BTreeMap::new();
        for i in 1..=cfg.nodes {
            let id = NodeId(i);
            let (peer_listener, peer_addr) = ephemeral_listener().await;
            let (client_listener, client_addr) = ephemeral_listener().await;
            peer_endpoints.insert(id, peer_addr.to_string());
            client_endpoints.insert(id, client_addr.to_string());
            pending.push((id, peer_listener, peer_addr, client_listener, client_addr));
        }

        let mut slots = BTreeMap::new();
        let mut sources = BTreeMap::new();
        for (id, peer_listener, peer_addr, client_listener, client_addr) in pending {
            let identity = cfg.identity(id);
            let gossip_source = Arc::new(SharedGossipSource::new());
            sources.insert(id, Arc::clone(&gossip_source));
            // Child of the test span, so every line this node emits — engine, storage,
            // OpenRaft core, and both gRPC planes — carries testMethod *and* node_id.
            let span = tracing::info_span!(
                "cluster_node",
                node_id = id.0,
                peer_endpoint = %peer_addr,
                client_endpoint = %client_addr,
            );
            let faults = cfg
                .faults
                .get(&id)
                .cloned()
                .unwrap_or_else(|| Arc::new(NoFaults) as Arc<dyn FaultInjector>);
            // A caller-supplied directory wins over the harness-allocated one, and wins even
            // when there is no `data_root` to allocate from — that is the whole point of
            // handing the harness a directory somebody else wrote (M5-92).
            let data_dir = cfg.data_dirs.get(&id).cloned().or_else(|| {
                data_root
                    .as_ref()
                    .map(|root| root.path().join(format!("node-{}", id.0)))
            });

            let transport = peer_transport_for(&cfg, id, &netfault);

            let running = start_running(
                &cfg,
                identity,
                &span,
                Arc::clone(&faults),
                Arc::clone(&gossip_source) as Arc<dyn GossipObservationSource>,
                transport,
                peer_listener,
                client_listener,
                data_dir.as_deref(),
                Duration::ZERO,
                cfg.gossip_poll,
            )
            .await
            .expect("opening a fresh data directory");

            slots.insert(
                id,
                NodeSlot {
                    identity,
                    peer_addr,
                    client_addr,
                    span,
                    faults,
                    gossip_source,
                    data_dir,
                    running: Some(running),
                },
            );
        }

        let gossip = GossipControl {
            sources,
            cluster_id: cfg.cluster_id,
            recovery_epoch: cfg.recovery_epoch,
            peer_endpoints,
            client_endpoints,
        };

        let cluster = Cluster {
            cfg,
            slots: Mutex::new(slots),
            netfault,
            gossip,
            data_root,
            journal_baseline: Mutex::new(BTreeMap::new()),
        };

        cluster.configure_gossip().await;

        if cluster.cfg.form {
            cluster.form().await.expect("formation of a fresh cluster");
            cluster
                .wait_formed(cluster.deadline(10))
                .await
                .expect("a leader and agreed membership after formation");
        }
        cluster
    }

    /// Apply the configured [`GossipKind`] now that every endpoint is known.
    async fn configure_gossip(&self) {
        match self.cfg.gossip.clone() {
            GossipKind::Disabled => {}
            GossipKind::Partitioned => self.gossip.stop_all(),
            GossipKind::Static(hints) => {
                for hint in hints {
                    self.gossip.inject_all(hint);
                }
            }
            GossipKind::Poisoned(spec) => {
                // Aim at node 1 unless it is the only node; a hint about a real voter is the
                // interesting case, because an unknown id is refused before endpoints matter.
                let about = self.ids().into_iter().next().unwrap_or(NodeId(1));
                self.gossip.poison_all(about, spec);
            }
            GossipKind::Real => self.start_real_gossip().await,
        }
    }

    /// Stand up one [`GossipNode`] per cluster node on `127.0.0.1:0`, all seeded to node 1.
    async fn start_real_gossip(&self) {
        let ids = self.ids();
        let mut seeds: Vec<SocketAddr> = Vec::new();
        for id in ids {
            let (identity, span, source, peer_ep, client_ep) = {
                let slots = self.lock();
                let slot = &slots[&id];
                (
                    slot.identity,
                    slot.span.clone(),
                    Arc::clone(&slot.gossip_source),
                    slot.peer_addr.to_string(),
                    slot.client_addr.to_string(),
                )
            };
            let mut gcfg = GossipConfig::new(
                identity.cluster_id,
                id,
                "127.0.0.1:0".parse().expect("loopback ephemeral addr"),
            );
            gcfg.seeds = seeds.clone();
            let hint = ObservedPeerHint {
                cluster_id: identity.cluster_id,
                recovery_epoch: identity.recovery_epoch,
                node_id: id,
                peer_endpoint: peer_ep,
                client_endpoint: Some(client_ep),
                software_version: env!("CARGO_PKG_VERSION").to_string(),
                protocol_version: 1,
                zone: None,
                liveness: Liveness::Alive,
            };
            let guard = span.enter();
            let node = GossipNode::start(gcfg, hint)
                .await
                .expect("gossip node start on an ephemeral port");
            drop(guard);
            if seeds.is_empty() {
                seeds.push(node.advertise_addr());
            }
            let node = Arc::new(node);
            *source
                .real
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                Some(Arc::clone(&node) as Arc<dyn GossipObservationSource>);
            let mut slots = self.lock();
            if let Some(running) = slots.get_mut(&id).and_then(|s| s.running.as_mut()) {
                running.gossip = Some(node);
            }
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<NodeId, NodeSlot>> {
        self.slots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    // ------------------------------- configuration -------------------------------

    /// The configuration this cluster was started with.
    pub fn config(&self) -> &ClusterConfig {
        &self.cfg
    }

    /// The timers the nodes run with and every deadline is derived from.
    pub fn timers(&self) -> TestTimers {
        self.cfg.timers
    }

    /// A deadline worth `n` worst-case election timeouts (anti-flake rule 3).
    pub fn deadline(&self, n: u32) -> Duration {
        self.cfg.timers.multiple(n)
    }

    /// The interval harness waits poll at: a fifth of a heartbeat, so a state change is
    /// observed promptly without spinning.
    pub fn poll_interval(&self) -> Duration {
        (self.cfg.timers.heartbeat / 5).max(Duration::from_millis(5))
    }

    /// Every configured node id, ascending.
    pub fn ids(&self) -> Vec<NodeId> {
        self.lock().keys().copied().collect()
    }

    /// The ids of the nodes that are currently running.
    pub fn running_ids(&self) -> Vec<NodeId> {
        self.lock()
            .iter()
            .filter(|(_, s)| s.running.is_some())
            .map(|(id, _)| *id)
            .collect()
    }

    /// The identity node `id` is bound to.
    pub fn identity(&self, id: NodeId) -> ClusterIdentity {
        self.lock()[&id].identity
    }

    /// The committed-membership peer endpoint of node `id` (`host:port`).
    pub fn peer_endpoint(&self, id: NodeId) -> String {
        self.lock()[&id].peer_addr.to_string()
    }

    /// The client-plane endpoint of node `id` (`host:port`).
    pub fn client_endpoint(&self, id: NodeId) -> String {
        self.lock()[&id].client_addr.to_string()
    }

    /// Every node's client-plane endpoint, ascending by id.
    pub fn client_endpoints(&self) -> Vec<String> {
        self.lock()
            .values()
            .map(|s| s.client_addr.to_string())
            .collect()
    }

    // ------------------------------- formation -------------------------------

    /// Form the cluster from node 1 with every configured node as a voter.
    ///
    /// The plan's endpoints are the bound peer addresses, which is why listeners are created
    /// before this is called.
    pub async fn form(&self) -> Result<(), config_engine::FormationError> {
        let plan = self.formation_plan();
        self.node(NodeId(1)).form_cluster(plan).await
    }

    /// The plan [`Cluster::form`] would submit: this cluster's identity and, for every node,
    /// both the peer endpoint its peers dial and the client endpoint a leader hint names.
    ///
    /// Both planes come out of this one plan, so a hint can never name an address the cluster
    /// did not commit to (ADR-0009, engine delta §2).
    pub fn formation_plan(&self) -> FormationPlan {
        self.formation_plan_of(&self.ids())
    }

    /// Form the cluster from its lowest member of `ids`, with exactly `ids` as voters.
    ///
    /// M3 §4.1 needs this: a node whose certificate is rejected can never join, so the rows
    /// that assert "the other two keep quorum" must commit a membership that excludes it.
    pub async fn form_with(&self, ids: &[NodeId]) -> Result<(), config_engine::FormationError> {
        let plan = self.formation_plan_of(ids);
        let first = *ids.iter().min().expect("a plan needs at least one voter");
        self.node(first).form_cluster(plan).await
    }

    /// [`Cluster::formation_plan`] restricted to `ids`.
    pub fn formation_plan_of(&self, ids: &[NodeId]) -> FormationPlan {
        let wanted: BTreeSet<NodeId> = ids.iter().copied().collect();
        let voters: Vec<(NodeId, String, String)> = self
            .lock()
            .iter()
            .filter(|(id, _)| wanted.contains(id))
            .map(|(id, slot)| {
                (
                    *id,
                    slot.peer_addr.to_string(),
                    slot.client_addr.to_string(),
                )
            })
            .collect();
        FormationPlan::with_client_endpoints(&self.cfg.identity(NodeId(1)), voters)
    }

    /// Wait until a leader exists and every running node has **committed** the same membership.
    ///
    /// Committed, not effective. `NodeMetrics::membership_voter_ids` moves as soon as the
    /// membership entry is *appended*, while `committed_membership()` reads the state machine
    /// and therefore lags it by one apply (engine delta §4.1). A harness that waited on the
    /// effective set and then read committed membership would race — and every M1 row that
    /// asserts on `membership_of` runs straight after this wait.
    pub async fn wait_formed(&self, deadline: Duration) -> Result<NodeId, Timeout> {
        self.wait_formed_on(&self.ids(), deadline).await
    }

    /// [`Cluster::wait_formed`] over exactly `ids`: they must all be running, agree on a
    /// committed membership whose voter set is `ids`, and have a leader.
    ///
    /// Nodes outside `ids` are ignored, which is what a row with one deliberately broken node
    /// needs (M3-13).
    pub async fn wait_formed_on(
        &self,
        ids: &[NodeId],
        deadline: Duration,
    ) -> Result<NodeId, Timeout> {
        let expected: BTreeSet<NodeId> = ids.iter().copied().collect();
        self.wait_for(
            "a leader and identical committed membership on every expected node",
            deadline,
            || {
                let running: Vec<NodeId> = self
                    .running_ids()
                    .into_iter()
                    .filter(|id| expected.contains(id))
                    .collect();
                if running.len() != expected.len() {
                    return None;
                }
                let leader = self.leader_now()?;
                let views: Vec<config_engine::MembershipView> =
                    running.iter().map(|id| self.membership_of(*id)).collect();
                let first = views[0].membership_log_id;
                views
                    .iter()
                    .all(|v| {
                        v.voters == expected
                            && v.membership_log_id == first
                            && v.membership_log_id.is_some()
                    })
                    .then_some(leader)
            },
        )
        .await
    }

    // ------------------------------- discovery -------------------------------

    /// The current leader, waiting up to eight election timeouts. Panics on timeout with
    /// every node's metrics.
    pub async fn leader(&self) -> NodeId {
        self.wait_for_leader(self.deadline(8))
            .await
            .unwrap_or_else(|t| panic!("{t}"))
    }

    /// Wait until some running node reports itself leader.
    ///
    /// Returns `Err(Timeout)` rather than panicking, so a test can assert "no leader was ever
    /// elected" as a positive result (test plan TA-6, M1-01).
    pub async fn wait_for_leader(&self, deadline: Duration) -> Result<NodeId, Timeout> {
        self.wait_for("a leader on some node", deadline, || self.leader_now())
            .await
    }

    /// Poll for a leader for at most `deadline`, returning `None` if none appears.
    pub async fn try_leader(&self, deadline: Duration) -> Option<NodeId> {
        self.wait_for_leader(deadline).await.ok()
    }

    /// The leader as of right now, without waiting.
    pub fn leader_now(&self) -> Option<NodeId> {
        self.running_metrics()
            .into_iter()
            .find(|m| m.role == NodeRole::Leader)
            .map(|m| m.node_id)
    }

    /// *Every* running node that currently calls itself leader, ascending by id.
    ///
    /// [`Cluster::leader_now`] returns the lowest one, which is the wrong oracle for a forced
    /// election: an isolated leader keeps reporting `Leader` until it learns of a higher term,
    /// so "the first node in `Leader` role" can stay the *old* leader indefinitely while a new
    /// one is already serving the majority. A row that isolates a leader and waits for its
    /// successor must look for a leader it does not already know, not for the only one.
    pub fn leaders_now(&self) -> Vec<NodeId> {
        self.running_metrics()
            .into_iter()
            .filter(|m| m.role == NodeRole::Leader)
            .map(|m| m.node_id)
            .collect()
    }

    /// Every running node that is not the current leader.
    pub fn followers(&self) -> Vec<NodeId> {
        let leader = self.leader_now();
        self.running_ids()
            .into_iter()
            .filter(|id| Some(*id) != leader)
            .collect()
    }

    /// A handle on node `id`.
    ///
    /// Returns a clone rather than a reference (the plan writes `&ConfigNode`): a node is
    /// replaced by [`Cluster::start_node`], so the map behind it must be mutable.
    /// `ConfigNode` is an `Arc` handle, so cloning is not a copy of the node.
    pub fn node(&self, id: NodeId) -> ConfigNode {
        self.try_node(id)
            .unwrap_or_else(|| panic!("node {id} is not running"))
    }

    /// A handle on node `id`, or `None` if it is stopped.
    pub fn try_node(&self, id: NodeId) -> Option<ConfigNode> {
        self.lock()
            .get(&id)
            .and_then(|s| s.running.as_ref())
            .map(|r| r.node.clone())
    }

    /// Node `id`'s metrics snapshot.
    pub fn metrics(&self, id: NodeId) -> NodeMetrics {
        self.node(id).metrics()
    }

    /// Node `id`'s advertised capabilities (ADR-0016).
    pub fn capabilities(&self, id: NodeId) -> Capabilities {
        self.node(id).capabilities()
    }

    /// Node `id`'s committed membership view.
    pub fn membership_of(&self, id: NodeId) -> config_engine::MembershipView {
        self.node(id).committed_membership()
    }

    /// Committed membership as node 1 sees it.
    pub fn membership(&self) -> config_engine::MembershipView {
        self.membership_of(NodeId(1))
    }

    /// What node `id` currently observes over gossip.
    pub fn gossip_peers(&self, id: NodeId) -> Vec<ObservedPeerHint> {
        self.gossip.peers(id)
    }

    /// Node `id`'s store handle, whichever backend it runs on.
    ///
    /// [`StorageHandle`] exposes what a test actually asserts on — durability, applied command
    /// count, log length, poison state, and the per-boundary fault counters — so a row that
    /// only reads those needs no backend-specific code and can run over both (M2-10).
    ///
    /// The returned handle keeps the store open. Drop it before [`Cluster::restart`], or
    /// RocksDB's `LOCK` stays held.
    pub fn store(&self, id: NodeId) -> StorageHandle {
        self.lock()
            .get(&id)
            .and_then(|s| s.running.as_ref())
            .map(|r| r.store.clone())
            .unwrap_or_else(|| panic!("node {id} is not running"))
    }

    /// Node `id`'s store as an [`EphemeralStore`]. Panics on a Rocks cluster.
    pub fn ephemeral_store(&self, id: NodeId) -> EphemeralStore {
        match self.store(id) {
            StorageHandle::Ephemeral(s) => s,
            other => panic!("node {id} does not run on EphemeralStore: {other:?}"),
        }
    }

    /// Node `id`'s store as a [`RocksStore`]. Panics on an Ephemeral cluster.
    ///
    /// Holding the returned clone blocks [`Cluster::restart`]; drop it first.
    pub fn rocks_store(&self, id: NodeId) -> RocksStore {
        match self.store(id) {
            StorageHandle::Rocks(s) => s,
            other => panic!("node {id} does not run on RocksStore: {other:?}"),
        }
    }

    /// Where node `id` keeps its persistent data.
    ///
    /// Returns an owned path rather than the plan's `&Path`: the slot lives behind a mutex, so
    /// a borrow could not outlive the lock guard. The path is stable across a restart.
    pub fn data_dir(&self, id: NodeId) -> PathBuf {
        self.lock()
            .get(&id)
            .and_then(|s| s.data_dir.clone())
            .unwrap_or_else(|| panic!("node {id} has no data directory (Ephemeral storage)"))
    }

    /// The fault injector node `id` was configured with (TA-15).
    ///
    /// Survives a restart: the injector belongs to the node, not to one open of its store, so a
    /// crash armed before a restart is still armed after it (M2-17).
    pub fn injector(&self, id: NodeId) -> Arc<dyn FaultInjector> {
        self.lock()
            .get(&id)
            .map(|s| Arc::clone(&s.faults))
            .unwrap_or_else(|| panic!("node {id} was never configured"))
    }

    /// Per-boundary crossing counters for node `id`'s **current** store open (TA-15).
    ///
    /// Per open, not per directory: a reopened store starts its counters at zero, which is what
    /// makes "how many syncs did *this* run do" answerable.
    pub fn counters(&self, id: NodeId) -> Arc<FaultCounters> {
        self.store(id).counters()
    }

    /// What node `id`'s storage actually guarantees survives a restart (ADR-0016).
    pub fn durability(&self, id: NodeId) -> Durability {
        self.store(id).durability()
    }

    /// Node `id`'s local applied-state digest, with no leader check and no Raft barrier.
    ///
    /// Deliberately node-local: comparing a follower against the leader is the whole point of
    /// the convergence oracle (TA-2, TA-16.2).
    pub fn state_hash(&self, id: NodeId) -> [u8; 32] {
        self.node(id).state_hash()
    }

    /// Node `id`'s health payload — the cross-process state oracle (TA-17).
    pub async fn health(&self, id: NodeId) -> HealthPayload {
        self.node(id).health_payload().await
    }

    /// Open node `id`'s data directory a second time, for read-only inspection while the node
    /// is stopped (TA-16).
    ///
    /// Returns [`StorageOpenError::Locked`] if the node is still running or if any store clone
    /// is still alive — which is exactly the invariant a restart depends on, so a test can
    /// assert it directly instead of trusting a comment.
    ///
    /// The returned store must be dropped before the node is started again.
    pub fn reopen_store(&self, id: NodeId) -> Result<RocksStore, StorageOpenError> {
        let (dir, identity, span) = {
            let slots = self.lock();
            let slot = slots
                .get(&id)
                .unwrap_or_else(|| panic!("node {id} was never configured"));
            let dir = slot
                .data_dir
                .clone()
                .unwrap_or_else(|| panic!("node {id} has no data directory (Ephemeral storage)"));
            (dir, slot.identity, slot.span.clone())
        };
        let options = self
            .cfg
            .storage
            .rocks_options(false)
            .expect("a data directory implies Rocks storage");
        RocksStore::open_with(
            &dir,
            identity,
            self.cfg.limits,
            Arc::new(NoFaults),
            tracing::info_span!(parent: span, "reopened_store", node_id = id.0),
            options,
            // A store opened only to be *inspected* publishes to nothing: this is the
            // crash-recovery reader, not a running node, and a hub attached here would have no
            // reader and no authorizer behind it.
            Arc::new(config_storage::NoopSink),
        )
    }

    /// The invariants that must hold on node `id` after an injected crash and a restart
    /// (test plan §3.3).
    ///
    /// Panics with the node's metrics on violation. This is the shared half of every crash row;
    /// the row itself still has to assert what its own boundary makes true.
    ///
    /// **Call it after the node has rejoined**, not the instant `restart` returns. A freshly
    /// restarted node has its state machine loaded from disk but has not yet had its first
    /// `RaftMetrics` published, so `last_applied` reads as `None` while `cluster_revision`
    /// already reflects the recovered state — which looks exactly like a violation. Wait on
    /// [`Cluster::wait_applied_all`] or [`Cluster::wait_converged`] first; the helper asserts a
    /// steady state, not a transient one, and it says so rather than sleeping past it.
    pub fn assert_crash_invariants(&self, id: NodeId) {
        let m = self.metrics(id);
        let store = self.store(id);
        assert!(
            m.last_log_index.is_some() || store.raft_log_len() == 0,
            "node {id} holds {} log entries but has not published metrics yet: call assert_crash_invariants after the node has rejoined, not the instant restart returns: {m:?}",
            store.raft_log_len()
        );
        let last_applied = m.last_applied.map(|l| l.index).unwrap_or(0);
        let last_log_index = m.last_log_index.unwrap_or(0);

        // A state machine ahead of the log is the startup trap from research §8.2: openraft
        // would then try to replay entries that do not exist.
        assert!(
            last_log_index >= last_applied,
            "node {id} has last_applied {last_applied} past last_log_index {last_log_index}: {m:?}"
        );

        // The log must be contiguous from the first retained entry to the last.
        assert!(
            store.raft_log_len() <= last_log_index.saturating_add(1),
            "node {id} holds {} log entries but its last index is {last_log_index}: {m:?}",
            store.raft_log_len()
        );

        // A restart that replayed correctly is not poisoned and the core is running.
        assert!(
            !store.is_poisoned(),
            "node {id} is still poisoned after a restart; reopen_store was skipped: {m:?}"
        );
        assert!(
            m.running_state_ok,
            "node {id}'s Raft core is not running after a restart: {m:?}"
        );

        // The public revision is derived from applied state, so it cannot outrun it.
        assert!(
            m.cluster_revision <= last_applied,
            "node {id} reports revision {} with only {last_applied} applied entries: {m:?}",
            m.cluster_revision
        );
        assert!(
            m.applied_commands <= last_applied,
            "node {id} applied {} commands in {last_applied} entries: {m:?}",
            m.applied_commands
        );
    }

    /// Metrics for every running node, ascending by id.
    pub fn running_metrics(&self) -> Vec<NodeMetrics> {
        self.lock()
            .values()
            .filter_map(|s| s.running.as_ref())
            .map(|r| r.node.metrics())
            .collect()
    }

    /// The applied-state hash of every running node (test plan TA-2): the convergence oracle.
    pub fn state_hashes(&self) -> BTreeMap<NodeId, [u8; 32]> {
        self.lock()
            .iter()
            .filter_map(|(id, s)| s.running.as_ref().map(|r| (*id, r.node.state_hash())))
            .collect()
    }

    // ------------------------------- watches (M4) -------------------------------

    /// Open a watch on node `id` through its embedded client, as the development principal.
    ///
    /// Direct rather than gRPC on purpose: a row that is about the *engine* — a replay
    /// boundary, an admission limit, a gate interleaving — should fail because the engine is
    /// wrong, not because a codec is. The gRPC half of the same row uses
    /// [`Cluster::watch_grpc`], and the conformance suite runs both.
    pub async fn watch(
        &self,
        id: NodeId,
        request: WatchRequest,
    ) -> Result<TrackedWatch, config_core::ConfigError> {
        self.watch_as(id, Principal::development(), request).await
    }

    /// Open a watch on node `id` as `principal` (test plan M4-103).
    pub async fn watch_as(
        &self,
        id: NodeId,
        principal: Principal,
        request: WatchRequest,
    ) -> Result<TrackedWatch, config_core::ConfigError> {
        self.node(id)
            .direct_client(principal)
            .watch_tracked(request)
            .await
    }

    /// Open a watch on node `id` over the wire, as the development principal.
    ///
    /// The client is pinned and makes exactly one attempt: a watch never shops for a leader
    /// (ADR-0015, test plan M4-108), so a row that expects `NotLeader` gets `NotLeader`.
    pub async fn watch_grpc(
        &self,
        id: NodeId,
        request: WatchRequest,
    ) -> Result<TrackedWatch, config_core::ConfigError> {
        self.grpc_client(id).watch_tracked(request).await
    }

    /// Node `id`'s watch counters.
    pub fn watch_stats(&self, id: NodeId) -> WatchStats {
        self.node(id).watch_stats()
    }

    /// Node `id`'s journal-gate test seam (test plan TA-30).
    ///
    /// The one way a row can pin a registration or a compaction at a named point and assert
    /// what the other one does — without which "registration and compaction are serialized"
    /// would only ever be tested by racing them and hoping (anti-flake rule 1).
    pub fn gate(&self, id: NodeId) -> GateHandle {
        self.node(id).watch_hub().testing()
    }

    /// What node `id` currently retains, as its retention task sees it.
    pub fn journal(&self, id: NodeId) -> JournalView {
        self.node(id).journal_view()
    }

    /// Node `id`'s compaction floor: the oldest revision a watch may still resume from.
    pub fn compact_revision(&self, id: NodeId) -> u64 {
        self.node(id).compact_revision()
    }

    /// The clock every node's retention task reads.
    ///
    /// Shared across the cluster, which is right for a harness: a row that advances it is
    /// saying "time passed", not "time passed on node 2".
    pub fn leader_clock(&self) -> Arc<ManualClock> {
        Arc::clone(&self.cfg.clock)
    }

    /// Propose a compaction to `up_to` through the normal write path and wait for it to apply.
    ///
    /// Goes to the leader, because compaction is a replicated command like any other: a
    /// follower proposing one would be the split-brain the whole design forbids (ADR-0019).
    pub async fn compact_now(&self, up_to: u64) -> Result<u64, config_core::ConfigError> {
        self.compact_now_as(&Principal::development(), up_to).await
    }

    /// [`Cluster::compact_now`], authorizing as `principal` instead of the insecure
    /// [`Principal::development`] identity.
    ///
    /// Compaction checks `Action::Write` against the empty prefix — "the whole keyspace"
    /// (`config_engine::node::NodeInner::propose_compact`) — so under a real
    /// [`AuthzKind::Static`] policy, `Principal::development()` is refused outright (its kind
    /// is never a verified one, ADR-0012) and even a verified principal needs a grant whose
    /// prefix is `""`. A row exercising compaction under mTLS/static-authz (e.g. M4-98's watch
    /// conformance fixture) calls this with such a principal instead.
    pub async fn compact_now_as(
        &self,
        principal: &Principal,
        up_to: u64,
    ) -> Result<u64, config_core::ConfigError> {
        let leader = self.leader().await;
        self.node(leader).propose_compact(principal, up_to).await
    }

    /// Register a real, authorized watch on node `id` and hand it back **without ever
    /// polling it** (test plan TA-35.3).
    ///
    /// A row that needs "apply never blocks on a slow watcher" (M4-62..M4-71) holds the
    /// returned [`StalledStream`] for as long as the queue should keep filling, then either
    /// drops it (simulating a disconnected consumer, M4-63) or lets the hub terminate it for
    /// overload and observes that termination through [`Cluster::watch_stats`].
    ///
    /// Deliberately `async fn -> Result<..>` rather than the harness sketch's bare
    /// `fn -> StalledStream` (test plan §6): registration itself is async and fallible (an
    /// admission or authorization denial must be observable, not panicked past), and a
    /// "stalled" stream is only interesting once it is actually registered.
    pub async fn stalled_stream(
        &self,
        id: NodeId,
        principal: Principal,
        request: WatchRequest,
    ) -> Result<StalledStream, config_core::ConfigError> {
        let watch = self.watch_as(id, principal, request).await?;
        Ok(StalledStream { _watch: watch })
    }

    /// The shared crash-matrix assertion for every M4 journal fault row (test plan §3.8,
    /// M4-89..M4-96): every running node's retained journal is contiguous from
    /// `compact_revision + 1` through its own `cluster_revision`, every node's `journal_hash`
    /// agrees above the cluster's highest `compact_revision` (TA-31 — below their own
    /// watermarks, two correct nodes are entitled to differ), and no node's `compact_revision`
    /// has regressed since the last call on this `Cluster`.
    ///
    /// Call after `wait_applied_all`/`wait_converged` so "contiguous" is checked against
    /// settled state, not a node still catching up.
    pub fn assert_journal_invariants(&self) {
        let ids = self.running_ids();
        assert!(
            !ids.is_empty(),
            "assert_journal_invariants: no running nodes to check"
        );

        let mut baseline = self
            .journal_baseline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let mut max_compact = 0u64;
        for id in &ids {
            let cr = self.node(*id).compact_revision();
            if let Some(&prev) = baseline.get(id) {
                assert!(
                    cr >= prev,
                    "node {id}: compact_revision regressed from {prev} to {cr}"
                );
            }
            baseline.insert(*id, cr);
            max_compact = max_compact.max(cr);

            let stats = self.node(*id).journal_view();
            if stats.count > 0 {
                assert_eq!(
                    stats.oldest_revision,
                    cr + 1,
                    "node {id}: retained journal must start at compact_revision+1"
                );
                let reader = self.store(*id).reader();
                let cluster_revision = reader.cluster_revision();
                assert_eq!(
                    stats.newest_revision, cluster_revision,
                    "node {id}: retained journal must run up to cluster_revision"
                );
                let events = reader
                    .read_events(cr, cluster_revision, &[], usize::MAX)
                    .unwrap_or_else(|e| panic!("node {id}: read_events for invariant check: {e}"));
                assert_eq!(
                    events.len() as u64,
                    stats.count,
                    "node {id}: journal_stats().count disagrees with read_events' own count"
                );
                let mut prev_rev = cr;
                for ev in &events {
                    assert!(
                        ev.revision > prev_rev,
                        "node {id}: journal revisions must be strictly ascending with no gap"
                    );
                    prev_rev = ev.revision;
                }
                assert_eq!(
                    prev_rev, cluster_revision,
                    "node {id}: retained journal has a gap before cluster_revision"
                );
            }
        }
        drop(baseline);

        let hashes: Vec<(NodeId, [u8; 32])> = ids
            .iter()
            .map(|id| (*id, self.node(*id).journal_hash(max_compact)))
            .collect();
        if let Some((_, first)) = hashes.first() {
            for (id, h) in &hashes {
                assert_eq!(
                    h, first,
                    "node {id}: journal_hash above the shared compact_revision floor \
                     ({max_compact}) disagrees with node {}",
                    hashes[0].0
                );
            }
        }
    }

    // ------------------------------- clients -------------------------------

    /// A [`ConfigStore`] over node `id`'s embedded client, authorized as the development
    /// principal.
    pub fn client(&self, id: NodeId) -> Arc<dyn ConfigStore> {
        self.client_as(id, Principal::development())
    }

    /// A [`ConfigStore`] over node `id`'s embedded client, authorized as `principal`.
    pub fn client_as(&self, id: NodeId, principal: Principal) -> Arc<dyn ConfigStore> {
        Arc::new(self.node(id).direct_client(principal))
    }

    /// A gRPC client pinned to node `id`'s client plane (it still follows leader hints, but
    /// its first attempt always goes to `id`).
    pub fn grpc_client(&self, id: NodeId) -> config_client::GrpcClient {
        self.grpc_client_multi()
            .pinned(&self.client_endpoint(id))
            .expect("the pinned endpoint is one of the configured ones")
    }

    /// A gRPC client whose first attempt goes to the node that is leader right now.
    ///
    /// # Why this exists
    ///
    /// `LeaderHint::endpoint` is documented as the leader's **client-plane** endpoint, but the
    /// engine builds it from committed membership, which records **peer** endpoints
    /// (`FormationPlan::voters`). A [`config_client::GrpcClient`] configured with client
    /// endpoints therefore never recognises the hinted endpoint as one of its own and stops
    /// with `NotLeader` instead of following it — see the crate-level note in the M1 handoff.
    /// Until the engine carries both endpoints, a conformance run over gRPC must start at the
    /// leader; [`Cluster::grpc_client_multi`] is kept unpinned so a test can still observe the
    /// `NotLeader` behaviour directly.
    pub async fn grpc_client_at_leader(&self) -> config_client::GrpcClient {
        let leader = self.leader().await;
        self.grpc_client(leader)
    }

    /// A gRPC client over every node's client plane, unpinned: it starts at node 1 and
    /// follows leader hints whose endpoint is one of the configured ones (ADR-0009).
    pub fn grpc_client_multi(&self) -> config_client::GrpcClient {
        // On an mTLS cluster there is no such thing as "no certificate", so the unqualified
        // client presents the development principal, matching `Cluster::client`.
        if self.cfg.tls.is_mutual() {
            return self.grpc_client_multi_tls(&Principal::development().name);
        }
        config_client::GrpcClient::connect(
            self.client_endpoints(),
            config_client::GrpcClientOptions {
                max_hint_follows: 3,
                request_deadline: self.cfg.read_timeout,
                tls: TlsMode::Insecure,
                expected_capabilities: None,
                limits: self.cfg.limits,
            },
        )
        .expect("a client over the cluster's own client endpoints")
    }

    /// A gRPC client over every node's client plane, presenting the client certificate of
    /// `principal_name` (M3 §4.2).
    ///
    /// The certificate carries `retcd://<cluster>/client/<name>`, so the server derives the
    /// principal from the SAN URI; no [`Principal`] is passed in-band. Panics on an insecure
    /// cluster, where there is no certificate to present.
    pub fn grpc_client_multi_tls(&self, principal_name: &str) -> config_client::GrpcClient {
        self.try_grpc_client_multi_tls(principal_name)
            .expect("a TLS client over the cluster's own client endpoints")
    }

    /// An admin-plane client over node `id`'s client listener, presenting `principal_name`'s
    /// certificate (test plan TA-45).
    ///
    /// The admin service shares the client-plane listener and its certificate profile (OQ-43),
    /// so this is deliberately the *same* connection an ordinary data client would make — the
    /// only thing separating the two is [`ClusterConfig::admins`]. Panics on an insecure
    /// cluster, like every other `*_tls` constructor here.
    pub fn admin(&self, id: NodeId, principal_name: &str) -> config_client::AdminClient {
        config_client::AdminClient::new(self.grpc_client_tls(id, principal_name))
    }

    /// [`Cluster::admin`] pointed at whichever node is leader now (TA-45).
    pub async fn admin_at_leader(&self, principal_name: &str) -> config_client::AdminClient {
        let leader = self.leader().await;
        self.admin(leader, principal_name)
    }

    /// [`Cluster::grpc_client_multi_tls`], pinned to node `id`.
    pub fn grpc_client_tls(&self, id: NodeId, principal_name: &str) -> config_client::GrpcClient {
        self.grpc_client_multi_tls(principal_name)
            .pinned(&self.client_endpoint(id))
            .expect("the pinned endpoint is one of the configured ones")
    }

    /// The fallible form, for rows that assert the *client side* refuses a configuration.
    pub fn try_grpc_client_multi_tls(
        &self,
        principal_name: &str,
    ) -> Result<config_client::GrpcClient, config_client::ClientError> {
        let pair = self.fixture().issue(CertProfile::client(principal_name));
        self.try_grpc_client_with_tls(None, TlsMode::MutualTls(pair.mtls()))
    }

    /// A gRPC client pinned to node `id`, presenting a certificate this cluster's fixture
    /// issued for `profile` with `overrides` applied — the §4.2 negative rows.
    ///
    /// Covers "wrong cluster id", "expired", "node certificate on the client plane" and
    /// "missing SAN, CN fallback" by varying `profile`/`overrides` alone. A foreign CA needs a
    /// different fixture, so it goes through [`Cluster::grpc_client_with_tls`]; plaintext to a
    /// TLS listener goes through [`Cluster::grpc_client_plaintext`].
    ///
    /// Handshake failures surface on the first *request*, not here: tonic connects lazily, so
    /// `Ok` from this call says only that the client configuration itself is usable.
    pub fn grpc_client_with_cert(
        &self,
        id: NodeId,
        profile: CertProfile,
        overrides: CertOverrides,
    ) -> Result<config_client::GrpcClient, config_client::ClientError> {
        let pair = self.fixture().issue_with(profile, overrides);
        self.try_grpc_client_with_tls(Some(id), TlsMode::MutualTls(pair.mtls()))
    }

    /// A gRPC client pinned to node `id` with a caller-supplied TLS profile: the escape hatch
    /// for material this cluster's fixture cannot mint, above all a foreign CA
    /// ([`TlsFixture::other_ca`]).
    pub fn grpc_client_with_tls(
        &self,
        id: NodeId,
        tls: TlsMode,
    ) -> Result<config_client::GrpcClient, config_client::ClientError> {
        self.try_grpc_client_with_tls(Some(id), tls)
    }

    /// A plaintext gRPC client pointed at node `id`'s TLS client plane (the "plaintext to a
    /// TLS listener" row): the configuration is valid, every request must fail.
    pub fn grpc_client_plaintext(
        &self,
        id: NodeId,
    ) -> Result<config_client::GrpcClient, config_client::ClientError> {
        self.try_grpc_client_with_tls(Some(id), TlsMode::Insecure)
    }

    fn try_grpc_client_with_tls(
        &self,
        pin: Option<NodeId>,
        tls: TlsMode,
    ) -> Result<config_client::GrpcClient, config_client::ClientError> {
        let mutual = matches!(tls, TlsMode::MutualTls(_));
        let client = config_client::GrpcClient::connect(
            self.client_endpoints(),
            config_client::GrpcClientOptions {
                max_hint_follows: 3,
                request_deadline: self.cfg.read_timeout,
                tls,
                expected_capabilities: None,
                limits: self.cfg.limits,
            },
        )?;
        // Without the cluster id a mutual-TLS client cannot name the node a hint points at
        // (`node-<id>.<cluster>.retcd`), so it refuses to follow hints at all and every
        // request to a follower ends in `NotLeader`. Every harness client is by definition a
        // client *of this cluster*, so it always gets the id.
        let client = if mutual {
            client.with_cluster_id(self.cfg.cluster_id)
        } else {
            client
        };
        match pin {
            Some(id) => client.pinned(&self.client_endpoint(id)),
            None => Ok(client),
        }
    }

    /// The TLS fixture every node of this cluster was issued from.
    ///
    /// Panics on an insecure cluster: a row that asks for certificate material has already
    /// assumed [`ClusterTls::MutualTls`].
    pub fn fixture(&self) -> &Arc<TlsFixture> {
        self.cfg
            .tls
            .fixture()
            .expect("this cluster is insecure: build it with ClusterTls::mutual(..)")
    }

    // ------------------------------- faults -------------------------------

    /// The shared fault switchboard every node's peer transport consults (TA-5).
    pub fn netfault(&self) -> NetFault {
        self.netfault.clone()
    }

    /// Alias for [`Cluster::netfault`].
    pub fn net(&self) -> NetFault {
        self.netfault()
    }

    /// Block Raft traffic between `a` and `b` in both directions.
    pub fn partition(&self, a: NodeId, b: NodeId) {
        self.netfault.block_pair(a, b);
    }

    /// Block Raft traffic `from → to` only.
    pub fn partition_one_way(&self, from: NodeId, to: NodeId) {
        self.netfault.block(from, to);
    }

    /// Split the cluster: no node in `a` can reach any node in `b`, in either direction.
    pub fn partition_sets(&self, a: &[NodeId], b: &[NodeId]) {
        for x in a {
            for y in b {
                if x != y {
                    self.netfault.block_pair(*x, *y);
                }
            }
        }
    }

    /// Cut node `id` off from every other configured node.
    pub fn isolate(&self, id: NodeId) {
        let peers = self.ids();
        self.netfault.isolate(id, &peers);
    }

    /// Clear every block, delay, and drop rule.
    pub fn heal(&self) {
        self.netfault.unblock_all();
    }

    /// Runtime control over gossip observations.
    pub fn gossip(&self) -> GossipControl {
        self.gossip.clone()
    }

    // ------------------------------- waits -------------------------------

    /// Poll `pred` until it yields, or fail with every node's metrics.
    ///
    /// This is the only waiting primitive in the harness. The diagnostic is the point: a bare
    /// "timed out" says nothing about which node was stuck in which role (anti-flake rule 2).
    pub async fn wait_for<T>(
        &self,
        what: &str,
        deadline: Duration,
        mut pred: impl FnMut() -> Option<T>,
    ) -> Result<T, Timeout> {
        poll_until(deadline, self.poll_interval(), &mut pred)
            .await
            .map_err(|t| Timeout {
                elapsed: t.elapsed,
                last_diagnostic: format!("waiting for {what}; {}", self.diagnostic()),
            })
    }

    /// Assert `predicate` never holds for the whole of `duration` (used for "no leader ever").
    pub async fn assert_never(
        &self,
        what: &str,
        duration: Duration,
        mut predicate: impl FnMut() -> bool,
    ) {
        let outcome = self
            .wait_for(what, duration, || predicate().then_some(()))
            .await;
        assert!(
            outcome.is_err(),
            "{what} happened, and must never happen; {}",
            self.diagnostic()
        );
    }

    /// Wait until every running node has applied at least log `index`.
    ///
    /// `index` is a **Raft log index**, not the revision a mutation returns. The two are
    /// different counters — a log holds blank and membership entries that allocate no revision,
    /// so a log index is always ahead. Passing a revision here is a silently weak wait; use
    /// [`Cluster::wait_revision_all`] for what a client was told.
    pub async fn wait_applied_all(&self, index: u64, deadline: Duration) -> Result<(), Timeout> {
        self.wait_for(
            &format!("last_applied >= {index} on every running node"),
            deadline,
            || {
                let metrics = self.running_metrics();
                (!metrics.is_empty()
                    && metrics
                        .iter()
                        .all(|m| m.last_applied.map_or(0, |l| l.index) >= index))
                .then_some(())
            },
        )
        .await
    }

    /// Alias for [`Cluster::wait_applied_all`].
    pub async fn wait_applied(&self, index: u64, deadline: Duration) -> Result<(), Timeout> {
        self.wait_applied_all(index, deadline).await
    }

    /// Wait until every running node has applied at least `index`, restricted to `ids`.
    pub async fn wait_applied_on(
        &self,
        ids: &[NodeId],
        index: u64,
        deadline: Duration,
    ) -> Result<(), Timeout> {
        let ids = ids.to_vec();
        self.wait_for(
            &format!("last_applied >= {index} on {ids:?}"),
            deadline,
            || {
                ids.iter()
                    .all(|id| {
                        self.try_node(*id)
                            .is_some_and(|n| n.applied_index() >= index)
                    })
                    .then_some(())
            },
        )
        .await
    }

    /// Wait until node `id` has actually rejoined: its Raft core has published metrics and it
    /// believes in a leader again.
    ///
    /// A persistent node comes back with its state machine already loaded, so `state_hash` and
    /// `cluster_revision` are correct *before* the core has said anything. Every wait based on
    /// applied state therefore succeeds instantly after a restart and proves nothing about the
    /// node being back in the cluster. This is the wait that does.
    pub async fn wait_rejoined(&self, id: NodeId, deadline: Duration) -> Result<(), Timeout> {
        self.wait_for(
            &format!("node {id} to rejoin the cluster"),
            deadline,
            || {
                let m = self.try_node(id)?.metrics();
                (m.last_log_index.is_some() && m.current_leader.is_some() && m.running_state_ok)
                    .then_some(())
            },
        )
        .await
    }

    /// Wait until every running node has applied at least public `revision` (ADR-0005).
    ///
    /// This is the counterpart to [`Cluster::wait_applied_all`] for the number a client
    /// actually gets back from a mutation.
    pub async fn wait_revision_all(
        &self,
        revision: u64,
        deadline: Duration,
    ) -> Result<(), Timeout> {
        self.wait_for(
            &format!("cluster_revision >= {revision} on every running node"),
            deadline,
            || {
                let metrics = self.running_metrics();
                (!metrics.is_empty() && metrics.iter().all(|m| m.cluster_revision >= revision))
                    .then_some(())
            },
        )
        .await
    }

    /// [`Cluster::wait_revision_all`] restricted to `ids`.
    pub async fn wait_revision_on(
        &self,
        ids: &[NodeId],
        revision: u64,
        deadline: Duration,
    ) -> Result<(), Timeout> {
        let ids = ids.to_vec();
        self.wait_for(
            &format!("cluster_revision >= {revision} on {ids:?}"),
            deadline,
            || {
                ids.iter()
                    .all(|id| {
                        self.try_node(*id)
                            .is_some_and(|n| n.metrics().cluster_revision >= revision)
                    })
                    .then_some(())
            },
        )
        .await
    }

    /// Wait until every running node reports the same `state_hash` **and** the same
    /// `last_applied`, and return that hash.
    ///
    /// Both halves are needed: equal hashes alone would also hold in the instant before a
    /// lagging node applies an entry that does not change the key space.
    pub async fn wait_converged(&self, deadline: Duration) -> Result<[u8; 32], Timeout> {
        self.wait_for("identical state on every running node", deadline, || {
            let ids = self.running_ids();
            if ids.is_empty() {
                return None;
            }
            let hashes = self.state_hashes();
            let applied: Vec<Option<u64>> = self
                .running_metrics()
                .iter()
                .map(|m| m.last_applied.map(|l| l.index))
                .collect();
            let first_hash = *hashes.values().next()?;
            let first_applied = *applied.first()?;
            (hashes.values().all(|h| *h == first_hash)
                && applied.iter().all(|a| *a == first_applied))
            .then_some(first_hash)
        })
        .await
    }

    /// Every node's metrics, formatted for a failure message.
    pub fn diagnostic(&self) -> String {
        let running = self.running_metrics();
        let stopped: Vec<NodeId> = {
            let slots = self.lock();
            slots
                .iter()
                .filter(|(_, s)| s.running.is_none())
                .map(|(id, _)| *id)
                .collect()
        };
        format!(
            "running node metrics: {running:#?}; stopped nodes: {stopped:?}; netfault: {:?}",
            self.netfault
        )
    }

    // ------------------------------- provisioning -------------------------------

    /// Add a slot for a **new** node id, bound and configured but not started (test plan
    /// TA-46).
    ///
    /// `data_dir` decides what the node will open on its first
    /// [`Cluster::try_start_node`]: `None` allocates a fresh directory under the cluster's own
    /// data root, `Some(dir)` points the new identity at a directory that already exists. The
    /// second form is what M5-70's "new node id over an old data directory" half needs, and
    /// the refusal it asserts happens at store open — which is why this deliberately stops
    /// short of starting the node.
    ///
    /// The id is harness-allocated (one past the highest configured id, never reused), so no
    /// row writes a literal id for a node that did not exist at `start` (anti-flake rule 30).
    /// Gossip sees the new node only once it starts, exactly like a configured one.
    async fn provision_slot(&self, data_dir: Option<PathBuf>) -> NodeId {
        let (peer_listener, peer_addr) = ephemeral_listener().await;
        let (client_listener, client_addr) = ephemeral_listener().await;
        // Bound only to reserve the addresses; `try_start_node` rebinds them for real. Holding
        // them open would make that rebind fail rather than wait.
        drop(peer_listener);
        drop(client_listener);

        let mut slots = self.lock();
        let id = NodeId(slots.keys().last().map_or(1, |last| last.0 + 1));
        let data_dir = data_dir.or_else(|| {
            self.data_root
                .as_ref()
                .map(|root| root.path().join(format!("node-{}", id.0)))
        });
        let span = tracing::info_span!(
            "cluster_node",
            node_id = id.0,
            peer_endpoint = %peer_addr,
            client_endpoint = %client_addr,
        );
        slots.insert(
            id,
            NodeSlot {
                identity: self.cfg.identity(id),
                peer_addr,
                client_addr,
                span,
                faults: Arc::new(NoFaults) as Arc<dyn FaultInjector>,
                gossip_source: Arc::new(SharedGossipSource::new()),
                data_dir,
                running: None,
            },
        );
        id
    }

    /// A fresh node id with a fresh data directory, provisioned but not started (TA-46).
    pub async fn provision(&self) -> NodeId {
        self.provision_slot(None).await
    }

    /// A fresh node id pointed at node `from`'s **existing** data directory (TA-46, M5-70).
    ///
    /// `from` must be stopped before the new id is started, or the new open meets RocksDB's
    /// exclusive `LOCK` instead of the identity check the row is about.
    ///
    /// Panics if `from` has no directory — an ephemeral cluster has nothing to reuse.
    pub async fn provision_reusing_dir(&self, from: NodeId) -> NodeId {
        let dir = {
            let slots = self.lock();
            slots
                .get(&from)
                .unwrap_or_else(|| panic!("node {from} was never configured"))
                .data_dir
                .clone()
                .unwrap_or_else(|| {
                    panic!("node {from} has no data directory to reuse; use StorageKind::ROCKS")
                })
        };
        self.provision_slot(Some(dir)).await
    }

    /// A fresh node id whose directory `seed` gets to write before the node ever opens it
    /// (TA-46's `provision_v1_dir`, generalized).
    ///
    /// The directory is created first and handed to `seed`, which is the only window in which
    /// a row can put bytes this build would refuse — a legacy-format store above all (M5-71).
    /// Seeding is the caller's because it needs a raw `rocksdb` handle, which the harness
    /// library deliberately does not depend on.
    ///
    /// Panics if the cluster's storage is not persistent.
    pub async fn provision_seeded_dir(&self, seed: impl FnOnce(&Path)) -> NodeId {
        let id = self.provision_slot(None).await;
        let dir = self.data_dir(id);
        std::fs::create_dir_all(&dir).expect("the provisioned data directory");
        seed(&dir);
        id
    }

    // ------------------------------- lifecycle -------------------------------

    /// Stop node `id`: shut down its Raft instance and both of its gRPC servers, so peers see
    /// a refused connection rather than a silent socket.
    pub async fn stop_node(&self, id: NodeId) {
        let running = {
            let mut slots = self.lock();
            slots.get_mut(&id).and_then(|s| s.running.take())
        };
        let Some(running) = running else { return };
        let _ = running.node.stop().await;
        if let Some(gossip) = &running.gossip {
            gossip.shutdown().await;
        }
        let _ = running.client_server.shutdown().await;
        let _ = running.peer_server.shutdown().await;
    }

    /// Alias for [`Cluster::stop_node`] matching the plan's `stop`.
    pub async fn stop(&self, id: NodeId) {
        self.stop_node(id).await;
    }

    /// Restart node `id` on the same node id and the same listener addresses.
    ///
    /// On `Ephemeral` storage the node comes back with a **fresh, empty store**: it rejoins as
    /// a voter with no state and is re-replicated from the leader. That is the documented M1
    /// behaviour, not a harness shortcut (test plan M1-43); reopening a persistent store is an
    /// M2 concern.
    pub async fn start_node(&self, id: NodeId) {
        self.try_start_node(id)
            .await
            .unwrap_or_else(|e| panic!("could not start node {id}: {e}"));
    }

    /// [`Cluster::start_node`], returning the failure instead of panicking.
    ///
    /// The M2 identity rows (M2-42..M2-46) assert on the store half and M2-17 on the engine
    /// half, so both must be reachable rather than a panic inside the harness.
    pub async fn try_start_node(&self, id: NodeId) -> Result<(), NodeStartError> {
        let (identity, span, faults, source, peer_addr, client_addr, data_dir, already) = {
            let slots = self.lock();
            let slot = slots
                .get(&id)
                .unwrap_or_else(|| panic!("node {id} was never configured"));
            (
                slot.identity,
                slot.span.clone(),
                Arc::clone(&slot.faults),
                Arc::clone(&slot.gossip_source),
                slot.peer_addr,
                slot.client_addr,
                slot.data_dir.clone(),
                slot.running.is_some(),
            )
        };
        if already {
            return Ok(());
        }

        let peer_listener = rebind(peer_addr, self.deadline(4), self.poll_interval()).await;
        let client_listener = rebind(client_addr, self.deadline(4), self.poll_interval()).await;

        let running = start_running(
            &self.cfg,
            identity,
            &span,
            faults,
            source as Arc<dyn GossipObservationSource>,
            peer_transport_for(&self.cfg, id, &self.netfault),
            peer_listener,
            client_listener,
            data_dir.as_deref(),
            self.deadline(4),
            self.poll_interval(),
        )
        .await?;

        let mut slots = self.lock();
        if let Some(slot) = slots.get_mut(&id) {
            slot.running = Some(running);
        }
        Ok(())
    }

    /// Stop node `id` and start it again on the same addresses and the same data directory.
    ///
    /// On [`StorageKind::Rocks`] this is the M2 restart: the store is closed (which releases
    /// RocksDB's exclusive `LOCK`) and the *same* directory is reopened, so every acknowledged
    /// mutation must still be there. On [`StorageKind::Ephemeral`] it keeps the documented M1
    /// semantics — the node comes back empty and is re-replicated (M1-43, M2-10).
    ///
    /// `stop_node` drops the `ConfigNode` and the store it owns. If a test is still holding a
    /// clone from [`Cluster::node`] or [`Cluster::rocks_store`], the lock is not released and
    /// this returns [`NodeStartError::Storage`] carrying [`StorageOpenError::Locked`] rather
    /// than hanging.
    pub async fn restart(&self, id: NodeId) -> Result<(), NodeStartError> {
        self.stop_node(id).await;
        self.try_start_node(id).await
    }

    /// Stop every running node, leaving the data directories intact.
    pub async fn stop_all(&self) {
        for id in self.running_ids() {
            self.stop_node(id).await;
        }
    }

    /// Start every stopped node — a cold cluster restart from disk (M2-05, M2-06).
    pub async fn start_all(&self) -> Result<(), NodeStartError> {
        for id in self.ids() {
            self.try_start_node(id).await?;
        }
        Ok(())
    }

    /// Stop every node, drain both gRPC planes, and shut gossip down.
    ///
    /// Awaiting the server handles is what makes "the ports are free afterwards" true; drop
    /// alone only *signals* shutdown.
    pub async fn shutdown(self) {
        for id in self.ids() {
            self.stop_node(id).await;
        }
        // Only now may the data directories go. Windows refuses to delete a directory RocksDB
        // still has open, so the order here is the difference between a clean test and a
        // sporadic "directory not empty" on teardown (TA-16.3).
        drop(self.slots);
        drop(self.data_root);
    }
}

/// Bind `addr` again, polling until the OS releases it.
///
/// A just-closed listener's port can linger for a moment on Windows; the wait is bounded and
/// polls the actual bind rather than assuming a duration (anti-flake rules 1-2). The bind
/// itself goes through `std`, which is synchronous, so the poll predicate stays synchronous.
async fn rebind(addr: SocketAddr, deadline: Duration, interval: Duration) -> TcpListener {
    let listener = poll_until(deadline, interval, || {
        std::net::TcpListener::bind(addr).ok()
    })
    .await
    .unwrap_or_else(|t| panic!("could not rebind {addr} for a restart: {t}"));
    listener
        .set_nonblocking(true)
        .expect("a freshly bound listener accepts non-blocking mode");
    TcpListener::from_std(listener).expect("tokio listener from a bound std listener")
}

/// Start one node: store, engine, peer plane, client plane — all inside `span`.
///
/// `lock_deadline` is how long to keep retrying a `Locked` RocksDB open. It is zero on a first
/// start (nobody can hold the lock yet) and a few election timeouts on a restart, where the
/// previous store's background threads may still be finishing their close.
#[allow(clippy::too_many_arguments)]
async fn start_running(
    cfg: &ClusterConfig,
    identity: ClusterIdentity,
    span: &tracing::Span,
    faults: Arc<dyn FaultInjector>,
    gossip: Arc<dyn GossipObservationSource>,
    transport: Arc<dyn config_engine::PeerTransport>,
    peer_listener: TcpListener,
    client_listener: TcpListener,
    data_dir: Option<&Path>,
    lock_deadline: Duration,
    lock_interval: Duration,
) -> Result<RunningNode, NodeStartError> {
    let peer_endpoint = peer_listener
        .local_addr()
        .expect("bound peer listener")
        .to_string();
    let client_endpoint = client_listener
        .local_addr()
        .expect("bound client listener")
        .to_string();

    let store_span = tracing::info_span!(parent: span, "store", node_id = identity.node_id.0);
    // The hub is the store's `AppliedBatchSink`, so it exists before the store does and long
    // before the node (ADR-0020). A harness that built it later would have to re-attach it,
    // and a batch applied in the gap would be invisible to every watcher.
    let watch = WatchHub::new(
        cfg.limits.watch,
        Arc::clone(&cfg.clock) as Arc<dyn config_engine::LeaderClock>,
    );
    let store: StorageHandle = match (data_dir, cfg.storage.rocks_options(true)) {
        (Some(dir), Some(options)) => open_rocks(
            dir,
            identity,
            cfg.limits,
            Arc::clone(&faults),
            store_span,
            options,
            lock_deadline,
            lock_interval,
            Arc::clone(&watch) as Arc<dyn config_storage::AppliedBatchSink>,
        )
        .await?
        .into(),
        _ => EphemeralStore::new(
            identity,
            cfg.limits,
            Arc::clone(&faults),
            store_span,
            Arc::clone(&watch) as Arc<dyn config_storage::AppliedBatchSink>,
        )
        .into(),
    };
    let mut node_cfg = NodeConfig::new(identity, peer_endpoint);
    // The client plane listens on its own socket, so a leader hint must name *that* address,
    // not the peer one. Without this the hint is undialable by a client (engine delta §4.2).
    node_cfg.client_endpoint = Some(client_endpoint);
    node_cfg.raft = cfg.raft_timers();
    node_cfg.limits = cfg.limits;
    node_cfg.read_timeout = cfg.read_timeout;
    node_cfg.write_timeout = cfg.write_timeout;
    node_cfg.gossip_poll = cfg.gossip_poll;
    node_cfg.transport_security = cfg.tls.transport_security();
    node_cfg.watch_retention = cfg.retention;
    node_cfg.watch_progress_interval = cfg.watch_progress_interval;
    // `with_snapshots` rather than a field assignment: it is the call that enforces the
    // three-knob latch, and a half-applied policy (build forever, purge never) is silent.
    node_cfg = node_cfg
        .with_snapshots(cfg.snapshot)
        .expect("a valid snapshot policy on the cluster configuration");
    node_cfg.promote_max_lag = cfg.promote_max_lag;
    let serving = cfg.tls.serving_mode(
        identity.node_id,
        cfg.node_cert_overrides
            .get(&identity.node_id)
            .unwrap_or(&NO_CERT_OVERRIDES),
    );
    let authorizer: Arc<dyn Authorizer> = match &cfg.authz {
        AuthzKind::AllowAll => Arc::new(AllowAll),
        AuthzKind::StaticAllowlist(policy) => {
            node_cfg.authz_kind = config_engine::AuthzKind::StaticAllowlist;
            // No document was loaded on this path — the caller handed over a parsed policy —
            // so the health payload honestly reports no hash. Only the grant count is known.
            node_cfg = node_cfg.with_policy_grants(policy.grants.len() as u64);
            Arc::new(StaticAllowlist::new(policy.clone()))
        }
        // M3 §4.4: a policy *document* is what config-server is configured with, so the
        // harness parses the same TOML the daemon would. A document that does not parse is
        // `Invalid`, not a panic: failing closed is the behaviour under test.
        AuthzKind::Static(toml_text) => {
            match toml::from_str::<config_core::AllowlistPolicy>(toml_text) {
                Ok(policy) => {
                    node_cfg.authz_kind = config_engine::AuthzKind::StaticAllowlist;
                    // The *exact bytes* every node was configured from, so `HealthPayload`'s
                    // `policy_hash_hex` is comparable across the cluster the same way it is
                    // for a daemon fleet reading one file (M3-42). Hashing the parsed value
                    // instead would make two differently-formatted documents indistinguishable.
                    node_cfg = node_cfg
                        .with_policy_document(toml_text.as_bytes())
                        .with_policy_grants(policy.grants.len() as u64);
                    Arc::new(StaticAllowlist::new(policy))
                }
                Err(_) => {
                    node_cfg.authz_kind = config_engine::AuthzKind::Invalid;
                    // An unparsable document is still a document the operator supplied, and
                    // the hash is what tells a fleet check that every node got the same bad
                    // file. `grants` is reported as 0 by the engine for `Invalid` regardless.
                    node_cfg = node_cfg.with_policy_document(toml_text.as_bytes());
                    Arc::new(StaticAllowlist::new(config_core::AllowlistPolicy::default()))
                }
            }
        }
        // Both denial kinds carry a deny-everything authorizer *and* the honest kind, because
        // the engine reports the kind in `health()` and denies before Raft is touched. An
        // empty allowlist is exactly "no grant covers this request".
        AuthzKind::Missing => {
            node_cfg.authz_kind = config_engine::AuthzKind::Missing;
            Arc::new(StaticAllowlist::new(config_core::AllowlistPolicy::default()))
        }
        AuthzKind::Invalid => {
            node_cfg.authz_kind = config_engine::AuthzKind::Invalid;
            Arc::new(StaticAllowlist::new(config_core::AllowlistPolicy::default()))
        }
    };

    // `ConfigNode::start` and both `serve_*` calls capture `Span::current()`, so they must be
    // made with the node span entered — otherwise their lines lose `node_id` and `testMethod`.
    let guard = span.enter();
    let node = ConfigNode::start(
        node_cfg,
        store.clone(),
        transport,
        gossip,
        authorizer,
        watch,
    )
    .await?;

    let peer_server = serve_peer_plane(
        node.peer_handler(),
        peer_listener,
        serving.clone(),
        PeerIdentity {
            cluster_id: identity.cluster_id,
            recovery_epoch: identity.recovery_epoch,
            node_id: identity.node_id,
        },
        cfg.limits,
    )
    .expect("peer plane listening on its bound listener");
    // One backend value behind both traits, exactly as `config-server` wires it: the admin
    // plane and the client plane can then never disagree about which node they address.
    let backend = Arc::new(NodeBackend { node: node.clone() });
    // Mounted unconditionally, like the daemon's. An empty `admins` set is not "no admin
    // plane", it is a closed one — the safe reading of an absent key, and a state a row can
    // assert on only if the service is actually there to refuse.
    let admin = Some(admin_service(
        Arc::clone(&backend) as Arc<dyn AdminBackend>,
        serving.clone(),
        identity.cluster_id,
        AdminAllowlist::new(cfg.admins.iter().cloned()),
    ));
    let client_server = serve_client_plane(
        backend as Arc<dyn ClientBackend>,
        client_listener,
        serving,
        identity.cluster_id,
        cfg.limits,
        admin,
    )
    .expect("client plane listening on its bound listener");
    drop(guard);

    Ok(RunningNode {
        node,
        store,
        peer_server,
        client_server,
        gossip: None,
    })
}

/// The `CertOverrides` a node with no entry in the map gets: none.
static NO_CERT_OVERRIDES: CertOverrides = CertOverrides {
    cluster_id: None,
    node_id: None,
    expired: false,
    san_uri: None,
    omit_san: false,
    self_signed: false,
};

/// Build the peer transport node `from` dials its peers with.
///
/// One transport per node, not per (source, target) pair: `GrpcPeerTransport` keys its channel
/// cache by `(endpoint, meta.to)` and pins each dial to
/// `peer_server_domain(meta.cluster_id, meta.to)` under mTLS, so the *dialled* node's DNS SAN
/// is already handled by the library. What still varies per node is the certificate
/// *presented*, which is why this is not one transport for the whole cluster.
///
/// A `server_domain` baked into the fixture's `MtlsConfig` is a no-op on the peer plane, so the
/// plain `mtls()` profile is what goes in. Every transport shares the one cluster-wide
/// [`NetFault`], which is what keeps `partition`/`isolate`/`heal` working.
fn peer_transport_for(
    cfg: &ClusterConfig,
    from: NodeId,
    netfault: &NetFault,
) -> Arc<dyn config_engine::PeerTransport> {
    let tls = match &cfg.tls {
        ClusterTls::Insecure => TlsMode::Insecure,
        ClusterTls::MutualTls(fixture) => TlsMode::MutualTls(
            fixture
                .issue_with(
                    CertProfile::node(from),
                    cfg.node_cert_overrides
                        .get(&from)
                        .cloned()
                        .unwrap_or_default(),
                )
                .mtls(),
        ),
    };
    GrpcPeerTransport::new(tls, netfault.clone(), cfg.limits)
        as Arc<dyn config_engine::PeerTransport>
}

/// Open a RocksDB data directory, retrying only while it is still `Locked`.
///
/// A restart is the one moment where the previous store may not have finished closing.
/// Retrying `Locked` (and nothing else) is the difference between "await store closure" and
/// "hope" — every other open error, an identity mismatch above all, is returned immediately
/// because retrying it would only turn a clear failure into a timeout (TA-16.1).
#[allow(clippy::too_many_arguments)]
async fn open_rocks(
    dir: &Path,
    identity: ClusterIdentity,
    limits: Limits,
    faults: Arc<dyn FaultInjector>,
    span: tracing::Span,
    options: RocksOptions,
    deadline: Duration,
    interval: Duration,
    sink: Arc<dyn config_storage::AppliedBatchSink>,
) -> Result<RocksStore, StorageOpenError> {
    let attempt = |_: ()| {
        RocksStore::open_with(
            dir,
            identity,
            limits,
            Arc::clone(&faults),
            span.clone(),
            options,
            Arc::clone(&sink),
        )
    };
    match attempt(()) {
        Err(StorageOpenError::Locked { .. }) if !deadline.is_zero() => {}
        other => return other,
    }

    let opened = poll_until(deadline, interval, || match attempt(()) {
        Err(StorageOpenError::Locked { .. }) => None,
        other => Some(other),
    })
    .await;
    match opened {
        Ok(result) => result,
        // The last attempt is re-run so the caller gets the real `Locked` error — with the
        // detail naming who holds the lock — rather than a bare harness timeout.
        Err(_) => attempt(()),
    }
}
