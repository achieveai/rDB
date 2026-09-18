//! Node configuration and the storage handle (`spec §6.3`, ADR-0002, ADR-0016).

use std::sync::Arc;
use std::time::Duration;

use config_core::{Authz, ClusterIdentity, Durability, Limits, TransportSecurity};
use config_storage::{EphemeralLog, EphemeralSm, EphemeralStore, StateReader};

/// Raft timing, in milliseconds.
///
/// The defaults are the Windows-safe values from the OpenRaft research note: OpenRaft ticks at
/// `heartbeat_interval * 3 / 2`, and Windows' ~15.6 ms timer granularity makes OpenRaft's own
/// 50/150/300 defaults fragile on a Server 2022 VM. Treat these as a benchmarking output
/// (spec §9.3.8), not a fixed truth.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RaftTimers {
    /// Leader heartbeat interval. The tick period is 1.5× this value.
    pub heartbeat_ms: u64,
    /// Minimum election timeout; must be strictly greater than `heartbeat_ms`.
    pub election_min_ms: u64,
    /// Maximum election timeout; must be strictly greater than `election_min_ms`.
    pub election_max_ms: u64,
}

impl Default for RaftTimers {
    fn default() -> Self {
        Self {
            heartbeat_ms: 250,
            election_min_ms: 750,
            election_max_ms: 1500,
        }
    }
}

impl RaftTimers {
    /// The maximum election timeout as a [`Duration`]; tests derive their deadlines from this
    /// rather than writing literals (test plan §6 rule 3).
    pub fn election_timeout(&self) -> Duration {
        Duration::from_millis(self.election_max_ms)
    }
}

/// Which authorization model the node was wired with.
///
/// Passed in rather than derived by downcasting the [`config_core::Authorizer`]: a node must
/// report the model it is actually enforcing, and a trait object cannot be asked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthzKind {
    /// [`config_core::AllowAll`] — development only.
    Development,
    /// The deployment-managed static prefix allowlist.
    StaticAllowlist,
}

impl From<AuthzKind> for Authz {
    fn from(k: AuthzKind) -> Self {
        match k {
            AuthzKind::Development => Authz::Development,
            AuthzKind::StaticAllowlist => Authz::StaticAllowlist,
        }
    }
}

/// Everything one [`crate::ConfigNode`] needs that is not a collaborator object.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// Cluster, epoch, and this node's id (ADR-0011).
    pub identity: ClusterIdentity,
    /// Advertised peer endpoint (`host:port`), stored in `BasicNode::addr` at formation and
    /// thereafter the *committed* address every peer dials and every leader hint names.
    pub peer_endpoint: String,
    /// Raft timing.
    pub raft: RaftTimers,
    /// Replicated apply-time caps; identical on every voter (spec §7.1).
    pub limits: Limits,
    /// Wraps `ensure_linearizable`, which has no timeout of its own.
    pub read_timeout: Duration,
    /// Wraps `client_write`. Elapsing it yields `DeadlineExceededUnknownOutcome` (ADR-0015).
    pub write_timeout: Duration,
    /// How often the advisory gossip source is polled and validated.
    pub gossip_poll: Duration,
    /// Which authorization model is active, for [`config_core::Capabilities`].
    pub authz_kind: AuthzKind,
    /// How the client plane is protected, for [`config_core::Capabilities`].
    pub transport_security: TransportSecurity,
    /// OpenRaft's `cluster_name`, used only in its own log lines.
    pub cluster_name: String,
}

impl NodeConfig {
    /// The M1 profile: allow-all authorization, insecure transport, default timers and caps.
    pub fn new(identity: ClusterIdentity, peer_endpoint: impl Into<String>) -> Self {
        Self {
            identity,
            peer_endpoint: peer_endpoint.into(),
            raft: RaftTimers::default(),
            limits: Limits::DEFAULT,
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(10),
            gossip_poll: Duration::from_secs(1),
            authz_kind: AuthzKind::Development,
            transport_security: TransportSecurity::Insecure,
            cluster_name: "retcd".to_string(),
        }
    }

    /// Build the OpenRaft config this node runs with.
    ///
    /// `SnapshotPolicy::Never` plus `max_in_snapshot_log_to_keep = 0` is what makes the
    /// no-snapshot store safe: nothing ever builds a snapshot, so nothing ever purges a log
    /// a follower might still need.
    pub fn openraft_config(&self) -> openraft::Config {
        openraft::Config {
            cluster_name: self.cluster_name.clone(),
            heartbeat_interval: self.raft.heartbeat_ms,
            election_timeout_min: self.raft.election_min_ms,
            election_timeout_max: self.raft.election_max_ms,
            snapshot_policy: openraft::SnapshotPolicy::Never,
            max_in_snapshot_log_to_keep: 0,
            purge_batch_size: 1,
            enable_tick: true,
            enable_heartbeat: true,
            enable_elect: true,
            ..Default::default()
        }
    }
}

/// The store a node runs on.
///
/// An enum rather than a trait object because OpenRaft's storage traits are not object safe;
/// M2 adds a `Rocks` variant beside `Ephemeral`.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum StorageHandle {
    /// In-memory storage (M1). A restart loses committed data, and the node says so.
    Ephemeral(EphemeralStore),
}

impl StorageHandle {
    /// The log store to hand to OpenRaft.
    pub fn log_store(&self) -> EphemeralLog {
        match self {
            StorageHandle::Ephemeral(s) => s.log_store(),
        }
    }

    /// The state machine to hand to OpenRaft.
    pub fn state_machine(&self) -> EphemeralSm {
        match self {
            StorageHandle::Ephemeral(s) => s.state_machine(),
        }
    }

    /// Synchronous applied-state access for the gated read path.
    pub fn reader(&self) -> Arc<dyn StateReader> {
        match self {
            StorageHandle::Ephemeral(s) => s.reader(),
        }
    }

    /// The identity the store is bound to.
    pub fn identity(&self) -> ClusterIdentity {
        match self {
            StorageHandle::Ephemeral(s) => s.identity(),
        }
    }

    /// Whether the store has never accepted a vote, an entry, or an apply (formation gate).
    pub fn is_fresh(&self) -> bool {
        match self {
            StorageHandle::Ephemeral(s) => s.is_fresh(),
        }
    }

    /// What this store actually guarantees survives a restart.
    pub fn durability(&self) -> Durability {
        match self {
            StorageHandle::Ephemeral(s) => s.durability(),
        }
    }

    /// Command-carrying entries applied so far.
    pub fn applied_commands(&self) -> u64 {
        match self {
            StorageHandle::Ephemeral(s) => s.applied_commands(),
        }
    }

    /// Entries currently present in the Raft log.
    pub fn raft_log_len(&self) -> u64 {
        match self {
            StorageHandle::Ephemeral(s) => s.raft_log_len(),
        }
    }
}

impl From<EphemeralStore> for StorageHandle {
    fn from(s: EphemeralStore) -> Self {
        StorageHandle::Ephemeral(s)
    }
}
