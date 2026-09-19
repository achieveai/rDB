//! Node configuration and the storage handle (`spec §6.3`, ADR-0002, ADR-0016).

use std::sync::Arc;
use std::time::Duration;

use config_core::{Authz, ClusterIdentity, Durability, Limits, TransportSecurity};
use config_storage::{EphemeralStore, RocksStore, StateReader};

/// How many log entries one `AppendEntries` may carry (`openraft::Config::max_payload_entries`).
///
/// OpenRaft's own default is 300, which under [`config_core::Limits::max_request_bytes`] would
/// let a single RPC reach hundreds of megabytes. A transport has to size its receive cap for the
/// largest message the leader can legally build, so this constant is the term that makes that
/// cap finite and small (`config_grpc::peer_plane_message_limit`). Lowering it costs at most an
/// extra round trip per 16 entries when a follower is catching up; leaving it at 300 costs a
/// wedged replication stream, because an over-size `AppendEntries` is rejected and OpenRaft
/// retries it forever.
///
/// Kept at 16 after the peer payload moved to postcard (ADR-0010, fix-round follow-up): the
/// binary encoding shrank the *cap*, not the per-RPC cost of receiving a full batch, and 16
/// maximum-size commands per `AppendEntries` is already more than a real write burst produces.
pub const MAX_PAYLOAD_ENTRIES: u64 = 16;

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
///
/// [`AuthzKind::Missing`] and [`AuthzKind::Invalid`] are not "no authorization" — they are
/// *failed* authorization. A node wired with either is never ready and denies every client
/// call before it reaches Raft (OQ-19), which is the fail-closed behaviour ADR-0012 requires
/// of a node whose policy could not be loaded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthzKind {
    /// [`config_core::AllowAll`] — development only.
    Development,
    /// The deployment-managed static prefix allowlist.
    StaticAllowlist,
    /// A policy was required and none was supplied.
    Missing,
    /// A policy was supplied but could not be parsed or validated.
    Invalid,
}

impl AuthzKind {
    /// Whether an authorization model is actually in force.
    ///
    /// `false` makes the node unready and turns every client call into
    /// [`config_core::ConfigError::PermissionDenied`].
    pub const fn is_present(self) -> bool {
        matches!(self, AuthzKind::Development | AuthzKind::StaticAllowlist)
    }

    /// Stable snake_case name for log and health fields.
    pub const fn as_str(self) -> &'static str {
        match self {
            AuthzKind::Development => "development",
            AuthzKind::StaticAllowlist => "static_allowlist",
            AuthzKind::Missing => "missing",
            AuthzKind::Invalid => "invalid",
        }
    }
}

impl std::fmt::Display for AuthzKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<AuthzKind> for Authz {
    /// `Missing`/`Invalid` have no [`Authz`] of their own — `config_core::Authz` names the
    /// models a node can *enforce*, and those two mean it enforces none. They report
    /// [`Authz::StaticAllowlist`], the deny-everything end of the scale, so a capability
    /// reader can never mistake a policy-less node for a permissive one. The honest,
    /// unambiguous answer is [`crate::HealthPayload::authz_kind`], which keeps all four.
    fn from(k: AuthzKind) -> Self {
        match k {
            AuthzKind::Development => Authz::Development,
            AuthzKind::StaticAllowlist | AuthzKind::Missing | AuthzKind::Invalid => {
                Authz::StaticAllowlist
            }
        }
    }
}

/// Everything one [`crate::ConfigNode`] needs that is not a collaborator object.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// Cluster, epoch, and this node's id (ADR-0011).
    pub identity: ClusterIdentity,
    /// Advertised peer endpoint (`host:port`), stored in [`config_storage::RaftNode::peer`] at
    /// formation and thereafter the *committed* address every peer dials.
    ///
    /// [`crate::ConfigNode::form_cluster`] refuses a plan whose entry for this node names a
    /// different peer endpoint: a node that advertised one address and committed another
    /// would be unreachable for exactly as long as nobody looked.
    pub peer_endpoint: String,
    /// Advertised client endpoint (`host:port`), stored in
    /// [`config_storage::RaftNode::client`] at formation and thereafter the *committed*
    /// address every leader hint names (ADR-0009).
    ///
    /// `None` means "the same endpoint as the peer plane", which is the in-process M1 profile
    /// where a node has one synthetic address and no second listener.
    pub client_endpoint: Option<String>,
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
    /// How many grant rules the allowlist this node was wired with holds (M3-42).
    ///
    /// Supplied rather than derived: the node holds an `Arc<dyn Authorizer>`, and a trait
    /// object cannot be asked how many rules it has without giving every authorizer a method
    /// that only one implementation can answer. Ignored unless
    /// [`AuthzKind::StaticAllowlist`] is in force — an `AllowAll`, missing, or invalid policy
    /// enforces no grants, and [`crate::PolicySummary`] reports `0` for all three whatever is
    /// set here.
    pub policy_grants: u64,
    /// SHA-256 of the policy **document bytes** the embedder loaded, when there was one.
    ///
    /// The bytes, not the parsed value: the point is that two nodes handed the same file
    /// print the same digest and a node handed an edited one does not, which is the check a
    /// fleet-wide "is everyone on the same policy" assertion actually wants.
    /// [`NodeConfig::with_policy_document`] is the usual way to set it.
    pub policy_document_sha256: Option<[u8; 32]>,
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
            client_endpoint: None,
            raft: RaftTimers::default(),
            limits: Limits::DEFAULT,
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(10),
            gossip_poll: Duration::from_secs(1),
            authz_kind: AuthzKind::Development,
            policy_grants: 0,
            policy_document_sha256: None,
            transport_security: TransportSecurity::Insecure,
            cluster_name: "retcd".to_string(),
        }
    }

    /// Record the policy document this node was configured from (M3-42).
    ///
    /// Hashes `document` with SHA-256 and stores the digest, which
    /// [`crate::HealthPayload::policy`] reports as lowercase hex. Pass the exact bytes that
    /// were loaded — the file for a daemon, the TOML string for a harness — because the digest
    /// is only comparable across nodes if every node hashed the same bytes.
    ///
    /// Does **not** set [`NodeConfig::policy_grants`]: this crate does not parse TOML, and the
    /// grant count comes from the `AllowlistPolicy` the embedder already has in hand.
    pub fn with_policy_document(mut self, document: &[u8]) -> Self {
        use sha2::{Digest, Sha256};
        let digest: [u8; 32] = Sha256::digest(document).into();
        self.policy_document_sha256 = Some(digest);
        self
    }

    /// Record how many grant rules the allowlist this node enforces holds (M3-42).
    pub fn with_policy_grants(mut self, grants: u64) -> Self {
        self.policy_grants = grants;
        self
    }

    /// The client endpoint this node advertises: [`NodeConfig::client_endpoint`] when set,
    /// otherwise [`NodeConfig::peer_endpoint`].
    pub fn client_endpoint(&self) -> &str {
        self.client_endpoint
            .as_deref()
            .unwrap_or(&self.peer_endpoint)
    }

    /// The committed membership entry this node's own configuration implies.
    pub fn raft_node(&self) -> config_storage::RaftNode {
        config_storage::RaftNode::new(&self.peer_endpoint, self.client_endpoint())
    }

    /// Build the OpenRaft config this node runs with.
    ///
    /// `max_payload_entries` is [`MAX_PAYLOAD_ENTRIES`] rather than OpenRaft's default; see
    /// that constant for why the transport's receive cap depends on it.
    ///
    /// # Why the log is never purged
    ///
    /// `SnapshotPolicy::Never` is the load-bearing setting: OpenRaft only ever purges below a
    /// snapshot, so with no snapshot `calc_purge_upto` returns `None` and `purge` is never
    /// called. `max_in_snapshot_log_to_keep = u64::MAX` is belt and braces for the same
    /// invariant — if a snapshot ever did appear, it would ask to keep every entry that
    /// snapshot covers rather than none of them (ADR-0008). The previous value of `0` read as
    /// "keep nothing", which is the opposite of what a store with no `install_snapshot` can
    /// survive.
    pub fn openraft_config(&self) -> openraft::Config {
        openraft::Config {
            cluster_name: self.cluster_name.clone(),
            heartbeat_interval: self.raft.heartbeat_ms,
            election_timeout_min: self.raft.election_min_ms,
            election_timeout_max: self.raft.election_max_ms,
            snapshot_policy: openraft::SnapshotPolicy::Never,
            max_payload_entries: MAX_PAYLOAD_ENTRIES,
            max_in_snapshot_log_to_keep: u64::MAX,
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
/// An enum rather than a trait object because OpenRaft's storage traits are not object safe.
/// Every accessor is hand-matched over the variants: the two stores expose the same method
/// names deliberately, so an arm that drifts is a compile error rather than a behaviour
/// difference.
///
/// A [`StorageHandle::Rocks`] store is opened by the **caller**, before
/// [`crate::ConfigNode::start`]. That is not a style choice: `RocksStore::open` is where an
/// identity mismatch, a locked directory, or a missing column family is detected, and a daemon
/// has to be able to exit on that error instead of discovering it inside a started node
/// (ADR-0011).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum StorageHandle {
    /// In-memory storage (M1). A restart loses committed data, and the node says so.
    Ephemeral(EphemeralStore),
    /// RocksDB storage (M2), already opened and identity-checked by the caller.
    Rocks(RocksStore),
}

impl StorageHandle {
    /// Synchronous applied-state access for the gated read path.
    pub fn reader(&self) -> Arc<dyn StateReader> {
        match self {
            StorageHandle::Ephemeral(s) => s.reader(),
            StorageHandle::Rocks(s) => s.reader(),
        }
    }

    /// The identity the store is bound to.
    pub fn identity(&self) -> ClusterIdentity {
        match self {
            StorageHandle::Ephemeral(s) => s.identity(),
            StorageHandle::Rocks(s) => s.identity(),
        }
    }

    /// Whether the store has never accepted a vote, an entry, or an apply (formation gate).
    pub fn is_fresh(&self) -> bool {
        match self {
            StorageHandle::Ephemeral(s) => s.is_fresh(),
            StorageHandle::Rocks(s) => s.is_fresh(),
        }
    }

    /// What this store actually guarantees survives a restart.
    pub fn durability(&self) -> Durability {
        match self {
            StorageHandle::Ephemeral(s) => s.durability(),
            StorageHandle::Rocks(s) => s.durability(),
        }
    }

    /// Command-carrying entries applied so far.
    pub fn applied_commands(&self) -> u64 {
        match self {
            StorageHandle::Ephemeral(s) => s.applied_commands(),
            StorageHandle::Rocks(s) => s.applied_commands(),
        }
    }

    /// Entries currently present in the Raft log.
    pub fn raft_log_len(&self) -> u64 {
        match self {
            StorageHandle::Ephemeral(s) => s.raft_log_len(),
            StorageHandle::Rocks(s) => s.raft_log_len(),
        }
    }

    /// Whether an injected crash or a backend failure has poisoned this store.
    ///
    /// A poisoned store is one of the three things that make a node unready
    /// ([`crate::HealthPayload::ready`]): it can no longer answer for its own state.
    pub fn is_poisoned(&self) -> bool {
        match self {
            StorageHandle::Ephemeral(s) => s.is_poisoned(),
            StorageHandle::Rocks(s) => s.is_poisoned(),
        }
    }

    /// Per-boundary fault-injection crossing counters (test plan TA-4).
    pub fn counters(&self) -> Arc<config_storage::FaultCounters> {
        match self {
            StorageHandle::Ephemeral(s) => s.counters(),
            StorageHandle::Rocks(s) => s.counters(),
        }
    }

    /// The store's `command -> trace_id` side table (ADR-0013).
    ///
    /// The engine records a client write's trace here before replicating it, and records the
    /// trace carried by an incoming `AppendEntries` envelope for every command entry it
    /// delivers; the store's apply path reads it so the apply line on **every** voter carries
    /// the originating client's `trace_id`.
    pub fn traces(&self) -> Arc<config_storage::TraceRegistry> {
        match self {
            StorageHandle::Ephemeral(s) => s.traces(),
            StorageHandle::Rocks(s) => s.traces(),
        }
    }
}

impl From<EphemeralStore> for StorageHandle {
    fn from(s: EphemeralStore) -> Self {
        StorageHandle::Ephemeral(s)
    }
}

impl From<RocksStore> for StorageHandle {
    fn from(s: RocksStore) -> Self {
        StorageHandle::Rocks(s)
    }
}
