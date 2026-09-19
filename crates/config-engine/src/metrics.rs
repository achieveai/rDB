//! Node-level observable state: metrics, health, and the committed membership view.
//!
//! None of these types name an OpenRaft type (ADR-0004), because they cross into the test
//! harness, the health endpoint, and eventually the gRPC surface.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use config_core::policy::PolicyState;
use config_core::{
    Authz, ClusterIdentity, Durability, LeaderHint, NodeId, RecoveryEpoch, RestoredFrom,
    TransportSecurity,
};
use config_storage::{DedupStats, StorageMetrics};

use crate::pagination::PinStats;
use crate::watch::WatchStats;
use serde::Serialize;

use crate::config::AuthzKind;

/// A Raft log id, flattened for callers that must not depend on OpenRaft.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct LogIdView {
    /// Term of the leader that proposed the entry.
    pub term: u64,
    /// Index of the entry.
    pub index: u64,
}

impl LogIdView {
    /// Build a view from its parts.
    pub const fn new(term: u64, index: u64) -> Self {
        Self { term, index }
    }
}

impl From<LogIdView> for (u64, u64) {
    fn from(v: LogIdView) -> Self {
        (v.term, v.index)
    }
}

/// A node's Raft role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeRole {
    /// Replicating, but neither voting nor timing out — including a node that has never been
    /// formed.
    Learner,
    /// Replicating from a leader.
    Follower,
    /// Campaigning.
    Candidate,
    /// Leading.
    Leader,
    /// Shutting down or shut down.
    Shutdown,
}

impl NodeRole {
    /// Stable snake_case name for log and metric fields.
    pub const fn as_str(self) -> &'static str {
        match self {
            NodeRole::Learner => "learner",
            NodeRole::Follower => "follower",
            NodeRole::Candidate => "candidate",
            NodeRole::Leader => "leader",
            NodeRole::Shutdown => "shutdown",
        }
    }
}

impl std::fmt::Display for NodeRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A cheap synchronous snapshot of one node's Raft and storage state (test plan TA-6).
///
/// `raft_log_len` and `membership_voter_ids` are load-bearing, not decorative: they are how a
/// test proves a direct write really went through Raft (the log grew on every node) and how
/// it proves gossip cannot change membership (the voter set is identical before and after).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeMetrics {
    /// This node.
    pub node_id: NodeId,
    /// Its current role.
    pub role: NodeRole,
    /// Its current Raft term.
    pub current_term: u64,
    /// The leader it currently believes in, if any. Metrics-derived and therefore stale by
    /// design — a routing hint, never a read guard.
    pub current_leader: Option<NodeId>,
    /// Last index appended to this node's log.
    pub last_log_index: Option<u64>,
    /// Last log id applied to this node's state machine.
    pub last_applied: Option<LogIdView>,
    /// Entries currently held in this node's log.
    pub raft_log_len: u64,
    /// Voter ids of the **effective** membership, ascending.
    ///
    /// Effective, not committed: this is OpenRaft's `membership_config`, which moves as soon
    /// as a membership entry is appended. It is the right thing for a harness to poll while
    /// waiting for a cluster to form. It is the wrong thing to derive a client-followable
    /// hint from — [`crate::ConfigNode::committed_membership`] is that (ADR-0009).
    pub membership_voter_ids: Vec<NodeId>,
    /// Log id of the effective membership entry, if any.
    pub membership_log_id: Option<LogIdView>,
    /// The public cluster revision this node has applied (not the log index; ADR-0005).
    pub cluster_revision: u64,
    /// Command-carrying entries applied so far, excluding blank and membership entries.
    pub applied_commands: u64,
    /// Whether OpenRaft's `running_state` is `Ok`. `false` means the core stopped, normally
    /// because storage failed fatally (spec §9.3.7).
    pub running_state_ok: bool,
    /// For a leader, milliseconds since a quorum last acknowledged it. The signal that a
    /// leader may be partitioned (spec §18.2).
    pub millis_since_quorum_ack: Option<u64>,
    /// Authorization decisions this node refused, since start (M3-81).
    ///
    /// Counted in the single authorize seam, so it covers every client entry point and
    /// includes the fail-closed denials a node with a missing or invalid policy issues. One
    /// `retcd.audit` `deny` line exists for each of these; the counter is what makes "a spike
    /// of refusals" assertable without parsing a log.
    pub authz_denied: u64,
    /// Admin-plane calls refused by the `[authz] admins` allowlist, since start (ADR-0023,
    /// review finding C5B-15).
    ///
    /// A separate counter rather than a second increment of `authz_denied`, because the two
    /// are different authorizations: `authz_denied` is the keyspace policy deciding what a
    /// principal may do to a key, and this one is the allowlist deciding whether a principal
    /// may reach the admin surface at all. They have different policies, different operators
    /// and different responses, and `retcd_authz_denied_total` already declares a `plane`
    /// label to tell them apart — a label that, until this counter existed, only ever carried
    /// one value.
    pub authz_denied_admin: u64,
    /// Connections whose transport identity could not be established, since start, across
    /// **both** planes.
    ///
    /// *Authentication*, not authorization: the caller never became a principal, so no
    /// authorization decision was reached and no audit line was written. The client-plane half
    /// is recorded by the transport through [`crate::ConfigNode::record_authn_rejection`],
    /// because the engine never sees a certificate; the peer-plane half is recorded by the
    /// engine itself, which is where the `identity_retired` fence lives.
    pub authn_rejected: u64,
    /// The peer-plane share of [`NodeMetrics::authn_rejected`].
    ///
    /// Held separately because `/metrics` labels the family by plane (ADR-0026) and a single
    /// undivided counter forced the exporter to publish every sample as `plane="client"` — so
    /// a fenced peer, which is a membership event, appeared in an operator's dashboard as a
    /// client authentication failure and sent them looking at certificates.
    pub authn_rejected_peer: u64,
}

/// What authorization policy a node is actually holding (M3-42).
///
/// Three facts, none of them a policy body. Together they answer the question a cross-process
/// test and an operator both need answered — *are these nodes enforcing the same policy?* —
/// without putting a grant, a principal name, or a key prefix on an unauthenticated health
/// endpoint.
///
/// `policy_hash_hex` is the digest of the **document bytes the embedder loaded**, not of the
/// parsed value: two nodes given byte-identical documents print the same string, and a node
/// given an edited copy prints a different one even when the edit happened to parse to the
/// same grants. That is the property a fleet check wants. It is `None` when there is no
/// document at all — an `AllowAll` node, or one whose policy was missing.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct PolicySummary {
    /// The model in force, mirroring [`crate::ConfigNode::capabilities`]'s `authz`.
    pub kind: Authz,
    /// How many grant rules the node's allowlist holds. `0` for `AllowAll`, and for a policy
    /// that was missing or unparsable — in those two cases the node enforces no grants at all
    /// and is unready besides.
    pub grants: u64,
    /// Lowercase hex SHA-256 of the policy document bytes, when the embedder supplied them.
    pub policy_hash_hex: Option<String>,
}

/// What a node will do with client traffic right now (spec §18.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Health {
    /// Leader, with a working quorum; strict reads and writes are served here.
    Ready,
    /// A follower. The hint, when present, names the committed endpoint of the leader.
    NotLeader {
        /// Where to retry.
        hint: Option<LeaderHint>,
    },
    /// Not formed, no leader known, or storage failed fatally.
    Unavailable {
        /// Operator-facing explanation; never contains a key or value.
        reason: String,
    },
    /// [`crate::ConfigNode::stop`] has been called.
    Stopped,
}

/// The committed membership: the only authoritative statement of who the voters are and
/// where they live (ADR-0003, ADR-0011).
///
/// Gossip observations are validated *against* this and never merged into it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MembershipView {
    /// Committed voter ids.
    pub voters: BTreeSet<NodeId>,
    /// Committed **peer-plane** endpoints, by voter id. What the Raft transport dials.
    pub endpoints: BTreeMap<NodeId, String>,
    /// Committed **client-plane** endpoints, by voter id. What a leader hint names.
    ///
    /// Both planes come out of the same membership entry, so a hint can never name an
    /// endpoint the cluster did not commit to (ADR-0009).
    pub client_endpoints: BTreeMap<NodeId, String>,
    /// `(term, index)` of the membership log entry, if membership has ever been committed.
    pub membership_log_id: Option<(u64, u64)>,
}

impl MembershipView {
    /// Whether any membership has been committed yet — i.e. whether the cluster is formed.
    pub fn is_formed(&self) -> bool {
        !self.voters.is_empty()
    }

    /// The committed peer endpoint of `node_id`, if it is a known member.
    pub fn endpoint_of(&self, node_id: NodeId) -> Option<&str> {
        self.endpoints.get(&node_id).map(String::as_str)
    }

    /// The committed client endpoint of `node_id`, if it is a known member.
    pub fn client_endpoint_of(&self, node_id: NodeId) -> Option<&str> {
        self.client_endpoints.get(&node_id).map(String::as_str)
    }
}

/// The serializable cross-process state oracle (spec §18.1, ADR-0016, test plan TA-17).
///
/// This is what a health endpoint serves and what an end-to-end test reads instead of calling
/// `state_hash()` in a process it does not share. It carries **no keys and no values**: every
/// field is an id, a count, a revision, an enum, or the [`state_hash_hex`] digest, so serving
/// it on an unauthenticated loopback listener leaks nothing (§15.2, OQ-16).
///
/// [`state_hash_hex`]: HealthPayload::state_hash_hex
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HealthPayload {
    /// This node's id.
    pub node_id: NodeId,
    /// Its cluster id, as 32 lowercase hex characters.
    pub cluster_id: String,
    /// Its recovery epoch.
    pub recovery_epoch: RecoveryEpoch,
    /// Its current Raft role.
    pub role: NodeRole,
    /// The leader it currently believes in, if any.
    pub current_leader: Option<NodeId>,
    /// Its current Raft term.
    pub term: u64,
    /// Last log index applied to the state machine.
    pub last_applied: Option<u64>,
    /// Last log index known to be committed.
    pub committed: Option<u64>,
    /// Committed voter ids, ascending.
    pub membership_voter_ids: Vec<NodeId>,
    /// Log id of the committed membership entry.
    pub membership_log_id: Option<LogIdView>,
    /// The public cluster revision applied here (ADR-0005), not a log index.
    pub cluster_revision: u64,
    /// The deterministic applied-state digest as 64 lowercase hex characters (TA-2). Two
    /// nodes on the same applied prefix print the same string, which is what makes a
    /// cross-process convergence check one assertion.
    pub state_hash_hex: String,
    /// Command-carrying entries applied so far.
    pub applied_commands: u64,
    /// What this node's store guarantees survives a restart.
    pub durability: Durability,
    /// Whether this node will serve client traffic: membership known, storage not poisoned,
    /// and an authorization model actually in force.
    pub ready: bool,
    /// Which authorization model is in force — including `missing` and `invalid`, which
    /// [`config_core::Capabilities::authz`] cannot express.
    pub authz_kind: AuthzKind,
    /// How the client plane is protected.
    pub transport_security: TransportSecurity,
    /// The policy this node holds, identically on every node given the same document (M3-42).
    pub policy: PolicySummary,
    /// Where this node's data directory was restored from, when it was restored at all
    /// (M5, ADR-0024). `null` for a directory that grew its own state, which is every node
    /// that has never been through a fenced restore - so a non-null value on a node the
    /// operator did not restore is itself the finding.
    pub restored_from: Option<RestoredFrom>,
    /// Authorization decisions refused since start. See [`NodeMetrics::authz_denied`].
    pub authz_denied: u64,
    /// Client connections whose identity could not be established since start. See
    /// [`NodeMetrics::authn_rejected`].
    pub authn_rejected: u64,
    /// The replicated compaction watermark: no journal event at or below it is retained
    /// (M4, ADR-0019, test plan TA-39).
    pub compact_revision: u64,
    /// Oldest revision still in the retained journal, or `None` when the journal is empty.
    ///
    /// `Option` rather than a zero, because there is no revision 0 and an empty journal must
    /// not be reported as one that begins at the beginning of time.
    pub journal_oldest_revision: Option<u64>,
    /// Newest revision in the retained journal, or `None` when it is empty.
    pub journal_newest_revision: Option<u64>,
    /// Digest over the retained journal above the compaction watermark, as 64 lowercase hex
    /// characters (TA-31). Two nodes holding the same retained events print the same string,
    /// which is what makes "the journal survived the restart" one cross-process assertion.
    pub journal_hash: String,
    /// Watch streams registered on this node right now.
    pub watch_streams_open: usize,
    /// The signed policy version in force, or `None` under the static allowlist and on a node
    /// that holds no valid document (M6, ADR-0027, M6-16).
    pub policy_version: Option<u64>,
    /// Why there is no active policy, or which versions this node is converging between.
    ///
    /// Filled by the daemon, which is the only layer that knows *why* the last load failed:
    /// the engine holds the authorizer, not the files. `None` on an embedded node and under
    /// the static allowlist. It carries versions and a closed-set reason and nothing else —
    /// this payload is unauthenticated on loopback (OQ-16), so it never carries a grant, a
    /// principal or any key material.
    pub policy_state: Option<PolicyState>,
}

impl HealthPayload {
    /// Render an identity into the payload's id fields.
    pub(crate) fn identity_fields(identity: &ClusterIdentity) -> (NodeId, String, RecoveryEpoch) {
        (
            identity.node_id,
            identity.cluster_id.to_string(),
            identity.recovery_epoch,
        )
    }
}

// ---------------------------------------------------------------------------------------
// Prometheus exposition (M5, ADR-0026)
// ---------------------------------------------------------------------------------------

/// Bucket upper bounds, in seconds, shared by every latency histogram (ADR-0026).
///
/// One set for all three histograms on purpose: an operator comparing proposal latency with
/// read latency in the same dashboard panel compares equal buckets, and a recording rule
/// written against one works against the others.
pub const LATENCY_BUCKETS_SECONDS: [f64; 12] = [
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
];

/// A lock-free latency histogram over [`LATENCY_BUCKETS_SECONDS`].
///
/// Observations come from the request path, which is hot and must not contend: each one is a
/// linear scan of twelve bucket bounds and two relaxed atomic adds. `sum_micros` is integer
/// microseconds rather than a float, because a float has no atomic form and a lock here would
/// put a mutex on every write.
#[derive(Debug, Default)]
pub struct LatencyHistogram {
    buckets: [AtomicU64; LATENCY_BUCKETS_SECONDS.len()],
    count: AtomicU64,
    sum_micros: AtomicU64,
}

impl LatencyHistogram {
    /// Record one observation.
    pub fn observe(&self, elapsed: Duration) {
        let seconds = elapsed.as_secs_f64();
        for (i, bound) in LATENCY_BUCKETS_SECONDS.iter().enumerate() {
            if seconds <= *bound {
                self.buckets[i].fetch_add(1, Ordering::Relaxed);
                break;
            }
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_micros
            .fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);
    }

    /// Read the histogram out in the cumulative form Prometheus exposes.
    ///
    /// The per-bucket counters above are *exclusive*; `le` buckets are cumulative, so the read
    /// side accumulates. Doing it here rather than on every observation keeps the hot path at
    /// one increment.
    pub fn snapshot(&self) -> HistogramSnapshot {
        let mut cumulative = [0u64; LATENCY_BUCKETS_SECONDS.len()];
        let mut running = 0u64;
        for (i, bucket) in self.buckets.iter().enumerate() {
            running += bucket.load(Ordering::Relaxed);
            cumulative[i] = running;
        }
        HistogramSnapshot {
            cumulative,
            count: self.count.load(Ordering::Relaxed),
            sum_seconds: self.sum_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0,
        }
    }
}

/// One [`LatencyHistogram`] read at a point in time.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct HistogramSnapshot {
    /// Observations at or below each [`LATENCY_BUCKETS_SECONDS`] bound, cumulative.
    pub cumulative: [u64; LATENCY_BUCKETS_SECONDS.len()],
    /// Total observations, which is also the `+Inf` bucket.
    pub count: u64,
    /// Total observed seconds.
    pub sum_seconds: f64,
}

/// Write-path latency, split by the `op` label ADR-0026 requires.
#[derive(Debug, Default)]
pub struct OpLatencies {
    /// `op="put"`.
    pub put: LatencyHistogram,
    /// `op="delete"`.
    pub delete: LatencyHistogram,
}

impl OpLatencies {
    /// The histogram for `op`, or `None` for an operation that is not a proposal.
    pub fn for_op(&self, op: &str) -> Option<&LatencyHistogram> {
        match op {
            "put" => Some(&self.put),
            "delete" => Some(&self.delete),
            _ => None,
        }
    }

    /// Both histograms, labelled, for a scrape.
    pub fn snapshot(&self) -> BTreeMap<&'static str, HistogramSnapshot> {
        BTreeMap::from([
            ("put", self.put.snapshot()),
            ("delete", self.delete.snapshot()),
        ])
    }
}

/// Everything one scrape needs, gathered once (ADR-0026 "Metric list").
///
/// A plain value struct rather than a process-global registry: the daemon builds one per
/// scrape from the node it owns, so two nodes in one process — which every integration test
/// runs — cannot write into each other's series, the failure a global registry has by
/// construction.
///
/// The last three fields are the ones the **engine cannot know**: free space on the data
/// directory's volume, certificate expiry, and the age of the last backup are facts about a
/// daemon's environment rather than about a Raft node, so the daemon fills them and an
/// embedder without a daemon leaves them out rather than having the engine guess.
#[derive(Debug, Clone)]
pub struct MetricsReport {
    /// Raft and apply state.
    pub node: NodeMetrics,
    /// RocksDB, snapshot, and purge counters — `None` for an ephemeral store, which has no
    /// disk to report on.
    pub storage: Option<StorageMetrics>,
    /// The bounded dedup index (M5, ADR-0025).
    pub dedup: DedupStats,
    /// Compactions applied on this node (ADR-0019).
    pub compactions: u64,
    /// Watch admission, delivery, and termination counters (ADR-0020).
    pub watch: WatchStats,
    /// Observed reachability of each peer, from the advisory gossip source (ADR-0003).
    pub gossip_reachable: BTreeMap<NodeId, bool>,
    /// Gossip hints refused because they disagreed with committed membership.
    pub gossip_endpoint_mismatch: u64,
    /// Observed leadership transitions on this node.
    pub leader_changes: u64,
    /// Leader-only: how far each peer is behind the leader's last log index.
    pub peer_lag: BTreeMap<NodeId, u64>,
    /// Last index this node knows to be committed.
    pub commit_index: Option<u64>,
    /// Submit-to-commit latency of client writes, by `op`.
    pub proposal_latency: BTreeMap<&'static str, HistogramSnapshot>,
    /// Linearizable read barrier latency (ADR-0009).
    pub read_latency: HistogramSnapshot,
    /// Free bytes on the data directory's volume, when the daemon measured it.
    pub disk_free_bytes: Option<u64>,
    /// Seconds until each plane's loaded leaf certificate expires, when the daemon knows.
    pub cert_expiry_seconds: BTreeMap<String, i64>,
    /// Age of the most recent successful backup, when the daemon knows (ADR-0024).
    pub backup_age_seconds: Option<u64>,
    /// Revision-pinned list snapshots this node is holding (M6, ADR-0029, TA-39).
    ///
    /// Daemon-filled, like the three environment facts above: the paginator is built by the
    /// server from its `[list]` section and handed to the client plane, so the engine node
    /// has no handle on it. `None` omits the series rather than exporting a zero, so "this
    /// build has no paginator" and "this node holds no pins" stay distinguishable.
    pub pagination: Option<PinStats>,
    /// The signed policy this node is enforcing (M6, ADR-0027).
    ///
    /// Daemon-filled for the same reason: the reload counters live with the loader that owns
    /// the files. `None` under the static allowlist, which exports no policy series at all.
    pub policy: Option<PolicyMetrics>,
}

/// What one node's signed-policy lifecycle looks like right now (M6, ADR-0027).
///
/// Versions, counts and closed-set reason tokens only. Like [`PolicySummary`], it must never
/// carry a grant, a principal or a key prefix: `/metrics` sits on the same unauthenticated
/// loopback listener as `/health` (OQ-16, ADR-0026).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PolicyMetrics {
    /// The version this node is enforcing, or `None` when it holds no valid document.
    pub version: Option<u64>,
    /// The newest version every voter this node knows of has reported.
    ///
    /// Equal to `version` once convergence completes; lower while the intersection is in
    /// force. This is the gauge an alert watches, because a cluster stuck mid-convergence
    /// denies changed prefixes indefinitely (M6-21).
    pub converged_version: Option<u64>,
    /// Rollbacks performed because `--break-glass-policy-rollback` permitted them (M6-09).
    pub rollbacks: u64,
    /// Refused reloads by reason, seeded with every token in
    /// [`config_core::policy::PolicyRejected::ALL_REASONS`] so a reason that has never fired
    /// reports `0` rather than being absent.
    pub reload_failures: BTreeMap<&'static str, u64>,
    /// Whether this process was started with `--break-glass-policy-rollback`.
    ///
    /// A gauge rather than a log line only: the flag disarms rollback protection for the
    /// whole process lifetime (OQ-57), so an operator must be able to alert on a node that is
    /// still running with it set.
    pub break_glass_active: bool,
}

/// A Prometheus text-exposition writer that emits each metric's `HELP`/`TYPE` exactly once.
struct Exposition {
    out: String,
    current: Option<&'static str>,
}

impl Exposition {
    fn new() -> Self {
        Self {
            out: String::with_capacity(8 * 1024),
            current: None,
        }
    }

    fn metric(&mut self, name: &'static str, kind: &str, help: &str) {
        if self.current != Some(name) {
            self.out.push_str("# HELP ");
            self.out.push_str(name);
            self.out.push(' ');
            self.out.push_str(help);
            self.out.push('\n');
            self.out.push_str("# TYPE ");
            self.out.push_str(name);
            self.out.push(' ');
            self.out.push_str(kind);
            self.out.push('\n');
            self.current = Some(name);
        }
    }

    /// One sample. Labels are written in the order given, so two samples of one metric always
    /// carry their labels in the same order.
    fn sample(&mut self, name: &str, labels: &[(&str, &str)], value: f64) {
        self.out.push_str(name);
        if !labels.is_empty() {
            self.out.push('{');
            for (i, (key, value)) in labels.iter().enumerate() {
                if i > 0 {
                    self.out.push(',');
                }
                self.out.push_str(key);
                self.out.push_str("=\"");
                escape_label_value(value, &mut self.out);
                self.out.push('"');
            }
            self.out.push('}');
        }
        self.out.push(' ');
        self.out.push_str(&format_value(value));
        self.out.push('\n');
    }
}

/// Prometheus label-value escaping: backslash, double quote, and newline.
///
/// ADR-0026's redaction rule keeps keys, values, and anything off the allowlist out of labels
/// in the first place; this is the mechanical backstop, so a reason string that ever gained a
/// quote cannot break the exposition into something a scraper misparses.
fn escape_label_value(value: &str, out: &mut String) {
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
}

/// Render a value the way the exposition format wants it: integers without a decimal point,
/// everything else with enough digits to resolve a microsecond.
fn format_value(value: f64) -> String {
    if value.fract() == 0.0 && value.abs() < 9.007_199_254_740_992e15 {
        format!("{}", value as i64)
    } else {
        format!("{value:.6}")
    }
}

impl MetricsReport {
    /// Render the Prometheus text exposition format (ADR-0026; content type
    /// `text/plain; version=0.0.4`).
    ///
    /// Every series carries `node_id`, because a scrape of three nodes in one process — what
    /// an integration test runs — must not collapse three nodes' series into one.
    pub fn render_prometheus(&self) -> String {
        let node_id = self.node.node_id.0.to_string();
        let node = [("node_id", node_id.as_str())];
        let mut e = Exposition::new();

        // ---- Raft ----
        e.metric(
            "retcd_raft_leader",
            "gauge",
            "1 when this node is the leader it knows about, 0 otherwise",
        );
        let is_leader = u64::from(self.node.current_leader == Some(self.node.node_id));
        e.sample("retcd_raft_leader", &node, is_leader as f64);

        e.metric("retcd_raft_role", "gauge", "1 for this node's current role");
        for role in [
            NodeRole::Learner,
            NodeRole::Follower,
            NodeRole::Candidate,
            NodeRole::Leader,
            NodeRole::Shutdown,
        ] {
            e.sample(
                "retcd_raft_role",
                &[("node_id", node_id.as_str()), ("role", role.as_str())],
                u64::from(self.node.role == role) as f64,
            );
        }

        e.metric("retcd_raft_term", "gauge", "current Raft term");
        e.sample("retcd_raft_term", &node, self.node.current_term as f64);

        e.metric(
            "retcd_raft_leader_changes_total",
            "counter",
            "observed leadership transitions since this node started",
        );
        e.sample(
            "retcd_raft_leader_changes_total",
            &node,
            self.leader_changes as f64,
        );

        e.metric(
            "retcd_raft_commit_index",
            "gauge",
            "last log index known to be committed",
        );
        e.sample(
            "retcd_raft_commit_index",
            &node,
            self.commit_index.unwrap_or(0) as f64,
        );

        e.metric(
            "retcd_raft_applied_index",
            "gauge",
            "last log index applied to the state machine",
        );
        e.sample(
            "retcd_raft_applied_index",
            &node,
            self.node.last_applied.map_or(0, |l| l.index) as f64,
        );

        e.metric(
            "retcd_raft_purged_index",
            "gauge",
            "highest durably purged log index (ADR-0022)",
        );
        e.sample(
            "retcd_raft_purged_index",
            &node,
            self.storage.as_ref().map_or(0, |s| s.purged_index) as f64,
        );

        e.metric(
            "retcd_raft_peer_lag",
            "gauge",
            "leader-only: entries this peer is behind the leader's last log index",
        );
        for (peer, lag) in &self.peer_lag {
            let peer_id = peer.0.to_string();
            e.sample(
                "retcd_raft_peer_lag",
                &[("node_id", node_id.as_str()), ("peer_id", &peer_id)],
                *lag as f64,
            );
        }

        e.metric(
            "retcd_cluster_revision",
            "gauge",
            "public cluster revision applied here (ADR-0005), not a log index",
        );
        e.sample(
            "retcd_cluster_revision",
            &node,
            self.node.cluster_revision as f64,
        );

        // ---- latencies ----
        for (op, hist) in &self.proposal_latency {
            render_histogram(
                &mut e,
                "retcd_proposal_latency_seconds",
                "client write submit-to-commit latency",
                &[("node_id", node_id.as_str()), ("op", op)],
                hist,
            );
        }
        render_histogram(
            &mut e,
            "retcd_linearizable_read_latency_seconds",
            "linearizable read barrier latency (ADR-0009)",
            &node,
            &self.read_latency,
        );

        // ---- storage (ADR-0022) ----
        if let Some(s) = &self.storage {
            e.metric(
                "retcd_rocks_mem_bytes",
                "gauge",
                "RocksDB memory in bytes; cf=\"all\" until per-family properties are read",
            );
            e.sample(
                "retcd_rocks_mem_bytes",
                &[
                    ("node_id", node_id.as_str()),
                    ("cf", "all"),
                    ("kind", "memtable"),
                ],
                s.rocks_memtable_bytes as f64,
            );
            e.sample(
                "retcd_rocks_mem_bytes",
                &[
                    ("node_id", node_id.as_str()),
                    ("cf", "all"),
                    ("kind", "table_readers"),
                ],
                s.rocks_table_readers_bytes as f64,
            );

            e.metric(
                "retcd_rocks_level0_files",
                "gauge",
                "files at level 0, the number that precedes a write stall",
            );
            e.sample(
                "retcd_rocks_level0_files",
                &node,
                s.rocks_level0_files as f64,
            );

            e.metric(
                "retcd_rocks_write_stopped",
                "gauge",
                "1 while RocksDB is stopping writes",
            );
            e.sample(
                "retcd_rocks_write_stopped",
                &node,
                s.rocks_write_stopped as f64,
            );

            e.metric(
                "retcd_snapshot_age_seconds",
                "gauge",
                "age of the current published snapshot",
            );
            e.sample(
                "retcd_snapshot_age_seconds",
                &node,
                s.snapshot_age_ms as f64 / 1000.0,
            );

            e.metric(
                "retcd_snapshot_bytes",
                "gauge",
                "size of the current published snapshot",
            );
            e.sample("retcd_snapshot_bytes", &node, s.snapshot_size_bytes as f64);

            e.metric(
                "retcd_snapshot_build_duration_seconds",
                "gauge",
                "duration of the most recent successful snapshot build",
            );
            e.sample(
                "retcd_snapshot_build_duration_seconds",
                &node,
                s.snapshot_build_duration_ms as f64 / 1000.0,
            );

            e.metric(
                "retcd_snapshot_builds_total",
                "counter",
                "snapshot builds that returned successfully",
            );
            e.sample(
                "retcd_snapshot_builds_total",
                &node,
                s.snapshot_builds as f64,
            );

            e.metric(
                "retcd_snapshot_builds_in_flight",
                "gauge",
                "builds started and not yet published or failed; stuck above zero means wedged",
            );
            e.sample(
                "retcd_snapshot_builds_in_flight",
                &node,
                s.snapshot_builds_in_flight as f64,
            );

            e.metric(
                "retcd_snapshot_installs_total",
                "counter",
                "snapshots received from a leader, by outcome",
            );
            for (outcome, value) in [
                ("success", s.snapshot_installs),
                ("validation_failed", s.snapshot_install_failures),
                ("crash_recovered", s.snapshot_install_redos),
            ] {
                e.sample(
                    "retcd_snapshot_installs_total",
                    &[("node_id", node_id.as_str()), ("outcome", outcome)],
                    value as f64,
                );
            }

            e.metric(
                "retcd_log_purges_total",
                "counter",
                "log purge calls, by outcome (ADR-0022)",
            );
            for (outcome, value) in [
                ("purged", s.purges),
                ("deferred", s.purge_deferrals),
                ("refused", s.purge_refusals),
            ] {
                e.sample(
                    "retcd_log_purges_total",
                    &[("node_id", node_id.as_str()), ("outcome", outcome)],
                    value as f64,
                );
            }
        }

        if let Some(free) = self.disk_free_bytes {
            e.metric(
                "retcd_rocks_disk_free_bytes",
                "gauge",
                "free bytes on the data directory's volume",
            );
            e.sample("retcd_rocks_disk_free_bytes", &node, free as f64);
        }

        // ---- watches (ADR-0020) ----
        e.metric(
            "retcd_watch_streams",
            "gauge",
            "watch streams currently registered",
        );
        e.sample("retcd_watch_streams", &node, self.watch.streams_open as f64);

        e.metric(
            "retcd_watch_queued_bytes_max",
            "gauge",
            "largest per-stream queued byte budget observed since start",
        );
        e.sample(
            "retcd_watch_queued_bytes_max",
            &node,
            self.watch.queue_bytes_max as f64,
        );

        e.metric(
            "retcd_watch_terminations_total",
            "counter",
            "watch streams terminated, by ADR-0020 reason",
        );
        for (reason, count) in &self.watch.terminated_by_reason {
            e.sample(
                "retcd_watch_terminations_total",
                &[("node_id", node_id.as_str()), ("reason", reason.as_str())],
                *count as f64,
            );
        }

        e.metric(
            "retcd_compactions_total",
            "counter",
            "replicated compactions applied here (ADR-0019)",
        );
        e.sample("retcd_compactions_total", &node, self.compactions as f64);

        // ---- dedup (ADR-0025) ----
        e.metric(
            "retcd_dedup_hits_total",
            "counter",
            "mutations answered from a retained deduplication record",
        );
        e.sample("retcd_dedup_hits_total", &node, self.dedup.hits as f64);

        e.metric(
            "retcd_dedup_records",
            "gauge",
            "deduplication records currently retained",
        );
        e.sample("retcd_dedup_records", &node, self.dedup.records as f64);

        e.metric(
            "retcd_dedup_max_records",
            "gauge",
            "configured global cap; at it, writes still apply but are not recorded",
        );
        e.sample(
            "retcd_dedup_max_records",
            &node,
            self.dedup.max_records as f64,
        );

        // Two reasons, and both are records that were actually dropped (C5B-04). `global_cap`
        // used to be reported here from the trim counter, which made every compaction look
        // like cap pressure; the cap's real event is a *refusal*, exported below.
        e.metric(
            "retcd_dedup_evictions_total",
            "counter",
            "deduplication records dropped, by reason",
        );
        for (reason, value) in [
            ("window", self.dedup.window_evictions),
            ("trim", self.dedup.trim_evictions),
        ] {
            e.sample(
                "retcd_dedup_evictions_total",
                &[("node_id", node_id.as_str()), ("reason", reason)],
                value as f64,
            );
        }

        e.metric(
            "retcd_dedup_cap_refusals_total",
            "counter",
            "mutations applied whose outcome the global record cap refused to retain; \
             a resubmission of one of these will apply a second time",
        );
        e.sample(
            "retcd_dedup_cap_refusals_total",
            &node,
            self.dedup.cap_refusals as f64,
        );

        // ---- gossip, authn/authz, certificates, backups ----
        e.metric(
            "retcd_gossip_reachable",
            "gauge",
            "1 when the advisory gossip source last saw this peer as reachable (ADR-0003)",
        );
        for (peer, reachable) in &self.gossip_reachable {
            let peer_id = peer.0.to_string();
            e.sample(
                "retcd_gossip_reachable",
                &[("node_id", node_id.as_str()), ("peer_id", &peer_id)],
                u64::from(*reachable) as f64,
            );
        }

        e.metric(
            "retcd_gossip_endpoint_mismatch_total",
            "counter",
            "gossip hints refused because they disagreed with committed membership",
        );
        e.sample(
            "retcd_gossip_endpoint_mismatch_total",
            &node,
            self.gossip_endpoint_mismatch as f64,
        );

        e.metric(
            "retcd_authn_rejected_total",
            "counter",
            "connections whose transport identity could not be established",
        );
        // One family, one label, two truthful samples (ADR-0026). The client share is the
        // remainder rather than its own counter so that the total stays exactly what
        // `/health` reports: the two can never drift apart by construction.
        e.sample(
            "retcd_authn_rejected_total",
            &[("node_id", node_id.as_str()), ("plane", "client")],
            self.node
                .authn_rejected
                .saturating_sub(self.node.authn_rejected_peer) as f64,
        );
        e.sample(
            "retcd_authn_rejected_total",
            &[("node_id", node_id.as_str()), ("plane", "peer")],
            self.node.authn_rejected_peer as f64,
        );

        e.metric(
            "retcd_authz_denied_total",
            "counter",
            "authorization decisions refused",
        );
        // Two planes, two policies: `authz_denied` is the keyspace policy deciding what a
        // principal may do to a key; `authz_denied_admin` is the `[authz] admins` allowlist
        // deciding whether a principal may reach the admin surface at all (C5B-15). The
        // `plane` label was declared from the start and, until the second sample existed,
        // only ever carried one value.
        for (plane, value) in [
            ("client", self.node.authz_denied),
            ("admin", self.node.authz_denied_admin),
        ] {
            e.sample(
                "retcd_authz_denied_total",
                &[("node_id", node_id.as_str()), ("plane", plane)],
                value as f64,
            );
        }

        // ---- revision-pinned pagination (M6, ADR-0029) ----
        if let Some(pins) = &self.pagination {
            e.metric(
                "retcd_pinned_snapshots",
                "gauge",
                "revision-pinned list snapshots held open for continuations (ADR-0029)",
            );
            e.sample("retcd_pinned_snapshots", &node, pins.len as f64);
        }

        // ---- signed policy lifecycle (M6, ADR-0027) ----
        //
        // The whole block is conditional: a node under the static allowlist has no version to
        // report, and exporting `retcd_policy_version 0` there would put every M3-style
        // deployment on the same dashboard panel as a signed node that failed to load.
        if let Some(policy) = &self.policy {
            // Absent, not zero: "no valid policy" is a different state from "version 0", and
            // an alert on a stuck rotation must be able to tell them apart.
            if let Some(version) = policy.version {
                e.metric(
                    "retcd_policy_version",
                    "gauge",
                    "signed policy document version this node is enforcing (ADR-0027)",
                );
                e.sample("retcd_policy_version", &node, version as f64);
            }
            if let Some(converged) = policy.converged_version {
                e.metric(
                    "retcd_policy_converged_version",
                    "gauge",
                    "newest policy version every known voter has reported; below \
                     retcd_policy_version while changed prefixes are intersected",
                );
                e.sample("retcd_policy_converged_version", &node, converged as f64);
            }

            e.metric(
                "retcd_policy_rollbacks_total",
                "counter",
                "policy rollbacks permitted by --break-glass-policy-rollback (ADR-0027)",
            );
            e.sample(
                "retcd_policy_rollbacks_total",
                &node,
                policy.rollbacks as f64,
            );

            e.metric(
                "retcd_policy_reload_failures_total",
                "counter",
                "policy reloads refused, by reason; the active document is retained",
            );
            for (reason, value) in &policy.reload_failures {
                e.sample(
                    "retcd_policy_reload_failures_total",
                    &[("node_id", node_id.as_str()), ("reason", reason)],
                    *value as f64,
                );
            }

            e.metric(
                "retcd_break_glass_active",
                "gauge",
                "1 while this process runs with --break-glass-policy-rollback (OQ-57)",
            );
            e.sample(
                "retcd_break_glass_active",
                &node,
                u64::from(policy.break_glass_active) as f64,
            );
        }

        if !self.cert_expiry_seconds.is_empty() {
            e.metric(
                "retcd_cert_expiry_seconds",
                "gauge",
                "seconds until the loaded leaf certificate expires; negative once expired",
            );
            for (plane, seconds) in &self.cert_expiry_seconds {
                e.sample(
                    "retcd_cert_expiry_seconds",
                    &[("node_id", node_id.as_str()), ("plane", plane)],
                    *seconds as f64,
                );
            }
        }

        if let Some(age) = self.backup_age_seconds {
            e.metric(
                "retcd_backup_age_seconds",
                "gauge",
                "age of the most recent successful backup (ADR-0024)",
            );
            e.sample("retcd_backup_age_seconds", &node, age as f64);
        }

        e.out
    }
}

/// Emit one histogram: cumulative `le` buckets, `+Inf`, `_sum`, and `_count`.
fn render_histogram(
    e: &mut Exposition,
    name: &'static str,
    help: &str,
    labels: &[(&str, &str)],
    hist: &HistogramSnapshot,
) {
    e.metric(name, "histogram", help);
    let bucket = format!("{name}_bucket");
    for (i, bound) in LATENCY_BUCKETS_SECONDS.iter().enumerate() {
        let bound = format!("{bound}");
        let mut with_le: Vec<(&str, &str)> = labels.to_vec();
        with_le.push(("le", &bound));
        e.sample(&bucket, &with_le, hist.cumulative[i] as f64);
    }
    let mut with_inf: Vec<(&str, &str)> = labels.to_vec();
    with_inf.push(("le", "+Inf"));
    e.sample(&bucket, &with_inf, hist.count as f64);
    e.sample(&format!("{name}_sum"), labels, hist.sum_seconds);
    e.sample(&format!("{name}_count"), labels, hist.count as f64);
}
