//! The node: OpenRaft lifecycle, explicit formation, the client entry points, and the
//! peer-plane server side (spec §6.3, §8.1, §10.1, §13.1; ADR-0009, ADR-0011, ADR-0015).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use config_core::{
    command_gate, Action, Authorizer, Capabilities, ClusterIdentity, Command, CommandResponse,
    ConfigError, Decision, Dedup, DeleteRequest, GetRequest, GetResponse, GossipObservationSource,
    IdentityMismatch, KvState, LeaderHint, Limits, ListRequest, ListResponse, Liveness,
    MutationResponse, NodeId, ObservedPeerHint, Pagination, Principal, PutRequest, SchemaTriple,
    WatchRequest, WatchResumption, WatchRetention, WatchStream, CURRENT_SCHEMA,
    UNAVAILABLE_FEATURE_NOT_ACTIVATED,
};
use config_log::TraceContext;
use config_storage::{
    RaftNode, RaftNodeId, StateReader, StorageMetrics, TraceRegistry, TypeConfig,
};
use openraft::error::{CheckIsLeaderError, ClientWriteError, Fatal, InitializeError, RaftError};
use openraft::{ChangeMembers, Raft, ServerState};
use tokio::task::JoinHandle;
use tracing::{Instrument, Span};

use crate::admin::{AdminError, MembershipReport, ReplicationProgress, SnapshotTriggered};
use crate::config::{AuthzKind, NodeConfig, StorageHandle};
use crate::error::{EngineError, FormationError, FormationPlan, Timeout};
use crate::hint::{validate_hint, HintVerdict};
use crate::metrics::{
    AuthnRejectReason, Health, HealthPayload, LatencyHistogram, LogIdView, MembershipView,
    MetricsReport, NodeMetrics, NodeRole, OpLatencies, PolicySummary,
};
use crate::network::EngineNetworkFactory;
use crate::transport::{
    PeerEnvelopeMeta, PeerHandler, PeerReject, PeerRequest, PeerResponse, PeerSchemas, PeerSink,
    PeerTransport,
};
use crate::watch::{retention_target, JournalView, LeaderClock, WatchHub, WatchStats};

/// How often a bounded wait re-checks its predicate when the metrics channel is quiet.
/// Small enough to keep test deadlines tight, large enough not to spin (test plan §6 rule 2).
const POLL_INTERVAL: Duration = Duration::from_millis(20);

/// Maximum key bytes rendered into a `key_hex` log field (ADR-0013).
const KEY_HEX_MAX_BYTES: usize = 32;

/// Leader-local `(revision, receipt_ms)` samples the retention task keeps.
///
/// One sample per `check_interval`, so 1,024 of them cover the default 24 h policy many
/// times over at the default 60 s cadence; the oldest are dropped first.
const MAX_RETENTION_SAMPLES: usize = 1024;

fn key_hex(key: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(KEY_HEX_MAX_BYTES * 2);
    for byte in key.iter().take(KEY_HEX_MAX_BYTES) {
        out.push(DIGITS[usize::from(byte >> 4)] as char);
        out.push(DIGITS[usize::from(byte & 0x0f)] as char);
    }
    out
}

/// What a `CommandResponse::Noop` for a *command* entry is reported as.
///
/// Deliberately **not** `Unavailable`. By the time `Raft::client_write` returns, the entry is
/// committed and applied on this node. `ConfigError::Unavailable` is contracted to mean
/// "rejected before entering the log", and `ConfigError::is_safe_to_resubmit` answers `true`
/// for it, so reporting this case as `Unavailable` invited a caller to replay a mutation that
/// had already taken effect — ADR-0015's no-duplicate rule broken by the one path that knows
/// for certain the write landed.
///
/// `Noop` for a command entry is a state-machine contract violation, not a condition a client
/// can do anything about: the store answers `Mutation` or `Rejected` for a command, and `Noop`
/// belongs to blank and membership entries. `config-core` has no `Internal` variant of its
/// own, so the internal-class error is [`ConfigError::FatalStorage`] — `INTERNAL` on the wire
/// (spec §6.2), and never resubmittable.
fn noop_for_command_entry() -> ConfigError {
    ConfigError::FatalStorage {
        detail: "the state machine answered Noop for a command entry; the entry is committed \
                 and applied, so this outcome must not be resubmitted"
            .to_string(),
    }
}

/// A 32-byte digest as 64 lowercase hex characters (test plan TA-2).
fn hex32(bytes: &[u8; 32]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for byte in bytes {
        out.push(DIGITS[usize::from(byte >> 4)] as char);
        out.push(DIGITS[usize::from(byte & 0x0f)] as char);
    }
    out
}

pub(crate) struct NodeInner {
    cfg: NodeConfig,
    storage: StorageHandle,
    reader: Arc<dyn StateReader>,
    raft: Raft<TypeConfig>,
    authorizer: Arc<dyn Authorizer>,
    gossip: Arc<dyn GossipObservationSource>,
    span: Span,
    /// `command -> trace_id`, shared with this node's store so the apply line can name the
    /// client trace that produced the entry (ADR-0013).
    traces: Arc<TraceRegistry>,
    stopped: AtomicBool,
    accepted_hints: Mutex<BTreeMap<NodeId, ObservedPeerHint>>,
    background: Mutex<Option<JoinHandle<()>>>,
    /// Authorization decisions refused, counted in the one authorize seam (M3-81).
    authz_denied: AtomicU64,
    /// Admin-plane calls refused by the `[authz] admins` allowlist (ADR-0023, C5B-15). The
    /// allowlist is checked in the transport, before any engine call, so only the transport
    /// can report these — exactly as with `authn_rejected`.
    authz_denied_admin: AtomicU64,
    /// Client-plane authentication rejections, one counter per reason (M3-81, ADR-0028).
    ///
    /// Indexed by `AuthnRejectReason::index`. Per reason rather than one total with a
    /// breakdown beside it: the total `/health` reports is the sum of these, so the two cannot
    /// disagree. The engine never sees a certificate, so only the transport can report them.
    authn_rejected_client: [AtomicU64; AuthnRejectReason::COUNT],
    /// The peer plane's counters, kept apart so `/metrics` can label the family by plane
    /// truthfully instead of attributing a fenced peer to the client plane (ADR-0026).
    authn_rejected_peer: [AtomicU64; AuthnRejectReason::COUNT],
    /// Watch fan-out, the journal gate, and the admission counters (ADR-0020).
    ///
    /// The same `Arc` the store was opened with as its `AppliedBatchSink`: the hub only sees
    /// applied batches because storage publishes into it, so a node whose store was opened
    /// with a different sink would replay history and then go silent.
    watch: Arc<WatchHub>,
    /// When the leader proposes a `Compact` (ADR-0019).
    retention: WatchRetention,
    /// The leader's clock. Read only by the retention task; `apply` has no clock (TA-33).
    clock: Arc<dyn LeaderClock>,
    /// Leader-local `(revision, receipt_ms)` samples, newest last.
    ///
    /// Not state-machine state: not hashed, not persisted, not replicated. A new leader
    /// starts empty and therefore proposes no age-based compaction of history it did not
    /// see arrive (test plan M4-28, M4-29).
    receipts: Mutex<Vec<(u64, u64)>>,
    retention_task: Mutex<Option<JoinHandle<()>>>,
    /// Observed leadership transitions, and the leader they were observed against
    /// (`retcd_raft_leader_changes_total`, ADR-0026).
    ///
    /// Sampled by the background ticker that already polls gossip, rather than derived at
    /// scrape time: a counter that only moved when somebody scraped would miss every change
    /// between two scrapes, which is the flapping this metric exists to show.
    leader_changes: AtomicU64,
    last_seen_leader: Mutex<Option<NodeId>>,
    /// Gossip hints refused because they disagreed with committed membership (spec 18.2,
    /// `retcd_gossip_endpoint_mismatch_total`).
    gossip_endpoint_mismatch: AtomicU64,
    /// Client-write submit-to-commit latency, by op (`retcd_proposal_latency_seconds`).
    proposal_latency: OpLatencies,
    /// Linearizable read barrier latency (`retcd_linearizable_read_latency_seconds`).
    read_latency: LatencyHistogram,
    /// What each peer last advertised on the peer plane (M6, ADR-0030).
    ///
    /// Shared with [`crate::network::EngineNetwork`], which is the only writer: the minimum is
    /// computed from answers this node actually received, never from gossip (M6-102).
    peer_schemas: Arc<PeerSchemas>,
    /// Whether `feature_activated` has already been logged in this process (M6-100, M6-123).
    ///
    /// Per-process and monotonic, with no on-disk marker: activation is a *derived* fact about
    /// the committed voter set, so a restart recomputes it from the same inputs and reaches the
    /// same answer. ADR-0030's Consequences already permit a new leader logging it once more
    /// after a failover, which is what "per node per activation" means in M6-123.
    schema_activated: AtomicBool,
    /// The minimum each gated feature was last refused at, so a refusal is logged once per
    /// window rather than once per request (M6-90, M6-123).
    ///
    /// Keyed by feature and valued by the minimum that refused it: a window is a period during
    /// which the computed minimum does not move, which is exactly the period over which a
    /// second line would tell an operator nothing new.
    gate_logged_at: Mutex<BTreeMap<&'static str, u16>>,
}

/// A running rEtcd node: one OpenRaft instance, one store, one authorizer.
///
/// Cheap to clone (one `Arc`); every clone is the same node. The embedder holds one and
/// hands out [`crate::DirectClient`]s from it.
///
/// # Dropping is not stopping
///
/// There is no `Drop` impl that shuts OpenRaft down, and dropping the last `ConfigNode` is
/// **not** a graceful stop. The background observer holds a `Weak` and exits on its next tick,
/// and OpenRaft's core task ends when its own handles drop — both asynchronously, at an
/// instant nothing here observes. Until then the node still answers peer RPCs and still
/// counts toward quorum. Call [`ConfigNode::stop`] and await it: that is the only point at
/// which the node has demonstrably stopped serving.
#[derive(Clone)]
pub struct ConfigNode {
    inner: Arc<NodeInner>,
}

impl std::fmt::Debug for ConfigNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConfigNode")
            .field("identity", &self.inner.cfg.identity)
            .finish()
    }
}

impl ConfigNode {
    /// Start the node's OpenRaft instance.
    ///
    /// Does **not** form a cluster: a fresh node starts idle and answers `Unavailable` until
    /// [`ConfigNode::form_cluster`] is called or a peer replicates membership to it. That is
    /// the whole point of ADR-0011 — an empty node that self-formed would become a
    /// one-voter authority and silently overwrite the real cluster's configuration.
    ///
    /// Requires a current Tokio runtime; the library never creates one (spec §6.3).
    pub async fn start(
        mut cfg: NodeConfig,
        storage: StorageHandle,
        transport: Arc<dyn PeerTransport>,
        gossip: Arc<dyn GossipObservationSource>,
        authorizer: Arc<dyn Authorizer>,
        watch: Arc<WatchHub>,
    ) -> Result<ConfigNode, EngineError> {
        tokio::runtime::Handle::try_current().map_err(|_| EngineError::NoRuntime)?;

        let identity = cfg.identity;
        if storage.identity() != identity {
            return Err(EngineError::Storage(
                IdentityMismatch {
                    stored: storage.identity(),
                    configured: identity,
                }
                .to_string(),
            ));
        }

        // Seeded with this node's own schema: the leader never calls itself on the peer plane,
        // so without this it would read its own entry as the schema-1 default and could never
        // reach a minimum of 2 (M6-96).
        let peer_schemas = Arc::new(PeerSchemas::default());
        peer_schemas.record(identity.node_id, cfg.schema);

        // Child of whatever span the caller is in, so under `#[retcd_test]` every line this
        // node ever emits — including from OpenRaft's own core task — carries `testMethod`.
        let span = tracing::info_span!(
            "node",
            node_id = identity.node_id.0,
            cluster_id = %identity.cluster_id,
            recovery_epoch = identity.recovery_epoch.0,
        );

        // The snapshot policy is settled here, against the store that has to honour it: an
        // ephemeral store cannot build a snapshot, and OpenRaft treats a failed build as fatal
        // (M5, ADR-0022). The store is told the part it owns — how many published files to
        // retain — because nothing else in the system knows the policy at open time.
        cfg.snapshot = cfg.effective_snapshot(&storage);
        if let StorageHandle::Rocks(store) = &storage {
            store.configure_snapshots(&cfg.snapshot);
        }
        let raft_config = Arc::new(
            cfg.openraft_config()
                .validate()
                .map_err(|e| EngineError::Raft(e.to_string()))?,
        );

        let traces = storage.traces();

        let factory = EngineNetworkFactory {
            identity,
            transport,
            span: span.clone(),
            traces: Arc::clone(&traces),
            schema: cfg.schema,
            peer_schemas: Arc::clone(&peer_schemas),
        };

        // Hand-matched per store kind rather than dispatched through a wrapper: `Raft::new` is
        // generic over the two storage types but always yields the same `Raft<TypeConfig>`, so
        // a match here costs one arm per store and keeps every OpenRaft call monomorphic.
        let raft = match &storage {
            StorageHandle::Ephemeral(s) => {
                Raft::new(
                    identity.node_id.0,
                    raft_config,
                    factory,
                    s.log_store(),
                    s.state_machine(),
                )
                .instrument(span.clone())
                .await
            }
            StorageHandle::Rocks(s) => {
                Raft::new(
                    identity.node_id.0,
                    raft_config,
                    factory,
                    s.log_store(),
                    s.state_machine(),
                )
                .instrument(span.clone())
                .await
            }
        }
        .map_err(|e| EngineError::Raft(e.to_string()))?;

        let cfg_watch_retention = cfg.watch_retention;
        let inner = Arc::new(NodeInner {
            reader: storage.reader(),
            cfg,
            storage,
            raft,
            authorizer,
            gossip,
            span: span.clone(),
            traces,
            stopped: AtomicBool::new(false),
            accepted_hints: Mutex::new(BTreeMap::new()),
            background: Mutex::new(None),
            authz_denied: AtomicU64::new(0),
            authz_denied_admin: AtomicU64::new(0),
            authn_rejected_client: std::array::from_fn(|_| AtomicU64::new(0)),
            authn_rejected_peer: std::array::from_fn(|_| AtomicU64::new(0)),
            retention: cfg_watch_retention,
            clock: watch.clock(),
            watch,
            receipts: Mutex::new(Vec::new()),
            retention_task: Mutex::new(None),
            leader_changes: AtomicU64::new(0),
            last_seen_leader: Mutex::new(None),
            gossip_endpoint_mismatch: AtomicU64::new(0),
            proposal_latency: OpLatencies::default(),
            read_latency: LatencyHistogram::default(),
            peer_schemas,
            schema_activated: AtomicBool::new(false),
            gate_logged_at: Mutex::new(BTreeMap::new()),
        });
        inner
            .watch
            // Signed mode is always "ready" for the hub's purposes, because its authorizer
            // is itself fail-closed: with no document every event is denied, and a document can
            // arrive later without a restart. A latch set once at startup would keep denying
            // after the document arrived (M6-27, C6R-01). The static models' latch is the truth
            // for their whole lifetime.
            .set_authz_ready(
                inner.cfg.authz_kind.is_present() || inner.cfg.authz_kind.is_signed_mode(),
            );
        inner
            .watch
            .attach(&inner.reader, Arc::clone(&inner.authorizer), span.clone());

        let retention_handle =
            tokio::spawn(retention_loop(Arc::downgrade(&inner)).instrument(span.clone()));
        *inner
            .retention_task
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(retention_handle);

        let handle = tokio::spawn(
            background_loop(
                Arc::downgrade(&inner),
                inner.raft.metrics(),
                inner.cfg.gossip_poll,
            )
            .instrument(span.clone()),
        );
        *inner.background.lock().unwrap_or_else(|e| e.into_inner()) = Some(handle);

        span.in_scope(|| {
            tracing::info!(
                peer_endpoint = %inner.cfg.peer_endpoint,
                client_endpoint = %inner.cfg.client_endpoint(),
                durability = ?inner.storage.durability(),
                authz_kind = inner.cfg.authz_kind.as_str(),
                "node started"
            )
        });
        Ok(ConfigNode { inner })
    }

    /// The server side of the peer plane, for `config-grpc`'s `PeerService` or the
    /// in-process router to deliver into.
    pub fn peer_handler(&self) -> PeerHandler {
        PeerHandler::new(Arc::clone(&self.inner) as Arc<dyn PeerSink>)
    }

    /// This node's identity.
    pub fn identity(&self) -> ClusterIdentity {
        self.inner.cfg.identity
    }

    /// The replicated caps this node applies.
    pub fn limits(&self) -> Limits {
        self.inner.cfg.limits
    }

    /// Create the cluster (spec §13.1).
    ///
    /// Succeeds only when the plan matches this node's identity, this node is one of the
    /// plan's voters, and the local store is genuinely fresh. Calling it twice, or calling it
    /// on a node that already has committed membership, is [`FormationError::AlreadyFormed`].
    pub async fn form_cluster(&self, plan: FormationPlan) -> Result<(), FormationError> {
        let inner = &self.inner;
        let identity = inner.cfg.identity;
        let span = inner.span.clone();

        if plan.cluster_id != identity.cluster_id || plan.recovery_epoch != identity.recovery_epoch
        {
            return Err(FormationError::IdentityMismatch(IdentityMismatch {
                stored: identity,
                configured: ClusterIdentity {
                    cluster_id: plan.cluster_id,
                    recovery_epoch: plan.recovery_epoch,
                    node_id: identity.node_id,
                },
            }));
        }
        let Some(planned_peer) = plan.voters.get(&identity.node_id) else {
            return Err(FormationError::NotAVoter);
        };
        // The plan's entry for *this* node must be what this node actually serves. Without
        // this check `NodeConfig::peer_endpoint` was decorative: the plan decided what went
        // into committed membership, and a node could advertise one address while the cluster
        // committed another. The disagreement would then surface as an unreachable peer or a
        // leader hint clients cannot dial.
        let planned_client = plan
            .client_endpoint_of(identity.node_id)
            .unwrap_or(planned_peer.as_str());
        for (plane, planned, configured) in [
            (
                "peer",
                planned_peer.as_str(),
                inner.cfg.peer_endpoint.as_str(),
            ),
            ("client", planned_client, inner.cfg.client_endpoint()),
        ] {
            if planned != configured {
                span.in_scope(|| {
                    tracing::warn!(
                        plane,
                        planned,
                        configured,
                        "formation plan disagrees with this node's configured endpoint"
                    )
                });
                return Err(FormationError::EndpointMismatch {
                    plane,
                    planned: planned.to_string(),
                    configured: configured.to_string(),
                });
            }
        }
        // Checked before freshness so a second `form_cluster` on a working cluster reports the
        // thing the caller actually did wrong. A store that is dirty but *unformed* — a
        // half-wiped data directory — still reports `StoreNotFresh`, which is the case
        // ADR-0011 exists for.
        //
        // This is the one place that deliberately consults the *effective* membership as well
        // as the committed one. A hint must only ever come from committed membership, but a
        // formation guard wants the stricter of the two: a node that has merely *appended* a
        // membership entry is already formed enough that forming it again is a mistake.
        if inner.committed_membership().is_formed() || inner.effective_membership().is_formed() {
            return Err(FormationError::AlreadyFormed);
        }
        if !inner.storage.is_fresh() {
            return Err(FormationError::StoreNotFresh);
        }

        let members: BTreeMap<RaftNodeId, RaftNode> = plan
            .voters
            .iter()
            .map(|(id, peer)| {
                let client = plan.client_endpoint_of(*id).unwrap_or(peer.as_str());
                (id.0, RaftNode::new(peer.clone(), client))
            })
            .collect();

        span.in_scope(|| {
            tracing::info!(
                voters = ?plan.voters.keys().map(|n| n.0).collect::<Vec<_>>(),
                "forming cluster"
            )
        });

        match inner
            .raft
            .initialize(members)
            .instrument(span.clone())
            .await
        {
            Ok(()) => Ok(()),
            Err(RaftError::APIError(InitializeError::NotAllowed(_))) => {
                Err(FormationError::AlreadyFormed)
            }
            Err(e) => Err(FormationError::Raft(e.to_string())),
        }
    }

    /// Stop the node: shut OpenRaft down and stop the background observer.
    ///
    /// Every later client call returns `Unavailable { reason: "stopped" }` rather than
    /// hanging (M1-40).
    pub async fn stop(&self) -> Result<(), EngineError> {
        if self.inner.stopped.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        if let Some(handle) = self
            .inner
            .background
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            handle.abort();
        }
        if let Some(handle) = self
            .inner
            .retention_task
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            handle.abort();
        }
        // Before OpenRaft shuts down, so an open stream ends with `Unavailable` (the node is
        // going away) rather than racing the metrics watcher into `NotLeader` (a redirect to
        // somewhere it could usefully reconnect). M4-85 is exactly that distinction.
        self.inner.watch.shutdown();
        let res = self
            .inner
            .raft
            .shutdown()
            .instrument(self.inner.span.clone())
            .await;
        self.inner.span.in_scope(|| tracing::info!("node stopped"));
        res.map_err(|e| EngineError::Raft(e.to_string()))
    }

    // ---------------- observable state ----------------

    /// A cheap synchronous snapshot of this node's Raft and storage state (TA-6).
    pub fn metrics(&self) -> NodeMetrics {
        self.inner.metrics()
    }

    /// Last applied log index, or `0` when nothing has been applied.
    pub fn applied_index(&self) -> u64 {
        self.inner.reader.last_applied().map_or(0, |id| id.index)
    }

    /// What this node advertises on all three planes (M6, ADR-0030).
    #[must_use]
    pub fn local_schema(&self) -> SchemaTriple {
        self.inner.cfg.schema
    }

    /// The lowest schema any committed voter is known to have, or `None` off the leader.
    ///
    /// Computed from committed membership plus the peer-plane answers this node has actually
    /// received — never from gossip, which any node can be made to say anything on (M6-102,
    /// OQ-63). **Voters only**: a learner that lags the cluster's schema must not hold a
    /// feature back, or M5's learner-replacement flow could never run during an upgrade
    /// (M6-88).
    ///
    /// `None` off the leader, because only a leader calls every voter; see the field docs on
    /// [`HealthPayload::cluster_min_schema`].
    #[must_use]
    pub fn cluster_min_schema(&self) -> Option<SchemaTriple> {
        self.inner.cluster_min_schema()
    }

    /// Refuse `cmd` unless every committed voter can decode it (M6-90..M6-92, ADR-0030 A7).
    ///
    /// Called on the propose path, before `client_write`, and **never** at apply time: once an
    /// entry is committed, a voter that cannot decode it has no recovery — it can neither skip
    /// it nor read it. That asymmetry is the whole reason the gate is a safety property rather
    /// than an optimisation.
    pub fn schema_gate(&self, cmd: &Command) -> Result<(), ConfigError> {
        self.inner.schema_gate(cmd)
    }

    /// Propose `cmd` with the schema gate **skipped** — harness only (test-plan row M6-101).
    ///
    /// Behind the `testing` feature, which `config-server` never enables, so a released daemon
    /// does not contain this function at all. It exists because M6-101 has to reach a state no
    /// supported path can produce: a committed entry the gate would have refused. That entry is
    /// the thing [`ConfigNode::schema_gate`] exists to prevent, and the only honest way to show
    /// why apply cannot be the place to stop it is to put one there and watch apply take it.
    #[cfg(feature = "testing")]
    pub async fn propose_skipping_the_schema_gate(&self, cmd: Command) -> Result<(), ConfigError> {
        self.inner
            .raft
            .client_write(cmd)
            .await
            .map(|_| ())
            .map_err(|e| ConfigError::Unavailable {
                reason: e.to_string(),
            })
    }

    /// The deterministic hash of applied replicated state (test plan TA-2). Two nodes with
    /// the same applied prefix hash identically.
    pub fn state_hash(&self) -> [u8; 32] {
        self.inner.reader.state_hash()
    }

    /// Committed membership: voter ids, their committed peer and client endpoints, and the
    /// membership log id. The only authoritative answer to "who is in this cluster".
    ///
    /// Read from the applied state machine, which only ever holds committed entries — not
    /// from OpenRaft's *effective* membership, which moves before any quorum has agreed
    /// (ADR-0009).
    pub fn committed_membership(&self) -> MembershipView {
        self.inner.committed_membership()
    }

    /// The Raft core's own committed membership, asked of the core task directly.
    ///
    /// Equivalent to [`ConfigNode::committed_membership`] — the state machine cannot have
    /// applied a membership entry the core has not committed, and the core commits nothing it
    /// will not apply — but it comes from the other side of the boundary, which is what makes
    /// it worth asserting the two agree. Async, because OpenRaft answers it on its core task.
    pub async fn raft_committed_membership(&self) -> Result<MembershipView, EngineError> {
        self.inner
            .raft
            .with_raft_state(|st| {
                let m = st.membership_state.committed();
                membership_view(
                    m.voter_ids(),
                    m.nodes().map(|(id, node)| (*id, node.clone())),
                    *m.log_id(),
                )
            })
            .await
            .map_err(|e| EngineError::Raft(e.to_string()))
    }

    /// The validated leader hint, built from `current_leader` plus committed membership.
    /// Never from gossip (ADR-0003, ADR-0009).
    pub fn leader_hint(&self) -> Option<LeaderHint> {
        self.inner.leader_hint()
    }

    /// What this node will do with client traffic right now (spec §18.1).
    pub fn health(&self) -> Health {
        self.inner.health()
    }

    /// Whether this node will serve client traffic: committed membership known, storage not
    /// poisoned, and an authorization model actually in force (OQ-19).
    pub fn is_ready(&self) -> bool {
        self.inner.is_ready()
    }

    /// The full serializable health payload (test plan TA-17).
    ///
    /// Async because `committed` — the Raft core's commit index — is only knowable by asking
    /// the core task. Carries no keys and no values, only ids, counts, revisions, enums and
    /// the state-hash digest, so it is safe to serve on an unauthenticated loopback listener
    /// (§15.2, OQ-16).
    ///
    /// [`ConfigNode::health`] stays as the cheap synchronous "what will you do with my
    /// request" answer; this is the cross-process state oracle.
    pub async fn health_payload(&self) -> HealthPayload {
        let inner = &self.inner;
        let committed = inner
            .raft
            .with_raft_state(|st| st.committed.map(|l| l.index))
            .await
            .ok()
            .flatten();
        let m = inner.metrics();
        let membership = inner.committed_membership();
        let (node_id, cluster_id, recovery_epoch) =
            HealthPayload::identity_fields(&inner.cfg.identity);
        // Read straight from the store rather than through `journal_view`, which collapses an
        // empty journal's bounds onto zero: the payload distinguishes "empty" from "starts at
        // the beginning", and a zero would answer a different question than the one TA-39 asks.
        let journal = inner.reader.journal_stats().ok();
        let compact_revision = self.compact_revision();
        HealthPayload {
            node_id,
            cluster_id,
            recovery_epoch,
            role: m.role,
            current_leader: m.current_leader,
            term: m.current_term,
            last_applied: m.last_applied.map(|l| l.index),
            committed,
            membership_voter_ids: membership.voters.iter().copied().collect(),
            membership_log_id: membership
                .membership_log_id
                .map(|(term, index)| LogIdView::new(term, index)),
            cluster_revision: m.cluster_revision,
            state_hash_hex: hex32(&inner.reader.state_hash()),
            applied_commands: m.applied_commands,
            durability: inner.storage.durability(),
            ready: inner.is_ready(),
            authz_kind: inner.authz_kind(),
            schema: inner.cfg.schema,
            cluster_min_schema: inner.cluster_min_schema(),
            transport_security: inner.cfg.transport_security,
            policy: inner.policy_summary(),
            restored_from: inner.reader.restored_from(),
            authz_denied: m.authz_denied,
            authn_rejected: m.authn_rejected,
            compact_revision,
            journal_oldest_revision: journal.as_ref().and_then(|j| j.oldest_revision),
            journal_newest_revision: journal.as_ref().and_then(|j| j.newest_revision),
            journal_hash: hex32(&self.journal_hash(compact_revision)),
            watch_streams_open: inner.watch.stats().streams_open,
            // The engine knows the version because it holds the authorizer; it does not know
            // *why* a load failed, because it does not hold the files. The daemon fills
            // `policy_state` (M6-16) exactly as it fills the three environment facts on
            // `MetricsReport`.
            policy_version: inner.authorizer.policy_version(),
            policy_state: None,
        }
    }

    /// Gather everything one `/metrics` scrape needs (M5, ADR-0026).
    ///
    /// Async because the commit index lives on OpenRaft's core task, like
    /// [`ConfigNode::health_payload`]. Everything else is a counter read or a synchronous
    /// state read, so a scrape costs one round trip to the core and no locks a request path
    /// waits on.
    ///
    /// Left for the daemon to fill: `disk_free_bytes`, `cert_expiry_seconds`, and
    /// `backup_age_seconds` are facts about an environment — a filesystem, a certificate file,
    /// a backup directory — not about a Raft node, and the engine has no handle on any of
    /// them. Each is an `Option`/empty map whose series is *omitted* rather than exported as a
    /// zero, because "no free-space reading" and "no free space" must not look alike on a
    /// dashboard. As of M5 the daemon leaves all three unset: free space needs a platform
    /// syscall, expiry needs X.509 parsing the TLS layer does not do today, and backup age
    /// belongs to the backup command's own bookkeeping. The three alerts in
    /// `docs/runbooks/alerts.md` that would use them are documented as not-yet-armed.
    pub async fn metrics_report(&self) -> MetricsReport {
        let inner = &self.inner;
        let commit_index = inner
            .raft
            .with_raft_state(|st| st.committed.map(|l| l.index))
            .await
            .ok()
            .flatten();
        let m = inner.metrics();
        // Off the leader this map is empty by construction (`MembershipReport::replication`),
        // so a follower exports no `retcd_raft_peer_lag` series at all rather than exporting
        // zeros that would read as "every peer is caught up".
        let peer_lag = inner
            .membership_report()
            .replication
            .into_iter()
            .map(|(id, progress)| (id, progress.lag))
            .collect();
        let gossip_reachable = inner
            .gossip
            .peers()
            .into_iter()
            .map(|hint| (hint.node_id, matches!(hint.liveness, Liveness::Alive)))
            .collect();
        MetricsReport {
            node: m,
            storage: self.storage_metrics(),
            dedup: inner.reader.dedup_stats(),
            compactions: inner.reader.compactions(),
            watch: inner.watch.stats(),
            gossip_reachable,
            gossip_endpoint_mismatch: inner.gossip_endpoint_mismatch.load(Ordering::Relaxed),
            leader_changes: inner.leader_changes.load(Ordering::Relaxed),
            peer_lag,
            commit_index,
            proposal_latency: inner.proposal_latency.snapshot(),
            read_latency: inner.read_latency.snapshot(),
            disk_free_bytes: None,
            cert_expiry_seconds: BTreeMap::new(),
            backup_age_seconds: None,
            authn_rejected_transport: Vec::new(),
            tls: None,
            pagination: None,
            policy: None,
        }
    }

    /// RocksDB, snapshot, and purge counters, or `None` for an ephemeral store.
    pub fn storage_metrics(&self) -> Option<StorageMetrics> {
        match &self.inner.storage {
            StorageHandle::Rocks(s) => Some(s.metrics()),
            StorageHandle::Ephemeral(_) => None,
        }
    }

    /// Record that a caller's transport identity could not be established (M3-81).
    ///
    /// Called by the transport — `config-grpc`'s client plane, when the peer certificate
    /// yields no principal — because the engine never sees a certificate. It is deliberately
    /// *not* an authorization denial: no principal existed, so no [`config_core::Authorizer`]
    /// was consulted and no audit line was written. The two counters are kept apart for that
    /// reason: "who are you" failing and "you may not" failing are different operator
    /// problems with different fixes.
    /// Records the **client-plane** share only; the peer plane's `identity_retired` fence is
    /// counted inside the engine, where that check lives.
    ///
    /// `reason` is a closed set rather than a string because it becomes a metric label, and a
    /// label fed from an error message grows a new time series every time a dependency rewords
    /// one (ADR-0026).
    pub fn record_authn_rejection(&self, reason: AuthnRejectReason) {
        self.inner.authn_rejected_client[reason.index()].fetch_add(1, Ordering::Relaxed);
    }

    /// Record one admin-plane refusal by the `[authz] admins` allowlist (ADR-0023, C5B-15).
    ///
    /// Same seam and same reason as [`ConfigNode::record_authn_rejection`]: the allowlist is
    /// enforced in the transport so a non-admin never reaches consensus, and the node is the
    /// only place that keeps counters. Without this, `retcd_authz_denied_total{plane="admin"}`
    /// reads zero no matter how many admin calls were turned away.
    pub fn record_admin_authz_denial(&self) {
        self.inner
            .authz_denied_admin
            .fetch_add(1, Ordering::Relaxed);
    }

    /// What this node actually guarantees (ADR-0016).
    pub fn capabilities(&self) -> Capabilities {
        Capabilities {
            durability: self.inner.storage.durability(),
            // M4: the journal is retained and its watermark is visible to a client, both as
            // `RevisionCompacted { minimum_available_revision }` and in the health payload
            // (ADR-0016, ADR-0020).
            watch_resumption: WatchResumption::Retained {
                compact_revision_visible: true,
            },
            // The live model and the live version, not the startup snapshot: a signed node
            // that adopted its first document after boot advertises `SignedPolicy` with that
            // version, and one that lost its document advertises no version. A capability that
            // can lie is worse than no capability at all (ADR-0016, M6-38, C6R-04).
            authz: self
                .inner
                .authz_kind()
                .to_capability(self.inner.authorizer.policy_version()),
            transport_security: self.inner.cfg.transport_security,
            pagination: Pagination::Unsupported,
            // M5: reported from the enforced limits, never from a build flag - a node whose
            // `[dedup]` section is absent or disabled is an M4 build as far as a client can
            // tell, and one that has it on advertises the window a resubmission survives
            // (ADR-0016, ADR-0025, M5-108).
            dedup: if self.inner.cfg.limits.dedup.enabled {
                Dedup::Bounded {
                    window_requests: self.inner.cfg.limits.dedup.window_requests,
                }
            } else {
                Dedup::Unsupported
            },
        }
    }

    /// Gossip hints that agreed with committed membership, for telemetry only.
    pub fn accepted_hints(&self) -> BTreeMap<NodeId, ObservedPeerHint> {
        self.inner
            .accepted_hints
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Wait until this node knows a leader, or `deadline` expires.
    ///
    /// Returns the observed leader rather than panicking on timeout, so a test can assert
    /// "no leader was ever elected" as a positive result (M1-01).
    pub async fn wait_for_leader(&self, deadline: Duration) -> Option<NodeId> {
        self.inner
            .wait_until(deadline, "a leader", |m| m.current_leader)
            .await
            .ok()
    }

    /// Wait until this node has applied up to `index`, or `deadline` expires.
    pub async fn wait_applied(&self, index: u64, deadline: Duration) -> Result<(), Timeout> {
        self.inner
            .wait_until(deadline, &format!("applied index >= {index}"), |m| {
                (m.last_applied.map_or(0, |l| l.index) >= index).then_some(())
            })
            .await
    }

    /// Wait until `predicate` holds for this node's metrics, or `deadline` expires.
    ///
    /// The general form behind [`ConfigNode::wait_for_leader`] and
    /// [`ConfigNode::wait_applied`]; a harness uses it instead of sleeping.
    pub async fn wait_until<T>(
        &self,
        deadline: Duration,
        what: &str,
        predicate: impl Fn(&NodeMetrics) -> Option<T>,
    ) -> Result<T, Timeout> {
        self.inner.wait_until(deadline, what, predicate).await
    }

    // ---------------- client entry points ----------------

    /// Write one key through Raft (spec §8.1).
    pub async fn put(
        &self,
        principal: &Principal,
        req: PutRequest,
    ) -> Result<MutationResponse, ConfigError> {
        let span = self.inner.op_span("put");
        self.inner
            .mutate(principal, "put", req.key.clone(), || {
                config_core::validate_put(&req, &self.inner.cfg.limits)?;
                Ok(Command::from(&req))
            })
            .instrument(span)
            .await
    }

    /// Remove one key through Raft (spec §8.1).
    pub async fn delete(
        &self,
        principal: &Principal,
        req: DeleteRequest,
    ) -> Result<MutationResponse, ConfigError> {
        let span = self.inner.op_span("delete");
        self.inner
            .mutate(principal, "delete", req.key.clone(), || {
                config_core::validate_delete(&req, &self.inner.cfg.limits)?;
                Ok(Command::from(&req))
            })
            .instrument(span)
            .await
    }

    /// Read one key at a leader-linearizable point (spec §10.1).
    pub async fn get(
        &self,
        principal: &Principal,
        req: GetRequest,
    ) -> Result<GetResponse, ConfigError> {
        let span = self.inner.op_span("get");
        let key = req.key.clone();
        let limits = self.inner.cfg.limits;
        self.inner
            .read_validated(
                principal,
                "get",
                key.clone(),
                move || config_core::validate_get(&req, &limits),
                move |s, _: &()| s.get_response(&key),
                |r: &GetResponse| r.read_revision,
            )
            .instrument(span)
            .await
    }

    /// Scan one prefix at a leader-linearizable point (spec §10.2).
    ///
    /// `List` is leader-linearized exactly like `Get`: an isolated former leader must not
    /// serve a partial scan out of its stale local state (M1-13).
    pub async fn list(
        &self,
        principal: &Principal,
        req: ListRequest,
    ) -> Result<ListResponse, ConfigError> {
        let span = self.inner.op_span("list");
        let limits = self.inner.cfg.limits;
        self.inner
            .read_validated(
                principal,
                "list",
                req.prefix.clone(),
                move || config_core::validate_list(&req, &limits),
                move |s, effective: &ListRequest| s.list(effective),
                |r: &ListResponse| r.read_revision,
            )
            .instrument(span)
            .await
    }

    // ---------------- watches (spec §11, ADR-0019, ADR-0020) ----------------

    /// Watch one prefix from a resume cursor (spec §11.2).
    ///
    /// The seven steps of §11.2, in the order the test plan pins them:
    ///
    /// 1. validate the request and authorize the prefix (M4-49: a denial must precede the
    ///    admission slot, or an unauthorized caller could exhaust the node's stream budget);
    /// 2. confirm leadership with `ensure_linearizable` (M4-84: a follower consumes no slot);
    /// 3. admit the stream against the node and per-principal caps;
    /// 4. under the serialized journal gate, validate the cursor against `compact_revision`,
    ///    capture the high-water revision `H`, and subscribe to the live fan-out;
    /// 5. replay the journal's `(R, H]` in pages, prefix-filtered and re-authorized per event;
    /// 6. buffer live items above `H` while replay runs;
    /// 7. drain the buffer and switch to live delivery.
    ///
    /// Steps 1-2 live here because only the node owns the authorizer seam and the Raft
    /// handle; steps 3-7 are [`crate::watch::WatchHub`].
    ///
    /// ADR-0020 lists admission *before* the leader check. It is done after it here, because
    /// the plan's own rows require a follower to consume no admission slot, and confirming
    /// leadership is the cheaper of the two ways to satisfy that.
    pub async fn watch(
        &self,
        principal: &Principal,
        req: WatchRequest,
    ) -> Result<WatchStream, ConfigError> {
        let span = self.inner.op_span("watch");
        self.inner
            .watch_inner(principal, req)
            .instrument(span)
            .await
    }

    /// This node's watch counters (test plan TA-34).
    pub fn watch_stats(&self) -> WatchStats {
        self.inner.watch.stats()
    }

    /// The hub itself, for the gate hooks the deterministic-interleaving rows drive
    /// (test plan TA-30).
    pub fn watch_hub(&self) -> &Arc<WatchHub> {
        &self.inner.watch
    }

    /// The replicated compaction watermark: no event at or below it is retained.
    pub fn compact_revision(&self) -> u64 {
        self.inner.reader.compact_revision().unwrap_or(0)
    }

    /// A digest over retained journal events above `from_exclusive` (test plan TA-31).
    ///
    /// Separate from [`ConfigNode::state_hash`], which keeps its M1/M2 definition: a
    /// node-local v1-to-v2 migration stamp makes `compact_revision` differ across a rolling
    /// upgrade, and folding it into `state_hash` would break every existing row.
    pub fn journal_hash(&self, from_exclusive: u64) -> [u8; 32] {
        self.inner
            .reader
            .journal_hash(from_exclusive)
            .unwrap_or([0u8; 32])
    }

    /// Retained-journal shape: oldest and newest revision, count and bytes.
    pub fn journal_view(&self) -> JournalView {
        self.inner.reader.journal_stats().map_or(
            JournalView {
                oldest_revision: 0,
                newest_revision: 0,
                count: 0,
                bytes: 0,
            },
            |s| JournalView {
                // `None` means "empty", and `JournalView` says the same thing with a
                // zero — there is no revision 0, so the two spellings cannot be confused.
                oldest_revision: s.oldest_revision.unwrap_or(0),
                newest_revision: s.newest_revision.unwrap_or(0),
                count: s.count,
                bytes: s.bytes,
            },
        )
    }

    /// Propose a compaction through the ordinary write path (test plan TA-36).
    ///
    /// There is deliberately no back door that writes `compact_revision` on one node: a
    /// watermark that did not travel through the log would make "followers compacted
    /// identically" pass vacuously.
    pub async fn propose_compact(
        &self,
        principal: &Principal,
        up_to_revision: u64,
    ) -> Result<u64, ConfigError> {
        self.inner.propose_compact(principal, up_to_revision).await
    }

    // ---------------- admin plane (M5, ADR-0023) ----------------

    /// Everything one node knows about membership: committed voters, effective learners, the
    /// joint-config length, the retired set, and — on the leader — per-peer replication
    /// progress (test plan TA-45).
    ///
    /// Synchronous and lock-light: it reads the metrics watch channel and the applied state,
    /// so an operator may poll it as tightly as they like while waiting for a learner.
    pub fn membership_report(&self) -> MembershipReport {
        self.inner.membership_report()
    }

    /// How many log entries `node_id` is behind this leader, or `None` when this node is not
    /// the leader or has heard nothing from that peer.
    ///
    /// The **only** admissible catch-up oracle (architecture A5, research trap T7): a
    /// `Raft::add_learner(blocking = true)` that returns `Ok` proves nothing, because
    /// openraft logs and discards the wait's result.
    pub fn replication_lag(&self, node_id: NodeId) -> Option<u64> {
        self.inner.membership_report().lag_of(node_id)
    }

    /// Ids a committed `RetireNode` has fenced out of this cluster for good (ADR-0023).
    pub fn retired_nodes(&self) -> BTreeSet<NodeId> {
        self.inner.retired_nodes()
    }

    /// Add `node_id` as a **learner** at the given endpoints (spec §13.2, ADR-0023).
    ///
    /// Non-blocking on purpose: openraft's blocking form waits for catch-up and then throws
    /// the wait's result away (trap T7), so an `Ok` from it would be an acknowledgement that
    /// looked like a promise. Catch-up is proven only by polling
    /// [`ConfigNode::membership_report`].
    ///
    /// Refuses a retired id — the readmission half of the fence, without which network
    /// fencing alone would let a retired node be invited back in (test plan M5-62).
    pub async fn add_learner(
        &self,
        node_id: NodeId,
        peer_endpoint: String,
        client_endpoint: String,
    ) -> Result<Option<LogIdView>, AdminError> {
        self.inner
            .add_learner(node_id, peer_endpoint, client_endpoint)
            .await
    }

    /// Promote a caught-up learner to voter (ADR-0023, A5).
    ///
    /// The lag predicate is evaluated **here, live, on the leader** at the moment of the
    /// call, against `RaftMetrics.replication` — not against anything the caller passed in.
    pub async fn promote_voter(&self, node_id: NodeId) -> Result<Option<LogIdView>, AdminError> {
        self.inner.promote_voter(node_id).await
    }

    /// Remove `node_id` and fence its identity (lead ruling M5-R5).
    ///
    /// Three replicated steps, in this order and no other:
    /// 1. `RemoveVoters({id})` with `retain = true` — the id becomes a learner, which is the
    ///    only shape `RemoveNodes` will accept next;
    /// 2. `RemoveNodes({id})` — it leaves membership entirely;
    /// 3. `Command::RetireNode { id }` — replicated state that every node, including one that
    ///    was partitioned while steps 1 and 2 happened, will refuse to talk to afterwards.
    ///
    /// Idempotent: re-issuing it against an already-removed, already-retired id is a no-op
    /// (test plan M5-67).
    pub async fn remove_member(&self, node_id: NodeId) -> Result<Option<LogIdView>, AdminError> {
        self.inner.remove_member(node_id).await
    }

    /// Ask this node to build a snapshot now (ADR-0022, OQ-44).
    ///
    /// Answers [`SnapshotTriggered::AlreadyInProgress`] rather than a silent `Ok` when a
    /// build is already running: openraft's own trigger returns `false` and does nothing in
    /// that case (trap T13), and an admin plane that passed that silence on would be telling
    /// an operator a snapshot had been requested when none had.
    pub async fn trigger_snapshot(&self) -> Result<SnapshotTriggered, AdminError> {
        self.inner.trigger_snapshot().await
    }
}

impl NodeInner {
    fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::SeqCst)
    }

    /// Open the per-operation span as a child of the node span, carrying the *caller's*
    /// trace context (read before the node span is entered, or it would be lost).
    fn op_span(&self, op: &'static str) -> Span {
        let trace = TraceContext::current_or_root();
        self.span.in_scope(|| trace.span(op))
    }

    fn metrics(&self) -> NodeMetrics {
        let m = self.raft.metrics().borrow().clone();
        let membership = m.membership_config;
        let authn_rejected_by_reason = self.authn_rejections();
        NodeMetrics {
            node_id: NodeId(m.id),
            role: role_of(m.state),
            current_term: m.current_term,
            current_leader: m.current_leader.map(NodeId),
            last_log_index: m.last_log_index,
            last_applied: m.last_applied.map(view),
            raft_log_len: self.storage.raft_log_len(),
            membership_voter_ids: membership.voter_ids().map(NodeId).collect(),
            membership_log_id: membership.log_id().map(view),
            cluster_revision: self.reader.cluster_revision(),
            applied_commands: self.storage.applied_commands(),
            running_state_ok: m.running_state.is_ok(),
            millis_since_quorum_ack: m.millis_since_quorum_ack,
            authz_denied: self.authz_denied.load(Ordering::Relaxed),
            authz_denied_admin: self.authz_denied_admin.load(Ordering::Relaxed),
            authn_rejected: authn_rejected_by_reason
                .iter()
                .map(|(_, _, count)| count)
                .sum(),
            authn_rejected_by_reason,
        }
    }

    /// Every engine-counted authentication rejection as `(plane, reason, count)`.
    ///
    /// Both planes and every reason, including the ones at zero: the exporter needs a sample
    /// for a reason that has never fired, or a `rate()` over a healthy window returns nothing
    /// at all and a dashboard draws a gap where a flat line belongs.
    fn authn_rejections(&self) -> Vec<(&'static str, AuthnRejectReason, u64)> {
        [
            ("client", &self.authn_rejected_client),
            ("peer", &self.authn_rejected_peer),
        ]
        .into_iter()
        .flat_map(|(plane, counters)| {
            AuthnRejectReason::ALL.into_iter().map(move |reason| {
                (
                    plane,
                    reason,
                    counters[reason.index()].load(Ordering::Relaxed),
                )
            })
        })
        .collect()
    }

    /// The policy this node holds, as the health payload reports it (M3-42).
    ///
    /// `grants` is forced to `0` unless a static allowlist is actually in force: an
    /// `AllowAll`, missing, or invalid policy enforces no grant, and reporting a non-zero
    /// count for one of those would be the exact "looks guarded, is not" reading ADR-0016
    /// exists to prevent.
    fn policy_summary(&self) -> PolicySummary {
        let kind = self.authz_kind();
        PolicySummary {
            // The live version, exactly as `capabilities()` reports it: the summary's whole job
            // is to answer "are these processes enforcing the same policy?" across a fleet,
            // and a `None` on a node that holds a document answers a different one (C6R-04).
            kind: kind.to_capability(self.authorizer.policy_version()),
            grants: match kind {
                crate::AuthzKind::StaticAllowlist => self.cfg.policy_grants,
                _ => 0,
            },
            policy_hash_hex: self.cfg.policy_document_sha256.as_ref().map(hex32),
        }
    }

    /// Committed membership, read from the state machine.
    ///
    /// The state machine's `StoredMembership` is set when a membership entry is *applied*, and
    /// an applied entry is committed by definition. `RaftMetrics::membership_config` is the
    /// **effective** membership, which moves as soon as an entry is appended — before any
    /// quorum has agreed to it. Deriving a client-followable hint from that would mean handing
    /// a client an address a quorum never committed to, which is precisely what ADR-0009
    /// forbids.
    ///
    /// Synchronous on purpose: `health()`, `hint_for()` and the gossip poll all need it, and
    /// OpenRaft's own `with_raft_state` is an async round trip through the core task. Use
    /// [`ConfigNode::raft_committed_membership`] when the Raft core's own committed view is
    /// the thing under test.
    fn committed_membership(&self) -> MembershipView {
        let m = self.reader.membership();
        membership_view(
            m.voter_ids(),
            m.nodes().map(|(id, node)| (*id, node.clone())),
            *m.log_id(),
        )
    }

    /// OpenRaft's *effective* membership: appended, not necessarily committed.
    ///
    /// Only the formation guard uses this (see `form_cluster`); nothing client-facing may.
    fn effective_membership(&self) -> MembershipView {
        let m = self.raft.metrics().borrow().membership_config.clone();
        membership_view(
            m.voter_ids(),
            m.nodes().map(|(id, node)| (*id, node.clone())),
            *m.log_id(),
        )
    }

    // ---------------- admin plane (M5, ADR-0023) ----------------

    /// The replicated retired set, read from applied state.
    ///
    /// Applied state rather than a node-local cache: "stale identities cannot rejoin" has to
    /// hold on every node a retired node might dial, including one that was partitioned while
    /// the removal happened, so the fence travels through the log like any other fact.
    fn retired_nodes(&self) -> BTreeSet<NodeId> {
        let mut out = BTreeSet::new();
        self.reader
            .with_state(&mut |s| out = s.retired_nodes().clone());
        out
    }

    fn is_retired(&self, node_id: NodeId) -> bool {
        let mut retired = false;
        self.reader
            .with_state(&mut |s| retired = s.is_retired(node_id));
        retired
    }

    /// Refuse unless this node is the leader *now*, with the best hint it can prove.
    ///
    /// A pre-check, not the authority: `change_membership` re-checks on the core task and
    /// answers `ForwardToLeader` if leadership moved in between. Doing it here first turns
    /// the common case into one cheap refusal carrying a followable hint, instead of a round
    /// trip through the Raft core.
    fn require_leader(&self) -> Result<(), AdminError> {
        if self.is_stopped() {
            return Err(AdminError::Unavailable {
                reason: "stopped".to_string(),
            });
        }
        let metrics = self.raft.metrics();
        let (state, leader) = {
            let m = metrics.borrow();
            (m.state, m.current_leader)
        };
        let me = self.cfg.identity.node_id;
        if matches!(state, ServerState::Leader) && leader == Some(me.0) {
            return Ok(());
        }
        Err(AdminError::NotLeader {
            hint: leader.map(NodeId).and_then(|id| self.hint_for(id)),
        })
    }

    fn membership_report(&self) -> MembershipReport {
        let m = self.raft.metrics().borrow().clone();
        let me = self.cfg.identity.node_id;
        let authoritative =
            matches!(m.state, ServerState::Leader) && m.current_leader == Some(me.0);
        let effective = m.membership_config;
        let voters: BTreeSet<NodeId> = effective.voter_ids().map(NodeId).collect();
        let mut learners = BTreeSet::new();
        let mut effective_endpoints = BTreeMap::new();
        for (id, node) in effective.nodes() {
            let id = NodeId(*id);
            if !voters.contains(&id) {
                learners.insert(id);
            }
            effective_endpoints.insert(id, (node.peer.clone(), node.client.clone()));
        }
        let leader_last_log_index = if authoritative {
            m.last_log_index.unwrap_or(0)
        } else {
            0
        };
        // `replication` is `Some` only on a leader (research §3.3). Off the leader the map is
        // left empty and `authoritative` is false, so a poller cannot read "no entries" as
        // "nothing is replicating".
        let replication = match (&m.replication, authoritative) {
            (Some(map), true) => map
                .iter()
                .filter(|(id, _)| NodeId(**id) != me)
                .map(|(id, matched)| {
                    let matched_index = matched.map(|l| l.index);
                    (
                        NodeId(*id),
                        ReplicationProgress {
                            matched_index,
                            lag: leader_last_log_index.saturating_sub(matched_index.unwrap_or(0)),
                        },
                    )
                })
                .collect(),
            _ => BTreeMap::new(),
        };
        MembershipReport {
            membership: self.committed_membership(),
            learners,
            joint_config_len: effective.membership().get_joint_config().len(),
            retired: self.retired_nodes(),
            replication,
            leader_last_log_index,
            current_leader: m.current_leader.map(NodeId),
            authoritative,
            promote_max_lag: self.cfg.promote_max_lag,
            effective_endpoints,
            effective_membership_log_id: (*effective.log_id()).map(view),
        }
    }

    /// Turn a membership-change failure into an [`AdminError`].
    ///
    /// `ForwardToLeader` becomes `NotLeader` with the hint the *error* named rather than the
    /// one this node last believed: leadership moved, and the fresher of the two answers is
    /// the one that came back with the refusal.
    fn membership_error(
        &self,
        e: RaftError<RaftNodeId, ClientWriteError<RaftNodeId, RaftNode>>,
    ) -> AdminError {
        match e {
            RaftError::APIError(ClientWriteError::ForwardToLeader(f)) => AdminError::NotLeader {
                hint: f.leader_id.map(NodeId).and_then(|id| self.hint_for(id)),
            },
            RaftError::APIError(ClientWriteError::ChangeMembershipError(e)) => {
                AdminError::Raft(e.to_string())
            }
            RaftError::Fatal(fatal) => AdminError::Fatal(fatal.to_string()),
        }
    }

    /// One `admin_op_local` line per attempt (ADR-0013, m5-interfaces).
    ///
    /// Deliberately **not** named `admin_op`. ADR-0023 requires exactly one `admin_op` audit
    /// record per operation and that record must name the principal, which only the admin
    /// plane knows: an engine-level line called `admin_op` produced a second, principal-less
    /// record for every gRPC call, so a log search for "who removed node 3" returned two hits
    /// and one of them could not answer the question.
    ///
    /// The engine still emits its own line, because an embedder that drives these calls
    /// directly — the in-process test harness, a future embedded control plane — would
    /// otherwise leave no trail at all. `admin_op_local` says exactly what it is: the engine
    /// saw this operation, and it carries no principal by construction.
    fn audit_admin<T>(
        &self,
        op: &'static str,
        target: Option<NodeId>,
        result: &Result<T, AdminError>,
    ) {
        let target_node = target.map_or(0, |n| n.0);
        match result {
            Ok(_) => self.span.in_scope(|| {
                tracing::info!(op, target_node, outcome = "ok", "admin_op_local");
            }),
            Err(e) => self.span.in_scope(|| {
                tracing::warn!(
                    op,
                    target_node,
                    outcome = "rejected",
                    reason = e.reason(),
                    detail = %e,
                    "admin_op_local"
                );
            }),
        }
    }

    async fn add_learner(
        &self,
        node_id: NodeId,
        peer_endpoint: String,
        client_endpoint: String,
    ) -> Result<Option<LogIdView>, AdminError> {
        let result = self
            .add_learner_inner(node_id, peer_endpoint, client_endpoint)
            .await;
        self.audit_admin("add_learner", Some(node_id), &result);
        result
    }

    async fn add_learner_inner(
        &self,
        node_id: NodeId,
        peer_endpoint: String,
        client_endpoint: String,
    ) -> Result<Option<LogIdView>, AdminError> {
        self.require_leader()?;
        if node_id == self.cfg.identity.node_id {
            return Err(AdminError::InvalidArgument {
                detail: "already_a_member: this node is the leader it was asked to add".to_string(),
            });
        }
        if peer_endpoint.trim().is_empty() || client_endpoint.trim().is_empty() {
            return Err(AdminError::InvalidArgument {
                detail: "endpoint_required: a learner needs both a peer and a client endpoint"
                    .to_string(),
            });
        }
        // Before anything is proposed: a retired id must never re-enter membership, not even
        // as a learner (M5-62).
        if self.is_retired(node_id) {
            return Err(AdminError::Retired { node_id });
        }
        if self
            .membership_report()
            .membership
            .voters
            .contains(&node_id)
        {
            return Err(AdminError::InvalidArgument {
                detail: format!("already_a_voter: node {node_id} is already a voter"),
            });
        }
        // `blocking = false`: the blocking form waits for catch-up and then logs and discards
        // the wait's outcome (trap T7), so its `Ok` would be indistinguishable from a wait
        // that timed out.
        let node = RaftNode::new(peer_endpoint, client_endpoint);
        match self.raft.add_learner(node_id.0, node, false).await {
            Ok(resp) => Ok(Some(view(resp.log_id))),
            Err(e) => Err(self.membership_error(e)),
        }
    }

    async fn promote_voter(&self, node_id: NodeId) -> Result<Option<LogIdView>, AdminError> {
        let result = self.promote_voter_inner(node_id).await;
        self.audit_admin("promote_voter", Some(node_id), &result);
        result
    }

    async fn promote_voter_inner(&self, node_id: NodeId) -> Result<Option<LogIdView>, AdminError> {
        self.require_leader()?;
        if self.is_retired(node_id) {
            return Err(AdminError::Retired { node_id });
        }
        let report = self.membership_report();
        if report.membership.voters.contains(&node_id) {
            // Idempotent: the promotion this call asked for has already been committed.
            return Ok(report
                .membership
                .membership_log_id
                .map(|(t, i)| LogIdView::new(t, i)));
        }
        if !report.learners.contains(&node_id) {
            return Err(AdminError::NotAMember { node_id });
        }
        if report.is_joint() {
            return Err(AdminError::Unavailable {
                reason: "joint_config_in_flight: re-issue the interrupted membership change first"
                    .to_string(),
            });
        }
        // A5/OQ-50: the predicate is `matched >= leader_last_log_index - promote_max_lag`,
        // read live from this leader's replication map. A peer the leader has not heard from
        // has no matched index at all, which is the maximum possible lag, not zero.
        let max = self.cfg.promote_max_lag;
        let lag = report
            .lag_of(node_id)
            .unwrap_or(report.leader_last_log_index);
        if lag > max {
            return Err(AdminError::Lagging { node_id, lag, max });
        }
        let change = ChangeMembers::AddVoterIds(BTreeSet::from([node_id.0]));
        // `retain` is irrelevant to an addition, and `true` is the value that never demotes
        // anything by accident.
        match self.raft.change_membership(change, true).await {
            Ok(resp) => Ok(Some(view(resp.log_id))),
            Err(e) => Err(self.membership_error(e)),
        }
    }

    async fn remove_member(&self, node_id: NodeId) -> Result<Option<LogIdView>, AdminError> {
        let result = self.remove_member_inner(node_id).await;
        self.audit_admin("remove_member", Some(node_id), &result);
        result
    }

    async fn remove_member_inner(&self, node_id: NodeId) -> Result<Option<LogIdView>, AdminError> {
        self.require_leader()?;
        if node_id == self.cfg.identity.node_id {
            return Err(AdminError::InvalidArgument {
                detail: "cannot_remove_self: step down first, then remove this node from the \
                         new leader"
                    .to_string(),
            });
        }
        let report = self.membership_report();
        let is_voter = report.membership.voters.contains(&node_id);
        let is_member = is_voter || report.learners.contains(&node_id);
        if !is_member && report.retired.contains(&node_id) {
            // Already removed and already fenced. Re-issuing must be a no-op, because that is
            // how an operator recovers from a crash part-way through the sequence (M5-67).
            return Ok(None);
        }
        // `!is_member && !retired` is deliberately **not** `NotAMember`: it is the state a
        // leader is left in when step 2 committed and step 3 did not — the id is out of
        // membership and its identity is still unfenced, which is the one intermediate state
        // that is actually dangerous. Refusing here would make the recovery `propose_retire`
        // itself tells the operator to run ("re-issue RemoveMember") permanently impossible,
        // so the call falls through to step 3 and finishes the sequence.
        //
        // The cost is that removing an id which was never a member now fences it instead of
        // reporting a typo. That is the right trade: after step 2 commits there is no evidence
        // left that distinguishes the two cases, and fencing an id nobody used is harmless and
        // idempotent, whereas leaving a removed node unfenced is not.

        // Step 1 — demote. `retain = true` is not a preference: `RemoveNodes` refuses an id
        // that is still a voter with `LearnerNotFound`, so the id has to survive step 1 as a
        // learner for step 2 to be able to remove it at all (ruling M5-R5).
        if is_voter {
            let change = ChangeMembers::RemoveVoters(BTreeSet::from([node_id.0]));
            if let Err(e) = self.raft.change_membership(change, true).await {
                return Err(self.membership_error(e));
            }
        }
        // Step 2 — drop the node entirely, so nothing replicates to it any more. Its log id
        // is the one reported, because it is the entry that actually ended the membership.
        // Skipped when the id is already out of membership: that is the resumed-sequence case
        // above, and `RemoveNodes` against an absent id is an error, not a no-op.
        let last = if is_member {
            let change = ChangeMembers::RemoveNodes(BTreeSet::from([node_id.0]));
            match self.raft.change_membership(change, false).await {
                Ok(resp) => Some(view(resp.log_id)),
                Err(e) => return Err(self.membership_error(e)),
            }
        } else {
            report
                .membership
                .membership_log_id
                .map(|(t, i)| LogIdView::new(t, i))
        };
        // Step 3 — fence the identity through the log, so a node that was partitioned during
        // steps 1 and 2 still refuses to talk to it afterwards.
        self.propose_retire(node_id).await?;
        Ok(last)
    }

    /// Replicate `RetireNode` and check that the state machine actually retired the id.
    async fn propose_retire(&self, node_id: NodeId) -> Result<(), AdminError> {
        let cmd = Command::RetireNode { node_id };
        // M6-91: the whole operation is refused rather than half-performed. `RemoveNodes` has
        // already been proposed by the caller, so a gate that let the fence through only
        // sometimes would leave an id out of membership but still able to talk.
        self.schema_gate(&cmd)
            .map_err(|e| AdminError::Unavailable {
                reason: e.to_string(),
            })?;
        match tokio::time::timeout(self.cfg.write_timeout, self.raft.client_write(cmd)).await {
            // The fence may or may not have committed. Reporting it as retryable is correct
            // *because* `RetireNode` is idempotent: re-issuing it changes nothing.
            Err(_) => Err(AdminError::Unavailable {
                reason: "retire_not_confirmed: the RetireNode entry did not commit before the \
                         write deadline; re-issue RemoveMember"
                    .to_string(),
            }),
            Ok(Ok(resp)) => match resp.data {
                CommandResponse::Retired { .. } => Ok(()),
                CommandResponse::Rejected { reason } => {
                    Err(AdminError::InvalidArgument { detail: reason })
                }
                other => Err(AdminError::Fatal(format!(
                    "the state machine answered {other:?} for a RetireNode entry"
                ))),
            },
            Ok(Err(e)) => Err(self.membership_error(e)),
        }
    }

    async fn trigger_snapshot(&self) -> Result<SnapshotTriggered, AdminError> {
        let result = self.trigger_snapshot_inner().await;
        self.audit_admin("trigger_snapshot", None, &result);
        result
    }

    async fn trigger_snapshot_inner(&self) -> Result<SnapshotTriggered, AdminError> {
        if self.is_stopped() {
            return Err(AdminError::Unavailable {
                reason: "stopped".to_string(),
            });
        }
        // Checked before the trigger, not after: openraft's own trigger returns `false` and
        // does nothing while a build is in flight, and swallows that `false` (trap T13).
        // Asking the store first is the only way to tell "requested" from "ignored".
        if self.storage.snapshot_builds_in_flight() > 0 {
            return Ok(SnapshotTriggered::AlreadyInProgress);
        }
        let before = self.reader.snapshot_meta().map(|m| m.snapshot_id);
        self.raft
            .trigger()
            .snapshot()
            .await
            .map_err(|e| AdminError::Fatal(e.to_string()))?;
        // A bounded wait for the build to publish, so the caller gets the snapshot's identity
        // rather than a promise. Timing out is not a failure: the build is still running, and
        // `GetMembership`/`/health` will show it when it lands.
        let deadline = Instant::now() + self.cfg.write_timeout;
        loop {
            if let Some(meta) = self.reader.snapshot_meta() {
                if Some(&meta.snapshot_id) != before.as_ref() {
                    return Ok(SnapshotTriggered::Started {
                        snapshot_id: Some(meta.snapshot_id),
                        last_log_id: meta.last_log_id.map(view),
                    });
                }
            }
            if Instant::now() >= deadline {
                return Ok(SnapshotTriggered::Started {
                    snapshot_id: None,
                    last_log_id: None,
                });
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// The committed **client-plane** endpoint of `node_id`, as a client-followable hint.
    ///
    /// The client endpoint, not the peer endpoint: a hint exists to tell a client where to
    /// retry, and the peer plane is not a place a client can go (ADR-0009). The peer endpoint
    /// of the same node stays available through [`MembershipView::endpoint_of`], which is
    /// what the transport dials.
    fn hint_for(&self, node_id: NodeId) -> Option<LeaderHint> {
        self.committed_membership()
            .client_endpoint_of(node_id)
            .map(|endpoint| LeaderHint {
                node_id,
                endpoint: endpoint.to_string(),
            })
    }

    fn leader_hint(&self) -> Option<LeaderHint> {
        let leader = self.raft.metrics().borrow().current_leader?;
        self.hint_for(NodeId(leader))
    }

    /// The authorization model in force **right now**.
    ///
    /// [`crate::NodeConfig::authz_kind`] records what the node was *wired* with, which for signed
    /// mode is only a startup snapshot: a node that booted with no valid document adopts one the
    /// moment the loader finds it, and must then become ready without a restart (M6-27). Signed
    /// mode therefore reads presence from the authorizer, the only thing that knows whether a
    /// document is in force; every other model is fixed by the configuration (C6R-01).
    fn authz_kind(&self) -> AuthzKind {
        if !self.cfg.authz_kind.is_signed_mode() {
            return self.cfg.authz_kind;
        }
        if self.authorizer.policy_version().is_some() {
            AuthzKind::SignedPolicy
        } else {
            AuthzKind::NoValidPolicy
        }
    }

    /// Whether this node will serve client traffic at all: membership known, storage not
    /// poisoned, and an authorization model actually in force (OQ-19).
    fn is_ready(&self) -> bool {
        !self.is_stopped()
            && self.authz_kind().is_present()
            && !self.storage.is_poisoned()
            && self.committed_membership().is_formed()
    }

    fn health(&self) -> Health {
        if self.is_stopped() {
            return Health::Stopped;
        }
        let rx = self.raft.metrics();
        let (running, leader) = {
            let m = rx.borrow();
            (m.running_state.clone(), m.current_leader)
        };
        if let Err(fatal) = running {
            return Health::Unavailable {
                reason: format!("raft core stopped: {fatal}"),
            };
        }
        let authz_kind = self.authz_kind();
        if !authz_kind.is_present() {
            return Health::Unavailable {
                reason: format!("authorization policy is {authz_kind}"),
            };
        }
        if self.storage.is_poisoned() {
            return Health::Unavailable {
                reason: "storage is poisoned".to_string(),
            };
        }
        if !self.committed_membership().is_formed() {
            return Health::Unavailable {
                reason: "cluster is not formed".to_string(),
            };
        }
        match leader {
            Some(id) if id == self.cfg.identity.node_id.0 => Health::Ready,
            Some(id) => Health::NotLeader {
                hint: self.hint_for(NodeId(id)),
            },
            None => Health::Unavailable {
                reason: "no leader known".to_string(),
            },
        }
    }

    /// Poll the metrics watch until `predicate` yields a value, bounded by `deadline`.
    ///
    /// Waits on the watch channel *and* a short interval, so a predicate over storage state
    /// (which does not push to the watch) still makes progress. No fixed sleeps.
    async fn wait_until<T>(
        &self,
        deadline: Duration,
        what: &str,
        predicate: impl Fn(&NodeMetrics) -> Option<T>,
    ) -> Result<T, Timeout> {
        let started = Instant::now();
        let mut rx = self.raft.metrics();
        let waited = async {
            loop {
                if let Some(v) = predicate(&self.metrics()) {
                    return Some(v);
                }
                // Sender dropped means the core is gone: check once more, then give up.
                if let Ok(Err(_)) = tokio::time::timeout(POLL_INTERVAL, rx.changed()).await {
                    return predicate(&self.metrics());
                }
            }
        };
        match tokio::time::timeout(deadline, waited).await {
            Ok(Some(v)) => Ok(v),
            Ok(None) | Err(_) => Err(Timeout {
                what: what.to_string(),
                waited: started.elapsed(),
                last: Box::new(self.metrics()),
            }),
        }
    }

    /// The one authorization seam (ADR-0012, spec §15.2).
    ///
    /// `put`/`delete` ask for [`Action::Write`] on the key; `get` asks for [`Action::Read`] on
    /// the key; `list` asks for [`Action::Read`] on the **prefix**, so a scan is checked
    /// against the range it would actually return rather than against one member of it
    /// (OQ-22). Every decision — allow or deny — produces exactly one
    /// [`config_core::audit`] line, so the audit trail has no gaps and no duplicates.
    ///
    /// A node whose policy is [`crate::AuthzKind::Missing`] or [`crate::AuthzKind::Invalid`]
    /// denies here, before Raft is touched: failing closed is the only safe reading of "the
    /// operator meant to restrict this and we could not load the restriction" (OQ-19).
    fn authorize(
        &self,
        principal: &Principal,
        action: Action,
        key_or_prefix: &[u8],
    ) -> Result<(), ConfigError> {
        let authz_kind = self.authz_kind();
        let decision = if authz_kind.is_present() {
            self.authorizer.authorize(principal, action, key_or_prefix)
        } else {
            Decision::deny(format!(
                "node is not ready to authorize: policy is {authz_kind}"
            ))
        };
        config_core::audit(
            principal,
            action,
            key_or_prefix,
            &decision,
            authz_kind.to_capability(self.authorizer.policy_version()),
        );
        match decision {
            Decision::Allow => Ok(()),
            Decision::Deny { reason } => {
                self.authz_denied.fetch_add(1, Ordering::Relaxed);
                // A signed-mode node holding no valid document is *declining traffic*, not
                // deciding that this principal may not do this — it has no document to decide
                // from. `PermissionDenied` would tell the client its identity is wrong and to
                // stop retrying, when the correct reading is "ask another node, or wait for the
                // document" (ADR-0027, M6-25). The static models keep M3's `PermissionDenied`:
                // there the operator configured something and got it wrong, which is a decision.
                // The counter increments either way; the refusal happened at this seam.
                if authz_kind == AuthzKind::NoValidPolicy {
                    return Err(ConfigError::Unavailable {
                        reason: format!("no valid policy in force ({reason})"),
                    });
                }
                Err(ConfigError::PermissionDenied {
                    detail: format!(
                        "principal {:?} may not {:?} key_hex={} ({reason})",
                        principal.name,
                        action,
                        key_hex(key_or_prefix)
                    ),
                })
            }
        }
    }

    /// Validate → authorize → replicate. A request that fails either of the first two steps
    /// never enters the log (M1-18, M1-27).
    async fn mutate(
        &self,
        principal: &Principal,
        op: &'static str,
        key: bytes::Bytes,
        build: impl FnOnce() -> Result<Command, ConfigError>,
    ) -> Result<MutationResponse, ConfigError> {
        let started = Instant::now();
        let result = self.mutate_inner(principal, key.as_ref(), build).await;
        if let Some(hist) = self.proposal_latency.for_op(op) {
            hist.observe(started.elapsed());
        }
        self.log_outcome(
            OutcomeLine::write(op),
            principal,
            key.as_ref(),
            started,
            &result,
            |r| r.revision,
        );
        result
    }

    async fn mutate_inner(
        &self,
        principal: &Principal,
        key: &[u8],
        build: impl FnOnce() -> Result<Command, ConfigError>,
    ) -> Result<MutationResponse, ConfigError> {
        let cmd = build()?;
        self.authorize(principal, Action::Write, key)?;
        // ADR-0025 "Principal binding": the leader overwrites the stamp's principal from the
        // session it authenticated, unconditionally and before the entry is proposed. Whatever
        // the caller put there is discarded, so one principal can never address another's
        // `client_id` namespace, and every voter applies the same bound stamp deterministically
        // from the committed entry (M5-101).
        let cmd = match cmd.dedup() {
            Some(_) => cmd.bind_principal(config_core::principal_hash(&principal.name)),
            None => cmd,
        };
        if self.is_stopped() {
            return Err(ConfigError::Unavailable {
                reason: "stopped".to_string(),
            });
        }
        // Before replication, so the entry is already labelled when the apply path (which runs
        // on OpenRaft's own state-machine task, outside this span) reaches it, and when the
        // replication task builds the `AppendEntries` that carries it to the followers.
        // OQ-64: a dedup-bearing mutation is refused, never applied without its record. A
        // client that was told "applied" would believe it holds a retained request identity it
        // does not hold, and would resubmit on the strength of it (ADR-0015, §16).
        self.schema_gate(&cmd)?;
        if let Some(trace) = TraceContext::current() {
            self.traces.record(&cmd, &trace.trace_id);
        }

        match tokio::time::timeout(self.cfg.write_timeout, self.raft.client_write(cmd)).await {
            // The deadline elapsed *after* submission, so the mutation may still commit.
            // ADR-0015: never replay it; the caller reads and CASes instead.
            Err(_) => Err(ConfigError::DeadlineExceededUnknownOutcome),
            Ok(Ok(resp)) => match resp.data {
                CommandResponse::Mutation { response, .. } => Ok(response),
                CommandResponse::Rejected { reason } => {
                    Err(ConfigError::InvalidArgument { detail: reason })
                }
                CommandResponse::Noop => Err(noop_for_command_entry()),
                // `Compact` is proposed by `propose_compact`, which reads the response
                // itself; anything that reached *here* asked for a mutation and got a
                // compaction acknowledgement, which is a state machine bug, not a rejection.
                CommandResponse::Compacted { .. } | CommandResponse::Retired { .. } => {
                    Err(ConfigError::FatalStorage {
                        detail: "a mutation command was answered with a maintenance \
                                 acknowledgement; the entry is committed and applied, so this \
                                 outcome must not be resubmitted"
                            .to_string(),
                    })
                }
            },
            Ok(Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f)))) => {
                Err(self.not_leader(f.leader_id.map(NodeId)))
            }
            Ok(Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(e)))) => {
                Err(ConfigError::Unavailable {
                    reason: e.to_string(),
                })
            }
            Ok(Err(RaftError::Fatal(fatal))) => Err(write_fatal_to_config_error(fatal)),
        }
    }

    /// Validate → authorize → `ensure_linearizable` → read applied state.
    ///
    /// The linearizable barrier is what makes an isolated former leader refuse: it has to
    /// hear from a quorum before it may answer, so it returns `Unavailable` rather than
    /// serving stale local state (M1-11, M1-13, M1-25).
    async fn read_validated<V, T>(
        &self,
        principal: &Principal,
        op: &'static str,
        key_or_prefix: bytes::Bytes,
        validate: impl FnOnce() -> Result<V, ConfigError>,
        project: impl FnOnce(&KvState, &V) -> T,
        revision: impl FnOnce(&T) -> u64,
    ) -> Result<T, ConfigError> {
        let started = Instant::now();
        let result = self
            .read_inner(principal, key_or_prefix.as_ref(), validate, project)
            .await;
        self.read_latency.observe(started.elapsed());
        self.log_outcome(
            OutcomeLine::read(op),
            principal,
            key_or_prefix.as_ref(),
            started,
            &result,
            revision,
        );
        result
    }

    async fn read_inner<V, T>(
        &self,
        principal: &Principal,
        key_or_prefix: &[u8],
        validate: impl FnOnce() -> Result<V, ConfigError>,
        project: impl FnOnce(&KvState, &V) -> T,
    ) -> Result<T, ConfigError> {
        let validated = validate()?;
        self.authorize(principal, Action::Read, key_or_prefix)?;
        if self.is_stopped() {
            return Err(ConfigError::Unavailable {
                reason: "stopped".to_string(),
            });
        }

        match tokio::time::timeout(self.cfg.read_timeout, self.raft.ensure_linearizable()).await {
            // A read has no side effects, so an expired deadline is plainly retryable.
            Err(_) => Err(ConfigError::Unavailable {
                reason: "read deadline exceeded before the linearizable barrier".to_string(),
            }),
            Ok(Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(f)))) => {
                Err(self.not_leader(f.leader_id.map(NodeId)))
            }
            Ok(Err(RaftError::APIError(CheckIsLeaderError::QuorumNotEnough(e)))) => {
                Err(ConfigError::Unavailable {
                    reason: format!("quorum not reached for a linearizable read: {e}"),
                })
            }
            Ok(Err(RaftError::Fatal(fatal))) => Err(read_fatal_to_config_error(fatal)),
            Ok(Ok(_read_log_id)) => {
                let mut project = Some(project);
                let mut out = None;
                self.reader.with_state(&mut |s| {
                    if let Some(f) = project.take() {
                        out = Some(f(s, &validated));
                    }
                });
                // `StateReader::with_state` is contracted to call its closure exactly once.
                // A store that broke that contract would be returning no state at all, so the
                // honest answer is a retryable `Unavailable` naming the broken contract —
                // panicking inside a client call would take the whole node's runtime with it.
                out.ok_or_else(|| ConfigError::Unavailable {
                    reason: "state reader did not yield applied state".to_string(),
                })
            }
        }
    }

    /// `NotLeader` when the leader is known and has a committed endpoint, `Unavailable` when
    /// it is not known at all — never a hint a client cannot trust (ADR-0009).
    fn not_leader(&self, leader: Option<NodeId>) -> ConfigError {
        match leader {
            Some(id) => ConfigError::NotLeader {
                hint: self.hint_for(id),
            },
            None => ConfigError::Unavailable {
                reason: "leader unknown".to_string(),
            },
        }
    }

    /// The one client-facing outcome line per request (ADR-0013 `info` level).
    ///
    /// `outcome` distinguishes success from failure so a query never has to know two message
    /// spellings for one operation. `role` is recorded here rather than on the node span
    /// because a node's role changes while the span lives, and a query that asks "what did the
    /// *leader* log" needs the role at the time of the line.
    fn log_outcome<T>(
        &self,
        line: OutcomeLine,
        principal: &Principal,
        key: &[u8],
        started: Instant,
        result: &Result<T, ConfigError>,
        revision: impl FnOnce(&T) -> u64,
    ) {
        let OutcomeLine { msg, op } = line;
        let latency_ms = started.elapsed().as_millis() as u64;
        let role = role_of(self.raft.metrics().borrow().state);
        match result {
            Ok(v) => tracing::info!(
                op,
                role = role.as_str(),
                principal = %principal.name,
                key_hex = %key_hex(key),
                outcome = "ok",
                revision = revision(v),
                latency_ms,
                "{msg}"
            ),
            Err(e) => tracing::info!(
                op,
                role = role.as_str(),
                principal = %principal.name,
                key_hex = %key_hex(key),
                outcome = "error",
                error = %e,
                error_kind = ?e.kind(),
                revision = 0u64,
                latency_ms,
                "{msg}"
            ),
        }
    }

    /// Steps 1-3 of §11.2; the hub owns 4-7.
    async fn watch_inner(
        &self,
        principal: &Principal,
        req: WatchRequest,
    ) -> Result<WatchStream, ConfigError> {
        let progress_interval =
            crate::watch::validate_watch(&req, &self.cfg.limits, self.cfg.watch_progress_interval)?;
        self.authorize(principal, Action::Read, req.prefix.as_ref())?;
        if self.is_stopped() {
            return Err(ConfigError::Unavailable {
                reason: "stopped".to_string(),
            });
        }
        match tokio::time::timeout(self.cfg.read_timeout, self.raft.ensure_linearizable()).await {
            Err(_) => {
                return Err(ConfigError::Unavailable {
                    reason: "read deadline exceeded before the linearizable barrier".to_string(),
                })
            }
            Ok(Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(f)))) => {
                return Err(self.not_leader(f.leader_id.map(NodeId)))
            }
            Ok(Err(RaftError::APIError(CheckIsLeaderError::QuorumNotEnough(e)))) => {
                return Err(ConfigError::Unavailable {
                    reason: format!("quorum not reached for a linearizable read: {e}"),
                })
            }
            Ok(Err(RaftError::Fatal(fatal))) => return Err(read_fatal_to_config_error(fatal)),
            // A quorum just confirmed this node is the leader, which is a stronger statement
            // than the metrics watcher's eventual view - publish it so the stream this call
            // is about to open does not terminate on a stale `NotLeader`.
            Ok(Ok(_read_log_id)) => self.watch.note_leader(),
        }
        self.watch
            .open(
                principal,
                req.prefix,
                req.start_after_revision,
                progress_interval,
            )
            .await
    }

    /// Tell the hub what the metrics watcher just saw.
    fn publish_leader_state(&self, leader: Option<NodeId>) {
        if self.is_stopped() {
            self.watch.shutdown();
            return;
        }
        match leader {
            Some(id) if id == self.cfg.identity.node_id => self.watch.note_leader(),
            Some(id) => self.watch.note_not_leader(self.hint_for(id)),
            None => self.watch.note_not_leader(None),
        }
    }

    /// Propose `Compact` through `client_write`, exactly like any other command.
    async fn propose_compact(
        &self,
        principal: &Principal,
        up_to_revision: u64,
    ) -> Result<u64, ConfigError> {
        // Compaction deletes retained history, so it is a write in the authorization model
        // even though it allocates no revision: the empty prefix is the whole keyspace.
        self.authorize(principal, Action::Write, b"")?;
        if self.is_stopped() {
            return Err(ConfigError::Unavailable {
                reason: "stopped".to_string(),
            });
        }
        // An operator-triggered compaction never trims the deduplication table (C5B-17).
        // The retention timer is ADR-0025's only trim mechanism because its watermark comes
        // from `retention_target`, which honours `min_revisions` and `max_age` and so gives
        // the runbook's sizing advice an age floor to stand on. An operator-supplied
        // `up_to_revision` has no such floor: `compact(current_revision)` would release every
        // record at once and silently void ADR-0015's bounded-retry exception for every client
        // with a mutation in flight. Nothing is lost by keeping the records — a duplicate is
        // answered from the stored response, never from the history this command deletes.
        let cmd = Command::Compact {
            up_to_revision,
            dedup_trim_below: None,
        };
        self.schema_gate(&cmd)?;
        match tokio::time::timeout(self.cfg.write_timeout, self.raft.client_write(cmd)).await {
            Err(_) => Err(ConfigError::DeadlineExceededUnknownOutcome),
            Ok(Ok(resp)) => match resp.data {
                CommandResponse::Compacted { compact_revision } => Ok(compact_revision),
                CommandResponse::Rejected { reason } => {
                    Err(ConfigError::InvalidArgument { detail: reason })
                }
                CommandResponse::Mutation { .. }
                | CommandResponse::Noop
                | CommandResponse::Retired { .. } => Err(noop_for_command_entry()),
            },
            Ok(Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f)))) => {
                Err(self.not_leader(f.leader_id.map(NodeId)))
            }
            Ok(Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(e)))) => {
                Err(ConfigError::Unavailable {
                    reason: e.to_string(),
                })
            }
            Ok(Err(RaftError::Fatal(fatal))) => Err(write_fatal_to_config_error(fatal)),
        }
    }

    /// One pass of the leader-only retention policy (ADR-0019, test plan TA-33).
    ///
    /// Everything here is leader-local: the receipt samples, the clock, and the decision.
    /// `apply` sees only the resulting replicated `Compact` command, which is why two voters
    /// configured with different retention still hold identical state.
    async fn evaluate_retention(&self) {
        if self.is_stopped() {
            return;
        }
        let leader = self.raft.metrics().borrow().current_leader.map(NodeId);
        if leader != Some(self.cfg.identity.node_id) {
            // A node that is not the leader keeps no receipts: a new leader must start with
            // an empty age map rather than inherit one it never observed (M4-29).
            self.receipts
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clear();
            return;
        }

        let applied = self.reader.cluster_revision();
        let now_ms = self.clock.now_ms();
        let oldest_within_max_age = {
            let mut receipts = self.receipts.lock().unwrap_or_else(|e| e.into_inner());
            if receipts.last().is_none_or(|(rev, _)| *rev < applied) {
                receipts.push((applied, now_ms));
            }
            if receipts.len() > MAX_RETENTION_SAMPLES {
                let excess = receipts.len() - MAX_RETENTION_SAMPLES;
                receipts.drain(..excess);
            }
            let cutoff = now_ms.saturating_sub(self.retention.max_age.as_millis() as u64);
            // Every revision at or below the newest sample taken before `cutoff` is older
            // than `max_age`; the next one up is the oldest still within policy.
            receipts
                .iter()
                .rev()
                .find(|(_, ms)| *ms <= cutoff)
                .map_or(0, |(rev, _)| rev.saturating_add(1))
        };

        let Ok(stats) = self.reader.journal_stats() else {
            return;
        };
        let view = JournalView {
            oldest_revision: stats.oldest_revision.unwrap_or(0),
            newest_revision: stats.newest_revision.unwrap_or(0),
            count: stats.count,
            bytes: stats.bytes,
        };
        let compact_revision = self.reader.compact_revision().unwrap_or(0);
        let Some((up_to, reason)) = retention_target(
            view,
            &self.retention,
            compact_revision,
            oldest_within_max_age,
        ) else {
            return;
        };
        // C5B-02: the same timer that trims history trims the deduplication table. ADR-0025
        // gives the global cap no other way to shrink — `dedup_trim_below` is only ever
        // carried by `Compact` — and until this line nothing ever proposed one, so
        // `dedup.max_records` was a limit that could be reached and never released.
        //
        // The watermark is `up_to + 1`, which is "every record whose revision's history this
        // same command is about to delete". Tying the two together rather than inventing a
        // second rule is deliberate:
        //
        // * it is leader-local and timer-driven, so `apply` gains no wall-clock dependency
        //   and the trim stays deterministic (ADR-0025's requirement, ADR-0019's property);
        // * it is never more aggressive than history compaction, so a record can only be
        //   dropped once the revision it answers with is itself unreadable — a trim that ran
        //   ahead of history would let a resubmission inside the client's own retry window
        //   (ADR-0015) apply a second time, which is the failure deduplication exists to
        //   prevent;
        // * `oldest_within_max_age` was the tempting alternative and is wrong: under
        //   `max_age = 0` it evaluates to the newest applied revision, which would trim
        //   records the instant they were written.
        //
        // `None` when deduplication is off or nothing is being compacted: a cluster that
        // retains no records must not start carrying a watermark in every `Compact` it
        // proposes.
        let dedup_trim_below =
            (self.cfg.limits.dedup.enabled && up_to > 0).then(|| up_to.saturating_add(1));
        tracing::info!(
            up_to,
            reason = reason.as_str(),
            dedup_trim_below,
            "compaction_proposed"
        );
        let cmd = Command::Compact {
            up_to_revision: up_to,
            dedup_trim_below,
        };
        // M6-90: retention is simply not enforced while the gate is shut. The journal then
        // grows within the budget its own admission control already polices and alerts on,
        // which is the honest failure — a silently-skipped compaction would be an unbounded
        // resource with nothing to show for it.
        if self.schema_gate(&cmd).is_err() {
            return;
        }
        // F-006: the timeout arm is logged, not discarded. To the retention timer a proposal
        // that never returned and one that returned an error are the same event — history was
        // not trimmed — and the timed-out case is the one an operator most needs to see,
        // because it is what a wedged leader looks like from here. Both arms keep the one
        // message name so an alert rule matches either (ADR-0026).
        match tokio::time::timeout(self.cfg.write_timeout, self.raft.client_write(cmd)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => tracing::warn!(up_to, error = %e, "compaction proposal failed"),
            Err(_) => tracing::warn!(
                up_to,
                error = "timeout",
                timeout_ms = self.cfg.write_timeout.as_millis() as u64,
                "compaction proposal failed"
            ),
        }
    }

    /// The lowest schema any committed voter is known to have (M6-R4, OQ-63, M6-88).
    fn cluster_min_schema(&self) -> Option<SchemaTriple> {
        if self.metrics().role != NodeRole::Leader {
            return None;
        }
        let voters = self.committed_membership().voters;
        // `min` over observed triples, so the answer is always a schema some voter actually
        // has. A field-wise blend could describe a build that does not exist and claim support
        // no voter has.
        voters.iter().map(|v| self.peer_schemas.get(*v)).min()
    }

    /// The highest `command_schema` this node's own applied state has ever carried.
    ///
    /// Durable and monotonic (ADR-0030 ruling M6-R15): a state machine that applied a
    /// schema-2 entry has proved it decodes that generation, and nothing later can make that
    /// untrue. Read from the state machine rather than cached, because a snapshot install can
    /// raise it without this process applying anything.
    fn max_applied_command_schema(&self) -> u16 {
        let mut out: u16 = config_core::COMMAND_SCHEMA_V1;
        self.reader
            .with_state(&mut |s| out = s.max_applied_command_schema());
        out
    }

    /// Whether a committed voter has *answered* naming a schema below `command_schema`.
    ///
    /// Positive evidence only: a voter the leader has never heard from is absent from
    /// `peer_schemas` and reports nothing, which is why this asks
    /// [`PeerSchemas::observed`](crate::transport::PeerSchemas::observed) rather than `get` —
    /// `get` would read an unreachable voter as schema 1 and turn every silent voter into a
    /// blocker, which is the write outage ruling M6-R15 was written to end.
    fn a_voter_reports_below(&self, command_schema: u16) -> bool {
        self.committed_membership()
            .voters
            .iter()
            .filter_map(|v| self.peer_schemas.observed(*v))
            .any(|s| s.command_schema < command_schema)
    }

    /// Refuse a command no committed voter set can carry yet (ADR-0030 A7).
    fn schema_gate(&self, cmd: &Command) -> Result<(), ConfigError> {
        let Some(gate) = command_gate(cmd) else {
            return Ok(());
        };
        // Ruling M6-R15, narrowed by finding F-014. Activation is a property of the
        // *replicated state*, not of who is answering right now: once an entry of this
        // generation is in the applied state, every voter that has it has already decoded it.
        // That is what stops one voter going down after a failover from turning into a write
        // outage, and it is why the durable watermark is consulted before the live minimum.
        //
        // But the watermark is a fact about the *past* voter set. M6-R15 justified ignoring
        // the present one by saying a voter that missed the commit "is fenced by its own
        // decode refusal when it returns" — true only now that F-015 wired that fence, and
        // true only of a voter that *missed* the entry. A voter added or restarted at
        // `--compat-schema 1` after activation has not missed anything; it has told the leader
        // it cannot decode this generation. Proposing anyway would over-report, which is the
        // one direction ADR-0030's safety property forbids, and would turn E2E-42's rehearsal
        // into that node's outage rather than a refused proposal.
        //
        // So the watermark clause holds only while no voter contradicts it. An unreachable
        // voter contradicts nothing (see `a_voter_reports_below`) and steady state survives.
        if self.max_applied_command_schema() >= gate.command_schema
            && !self.a_voter_reports_below(gate.command_schema)
        {
            return Ok(());
        }
        // Off the leader there is no minimum to judge against, and the proposal is about to be
        // refused by Raft with a leader hint — a strictly more useful answer than a gate
        // verdict computed from the one peer a follower ever hears from.
        let Some(min) = self.cluster_min_schema() else {
            return Ok(());
        };
        if min.command_schema >= gate.command_schema {
            return Ok(());
        }
        self.note_feature_gated(gate.feature, min);
        Err(ConfigError::Unavailable {
            reason: UNAVAILABLE_FEATURE_NOT_ACTIVATED.to_string(),
        })
    }

    /// Log a refusal once per window per feature (M6-90, M6-123).
    ///
    /// A "window" is a period over which the computed minimum does not move: a second line at
    /// the same minimum tells an operator nothing the first did not, and the gate sits on the
    /// retention timer's path, which fires forever. Keyed on the value rather than on a clock
    /// so the rate limit is deterministic and needs no timer of its own.
    fn note_feature_gated(&self, feature: &'static str, min: SchemaTriple) {
        let mut logged = self
            .gate_logged_at
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if logged.insert(feature, min.command_schema) == Some(min.command_schema) {
            return;
        }
        drop(logged);
        tracing::warn!(
            feature,
            cluster_min_schema = min.command_schema,
            schema = self.cfg.schema.command_schema,
            "feature_gated"
        );
    }

    /// Latch and announce activation exactly once per process (M6-96, M6-100, M6-123).
    ///
    /// Sampled by the background ticker rather than by the gate: activation is a fact about the
    /// voter set, and an operator must see it when the last old voter is replaced, not only
    /// when something later happens to want a gated feature.
    fn sample_schema_activation(&self) {
        let Some(min) = self.cluster_min_schema() else {
            return;
        };
        // Either route into the announcement, matching the gate exactly: the durable watermark
        // is the same proof of activation there and here (M6-R15), and it carries the same
        // qualification (F-014). Mirroring the predicate rather than restating half of it is
        // the point — a `feature_activated` line an operator reads while the gate is in fact
        // refusing the feature is worse than no line at all.
        if min.command_schema < CURRENT_SCHEMA.command_schema
            && (self.max_applied_command_schema() < CURRENT_SCHEMA.command_schema
                || self.a_voter_reports_below(CURRENT_SCHEMA.command_schema))
        {
            return;
        }
        if self
            .schema_activated
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return;
        }
        tracing::info!(
            schema = self.cfg.schema.command_schema,
            cluster_min_schema = min.command_schema,
            voters = self.committed_membership().voters.len(),
            "feature_activated"
        );
    }

    /// Sample the leader this node believes in, counting a transition (ADR-0026).
    ///
    /// A *transition*, not a term change: an election that returns the same leader is not a
    /// leadership change an operator cares about, and counting one would make the alert fire
    /// on every heartbeat gap.
    fn sample_leader(&self) {
        let current = self.metrics().current_leader;
        let mut last = self
            .last_seen_leader
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if *last != current {
            if current.is_some() {
                self.leader_changes.fetch_add(1, Ordering::Relaxed);
            }
            *last = current;
        }
    }

    /// One pass over the advisory gossip source (ADR-0003).
    fn poll_gossip(&self) {
        let membership = self.committed_membership();
        let identity = self.cfg.identity;
        let mut accepted = BTreeMap::new();
        for hint in self.gossip.peers() {
            match validate_hint(&hint, &membership, &identity) {
                HintVerdict::Accepted => {
                    tracing::debug!(
                        peer_node_id = hint.node_id.0,
                        peer_endpoint = %hint.peer_endpoint,
                        liveness = ?hint.liveness,
                        "gossip_hint_accepted"
                    );
                    accepted.insert(hint.node_id, hint);
                }
                HintVerdict::Rejected { reason } => {
                    self.gossip_endpoint_mismatch
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        reason,
                        peer_node_id = hint.node_id.0,
                        peer_cluster_id = %hint.cluster_id,
                        peer_endpoint = %hint.peer_endpoint,
                        "gossip_hint_rejected"
                    );
                }
            }
        }
        *self
            .accepted_hints
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = accepted;
    }
}

fn view(id: openraft::LogId<RaftNodeId>) -> LogIdView {
    LogIdView::new(id.leader_id.term, id.index)
}

/// Flatten one OpenRaft membership into the engine's OpenRaft-free view.
///
/// Takes the three parts rather than a membership type because the committed membership is a
/// `StoredMembership` on one side of the boundary and an `EffectiveMembership` on the other,
/// and the two do not share a trait.
fn membership_view(
    voters: impl Iterator<Item = RaftNodeId>,
    nodes: impl Iterator<Item = (RaftNodeId, RaftNode)>,
    log_id: Option<openraft::LogId<RaftNodeId>>,
) -> MembershipView {
    let mut endpoints = BTreeMap::new();
    let mut client_endpoints = BTreeMap::new();
    for (id, node) in nodes {
        endpoints.insert(NodeId(id), node.peer);
        client_endpoints.insert(NodeId(id), node.client);
    }
    MembershipView {
        voters: voters.map(NodeId).collect(),
        endpoints,
        client_endpoints,
        membership_log_id: log_id.map(|l| (l.leader_id.term, l.index)),
    }
}

/// Which client-facing outcome line to emit, and for which operation.
///
/// `msg` is what the §5 log queries join on (`client_write` for a mutation, `client_read` for
/// a read); `op` names the specific operation within it (`put`, `delete`, `get`, `list`).
/// Bundled because the two are never chosen independently.
#[derive(Debug, Clone, Copy)]
struct OutcomeLine {
    msg: &'static str,
    op: &'static str,
}

impl OutcomeLine {
    fn write(op: &'static str) -> Self {
        Self {
            msg: "client_write",
            op,
        }
    }

    fn read(op: &'static str) -> Self {
        Self {
            msg: "client_read",
            op,
        }
    }
}

fn role_of(state: ServerState) -> NodeRole {
    match state {
        ServerState::Learner => NodeRole::Learner,
        ServerState::Follower => NodeRole::Follower,
        ServerState::Candidate => NodeRole::Candidate,
        ServerState::Leader => NodeRole::Leader,
        ServerState::Shutdown => NodeRole::Shutdown,
    }
}

/// `Fatal` on the **read** path: nothing was submitted, so the request is plainly retryable.
///
/// `ensure_linearizable` has no side effect on the log, so a Raft core that stopped or panicked
/// while the barrier was outstanding leaves nothing behind — `Unavailable` (resubmittable) is
/// the honest answer. See [`write_fatal_to_config_error`] for why the write path cannot say
/// the same thing.
fn read_fatal_to_config_error(fatal: Fatal<RaftNodeId>) -> ConfigError {
    match fatal {
        Fatal::StorageError(e) => ConfigError::FatalStorage {
            detail: e.to_string(),
        },
        other => ConfigError::Unavailable {
            reason: other.to_string(),
        },
    }
}

/// `Fatal` on the **write** path: the proposal may already have committed, so the outcome is
/// unknown (ADR-0015).
///
/// openraft delivers `Fatal::Stopped`/`Fatal::Panicked` through the `client_write` reply
/// channel, which the Raft core only drops *after* the proposal has been enqueued. The entry
/// may therefore be committed and applied while the caller sees the error. Reporting it as
/// `Unavailable` — whose contract is "rejected before entering the log",
/// `is_safe_to_resubmit() == true` — would invite a replay of a mutation that already took
/// effect. ADR-0015 permits "resubmittable" only when the mutation provably did not commit,
/// so every non-storage `Fatal` here becomes [`ConfigError::DeadlineExceededUnknownOutcome`]
/// and the caller runs the read-back-then-CAS recovery recipe.
///
/// `Fatal::StorageError` keeps its `FatalStorage` mapping: it is an internal-class failure of
/// this node, not a retry decision for the client.
fn write_fatal_to_config_error(fatal: Fatal<RaftNodeId>) -> ConfigError {
    match fatal {
        Fatal::StorageError(e) => ConfigError::FatalStorage {
            detail: e.to_string(),
        },
        Fatal::Stopped | Fatal::Panicked => ConfigError::DeadlineExceededUnknownOutcome,
    }
}

/// The leader-only retention task (ADR-0019).
///
/// Holds a `Weak`, so it never keeps the node alive, and is aborted outright by
/// [`ConfigNode::stop`].
async fn retention_loop(inner: Weak<NodeInner>) {
    let interval = match inner.upgrade() {
        Some(node) => node.retention.check_interval,
        None => return,
    };
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // The first tick of a tokio interval completes immediately, and a compaction proposal
    // before the node has applied anything is noise.
    ticker.tick().await;
    loop {
        ticker.tick().await;
        let Some(node) = inner.upgrade() else { return };
        node.evaluate_retention().await;
    }
}

/// Holds a `Weak`, so it never keeps the node alive; it exits when the last [`ConfigNode`]
/// is dropped, and is aborted outright by [`ConfigNode::stop`].
async fn background_loop(
    inner: Weak<NodeInner>,
    mut metrics: tokio::sync::watch::Receiver<openraft::RaftMetrics<RaftNodeId, RaftNode>>,
    gossip_poll: Duration,
) {
    let mut ticker = tokio::time::interval(gossip_poll);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last = {
        let m = metrics.borrow_and_update();
        (m.state, m.current_leader, m.current_term)
    };
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let Some(node) = inner.upgrade() else { return };
                node.poll_gossip();
                node.sample_leader();
                node.sample_schema_activation();
            }
            changed = metrics.changed() => {
                if changed.is_err() {
                    return;
                }
                if inner.upgrade().is_none() {
                    return;
                }
                let now = {
                    let m = metrics.borrow_and_update();
                    (m.state, m.current_leader, m.current_term)
                };
                if now.0 != last.0 {
                    tracing::info!(old = %role_of(last.0), new = %role_of(now.0), "raft role changed");
                }
                if now.1 != last.1 {
                    tracing::info!(old = ?last.1, new = ?now.1, "raft leader changed");
                }
                // Watches are leader-served (§11.1), so every leadership observation is a
                // termination decision for the streams this node holds.
                if let Some(node) = inner.upgrade() {
                    node.publish_leader_state(now.1.map(NodeId));
                }
                if now.2 != last.2 {
                    tracing::info!(old = last.2, new = now.2, "raft term changed");
                }
                last = now;
            }
        }
    }
}

#[async_trait]
impl PeerSink for NodeInner {
    fn local_schema(&self) -> SchemaTriple {
        self.cfg.schema
    }

    async fn handle(
        &self,
        meta: PeerEnvelopeMeta,
        req: PeerRequest,
    ) -> Result<PeerResponse, PeerReject> {
        let identity = self.cfg.identity;
        // Identity binding is checked before the payload is handed to OpenRaft: a peer from
        // another cluster or epoch must not be able to move our term (ADR-0011).
        if meta.cluster_id != identity.cluster_id {
            return Err(PeerReject::WrongCluster {
                expected: identity.cluster_id,
                got: meta.cluster_id,
            });
        }
        if meta.recovery_epoch != identity.recovery_epoch {
            return Err(PeerReject::WrongEpoch {
                expected: identity.recovery_epoch,
                got: meta.recovery_epoch,
            });
        }
        if meta.to != identity.node_id {
            return Err(PeerReject::WrongDestination {
                expected: identity.node_id,
                got: meta.to,
            });
        }
        // The fence (M5, ADR-0023, spec §21). Checked after the identity binding and before
        // anything is handed to OpenRaft: a retired node holds a genuine certificate and a
        // consistent envelope, so nothing above this line refuses it. Checked on *every*
        // node rather than only the leader, because the retired node will dial whichever
        // peer answers first.
        if self.is_retired(meta.from) {
            tracing::warn!(
                reason = "identity_retired",
                node_id = meta.from.0,
                rpc = req.kind(),
                "peer_identity_rejected"
            );
            // The peer array, so the exporter can say `plane="peer"` and mean it. The total
            // `/health` reports is the sum over both arrays, so this one add is enough.
            self.authn_rejected_peer[AuthnRejectReason::IdentityRetired.index()]
                .fetch_add(1, Ordering::Relaxed);
            return Err(PeerReject::Retired { node_id: meta.from });
        }
        if self.is_stopped() {
            return Err(PeerReject::NotRunning);
        }

        // Every command entry this envelope delivers belongs to the envelope's trace: when the
        // leader was replicating one client write it is that client's trace, otherwise it is
        // the replication RPC's own. Recorded before OpenRaft sees the request, because apply
        // can follow immediately (ADR-0013; `config_storage::trace`).
        if let PeerRequest::AppendEntries(rpc) = &req {
            for entry in &rpc.entries {
                if let openraft::EntryPayload::Normal(cmd) = &entry.payload {
                    self.traces.record(cmd, &meta.trace.trace_id);
                }
            }
        }

        let span = tracing::debug_span!(
            "peer_in",
            rpc = req.kind(),
            from = meta.from.0,
            trace_id = %meta.trace.trace_id,
        );
        async {
            match req {
                PeerRequest::AppendEntries(rpc) => self
                    .raft
                    .append_entries(rpc)
                    .await
                    .map(PeerResponse::AppendEntries)
                    .map_err(|e| PeerReject::Raft(e.to_string())),
                PeerRequest::Vote(rpc) => self
                    .raft
                    .vote(rpc)
                    .await
                    .map(PeerResponse::Vote)
                    .map_err(|e| PeerReject::Raft(e.to_string())),
                // Wired from M5 on: a learner whose log the leader has already purged can
                // only be caught up by a snapshot install (D5.2), so refusing this RPC
                // would make learner replacement impossible on exactly the cluster that
                // needs it. Chunk reassembly and the vote check are openraft's.
                PeerRequest::InstallSnapshot(rpc) => self
                    .raft
                    .install_snapshot(rpc)
                    .await
                    .map(PeerResponse::InstallSnapshot)
                    .map_err(|e| PeerReject::Raft(e.to_string())),
            }
        }
        .instrument(span)
        .instrument(self.span.clone())
        .await
    }

    fn is_retired(&self, node_id: NodeId) -> bool {
        NodeInner::is_retired(self, node_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use config_core::StatusClass;

    /// L3 (critic A1): a command entry that comes back `Noop` is committed and applied, so
    /// whatever it is reported as must not tell the caller to send it again.
    ///
    /// The regression this pins is one word: the arm used to build `ConfigError::Unavailable`,
    /// whose whole contract is "rejected before entering the log, safe to retry" — on the one
    /// path that knows the entry already took effect (ADR-0015).
    #[test]
    fn a_noop_for_a_command_entry_is_never_resubmittable() {
        let error = noop_for_command_entry();
        assert!(
            !error.is_safe_to_resubmit(),
            "the entry is applied; resubmitting would apply it twice: {error:?}"
        );
        assert_eq!(
            error.kind(),
            StatusClass::Internal,
            "a broken state-machine contract is an internal failure, not a retryable one"
        );
    }

    /// F-021: openraft hands `Fatal::Stopped`/`Fatal::Panicked` back through the `client_write`
    /// reply channel, which it only drops after the proposal was enqueued — so the entry may be
    /// committed. ADR-0015 lets a mutation be called resubmittable only when it provably did not
    /// commit, so the write path must not answer `Unavailable` here the way the read path does.
    #[test]
    fn a_fatal_on_the_write_path_is_never_resubmittable() {
        for fatal in [Fatal::Stopped, Fatal::Panicked] {
            let error = write_fatal_to_config_error(fatal.clone());
            assert!(
                !error.is_safe_to_resubmit(),
                "{fatal:?} arrives after the proposal was enqueued; replaying could apply it \
                 twice: {error:?}"
            );
            assert_eq!(
                error.kind(),
                StatusClass::DeadlineExceeded,
                "the outcome is unknown, so the caller must read back and CAS (ADR-0015)"
            );
            // The same `Fatal` on a read is retryable: `ensure_linearizable` submits nothing.
            assert!(
                read_fatal_to_config_error(fatal).is_safe_to_resubmit(),
                "a read has no outcome to be uncertain about"
            );
        }
    }
}
