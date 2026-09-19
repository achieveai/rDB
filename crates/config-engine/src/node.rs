//! The node: OpenRaft lifecycle, explicit formation, the client entry points, and the
//! peer-plane server side (spec §6.3, §8.1, §10.1, §13.1; ADR-0009, ADR-0011, ADR-0015).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use config_core::{
    Action, Authorizer, Capabilities, ClusterIdentity, Command, CommandResponse, ConfigError,
    Decision, Dedup, DeleteRequest, GetRequest, GetResponse, GossipObservationSource,
    IdentityMismatch, KvState, LeaderHint, Limits, ListRequest, ListResponse, MutationResponse,
    NodeId, ObservedPeerHint, Pagination, Principal, PutRequest, WatchResumption,
};
use config_log::TraceContext;
use config_storage::{RaftNode, RaftNodeId, StateReader, TraceRegistry, TypeConfig};
use openraft::error::{CheckIsLeaderError, ClientWriteError, Fatal, InitializeError, RaftError};
use openraft::{Raft, ServerState};
use tokio::task::JoinHandle;
use tracing::{Instrument, Span};

use crate::config::{NodeConfig, StorageHandle};
use crate::error::{EngineError, FormationError, FormationPlan, Timeout};
use crate::hint::{validate_hint, HintVerdict};
use crate::metrics::{
    Health, HealthPayload, LogIdView, MembershipView, NodeMetrics, NodeRole, PolicySummary,
};
use crate::network::EngineNetworkFactory;
use crate::transport::{
    PeerEnvelopeMeta, PeerHandler, PeerReject, PeerRequest, PeerResponse, PeerSink, PeerTransport,
};

/// How often a bounded wait re-checks its predicate when the metrics channel is quiet.
/// Small enough to keep test deadlines tight, large enough not to spin (test plan §6 rule 2).
const POLL_INTERVAL: Duration = Duration::from_millis(20);

/// Maximum key bytes rendered into a `key_hex` log field (ADR-0013).
const KEY_HEX_MAX_BYTES: usize = 32;

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
    /// Client connections whose transport identity could not be established (M3-81). The
    /// engine never sees a certificate, so only the transport can count these.
    authn_rejected: AtomicU64,
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
        cfg: NodeConfig,
        storage: StorageHandle,
        transport: Arc<dyn PeerTransport>,
        gossip: Arc<dyn GossipObservationSource>,
        authorizer: Arc<dyn Authorizer>,
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

        // Child of whatever span the caller is in, so under `#[retcd_test]` every line this
        // node ever emits — including from OpenRaft's own core task — carries `testMethod`.
        let span = tracing::info_span!(
            "node",
            node_id = identity.node_id.0,
            cluster_id = %identity.cluster_id,
            recovery_epoch = identity.recovery_epoch.0,
        );

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
            authn_rejected: AtomicU64::new(0),
        });

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
            authz_kind: inner.cfg.authz_kind,
            transport_security: inner.cfg.transport_security,
            policy: inner.policy_summary(),
            authz_denied: m.authz_denied,
            authn_rejected: m.authn_rejected,
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
    pub fn record_authn_rejection(&self) {
        self.inner.authn_rejected.fetch_add(1, Ordering::Relaxed);
    }

    /// What this node actually guarantees (ADR-0016).
    pub fn capabilities(&self) -> Capabilities {
        Capabilities {
            durability: self.inner.storage.durability(),
            watch_resumption: WatchResumption::Unsupported,
            authz: self.inner.cfg.authz_kind.into(),
            transport_security: self.inner.cfg.transport_security,
            pagination: Pagination::Unsupported,
            dedup: Dedup::Unsupported,
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
            authn_rejected: self.authn_rejected.load(Ordering::Relaxed),
        }
    }

    /// The policy this node holds, as the health payload reports it (M3-42).
    ///
    /// `grants` is forced to `0` unless a static allowlist is actually in force: an
    /// `AllowAll`, missing, or invalid policy enforces no grant, and reporting a non-zero
    /// count for one of those would be the exact "looks guarded, is not" reading ADR-0016
    /// exists to prevent.
    fn policy_summary(&self) -> PolicySummary {
        let kind = self.cfg.authz_kind;
        PolicySummary {
            kind: kind.into(),
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

    /// Whether this node will serve client traffic at all: membership known, storage not
    /// poisoned, and an authorization model actually in force (OQ-19).
    fn is_ready(&self) -> bool {
        !self.is_stopped()
            && self.cfg.authz_kind.is_present()
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
        if !self.cfg.authz_kind.is_present() {
            return Health::Unavailable {
                reason: format!("authorization policy is {}", self.cfg.authz_kind),
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
        let decision = if self.cfg.authz_kind.is_present() {
            self.authorizer.authorize(principal, action, key_or_prefix)
        } else {
            Decision::deny(format!(
                "node is not ready to authorize: policy is {}",
                self.cfg.authz_kind
            ))
        };
        config_core::audit(
            principal,
            action,
            key_or_prefix,
            &decision,
            self.cfg.authz_kind.into(),
        );
        match decision {
            Decision::Allow => Ok(()),
            Decision::Deny { reason } => {
                self.authz_denied.fetch_add(1, Ordering::Relaxed);
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
        if self.is_stopped() {
            return Err(ConfigError::Unavailable {
                reason: "stopped".to_string(),
            });
        }
        // Before replication, so the entry is already labelled when the apply path (which runs
        // on OpenRaft's own state-machine task, outside this span) reaches it, and when the
        // replication task builds the `AppendEntries` that carries it to the followers.
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
                HintVerdict::Rejected { reason } => tracing::warn!(
                    reason,
                    peer_node_id = hint.node_id.0,
                    peer_cluster_id = %hint.cluster_id,
                    peer_endpoint = %hint.peer_endpoint,
                    "gossip_hint_rejected"
                ),
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

/// Background observer: polls advisory gossip on a timer and logs Raft state transitions.
///
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
                PeerRequest::InstallSnapshot(_) => {
                    Err(PeerReject::Raft("snapshots unsupported".to_string()))
                }
            }
        }
        .instrument(span)
        .instrument(self.span.clone())
        .await
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
