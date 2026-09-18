//! The node: OpenRaft lifecycle, explicit formation, the client entry points, and the
//! peer-plane server side (spec §6.3, §8.1, §10.1, §13.1; ADR-0009, ADR-0011, ADR-0015).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
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
use config_storage::{RaftNodeId, StateReader, TypeConfig};
use openraft::error::{CheckIsLeaderError, ClientWriteError, Fatal, InitializeError, RaftError};
use openraft::{BasicNode, Raft, ServerState};
use tokio::task::JoinHandle;
use tracing::{Instrument, Span};

use crate::config::{NodeConfig, StorageHandle};
use crate::error::{EngineError, FormationError, FormationPlan, Timeout};
use crate::hint::{validate_hint, HintVerdict};
use crate::metrics::{Health, LogIdView, MembershipView, NodeMetrics, NodeRole};
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

/// Validate a [`GetRequest`]'s key.
///
/// `config-core` exposes `validate_put`/`validate_delete`/`validate_list` but no
/// `validate_get`, because a `Get` never becomes a command and so never needs apply-time
/// re-validation. The edge still has to reject an empty or over-long key, and it must produce
/// *byte-identical* `InvalidArgument` detail to `config-core`'s shared key validator so a
/// client cannot tell `Get` from `Put` by its error text.
fn validate_get(req: &GetRequest, limits: &Limits) -> Result<(), ConfigError> {
    if req.key.is_empty() {
        return Err(ConfigError::invalid_argument("key must not be empty"));
    }
    if req.key.len() > limits.max_key_bytes {
        return Err(ConfigError::invalid_argument(format!(
            "key is {} bytes, limit is {}",
            req.key.len(),
            limits.max_key_bytes
        )));
    }
    Ok(())
}

pub(crate) struct NodeInner {
    cfg: NodeConfig,
    storage: StorageHandle,
    reader: Arc<dyn StateReader>,
    raft: Raft<TypeConfig>,
    authorizer: Arc<dyn Authorizer>,
    gossip: Arc<dyn GossipObservationSource>,
    span: Span,
    stopped: AtomicBool,
    accepted_hints: Mutex<BTreeMap<NodeId, ObservedPeerHint>>,
    background: Mutex<Option<JoinHandle<()>>>,
}

/// A running rEtcd node: one OpenRaft instance, one store, one authorizer.
///
/// Cheap to clone (one `Arc`); every clone is the same node. The embedder holds one and
/// hands out [`crate::DirectClient`]s from it.
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

        let factory = EngineNetworkFactory {
            identity,
            transport,
            span: span.clone(),
        };

        let raft = Raft::new(
            identity.node_id.0,
            raft_config,
            factory,
            storage.log_store(),
            storage.state_machine(),
        )
        .instrument(span.clone())
        .await
        .map_err(|e| EngineError::Raft(e.to_string()))?;

        let inner = Arc::new(NodeInner {
            reader: storage.reader(),
            cfg,
            storage,
            raft,
            authorizer,
            gossip,
            span: span.clone(),
            stopped: AtomicBool::new(false),
            accepted_hints: Mutex::new(BTreeMap::new()),
            background: Mutex::new(None),
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

        span.in_scope(|| tracing::info!(peer_endpoint = %inner.cfg.peer_endpoint, "node started"));
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
        if !plan.voters.contains_key(&identity.node_id) {
            return Err(FormationError::NotAVoter);
        }
        // Checked before freshness so a second `form_cluster` on a working cluster reports the
        // thing the caller actually did wrong. A store that is dirty but *unformed* — a
        // half-wiped data directory — still reports `StoreNotFresh`, which is the case
        // ADR-0011 exists for.
        if inner.committed_membership().is_formed() {
            return Err(FormationError::AlreadyFormed);
        }
        if !inner.storage.is_fresh() {
            return Err(FormationError::StoreNotFresh);
        }

        let members: BTreeMap<RaftNodeId, BasicNode> = plan
            .voters
            .iter()
            .map(|(id, endpoint)| (id.0, BasicNode::new(endpoint.clone())))
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

    /// Committed membership: voter ids, their committed peer endpoints, and the membership
    /// log id. The only authoritative answer to "who is in this cluster".
    pub fn committed_membership(&self) -> MembershipView {
        self.inner.committed_membership()
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
                move || validate_get(&req, &limits),
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
        }
    }

    fn committed_membership(&self) -> MembershipView {
        let m = self.raft.metrics().borrow().membership_config.clone();
        MembershipView {
            voters: m.voter_ids().map(NodeId).collect(),
            endpoints: m
                .nodes()
                .map(|(id, node)| (NodeId(*id), node.addr.clone()))
                .collect(),
            membership_log_id: m.log_id().map(|l| (l.leader_id.term, l.index)),
        }
    }

    /// The committed endpoint of `node_id`, as a client-followable hint.
    fn hint_for(&self, node_id: NodeId) -> Option<LeaderHint> {
        self.committed_membership()
            .endpoint_of(node_id)
            .map(|endpoint| LeaderHint {
                node_id,
                endpoint: endpoint.to_string(),
            })
    }

    fn leader_hint(&self) -> Option<LeaderHint> {
        let leader = self.raft.metrics().borrow().current_leader?;
        self.hint_for(NodeId(leader))
    }

    fn health(&self) -> Health {
        if self.is_stopped() {
            return Health::Stopped;
        }
        let rx = self.raft.metrics();
        let (running, leader, formed) = {
            let m = rx.borrow();
            (
                m.running_state.clone(),
                m.current_leader,
                m.membership_config.voter_ids().next().is_some(),
            )
        };
        if let Err(fatal) = running {
            return Health::Unavailable {
                reason: format!("raft core stopped: {fatal}"),
            };
        }
        if !formed {
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

    fn authorize(
        &self,
        principal: &Principal,
        action: Action,
        key_or_prefix: &[u8],
    ) -> Result<(), ConfigError> {
        match self.authorizer.authorize(principal, action, key_or_prefix) {
            Decision::Allow => Ok(()),
            Decision::Deny { reason } => Err(ConfigError::PermissionDenied {
                detail: format!(
                    "principal {:?} may not {:?} key_hex={} ({reason})",
                    principal.name,
                    action,
                    key_hex(key_or_prefix)
                ),
            }),
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
        self.log_outcome(op, principal, key.as_ref(), started, &result, |r| {
            r.revision
        });
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

        match tokio::time::timeout(self.cfg.write_timeout, self.raft.client_write(cmd)).await {
            // The deadline elapsed *after* submission, so the mutation may still commit.
            // ADR-0015: never replay it; the caller reads and CASes instead.
            Err(_) => Err(ConfigError::DeadlineExceededUnknownOutcome),
            Ok(Ok(resp)) => match resp.data {
                CommandResponse::Mutation { response, .. } => Ok(response),
                CommandResponse::Rejected { reason } => {
                    Err(ConfigError::InvalidArgument { detail: reason })
                }
                CommandResponse::Noop => Err(ConfigError::Unavailable {
                    reason: "state machine returned no answer for a command entry".to_string(),
                }),
            },
            Ok(Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f)))) => {
                Err(self.not_leader(f.leader_id.map(NodeId)))
            }
            Ok(Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(e)))) => {
                Err(ConfigError::Unavailable {
                    reason: e.to_string(),
                })
            }
            Ok(Err(RaftError::Fatal(fatal))) => Err(fatal_to_config_error(fatal)),
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
            op,
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
            Ok(Err(RaftError::Fatal(fatal))) => Err(fatal_to_config_error(fatal)),
            Ok(Ok(_read_log_id)) => {
                let mut project = Some(project);
                let mut out = None;
                self.reader.with_state(&mut |s| {
                    if let Some(f) = project.take() {
                        out = Some(f(s, &validated));
                    }
                });
                Ok(out.expect("with_state always invokes its closure exactly once"))
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

    fn log_outcome<T>(
        &self,
        op: &'static str,
        principal: &Principal,
        key: &[u8],
        started: Instant,
        result: &Result<T, ConfigError>,
        revision: impl FnOnce(&T) -> u64,
    ) {
        let latency_ms = started.elapsed().as_millis() as u64;
        match result {
            Ok(v) => tracing::info!(
                op,
                principal = %principal.name,
                key_hex = %key_hex(key),
                outcome = "ok",
                revision = revision(v),
                latency_ms,
                "client operation completed"
            ),
            Err(e) => tracing::info!(
                op,
                principal = %principal.name,
                key_hex = %key_hex(key),
                outcome = "error",
                error = %e,
                error_kind = ?e.kind(),
                revision = 0u64,
                latency_ms,
                "client operation failed"
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

fn role_of(state: ServerState) -> NodeRole {
    match state {
        ServerState::Learner => NodeRole::Learner,
        ServerState::Follower => NodeRole::Follower,
        ServerState::Candidate => NodeRole::Candidate,
        ServerState::Leader => NodeRole::Leader,
        ServerState::Shutdown => NodeRole::Shutdown,
    }
}

fn fatal_to_config_error(fatal: Fatal<RaftNodeId>) -> ConfigError {
    match fatal {
        Fatal::StorageError(e) => ConfigError::FatalStorage {
            detail: e.to_string(),
        },
        other => ConfigError::Unavailable {
            reason: other.to_string(),
        },
    }
}

/// Background observer: polls advisory gossip on a timer and logs Raft state transitions.
///
/// Holds a `Weak`, so it never keeps the node alive; it exits when the last [`ConfigNode`]
/// is dropped, and is aborted outright by [`ConfigNode::stop`].
async fn background_loop(
    inner: Weak<NodeInner>,
    mut metrics: tokio::sync::watch::Receiver<openraft::RaftMetrics<RaftNodeId, BasicNode>>,
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
