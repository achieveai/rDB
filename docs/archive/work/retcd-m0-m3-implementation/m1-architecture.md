# M1 architecture note (Architect: Fable, 2026-09-18)

Binding contracts for the M1 crates. Developers implement exactly these names; deviations are
reported in the handoff. Sources of truth above this note: DesignSpec-01 §6, §8, §9, §10, §13,
§21; ADR-0002/0004/0008/0009/0010/0011/0013/0014/0016; docs/testing/test-plan-m0-m1.md §1, §4;
research-openraft-0.9.25.md §10 (compile-verified skeleton) and its adaptation checklist.

## 0. Dependency graph (ADR-0004, inward only)

```
config-server ──► config-engine, config-grpc, config-client, config-storage, config-gossip, config-log
config-testkit ─► (same as server) + duckdb (dev)
config-client ──► config-engine (DirectClient), config-grpc (GrpcClient), config-core
config-grpc ────► config-engine (PeerHandler, ConfigNode), config-core, config-log
config-engine ──► config-storage, config-core, config-log, openraft
config-storage ─► config-core, config-log, openraft, rocksdb (M2)
config-gossip ──► config-core, config-log, memberlist
config-core ────► (nothing async / network)
```

Rule: `openraft` types appear in the public API of `config-storage` and `config-engine` only
(they are the Raft layer); never in `config-core`, `config-client`, `config-testkit` tests.

## 1. `config-storage`

```rust
pub type RaftNodeId = u64;                       // openraft NodeId (config_core::NodeId is a newtype)
openraft::declare_raft_types!(pub TypeConfig: D = config_core::Command, R = config_core::CommandResponse,
    NodeId = RaftNodeId, Node = openraft::BasicNode, Entry = openraft::Entry<TypeConfig>,
    SnapshotData = std::io::Cursor<Vec<u8>>, Responder = openraft::impls::OneshotResponder<TypeConfig>,
    AsyncRuntime = openraft::TokioRuntime);

/// TA-4. Checked at every durability boundary by both stores.
pub enum Boundary { BeforeVoteSync, AfterVoteSync, BeforeLogAppend, AfterLogAppend,
                    BeforeLogFlush, AfterLogFlush, BeforeStateBatch, AfterStateBatch }
pub enum FaultAction { Proceed, Fail, Crash }
pub trait FaultInjector: Send + Sync { fn before(&self, b: Boundary) -> FaultAction; }
pub struct NoFaults;                                              // default
/// Counters per boundary (TA-13 in the M2 plan will use them).
pub struct FaultCounters { .. } // Arc<AtomicU64> per boundary, `snapshot() -> BTreeMap<Boundary,u64>`

/// What the engine reads without going through Raft (after ensure_linearizable).
pub trait StateReader: Send + Sync {
    fn with_state<T>(&self, f: &mut dyn FnMut(&KvState) -> T) -> T;   // sync, cheap lock
    fn last_applied(&self) -> Option<openraft::LogId<RaftNodeId>>;
    fn membership(&self) -> openraft::StoredMembership<RaftNodeId, openraft::BasicNode>;
}

pub struct EphemeralStore { .. }   // Clone (Arc inside)
impl EphemeralStore {
    pub fn new(identity: ClusterIdentity, faults: Arc<dyn FaultInjector>) -> Self;
    pub fn log_store(&self) -> EphemeralLog;            // impl RaftLogStorage + RaftLogReader
    pub fn state_machine(&self) -> EphemeralSm;         // impl RaftStateMachine
    pub fn reader(&self) -> Arc<dyn StateReader>;
    pub fn identity(&self) -> ClusterIdentity;
    pub fn is_fresh(&self) -> bool;                     // no vote, no log, no applied (formation gate)
    pub fn durability(&self) -> config_core::Durability; // Ephemeral, always
}
```

Rules (ADR-0008, research §3/§8):
- `apply()` returns exactly one `CommandResponse` per entry. Blank/Membership entries yield
  `CommandResponse::Mutation{ outcome: Applied?? }` — NO. They yield a dedicated variant:
  add `CommandResponse::Noop` in config-core (Architect decision; dev-core is told separately).
  If dev-core has finished without it, config-storage adds it via a follow-up edit to
  config-core (single small change, reported).
- `save_committed`/`read_committed` implemented (not the default no-op).
- `FaultAction::Fail` → return `StorageError` (io error kind); `Crash` → return error AND set a
  `poisoned` flag: every later call returns an error until the store is reopened (RocksStore) or
  recreated (Ephemeral). The engine surfaces `FatalStorage` and health `Unavailable`.
- `EphemeralSm` = `Mutex<{ kv: KvState, last_applied, membership }>`; last_applied and
  membership updated in the same critical section as `kv.apply` (mirrors the M2 atomic batch).
- Every apply logs at debug: `log_index`, `term`, `op`, `key_hex`, `outcome`, `revision`.
  Every boundary logs at trace: `boundary`, `fault_action` (only when != Proceed at debug+).
- Node span: the store receives `span: tracing::Span` (the engine's node span) and instruments
  each trait method (`let _g = self.span.enter()` or `.instrument(self.span.clone())`) so lines
  emitted from openraft's core task still carry `testMethod`/`node_id` (ADR-0013).

## 2. `config-engine`

```rust
pub struct NodeConfig {
    pub identity: ClusterIdentity,
    pub peer_endpoint: String,                         // advertised "host:port" for BasicNode.addr
    pub raft: RaftTimers { heartbeat_ms: 250, election_min_ms: 750, election_max_ms: 1500 }, // Windows-safe
    pub limits: config_core::Limits,
    pub read_timeout: Duration,                        // ensure_linearizable wrapper, default 5 s
    pub write_timeout: Duration,                       // client_write wrapper, default 10 s
}

/// Peer RPCs, transport-agnostic (serde). config-grpc maps them to PeerEnvelope.
pub enum PeerRequest  { AppendEntries(AppendEntriesRequest<TypeConfig>), Vote(VoteRequest<RaftNodeId>), InstallSnapshot(..) }
pub enum PeerResponse { AppendEntries(AppendEntriesResponse<RaftNodeId>), Vote(VoteResponse<RaftNodeId>), InstallSnapshot(..) }
pub struct PeerEnvelopeMeta { cluster_id, recovery_epoch, from: NodeId, to: NodeId, trace: TraceContext }

#[derive(thiserror::Error)] pub enum TransportError { Unreachable(String), Network(String), Remote(String), IdentityRejected(String) }

#[async_trait] pub trait PeerTransport: Send + Sync {
    async fn send(&self, to: NodeId, endpoint: &str, meta: PeerEnvelopeMeta, req: PeerRequest, deadline: Duration)
        -> Result<PeerResponse, TransportError>;
}

/// Server side of the peer plane; config-grpc's PeerService and the testkit call this.
pub struct PeerHandler(Arc<NodeInner>);
impl PeerHandler { pub async fn handle(&self, meta: PeerEnvelopeMeta, req: PeerRequest) -> Result<PeerResponse, PeerReject>; }
// PeerReject: WrongCluster | WrongEpoch | WrongDestination | NotRunning | Raft(RaftError)

pub struct FormationPlan { pub cluster_id: ClusterId, pub recovery_epoch: RecoveryEpoch, pub voters: BTreeMap<NodeId, String /*peer endpoint*/> }
#[derive(thiserror::Error)] pub enum FormationError { IdentityMismatch(IdentityMismatch), StoreNotFresh, AlreadyFormed, NotAVoter, Raft(String) }

pub enum Health { Ready, NotLeader { hint: Option<LeaderHint> }, Unavailable { reason: String }, Stopped }
pub struct NodeMetrics { .. as TA-6 .. }   // + `running_state_ok: bool`

pub struct ConfigNode { .. }               // Clone-able handle (Arc inside)
impl ConfigNode {
    pub async fn start(cfg: NodeConfig, storage: StorageHandle, transport: Arc<dyn PeerTransport>,
                       gossip: Arc<dyn GossipObservationSource>, authorizer: Arc<dyn Authorizer>) -> Result<ConfigNode, EngineError>;
    pub fn peer_handler(&self) -> PeerHandler;
    pub async fn form_cluster(&self, plan: FormationPlan) -> Result<(), FormationError>;
    pub async fn wait_for_leader(&self, deadline: Duration) -> Option<NodeId>;
    pub async fn wait_applied(&self, index: u64, deadline: Duration) -> Result<(), Timeout>;
    pub fn metrics(&self) -> NodeMetrics;
    pub fn applied_index(&self) -> u64;
    pub fn capabilities(&self) -> Capabilities;       // from storage.durability(), authorizer kind, tls mode passed in cfg
    pub fn health(&self) -> Health;
    pub fn state_hash(&self) -> [u8; 32];              // harness convergence oracle
    pub fn committed_membership(&self) -> MembershipView; // voter ids + endpoints + membership log id
    pub fn leader_hint(&self) -> Option<LeaderHint>;   // from metrics.current_leader + committed membership (never gossip)
    pub fn validate_hint(hint: &ObservedPeerHint, membership: &MembershipView, id: &ClusterIdentity) -> HintVerdict; // pure, pub fn
    // The engine calls
    pub async fn put(&self, p: &Principal, req: PutRequest) -> Result<MutationResponse, ConfigError>;   // validate → authz → client_write
    pub async fn delete(..); pub async fn get(..); pub async fn list(..);                                  // get/list: ensure_linearizable (timeout) → StateReader
    pub async fn stop(&self) -> Result<(), EngineError>;   // raft.shutdown, transport idle; later calls → Unavailable("stopped")
}
pub enum StorageHandle { Ephemeral(EphemeralStore) /* , Rocks(RocksStore) in M2 */ }
```

Error mapping (ADR-0009/0015, research §5.3/§5.4):
- `client_write` → `ForwardToLeader{leader_id: Some}` → `NotLeader{hint}` (hint from committed
  membership); `ForwardToLeader{None}` → `Unavailable`; `Fatal`/`QuorumNotEnough`… → `Unavailable`;
  timeout after submit → `DeadlineExceededUnknownOutcome`; storage fatal → `FatalStorage`.
- `ensure_linearizable` → `ForwardToLeader` → NotLeader/Unavailable as above; `QuorumNotEnough`
  → `Unavailable`; timeout → `Unavailable` (reads have no side effects).
- Not formed (no membership, no leader) → `Unavailable`.
- Validation (`config_core::validate_*`) and authorization run BEFORE `client_write`; a rejected
  request never enters the log (M1-18, M1-27).

Gossip: the engine polls `gossip.peers()` every ~1 s in a background task, runs
`validate_hint` on each, logs `warn` with `reason` for rejected hints, and keeps a
`BTreeMap<NodeId, ObservedPeerHint>` of accepted hints for telemetry only. It never feeds
them into Raft membership or the transport target (committed endpoint is authoritative).

Logging: `ConfigNode::start` creates `node_span = info_span!("node", node_id, cluster_id, epoch)`
as a child of the caller's current span (so under `#[retcd_test]` it inherits testMethod).
Every engine entry point, storage trait method, transport call, and the gossip poll task run
inside that span (`Instrument`). Each client call additionally opens `TraceContext::current_or_root().span(op)`.
Metrics deltas (role change, leader change, term change) log at info with `old`/`new` fields.

Runtime: the library never creates a runtime; `start` requires a current Tokio runtime
(`Handle::try_current()` → `EngineError::NoRuntime`).

## 3. `config-grpc`

- `build.rs`: protox compiles `proto/retcd/v1/{config,peer}.proto`; generated modules
  `retcd::v1` (`pub mod pb`). No `protoc`.
- `TlsMode { Insecure, MutualTls(MtlsConfig{ ca_pem, cert_pem, key_pem }) }` (pem bytes; files
  are the server binary's concern).
- `serve_client_plane(node: ConfigNode, listener: TcpListener, tls: TlsMode, principal_source) -> ServerHandle`
  ConfigService impl: extract `TraceContext` from metadata, derive `Principal` (M1 Insecure:
  `Principal::development()`; M3: from client cert SAN), call the engine, map `ConfigError` →
  `tonic::Status` per §6.2 table, add `retcd-leader-node-id`/`retcd-leader-endpoint` on NotLeader.
- `serve_peer_plane(handler: PeerHandler, listener, tls) -> ServerHandle`; PeerService impl
  decodes `PeerEnvelope` (payload_encoding 1 = serde_json), builds `PeerEnvelopeMeta`, calls
  `handler.handle`, maps `PeerReject` → `UNAUTHENTICATED`/`FAILED_PRECONDITION`.
- `GrpcPeerTransport::new(tls: TlsMode, faults: NetFault) -> Arc<dyn PeerTransport>`: lazy
  connect per endpoint (`Channel`), bounded backoff, `tonic::Request` with trace headers;
  maps connect errors → `TransportError::Unreachable`, transport failures → `Network`, status
  errors → `Remote`. **NetFault (TA-5) lives here** (`config_engine::NetFault` type, shared
  handle): before send it checks `blocked(from,to)`; a block set while a call is in flight
  cancels it (`tokio::select!` on a `Notify`) → `Unreachable`. `delay(from,to,d)` sleeps before
  send; `drop_response(from,to)` sends, then discards the response → `Network`.
- `ServerHandle { local_addr(), shutdown().await }`.

## 4. `config-client`

- `DirectClient::new(node: ConfigNode, principal: Principal) -> Arc<dyn ConfigStore>`; each
  call goes to the engine (no forwarding; NotLeader is returned to the embedder).
- `GrpcClient::connect(endpoints: Vec<String>, tls: TlsMode, opts: GrpcClientOptions{ max_hint_follows: 3, deadline })`
  implements `ConfigStore`; on `FAILED_PRECONDITION` with leader metadata, re-dials the hinted
  endpoint (must be one of the configured endpoints OR pass mTLS identity check in M3) up to
  3 times; each attempt carries the same `request_id`; a `DEADLINE_EXCEEDED` on a mutation is
  returned as `DeadlineExceededUnknownOutcome` and is never retried (ADR-0015).

## 5. `config-testkit`

Exactly test-plan §4.1. Wiring per node: ephemeral listeners on `127.0.0.1:0` for peer and
client planes; `GrpcPeerTransport` sharing one `NetFault` across the cluster; gossip per
`GossipKind` (Real = `config_gossip::GossipNode`, Disabled = `NoGossip`, Poisoned/Partitioned =
`StaticObservationSource` + `GossipControl`). `Cluster::start(3, Ephemeral)` returns after a
leader exists and voter ids agree on all nodes. `conformance::run_all` per §4.3. `logs` module:
`duckdb_query(sql) -> Vec<Row>` over `target/test-logs/**/*.jsonl` filtered by the current
`testRun`; fails loudly if the file is missing. `poll_until(pred, deadline, interval)` with a
diagnostic containing every node's `NodeMetrics` on timeout.

## 6. Rulings after dev-core handoff (2026-09-18)

- `config_core::ConfigStore` has NO principal parameter: identity is bound at client
  construction (`DirectClient::new(node, principal)`; gRPC derives it per connection). The
  engine's internal entry points DO take `&Principal` (they are not the trait).
- `MutationOutcome` is flat `{Applied, Conflict, NotFound}`; `MutationResponse { outcome,
  revision, exists, current_mod_revision }` (1:1 with proto). `CommandResponse::Mutation {
  response, event }`, `Rejected { reason }`, `Noop` (storage produces Noop for Blank/Membership).
- `StatusClass` has 10 variants incl. `NotFound` and `Ok`-like classes; config-grpc maps by
  `ConfigError::kind()`.
- `validate_list` clamps caps (never ResourceExhausted). `KvState::with_limits(limits)` exists;
  all voters must use identical limits (engine passes `NodeConfig.limits` to storage).
- `PrincipalKind = { Certificate, Peer, Embedded, Development }`; `Principal::development()`.
- Apply logs at debug; the engine logs each client mutation outcome at info.
- Shared interface crates scaffolded by the lead: `config-storage` (TypeConfig, Boundary,
  FaultAction, FaultInjector, NoFaults, StateReader) and `config-engine::transport` +
  `config-engine::netfault`. Developers extend; they do not rename.
