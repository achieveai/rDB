//! Shared doubles for the transport tests.
//!
//! The point of these is that the transport can be proved correct without a Raft node: a
//! scripted [`FakeStore`] decides what the client plane sees, and a scripted [`FakeSink`]
//! decides what the peer plane sees. Nothing here waits on time.
//!
//! [`FakeStore`] also *records* the request it was handed. Without that, a conversion that
//! dropped `expected_mod_revision` on the floor would pass every test in this directory: the
//! call would still succeed, and nothing would ever look at what arrived.

#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use config_core::{
    Capabilities, ClusterId, ClusterIdentity, ConfigError, ConfigStore, DeleteRequest, GetRequest,
    GetResponse, ListRequest, ListResponse, MutationResponse, NodeId, Principal, PutRequest,
    RecoveryEpoch,
};
use config_engine::transport::{
    PeerEnvelopeMeta, PeerHandler, PeerReject, PeerRequest, PeerResponse, PeerSink,
};
use config_engine::{ConfigNode, InProcTransport, NetFault, NodeConfig, StorageHandle};
use config_grpc::{serve_client_plane, ClientBackend, PeerIdentity, ServerHandle, TlsMode};
use tokio::net::TcpListener;

/// The cluster every test in this crate serves, unless it is testing a mismatch.
pub const CLUSTER: &str = "0123456789abcdef0123456789abcdef";

/// [`CLUSTER`] parsed.
pub fn cluster() -> ClusterId {
    CLUSTER.parse().expect("test cluster id is valid hex")
}

/// What the fake answers with, until a test changes it.
pub struct FakeScript {
    pub error: Option<ConfigError>,
    pub get: GetResponse,
    pub list: ListResponse,
    pub mutation: MutationResponse,
    /// When set, every call awaits this and never returns — a server that is up but silent,
    /// which is how a client deadline is exercised without a sleep.
    pub hang: Option<Arc<tokio::sync::Notify>>,
}

impl Default for FakeScript {
    fn default() -> Self {
        Self {
            error: None,
            get: GetResponse {
                record: None,
                read_revision: 7,
            },
            list: ListResponse::default(),
            mutation: MutationResponse::applied_put(7),
            hang: None,
        }
    }
}

/// The last request seen on each method, as it arrived over the wire.
#[derive(Default)]
pub struct SeenRequests {
    pub get: Option<GetRequest>,
    pub list: Option<ListRequest>,
    pub put: Option<PutRequest>,
    pub delete: Option<DeleteRequest>,
}

/// A `ConfigStore` that returns whatever the test told it to, and remembers what it was asked.
#[derive(Default)]
pub struct FakeStore {
    script: Mutex<FakeScript>,
    seen: Mutex<SeenRequests>,
    pub calls: AtomicU64,
    pub principals: Mutex<Vec<Principal>>,
}

impl FakeStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn set_error(&self, error: Option<ConfigError>) {
        self.script.lock().unwrap().error = error;
    }

    pub fn set_mutation(&self, mutation: MutationResponse) {
        self.script.lock().unwrap().mutation = mutation;
    }

    pub fn set_get(&self, get: GetResponse) {
        self.script.lock().unwrap().get = get;
    }

    pub fn set_list(&self, list: ListResponse) {
        self.script.lock().unwrap().list = list;
    }

    pub fn hang_forever(&self) {
        self.script.lock().unwrap().hang = Some(Arc::new(tokio::sync::Notify::new()));
    }

    pub fn call_count(&self) -> u64 {
        self.calls.load(Ordering::Relaxed)
    }

    pub fn seen_principals(&self) -> Vec<Principal> {
        self.principals.lock().unwrap().clone()
    }

    pub fn last_get(&self) -> Option<GetRequest> {
        self.seen.lock().unwrap().get.clone()
    }

    pub fn last_list(&self) -> Option<ListRequest> {
        self.seen.lock().unwrap().list.clone()
    }

    pub fn last_put(&self) -> Option<PutRequest> {
        self.seen.lock().unwrap().put.clone()
    }

    pub fn last_delete(&self) -> Option<DeleteRequest> {
        self.seen.lock().unwrap().delete.clone()
    }

    async fn enter(&self) -> Option<ConfigError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let (error, hang) = {
            let s = self.script.lock().unwrap();
            (s.error.clone(), s.hang.clone())
        };
        if let Some(notify) = hang {
            // Never notified: the call parks here until the caller's deadline fires.
            notify.notified().await;
        }
        error
    }
}

#[async_trait]
impl ConfigStore for FakeStore {
    async fn get(&self, request: GetRequest) -> Result<GetResponse, ConfigError> {
        self.seen.lock().unwrap().get = Some(request);
        match self.enter().await {
            Some(e) => Err(e),
            None => Ok(self.script.lock().unwrap().get.clone()),
        }
    }

    async fn list(&self, request: ListRequest) -> Result<ListResponse, ConfigError> {
        self.seen.lock().unwrap().list = Some(request);
        match self.enter().await {
            Some(e) => Err(e),
            None => Ok(self.script.lock().unwrap().list.clone()),
        }
    }

    async fn put(&self, request: PutRequest) -> Result<MutationResponse, ConfigError> {
        self.seen.lock().unwrap().put = Some(request);
        match self.enter().await {
            Some(e) => Err(e),
            None => Ok(self.script.lock().unwrap().mutation.clone()),
        }
    }

    async fn delete(&self, request: DeleteRequest) -> Result<MutationResponse, ConfigError> {
        self.seen.lock().unwrap().delete = Some(request);
        match self.enter().await {
            Some(e) => Err(e),
            None => Ok(self.script.lock().unwrap().mutation.clone()),
        }
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::EPHEMERAL_DEVELOPMENT
    }
}

/// Serves one [`FakeStore`] to every principal, recording who asked.
pub struct FakeBackend(pub Arc<FakeStore>);

impl ClientBackend for FakeBackend {
    fn store_for(&self, principal: Principal) -> Arc<dyn ConfigStore> {
        self.0.principals.lock().unwrap().push(principal);
        self.0.clone()
    }
}

/// A backend that serves a [`FakeStore`] but reports authentication failures to a **real**
/// [`ConfigNode`] (M3-81).
///
/// The store stays fake because the claim is about the seam, not about consensus; the node is
/// real because the counter under test is the node's, and a double that counted for itself
/// would prove only that the test can add one to a number.
pub struct NodeBackend {
    pub node: ConfigNode,
    pub store: Arc<FakeStore>,
}

impl ClientBackend for NodeBackend {
    fn store_for(&self, principal: Principal) -> Arc<dyn ConfigStore> {
        self.store.principals.lock().unwrap().push(principal);
        self.store.clone()
    }

    fn record_authn_rejection(&self) {
        self.node.record_authn_rejection();
    }
}

/// An unformed single node, enough to own the counters a transport reports into.
///
/// Unformed on purpose: an authentication failure is refused before any store is chosen, so
/// forming a cluster would add election timing to a test that never reaches consensus.
pub async fn start_idle_node(node_id: NodeId) -> ConfigNode {
    let identity = ClusterIdentity {
        cluster_id: cluster(),
        recovery_epoch: RecoveryEpoch(1),
        node_id,
    };
    let store = config_storage::EphemeralStore::new(
        identity,
        config_core::Limits::DEFAULT,
        Arc::new(config_storage::NoFaults),
        tracing::info_span!("store", node_id = node_id.0),
    );
    ConfigNode::start(
        NodeConfig::new(identity, InProcTransport::endpoint(node_id)),
        StorageHandle::from(store),
        Arc::new(InProcTransport::new(NetFault::new())),
        Arc::new(config_core::NoGossip),
        Arc::new(config_core::AllowAll),
    )
    .await
    .expect("node start")
}

/// Serve the client plane over `backend` on an ephemeral port.
pub async fn start_client_plane_with(
    backend: Arc<dyn ClientBackend>,
    tls: TlsMode,
    cluster_id: ClusterId,
) -> TestServer {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral client-plane port");
    let node_id = u64::from(listener.local_addr().expect("addr").port());
    let handle = node_span(node_id)
        .in_scope(|| serve_client_plane(backend, listener, tls, cluster_id).expect("serve"));
    let endpoint = handle.local_addr().to_string();
    TestServer { handle, endpoint }
}

/// A client plane on an ephemeral port, plus the `host:port` a client dials.
pub struct TestServer {
    pub handle: ServerHandle,
    pub endpoint: String,
}

pub async fn start_client_plane(store: Arc<FakeStore>, tls: TlsMode) -> TestServer {
    start_client_plane_for(store, tls, cluster()).await
}

/// Serve the client plane as `cluster_id`, for the tests that present a foreign certificate.
pub async fn start_client_plane_for(
    store: Arc<FakeStore>,
    tls: TlsMode,
    cluster_id: ClusterId,
) -> TestServer {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral client-plane port");
    let node_id = u64::from(listener.local_addr().expect("addr").port());
    let handle = node_span(node_id).in_scope(|| {
        serve_client_plane(Arc::new(FakeBackend(store)), listener, tls, cluster_id)
            .expect("serve client plane")
    });
    let endpoint = handle.local_addr().to_string();
    TestServer { handle, endpoint }
}

/// A span carrying `node_id`, entered around a server start.
///
/// ADR-0013 requires every `config_grpc*` line to name the node it was written on behalf of,
/// and `config_grpc::server::spawn` captures the current span for the serving task, so the
/// span has to be current at the call — `in_scope`, never a guard held across an await.
///
/// These doubles have no cluster identity, so the id is whatever the row is pretending to be:
/// a peer plane uses the identity it serves, and a client plane — which speaks for no
/// particular member — uses its own listening port, which is at least unique per server.
pub fn node_span(node_id: u64) -> tracing::Span {
    tracing::info_span!("fake_node", node_id)
}

/// A `PeerSink` that answers whatever the test scripted, without consensus.
pub struct FakeSink {
    pub reject: Mutex<Option<PeerReject>>,
    pub seen: Mutex<Vec<PeerEnvelopeMeta>>,
    /// Cluster this fake believes it belongs to; a mismatch is refused like a real node.
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
}

impl FakeSink {
    pub fn new(cluster_id: ClusterId, node_id: NodeId) -> Arc<Self> {
        Arc::new(Self {
            reject: Mutex::new(None),
            seen: Mutex::new(Vec::new()),
            cluster_id,
            node_id,
        })
    }

    pub fn handler(self: &Arc<Self>) -> PeerHandler {
        PeerHandler::new(self.clone())
    }

    /// The identity a truthful server would stamp on its answers.
    pub fn identity(&self) -> PeerIdentity {
        PeerIdentity {
            cluster_id: self.cluster_id,
            recovery_epoch: RecoveryEpoch(0),
            node_id: self.node_id,
        }
    }

    pub fn seen_metas(&self) -> Vec<PeerEnvelopeMeta> {
        self.seen.lock().unwrap().clone()
    }
}

#[async_trait]
impl PeerSink for FakeSink {
    async fn handle(
        &self,
        meta: PeerEnvelopeMeta,
        req: PeerRequest,
    ) -> Result<PeerResponse, PeerReject> {
        if let Some(reject) = self.reject.lock().unwrap().clone() {
            return Err(reject);
        }
        if meta.cluster_id != self.cluster_id {
            return Err(PeerReject::WrongCluster {
                expected: self.cluster_id,
                got: meta.cluster_id,
            });
        }
        if meta.to != self.node_id {
            return Err(PeerReject::WrongDestination {
                expected: self.node_id,
                got: meta.to,
            });
        }
        self.seen.lock().unwrap().push(meta);
        match req {
            PeerRequest::Vote(v) => Ok(PeerResponse::Vote(openraft::raft::VoteResponse {
                vote: v.vote,
                vote_granted: true,
                last_log_id: None,
            })),
            other => Err(PeerReject::Raft(format!(
                "fake sink does not answer {}",
                other.kind()
            ))),
        }
    }
}

/// Every `rpc`-ish JSONL line this test wrote, scoped to this process's run.
///
/// Anti-flake rule 11: callers assert a positive count before asserting a property, and this
/// returns the rows rather than a boolean so a failure message can print what was there.
pub fn log_lines(module: &str, method: &str) -> Vec<serde_json::Value> {
    let path =
        config_log::layer::test_file_path(&config_log::testing::test_log_dir(), module, method);
    let contents = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("test log {} is readable: {e}", path.display()));
    let test_run = config_log::testing::test_run_id();
    contents
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter(|v| v.get("testRun").and_then(serde_json::Value::as_str) == Some(test_run))
        .collect()
}
