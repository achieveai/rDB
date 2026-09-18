//! Shared doubles for the transport tests.
//!
//! The point of these is that the transport can be proved correct without a Raft node: a
//! scripted [`FakeStore`] decides what the client plane sees, and a scripted [`FakeSink`]
//! decides what the peer plane sees. Nothing here waits on time.

#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use config_core::{
    Capabilities, ConfigError, ConfigStore, DeleteRequest, GetRequest, GetResponse, ListRequest,
    ListResponse, MutationResponse, Principal, PutRequest,
};
use config_engine::transport::{
    PeerEnvelopeMeta, PeerHandler, PeerReject, PeerRequest, PeerResponse, PeerSink,
};
use config_grpc::{serve_client_plane, ClientBackend, ServerHandle, TlsMode};
use tokio::net::TcpListener;

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

/// A `ConfigStore` that returns whatever the test told it to.
#[derive(Default)]
pub struct FakeStore {
    script: Mutex<FakeScript>,
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

    pub fn hang_forever(&self) {
        self.script.lock().unwrap().hang = Some(Arc::new(tokio::sync::Notify::new()));
    }

    pub fn call_count(&self) -> u64 {
        self.calls.load(Ordering::Relaxed)
    }

    pub fn seen_principals(&self) -> Vec<Principal> {
        self.principals.lock().unwrap().clone()
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
    async fn get(&self, _request: GetRequest) -> Result<GetResponse, ConfigError> {
        match self.enter().await {
            Some(e) => Err(e),
            None => Ok(self.script.lock().unwrap().get.clone()),
        }
    }

    async fn list(&self, _request: ListRequest) -> Result<ListResponse, ConfigError> {
        match self.enter().await {
            Some(e) => Err(e),
            None => Ok(self.script.lock().unwrap().list.clone()),
        }
    }

    async fn put(&self, _request: PutRequest) -> Result<MutationResponse, ConfigError> {
        match self.enter().await {
            Some(e) => Err(e),
            None => Ok(self.script.lock().unwrap().mutation.clone()),
        }
    }

    async fn delete(&self, _request: DeleteRequest) -> Result<MutationResponse, ConfigError> {
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

/// A client plane on an ephemeral port, plus the `host:port` a client dials.
pub struct TestServer {
    pub handle: ServerHandle,
    pub endpoint: String,
}

pub async fn start_client_plane(store: Arc<FakeStore>, tls: TlsMode) -> TestServer {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral client-plane port");
    let handle = serve_client_plane(Arc::new(FakeBackend(store)), listener, tls)
        .expect("serve client plane");
    let endpoint = handle.local_addr().to_string();
    TestServer { handle, endpoint }
}

/// A `PeerSink` that answers whatever the test scripted, without consensus.
pub struct FakeSink {
    pub reject: Mutex<Option<PeerReject>>,
    pub seen: Mutex<Vec<PeerEnvelopeMeta>>,
    /// Cluster this fake believes it belongs to; a mismatch is refused like a real node.
    pub cluster_id: config_core::ClusterId,
    pub node_id: config_core::NodeId,
}

impl FakeSink {
    pub fn new(cluster_id: config_core::ClusterId, node_id: config_core::NodeId) -> Arc<Self> {
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
