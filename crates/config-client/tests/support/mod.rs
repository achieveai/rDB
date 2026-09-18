//! A scripted server for the client tests.
//!
//! Small on purpose: the client's job is retry policy and status translation, so the thing it
//! talks to only has to be able to say "here is an answer" or "here is an error".

#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use config_core::{
    Capabilities, ConfigError, ConfigStore, DeleteRequest, GetRequest, GetResponse, ListRequest,
    ListResponse, MutationResponse, Principal, PutRequest,
};
use config_grpc::{serve_client_plane, ClientBackend, ServerHandle, TlsMode};
use tokio::net::TcpListener;

struct Script {
    error: Option<ConfigError>,
    mutation: MutationResponse,
    hang: Option<Arc<tokio::sync::Notify>>,
}

/// A `ConfigStore` that answers with whatever the test scripted.
pub struct FakeStore {
    script: Mutex<Script>,
    calls: AtomicU64,
}

impl FakeStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            script: Mutex::new(Script {
                error: None,
                mutation: MutationResponse::applied_put(11),
                hang: None,
            }),
            calls: AtomicU64::new(0),
        })
    }

    pub fn set_error(&self, error: Option<ConfigError>) {
        self.script.lock().unwrap().error = error;
    }

    pub fn set_mutation(&self, mutation: MutationResponse) {
        self.script.lock().unwrap().mutation = mutation;
    }

    /// Accept the call and never answer, so the caller's deadline is what ends it.
    pub fn hang_forever(&self) {
        self.script.lock().unwrap().hang = Some(Arc::new(tokio::sync::Notify::new()));
    }

    pub fn call_count(&self) -> u64 {
        self.calls.load(Ordering::Relaxed)
    }

    async fn answer(&self) -> Result<MutationResponse, ConfigError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let (error, mutation, hang) = {
            let s = self.script.lock().unwrap();
            (s.error.clone(), s.mutation.clone(), s.hang.clone())
        };
        if let Some(notify) = hang {
            notify.notified().await;
        }
        match error {
            Some(e) => Err(e),
            None => Ok(mutation),
        }
    }
}

#[async_trait]
impl ConfigStore for FakeStore {
    async fn get(&self, _request: GetRequest) -> Result<GetResponse, ConfigError> {
        self.answer().await.map(|_| GetResponse {
            record: None,
            read_revision: 11,
        })
    }

    async fn list(&self, _request: ListRequest) -> Result<ListResponse, ConfigError> {
        self.answer().await.map(|_| ListResponse {
            records: Vec::new(),
            read_revision: 11,
            truncated: false,
        })
    }

    async fn put(&self, _request: PutRequest) -> Result<MutationResponse, ConfigError> {
        self.answer().await
    }

    async fn delete(&self, _request: DeleteRequest) -> Result<MutationResponse, ConfigError> {
        self.answer().await
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::EPHEMERAL_DEVELOPMENT
    }
}

struct OneStore(Arc<FakeStore>);

impl ClientBackend for OneStore {
    fn store_for(&self, _principal: Principal) -> Arc<dyn ConfigStore> {
        self.0.clone()
    }
}

/// A running client plane over one [`FakeStore`].
pub struct Node {
    pub store: Arc<FakeStore>,
    pub endpoint: String,
    handle: ServerHandle,
}

impl Node {
    pub async fn start() -> Node {
        let store = FakeStore::new();
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral client-plane port");
        let handle = serve_client_plane(
            Arc::new(OneStore(store.clone())),
            listener,
            TlsMode::Insecure,
        )
        .expect("serve client plane");
        let endpoint = handle.local_addr().to_string();
        Node {
            store,
            endpoint,
            handle,
        }
    }

    pub async fn shutdown(self) {
        let _ = self.handle.shutdown().await;
    }
}

/// Start `n` nodes on ephemeral ports.
pub async fn start_nodes(n: usize) -> Vec<Node> {
    let mut nodes = Vec::with_capacity(n);
    for _ in 0..n {
        nodes.push(Node::start().await);
    }
    nodes
}
