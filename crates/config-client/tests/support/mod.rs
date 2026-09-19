//! A scripted server for the client tests.
//!
//! Small on purpose: the client's job is retry policy and status translation, so the thing it
//! talks to only has to be able to say "here is an answer" or "here is an error".

#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use std::time::Duration;

use async_trait::async_trait;
use config_core::{
    Capabilities, ClusterId, ConfigError, ConfigStore, DeleteRequest, GetRequest, GetResponse,
    Limits, ListRequest, ListResponse, MutationResponse, Principal, PutRequest, WatchItem,
    WatchRequest, WatchStream,
};
use config_grpc::{pb, serve_client_plane, ClientBackend, ServerHandle, TlsMode};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// The cluster these tests serve. Insecure mode never looks at it, but the plane still has to
/// be told which cluster it is.
pub const CLUSTER: &str = "0123456789abcdef0123456789abcdef";

/// [`CLUSTER`] parsed.
pub fn cluster() -> ClusterId {
    CLUSTER.parse().expect("test cluster id is valid hex")
}

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

/// Requests are validated here exactly as a real node validates them, and before anything is
/// counted or scripted.
///
/// A double that accepted an illegal request would be lying about the one thing every rEtcd
/// server does first: `config_core::validate_*` runs at the API edge ahead of authorization
/// and consensus. Tests that assert an error is reported *before* submission are only honest
/// against a double that refuses what a node refuses.
#[async_trait]
impl ConfigStore for FakeStore {
    /// Scripted like every other call: the stream carries whatever `answer` would have
    /// returned, then ends. Enough for the transport rows, which are about *opening* a watch
    /// and about what the client does with a termination, never about delivery order.
    async fn watch(&self, _request: WatchRequest) -> Result<WatchStream, ConfigError> {
        self.answer().await?;
        Ok(Box::pin(tokio_stream::iter(Vec::<
            Result<WatchItem, ConfigError>,
        >::new())))
    }

    async fn get(&self, request: GetRequest) -> Result<GetResponse, ConfigError> {
        config_core::validate_get(&request, &Limits::DEFAULT)?;
        self.answer().await.map(|_| GetResponse {
            record: None,
            read_revision: 11,
        })
    }

    async fn list(&self, request: ListRequest) -> Result<ListResponse, ConfigError> {
        config_core::validate_list(&request, &Limits::DEFAULT)?;
        self.answer().await.map(|_| ListResponse {
            records: Vec::new(),
            read_revision: 11,
            truncated: false,
        })
    }

    async fn put(&self, request: PutRequest) -> Result<MutationResponse, ConfigError> {
        config_core::validate_put(&request, &Limits::DEFAULT)?;
        self.answer().await
    }

    async fn delete(&self, request: DeleteRequest) -> Result<MutationResponse, ConfigError> {
        config_core::validate_delete(&request, &Limits::DEFAULT)?;
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
        Node::start_with(TlsMode::Insecure, cluster()).await
    }

    /// Serve the plane under `tls` as `cluster_id`.
    ///
    /// The span carries `node_id` so every `config_grpc` line this server writes is
    /// attributable to a node, which is what ADR-0013's mandatory node context asks for. The
    /// id is the plane's own port: these fakes have no cluster identity, and a made-up
    /// constant would put several unrelated servers under one node.
    pub async fn start_with(tls: TlsMode, cluster_id: ClusterId) -> Node {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral client-plane port");
        let node_id = u64::from(listener.local_addr().expect("addr").port());
        let store = FakeStore::new();
        // `in_scope`, not a guard held across an await: the span has to be current when the
        // serving task is spawned, and nothing else (ADR-0013).
        let handle = tracing::info_span!("fake_node", node_id).in_scope(|| {
            serve_client_plane(
                Arc::new(OneStore(store.clone())),
                listener,
                tls,
                cluster_id,
                Limits::DEFAULT,
                // No admin plane on the fake node: these rows exercise the data plane, and a
                // service nothing calls would only add a way for them to fail.
                None,
            )
            .expect("serve client plane")
        });
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

/// A TCP listener that speaks just enough HTTP/2 to accept a request and then destroy the
/// connection under it.
///
/// This is the only way to reproduce the case the client's safety argument turns on: the
/// request *was* written to the socket, so a server may have applied it, and the caller
/// nevertheless gets an error. A scripted `ConfigStore` cannot produce it — anything it
/// returns is by definition a decision the server reached.
pub struct ResetServer {
    pub endpoint: String,
    accepted: Arc<AtomicU64>,
}

impl ResetServer {
    /// Accept connections forever, read the request, then reset.
    pub async fn start() -> ResetServer {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral reset-server port");
        let endpoint = listener.local_addr().expect("addr").to_string();
        let accepted = Arc::new(AtomicU64::new(0));
        let counter = accepted.clone();

        tokio::spawn(config_log::testing::in_current_span(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                counter.fetch_add(1, Ordering::Relaxed);
                // An empty SETTINGS frame, so the client's handshake completes and it goes on
                // to write its HEADERS and DATA frames rather than stalling.
                const SETTINGS: [u8; 9] = [0, 0, 0, 4, 0, 0, 0, 0, 0];
                if socket.write_all(&SETTINGS).await.is_err() {
                    continue;
                }

                // Read until the request is plainly on the wire: the 24-byte preface, the
                // client's own SETTINGS and window update, then HEADERS and DATA.
                let mut seen = 0usize;
                let mut buf = [0u8; 1024];
                let _ = tokio::time::timeout(Duration::from_secs(5), async {
                    while seen < 120 {
                        match socket.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => seen += n,
                        }
                    }
                })
                .await;

                // Linger zero turns the close into an RST, which is what a machine losing
                // power or a middlebox dropping the flow looks like to the peer.
                // Deprecated because a non-zero linger blocks the closing thread. A linger of
                // zero is the opposite: it discards the send buffer and emits the RST
                // immediately, which is exactly the abrupt loss this fake exists to produce.
                #[allow(deprecated)]
                let _ = socket.set_linger(Some(Duration::ZERO));
                drop(socket);
            }
        }));

        ResetServer { endpoint, accepted }
    }

    /// How many connections were accepted and then reset.
    pub fn accepted(&self) -> u64 {
        self.accepted.load(Ordering::Relaxed)
    }
}

/// A `ConfigService` that records the `grpc-timeout` metadata of every call it receives.
///
/// The client plane hands a store the request body only, so a deadline the client claims to
/// send can only be proved by looking at the metadata the server actually saw.
#[derive(Default)]
pub struct DeadlineSpy {
    seen: Mutex<Vec<String>>,
    answer: Mutex<Option<ConfigError>>,
    hang: tokio::sync::Notify,
}

impl DeadlineSpy {
    /// Every `grpc-timeout` value seen so far, in arrival order. A call that carried no
    /// deadline at all is absent, which is what makes "the client sends one" assertable.
    pub fn timeouts(&self) -> Vec<String> {
        self.seen.lock().unwrap().clone()
    }

    /// Answer every call with `error` instead of parking on it.
    pub fn answer_with(&self, error: ConfigError) {
        *self.answer.lock().unwrap() = Some(error);
    }

    fn record<T>(&self, request: &tonic::Request<T>) {
        if let Some(value) = request
            .metadata()
            .get("grpc-timeout")
            .and_then(|v| v.to_str().ok())
        {
            self.seen.lock().unwrap().push(value.to_string());
        }
    }

    async fn answer<T>(&self) -> Result<tonic::Response<T>, tonic::Status> {
        let scripted = self.answer.lock().unwrap().clone();
        match scripted {
            Some(error) => Err(config_grpc::status_from_error(&error)),
            // Never notified: the caller's budget is the only thing that ends this call.
            None => {
                self.hang.notified().await;
                unreachable!("the spy is never notified")
            }
        }
    }
}

/// Newtype so the generated service trait is implemented on a local type.
pub struct SpyService(pub Arc<DeadlineSpy>);

#[tonic::async_trait]
impl pb::config_service_server::ConfigService for SpyService {
    type WatchStream =
        futures_core::stream::BoxStream<'static, Result<pb::WatchResponse, tonic::Status>>;

    async fn watch(
        &self,
        request: tonic::Request<pb::WatchRequest>,
    ) -> Result<tonic::Response<Self::WatchStream>, tonic::Status> {
        self.0.record(&request);
        // A watch that opens and immediately ends. The spy exists to observe *headers* — the
        // deadline the client set, or did not set — so the stream's content is beside the
        // point and an empty one cannot hang a test.
        self.0
            .answer::<Self::WatchStream>()
            .await
            .map(|_| tonic::Response::new(Box::pin(tokio_stream::empty()) as Self::WatchStream))
    }

    async fn get(
        &self,
        request: tonic::Request<pb::GetRequest>,
    ) -> Result<tonic::Response<pb::GetResponse>, tonic::Status> {
        self.0.record(&request);
        self.0.answer().await
    }

    async fn list(
        &self,
        request: tonic::Request<pb::ListRequest>,
    ) -> Result<tonic::Response<pb::ListResponse>, tonic::Status> {
        self.0.record(&request);
        self.0.answer().await
    }

    async fn put(
        &self,
        request: tonic::Request<pb::PutRequest>,
    ) -> Result<tonic::Response<pb::MutationResponse>, tonic::Status> {
        self.0.record(&request);
        self.0.answer().await
    }

    async fn delete(
        &self,
        request: tonic::Request<pb::DeleteRequest>,
    ) -> Result<tonic::Response<pb::MutationResponse>, tonic::Status> {
        self.0.record(&request);
        self.0.answer().await
    }
}

/// One running [`DeadlineSpy`].
pub struct SpyServer {
    pub endpoint: String,
    pub spy: Arc<DeadlineSpy>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Drop for SpyServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

/// Serve a [`DeadlineSpy`] on an ephemeral port.
///
/// Spawned here rather than through `serve_client_plane`, because the point of this double is
/// to sit *below* the plane and look at raw gRPC metadata.
pub async fn start_spy() -> SpyServer {
    let spy = Arc::new(DeadlineSpy::default());
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral spy port");
    let endpoint = listener.local_addr().expect("addr").to_string();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    let router = tonic::transport::Server::builder().add_service(
        pb::config_service_server::ConfigServiceServer::new(SpyService(spy.clone())),
    );
    tokio::spawn(config_log::testing::in_current_span(async move {
        let _ = router
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async move {
                    let _ = rx.await;
                },
            )
            .await;
    }));

    SpyServer {
        endpoint,
        spy,
        shutdown: Some(tx),
    }
}

/// Parse a `grpc-timeout` header value (`"400000u"`) into nanoseconds.
pub fn grpc_timeout_nanos(raw: &str) -> u128 {
    let (digits, unit) = raw.split_at(raw.len() - 1);
    let value: u128 = digits
        .parse()
        .unwrap_or_else(|e| panic!("bad grpc-timeout {raw:?}: {e}"));
    match unit {
        "n" => value,
        "u" => value * 1_000,
        "m" => value * 1_000_000,
        "S" => value * 1_000_000_000,
        "M" => value * 60 * 1_000_000_000,
        "H" => value * 3_600 * 1_000_000_000,
        other => panic!("unknown grpc-timeout unit {other:?} in {raw:?}"),
    }
}

/// Every JSONL line this test wrote, scoped to this process's run.
///
/// Anti-flake rule 11: returns the rows rather than a boolean, so a failure message can print
/// what was actually there.
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
