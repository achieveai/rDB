//! Startup order, the ready line, and graceful shutdown (ADR-0018 §3, §4, m3-architecture §4).
//!
//! Everything the daemon does is library behaviour composed here: an embedder that wires
//! `ConfigNode` and `config-grpc` the same way gets the same semantics, which is why this
//! module creates no types of its own beyond the ready line.
//!
//! # Order, and why each step is where it is
//!
//! 1. **Open the store.** `RocksStore::open` is where an identity mismatch, a locked
//!    directory or a missing column family is detected. It happens before any socket exists,
//!    so a directory bound to another identity exits 2 and an unopenable one (locked, missing
//!    column family) exits 3, without either ever having been reachable (ADR-0011, ADR-0018).
//! 2. **Verify the manifest** (with `--form`), except the endpoint check. A forged or expired
//!    manifest must not reach a listener either.
//! 3. **Load the policy.** Missing or invalid without `--dev-allow-all` starts the node
//!    *unready* — it is not a startup failure, because a node that cannot authorize still
//!    has to replicate (ADR-0018 §6).
//! 4. **Bind** peer, client, gossip and health. Port `0` is resolved here, and the resolved
//!    addresses are what the node advertises and what the ready line reports. Binding is not
//!    serving: the listener exists, nothing answers on it yet.
//! 5. **Check the manifest's endpoints** (with `--form`) against the addresses just bound.
//! 6. **Start** gossip, then the node, and — with `--form` — **form**, which is also where the
//!    already-formed refusal happens.
//! 7. **Serve** both gRPC planes and health. Nothing before this point can accept a client or
//!    peer RPC, so every refusal above exits 2 or 3 without a single connection having been
//!    answered (ADR-0018 §5).
//! 8. **Announce** — exactly one JSON line on stdout, and nothing else on stdout ever.
//! 9. **Wait** for Ctrl-C or the shutdown file, then drain in the ADR-0018 §4 order.
//!
//! Steps 5 and 6 sit *between* binding and serving on purpose. An earlier revision served both
//! planes before the endpoint and already-formed checks ran, so a node destined to exit 2 spent
//! the interval accepting connections it was about to abandon (E2E-18).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use config_core::{
    AllowAll, Authorizer, Capabilities, ClusterIdentity, Dedup, Durability,
    GossipObservationSource, Liveness, NoGossip, ObservedPeerHint, Pagination, Principal,
    StaticAllowlist, TransportSecurity, WatchResumption,
};
use config_engine::{
    AuthzKind, ConfigNode, FormationError, FormationPlan, NodeConfig, StorageHandle,
};
use config_gossip::{GossipConfig, GossipNode};
use config_grpc::{
    serve_client_plane, serve_peer_plane, ClientBackend, GrpcPeerTransport, MtlsConfig,
    PeerIdentity, ServerHandle, TlsMode,
};
use config_storage::{NoFaults, RocksOptions, RocksStore, StorageOpenError};
use serde::Serialize;
use tokio::net::TcpListener;

use crate::cli::Cli;
use crate::config::{ServerConfig, TlsModeName};
use crate::{health, manifest};

/// How often the shutdown file is looked for (ADR-0018 §4).
///
/// The one periodic timer in the daemon. Windows has no `SIGTERM` and no portable way to
/// watch a single path cheaply, so this is a poll; 100 ms is far below any test's deadline
/// and far above any cost worth measuring.
const SHUTDOWN_POLL: Duration = Duration::from_millis(100);

/// What the process exits with (ADR-0018 §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitCode {
    /// Clean shutdown.
    Ok = 0,
    /// Configuration, identity, TLS-gate or manifest rejection.
    Rejected = 2,
    /// Fatal storage failure.
    Storage = 3,
}

/// Anything that stops the daemon before or during startup, already carrying its exit code.
#[derive(Debug)]
pub struct Fatal {
    /// The exit code to use.
    pub code: ExitCode,
    /// A stable `msg=` value for the log line, so a test can assert on the reason without
    /// matching prose (`identity_mismatch`, `manifest_rejected`, …).
    pub reason: &'static str,
    /// The operator-facing detail. Never contains a key, a value, or key material.
    pub detail: String,
}

impl Fatal {
    pub(crate) fn rejected(reason: &'static str, detail: impl std::fmt::Display) -> Self {
        Self {
            code: ExitCode::Rejected,
            reason,
            detail: detail.to_string(),
        }
    }

    pub(crate) fn storage(reason: &'static str, detail: impl std::fmt::Display) -> Self {
        Self {
            code: ExitCode::Storage,
            reason,
            detail: detail.to_string(),
        }
    }
}

/// The one line the daemon writes to stdout (ADR-0018 §3, TA-20.2).
///
/// Field order is the serialization order and is part of the contract only in the sense that
/// a parser must not depend on it; `wait_ready` parses JSON, not a prefix.
#[derive(Debug, Clone, Serialize)]
pub struct ReadyLine {
    /// Always `true`. Present so a reader can tell this line from any future stdout protocol.
    pub ready: bool,
    /// This node's id.
    pub node_id: u64,
    /// Bound peer-plane address.
    pub peer: String,
    /// Bound client-plane address.
    pub client: String,
    /// Bound gossip address, when gossip is configured.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gossip: Option<String>,
    /// Bound health address, when `--health-listen` was given.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub health: Option<String>,
}

/// Compute the capability report from configuration alone, opening nothing (`--capabilities`).
///
/// Deliberately duplicated from `ConfigNode::capabilities` rather than obtained from a started
/// node: the whole point of the flag is to answer without a store, a listener, or a lock on
/// the data directory. E2E-02 asserts the two agree, which is what keeps the duplication
/// honest.
pub fn capabilities_without_opening(cfg: &ServerConfig, cli: &Cli) -> Capabilities {
    Capabilities {
        durability: if cli.unsafe_no_sync {
            Durability::PersistentUnverified
        } else {
            Durability::Persistent
        },
        watch_resumption: WatchResumption::Unsupported,
        authz: authz_kind(cfg, cli).into(),
        transport_security: match cfg.tls_mode {
            TlsModeName::Mutual => TransportSecurity::MutualTls,
            TlsModeName::Insecure => TransportSecurity::Insecure,
        },
        pagination: Pagination::Unsupported,
        dedup: Dedup::Unsupported,
    }
}

/// Which authorization model this run will enforce, without reading the policy file.
///
/// `--dev-allow-all` wins outright; otherwise a configured policy path means
/// `StaticAllowlist`. Whether that file actually loads is decided at startup and can still
/// downgrade the *running* node to `Missing`/`Invalid` — `--capabilities` cannot promise that
/// a file it has not read will parse, and says the strictest thing instead.
fn authz_kind(cfg: &ServerConfig, cli: &Cli) -> AuthzKind {
    if cli.dev_allow_all {
        AuthzKind::Development
    } else if cfg.policy_path.is_some() {
        AuthzKind::StaticAllowlist
    } else {
        AuthzKind::Missing
    }
}

/// Serves each authenticated principal a `DirectClient` over the local node.
///
/// The client plane holds no state of its own; it binds the transport-derived principal to
/// this node and nothing else (ADR-0012: a request field can never influence identity).
struct NodeBackend {
    node: ConfigNode,
}

impl ClientBackend for NodeBackend {
    fn store_for(&self, principal: Principal) -> Arc<dyn config_core::ConfigStore> {
        Arc::new(self.node.direct_client(principal))
    }

    /// Forward the plane's authentication refusals to the node's counter (M3-81).
    ///
    /// The client plane is the only place that sees a certificate and the node is the only
    /// place that keeps counters, so without this the health endpoint reports zero rejections
    /// no matter how many certificates the listener turned away.
    fn record_authn_rejection(&self) {
        self.node.record_authn_rejection();
    }
}

/// Everything a running daemon holds, in the order it must be torn down.
///
/// The two plane handles are `Option` because the value exists before either plane is served:
/// formation runs between binding and serving (see the module docs), and a formation refusal
/// has to tear down the node, the store and gossip without ever having had a server to drain.
struct Running {
    node: ConfigNode,
    storage: StorageHandle,
    peer_server: Option<ServerHandle>,
    client_server: Option<ServerHandle>,
    gossip: Option<Arc<GossipNode>>,
    health_shutdown: Option<Arc<tokio::sync::Notify>>,
    health_task: Option<tokio::task::JoinHandle<()>>,
}

/// Start, serve, and shut down. Returns the process exit code.
pub async fn run(cfg: ServerConfig, cli: Cli) -> Result<ExitCode, Fatal> {
    let identity = cfg.identity;

    // ---- 1. store -------------------------------------------------------------------
    let storage = open_store(&cfg, &cli)?;

    // ---- 2. manifest (everything that does not need a bound port) ---------------------
    let verified = if cli.form {
        let files = cfg.manifest.as_ref().ok_or_else(|| {
            Fatal::rejected(
                "manifest_rejected",
                "--form requires a [manifest] section naming path, sig and signing_key_pub",
            )
        })?;
        Some(
            manifest::verify_document(files, &identity)
                .map_err(|e| Fatal::rejected("manifest_rejected", e))?,
        )
    } else {
        None
    };

    // ---- 3. policy -------------------------------------------------------------------
    let policy = load_authorizer(&cfg, &cli);

    // ---- 4. bind ---------------------------------------------------------------------
    let tls = tls_mode(&cfg)?;
    let peer_listener = bind("peer", cfg.peer_listen).await?;
    let client_listener = bind("client", cfg.client_listen).await?;
    let peer_endpoint = local_addr(&peer_listener)?;
    let client_endpoint = local_addr(&client_listener)?;
    let health_listener = match cfg.health_listen {
        Some(addr) => Some(bind("health", addr).await?),
        None => None,
    };
    let health_endpoint = health_listener.as_ref().map(local_addr).transpose()?;

    // ---- 5. endpoint check ------------------------------------------------------------
    // The one manifest check that needs a bound port. It runs here, before a single byte is
    // served, so a manifest that publishes an address this node does not serve is refused
    // without the node ever having answered anyone (ADR-0018 §5).
    if let Some(verified) = &verified {
        manifest::check_endpoints(verified, &identity, &peer_endpoint, &client_endpoint)
            .map_err(|e| Fatal::rejected("manifest_rejected", e))?;
    }

    // ---- 6. start and form ------------------------------------------------------------
    let (gossip_node, gossip_endpoint, gossip_source) =
        start_gossip(&cfg, &peer_endpoint, &client_endpoint).await?;

    let mut node_cfg = NodeConfig::new(identity, peer_endpoint.clone());
    node_cfg.client_endpoint = Some(client_endpoint.clone());
    node_cfg.raft = cfg.raft;
    node_cfg.authz_kind = policy.kind;
    node_cfg.transport_security = tls.transport_security();
    // The engine cannot count grants through `Arc<dyn Authorizer>` and never sees the file, so
    // both facts the health endpoint publishes have to be handed to it here (M3-42).
    node_cfg = node_cfg.with_policy_grants(policy.grants);
    if let Some(document) = &policy.document {
        node_cfg = node_cfg.with_policy_document(document);
    }

    // Read out before `node_cfg` is moved into the node: both planes and the peer transport
    // size their codecs from the same caps this node enforces (ADR-0010 fix-round note).
    let limits = node_cfg.limits;

    let transport = GrpcPeerTransport::new(tls.clone(), config_engine::NetFault::new(), limits);
    let node = ConfigNode::start(
        node_cfg,
        storage.clone(),
        transport as Arc<dyn config_engine::PeerTransport>,
        gossip_source,
        policy.authorizer,
    )
    .await
    .map_err(node_start_fatal)?;

    let mut running = Running {
        node,
        storage,
        peer_server: None,
        client_server: None,
        gossip: gossip_node,
        health_shutdown: None,
        health_task: None,
    };

    if let Some(verified) = verified {
        if let Err(fatal) = form(&running.node, &verified, &identity).await {
            shutdown(running).await;
            return Err(fatal);
        }
    }

    // ---- 7. serve ---------------------------------------------------------------------
    let peer_server = match serve_peer_plane(
        running.node.peer_handler(),
        peer_listener,
        tls.clone(),
        PeerIdentity {
            cluster_id: identity.cluster_id,
            recovery_epoch: identity.recovery_epoch,
            node_id: identity.node_id,
        },
        limits,
    ) {
        Ok(handle) => handle,
        Err(e) => {
            shutdown(running).await;
            return Err(Fatal::rejected("peer_plane_failed", e));
        }
    };
    running.peer_server = Some(peer_server);

    let client_server = match serve_client_plane(
        Arc::new(NodeBackend {
            node: running.node.clone(),
        }) as Arc<dyn ClientBackend>,
        client_listener,
        tls,
        identity.cluster_id,
        limits,
    ) {
        Ok(handle) => handle,
        Err(e) => {
            shutdown(running).await;
            return Err(Fatal::rejected("client_plane_failed", e));
        }
    };
    running.client_server = Some(client_server);

    if let Some(listener) = health_listener {
        let notify = Arc::new(tokio::sync::Notify::new());
        let task = tokio::spawn(config_log::testing::in_current_span(health::serve(
            listener,
            running.node.clone(),
            Arc::clone(&notify),
        )));
        running.health_shutdown = Some(notify);
        running.health_task = Some(task);
    }

    // ---- 8. announce -----------------------------------------------------------------
    let ready = ReadyLine {
        ready: true,
        node_id: identity.node_id.0,
        peer: peer_endpoint.clone(),
        client: client_endpoint.clone(),
        gossip: gossip_endpoint.clone(),
        health: health_endpoint.clone(),
    };
    announce(&ready);
    tracing::info!(
        peer = %peer_endpoint,
        client = %client_endpoint,
        gossip = gossip_endpoint.as_deref().unwrap_or("-"),
        health = health_endpoint.as_deref().unwrap_or("-"),
        authz_kind = policy.kind.as_str(),
        ready = running.node.is_ready(),
        "serving"
    );

    // ---- 9. wait and drain -----------------------------------------------------------
    let trigger = wait_for_shutdown(cli.shutdown_file.as_deref()).await;
    tracing::info!(trigger, "shutdown requested");
    // `stop()` before the planes would leave a listener accepting calls into a stopped node,
    // so the servers drain first (ADR-0018 §4).
    shutdown(running).await;
    Ok(ExitCode::Ok)
}

/// Write the one stdout line the process is allowed to write.
fn announce(ready: &ReadyLine) {
    let line = serde_json::to_string(ready).expect("the ready line always serializes");
    println!("{line}");
    use std::io::Write;
    // Flushed explicitly: a parent process blocks on this line, and Rust's stdout is line
    // buffered only when it is a terminal — under a pipe it is block buffered, which would
    // make `wait_ready` hang until the child exits.
    let _ = std::io::stdout().flush();
}

fn open_store(cfg: &ServerConfig, cli: &Cli) -> Result<StorageHandle, Fatal> {
    let options = RocksOptions {
        sync_writes: !cli.unsafe_no_sync,
        create_if_missing: true,
    };
    if cli.unsafe_no_sync {
        tracing::warn!(
            reason = "sync_disabled",
            detail = "--unsafe-no-sync disables fsync; this node reports PersistentUnverified",
            "durability_unverified"
        );
    }
    let span = tracing::info_span!("store", node_id = cfg.identity.node_id.0);
    match RocksStore::open_with(
        &cfg.data_dir,
        cfg.identity,
        config_core::Limits::DEFAULT,
        Arc::new(NoFaults),
        span,
        options,
    ) {
        Ok(store) => Ok(store.into()),
        Err(e @ StorageOpenError::IdentityMismatch { .. }) => {
            Err(Fatal::rejected("identity_mismatch", e))
        }
        Err(e) => Err(Fatal::storage("storage_open_failed", e)),
    }
}

/// Load the allowlist policy, or record honestly why there is none.
///
/// Never fatal: `Missing` and `Invalid` are *failed* authorization, and a node with failed
/// authorization still replicates while denying every client call (OQ-19).
fn load_authorizer(cfg: &ServerConfig, cli: &Cli) -> LoadedPolicy {
    if cli.dev_allow_all {
        tracing::warn!(
            "authorization is --dev-allow-all: every request is permitted (development only)"
        );
        return LoadedPolicy::without_document(Arc::new(AllowAll), AuthzKind::Development);
    }
    let Some(path) = cfg.policy_path.as_deref() else {
        tracing::warn!(
            reason = "no_policy_configured",
            detail = "no [authz] policy and no --dev-allow-all; this node starts unready and \
                      denies every client call",
            "authz_unavailable"
        );
        return LoadedPolicy::without_document(deny_everything(), AuthzKind::Missing);
    };
    let text = match crate::config::read_policy(path) {
        Ok(text) => text,
        Err(e) => {
            tracing::error!(error = %e, reason = "unreadable", "authz_unavailable");
            return LoadedPolicy::without_document(deny_everything(), AuthzKind::Missing);
        }
    };
    match crate::config::parse_policy(path, &text) {
        Ok(policy) => {
            let summary = crate::config::grant_summary(&policy);
            let grants = policy.grants.len();
            tracing::info!(
                grants,
                principals = summary.len(),
                "allowlist policy loaded"
            );
            LoadedPolicy {
                authorizer: Arc::new(StaticAllowlist::new(policy)),
                kind: AuthzKind::StaticAllowlist,
                grants: grants as u64,
                document: Some(text.into_bytes()),
            }
        }
        Err(e) => {
            tracing::error!(error = %e, reason = "invalid", "authz_unavailable");
            LoadedPolicy {
                authorizer: deny_everything(),
                kind: AuthzKind::Invalid,
                grants: 0,
                // The bytes still exist and are still worth publishing a digest of: a fleet
                // holding one identical broken file is a different incident from a fleet
                // holding several different ones.
                document: Some(text.into_bytes()),
            }
        }
    }
}

/// What [`load_authorizer`] produced: the authorizer itself, plus the facts the node's health
/// payload reports about the policy in force (M3-42).
struct LoadedPolicy {
    /// Consulted on every client request.
    authorizer: Arc<dyn Authorizer>,
    /// The model, as `capabilities().authz` reports it.
    kind: AuthzKind,
    /// Grant rules held; `0` for allow-all and for a policy that did not load.
    grants: u64,
    /// The exact document bytes, when a document was read. `None` means there was nothing to
    /// hash, not that the hash was dropped.
    document: Option<Vec<u8>>,
}

impl LoadedPolicy {
    /// The cases with no document behind them: allow-all, no policy configured, and a policy
    /// file that could not be read at all.
    fn without_document(authorizer: Arc<dyn Authorizer>, kind: AuthzKind) -> Self {
        Self {
            authorizer,
            kind,
            grants: 0,
            document: None,
        }
    }
}

/// An allowlist with no grants: the deny-everything end of the scale, which is what
/// "authorization failed to load" must behave like.
fn deny_everything() -> Arc<dyn Authorizer> {
    Arc::new(StaticAllowlist::new(config_core::AllowlistPolicy::default()))
}

/// Which transport this run serves, from the validated document.
///
/// Deliberately exhaustive over [`TlsModeName`], with no catch-all arm: a wildcard here would
/// silently map a future mode the daemon does not understand onto plaintext, which is the one
/// mistake this function must never be able to make. A new variant is a compile error instead
/// (critic A2). `(Mutual, None)` is likewise a refusal rather than a downgrade — configuration
/// validation guarantees mutual mode carries material, so reaching it means the two have
/// drifted apart, and answering "then serve plaintext" would be the worst possible reading.
fn tls_mode(cfg: &ServerConfig) -> Result<TlsMode, Fatal> {
    match (cfg.tls_mode, &cfg.tls_material) {
        (TlsModeName::Mutual, Some(material)) => Ok(TlsMode::MutualTls(
            MtlsConfig::new(
                material.ca_pem.clone(),
                material.cert_pem.clone(),
                material.key_pem.clone(),
            )
            // `tls.allow_common_name_principals`: shut unless the document opened it;
            // `config::validate` has already logged `common_name_principals_enabled` if so.
            .with_common_name_principals(material.allow_common_name_principals),
        )),
        (TlsModeName::Mutual, None) => Err(Fatal::rejected(
            "invalid_config",
            "tls.mode = \"mutual\" without ca/cert/key material; refusing to start rather than \
             falling back to plaintext",
        )),
        (TlsModeName::Insecure, _) => {
            // Reachable only behind `--allow-insecure-dev` (config::validate refuses the
            // document otherwise), so this line is exactly "the insecure gate was opened"
            // (M3-44, ADR-0018 §2).
            tracing::warn!(
                detail = "tls.mode = \"insecure\" was accepted via --allow-insecure-dev; both \
                          planes serve plaintext and no peer or client identity is \
                          authenticated (ADR-0010)",
                "insecure_transport_enabled"
            );
            Ok(TlsMode::Insecure)
        }
    }
}

/// Map a [`config_engine::EngineError`] from `ConfigNode::start` onto an exit code.
///
/// Before this existed every failure became exit 3 "storage", so a node that refused to start
/// for a reason the disk had nothing to do with told an operator to go and look at the disk
/// (critic A6). The split is by variant, and ADR-0018 §5 records the one residual imprecision:
/// `EngineError::Raft` covers both "OpenRaft rejected the `[raft]` timers" and "OpenRaft could
/// not replay the log", and the engine does not distinguish them, so both exit 2.
fn node_start_fatal(error: config_engine::EngineError) -> Fatal {
    use config_engine::EngineError;
    match &error {
        EngineError::Storage(_) => Fatal::storage("storage_open_failed", error),
        EngineError::Raft(_) => Fatal::rejected("node_start_failed", error),
        EngineError::NoRuntime => Fatal::rejected("runtime_unavailable", error),
    }
}

async fn bind(what: &'static str, addr: std::net::SocketAddr) -> Result<TcpListener, Fatal> {
    TcpListener::bind(addr)
        .await
        .map_err(|e| Fatal::rejected("bind_failed", format!("{what} listener on {addr}: {e}")))
}

fn local_addr(listener: &TcpListener) -> Result<String, Fatal> {
    listener
        .local_addr()
        .map(|a| a.to_string())
        .map_err(|e| Fatal::rejected("bind_failed", format!("bound listener has no address: {e}")))
}

/// Start advisory gossip, if the configuration asks for it.
///
/// A gossip failure is an alert, not an outage (ADR-0003): if the node cannot start gossip it
/// keeps running with no observation source at all rather than refusing to serve.
async fn start_gossip(
    cfg: &ServerConfig,
    peer_endpoint: &str,
    client_endpoint: &str,
) -> Result<
    (
        Option<Arc<GossipNode>>,
        Option<String>,
        Arc<dyn GossipObservationSource>,
    ),
    Fatal,
> {
    let Some(bind_addr) = cfg.gossip_listen else {
        return Ok((None, None, Arc::new(NoGossip)));
    };
    let mut gcfg = GossipConfig::new(cfg.identity.cluster_id, cfg.identity.node_id, bind_addr);
    gcfg.seeds = cfg.gossip_seeds.clone();
    gcfg.secret_key = cfg.gossip_secret_key;
    let hint = ObservedPeerHint {
        cluster_id: cfg.identity.cluster_id,
        recovery_epoch: cfg.identity.recovery_epoch,
        node_id: cfg.identity.node_id,
        peer_endpoint: peer_endpoint.to_string(),
        client_endpoint: Some(client_endpoint.to_string()),
        software_version: env!("CARGO_PKG_VERSION").to_string(),
        protocol_version: 1,
        zone: None,
        liveness: Liveness::Alive,
    };
    match GossipNode::start(gcfg, hint).await {
        Ok(node) => {
            let addr = node.advertise_addr().to_string();
            let node = Arc::new(node);
            let source = Arc::clone(&node) as Arc<dyn GossipObservationSource>;
            Ok((Some(node), Some(addr), source))
        }
        Err(e) => {
            tracing::warn!(error = %e, "gossip failed to start; continuing without it");
            Ok((None, None, Arc::new(NoGossip)))
        }
    }
}

/// Create the cluster from the verified manifest.
///
/// The endpoint check has already run (step 5); what is left here is `form_cluster` itself,
/// whose refusals — above all "already formed" — are the last thing that can turn a start into
/// an exit 2, and therefore the last thing that must happen before either plane serves.
async fn form(
    node: &ConfigNode,
    verified: &manifest::VerifiedManifest,
    identity: &ClusterIdentity,
) -> Result<(), Fatal> {
    let plan = FormationPlan::with_client_endpoints(
        identity,
        verified
            .voters
            .iter()
            .map(|(id, peer, client)| (*id, peer.clone(), client.clone())),
    );
    tracing::info!(voters = verified.voters.len(), "formation_started");
    match node.form_cluster(plan).await {
        Ok(()) => {
            tracing::info!("formation_succeeded");
            Ok(())
        }
        Err(e @ (FormationError::AlreadyFormed | FormationError::StoreNotFresh)) => {
            Err(Fatal::rejected(
                "already_formed",
                format!("{e}; --form is a one-time action"),
            ))
        }
        Err(e) => Err(Fatal::rejected("formation_failed", e)),
    }
}

/// Block until Ctrl-C or the shutdown file appears, and say which it was (OQ-17).
async fn wait_for_shutdown(shutdown_file: Option<&Path>) -> &'static str {
    let file = shutdown_file.map(Path::to_path_buf);
    tokio::select! {
        _ = tokio::signal::ctrl_c() => "ctrl_c",
        () = wait_for_file(file) => "shutdown_file",
    }
}

/// Resolve when `path` exists. With no path, never resolves.
async fn wait_for_file(path: Option<PathBuf>) {
    let Some(path) = path else {
        return std::future::pending().await;
    };
    let mut ticker = tokio::time::interval(SHUTDOWN_POLL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        if path.exists() {
            return;
        }
    }
}

/// Drain in the ADR-0018 §4 order and log the final line.
async fn shutdown(running: Running) {
    let Running {
        node,
        storage,
        peer_server,
        client_server,
        gossip,
        health_shutdown,
        health_task,
    } = running;

    if let Some(notify) = health_shutdown {
        notify.notify_waiters();
    }
    if let Some(task) = health_task {
        task.abort();
        let _ = task.await;
    }
    // A plane that was never served has nothing to drain — the refusal happened between
    // binding and serving, and the listener is dropped with its handle.
    if let Some(client_server) = client_server {
        if let Err(e) = client_server.shutdown().await {
            tracing::warn!(plane = "client", error = %e, "plane did not drain cleanly");
        }
    }
    if let Some(peer_server) = peer_server {
        if let Err(e) = peer_server.shutdown().await {
            tracing::warn!(plane = "peer", error = %e, "plane did not drain cleanly");
        }
    }
    if let Err(e) = node.stop().await {
        tracing::warn!(error = %e, "node did not stop cleanly");
    }
    if let Some(gossip) = gossip {
        gossip.shutdown().await;
    }
    // Dropping the handle closes RocksDB; nothing may reopen the directory until it has.
    drop(storage);
    tracing::info!("drained");
    // `shutdown_complete` is logged by `main` after the runtime itself has stopped: OpenRaft's
    // tick loop logs as it is cancelled, and a "final line" that another task can still write
    // after is not a final line (ADR-0018 §4, E2E-09).
}
