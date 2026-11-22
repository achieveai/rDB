//! `rdb-server`: the loopback gRPC gateway over one `rdb_api::Db` (rDB from C#, slice 1).
//!
//! ```text
//! rdb-server --control E --nodes 1=A,2=B,3=C --cert-dir D --listen 127.0.0.1:P
//!            [--app NAME] [--client-id N] [--log-dir L] [--ready-secs S] [--stop-file F]
//!            [--log-max-mb M] [--log-files N] [--control-tls T]
//! ```
//!
//! Its log is `--log-dir`/`rdb-server.jsonl` (default folder `rdb-server.logs`). It rotates by
//! size (#158): before a write would take it past `--log-max-mb` (default 256) it becomes
//! `rdb-server.jsonl.1`, the older ones shift up, and at most `--log-files` (default 8, at least
//! 2) files are kept, the live one included. `--log-max-mb 0` never rotates. A restart with a
//! smaller `--log-files` removes the files past the count. A second rdb-server on the same
//! `--log-dir` is refused at start (exit 2, `log init`): two would rename and remove each
//! other's files, so give each its own folder.
//!
//! `--control-tls T` dials rEtcd over mutual TLS with `T/ca.pem`, `T/client.pem` and
//! `T/client.key`, the cluster id read from the client certificate (#80 S2, ADR-rdb-0024).
//! Without it the link is plaintext. rdb-server has no start check of its own; its app open
//! (`local = []`) refuses a plaintext link, with nothing written, when the database is
//! declared and any `nodes/` record is not on a loopback IP (owner answer 1 on PR #145).
//!
//! It checks `--listen` first (loopback only, `rdb_server::check_listen`), then opens its `Db`
//! as an app (`local = []`, links to every node in `--nodes` as app `NAME`, default `pilot-b`,
//! under client id `N`, default 100), waits until it is ready, and only then binds `--listen`,
//! so "connection refused" means not ready. Then it prints
//! `rdb-server ready listen=ADDR client=N log=PATH` and serves until Ctrl-C, or until the file
//! `--stop-file` names exists: a graceful stop a script can ask for on Windows, where it cannot
//! send Ctrl-C to another console. Either way it then waits for running calls to write their lines.
//!
//! Exits: 0 stopped by Ctrl-C or the stop file; 2 bad arguments, a non-loopback `--listen`, or setup; 3 the open
//! or a link was refused; 5 not ready in time; 6 the listener could not bind.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use config_core::ConfigStore;
use rdb_api::net::faults::Faults;
use rdb_api::net::wire::Label;
use rdb_api::net::{certs, LinkState, Net, NetConfig, PROTOCOL_VERSION};
use rdb_api::transport::Links;
use rdb_api::{BootId, ClientId, Db, DbConfig, NodeId, Timeouts};
use rdb_server::{check_listen, serve, Gateway};

const USAGE: &str = "usage: rdb-server --control host:port[,host:port...] \
                     --nodes 1=ADDR,2=ADDR,3=ADDR[,...] --cert-dir D --listen 127.0.0.1:PORT \
                     [--app NAME] [--client-id N] [--log-dir L] [--ready-secs S] [--stop-file F] \
                     [--log-max-mb M] [--log-files N] [--control-tls T]\n\
                     log rotation: default 256 MiB x 8 files; --log-max-mb 0 never rotates\n\
                     exits: 0 stopped by Ctrl-C or the stop file; 2 bad arguments, a non-loopback --listen, or \
                     setup; 3 the open or a link was refused; 5 not ready in time; 6 the \
                     listener could not bind";

/// How long the links get to come up before the open (as `pilot_kv`).
const LINK_WAIT: Duration = Duration::from_secs(5);

#[derive(Debug)]
struct Args {
    control: Vec<String>,
    nodes: BTreeMap<u32, String>,
    cert_dir: PathBuf,
    listen: std::net::SocketAddr,
    app: String,
    client_id: u32,
    log_dir: PathBuf,
    /// From `--log-max-mb` and `--log-files`; `None` never rotates (#158).
    rotate: Option<config_log::SizeRotation>,
    ready: Duration,
    stop_file: Option<PathBuf>,
    /// `--control-tls T` (#80 S2): mutual TLS to rEtcd, from `certs::load_control(T)`.
    control_tls: Option<PathBuf>,
}

fn parse(mut args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut control = Vec::new();
    let mut nodes = BTreeMap::new();
    let mut cert_dir = None;
    let mut listen = None;
    let mut app = "pilot-b".to_owned();
    let mut client_id = 100;
    let mut log_dir = None;
    let mut log_max_mb = None;
    let mut log_files = None;
    let mut ready = Duration::from_secs(60);
    let mut stop_file = None;
    let mut control_tls = None;
    while let Some(arg) = args.next() {
        let mut value = |name: &str| args.next().ok_or_else(|| format!("{name} needs a value"));
        match arg.as_str() {
            "--control" => {
                control = value("--control")?
                    .split(',')
                    .map(str::trim)
                    .filter(|e| !e.is_empty())
                    .map(str::to_owned)
                    .collect();
            }
            "--nodes" => {
                for pair in value("--nodes")?
                    .split(',')
                    .filter(|p| !p.trim().is_empty())
                {
                    let (n, addr) = pair
                        .trim()
                        .split_once('=')
                        .ok_or_else(|| format!("--nodes: {pair:?} is not N=host:port"))?;
                    let n = n
                        .parse::<u32>()
                        .map_err(|_| format!("--nodes: {n:?} is not a node number"))?;
                    nodes.insert(n, addr.to_owned());
                }
            }
            "--cert-dir" => cert_dir = Some(PathBuf::from(value("--cert-dir")?)),
            // Checked here, before anything is opened: a refused address costs nothing.
            "--listen" => listen = Some(check_listen(&value("--listen")?)?),
            "--app" => app = value("--app")?,
            "--client-id" => {
                let text = value("--client-id")?;
                client_id = text
                    .parse::<u32>()
                    .map_err(|_| format!("--client-id takes a number, got {text}"))?;
            }
            "--log-dir" => log_dir = Some(PathBuf::from(value("--log-dir")?)),
            "--log-max-mb" => {
                let text = value("--log-max-mb")?;
                log_max_mb = Some(
                    text.parse::<u64>()
                        .map_err(|_| format!("--log-max-mb takes whole megabytes, got {text}"))?,
                );
            }
            "--log-files" => {
                let text = value("--log-files")?;
                log_files = Some(
                    text.parse::<usize>()
                        .map_err(|_| format!("--log-files takes a file count, got {text}"))?,
                );
            }
            "--stop-file" => stop_file = Some(PathBuf::from(value("--stop-file")?)),
            "--control-tls" => control_tls = Some(PathBuf::from(value("--control-tls")?)),
            "--ready-secs" => {
                let text = value("--ready-secs")?;
                let secs = text
                    .parse::<u64>()
                    .map_err(|_| format!("--ready-secs takes whole seconds, got {text}"))?;
                ready = Duration::from_secs(secs);
            }
            "--help" | "-h" => return Err("help".to_owned()),
            other => return Err(format!("unknown argument {other:?}")),
        }
    }
    if control.is_empty() {
        return Err("--control is required".to_owned());
    }
    if nodes.is_empty() {
        return Err("--nodes is required".to_owned());
    }
    let cert_dir = cert_dir.ok_or("--cert-dir is required")?;
    let listen = listen.ok_or("--listen is required")?;
    let log_dir = log_dir.unwrap_or_else(|| PathBuf::from("rdb-server.logs"));
    let rotate = config_log::SizeRotation::from_flags(log_max_mb, log_files)?;
    Ok(Args {
        control,
        nodes,
        cert_dir,
        listen,
        app,
        client_id,
        log_dir,
        rotate,
        ready,
        stop_file,
        control_tls,
    })
}

fn main() -> ExitCode {
    let args = match parse(std::env::args().skip(1)) {
        Ok(args) => args,
        Err(message) => {
            eprintln!("rdb-server: {message}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    let mut log = config_log::LogConfig::for_application("rdb-server");
    log.dir.clone_from(&args.log_dir);
    log.file_name = "rdb-server.jsonl".to_owned();
    log.rotate = args.rotate;
    let guard = match config_log::init(log) {
        Ok(guard) => guard,
        Err(error) => {
            eprintln!("rdb-server: log init: {error}");
            return ExitCode::from(2);
        }
    };
    let code = run(&args);
    drop(guard);
    code
}

fn run(args: &Args) -> ExitCode {
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(error) => {
            eprintln!("rdb-server: runtime: {error}");
            return ExitCode::from(2);
        }
    };
    let code = rt.block_on(run_async(args, &rt));
    // The Db is gone by now; stop whatever tasks are left without waiting on them.
    rt.shutdown_timeout(Duration::from_secs(2));
    code
}

/// The rEtcd client: plaintext, or mutual TLS under `--control-tls` with the cluster id from
/// the client certificate, so a leader hint is followed with its voter's identity checked.
fn connect_control(args: &Args) -> Result<config_client::GrpcClient, String> {
    let Some(dir) = &args.control_tls else {
        return config_client::GrpcClient::connect(
            args.control.clone(),
            config_client::GrpcClientOptions::default(),
        )
        .map_err(|e| format!("--control: {e}"));
    };
    let tls = certs::load_control(dir).map_err(|e| format!("--control-tls: {e}"))?;
    let client = config_client::GrpcClient::connect(
        args.control.clone(),
        config_client::GrpcClientOptions {
            tls: config_client::TlsMode::MutualTls(tls.mtls),
            ..config_client::GrpcClientOptions::default()
        },
    )
    .map_err(|e| format!("--control: {e}"))?
    .with_cluster_id(tls.cluster);
    tracing::info!(cluster = %tls.cluster, principal = %tls.principal, "control_tls");
    Ok(client)
}

async fn run_async(args: &Args, rt: &tokio::runtime::Runtime) -> ExitCode {
    let client = match connect_control(args) {
        Ok(client) => client,
        Err(error) => {
            eprintln!("rdb-server: {error}");
            return ExitCode::from(2);
        }
    };
    let store: Arc<dyn ConfigStore> = Arc::new(client);
    let (links, _net) = match connect(args).await {
        Ok(linked) => linked,
        Err(code) => return code,
    };
    let timeouts = Timeouts::default();
    let config = DbConfig {
        partitions: None,
        ack_floor: true,
        // An app holds no copy and claims no folder: `dir` is not used.
        dir: args.log_dir.clone(),
        hold: Vec::new(),
        timeouts,
        dedup_cap: None,
        dedup_retention: None,
        history_keep: None,
        history_budget_bytes: None,
        local: Vec::new(),
        links: Some(links),
        client: ClientId(args.client_id),
        boot: None,
        storage: rdb_api::StorageOptions::default(),
        failover: None,
        addresses: None,
    };
    let started = Instant::now();
    let db = match Db::open(config, store, rt.handle().clone()).await {
        Ok(db) => Arc::new(db),
        Err(error) => {
            tracing::error!(%error, "rdb_server_open_refused");
            println!("open refused: {error}");
            return ExitCode::from(3);
        }
    };
    let ready = {
        let db = Arc::clone(&db);
        let within = args.ready;
        tokio::task::spawn_blocking(move || db.wait_ready(within)).await
    };
    match ready {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::error!(%error, "rdb_server_not_ready");
            println!("not ready: {error}");
            return ExitCode::from(5);
        }
        Err(join) => {
            eprintln!("rdb-server: ready wait failed: {join}");
            return ExitCode::from(2);
        }
    }
    let listener = match config_gossip::ports::bind_tcp("rdb_server", args.listen).await {
        Ok(listener) => listener,
        Err(error) => {
            tracing::error!(listen = %args.listen, %error, "rdb_server_bind_failed");
            println!("listen {} failed: {error}", args.listen);
            return ExitCode::from(6);
        }
    };
    let bound = listener.local_addr().unwrap_or(args.listen);
    let log_path = args.log_dir.join("rdb-server.jsonl");
    tracing::info!(
        listen = %bound,
        client = args.client_id,
        ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        "rdb_server_ready"
    );
    println!(
        "rdb-server ready listen={bound} client={} log={}",
        args.client_id,
        one_separator(&log_path)
    );
    let gateway = Gateway::new(Arc::clone(&db), args.client_id, timeouts.put, timeouts.read);
    let stop_file = args.stop_file.clone();
    let shutdown = async move {
        let asked = async {
            let Some(path) = stop_file else {
                return std::future::pending::<()>().await;
            };
            while !path.exists() {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        };
        tokio::select! {
            // A failed signal hook ends the serve rather than serving with no way to stop.
            signal = tokio::signal::ctrl_c() => {
                if let Err(error) = signal {
                    tracing::error!(%error, "rdb_server_signal_failed");
                }
            }
            () = asked => tracing::info!("rdb_server_stop_file_seen"),
        }
        tracing::info!("rdb_server_stopping");
    };
    let served = serve(gateway, listener, shutdown).await;
    // The server and its calls are done, so this is the last Arc: dropping it shuts the Db down.
    match Arc::try_unwrap(db) {
        Ok(db) => drop(db),
        Err(db) => {
            tracing::error!(
                holders = Arc::strong_count(&db),
                "rdb_server_db_still_shared_at_stop"
            );
            drop(db);
        }
    }
    match served {
        Ok(()) => {
            println!("rdb-server stopped");
            ExitCode::SUCCESS
        }
        Err(error) => {
            tracing::error!(%error, "rdb_server_serve_failed");
            println!("serve failed: {error}");
            ExitCode::from(2)
        }
    }
}

/// Links that dial every `--nodes` node as app `--app`. Exit 2 when the certificates cannot be
/// read or the links cannot start, 3 when a node refuses the handshake. A node that is down is
/// said and redialled: the open decides whether the nodes that are up can serve.
async fn connect(args: &Args) -> Result<(Arc<Links>, Arc<Net>), ExitCode> {
    let label = Label::App(args.app.clone());
    let tls = certs::load(&args.cert_dir, &label).map_err(|error| {
        eprintln!("rdb-server: --cert-dir: {error}");
        ExitCode::from(2)
    })?;
    let dial: BTreeMap<NodeId, String> = args
        .nodes
        .iter()
        .map(|(n, addr)| (NodeId(*n), addr.clone()))
        .collect();
    let peers: Vec<NodeId> = dial.keys().copied().collect();
    let links = Links::new();
    let net = Net::start(
        NetConfig {
            label,
            boot: BootId(0),
            tls,
            peers: peers.iter().copied().collect(),
            dial,
            protocol_version: PROTOCOL_VERSION,
            faults: Faults::new(),
            // Required, but unused here: the list filters the Move and Rebuild calls that a node
            // of this process receives, and the gateway hosts no node (`local` is empty). It
            // sends no Move or Rebuild either: its wire has none. Whether app `--app` may is up
            // to each node's own `--admin-apps`.
            admin_apps: BTreeSet::new(),
        },
        &links,
        None,
    )
    .map_err(|error| {
        eprintln!("rdb-server: links: {error}");
        ExitCode::from(2)
    })?;
    let _ = net.wait_up(&peers, args.ready.min(LINK_WAIT)).await;
    let states = net.states();
    let refused: Vec<String> = states
        .iter()
        .filter_map(|(node, state)| match state {
            LinkState::Refused(why) => Some(format!("node {}: refused: {why}", node.0)),
            _ => None,
        })
        .collect();
    if !refused.is_empty() {
        let refused = refused.join("; ");
        tracing::error!(%refused, "rdb_server_links_refused");
        println!("links refused: {refused}");
        return Err(ExitCode::from(3));
    }
    for (node, state) in &states {
        if let LinkState::Waiting(why) = state {
            println!(
                "link node={} down: {}",
                node.0,
                why.as_deref().unwrap_or("not reached yet")
            );
        }
    }
    let up: Vec<String> = states
        .iter()
        .filter(|(_, state)| matches!(state, LinkState::Up(_)))
        .map(|(node, _)| node.0.to_string())
        .collect();
    tracing::info!(up = ?up, client = args.client_id, "rdb_server_links_up");
    println!("links up nodes={}", up.join(","));
    Ok((links, net))
}

/// `path` with one separator throughout. On Windows `--log-dir C:/x` joined with a file name
/// prints `C:/x\rdb-server.jsonl`; `/` is accepted everywhere on Windows, so it is the one used.
fn one_separator(path: &std::path::Path) -> String {
    let shown = path.display().to_string();
    if cfg!(windows) {
        shown.replace('\\', "/")
    } else {
        shown
    }
}

#[cfg(test)]
mod tests {

    /// A 3-voter mutual-TLS rEtcd of cluster `cluster` granting only the `rdb` client, a
    /// `--control-tls` folder of its fixture's client certificate, and its client endpoints
    /// followers first, so a client that cannot follow a leader hint fails (#80 S2 review r1).
    fn tls_control(
        rt: &tokio::runtime::Runtime,
        cluster: config_core::ClusterId,
        seed: u64,
    ) -> (
        config_testkit::Cluster,
        Vec<String>,
        std::path::PathBuf,
        Box<dyn std::any::Any>,
    ) {
        let voters = rt.block_on(
            config_testkit::Cluster::builder()
                .nodes(3)
                .cluster_id(cluster)
                .storage(config_testkit::cluster::StorageKind::Ephemeral)
                .mutual_tls(seed)
                .authz(config_testkit::cluster::AuthzKind::Static(
                    "[[grant]]\nprincipal = \"rdb\"\nprefix = \"\"\naccess = [\"read\", \"write\"]\n"
                        .to_owned(),
                ))
                .start(),
        );
        let leader = rt.block_on(voters.leader());
        let dir = config_testkit::fs::temp_dir();
        let pair = voters
            .fixture()
            .issue(config_testkit::CertProfile::client("rdb"));
        for (name, text) in [
            ("ca.pem", &pair.ca_pem),
            ("client.pem", &pair.cert_pem),
            ("client.key", &pair.key_pem),
        ] {
            std::fs::write(dir.path().join(name), text).expect("a control file");
        }
        let mut endpoints: Vec<String> = voters
            .followers()
            .into_iter()
            .map(|id| voters.client_endpoint(id))
            .collect();
        endpoints.push(voters.client_endpoint(leader));
        let path = dir.path().to_path_buf();
        (voters, endpoints, path, Box::new(dir))
    }

    /// A create-only put of `key` through `store`, and its exact read back (#80 S2 review r1).
    fn sentinel(
        rt: &tokio::runtime::Runtime,
        store: &dyn config_core::ConfigStore,
        key: &'static [u8],
    ) {
        let key = bytes::Bytes::from_static(key);
        let value = bytes::Bytes::from_static(b"over tls from a follower");
        rt.block_on(store.put(config_core::PutRequest {
            key: key.clone(),
            value: value.clone(),
            expected_mod_revision: Some(0),
            ..config_core::PutRequest::default()
        }))
        .expect("a create-only put over TLS");
        let read = rt
            .block_on(store.get(config_core::GetRequest { key: key.clone() }))
            .expect("the read back");
        assert_eq!(
            read.record.map(|record| (record.key, record.value)),
            Some((key, value))
        );
    }

    /// #80 S2 review r1: rdb-server's `connect_control` under `--control-tls` reaches a
    /// mutual-TLS rEtcd whose cluster is not the default, starting at a follower: a
    /// create-only put and its exact read back succeed. Cluster row (~3 s): a 3-voter TLS
    /// rEtcd, the only way to prove the constructor's TLS and cluster wiring.
    /// Cut 1: `connect_control` builds a plaintext client under a TLS folder: it reports
    /// `Insecure`, red at the transport-security assertion. Cut 2: it drops
    /// `.with_cluster_id`: the put fails `NotLeader` with the hint unfollowed, red (both
    /// observed 2026-11-19).
    #[config_log::retcd_test]
    fn s2_connect_control_puts_and_reads_back_over_tls_from_a_follower() {
        use config_core::ConfigStore as _;
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let cluster = config_core::ClusterId::from_bytes([0x83; 16]);
        let (voters, endpoints, path, _dir) = tls_control(&rt, cluster, 813);
        let mut argv = vec![
            "--control".to_owned(),
            endpoints.join(","),
            "--nodes".to_owned(),
            "1=127.0.0.1:1".to_owned(),
            "--cert-dir".to_owned(),
            "c".to_owned(),
            "--listen".to_owned(),
            "127.0.0.1:1".to_owned(),
            "--control-tls".to_owned(),
        ];
        argv.push(path.display().to_string());
        let args = super::parse(argv.into_iter()).expect("parses");
        let client = {
            let _entered = rt.enter();
            super::connect_control(&args).expect("the control client")
        };
        assert_eq!(
            client.capabilities().transport_security,
            config_core::TransportSecurity::MutualTls
        );
        sentinel(&rt, &client, b"s2-sentinel/rdb-server-connect_control");
        drop(client);
        rt.block_on(voters.shutdown());
    }
}
