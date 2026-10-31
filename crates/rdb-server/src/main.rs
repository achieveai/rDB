//! `rdb-server`: the loopback gRPC gateway over one `rdb_api::Db` (rDB from C#, slice 1).
//!
//! ```text
//! rdb-server --control E --nodes 1=A,2=B,3=C --cert-dir D --listen 127.0.0.1:P
//!            [--app NAME] [--client-id N] [--log-dir L] [--ready-secs S]
//! ```
//!
//! It checks `--listen` first (loopback only, `rdb_server::check_listen`), then opens its `Db`
//! as an app (`local = []`, links to every node in `--nodes` as app `NAME`, default `pilot-b`,
//! under client id `N`, default 100), waits until it is ready, and only then binds `--listen`,
//! so "connection refused" means not ready. Then it prints
//! `rdb-server ready listen=ADDR client=N log=PATH` and serves until Ctrl-C.
//!
//! Exits: 0 stopped by Ctrl-C; 2 bad arguments, a non-loopback `--listen`, or setup; 3 the open
//! or a link was refused; 5 not ready in time; 6 the listener could not bind.

use std::collections::BTreeMap;
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
                     [--app NAME] [--client-id N] [--log-dir L] [--ready-secs S]\n\
                     exits: 0 stopped by Ctrl-C; 2 bad arguments, a non-loopback --listen, or \
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
    ready: Duration,
}

fn parse(mut args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut control = Vec::new();
    let mut nodes = BTreeMap::new();
    let mut cert_dir = None;
    let mut listen = None;
    let mut app = "pilot-b".to_owned();
    let mut client_id = 100;
    let mut log_dir = None;
    let mut ready = Duration::from_secs(60);
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
    Ok(Args {
        control,
        nodes,
        cert_dir,
        listen,
        app,
        client_id,
        log_dir,
        ready,
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

async fn run_async(args: &Args, rt: &tokio::runtime::Runtime) -> ExitCode {
    let client = match config_client::GrpcClient::connect(
        args.control.clone(),
        config_client::GrpcClientOptions::default(),
    ) {
        Ok(client) => client,
        Err(error) => {
            eprintln!("rdb-server: --control: {error}");
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
        ack_floor: true,
        // An app holds no copy and claims no folder: `dir` is not used.
        dir: args.log_dir.clone(),
        hold: Vec::new(),
        timeouts,
        dedup_cap: None,
        dedup_retention: None,
        local: Vec::new(),
        links: Some(links),
        client: ClientId(args.client_id),
        boot: None,
        storage: rdb_api::StorageOptions::default(),
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
        log_path.display()
    );
    let gateway = Gateway::new(Arc::clone(&db), args.client_id, timeouts.put, timeouts.read);
    let shutdown = async {
        // A failed signal hook ends the serve rather than serving with no way to stop.
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "rdb_server_signal_failed");
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
