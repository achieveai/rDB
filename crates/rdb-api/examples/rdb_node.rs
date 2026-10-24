//! `rdb_node`: one rDB node in its own process (M10 S2, W4). `scripts/local-rdb.sh` starts
//! three of them; run it by hand only to walk one start.
//!
//! ```text
//! rdb_node --gen-certs --cert-dir C [--apps a,b]
//! rdb_node --form --dir D --control E [--log-dir L]
//! rdb_node --node N --dir D --listen ADDR --peers 1=ADDR,2=ADDR,3=ADDR --control E
//!          --cert-dir C [--protocol-version V] [--faults F] [--stop-file S] [--log-dir L]
//!          [--ready-secs S]
//! ```
//!
//! * `--gen-certs` writes a fresh development CA and certificates for nodes 1-3 and each app
//!   named by `--apps` into `--cert-dir`, then exits. `--cert-dir` pointing at another run's
//!   certificates is how a walk presents a certificate this run's nodes do not trust.
//! * `--form` makes the database once (ruling A-R231): all three copies in this process on
//!   `--dir`, until ready, then shuts down. A node process only ever opens a formed database.
//! * Otherwise the process runs node `N` of the database in `--dir`, holding only `nodes/N`.
//!   It serves its peers and apps on `--listen` and dials each lower-numbered node. Its boot is
//!   `next_boot(--dir)`, the same value in the handshake and in the `Db` (ruling A-R227).
//!   `--protocol-version` makes it speak another wire version, to walk the refusal.
//!   `--faults` names the fault file it polls every 50 ms (`hold <n>|hold all|drop-request N|
//!   drop-reply N|cut-control`). It stops cleanly when `--stop-file` appears.
//!
//! Stdout, one line per step, for a script to wait on:
//! `rdb_node listening node=N addr=ADDR boot=B`, then `rdb_node open node=N`, then
//! `rdb_node ready node=N` or `rdb_node not-ready node=N: <why>`, then `rdb_node stopped node=N`.
//! Errors go to stderr with the step that failed. Exit codes: 0 stopped or formed, 1 a start
//! step failed, 2 bad arguments.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use config_core::ConfigStore;
use rdb_api::db::ReadyError;
use rdb_api::db::{next_boot, ALL_NODES, DEFAULT_CLIENT};
use rdb_api::net::faults::{CutStore, Faults};
use rdb_api::net::wire::Label;
use rdb_api::net::{certs, Net, NetConfig, PROTOCOL_VERSION};
use rdb_api::transport::Links;
use rdb_api::{Db, DbConfig, NodeId, Timeouts};

#[derive(Debug, Default)]
struct Args {
    gen_certs: bool,
    form: bool,
    apps: Vec<String>,
    node: Option<u32>,
    dir: Option<PathBuf>,
    listen: Option<String>,
    peers: BTreeMap<NodeId, String>,
    control: Vec<String>,
    cert_dir: Option<PathBuf>,
    protocol_version: Option<u32>,
    faults: Option<PathBuf>,
    stop_file: Option<PathBuf>,
    log_dir: Option<PathBuf>,
    ready_secs: Option<u64>,
}

const USAGE: &str = "usage:
  rdb_node --gen-certs --cert-dir C [--apps a,b]
  rdb_node --form --dir D --control E [--log-dir L]
  rdb_node --node N --dir D --listen ADDR --peers 1=ADDR,2=ADDR,3=ADDR --control E --cert-dir C
           [--protocol-version V] [--faults F] [--stop-file S] [--log-dir L] [--ready-secs S]
";

fn parse(mut argv: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut args = Args::default();
    while let Some(flag) = argv.next() {
        let mut value = |name: &str| argv.next().ok_or_else(|| format!("{name} needs a value"));
        match flag.as_str() {
            "--gen-certs" => args.gen_certs = true,
            "--form" => args.form = true,
            "--apps" => {
                args.apps = value("--apps")?
                    .split(',')
                    .filter(|a| !a.is_empty())
                    .map(str::to_owned)
                    .collect();
            }
            "--node" => {
                let n: u32 = value("--node")?
                    .parse()
                    .map_err(|_| "--node takes 1, 2 or 3".to_owned())?;
                if !ALL_NODES.contains(&NodeId(n)) {
                    return Err(format!("--node {n}: this database has nodes 1, 2 and 3"));
                }
                args.node = Some(n);
            }
            "--dir" => args.dir = Some(value("--dir")?.into()),
            "--listen" => args.listen = Some(value("--listen")?),
            "--peers" => {
                for pair in value("--peers")?.split(',') {
                    let (n, addr) = pair
                        .split_once('=')
                        .ok_or_else(|| format!("--peers: {pair:?} is not N=host:port"))?;
                    let n: u32 = n
                        .parse()
                        .map_err(|_| format!("--peers: {n:?} is not a node number"))?;
                    args.peers.insert(NodeId(n), addr.to_owned());
                }
            }
            "--control" => {
                args.control = value("--control")?
                    .split(',')
                    .filter(|e| !e.is_empty())
                    .map(str::to_owned)
                    .collect();
            }
            "--cert-dir" => args.cert_dir = Some(value("--cert-dir")?.into()),
            "--protocol-version" => {
                args.protocol_version = Some(
                    value("--protocol-version")?
                        .parse()
                        .map_err(|_| "--protocol-version takes a number".to_owned())?,
                );
            }
            "--faults" => args.faults = Some(value("--faults")?.into()),
            "--stop-file" => args.stop_file = Some(value("--stop-file")?.into()),
            "--log-dir" => args.log_dir = Some(value("--log-dir")?.into()),
            "--ready-secs" => {
                args.ready_secs = Some(
                    value("--ready-secs")?
                        .parse()
                        .map_err(|_| "--ready-secs takes whole seconds".to_owned())?,
                );
            }
            "-h" | "--help" => return Err(String::new()),
            other => return Err(format!("unknown argument {other}")),
        }
    }
    Ok(args)
}

fn need<T: Clone>(value: Option<&T>, name: &str) -> Result<T, String> {
    value.cloned().ok_or_else(|| format!("{name} is required"))
}

fn main() -> ExitCode {
    let args = match parse(std::env::args().skip(1)) {
        Ok(args) => args,
        Err(message) => {
            if !message.is_empty() {
                eprintln!("rdb_node: {message}");
            }
            eprint!("{USAGE}");
            return ExitCode::from(2);
        }
    };
    if args.gen_certs {
        return gen_certs(&args);
    }
    let name = args
        .node
        .map_or_else(|| "rdb_node-form".to_owned(), |n| format!("rdb_node-{n}"));
    let log_dir = args.log_dir.clone().unwrap_or_else(|| PathBuf::from("."));
    let mut config = config_log::LogConfig::for_application("rdb_node");
    config.dir.clone_from(&log_dir);
    config.file_name = format!("{name}.jsonl");
    let guard = match config_log::init(config) {
        Ok(guard) => guard,
        Err(e) => {
            eprintln!("rdb_node: log init: {e}");
            return ExitCode::from(2);
        }
    };
    eprintln!("log={}", log_dir.join(format!("{name}.jsonl")).display());
    let code = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => {
            let result = if args.form {
                form(&args, &rt)
            } else {
                serve(&args, &rt)
            };
            match result {
                Ok(()) => ExitCode::SUCCESS,
                Err(message) => {
                    tracing::error!(%message, "rdb_node_failed");
                    eprintln!("rdb_node: {message}");
                    ExitCode::from(1)
                }
            }
        }
        Err(e) => {
            eprintln!("rdb_node: runtime: {e}");
            ExitCode::from(1)
        }
    };
    drop(guard);
    code
}

fn gen_certs(args: &Args) -> ExitCode {
    let Some(dir) = &args.cert_dir else {
        eprintln!("rdb_node: --gen-certs needs --cert-dir");
        return ExitCode::from(2);
    };
    let labels: Vec<Label> = ALL_NODES
        .iter()
        .map(|n| Label::Node(*n))
        .chain(args.apps.iter().cloned().map(Label::App))
        .collect();
    match certs::generate(dir, &labels) {
        Ok(()) => {
            let names: Vec<String> = labels.iter().map(ToString::to_string).collect();
            println!(
                "rdb_node certs dir={} labels={}",
                dir.display(),
                names.join(",")
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("rdb_node: --gen-certs: {e}");
            ExitCode::from(1)
        }
    }
}

fn control(args: &Args) -> Result<Arc<dyn ConfigStore>, String> {
    if args.control.is_empty() {
        return Err("--control is required: the rEtcd client endpoints (lr up starts one)".into());
    }
    config_client::GrpcClient::connect(
        args.control.clone(),
        config_client::GrpcClientOptions::default(),
    )
    .map(|client| Arc::new(client) as Arc<dyn ConfigStore>)
    .map_err(|e| format!("--control {}: {e}", args.control.join(",")))
}

/// Make the database once: all three copies here, until ready, then shut down (A-R231).
fn form(args: &Args, rt: &tokio::runtime::Runtime) -> Result<(), String> {
    let dir = need(args.dir.as_ref(), "--dir")?;
    let store = control(args)?;
    let config = DbConfig {
        dir: dir.clone(),
        hold: Vec::new(),
        timeouts: Timeouts::default(),
        dedup_cap: None,
        dedup_retention: None,
        local: ALL_NODES.to_vec(),
        links: None,
        client: DEFAULT_CLIENT,
        boot: None,
        storage: rdb_api::StorageOptions::default(),
    };
    let mut db = rt
        .block_on(Db::open(config, store, rt.handle().clone()))
        .map_err(|e| format!("form: open {}: {e}", dir.display()))?;
    let within = Duration::from_secs(args.ready_secs.unwrap_or(60));
    let ready = db.wait_ready(within);
    db.shutdown();
    ready.map_err(|e| format!("form: not ready within {within:?}: {e}"))?;
    tracing::info!(dir = %dir.display(), "rdb_node_formed");
    println!("rdb_node formed dir={}", dir.display());
    Ok(())
}

/// Run node `--node` of the database in `--dir` until `--stop-file` appears.
/// Prints a wait's outcome. True when it timed out, so `serve` keeps waiting: the
/// line then ends `(retrying)`, and `lr` reads a not-ready line without it as final.
fn report_ready(node: NodeId, waited: Result<(), ReadyError>) -> bool {
    match waited {
        Ok(()) => {
            println!("rdb_node ready node={}", node.0);
            false
        }
        Err(e) => {
            let retrying = matches!(e, ReadyError::TimedOut { .. });
            tracing::warn!(node = node.0, error = %e, retrying, "rdb_node_not_ready");
            let tail = if retrying { " (retrying)" } else { "" };
            println!("rdb_node not-ready node={}: {e}{tail}", node.0);
            retrying
        }
    }
}

fn serve(args: &Args, rt: &tokio::runtime::Runtime) -> Result<(), String> {
    let node = NodeId(need(args.node.as_ref(), "--node")?);
    let dir = need(args.dir.as_ref(), "--dir")?;
    let listen = need(args.listen.as_ref(), "--listen")?;
    let cert_dir = need(args.cert_dir.as_ref(), "--cert-dir")?;
    for n in ALL_NODES {
        if n != node && !args.peers.contains_key(&n) {
            return Err(format!("--peers names no address for node {}", n.0));
        }
    }
    let _entered = rt.enter();
    let faults = Faults::new();
    if let Some(path) = &args.faults {
        faults.watch_file(path.clone());
    }
    let store: Arc<dyn ConfigStore> = CutStore::new(control(args)?, Arc::clone(&faults));
    let boot = next_boot(&dir).map_err(|e| format!("boot: {}: {e}", dir.display()))?;
    let label = Label::Node(node);
    let tls = certs::load(&cert_dir, &label)?;
    let listener = rt
        .block_on(tokio::net::TcpListener::bind(&listen))
        .map_err(|e| format!("--listen {listen}: {e}"))?;
    let links = Links::new();
    let peers: BTreeSet<NodeId> = ALL_NODES.iter().copied().filter(|n| *n != node).collect();
    let net = Net::start(
        NetConfig {
            label,
            boot,
            tls,
            peers,
            dial: NetConfig::dial_lower(node, &args.peers),
            protocol_version: args.protocol_version.unwrap_or(PROTOCOL_VERSION),
            faults,
        },
        &links,
        Some(listener),
    )?;
    let addr = net.addr().map_or_else(|| listen.clone(), |a| a.to_string());
    tracing::info!(node = node.0, %addr, boot = boot.0, "rdb_node_listening");
    println!(
        "rdb_node listening node={} addr={addr} boot={}",
        node.0, boot.0
    );

    let config = DbConfig {
        dir: dir.clone(),
        hold: Vec::new(),
        timeouts: Timeouts::default(),
        dedup_cap: None,
        dedup_retention: None,
        local: vec![node],
        links: Some(Arc::clone(&links)),
        client: DEFAULT_CLIENT,
        boot: Some(boot),
        storage: rdb_api::StorageOptions::default(),
    };
    let mut db = rt
        .block_on(Db::open(config, store, rt.handle().clone()))
        .map_err(|e| format!("open {} as node {}: {e}", dir.display(), node.0))?;
    println!("rdb_node open node={}", node.0);
    let within = Duration::from_secs(args.ready_secs.unwrap_or(60));
    let waited = std::time::Instant::now();
    // A missed deadline is a line, not a stop (tester-s2 `lr up` race, A-R271): only a time-out
    // keeps waiting, and the late `ready` line says how long it took. Any other refusal ends it.
    let mut retrying = report_ready(node, db.wait_ready(within));
    let stopped = || args.stop_file.as_ref().is_some_and(|stop| stop.exists());
    while !stopped() {
        if !retrying {
            std::thread::sleep(Duration::from_millis(100));
            continue;
        }
        match db.wait_ready(Duration::from_secs(1)) {
            Ok(()) => {
                let after = waited.elapsed().as_secs();
                tracing::info!(node = node.0, after_s = after, "rdb_node_ready_late");
                println!("rdb_node ready node={} (after {after}s)", node.0);
                retrying = false;
            }
            Err(ReadyError::TimedOut { .. }) => {}
            ended => retrying = report_ready(node, ended),
        }
    }
    if let Some(stop) = &args.stop_file {
        tracing::info!(node = node.0, stop = %stop.display(), "rdb_node_stop_file");
    }
    db.shutdown();
    drop(db);
    drop(net);
    println!("rdb_node stopped node={}", node.0);
    Ok(())
}
