//! `rdb_node`: one rDB node in its own process (M10 S2, W4). `scripts/local-rdb.sh` starts
//! three of them; run it by hand only to walk one start.
//!
//! ```text
//! rdb_node --gen-certs --cert-dir C [--apps a,b] [--nodes K]
//! rdb_node --form --dir D --control E [--log-dir L] [--partitions P]
//! rdb_node --node N --dir D --listen ADDR --peers 1=ADDR,2=ADDR,3=ADDR --control E
//!          --cert-dir C [--protocol-version V] [--faults F] [--stop-file S] [--log-dir L]
//!          [--ready-secs S] [--admin-apps a,b] [--partitions P]
//!          [--clock-offset-ms M] [--clock-step-file F] [--unreachable-drop-after-ms M]
//! ```
//!
//! * `--gen-certs` writes a fresh development CA and certificates for nodes 1 to `--nodes`
//!   (default 3) and each app named by `--apps` into `--cert-dir`, then exits. `--cert-dir` pointing at another run's
//!   certificates is how a walk presents a certificate this run's nodes do not trust.
//! * `--form` makes the database once (ruling A-R231): all three copies in this process on
//!   `--dir`, until ready, then shuts down. A node process only ever opens a formed database.
//!   `--partitions` is how many partitions the database is formed with (issue #79), fixed from
//!   then on. Unset, a new database has one and a node process reads the count from the store.
//! * Otherwise the process runs node `N` of the database in `--dir`, holding only `nodes/N`.
//!   `--peers` names every other node; with this one they are nodes 1 to K, K at least 3 (M11
//!   WS A). Nodes past 3 are spares: they hold nothing until `admin move` pins them.
//!   It serves its peers and apps on `--listen` and dials each lower-numbered node. Its boot is
//!   `next_boot(--dir)`, the same value in the handshake and in the `Db` (ruling A-R227).
//!   `--protocol-version` makes it speak another wire version, to walk the refusal.
//!   `--faults` names the fault file it polls every 50 ms (`hold <n>|hold all|drop-request N|
//!   drop-reply N|cut-control`). It stops cleanly when `--stop-file` appears.
//!   `--admin-apps` names the apps whose certificate may send `admin move` and `admin rebuild`
//!   (ruling A-R479). Unset or empty, no app may (ruling A-R480), as in the library. `lr`
//!   passes `--admin-apps admin`, the app `lr` and `rdb_dev` run as; a production node lists
//!   its operator apps. Any other app's move or rebuild is refused before the node sees it.
//!   Names are trimmed, so `ops, admin` lists `ops` and `admin`.
//!   `--unreachable-drop-after-ms` is how long this node, as an owner, waits with no link to a
//!   copy before it drops that copy (default 10000, least 2000; `Timeouts::unreachable_drop_after`).
//!   `--live-readd` lets this node, as an owner, add back a copy its own drop clock left out
//!   once that copy's link is back, with no restart of the copy (`Timeouts::live_readd`, #107).
//!   Off unless given.
//!   `--clock-offset-ms` shifts this process's `Tick` once at start, real skew (#80 S2c).
//!   `--clock-step-file` names a file it polls every 20 ms: its contents, whole milliseconds,
//!   step `Tick` back once, then the file is removed. `kill -STOP`, wait, write the file,
//!   `kill -CONT` is a frozen-clock pause.
//!
//! Stdout, one line per step, for a script to wait on:
//! `rdb_node listening node=N addr=ADDR boot=B`, then `rdb_node open node=N`, then
//! `rdb_node ready node=N` or `rdb_node not-ready node=N: <why>`, then `rdb_node stopped node=N`.
//! While it runs, `rdb_node ownership node=N partition=P token=G.E` when this node starts leading
//! a partition, and `rdb_node ownership-lapsed node=N partition=P token=G.E` when
//! `Db::still_owner` turns false for that token (#84; logged `ownership_held`, `ownership_lapsed`).
//! Errors go to stderr with the step that failed. Exit codes: 0 stopped or formed, 1 a start
//! step failed, 2 bad arguments.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use config_core::ConfigStore;
use rdb_api::db::ReadyError;
use rdb_api::db::{next_boot, ALL_NODES, DEFAULT_CLIENT, MAX_NODES};
use rdb_api::net::faults::{CutStore, Faults};
use rdb_api::net::wire::Label;
use rdb_api::net::{certs, Net, NetConfig, PROTOCOL_VERSION};
use rdb_api::transport::Links;
use rdb_api::{Db, DbConfig, NodeId, OwnerToken, Ownership, PartitionId, Timeouts};

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
    /// `--gen-certs`: certificates for nodes 1 to this (M11 WS A); `None` is 3.
    nodes: Option<u32>,
    /// `--admin-apps`: the apps that may move or rebuild a copy (A-R479); `None` is none
    /// (A-R480).
    admin_apps: Option<BTreeSet<String>>,
    /// `--partitions`: the partition count (issue #79); `None` reads it from the control store.
    partitions: Option<u32>,
    clock_offset_ms: i64,
    clock_step_file: Option<PathBuf>,
    /// `--unreachable-drop-after-ms`; `None` is `Timeouts::default()`'s.
    unreachable_drop_after_ms: Option<u64>,
    /// `--live-readd` (#107).
    live_readd: bool,
}

/// The apps this node lets move or rebuild a copy: `--admin-apps` as given, or none when the
/// flag is absent, as in the library (A-R479, A-R480).
fn admin_apps(args: &Args) -> BTreeSet<String> {
    args.admin_apps.clone().unwrap_or_default()
}

const USAGE: &str = "usage:
  rdb_node --gen-certs --cert-dir C [--apps a,b] [--nodes K]
  rdb_node --form --dir D --control E [--log-dir L] [--partitions P]
  rdb_node --node N --dir D --listen ADDR --peers 1=ADDR,2=ADDR,3=ADDR --control E --cert-dir C
           [--protocol-version V] [--faults F] [--stop-file S] [--log-dir L] [--ready-secs S]
           [--admin-apps a,b] [--partitions P]
           [--clock-offset-ms M] [--clock-step-file F] [--unreachable-drop-after-ms M]
           [--live-readd]
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
            "--admin-apps" => {
                args.admin_apps = Some(
                    value("--admin-apps")?
                        .split(',')
                        .map(str::trim)
                        .filter(|a| !a.is_empty())
                        .map(str::to_owned)
                        .collect(),
                );
            }
            "--node" => {
                let n: u32 = value("--node")?
                    .parse()
                    .map_err(|_| format!("--node takes a node number, 1 to {MAX_NODES}"))?;
                if !(1..=MAX_NODES).contains(&n) {
                    return Err(format!("--node {n}: a node number is 1 to {MAX_NODES}"));
                }
                args.node = Some(n);
            }
            "--partitions" => {
                args.partitions = Some(
                    value("--partitions")?
                        .parse()
                        .map_err(|_| "--partitions takes a partition count".to_owned())?,
                );
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
            "--nodes" => {
                let k: u32 = value("--nodes")?
                    .parse()
                    .map_err(|_| format!("--nodes takes a count, 3 to {MAX_NODES}"))?;
                if !(3..=MAX_NODES).contains(&k) {
                    return Err(format!(
                        "--nodes {k}: a database has 3 to {MAX_NODES} nodes"
                    ));
                }
                args.nodes = Some(k);
            }
            "--ready-secs" => {
                args.ready_secs = Some(
                    value("--ready-secs")?
                        .parse()
                        .map_err(|_| "--ready-secs takes whole seconds".to_owned())?,
                );
            }
            "--clock-offset-ms" => {
                args.clock_offset_ms = value("--clock-offset-ms")?
                    .parse()
                    .map_err(|_| "--clock-offset-ms takes whole milliseconds".to_owned())?;
            }
            "--clock-step-file" => args.clock_step_file = Some(value("--clock-step-file")?.into()),
            "--unreachable-drop-after-ms" => {
                args.unreachable_drop_after_ms =
                    Some(value("--unreachable-drop-after-ms")?.parse().map_err(|_| {
                        "--unreachable-drop-after-ms takes whole milliseconds".to_owned()
                    })?);
            }
            "--live-readd" => args.live_readd = true,
            "-h" | "--help" => return Err(String::new()),
            other => return Err(format!("unknown argument {other}")),
        }
    }
    Ok(args)
}

/// Poll `file`: whole milliseconds in it step this process's `Tick` back once. A file that
/// does not parse is said out loud and removed, not read as zero.
fn watch_clock_step(file: PathBuf, links: Arc<Links>) {
    std::thread::spawn(move || loop {
        if let Ok(text) = std::fs::read_to_string(&file) {
            match text.trim().parse::<u64>() {
                Ok(millis) => links.clock().step_back(millis),
                Err(e) => {
                    tracing::error!(file = %file.display(), error = %e, "clock_step_file_bad")
                }
            }
            if let Err(e) = std::fs::remove_file(&file) {
                tracing::error!(file = %file.display(), error = %e, "clock_step_file_stuck");
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    });
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
    let labels: Vec<Label> = (1..=args.nodes.unwrap_or(3))
        .map(|n| Label::Node(NodeId(n)))
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
        partitions: args.partitions,
        ack_floor: true,
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

/// How often [`watch_ownership`] reads each partition's ownership.
const OWNERSHIP_POLL: Duration = Duration::from_millis(10);

/// One change [`poll_ownership`] saw for a partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OwnershipSeen {
    /// This node started leading it, or leads it again after a lapse.
    Held(Ownership),
    /// `still_owner` turned false for the token last held.
    Lapsed(PartitionId, OwnerToken),
}

/// One poll of partitions `1..=partitions`: a held token whose `still_owner` reads false is
/// reported lapsed and forgotten, then a partition not held whose `ownership` answers is
/// reported held. `held` carries the tokens from poll to poll.
fn poll_ownership(
    held: &mut BTreeMap<PartitionId, OwnerToken>,
    partitions: u32,
    ownership: impl Fn(PartitionId) -> Option<Ownership>,
    still_owner: impl Fn(PartitionId, OwnerToken) -> bool,
) -> Vec<OwnershipSeen> {
    let mut seen = Vec::new();
    for partition in (1..=partitions).map(PartitionId) {
        if let Some(token) = held.get(&partition).copied() {
            if still_owner(partition, token) {
                continue;
            }
            held.remove(&partition);
            seen.push(OwnershipSeen::Lapsed(partition, token));
        }
        if let Some(owned) = ownership(partition) {
            held.insert(partition, owned.token);
            seen.push(OwnershipSeen::Held(owned));
        }
    }
    seen
}

/// Poll every [`OWNERSHIP_POLL`] until `done`, handing each change to `report`.
fn watch_ownership_with(
    done: &AtomicBool,
    partitions: impl Fn() -> u32,
    ownership: impl Fn(PartitionId) -> Option<Ownership>,
    still_owner: impl Fn(PartitionId, OwnerToken) -> bool,
    mut report: impl FnMut(OwnershipSeen),
) {
    let mut held: BTreeMap<PartitionId, OwnerToken> = BTreeMap::new();
    while !done.load(Ordering::Relaxed) {
        for seen in poll_ownership(&mut held, partitions(), &ownership, &still_owner) {
            report(seen);
        }
        std::thread::sleep(OWNERSHIP_POLL);
    }
}

/// #84's manual entry point: log each partition this node starts leading, `ownership_held`
/// with its owner token, and the first poll at which `Db::still_owner` is false for that token,
/// `ownership_lapsed`. Polled every [`OWNERSHIP_POLL`], so a lapse line is up to that much after
/// the tick write admission stopped; the lapse itself is never later than admission's.
fn watch_ownership(db: &Db, node: NodeId, done: &AtomicBool) {
    watch_ownership_with(
        done,
        || db.partition_count(),
        |partition| db.ownership(partition),
        |partition, token| db.still_owner(partition, token),
        |seen| match seen {
            OwnershipSeen::Lapsed(partition, token) => {
                tracing::warn!(
                    node = node.0,
                    partition = partition.0,
                    generation = token.generation.0,
                    owner_epoch = token.owner_epoch.0,
                    token = %token,
                    "ownership_lapsed"
                );
                println!(
                    "rdb_node ownership-lapsed node={} partition={} token={token}",
                    node.0, partition.0
                );
            }
            OwnershipSeen::Held(owned) => {
                tracing::info!(
                    node = node.0,
                    partition = owned.partition.0,
                    generation = owned.token.generation.0,
                    owner_epoch = owned.token.owner_epoch.0,
                    token = %owned.token,
                    remaining_ms = u64::try_from(owned.remaining.as_millis()).unwrap_or(u64::MAX),
                    "ownership_held"
                );
                println!(
                    "rdb_node ownership node={} partition={} token={}",
                    node.0, owned.partition.0, owned.token
                );
            }
        },
    );
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

/// The nodes `node` links to: every node `--peers` names. With `node` they must be nodes 1
/// to K, K at least 3 (M11 WS A): each formed member, then any spares, with no gap.
fn peer_set(node: NodeId, peers: &BTreeMap<NodeId, String>) -> Result<BTreeSet<NodeId>, String> {
    for n in ALL_NODES {
        if n != node && !peers.contains_key(&n) {
            return Err(format!("--peers names no address for node {}", n.0));
        }
    }
    // `lr` lists every node, this one included; its own address is not dialled.
    let mut all: BTreeSet<NodeId> = peers.keys().copied().collect();
    all.insert(node);
    let k = u32::try_from(all.len()).unwrap_or(u32::MAX);
    if all.iter().map(|n| n.0).ne(1..=k) {
        return Err(format!(
            "--peers and --node name nodes {:?}: they must be nodes 1 to K with no gap",
            all.iter().map(|n| n.0).collect::<Vec<_>>()
        ));
    }
    all.remove(&node);
    Ok(all)
}

fn serve(args: &Args, rt: &tokio::runtime::Runtime) -> Result<(), String> {
    let node = NodeId(need(args.node.as_ref(), "--node")?);
    let dir = need(args.dir.as_ref(), "--dir")?;
    let listen = need(args.listen.as_ref(), "--listen")?;
    let cert_dir = need(args.cert_dir.as_ref(), "--cert-dir")?;
    let peers = peer_set(node, &args.peers)?;
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
    let links = Links::with_clock_offset(args.clock_offset_ms);
    if let Some(file) = args.clock_step_file.clone() {
        watch_clock_step(file, Arc::clone(&links));
    }
    let net = Net::start(
        NetConfig {
            label,
            boot,
            tls,
            peers,
            dial: NetConfig::dial_lower(node, &args.peers),
            protocol_version: args.protocol_version.unwrap_or(PROTOCOL_VERSION),
            faults,
            admin_apps: admin_apps(args),
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
        partitions: args.partitions,
        ack_floor: true,
        dir: dir.clone(),
        hold: Vec::new(),
        timeouts: Timeouts {
            unreachable_drop_after: args.unreachable_drop_after_ms.map_or(
                Timeouts::default().unreachable_drop_after,
                Duration::from_millis,
            ),
            live_readd: args.live_readd,
            ..Timeouts::default()
        },
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
    let done = AtomicBool::new(false);
    std::thread::scope(|scope| {
        scope.spawn(|| watch_ownership(&db, node, &done));
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
        done.store(true, Ordering::Relaxed);
    });
    if let Some(stop) = &args.stop_file {
        tracing::info!(node = node.0, stop = %stop.display(), "rdb_node_stop_file");
    }
    db.shutdown();
    drop(db);
    drop(net);
    println!("rdb_node stopped node={}", node.0);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{admin_apps, parse};

    fn apps(argv: &[&str]) -> Vec<String> {
        let args = parse(argv.iter().map(|a| (*a).to_owned())).expect("parses");
        admin_apps(&args).into_iter().collect()
    }

    /// Critic-479 A6 (A-R479), ruling A-R480: how `--admin-apps` is read. Unset is no app, as
    /// the library's empty `NetConfig::admin_apps` is, so a production node grants nobody a
    /// move by default; `lr` passes `--admin-apps admin`. An empty value is no app too; a list
    /// is that list. A regression here fails open without a word. Unit.
    #[test]
    fn pr74_r480_admin_apps_unset_and_empty_are_none() {
        assert_eq!(apps(&[]), Vec::<String>::new());
        assert_eq!(apps(&["--admin-apps", ""]), Vec::<String>::new());
        assert_eq!(apps(&["--admin-apps", "ops,admin"]), ["admin", "ops"]);
    }

    /// Revobot R3 4489400304: each name is trimmed, so `--admin-apps "ops, admin"` lists
    /// `admin`, not ` admin`, which the gate would refuse; a blank name is dropped, so a list
    /// of blanks is still none. Unit.
    #[test]
    fn pr74_r3_admin_apps_names_are_trimmed_and_blanks_dropped() {
        assert_eq!(apps(&["--admin-apps", "ops, admin"]), ["admin", "ops"]);
        assert_eq!(apps(&["--admin-apps", " ops ,  admin "]), ["admin", "ops"]);
        assert_eq!(apps(&["--admin-apps", " , "]), Vec::<String>::new());
    }

    /// own84_10 (#84 review): the watcher's transitions. A partition is reported held once, then
    /// lapsed at the first poll `still_owner` reads false, then held again on reacquire, with
    /// the same token after a self-replace or a higher one after a promote. Unit.
    #[test]
    fn own84_10_the_watcher_reports_held_lapsed_and_reacquired() {
        use super::{poll_ownership, OwnershipSeen};
        use rdb_api::{Generation, NodeId, OwnerEpoch, OwnerToken, Ownership, PartitionId};
        use std::cell::Cell;
        use std::collections::BTreeMap;
        use std::time::Duration;

        let p = PartitionId(1);
        let token = |g, e| OwnerToken {
            generation: Generation(g),
            owner_epoch: OwnerEpoch(e),
        };
        let owned = |t| Ownership {
            node: NodeId(1),
            partition: p,
            token: t,
            remaining: Duration::from_millis(5),
        };
        // What the node owns now; `still_owner` is true for exactly that token.
        let now: Cell<Option<OwnerToken>> = Cell::new(None);
        let mut held = BTreeMap::new();
        let mut poll = || {
            poll_ownership(
                &mut held,
                1,
                |q| now.get().filter(|_| q == p).map(owned),
                |q, t| q == p && now.get() == Some(t),
            )
        };
        assert_eq!(poll(), [], "nothing owned");
        now.set(Some(token(1, 1)));
        assert_eq!(poll(), [OwnershipSeen::Held(owned(token(1, 1)))]);
        assert_eq!(poll(), [], "held: no repeat");
        now.set(None);
        assert_eq!(poll(), [OwnershipSeen::Lapsed(p, token(1, 1))]);
        assert_eq!(poll(), [], "lapsed: no repeat");
        now.set(Some(token(1, 1)));
        assert_eq!(
            poll(),
            [OwnershipSeen::Held(owned(token(1, 1)))],
            "the same token again"
        );
        now.set(Some(token(2, 2)));
        assert_eq!(
            poll(),
            [
                OwnershipSeen::Lapsed(p, token(1, 1)),
                OwnershipSeen::Held(owned(token(2, 2)))
            ],
            "a higher token in one poll: the old lapses first"
        );
    }

    /// own84_10b: the watcher thread ends once `done` is set, so `serve`'s scope joins it at
    /// shutdown; within a few polls. Unit (~20 ms).
    #[test]
    fn own84_10b_the_watcher_stops_when_done_and_is_joined() {
        use super::{watch_ownership_with, OWNERSHIP_POLL};
        use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
        use std::time::Instant;

        let done = AtomicBool::new(false);
        let polls = AtomicU32::new(0);
        let started = Instant::now();
        std::thread::scope(|scope| {
            scope.spawn(|| {
                watch_ownership_with(
                    &done,
                    || {
                        polls.fetch_add(1, Ordering::Relaxed);
                        4
                    },
                    |_| None,
                    |_, _| false,
                    |seen| panic!("nothing is owned: {seen:?}"),
                );
            });
            while polls.load(Ordering::Relaxed) < 2 {
                std::thread::yield_now();
            }
            done.store(true, Ordering::Relaxed);
        });
        assert!(
            started.elapsed() < OWNERSHIP_POLL * 50,
            "joined after {:?}",
            started.elapsed()
        );
    }
}
