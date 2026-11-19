//! `rdb_node`: one rDB node in its own process (M10 S2, W4). `scripts/local-rdb.sh` starts
//! three of them; run it by hand only to walk one start.
//!
//! ```text
//! rdb_node --gen-certs --cert-dir C [--apps a,b] [--nodes K]
//! rdb_node --form --dir D --control E [--log-dir L] [--partitions P]
//! rdb_node --declare --control E [--partitions P] [--nodes K] [--log-dir L]
//! rdb_node --node N --dir D --listen ADDR --peers 1=ADDR,2=ADDR,3=ADDR --control E
//!          --cert-dir C [--protocol-version V] [--faults F] [--stop-file S] [--log-dir L]
//!          [--ready-secs S] [--admin-apps a,b] [--partitions P]
//!          [--clock-offset-ms M] [--clock-step-file F] [--unreachable-drop-after-ms M]
//!          [--failover] [--failover-silent-ms M] [--failover-max-concurrent K]
//! ```
//!
//! * `--gen-certs` writes a fresh development CA and certificates for nodes 1 to `--nodes`
//!   (default 3) and each app named by `--apps` into `--cert-dir`, then exits. `--cert-dir` pointing at another run's
//!   certificates is how a walk presents a certificate this run's nodes do not trust.
//! * `--form` makes the database once (ruling A-R231): all three copies in this process on
//!   `--dir`, until ready, then shuts down. A node process opens only a formed or declared database.
//!   `--partitions` is how many partitions the database is formed with (issue #79), fixed from
//!   then on. Unset, a new database has one and a node process reads the count from the store.
//! * `--declare` makes the database with no copy at all (#80 S1a, ADR-rdb-0022): it writes the
//!   control records and, last, the formation record naming the database's instance, then exits.
//!   Each node process then opens its own empty `--dir`, and the owner of each partition forms
//!   it. `--partitions` defaults to 1; `--nodes` is the node count, and this build declares 3.
//!   Run again with the same arguments it says `already`; with another count it is refused.
//!   It takes no `--form`, `--gen-certs`, `--node`, `--dir` or `--live-readd`: each of these
//!   five is refused. Other node-process flags, such as `--listen` and `--peers`, still parse
//!   and are ignored.
//! * Otherwise the process runs node `N` of the database in `--dir`, holding only `nodes/N`.
//!   `--peers` names every other node; with this one they are nodes 1 to K, K at least 3 (M11
//!   WS A). Nodes past 3 are spares: they hold nothing until `admin move` pins them.
//!   It serves its peers and apps on `--listen` and dials each lower-numbered node. Its boot is
//!   `next_boot(--dir)`, the same value in the handshake and in the `Db` (ruling A-R227).
//!   `--protocol-version` makes it speak another wire version, to walk the refusal.
//!   `--faults` names the fault file it polls every 50 ms (`hold <n>|hold all|drop-request N|
//!   drop-reply N|cut-control|mute`). It stops cleanly when `--stop-file` appears.
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
//!   `--failover` runs the automatic-failover planner in this process (#81 C1,
//!   `DbConfig::failover`): while it holds `planner/grant` it promotes each partition of a node
//!   whose link has echoed no clock probe for `--failover-silent-ms` (default 2000, least
//!   1000), that answers no look, and whose grant has run out; or of a node whose grant is
//!   frozen. At most `--failover-max-concurrent` (default 16, least 1) promotes run at once.
//!   Off by default. Either knob without `--failover` is refused, and so is `--failover` with
//!   `--form` or `--gen-certs`: neither runs a planner.
//!
//! Stdout, one line per step, for a script to wait on:
//! `rdb_node listening node=N addr=ADDR boot=B`, then `rdb_node open node=N`, then
//! `rdb_node ready node=N` or `rdb_node not-ready node=N: <why>`, then `rdb_node stopped node=N`.
//! While it runs, `rdb_node ownership node=N partition=P token=G.E` when this node starts leading
//! a partition, and `rdb_node ownership-lapsed node=N partition=P token=G.E` when
//! `Db::still_owner` turns false for that token (#84; logged `ownership_held`, `ownership_lapsed`).
//! And each event of `Db::ownership_events` (#83), as it arrives:
//! `rdb_node ownership-event node=N partition=P state=activated token=G.E`, `state=deactivating`
//! with `deadline_in_ms=D` (time left until writes stop), or `state=deactivated`. The host logs
//! each as `ownership_event` with its clock tick; a dropped subscriber subscribes again
//! (`rdb_node_ownership_resubscribed`).
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
use rdb_api::{Db, DbConfig, NodeId, OwnerToken, Ownership, OwnershipEvent, PartitionId, Timeouts};

#[derive(Debug, Default)]
struct Args {
    gen_certs: bool,
    form: bool,
    declare: bool,
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
    /// `--failover`, with its two knobs: `DbConfig::failover` (#81 C1).
    failover: bool,
    failover_silent_ms: Option<u64>,
    failover_max_concurrent: Option<usize>,
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
  rdb_node --declare --control E [--partitions P] [--nodes K] [--log-dir L]
  rdb_node --node N --dir D --listen ADDR --peers 1=ADDR,2=ADDR,3=ADDR --control E --cert-dir C
           [--protocol-version V] [--faults F] [--stop-file S] [--log-dir L] [--ready-secs S]
           [--admin-apps a,b] [--partitions P]
           [--clock-offset-ms M] [--clock-step-file F] [--unreachable-drop-after-ms M]
           [--failover] [--failover-silent-ms M] [--failover-max-concurrent K]
           [--live-readd]
  --faults F: a file of lines, each one of
           hold N | hold all | drop-request N | drop-reply N | cut-control | mute
";

fn parse(mut argv: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut args = Args::default();
    while let Some(flag) = argv.next() {
        let mut value = |name: &str| argv.next().ok_or_else(|| format!("{name} needs a value"));
        match flag.as_str() {
            "--gen-certs" => args.gen_certs = true,
            "--form" => args.form = true,
            "--declare" => args.declare = true,
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
            "--failover" => args.failover = true,
            "--failover-silent-ms" => {
                let millis: u64 = value("--failover-silent-ms")?
                    .parse()
                    .map_err(|_| "--failover-silent-ms wants whole milliseconds".to_owned())?;
                if millis < FAILOVER_SILENT_MIN_MS {
                    return Err(format!(
                        "--failover-silent-ms {millis} is under one clock-probe period \
                         ({FAILOVER_SILENT_MIN_MS} ms): a healthy link would read as silent"
                    ));
                }
                args.failover_silent_ms = Some(millis);
            }
            "--failover-max-concurrent" => {
                let count: usize = value("--failover-max-concurrent")?.parse().map_err(|_| {
                    "--failover-max-concurrent wants a count of at least 1".to_owned()
                })?;
                if count == 0 {
                    return Err("--failover-max-concurrent wants a count of at least 1".to_owned());
                }
                args.failover_max_concurrent = Some(count);
            }
            "--live-readd" => args.live_readd = true,
            "-h" | "--help" => return Err(String::new()),
            other => return Err(format!("unknown argument {other}")),
        }
    }
    failover_refusal(&args)?;
    declare_refusal(&args)?;
    Ok(args)
}

/// Five mode and copy flags `--declare` refuses (PR #138 review): it writes control records
/// only, so it makes no copy (`--form`), certificates (`--gen-certs`) or node process (`--node`,
/// `--dir`, `--live-readd`). Not every flag it does not read: `--listen`, `--peers` and the
/// other node-process flags still parse and are ignored.
fn declare_refusal(args: &Args) -> Result<(), String> {
    if !args.declare {
        return Ok(());
    }
    for (given, flag) in [
        (args.form, "--form"),
        (args.gen_certs, "--gen-certs"),
        (args.node.is_some(), "--node"),
        (args.dir.is_some(), "--dir"),
        (args.live_readd, "--live-readd"),
    ] {
        if given {
            return Err(format!(
                "--declare writes only control records and takes no {flag}"
            ));
        }
    }
    Ok(())
}

/// The shortest `--failover-silent-ms` taken: one clock-probe period, the same floor
/// `Db::open_with` holds `Failover::silent_after` to (`net::PROBE_PERIOD`, 1 s). The CLI
/// refuses below it while parsing, before this process writes its boot or listens (PR #119
/// review); `Db::open_with` still refuses it for an embedder.
const FAILOVER_SILENT_MIN_MS: u64 = 1_000;

/// `--failover` and its knobs where nothing would read them, refused rather than ignored (PR
/// #119 review): `--form` and `--gen-certs` run no planner, and a knob does nothing without
/// `--failover`.
fn failover_refusal(args: &Args) -> Result<(), String> {
    if args.failover {
        for (given, mode) in [
            (args.form, "--form"),
            (args.declare, "--declare"),
            (args.gen_certs, "--gen-certs"),
        ] {
            if given {
                return Err(format!(
                    "--failover runs only in a node process, not with {mode}"
                ));
            }
        }
        return Ok(());
    }
    for (given, knob) in [
        (args.failover_silent_ms.is_some(), "--failover-silent-ms"),
        (
            args.failover_max_concurrent.is_some(),
            "--failover-max-concurrent",
        ),
    ] {
        if given {
            return Err(format!("{knob} needs --failover"));
        }
    }
    Ok(())
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

/// `DbConfig::failover` from `--failover` and its knobs; `None` without `--failover`.
fn failover(args: &Args) -> Option<rdb_api::Failover> {
    let default = rdb_api::Failover::default();
    args.failover.then(|| rdb_api::Failover {
        silent_after: args
            .failover_silent_ms
            .map_or(default.silent_after, Duration::from_millis),
        max_concurrent: args
            .failover_max_concurrent
            .unwrap_or(default.max_concurrent),
        ..default
    })
}

fn need<T: Clone>(value: Option<&T>, name: &str) -> Result<T, String> {
    value.cloned().ok_or_else(|| format!("{name} is required"))
}

fn main() -> ExitCode {
    run(std::env::args().skip(1), |config| {
        config_log::init(config)
            .map(Some)
            .map_err(|e| e.to_string())
    })
}

/// `main`, with how logging starts passed in: a row runs this same path under the subscriber
/// its `#[config_log::retcd_test]` installed, where a second `config_log::init` is refused.
fn run(
    argv: impl Iterator<Item = String>,
    start_logging: impl FnOnce(config_log::LogConfig) -> Result<Option<config_log::LogGuard>, String>,
) -> ExitCode {
    let args = match parse(argv) {
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
    let name = match args.node {
        Some(n) => format!("rdb_node-{n}"),
        None if args.declare => "rdb_node-declare".to_owned(),
        None => "rdb_node-form".to_owned(),
    };
    let log_dir = args.log_dir.clone().unwrap_or_else(|| PathBuf::from("."));
    let mut config = config_log::LogConfig::for_application("rdb_node");
    config.dir.clone_from(&log_dir);
    config.file_name = format!("{name}.jsonl");
    let guard = match start_logging(config) {
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
            } else if args.declare {
                declare(&args, &rt)
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
        failover: None,
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

/// Declare the database (#80 S1a): control records only, no copy, then exit.
fn declare(args: &Args, rt: &tokio::runtime::Runtime) -> Result<(), String> {
    let store = control(args)?;
    let line = declare_on(args, &store, rt)?;
    println!("{line}");
    Ok(())
}

/// [`declare`] on `store`, answering the line it prints: the parsed `--nodes` and
/// `--partitions` go to `admin::declare` as given, so a row can run this path on its own store.
fn declare_on(
    args: &Args,
    store: &Arc<dyn ConfigStore>,
    rt: &tokio::runtime::Runtime,
) -> Result<String, String> {
    let declaration = rdb_api::admin::Declaration {
        nodes: (1..=args.nodes.unwrap_or(3)).map(NodeId).collect(),
        partitions: args.partitions.unwrap_or(1),
    };
    let (word, formation) = match rt.block_on(rdb_api::admin::declare(store, &declaration)) {
        Ok(rdb_api::admin::Declared::Created(formation)) => ("declared", formation),
        Ok(rdb_api::admin::Declared::Already(formation)) => ("already", formation),
        Err(e) => return Err(format!("declare: {e}")),
    };
    let instance = rdb_api::admin::instance_hex(&formation.instance);
    let nodes: Vec<String> = formation.nodes.iter().map(|n| n.0.to_string()).collect();
    tracing::info!(%instance, partitions = formation.partitions, word, "rdb_node_declared");
    Ok(format!(
        "rdb_node {word} instance={instance} partitions={} nodes={}",
        formation.partitions,
        nodes.join(",")
    ))
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

/// #83's manual entry point: print each ownership event as it arrives. A receiver that ends
/// (this subscriber lagged, or a node stopped) is subscribed again, which starts from the
/// current state.
fn print_ownership_events(db: &Db, done: &AtomicBool) {
    let mut events = db.ownership_events();
    while !done.load(Ordering::Relaxed) {
        let event = match events.recv_timeout(Duration::from_millis(100)) {
            Ok(event) => event,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                tracing::warn!("rdb_node_ownership_resubscribed");
                std::thread::sleep(Duration::from_millis(100));
                events = db.ownership_events();
                continue;
            }
        };
        let tail = match event {
            OwnershipEvent::Deactivating { deadline, .. } => format!(
                " deadline_in_ms={}",
                deadline
                    .saturating_duration_since(std::time::Instant::now())
                    .as_millis()
            ),
            _ => String::new(),
        };
        println!(
            "rdb_node ownership-event node={} partition={} state={} token={}{tail}",
            event.node().0,
            event.partition().0,
            event.state(),
            event.token()
        );
    }
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
        failover: failover(args),
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
        scope.spawn(|| print_ownership_events(&db, &done));
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
    use std::sync::Arc;

    use super::{admin_apps, declare_on, parse, ConfigStore};

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

    fn parsed(argv: &[&str]) -> Result<super::Args, String> {
        parse(argv.iter().map(|a| (*a).to_owned()))
    }

    /// #80 S1a: `--declare` runs no planner either, so `--failover` with it is refused; plain
    /// `--declare` with its count parses. Unit.
    #[test]
    fn s1a_declare_rejects_failover() {
        assert_eq!(
            parsed(&["--declare", "--failover"]).map(|_| ()),
            Err("--failover runs only in a node process, not with --declare".to_owned())
        );
        assert!(parsed(&["--declare", "--partitions", "8"])
            .is_ok_and(|args| args.declare && args.partitions == Some(8) && !args.failover));
    }

    /// PR #138 review: each planner knob with `--declare` is refused, with `--failover` (no
    /// planner runs) and without it (an orphan knob). Unit.
    #[test]
    fn s1a_declare_rejects_each_planner_knob() {
        for knob in [
            ["--failover-silent-ms", "2000"],
            ["--failover-max-concurrent", "2"],
        ] {
            assert_eq!(
                parsed(&["--declare", "--failover", knob[0], knob[1]]).map(|_| ()),
                Err("--failover runs only in a node process, not with --declare".to_owned()),
                "{knob:?} with --failover"
            );
            assert_eq!(
                parsed(&["--declare", knob[0], knob[1]]).map(|_| ()),
                Err(format!("{} needs --failover", knob[0])),
                "{knob:?} alone"
            );
        }
    }

    /// PR #138 review: `--declare` refuses at parse time the five mode and copy flags
    /// `declare_refusal` names: three it would ignore (`--node`, `--dir`, `--live-readd`) and
    /// two (`--form`, `--gen-certs`) that would run instead of declaring. Unit.
    /// Cut: drop `declare_refusal` from `parse`; every pair parses, red.
    #[test]
    fn s1a_declare_rejects_five_mode_and_copy_flags() {
        for (flags, named) in [
            (&["--form"][..], "--form"),
            (&["--gen-certs"][..], "--gen-certs"),
            (&["--node", "2"][..], "--node"),
            (&["--dir", "d"][..], "--dir"),
            (&["--live-readd"][..], "--live-readd"),
        ] {
            let argv: Vec<&str> = ["--declare"].iter().chain(flags).copied().collect();
            assert_eq!(
                parsed(&argv).map(|_| ()),
                Err(format!(
                    "--declare writes only control records and takes no {named}"
                )),
                "{argv:?}"
            );
        }
    }

    /// PR #138 review: `--declare --nodes` outside 3 to `MAX_NODES`, or not a number, is
    /// refused while parsing; the shared parser still takes 4 and 8 for `--gen-certs`. Unit.
    #[test]
    fn s1a_declare_node_counts_parse_as_gen_certs_does() {
        for bad in ["0", "9", "three"] {
            assert!(
                parsed(&["--declare", "--nodes", bad]).is_err(),
                "--declare --nodes {bad}"
            );
        }
        for good in [4, 8] {
            let text = good.to_string();
            assert!(
                parsed(&["--gen-certs", "--cert-dir", "c", "--nodes", &text])
                    .is_ok_and(|args| args.nodes == Some(good)),
                "--gen-certs --nodes {good}"
            );
        }
    }

    /// PR #138 review: `--declare --nodes 4` parses, reaches `admin::declare` with four nodes
    /// (not the default three), and is refused there with no formation record written. Unit
    /// (<100 ms, an in-memory store).
    /// Cut: `declare_on` builds nodes 1 to 3 whatever `--nodes` says; it declares, red.
    #[config_log::retcd_test]
    fn s1a_declare_four_nodes_reaches_the_refusal_and_writes_nothing() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let args = parsed(&["--declare", "--nodes", "4", "--partitions", "8"]).expect("parses");
        let refused = declare_on(&args, &store, &rt);
        assert!(
            matches!(&refused, Err(text) if text.contains("nodes [1, 2, 3, 4]")
                && text.contains("nothing was written")),
            "{refused:?}"
        );
        assert_eq!(
            rt.block_on(rdb_api::admin::read_formation(&*store))
                .expect("the store answers"),
            None,
            "no formation record"
        );
        let line = declare_on(
            &parsed(&["--declare", "--partitions", "8"]).expect("parses"),
            &store,
            &rt,
        )
        .expect("three nodes declare");
        assert!(line.starts_with("rdb_node declared "), "{line}");
    }

    /// PR #119 review: `--failover` with `--form` is a parse error, since forming runs no
    /// planner; plain `--form` parses. Unit.
    /// Cut: drop the mode check in `failover_refusal`; the pair parses, red.
    #[test]
    fn own81_form_rejects_failover() {
        assert_eq!(
            parsed(&["--form", "--failover"]).map(|_| ()),
            Err("--failover runs only in a node process, not with --form".to_owned())
        );
        assert!(parsed(&["--form"]).is_ok_and(|args| args.form && !args.failover));
    }

    /// PR #119 review: `--failover` with `--gen-certs` is a parse error; plain `--gen-certs`
    /// parses. Unit.
    /// Cut: as above.
    #[test]
    fn own81_certs_reject_failover() {
        assert_eq!(
            parsed(&["--gen-certs", "--failover"]).map(|_| ()),
            Err("--failover runs only in a node process, not with --gen-certs".to_owned())
        );
        assert!(parsed(&["--gen-certs"]).is_ok_and(|args| args.gen_certs));
    }

    /// PR #119 review: `--failover-silent-ms` without `--failover` is a parse error; with it,
    /// the knob is taken. Unit.
    /// Cut: drop the knob check in `failover_refusal`; the orphan parses, red.
    #[test]
    fn own81_silent_knob_needs_failover() {
        assert_eq!(
            parsed(&["--failover-silent-ms", "3000"]).map(|_| ()),
            Err("--failover-silent-ms needs --failover".to_owned())
        );
        let args = parsed(&["--failover", "--failover-silent-ms", "3000"]).expect("parses");
        assert_eq!(
            super::failover(&args).map(|f| f.silent_after),
            Some(std::time::Duration::from_millis(3_000))
        );
    }

    /// PR #119 review: `--failover-max-concurrent` without `--failover` is a parse error; with
    /// it, the knob is taken. Unit.
    /// Cut: as above.
    #[test]
    fn own81_concurrency_knob_needs_failover() {
        assert_eq!(
            parsed(&["--failover-max-concurrent", "4"]).map(|_| ()),
            Err("--failover-max-concurrent needs --failover".to_owned())
        );
        let args = parsed(&["--failover-max-concurrent", "4", "--failover"]).expect("parses");
        assert_eq!(super::failover(&args).map(|f| f.max_concurrent), Some(4));
    }

    /// PR #119 review: a zero `--failover-max-concurrent` and a `--failover-silent-ms` under
    /// one clock-probe period are refused while parsing, before the process writes its boot
    /// or listens; exactly 1000 ms is taken. Unit.
    /// Cut: drop either parse-time check, red (the flags parse).
    #[config_log::retcd_test]
    fn own81_failover_knobs_refused_while_parsing() {
        assert_eq!(
            parsed(&["--failover", "--failover-max-concurrent", "0"]).map(|_| ()),
            Err("--failover-max-concurrent wants a count of at least 1".to_owned())
        );
        for millis in ["0", "999"] {
            assert_eq!(
                parsed(&["--failover", "--failover-silent-ms", millis]).map(|_| ()),
                Err(format!(
                    "--failover-silent-ms {millis} is under one clock-probe period (1000 ms): \
                     a healthy link would read as silent"
                ))
            );
        }
        let args = parsed(&["--failover", "--failover-silent-ms", "1000"]).expect("parses");
        assert_eq!(
            super::failover(&args).map(|f| f.silent_after),
            Some(std::time::Duration::from_millis(1_000))
        );
    }

    /// Runs `main`'s path on a node process's arguments plus `extra`, with node 1's data
    /// directory, cert dir and log dir under a fresh root, and `--listen` on a port this row
    /// holds, and asserts it stopped as a bad argument (exit 2) before any startup effect: no
    /// boot written (`next_boot` creates `--dir`), and no bind tried (the held port would make
    /// a bind fail, exit 1).
    fn refused_before_start(extra: &[&str]) {
        let root = config_testkit::fs::temp_dir();
        let held = std::net::TcpListener::bind("127.0.0.1:0").expect("a free port");
        let listen = held.local_addr().expect("bound").to_string();
        let dir = root.path().join("node-1");
        let path = |p: &std::path::Path| p.display().to_string();
        let mut argv: Vec<String> = [
            "--node".to_owned(),
            "1".to_owned(),
            "--dir".to_owned(),
            path(&dir),
            "--listen".to_owned(),
            listen.clone(),
            "--peers".to_owned(),
            format!("2={listen},3={listen}"),
            "--control".to_owned(),
            format!("http://{listen}"),
            "--cert-dir".to_owned(),
            path(&root.path().join("certs")),
            "--log-dir".to_owned(),
            path(&root.path().join("logs")),
        ]
        .into();
        argv.extend(extra.iter().map(|a| (*a).to_owned()));
        let code = super::run(argv.into_iter(), |_| Ok(None));
        assert_eq!(code, std::process::ExitCode::from(2), "{extra:?}: exit 2");
        assert!(
            !dir.exists(),
            "{extra:?}: a boot was written in {}",
            dir.display()
        );
        drop(held);
    }

    /// PR #119 review: a zero `--failover-max-concurrent` stops the node process before it
    /// writes its boot or binds `--listen`, and the parse error is the one the parser row pins.
    /// Unit.
    /// Cut: move the zero check from `parse` to `serve`, after `next_boot` and the bind; the
    /// boot is written and the bind fails (exit 1), red.
    #[config_log::retcd_test]
    fn own81_zero_concurrency_refused_before_boot_or_listen() {
        refused_before_start(&["--failover", "--failover-max-concurrent", "0"]);
        assert_eq!(
            parsed(&["--failover", "--failover-max-concurrent", "0"]).map(|_| ()),
            Err("--failover-max-concurrent wants a count of at least 1".to_owned())
        );
    }

    /// PR #119 review: a `--failover-silent-ms` of 0 or 999 stops the node process before it
    /// writes its boot or binds `--listen`; exactly 1000 ms is taken. Unit.
    /// Cut: move the floor check from `parse` to `serve`, after `next_boot`; red.
    #[config_log::retcd_test]
    fn own81_short_silence_refused_before_boot_or_listen() {
        for millis in ["0", "999"] {
            refused_before_start(&["--failover", "--failover-silent-ms", millis]);
        }
        let args = parsed(&["--failover", "--failover-silent-ms", "1000"]).expect("parses");
        assert_eq!(
            super::failover(&args).map(|f| f.silent_after),
            Some(std::time::Duration::from_millis(1_000))
        );
    }

    /// PR #119 review: a value that is no count, or a negative one, is refused for
    /// `--failover-max-concurrent`; one that is no number, or past `u64`, for
    /// `--failover-silent-ms`. Each with the flag's existing parse error. Unit.
    #[config_log::retcd_test]
    fn own81_failover_knob_values_that_are_no_number_are_refused() {
        for value in ["abc", "-1"] {
            assert_eq!(
                parsed(&["--failover", "--failover-max-concurrent", value]).map(|_| ()),
                Err("--failover-max-concurrent wants a count of at least 1".to_owned()),
                "{value}"
            );
        }
        for value in ["abc", "18446744073709551616"] {
            assert_eq!(
                parsed(&["--failover", "--failover-silent-ms", value]).map(|_| ()),
                Err("--failover-silent-ms wants whole milliseconds".to_owned()),
                "{value}"
            );
        }
    }

    /// PR #119 review: `--help` prints `USAGE`, and it lists every command a `--faults` file
    /// takes; each listed command is one the fault file accepts. Unit.
    /// Cut: drop the fault-command line from `USAGE`; red.
    #[config_log::retcd_test]
    fn own81_help_lists_the_fault_commands() {
        assert_eq!(parsed(&["--help"]).map(|_| ()), Err(String::new()));
        let commands = [
            "hold N",
            "hold all",
            "drop-request N",
            "drop-reply N",
            "cut-control",
            "mute",
        ];
        for command in commands {
            assert!(
                super::USAGE.contains(command),
                "{command}: {}",
                super::USAGE
            );
            let (_, refused) = rdb_api::net::faults::parse(&command.replace('N', "2"));
            assert!(refused.is_empty(), "{command}: {refused:?}");
        }
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
