//! `rdb_dev`: three rDB nodes and three rEtcd voters in one process, driven line by line.
//!
//! ```text
//! cargo run -p rdb-api --example rdb_dev -- [--dir D] [--log-dir L] [--script F] [--keep]
//!                                           [--hold 1-2,1-3]
//! ```
//!
//! Commands are read from `--script` or stdin, one per line (`#` starts a comment):
//!
//! ```text
//! put <object> <value> [--if-version N]   write a byte string
//! retry                                   send the last put's request again, unchanged
//! get <object>                            read at the publication barrier
//! status <request> [<generation>]         what became of a request this session sent
//! nodes                                   each node's view of partition 1
//! control                                 the partitions/ and grants/ records in rEtcd
//! link hold|heal <a>-<b> | link heal all | links
//! wait recovered|ready [<seconds>]        block until the partition gets there
//! sleep <millis>
//! quit
//! ```
//!
//! While it runs, a poller prints the partition's progress as it changes: `recovered gen=N`,
//! `waiting for write protection (...)`, and `ready` once node 1's L1 admits writes. A put
//! before `ready` is refused, never left hanging.
//!
//! The JSONL log goes to `<log-dir>/rdb_dev.jsonl`; its path is printed to stderr as `log=`.

use std::io::{BufRead, Write as _};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use config_core::{ConfigStore, ListRequest};
use config_testkit::cluster::{Cluster, StorageKind};
use rdb_api::host::NodeStatus;
use rdb_api::{Db, DbConfig, PutError, Timeouts};
use rdb_core::authority::grant::GrantRecord;
use rdb_core::authority::partition::PartitionRecord;
use rdb_core::contracts::ids::{Generation, NodeId, RequestId};
use rdb_core::contracts::txn::TxnRequest;

#[derive(Debug)]
struct Args {
    dir: PathBuf,
    log_dir: PathBuf,
    script: Option<PathBuf>,
    keep: bool,
    hold: Vec<(NodeId, NodeId)>,
}

fn parse(mut args: impl Iterator<Item = String>) -> Result<Args, String> {
    let pid = std::process::id();
    let mut parsed = Args {
        dir: PathBuf::from(format!("C:/rdb_test_data/m9/dev-{pid}")),
        log_dir: PathBuf::from(format!("C:/rdb_test_data/m9/logs-{pid}")),
        script: None,
        keep: false,
        hold: Vec::new(),
    };
    while let Some(arg) = args.next() {
        let mut value = |name: &str| args.next().ok_or_else(|| format!("{name} needs a value"));
        match arg.as_str() {
            "--dir" => parsed.dir = PathBuf::from(value("--dir")?),
            "--log-dir" => parsed.log_dir = PathBuf::from(value("--log-dir")?),
            "--script" => parsed.script = Some(PathBuf::from(value("--script")?)),
            "--keep" => parsed.keep = true,
            "--hold" => {
                for pair in value("--hold")?.split(',') {
                    parsed.hold.push(parse_pair(pair)?);
                }
            }
            other => return Err(format!("unknown argument {other:?}")),
        }
    }
    Ok(parsed)
}

fn parse_pair(text: &str) -> Result<(NodeId, NodeId), String> {
    let (a, b) = text
        .split_once('-')
        .ok_or_else(|| format!("{text:?}: a link is <a>-<b>"))?;
    let node = |s: &str| -> Result<NodeId, String> {
        match s.trim().parse::<u32>() {
            Ok(n @ 1..=3) => Ok(NodeId(n)),
            _ => Err(format!("{s:?}: a node is 1, 2 or 3")),
        }
    };
    let (a, b) = (node(a)?, node(b)?);
    if a == b {
        return Err(format!("{text:?}: a link joins two different nodes"));
    }
    Ok((a, b))
}

fn main() -> ExitCode {
    let args = match parse(std::env::args().skip(1)) {
        Ok(args) => args,
        Err(message) => {
            eprintln!("rdb_dev: {message}");
            return ExitCode::from(2);
        }
    };
    if args.dir.exists() && std::fs::read_dir(&args.dir).is_ok_and(|mut d| d.next().is_some()) {
        eprintln!(
            "rdb_dev: {} is not empty; pass a new --dir",
            args.dir.display()
        );
        return ExitCode::from(2);
    }
    let mut config = config_log::LogConfig::for_application("rdb_dev");
    config.dir.clone_from(&args.log_dir);
    config.file_name = "rdb_dev.jsonl".to_string();
    let guard = match config_log::init(config) {
        Ok(guard) => guard,
        Err(e) => {
            eprintln!("rdb_dev: log init: {e}");
            return ExitCode::from(2);
        }
    };
    eprintln!("log={}", args.log_dir.join("rdb_dev.jsonl").display());
    // The gate's port range keeps the voters' port-0 binds out of the host's busy dynamic pool,
    // and the voters' data goes under --dir, never %TEMP%.
    if std::env::var_os("RETCD_TEST_PORT_RANGE").is_none() {
        std::env::set_var("RETCD_TEST_PORT_RANGE", "20000-26999");
    }
    if std::env::var_os("RETCD_TEST_DATA_DIR").is_none() {
        std::env::set_var("RETCD_TEST_DATA_DIR", args.dir.join("control"));
    }
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
        Err(e) => {
            eprintln!("rdb_dev: runtime: {e}");
            return ExitCode::from(1);
        }
    };
    let cluster = rt.block_on(Cluster::start(3, StorageKind::ROCKS));
    let leader = rt.block_on(cluster.leader());
    let store = cluster.client(leader);
    say(&format!("control up: 3 voters, leader {leader:?}"));
    tracing::info!(dir = %args.dir.display(), holds = ?args.hold, "rdb_dev_start");
    let config = DbConfig {
        dir: args.dir.clone(),
        hold: args.hold.clone(),
        timeouts: Timeouts::default(),
    };
    let mut db = match rt.block_on(Db::open(config, Arc::clone(&store), rt.handle().clone())) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("rdb_dev: open: {e}");
            rt.block_on(cluster.shutdown());
            return ExitCode::from(1);
        }
    };
    say("bootstrap: partition 1 recovery started");
    let progress = Progress::default();
    let stop = AtomicBool::new(false);
    let code = std::thread::scope(|scope| {
        scope.spawn(|| poll(&db, &progress, &stop));
        let code = repl(args, &db, &rt, &*store, &progress);
        stop.store(true, Ordering::Relaxed);
        code
    });
    db.shutdown();
    // The direct client holds the voter's store open: drop every handle on it before the
    // cluster closes its stores, or their RocksDB files outlive the run.
    drop(db);
    drop(store);
    rt.block_on(cluster.shutdown());
    drop(rt);
    if !args.keep {
        remove(&args.dir);
    }
    code
}

/// Remove `dir`, retrying for up to 2 s while Windows still holds a file the voters closed.
fn remove(dir: &std::path::Path) {
    let started = Instant::now();
    loop {
        match std::fs::remove_dir_all(dir) {
            Ok(()) => return,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
            Err(e) if started.elapsed() >= Duration::from_secs(2) => {
                eprintln!("rdb_dev: remove {}: {e}", dir.display());
                return;
            }
            Err(_) => std::thread::sleep(Duration::from_millis(100)),
        }
    }
}

fn say(line: &str) {
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "{line}");
    let _ = out.flush();
}

/// What the poller has seen.
#[derive(Debug, Default)]
struct Progress {
    inner: Mutex<Seen>,
}

#[derive(Debug, Default, Clone)]
struct Seen {
    recovered: Option<Generation>,
    ready: bool,
    waiting: Option<String>,
    faults: Vec<String>,
}

impl Progress {
    fn seen(&self) -> Seen {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

/// Print the partition's progress lines as it changes, every 100 ms.
fn poll(db: &Db, progress: &Progress, stop: &AtomicBool) {
    while !stop.load(Ordering::Relaxed) {
        let nodes = db.node_status();
        let mut seen = progress
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for node in &nodes {
            if let Some(fault) = &node.fault {
                let line = format!("fault node={} {fault}", node.node.0);
                if !seen.faults.contains(&line) {
                    say(&line);
                    seen.faults.push(line);
                }
            }
        }
        if let Some(owner) = nodes.iter().find(|n| n.node == NodeId(1)) {
            if owner.recovered.is_some() && owner.recovered != seen.recovered {
                seen.recovered = owner.recovered;
                say(&format!(
                    "recovered gen={}",
                    owner.recovered.map_or(0, |g| g.0)
                ));
            }
            let admits = owner.admits == Some(true);
            if admits != seen.ready {
                seen.ready = admits;
                say(if admits {
                    "ready"
                } else {
                    "not ready: write protection lost"
                });
            }
            if seen.recovered.is_some() && !admits {
                let why = waiting_line(owner);
                if seen.waiting.as_ref() != Some(&why) {
                    say(&why);
                    seen.waiting = Some(why);
                }
            }
        }
        drop(seen);
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn waiting_line(owner: &NodeStatus) -> String {
    format!(
        "waiting for write protection (l1={} authority={} applied={} durable={})",
        owner.protection.as_deref().unwrap_or("inert"),
        owner.authority,
        owner.applied,
        owner.durable
    )
}

/// One command per line; the exit code is 1 when any command failed to parse.
fn repl(
    args: &Args,
    db: &Db,
    rt: &tokio::runtime::Runtime,
    store: &dyn ConfigStore,
    progress: &Progress,
) -> ExitCode {
    let input: Box<dyn BufRead> = match &args.script {
        Some(path) => match std::fs::File::open(path) {
            Ok(file) => Box::new(std::io::BufReader::new(file)),
            Err(e) => {
                eprintln!("rdb_dev: --script {}: {e}", path.display());
                return ExitCode::from(2);
            }
        },
        None => Box::new(std::io::BufReader::new(std::io::stdin())),
    };
    let mut last: Option<TxnRequest> = None;
    let mut bad = false;
    for line in input.lines() {
        let Ok(line) = line else { break };
        let line = line.split('#').next().unwrap_or("").trim().to_owned();
        if line.is_empty() {
            continue;
        }
        if args.script.is_some() {
            say(&format!("> {line}"));
        }
        tracing::info!(command = %line, "repl_command");
        let words: Vec<&str> = line.split_whitespace().collect();
        let outcome = match words.as_slice() {
            ["quit" | "exit"] => break,
            ["put", object, value, rest @ ..] => match parse_if_version(rest) {
                Ok(if_version) => {
                    put_line(
                        db.put(object.as_bytes(), value.as_bytes(), if_version),
                        &mut last,
                    );
                    Ok(())
                }
                Err(e) => Err(e),
            },
            ["retry"] => match last.clone() {
                Some(request) => {
                    put_line(db.resend(request), &mut last);
                    Ok(())
                }
                None => Err("no put to retry yet".to_owned()),
            },
            ["get", object] => {
                get_line(db, object.as_bytes());
                Ok(())
            }
            ["status", request, rest @ ..] => status_line(db, request, rest),
            ["nodes"] => {
                for node in db.node_status() {
                    say(&node_line(&node));
                }
                Ok(())
            }
            ["control"] => {
                control_lines(rt, store);
                Ok(())
            }
            ["links"] => {
                say(&format!(
                    "held={:?}",
                    db.links()
                        .held()
                        .iter()
                        .map(|(a, b)| format!("{}-{}", a.0, b.0))
                        .collect::<Vec<_>>()
                ));
                Ok(())
            }
            ["link", "heal", "all"] => {
                say(&format!("healed delivered={}", db.links().heal_all()));
                Ok(())
            }
            ["link", verb @ ("hold" | "heal"), pair] => parse_pair(pair).map(|(a, b)| {
                if *verb == "hold" {
                    db.links().hold(a, b);
                    say(&format!("held {}-{}", a.0, b.0));
                } else {
                    say(&format!(
                        "healed {}-{} delivered={}",
                        a.0,
                        b.0,
                        db.links().heal(a, b)
                    ));
                }
            }),
            ["wait", what @ ("recovered" | "ready"), rest @ ..] => {
                let secs = rest.first().map_or(Ok(30), |s| s.parse::<u64>());
                match secs {
                    Ok(secs) => {
                        wait_for(progress, what, Duration::from_secs(secs));
                        Ok(())
                    }
                    Err(_) => Err("wait takes whole seconds".to_owned()),
                }
            }
            ["sleep", millis] => match millis.parse::<u64>() {
                Ok(ms) => {
                    std::thread::sleep(Duration::from_millis(ms));
                    Ok(())
                }
                Err(_) => Err("sleep takes milliseconds".to_owned()),
            },
            _ => Err(format!("unknown command {line:?}")),
        };
        if let Err(e) = outcome {
            say(&format!("usage: {e}"));
            bad = true;
        }
    }
    if bad {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn parse_if_version(rest: &[&str]) -> Result<Option<u64>, String> {
    match rest {
        [] => Ok(None),
        ["--if-version", n] => n
            .parse()
            .map(Some)
            .map_err(|_| "--if-version takes a number".to_owned()),
        _ => Err("put <object> <value> [--if-version N]".to_owned()),
    }
}

fn put_line(answer: Result<rdb_api::PutOk, PutError>, last: &mut Option<TxnRequest>) {
    match answer {
        Ok(ok) => say(&format!(
            "ok p=1 gen={} seq={} {:?} request={}",
            ok.generation.0, ok.seq.0, ok.durability, ok.request.0
        )),
        Err(PutError { error, request }) => {
            let id = request
                .as_ref()
                .map_or_else(|| "-".to_owned(), |r| r.identity.request.0.to_string());
            say(&format!(
                "err {} retry={:?} no_mutation={} request={id} ({})",
                error.name(),
                error.retry,
                error.no_mutation,
                error.detail
            ));
            if let Some(request) = request {
                *last = Some(*request);
            }
        }
    }
}

fn get_line(db: &Db, object: &[u8]) {
    match db.get(object) {
        Ok(ok) => match ok.value {
            Some((version, bytes)) => say(&format!(
                "value={} version={version} gen={} at={}{}",
                String::from_utf8_lossy(&bytes),
                ok.generation.0,
                ok.at.0,
                if ok.waited { " waited" } else { "" }
            )),
            None => say(&format!("absent gen={} at={}", ok.generation.0, ok.at.0)),
        },
        Err(error) => say(&format!("err {} ({})", error.name(), error.detail)),
    }
}

fn status_line(db: &Db, request: &str, rest: &[&str]) -> Result<(), String> {
    let request = request
        .parse::<u64>()
        .map_err(|_| "status <request> [<generation>]".to_owned())?;
    let generation = match rest {
        [] => None,
        [g] => Some(Generation(
            g.parse()
                .map_err(|_| "a generation is a number".to_owned())?,
        )),
        _ => return Err("status <request> [<generation>]".to_owned()),
    };
    match db.status(RequestId(request), generation) {
        Ok(status) => say(&format!("status {status:?}")),
        Err(error) => say(&format!("err {} ({})", error.name(), error.detail)),
    }
    Ok(())
}

fn node_line(node: &NodeStatus) -> String {
    format!(
        "node={} gen={} epoch={} authority={} recovery={} recovered={} role={} l1={} admits={} applied={} durable={} published={}{}",
        node.node.0,
        node.generation.0,
        node.owner_epoch.0,
        node.authority,
        node.recovery.as_deref().unwrap_or("-"),
        node.recovered.map_or_else(|| "-".to_owned(), |g| g.0.to_string()),
        node.role,
        node.protection.as_deref().unwrap_or("inert"),
        node.admits.map_or_else(|| "-".to_owned(), |a| a.to_string()),
        node.applied,
        node.durable,
        node.published
            .map_or_else(|| "-".to_owned(), |(g, s)| format!("{}@{}", s.0, g.0)),
        node.fault.as_ref().map_or_else(String::new, |f| format!(" FAULT={f}")),
    )
}

fn control_lines(rt: &tokio::runtime::Runtime, store: &dyn ConfigStore) {
    for prefix in ["partitions/", "grants/"] {
        let listed = rt.block_on(store.list(ListRequest {
            prefix: Bytes::from_static(prefix.as_bytes()),
            max_items: 0,
            max_bytes: 0,
        }));
        match listed {
            Ok(response) => {
                for record in response.records {
                    let key = String::from_utf8_lossy(&record.key).into_owned();
                    let body = if prefix == "partitions/" {
                        PartitionRecord::decode(&record.value).map(|r| format!("{r:?}"))
                    } else {
                        GrantRecord::decode(&record.value).map(|r| format!("{r:?}"))
                    };
                    say(&format!(
                        "{key} rev={} {}",
                        record.mod_revision,
                        body.unwrap_or_else(|| "<undecodable>".to_owned())
                    ));
                }
            }
            Err(e) => say(&format!("err control list {prefix}: {e}")),
        }
    }
}

fn wait_for(progress: &Progress, what: &str, limit: Duration) {
    let started = Instant::now();
    loop {
        let seen = progress.seen();
        let reached = match what {
            "recovered" => seen.recovered.is_some(),
            _ => seen.ready,
        };
        if reached {
            say(&format!(
                "reached {what} in {}ms",
                started.elapsed().as_millis()
            ));
            return;
        }
        if started.elapsed() >= limit {
            say(&format!(
                "timeout waiting for {what} after {}s",
                limit.as_secs()
            ));
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}
