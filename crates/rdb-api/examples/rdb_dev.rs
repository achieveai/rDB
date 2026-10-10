//! `rdb_dev`: three rDB nodes and three rEtcd voters in one process, driven line by line.
//!
//! ```text
//! cargo run -p rdb-api --example rdb_dev -- [--dir D] [--log-dir L] [--script F] [--keep]
//!                                           [--hold 1-2,1-3] [--put-timeout-ms N]
//!                                           [--dedup-cap N]
//! ```
//!
//! `--dedup-cap N` lowers T1's dedup cap, the retained answers allowed before a new write is
//! `OVERLOADED`, so a full index can be reached by hand. Above 65536 the open is refused.
//!
//! Commands are read from `--script` or stdin, one per line (`#` starts a comment). Words split
//! on spaces; `"..."` makes one word, so `""` is an empty object or value:
//!
//! ```text
//! put <object> <value> [--if-version N]   write a byte string
//! put& <object> <value> [--if-version N]  the same put in the background: prints `bg
//!                                         <object>: queued` and returns at once, and `bg
//!                                         <object>: <answer>` when it lands; not remembered
//!                                         for `retry`. Queued is not sent: `sleep` before a
//!                                         next command that needs the put in flight
//! txn [--deadline-ms N] <op>...           write every op as one transaction, or none. An op
//!                                         is [<group>:]<object>=<value>[@<version>]: the
//!                                         object ends at the first `=`, a group is only a
//!                                         leading `digits:` (default 1), and a version only a
//!                                         trailing `@digits`. The deadline defaults to the put
//!                                         timeout; past 30000 it is refused
//! txn& [--deadline-ms N] <op>...          the same txn in the background, as `put&`: `bg txn:
//!                                         queued`, then `bg txn: <answer>`; not remembered
//! retry [<request>]                       send a put's or txn's request again, unchanged: the
//!                                         latest one's, or the one this session sent as
//!                                         <request>
//! retry [<request>] --deadline-ms N       the same request with a fresh deadline of N ms
//! retry [<request>] --payload <value>     the same put and request id with another value,
//!                                         compiled afresh (REQUEST_ID_REUSE); not remembered.
//!                                         A put's only, never a txn's
//! get <object>                            read at the publication barrier
//! get <object> --previous                 read the previously published view at once, never
//!                                         waiting for a write in flight (ReadPrevious)
//! status <request> [<generation>]         what became of a request this session sent
//! nodes                                   each node's view of partition 1
//! control                                 the partitions/ and grants/ records in rEtcd, read
//!                                         from the rEtcd leader now, never through the Db's
//!                                         store
//! control voters                          `voters leader=N running=[..] bound=N`: the rEtcd
//!                                         leader, the running voters, and the voter the Db's
//!                                         control store calls now
//! control stop <voter>                    stop one rEtcd voter (1, 2 or 3). The Db's store
//!                                         moves off it first; refused if no other runs
//! control start <voter>                   start a stopped voter again on its own data
//! control gap                             compact rEtcd through its current revision, then
//!                                         drop the Db's watches: a re-watch from an older
//!                                         cursor is refused `RevisionCompacted` and reloads
//! control drop-watches                    end the Db's watch streams; they re-watch from
//!                                         their cursors with no reload
//! control fail list <unavailable|deadline|notleader|invalid|off>
//!                                         every list the Db's store sends answers that error,
//!                                         until `off`. `control` reads are not affected
//! link hold|heal <a>-<b> | link heal all | links
//! wait recovered|ready [<seconds>]        block until the partition gets there
//! sleep <millis>
//! quit
//! ```
//!
//! While it runs, a poller prints the partition's progress as it changes: `recovered gen=N`,
//! `waiting for write protection (...)`, and `ready` once node 1's L1 admits writes. A put
//! before `ready` is refused, never left hanging. A recovery committed below `Active` that
//! does not activate prints `stalled node=N recovery_rebuild_stalled ...`, and again whenever
//! the line changes:
//!
//! - when F1 pinned its rebuild, at F1's rebuild deadline (the discovery window, ~2 s) with
//!   `unproven=[..]`, the copies that have not proved the pinned point;
//! - when F1 never pinned it, after `REBUILD_PIN_WAIT_MILLIS` (5 s), with `waited_ms=` and
//!   "F1 never pinned its rebuild".
//!
//! The line clears when the partition activates, or when a newer recovery starts watching its
//! own rebuild, and the poller then prints `stall cleared node=N recovery=.. recovered=..
//! admits=..`.
//!
//! The Db's control store follows the rEtcd leader (`support/hostile_store.rs`): a call a
//! voter answers `NotLeader`, or refuses because it stopped, sends the next call to the leader
//! then, logged as `control_store_rebind`.
//!
//! The JSONL log goes to `<log-dir>/rdb_dev.jsonl`; its path is printed to stderr as `log=`.

#[path = "support/hostile_store.rs"]
mod hostile_store;

use std::io::{BufRead, Write as _};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use config_core::{ConfigStore, GetRequest, ListRequest, NodeId as VoterId};
use config_testkit::cluster::{Cluster, StorageKind};
use config_testkit::TestTimers;
use rdb_api::host::NodeStatus;
use rdb_api::{Db, DbConfig, OpenError, PutError, Timeouts, TxnPut};
use rdb_core::authority::grant::GrantRecord;
use rdb_core::authority::partition::PartitionRecord;
use rdb_core::contracts::ids::{AffinityId, Generation, NodeId, RequestId};
use rdb_core::contracts::txn::TxnRequest;

use hostile_store::{HostileStore, ListFault};

/// The control voters' Raft timers. Faster than the harness default, whose failover can outlast
/// A1's write horizon (walk 1L): `tests::control_failover_millis` is 1000 ms here, 3000 ms there.
const CONTROL_TIMERS: TestTimers = TestTimers {
    heartbeat: Duration::from_millis(100),
    election_timeout_min: Duration::from_millis(300),
    election_timeout_max: Duration::from_millis(500),
};

#[derive(Debug)]
struct Args {
    dir: PathBuf,
    log_dir: PathBuf,
    script: Option<PathBuf>,
    keep: bool,
    hold: Vec<(NodeId, NodeId)>,
    /// How long a put waits for its answer. Short values reach the Db's own timeout,
    /// before the kernel answers (walk w24).
    put_timeout: Option<std::time::Duration>,
    /// T1's dedup cap, lowered as a dev budget.
    dedup_cap: Option<usize>,
}

fn parse(mut args: impl Iterator<Item = String>) -> Result<Args, String> {
    let pid = std::process::id();
    let mut parsed = Args {
        dir: PathBuf::from(format!("C:/rdb_test_data/m9/dev-{pid}")),
        log_dir: PathBuf::from(format!("C:/rdb_test_data/m9/logs-{pid}")),
        script: None,
        keep: false,
        hold: Vec::new(),
        put_timeout: None,
        dedup_cap: None,
    };
    while let Some(arg) = args.next() {
        let mut value = |name: &str| args.next().ok_or_else(|| format!("{name} needs a value"));
        match arg.as_str() {
            "--dir" => parsed.dir = PathBuf::from(value("--dir")?),
            "--log-dir" => parsed.log_dir = PathBuf::from(value("--log-dir")?),
            "--script" => parsed.script = Some(PathBuf::from(value("--script")?)),
            "--keep" => parsed.keep = true,
            "--put-timeout-ms" => {
                let text = value("--put-timeout-ms")?;
                let millis = text
                    .parse::<u64>()
                    .map_err(|_| format!("--put-timeout-ms takes a number, not {text:?}"))?;
                parsed.put_timeout = Some(std::time::Duration::from_millis(millis));
            }
            "--dedup-cap" => {
                let text = value("--dedup-cap")?;
                parsed.dedup_cap = Some(
                    text.parse::<usize>()
                        .map_err(|_| format!("--dedup-cap takes a number, not {text:?}"))?,
                );
            }
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
    let code = run(&args, Db::open);
    drop(guard);
    code
}

/// The run, with `open` in place of [`Db::open`] so a row can fail it.
fn run<Open, Opening>(args: &Args, open: Open) -> ExitCode
where
    Open: FnOnce(DbConfig, Arc<dyn ConfigStore>, tokio::runtime::Handle) -> Opening,
    Opening: std::future::Future<Output = Result<Db, OpenError>>,
{
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
    let config = DbConfig {
        dir: args.dir.clone(),
        hold: args.hold.clone(),
        timeouts: Timeouts {
            put: args.put_timeout.unwrap_or(Timeouts::default().put),
            ..Timeouts::default()
        },
        dedup_cap: args.dedup_cap,
    };
    // Before the voters start (PC-2): a refused option starts nothing.
    if let Err(e) = config.check() {
        eprintln!("rdb_dev: open: {e}");
        return ExitCode::from(1);
    }
    let cluster = Arc::new(
        rt.block_on(
            Cluster::builder()
                .nodes(3)
                .storage(StorageKind::ROCKS)
                .timers(CONTROL_TIMERS)
                .start(),
        ),
    );
    let leader = rt.block_on(cluster.leader());
    let hostile = Arc::new(HostileStore::new(&cluster, leader));
    let store: Arc<dyn ConfigStore> = Arc::clone(&hostile) as Arc<dyn ConfigStore>;
    say(&format!("control up: 3 voters, leader {leader:?}"));
    tracing::info!(dir = %args.dir.display(), holds = ?args.hold, "rdb_dev_start");
    let mut db = match rt.block_on(open(config, Arc::clone(&store), rt.handle().clone())) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("rdb_dev: open: {e}");
            drop(hostile);
            close(store, cluster, rt, args);
            return ExitCode::from(1);
        }
    };
    say("bootstrap: partition 1 recovery started");
    let progress = Progress::default();
    let stop = AtomicBool::new(false);
    let control = Control {
        cluster: &cluster,
        store: &hostile,
    };
    let code = std::thread::scope(|scope| {
        scope.spawn(|| poll(&db, &progress, &stop));
        let code = repl(scope, args, &db, &rt, &control, &progress);
        stop.store(true, Ordering::Relaxed);
        code
    });
    db.shutdown();
    drop(db);
    drop(hostile);
    close(store, cluster, rt, args);
    code
}

/// End the run once the Db is gone, whether it opened or not (F-006). The direct client holds
/// the voter's store open: drop every handle on it before the cluster closes its stores, or
/// their RocksDB files outlive the run. Then `--dir` goes, unless `--keep`.
///
/// The store holds the cluster weakly, so the cluster comes back here unless a call is
/// resolving the leader at this instant; that is waited out for up to 2 s, and past it the
/// voters are not shut down and the run says so.
fn close(
    store: Arc<dyn ConfigStore>,
    cluster: Arc<Cluster>,
    rt: tokio::runtime::Runtime,
    args: &Args,
) {
    drop(store);
    let started = Instant::now();
    let mut shared = cluster;
    let cluster = loop {
        match Arc::try_unwrap(shared) {
            Ok(cluster) => break Some(cluster),
            Err(still) if started.elapsed() >= Duration::from_secs(2) => {
                eprintln!(
                    "rdb_dev: the cluster is still shared ({} handles); voters not shut down",
                    Arc::strong_count(&still)
                );
                break None;
            }
            Err(still) => {
                shared = still;
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    };
    if let Some(cluster) = cluster {
        rt.block_on(cluster.shutdown());
    }
    drop(rt);
    if !args.keep {
        remove(&args.dir);
    }
}

/// What the REPL's `control` verbs reach: the voters, and the Db's store over them.
struct Control<'a> {
    cluster: &'a Arc<Cluster>,
    store: &'a HostileStore,
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
    /// Each node's stall line as last printed, until it clears.
    stalled: std::collections::BTreeMap<u32, String>,
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
            if let Some(line) = stall_change(seen.stalled.get(&node.node.0), node) {
                say(&line);
                match &node.stalled {
                    Some(stalled) => seen.stalled.insert(node.node.0, stalled.clone()),
                    None => seen.stalled.remove(&node.node.0),
                };
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

/// The line to print when a node's stall line changes, given the one printed last. Not a node
/// fault (the node keeps serving), so it is not kept with them: a stall that clears says so,
/// or the last stall line would still read as writes paused after the partition healed.
fn stall_change(printed: Option<&String>, node: &NodeStatus) -> Option<String> {
    match (printed, &node.stalled) {
        (Some(was), Some(now)) if was == now => None,
        (_, Some(now)) => Some(format!("stalled node={} {now}", node.node.0)),
        (Some(_), None) => Some(format!(
            "stall cleared node={} recovery={} recovered={} admits={}",
            node.node.0,
            node.recovery.as_deref().unwrap_or("-"),
            node.recovered
                .map_or_else(|| "-".to_owned(), |g| g.0.to_string()),
            node.admits
                .map_or_else(|| "-".to_owned(), |a| a.to_string()),
        )),
        (None, None) => None,
    }
}

fn waiting_line(owner: &NodeStatus) -> String {
    format!(
        "waiting for write protection (l1={} authority={} {})",
        owner.protection.as_deref().unwrap_or("inert"),
        owner.authority,
        owner.holds.map_or_else(
            || "holds=nothing".to_owned(),
            |h| format!("applied={} durable={}", h.applied, h.durable)
        ),
    )
}

/// `gen= epoch= applied= durable=` of what the node holds, or `holds=nothing` before it holds
/// a lineage: a secondary adopts no generation, so zeros there would read as real (OB2).
fn holds_part(node: &NodeStatus) -> String {
    node.holds.map_or_else(
        || "holds=nothing".to_owned(),
        |h| {
            format!(
                "gen={} epoch={} applied={} durable={}",
                h.generation.0, h.owner_epoch.0, h.applied, h.durable
            )
        },
    )
}

/// One command per line; the exit code is 1 when any command failed to parse, and 2 at once
/// when a line cannot be read. A background put runs on `scope`, so the run waits for it before
/// shutting down.
fn repl<'scope, 'env>(
    scope: &'scope std::thread::Scope<'scope, 'env>,
    args: &Args,
    db: &'env Db,
    rt: &tokio::runtime::Runtime,
    control: &Control<'_>,
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
    let mut sent = Sent::default();
    let mut bad = false;
    let deadline = args.put_timeout.unwrap_or(Timeouts::default().put);
    for line in input.lines() {
        let line = match command(line) {
            Ok(Some(line)) => line,
            Ok(None) => continue,
            Err(e) => {
                tracing::error!(error = %e, "repl_input_failed");
                eprintln!("rdb_dev: {e}");
                return ExitCode::from(2);
            }
        };
        if args.script.is_some() {
            say(&format!("> {line}"));
        }
        tracing::info!(command = %line, "repl_command");
        let words = match split_words(&line) {
            Ok(words) => words,
            Err(e) => {
                say(&format!("usage: {e}"));
                bad = true;
                continue;
            }
        };
        let words: Vec<&str> = words.iter().map(String::as_str).collect();
        let outcome = match words.as_slice() {
            ["quit" | "exit"] => break,
            ["put", object, value, rest @ ..] => match parse_if_version(rest) {
                Ok(if_version) => {
                    let answer = db.put(object.as_bytes(), value.as_bytes(), if_version);
                    let kind = Kind::Put {
                        object: object.as_bytes().to_vec(),
                        if_version,
                    };
                    put_line(answer, Some((&mut sent, kind)));
                    Ok(())
                }
                Err(e) => Err(e),
            },
            ["put&", object, value, rest @ ..] => parse_if_version(rest).map(|if_version| {
                let (object, value) = ((*object).to_owned(), (*value).to_owned());
                say(&format!("bg {object}: queued"));
                scope.spawn(move || {
                    let answer = db.put(object.as_bytes(), value.as_bytes(), if_version);
                    say(&format!("bg {object}: {}", put_text(&answer)));
                });
            }),
            ["txn", rest @ ..] => parse_txn(rest, deadline).map(|(deadline, ops)| {
                let answer = db.txn(&txn_puts(&ops), deadline);
                put_line(answer, Some((&mut sent, Kind::Txn)));
            }),
            ["txn&", rest @ ..] => parse_txn(rest, deadline).map(|(deadline, ops)| {
                say("bg txn: queued");
                scope.spawn(move || {
                    let answer = db.txn(&txn_puts(&ops), deadline);
                    say(&format!("bg txn: {}", put_text(&answer)));
                });
            }),
            ["retry", rest @ .., "--payload", value] => {
                sent.pick(rest).and_then(|(request, kind)| {
                    let id = request.identity.request;
                    let (object, if_version) = payload_put(kind, id)?;
                    put_line(db.put_as(id, &object, value.as_bytes(), if_version), None);
                    Ok(())
                })
            }
            ["retry", rest @ .., "--deadline-ms", millis] => match millis.parse::<u64>() {
                Ok(ms) => sent.pick(rest).map(|(request, _)| {
                    let deadline = Duration::from_millis(ms);
                    let answer = db.resend_within(request, deadline);
                    sent.resent_within(deadline, &answer);
                    put_line(answer, None);
                }),
                Err(_) => Err("retry [<request>] --deadline-ms N: N is milliseconds".to_owned()),
            },
            ["retry", rest @ ..] => sent.pick(rest).map(|(request, kind)| {
                let answer = match kind {
                    Kind::Put { .. } => db.resend(request),
                    // A txn waits as it was sent: its own deadline, not the put timeout.
                    Kind::Txn => {
                        let deadline = Duration::from_millis(request.remaining_millis);
                        db.resend_within(request, deadline)
                    }
                };
                put_line(answer, None);
            }),
            ["get", object] => {
                get_line(db.get(object.as_bytes()));
                Ok(())
            }
            ["get", object, "--previous"] => {
                get_line(db.get_previous(object.as_bytes()));
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
                control_lines(rt, control.cluster);
                Ok(())
            }
            ["control", "voters"] => {
                say(&voters_line(control));
                Ok(())
            }
            ["control", "gap"] => {
                say(&gap(rt, control));
                Ok(())
            }
            ["control", "drop-watches"] => {
                say(&format!("dropped {} watches", control.store.drop_watches()));
                Ok(())
            }
            ["control", "fail", "list", word] => ListFault::parse(word).map(|fault| {
                control.store.fail_lists(fault);
                say(&format!("fail list {word}"));
            }),
            ["control", "fail", ..] => Err(format!("control fail list <{}>", ListFault::WORDS)),
            ["control", verb @ ("stop" | "start"), voter] => parse_voter(control.cluster, voter)
                .map(|voter| {
                    say(&if *verb == "stop" {
                        stop_voter(rt, control, voter)
                    } else {
                        start_voter(rt, control.cluster, voter)
                    });
                }),
            ["control", "stop" | "start", ..] => Err(format!(
                "control stop|start <voter>: exactly one voter, one of {:?}",
                control
                    .cluster
                    .ids()
                    .iter()
                    .map(|id| id.0)
                    .collect::<Vec<_>>()
            )),
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

/// One input line as a command: `None` for a blank or comment-only line. An unreadable line
/// is an error, never the end of input (F-007): a script that is not UTF-8, such as one
/// PowerShell 5.1 wrote as UTF-16LE, otherwise ran nothing and exited 0.
fn command(line: std::io::Result<String>) -> Result<Option<String>, String> {
    let line = line.map_err(|e| format!("input: {e}; scripts must be UTF-8"))?;
    let line = line.split('#').next().unwrap_or("").trim().to_owned();
    Ok((!line.is_empty()).then_some(line))
}

/// Split a command line on spaces; `"..."` is one word, and `""` an empty one.
fn split_words(line: &str) -> Result<Vec<String>, String> {
    let mut words = Vec::new();
    let mut chars = line.chars().peekable();
    while let Some(&c) = chars.peek() {
        if c.is_whitespace() {
            chars.next();
            continue;
        }
        let mut word = String::new();
        if c == '"' {
            chars.next();
            loop {
                match chars.next() {
                    Some('"') => break,
                    Some(c) => word.push(c),
                    None => return Err(format!("unclosed quote in {line:?}")),
                }
            }
        } else {
            while let Some(&c) = chars.peek() {
                if c.is_whitespace() {
                    break;
                }
                word.push(c);
                chars.next();
            }
        }
        words.push(word);
    }
    Ok(words)
}

/// One txn op as typed.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TxnOp {
    group: u64,
    object: String,
    value: String,
    if_version: Option<u64>,
}

fn txn_puts(ops: &[TxnOp]) -> Vec<TxnPut<'_>> {
    ops.iter()
        .map(|op| TxnPut {
            group: AffinityId(op.group),
            object: op.object.as_bytes(),
            value: op.value.as_bytes(),
            if_version: op.if_version,
        })
        .collect()
}

/// `[--deadline-ms N] <op>...`, with `default` as the deadline when none is given. No ops is
/// not a usage error: the kernel refuses an empty transaction, and that is worth seeing.
fn parse_txn(rest: &[&str], default: Duration) -> Result<(Duration, Vec<TxnOp>), String> {
    const USAGE: &str = "txn [--deadline-ms N] [<group>:]<object>=<value>[@<version>]...";
    let (deadline, ops) = match rest {
        ["--deadline-ms", millis, ops @ ..] => (
            Duration::from_millis(
                millis
                    .parse()
                    .map_err(|_| format!("{USAGE}: --deadline-ms takes milliseconds"))?,
            ),
            ops,
        ),
        ops => (default, ops),
    };
    let ops = ops
        .iter()
        .map(|op| parse_op(op).map_err(|e| format!("{USAGE}: {e}")))
        .collect::<Result<_, _>>()?;
    Ok((deadline, ops))
}

/// `[<group>:]<object>=<value>[@<version>]` (S1 ruling A5): the object ends at the first `=`,
/// a group is only a leading `digits:`, and a version only a trailing `@digits`. Anything else
/// stays in the object or the value.
fn parse_op(op: &str) -> Result<TxnOp, String> {
    let (left, right) = op
        .split_once('=')
        .ok_or_else(|| format!("{op:?} has no `=`"))?;
    let (group, object) = match left.split_once(':') {
        Some((digits, object)) if is_digits(digits) => (
            digits
                .parse()
                .map_err(|_| format!("{op:?}: group {digits} is too large"))?,
            object,
        ),
        _ => (1, left),
    };
    let (value, if_version) = match right.rsplit_once('@') {
        Some((value, digits)) if is_digits(digits) => (
            value,
            Some(
                digits
                    .parse()
                    .map_err(|_| format!("{op:?}: version {digits} is too large"))?,
            ),
        ),
        _ => (right, None),
    };
    Ok(TxnOp {
        group,
        object: object.to_owned(),
        value: value.to_owned(),
        if_version,
    })
}

fn is_digits(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit())
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

/// What `retry <id> --payload` re-puts: the object and condition of the put `request` was
/// sent as. A txn has no single object, so it is refused (S1 guard G10).
fn payload_put(kind: Kind, request: RequestId) -> Result<(Vec<u8>, Option<u64>), String> {
    match kind {
        Kind::Put { object, if_version } => Ok((object, if_version)),
        Kind::Txn => Err(format!(
            "retry --payload changes a put's value; request {} was a txn",
            request.0
        )),
    }
}

/// What a recorded request was sent as.
#[derive(Debug, Clone)]
enum Kind {
    /// A put, with the object and condition it was put with, for `retry --payload`.
    Put {
        object: Vec<u8>,
        if_version: Option<u64>,
    },
    /// A txn.
    Txn,
}

/// Every put and txn request this session sent, and which came last. `retry` replays the
/// latest, whatever it answered, or nothing when the latest sent nothing (PC11).
#[derive(Debug, Default)]
struct Sent {
    /// The request, and what it was sent as.
    by_id: std::collections::BTreeMap<u64, (TxnRequest, Kind)>,
    latest: Option<Option<u64>>,
}

impl Sent {
    fn record(&mut self, request: Option<TxnRequest>, kind: Kind) {
        let id = request.map(|request| {
            let id = request.identity.request.0;
            self.by_id.insert(id, (request, kind));
            id
        });
        self.latest = Some(id);
    }

    /// A retry's answer: the request it carries is what was sent last under its id, so the
    /// next `retry` of that id sends it (D1). An answer with no request sent nothing, and
    /// changes nothing.
    fn resent(&mut self, answer: &Result<rdb_api::PutOk, PutError>) {
        let request = match answer {
            Ok(ok) => Some(&*ok.sent),
            Err(PutError { request, .. }) => request.as_deref(),
        };
        if let Some(request) = request {
            let id = request.identity.request.0;
            if let Some(entry) = self.by_id.get_mut(&id) {
                entry.0 = request.clone();
                self.latest = Some(Some(id));
            }
        }
    }

    /// A `retry --deadline-ms` answer. A deadline past `MAX_TXN_DEADLINE` is refused before
    /// anything is sent, and the request handed back is the one picked, unchanged, so nothing
    /// changes: the next bare `retry` still sends the latest (PR #36 F-002).
    fn resent_within(&mut self, deadline: Duration, answer: &Result<rdb_api::PutOk, PutError>) {
        if deadline <= rdb_api::MAX_TXN_DEADLINE {
            self.resent(answer);
        }
    }

    fn pick(&self, rest: &[&str]) -> Result<(TxnRequest, Kind), String> {
        let id = match rest {
            [] => match self.latest {
                None => return Err("no put or txn to retry yet".to_owned()),
                Some(None) => {
                    return Err("the latest put or txn sent no request; nothing to retry".to_owned())
                }
                Some(Some(id)) => id,
            },
            [id] => id
                .parse()
                .map_err(|_| "retry [<request>]: a request id is a number".to_owned())?,
            _ => return Err("retry [<request>] [--payload <value> | --deadline-ms N]".to_owned()),
        };
        self.by_id
            .get(&id)
            .cloned()
            .ok_or_else(|| format!("this session sent no put or txn as request {id}"))
    }
}

/// Print a put's or txn's answer, and remember what it sent when `record` names where.
fn put_line(answer: Result<rdb_api::PutOk, PutError>, record: Option<(&mut Sent, Kind)>) {
    let remember = |request: Option<TxnRequest>| {
        if let Some((sent, kind)) = record {
            sent.record(request, kind);
        }
    };
    say(&put_text(&answer));
    match answer {
        Ok(ok) => remember(Some(*ok.sent)),
        Err(PutError { request, .. }) => remember(request.map(|request| *request)),
    }
}

/// A put's answer as one line.
fn put_text(answer: &Result<rdb_api::PutOk, PutError>) -> String {
    match answer {
        Ok(ok) => format!(
            "ok p=1 gen={} seq={} {:?} request={}",
            ok.generation.0, ok.seq.0, ok.durability, ok.request.0
        ),
        Err(PutError { error, request }) => {
            let id = request
                .as_ref()
                .map_or_else(|| "-".to_owned(), |r| r.identity.request.0.to_string());
            format!(
                "err {} retry={:?} no_mutation={} request={id} ({})",
                error.name(),
                error.retry,
                error.no_mutation,
                error.detail
            )
        }
    }
}

fn get_line(answer: Result<rdb_api::GetOk, rdb_api::ApiError>) {
    match answer {
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

/// One node's line. `published=` is left off a secondary: P1 publishes nothing there, so its
/// `0@<gen>` would read as a real position (PC12).
fn node_line(node: &NodeStatus) -> String {
    let published = match node.published {
        _ if node.role == "secondary" => String::new(),
        Some((g, s)) => format!(" published={}@{}", s.0, g.0),
        None => " published=-".to_owned(),
    };
    format!(
        "node={} {} authority={} recovery={} recovered={} role={} l1={} admits={}{published}{}{}",
        node.node.0,
        holds_part(node),
        node.authority,
        node.recovery.as_deref().unwrap_or("-"),
        node.recovered
            .map_or_else(|| "-".to_owned(), |g| g.0.to_string()),
        node.role,
        node.protection.as_deref().unwrap_or("inert"),
        node.admits
            .map_or_else(|| "-".to_owned(), |a| a.to_string()),
        node.reload_pending
            .map_or_else(String::new, |n| format!(" reload pending attempt={n}")),
        node.fault
            .as_ref()
            .map_or_else(String::new, |f| format!(" FAULT={f}")),
    )
}

/// `control`: the records, read from the voter that is leader now through a client of its own,
/// never through the Db's store, so what that store is doing never hides them (critic A3).
fn control_lines(rt: &tokio::runtime::Runtime, cluster: &Cluster) {
    let Some(leader) = cluster.leader_now() else {
        say("err control: no rEtcd leader now; try again");
        return;
    };
    // Only this thread stops a voter, so the leader is still running here.
    let store = cluster.client(leader);
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

/// `control gap`: compact through the revision the leader reads now, then drop the Db's
/// watches. Returns the line to print; a failure, a panic in the harness included, is printed.
fn gap(rt: &tokio::runtime::Runtime, control: &Control<'_>) -> String {
    let cluster = Arc::clone(control.cluster);
    let compacted = rt.block_on(rt.spawn(async move {
        let leader = cluster
            .leader_now()
            .ok_or_else(|| "no rEtcd leader now; try again".to_owned())?;
        let read = cluster
            .client(leader)
            .get(GetRequest {
                key: Bytes::from_static(b"partitions/1"),
            })
            .await
            .map_err(|e| e.to_string())?;
        cluster
            .compact_now(read.read_revision)
            .await
            .map_err(|e| e.to_string())
    }));
    let compacted = match compacted {
        Ok(Ok(revision)) => revision,
        Ok(Err(e)) => return format!("err control gap: {e}"),
        Err(e) => return format!("err control gap: {e}"),
    };
    tracing::info!(revision = compacted, "control_compacted");
    let dropped = control.store.drop_watches();
    format!("compacted through revision {compacted}; dropped {dropped} watches")
}

/// `voters leader=N running=[..] bound=N`; `leader=-` while none is elected.
fn voters_line(control: &Control<'_>) -> String {
    format!(
        "voters leader={} running={:?} bound={}",
        control
            .cluster
            .leader_now()
            .map_or_else(|| "-".to_owned(), |l| l.0.to_string()),
        control
            .cluster
            .running_ids()
            .iter()
            .map(|id| id.0)
            .collect::<Vec<_>>(),
        control.store.bound().0
    )
}

/// A voter the cluster was started with.
fn parse_voter(cluster: &Cluster, text: &str) -> Result<VoterId, String> {
    let ids = cluster.ids();
    text.parse::<u64>()
        .ok()
        .map(VoterId)
        .filter(|id| ids.contains(id))
        .ok_or_else(|| {
            format!(
                "control stop|start <voter>: a voter is one of {:?}, not {text:?}",
                ids.iter().map(|id| id.0).collect::<Vec<_>>()
            )
        })
}

/// `control stop <voter>`: move the Db's store off it, then stop it. Returns the line to print.
/// A failure is printed, never a panic: the stop runs as a task, so even a panic inside the
/// harness comes back as an error.
fn stop_voter(rt: &tokio::runtime::Runtime, control: &Control<'_>, voter: VoterId) -> String {
    if !control.cluster.running_ids().contains(&voter) {
        return format!("err control stop {}: not running", voter.0);
    }
    let moved = match control.store.stopping(voter) {
        Ok(moved) => moved,
        Err(e) => {
            tracing::warn!(voter = voter.0, error = %e, "control_stop_refused");
            return format!("err control stop {}: refused: {e}", voter.0);
        }
    };
    let cluster = Arc::clone(control.cluster);
    let stopped = rt.block_on(rt.spawn(async move { cluster.stop_node(voter).await }));
    control.store.stopped(voter);
    let moved = moved.map_or_else(String::new, |to| {
        format!(" (control store moved to voter {} first)", to.0)
    });
    match stopped {
        Ok(()) => {
            tracing::info!(voter = voter.0, "control_voter_stopped");
            format!("stopped voter {}{moved}", voter.0)
        }
        Err(e) => {
            tracing::error!(voter = voter.0, error = %e, "control_voter_stop_failed");
            format!("err control stop {}: {e}{moved}", voter.0)
        }
    }
}

/// `control start <voter>`, on its own data directory. Returns the line to print; a failure,
/// a panic inside the harness included, is printed, never raised.
fn start_voter(rt: &tokio::runtime::Runtime, cluster: &Arc<Cluster>, voter: VoterId) -> String {
    if cluster.running_ids().contains(&voter) {
        return format!("voter {} is already running", voter.0);
    }
    let shared = Arc::clone(cluster);
    let started = rt.block_on(rt.spawn(async move { shared.try_start_node(voter).await }));
    let failed = match started {
        Ok(Ok(())) => None,
        Ok(Err(e)) => Some(e.to_string()),
        Err(e) => Some(e.to_string()),
    };
    match failed {
        None => {
            tracing::info!(voter = voter.0, "control_voter_started");
            format!("started voter {}", voter.0)
        }
        Some(e) => {
            tracing::error!(voter = voter.0, error = %e, "control_voter_start_failed");
            format!("err control start {}: {e}", voter.0)
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

#[cfg(test)]
mod tests {
    use super::*;
    use rdb_core::contracts::event::Budgets;

    /// The longest a stopped control leader can leave the voters without one, in ms. A follower
    /// that saw a committed leader waits out the leader lease (`election_timeout_max` in
    /// openraft 0.9) and then its own election timeout before it stands
    /// (`RaftCore::handle_tick_election`).
    const fn control_failover_millis(timers: TestTimers) -> u128 {
        timers.election_timeout_max.as_millis() * 2
    }

    /// The shortest time A1 still admits writes after the control leader stops, in ms: the grant,
    /// less the dispatch margin, the clock error and the one tick A1 keeps back, counted from a
    /// renewal that can be a whole renewal interval old when the leader stops.
    const fn write_horizon_floor_millis(budgets: &Budgets) -> u128 {
        (budgets.grant_millis
            - budgets.dispatch_margin_millis
            - budgets.clock_error_millis
            - 1
            - budgets.renew_millis) as u128
    }

    /// The rows that set `RETCD_TEST_DATA_DIR`, as `main` does, hold this: the variable is the
    /// process's, and these rows run in parallel.
    static ENV: Mutex<()> = Mutex::new(());

    fn owner(stalled: Option<&str>, admits: bool) -> NodeStatus {
        NodeStatus {
            node: NodeId(1),
            fault: None,
            holds: None,
            authority: "valid".to_owned(),
            recovery: Some("Committed".to_owned()),
            recovered: Some(Generation(1)),
            role: "primary",
            protection: None,
            admits: Some(admits),
            published: None,
            stalled: stalled.map(str::to_owned),
            reload_pending: None,
        }
    }

    /// Walk 1L (M9 S2a): `control stop` of the control leader fenced the owner `Expired` and
    /// writes never came back. The voters elected 2338 ms after the stop, 3 ms past A1's
    /// horizon, because the default timers make a failover take up to 3000 ms against a
    /// horizon that can be as short as 2299 ms. A new leader has to be in place with a whole
    /// renewal interval to spare, so the renewal sent to it commits before the horizon.
    #[test]
    fn control_failover_leaves_a_renewal_inside_the_write_horizon() {
        let budgets = Budgets::SPEC_DEFAULTS;
        let failover = control_failover_millis(CONTROL_TIMERS);
        let renewed_by = failover + u128::from(budgets.renew_millis);
        let horizon = write_horizon_floor_millis(&budgets);
        assert!(
            renewed_by < horizon,
            "a control failover of up to {failover} ms plus one renewal ({renewed_by} ms) \
             outlasts the {horizon} ms A1 still admits after the leader stops"
        );
    }

    /// Paper cut after the tester's walk at 1ea889e: a stall that clears said nothing, so the
    /// last line on screen still read as writes paused after a heal. Each change prints once,
    /// and a stall that returns after clearing prints again.
    #[test]
    fn a_stall_that_clears_prints_a_cleared_line_and_a_new_stall_prints_again() {
        let stall =
            "recovery_rebuild_stalled ... its writes stay paused until the absent copy returns";
        let mut printed: Option<String> = None;
        let mut lines = Vec::new();
        for node in [
            owner(None, false),
            owner(Some(stall), false),
            owner(Some(stall), false),
            owner(None, true),
            owner(None, true),
            owner(Some(stall), false),
        ] {
            if let Some(line) = stall_change(printed.as_ref(), &node) {
                lines.push(line);
            }
            printed = node.stalled.clone();
        }
        assert_eq!(
            lines,
            [
                format!("stalled node=1 {stall}"),
                "stall cleared node=1 recovery=Committed recovered=1 admits=true".to_owned(),
                format!("stalled node=1 {stall}"),
            ]
        );
    }

    /// F-005, F-006: an open that fails tears the run down as a clean quit does, and without
    /// `--keep` leaves no directory. The voters' data goes under `--dir`, as `main` puts it, so
    /// the row also proves the order: a store handle still open when the voters close keeps
    /// their RocksDB files, and on Windows `--dir` then cannot be removed.
    /// Integration (~2 s): three real rEtcd voters; the Db open is the injected failure.
    #[test]
    fn an_open_that_fails_exits_1_and_leaves_no_directory() {
        let _env = ENV
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = config_testkit::fs::temp_dir();
        let dir = root.path().join("run");
        // As `main` does.
        std::env::set_var("RETCD_TEST_DATA_DIR", dir.join("control"));
        let args = Args {
            dir: dir.clone(),
            log_dir: root.path().join("logs"),
            script: None,
            keep: false,
            hold: Vec::new(),
            put_timeout: None,
            dedup_cap: None,
        };
        let code = run(&args, |_, _, _| async {
            Err(OpenError::Node("node 2: injected".to_owned()))
        });
        assert_eq!(code, ExitCode::from(1), "a failed open exits 1");
        assert!(
            !dir.exists(),
            "the failed open left {} behind",
            dir.display()
        );
    }

    /// D1 (S1 dev walk, fbf2b95): `txn --deadline-ms 0 a=5` was refused, `retry 4
    /// --deadline-ms 2000` published it at seq 3, and a plain `retry 4` then resent the
    /// deadline-0 request it was first sent with. Check 2 refused that before the dedup replay,
    /// so the retry answered DEADLINE_BEFORE_ADMISSION, not seq 3. The next retry sends what
    /// was sent last, whether that send was answered (D1's own path) or refused. Unit.
    #[test]
    fn the_next_retry_resends_the_deadline_the_last_retry_was_sent_with() {
        use rdb_core::contracts::ids::{ClientId, RequestIdentity, TenantId};
        let request = |remaining_millis| TxnRequest {
            api_version: rdb_core::contracts::version::API_VERSION,
            identity: RequestIdentity {
                tenant: TenantId(1),
                client: ClientId(1),
                request: RequestId(4),
            },
            affinity: AffinityId(1),
            expected_generation: Some(Generation(1)),
            remaining_millis,
            conditions: Vec::new(),
            mutations: Vec::new(),
        };
        let mut sent = Sent::default();
        sent.record(Some(request(0)), Kind::Txn);
        let deadline = |sent: &Sent| sent.pick(&["4"]).expect("request 4").0.remaining_millis;
        // D1: `retry 4 --deadline-ms 2000` published at seq 3.
        sent.resent(&Ok(rdb_api::PutOk {
            request: RequestId(4),
            generation: Generation(1),
            owner_epoch: rdb_core::contracts::ids::OwnerEpoch(1),
            seq: rdb_core::contracts::ids::Seq(3),
            outcome: rdb_core::contracts::txn::Outcome::Published,
            durability: rdb_core::contracts::txn::Durability::BufferedOnTwo,
            sent: Box::new(request(2_000)),
        }));
        assert_eq!(
            deadline(&sent),
            2_000,
            "the answered retry's deadline is kept"
        );
        assert_eq!(sent.pick(&[]).expect("latest").0.remaining_millis, 2_000);
        // A refusal hands the request it sent back; that is the next retry's too.
        sent.resent(&Err(PutError {
            error: rdb_api::ApiError::invalid("refused after it was sent"),
            request: Some(Box::new(request(1_500))),
        }));
        assert_eq!(
            deadline(&sent),
            1_500,
            "the refused retry's deadline is kept"
        );
    }

    /// PR #36 F-002: `txn --deadline-ms 0 a=1` (request 1), `txn b=1` (request 2), then `retry 1
    /// --deadline-ms 30001`. The deadline is past `MAX_TXN_DEADLINE`, so nothing was sent, but
    /// the request it handed back made request 1 the latest, and a bare `retry` resent it, not
    /// request 2. A retry that sends still makes its request the latest. Unit.
    #[test]
    fn a_retry_refused_before_sending_leaves_the_latest_request_alone() {
        use rdb_core::contracts::ids::{ClientId, RequestIdentity, TenantId};
        let request = |id, remaining_millis| TxnRequest {
            api_version: rdb_core::contracts::version::API_VERSION,
            identity: RequestIdentity {
                tenant: TenantId(1),
                client: ClientId(1),
                request: RequestId(id),
            },
            affinity: AffinityId(1),
            expected_generation: Some(Generation(1)),
            remaining_millis,
            conditions: Vec::new(),
            mutations: Vec::new(),
        };
        let mut sent = Sent::default();
        sent.record(Some(request(1, 0)), Kind::Txn);
        sent.record(Some(request(2, 2_000)), Kind::Txn);
        let latest = |sent: &Sent| sent.pick(&[]).expect("latest").0.identity.request;
        let over = rdb_api::MAX_TXN_DEADLINE + Duration::from_millis(1);
        sent.resent_within(
            over,
            &Err(PutError {
                error: rdb_api::ApiError::invalid("past the maximum; nothing was sent"),
                request: Some(Box::new(request(1, 0))),
            }),
        );
        assert_eq!(latest(&sent), RequestId(2), "nothing was sent");
        // At the maximum it is sent: the refusal's request is the next retry's.
        sent.resent_within(
            rdb_api::MAX_TXN_DEADLINE,
            &Err(PutError {
                error: rdb_api::ApiError::invalid("refused after it was sent"),
                request: Some(Box::new(request(1, 30_000))),
            }),
        );
        assert_eq!(
            latest(&sent),
            RequestId(1),
            "a retry that sends is the latest"
        );
        assert_eq!(
            sent.pick(&["1"]).expect("request 1").0.remaining_millis,
            30_000
        );
    }

    /// The tester's PC-2: `--dedup-cap 65537` started three rEtcd voters, then the open refused
    /// the cap. The cap is checked first now, so a refused cap starts nothing and exits 1. With
    /// `--keep`, voters that had started would leave their data under `--dir`. Unit when green
    /// (~10 ms); its red run started the voters (~2 s).
    #[test]
    fn a_refused_dedup_cap_exits_1_before_any_voter_starts() {
        let _env = ENV
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = config_testkit::fs::temp_dir();
        let dir = root.path().join("run");
        std::env::set_var("RETCD_TEST_DATA_DIR", dir.join("control"));
        let args = Args {
            dir: dir.clone(),
            log_dir: root.path().join("logs"),
            script: None,
            keep: true,
            hold: Vec::new(),
            put_timeout: None,
            dedup_cap: Some(65_537),
        };
        let opened = AtomicBool::new(false);
        let code = run(&args, |_, _, _| {
            opened.store(true, Ordering::Relaxed);
            async { Err(OpenError::Node("never reached".to_owned())) }
        });
        assert_eq!(code, ExitCode::from(1), "a refused cap exits 1");
        assert!(!opened.load(Ordering::Relaxed), "the open was never tried");
        assert!(
            !dir.join("control").exists(),
            "no voter started: {} exists",
            dir.join("control").display()
        );
    }

    /// S1 ruling A5 (guard G16): an op is `[<group>:]<object>=<value>[@<version>]`. The object
    /// ends at the first `=`, a group is only a leading `digits:`, and a version only a
    /// trailing `@digits`; anything else stays in the object or the value. Unit (~1 ms).
    #[test]
    fn a_txn_op_reads_a_group_and_a_version_only_where_the_grammar_puts_them() {
        let op = |group, object: &str, value: &str, if_version| TxnOp {
            group,
            object: object.to_owned(),
            value: value.to_owned(),
            if_version,
        };
        let default = Duration::from_millis(7);
        let (deadline, ops) = parse_txn(
            &["2:a=1@3", "a=x=y", "k:v=1", "a=b@c", "1x:a=1", "a=@1"],
            default,
        )
        .expect("all parse");
        assert_eq!(deadline, default);
        assert_eq!(
            ops,
            [
                op(2, "a", "1", Some(3)),
                op(1, "a", "x=y", None),
                op(1, "k:v", "1", None),
                op(1, "a", "b@c", None),
                op(1, "1x:a", "1", None),
                op(1, "a", "", Some(1)),
            ]
        );
        let (deadline, _) = parse_txn(&["--deadline-ms", "0", "a=1"], default).expect("deadline");
        assert_eq!(deadline, Duration::ZERO);
        assert!(parse_txn(&["a"], default).is_err(), "no `=`");
        assert!(
            parse_txn(&["99999999999999999999:a=1"], default).is_err(),
            "group too large"
        );
        assert!(
            parse_txn(&["--deadline-ms", "x", "a=1"], default).is_err(),
            "bad deadline"
        );
    }

    /// S1 ruling A7, guard G10: `retry <id> --payload` re-puts the object and condition of a
    /// put. A txn has no single object, so it is refused with a usage line. Unit (~1 ms).
    #[test]
    fn retry_payload_re_puts_a_put_and_refuses_a_txn() {
        let put = Kind::Put {
            object: b"a".to_vec(),
            if_version: Some(3),
        };
        assert_eq!(payload_put(put, RequestId(4)), Ok((b"a".to_vec(), Some(3))));
        let refused = payload_put(Kind::Txn, RequestId(5)).expect_err("a txn has no one object");
        assert!(refused.ends_with("request 5 was a txn"), "{refused}");
    }

    /// F-007: a script PowerShell 5.1 wrote as UTF-16LE is an input error, not the end of the
    /// input, so the run says so and exits 2 instead of running nothing and exiting 0. Unit.
    #[test]
    fn an_unreadable_line_is_an_input_error_not_the_end_of_input() {
        let utf16le: Vec<u8> = [0xFF, 0xFE]
            .into_iter()
            .chain(
                "put a 1\r\nquit\r\n"
                    .encode_utf16()
                    .flat_map(u16::to_le_bytes),
            )
            .collect();
        let first = std::io::BufRead::lines(std::io::Cursor::new(utf16le))
            .next()
            .expect("one line");
        let error = command(first).expect_err("a UTF-16LE line is not a command");
        assert!(error.starts_with("input: "), "{error}");
        assert_eq!(command(Ok("  # a comment".to_owned())), Ok(None));
        assert_eq!(
            command(Ok("put a 1 # set a".to_owned())),
            Ok(Some("put a 1".to_owned()))
        );
    }
}
