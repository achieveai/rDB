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
//! nodes                                   each node's view of partition 1, with the boot its
//!                                         running process has
//! node restart <n>                        stop node n and start it again on its data at its
//!                                         boot plus one; prints `node n boot b`. The new
//!                                         process holds no role and stays out until M10. So
//!                                         any restart stops writes, safely, for the life of
//!                                         this run: after a secondary's, L1 pauses within
//!                                         about 2 s of the next write (`PROTECTION_PAUSED`)
//!                                         because that copy is never durable again; after
//!                                         the owner's, no node
//!                                         holds a grant. Reads: after a secondary's they keep
//!                                         answering, unless a write was in flight that no
//!                                         other copy can take yet: then fresh reads answer
//!                                         `UNKNOWN_OUTCOME` and `get --previous` still
//!                                         answers. That lasts while no copy that did not
//!                                         restart can take the write: if the other secondary
//!                                         was only unreachable, healing its link resolves
//!                                         it; only with both secondaries restarted does it
//!                                         wait for M10. After the owner's every read is
//!                                         `UNAVAILABLE`. Every `PROTECTION_PAUSED`,
//!                                         `UNAVAILABLE`, `UNKNOWN_OUTCOME` or `NOT_PRIMARY`
//!                                         after it names the restart, and so does a `status`
//!                                         of `Unknown` for a request this session sent: that
//!                                         is not evidence of a lost write. After one
//!                                         secondary's restart it resolves once the other can
//!                                         take the write; after the owner's, or both
//!                                         secondaries', it waits for the node to rejoin.
//!                                         Restarting is not a recovery: catch-up and re-pin
//!                                         are M10 (achieveai/rDB#42). To write again,
//!                                         quit and start rdb_dev again. Refused until the
//!                                         first `ready`
//! admin clear-grant <n>                   remove grants/<n> if node n's old process left it
//!                                         and it may go: `absent`, `current` (the running
//!                                         process's own), `cleared rev=R`, `conflict`, or
//!                                         `refused ...`. Only `not-proven-expired` clears by
//!                                         waiting, and says how long; `frozen` never clears
//!                                         in M9 (no takeover runs), `partition-in-transfer
//!                                         pP` waits for pP to be `serving` again. Clearing
//!                                         never lets a restarted owner write again in M9
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
//! control lifecycle <p> <serving|fencing|drained>
//!                                         rewrite partitions/<p>'s lifecycle on the rEtcd
//!                                         leader, at its revision. Put `serving` back after
//! control freeze-grant <n>                set grants/<n>'s frozen bit on the rEtcd leader, at
//!                                         its revision. Nothing in M9 undoes it: run it last
//! link hold|heal <a>-<b> | link heal all | links
//! wait recovered|ready [<seconds>]        block until the partition gets there
//! sleep <millis>
//! quit
//! ```
//!
//! While it runs, a poller prints the partition's progress as it changes: `recovered gen=N`,
//! `waiting to admit writes (l1=.. authority=..)`, and `ready` once node 1 admits writes: its L1
//! admits and its A1 holds a grant. `not ready: writes not admitted` when either half stops, the
//! waiting line then saying which. When node 1's grant is fenced, that line says why. While A1
//! still polls its own record it adds that writes resume by themselves, so the node is not
//! restarted (#42); once the poll has stopped, or for a frozen, foreign-boot or other-authority
//! record, it says nothing in M9 replaces the grant and writes stay stopped for the run. After a secondary restarts,
//! L1 still admits writes for a while, so the line reads `ready for now; writes pause within
//! about 2 s of the next write because ..` until they do: about 2 s after the restart, or,
//! after a fence, about 2 s after the next write (tester q8). A put before `ready` is refused,
//! never left hanging. A recovery committed below `Active` that does not activate prints
//! `stalled node=N recovery_rebuild_stalled ...`, and again whenever the line changes:
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
use config_core::{
    ConfigStore, GetRequest, ListRequest, MutationOutcome, NodeId as VoterId, PutRequest,
};
use config_testkit::cluster::{Cluster, StorageKind};
use config_testkit::TestTimers;
use rdb_api::admin::{self, Clearance};
use rdb_api::host::NodeStatus;
use rdb_api::{ApiError, Db, DbConfig, OpenError, PutError, Timeouts, TxnPut};
use rdb_core::authority::grant::{ClearRefusal, GrantRecord};
use rdb_core::authority::partition::{PartitionLifecycle, PartitionRecord};
use rdb_core::contracts::authority::DenyReason;
use rdb_core::contracts::control::ControlKey;
use rdb_core::contracts::errors::RetryRule;
use rdb_core::contracts::event::Budgets;
use rdb_core::contracts::ids::{AffinityId, BootId, Generation, NodeId, PartitionId, RequestId};
use rdb_core::contracts::txn::{TxnRequest, TxnStatus};

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

fn parse_node(text: &str) -> Result<NodeId, String> {
    match text.trim().parse::<u32>() {
        Ok(n @ 1..=3) => Ok(NodeId(n)),
        _ => Err(format!("{text:?}: a node is 1, 2 or 3")),
    }
}

fn parse_pair(text: &str) -> Result<(NodeId, NodeId), String> {
    let (a, b) = text
        .split_once('-')
        .ok_or_else(|| format!("{text:?}: a link is <a>-<b>"))?;
    let (a, b) = (parse_node(a)?, parse_node(b)?);
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
    /// The reason last printed on a `ready` or `not ready` line.
    reason: Option<String>,
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
            if let Some(line) = ready_step(
                &mut seen,
                owner,
                db.restart_note().as_deref(),
                &db.links().held(),
            ) {
                say(&line);
            }
            if seen.recovered.is_some() && !admits {
                let why = waiting_line(owner);
                if seen.waiting.as_ref() != Some(&why) {
                    say(&why);
                    seen.waiting = Some(why);
                }
            }
        } else if db.restart_failed(NodeId(1)) {
            if let Some(line) = stopped_owner_step(&mut seen, db.restart_note().as_deref()) {
                say(&line);
            }
        }
        drop(seen);
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// L1's pause age, in whole seconds rounded up, under the budgets [`Db::open`] runs every node
/// with: a write made while a copy is out stays unsafe and pauses writes this long after it.
const PAUSE_AGE_SECS: u64 = Budgets::SPEC_DEFAULTS.pause_age_millis.div_ceil(1000);

/// What a line says when nothing in M9 brings writes back.
const STUCK: &str =
    "writes stay stopped for this run: to write again, quit and start rdb_dev again";

/// The reason a `ready` or `not ready` line carries, when a user could act on it wrongly. Each
/// part is derived from the state that decides it (tester walk 7, W7-D1, W7-D2).
///
/// After a restart (`restart_note`), while node 1 still admits writes, L1 pauses them soon: on
/// its own within about 2 s of the restart (walk 6c, tester r1, with no write), and at the
/// latest about [`PAUSE_AGE_SECS`] after the next write, which stays unsafe with a copy out
/// (tester r3). So the line promises "soon" and that bound, never a pause tied only to the
/// next write (W7-D1). Once node 1 does not admit, the line says writes stay stopped only when
/// that holds: L1 paused or reprotecting, which with a copy out nothing in M9 resumes, or the
/// owner itself restarted. Not admitting because of a fence the poll can lift (FR, tester r3)
/// promises the resume and then the pause (W7-D2).
///
/// A fenced grant. While A1's self-replace poll is live it looks stuck but is not: A1 replaces
/// the grant itself once the old one's expiry is proven, and a restart there would end recovery
/// for the run (Message 6). The line promises that only while the poll is live and the reason
/// is not one [`never_replaced`] names (F-004): once the poll stopped (a frozen, foreign or
/// deleted record, F-016), nothing in M9 lifts the fence.
///
/// With no restart, L1 paused or reprotecting while `held` holds a link of node 1's keeps writes
/// out until those links heal and the copies catch up (W8-D2): the line names the links, fenced
/// or not, as a condition. Without a held link (startup) the copies catch up by themselves.
///
/// Not fenced, with A1's self-replace stopped (W8-D1): stuck, as [`gave_up_line`] says.
///
/// F1 blocked comes first (W10-D1): stuck, as [`blocked_line`] says, over any other reason.
/// F1 resolving its activation resumes by itself ([`resolving_line`]), over the fence's resume
/// line and the held-link line, but never over a stuck state: a fence nothing lifts, a stopped
/// self-replace, or a restart with L1 refusing. After a restart, the pause follows the resume.
fn ready_reason(
    owner: &NodeStatus,
    admits: bool,
    restart_note: Option<&str>,
    held: &[(NodeId, NodeId)],
) -> Option<String> {
    let fence = owner.fence.filter(|_| !admits);
    if let Some(line) = blocked_line(owner).filter(|_| !admits) {
        return Some(match restart_note {
            Some(note) => format!("{line}; {note}"),
            None => line,
        });
    }
    let gave_up =
        (!admits && fence.is_none() && owner.replace_stopped).then(|| gave_up_line(owner));
    let resolving = resolving_line(owner)
        .filter(|_| !admits && fence.is_none_or(|reason| fence_lifts(owner, reason)));
    let Some(note) = restart_note else {
        if let Some(line) = gave_up.or(resolving) {
            return Some(line);
        }
        return match (fence, links_wait(owner, held)) {
            (Some(reason), _) => Some(fence_line(owner, reason, held)),
            (None, Some((mode, links))) if !admits => Some(format!(
                "l1={mode} and links {links} are held: writes wait for node {}'s copies to catch \
                 up, which needs those links: heal them (link heal all)",
                owner.node.0
            )),
            (None, _) => None,
        };
    };
    let pause = format!(
        "pause soon, at the latest about {PAUSE_AGE_SECS} s after the next write, because {note}"
    );
    if admits {
        return Some(format!("writes {pause}"));
    }
    if owner.boot != rdb_api::host::BOOT || l1_rejecting(owner).is_some() {
        return Some(format!("{note}; {STUCK}"));
    }
    if let Some(line) = gave_up {
        return Some(format!("{line}; {note}"));
    }
    if let Some(line) = resolving {
        return Some(format!("{line}; once they resume, they {pause}"));
    }
    Some(match fence {
        Some(reason) if fence_lifts(owner, reason) => {
            format!(
                "{}; once they resume, they {pause}",
                fence_line(owner, reason, held)
            )
        }
        Some(reason) => format!("{}; {note}", fence_line(owner, reason, held)),
        None => format!("writes are not admitted now; if they come back, they {pause}"),
    })
}

/// L1's mode while it refuses writes, `paused` or `reprotecting`: with a copy out it waits on
/// that copy and its links, and after a restart nothing in M9 brings the copy back.
fn l1_rejecting(owner: &NodeStatus) -> Option<&str> {
    owner
        .protection
        .as_deref()
        .filter(|mode| matches!(*mode, "paused" | "reprotecting"))
}

/// L1's mode while it refuses writes, and the links of `owner`'s that `held` holds, when there
/// are both: writes then wait on those links (W8-D2).
fn links_wait<'a>(owner: &'a NodeStatus, held: &[(NodeId, NodeId)]) -> Option<(&'a str, String)> {
    let mode = l1_rejecting(owner)?;
    let links: Vec<String> = held
        .iter()
        .filter(|(a, b)| *a == owner.node || *b == owner.node)
        .map(|(a, b)| format!("{}-{}", a.0, b.0))
        .collect();
    (!links.is_empty()).then(|| (mode, links.join(", ")))
}

/// The line for F1 `Blocked(<reason>)` on node 1: recovery cannot finish, so writes stay out
/// whatever the grant, the links or L1 do (W10-D1). Since A-R102 an unanswered activation
/// resolves instead ([`resolving_line`]), so this is only the recovery CAS or a real dead end.
/// It names the reason and promises no way out.
fn blocked_line(owner: &NodeStatus) -> Option<String> {
    let recovery = owner.recovery.as_deref()?;
    recovery
        .starts_with("Blocked(")
        .then(|| format!("node {}'s recovery is {recovery}, so {STUCK}", owner.node.0))
}

/// The line for F1 `ActivationUnresolved` on node 1: the activation CAS went unanswered (tester
/// r24n: the store down), and F1 reads and proposes again by itself once the store answers
/// (A-R102), so writes come back with no restart.
fn resolving_line(owner: &NodeStatus) -> Option<String> {
    (owner.recovery.as_deref() == Some("ActivationUnresolved")).then(|| {
        format!(
            "node {}'s recovery is waiting for the control store to confirm activation; writes \
             resume by themselves once it answers",
            owner.node.0
        )
    })
}

/// The line for A1 `unheld` with its self-replace stopped (`replace_stopped`): a freeze, or a
/// record not this boot's, landed between grants, and A1 will not try again (W8-D1, tester
/// r6_3200). Nothing in M9 replaces the grant.
fn gave_up_line(owner: &NodeStatus) -> String {
    let node = owner.node.0;
    format!(
        "node {node}'s grant is {} and node {node} stopped trying for one: its grant record is \
         frozen or not its own; nothing in M9 replaces it, so {STUCK}",
        owner.authority
    )
}

/// Whether A1's self-replace can still lift a fence for `reason`: its poll is live, and the
/// record is not one it answers `Never` ([`never_replaced`]).
const fn fence_lifts(owner: &NodeStatus, reason: DenyReason) -> bool {
    owner.fence_polling && !never_replaced(reason)
}

/// The fenced grant's line: it resumes by itself while [`fence_lifts`], else it is stuck. With
/// L1 paused or reprotecting and a link of node 1's held, the grant comes back by itself and
/// writes still wait on those links (W8-D2, tester r9: `held(grant 3)`, `l1=paused`, writes back
/// only after `link heal all`).
fn fence_line(owner: &NodeStatus, reason: DenyReason, held: &[(NodeId, NodeId)]) -> String {
    let node = owner.node.0;
    if let (true, Some((mode, links))) = (fence_lifts(owner, reason), links_wait(owner, held)) {
        format!(
            "node {node}'s grant is {}, l1={mode} and links {links} are held; writes resume only \
             once the old grant's expiry is proven and node {node}'s copies catch up, which needs \
             those links: heal them (link heal all); do not restart node {node}: a restarted \
             node does not rejoin in M9 (achieveai/rDB#42)",
            owner.authority
        )
    } else if fence_lifts(owner, reason) {
        format!(
            "node {node}'s grant is {}; writes resume by themselves once the old grant's expiry \
             is proven, so do not restart node {node}: a restarted node does not rejoin in M9 \
             (achieveai/rDB#42)",
            owner.authority
        )
    } else {
        format!(
            "node {node}'s grant is {}; nothing in M9 replaces it, so {STUCK}",
            owner.authority
        )
    }
}

/// A fence reason whose grant record A1's self-replace answers `Never` (`grant::self_replace`):
/// frozen, another boot's, or under another authority generation. The first poll stops on it,
/// so the line says so from the fence on rather than for one renewal interval later.
const fn never_replaced(reason: DenyReason) -> bool {
    matches!(
        reason,
        DenyReason::Frozen | DenyReason::BootMismatch | DenyReason::AuthorityGenerationChanged
    )
}

/// The `ready` or `not ready` line to print for `owner`, given what was printed last, and what
/// `seen` now holds as printed: on each change of `admits`, and again when a new reason appears
/// (a restart while ready, a fence after the line, a restart after a fence). A reason that goes
/// away prints the bare line, so a promise it made never outlives its state (W8-D1, tester
/// r6_3200: the fence's resume line stayed the last word after A1 stopped, `unheld`).
fn ready_step(
    seen: &mut Seen,
    owner: &NodeStatus,
    restart_note: Option<&str>,
    held: &[(NodeId, NodeId)],
) -> Option<String> {
    let admits = owner.admits == Some(true);
    step_line(
        seen,
        admits,
        ready_reason(owner, admits, restart_note, held),
    )
}

/// The poller's line once node 1's restart failed: no process runs it, so `node_status` leaves
/// it out, and writes stay stopped for the run (W7-D3). `restart_note` names the failed
/// restart; it is never `None` after one.
fn stopped_owner_step(seen: &mut Seen, restart_note: Option<&str>) -> Option<String> {
    let note = restart_note?;
    step_line(seen, false, Some(format!("{note}; {STUCK}")))
}

/// The `ready` or `not ready` line for `admits` and `reason`, given what `seen` holds as
/// printed, as [`ready_step`] describes.
fn step_line(seen: &mut Seen, admits: bool, reason: Option<String>) -> Option<String> {
    if admits == seen.ready && reason == seen.reason {
        return None;
    }
    let line = match (admits, &reason) {
        (true, None) => "ready".to_owned(),
        (true, Some(why)) => format!("ready for now; {why}"),
        (false, None) => "not ready: writes not admitted".to_owned(),
        (false, Some(why)) => format!("not ready: writes not admitted: {why}"),
    };
    seen.ready = admits;
    seen.reason = reason;
    Some(line)
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
        "waiting to admit writes (l1={} authority={} {})",
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
            ["status", request, rest @ ..] => status_line(db, &sent, request, rest),
            ["nodes"] => {
                let answered = db.node_status();
                for node in [1, 2, 3].map(NodeId) {
                    say(&answered
                        .iter()
                        .find(|status| status.node == node)
                        .map_or_else(
                            || missing_node_line(node, db.boot_of(node), db.restart_failed(node)),
                            node_line,
                        ));
                }
                Ok(())
            }
            ["node", "restart", node] => parse_node(node).map(|node| {
                say(&match db.restart_node(node) {
                    Ok(boot) => format!("node {} boot {}", node.0, boot.0),
                    Err(e) => format!("err node restart {}: {e}", node.0),
                });
            }),
            ["node", ..] => Err("node restart <n>: n is 1, 2 or 3".to_owned()),
            ["admin", "clear-grant", node] => parse_node(node).map(|node| {
                say(
                    &match rt.block_on(admin::clear_grant(db, control.store, node)) {
                        Ok(clearance) => clearance_line(clearance, node, db),
                        Err(e) => admin_error_line(node, &e),
                    },
                );
            }),
            ["admin", ..] => Err("admin clear-grant <n>: n is 1, 2 or 3".to_owned()),
            ["control", "lifecycle", partition, word] => {
                parse_lifecycle(partition, word).map(|(partition, lifecycle)| {
                    say(&set_lifecycle(rt, control.cluster, partition, lifecycle));
                })
            }
            ["control", "lifecycle", ..] => Err(format!(
                "control lifecycle <p> <{}>",
                LIFECYCLE_WORDS.join("|")
            )),
            ["control", "freeze-grant", node] => parse_node(node).map(|node| {
                say(&freeze_grant(rt, control.cluster, node));
            }),
            ["control", "freeze-grant", ..] => Err("control freeze-grant <n>".to_owned()),
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
            ["control", verb @ ("gap" | "drop-watches"), ..] => {
                Err(format!("control {verb}: takes no arguments"))
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

fn status_line(db: &Db, sent: &Sent, request: &str, rest: &[&str]) -> Result<(), String> {
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
    // The nodes first: a request node 1 holds in flight that publishes between the two looks is
    // then answered by the status, never called not in flight (Msg 8, W6-D1).
    let nodes = db.node_status();
    match db.status(RequestId(request), generation) {
        // After a restart, `Unknown` can be a write this session saw `ok` (tester W3-D2).
        Ok(TxnStatus::Unknown) => {
            // `None` when node 1 did not answer in time (F-025).
            let in_flight = nodes
                .iter()
                .find(|node| node.node == NodeId(1))
                .map(|owner| owner.in_flight == Some(RequestId(request)));
            let restarted = restarted_nodes(|node| db.boot_of(node));
            let note = db.restart_note();
            let ours = sent.by_id.contains_key(&request);
            say(&unknown_line(ours, in_flight, &restarted, note.as_deref()));
        }
        Ok(status) => say(&format!("status {status:?}")),
        Err(error) => say(&format!("err {} ({})", error.name(), error.detail)),
    }
    Ok(())
}

/// The nodes that restarted, from each node's boot (`boot_of`), never from the nodes that
/// answered `node_status`: that leaves out a node whose restart failed, and `status` of a write
/// in flight then said it waits for M10 while the other secondary could still take it.
fn restarted_nodes(boot_of: impl Fn(NodeId) -> Option<BootId>) -> Vec<NodeId> {
    [1, 2, 3]
        .map(NodeId)
        .into_iter()
        .filter(|&node| boot_of(node).is_some_and(|boot| boot != rdb_api::host::BOOT))
        .collect()
}

/// `status Unknown`, with the restart note when there is one, unless this session never sent
/// the request (`ours` false): that outcome is known (tester walk 5). After the owner's
/// restart its outcome waits for M10. Otherwise it follows what node 1 holds (Msg 8, W6-D1): a
/// request not `in_flight` there was refused before it was queued, or never arrived, and
/// nothing will resolve it. One in flight waits for M10 after both secondaries' restarts, and
/// after one secondary's resolves once the other can take it, as when a held link heals
/// (W5-D1). `in_flight` is `None` when node 1 did not answer in time: whether it holds the
/// request is then not known, so the line promises nothing and says to ask again (F-025).
fn unknown_line(
    ours: bool,
    in_flight: Option<bool>,
    restarted: &[NodeId],
    restart_note: Option<&str>,
) -> String {
    let Some(note) = restart_note.filter(|_| ours) else {
        return "status Unknown".to_owned();
    };
    let others: Vec<u32> = [2, 3]
        .into_iter()
        .filter(|node| !restarted.contains(&NodeId(*node)))
        .collect();
    let owner_restarted = restarted.contains(&NodeId(1));
    match others.as_slice() {
        _ if !owner_restarted && in_flight.is_none() => format!(
            "status Unknown (node 1 did not answer in time, so whether it holds it in flight is \
             not known; try again; {note})"
        ),
        _ if !owner_restarted && in_flight == Some(false) => format!(
            "status Unknown (node 1 does not hold it in flight, so waiting will not change \
             this: it was refused before it was queued, or never arrived; {note})"
        ),
        [other] if !owner_restarted => format!(
            "status Unknown (not evidence of a lost write: it resolves once node {other}, which \
             did not restart, can take it; {note})"
        ),
        _ => format!(
            "status Unknown (not evidence of a lost write: its outcome cannot be known until a \
             restarted node rejoins, which is M10; {note})"
        ),
    }
}

/// The `nodes` line for a node `node_status` left out, which it does both for a node whose
/// restart failed, so no process runs it (`failed`, F-013), and for one that did not answer in
/// time. A node left out is never dropped from the list silently (W7-D3).
fn missing_node_line(node: NodeId, boot: Option<BootId>, failed: bool) -> String {
    let boot = boot.map_or_else(|| "?".to_owned(), |boot| boot.0.to_string());
    if failed {
        format!(
            "node={} boot={boot} stopped: its restart as boot {boot} failed, so no process runs \
             it",
            node.0
        )
    } else {
        format!(
            "node={} boot={boot} did not answer in time; try again",
            node.0
        )
    }
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
        "node={} boot={} {} authority={} recovery={} recovered={} role={} l1={} admits={}{published}{}{}",
        node.node.0,
        node.boot.0,
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

/// `admin clear-grant`'s error as one line, with its retry rule and mutation proof as a `put`
/// error prints them, and in plain words whether running it again can help (F-012): a permanent
/// store error and a transient one otherwise read alike. The words follow the retry rule and
/// the mutation proof together (F-027).
fn admin_error_line(node: NodeId, error: &ApiError) -> String {
    // From `retry` and `no_mutation` together (F-027): a delete that may have applied is worth
    // running again even when the call itself fails the same way, because the rerun reads the
    // record before it deletes anything.
    let transient = match error.retry {
        RetryRule::RefreshRoute | RetryRule::RetryAfterRecovery | RetryRule::BoundedJitter => {
            Some(true)
        }
        RetryRule::QueryStatus => None,
        RetryRule::Definitive
        | RetryRule::Reconcile
        | RetryRule::Quarantine
        | RetryRule::NotWired => Some(false),
    };
    let words = match (transient, error.no_mutation) {
        (None, _) => "run it again: it reads where the record stands before it deletes anything",
        (Some(true), true) => "running it again can help",
        (Some(true), false) => {
            "running it again can help, and shows whether the delete applied: it reads the \
             record first"
        }
        (Some(false), true) => "running it again will not help: it fails the same way each time",
        (Some(false), false) => {
            "run it again to see whether the delete applied: it reads the record first, and \
             only a record still there fails the same way again"
        }
    };
    format!(
        "err admin clear-grant {}: {} retry={:?} no_mutation={} ({}); {words}",
        node.0,
        error.name(),
        error.retry,
        error.no_mutation,
        error.detail
    )
}

/// `admin clear-grant`'s answer as one line. Only `not-proven-expired` clears with time, so
/// only it says when to try again (W2-D1); the raw `until` stays in the `grant_clear` log line.
fn clearance_line(clearance: Clearance, node: NodeId, db: &Db) -> String {
    match clearance {
        Clearance::Absent => "absent".to_owned(),
        Clearance::Current => format!(
            "current: the record is node {}'s running boot {}; in M9 it clears only after that \
             node restarts, and a restart stops writes for the run (achieveai/rDB#42)",
            node.0,
            db.boot_of(node)
                .map_or_else(|| "?".to_owned(), |boot| boot.0.to_string())
        ),
        Clearance::Cleared(revision) => format!("cleared rev={}", revision.0),
        Clearance::Conflict => "conflict: the record moved; run it again".to_owned(),
        Clearance::Refused {
            refusal: ClearRefusal::NotProvenExpired,
            until_utc_ms,
        } => {
            let wait =
                u64::try_from(until_utc_ms).map_or(0, |until| until.saturating_sub(db.now().0));
            format!(
                "refused not-proven-expired: the grant may still be live; try again in {wait} ms"
            )
        }
        Clearance::Refused {
            refusal: ClearRefusal::Frozen,
            ..
        } => "refused frozen: this grant is frozen; M9 runs no takeover and nothing in M9 \
              unfreezes it, so it never clears in this run"
            .to_owned(),
        Clearance::Refused {
            refusal: ClearRefusal::PartitionInTransfer(partition),
            ..
        } => format!(
            "refused partition-in-transfer p{0}: p{0} is not serving; it clears only once p{0} \
             is back to serving (control lifecycle {0} serving), never by waiting",
            partition.0
        ),
    }
}

/// The words `control lifecycle` takes, in `PartitionLifecycle` order.
const LIFECYCLE_WORDS: [&str; 3] = ["serving", "fencing", "drained"];

fn parse_lifecycle(
    partition: &str,
    word: &str,
) -> Result<(PartitionId, PartitionLifecycle), String> {
    let usage = || format!("control lifecycle <p> <{}>", LIFECYCLE_WORDS.join("|"));
    let partition = partition
        .parse::<u32>()
        .map(PartitionId)
        .map_err(|_| format!("{}: {partition:?} is not a partition id", usage()))?;
    let lifecycle = match word {
        "serving" => PartitionLifecycle::Serving,
        "fencing" => PartitionLifecycle::Fencing,
        "drained" => PartitionLifecycle::FencingDrained,
        other => return Err(format!("{}: not {other:?}", usage())),
    };
    Ok((partition, lifecycle))
}

/// `control lifecycle`: read `partitions/<p>` on the rEtcd leader and write it back with
/// `lifecycle`, at the revision read. Example-only, like every raw `control` verb: no product
/// path writes a lifecycle other than `Serving` in M9. Returns the line to print.
fn set_lifecycle(
    rt: &tokio::runtime::Runtime,
    cluster: &Cluster,
    partition: PartitionId,
    lifecycle: PartitionLifecycle,
) -> String {
    let key = ControlKey::Partition(partition).encode();
    rewrite(rt, cluster, &key, |body| {
        let mut record = PartitionRecord::decode(body)?;
        record.lifecycle = lifecycle;
        Some((record.encode(), format!("{:?}", record.lifecycle)))
    })
}

/// `control freeze-grant`: read `grants/<n>` on the rEtcd leader and write it back frozen, at
/// the revision read. Returns the line to print.
fn freeze_grant(rt: &tokio::runtime::Runtime, cluster: &Cluster, node: NodeId) -> String {
    let key = ControlKey::Grant(node).encode();
    rewrite(rt, cluster, &key, |body| {
        let mut record = GrantRecord::decode(body)?;
        record.frozen = true;
        Some((record.encode(), format!("frozen boot={}", record.boot.0)))
    })
}

/// Read `key` on the rEtcd leader, let `change` rewrite its body, and put the result at the
/// revision read. `change` answers `None` for a body it cannot decode.
fn rewrite(
    rt: &tokio::runtime::Runtime,
    cluster: &Cluster,
    key: &str,
    change: impl FnOnce(&[u8]) -> Option<(Bytes, String)>,
) -> String {
    let Some(leader) = cluster.leader_now() else {
        return format!("err control {key}: no rEtcd leader now; try again");
    };
    let store = cluster.client(leader);
    let read = match rt.block_on(store.get(GetRequest {
        key: Bytes::from(key.to_owned()),
    })) {
        Ok(read) => read,
        Err(e) => return format!("err control get {key}: {e}"),
    };
    let Some(found) = read.record else {
        return format!("err control {key}: absent");
    };
    let Some((value, what)) = change(&found.value) else {
        return format!(
            "err control {key}: rev={} does not decode",
            found.mod_revision
        );
    };
    let put = rt.block_on(store.put(PutRequest {
        key: Bytes::from(key.to_owned()),
        value,
        expected_mod_revision: Some(found.mod_revision),
        dedup: None,
    }));
    match put {
        Ok(response) if response.outcome == MutationOutcome::Applied => {
            tracing::info!(key, from = found.mod_revision, revision = response.revision, what = %what, "control_rewrite");
            format!("{key} rev={} {what}", response.revision)
        }
        Ok(response) => format!(
            "err control {key}: {:?} at rev={}",
            response.outcome, found.mod_revision
        ),
        Err(e) => format!("err control put {key}: {e}"),
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
            boot: rdb_api::host::BOOT,
            fault: None,
            holds: None,
            authority: "valid".to_owned(),
            fence: None,
            fence_polling: false,
            replace_stopped: false,
            recovery: Some("Committed".to_owned()),
            recovered: Some(Generation(1)),
            role: "primary",
            protection: None,
            admits: Some(admits),
            published: None,
            in_flight: None,
            stalled: stalled.map(str::to_owned),
            reload_pending: None,
        }
    }

    /// Walk 1L (M9 S2a): `control stop` of the control leader fenced the owner `Expired` and
    /// writes never came back. The voters elected 2338 ms after the stop, 3 ms past A1's
    /// horizon, because the default timers make a failover take up to 3000 ms against a
    /// horizon that can be as short as 2299 ms. A new leader has to be in place with a whole
    /// renewal interval to spare, so the renewal sent to it commits before the horizon.
    ///
    /// The slack this relies on (review F-004): with `CONTROL_TIMERS` and the spec budgets it is
    /// 1500 ms (1000 ms failover + 500 ms renewal) against the 2299 ms horizon, so 799 ms. The
    /// floor already takes off one renewal for the age of the last one, and the added renewal
    /// covers the 500 ms renewal tick. Nothing here counts the renewal's RPC and commit time
    /// on the new leader: those must fit in the 799 ms, and no term is added for them.
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

    /// Message 6 paper cut: a fenced owner printed a bare `not ready`, and a user may then restart
    /// the node, which ends recovery in M9. The fence prints its reason when it comes after the
    /// line, nothing more while it lasts, the bare line as it clears with writes still out
    /// (W8-D1), and a restart after it prints again.
    /// W7-D2 (tester r3): that restart left the poll live, and writes came back, so the line
    /// promises the resume and the pause after it, not that writes stay stopped.
    #[test]
    fn a_fenced_owner_says_writes_resume_by_themselves_and_not_to_restart() {
        let mut fenced = owner(None, false);
        fenced.authority = "fenced(Expired)".to_owned();
        fenced.fence = Some(DenyReason::Expired);
        fenced.fence_polling = true;
        let note = "node 2 restarted (now boot 2); a restarted node stays out until M10 (#42)";
        let mut seen = Seen {
            ready: true,
            ..Seen::default()
        };
        let mut lines = Vec::new();
        for (node, restart) in [
            (owner(None, false), None),
            (fenced.clone(), None),
            (fenced.clone(), None),
            (owner(None, false), None),
            (fenced.clone(), Some(note)),
            (owner(None, true), None),
        ] {
            lines.extend(ready_step(&mut seen, &node, restart, &[]));
        }
        assert_eq!(
            lines,
            [
                "not ready: writes not admitted".to_owned(),
                "not ready: writes not admitted: node 1's grant is fenced(Expired); writes \
                 resume by themselves once the old grant's expiry is proven, so do not restart \
                 node 1: a restarted node does not rejoin in M9 (achieveai/rDB#42)"
                    .to_owned(),
                "not ready: writes not admitted".to_owned(),
                format!(
                    "not ready: writes not admitted: node 1's grant is fenced(Expired); writes \
                     resume by themselves once the old grant's expiry is proven, so do not \
                     restart node 1: a restarted node does not rejoin in M9 (achieveai/rDB#42); \
                     once they resume, they pause soon, at the latest about 2 s after the next \
                     write, because {note}"
                ),
                "ready".to_owned(),
            ]
        );
    }

    /// Msg 8, F-004 and F-016: "resume by themselves" only while A1's after-fence poll is live
    /// and the reason is one the self-replace can lift. `fenced(Frozen)` never resumes, even
    /// before its first poll stops; `fenced(Expired)` whose poll stopped (a later freeze or
    /// delete) prints a new line saying so. The host row
    /// `status_says_the_after_fence_poll_stopped_when_the_record_is_frozen_later` proves the
    /// poll field on a real host.
    #[test]
    fn a_fence_promises_resume_only_while_the_poll_is_live_and_can_lift_it() {
        let fenced = |reason: DenyReason, polling: bool| {
            let mut node = owner(None, false);
            node.authority = format!("fenced({reason:?})");
            node.fence = Some(reason);
            node.fence_polling = polling;
            node
        };
        let resume = |reason: &str| {
            format!(
                "not ready: writes not admitted: node 1's grant is fenced({reason}); writes \
                 resume by themselves once the old grant's expiry is proven, so do not restart \
                 node 1: a restarted node does not rejoin in M9 (achieveai/rDB#42)"
            )
        };
        let stuck = |reason: &str| {
            format!(
                "not ready: writes not admitted: node 1's grant is fenced({reason}); nothing in \
                 M9 replaces it, so writes stay stopped for this run: to write again, quit and \
                 start rdb_dev again"
            )
        };
        for (reason, name) in [
            (DenyReason::Frozen, "Frozen"),
            (DenyReason::BootMismatch, "BootMismatch"),
            (
                DenyReason::AuthorityGenerationChanged,
                "AuthorityGenerationChanged",
            ),
        ] {
            for polling in [true, false] {
                let mut seen = Seen::default();
                assert_eq!(
                    ready_step(&mut seen, &fenced(reason, polling), None, &[]),
                    Some(stuck(name)),
                    "{name}, polling={polling}"
                );
            }
        }
        let mut seen = Seen {
            ready: true,
            ..Seen::default()
        };
        let lines: Vec<String> = [
            fenced(DenyReason::Expired, true),
            fenced(DenyReason::Expired, true),
            fenced(DenyReason::Expired, false),
            fenced(DenyReason::Expired, false),
        ]
        .iter()
        .filter_map(|node| ready_step(&mut seen, node, None, &[]))
        .collect();
        assert_eq!(lines, [resume("Expired"), stuck("Expired")]);
    }

    /// Walk 10, W10-D1 (tester r24n, r24n2, r26): a read-only start with links held, the store
    /// down, then `link heal all`. F1's activation CAS went unanswered and F1 blocked
    /// (`recovery=Blocked(ControlUnknown)`), while the poller kept promising writes resume by
    /// themselves; once a grant was back it printed the bare line, and writes never came back.
    /// A blocked recovery says so, names its reason and says writes stay stopped, over the
    /// fence's resume line and the held-link line. It promises no way out. Driven directly from
    /// r24n's states. Unit.
    #[test]
    fn a_blocked_recovery_says_writes_stay_stopped_over_any_resume_promise() {
        let blocked = "not ready: writes not admitted: node 1's recovery is \
                       Blocked(ControlUnknown), so writes stay stopped for this run: to write \
                       again, quit and start rdb_dev again";
        let with = |authority: &str, recovery: &str, fence: Option<DenyReason>| {
            let mut node = owner(None, false);
            node.authority = authority.to_owned();
            node.recovery = Some(recovery.to_owned());
            node.protection = Some("paused".to_owned());
            node.fence = fence;
            node.fence_polling = fence.is_some();
            node
        };
        let fenced = |recovery: &str| with("fenced(Expired)", recovery, Some(DenyReason::Expired));
        let held_links = [(NodeId(1), NodeId(2)), (NodeId(1), NodeId(3))];
        let mut seen = Seen::default();
        let lines: Vec<String> = [
            (fenced("Rebuilding"), &held_links[..]),
            (fenced("ActivationProposed"), &[][..]),
            (fenced("Blocked(ControlUnknown)"), &[][..]),
            (with("unheld", "Blocked(ControlUnknown)", None), &[][..]),
            (
                with("held(grant 2)", "Blocked(ControlUnknown)", None),
                &[][..],
            ),
            (
                with("held(grant 2)", "Blocked(ControlUnknown)", None),
                &held_links[..],
            ),
        ]
        .iter()
        .filter_map(|(node, links)| ready_step(&mut seen, node, None, links))
        .skip(2)
        .collect();
        assert_eq!(lines, [blocked]);
        let note = "node 2 restarted (now boot 2); a restarted node stays out until M10 (#42)";
        let stuck = with("held(grant 2)", "Blocked(ControlUnknown)", None);
        let mut healthy = stuck.clone();
        healthy.protection = Some("healthy".to_owned());
        for node in [stuck, healthy] {
            assert_eq!(
                ready_step(&mut Seen::default(), &node, Some(note), &[]),
                Some(format!("{blocked}; {note}")),
                "{node:?}"
            );
        }
    }

    /// Walk 10, W10-D1 after the kernel fix (A-R102 and its addendum): r24n's activation CAS,
    /// unanswered with the store down, now leaves F1 in `ActivationUnresolved`, and F1 resolves
    /// by itself once the store answers. So the line says writes wait for the store and resume
    /// by themselves, over the fence's resume line, and `ready` follows once they do. A stuck
    /// state still wins: a fence nothing lifts, a self-replace that stopped, or a restart with
    /// L1 refusing keeps the stay-stopped line. After a restart with L1 healthy, the pause
    /// promise follows the resume. Driven directly from r24n's states. Unit.
    #[test]
    fn a_resolving_activation_says_writes_resume_once_the_store_answers() {
        let resolving = "node 1's recovery is waiting for the control store to confirm \
                         activation; writes resume by themselves once it answers";
        let with = |authority: &str, recovery: &str, fence: Option<DenyReason>| {
            let mut node = owner(None, false);
            node.authority = authority.to_owned();
            node.recovery = Some(recovery.to_owned());
            node.protection = Some("paused".to_owned());
            node.fence = fence;
            node.fence_polling = fence.is_some();
            node
        };
        let fenced = |recovery: &str| with("fenced(Expired)", recovery, Some(DenyReason::Expired));
        let held_links = [(NodeId(1), NodeId(2)), (NodeId(1), NodeId(3))];
        let mut serving = owner(None, true);
        serving.authority = "held(grant 2)".to_owned();
        serving.protection = Some("healthy".to_owned());
        let mut seen = Seen::default();
        let lines: Vec<String> = [
            (fenced("Rebuilding"), &held_links[..]),
            (fenced("ActivationProposed"), &[][..]),
            (fenced("ActivationUnresolved"), &[][..]),
            (with("held(grant 2)", "ActivationUnresolved", None), &[][..]),
            (serving, &[][..]),
        ]
        .iter()
        .filter_map(|(node, links)| ready_step(&mut seen, node, None, links))
        .skip(2)
        .collect();
        assert_eq!(
            lines,
            [format!("not ready: writes not admitted: {resolving}"), "ready".to_owned()]
        );

        let reason = |node: &NodeStatus, note| ready_reason(node, false, note, &[]);
        let note = "node 2 restarted (now boot 2); a restarted node stays out until M10 (#42)";
        let mut healthy = with("held(grant 2)", "ActivationUnresolved", None);
        healthy.protection = Some("healthy".to_owned());
        assert_eq!(
            reason(&healthy, Some(note)),
            Some(format!(
                "{resolving}; once they resume, they pause soon, at the latest about \
                 {PAUSE_AGE_SECS} s after the next write, because {note}"
            ))
        );
        let paused = with("held(grant 2)", "ActivationUnresolved", None);
        assert_eq!(reason(&paused, Some(note)), Some(format!("{note}; {STUCK}")));
        let frozen = with(
            "fenced(Frozen)",
            "ActivationUnresolved",
            Some(DenyReason::Frozen),
        );
        assert_eq!(
            reason(&frozen, None),
            Some(format!(
                "node 1's grant is fenced(Frozen); nothing in M9 replaces it, so {STUCK}"
            ))
        );
        let mut stopped = with("unheld", "ActivationUnresolved", None);
        stopped.replace_stopped = true;
        assert_eq!(reason(&stopped, None), Some(gave_up_line(&stopped)));
    }

    /// Walk 8, W8-D1 (tester r6_3200): a freeze landed while A1 was between grants. A1 lost
    /// its self-replace CAS, answered the frozen record `NotOurs` and stopped, `unheld` for the
    /// rest of the run, and the fence's "resume by themselves" line stayed the last word because
    /// a reason that went away printed nothing. A reason that goes away while writes stay out
    /// is withdrawn with the bare line, so no promise outlives the state that made it.
    #[test]
    fn a_resume_promise_is_withdrawn_when_its_fence_goes_and_writes_stay_out() {
        // The tester's r6_3200 state, driven directly (the race took 0 of 16 live runs): A1
        // fenced with its poll live, then `unheld` with its self-replace stopped
        // (`replace_stopped`, kernel `self_replace_stopped`), then a later poll that changes
        // nothing. With and without a restart, the stuck line, once.
        let note = "node 2 restarted (now boot 2); a restarted node stays out until M10 (#42)";
        let gave_up = "node 1's grant is unheld and node 1 stopped trying for one: its grant \
                       record is frozen or not its own; nothing in M9 replaces it, so writes \
                       stay stopped for this run: to write again, quit and start rdb_dev again";
        let mut fenced = owner(None, false);
        fenced.authority = "fenced(Expired)".to_owned();
        fenced.fence = Some(DenyReason::Expired);
        fenced.fence_polling = true;
        let mut stopped = owner(None, false);
        stopped.authority = "unheld".to_owned();
        stopped.replace_stopped = true;
        for restart in [None, Some(note)] {
            let mut seen = Seen {
                ready: true,
                ..Seen::default()
            };
            let lines: Vec<String> = [&fenced, &stopped, &stopped]
                .iter()
                .filter_map(|node| ready_step(&mut seen, node, restart, &[]))
                .skip(1)
                .collect();
            let want = restart.map_or_else(
                || format!("not ready: writes not admitted: {gave_up}"),
                |note| format!("not ready: writes not admitted: {gave_up}; {note}"),
            );
            assert_eq!(lines, [want], "restart={restart:?}");
        }
        // Fenced with the poll stopped keeps the fence's own stuck line.
        let mut fenced_stopped = fenced.clone();
        fenced_stopped.fence_polling = false;
        fenced_stopped.replace_stopped = true;
        assert_eq!(
            ready_step(&mut Seen::default(), &fenced_stopped, None, &[]),
            Some(
                "not ready: writes not admitted: node 1's grant is fenced(Expired); nothing in M9 \
                 replaces it, so writes stay stopped for this run: to write again, quit and \
                 start rdb_dev again"
                    .to_owned()
            )
        );

        let mut fenced = owner(None, false);
        fenced.authority = "fenced(Expired)".to_owned();
        fenced.fence = Some(DenyReason::Expired);
        fenced.fence_polling = true;
        let mut unheld = owner(None, false);
        unheld.authority = "unheld".to_owned();
        let mut seen = Seen {
            ready: true,
            ..Seen::default()
        };
        let lines: Vec<String> = [fenced, unheld.clone(), unheld]
            .iter()
            .filter_map(|node| ready_step(&mut seen, node, None, &[]))
            .collect();
        assert_eq!(
            lines,
            [
                "not ready: writes not admitted: node 1's grant is fenced(Expired); writes \
                 resume by themselves once the old grant's expiry is proven, so do not restart \
                 node 1: a restarted node does not rejoin in M9 (achieveai/rDB#42)",
                "not ready: writes not admitted",
            ]
        );
    }

    /// Node 1's links held, as `link hold 1-2; link hold 1-3` leaves them.
    const HELD: [(NodeId, NodeId); 2] = [(NodeId(1), NodeId(2)), (NodeId(1), NodeId(3))];

    /// Walk 8, W8-D2 (tester r9, walk 7's 4b): links held, a write in flight, then the fence.
    /// L1 was paused, and the fence line promised writes resume by themselves. The grant came
    /// back (`held(grant 3)`) and writes did not, until `link heal all`. With L1 paused or
    /// reprotecting and a link of node 1's held, under a live poll, the line names the links
    /// writes also wait on, and still says not to restart node 1. With no link of node 1's held,
    /// the copies catch up by themselves and the resume line stands.
    #[test]
    fn a_fence_with_l1_paused_and_links_held_says_writes_also_wait_on_the_links() {
        for mode in ["paused", "reprotecting"] {
            let mut node = owner(None, false);
            node.authority = "fenced(Expired)".to_owned();
            node.fence = Some(DenyReason::Expired);
            node.fence_polling = true;
            node.protection = Some(mode.to_owned());
            assert_eq!(
                ready_step(&mut Seen::default(), &node, None, &HELD),
                Some(format!(
                    "not ready: writes not admitted: node 1's grant is fenced(Expired), \
                     l1={mode} and links 1-2, 1-3 are held; writes resume only once the old \
                     grant's expiry is proven and node 1's copies catch up, which needs those \
                     links: heal them (link heal all); do not restart node 1: a restarted node \
                     does not rejoin in M9 (achieveai/rDB#42)"
                )),
                "l1={mode}"
            );
            for held in [&[][..], &[(NodeId(2), NodeId(3))][..]] {
                assert_eq!(
                    ready_step(&mut Seen::default(), &node, None, held),
                    Some(
                        "not ready: writes not admitted: node 1's grant is fenced(Expired); \
                         writes resume by themselves once the old grant's expiry is proven, so \
                         do not restart node 1: a restarted node does not rejoin in M9 \
                         (achieveai/rDB#42)"
                            .to_owned()
                    ),
                    "l1={mode} held={held:?}"
                );
            }
        }
    }

    /// W8-D2 live re-walk (`r9/walks/w8/w8d2.out`): once A1 held grant 2 again, the fence's
    /// reason went, and the withdrawal (W8-D1) printed the bare line while L1 stayed paused for
    /// 20 s, until `link heal all`. Not admitting with L1 paused or reprotecting, a link of node
    /// 1's held and no restart, names the links writes wait on, as a condition and not a
    /// promise: a stopped A1 may also hold them out. At startup (`w8d2b.out`) L1 is paused, then
    /// reprotecting, with no link held, and catches up by itself: no reason.
    #[test]
    fn l1_paused_with_links_held_says_writes_wait_on_those_links() {
        let fenced = |mode: &str| {
            let mut node = owner(None, false);
            node.authority = "fenced(Expired)".to_owned();
            node.fence = Some(DenyReason::Expired);
            node.fence_polling = true;
            node.protection = Some(mode.to_owned());
            node
        };
        let held = |mode: &str, admits: bool| {
            let mut node = owner(None, admits);
            node.authority = "held(grant 2)".to_owned();
            node.protection = Some(mode.to_owned());
            node
        };
        let mut startup = Seen::default();
        assert_eq!(
            ready_step(&mut startup, &held("paused", false), None, &[]),
            None
        );
        assert_eq!(
            ready_step(&mut startup, &held("reprotecting", false), None, &[]),
            None
        );
        let wait = "not ready: writes not admitted: l1=paused and links 1-2, 1-3 are held: \
                    writes wait for node 1's copies to catch up, which needs those links: heal \
                    them (link heal all)";
        let mut seen = Seen {
            ready: true,
            ..Seen::default()
        };
        let lines: Vec<String> = [
            (held("paused", false), &HELD[..]),
            (fenced("paused"), &HELD[..]),
            (held("paused", false), &HELD[..]),
            (held("paused", false), &HELD[..]),
            (held("reprotecting", false), &[][..]),
            (held("healthy", true), &[][..]),
        ]
        .iter()
        .filter_map(|(node, links)| ready_step(&mut seen, node, None, links))
        .collect();
        assert_eq!(
            lines,
            [
                wait.to_owned(),
                "not ready: writes not admitted: node 1's grant is fenced(Expired), l1=paused \
                 and links 1-2, 1-3 are held; writes resume only once the old grant's expiry is \
                 proven and node 1's copies catch up, which needs those links: heal them (link \
                 heal all); do not restart node 1: a restarted node does not rejoin in M9 \
                 (achieveai/rDB#42)"
                    .to_owned(),
                wait.to_owned(),
                "not ready: writes not admitted".to_owned(),
                "ready".to_owned(),
            ]
        );
    }

    /// Message 7: after a secondary restarts, L1 keeps admitting writes for a while (walk 6c),
    /// and a bare `ready` then read as if writes would last. While admitted the line says they
    /// are about to pause and why; once L1 pauses, the not-ready line says so for the run.
    /// W7-D1 (tester r1): the pause came 1.89 s after the restart with no write at all, so the
    /// line promises only "soon", and an upper bound counted from the next write: a write with
    /// a copy out stays unsafe and pauses at L1's pause age (r3: 2.02 s).
    #[test]
    fn ready_after_a_restart_says_writes_pause_soon_not_only_after_the_next_write() {
        let note = "node 2 restarted (now boot 2); a restarted node stays out until M10 (#42)";
        let mut paused = owner(None, false);
        paused.protection = Some("paused".to_owned());
        let mut seen = Seen::default();
        let mut lines = Vec::new();
        for (node, restart) in [
            (owner(None, true), None),
            (owner(None, true), Some(note)),
            (owner(None, true), Some(note)),
            (paused, Some(note)),
        ] {
            lines.extend(ready_step(&mut seen, &node, restart, &[]));
        }
        assert_eq!(
            lines,
            [
                "ready".to_owned(),
                format!(
                    "ready for now; writes pause soon, at the latest about 2 s after the next \
                     write, because {note}"
                ),
                format!(
                    "not ready: writes not admitted: {note}; writes stay stopped for this run: \
                     to write again, quit and start rdb_dev again"
                ),
            ]
        );
    }

    /// W7-D1, W7-D2: every `ready`/`not ready` reason is derived from the state that decides it,
    /// checked against each state of the tester's walk-7 text matrix (s2b-tester.md, "Text
    /// matrix"). The stuck line ("writes stay stopped for this run") prints only where nothing
    /// in M9 brings writes back: L1 paused or reprotecting with a copy out, the owner itself
    /// restarted, or a fence no self-replace lifts. A fence whose poll is live, with a
    /// secondary restarted (FR, r3), promises the resume and the pause after it. Unit.
    #[test]
    fn every_ready_line_matches_the_state_that_decides_it() {
        let note = "node 2 restarted (now boot 2); a restarted node stays out until M10 (#42)";
        let failed = "node 2 stopped: its restart as boot 2 failed; a restarted node stays out \
                      until M10 (#42)";
        let owner_note = "the owner, node 1, restarted (now boot 2); a restarted node stays out \
                          until M10 (#42)";
        let stuck = "writes stay stopped for this run: to write again, quit and start rdb_dev \
                     again";
        let pause = |note: &str| {
            format!("pause soon, at the latest about 2 s after the next write, because {note}")
        };
        let resume = |reason: &str| {
            format!(
                "node 1's grant is fenced({reason}); writes resume by themselves once the old \
                 grant's expiry is proven, so do not restart node 1: a restarted node does not \
                 rejoin in M9 (achieveai/rDB#42)"
            )
        };
        let fenced_stuck = |reason: &str| {
            format!("node 1's grant is fenced({reason}); nothing in M9 replaces it, so {stuck}")
        };
        let with = |admits: bool, protection: Option<&str>, fence: Option<(DenyReason, bool)>| {
            let mut node = owner(None, admits);
            node.protection = protection.map(str::to_owned);
            if let Some((reason, polling)) = fence {
                node.authority = format!("fenced({reason:?})");
                node.fence = Some(reason);
                node.fence_polling = polling;
            }
            node
        };
        let mut restarted_owner = owner(None, false);
        restarted_owner.boot = BootId(2);
        restarted_owner.admits = None;
        restarted_owner.protection = None;
        let mut unheld = owner(None, false);
        unheld.authority = "unheld".to_owned();
        let expiring = Some((DenyReason::Expired, true));
        let rows: Vec<(&str, NodeStatus, Option<&str>, Option<String>)> = vec![
            (
                "H",
                with(true, Some("healthy"), None),
                None,
                Some("ready".to_owned()),
            ),
            (
                "HR",
                with(true, Some("healthy"), None),
                Some(note),
                Some(format!("ready for now; writes {}", pause(note))),
            ),
            (
                "SF admitting",
                with(true, Some("healthy"), None),
                Some(failed),
                Some(format!("ready for now; writes {}", pause(failed))),
            ),
            (
                "HP",
                with(false, Some("paused"), None),
                Some(note),
                Some(format!("not ready: writes not admitted: {note}; {stuck}")),
            ),
            (
                "HP reprotecting",
                with(false, Some("reprotecting"), None),
                Some(note),
                Some(format!("not ready: writes not admitted: {note}; {stuck}")),
            ),
            (
                "SF paused",
                with(false, Some("paused"), None),
                Some(failed),
                Some(format!("not ready: writes not admitted: {failed}; {stuck}")),
            ),
            (
                "FP",
                with(false, Some("healthy"), expiring),
                None,
                Some(format!(
                    "not ready: writes not admitted: {}",
                    resume("Expired")
                )),
            ),
            (
                "FS",
                with(false, Some("healthy"), Some((DenyReason::Expired, false))),
                None,
                Some(format!(
                    "not ready: writes not admitted: {}",
                    fenced_stuck("Expired")
                )),
            ),
            (
                "FZ",
                with(false, Some("healthy"), Some((DenyReason::Frozen, true))),
                None,
                Some(format!(
                    "not ready: writes not admitted: {}",
                    fenced_stuck("Frozen")
                )),
            ),
            (
                "U",
                unheld.clone(),
                None,
                Some("not ready: writes not admitted".to_owned()),
            ),
            (
                "U with a restart",
                unheld,
                Some(note),
                Some(format!(
                    "not ready: writes not admitted: writes are not admitted now; if they come \
                     back, they {}",
                    pause(note)
                )),
            ),
            (
                "FR",
                with(false, Some("healthy"), expiring),
                Some(note),
                Some(format!(
                    "not ready: writes not admitted: {}; once they resume, they {}",
                    resume("Expired"),
                    pause(note)
                )),
            ),
            (
                "FR, L1 paused",
                with(false, Some("paused"), expiring),
                Some(note),
                Some(format!("not ready: writes not admitted: {note}; {stuck}")),
            ),
            (
                "FR, poll stopped",
                with(false, Some("healthy"), Some((DenyReason::Expired, false))),
                Some(note),
                Some(format!(
                    "not ready: writes not admitted: {}; {note}",
                    fenced_stuck("Expired")
                )),
            ),
            (
                "RO",
                restarted_owner,
                Some(owner_note),
                Some(format!(
                    "not ready: writes not admitted: {owner_note}; {stuck}"
                )),
            ),
        ];
        for (state, node, restart, line) in rows {
            let admits = node.admits == Some(true);
            let mut seen = Seen {
                ready: !admits,
                ..Seen::default()
            };
            assert_eq!(
                ready_step(&mut seen, &node, restart, &[]),
                line,
                "state {state}"
            );
        }
    }

    /// Tester W3-D2 and walk 5: after a restart, `Unknown` for a request this session sent
    /// carries the note, and promises M10 only after the owner's restart or both secondaries'.
    /// After one secondary's it names the copy that can still resolve it (W5-D1). A request
    /// never sent, or no restart, stays bare. Msg 8, W6-D1: one node 1 does not hold in flight
    /// promises nothing, unless the owner restarted; the Db row
    /// `a_retry_of_a_put_refused_paused_after_a_restart_does_not_promise_a_status` proves
    /// `in_flight` on a real Db.
    #[test]
    fn status_unknown_carries_the_restart_note_and_waits_for_m10_only_when_no_copy_can_resolve() {
        let note = Some("NOTE");
        assert_eq!(unknown_line(true, Some(true), &[], None), "status Unknown");
        assert_eq!(
            unknown_line(false, Some(false), &[NodeId(2)], note),
            "status Unknown"
        );
        assert_eq!(
            unknown_line(true, Some(true), &[NodeId(2)], note),
            "status Unknown (not evidence of a lost write: it resolves once node 3, which did \
             not restart, can take it; NOTE)"
        );
        assert_eq!(
            unknown_line(true, Some(true), &[NodeId(3)], note),
            "status Unknown (not evidence of a lost write: it resolves once node 2, which did \
             not restart, can take it; NOTE)"
        );
        let m10 = "status Unknown (not evidence of a lost write: its outcome cannot be known \
                   until a restarted node rejoins, which is M10; NOTE)";
        assert_eq!(
            unknown_line(true, Some(true), &[NodeId(2), NodeId(3)], note),
            m10
        );
        assert_eq!(unknown_line(true, Some(true), &[NodeId(1)], note), m10);
        assert_eq!(
            unknown_line(true, Some(true), &[NodeId(1), NodeId(2)], note),
            m10
        );
        assert_eq!(unknown_line(true, Some(false), &[NodeId(1)], note), m10);
        let refused = "status Unknown (node 1 does not hold it in flight, so waiting will not \
                       change this: it was refused before it was queued, or never arrived; NOTE)";
        assert_eq!(unknown_line(true, Some(false), &[NodeId(3)], note), refused);
        assert_eq!(
            unknown_line(true, Some(false), &[NodeId(2), NodeId(3)], note),
            refused
        );
    }

    /// F-025: `node_status` leaves out a node 1 that does not answer in time, and the line then
    /// read as "not in flight; waiting will not change this", which nothing showed. Without an
    /// answer it says so and to ask again; after the owner's own restart the M10 line stands.
    /// Unit.
    #[test]
    fn status_unknown_says_try_again_when_node_1_did_not_answer() {
        let note = Some("NOTE");
        let silent = "status Unknown (node 1 did not answer in time, so whether it holds it in \
                      flight is not known; try again; NOTE)";
        assert_eq!(unknown_line(true, None, &[NodeId(3)], note), silent);
        assert_eq!(
            unknown_line(true, None, &[NodeId(2), NodeId(3)], note),
            silent
        );
        assert_eq!(
            unknown_line(false, None, &[NodeId(3)], note),
            "status Unknown"
        );
        assert_eq!(
            unknown_line(true, None, &[NodeId(1)], note),
            "status Unknown (not evidence of a lost write: its outcome cannot be known until a \
             restarted node rejoins, which is M10; NOTE)"
        );
    }

    /// Msg 9 adjacent defect: `node_status` leaves out a node whose restart failed (SF), so a
    /// restart set built from it missed node 2, and `status` of a write in flight said it waits
    /// for M10 although node 3, which did not restart, can still take it. The set comes from
    /// each node's boot, answered or not. Unit; walk w13 (r9/walks) shows it live.
    #[test]
    fn a_failed_secondary_restart_counts_as_restarted_for_status() {
        // Node 2's restart failed: it answers no `node_status`, but its boot moved on.
        let boots = |node: NodeId| {
            Some(if node == NodeId(2) {
                BootId(2)
            } else {
                BootId(1)
            })
        };
        let restarted = restarted_nodes(boots);
        assert_eq!(
            restarted,
            [NodeId(2)],
            "node 2 did not answer: its restart failed"
        );
        assert_eq!(
            unknown_line(true, Some(true), &restarted, Some("NOTE")),
            "status Unknown (not evidence of a lost write: it resolves once node 3, which did \
             not restart, can take it; NOTE)"
        );
    }

    /// W7-D3 (tester r12): after node 1's restart failed, the poller kept its last `ready`,
    /// because `node_status` leaves node 1 out and the poll printed nothing then. It now prints
    /// the stuck line once, naming the failed restart. `nodes` names a node left out, as
    /// stopped or as not answering, instead of dropping it (r11, r12). Unit; the live walk
    /// w12 (r9/walks) shows both on a real run.
    #[test]
    fn a_failed_owner_restart_says_not_ready_and_nodes_names_the_stopped_node() {
        let note = "the owner, node 1, stopped: its restart as boot 2 failed; a restarted node \
                    stays out until M10 (#42)";
        let mut seen = Seen::default();
        lines_ready(&mut seen);
        assert_eq!(
            stopped_owner_step(&mut seen, Some(note)),
            Some(format!(
                "not ready: writes not admitted: {note}; writes stay stopped for this run: to \
                 write again, quit and start rdb_dev again"
            ))
        );
        assert_eq!(
            stopped_owner_step(&mut seen, Some(note)),
            None,
            "printed once"
        );
        assert_eq!(
            missing_node_line(NodeId(1), Some(BootId(2)), true),
            "node=1 boot=2 stopped: its restart as boot 2 failed, so no process runs it"
        );
        assert_eq!(
            missing_node_line(NodeId(2), Some(BootId(1)), false),
            "node=2 boot=1 did not answer in time; try again"
        );
    }

    /// `seen` after a `ready` line, as the poller holds it before a failure.
    fn lines_ready(seen: &mut Seen) {
        assert_eq!(
            ready_step(seen, &owner(None, true), None, &[]),
            Some("ready".to_owned())
        );
    }

    /// F-012 (tester walk 7): rdb_dev printed an admin error as `KIND (detail)` only, so a
    /// permanent store error and a transient one read alike. The line carries `retry=` and
    /// `no_mutation=` as a `put` error does, and says in words whether running it again helps.
    /// Unit.
    #[test]
    fn an_admin_error_says_whether_running_it_again_can_help() {
        let error = |retry, no_mutation| ApiError {
            kind: rdb_core::contracts::errors::ErrorKind::Unavailable,
            retry,
            no_mutation,
            detail: "control store: list partitions/: x; nothing deleted".to_owned(),
        };
        assert_eq!(
            admin_error_line(NodeId(1), &error(RetryRule::BoundedJitter, true)),
            "err admin clear-grant 1: UNAVAILABLE retry=BoundedJitter no_mutation=true (control \
             store: list partitions/: x; nothing deleted); running it again can help"
        );
        assert_eq!(
            admin_error_line(NodeId(1), &error(RetryRule::NotWired, true)),
            "err admin clear-grant 1: UNAVAILABLE retry=NotWired no_mutation=true (control \
             store: list partitions/: x; nothing deleted); running it again will not help: it \
             fails the same way each time"
        );
        assert!(
            admin_error_line(NodeId(1), &error(RetryRule::QueryStatus, false)).ends_with(
                "no_mutation=false (control store: list partitions/: x; nothing deleted); \
                        run it again: it reads where the record stands before it deletes \
                        anything"
            )
        );
    }

    /// Re-review F-027: the words follow `retry` and `no_mutation` together, never `retry`
    /// alone. A permanent delete error is `NotWired` with `no_mutation=false`, and its detail
    /// says the delete may have applied and to run it again to read where it stands; "running
    /// it again will not help" then contradicted it. A rerun reads the record first, so it
    /// shows whether the delete applied; only a record still there fails the same way. Unit.
    #[test]
    fn an_admin_error_that_may_have_deleted_says_a_rerun_shows_whether_it_did() {
        let error = |retry| ApiError {
            kind: rdb_core::contracts::errors::ErrorKind::Unavailable,
            retry,
            no_mutation: false,
            detail: "control store: delete grants/1 at revision 5: permission denied; the \
                     delete may have applied; run it again to read where it stands"
                .to_owned(),
        };
        let permanent = admin_error_line(NodeId(1), &error(RetryRule::NotWired));
        assert_eq!(
            permanent,
            "err admin clear-grant 1: UNAVAILABLE retry=NotWired no_mutation=false (control \
             store: delete grants/1 at revision 5: permission denied; the delete may have \
             applied; run it again to read where it stands); run it again to see whether the \
             delete applied: it reads the record first, and only a record still there fails \
             the same way again"
        );
        let transient = admin_error_line(NodeId(1), &error(RetryRule::BoundedJitter));
        assert!(
            transient.ends_with(
                "; running it again can help, and shows whether the delete applied: it reads \
                 the record first"
            ),
            "{transient}"
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
