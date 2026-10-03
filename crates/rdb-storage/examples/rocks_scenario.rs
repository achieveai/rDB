//! M8 manual entry point: write canonical batches to a real RocksDB engine, crash, reopen,
//! verify (S0); switch a partition to a new generation and read through to the old one (S1).
//!
//! ```text
//! cargo run -p rdb-storage --example rocks_scenario -- --dir <D> [--log-dir <L>] <command>
//!
//!   write --records N [--partition P] [--generation G] [--abort]
//!   flush [--partition P --generation G]
//!   inherit --partition P --from G1 --to G2 --base B [--abort]
//!   delete [--partition P] [--generation G]
//!   snapshot [--partition P] [--generation G]
//!   read [--partition P] [--generation G] [--ns NS] [--key-hex H]
//!   verify
//!   dump
//! ```
//!
//! See `crates/rdb-storage/README.md` for the scenario, the exit-code table and the log events.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use rdb_core::contracts::authority::Lineage;
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::envelope::ReplicationEnvelope;
use rdb_core::contracts::ids::{
    AffinityId, ConfigVersion, Generation, OwnerEpoch, PartitionId, Seq, SnapshotHandle, TenantId,
};
use rdb_core::contracts::storage::{CapturedPrefix, Namespace, SnapshotRead};
use rdb_core::contracts::txn::scoped_key;
use rdb_core::replication::append::PROGRESS_KEY;
use rdb_sim::storage::history::canonical_history_from;
use rdb_storage::{
    dump, verify_lineage, InheritError, Inherited, InjectedFault, OpenError, RocksEngine,
};

#[path = "../tests/support/delete.rs"]
mod delete;
use delete::delete_batch;

const LOG_TARGET: &str = "rocks_scenario";

/// Exit codes. Pinned: the tester's scenario sheet and the README name them.
mod exit {
    pub const OK: u8 = 0;
    pub const USAGE: u8 = 2;
    pub const LINEAGE_FAULT: u8 = 3;
    pub const DURABLE_ABOVE_APPLIED: u8 = 4;
    pub const LOCKED: u8 = 5;
    pub const LAYOUT_REFUSED: u8 = 6;
    pub const BACKEND: u8 = 7;
    pub const STORAGE_FAULT: u8 = 8;
    pub const NO_DATABASE: u8 = 9;
    pub const LINEAGE_CONFLICT: u8 = 10;
    pub const BEHIND_CUTOFF: u8 = 11;
    pub const HISTORY_MISSING: u8 = 12;
}

const USAGE: &str = "\
usage: rocks_scenario --dir <D> [--log-dir <L>] <command>
  write --records N [--partition P] [--generation G] [--abort] [--inject commit]
        commit the next N canonical batches of lineage (P, G) (default 1, 1), resuming after
        its applied seq. --abort: after the last commit, flush the log and abort() the process.
  flush [--partition P --generation G] [--inject wal-flush|mark-write]
          sync the WAL; every lineage's durable (or only (P, G)'s) moves to its applied,
          and each captured lineage's ancestors' durable to their base
  inherit --partition P --from G1 --to G2 --base B [--abort] [--inject switch|copy-batch]
          make (P, G2) read through to (P, G1) as of seq B. --abort: abort() right after
          inherit returns, whatever it returned.
  delete [--partition P] [--generation G]
          commit the next seq as a delete of k, with its chained History record
  snapshot [--partition P] [--generation G]
          every record of (P, G)'s RocksSnapshot, through the chain
  read [--partition P] [--generation G] [--ns user|dedup|history|progress|meta] [--key-hex H]
          one record as (P, G) sees it, through the chain; default key: the canonical user key
          k (ns user) or the progress key (ns progress); other namespaces need --key-hex
  --inject  debug builds only: that storage call fails as RocksDB would (exit 8)
  verify  open, then check every lineage's chain; exit 0 only if all pass
  dump    list every stored record, read-only (does not flush the WAL)
exit codes: 0 ok, 2 usage, 3 lineage fault, 4 durable>applied, 5 locked,
            6 layout refused (column families / format marker / engine record), 7 backend,
            8 storage fault, 9 no database at --dir (all but write),
            10 inherit lineage conflict, 11 inherit behind cutoff,
            12 inherit: history cannot rebuild the parent as of base;
            --abort ends with abort() (Windows 0xC0000409 = 3221226505)
logs: <L>/rocks_scenario.jsonl, default <parent of D>/logs";

#[derive(Debug)]
enum Command {
    Write {
        records: u64,
        partition: PartitionId,
        generation: Generation,
        abort: bool,
        inject: Option<InjectedFault>,
    },
    Flush {
        /// One lineage to capture; `None` captures every lineage.
        only: Option<(PartitionId, Generation)>,
        inject: Option<InjectedFault>,
    },
    Inherit {
        partition: PartitionId,
        from: Generation,
        to: Generation,
        base: Seq,
        abort: bool,
        inject: Option<InjectedFault>,
    },
    Delete {
        partition: PartitionId,
        generation: Generation,
    },
    Snapshot {
        partition: PartitionId,
        generation: Generation,
    },
    Read {
        partition: PartitionId,
        generation: Generation,
        ns: Namespace,
        key: Vec<u8>,
    },
    Verify,
    Dump,
}

impl Command {
    const fn name(&self) -> &'static str {
        match self {
            Self::Write { .. } => "write",
            Self::Flush { .. } => "flush",
            Self::Inherit { .. } => "inherit",
            Self::Delete { .. } => "delete",
            Self::Snapshot { .. } => "snapshot",
            Self::Read { .. } => "read",
            Self::Verify => "verify",
            Self::Dump => "dump",
        }
    }
}

#[derive(Debug)]
struct Args {
    dir: PathBuf,
    log_dir: PathBuf,
    command: Command,
}

/// Upper bound on `write --records`. The canonical history is built in memory, so an unbounded
/// N is an out-of-memory crash rather than a usage error (defect T3).
const MAX_RECORDS: u64 = 1_000_000;

/// Every per-command flag as given. Each command names the ones it takes; any other one given
/// is a usage error, never silently ignored (paper cut P3).
#[derive(Debug, Default)]
struct Flags {
    records: Option<u64>,
    partition: Option<u32>,
    generation: Option<u64>,
    from: Option<u64>,
    to: Option<u64>,
    base: Option<u64>,
    ns: Option<Namespace>,
    key: Option<Vec<u8>>,
    abort: bool,
    inject: Option<InjectedFault>,
}

impl Flags {
    fn allow(&self, cmd: &str, allowed: &[&str]) -> Result<(), String> {
        let given = [
            ("--records", self.records.is_some()),
            ("--partition", self.partition.is_some()),
            ("--generation", self.generation.is_some()),
            ("--from", self.from.is_some()),
            ("--to", self.to.is_some()),
            ("--base", self.base.is_some()),
            ("--ns", self.ns.is_some()),
            ("--key-hex", self.key.is_some()),
            ("--abort", self.abort),
            ("--inject", self.inject.is_some()),
        ];
        match given
            .iter()
            .find(|(name, set)| *set && !allowed.contains(name))
        {
            Some((name, _)) => Err(format!("{name} does not apply to {cmd}")),
            None => Ok(()),
        }
    }

    /// The `--inject` point, if given and one `cmd` reaches.
    fn inject_for(
        &self,
        cmd: &str,
        points: &[InjectedFault],
    ) -> Result<Option<InjectedFault>, String> {
        match self.inject {
            Some(fault) if !points.contains(&fault) => {
                let names: Vec<&str> = points.iter().map(|p| cli_name(*p)).collect();
                Err(format!(
                    "{cmd} takes --inject {}, not {}",
                    names.join("|"),
                    cli_name(fault)
                ))
            }
            inject => Ok(inject),
        }
    }
}

fn parse(mut argv: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut dir = None;
    let mut log_dir = None;
    let mut command = None;
    let mut f = Flags::default();
    let mut seen = std::collections::BTreeSet::new();
    while let Some(arg) = argv.next() {
        // S1-P4: a repeated flag is refused, never last-wins.
        if arg.starts_with("--") && !seen.insert(arg.clone()) {
            return Err(format!("{arg} given twice"));
        }
        let mut value = |name: &str| argv.next().ok_or_else(|| format!("{name} needs a value"));
        match arg.as_str() {
            "--dir" => dir = Some(PathBuf::from(value("--dir")?)),
            "--log-dir" => log_dir = Some(PathBuf::from(value("--log-dir")?)),
            "--records" => f.records = Some(number(&value("--records")?, "--records")?),
            "--partition" => f.partition = Some(number(&value("--partition")?, "--partition")?),
            "--generation" => {
                f.generation = Some(number(&value("--generation")?, "--generation")?);
            }
            "--from" => f.from = Some(number(&value("--from")?, "--from")?),
            "--to" => f.to = Some(number(&value("--to")?, "--to")?),
            "--base" => f.base = Some(number(&value("--base")?, "--base")?),
            "--ns" => f.ns = Some(namespace(&value("--ns")?)?),
            "--key-hex" => f.key = Some(unhex(&value("--key-hex")?)?),
            "--abort" => f.abort = true,
            "--inject" => f.inject = Some(fault(&value("--inject")?)?),
            "write" | "flush" | "inherit" | "delete" | "snapshot" | "read" | "verify" | "dump"
                if command.is_none() =>
            {
                command = Some(arg);
            }
            "-h" | "--help" => return Err(String::new()),
            other => return Err(format!("unexpected argument {other:?}")),
        }
    }
    let dir = dir.ok_or("--dir is required")?;
    let log_dir = log_dir.unwrap_or_else(|| {
        dir.parent()
            .map_or_else(|| PathBuf::from("logs"), |p| p.join("logs"))
    });
    let partition = PartitionId(f.partition.unwrap_or(1));
    let command = match command.as_deref() {
        Some("write") => {
            f.allow(
                "write",
                &[
                    "--records",
                    "--partition",
                    "--generation",
                    "--abort",
                    "--inject",
                ],
            )?;
            Command::Write {
                records: match f.records {
                    Some(n @ 1..=MAX_RECORDS) => n,
                    Some(n) => return Err(format!("--records takes 1..={MAX_RECORDS}, got {n}")),
                    None => return Err("write needs --records N".to_string()),
                },
                partition,
                generation: Generation(f.generation.unwrap_or(1)),
                abort: f.abort,
                inject: f.inject_for("write", &[InjectedFault::Commit])?,
            }
        }
        Some("flush") => {
            f.allow("flush", &["--partition", "--generation", "--inject"])?;
            Command::Flush {
                only: match (f.partition, f.generation) {
                    (Some(p), Some(g)) => Some((PartitionId(p), Generation(g))),
                    (None, None) => None,
                    _ => {
                        return Err("flush takes --partition and --generation together".to_string())
                    }
                },
                inject: f.inject_for(
                    "flush",
                    &[InjectedFault::WalFlush, InjectedFault::MarkWrite],
                )?,
            }
        }
        Some("inherit") => {
            f.allow(
                "inherit",
                &[
                    "--partition",
                    "--from",
                    "--to",
                    "--base",
                    "--abort",
                    "--inject",
                ],
            )?;
            Command::Inherit {
                partition,
                from: Generation(f.from.ok_or("inherit needs --from G1")?),
                to: Generation(f.to.ok_or("inherit needs --to G2")?),
                base: Seq(f.base.ok_or("inherit needs --base B")?),
                abort: f.abort,
                inject: f.inject_for(
                    "inherit",
                    &[InjectedFault::Switch, InjectedFault::CopyBatch],
                )?,
            }
        }
        Some("delete") => {
            f.allow("delete", &["--partition", "--generation"])?;
            Command::Delete {
                partition,
                generation: Generation(f.generation.unwrap_or(1)),
            }
        }
        Some("snapshot") => {
            f.allow("snapshot", &["--partition", "--generation"])?;
            Command::Snapshot {
                partition,
                generation: Generation(f.generation.unwrap_or(1)),
            }
        }
        Some("read") => {
            f.allow(
                "read",
                &["--partition", "--generation", "--ns", "--key-hex"],
            )?;
            let ns = f.ns.unwrap_or(Namespace::User);
            let key = match (f.key, ns) {
                (Some(key), _) => key,
                (None, Namespace::User) => scoped_key(TenantId(1), AffinityId(1), b"k").to_vec(),
                (None, Namespace::Progress) => PROGRESS_KEY.to_vec(),
                (None, other) => {
                    return Err(format!("read --ns {} needs --key-hex", ns_name(other)))
                }
            };
            Command::Read {
                partition,
                generation: Generation(f.generation.unwrap_or(1)),
                ns,
                key,
            }
        }
        Some("verify") => {
            f.allow("verify", &[])?;
            Command::Verify
        }
        Some("dump") => {
            f.allow("dump", &[])?;
            Command::Dump
        }
        _ => return Err(
            "a command is required: write, flush, inherit, delete, snapshot, read, verify or dump"
                .to_string(),
        ),
    };
    Ok(Args {
        dir,
        log_dir,
        command,
    })
}

/// The last seq `write` commits. A lineage resumed near `u64::MAX` must refuse, not wrap or
/// panic (defect T3).
fn last_seq(start: u64, records: u64) -> Result<u64, String> {
    start
        .checked_add(records)
        .ok_or_else(|| format!("applied {start} + --records {records} overflows the seq space"))
}

/// `--inject` takes a storage call by name. Debug builds only: a release exe has no way in.
fn fault(raw: &str) -> Result<InjectedFault, String> {
    if cfg!(not(debug_assertions)) {
        return Err("--inject needs a debug build".to_string());
    }
    match raw {
        "commit" => Ok(InjectedFault::Commit),
        "wal-flush" => Ok(InjectedFault::WalFlush),
        "mark-write" => Ok(InjectedFault::MarkWrite),
        "switch" => Ok(InjectedFault::Switch),
        "copy-batch" => Ok(InjectedFault::CopyBatch),
        other => Err(format!(
            "--inject takes commit, wal-flush, mark-write, switch or copy-batch, got {other:?}"
        )),
    }
}

/// The `--inject` spelling of `fault`, for messages the operator reads (paper cut P8).
const fn cli_name(fault: InjectedFault) -> &'static str {
    fault.point()
}

/// `--ns` takes a contract namespace by its lowercase name.
fn namespace(raw: &str) -> Result<Namespace, String> {
    [
        Namespace::User,
        Namespace::Dedup,
        Namespace::History,
        Namespace::Progress,
        Namespace::Meta,
    ]
    .into_iter()
    .find(|ns| ns_name(*ns) == raw)
    .ok_or_else(|| format!("--ns takes user, dedup, history, progress or meta, got {raw:?}"))
}

const fn ns_name(ns: Namespace) -> &'static str {
    match ns {
        Namespace::User => "user",
        Namespace::Dedup => "dedup",
        Namespace::History => "history",
        Namespace::Progress => "progress",
        Namespace::Meta => "meta",
    }
}

/// `--key-hex`: an even number of hex digits.
fn unhex(raw: &str) -> Result<Vec<u8>, String> {
    let bad = || format!("--key-hex takes an even number of hex digits, got {raw:?}");
    if !raw.len().is_multiple_of(2) || !raw.is_ascii() {
        return Err(bad());
    }
    (0..raw.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&raw[i..i + 2], 16).map_err(|_| bad()))
        .collect()
}

/// Say so when an armed fault was never reached, e.g. `flush --inject mark-write` with every
/// durable already at applied: no mark to write, so nothing failed (defect T4). Without this the
/// run reads as a fault path that ran and passed.
fn report_unreached(engine: &RocksEngine, cmd: &str) {
    #[cfg(debug_assertions)]
    if let Some(fault) = engine.armed_fault() {
        let point = cli_name(fault);
        tracing::warn!(target: LOG_TARGET, point, "fault_not_reached");
        println!("{cmd} inject point {point} not reached: no storage call needed it");
    }
    #[cfg(not(debug_assertions))]
    let _ = (engine, cmd);
}

/// Arm `inject` on `engine`. The release arm is unreachable: `parse` refuses `--inject` there.
#[allow(clippy::needless_pass_by_ref_mut)]
fn arm(engine: &mut RocksEngine, inject: Option<InjectedFault>) {
    #[cfg(debug_assertions)]
    if let Some(fault) = inject {
        engine.inject_fault(fault);
    }
    #[cfg(not(debug_assertions))]
    let _ = (engine, inject);
}

fn number<T: std::str::FromStr>(raw: &str, name: &str) -> Result<T, String> {
    raw.parse()
        .map_err(|_| format!("{name} takes a number, got {raw:?}"))
}

fn main() -> ExitCode {
    let args = match parse(std::env::args().skip(1)) {
        Ok(args) => args,
        Err(message) => {
            if !message.is_empty() {
                eprintln!("error: {message}");
            }
            eprintln!("{USAGE}");
            return ExitCode::from(if message.is_empty() {
                exit::OK
            } else {
                exit::USAGE
            });
        }
    };
    let mut config = config_log::LogConfig::for_application("rocks_scenario");
    config.dir.clone_from(&args.log_dir);
    config.file_name = "rocks_scenario.jsonl".to_string();
    let guard = match config_log::init(config) {
        Ok(guard) => guard,
        Err(e) => {
            eprintln!(
                "error: cannot start logging in {}: {e}",
                args.log_dir.display()
            );
            return ExitCode::from(exit::BACKEND);
        }
    };
    eprintln!(
        "log={}",
        args.log_dir.join("rocks_scenario.jsonl").display()
    );
    // Every line this invocation writes carries `cmd`, `pid` and `db_dir` (critic F7): the log
    // file is shared by every invocation, so these are what tell them apart.
    let span = tracing::info_span!(
        target: LOG_TARGET,
        "invocation",
        cmd = args.command.name(),
        pid = std::process::id(),
        db_dir = %args.dir.display()
    );
    let entered = span.enter();
    tracing::info!(target: LOG_TARGET, command = ?args.command, "invocation_start");
    let (code, guard) = run(&args, guard);
    tracing::info!(target: LOG_TARGET, exit_code = code, "invocation_end");
    drop(entered);
    drop(guard);
    ExitCode::from(code)
}

/// Returns the exit code and hands the log guard back for an orderly drop. The guard travels
/// in so `write --abort` can flush it before dying (critic F2): `abort()` skips every
/// destructor, and the non-blocking writer flushes only on drop.
fn run(args: &Args, guard: config_log::LogGuard) -> (u8, config_log::LogGuard) {
    match &args.command {
        Command::Write {
            records,
            partition,
            generation,
            abort,
            inject,
        } => write(
            args,
            *records,
            *partition,
            *generation,
            *abort,
            *inject,
            guard,
        ),
        Command::Flush { only, inject } => (flush(&args.dir, *only, *inject), guard),
        Command::Inherit {
            partition,
            from,
            to,
            base,
            abort,
            inject,
        } => inherit(
            &args.dir,
            (*partition, *from, *to, *base),
            *abort,
            *inject,
            guard,
        ),
        Command::Delete {
            partition,
            generation,
        } => (delete(&args.dir, *partition, *generation), guard),
        Command::Snapshot {
            partition,
            generation,
        } => (snapshot(&args.dir, *partition, *generation), guard),
        Command::Read {
            partition,
            generation,
            ns,
            key,
        } => (read(&args.dir, *partition, *generation, *ns, key), guard),
        Command::Verify => (verify(&args.dir), guard),
        Command::Dump => (dump_all(&args.dir), guard),
    }
}

/// `write` may create the database; `verify` and `flush` only inspect one (defect D2);
/// `read` and `snapshot` only read, so they take no lock (S1-P2, L-R183u).
fn open(dir: &Path, cmd: &str) -> Result<RocksEngine, u8> {
    let opened = match cmd {
        "write" => RocksEngine::open(dir),
        "read" | "snapshot" => RocksEngine::open_read_only(dir),
        _ => RocksEngine::open_existing(dir),
    };
    opened.map_err(|e| {
        println!("{cmd} REFUSED {e}");
        open_code(&e)
    })
}

fn open_code(e: &OpenError) -> u8 {
    match e {
        OpenError::Locked { .. } => exit::LOCKED,
        OpenError::DurableAboveApplied { .. } => exit::DURABLE_ABOVE_APPLIED,
        OpenError::ColumnFamilies { .. }
        | OpenError::Format { .. }
        | OpenError::CorruptRecord { .. } => exit::LAYOUT_REFUSED,
        OpenError::NoDatabase { .. } => exit::NO_DATABASE,
        OpenError::Backend { .. } => exit::BACKEND,
    }
}

fn write(
    args: &Args,
    records: u64,
    partition: PartitionId,
    generation: Generation,
    abort: bool,
    inject: Option<InjectedFault>,
    guard: config_log::LogGuard,
) -> (u8, config_log::LogGuard) {
    let mut engine = match open(&args.dir, "write") {
        Ok(engine) => engine,
        Err(code) => return (code, guard),
    };
    arm(&mut engine, inject);
    // Resume rule (critic F7, S1 row #2): build records applied + 1..=applied + N chained from
    // the head digest the lineage holds, so a second run never re-commits seq 1 and an inherited
    // lineage chains from its parent's record at base.
    let start = engine.buffered_applied(partition, generation).0;
    let head = match head_digest(&engine, partition, generation, start) {
        Ok(head) => head,
        Err(message) => {
            println!("write REFUSED {message}");
            return (exit::LINEAGE_FAULT, guard);
        }
    };
    let lineage = Lineage {
        partition,
        generation,
        owner_epoch: OwnerEpoch(1),
    };
    let last = match last_seq(start, records) {
        Ok(last) => last,
        Err(message) => {
            println!("write REFUSED {message}");
            return (exit::USAGE, guard);
        }
    };
    // rdb-sim documents that encoding cannot fail for records this small, so no hand scenario
    // reaches an error here; a panic is the loud failure if that ever stops holding (L-R182w).
    let history = canonical_history_from(lineage, ConfigVersion(1), (Seq(start), head), last)
        .expect("rdb-sim: canonical records always encode");
    for batch in history.batches {
        let seq = batch.seq.0;
        if let Err(fault) = engine.commit(batch) {
            println!(
                "write FAILED at seq={seq}: {fault:?}{}",
                refusal(&engine, partition, generation)
            );
            report_unreached(&engine, "write");
            return (exit::STORAGE_FAULT, guard);
        }
    }
    let applied = engine.buffered_applied(partition, generation).0;
    let durable = engine.durable(partition, generation).0;
    println!(
        "write partition={} generation={} committed={}..={} applied={applied} durable={durable}",
        partition.0,
        generation.0,
        start + 1,
        last
    );
    if abort {
        tracing::warn!(
            target: LOG_TARGET,
            seq = applied,
            applied,
            durable,
            "abort_intentional"
        );
        die(guard, &format!("after seq={applied} applied={applied}"));
    }
    (exit::OK, guard)
}

/// The record digest at `start`, read through the chain: [`Digest::ROOT`] at 0.
fn head_digest(
    engine: &RocksEngine,
    partition: PartitionId,
    generation: Generation,
    start: u64,
) -> Result<Digest, String> {
    if start == 0 {
        return Ok(Digest::ROOT);
    }
    let record = engine
        .history_at(partition, generation, Seq(start))
        .map_err(|fault| format!("head record at seq={start}: {fault:?}"))?
        .ok_or_else(|| format!("no head record at seq={start}"))?
        .0;
    ReplicationEnvelope::decode(&record)
        .map(|envelope| envelope.record_digest)
        .map_err(|_| format!("head record at seq={start} does not decode"))
}

/// Flush the log first (critic F2), then `abort()`. The engine is never dropped, so the crash is
/// real.
fn die(guard: config_log::LogGuard, what: &str) -> ! {
    drop(guard);
    eprintln!("ABORT intentional {what}");
    std::process::abort();
}

/// `inherit`: open, arm, inherit, report. `--abort` dies right after `inherit` returns, Ok or
/// Err, so the directory holds exactly what the call left (design R2 §1).
fn inherit(
    dir: &Path,
    (partition, from, to, base): (PartitionId, Generation, Generation, Seq),
    abort: bool,
    inject: Option<InjectedFault>,
    guard: config_log::LogGuard,
) -> (u8, config_log::LogGuard) {
    let mut engine = match open(dir, "inherit") {
        Ok(engine) => engine,
        Err(code) => return (code, guard),
    };
    arm(&mut engine, inject);
    let line = format!(
        "inherit partition={} from={} to={} base={}",
        partition.0, from.0, to.0, base.0
    );
    let code = match engine.inherit(partition, from, to, base) {
        Ok(done) => {
            let mode = match done {
                Inherited::Linked => "linked".to_string(),
                Inherited::Copied { keys } => format!("copied keys={keys}"),
                Inherited::AlreadyInherited => "noop".to_string(),
            };
            println!(
                "{line} mode={mode} applied={} durable={}",
                engine.buffered_applied(partition, to).0,
                engine.durable(partition, to).0
            );
            exit::OK
        }
        Err(e) => {
            let (verdict, code) = match e {
                InheritError::LineageConflict { .. } => ("REFUSED", exit::LINEAGE_CONFLICT),
                InheritError::BehindCutoff { .. } => ("REFUSED", exit::BEHIND_CUTOFF),
                InheritError::HistoryMissing { .. } => ("REFUSED", exit::HISTORY_MISSING),
                InheritError::Storage(_) => ("FAILED", exit::STORAGE_FAULT),
            };
            println!("{line} {verdict} {e}");
            code
        }
    };
    report_unreached(&engine, "inherit");
    if abort {
        tracing::warn!(target: LOG_TARGET, exit_code = code, "abort_intentional");
        die(guard, &format!("after inherit exit_code={code}"));
    }
    (code, guard)
}

/// `delete`: the next seq of `(partition, generation)` deletes the canonical user key k.
fn delete(dir: &Path, partition: PartitionId, generation: Generation) -> u8 {
    let mut engine = match open(dir, "delete") {
        Ok(engine) => engine,
        Err(code) => return code,
    };
    let start = engine.buffered_applied(partition, generation).0;
    let line = format!(
        "delete partition={} generation={}",
        partition.0, generation.0
    );
    let Some(seq) = start.checked_add(1) else {
        println!("{line} REFUSED applied {start} is the top of the seq space");
        return exit::USAGE;
    };
    let head = match head_digest(&engine, partition, generation, start) {
        Ok(head) => head,
        Err(message) => {
            println!("{line} REFUSED {message}");
            return exit::LINEAGE_FAULT;
        }
    };
    let lineage = Lineage {
        partition,
        generation,
        owner_epoch: OwnerEpoch(1),
    };
    // rdb-core documents that encoding cannot fail for records this small (as `write`).
    let batch = delete_batch(lineage, Seq(seq), head).expect("rdb-core: a delete record encodes");
    match engine.commit(batch) {
        Ok(applied) => {
            println!("{line} seq={seq} applied={}", applied.0);
            exit::OK
        }
        Err(fault) => {
            println!(
                "{line} FAILED at seq={seq}: {fault:?}{}",
                refusal(&engine, partition, generation)
            );
            exit::STORAGE_FAULT
        }
    }
}

/// `snapshot`: every record of the lineage's view, as the differential will compare it.
fn snapshot(dir: &Path, partition: PartitionId, generation: Generation) -> u8 {
    let engine = match open(dir, "snapshot") {
        Ok(engine) => engine,
        Err(code) => return code,
    };
    let line = format!(
        "snapshot partition={} generation={}",
        partition.0, generation.0
    );
    if let Some((parent, base)) = engine.staging(partition, generation) {
        println!(
            "{line} REFUSED a full copy from generation {} at base {} is staged and never switched",
            parent.0, base.0
        );
        return exit::LINEAGE_FAULT;
    }
    match engine.snapshot(partition, generation, SnapshotHandle(1)) {
        Ok(view) => {
            for (ns, key, value, version, from) in view.entries() {
                println!(
                    "{} key={} version={version} from={} value={}",
                    ns_name(ns),
                    hex(key),
                    from.0,
                    hex(value)
                );
            }
            println!("{line} at={} records={}", view.at().0, view.len());
            exit::OK
        }
        Err(fault) => {
            println!("{line} FAILED {fault:?}");
            exit::STORAGE_FAULT
        }
    }
}

/// `read`: one record as the lineage sees it, through the chain.
fn read(
    dir: &Path,
    partition: PartitionId,
    generation: Generation,
    ns: Namespace,
    key: &[u8],
) -> u8 {
    let engine = match open(dir, "read") {
        Ok(engine) => engine,
        Err(code) => return code,
    };
    let line = format!(
        "read partition={} generation={} ns={} key={}",
        partition.0,
        generation.0,
        ns_name(ns),
        hex(key)
    );
    if let Some((parent, base)) = engine.staging(partition, generation) {
        println!(
            "{line} REFUSED a full copy from generation {} at base {} is staged and never switched",
            parent.0, base.0
        );
        return exit::LINEAGE_FAULT;
    }
    match engine.get(partition, generation, ns, key) {
        Ok(Some((from, version, value))) => {
            tracing::info!(
                target: LOG_TARGET,
                partition = partition.0,
                generation = generation.0,
                ns = ns_name(ns),
                key = %hex(key),
                from = from.0,
                version,
                "read_found"
            );
            println!(
                "{line} version={version} from={} value={}",
                from.0,
                hex(&value)
            );
            exit::OK
        }
        Ok(None) => {
            tracing::info!(
                target: LOG_TARGET,
                partition = partition.0,
                generation = generation.0,
                ns = ns_name(ns),
                key = %hex(key),
                "read_absent"
            );
            println!("{line} absent");
            exit::OK
        }
        Err(fault) => {
            println!("{line} FAILED {fault:?}");
            exit::STORAGE_FAULT
        }
    }
}

/// Why commit refuses every write to `(partition, generation)`, as a suffix for a FAILED line
/// (S1-P8), or `""` when nothing about the lineage refuses writes. The engine's answer is the
/// contract's bare `WriteFailed`; the reason is the lineage's own state.
fn refusal(engine: &RocksEngine, partition: PartitionId, generation: Generation) -> String {
    let at = format!(" partition={} generation={}", partition.0, generation.0);
    if let Some(child) = engine.sealed_by(partition, generation) {
        return format!(
            "{at} refused: generation {} is sealed by {}",
            generation.0, child.0
        );
    }
    if let Some((parent, base)) = engine.staging(partition, generation) {
        return format!(
            "{at} refused: generation {} is staging a copy from generation {} at base {}",
            generation.0, parent.0, base.0
        );
    }
    String::new()
}

/// `parent=.. base=.. mode=root|linked|copied sealed_by=..` for one lineage.
fn describe(engine: &RocksEngine, partition: PartitionId, generation: Generation) -> String {
    let staging = engine.staging(partition, generation).map_or_else(
        || "-".to_string(),
        |(parent, base)| format!("{}@{}", parent.0, base.0),
    );
    let sealed_by = engine
        .sealed_by(partition, generation)
        .map_or_else(|| "-".to_string(), |child| child.0.to_string());
    match engine.link(partition, generation) {
        None => format!("parent=- base=0 mode=root sealed_by={sealed_by} staging={staging}"),
        Some(link) => format!(
            "parent={} base={} mode={} sealed_by={sealed_by} staging={staging}",
            link.parent.0,
            link.base.0,
            if link.copied { "copied" } else { "linked" }
        ),
    }
}

/// Lowercase hex, capped at 32 bytes with a `..` marker.
fn hex(bytes: &[u8]) -> String {
    let mut out: String = bytes.iter().take(32).map(|b| format!("{b:02x}")).collect();
    if bytes.len() > 32 {
        out.push_str("..");
    }
    out
}

fn flush(dir: &Path, only: Option<(PartitionId, Generation)>, inject: Option<InjectedFault>) -> u8 {
    let mut engine = match open(dir, "flush") {
        Ok(engine) => engine,
        Err(code) => return code,
    };
    if let Some((partition, generation)) = only.filter(|one| !engine.lineages().contains(one)) {
        // S1-P7: flushing a lineage the database does not hold would report `durable=0` OK.
        tracing::warn!(
            target: LOG_TARGET,
            partition = partition.0,
            generation = generation.0,
            "flush_no_such_lineage"
        );
        println!(
            "flush REFUSED partition={} generation={}: no such lineage",
            partition.0, generation.0
        );
        return exit::LINEAGE_FAULT;
    }
    arm(&mut engine, inject);
    let lineages = only.map_or_else(|| engine.lineages(), |one| vec![one]);
    let captured: Vec<CapturedPrefix> = lineages
        .into_iter()
        .map(|(partition, generation)| CapturedPrefix {
            partition,
            generation,
            through: engine.buffered_applied(partition, generation),
        })
        .collect();
    match engine.sync_wal_through(captured.clone()) {
        Ok(durable) => {
            for (capture, prefix) in captured.iter().zip(&durable) {
                println!(
                    "flush partition={} generation={} captured={} durable={}",
                    capture.partition.0, capture.generation.0, capture.through.0, prefix.through.0
                );
            }
            if durable.is_empty() {
                println!("flush lineages=0");
            }
            report_unreached(&engine, "flush");
            exit::OK
        }
        Err(fault) => {
            println!("flush FAILED {fault:?}");
            exit::STORAGE_FAULT
        }
    }
}

fn verify(dir: &Path) -> u8 {
    let engine = match open(dir, "verify") {
        Ok(engine) => engine,
        Err(code) => return code,
    };
    let lineages = engine.lineages();
    if lineages.is_empty() {
        println!("verify lineages=0 OK");
    }
    let mut code = exit::OK;
    for (partition, generation) in lineages {
        let lineage = describe(&engine, partition, generation);
        match verify_lineage(&engine, partition, generation) {
            Ok(v) => println!(
                "verify partition={} generation={} {lineage} applied={} durable={} head_digest={} OK",
                partition.0,
                generation.0,
                v.applied.0,
                v.durable.0,
                short_hex(&v.head_digest.0)
            ),
            Err(fault) => {
                println!(
                    "verify partition={} generation={} {lineage} applied={} durable={} FAULT {fault}",
                    partition.0,
                    generation.0,
                    engine.buffered_applied(partition, generation).0,
                    engine.durable(partition, generation).0
                );
                code = exit::LINEAGE_FAULT;
            }
        }
    }
    code
}

fn dump_all(dir: &Path) -> u8 {
    match dump(dir) {
        Ok(records) => {
            for record in &records {
                println!("{record}");
            }
            println!("dump records={}", records.len());
            exit::OK
        }
        Err(e) => {
            println!("dump REFUSED {e}");
            open_code(&e)
        }
    }
}

fn short_hex(bytes: &[u8]) -> String {
    bytes.iter().take(8).map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(line: &str) -> Result<Args, String> {
        parse(line.split_whitespace().map(str::to_string))
    }

    /// Defect T3: `--records u64::MAX` panicked on `start + records` (exit 101). It must be a
    /// usage error (exit 2) before anything is opened.
    #[test]
    fn t3_records_overflow_is_a_usage_error() {
        let err = args("--dir d write --records 18446744073709551615").unwrap_err();
        assert!(err.contains("--records"), "{err}");
    }

    /// Defect T3: a resumed lineage near the top of the seq space must not overflow either.
    #[test]
    fn t3_last_seq_refuses_overflow() {
        assert_eq!(last_seq(3, 2), Ok(5));
        assert!(last_seq(u64::MAX - 1, 2).is_err());
    }

    /// Paper cut P2: `--records 0` printed `committed=1..=0`.
    #[test]
    fn p2_zero_records_is_a_usage_error() {
        assert!(args("--dir d write --records 0").is_err());
    }

    /// Paper cut P3: `--partition` / `--generation` were silently ignored outside `write`.
    #[test]
    fn p3_lineage_flags_apply_only_to_write() {
        assert!(args("--dir d verify --partition 2").is_err());
        assert!(args("--dir d dump --generation 2").is_err());
        assert!(args("--dir d write --records 1 --partition 2 --generation 3").is_ok());
    }

    /// S1-P4: `--base 10 --base 11` silently used 11. A repeated flag is a usage error (exit 2)
    /// that names the flag.
    #[test]
    fn s1_p4_a_repeated_flag_is_a_usage_error() {
        let err = args("--dir d inherit --from 1 --to 2 --base 10 --base 11").unwrap_err();
        assert!(err.contains("--base"), "{err}");
        let err = args("--dir d --dir e dump").unwrap_err();
        assert!(err.contains("--dir"), "{err}");
        let err = args("--dir d write --records 1 --abort --abort").unwrap_err();
        assert!(err.contains("--abort"), "{err}");
    }

    /// P8 (tester-m8 on v5): a wrong inject point was named by its Rust Debug name
    /// (`WalFlush`), not the spelling the operator typed.
    #[test]
    fn p8_inject_errors_use_the_cli_spelling() {
        let err = args("--dir d write --records 1 --inject wal-flush").unwrap_err();
        assert!(err.contains("not wal-flush"), "{err}");
        let err = args("--dir d flush --inject commit").unwrap_err();
        assert!(err.contains("not commit"), "{err}");
    }

    /// A fresh directory under `RETCD_TEST_DATA_DIR` (set by `scripts/gate.sh`), else beside
    /// this test binary in the target directory; never `%TEMP%`.
    fn data_dir(name: &str) -> PathBuf {
        let root = std::env::var_os("RETCD_TEST_DATA_DIR").map_or_else(
            || {
                let exe = std::env::current_exe().expect("test binary path");
                exe.parent().expect("binary dir").join("test-data")
            },
            PathBuf::from,
        );
        let dir = root.join(format!("rocks-scenario-{name}-{}", std::process::id()));
        if dir.exists() {
            std::fs::remove_dir_all(&dir).expect("remove a stale test directory");
        }
        std::fs::create_dir_all(&dir).expect("create the test directory");
        dir
    }

    fn lineage(generation: u64) -> Lineage {
        Lineage {
            partition: PartitionId(1),
            generation: Generation(generation),
            owner_epoch: OwnerEpoch(1),
        }
    }

    /// g1 holds the canonical records 1..=10; returns their digest at 10.
    fn write_g1(engine: &mut RocksEngine) -> Digest {
        let history =
            canonical_history_from(lineage(1), ConfigVersion(1), (Seq::ZERO, Digest::ROOT), 10)
                .expect("canonical history");
        for batch in &history.batches {
            engine.commit(batch.clone()).expect("commit");
        }
        history.digest(10)
    }

    /// `k` as `(1, generation)` sees it: holder and version.
    fn read_k(engine: &RocksEngine, generation: u64) -> Option<(Generation, u64)> {
        let key = scoped_key(TenantId(1), AffinityId(1), b"k");
        engine
            .get(
                PartitionId(1),
                Generation(generation),
                Namespace::User,
                &key,
            )
            .expect("get k")
            .map(|(from, version, _)| (from, version))
    }

    /// The dumped `data` records of `(1, generation)`, as `dump` prints them.
    fn dumped_data(db: &Path, generation: u64) -> Vec<String> {
        let prefix = format!("data     p=1 g={generation} ");
        dump(db)
            .expect("dump")
            .iter()
            .map(ToString::to_string)
            .filter(|line| line.starts_with(&prefix))
            .collect()
    }

    /// #13: a delete in a linked lineage writes a tombstone that hides the parent's value, and
    /// still hides it after a reopen; the parent keeps its value.
    #[test]
    fn m8s_13_a_delete_in_a_linked_child_is_a_tombstone() {
        let db = data_dir("13-tombstone").join("db");
        {
            let mut engine = RocksEngine::open(&db).expect("open");
            let head = write_g1(&mut engine);
            assert_eq!(
                engine.inherit(PartitionId(1), Generation(1), Generation(2), Seq(10)),
                Ok(Inherited::Linked)
            );
            let delete = delete_batch(lineage(2), Seq(11), head).expect("delete batch");
            engine.commit(delete).expect("commit the delete");
            assert_eq!(read_k(&engine, 2), None);
            assert_eq!(read_k(&engine, 1), Some((Generation(1), 10)));
        }
        let lines = dumped_data(&db, 2);
        assert!(
            lines.len() == 1 && lines[0].ends_with("tombstone v=11"),
            "{lines:?}"
        );
        let engine = RocksEngine::open(&db).expect("reopen");
        assert_eq!(
            read_k(&engine, 2),
            None,
            "the tombstone did not survive a reopen"
        );
        assert_eq!(read_k(&engine, 1), Some((Generation(1), 10)));
        verify_lineage(&engine, PartitionId(1), Generation(2)).expect("g2 verifies");
    }

    /// #14: a delete in a root lineage is a plain delete: no tombstone is stored.
    #[test]
    fn m8s_14_a_delete_in_a_root_lineage_stores_no_tombstone() {
        let db = data_dir("14-root-delete").join("db");
        {
            let mut engine = RocksEngine::open(&db).expect("open");
            let head = write_g1(&mut engine);
            let delete = delete_batch(lineage(1), Seq(11), head).expect("delete batch");
            engine.commit(delete).expect("commit the delete");
            assert_eq!(read_k(&engine, 1), None);
            verify_lineage(&engine, PartitionId(1), Generation(1)).expect("g1 verifies");
        }
        assert_eq!(dumped_data(&db, 1), Vec::<String>::new());
    }

    /// #11: a late delete before the switch is undone in the copy: k is restored at its value as
    /// of base, while the parent keeps the delete.
    #[test]
    fn m8s_11_a_late_delete_is_restored_as_of_base() {
        let db = data_dir("11-late-delete").join("db");
        let mut engine = RocksEngine::open(&db).expect("open");
        let head = write_g1(&mut engine);
        let delete = delete_batch(lineage(1), Seq(11), head).expect("delete batch");
        engine.commit(delete).expect("commit the delete");

        let inherited = engine.inherit(PartitionId(1), Generation(1), Generation(2), Seq(10));

        assert!(
            matches!(inherited, Ok(Inherited::Copied { .. })),
            "{inherited:?}"
        );
        assert_eq!(read_k(&engine, 2), Some((Generation(2), 10)));
        assert_eq!(read_k(&engine, 1), None);
        verify_lineage(&engine, PartitionId(1), Generation(2)).expect("g2 verifies");
    }

    /// #29-#31: each `--inject` point belongs to the command that reaches it.
    #[test]
    fn inject_points_belong_to_their_command() {
        assert!(args("--dir d write --records 1 --inject commit").is_ok());
        assert!(args("--dir d flush --inject wal-flush").is_ok());
        assert!(args("--dir d flush --inject mark-write").is_ok());
        assert!(args("--dir d flush --inject commit").is_err());
        assert!(args("--dir d write --records 1 --inject mark-write").is_err());
        assert!(args("--dir d verify --inject commit").is_err());
        assert!(args("--dir d write --records 1 --inject bogus").is_err());
    }

    /// S1-P8 (#3, #18): a write or delete into a sealed or staging generation printed only
    /// `FAILED at seq=N: WriteFailed`; the reason was only in the log. The FAILED line now ends
    /// with it, and with nothing for a lineage that refuses nothing.
    #[test]
    fn s1_p8_a_refused_write_says_why() {
        let dir = data_dir("p8");
        let mut engine = RocksEngine::open(dir.join("db")).expect("open");
        write_g1(&mut engine);
        assert_eq!(
            engine.inherit(PartitionId(1), Generation(1), Generation(2), Seq(10)),
            Ok(Inherited::Linked)
        );
        assert_eq!(
            refusal(&engine, PartitionId(1), Generation(1)),
            " partition=1 generation=1 refused: generation 1 is sealed by 2"
        );
        assert_eq!(refusal(&engine, PartitionId(1), Generation(2)), "");
        // A staged copy: g3 from g2 at base 9 with the copy's last batch failed.
        engine.inject_fault(InjectedFault::CopyBatch);
        assert!(engine
            .inherit(PartitionId(1), Generation(2), Generation(3), Seq(9))
            .is_err());
        assert_eq!(
            refusal(&engine, PartitionId(1), Generation(3)),
            " partition=1 generation=3 refused: generation 3 is staging a copy from generation 2 \
             at base 9"
        );
    }

    /// S1-P6: `read_found` / `read_absent` name the partition, generation and key, so a log
    /// reader can tell which read a line belongs to.
    #[config_log::retcd_test]
    fn s1_p6_read_log_lines_name_the_read() {
        let dir = data_dir("p6");
        let db = dir.join("db");
        {
            let mut engine = RocksEngine::open(&db).expect("open");
            write_g1(&mut engine);
        }
        let key = scoped_key(TenantId(1), AffinityId(1), b"k");
        assert_eq!(
            read(&db, PartitionId(1), Generation(1), Namespace::User, &key),
            exit::OK
        );
        assert_eq!(
            read(&db, PartitionId(1), Generation(1), Namespace::User, b"nope"),
            exit::OK
        );
        let lines = config_testkit::logs::lines_for_current_test(
            module_path!(),
            "s1_p6_read_log_lines_name_the_read",
        );
        for (event, key_hex) in [("read_found", hex(&key)), ("read_absent", hex(b"nope"))] {
            let row = lines
                .iter()
                .find(|row| row["@m"] == event)
                .unwrap_or_else(|| panic!("no {event} line: {lines:?}"));
            assert_eq!(row["partition"], 1, "{row:?}");
            assert_eq!(row["generation"], 1, "{row:?}");
            assert_eq!(row["key"], key_hex.as_str(), "{row:?}");
        }
    }

    /// S1-P2: `read` only reads, so it works while a writer holds the database, as `dump` does.
    #[test]
    fn s1_p2_read_works_while_a_writer_holds_the_database() {
        let dir = data_dir("p2");
        let db = dir.join("db");
        let mut writer = RocksEngine::open(&db).expect("open");
        write_g1(&mut writer);
        let key = scoped_key(TenantId(1), AffinityId(1), b"k");
        assert_eq!(
            read(&db, PartitionId(1), Generation(1), Namespace::User, &key),
            exit::OK
        );
        drop(writer);
    }

    /// S1-P2, ruling L-R183u: `snapshot` only reads too, so it also works while a writer holds
    /// the database.
    #[test]
    fn s1_p2_snapshot_works_while_a_writer_holds_the_database() {
        let dir = data_dir("p2-snapshot");
        let db = dir.join("db");
        let mut writer = RocksEngine::open(&db).expect("open");
        write_g1(&mut writer);
        assert_eq!(snapshot(&db, PartitionId(1), Generation(1)), exit::OK);
        drop(writer);
    }

    /// S1-P7: `flush` of a lineage the database does not hold is refused, exit 3, and writes
    /// nothing; the same flush of one it holds still succeeds.
    #[test]
    fn s1_p7_flush_of_a_missing_lineage_is_refused() {
        let dir = data_dir("p7");
        let db = dir.join("db");
        {
            let mut engine = RocksEngine::open(&db).expect("open");
            write_g1(&mut engine);
        }
        let before = dump(&db).expect("dump").len();
        assert_eq!(
            flush(&db, Some((PartitionId(1), Generation(7))), None),
            exit::LINEAGE_FAULT
        );
        assert_eq!(
            dump(&db).expect("dump").len(),
            before,
            "a refused flush wrote"
        );
        assert_eq!(
            flush(&db, Some((PartitionId(1), Generation(1))), None),
            exit::OK
        );
    }
}
