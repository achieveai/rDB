//! `rdb_bench`: sustained writes on one partition, one in flight, at a fixed rate (M10 S2,
//! ruling A-R234).
//!
//! ```text
//! cargo run --release -p rdb-api --example rdb_bench -- --dir D --log-dir L
//!     [--dataset K] [--rate R] [--writers N] [--minutes M] [--keys K] [--value-bytes B] [--keep]
//!     [--no-history]
//! cargo run --release -p rdb-api --example rdb_bench -- --build-dataset K --log-dir L
//!     [--dataset-root P]
//! ```
//!
//! Opens a `Db` the way `rdb_dev` does (three nodes and three rEtcd voters in this process, the
//! spec's default dedup cap and retention), waits until it is ready, then puts at `R` writes a
//! second for `M` minutes. One write is in flight at a time (spec §5.2): the next is sent at
//! its slot or, when the last answer came after it, as soon as that answer is in. `--rate 0`
//! sends back to back, the most one-in-flight can do.
//!
//! Every write is a plain put of `B` bytes (default 100) to one of `K` objects (default 1,000,
//! or the dataset's count with `--dataset`), `bench/<i mod K>`, at the version the owner sees.
//!
//! Every minute it prints, and logs as `bench_minute`, the writes answered in that minute, the
//! rate, p50, p99 and max of the put call in milliseconds, refusals by §5.4 name, and the
//! process's resident memory. Every refusal is logged as `bench_refusal` (its name, writer,
//! request id and the count of writes that published before it); the first of each name is
//! also printed. The end prints the whole run the same way, as `bench_total`.
//!
//! `--writers N` (lead, M10 S2): N threads share the one `Db`, each with one put in flight and
//! a slot every `N / R` seconds, so together they offer `R`. Writer `w` puts
//! `bench/<(i * N + w) mod K>`. The partition still runs one transaction at a time (spec §5.2);
//! the others wait in its queue.
//!
//! A slot more than one second late is not caught up: the schedule restarts from now, and the
//! slip is counted (`slips`), so a stall shows as a lower rate, never as a burst after it.
//!
//! **Datasets** (Gautam, k430). `--build-dataset K` writes `bench/0` to `bench/<K-1>` (100 bytes
//! each, 255 to a transaction, so the dataset spends only `K/255` dedup entries) into a new
//! database, closes it, and keeps it under `<root>/f<storage format>-<K>/` (root default
//! `C:/rdb_test_data/bench`): the copies in `db/`, the control records in `control.jsonl`, and
//! `STAMP` (format, keys, the git commit it was built at). The in-process voters cannot reopen
//! (their peer ports are new each start), so a run starts fresh voters and puts the saved
//! control records back, then pads the store revision past the saved one.
//! `--dataset K` copies that dataset's `db/` to `<D>/db` and runs on the copy, a reopen: the
//! copy heals into a new generation before it admits. A dataset of another storage format is
//! refused: rebuild it with `--build-dataset K`, which replaces one of another format and
//! refuses to touch a folder without a `STAMP`.
//!
//! **Memory split** (lead, M10 S2). Each minute also shows `heap_mb`, the live bytes Rust code
//! holds (a counting global allocator; RocksDB's C++ heap is not in it), and `wal_mb`, the
//! three copies' write-ahead logs on disk, which is what their memtables hold until a flush.
//! At start it measures what one entry costs in each per-write index (`bench_struct_costs`):
//! the digest ladder, the dedup index and the status index. `--no-history` skips the History
//! sampler, whose read-only open replays the WAL in this process and so adds to `rss_mb`.
//!
//! Exit 0 when every write published (or the dataset was built), 1 when any was refused or the
//! open failed, 2 on bad arguments. `--dir` is removed at the end unless `--keep`; a dataset is
//! kept on purpose.

#[path = "support/hostile_store.rs"]
#[allow(dead_code)] // `rdb_bench` uses only the leader-following store, none of its faults.
mod hostile_store;

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use config_core::{ConfigStore, ListRequest, PutRequest};
use config_testkit::cluster::{Cluster, StorageKind};
use config_testkit::TestTimers;
use rdb_api::{AffinityId, Db, DbConfig, Timeouts, TxnPut};
use rdb_storage::keys::FORMAT_VERSION;

use hostile_store::HostileStore;

/// Live bytes allocated through Rust's allocator. RocksDB's C++ allocations bypass it.
static HEAP: AtomicI64 = AtomicI64::new(0);

/// The system allocator, counting live bytes into [`HEAP`].
struct Counting;

// SAFETY: every call forwards to `System` with the caller's own arguments; the counter is a
// relaxed atomic and touches no allocation.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc(layout);
        if !p.is_null() {
            HEAP.fetch_add(layout.size() as i64, Ordering::Relaxed);
        }
        p
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc_zeroed(layout);
        if !p.is_null() {
            HEAP.fetch_add(layout.size() as i64, Ordering::Relaxed);
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        HEAP.fetch_sub(layout.size() as i64, Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = System.realloc(ptr, layout, new_size);
        if !p.is_null() {
            HEAP.fetch_add(new_size as i64 - layout.size() as i64, Ordering::Relaxed);
        }
        p
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Live Rust heap in MB.
fn heap_mb() -> f64 {
    #[allow(clippy::cast_precision_loss)]
    let mb = HEAP.load(Ordering::Relaxed) as f64 / 1_048_576.0;
    mb
}

/// The control voters' Raft timers, as `rdb_dev` runs them.
const CONTROL_TIMERS: TestTimers = TestTimers {
    heartbeat: Duration::from_millis(100),
    election_timeout_min: Duration::from_millis(300),
    election_timeout_max: Duration::from_millis(500),
};

/// How late a slot may be before the schedule restarts from now.
const SLIP: Duration = Duration::from_secs(1);

/// Puts per dataset transaction: the most one transaction takes.
const FILL_BATCH: usize = 255;

/// The key the revision pad writes; outside every rDB control family.
const PAD_KEY: &str = "zz-rdb-bench/pad";

#[derive(Debug)]
struct Args {
    dir: PathBuf,
    log_dir: PathBuf,
    rate: u32,
    writers: u32,
    minutes: f64,
    keys: Option<u64>,
    value_bytes: usize,
    keep: bool,
    no_history: bool,
    dataset: Option<u64>,
    build: Option<u64>,
    root: PathBuf,
}

fn usage() -> &'static str {
    "usage: rdb_bench --dir D --log-dir L [--dataset K] [--rate R] [--writers N] [--minutes M] \
     [--keys K] [--value-bytes B] [--keep] [--no-history]\n       \
     rdb_bench --build-dataset K --log-dir L [--dataset-root P]\n  \
     --rate 0 sends back to back; default 100. --minutes default 1 (fractions allowed).\n  \
     --writers N: N threads share the rate, each with one write in flight; default 1.\n"
}

fn parse(mut words: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut dir = None;
    let mut log_dir = None;
    let mut args = Args {
        dir: PathBuf::new(),
        log_dir: PathBuf::new(),
        rate: 100,
        writers: 1,
        minutes: 1.0,
        keys: None,
        value_bytes: 100,
        keep: false,
        no_history: false,
        dataset: None,
        build: None,
        root: PathBuf::from("C:/rdb_test_data/bench"),
    };
    while let Some(word) = words.next() {
        let mut value = |name: &str| words.next().ok_or_else(|| format!("{name} needs a value"));
        match word.as_str() {
            "--dir" => dir = Some(PathBuf::from(value("--dir")?)),
            "--log-dir" => log_dir = Some(PathBuf::from(value("--log-dir")?)),
            "--rate" => args.rate = number(&value("--rate")?, "--rate")?,
            "--writers" => {
                let n = positive(&value("--writers")?, "--writers")?;
                args.writers = u32::try_from(n).map_err(|_| format!("--writers {n}: too many"))?;
            }
            "--minutes" => {
                let text = value("--minutes")?;
                args.minutes = text
                    .parse::<f64>()
                    .ok()
                    .filter(|m| m.is_finite() && *m > 0.0)
                    .ok_or_else(|| format!("--minutes {text:?}: a positive number"))?;
            }
            "--keys" => args.keys = Some(positive(&value("--keys")?, "--keys")?),
            "--value-bytes" => {
                args.value_bytes = number(&value("--value-bytes")?, "--value-bytes")?
            }
            "--keep" => args.keep = true,
            "--no-history" => args.no_history = true,
            "--dataset" => args.dataset = Some(positive(&value("--dataset")?, "--dataset")?),
            "--build-dataset" => {
                args.build = Some(positive(&value("--build-dataset")?, "--build-dataset")?);
            }
            "--dataset-root" => args.root = PathBuf::from(value("--dataset-root")?),
            other => return Err(format!("unknown argument {other:?}\n{}", usage())),
        }
    }
    args.log_dir = log_dir.ok_or_else(|| format!("--log-dir is required\n{}", usage()))?;
    match (args.build, dir) {
        (Some(_), Some(_)) => return Err("--build-dataset takes no --dir".to_owned()),
        (Some(keys), None) => {
            args.dir = args.root.join(format!("building-f{FORMAT_VERSION}-{keys}"))
        }
        (None, Some(dir)) => args.dir = dir,
        (None, None) => return Err(format!("--dir is required\n{}", usage())),
    }
    Ok(args)
}

fn number<T: std::str::FromStr>(text: &str, name: &str) -> Result<T, String> {
    text.parse()
        .map_err(|_| format!("{name} {text:?}: a whole number"))
}

fn positive(text: &str, name: &str) -> Result<u64, String> {
    let n: u64 = number(text, name)?;
    if n == 0 {
        return Err(format!("{name} 0: at least one"));
    }
    Ok(n)
}

fn main() -> ExitCode {
    let args = match parse(std::env::args().skip(1)) {
        Ok(args) => args,
        Err(message) => {
            eprintln!("rdb_bench: {message}");
            return ExitCode::from(2);
        }
    };
    // Before any other thread starts, so the heap counter sees only these structures.
    let costs = struct_costs();
    let mut config = config_log::LogConfig::for_application("rdb_bench");
    config.dir.clone_from(&args.log_dir);
    config.file_name = "rdb_bench.jsonl".to_string();
    let guard = match config_log::init(config) {
        Ok(guard) => guard,
        Err(e) => {
            eprintln!("rdb_bench: log init: {e}");
            return ExitCode::from(2);
        }
    };
    eprintln!("log={}", args.log_dir.join("rdb_bench.jsonl").display());
    tracing::info!(
        ladder_bytes = costs.ladder,
        dedup_bytes = costs.dedup,
        status_bytes = costs.status,
        entries = COST_ENTRIES,
        "bench_struct_costs"
    );
    println!(
        "heap bytes per entry: digest ladder {:.1}, dedup index {:.1}, status index {:.1}",
        costs.ladder, costs.dedup, costs.status
    );
    if std::env::var_os("RETCD_TEST_PORT_RANGE").is_none() {
        std::env::set_var("RETCD_TEST_PORT_RANGE", "19500-19999");
    }
    if std::env::var_os("RETCD_TEST_DATA_DIR").is_none() {
        std::env::set_var("RETCD_TEST_DATA_DIR", args.dir.join("control"));
    }
    let code = match run(&args) {
        Ok(code) => code,
        Err(message) => {
            eprintln!("rdb_bench: {message}");
            tracing::error!(%message, "bench_failed");
            ExitCode::from(1)
        }
    };
    drop(guard);
    code
}

/// Entries [`struct_costs`] inserts into each index: sequential keys, as a run writes them.
const COST_ENTRIES: u32 = 100_000;

/// Heap bytes one entry costs in each per-write index.
struct Costs {
    ladder: f64,
    dedup: f64,
    status: f64,
}

/// What one entry costs, measured with the counting allocator: a `DigestLadder` rung (every
/// copy adds one per write), a `DedupIndex` entry (T1, one per write) and a `StatusIndex`
/// entry (P1, one per write). Runs single-threaded, before the logger or the cluster starts.
fn struct_costs() -> Costs {
    use rdb_core::contracts::authority::Lineage;
    use rdb_core::contracts::digest::Digest;
    use rdb_core::contracts::ids::{
        ClientId, Generation, OwnerEpoch, PartitionId, RequestId, RequestIdentity, Seq, TenantId,
    };
    use rdb_core::contracts::publication::{StatusEntry, StatusOutcome};
    use rdb_core::contracts::time::Tick;
    use rdb_core::contracts::txn::{Durability, Outcome, TxnResult};
    use rdb_core::publication::status::StatusIndex;
    use rdb_core::replication::progress::DigestLadder;
    use rdb_core::transaction::dedup::{DedupIndex, Retained, RetainedAnswer};

    let n = u64::from(COST_ENTRIES);
    let per = |before: i64| {
        #[allow(clippy::cast_precision_loss)]
        let bytes = (HEAP.load(Ordering::Relaxed) - before) as f64 / f64::from(COST_ENTRIES);
        bytes
    };
    let identity = |i: u64| RequestIdentity {
        tenant: TenantId(1),
        client: ClientId(1),
        request: RequestId(i),
    };
    let (g, partition, epoch) = (Generation(2), PartitionId(1), OwnerEpoch(1));

    let before = HEAP.load(Ordering::Relaxed);
    let mut ladder = DigestLadder::new();
    for i in 1..=n {
        ladder.insert(Seq(i), Digest([7; 32]));
    }
    let ladder_cost = per(before);
    drop(ladder);

    let before = HEAP.load(Ordering::Relaxed);
    let mut dedup = DedupIndex::default();
    for i in 1..=n {
        let result = TxnResult {
            partition,
            owner_epoch: epoch,
            generation: g,
            seq: Seq(i),
            outcome: Outcome::Published,
            durability: Durability::DurableOnRequiredCopies,
        };
        let retained = Retained {
            request_digest: Digest([7; 32]),
            answer: RetainedAnswer::Applied(result),
            applied_at_seq: Seq(i),
        };
        dedup.insert(g, AffinityId(1), identity(i), retained);
    }
    let dedup_cost = per(before);
    drop(dedup);

    let before = HEAP.load(Ordering::Relaxed);
    let mut status = StatusIndex::with_cap(g, usize::MAX);
    let lineage = Lineage {
        partition,
        generation: g,
        owner_epoch: epoch,
    };
    for i in 1..=n {
        let entry = StatusEntry {
            request: identity(i),
            lineage,
            seq: Some(Seq(i)),
            record_digest: Some(Digest([7; 32])),
            outcome: StatusOutcome::Unknown,
            snapshot: None,
            at: Tick(i),
        };
        status
            .record(entry)
            .expect("an uncapped index takes every entry");
    }
    let status_cost = per(before);
    drop(status);

    Costs {
        ladder: ladder_cost,
        dedup: dedup_cost,
        status: status_cost,
    }
}

/// What a dataset folder's `STAMP` says.
struct Stamp {
    format: u32,
    keys: u64,
}

fn read_stamp(dataset: &Path) -> Result<Option<Stamp>, String> {
    let path = dataset.join("STAMP");
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("read {}: {e}", path.display())),
    };
    let field = |name: &str| {
        text.lines()
            .find_map(|l| l.strip_prefix(name)?.strip_prefix('='))
            .ok_or_else(|| format!("{}: no {name}= line", path.display()))
    };
    let format = field("format")?
        .parse()
        .map_err(|_| format!("{}: format is not a number", path.display()))?;
    let keys = field("keys")?
        .parse()
        .map_err(|_| format!("{}: keys is not a number", path.display()))?;
    Ok(Some(Stamp { format, keys }))
}

fn git_head() -> String {
    let out = |args: &[&str]| {
        std::process::Command::new("git")
            .arg("-C")
            .arg(env!("CARGO_MANIFEST_DIR"))
            .args(args)
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
    };
    let sha = out(&["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".to_owned());
    let dirty = out(&["status", "--porcelain", "--untracked-files=no"]).map_or("unknown", |s| {
        if s.is_empty() {
            "false"
        } else {
            "true"
        }
    });
    format!("sha={sha}\ndirty={dirty}")
}

/// The dataset folder for `keys` at this build's storage format.
fn dataset_dir(root: &Path, keys: u64) -> PathBuf {
    root.join(format!("f{FORMAT_VERSION}-{keys}"))
}

/// Before anything starts: check the dataset a run names and copy its copies to `<dir>/db`.
/// Returns the saved control records.
fn prepare_run(args: &Args, keys: u64) -> Result<Vec<(Bytes, Bytes)>, String> {
    let dataset = dataset_dir(&args.root, keys);
    let stamp = read_stamp(&dataset)?;
    let Some(stamp) = stamp else {
        // Another format's dataset of the same size names the remedy too.
        return Err(format!(
            "no dataset at {} (storage format {FORMAT_VERSION}); build it with \
             `rdb_bench --build-dataset {keys} --log-dir <L>`",
            dataset.display()
        ));
    };
    if stamp.format != FORMAT_VERSION || stamp.keys != keys {
        return Err(format!(
            "{} is storage format {} with {} keys, this build writes format {FORMAT_VERSION}: \
             rebuild the dataset with `rdb_bench --build-dataset {keys} --log-dir <L>`",
            dataset.display(),
            stamp.format,
            stamp.keys
        ));
    }
    if args.dir.exists() {
        return Err(format!(
            "{} exists; a dataset run copies into a new folder",
            args.dir.display()
        ));
    }
    let started = Instant::now();
    copy_tree(&dataset.join("db"), &args.dir.join("db"))?;
    let control = read_control(&dataset.join("control.jsonl"))?;
    tracing::info!(
        dataset = %dataset.display(),
        copy = %args.dir.display(),
        records = control.len(),
        millis = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        "bench_dataset_copied"
    );
    println!(
        "dataset {} copied to {} in {:.1} s",
        dataset.display(),
        args.dir.display(),
        started.elapsed().as_secs_f64()
    );
    Ok(control)
}

fn copy_tree(from: &Path, to: &Path) -> Result<(), String> {
    std::fs::create_dir_all(to).map_err(|e| format!("create {}: {e}", to.display()))?;
    let entries = std::fs::read_dir(from).map_err(|e| format!("read {}: {e}", from.display()))?;
    for entry in entries {
        let entry = entry.map_err(|e| format!("read {}: {e}", from.display()))?;
        let target = to.join(entry.file_name());
        let kind = entry
            .file_type()
            .map_err(|e| format!("stat {}: {e}", entry.path().display()))?;
        if kind.is_dir() {
            copy_tree(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), &target).map_err(|e| {
                format!(
                    "copy {} to {}: {e}",
                    entry.path().display(),
                    target.display()
                )
            })?;
        }
    }
    Ok(())
}

/// `control.jsonl`: one `{"key":"<hex>","value":"<hex>"}` per record, then one
/// `{"revision":N}` line, the store revision the records were read at.
fn read_control(path: &Path) -> Result<Vec<(Bytes, Bytes)>, String> {
    let text =
        std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    let mut records = Vec::new();
    for (n, line) in text.lines().enumerate() {
        let row: serde_json::Value = serde_json::from_str(line)
            .map_err(|e| format!("{} line {}: {e}", path.display(), n + 1))?;
        if row.get("revision").is_some() {
            continue;
        }
        let field = |name: &str| -> Result<Bytes, String> {
            let text = row[name]
                .as_str()
                .ok_or_else(|| format!("{} line {}: no {name}", path.display(), n + 1))?;
            hex::decode(text)
                .map(Bytes::from)
                .map_err(|e| format!("{} line {}: {name}: {e}", path.display(), n + 1))
        };
        records.push((field("key")?, field("value")?));
    }
    Ok(records)
}

fn saved_revision(path: &Path) -> Result<u64, String> {
    let text =
        std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    text.lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .find_map(|row| row.get("revision")?.as_u64())
        .ok_or_else(|| format!("{}: no revision line", path.display()))
}

fn run(args: &Args) -> Result<ExitCode, String> {
    let control = match (args.build, args.dataset) {
        (Some(keys), _) => {
            let dataset = dataset_dir(&args.root, keys);
            match read_stamp(&dataset)? {
                Some(stamp) if stamp.format == FORMAT_VERSION => {
                    println!("dataset {} is already built", dataset.display());
                    return Ok(ExitCode::SUCCESS);
                }
                Some(stamp) => {
                    println!(
                        "dataset {} is storage format {}; rebuilding at {FORMAT_VERSION}",
                        dataset.display(),
                        stamp.format
                    );
                    remove(&dataset);
                }
                None if dataset.exists() => {
                    return Err(format!(
                        "{} exists without a STAMP; it is not a dataset this bench built. \
                         Move it away, then build again",
                        dataset.display()
                    ));
                }
                None => {}
            }
            if args.dir.exists() {
                remove(&args.dir);
            }
            None
        }
        (None, Some(keys)) => Some(prepare_run(args, keys)?),
        (None, None) => None,
    };
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("runtime: {e}"))?;
    let voters = args.dir.join("control").join("voters");
    let mut builder = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::ROCKS)
        .timers(CONTROL_TIMERS);
    for id in 1..=3 {
        let node = voters.join(format!("node-{id}"));
        std::fs::create_dir_all(&node).map_err(|e| format!("create {}: {e}", node.display()))?;
        builder = builder.data_dir(config_core::NodeId(id), node);
    }
    let cluster = Arc::new(rt.block_on(builder.start()));
    let leader = rt.block_on(cluster.leader());
    let store: Arc<dyn ConfigStore> = Arc::new(HostileStore::new(&cluster, leader));
    let result = (|| -> Result<ExitCode, String> {
        if let (Some(records), Some(keys)) = (&control, args.dataset) {
            let target = saved_revision(&dataset_dir(&args.root, keys).join("control.jsonl"))?;
            rt.block_on(restore_control(&*store, records, target))?;
        }
        let config = DbConfig {
            dir: args.dir.join("db"),
            hold: Vec::new(),
            timeouts: Timeouts::default(),
            dedup_cap: None,
            dedup_retention: None,
            local: rdb_api::db::ALL_NODES.to_vec(),
            links: None,
            client: rdb_api::db::DEFAULT_CLIENT,
            boot: None,
            storage: rdb_api::db::StorageOptions::default(),
        };
        let opened = Instant::now();
        let mut db = rt
            .block_on(Db::open(config, Arc::clone(&store), rt.handle().clone()))
            .map_err(|e| format!("open: {e}"))?;
        let ready = db.wait_ready(Duration::from_secs(60));
        println!("ready in {:.1} s", opened.elapsed().as_secs_f64());
        let code = match (ready, args.build) {
            (Err(e), _) => Err(format!("not ready: {e}")),
            (Ok(()), Some(keys)) => fill(&db, keys).map(|()| ExitCode::SUCCESS),
            (Ok(()), None) => {
                let keys = args.keys.or(args.dataset).unwrap_or(1_000);
                Ok(drive(&db, args, keys))
            }
        };
        db.shutdown();
        drop(db);
        if let (Ok(_), Some(_)) = (&code, args.build) {
            rt.block_on(save_control(&*store, &args.dir.join("control.jsonl")))?;
        }
        code
    })();
    drop(store);
    // The store holds the cluster weakly; wait out a call resolving the leader at this instant.
    let started = Instant::now();
    let mut shared = cluster;
    loop {
        match Arc::try_unwrap(shared) {
            Ok(cluster) => {
                rt.block_on(cluster.shutdown());
                break;
            }
            Err(still) if started.elapsed() >= Duration::from_secs(2) => {
                eprintln!("rdb_bench: the cluster is still shared; voters not shut down");
                drop(still);
                break;
            }
            Err(still) => {
                shared = still;
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
    drop(rt);
    match (args.build, &result) {
        (Some(keys), Ok(_)) => {
            remove(&args.dir.join("control"));
            let stamp = format!(
                "format={FORMAT_VERSION}\nkeys={keys}\nvalue_bytes=100\n{}\n",
                git_head()
            );
            std::fs::write(args.dir.join("STAMP"), stamp)
                .map_err(|e| format!("write STAMP: {e}"))?;
            let dataset = dataset_dir(&args.root, keys);
            std::fs::rename(&args.dir, &dataset).map_err(|e| {
                format!(
                    "rename {} to {}: {e}",
                    args.dir.display(),
                    dataset.display()
                )
            })?;
            println!("dataset built: {}", dataset.display());
        }
        (Some(_), Err(_)) => remove(&args.dir),
        (None, _) if !args.keep => remove(&args.dir),
        (None, _) => {}
    }
    result
}

/// Put the saved control records back into fresh voters, then write [`PAD_KEY`] until the
/// store revision passes the one they were read at, so no revision a record names is ahead of
/// the store.
async fn restore_control(
    store: &dyn ConfigStore,
    records: &[(Bytes, Bytes)],
    target: u64,
) -> Result<(), String> {
    let mut revision = 0;
    for (key, value) in records {
        let put = PutRequest {
            key: key.clone(),
            value: value.clone(),
            ..PutRequest::default()
        };
        revision = put_retrying(store, put).await?;
    }
    while revision <= target {
        let put = PutRequest {
            key: Bytes::from_static(PAD_KEY.as_bytes()),
            value: Bytes::from(revision.to_string()),
            ..PutRequest::default()
        };
        revision = put_retrying(store, put).await?;
    }
    tracing::info!(
        records = records.len(),
        target,
        revision,
        "bench_control_restored"
    );
    Ok(())
}

/// A control put, retried while the voters elect a leader (up to 10 s).
async fn put_retrying(store: &dyn ConfigStore, put: PutRequest) -> Result<u64, String> {
    let started = Instant::now();
    loop {
        match store.put(put.clone()).await {
            Ok(done) => return Ok(done.revision),
            Err(e) if started.elapsed() >= Duration::from_secs(10) => {
                return Err(format!("restore control record: {e}"));
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
}

/// Write every control record to `path` as [`read_control`] reads it.
async fn save_control(store: &dyn ConfigStore, path: &Path) -> Result<(), String> {
    let listed = store
        .list(ListRequest {
            prefix: Bytes::new(),
            max_items: 100_000,
            max_bytes: 1 << 30,
        })
        .await
        .map_err(|e| format!("list control records: {e}"))?;
    if listed.truncated {
        return Err("list control records: truncated".to_owned());
    }
    let mut out =
        std::fs::File::create(path).map_err(|e| format!("create {}: {e}", path.display()))?;
    for record in &listed.records {
        let line = serde_json::json!({
            "key": hex::encode(&record.key),
            "value": hex::encode(&record.value),
        });
        writeln!(out, "{line}").map_err(|e| format!("write {}: {e}", path.display()))?;
    }
    writeln!(
        out,
        "{}",
        serde_json::json!({ "revision": listed.read_revision })
    )
    .map_err(|e| format!("write {}: {e}", path.display()))?;
    out.sync_all()
        .map_err(|e| format!("sync {}: {e}", path.display()))?;
    tracing::info!(
        records = listed.records.len(),
        revision = listed.read_revision,
        "bench_control_saved"
    );
    Ok(())
}

/// Write `bench/0` to `bench/<keys-1>`, [`FILL_BATCH`] to a transaction.
fn fill(db: &Db, keys: u64) -> Result<(), String> {
    let value = vec![b'v'; 100];
    let names: Vec<String> = (0..keys).map(|i| format!("bench/{i}")).collect();
    let started = Instant::now();
    for chunk in names.chunks(FILL_BATCH) {
        let puts: Vec<TxnPut<'_>> = chunk
            .iter()
            .map(|name| TxnPut {
                group: AffinityId(1),
                object: name.as_bytes(),
                value: &value,
                if_version: None,
            })
            .collect();
        db.txn(&puts, Duration::from_secs(10))
            .map_err(|e| format!("fill txn at {}: {e}", chunk[0]))?;
    }
    tracing::info!(
        keys,
        millis = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        "bench_dataset_filled"
    );
    println!(
        "filled {keys} keys in {:.1} s",
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

/// One window's answers: put-call latencies in microseconds and refusals by §5.4 name.
#[derive(Default)]
struct Window {
    micros: Vec<u64>,
    refused: BTreeMap<String, u64>,
}

impl Window {
    fn add(&mut self, other: &Self) {
        self.micros.extend_from_slice(&other.micros);
        for (kind, n) in &other.refused {
            *self.refused.entry(kind.clone()).or_default() += n;
        }
    }

    /// `(p50, p99, max)` in milliseconds, nearest rank; zeros when empty.
    fn quantiles(&self) -> (f64, f64, f64) {
        let mut sorted = self.micros.clone();
        sorted.sort_unstable();
        let at = |q: f64| -> f64 {
            if sorted.is_empty() {
                return 0.0;
            }
            #[allow(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                clippy::cast_precision_loss
            )]
            let rank = ((q * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
            micros_to_ms(sorted[rank - 1])
        };
        (
            at(0.50),
            at(0.99),
            sorted.last().map_or(0.0, |&m| micros_to_ms(m)),
        )
    }

    fn refusals(&self) -> u64 {
        self.refused.values().sum()
    }

    fn refused_text(&self) -> String {
        if self.refused.is_empty() {
            return "-".to_owned();
        }
        self.refused
            .iter()
            .map(|(kind, n)| format!("{kind}:{n}"))
            .collect::<Vec<_>>()
            .join(",")
    }
}

#[allow(clippy::cast_precision_loss)]
fn micros_to_ms(micros: u64) -> f64 {
    micros as f64 / 1_000.0
}

/// What the writers record and the reporter takes once a minute.
#[derive(Default)]
struct Shared {
    minute: Window,
    minute_slips: u64,
    slips: u64,
    published: u64,
    first_refusal: BTreeMap<String, u64>,
}

impl Shared {
    /// This minute's window and slips, and the run's published count; starts a new minute.
    fn take(&mut self) -> (Window, u64, u64) {
        let minute = std::mem::take(&mut self.minute);
        let slips = std::mem::take(&mut self.minute_slips);
        (minute, slips, self.published)
    }
}

/// One writer: client thread `w` of `writers`, one put in flight, a slot every `period` (or
/// back to back), until `end`. Writer `w` puts `bench/<(i * writers + w) mod keys>`, so writers
/// touch different objects while `keys` is a multiple of `writers`.
#[allow(clippy::too_many_arguments)]
fn writer(
    db: &Db,
    w: u32,
    writers: u64,
    period: Option<Duration>,
    keys: u64,
    value: &[u8],
    start: Instant,
    end: Instant,
    shared: &Mutex<Shared>,
) {
    let mut next_slot = start;
    let mut i: u64 = 0;
    loop {
        let now = Instant::now();
        if now >= end {
            return;
        }
        if let Some(period) = period {
            if now < next_slot {
                std::thread::sleep(next_slot - now);
            } else if now - next_slot > SLIP {
                next_slot = now;
                let mut s = shared.lock().expect("no writer panics");
                s.slips += 1;
                s.minute_slips += 1;
            }
            next_slot += period;
        }
        let object = format!("bench/{}", (i * writers + u64::from(w)) % keys);
        i += 1;
        let sent = Instant::now();
        let answer = db.put(object.as_bytes(), value, None);
        let took = u64::try_from(sent.elapsed().as_micros()).unwrap_or(u64::MAX);
        let mut s = shared.lock().expect("no writer panics");
        match answer {
            Ok(_) => {
                s.published += 1;
                s.minute.micros.push(took);
            }
            Err(e) => {
                let kind = e.error.name();
                let secs = start.elapsed().as_secs_f64();
                let published = s.published;
                let request = e.request.as_ref().map(|r| r.identity.request.0);
                tracing::warn!(
                    kind = %kind,
                    writer = w,
                    request,
                    published,
                    secs,
                    took_ms = micros_to_ms(took),
                    detail = %e.error.detail,
                    "bench_refusal"
                );
                if !s.first_refusal.contains_key(&kind) {
                    println!(
                        "first {kind} (writer {w}) after {published} published writes, \
                         {secs:.1} s in: {}",
                        e.error.detail
                    );
                    s.first_refusal.insert(kind.clone(), published);
                }
                *s.minute.refused.entry(kind).or_default() += 1;
            }
        }
    }
}

fn drive(db: &Db, args: &Args, keys: u64) -> ExitCode {
    let value = vec![b'v'; args.value_bytes];
    let period = (args.rate > 0).then(|| Duration::from_secs(1) / args.rate);
    let length = Duration::from_secs_f64(args.minutes * 60.0);
    tracing::info!(
        rate = args.rate,
        writers = args.writers,
        minutes = args.minutes,
        keys,
        value_bytes = args.value_bytes,
        "bench_start"
    );
    println!(
        "bench rate={} writers={} minutes={} keys={} value_bytes={} pid={}",
        args.rate,
        args.writers,
        args.minutes,
        keys,
        args.value_bytes,
        std::process::id()
    );
    println!(
        "minute  ok  rate/s  p50ms  p99ms  maxms  refused  slips  rss_mb  published_total  \
         history_rows  history_mb  heap_mb  wal_mb"
    );
    sample_rss();
    if !args.no_history {
        sample_history(args.dir.join("db").join("nodes").join("1"));
    }
    let nodes = args.dir.join("db").join("nodes");
    let start = Instant::now();
    let shared = Mutex::new(Shared::default());
    let mut total = Window::default();
    let mut minute_no = 1_u32;
    let mut minute_start = start;
    let end = start + length;
    // Each writer sends at `rate / writers`, so the writers together offer `rate`.
    let period = period.map(|p| p * args.writers);
    std::thread::scope(|scope| {
        for w in 0..args.writers {
            let (shared, value) = (&shared, &value);
            let writers = u64::from(args.writers);
            scope.spawn(move || writer(db, w, writers, period, keys, value, start, end, shared));
        }
        loop {
            let minute_end = minute_start + Duration::from_secs(60);
            if minute_end >= end {
                break;
            }
            std::thread::sleep(minute_end.saturating_duration_since(Instant::now()));
            let (minute, slips, published) = shared.lock().expect("no writer panics").take();
            report_minute(
                minute_no,
                &minute,
                Duration::from_secs(60),
                slips,
                published,
                &nodes,
            );
            total.add(&minute);
            minute_no += 1;
            minute_start = minute_end;
        }
    });
    // Every writer has stopped: report the last, partial minute.
    let ended_at = Instant::now();
    let (minute, minute_slips, published) = shared.lock().expect("no writer panics").take();
    let span = ended_at.duration_since(minute_start);
    report_minute(minute_no, &minute, span, minute_slips, published, &nodes);
    total.add(&minute);
    let slips = shared.lock().expect("no writer panics").slips;
    let (p50, p99, max) = total.quantiles();
    let secs = ended_at.duration_since(start).as_secs_f64();
    #[allow(clippy::cast_precision_loss)]
    let rate = published as f64 / secs;
    let rss = rss_mb();
    tracing::info!(
        published,
        secs,
        rate,
        p50,
        p99,
        max,
        refused = %total.refused_text(),
        slips,
        rss_mb = rss,
        "bench_total"
    );
    println!(
        "total published={published} secs={secs:.1} rate/s={rate:.1} p50ms={p50:.2} \
         p99ms={p99:.2} maxms={max:.2} refused={} slips={slips} rss_mb={rss:.0}",
        total.refused_text()
    );
    if total.refusals() == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}

fn report_minute(n: u32, w: &Window, span: Duration, slips: u64, published: u64, nodes: &Path) {
    let (p50, p99, max) = w.quantiles();
    #[allow(clippy::cast_precision_loss)]
    let rate = w.micros.len() as f64 / span.as_secs_f64().max(f64::EPSILON);
    let rss = rss_mb();
    let (history_rows, history_mb) = history();
    let heap = heap_mb();
    let wal = wal_mb(nodes);
    tracing::info!(
        minute = n,
        history_rows,
        history_mb,
        heap_mb = heap,
        wal_mb = wal,
        ok = w.micros.len(),
        rate,
        p50,
        p99,
        max,
        refused = %w.refused_text(),
        slips,
        rss_mb = rss,
        published,
        "bench_minute"
    );
    println!(
        "{n:>6} {:>4} {rate:>7.1} {p50:>6.2} {p99:>6.2} {max:>6.2} {:>8} {slips:>6} {rss:>7.0} \
         {published:>16} {history_rows:>13.0} {history_mb:>11.1} {heap:>8.1} {wal:>7.1}",
        w.micros.len(),
        w.refused_text()
    );
}

/// The write-ahead logs (`*.log`) of every copy under `nodes`, in MB: what the copies'
/// memtables hold until RocksDB flushes them. -1 when the folder cannot be read.
fn wal_mb(nodes: &Path) -> f64 {
    let Ok(copies) = std::fs::read_dir(nodes) else {
        return -1.0;
    };
    let mut bytes = 0_u64;
    for copy in copies.flatten() {
        let Ok(files) = std::fs::read_dir(copy.path()) else {
            return -1.0;
        };
        for file in files.flatten() {
            if file.path().extension().is_some_and(|e| e == "log") {
                bytes += file.metadata().map_or(0, |m| m.len());
            }
        }
    }
    #[allow(clippy::cast_precision_loss)]
    let mb = bytes as f64 / 1_048_576.0;
    mb
}

/// This process's resident memory in MB, or -1 when it cannot be read. Slow on Windows (it runs
/// `tasklist`), so only [`sample_rss`]'s thread calls it.
fn read_rss_mb() -> f64 {
    #[cfg(windows)]
    {
        // `tasklist` prints `"name","pid","session","#","12,345 K"`: the working set.
        let pid = std::process::id().to_string();
        let out = std::process::Command::new("tasklist")
            .args(["/FI", &format!("PID eq {pid}"), "/FO", "CSV", "/NH"])
            .output();
        let Ok(out) = out else { return -1.0 };
        let text = String::from_utf8_lossy(&out.stdout);
        let Some(field) = text.trim().rsplit("\",\"").next() else {
            return -1.0;
        };
        let digits: String = field.chars().filter(char::is_ascii_digit).collect();
        #[allow(clippy::cast_precision_loss)]
        digits.parse::<u64>().map_or(-1.0, |kb| kb as f64 / 1_024.0)
    }
    #[cfg(not(windows))]
    {
        let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
            return -1.0;
        };
        status
            .lines()
            .find_map(|l| l.strip_prefix("VmRSS:"))
            .and_then(|v| v.split_whitespace().next()?.parse::<f64>().ok())
            .map_or(-1.0, |kb| kb / 1_024.0)
    }
}

/// Remove `dir`, retrying for up to 2 s while RocksDB lets go of its files.
fn remove(dir: &std::path::Path) {
    let started = Instant::now();
    loop {
        match std::fs::remove_dir_all(dir) {
            Ok(()) => return,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
            Err(e) if started.elapsed() >= Duration::from_secs(2) => {
                eprintln!("rdb_bench: remove {}: {e}", dir.display());
                return;
            }
            Err(_) => std::thread::sleep(Duration::from_millis(100)),
        }
    }
}

/// The latest [`read_rss_mb`], as `f64` bits; `-1` until the first sample.
static RSS_MB: AtomicU64 = AtomicU64::new(0xBFF0_0000_0000_0000);

/// Sample [`read_rss_mb`] every 10 s on its own thread, so the write loop never waits on it.
fn sample_rss() {
    RSS_MB.store(read_rss_mb().to_bits(), Ordering::Relaxed);
    std::thread::spawn(|| loop {
        std::thread::sleep(Duration::from_secs(10));
        RSS_MB.store(read_rss_mb().to_bits(), Ordering::Relaxed);
    });
}

/// The latest resident memory sample in MB.
fn rss_mb() -> f64 {
    f64::from_bits(RSS_MB.load(Ordering::Relaxed))
}

/// The latest [`read_history`]: History rows (RocksDB's estimate) and MB, as `f64` bits.
static HISTORY_ROWS: AtomicU64 = AtomicU64::new(0xBFF0_0000_0000_0000);
static HISTORY_MB: AtomicU64 = AtomicU64::new(0xBFF0_0000_0000_0000);

/// Node 1's `history` column family: `(estimated rows, MB in SST files plus memtables)`, read
/// through a read-only open that takes no lock (as `rdb_storage::dump` does). `None` when it
/// cannot be opened. Replays the WAL, so only [`sample_history`]'s thread calls it.
fn read_history(node: &Path) -> Option<(f64, f64)> {
    let names = rocksdb::DB::list_cf(&rocksdb::Options::default(), node).ok()?;
    let db = rocksdb::DB::open_cf_for_read_only(&rocksdb::Options::default(), node, &names, false)
        .ok()?;
    let cf = db.cf_handle(rdb_storage::keys::CF_HISTORY)?;
    let int = |name: &str| db.property_int_value_cf(&cf, name).ok().flatten();
    let rows = int("rocksdb.estimate-num-keys")?;
    let bytes = int("rocksdb.total-sst-files-size")? + int("rocksdb.cur-size-all-mem-tables")?;
    #[allow(clippy::cast_precision_loss)]
    Some((rows as f64, bytes as f64 / 1_048_576.0))
}

/// Sample [`read_history`] of `node` every 60 s on its own thread, 2 s before each minute's
/// report, so the report shows that minute's size.
fn sample_history(node: PathBuf) {
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(58));
        loop {
            if let Some((rows, mb)) = read_history(&node) {
                HISTORY_ROWS.store(rows.to_bits(), Ordering::Relaxed);
                HISTORY_MB.store(mb.to_bits(), Ordering::Relaxed);
            }
            std::thread::sleep(Duration::from_secs(60));
        }
    });
}

/// The latest History sample: `(rows, MB)`.
fn history() -> (f64, f64) {
    (
        f64::from_bits(HISTORY_ROWS.load(Ordering::Relaxed)),
        f64::from_bits(HISTORY_MB.load(Ordering::Relaxed)),
    )
}
