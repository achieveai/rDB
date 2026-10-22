//! M10 S1-7 storage measurement: what a read view costs as keys and chain depth grow.
//!
//! ```text
//! cargo run -p rdb-storage --example snapshot_bench -- --dir <D> \
//!     [--keys 1000,10000,100000] [--depths 1,10,100] [--depth-keys 1000] [--samples 2000]
//! ```
//!
//! Each case builds a fresh engine under `<D>` and loads it the way the host does: one batch per
//! put, each writing one `User` key (100 B), its `History` record (200 B), a `Dedup` row and
//! `Progress`. Then it measures:
//!
//! - `view_ms`: [`RocksEngine::snapshot`], the step view the host rebuilds after **every**
//!   commit (`Host::refresh_view`), so a write pays it once per node;
//! - `get_us`: one [`SnapshotRead::get`] of a random present key on a view already built;
//! - `miss_us`: one get of an absent key (a lazy view walks every level for it);
//! - `step_us`: one commit, then a fresh view, then one get: what a read right after a write
//!   costs a node.
//!
//! The keys axis uses chain depth 1. The depth axis loads `--depth-keys` keys at generation 1,
//! then links `depth - 1` more generations (`inherit` at the applied seq, as a reopen does),
//! each overwriting 10 keys. The directory is removed after each case. No log subscriber is
//! installed, so logging costs nothing here.

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Instant;

use bytes::Bytes;
use rdb_core::contracts::ids::{BatchId, Generation, PartitionId, Seq, SnapshotHandle};
use rdb_core::contracts::storage::{Batch, Namespace, SnapshotRead, Write};
use rdb_core::replication::append::PROGRESS_KEY;
use rdb_storage::{Inherited, RocksEngine};

const P: PartitionId = PartitionId(1);
const USAGE: &str = "usage: snapshot_bench --dir <D> [--keys LIST] [--depths LIST] \
                     [--depth-keys N] [--samples N]";

struct Args {
    dir: PathBuf,
    keys: Vec<u64>,
    depths: Vec<u64>,
    depth_keys: u64,
    samples: usize,
}

fn parse() -> Result<Args, String> {
    let mut args = Args {
        dir: PathBuf::new(),
        keys: vec![1_000, 10_000, 100_000],
        depths: vec![1, 10, 100],
        depth_keys: 1_000,
        samples: 2_000,
    };
    let list = |v: &str| -> Result<Vec<u64>, String> {
        v.split(',')
            .map(|n| n.parse::<u64>().map_err(|e| format!("{n}: {e}")))
            .collect()
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let value = it.next().ok_or_else(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--dir" => args.dir = PathBuf::from(value),
            "--keys" => args.keys = list(&value)?,
            "--depths" => args.depths = list(&value)?,
            "--depth-keys" => args.depth_keys = value.parse().map_err(|e| format!("{e}"))?,
            "--samples" => args.samples = value.parse().map_err(|e| format!("{e}"))?,
            _ => return Err(format!("unknown flag {flag}")),
        }
    }
    if args.dir.as_os_str().is_empty() {
        return Err("--dir is required".into());
    }
    if args.depths.contains(&0) {
        return Err("a depth is at least 1".into());
    }
    Ok(args)
}

/// A tiny deterministic generator, so two runs measure the same keys.
struct Lcg(u64);

impl Lcg {
    fn below(&mut self, n: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % n
    }
}

fn user_key(i: u64) -> Bytes {
    Bytes::from(format!("k{i:010}"))
}

/// One put as the host commits it: the user key, its History record, a Dedup row, Progress.
fn put(engine: &mut RocksEngine, generation: Generation, seq: u64, key: u64) {
    let fill = |n: usize, byte: u64| Bytes::from(vec![(byte % 251) as u8; n]);
    let batch = Batch {
        id: BatchId(seq),
        partition: P,
        generation,
        seq: Seq(seq),
        writes: vec![
            Write {
                ns: Namespace::User,
                key: user_key(key),
                value: Some(fill(100, seq)),
            },
            Write {
                ns: Namespace::History,
                key: Bytes::copy_from_slice(&seq.to_be_bytes()),
                value: Some(fill(200, seq)),
            },
            Write {
                ns: Namespace::Dedup,
                key: Bytes::from(format!("d{seq:015}")),
                value: Some(fill(40, seq)),
            },
            Write {
                ns: Namespace::Progress,
                key: Bytes::from_static(PROGRESS_KEY),
                value: Some(fill(40, seq)),
            },
        ],
    };
    engine.commit(batch).expect("commit");
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let at = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[at]
}

fn sorted(mut v: Vec<f64>) -> Vec<f64> {
    v.sort_by(f64::total_cmp);
    v
}

/// Measure one loaded engine whose top lineage is `top`, then add `step` puts.
fn measure(
    engine: &mut RocksEngine,
    top: Generation,
    keys: u64,
    samples: usize,
    rng: &mut Lcg,
) -> String {
    let builds = 5;
    let mut view_ms = Vec::with_capacity(builds);
    for _ in 0..builds {
        let started = Instant::now();
        let view = engine.snapshot(P, top, SnapshotHandle(1)).expect("view");
        view_ms.push(started.elapsed().as_secs_f64() * 1e3);
        drop(view);
    }
    let view = engine.snapshot(P, top, SnapshotHandle(1)).expect("view");
    let mut get_us = Vec::with_capacity(samples);
    for _ in 0..samples {
        let key = user_key(rng.below(keys));
        let started = Instant::now();
        let found = view.get(Namespace::User, &key);
        get_us.push(started.elapsed().as_secs_f64() * 1e6);
        assert!(found.is_some(), "a loaded key reads back");
    }
    let mut miss_us = Vec::with_capacity(samples);
    for i in 0..samples as u64 {
        let key = user_key(keys + i);
        let started = Instant::now();
        let found = view.get(Namespace::User, &key);
        miss_us.push(started.elapsed().as_secs_f64() * 1e6);
        assert!(found.is_none(), "an absent key reads absent");
    }
    drop(view);
    let steps = 5;
    let mut step_us = Vec::with_capacity(steps);
    let mut seq = engine.buffered_applied(P, top).0;
    for _ in 0..steps {
        seq += 1;
        let key = rng.below(keys);
        let started = Instant::now();
        put(engine, top, seq, key);
        let view = engine.snapshot(P, top, SnapshotHandle(1)).expect("view");
        let found = view.get(Namespace::User, &user_key(key));
        step_us.push(started.elapsed().as_secs_f64() * 1e6);
        assert!(found.is_some(), "the write just made reads back");
    }
    let (view_ms, get_us, miss_us, step_us) =
        (sorted(view_ms), sorted(get_us), sorted(miss_us), sorted(step_us));
    format!(
        "view_ms_p50={:.3} view_ms_max={:.3} get_us_p50={:.2} get_us_p99={:.2} \
         miss_us_p50={:.2} miss_us_p99={:.2} step_us_p50={:.1} step_us_max={:.1}",
        percentile(&view_ms, 0.5),
        percentile(&view_ms, 1.0),
        percentile(&get_us, 0.5),
        percentile(&get_us, 0.99),
        percentile(&miss_us, 0.5),
        percentile(&miss_us, 0.99),
        percentile(&step_us, 0.5),
        percentile(&step_us, 1.0),
    )
}

/// A fresh engine with `keys` keys at generation 1, then `depth - 1` linked generations, each
/// overwriting 10 keys. Returns the top generation.
fn load(engine: &mut RocksEngine, keys: u64, depth: u64, rng: &mut Lcg) -> Generation {
    let mut seq = 0;
    for key in 0..keys {
        seq += 1;
        put(engine, Generation(1), seq, key);
    }
    let mut top = Generation(1);
    for level in 2..=depth {
        let child = Generation(level);
        let linked = engine.inherit(P, top, child, Seq(seq)).expect("inherit");
        assert_eq!(linked, Inherited::Linked, "a reopen links at the applied seq");
        top = child;
        for _ in 0..10 {
            seq += 1;
            put(engine, top, seq, rng.below(keys));
        }
    }
    top
}

fn case(root: &Path, axis: &str, keys: u64, depth: u64, samples: usize) {
    let dir = root.join(format!("{axis}-{keys}-{depth}"));
    if dir.exists() {
        std::fs::remove_dir_all(&dir).expect("remove a stale case directory");
    }
    let mut rng = Lcg(keys ^ (depth << 32));
    let loaded = Instant::now();
    let mut engine = RocksEngine::open(&dir).expect("open");
    let top = load(&mut engine, keys, depth, &mut rng);
    let load_s = loaded.elapsed().as_secs_f64();
    let line = measure(&mut engine, top, keys, samples, &mut rng);
    println!("axis={axis} keys={keys} depth={depth} load_s={load_s:.1} {line}");
    drop(engine);
    std::fs::remove_dir_all(&dir).expect("remove the case directory");
}

fn main() -> ExitCode {
    let args = match parse() {
        Ok(args) => args,
        Err(e) => {
            eprintln!("{e}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    println!(
        "snapshot_bench profile={} samples={}",
        if cfg!(debug_assertions) { "dev" } else { "release" },
        args.samples
    );
    for &keys in &args.keys {
        case(&args.dir, "keys", keys, 1, args.samples);
    }
    for &depth in &args.depths {
        case(&args.dir, "depth", args.depth_keys, depth, args.samples);
    }
    ExitCode::SUCCESS
}
