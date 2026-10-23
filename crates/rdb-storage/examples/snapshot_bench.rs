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
//! - `scan10_us`: one page of 10 `User` records from a random key, as `items()` and blob reads
//!   page (a lazy view opens one iterator per level for it);
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
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::envelope::{EnvelopeHeader, ReplicationEnvelope};
use rdb_core::contracts::ids::{
    BatchId, ClientId, ConfigVersion, Generation, LeaseId, OwnerEpoch, PartitionId, RequestId,
    RequestIdentity, Seq, SnapshotHandle, TenantId,
};
use rdb_core::contracts::storage::{Batch, Namespace, SnapshotRead, Write};
use rdb_core::contracts::txn::Outcome;
use rdb_core::contracts::version::ENVELOPE_VERSION;
use rdb_core::replication::append::PROGRESS_KEY;
use rdb_storage::{Inherited, RocksEngine, MAX_CHAIN_DEPTH};

const P: PartitionId = PartitionId(1);
const USAGE: &str = "usage: snapshot_bench --dir <D> [--keys LIST] [--depths LIST] \
                     [--depth-keys N] [--flatten-keys LIST] [--samples N]";

struct Args {
    dir: PathBuf,
    keys: Vec<u64>,
    depths: Vec<u64>,
    depth_keys: u64,
    flatten_keys: Vec<u64>,
    samples: usize,
}

fn parse() -> Result<Args, String> {
    let mut args = Args {
        dir: PathBuf::new(),
        keys: vec![1_000, 10_000, 100_000],
        depths: vec![1, 10, 100],
        depth_keys: 1_000,
        flatten_keys: vec![1_000, 10_000, 100_000],
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
            "--flatten-keys" => args.flatten_keys = list(&value)?,
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
///
/// The History record is a real envelope, because an inherit that copies (the flatten axis)
/// decodes the record at its base. The records do not chain (`prev_digest` is the root), so
/// `verify_lineage` would refuse this data; nothing here calls it.
fn put(engine: &mut RocksEngine, generation: Generation, seq: u64, key: u64) {
    let fill = |n: usize, byte: u64| Bytes::from(vec![(byte % 251) as u8; n]);
    let mutations = vec![
        Write {
            ns: Namespace::User,
            key: user_key(key),
            value: Some(fill(100, seq)),
        },
        Write {
            ns: Namespace::Dedup,
            key: Bytes::from(format!("d{seq:015}")),
            value: Some(fill(40, seq)),
        },
    ];
    let mut envelope = ReplicationEnvelope {
        header: EnvelopeHeader {
            protocol_version: ENVELOPE_VERSION,
            partition: P,
            generation,
            config_version: ConfigVersion(1),
            owner_epoch: OwnerEpoch(1),
            seq: Seq(seq),
            body_len: 0,
        },
        lease_id: LeaseId(1),
        prev_digest: Digest::ROOT,
        request_identity: RequestIdentity {
            tenant: TenantId(1),
            client: ClientId(1),
            request: RequestId(seq),
        },
        request_digest: Digest::ROOT,
        conditions_result: Vec::new(),
        mutations,
        result: Outcome::Published,
        record_digest: Digest::ROOT,
    };
    envelope.record_digest = envelope.compute_record_digest().expect("record digest");
    let mut progress = seq.to_le_bytes().to_vec();
    progress.extend_from_slice(&envelope.record_digest.0);
    let mut writes = envelope.mutations.clone();
    writes.push(Write {
        ns: Namespace::History,
        key: Bytes::copy_from_slice(&seq.to_be_bytes()),
        value: Some(envelope.encode().expect("encode the envelope")),
    });
    writes.push(Write {
        ns: Namespace::Progress,
        key: Bytes::from_static(PROGRESS_KEY),
        value: Some(Bytes::from(progress)),
    });
    let batch = Batch {
        id: BatchId(seq),
        partition: P,
        generation,
        seq: Seq(seq),
        writes,
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
    // One page of 10 from a random key, as `items()` and blob reads page.
    let mut scan_us = Vec::with_capacity(samples);
    for _ in 0..samples {
        let from = user_key(rng.below(keys));
        let started = Instant::now();
        let page = view.scan(Namespace::User, &from, 10);
        scan_us.push(started.elapsed().as_secs_f64() * 1e6);
        assert!(!page.is_empty(), "a page from a loaded key is not empty");
    }
    assert_eq!(view.fault(), None, "the bench read no corrupt value");
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
    let (view_ms, get_us, miss_us, scan_us, step_us) = (
        sorted(view_ms),
        sorted(get_us),
        sorted(miss_us),
        sorted(scan_us),
        sorted(step_us),
    );
    format!(
        "view_ms_p50={:.3} view_ms_max={:.3} get_us_p50={:.2} get_us_p99={:.2} \
         miss_us_p50={:.2} miss_us_p99={:.2} scan10_us_p50={:.1} scan10_us_p99={:.1} \
         step_us_p50={:.1} step_us_max={:.1}",
        percentile(&view_ms, 0.5),
        percentile(&view_ms, 1.0),
        percentile(&get_us, 0.5),
        percentile(&get_us, 0.99),
        percentile(&miss_us, 0.5),
        percentile(&miss_us, 0.99),
        percentile(&scan_us, 0.5),
        percentile(&scan_us, 0.99),
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
        let parent_depth = engine.chain_depth(P, top);
        let inherited = engine.inherit(P, top, child, Seq(seq)).expect("inherit");
        if parent_depth < MAX_CHAIN_DEPTH {
            assert_eq!(
                inherited,
                Inherited::Linked,
                "a reopen links at the applied seq"
            );
        } else {
            assert!(
                matches!(inherited, Inherited::Copied { .. }),
                "past the ceiling a reopen copies: {inherited:?}"
            );
        }
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
    let reached = engine.chain_depth(P, top);
    let line = measure(&mut engine, top, keys, samples, &mut rng);
    println!(
        "axis={axis} keys={keys} depth={depth} chain_depth={reached} load_s={load_s:.1} {line}"
    );
    drop(engine);
    std::fs::remove_dir_all(&dir).expect("remove the case directory");
}

/// The one-time cost of the flatten (M10 ruling A-R166): `keys` keys under a chain at the
/// ceiling, then the one reopen that copies instead of linking.
fn flatten_case(root: &Path, keys: u64, samples: usize) {
    let dir = root.join(format!("flatten-{keys}"));
    if dir.exists() {
        std::fs::remove_dir_all(&dir).expect("remove a stale case directory");
    }
    let mut rng = Lcg(keys ^ 0xF1A7);
    let mut engine = RocksEngine::open(&dir).expect("open");
    let depth = u64::try_from(MAX_CHAIN_DEPTH).expect("a small constant");
    let top = load(&mut engine, keys, depth, &mut rng);
    let before = engine.chain_depth(P, top);
    let seq = engine.buffered_applied(P, top).0;
    let child = Generation(top.0 + 1);
    let started = Instant::now();
    let inherited = engine.inherit(P, top, child, Seq(seq)).expect("inherit");
    let flatten_ms = started.elapsed().as_secs_f64() * 1e3;
    let Inherited::Copied { keys: copied } = inherited else {
        panic!("a reopen at the ceiling copies: {inherited:?}");
    };
    let after = engine.chain_depth(P, child);
    let line = measure(&mut engine, child, keys, samples, &mut rng);
    println!(
        "axis=flatten keys={keys} depth_before={before} depth_after={after} \
         flatten_ms={flatten_ms:.1} copied={copied} {line}"
    );
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
        if cfg!(debug_assertions) {
            "dev"
        } else {
            "release"
        },
        args.samples
    );
    for &keys in &args.keys {
        case(&args.dir, "keys", keys, 1, args.samples);
    }
    for &depth in &args.depths {
        case(&args.dir, "depth", args.depth_keys, depth, args.samples);
    }
    for &keys in &args.flatten_keys {
        flatten_case(&args.dir, keys, args.samples);
    }
    ExitCode::SUCCESS
}
