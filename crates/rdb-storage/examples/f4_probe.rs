//! F4 probe (rulings A-R317 (3), A-R351; critic-f4 M3): does a real WAL fsync over a large WAL
//! hold up commits made while it runs?
//!
//! ```text
//! cargo run --release -p rdb-storage --example f4_probe -- <dir> [wal_mb]
//! ```
//!
//! It writes `wal_mb` (default 64) of unsynced commits, begins one sync, and runs its
//! `flush_wal(true)` on a second thread, as a node's syncer does. Meanwhile this thread, the
//! node thread's stand-in, commits one small batch every 20 ms (50/s) until the sync returns,
//! then for one more second. A commit's latency counts from when it was due, not when it
//! started, so a commit thread held by the fsync shows as late commits.
//!
//! Verdict, last line: `PASS` when the sync took over 1 s and no commit was over 500 ms late;
//! `FAIL` when a commit was over 500 ms late; `NOT_EXERCISED` when the sync took 1 s or less,
//! so the run proves nothing either way. Exit code 0, 1 and 2 respectively. `<dir>` must not
//! exist, or be empty; the probe never deletes it.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use bytes::Bytes;
use rdb_core::contracts::ids::{BatchId, Generation, PartitionId, Seq};
use rdb_core::contracts::storage::{Batch, Namespace, Write};
use rdb_storage::{RocksEngine, StorageOptions, WalSync};

const PACE: Duration = Duration::from_millis(20);
const LATE_LIMIT_MILLIS: u128 = 500;
const SYNC_FLOOR_MILLIS: u128 = 1_000;
const BIG_VALUE: usize = 64 << 10;

fn batch(seq: u64, value: &Bytes) -> Batch {
    Batch {
        id: BatchId(seq),
        partition: PartitionId(1),
        generation: Generation(1),
        seq: Seq(seq),
        writes: vec![Write {
            ns: Namespace::User,
            key: Bytes::from(seq.to_be_bytes().to_vec()),
            value: Some(value.clone()),
        }],
    }
}

/// The fsync on this thread, timed: what a node's syncer runs.
fn flush_timed(sync: WalSync) -> (WalSync, Result<(), String>, Duration) {
    let started = Instant::now();
    let flushed = sync.flush();
    (sync, flushed, started.elapsed())
}

fn wal_bytes(dir: &std::path::Path) -> u64 {
    std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "log"))
                .filter_map(|entry| entry.metadata().ok())
                .map(|meta| meta.len())
                .sum()
        })
        .unwrap_or(0)
}

fn main() {
    let mut args = std::env::args().skip(1);
    let Some(dir) = args.next().map(PathBuf::from) else {
        eprintln!("usage: f4_probe <dir> [wal_mb]");
        std::process::exit(64);
    };
    let wal_mb: u64 = args
        .next()
        .map_or(64, |text| text.parse().expect("wal_mb: a whole number"));

    // Budgets far above the probe's writes, so no memtable flush or WAL switch syncs anything
    // before the probe's own sync does.
    let options = StorageOptions {
        write_buffer_bytes: Some(1 << 30),
        memtable_budget_bytes: Some(4 << 30),
        ..StorageOptions::default()
    };
    let mut engine = RocksEngine::open_with(&dir, options).expect("open");
    let big = Bytes::from(vec![0x5a; BIG_VALUE]);
    let small = Bytes::from(vec![0x33; 100]);

    let mut seq = 0;
    let filled = Instant::now();
    while seq * (BIG_VALUE as u64) < wal_mb << 20 || wal_bytes(&dir) < wal_mb << 20 {
        seq += 1;
        engine.commit(batch(seq, &big)).expect("fill commit");
    }
    // The `.log` length is what the fill loop reads, and on Windows RocksDB preallocates it, so
    // it can run far past what was written. The written bytes are the count that matters.
    let unsynced = seq * BIG_VALUE as u64;
    println!(
        "fill: {seq} commits, {} MB written unsynced, wal file {} MB, {} ms",
        unsynced >> 20,
        wal_bytes(&dir) >> 20,
        filled.elapsed().as_millis()
    );

    let sync = engine.begin_sync(Vec::new());
    let start = Instant::now();
    let handle = std::thread::spawn(move || flush_timed(sync));
    let mut late = Vec::new();
    let mut returned_at = None;
    for tick in 1u32.. {
        let due = start + PACE * tick;
        if let Some(wait) = due.checked_duration_since(Instant::now()) {
            std::thread::sleep(wait);
        }
        seq += 1;
        engine.commit(batch(seq, &small)).expect("probe commit");
        let during = returned_at.is_none();
        late.push((Instant::now().duration_since(due).as_millis(), during));
        if during && handle.is_finished() {
            returned_at = Some(start.elapsed());
        }
        if returned_at.is_some_and(|at| start.elapsed() > at + Duration::from_secs(1)) {
            break;
        }
    }
    let (sync, flushed, took) = handle.join().expect("the sync thread");
    let ok = flushed.is_ok();
    engine.finish_sync(sync, flushed).expect("finish the sync");

    let during: Vec<u128> = late.iter().filter(|(_, d)| *d).map(|(l, _)| *l).collect();
    let mut sorted = during.clone();
    sorted.sort_unstable();
    let p99 = sorted
        .get((sorted.len() * 99 / 100).min(sorted.len().saturating_sub(1)))
        .copied()
        .unwrap_or(0);
    let worst = late.iter().map(|(l, _)| *l).max().unwrap_or(0);
    let over: Vec<u128> = late
        .iter()
        .map(|(l, _)| *l)
        .filter(|l| *l > LATE_LIMIT_MILLIS)
        .collect();
    println!(
        "sync: {} ms over {} MB, ok={ok}",
        took.as_millis(),
        unsynced >> 20
    );
    println!(
        "commits: {} total, {} while the sync ran; late p99 {p99} ms, worst {worst} ms, \
         over {LATE_LIMIT_MILLIS} ms: {over:?}",
        late.len(),
        during.len()
    );
    let (verdict, code) = if !over.is_empty() {
        ("FAIL", 1)
    } else if took.as_millis() <= SYNC_FLOOR_MILLIS {
        ("NOT_EXERCISED", 2)
    } else {
        ("PASS", 0)
    };
    println!("verdict: {verdict}");
    std::process::exit(code);
}
