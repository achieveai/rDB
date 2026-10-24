//! F4 probe (rulings A-R317 (3), A-R351; critic-f4 M3): does a real WAL fsync over a large WAL
//! hold up commits made while it runs?
//!
//! ```text
//! cargo run --release -p rdb-storage --example f4_probe -- <dir> [wal_mb] [pace_us]
//! ```
//!
//! It writes `wal_mb` (default 64) of unsynced commits, begins one sync, and runs its
//! `WalSync::flush` on a second thread, as a node's syncer does. Meanwhile this thread, the
//! node thread's stand-in, commits one small batch every `pace_us` (default 20 000, 50/s; 1 000
//! packs the fsync window, critic-f4 P1) until the sync returns, then for one more second. Each
//! commit issued inside the fsync is printed with its start and end against the sync's.
//!
//! Verdict, last line (A-R373): `FAIL` (exit 1) when a commit issued inside the fsync took over
//! 10 ms, or half the fsync or more, or any commit returned over 500 ms after it was due;
//! `NOT_EXERCISED` (exit 2) when the fsync took under 20 ms or fewer than 5 commits were issued
//! inside it; else `PASS` (exit 0). `<dir>` must not exist, or be empty; the probe never
//! deletes it.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use bytes::Bytes;
use rdb_core::contracts::ids::{BatchId, Generation, PartitionId, Seq};
use rdb_core::contracts::storage::{Batch, Namespace, Write};
use rdb_storage::{RocksEngine, StorageOptions, WalSync};

const LATE_LIMIT_MILLIS: u128 = 500;
/// A commit inside the fsync that takes longer than this waited on it (A-R373).
const INSIDE_LIMIT: Duration = Duration::from_millis(10);
/// An fsync shorter than this leaves too little window to tell a wait from noise.
const MIN_SYNC: Duration = Duration::from_millis(20);
/// Fewer commits than this issued inside the fsync and the run proves nothing.
const MIN_INSIDE: usize = 5;
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
fn flush_timed(sync: WalSync) -> (WalSync, Result<(), String>, Instant, Instant) {
    let began = Instant::now();
    let flushed = sync.flush();
    (sync, flushed, began, Instant::now())
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
    let pace = Duration::from_micros(args.next().map_or(20_000, |text| {
        text.parse().expect("pace_us: a whole number")
    }));

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
    // Per commit: when it was due, when it started, when it returned.
    let mut commits: Vec<(Instant, Instant, Instant)> = Vec::new();
    let mut returned_at = None;
    for tick in 1u32.. {
        let due = start + pace * tick;
        // `sleep` on Windows rounds up to its timer tick, so a pace under 2 ms spins instead.
        while let Some(wait) = due.checked_duration_since(Instant::now()) {
            if wait > Duration::from_millis(2) {
                std::thread::sleep(wait);
            } else {
                std::hint::spin_loop();
            }
        }
        seq += 1;
        let began = Instant::now();
        engine.commit(batch(seq, &small)).expect("probe commit");
        commits.push((due, began, Instant::now()));
        if returned_at.is_none() && handle.is_finished() {
            returned_at = Some(start.elapsed());
        }
        if returned_at.is_some_and(|at| start.elapsed() > at + Duration::from_secs(1)) {
            break;
        }
    }
    let (sync, flushed, sync_began, sync_ended) = handle.join().expect("the sync thread");
    let ok = flushed.is_ok();
    engine.finish_sync(sync, flushed).expect("finish the sync");
    let took = sync_ended.duration_since(sync_began);
    let at = |instant: Instant| instant.saturating_duration_since(sync_began).as_secs_f64() * 1e3;

    // A commit issued inside the fsync, `[sync_began, sync_ended)`, "waited" when it took over
    // 10 ms, or half the fsync or more (A-R373).
    let inside: Vec<&(Instant, Instant, Instant)> = commits
        .iter()
        .filter(|(_, began, _)| *began >= sync_began && *began < sync_ended)
        .collect();
    let waited: Vec<&&(Instant, Instant, Instant)> = inside
        .iter()
        .filter(|(_, began, ended)| {
            let took_it = ended.duration_since(*began);
            took_it > INSIDE_LIMIT || took_it * 2 >= took
        })
        .collect();
    let inside_max = inside
        .iter()
        .map(|(_, began, ended)| ended.duration_since(*began))
        .max()
        .unwrap_or_default();
    let late_max = commits
        .iter()
        .map(|(due, _, ended)| ended.saturating_duration_since(*due))
        .max()
        .unwrap_or_default();
    println!(
        "sync: {:.1} ms over {} MB, ok={ok}; pace {} us",
        took.as_secs_f64() * 1e3,
        unsynced >> 20,
        pace.as_micros()
    );
    for (_, began, ended) in &inside {
        println!(
            "inside: start {:.3} ms, end {:.3} ms, took {:.3} ms (sync ends {:.3} ms)",
            at(*began),
            at(*ended),
            ended.duration_since(*began).as_secs_f64() * 1e3,
            at(sync_ended)
        );
    }
    println!(
        "commits: {} total, {} issued inside the sync, longest inside {:.3} ms, waited on it {}, \
         latest return after due {:.3} ms",
        commits.len(),
        inside.len(),
        inside_max.as_secs_f64() * 1e3,
        waited.len(),
        late_max.as_secs_f64() * 1e3
    );
    let (verdict, code) = if !waited.is_empty() || late_max.as_millis() > LATE_LIMIT_MILLIS {
        ("FAIL", 1)
    } else if took < MIN_SYNC || inside.len() < MIN_INSIDE {
        ("NOT_EXERCISED", 2)
    } else {
        ("PASS", 0)
    };
    println!("verdict: {verdict}");
    std::process::exit(code);
}
