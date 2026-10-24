//! M10 slice 2, F4 (ruling A-R317): the WAL sync split into [`RocksEngine::begin_sync`],
//! [`WalSync::flush`] (on any thread) and [`RocksEngine::finish_sync`].
//!
//! 1. The narrower contract (spec §6.1): a sync makes durable exactly the writes that returned
//!    before it began. A commit that returns while it runs is durable only after the next one.
//! 2. Critic-f4 M6: what an engine call sets while the fsync runs (a commit, an inherit's link
//!    and seal) survives the finish, and the A-R214 synced marker never moves backwards.
//! 3. The staged writes a node's thread makes (publish marks, an epoch revocation, a dedup trim)
//!    fsync nothing and do not count as synced until the next sync completes (A-R322).
//!
//! The debug-only `wal_syncs` count is RocksDB's own ticker, so these rows run in debug builds.
//! Data goes under `RETCD_TEST_DATA_DIR` (set by `scripts/gate.sh`), else Cargo's per-target tmp
//! dir; never `%TEMP%`.
#![cfg(debug_assertions)]

use std::path::PathBuf;

use config_log::retcd_test;
use rdb_core::contracts::authority::Lineage;
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::ids::{
    AppliedSeq, ConfigVersion, DurableSeq, Generation, OwnerEpoch, PartitionId, Seq,
};
use rdb_core::contracts::storage::{CapturedPrefix, DurablePrefix, StorageFault};
use rdb_sim::storage::history::{canonical_history_from, CanonicalHistory};
use rdb_storage::{Inherited, Link, RocksEngine};

const P: PartitionId = PartitionId(1);
const G1: Generation = Generation(1);
const G2: Generation = Generation(2);

fn data_dir(name: &str) -> PathBuf {
    let root = std::env::var_os("RETCD_TEST_DATA_DIR")
        .map_or_else(|| PathBuf::from(env!("CARGO_TARGET_TMPDIR")), PathBuf::from);
    let dir = root.join(format!("rdb-storage-f4-{name}-{}", std::process::id()));
    if dir.exists() {
        std::fs::remove_dir_all(&dir).expect("remove a stale test directory");
    }
    std::fs::create_dir_all(&dir).expect("create the test directory");
    dir
}

/// The canonical records `start.0 + 1..=n` of `(P, generation)`, chained from `start`.
fn chain(generation: Generation, start: (Seq, Digest), n: u64) -> CanonicalHistory {
    let lineage = Lineage {
        partition: P,
        generation,
        owner_epoch: OwnerEpoch(1),
    };
    canonical_history_from(lineage, ConfigVersion(1), start, n).expect("canonical history")
}

/// Commit `history`'s records `from..=to`.
fn commit(engine: &mut RocksEngine, history: &CanonicalHistory, from: u64, to: u64) {
    for seq in from..=to {
        engine
            .commit(history.batch(seq).clone())
            .expect("commit a canonical batch");
    }
}

fn capture(generation: Generation, through: u64) -> CapturedPrefix {
    CapturedPrefix {
        partition: P,
        generation,
        through: AppliedSeq(through),
    }
}

/// One sync that captures `captured`, begun and finished on this thread.
fn sync_now(engine: &mut RocksEngine, captured: Vec<CapturedPrefix>) -> Vec<DurablePrefix> {
    let sync = engine.begin_sync(captured);
    let flushed = sync.flush();
    engine.finish_sync(sync, flushed).expect("a sync")
}

/// Sync `(P, G1)` through `through`, then once more with nothing captured: a sync that raises
/// a durable mark writes it unsynced after the fsync (it may lag, never lead), so only the
/// second leaves every write synced.
fn settle(engine: &mut RocksEngine, through: u64) {
    sync_now(engine, vec![capture(G1, through)]);
    sync_now(engine, Vec::new());
    assert!(engine.wal_synced_through_latest(), "settled");
}

fn durable(generation: Generation, through: u64) -> DurablePrefix {
    DurablePrefix {
        partition: P,
        generation,
        through: DurableSeq(through),
    }
}

/// A commit that returns after a sync began is not made durable by that sync, even when the
/// capture already names it; the next sync makes it durable. The synced marker stays behind it.
/// Cut: credit `capture.through` at finish instead of `min(capture, applied at begin)`, red
/// (durable 11). Cut: read `latest` at finish, red (counted as synced through the commit).
#[retcd_test]
fn m10_f4_a_commit_made_while_a_sync_runs_is_durable_only_after_the_next_sync() {
    let dir = data_dir("mid-sync-commit");
    {
        let mut engine = RocksEngine::open(dir.join("db")).expect("open");
        let g1 = chain(G1, (Seq::ZERO, Digest::ROOT), 11);
        commit(&mut engine, &g1, 1, 10);
        settle(&mut engine, 10);

        let sync = engine.begin_sync(vec![capture(G1, 11)]);
        commit(&mut engine, &g1, 11, 11);
        let flushed = sync.flush();
        assert_eq!(flushed, Ok(()), "the fsync succeeds");
        let first = engine.finish_sync(sync, flushed).expect("finish the sync");

        assert_eq!(
            first,
            vec![durable(G1, 10)],
            "the sync credits only what had returned when it began"
        );
        assert_eq!(engine.durable(P, G1), DurableSeq(10));
        assert_eq!(engine.buffered_applied(P, G1), AppliedSeq(11));
        assert!(
            !engine.wal_synced_through_latest(),
            "the commit made during the sync is not counted as synced"
        );

        let next = engine
            .sync_wal_through(vec![capture(G1, 11)])
            .expect("the next sync");
        assert_eq!(
            next,
            vec![durable(G1, 11)],
            "the next sync makes it durable"
        );
        assert_eq!(engine.durable(P, G1), DurableSeq(11));
    }
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// Critic-f4 M6: an inherit (link, seal, child applied) and a child commit made while a sync
/// runs survive its finish, which raises the parent's durable mark; the sealed parent still
/// refuses a write. A synced put made while a later sync runs moves the synced marker past that
/// sync's start, and the finish does not move it back.
/// Cut: snapshot the lineages at begin and restore them at finish, red (link and seal lost).
/// Cut: `synced_seq = Some(sync.latest)` in `finish_sync`, red (not synced through latest).
#[retcd_test]
fn m10_f4_state_set_while_a_sync_runs_is_kept_and_the_synced_marker_never_moves_back() {
    let dir = data_dir("m6");
    {
        let mut engine = RocksEngine::open(dir.join("db")).expect("open");
        let g1 = chain(G1, (Seq::ZERO, Digest::ROOT), 11);
        commit(&mut engine, &g1, 1, 10);

        let sync = engine.begin_sync(vec![capture(G1, 10)]);
        assert_eq!(engine.inherit(P, G1, G2, Seq(10)), Ok(Inherited::Linked));
        let g2 = chain(G2, (Seq(10), g1.digest(10)), 12);
        commit(&mut engine, &g2, 11, 12);
        let flushed = sync.flush();
        let durable_list = engine.finish_sync(sync, flushed).expect("finish the sync");

        assert_eq!(durable_list, vec![durable(G1, 10)]);
        assert_eq!(engine.durable(P, G1), DurableSeq(10));
        assert_eq!(
            engine.link(P, G2),
            Some(Link {
                parent: G1,
                base: Seq(10),
                copied: false,
            }),
            "the link made during the sync is kept"
        );
        assert_eq!(
            engine.sealed_by(P, G1),
            Some(G2),
            "the seal made during the sync is kept"
        );
        assert_eq!(
            engine.buffered_applied(P, G2),
            AppliedSeq(12),
            "the child's commits made during the sync are kept"
        );
        assert_eq!(
            engine.commit(g1.batch(11).clone()),
            Err(StorageFault::WriteFailed),
            "the sealed parent still refuses a write"
        );

        sync_now(&mut engine, Vec::new());
        let sync = engine.begin_sync(Vec::new());
        engine
            .put_publish_marks(P, &[])
            .expect("a synced put during the sync");
        assert!(engine.wal_synced_through_latest(), "the synced put synced");
        let flushed = sync.flush();
        engine.finish_sync(sync, flushed).expect("finish the sync");
        assert!(
            engine.wal_synced_through_latest(),
            "the synced marker did not move back to the sync's start"
        );
    }
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// A-R317 and A-R322: the staged forms of the node thread's metadata writes and of the dedup
/// trim fsync nothing and do not count as synced; the next sync, begun after they returned,
/// makes them synced. Each synced twin still syncs at once.
/// Cut: `true` for `sync` in `stage_publish_marks`, `stage_epoch_revocation` or
/// `stage_dedup_trimmed`, each red (the WAL synced at the stage).
#[retcd_test]
fn m10_f4_a_staged_write_is_not_synced_until_the_next_sync_completes() {
    let dir = data_dir("staged");
    {
        let mut engine = RocksEngine::open(dir.join("db")).expect("open");
        let g1 = chain(G1, (Seq::ZERO, Digest::ROOT), 10);
        commit(&mut engine, &g1, 1, 10);
        settle(&mut engine, 10);

        let staged: [(&str, fn(&mut RocksEngine)); 3] = [
            ("publish marks", |engine| {
                engine.stage_publish_marks(P, &[]).expect("stage marks");
            }),
            ("epoch revocation", |engine| {
                engine
                    .stage_epoch_revocation(P, OwnerEpoch(1))
                    .expect("stage the revocation");
            }),
            ("dedup trim", |engine| {
                let deleted = engine
                    .stage_dedup_trimmed(P, G1, Seq(6))
                    .expect("stage the trim");
                assert_eq!(deleted, 5, "the trim deletes the rows below seq 6");
            }),
        ];
        for (what, stage) in staged {
            let before = engine.wal_syncs();
            stage(&mut engine);
            assert_eq!(engine.wal_syncs(), before, "{what}: staging fsyncs nothing");
            assert!(
                !engine.wal_synced_through_latest(),
                "{what}: staged is not synced"
            );

            sync_now(&mut engine, Vec::new());
            assert_eq!(
                engine.wal_syncs(),
                before + 1,
                "{what}: the next sync fsyncs"
            );
            assert!(
                engine.wal_synced_through_latest(),
                "{what}: synced once the next sync completes"
            );
        }
        assert_eq!(
            engine.epoch_revocations().expect("read revocations"),
            vec![(P, OwnerEpoch(1))]
        );

        let before = engine.wal_syncs();
        engine.put_publish_marks(P, &[]).expect("synced marks");
        assert_eq!(
            engine.wal_syncs(),
            before + 1,
            "the synced twin syncs at once"
        );
        assert!(engine.wal_synced_through_latest());
    }
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}
