//! M10 slice 2, F4 (ruling A-R317): the WAL sync split into [`RocksEngine::begin_sync`],
//! [`WalSync::flush`] (on any thread) and [`RocksEngine::finish_sync`].
//!
//! 1. The narrower contract (spec §6.1): a sync makes durable exactly the writes that returned
//!    before it began. A commit that returns while it runs is durable only after the next one.
//! 2. Critic-f4 M6: what an engine call sets while the fsync runs (a commit, an inherit's link
//!    and seal) survives the finish, and the A-R214 synced marker never moves backwards.
//! 3. The staged writes a node's thread makes (publish marks, an epoch revocation, a dedup trim)
//!    fsync nothing and do not count as synced until the next sync completes (A-R322).
//! 4. Unix only (issue #71): the WAL directory fsync runs on every sync while RocksDB rolls to
//!    new WAL files and deletes old ones, and the credited prefixes survive a process crash.
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

/// One staged write, named, as the staged-writes row applies it.
type Stage = fn(&mut RocksEngine);

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
/// sync's start, and the finish does not move it back. That put would wait on a real fsync in
/// flight, so the engine logs it as `synced_write_during_sync` (the guard b3 asserts absent).
/// Cut: snapshot the lineages at begin and restore them at finish, red (link and seal lost).
/// Cut: `synced_seq = Some(sync.latest)` in `finish_sync`, red (not synced through latest).
/// Cut: drop the `synced_write_during_sync` log, red (no line).
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
    let lines = config_testkit::logs::lines_for_current_test(
        module_path!(),
        "m10_f4_state_set_while_a_sync_runs_is_kept_and_the_synced_marker_never_moves_back",
    );
    let during: Vec<_> = lines
        .iter()
        .filter(|row| row["@m"] == "synced_write_during_sync")
        .collect();
    assert_eq!(during.len(), 1, "one synced write ran during a sync");
    assert_eq!(during[0]["what"], "metadata");
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

        let staged: [(&str, Stage); 3] = [
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

/// A-R373: an open refused on the first listing (as for a WAL being deleted) is passed over
/// and listed again; one still refused on that second listing fails the sync, naming the file,
/// never skipped silently. A read-only `*.log` in the database directory stands in for a WAL
/// the syncer may not open.
/// Cut: skip a refused open on the second pass too, red (the sync succeeds).
#[retcd_test]
fn m10_f4_a_wal_whose_open_is_still_refused_after_the_relist_fails_the_sync() {
    let dir = data_dir("refused-wal");
    {
        let mut engine = RocksEngine::open(dir.join("db")).expect("open");
        let g1 = chain(G1, (Seq::ZERO, Digest::ROOT), 1);
        commit(&mut engine, &g1, 1, 1);
        let refused = dir.join("db").join("zz-refused.log");
        std::fs::write(&refused, b"").expect("plant a file");
        let mut perms = std::fs::metadata(&refused).expect("metadata").permissions();
        perms.set_readonly(true);
        std::fs::set_permissions(&refused, perms.clone()).expect("make it read-only");

        let sync = engine.begin_sync(vec![capture(G1, 1)]);
        let flushed = sync.flush();
        let error = flushed.expect_err("a WAL still refused after the re-list fails the sync");
        assert!(
            error.contains("zz-refused.log") && error.contains("open WAL"),
            "the error names the file: {error}"
        );
        drop(sync);

        #[allow(clippy::permissions_set_readonly_false)]
        perms.set_readonly(false);
        std::fs::set_permissions(&refused, perms).expect("make it writable");
        std::fs::remove_file(&refused).expect("remove the planted file");
        assert_eq!(
            sync_now(&mut engine, vec![capture(G1, 1)]),
            vec![durable(G1, 1)]
        );
    }
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// Env var that turns [`zz_child_rolls_wals_under_syncs_then_is_killed`] from a no-op into the
/// crashing child of the issue #71 row; it names the database directory.
#[cfg(unix)]
const ROLLOVER_CHILD_DIR: &str = "RDB_STORAGE_F4_ROLLOVER_CHILD_DIR";
/// The child's report, written only once every sync it made succeeded.
#[cfg(unix)]
const ROLLOVER_REPORT: &str = "rollover-report.txt";
/// Records the child commits and syncs under a running sync, in chunks of [`ROLLOVER_CHUNK`].
#[cfg(unix)]
const ROLLOVER_RECORDS: u64 = 200;
#[cfg(unix)]
const ROLLOVER_CHUNK: u64 = 20;
/// Records committed after the last sync, never synced, before the kill.
#[cfg(unix)]
const ROLLOVER_TAIL: u64 = 5;

/// The `*.log` file names in `db`, which sort in WAL number order.
#[cfg(unix)]
fn wal_files(db: &std::path::Path) -> std::collections::BTreeSet<String> {
    std::fs::read_dir(db)
        .expect("list the database directory")
        .map(|entry| entry.expect("dir entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "log"))
        .map(|path| {
            path.file_name()
                .expect("file name")
                .to_string_lossy()
                .into_owned()
        })
        .collect()
}

/// The crashing child of the issue #71 row. A no-op unless the parent set
/// [`ROLLOVER_CHILD_DIR`]. Then, on a store with a 64 KiB memtable budget (so a 256 KiB
/// `max_total_wal_size`), it commits [`ROLLOVER_RECORDS`] canonical records of `(P, G1)`. Each
/// chunk of [`ROLLOVER_CHUNK`] is committed on this thread while a [`WalSync::flush`] begun
/// before it runs on another, so RocksDB switches and deletes WAL files while syncs go on. One
/// more sync credits everything, [`ROLLOVER_TAIL`] records follow unsynced, the report is
/// written, and the process SIGKILLs itself with the engine open: no close, no destructor, no
/// handler. Not `abort()`: that runs the host's core-dump handler, about 1.2 s here.
#[cfg(unix)]
#[test]
fn zz_child_rolls_wals_under_syncs_then_is_killed() {
    use rdb_storage::StorageOptions;

    let Some(db) = std::env::var_os(ROLLOVER_CHILD_DIR).map(PathBuf::from) else {
        return;
    };
    let mut engine = RocksEngine::open_with(
        &db,
        StorageOptions {
            memtable_budget_bytes: Some(64 << 10),
            ..StorageOptions::default()
        },
    )
    .expect("child open");
    let g1 = chain(
        G1,
        (Seq::ZERO, Digest::ROOT),
        ROLLOVER_RECORDS + ROLLOVER_TAIL,
    );
    commit(&mut engine, &g1, 1, ROLLOVER_CHUNK);

    let mut seen = std::collections::BTreeSet::new();
    let mut deleted = 0usize;
    let mut syncs = 0u64;
    let mut syncs_after_a_delete = 0u64;
    let mut next = ROLLOVER_CHUNK + 1;
    while next <= ROLLOVER_RECORDS {
        let live = wal_files(&db);
        deleted = seen.difference(&live).count();
        seen.extend(live);

        let sync = engine.begin_sync(vec![capture(G1, next - 1)]);
        let to = (next + ROLLOVER_CHUNK - 1).min(ROLLOVER_RECORDS);
        let flushed = std::thread::scope(|scope| {
            let flushing = scope.spawn(|| sync.flush());
            commit(&mut engine, &g1, next, to);
            flushing.join().expect("the flush thread")
        });
        assert_eq!(
            flushed,
            Ok(()),
            "sync {syncs} with records {next}..={to} under way"
        );
        let credited = engine.finish_sync(sync, flushed).expect("finish the sync");
        assert_eq!(credited, vec![durable(G1, next - 1)], "sync {syncs}");
        syncs += 1;
        if deleted > 0 {
            syncs_after_a_delete += 1;
        }
        next = to + 1;
    }
    let credited = sync_now(&mut engine, vec![capture(G1, ROLLOVER_RECORDS)]);
    assert_eq!(
        credited,
        vec![durable(G1, ROLLOVER_RECORDS)],
        "the last sync"
    );
    syncs += 1;
    seen.extend(wal_files(&db));
    commit(
        &mut engine,
        &g1,
        ROLLOVER_RECORDS + 1,
        ROLLOVER_RECORDS + ROLLOVER_TAIL,
    );

    let report = format!(
        "syncs={syncs}\ndir_syncs={}\nwal_files_seen={}\nwal_files_deleted={deleted}\n\
         syncs_after_a_delete={syncs_after_a_delete}\ncredited={ROLLOVER_RECORDS}\n",
        engine.wal_dir_syncs(),
        seen.len(),
    );
    std::fs::write(
        db.parent()
            .expect("the test directory")
            .join(ROLLOVER_REPORT),
        report,
    )
    .expect("write the report");
    let killed = std::process::Command::new("kill")
        .args(["-KILL", &std::process::id().to_string()])
        .status();
    // Reached only if the kill failed; the parent then sees SIGABRT, not SIGKILL, and fails.
    eprintln!("kill -KILL of the child did not end it: {killed:?}");
    std::process::abort();
}

/// Issue #71, the WAL directory fsync of [`WalSync::flush`] on unix (A-R373). A child process
/// commits 205 canonical records on a 64 KiB memtable budget, each chunk while a sync runs on
/// another thread, so RocksDB rolls to new WAL files and deletes old ones between and during
/// syncs; then it SIGKILLs itself with the engine open. Here:
/// - every sync it made fsynced the directory (the debug-only `wal_dir_syncs` count equals its
///   syncs), including the syncs made after a WAL it had seen was deleted;
/// - it saw at least three WAL files and at least one deleted;
/// - the reopen after the kill verifies the whole chain, keeps the durable mark the last sync
///   credited (200), and keeps the 5 unsynced records too.
///
/// What this proves: the directory step runs and succeeds on Linux across rollover and
/// deletion, never failing a sync, and a crash with no close keeps every credited prefix. What
/// it does not: that the fsync made a new WAL's entry survive a power cut. A killed process
/// leaves the OS cache, so the reopen would read the same with no fsync at all; proving that
/// needs a power cut or a block-level fault device (dm-flakey), which need root.
/// Integration (1.6-5.8 s here, at load ~140): disk-latency bound, a child process making ~60
/// real fsyncs (WAL files, the directory, RocksDB's own flushes) and a reopen.
/// Cut: remove the directory fsync in `WalSync::flush`, red (`dir_syncs=0`).
/// Cut: run it only on an open's first sync, so every sync after a rollover skips it, red
/// (`dir_syncs=1`).
#[cfg(unix)]
#[retcd_test]
fn m10_f4_unix_wal_rollover_under_syncs_keeps_credited_prefixes_after_a_crash() {
    use std::os::unix::process::ExitStatusExt;

    use rdb_storage::verify_lineage;

    let dir = data_dir("unix-rollover");
    let db = dir.join("db");
    let out = std::process::Command::new(std::env::current_exe().expect("current exe"))
        .args([
            "zz_child_rolls_wals_under_syncs_then_is_killed",
            "--exact",
            "--test-threads=1",
        ])
        .env(ROLLOVER_CHILD_DIR, &db)
        .output()
        .expect("spawn the child");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(
        out.status.signal(),
        Some(9),
        "the child must die by SIGKILL, got {:?}; stderr:\n{stderr}",
        out.status
    );
    let report = std::fs::read_to_string(dir.join(ROLLOVER_REPORT))
        .unwrap_or_else(|e| panic!("the child's report: {e}; stderr:\n{stderr}"));
    let field = |name: &str| -> u64 {
        report
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{name}=")))
            .unwrap_or_else(|| panic!("{name} in the report:\n{report}"))
            .parse()
            .expect("a number")
    };
    tracing::info!(report = %report.replace('\n', " "), "m10_f4_unix_rollover_report");

    let syncs = field("syncs");
    assert_eq!(
        syncs,
        ROLLOVER_RECORDS / ROLLOVER_CHUNK,
        "the child's syncs"
    );
    assert_eq!(
        field("dir_syncs"),
        syncs,
        "every sync fsynced the WAL directory:\n{report}"
    );
    assert!(
        field("wal_files_seen") >= 3,
        "RocksDB rolled to new WAL files:\n{report}"
    );
    assert!(
        field("wal_files_deleted") >= 1 && field("syncs_after_a_delete") >= 1,
        "RocksDB deleted a WAL file and syncs went on after it:\n{report}"
    );

    let engine = RocksEngine::open(&db).expect("reopen after the kill");
    let verified = verify_lineage(&engine, P, G1).expect("the chain verifies after the kill");
    let committed = ROLLOVER_RECORDS + ROLLOVER_TAIL;
    let g1 = chain(G1, (Seq::ZERO, Digest::ROOT), committed);
    assert_eq!(
        verified.durable,
        DurableSeq(field("credited")),
        "the credited prefix is durable after the kill"
    );
    assert_eq!(
        verified.applied,
        AppliedSeq(committed),
        "a process crash keeps the unsynced tail too"
    );
    assert_eq!(verified.head_digest, g1.digest(committed));
    drop(engine);
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}
