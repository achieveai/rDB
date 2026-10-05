//! M8 S0 behavioural contracts on a real RocksDB directory.
//!
//! 1. A persisted `durable` above `applied` is refused at open (critic F6).
//! 2. After a real process crash (`abort()` in a self-exec child), the stored history is a whole,
//!    digest-chained prefix, and a chain that does not link is caught.
//! 3. Lineages `(partition, generation)` do not see each other's records.
//!
//! Each test names the hand-walked scenario it protects: `#N` is row N of the M8 tester's
//! scenario table, in working notes not in the repository.
//!
//! Data goes under `RETCD_TEST_DATA_DIR` (set by `scripts/gate.sh`), else Cargo's per-target tmp
//! dir; never `%TEMP%`.

use std::path::{Path, PathBuf};
use std::process::Command;

use config_log::retcd_test;
use rdb_core::contracts::authority::Lineage;
use rdb_core::contracts::ids::{
    AppliedSeq, ConfigVersion, DurableSeq, Generation, OwnerEpoch, PartitionId, Seq,
};
use rdb_core::contracts::storage::CapturedPrefix;
use rdb_sim::storage::history::{canonical_history, CanonicalHistory};
use rdb_storage::{verify_lineage, LineageFault, OpenError, RocksEngine};

/// Env var that turns [`zz_child_commits_then_aborts`] from a no-op into the crashing child.
const CHILD_DIR: &str = "RDB_STORAGE_S0_CHILD_DIR";
/// Env var that makes the child sync before it aborts.
const CHILD_SYNC: &str = "RDB_STORAGE_S0_CHILD_SYNC";

fn data_dir(name: &str) -> PathBuf {
    let root = std::env::var_os("RETCD_TEST_DATA_DIR")
        .map_or_else(|| PathBuf::from(env!("CARGO_TARGET_TMPDIR")), PathBuf::from);
    let dir = root.join(format!("rdb-storage-s0-{name}-{}", std::process::id()));
    if dir.exists() {
        std::fs::remove_dir_all(&dir).expect("remove a stale test directory");
    }
    std::fs::create_dir_all(&dir).expect("create the test directory");
    dir
}

fn history(partition: u32, generation: u64, epoch: u64, n: u64) -> CanonicalHistory {
    let lineage = Lineage {
        partition: PartitionId(partition),
        generation: Generation(generation),
        owner_epoch: OwnerEpoch(epoch),
    };
    canonical_history(lineage, ConfigVersion(1), n).expect("canonical history")
}

fn commit_all(engine: &mut RocksEngine, history: &CanonicalHistory) {
    for batch in &history.batches {
        engine.commit(batch.clone()).expect("commit");
    }
}

/// Raw RocksDB key of an engine-private watermark: `partition BE | generation BE | 0xFF | name`.
/// Spelled out here on purpose: this test pins the provisional layout it edits.
fn private_key(partition: u32, generation: u64, name: &[u8]) -> Vec<u8> {
    let mut key = partition.to_be_bytes().to_vec();
    key.extend_from_slice(&generation.to_be_bytes());
    key.push(0xFF);
    key.extend_from_slice(name);
    key
}

/// #13 (critic F6): persisted durable above applied is refused at open, every time, unrepaired.
#[retcd_test]
fn s0_open_refuses_durable_above_applied() {
    let dir = data_dir("durable-above-applied");
    let db = dir.join("db");
    {
        let mut engine = RocksEngine::open(&db).expect("open");
        commit_all(&mut engine, &history(1, 1, 1, 2));
        let durable = engine
            .sync_wal_through(vec![CapturedPrefix {
                partition: PartitionId(1),
                generation: Generation(1),
                through: AppliedSeq(2),
            }])
            .expect("sync");
        assert_eq!(durable[0].through, DurableSeq(2));
    }
    // A lying disk: the applied mark falls back to 1 while durable stays 2.
    {
        let cfs = rdb_storage::keys::COLUMN_FAMILIES;
        let raw = rocksdb::DB::open_cf(&rocksdb::Options::default(), &db, cfs).expect("raw open");
        let meta = raw.cf_handle("metadata").expect("metadata cf");
        raw.put_cf(meta, private_key(1, 1, b"applied"), 1u64.to_be_bytes())
            .expect("raw put");
    }
    let refused = RocksEngine::open(&db).expect_err("durable > applied must be refused");
    assert!(
        matches!(
            refused,
            OpenError::DurableAboveApplied {
                partition: 1,
                generation: 1,
                applied: 1,
                durable: 2
            }
        ),
        "{refused:?}"
    );
    // #13 "every time": a refused open repairs nothing, so the next one refuses the same way.
    let again = RocksEngine::open(&db).expect_err("still refused");
    assert_eq!(again.to_string(), refused.to_string());
    let marks: Vec<_> = rdb_storage::dump(&db)
        .expect("dump")
        .into_iter()
        .filter(|r| {
            r.key.as_ref() == private_key(1, 1, b"applied")
                || r.key.as_ref() == private_key(1, 1, b"durable")
        })
        .map(|r| r.value.to_vec())
        .collect();
    assert_eq!(
        marks,
        vec![1u64.to_be_bytes().to_vec(), 2u64.to_be_bytes().to_vec()],
        "applied=1, durable=2 untouched"
    );
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// The crashing child. A no-op unless the parent set [`CHILD_DIR`]; then it commits three
/// canonical batches and aborts with the engine still open. With [`CHILD_SYNC`] set it first
/// syncs through 3, so the durable mark is the last record in the WAL.
#[test]
fn zz_child_commits_then_aborts() {
    let Some(db) = std::env::var_os(CHILD_DIR) else {
        return;
    };
    let mut engine = RocksEngine::open(PathBuf::from(db)).expect("child open");
    commit_all(&mut engine, &history(1, 1, 1, 3));
    if std::env::var_os(CHILD_SYNC).is_some() {
        engine
            .sync_wal_through(vec![CapturedPrefix {
                partition: PartitionId(1),
                generation: Generation(1),
                through: AppliedSeq(3),
            }])
            .expect("child sync");
    }
    std::process::abort();
}

/// Run [`zz_child_commits_then_aborts`] in a child process against `db` and wait for it to die.
fn crash_child(db: &Path, sync: bool) {
    let mut child = Command::new(std::env::current_exe().expect("current exe"));
    child
        .args([
            "zz_child_commits_then_aborts",
            "--exact",
            "--test-threads=1",
        ])
        .env(CHILD_DIR, db);
    if sync {
        child.env(CHILD_SYNC, "1");
    }
    let out = child.output().expect("spawn the child");
    assert!(
        !out.status.success(),
        "the child must die by abort(), got {:?}",
        out.status
    );
}

/// #4: abort after 3 unsynced commits; reopen keeps all 3, chained.
#[retcd_test]
fn s0_chain_verified_after_process_crash() {
    let dir = data_dir("crash-chain");
    let db = dir.join("db");
    crash_child(&db, false);

    let engine = RocksEngine::open(&db).expect("reopen after crash");
    let verified =
        verify_lineage(&engine, PartitionId(1), Generation(1)).expect("chain after crash");
    let expected = history(1, 1, 1, 3);
    assert_eq!(
        verified.applied,
        AppliedSeq(3),
        "process crash keeps applied"
    );
    assert_eq!(verified.durable, DurableSeq(0), "nothing was synced");
    assert_eq!(verified.head_digest, expected.digest(3));
    drop(engine);
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// #11 (c03) and #17: a record whose `prev_digest` does not link is caught, and only its own
/// lineage faults.
#[retcd_test]
fn s0_verify_catches_a_record_that_does_not_chain() {
    let dir = data_dir("broken-chain");
    let db = dir.join("db");
    let mut engine = RocksEngine::open(&db).expect("open");
    let good = history(1, 1, 1, 3);
    // Same lineage and seq, other owner epoch: a valid record whose digests differ.
    let other = history(1, 1, 2, 3);
    engine.commit(good.batch(1).clone()).expect("commit 1");
    engine.commit(good.batch(2).clone()).expect("commit 2");
    engine.commit(other.batch(3).clone()).expect("commit 3");
    commit_all(&mut engine, &history(2, 1, 1, 3));
    assert_eq!(
        verify_lineage(&engine, PartitionId(1), Generation(1)),
        Err(LineageFault::BrokenChain(Seq(3)))
    );
    assert!(
        verify_lineage(&engine, PartitionId(2), Generation(1)).is_ok(),
        "a fault in (1, 1) must not touch (2, 1)"
    );
    drop(engine);
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// #17 and #28: four lineages, (0, 0) among them, each with its own marks and chain.
#[retcd_test]
fn s0_lineages_are_isolated_by_partition_and_generation() {
    let dir = data_dir("isolation");
    let db = dir.join("db");
    // #28 (lead L-R182w, P6): partition 0 and generation 0 are legal lineages.
    let lineages = [(1u32, 1u64, 2u64), (2, 1, 1), (1, 2, 3), (0, 0, 1)];
    {
        let mut engine = RocksEngine::open(&db).expect("open");
        for &(partition, generation, n) in &lineages {
            commit_all(&mut engine, &history(partition, generation, 1, n));
        }
    }
    let engine = RocksEngine::open(&db).expect("reopen");
    assert_eq!(
        engine.lineages(),
        vec![
            (PartitionId(0), Generation(0)),
            (PartitionId(1), Generation(1)),
            (PartitionId(1), Generation(2)),
            (PartitionId(2), Generation(1)),
        ]
    );
    for &(partition, generation, n) in &lineages {
        let (p, g) = (PartitionId(partition), Generation(generation));
        let verified = verify_lineage(&engine, p, g).expect("each lineage verifies alone");
        assert_eq!(verified.applied, AppliedSeq(n));
        assert_eq!(
            verified.head_digest,
            history(partition, generation, 1, n).digest(n)
        );
        assert_eq!(
            engine.history_at(p, g, Seq(n + 1)).expect("read"),
            None,
            "no record of another lineage shows above this one's head"
        );
    }
    drop(engine);
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

// --- Regressions for observed defects -------------------------------------------------------

/// #16. D1 (dev-m8 by hand, 2026-10-02): a corrupt SST was reported as `Locked`, exit 5, because
/// "block checksum mismatch" contains "lock". A tester told "locked" goes looking for a second
/// process that does not exist.
#[retcd_test]
fn s0_d1_corrupt_sst_is_not_reported_as_locked() {
    let dir = data_dir("d1-corrupt-sst");
    let db = dir.join("db");
    {
        let mut engine = RocksEngine::open(&db).expect("open");
        commit_all(&mut engine, &history(1, 1, 1, 3));
    }
    // A writable reopen flushes the recovered WAL into SST files (critic F3).
    drop(RocksEngine::open(&db).expect("reopen"));
    let mut corrupted = 0;
    for entry in std::fs::read_dir(&db).expect("list db") {
        let path = entry.expect("dir entry").path();
        if path.extension().is_some_and(|ext| ext == "sst") {
            let mut bytes = std::fs::read(&path).expect("read sst");
            let at = bytes.len() / 3;
            bytes[at..at + 8].fill(0xFF);
            std::fs::write(&path, bytes).expect("write sst");
            corrupted += 1;
        }
    }
    assert!(
        corrupted > 0,
        "the reopen must have produced an SST to corrupt"
    );
    let refused = RocksEngine::open(&db).expect_err("a corrupt SST must refuse the open");
    assert!(
        matches!(&refused, OpenError::Backend { detail, .. } if detail.contains("Corruption")),
        "{refused:?}"
    );
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// #20. D1's other side: a real lock is still `Locked`.
#[retcd_test]
fn s0_d1_second_open_is_locked() {
    let dir = data_dir("d1-locked");
    let db = dir.join("db");
    let first = RocksEngine::open(&db).expect("open");
    let refused = RocksEngine::open(&db).expect_err("a second open must be refused");
    assert!(matches!(refused, OpenError::Locked { .. }), "{refused:?}");
    drop(first);
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// #1. D2 (dev-m8 by hand, 2026-10-02): `verify --dir <typo>` created an empty database and
/// reported `lineages=0 OK`, exit 0. A check that passes on the wrong directory is a false pass.
#[retcd_test]
fn s0_d2_open_existing_refuses_a_missing_database() {
    let dir = data_dir("d2-missing");
    let db = dir.join("not-there");
    let refused = RocksEngine::open_existing(&db).expect_err("no database there");
    assert!(
        matches!(refused, OpenError::NoDatabase { .. }),
        "{refused:?}"
    );
    assert!(!db.exists(), "a refused open must not create the directory");
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// Records `(level, message)` of every event while installed with `with_default`. Thread-local,
/// so it sees only this test's events and nothing races a file writer.
#[derive(Clone, Default)]
struct Events(std::sync::Arc<std::sync::Mutex<Vec<(tracing::Level, String)>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Events {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        struct Message(String);
        impl tracing::field::Visit for Message {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    self.0 = format!("{value:?}");
                }
            }
        }
        let mut message = Message(String::new());
        event.record(&mut message);
        self.0
            .lock()
            .expect("events lock")
            .push((*event.metadata().level(), message.0));
    }
}

/// #14. T2 (tester-m8, 2026-10-02): an open refused with `CorruptRecord` wrote no log event; only
/// the exit code said anything. Every refusal must leave an Error line naming it.
#[test]
fn s0_t2_corrupt_record_refusal_is_logged() {
    use tracing_subscriber::layer::SubscriberExt as _;
    let dir = data_dir("t2-corrupt-record");
    let db = dir.join("db");
    drop(RocksEngine::open(&db).expect("open"));
    {
        let cfs = rdb_storage::keys::COLUMN_FAMILIES;
        let raw = rocksdb::DB::open_cf(&rocksdb::Options::default(), &db, cfs).expect("raw open");
        let meta = raw.cf_handle("metadata").expect("metadata cf");
        raw.put_cf(meta, private_key(1, 1, b"appliex"), 1u64.to_be_bytes())
            .expect("raw put");
    }
    let events = Events::default();
    let subscriber = tracing_subscriber::registry().with(events.clone());
    let refused = tracing::subscriber::with_default(subscriber, || RocksEngine::open(&db))
        .expect_err("an unknown engine record must refuse the open");
    assert!(
        matches!(refused, OpenError::CorruptRecord { .. }),
        "{refused:?}"
    );
    let seen = events.0.lock().expect("events lock").clone();
    assert!(
        seen.iter()
            .any(|(level, message)| *level == tracing::Level::ERROR
                && message == "storage_open_refused_corrupt_record"),
        "no Error event storage_open_refused_corrupt_record in {seen:?}"
    );
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// Flat copy of a RocksDB directory: the post-crash state, before any reopen (critic F3).
fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("create the copy");
    for entry in std::fs::read_dir(from).expect("list db") {
        let path = entry.expect("dir entry").path();
        std::fs::copy(&path, to.join(path.file_name().expect("file name"))).expect("copy file");
    }
}

/// Every file name and its bytes, in name order.
fn snapshot(dir: &Path) -> Vec<(std::ffi::OsString, Vec<u8>)> {
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .expect("list db")
        .map(|e| {
            let path = e.expect("dir entry").path();
            (
                path.file_name().expect("file name").to_owned(),
                std::fs::read(&path).expect("read"),
            )
        })
        .collect();
    files.sort();
    files
}

/// The WAL file with the highest number: the one the crashed process was writing.
fn newest_wal(dir: &Path) -> PathBuf {
    std::fs::read_dir(dir)
        .expect("list db")
        .map(|e| e.expect("dir entry").path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "log"))
        .max()
        .expect("a WAL file after the crash")
}

fn truncate(path: &Path, len: u64) {
    std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open the WAL")
        .set_len(len)
        .expect("truncate the WAL");
}

/// #6 and #7: on the directory a crash left behind, `dump` changes no byte; and a WAL torn at
/// any point recovers a whole-batch prefix that verifies, never a part of a batch.
#[retcd_test]
fn s0_crashed_dir_dump_is_read_only_and_a_torn_wal_keeps_whole_batches() {
    let dir = data_dir("torn-wal");
    let db = dir.join("db");
    crash_child(&db, false);

    let before = snapshot(&db);
    let records = rdb_storage::dump(&db).expect("dump the crashed dir");
    assert!(!records.is_empty(), "dump must see the WAL's records");
    assert!(snapshot(&db) == before, "#6: dump must not change any file");

    let wal_name = newest_wal(&db).file_name().expect("wal name").to_owned();
    let len = std::fs::metadata(db.join(&wal_name))
        .expect("wal size")
        .len();
    let mut last = 0;
    for k in 0..=6 {
        let cut = len * k / 6;
        let copy = dir.join(format!("cut-{cut}"));
        copy_dir(&db, &copy);
        truncate(&copy.join(&wal_name), cut);
        let engine = RocksEngine::open(&copy).expect("open the torn copy");
        let verified = verify_lineage(&engine, PartitionId(1), Generation(1))
            .unwrap_or_else(|f| panic!("cut {cut}/{len}: {f}"));
        let applied = verified.applied.0;
        assert!(
            applied >= last,
            "cut {cut}/{len}: applied {applied} fell below {last}"
        );
        assert_eq!(
            verified.durable,
            DurableSeq(0),
            "cut {cut}/{len}: nothing was synced"
        );
        if cut == 0 {
            assert_eq!(
                applied, 0,
                "an empty WAL keeps nothing: the cut must take effect"
            );
        }
        last = applied;
    }
    assert_eq!(last, 3, "the untorn WAL keeps every batch");
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// #10 and #8: crash after a flush; then tear off the durable mark, the WAL's last record.
/// Durable falls back to 0 and applied stays 3: durable may lag, never lead.
#[retcd_test]
fn s0_torn_flush_mark_lets_durable_fall_back_never_lead() {
    let dir = data_dir("torn-flush-mark");
    let db = dir.join("db");
    crash_child(&db, true);
    let torn = dir.join("torn");
    copy_dir(&db, &torn);

    let engine = RocksEngine::open(&db).expect("reopen after flush and crash");
    let whole = verify_lineage(&engine, PartitionId(1), Generation(1)).expect("verify");
    assert_eq!(
        (whole.applied, whole.durable),
        (AppliedSeq(3), DurableSeq(3)),
        "#8"
    );
    drop(engine);

    let wal = newest_wal(&torn);
    let len = std::fs::metadata(&wal).expect("wal size").len();
    truncate(&wal, len - 1);
    let engine = RocksEngine::open(&torn).expect("open with the mark torn");
    let fallen = verify_lineage(&engine, PartitionId(1), Generation(1)).expect("verify");
    assert_eq!(
        (fallen.applied, fallen.durable),
        (AppliedSeq(3), DurableSeq(0)),
        "#10"
    );
    drop(engine);
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// One hand-crafted corruption from scenarios #11 and #12, applied with RocksDB directly.
#[derive(Debug, Clone, Copy)]
enum Craft {
    /// c02: flip the last byte of record 2, inside its carried `record_digest`.
    RecordDigest,
    /// c07: record 2's frame version byte.
    FrameByte,
    /// c11: flip the last byte of the Progress value, inside the head digest.
    Progress,
    /// c05/c06: move the applied mark.
    Applied(u64),
    /// #24 u1: record 2's envelope magic `RDBE` becomes `RDBD` (after the 9-byte frame).
    Magic,
    /// #25: records 2 and 3 swap places.
    Swap,
}

/// #11, #12, #24 and #25: each crafted corruption the tester walked names its own fault. One lineage per
/// case in one directory (#17 keeps them apart), so the table costs three opens, not fifteen.
#[retcd_test]
fn s0_verify_names_each_crafted_fault() {
    use rdb_core::contracts::storage::{Namespace, StorageFault};
    use rdb_core::replication::append::PROGRESS_KEY;
    use rdb_storage::keys::{cf_for, encode_key, CF_METADATA, COLUMN_FAMILIES};

    let cases = [
        (Craft::RecordDigest, LineageFault::DigestMismatch(Seq(2))),
        (Craft::FrameByte, LineageFault::Read(StorageFault::Corrupt)),
        (Craft::Progress, LineageFault::ProgressMismatch(Seq(3))),
        (Craft::Applied(5), LineageFault::Missing(Seq(4))),
        (Craft::Applied(2), LineageFault::AboveApplied(Seq(3))),
        (Craft::Magic, LineageFault::Undecodable(Seq(2))),
        (
            Craft::Swap,
            LineageFault::WrongSeq {
                at: Seq(2),
                claims: Seq(3),
            },
        ),
    ];
    let partition = |i: usize| u32::try_from(i + 1).expect("small");
    let dir = data_dir("crafted-faults");
    let db = dir.join("db");
    {
        let mut engine = RocksEngine::open(&db).expect("open");
        for i in 0..cases.len() {
            commit_all(&mut engine, &history(partition(i), 1, 1, 3));
        }
    }
    {
        let raw = rocksdb::DB::open_cf(&rocksdb::Options::default(), &db, COLUMN_FAMILIES)
            .expect("raw open");
        for (i, (craft, _)) in cases.iter().enumerate() {
            let (p, g) = (PartitionId(partition(i)), Generation(1));
            let edit = |ns: Namespace, key: &[u8], change: &dyn Fn(&mut Vec<u8>)| {
                let cf = raw.cf_handle(cf_for(ns)).expect("cf");
                let raw_key = encode_key(p, g, ns, key);
                let mut value = raw.get_cf(cf, &raw_key).expect("get").expect("present");
                change(&mut value);
                raw.put_cf(cf, &raw_key, value).expect("put");
            };
            let flip_last = |v: &mut Vec<u8>| *v.last_mut().expect("non-empty") ^= 0x01;
            match *craft {
                Craft::RecordDigest => edit(Namespace::History, &2u64.to_be_bytes(), &flip_last),
                Craft::FrameByte => edit(Namespace::History, &2u64.to_be_bytes(), &|v| v[0] = 0x01),
                Craft::Progress => edit(Namespace::Progress, PROGRESS_KEY, &flip_last),
                Craft::Magic => edit(Namespace::History, &2u64.to_be_bytes(), &|v| {
                    assert_eq!(&v[9..13], b"RDBE", "frame then envelope magic");
                    v[12] = b'D';
                }),
                Craft::Swap => {
                    let cf = raw.cf_handle(cf_for(Namespace::History)).expect("cf");
                    let key = |seq: u64| encode_key(p, g, Namespace::History, &seq.to_be_bytes());
                    let two = raw.get_cf(cf, key(2)).expect("get").expect("present");
                    let three = raw.get_cf(cf, key(3)).expect("get").expect("present");
                    raw.put_cf(cf, key(2), three).expect("put");
                    raw.put_cf(cf, key(3), two).expect("put");
                }
                Craft::Applied(applied) => {
                    let meta = raw.cf_handle(CF_METADATA).expect("metadata cf");
                    raw.put_cf(meta, private_key(p.0, 1, b"applied"), applied.to_be_bytes())
                        .expect("put");
                }
            }
        }
    }
    let engine = RocksEngine::open(&db).expect("open the crafted dir");
    for (i, (craft, fault)) in cases.into_iter().enumerate() {
        let p = PartitionId(partition(i));
        assert_eq!(
            verify_lineage(&engine, p, Generation(1)),
            Err(fault),
            "{craft:?}"
        );
    }
    drop(engine);
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// #14 (c08), #21, #26, #27 and S1 #29: a directory with another, a missing or a malformed
/// format marker, a malformed watermark, or another column-family set is refused at open with the
/// error that names it. S1 moved the format to 1 (ADR-rdb-0010 decision 11), so S0's 0 is now a
/// foreign marker too.
#[retcd_test]
fn s0_open_refuses_a_foreign_layout() {
    use rdb_storage::keys::{CF_METADATA, COLUMN_FAMILIES, FORMAT_VERSION};

    let dir = data_dir("foreign-layout");
    // (row, the metadata records of a directory built raw, the refusal it must produce). Every
    // row holds data (an applied mark), so a missing marker cannot be stamped as a fresh dir.
    let applied = private_key(1, 1, b"applied");
    type Records<'a> = Vec<(&'a [u8], Vec<u8>)>;
    type Refusal = fn(&OpenError) -> bool;
    let current = FORMAT_VERSION.to_be_bytes().to_vec();
    let rows: [(&str, Records, Refusal); 5] = [
        (
            "#14 c08 format marker from a newer build",
            vec![
                (b"format", (FORMAT_VERSION + 1).to_be_bytes().to_vec()),
                (&applied, 1u64.to_be_bytes().to_vec()),
            ],
            |e| matches!(e, OpenError::Format { found: Some(v), .. } if *v == FORMAT_VERSION + 1),
        ),
        (
            "S1 #29 S0's provisional format 0",
            vec![
                (b"format", 0u32.to_be_bytes().to_vec()),
                (&applied, 1u64.to_be_bytes().to_vec()),
            ],
            |e| matches!(e, OpenError::Format { found: Some(0), .. }),
        ),
        (
            "#26 marker missing, data present",
            vec![(&applied, 1u64.to_be_bytes().to_vec())],
            |e| matches!(e, OpenError::Format { found: None, .. }),
        ),
        (
            "#27 marker of 3 bytes",
            vec![
                (b"format", vec![0; 3]),
                (&applied, 1u64.to_be_bytes().to_vec()),
            ],
            |e| matches!(e, OpenError::CorruptRecord { .. }),
        ),
        (
            "#27 applied mark of 7 bytes",
            vec![(b"format", current), (&applied, vec![0; 7])],
            |e| matches!(e, OpenError::CorruptRecord { .. }),
        ),
    ];
    for (i, (row, records, expected)) in rows.into_iter().enumerate() {
        let db = dir.join(format!("db-{i}"));
        {
            let mut opts = rocksdb::Options::default();
            opts.create_if_missing(true);
            opts.create_missing_column_families(true);
            let raw = rocksdb::DB::open_cf(&opts, &db, COLUMN_FAMILIES).expect("raw create");
            let meta = raw.cf_handle(CF_METADATA).expect("metadata cf");
            for (key, value) in records {
                raw.put_cf(meta, key, value).expect("put");
            }
        }
        let refused = RocksEngine::open(&db).expect_err(row);
        assert!(expected(&refused), "{row}: {refused:?}");
    }

    let foreign = dir.join("foreign");
    {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        rocksdb::DB::open_cf(&opts, &foreign, ["default", "raft_log"]).expect("foreign db");
    }
    let refused = RocksEngine::open(&foreign).expect_err("a foreign column-family set");
    assert!(
        matches!(refused, OpenError::ColumnFamilies { .. }),
        "{refused:?}"
    );
    assert!(
        rocksdb::DB::list_cf(&rocksdb::Options::default(), &foreign).expect("list")
            == ["default", "raft_log"],
        "a refused open must not add column families"
    );
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// #29, #30 and #31: an injected storage fault fails the call, moves no watermark, and leaves a
/// directory that still verifies. #31's mark-write fault is the state a crash between the WAL
/// sync and the durable-mark write leaves; a later sync recovers it.
#[cfg(debug_assertions)]
#[retcd_test]
fn s0_injected_storage_faults_move_no_watermark() {
    use rdb_core::contracts::storage::StorageFault;
    use rdb_storage::InjectedFault;

    let (p, g) = (PartitionId(1), Generation(1));
    let capture = || {
        vec![CapturedPrefix {
            partition: p,
            generation: g,
            through: AppliedSeq(3),
        }]
    };
    let dir = data_dir("injected-faults");
    let db = dir.join("db");
    let full = history(1, 1, 1, 4);
    {
        let mut engine = RocksEngine::open(&db).expect("open");
        for seq in 1..=3 {
            engine.commit(full.batch(seq).clone()).expect("commit");
        }
        engine.inject_fault(InjectedFault::Commit);
        assert_eq!(
            engine.commit(full.batch(4).clone()),
            Err(StorageFault::WriteFailed),
            "#29"
        );
        assert_eq!(engine.buffered_applied(p, g), AppliedSeq(3), "#29");
        engine.inject_fault(InjectedFault::WalFlush);
        assert_eq!(
            engine.sync_wal_through(capture()),
            Err(StorageFault::FlushFailed),
            "#30"
        );
        engine.inject_fault(InjectedFault::MarkWrite);
        assert_eq!(
            engine.sync_wal_through(capture()),
            Err(StorageFault::FlushFailed),
            "#31"
        );
        assert_eq!(engine.durable(p, g), DurableSeq(0), "#30/#31");
    }
    let mut engine = RocksEngine::open(&db).expect("reopen");
    let verified = verify_lineage(&engine, p, g).expect("verify after the faults");
    assert_eq!(
        (verified.applied, verified.durable),
        (AppliedSeq(3), DurableSeq(0))
    );
    let durable = engine
        .sync_wal_through(capture())
        .expect("a clean sync recovers #31");
    assert_eq!(durable[0].through, DurableSeq(3));
    drop(engine);
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// #31 / T4 (tester-m8 on v5, 2026-10-02): `flush --inject mark-write` with every durable
/// already at applied logged `fault_injected` and exited 0: there was no mark to write, so the
/// fault never fired, yet the log said it had. The log must name a fault only when it fires.
#[cfg(debug_assertions)]
#[test]
fn s0_t4_an_unreached_inject_point_logs_no_fault() {
    use rdb_storage::InjectedFault;
    use tracing_subscriber::layer::SubscriberExt as _;

    let (p, g) = (PartitionId(1), Generation(1));
    let capture = || {
        vec![CapturedPrefix {
            partition: p,
            generation: g,
            through: AppliedSeq(2),
        }]
    };
    let dir = data_dir("t4-unreached");
    let mut engine = RocksEngine::open(dir.join("db")).expect("open");
    commit_all(&mut engine, &history(1, 1, 1, 2));
    engine
        .sync_wal_through(capture())
        .expect("durable reaches applied");
    let events = Events::default();
    let subscriber = tracing_subscriber::registry().with(events.clone());
    tracing::subscriber::with_default(subscriber, || {
        engine.inject_fault(InjectedFault::MarkWrite);
        engine
            .sync_wal_through(capture())
            .expect("nothing to mark: the sync succeeds");
    });
    let seen = events.0.lock().expect("events lock").clone();
    assert!(
        !seen.iter().any(|(_, message)| message == "fault_injected"),
        "fault_injected logged for a fault that never fired: {seen:?}"
    );
    assert_eq!(
        engine.armed_fault(),
        Some(InjectedFault::MarkWrite),
        "an unreached fault stays armed, so the caller can report it"
    );
    drop(engine);
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}
