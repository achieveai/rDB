//! M10 slice 1, stream W2: storage contracts for reopen (arch-m10 R3b, R3c `rdb-storage` row).
//!
//! 1. The instance marker: absent means unbound; a bind is durable across a reopen and visible
//!    to a read-only open; binding the same id again writes nothing; another id is refused and
//!    the copy keeps its first id; a marker of the wrong length is refused at open.
//! 2. The publish marks (S1-6, A-R183, critic-d6b row 6): a synced per-partition list that
//!    survives a reopen, which no lineage read, seed or retire scan meets, and which an open
//!    of a copy holding it still accepts.
//!
//! Data goes under `RETCD_TEST_DATA_DIR` (set by `scripts/gate.sh`), else Cargo's per-target tmp
//! dir; never `%TEMP%`.

use std::path::PathBuf;

use config_log::retcd_test;
use rdb_core::contracts::authority::Lineage;
use rdb_core::contracts::ids::{
    ConfigVersion, Generation, OwnerEpoch, PartitionId, Seq, SnapshotHandle,
};
use rdb_core::contracts::storage::{Namespace, SnapshotRead, StorageFault};
use rdb_core::contracts::time::Tick;
use rdb_core::contracts::txn::{Durability, Outcome, TxnResult};
use rdb_core::transaction::dedup::{DedupIndex, SeedPending};
use rdb_sim::storage::history::canonical_history;
use rdb_storage::{dump, BindError, OpenError, PublishMark, RocksEngine, INSTANCE_LEN};

const X: [u8; INSTANCE_LEN] = [0x11; INSTANCE_LEN];
const Y: [u8; INSTANCE_LEN] = [0x22; INSTANCE_LEN];

fn data_dir(name: &str) -> PathBuf {
    let root = std::env::var_os("RETCD_TEST_DATA_DIR")
        .map_or_else(|| PathBuf::from(env!("CARGO_TARGET_TMPDIR")), PathBuf::from);
    let dir = root.join(format!("rdb-storage-m10-{name}-{}", std::process::id()));
    if dir.exists() {
        std::fs::remove_dir_all(&dir).expect("remove a stale test directory");
    }
    std::fs::create_dir_all(&dir).expect("create the test directory");
    dir
}

/// Commit `n` canonical batches into lineage (1, 1).
fn write(engine: &mut RocksEngine, n: u64) {
    let lineage = Lineage {
        partition: PartitionId(1),
        generation: Generation(1),
        owner_epoch: OwnerEpoch(1),
    };
    let history = canonical_history(lineage, ConfigVersion(1), n).expect("canonical history");
    for batch in history.batches {
        engine.commit(batch).expect("commit");
    }
}

/// An M9 copy (lineage, no marker) reads as unbound; a bind survives a reopen, is seen by a
/// read-only open, and is never replaced by another id.
#[retcd_test]
fn m10_w2_instance_marker_binds_once_and_survives_reopen() {
    let dir = data_dir("instance-marker");
    let db = dir.join("db");
    {
        let mut engine = RocksEngine::open(&db).expect("open");
        write(&mut engine, 3);
        assert_eq!(engine.instance(), None, "a copy with lineage and no marker");
    }
    {
        let mut engine = RocksEngine::open(&db).expect("reopen unbound");
        assert_eq!(engine.instance(), None, "still unbound after a reopen");
        engine.bind_instance(X).expect("bind X");
        assert_eq!(engine.instance(), Some(X));
    }
    let before = dump(&db).expect("dump");
    {
        let mut engine = RocksEngine::open(&db).expect("reopen bound");
        assert_eq!(engine.instance(), Some(X), "the bind is durable");
        engine.bind_instance(X).expect("binding the same id again");
        assert_eq!(
            engine.bind_instance(Y),
            Err(BindError::OtherInstance {
                found: X,
                wanted: Y
            })
        );
        assert_eq!(engine.instance(), Some(X), "a refused bind changes nothing");
    }
    assert_eq!(
        dump(&db).expect("dump"),
        before,
        "neither the repeat bind nor the refused one wrote a record"
    );
    assert!(
        before.iter().any(|r| r
            .to_string()
            .contains(&format!("instance_marker={}", "11".repeat(16)))),
        "dump names the marker"
    );
    let reader = RocksEngine::open_read_only(&db).expect("read-only open");
    assert_eq!(
        reader.instance(),
        Some(X),
        "a read-only open sees the marker"
    );
    assert_eq!(
        reader.buffered_applied(PartitionId(1), Generation(1)).0,
        3,
        "the lineage is untouched by the marker"
    );
    drop(reader);
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// A marker that is not 16 bytes is refused at open, writable and read-only alike, and never
/// read as unbound.
#[retcd_test]
fn m10_w2_instance_marker_of_the_wrong_length_is_refused_at_open() {
    use rdb_storage::keys::CF_METADATA;

    let dir = data_dir("instance-wrong-length");
    let db = dir.join("db");
    drop(RocksEngine::open(&db).expect("create"));
    {
        let raw = rocksdb::DB::open_cf(
            &rocksdb::Options::default(),
            &db,
            rdb_storage::keys::COLUMN_FAMILIES,
        )
        .expect("raw open");
        let meta = raw.cf_handle(CF_METADATA).expect("metadata cf");
        raw.put_cf(meta, b"instance", [0x11; 15]).expect("put");
    }
    for (row, refused) in [
        ("writable", RocksEngine::open(&db).expect_err("writable")),
        (
            "read-only",
            RocksEngine::open_read_only(&db).expect_err("read-only"),
        ),
    ] {
        assert!(
            matches!(&refused, OpenError::CorruptRecord { problem, .. }
                if problem.contains("15 bytes")),
            "{row}: {refused:?}"
        );
    }
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// Every record a lineage read of (1, 1) returns, namespace by namespace.
fn every_namespace(engine: &RocksEngine) -> Vec<Vec<(bytes::Bytes, bytes::Bytes)>> {
    let snapshot = engine
        .snapshot(PartitionId(1), Generation(1), SnapshotHandle(1))
        .expect("snapshot");
    [
        Namespace::User,
        Namespace::History,
        Namespace::Dedup,
        Namespace::Progress,
    ]
    .into_iter()
    .map(|ns| snapshot.scan(ns, &[], 10_000))
    .collect()
}

/// How many entries T1's seed loads from (1, 1) for a successor serving generation 2.
fn seeded(engine: &RocksEngine) -> usize {
    let snapshot = engine
        .snapshot(PartitionId(1), Generation(1), SnapshotHandle(1))
        .expect("snapshot");
    let pending = SeedPending {
        partition: PartitionId(1),
        serving: Generation(2),
        retained_through: snapshot.at(),
    };
    DedupIndex::default()
        .seed(&pending, &snapshot, |lineage, seq| TxnResult {
            partition: lineage.partition,
            owner_epoch: lineage.owner_epoch,
            generation: lineage.generation,
            seq,
            outcome: Outcome::Published,
            durability: Durability::BufferedOnTwo,
        })
        .expect("the seed is not blocked")
}

/// S1-6 row 6 (A-R183, critic-d6b): the publish marks are a synced per-partition list. They
/// survive a reopen, read-only included, and are replaced whole. No lineage read meets them, in
/// any namespace, so T1's seed loads the same rows with or without them, and the retire scan
/// (`delete_dedup_below`) leaves them. An open of a copy holding them succeeds and finds no extra
/// lineage. A list that is not whole marks is `Corrupt`, never "no marks", and does not stop
/// the open. `dump` names them.
#[retcd_test]
fn m10_w2_publish_marks_survive_a_reopen_and_no_lineage_scan_meets_them() {
    let dir = data_dir("publish-marks");
    let db = dir.join("db");
    let (p1, p2) = (PartitionId(1), PartitionId(2));
    let marks = vec![
        PublishMark {
            generation: Generation(1),
            seq: Seq(2),
            tick: Tick(1_000),
        },
        PublishMark {
            generation: Generation(1),
            seq: Seq(3),
            tick: Tick(1_500),
        },
    ];
    let (records, loaded) = {
        let mut engine = RocksEngine::open(&db).expect("open");
        write(&mut engine, 3);
        assert_eq!(
            engine.publish_marks(p1),
            Ok(Vec::new()),
            "none before a put"
        );
        let records = every_namespace(&engine);
        let loaded = seeded(&engine);
        assert_eq!(loaded, 3, "the seed loads every applied request");
        engine.put_publish_marks(p1, &marks).expect("put");
        engine
            .put_publish_marks(p2, &[])
            .expect("put an empty list");
        assert_eq!(engine.publish_marks(p1), Ok(marks.clone()));
        assert_eq!(
            every_namespace(&engine),
            records,
            "no lineage read meets them"
        );
        assert_eq!(seeded(&engine), loaded, "the seed loads the same rows");
        (records, loaded)
    };
    {
        let mut engine = RocksEngine::open(&db).expect("an open of a copy holding marks");
        assert_eq!(
            engine.lineages(),
            vec![(p1, Generation(1))],
            "the marks are no lineage"
        );
        assert_eq!(engine.publish_marks(p1), Ok(marks.clone()), "durable");
        assert_eq!(engine.publish_marks(p2), Ok(Vec::new()), "stored empty");
        assert_eq!(every_namespace(&engine), records);
        assert_eq!(seeded(&engine), loaded);
        engine
            .delete_dedup_below(p1, Generation(1), &[0xFF; 8])
            .expect("retire every Dedup row");
        assert_eq!(
            seeded(&engine),
            0,
            "the retire scan removed every Dedup row"
        );
        assert_eq!(
            engine.publish_marks(p1),
            Ok(marks.clone()),
            "and left the marks"
        );
        engine
            .put_publish_marks(p1, &marks[1..])
            .expect("replace with one");
    }
    let reader = RocksEngine::open_read_only(&db).expect("read-only open");
    assert_eq!(
        reader.publish_marks(p1),
        Ok(marks[1..].to_vec()),
        "replaced whole"
    );
    drop(reader);
    assert!(
        dump(&db)
            .expect("dump")
            .iter()
            .any(|r| r.to_string() == "metadata publish_marks p=1 [1:3@1500]"),
        "dump names them"
    );

    {
        let raw = rocksdb::DB::open_cf(
            &rocksdb::Options::default(),
            &db,
            rdb_storage::keys::COLUMN_FAMILIES,
        )
        .expect("raw open");
        let meta = raw
            .cf_handle(rdb_storage::keys::CF_METADATA)
            .expect("metadata cf");
        let mut key = b"publish".to_vec();
        key.extend_from_slice(&1_u32.to_be_bytes());
        raw.put_cf(meta, key, [0; 23]).expect("put 23 bytes");
    }
    let engine = RocksEngine::open(&db).expect("open with an undecodable list");
    assert_eq!(engine.publish_marks(p1), Err(StorageFault::Corrupt));
    drop(engine);
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// A metadata key shorter than a lineage prefix that this build does not define is refused at
/// open by name, writable and read-only alike, never skipped (A-R164, lead ruling A-R184). The
/// format and instance markers and a publish-mark key stay accepted (the rows above reopen
/// copies holding them); a key that only starts like a publish-mark key is refused too.
#[retcd_test]
fn m10_w2_an_unknown_short_metadata_key_is_refused_at_open() {
    use rdb_storage::keys::CF_METADATA;

    let dir = data_dir("unknown-short-key");
    let db = dir.join("db");
    {
        let mut engine = RocksEngine::open(&db).expect("create");
        write(&mut engine, 2);
    }
    for key in [&b"zzz"[..], &b"publish\x00\x00\x00\x00\x01"[..]] {
        {
            let raw = rocksdb::DB::open_cf(
                &rocksdb::Options::default(),
                &db,
                rdb_storage::keys::COLUMN_FAMILIES,
            )
            .expect("raw open");
            let meta = raw.cf_handle(CF_METADATA).expect("metadata cf");
            raw.put_cf(meta, key, b"x").expect("put the unknown key");
        }
        let key_hex: String = key.iter().map(|b| format!("{b:02x}")).collect();
        for (row, refused) in [
            ("writable", RocksEngine::open(&db).expect_err("writable")),
            (
                "read-only",
                RocksEngine::open_read_only(&db).expect_err("read-only"),
            ),
        ] {
            match &refused {
                OpenError::CorruptRecord {
                    key_hex: found,
                    problem,
                    ..
                } => assert_eq!(
                    (found.as_str(), problem.as_str()),
                    (
                        key_hex.as_str(),
                        "a metadata key shorter than a lineage prefix that this build does not \
                         define"
                    ),
                    "{row}"
                ),
                other => panic!("{row}: refused as CorruptRecord, not {other:?}"),
            }
        }
        {
            let raw = rocksdb::DB::open_cf(
                &rocksdb::Options::default(),
                &db,
                rdb_storage::keys::COLUMN_FAMILIES,
            )
            .expect("raw open");
            let meta = raw.cf_handle(CF_METADATA).expect("metadata cf");
            raw.delete_cf(meta, key).expect("delete the unknown key");
        }
        let engine = RocksEngine::open(&db).expect("the copy opens once the key is gone");
        assert_eq!(engine.buffered_applied(PartitionId(1), Generation(1)).0, 2);
    }
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// critic-rollfwd A3 (lead k232): a lineage key in the metadata family whose namespace byte is
/// neither progress nor the engine's own is refused at open by name, writable and read-only
/// alike, with the next step. Here lineage (1, 1)'s `applied` record has its ns byte garbled, so
/// skipping it would open the copy as if nothing were applied there. Putting the byte back opens
/// the copy with its two batches. Cut: `restore_lineages` skips the key (`continue`).
/// Integration (~1 s): one engine, raw RocksDB edits.
#[retcd_test]
fn m10_w2_a_metadata_key_with_a_garbled_namespace_byte_is_refused_at_open() {
    use rdb_storage::keys::CF_METADATA;

    let dir = data_dir("garbled-ns");
    let db = dir.join("db");
    {
        let mut engine = RocksEngine::open(&db).expect("create");
        write(&mut engine, 2);
    }
    let applied_key = |ns: u8| {
        let mut key = 1_u32.to_be_bytes().to_vec();
        key.extend_from_slice(&1_u64.to_be_bytes());
        key.push(ns);
        key.extend_from_slice(b"applied");
        key
    };
    let raw_move = |from: &[u8], to: &[u8]| {
        let raw = rocksdb::DB::open_cf(
            &rocksdb::Options::default(),
            &db,
            rdb_storage::keys::COLUMN_FAMILIES,
        )
        .expect("raw open");
        let meta = raw.cf_handle(CF_METADATA).expect("metadata cf");
        let value = raw
            .get_cf(meta, from)
            .expect("get")
            .expect("the record to move");
        raw.put_cf(meta, to, &value).expect("put");
        raw.delete_cf(meta, from).expect("delete");
    };
    let engine_ns = applied_key(0xFF);
    for ns in [0x00_u8, 0x07, 0xFE] {
        let garbled = applied_key(ns);
        raw_move(&engine_ns, &garbled);
        let key_hex: String = garbled.iter().map(|b| format!("{b:02x}")).collect();
        let problem = format!(
            "a metadata key with namespace byte {ns:#04x}, which this build never writes there; \
             this copy cannot be opened: restore it from a backup of this directory, then open \
             it again"
        );
        for (row, refused) in [
            ("writable", RocksEngine::open(&db).map(|_| ())),
            ("read-only", RocksEngine::open_read_only(&db).map(|_| ())),
        ] {
            match &refused {
                Err(OpenError::CorruptRecord {
                    key_hex: found,
                    problem: said,
                    ..
                }) => assert_eq!(
                    (found.as_str(), said.as_str()),
                    (key_hex.as_str(), problem.as_str()),
                    "{row}, ns {ns:#04x}"
                ),
                other => panic!("{row}, ns {ns:#04x}: refused as CorruptRecord, not {other:?}"),
            }
        }
        raw_move(&garbled, &engine_ns);
        let engine = RocksEngine::open(&db).expect("the copy opens once the byte is back");
        assert_eq!(engine.buffered_applied(PartitionId(1), Generation(1)).0, 2);
    }
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// Review-s1 F-008: both synced metadata writes go through one helper. Each sync is seen by
/// RocksDB's own WAL-sync count, and only then does the engine count every write synced, which
/// is what lets the flush duty skip its next fsync (A-R214). An armed `SyncedPut` fails the
/// write: nothing is written, nothing counts as synced, and the copy stays unbound. Cut: drop
/// `set_sync(true)` from `put_metadata_synced`; the count stays put and the row is red.
/// Debug builds only, as the fault point and the count are. Unit, under a second.
#[cfg(debug_assertions)]
#[retcd_test]
fn m10_a_synced_metadata_write_syncs_the_wal_before_it_counts_as_synced() {
    use rdb_storage::InjectedFault;
    let dir = data_dir("synced-put");
    let lineage = Lineage {
        partition: PartitionId(1),
        generation: Generation(1),
        owner_epoch: OwnerEpoch(1),
    };
    let mut batches = canonical_history(lineage, ConfigVersion(1), 2)
        .expect("canonical history")
        .batches
        .into_iter();
    {
        let mut engine = RocksEngine::open(dir.join("db")).expect("open");
        engine
            .commit(batches.next().expect("batch 1"))
            .expect("commit");
        assert!(!engine.wal_synced_through_latest(), "a commit is unsynced");

        engine.inject_fault(InjectedFault::SyncedPut);
        let before = engine.wal_syncs();
        assert!(
            matches!(engine.bind_instance(X), Err(BindError::WriteFailed { .. })),
            "the armed put fails the bind"
        );
        assert_eq!(
            engine.armed_fault(),
            None,
            "the bind reached the fault point"
        );
        assert_eq!(engine.instance(), None, "the copy stays unbound");
        assert_eq!(engine.wal_syncs(), before, "nothing was synced");
        assert!(
            !engine.wal_synced_through_latest(),
            "nothing counts as synced"
        );

        engine.bind_instance(X).expect("bind X");
        assert_eq!(engine.wal_syncs(), before + 1, "the bind synced the WAL");
        assert!(
            engine.wal_synced_through_latest(),
            "the bind counts as synced"
        );

        engine
            .commit(batches.next().expect("batch 2"))
            .expect("commit");
        assert!(!engine.wal_synced_through_latest(), "a commit is unsynced");
        engine
            .put_publish_marks(PartitionId(1), &[])
            .expect("put the publish marks");
        assert_eq!(engine.wal_syncs(), before + 2, "the marks synced the WAL");
        assert!(
            engine.wal_synced_through_latest(),
            "the marks count as synced"
        );
    }
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}
