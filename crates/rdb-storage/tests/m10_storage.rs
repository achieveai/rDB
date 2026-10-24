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
use rdb_storage::{
    dump, BindError, OpenError, PublishMark, RocksEngine, StorageOptions,
    DEFAULT_WRITE_BUFFER_BYTES, INSTANCE_LEN, WAL_BUDGETS,
};

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

/// Put `value` at `key` in the `metadata` column family of the closed copy at `db`.
fn put_raw(db: &std::path::Path, key: &[u8], value: &[u8]) {
    let raw = rocksdb::DB::open_cf(
        &rocksdb::Options::default(),
        db,
        rdb_storage::keys::COLUMN_FAMILIES,
    )
    .expect("raw open");
    let meta = raw
        .cf_handle(rdb_storage::keys::CF_METADATA)
        .expect("metadata cf");
    raw.put_cf(meta, key, value).expect("raw put");
}

/// The format marker of the closed copy at `db`.
fn marker(db: &std::path::Path) -> u32 {
    let raw = rocksdb::DB::open_cf_for_read_only(
        &rocksdb::Options::default(),
        db,
        rdb_storage::keys::COLUMN_FAMILIES,
        false,
    )
    .expect("raw read-only open");
    let meta = raw
        .cf_handle(rdb_storage::keys::CF_METADATA)
        .expect("metadata cf");
    let found = raw
        .get_cf(meta, b"format")
        .expect("read")
        .expect("a marker");
    u32::from_be_bytes(found.as_slice().try_into().expect("4 bytes"))
}

/// Format 2 (M10 S2, arch-s2 "Restart heal and drain"): A1's `PersistEpochRevocation` lands
/// here. A format-1 copy (an S1 directory) still opens and stays format 1, so an S1 build can
/// still open it, until its first revocation, whose synced batch moves the marker to 2. The
/// revocations survive a reopen, read-only included, come back by partition then epoch for the
/// host's replay, and `dump` names them. A stored value that is not whole epochs is `Corrupt`,
/// never "none revoked", for the read and for the next revocation alike. Cuts: accept only
/// format 2 (the S1 copy is refused at open); write no marker with the revocation (it reads 1);
/// drop the `revoked` key from the keys an open accepts (the reopen is refused). Unit, under a
/// second.
#[retcd_test]
fn m10_s2_epoch_revocations_survive_a_reopen_and_move_a_format_1_copy_to_2() {
    use rdb_storage::keys::{FORMAT_VERSION, OLDEST_FORMAT_VERSION};
    let dir = data_dir("revocations");
    let db = dir.join("db");
    let (p1, p2) = (PartitionId(1), PartitionId(2));
    {
        let mut engine = RocksEngine::open(&db).expect("open");
        write(&mut engine, 2);
    }
    assert_eq!(marker(&db), FORMAT_VERSION, "a new copy is format 2");
    put_raw(&db, b"format", &OLDEST_FORMAT_VERSION.to_be_bytes());
    {
        let mut engine = RocksEngine::open(&db).expect("an S1 copy opens");
        assert_eq!(engine.epoch_revocations(), Ok(Vec::new()), "none held");
        write(&mut engine, 3);
    }
    assert_eq!(marker(&db), 1, "opens and writes leave an S1 copy format 1");
    {
        let mut engine = RocksEngine::open(&db).expect("open");
        for (partition, epoch) in [(p1, 5), (p2, 4), (p1, 3), (p1, 5)] {
            engine
                .persist_epoch_revocation(partition, OwnerEpoch(epoch))
                .expect("revoke");
        }
    }
    assert_eq!(marker(&db), 2, "the first revocation moved the marker");
    let held = vec![
        (p1, OwnerEpoch(3)),
        (p1, OwnerEpoch(5)),
        (p2, OwnerEpoch(4)),
    ];
    {
        let engine = RocksEngine::open(&db).expect("a reopen");
        assert_eq!(engine.epoch_revocations(), Ok(held.clone()), "durable");
    }
    let reader = RocksEngine::open_read_only(&db).expect("read-only open");
    assert_eq!(reader.epoch_revocations(), Ok(held));
    drop(reader);
    assert!(
        dump(&db)
            .expect("dump")
            .iter()
            .any(|r| r.to_string() == "metadata revoked_epochs p=1 [3 5]"),
        "dump names them"
    );

    let mut key = b"revoked".to_vec();
    key.extend_from_slice(&1_u32.to_be_bytes());
    put_raw(&db, &key, &[0; 7]);
    let mut engine = RocksEngine::open(&db).expect("open with an undecodable list");
    assert_eq!(engine.epoch_revocations(), Err(StorageFault::Corrupt));
    assert_eq!(
        engine.persist_epoch_revocation(p1, OwnerEpoch(6)),
        Err(StorageFault::Corrupt),
        "never written over"
    );
    drop(engine);
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// Found by the guard-deletion pass, not guessed: a lineage-scoped `metadata` key can start with
/// the bytes `revoked` (partition `0x7265766f` is "revo", a generation whose first bytes are
/// "ked"), and the revocation scan read it as an undecodable revocation, so a legal copy's
/// `epoch_revocations` came back `Corrupt`. The scan now skips a key the lineage layout decodes,
/// as open does. A lineage of the highest partition id sorts after every revocation and is
/// never reached. Cut: return `Corrupt` for a lineage-shaped key again (this row reads
/// `Err(Corrupt)`). Unit, under a second.
#[retcd_test]
fn m10_s2_a_lineage_whose_key_starts_like_a_revocation_is_not_one() {
    let dir = data_dir("revoked-lookalike");
    let db = dir.join("db");
    let lookalike = Lineage {
        partition: PartitionId(u32::from_be_bytes(*b"revo")),
        generation: Generation(u64::from_be_bytes(*b"ked\0\0\0\0\x01")),
        owner_epoch: OwnerEpoch(1),
    };
    let last = Lineage {
        partition: PartitionId(u32::MAX),
        generation: Generation(1),
        owner_epoch: OwnerEpoch(1),
    };
    let held = vec![(PartitionId(1), OwnerEpoch(2))];
    {
        let mut engine = RocksEngine::open(&db).expect("open");
        for lineage in [lookalike, last] {
            let history =
                canonical_history(lineage, ConfigVersion(1), 2).expect("canonical history");
            for batch in history.batches {
                engine.commit(batch).expect("commit");
            }
        }
        engine
            .persist_epoch_revocation(PartitionId(1), OwnerEpoch(2))
            .expect("revoke");
        assert_eq!(engine.epoch_revocations(), Ok(held.clone()));
    }
    let engine = RocksEngine::open(&db).expect("a reopen");
    assert_eq!(engine.epoch_revocations(), Ok(held), "after a reopen");
    drop(engine);
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// A revocation's write is synced and counts as synced (A-R214). An armed `SyncedPut` fails it:
/// nothing is written, the copy stays format 1, and nothing counts as synced. Debug builds only,
/// as the fault point and the count are. Unit, under a second.
#[cfg(debug_assertions)]
#[retcd_test]
fn m10_s2_an_epoch_revocation_is_one_synced_write_or_nothing() {
    use rdb_storage::InjectedFault;
    let dir = data_dir("revocation-synced");
    let db = dir.join("db");
    {
        let mut engine = RocksEngine::open(&db).expect("open");
        write(&mut engine, 1);
    }
    put_raw(&db, b"format", &1_u32.to_be_bytes());
    {
        let mut engine = RocksEngine::open(&db).expect("open");
        write(&mut engine, 2);
        engine.inject_fault(InjectedFault::SyncedPut);
        let before = engine.wal_syncs();
        assert_eq!(
            engine.persist_epoch_revocation(PartitionId(1), OwnerEpoch(2)),
            Err(StorageFault::WriteFailed)
        );
        assert_eq!(
            engine.epoch_revocations(),
            Ok(Vec::new()),
            "nothing written"
        );
        assert_eq!(engine.wal_syncs(), before, "nothing synced");
        assert!(!engine.wal_synced_through_latest());
    }
    assert_eq!(marker(&db), 1, "the failed write left the copy format 1");
    {
        let mut engine = RocksEngine::open(&db).expect("open");
        write(&mut engine, 3);
        let before = engine.wal_syncs();
        engine
            .persist_epoch_revocation(PartitionId(1), OwnerEpoch(2))
            .expect("revoke");
        assert_eq!(engine.wal_syncs(), before + 1, "one sync");
        assert!(engine.wal_synced_through_latest(), "it counts as synced");
    }
    assert_eq!(marker(&db), 2);
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// Ruling A-R230 (`rdb_dev admin damage-copy`): the dev hook flips one stored object of one
/// copy, and the next read of it fails its own check, naming it. Here the object is history
/// record 2: the lineage check fails at seq 2 and nowhere else, records 1 and 3 read as before,
/// and the damage survives a reopen (a synced write). A key holding no value is refused and
/// nothing is written. Cuts: flip no byte (the check passes, red); skip the stored-value check
/// (the missing key gets a record, red). Debug builds only, as the hook is. Unit, under a
/// second.
#[cfg(debug_assertions)]
#[retcd_test]
fn m10_s2_a_damaged_record_fails_its_check_on_one_copy_naming_it() {
    use rdb_storage::{verify_lineage, DamageRefused, LineageFault};
    let dir = data_dir("damage-record");
    let db = dir.join("db");
    let (p, g) = (PartitionId(1), Generation(1));
    let record = |engine: &RocksEngine, seq: u64| {
        engine
            .history_at(p, g, Seq(seq))
            .expect("read")
            .expect("stored")
            .0
    };
    {
        let mut engine = RocksEngine::open(&db).expect("open");
        write(&mut engine, 3);
        assert!(verify_lineage(&engine, p, g).is_ok(), "whole before");
        let before: Vec<_> = (1..=3).map(|seq| record(&engine, seq)).collect();
        engine
            .damage_record(p, g, Namespace::History, &2_u64.to_be_bytes())
            .expect("damage record 2");
        assert!(engine.wal_synced_through_latest(), "a synced write");
        assert_eq!(
            verify_lineage(&engine, p, g),
            Err(LineageFault::DigestMismatch(Seq(2))),
            "the check fails at the damaged record"
        );
        assert_eq!(record(&engine, 1), before[0]);
        assert_eq!(record(&engine, 3), before[2]);
        let (old, new) = (before[1].to_vec(), record(&engine, 2).to_vec());
        let differ: Vec<usize> = (0..old.len()).filter(|&i| old[i] != new[i]).collect();
        assert_eq!(differ, vec![old.len() - 1], "one byte, the last");

        let records = stored_history(&engine);
        assert!(matches!(
            engine.damage_record(p, g, Namespace::History, &9_u64.to_be_bytes()),
            Err(DamageRefused::NoValue { .. })
        ));
        assert_eq!(stored_history(&engine), records, "nothing written");
    }
    let engine = RocksEngine::open(&db).expect("reopen");
    assert_eq!(
        verify_lineage(&engine, p, g),
        Err(LineageFault::DigestMismatch(Seq(2))),
        "durable"
    );
    drop(engine);
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// Every History record of lineage (1, 1), for a nothing-written check.
#[cfg(debug_assertions)]
fn stored_history(engine: &RocksEngine) -> Vec<Option<bytes::Bytes>> {
    (1..=10)
        .map(|seq| {
            engine
                .history_at(PartitionId(1), Generation(1), Seq(seq))
                .expect("read")
                .map(|(record, _)| record)
        })
        .collect()
}

// ---------------------------------------------------------------------------------------------
// M10 S2 W9, ruling A-R248: the memtable budget is a storage option. Its default is RocksDB
// 0.23's own, 64 MiB, so an engine opened without it is the engine as it was.
// ---------------------------------------------------------------------------------------------

/// The smallest budget RocksDB keeps as given; it raises anything smaller to this.
const SMALL_BUFFER: usize = 64 << 10;

/// `write_buffer_size` of each column family in the newest `OPTIONS-*` file RocksDB wrote at
/// open, by column family name.
fn write_buffer_sizes(dir: &std::path::Path) -> std::collections::BTreeMap<String, usize> {
    let newest = std::fs::read_dir(dir)
        .expect("read the database directory")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("OPTIONS-") && !name.ends_with(".dbtmp"))
        })
        .max()
        .expect("RocksDB writes an OPTIONS file at open");
    let text = std::fs::read_to_string(&newest).expect("read the OPTIONS file");
    let mut sizes = std::collections::BTreeMap::new();
    let mut cf = None;
    for line in text.lines().map(str::trim) {
        if let Some(name) = line
            .strip_prefix("[CFOptions \"")
            .and_then(|rest| rest.strip_suffix("\"]"))
        {
            cf = Some(name.to_owned());
        } else if line.starts_with('[') {
            cf = None;
        } else if let (Some(name), Some(value)) = (&cf, line.strip_prefix("write_buffer_size=")) {
            sizes.insert(name.clone(), value.parse().expect("a byte count"));
        }
    }
    sizes
}

/// The budget is applied to each of our column families as given; an open without it keeps
/// 64 MiB. RocksDB's `default` column family holds nothing of ours and keeps its own 64 MiB.
/// RocksDB's own `OPTIONS` file is the witness, and each open logs the value it asked for
/// once, as `write_buffer_bytes_asked` on its `storage_open` line.
#[retcd_test]
fn m10_s2_the_write_buffer_option_reaches_every_column_family_and_is_logged() {
    let every = |bytes: usize| {
        let mut all: std::collections::BTreeMap<String, usize> = rdb_storage::keys::COLUMN_FAMILIES
            .iter()
            .map(|name| ((*name).to_owned(), bytes))
            .collect();
        all.insert("default".to_owned(), DEFAULT_WRITE_BUFFER_BYTES);
        all
    };
    let small = data_dir("write-buffer-small");
    drop(
        RocksEngine::open_with(
            &small,
            StorageOptions {
                write_buffer_bytes: Some(SMALL_BUFFER),
                memtable_budget_bytes: None,
                block_cache_bytes: None,
            },
        )
        .expect("open with a small budget"),
    );
    assert_eq!(write_buffer_sizes(&small), every(SMALL_BUFFER));

    let plain = data_dir("write-buffer-default");
    drop(RocksEngine::open(&plain).expect("open"));
    assert_eq!(DEFAULT_WRITE_BUFFER_BYTES, 64 << 20);
    assert_eq!(
        write_buffer_sizes(&plain),
        every(DEFAULT_WRITE_BUFFER_BYTES),
        "no option is RocksDB's own 64 MiB"
    );

    let logged: Vec<u64> = config_testkit::logs::lines_for_current_test(
        module_path!(),
        "m10_s2_the_write_buffer_option_reaches_every_column_family_and_is_logged",
    )
    .iter()
    .filter(|line| line["@m"] == "storage_open")
    .map(|line| line["write_buffer_bytes_asked"].as_u64().expect("a number"))
    .collect();
    assert_eq!(
        logged,
        vec![SMALL_BUFFER as u64, DEFAULT_WRITE_BUFFER_BYTES as u64]
    );
}

/// The same writes on two engines: under a 64 KiB budget the history column family's memtable
/// filled, was switched out and flushed, so the active one holds less than a budget's worth
/// and fewer entries than were written. Under the default every entry is still in memory.
#[retcd_test]
fn m10_s2_a_small_write_buffer_flushes_where_the_default_keeps_everything_in_memory() {
    let cf = rdb_storage::keys::CF_HISTORY;
    let active = |options: StorageOptions, name: &str| {
        let dir = data_dir(name);
        let mut engine = RocksEngine::open_with(&dir, options).expect("open");
        write(&mut engine, 600);
        assert!(
            engine.active_memtable("no-such-cf").is_err(),
            "an unknown name is an error"
        );
        engine.active_memtable(cf).expect("memtable properties")
    };
    let (held_bytes, held_entries) = active(StorageOptions::default(), "flush-default");
    assert!(
        held_bytes > 4 * SMALL_BUFFER as u64,
        "the writes are several small budgets' worth: {held_bytes} bytes"
    );
    let (small_bytes, small_entries) = active(
        StorageOptions {
            write_buffer_bytes: Some(SMALL_BUFFER),
            memtable_budget_bytes: None,
            block_cache_bytes: None,
        },
        "flush-small",
    );
    assert!(
        small_bytes <= 2 * SMALL_BUFFER as u64,
        "the small budget switched its memtable out: {small_bytes} bytes active"
    );
    assert!(
        small_entries < held_entries,
        "entries left memory: {small_entries} active of {held_entries}"
    );
}

// ---------------------------------------------------------------------------------------------
// M10 S2 W8, ruling A-R300: one memtable budget for the whole store, and a WAL bounded by it.
// ---------------------------------------------------------------------------------------------

/// The value of `name` in the `[DBOptions]` section of the newest `OPTIONS-*` file.
fn db_option(dir: &std::path::Path, name: &str) -> Option<u64> {
    let newest = std::fs::read_dir(dir)
        .expect("read the database directory")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("OPTIONS-") && !name.ends_with(".dbtmp"))
        })
        .max()
        .expect("RocksDB writes an OPTIONS file at open");
    let text = std::fs::read_to_string(&newest).expect("read the OPTIONS file");
    let prefix = format!("{name}=");
    text.lines()
        .map(str::trim)
        .skip_while(|line| *line != "[DBOptions]")
        .take_while(|line| !line.starts_with("[CFOptions"))
        .find_map(|line| line.strip_prefix(prefix.as_str()))
        .map(|value| value.parse().expect("a byte count"))
}

/// A store with a 1 MiB memtable budget takes 10,000 puts of 1 KiB (about 10 MiB), spread over
/// three column families so no single column family's budget can be what holds memory down.
/// After every put the store's memtable bytes are at most the budget plus one column family's
/// buffer (capped to the budget, so 1 MiB). Without the budget the memtables keep all of it.
/// The budget also sets `max_total_wal_size`.
#[retcd_test]
fn a_r300_memtable_memory_stays_within_the_store_budget() {
    use bytes::Bytes;
    use rdb_core::contracts::ids::BatchId;
    use rdb_core::contracts::storage::{Batch, Write};

    const BUDGET: usize = 1 << 20;
    let dir = data_dir("memtable-budget");
    let mut engine = RocksEngine::open_with(
        &dir,
        StorageOptions {
            write_buffer_bytes: None,
            memtable_budget_bytes: Some(BUDGET),
            block_cache_bytes: None,
        },
    )
    .expect("open with a 1 MiB budget");
    let value = Bytes::from(vec![0x5a; 1024]);
    let spread = [Namespace::User, Namespace::History, Namespace::Dedup];
    let mut peak = 0;
    for i in 0..10_000u64 {
        engine
            .commit(Batch {
                id: BatchId(i + 1),
                partition: PartitionId(1),
                generation: Generation(1),
                seq: Seq(i + 1),
                writes: vec![Write {
                    ns: spread[usize::try_from(i % 3).expect("small")],
                    key: Bytes::from(i.to_be_bytes().to_vec()),
                    value: Some(value.clone()),
                }],
            })
            .expect("commit");
        peak = peak.max(engine.memtable_bytes().expect("memtable bytes"));
    }
    let wal_bytes: u64 = std::fs::read_dir(&dir)
        .expect("read the database directory")
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "log"))
        .map(|entry| entry.metadata().expect("WAL metadata").len())
        .sum();
    tracing::info!(peak, wal_bytes, budget = BUDGET, "a_r300_memtable_peak");
    let limit = 2 * BUDGET as u64;
    assert!(
        peak <= limit,
        "memtables peaked at {peak} bytes, over the budget plus one buffer ({limit})"
    );
    assert_eq!(db_option(&dir, "db_write_buffer_size"), Some(BUDGET as u64));
    assert_eq!(
        db_option(&dir, "max_total_wal_size"),
        Some(WAL_BUDGETS * BUDGET as u64)
    );
    assert_eq!(
        write_buffer_sizes(&dir)[rdb_storage::keys::CF_DATA],
        BUDGET,
        "a column family's buffer is capped to the store's budget"
    );
}

/// A-R315: one block cache per store, sized by `block_cache_bytes`. A store with a 1 MiB
/// cache and a 1 MiB memtable budget takes 6,000 puts of 1 KiB (about 6 MiB, flushed to SSTs)
/// over three column families, then reads every one back, so each column family's reads
/// alone would fill the cache. After every 100 reads the store's block caches, each counted
/// once, use at most the configured capacity, and their capacity is that size. Before
/// A-R315 every column family and `default` had a 32 MiB cache of its own. A store opened
/// with the default options has one 64 MiB cache.
#[retcd_test]
fn a_r315_block_cache_memory_stays_within_the_store_capacity() {
    use bytes::Bytes;
    use rdb_core::contracts::ids::BatchId;
    use rdb_core::contracts::storage::{Batch, Write};
    use rdb_storage::DEFAULT_BLOCK_CACHE_BYTES;

    const CACHE: usize = 1 << 20;
    const PUTS: u64 = 6_000;
    let dir = data_dir("block-cache");
    let mut engine = RocksEngine::open_with(
        &dir,
        StorageOptions {
            write_buffer_bytes: None,
            memtable_budget_bytes: Some(1 << 20),
            block_cache_bytes: Some(CACHE),
        },
    )
    .expect("open with a 1 MiB block cache");
    let value = Bytes::from(vec![0x5a; 1024]);
    let spread = [Namespace::User, Namespace::History, Namespace::Dedup];
    let ns = |i: u64| spread[usize::try_from(i % 3).expect("small")];
    for i in 0..PUTS {
        engine
            .commit(Batch {
                id: BatchId(i + 1),
                partition: PartitionId(1),
                generation: Generation(1),
                seq: Seq(i + 1),
                writes: vec![Write {
                    ns: ns(i),
                    key: Bytes::from(i.to_be_bytes().to_vec()),
                    value: Some(value.clone()),
                }],
            })
            .expect("commit");
    }
    let mut peak = 0;
    let mut capacity = 0;
    for i in 0..PUTS {
        let read = engine
            .get(PartitionId(1), Generation(1), ns(i), &i.to_be_bytes())
            .expect("get");
        assert!(read.is_some(), "put {i} reads back");
        if i % 100 == 99 {
            let (cap, used) = engine.block_cache().expect("block cache");
            capacity = cap;
            peak = peak.max(used);
        }
    }
    tracing::info!(peak, capacity, asked = CACHE, "a_r315_block_cache_peak");
    assert!(
        peak > CACHE as u64 / 2,
        "the reads filled the cache: peak {peak} bytes of {CACHE}"
    );
    assert!(
        peak <= CACHE as u64,
        "block caches peaked at {peak} bytes, over the store's capacity ({CACHE})"
    );
    assert_eq!(
        capacity, CACHE as u64,
        "the store's block caches hold {CACHE} bytes in all"
    );
    drop(engine);
    let defaults = RocksEngine::open(data_dir("block-cache-default")).expect("open with defaults");
    assert_eq!(
        defaults.block_cache().expect("block cache").0,
        DEFAULT_BLOCK_CACHE_BYTES as u64,
        "the default is one 64 MiB cache"
    );
}
