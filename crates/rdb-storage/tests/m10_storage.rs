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
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::ids::{
    ConfigVersion, Generation, OwnerEpoch, PartitionId, Seq, SnapshotHandle,
};
use rdb_core::contracts::storage::{Namespace, SnapshotRead, StorageFault};
use rdb_core::contracts::time::Tick;
use rdb_core::contracts::txn::{Durability, Outcome, TxnResult};
use rdb_core::replication::append::PROGRESS_KEY;
use rdb_core::transaction::dedup::{DedupIndex, SeedPending};
use rdb_sim::storage::history::{canonical_history, canonical_history_from, CanonicalHistory};
#[cfg(debug_assertions)]
use rdb_storage::LineageFault;
use rdb_storage::{
    dump, verify_lineage, BindError, OpenError, PublishMark, RocksEngine, RocksSnapshot,
    SnapshotEntry, StorageOptions, TrimFloor, DEFAULT_WRITE_BUFFER_BYTES,
    HISTORY_RECORD_BYTES_SEED, INSTANCE_LEN, WAL_BUDGETS,
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

/// One commit of `writes` into lineage (1, 1) at `seq`.
fn commit_dedup(
    engine: &mut RocksEngine,
    seq: u64,
    writes: Vec<rdb_core::contracts::storage::Write>,
) {
    engine
        .commit(rdb_core::contracts::storage::Batch {
            id: rdb_core::contracts::ids::BatchId(rdb_core::transaction::BATCH_TAG | seq),
            partition: PartitionId(1),
            generation: Generation(1),
            seq: Seq(seq),
            writes,
        })
        .expect("commit");
}

/// A `Dedup` put of `identity` under `affinity`, with `value`.
fn dedup_put(
    affinity: u64,
    request: u64,
    value: bytes::Bytes,
) -> rdb_core::contracts::storage::Write {
    use rdb_core::contracts::ids::{AffinityId, ClientId, RequestId, RequestIdentity, TenantId};
    rdb_core::contracts::storage::Write {
        ns: Namespace::Dedup,
        key: rdb_core::transaction::dedup::dedup_key(
            Generation(1),
            AffinityId(affinity),
            RequestIdentity {
                tenant: TenantId(1),
                client: ClientId(1),
                request: RequestId(request),
            },
        ),
        value: Some(value),
    }
}

/// review-s2 F-006, A-R411: an ack floor's rows parse in format 1 but mean nothing there, so a
/// format-1 build would open the copy and re-execute a resend the floor dropped. A format-1 copy
/// stays format 1 through plain dedup rows (a reserved-shape id under another affinity, an
/// ordinary id under the floor affinity), and moves to 2 in the commit that first carries a
/// floor record, or a `Dedup` delete (only a floor licenses one). A format-1 build accepts only
/// marker 1 (gate V12), so marker 2 is its refusal. Cuts: no marker in `commit` (both copies read
/// 1); drop the delete arm (the delete copy reads 1); drop the floor-record arm (the record copy
/// reads 1); drop the affinity check (the plain copy reads 2); drop the `Dedup` namespace check
/// (the plain copy's user delete reads 2). Unit, under a second.
#[retcd_test]
fn m10_s2_f006_an_ack_floor_moves_a_format_1_copy_to_2() {
    use rdb_core::contracts::digest::Digest;
    use rdb_core::transaction::dedup::{
        dedup_value, floor_record_id, floor_value, FLOOR_AFFINITY, OPEN_MASK,
    };
    use rdb_storage::keys::OLDEST_FORMAT_VERSION;
    let dir = data_dir("ack-floor-format");
    let plain = || dedup_value(Digest([1; 32]), Seq(9), OwnerEpoch(1));
    let record = floor_value(
        rdb_core::contracts::ids::RequestId(5),
        Seq(9),
        OwnerEpoch(1),
    );
    let s1_copy = |name: &str| {
        let db = dir.join(name);
        {
            let mut engine = RocksEngine::open(&db).expect("open");
            write(&mut engine, 2);
        }
        put_raw(&db, b"format", &OLDEST_FORMAT_VERSION.to_be_bytes());
        db
    };

    // Plain dedup rows, and a delete outside `Dedup`, leave an S1 copy format 1.
    let db = s1_copy("plain");
    {
        let mut engine = RocksEngine::open(&db).expect("an S1 copy opens");
        commit_dedup(
            &mut engine,
            10,
            vec![
                dedup_put(7, OPEN_MASK, plain()),
                dedup_put(FLOOR_AFFINITY.0, 3, plain()),
                rdb_core::contracts::storage::Write {
                    ns: Namespace::User,
                    key: bytes::Bytes::from_static(b"k"),
                    value: None,
                },
            ],
        );
    }
    assert_eq!(
        marker(&db),
        1,
        "plain dedup rows and a user delete keep format 1"
    );

    // A floor record moves it, in the same batch.
    let db = s1_copy("record");
    {
        let mut engine = RocksEngine::open(&db).expect("an S1 copy opens");
        commit_dedup(
            &mut engine,
            10,
            vec![dedup_put(FLOOR_AFFINITY.0, floor_record_id(0).0, record)],
        );
    }
    assert_eq!(marker(&db), 2, "a floor record moved the marker");

    // A floor's delete moves it too.
    let db = s1_copy("delete");
    {
        let mut engine = RocksEngine::open(&db).expect("an S1 copy opens");
        let mut delete = dedup_put(9, 3, plain());
        delete.value = None;
        commit_dedup(&mut engine, 10, vec![delete]);
    }
    assert_eq!(marker(&db), 2, "a dedup delete moved the marker");
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
                sync_stuck_millis: None,
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
            sync_stuck_millis: None,
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
            sync_stuck_millis: None,
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
            sync_stuck_millis: None,
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

// Issue #61 A1: the trim floor mark and the trim walk.

const P61: PartitionId = PartitionId(1);

/// The canonical records `start.0 + 1..=n` of `(P61, generation)`, chained from `start`, committed.
fn t61_commit(
    engine: &mut RocksEngine,
    generation: Generation,
    start: (Seq, Digest),
    n: u64,
) -> CanonicalHistory {
    let lineage = Lineage {
        partition: P61,
        generation,
        owner_epoch: OwnerEpoch(1),
    };
    let history =
        canonical_history_from(lineage, ConfigVersion(1), start, n).expect("canonical history");
    for batch in &history.batches {
        engine.commit(batch.clone()).expect("commit");
    }
    history
}

/// A floor at `seq` whose root is the lineage root.
const fn t61_floor(seq: u64) -> TrimFloor {
    TrimFloor {
        seq: Seq(seq),
        root_base: Seq::ZERO,
        root_digest: Digest::ROOT,
    }
}

/// Whether `level`'s **own** key range holds the History record at `seq` (a read of `level`
/// that falls through to a parent does not count).
fn t61_own(engine: &RocksEngine, level: Generation, seq: u64) -> bool {
    engine
        .get(P61, level, Namespace::History, &seq.to_be_bytes())
        .expect("read history")
        .is_some_and(|(from, _, _)| from == level)
}

/// The seqs in `range` that lineage `generation` reads a History record at, through its chain.
fn t61_seen(
    engine: &RocksEngine,
    generation: Generation,
    range: std::ops::RangeInclusive<u64>,
) -> Vec<u64> {
    range
        .filter(|seq| {
            engine
                .history_at(P61, generation, Seq(*seq))
                .expect("history_at")
                .is_some()
        })
        .collect()
}

/// The trim walk deletes below F on every linked level and keeps the record at F (B1): g1 holds
/// 1..=10, g2 is linked at 10 and holds 11..=20, g3 is linked at 20 and holds 21..=30. A trim of
/// g3 at 25 empties g1 and g2 and g3's 21..=24, keeps 25..=30, and verifies with the same head
/// digest. The trim is staged: it syncs nothing (M6). A second trim at F = applied keeps the head
/// record. Red if the delete ends at F + 1 (the record at F goes), or the walk stops at g3.
#[retcd_test]
fn t61_trim_keeps_the_record_at_f_and_deletes_below() {
    let (g1, g2, g3) = (Generation(1), Generation(2), Generation(3));
    let dir = data_dir("t61-trim-at-f");
    let mut engine = RocksEngine::open(dir.join("db")).expect("open");
    let h1 = t61_commit(&mut engine, g1, (Seq::ZERO, Digest::ROOT), 10);
    engine.inherit(P61, g1, g2, Seq(10)).expect("link g2");
    let h2 = t61_commit(&mut engine, g2, (Seq(10), h1.digest(10)), 20);
    engine.inherit(P61, g2, g3, Seq(20)).expect("link g3");
    let h3 = t61_commit(&mut engine, g3, (Seq(20), h2.digest(20)), 30);
    assert_eq!(
        t61_seen(&engine, g3, 1..=30).len(),
        30,
        "whole before the trim"
    );
    #[cfg(debug_assertions)]
    let syncs = engine.wal_syncs();

    assert_eq!(engine.stage_trim_history(P61, g3, t61_floor(25)), Ok(3));

    assert_eq!(engine.floor_seq(P61, g3), Seq(25));
    assert_eq!(engine.floor(P61, g3), Some((g3, t61_floor(25))));
    assert_eq!(t61_seen(&engine, g3, 1..=30), (25..=30).collect::<Vec<_>>());
    assert!(
        (1..=10).all(|seq| !t61_own(&engine, g1, seq)),
        "g1's History is gone"
    );
    assert!(
        (11..=20).all(|seq| !t61_own(&engine, g2, seq)),
        "g2's History is gone"
    );
    let verified = verify_lineage(&engine, P61, g3).expect("g3 verifies from its floor");
    assert_eq!(verified.head_digest, h3.digest(30));
    #[cfg(debug_assertions)]
    assert_eq!(
        engine.wal_syncs(),
        syncs,
        "the trim is staged, never synced (M6)"
    );

    assert_eq!(engine.stage_trim_history(P61, g3, t61_floor(30)), Ok(3));
    assert_eq!(
        t61_seen(&engine, g3, 1..=30),
        vec![30],
        "the head record stays (B1)"
    );
    let verified = verify_lineage(&engine, P61, g3).expect("g3 verifies at F = head");
    assert_eq!(verified.head_digest, h3.digest(30));
    drop(engine);
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// B2: a parent's records above the child's base are its own tail (#105 keeps them), so a child's
/// trim with F above the base keeps them. g1 holds 1..=12, g2 is a copied inherit at base 10 and
/// holds 11..=20 of its own. A trim of g2 at 15 deletes g1's 1..=10 only. Red if the parent level
/// is deleted over `[0, F)`.
#[retcd_test]
fn t61_trim_keeps_a_parent_tail_above_base() {
    let (g1, g2) = (Generation(1), Generation(2));
    let dir = data_dir("t61-parent-tail");
    let mut engine = RocksEngine::open(dir.join("db")).expect("open");
    let h1 = t61_commit(&mut engine, g1, (Seq::ZERO, Digest::ROOT), 12);
    assert!(matches!(
        engine.inherit(P61, g1, g2, Seq(10)),
        Ok(rdb_storage::Inherited::Copied { .. })
    ));
    t61_commit(&mut engine, g2, (Seq(10), h1.digest(10)), 20);

    assert_eq!(engine.stage_trim_history(P61, g2, t61_floor(15)), Ok(2));

    assert!(
        t61_own(&engine, g1, 11) && t61_own(&engine, g1, 12),
        "g1's tail stays"
    );
    assert!(
        (1..=10).all(|seq| !t61_own(&engine, g1, seq)),
        "g1 below the base goes"
    );
    assert_eq!(t61_seen(&engine, g2, 1..=20), (15..=20).collect::<Vec<_>>());
    verify_lineage(&engine, P61, g2).expect("g2 verifies from its floor");
    drop(engine);
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// B2: History falls through a `copied` link, so the trim walk follows it. A flatten makes g2's
/// link to g1 `copied`; a trim of g2 at 15 deletes g1's 1..=10. Red if the walk follows
/// `levels()`, which stops at a copied link (the rows would leak for good).
#[cfg(debug_assertions)]
#[retcd_test]
fn t61_trim_reaches_history_behind_a_copied_link() {
    let (g1, g2) = (Generation(1), Generation(2));
    let dir = data_dir("t61-copied-link");
    rdb_storage::flatten_at_depth(&dir, 1);
    let mut engine = RocksEngine::open(dir.join("db")).expect("open");
    let h1 = t61_commit(&mut engine, g1, (Seq::ZERO, Digest::ROOT), 10);
    assert!(matches!(
        engine.inherit(P61, g1, g2, Seq(10)),
        Ok(rdb_storage::Inherited::Copied { .. })
    ));
    assert!(
        engine.link(P61, g2).is_some_and(|link| link.copied),
        "a flatten"
    );
    t61_commit(&mut engine, g2, (Seq(10), h1.digest(10)), 20);

    assert_eq!(engine.stage_trim_history(P61, g2, t61_floor(15)), Ok(2));

    assert!(
        (1..=10).all(|seq| !t61_own(&engine, g1, seq)),
        "g1's History is gone"
    );
    assert_eq!(t61_seen(&engine, g2, 1..=20), (15..=20).collect::<Vec<_>>());
    verify_lineage(&engine, P61, g2).expect("g2 verifies from its floor");
    drop(engine);
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// Every refusal is named and writes nothing: a lineage mid full copy (`staging`), a sealed one,
/// a floor above applied, a floor below the current one, an unknown lineage. The same floor again
/// is accepted. Red if a guard is removed (the staging row trims at 0, which only that guard
/// refuses).
#[cfg(debug_assertions)]
#[retcd_test]
fn t61_trim_refused_while_staging_and_never_lowers() {
    use rdb_storage::{InjectedFault, TrimRefused};

    let (g1, g2, g3) = (Generation(1), Generation(2), Generation(3));
    let dir = data_dir("t61-refused");
    let db = dir.join("db");
    let mut engine = RocksEngine::open(&db).expect("open");
    t61_commit(&mut engine, g1, (Seq::ZERO, Digest::ROOT), 11);
    engine.inject_fault(InjectedFault::CopyBatch);
    engine
        .inherit(P61, g1, g2, Seq(10))
        .expect_err("the copy batch fails");
    assert!(engine.staging(P61, g2).is_some(), "g2 is left staging");
    assert_eq!(engine.stage_trim_history(P61, g1, t61_floor(8)), Ok(1));
    drop(engine);
    let before = dump(&db).expect("dump");
    let mut engine = RocksEngine::open(&db).expect("reopen");
    for (asked, (generation, floor)) in [
        (TrimRefused::Staging, (g2, 0)),
        (
            TrimRefused::AboveApplied {
                floor: Seq(12),
                applied: rdb_core::contracts::ids::AppliedSeq(11),
            },
            (g1, 12),
        ),
        (
            TrimRefused::BelowFloor {
                floor: Seq(6),
                current: Seq(8),
            },
            (g1, 6),
        ),
        (TrimRefused::UnknownLineage, (g3, 0)),
    ] {
        assert_eq!(
            engine.stage_trim_history(P61, generation, t61_floor(floor)),
            Err(asked),
            "{}",
            asked.reason()
        );
    }
    assert_eq!(
        engine.floor_seq(P61, g1),
        Seq(8),
        "the floor never moves down"
    );
    drop(engine);
    assert_eq!(
        dump(&db).expect("dump"),
        before,
        "a refusal wrote something"
    );

    // A sealed parent is trimmed only through its child's walk.
    let mut engine = RocksEngine::open(&db).expect("reopen");
    let p2 = PartitionId(2);
    write_partition(&mut engine, p2, 10);
    engine.inherit(p2, g1, g2, Seq(10)).expect("link");
    assert_eq!(
        engine.stage_trim_history(p2, g1, t61_floor(5)),
        Err(TrimRefused::Sealed { child: g2 })
    );
    assert_eq!(
        engine.stage_trim_history(P61, g1, t61_floor(8)),
        Ok(1),
        "the same floor again is accepted"
    );
    drop(engine);
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// Commit `n` canonical batches into lineage `(partition, 1)`.
fn write_partition(engine: &mut RocksEngine, partition: PartitionId, n: u64) {
    let lineage = Lineage {
        partition,
        generation: Generation(1),
        owner_epoch: OwnerEpoch(1),
    };
    let history = canonical_history(lineage, ConfigVersion(1), n).expect("canonical history");
    for batch in history.batches {
        engine.commit(batch).expect("commit");
    }
}

/// The mark is restored at open, writable and read-only, root and level included, and `dump`
/// names it. A linked child reads under its parent's mark and `floor` names the parent's level.
/// A mark above applied, or of the wrong length, is refused at open. Red if the open does not
/// restore the mark (the floor reads 0 after a reopen).
#[retcd_test]
fn t61_floor_survives_reopen() {
    let g1 = Generation(1);
    let dir = data_dir("t61-reopen");
    let db = dir.join("db");
    let mark = {
        let mut engine = RocksEngine::open(&db).expect("open");
        let h1 = t61_commit(&mut engine, g1, (Seq::ZERO, Digest::ROOT), 10);
        let mark = TrimFloor {
            seq: Seq(6),
            root_base: Seq(3),
            root_digest: h1.digest(3),
        };
        engine.stage_trim_history(P61, g1, mark).expect("trim");
        mark
    };
    {
        let mut engine = RocksEngine::open(&db).expect("reopen");
        assert_eq!(
            engine.floor(P61, g1),
            Some((g1, mark)),
            "the mark and its level"
        );
        assert_eq!(t61_seen(&engine, g1, 1..=10), (6..=10).collect::<Vec<_>>());
        verify_lineage(&engine, P61, g1).expect("verifies after a reopen");
        // A child reads under its parent's mark, and is told whose it is (critic m-e).
        engine
            .inherit(P61, g1, Generation(2), Seq(10))
            .expect("link");
        assert_eq!(engine.floor(P61, Generation(2)), Some((g1, mark)));
    }
    let reader = RocksEngine::open_read_only(&db).expect("read-only open");
    assert_eq!(reader.floor(P61, g1), Some((g1, mark)));
    drop(reader);
    assert!(
        dump(&db).expect("dump").iter().any(|r| r
            .to_string()
            .contains("engine.trim_floor=seq:6 root_base:3")),
        "dump names the mark"
    );

    let key = rdb_storage::keys::encode_key(P61, g1, Namespace::User, b"")[..12].to_vec();
    let mut mark_key = key.clone();
    mark_key.push(0xFF);
    mark_key.extend_from_slice(b"trim_floor");
    let mut above = vec![0; 48];
    // g1 is g2's parent here, so a mark one past its applied (11) is a cut level's (rev 5c).
    above[0..8].copy_from_slice(&12_u64.to_be_bytes());
    for (case, value, says) in [
        ("above applied", above, "trim floor 12 above applied 10"),
        (
            "47 bytes",
            vec![0; 47],
            "trim_floor record is 47 bytes, not 48",
        ),
    ] {
        put_raw(&db, &mark_key, &value);
        let refused = RocksEngine::open(&db).expect_err(case);
        assert!(
            matches!(&refused, OpenError::CorruptRecord { problem, .. } if problem.contains(says)),
            "{case}: {refused}"
        );
    }
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// `verify_lineage` walks a trimmed lineage from its floor: the record at F is checked on its
/// own and its digest starts the chain. Damaging the record at F fails the check at F. Red if
/// the walk starts at 1 (`Missing(1)`), or skips the check of the record at F.
#[cfg(debug_assertions)]
#[retcd_test]
fn t61_verify_lineage_from_floor() {
    let g1 = Generation(1);
    let dir = data_dir("t61-verify");
    let mut engine = RocksEngine::open(dir.join("db")).expect("open");
    let h1 = t61_commit(&mut engine, g1, (Seq::ZERO, Digest::ROOT), 10);
    engine
        .stage_trim_history(P61, g1, t61_floor(6))
        .expect("trim");
    let verified = verify_lineage(&engine, P61, g1).expect("verifies from the floor");
    assert_eq!(verified.head_digest, h1.digest(10));

    engine
        .damage_record(P61, g1, Namespace::History, &6_u64.to_be_bytes())
        .expect("damage the record at F");
    let fault = verify_lineage(&engine, P61, g1).expect_err("the record at F is checked");
    assert!(
        matches!(
            fault,
            LineageFault::Undecodable(Seq(6)) | LineageFault::DigestMismatch(Seq(6))
        ),
        "{fault}"
    );
    drop(engine);
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// A floor at `seq` whose root is record `root` of `h`: a mark whose root a reader can tell apart.
fn t61_rooted(seq: u64, h: &CanonicalHistory, root: u64) -> TrimFloor {
    TrimFloor {
        seq: Seq(seq),
        root_base: Seq(root),
        root_digest: h.digest(root),
    }
}

/// Design rev 5c: the trim marks every level it cuts, in its batch, at that level's cut
/// `min(F, base+1)`, with the child's root. g1 holds 1..=10, g2 is linked at 10 and holds
/// 11..=20, g3 is linked at 20 and holds 21..=30. A trim of g3 at 25 marks g3 at 25, g2 at 21 and
/// g1 at 11, each `head + 1` on a parent emptied, and each mark survives a reopen. Red if the trim
/// marks only its own level (`floor(g2)` is `None`).
#[retcd_test]
fn t61_trim_marks_each_cut_parent_level() {
    let (g1, g2, g3) = (Generation(1), Generation(2), Generation(3));
    let dir = data_dir("t61-marks-each-level");
    let db = dir.join("db");
    let mut engine = RocksEngine::open(&db).expect("open");
    let h1 = t61_commit(&mut engine, g1, (Seq::ZERO, Digest::ROOT), 10);
    engine.inherit(P61, g1, g2, Seq(10)).expect("link g2");
    let h2 = t61_commit(&mut engine, g2, (Seq(10), h1.digest(10)), 20);
    engine.inherit(P61, g2, g3, Seq(20)).expect("link g3");
    t61_commit(&mut engine, g3, (Seq(20), h2.digest(20)), 30);

    assert_eq!(
        engine.stage_trim_history(P61, g3, t61_rooted(25, &h1, 3)),
        Ok(3)
    );

    for round in ["staged", "reopened"] {
        for (level, seq) in [(g3, 25), (g2, 21), (g1, 11)] {
            assert_eq!(
                engine.floor(P61, level),
                Some((level, t61_rooted(seq, &h1, 3))),
                "{round}: the mark on g{}",
                level.0
            );
        }
        drop(engine);
        engine = RocksEngine::open(&db).expect("reopen");
    }
    drop(engine);
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// Design rev 5d: on one engine a cut level is always on the trimmed lineage's own chain, so a
/// sealed intermediate lineage reads under its **own** mark, and so does its parent. g1 holds
/// 1..=10; g2 is linked at 10 and holds 11..=22; g3 is a copied inherit of g2 at 20 (g2 keeps
/// its tail 21..=22) and holds 21..=30. A trim of g3 at 25 marks g2 at 21, inside its tail, and
/// g1 at 11, past its head. `floor(g2)` is g2's own 21 and g2 verifies from it, head 22;
/// `floor(g1)` is g1's own 11 and g1 verifies as empty, through a reopen. Red if the trim marks
/// only its own level (`floor(g2)` falls back to `None`).
#[retcd_test]
fn t61_floor_of_a_sealed_intermediate_lineage() {
    let (g1, g2, g3) = (Generation(1), Generation(2), Generation(3));
    let dir = data_dir("t61-sealed-intermediate");
    let db = dir.join("db");
    let mut engine = RocksEngine::open(&db).expect("open");
    let h1 = t61_commit(&mut engine, g1, (Seq::ZERO, Digest::ROOT), 10);
    engine.inherit(P61, g1, g2, Seq(10)).expect("link g2");
    let h2 = t61_commit(&mut engine, g2, (Seq(10), h1.digest(10)), 22);
    assert!(matches!(
        engine.inherit(P61, g2, g3, Seq(20)),
        Ok(rdb_storage::Inherited::Copied { .. })
    ));
    t61_commit(&mut engine, g3, (Seq(20), h2.digest(20)), 30);

    assert_eq!(
        engine.stage_trim_history(P61, g3, t61_rooted(25, &h1, 3)),
        Ok(3)
    );

    for round in ["staged", "reopened"] {
        assert_eq!(engine.sealed_by(P61, g2), Some(g3), "{round}: g2 is sealed");
        assert_eq!(
            engine.floor(P61, g2),
            Some((g2, t61_rooted(21, &h1, 3))),
            "{round}: g2 reads under its own mark"
        );
        assert_eq!(
            t61_seen(&engine, g2, 1..=22),
            vec![21, 22],
            "{round}: g2's tail stays"
        );
        let verified = verify_lineage(&engine, P61, g2).expect("g2 verifies from 21");
        assert_eq!(verified.head_digest, h2.digest(22), "{round}");
        assert_eq!(
            engine.floor(P61, g1),
            Some((g1, t61_rooted(11, &h1, 3))),
            "{round}: g1 reads under its own mark"
        );
        let verified = verify_lineage(&engine, P61, g1).expect("g1 verifies as empty");
        assert_eq!(verified.head_digest, h1.digest(10), "{round}");
        drop(engine);
        engine = RocksEngine::open(&db).expect("reopen");
    }
    drop(engine);
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// Design rev 5c, B1 per level: a parent with no records above the child's base is cut whole and
/// marked `head + 1`; it verifies as empty, its head digest named by its `Progress`, and opens
/// again. A parent with a tail keeps the record at its mark and verifies from it. Red if verify
/// reads the record at an emptied level's mark (`Missing(11)`), or open refuses a parent's mark
/// one past its applied.
#[retcd_test]
fn t61_a_parent_with_no_tail_verifies_empty_after_the_cut() {
    let (g1, g2) = (Generation(1), Generation(2));
    let dir = data_dir("t61-parent-empty");
    let db = dir.join("empty");
    let mut engine = RocksEngine::open(&db).expect("open");
    let h1 = t61_commit(&mut engine, g1, (Seq::ZERO, Digest::ROOT), 10);
    engine.inherit(P61, g1, g2, Seq(10)).expect("link g2");
    t61_commit(&mut engine, g2, (Seq(10), h1.digest(10)), 20);
    assert_eq!(engine.stage_trim_history(P61, g2, t61_floor(15)), Ok(2));
    for round in ["staged", "reopened"] {
        assert_eq!(engine.floor(P61, g1), Some((g1, t61_floor(11))), "{round}");
        assert_eq!(t61_seen(&engine, g1, 1..=10), Vec::<u64>::new(), "{round}");
        let verified = verify_lineage(&engine, P61, g1).expect("g1 verifies as empty");
        assert_eq!(verified.head_digest, h1.digest(10), "{round}");
        drop(engine);
        engine = RocksEngine::open(&db).expect("reopen with g1 marked head + 1");
    }
    drop(engine);

    let db = dir.join("tail");
    let mut engine = RocksEngine::open(&db).expect("open");
    let h1 = t61_commit(&mut engine, g1, (Seq::ZERO, Digest::ROOT), 12);
    assert!(matches!(
        engine.inherit(P61, g1, g2, Seq(10)),
        Ok(rdb_storage::Inherited::Copied { .. })
    ));
    t61_commit(&mut engine, g2, (Seq(10), h1.digest(10)), 20);
    assert_eq!(engine.stage_trim_history(P61, g2, t61_floor(15)), Ok(2));
    assert_eq!(engine.floor(P61, g1), Some((g1, t61_floor(11))));
    assert_eq!(
        t61_seen(&engine, g1, 1..=12),
        vec![11, 12],
        "the tail keeps 11"
    );
    let verified = verify_lineage(&engine, P61, g1).expect("g1 verifies from 11");
    assert_eq!(verified.head_digest, h1.digest(12));
    drop(engine);
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

// Issue #61 A7: the paged export and the snapshot install.

/// g1 holds 1..=10, g2 is linked at 10 and holds 11..=15: a two-level view.
fn t61_two_levels(engine: &mut RocksEngine) -> CanonicalHistory {
    let (g1, g2) = (Generation(1), Generation(2));
    let h1 = t61_commit(engine, g1, (Seq::ZERO, Digest::ROOT), 10);
    engine.inherit(P61, g1, g2, Seq(10)).expect("link g2");
    t61_commit(engine, g2, (Seq(10), h1.digest(10)), 15)
}

/// Every page of `view` at `limit`, cursor from the last entry of the page before.
fn t61_pages(view: &RocksSnapshot, limit: usize) -> Vec<Vec<SnapshotEntry>> {
    let mut pages = Vec::new();
    let mut after: Option<(Namespace, bytes::Bytes)> = None;
    loop {
        let page = view
            .entries_after(after.as_ref().map(|(ns, key)| (*ns, key.as_ref())), limit)
            .expect("page");
        if page.is_empty() {
            return pages;
        }
        let last = page.last().expect("a page");
        let next = Some((last.0, last.1.clone()));
        // A cursor that does not move would page forever: fail at once instead.
        assert_ne!(next, after, "the cursor did not advance past {next:?}");
        after = next;
        pages.push(page);
    }
}

/// The paged export covers the view exactly once, in order, without the head `Progress`, and no
/// page is longer than its limit (M3: the source never holds the whole partition). A cursor in
/// `History` is refused. Red if the cursor is inclusive (a key twice) or the head `Progress`
/// is sent.
#[retcd_test]
fn t61_entries_after_pages_cover_the_view_once() {
    let dir = data_dir("t61-pages");
    let mut engine = RocksEngine::open(dir.join("db")).expect("open");
    t61_two_levels(&mut engine);
    let view = engine
        .snapshot(P61, Generation(2), SnapshotHandle(1))
        .expect("view");
    let whole: Vec<SnapshotEntry> = view
        .entries()
        .expect("entries")
        .into_iter()
        .filter(|(ns, key, ..)| !(*ns == Namespace::Progress && key.as_ref() == PROGRESS_KEY))
        .collect();
    assert!(whole.len() > 10, "a view of both levels: {}", whole.len());
    for limit in [1, 3, 1000] {
        let pages = t61_pages(&view, limit);
        assert!(
            pages.iter().all(|page| page.len() <= limit),
            "limit {limit}"
        );
        assert_eq!(
            pages.concat(),
            whole,
            "limit {limit}: every entry once, in order"
        );
    }
    assert_eq!(
        view.entries_after(Some((Namespace::History, b"")), 3),
        Err(StorageFault::Corrupt),
        "History is not in the view"
    );
    drop((view, engine));
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// The lineage-keyed records of `partition` in the closed copy at `db`, as `dump` prints them.
fn t61_partition_records(db: &std::path::Path, partition: PartitionId) -> Vec<String> {
    dump(db)
        .expect("dump")
        .iter()
        .filter(|r| r.key.len() >= 13 && r.key[0..4] == partition.0.to_be_bytes())
        .map(ToString::to_string)
        .collect()
}

/// An install of a source view on a copy that holds an older lineage of the partition: the old
/// lineage goes, the installed lineage reads the source's state, holds the barrier record at its
/// floor S and verifies with the source's head digest, through a reopen. Another partition is
/// untouched. A newer generation held, or a barrier that is not the record at S, is refused and
/// writes nothing. Red if finish omits the barrier record (`Missing(15)`) or a chunk is lost.
#[retcd_test]
fn t61_install_round_trip_equals_source_view() {
    use rdb_storage::InstallError;

    let (g1, g2) = (Generation(1), Generation(2));
    let dir = data_dir("t61-install");
    let mut source = RocksEngine::open(dir.join("source")).expect("open source");
    let h2 = t61_two_levels(&mut source);
    let view = source.snapshot(P61, g2, SnapshotHandle(1)).expect("view");
    let at = Seq(15);
    let barrier = source
        .history_at(P61, g2, at)
        .expect("read")
        .expect("the barrier record")
        .0;
    let state = |entries: Vec<SnapshotEntry>| -> Vec<(Namespace, bytes::Bytes, bytes::Bytes, u64)> {
        entries
            .into_iter()
            .map(|(ns, key, value, version, _)| (ns, key, value, version))
            .collect()
    };

    let db = dir.join("target");
    let mut target = RocksEngine::open(&db).expect("open target");
    t61_commit(&mut target, g1, (Seq::ZERO, Digest::ROOT), 5);
    write_partition(&mut target, PartitionId(2), 3);
    target.install_begin(P61, g2, at).expect("begin");
    for page in t61_pages(&view, 4) {
        target.install_chunk(P61, g2, at, &page).expect("chunk");
    }
    let wrong = source
        .history_at(P61, g2, Seq(14))
        .expect("read")
        .expect("14")
        .0;
    assert_eq!(
        target.install_finish(P61, g2, at, &wrong, (Seq::ZERO, Digest::ROOT)),
        Err(InstallError::BadBarrier { at })
    );
    target
        .install_finish(P61, g2, at, &barrier, (Seq::ZERO, Digest::ROOT))
        .expect("finish");
    let mark = TrimFloor {
        seq: at,
        root_base: Seq::ZERO,
        root_digest: Digest::ROOT,
    };
    for round in ["installed", "reopened"] {
        assert_eq!(
            target.generations(P61),
            vec![g2],
            "{round}: the old lineage is gone"
        );
        assert_eq!(target.buffered_applied(P61, g2).0, 15, "{round}");
        assert_eq!(target.floor(P61, g2), Some((g2, mark)), "{round}");
        let installed = target.snapshot(P61, g2, SnapshotHandle(2)).expect("view");
        assert_eq!(
            state(installed.entries().expect("entries")),
            state(view.entries().expect("entries")),
            "{round}: the source's state, head Progress included"
        );
        let verified = verify_lineage(&target, P61, g2).expect("verifies from S");
        assert_eq!(verified.head_digest, h2.digest(15), "{round}");
        assert_eq!(t61_seen(&target, g2, 1..=15), vec![15], "{round}");
        assert_eq!(
            target.buffered_applied(PartitionId(2), g1).0,
            3,
            "{round}: p2"
        );
        drop((installed, target));
        target = RocksEngine::open(&db).expect("reopen target");
    }
    drop(target);
    let before = dump(&db).expect("dump");
    let mut target = RocksEngine::open(&db).expect("reopen target");
    assert_eq!(
        target.install_begin(P61, g1, Seq(20)),
        Err(InstallError::NewerGeneration { held: g2 })
    );
    drop(target);
    assert_eq!(
        dump(&db).expect("dump"),
        before,
        "a refused begin wrote something"
    );
    drop((view, source));
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// A crash mid install leaves the lineage `installing`: verify refuses it as `CopyIncomplete`, a
/// commit and a trim into it are refused, and the next open clears it, so the copy rejoins empty.
/// Two cuts: (a) right after `install_begin`, with no chunk, and (b) a failed chunk 2 after
/// chunk 1 landed. Either way neither the old lineage's levels nor its engine records survive
/// (`install_begin` wiped them in the same batch as the marker). Red if the open refuses an
/// `installing` lineage, or keeps its keys.
#[cfg(debug_assertions)]
#[retcd_test]
fn t61_install_crash_mid_chunks_clears_at_open() {
    use rdb_storage::{InjectedFault, LineageFault, TrimRefused};

    let (g1, g2) = (Generation(1), Generation(2));
    let dir = data_dir("t61-install-crash");
    let mut source = RocksEngine::open(dir.join("source")).expect("open source");
    let h2 = t61_two_levels(&mut source);
    let view = source.snapshot(P61, g2, SnapshotHandle(1)).expect("view");
    let pages = t61_pages(&view, 4);
    assert!(pages.len() >= 3, "enough pages to cut after the first");
    for (cut, chunks) in [("after-begin", 0), ("after-chunk-1", 1)] {
        let db = dir.join(cut);
        let mut target = RocksEngine::open(&db).expect("open target");
        t61_commit(&mut target, g1, (Seq::ZERO, Digest::ROOT), 5);
        target.install_begin(P61, g2, Seq(15)).expect("begin");
        for page in &pages[..chunks] {
            target.install_chunk(P61, g2, Seq(15), page).expect("chunk");
        }
        if chunks > 0 {
            target.inject_fault(InjectedFault::InstallChunk);
            target
                .install_chunk(P61, g2, Seq(15), &pages[chunks])
                .expect_err("the injected chunk fails");
        }
        assert_eq!(target.installing(P61, g2), Some((g2, Seq(15))), "{cut}");
        assert_eq!(
            verify_lineage(&target, P61, g2),
            Err(LineageFault::CopyIncomplete),
            "{cut}"
        );
        assert_eq!(
            target.stage_trim_history(P61, g2, t61_floor(0)),
            Err(TrimRefused::Installing),
            "{cut}"
        );
        assert_eq!(
            target.commit(h2.batch(11).clone()),
            Err(StorageFault::WriteFailed),
            "{cut}: a commit into an installing lineage"
        );
        drop(target);
        let target = RocksEngine::open(&db).expect("the open clears the install");
        assert_eq!(target.generations(P61), Vec::<Generation>::new(), "{cut}");
        drop(target);
        assert_eq!(
            t61_partition_records(&db, P61),
            Vec::<String>::new(),
            "{cut}: no level or engine record of the partition survives"
        );
    }
    drop((view, source));
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// `install_begin` is one batch (design rev 5b): the old levels and their engine records go in
/// the same write that marks the lineage installing. Cut before the batch (fault point
/// `install-begin`): every level and the keys stay, through a reopen, and nothing is
/// installing. Cut after it: only the `installing` record of the partition is on disk, and the
/// open clears it. Another partition is untouched. Red if the wipe misses the private prefix
/// (an old level's engine records survive) or is a write of its own.
#[cfg(debug_assertions)]
#[retcd_test]
fn t61_install_begin_is_one_batch() {
    use rdb_storage::{InjectedFault, InstallError};

    let (g1, g2, g3) = (Generation(1), Generation(2), Generation(3));
    let dir = data_dir("t61-install-begin");
    let db = dir.join("db");
    let mut engine = RocksEngine::open(&db).expect("open");
    t61_two_levels(&mut engine);
    write_partition(&mut engine, PartitionId(2), 3);
    drop(engine);
    let before = dump(&db).expect("dump");
    let mut engine = RocksEngine::open(&db).expect("reopen");

    engine.inject_fault(InjectedFault::InstallBegin);
    assert_eq!(
        engine.install_begin(P61, g3, Seq(15)),
        Err(InstallError::Storage(StorageFault::WriteFailed)),
        "the cut before the batch"
    );
    for round in ["cut before", "reopened"] {
        assert_eq!(engine.generations(P61), vec![g1, g2], "{round}");
        assert_eq!(engine.installing(P61, g3), None, "{round}");
        assert_eq!(
            t61_seen(&engine, g2, 1..=15),
            (1..=15).collect::<Vec<_>>(),
            "{round}"
        );
        drop(engine);
        assert_eq!(
            dump(&db).expect("dump"),
            before,
            "{round}: nothing was written"
        );
        engine = RocksEngine::open(&db).expect("reopen");
    }

    engine.install_begin(P61, g3, Seq(15)).expect("begin");
    assert_eq!(engine.generations(P61), vec![g3], "cut after");
    assert_eq!(engine.installing(P61, g3), Some((g3, Seq(15))), "cut after");
    drop(engine);
    assert_eq!(
        t61_partition_records(&db, P61),
        vec!["metadata p=1 g=3 engine.installing=generation:3 at:15".to_owned()],
        "cut after: no level or engine record of the old lineages survives"
    );
    let engine = RocksEngine::open(&db).expect("the open clears the install");
    assert_eq!(engine.generations(P61), Vec::<Generation>::new());
    assert_eq!(
        engine.buffered_applied(PartitionId(2), g1).0,
        3,
        "p2 is untouched"
    );
    drop(engine);
    assert_eq!(t61_partition_records(&db, P61), Vec::<String>::new());
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// An inherit from a lineage mid snapshot install is refused (`installing_inherit_refused`):
/// its view is half written, so neither a link nor a copy may read it. Refused at base 0 (an
/// empty view a link would accept) and at the barrier S (which would otherwise read as behind
/// the cutoff), and nothing is written. After `install_finish` the same inherit at S links. Red
/// if the refusal is dropped (base 0 links a half-installed parent).
#[retcd_test]
fn t61_inherit_from_installing_lineage_is_refused() {
    use rdb_storage::{InheritError, Inherited};

    let (g2, g3) = (Generation(2), Generation(3));
    let at = Seq(15);
    let dir = data_dir("t61-inherit-installing");
    let mut source = RocksEngine::open(dir.join("source")).expect("open source");
    t61_two_levels(&mut source);
    let view = source.snapshot(P61, g2, SnapshotHandle(1)).expect("view");
    let barrier = source.history_at(P61, g2, at).expect("read").expect("15").0;
    let pages = t61_pages(&view, 4);

    let db = dir.join("target");
    let mut target = RocksEngine::open(&db).expect("open target");
    target.install_begin(P61, g2, at).expect("begin");
    target.install_chunk(P61, g2, at, &pages[0]).expect("chunk");
    drop(target);
    let before = dump(&db).expect("dump");
    let mut target = RocksEngine::open(&db).expect("reopen");
    // The reopen cleared that install; start one again and stop mid chunks.
    target.install_begin(P61, g2, at).expect("begin");
    target.install_chunk(P61, g2, at, &pages[0]).expect("chunk");
    for base in [Seq::ZERO, at] {
        assert_eq!(
            target.inherit(P61, g2, g3, base),
            Err(InheritError::ParentInstalling { at }),
            "base {}",
            base.0
        );
    }
    assert_eq!(
        target.generations(P61),
        vec![g2],
        "no child lineage was made"
    );
    drop(target);
    assert_eq!(
        dump(&db).expect("dump"),
        before,
        "a refused inherit wrote something"
    );

    let mut target = RocksEngine::open(&db).expect("reopen");
    target.install_begin(P61, g2, at).expect("begin");
    for page in &pages {
        target.install_chunk(P61, g2, at, page).expect("chunk");
    }
    target
        .install_finish(P61, g2, at, &barrier, (Seq::ZERO, Digest::ROOT))
        .expect("finish");
    assert_eq!(target.inherit(P61, g2, g3, at), Ok(Inherited::Linked));
    drop((view, source, target));
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

// Issue #61 S3, lead ruling R25: `RocksEngine::history_bytes`, the estimate the retention cap
// binary-searches.

/// The exact bytes, key plus framed value, of the History records lineage `(P61, generation)`'s
/// own level holds at seqs `from..`, read from the database at `db` by [`dump`].
fn t61_exact_history_bytes(db: &std::path::Path, generation: Generation, from: u64) -> u64 {
    dump(db)
        .expect("dump")
        .iter()
        .filter(|r| r.cf == "history" && r.key.len() == 21)
        .filter(|r| r.key[0..4] == P61.0.to_be_bytes())
        .filter(|r| r.key[4..12] == generation.0.to_be_bytes())
        .filter(|r| u64::from_be_bytes(r.key[13..21].try_into().expect("seq")) >= from)
        .map(|r| (r.key.len() + r.value.len()) as u64)
        .sum()
}

/// Within ±25% of exact.
fn t61_within_quarter(estimate: u64, exact: u64) -> bool {
    estimate.abs_diff(exact) * 4 <= exact
}

/// g1 holds 1..=100 canonical records. For every `from` the estimate is within ±25% of the exact
/// bytes `dump` reads (canonical records are near uniform), and it never grows as `from` does.
/// A trim at 40 makes every `from <= 40` answer the same, the 61 records 40..=100 kept. A trim
/// at the head leaves the one record at F (B1). `from` above the head, an unknown lineage, and a
/// lineage with nothing committed answer 0. Red if the floor is ignored (the trim leaves the
/// answer unchanged) or the count runs past the head.
#[retcd_test]
fn t61_history_bytes_tracks_exact_bytes_and_falls_with_from_and_floor() {
    let g1 = Generation(1);
    let dir = data_dir("t61-history-bytes");
    let db = dir.join("db");
    let mut engine = RocksEngine::open(&db).expect("open");
    assert_eq!(
        engine.history_bytes(P61, g1, Seq(1)),
        0,
        "nothing committed"
    );
    t61_commit(&mut engine, g1, (Seq::ZERO, Digest::ROOT), 100);
    let mean = engine.history_record_bytes(P61);

    let mut last = u64::MAX;
    for from in 0..=101 {
        let estimate = engine.history_bytes(P61, g1, Seq(from));
        assert!(estimate <= last, "grew at from={from}: {estimate} > {last}");
        last = estimate;
        let exact = t61_exact_history_bytes(&db, g1, from);
        assert!(
            t61_within_quarter(estimate, exact),
            "from={from}: estimate {estimate}, exact {exact}"
        );
    }
    assert_eq!(engine.history_bytes(P61, g1, Seq(1)), 100 * mean);
    assert_eq!(engine.history_bytes(P61, g1, Seq(101)), 0, "above the head");
    assert_eq!(
        engine.history_bytes(P61, Generation(9), Seq(1)),
        0,
        "unknown"
    );

    let whole = engine.history_bytes(P61, g1, Seq(1));
    assert_eq!(engine.stage_trim_history(P61, g1, t61_floor(40)), Ok(1));
    let trimmed = engine.history_bytes(P61, g1, Seq(1));
    assert!(
        trimmed < whole,
        "the floor rose and the answer did not fall"
    );
    assert_eq!(trimmed, 61 * mean);
    assert_eq!(engine.history_bytes(P61, g1, Seq(40)), trimmed);
    assert_eq!(engine.history_bytes(P61, g1, Seq(41)), 60 * mean);
    let exact = t61_exact_history_bytes(&db, g1, 0);
    assert!(
        t61_within_quarter(trimmed, exact),
        "after the trim: estimate {trimmed}, exact {exact}"
    );

    assert_eq!(engine.stage_trim_history(P61, g1, t61_floor(100)), Ok(1));
    assert_eq!(
        engine.history_bytes(P61, g1, Seq(1)),
        mean,
        "the record at F"
    );
    assert_eq!(engine.history_bytes(P61, g1, Seq(101)), 0);
    drop(engine);
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// The walk counts through links and stops at the lineage: g1 holds 1..=10, g2 is linked at 10
/// and holds 11..=20, so g2 counts 20 records and g1 still 10. A trim of g2 at 15 drops g2 to 6
/// (15..=20) and g1, cut at its own level, to 0 below its new mark. Partition 2's records are
/// sixteen times larger and move partition 1's mean not at all. Red if the count stops at the
/// link (g2 answers 10), or one mean is shared by every partition.
#[retcd_test]
fn t61_history_bytes_walks_the_chain_and_keeps_partitions_apart() {
    let (g1, g2, p2) = (Generation(1), Generation(2), PartitionId(2));
    let dir = data_dir("t61-history-bytes-chain");
    let mut engine = RocksEngine::open(dir.join("db")).expect("open");
    let h1 = t61_commit(&mut engine, g1, (Seq::ZERO, Digest::ROOT), 10);
    engine.inherit(P61, g1, g2, Seq(10)).expect("link g2");
    t61_commit(&mut engine, g2, (Seq(10), h1.digest(10)), 20);
    let mean = engine.history_record_bytes(P61);
    assert_eq!(engine.history_bytes(P61, g2, Seq(1)), 20 * mean);
    assert_eq!(engine.history_bytes(P61, g1, Seq(1)), 10 * mean);

    let lineage = Lineage {
        partition: p2,
        generation: g1,
        owner_epoch: OwnerEpoch(1),
    };
    let big = bytes::Bytes::from(vec![0xab_u8; 16 * usize::try_from(mean).expect("mean")]);
    for mut batch in canonical_history(lineage, ConfigVersion(1), 5)
        .expect("canonical history")
        .batches
    {
        for w in &mut batch.writes {
            if w.ns == Namespace::History {
                w.value = Some(big.clone());
            }
        }
        engine.commit(batch).expect("commit");
    }
    assert_eq!(engine.history_record_bytes(P61), mean, "p2 moved p1's mean");
    assert!(engine.history_record_bytes(p2) > 15 * mean);

    assert_eq!(engine.stage_trim_history(P61, g2, t61_floor(15)), Ok(2));
    assert_eq!(engine.history_bytes(P61, g2, Seq(1)), 6 * mean);
    assert_eq!(
        engine.history_bytes(P61, g1, Seq(1)),
        0,
        "g1 is cut below its own mark"
    );
    drop(engine);
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// The learning resets at restart and the seed takes over. A fresh database answers with
/// [`HISTORY_RECORD_BYTES_SEED`]; after 200 records the mean is learnt; after a reopen nothing
/// is learnt, so the mean comes from the History column family's own estimates (RocksDB
/// flushes the replayed WAL to a table at open), which stay within ±25% of the learnt mean on
/// these near-uniform records. One commit after the reopen moves it back to the learnt mean.
#[retcd_test]
fn t61_history_bytes_seeds_from_column_family_estimates_after_reopen() {
    let g1 = Generation(1);
    let dir = data_dir("t61-history-bytes-seed");
    let db = dir.join("db");
    let mut engine = RocksEngine::open(&db).expect("open");
    assert_eq!(engine.history_record_bytes(P61), HISTORY_RECORD_BYTES_SEED);
    let h = t61_commit(&mut engine, g1, (Seq::ZERO, Digest::ROOT), 200);
    let learnt = engine.history_record_bytes(P61);
    assert_ne!(
        learnt, HISTORY_RECORD_BYTES_SEED,
        "the fixture must tell them apart"
    );
    drop(engine);

    let mut engine = RocksEngine::open(&db).expect("reopen");
    let seeded = engine.history_record_bytes(P61);
    assert_ne!(
        seeded, HISTORY_RECORD_BYTES_SEED,
        "the seed path, not the constant"
    );
    assert!(
        t61_within_quarter(seeded, learnt),
        "seeded {seeded}, learnt before the restart {learnt}"
    );
    assert!(t61_within_quarter(
        engine.history_bytes(P61, g1, Seq(1)),
        t61_exact_history_bytes(&db, g1, 0)
    ));
    t61_commit(&mut engine, g1, (Seq(200), h.digest(200)), 201);
    assert!(t61_within_quarter(engine.history_record_bytes(P61), learnt));
    drop(engine);
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}

/// Cost bound: the estimate reads no data. On a lineage of 2000 records, reopened so they sit
/// in a table and the mean is seeded, RocksDB's per-thread perf counters stay at 0 across a
/// `history_bytes` call, where a single `history_at` read moves them (the positive control). Red
/// if the estimate reads or iterates History.
#[retcd_test]
fn t61_history_bytes_reads_no_data() {
    use rocksdb::perf::{set_perf_stats, PerfContext, PerfMetric, PerfStatsLevel};
    let g1 = Generation(1);
    let dir = data_dir("t61-history-bytes-cost");
    let mut engine = RocksEngine::open(dir.join("db")).expect("open");
    t61_commit(&mut engine, g1, (Seq::ZERO, Digest::ROOT), 2000);
    drop(engine);
    // A reopen moves the records into a table and empties what was learnt, so the call below
    // takes the seed path, column-family properties, too.
    let engine = RocksEngine::open(dir.join("db")).expect("reopen");
    let metrics = [
        PerfMetric::UserKeyComparisonCount,
        PerfMetric::BlockReadCount,
        PerfMetric::GetFromMemtableCount,
        PerfMetric::SeekOnMemtableCount,
        PerfMetric::InternalKeySkippedCount,
    ];
    set_perf_stats(PerfStatsLevel::EnableCount);
    let mut perf = PerfContext::default();
    let read = |perf: &PerfContext| metrics.map(|m| perf.metric(m));

    perf.reset();
    let estimate = engine.history_bytes(P61, g1, Seq(1));
    assert!(estimate > 0);
    assert_eq!(read(&perf), [0; 5], "{}", perf.report(true));

    perf.reset();
    engine
        .history_at(P61, g1, Seq(1000))
        .expect("history_at")
        .expect("record 1000");
    assert_ne!(read(&perf), [0; 5], "the control must move a counter");
    set_perf_stats(PerfStatsLevel::Disable);
    drop(engine);
    std::fs::remove_dir_all(&dir).expect("remove the test directory");
}
