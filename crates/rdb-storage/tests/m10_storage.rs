//! M10 slice 1, stream W2: storage contracts for reopen (arch-m10 R3b, R3c `rdb-storage` row).
//!
//! 1. The instance marker: absent means unbound; a bind is durable across a reopen and visible
//!    to a read-only open; binding the same id again writes nothing; another id is refused and
//!    the copy keeps its first id; a marker of the wrong length is refused at open.
//!
//! Data goes under `RETCD_TEST_DATA_DIR` (set by `scripts/gate.sh`), else Cargo's per-target tmp
//! dir; never `%TEMP%`.

use std::path::PathBuf;

use config_log::retcd_test;
use rdb_core::contracts::authority::Lineage;
use rdb_core::contracts::ids::{ConfigVersion, Generation, OwnerEpoch, PartitionId};
use rdb_sim::storage::history::canonical_history;
use rdb_storage::{dump, BindError, OpenError, RocksEngine, INSTANCE_LEN};

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
