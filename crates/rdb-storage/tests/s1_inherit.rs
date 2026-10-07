//! M8 S1 behavioural contracts and regressions for [`RocksEngine::inherit`] and the read-through
//! chain (ADR-rdb-0010).
//!
//! Each test's doc says which hand-walked scenario or found defect it protects. The scenarios
//! that need a delete batch live in the `rocks_scenario` example's tests, beside the delete
//! builder they share.
//!
//! Data goes under `RETCD_TEST_DATA_DIR` (set by `scripts/gate.sh`), else Cargo's per-target tmp
//! dir; never `%TEMP%`.

use std::path::{Path, PathBuf};
use std::process::Command;

use bytes::Bytes;
use config_log::retcd_test;
use rdb_core::contracts::authority::Lineage;
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::ids::{
    AffinityId, AppliedSeq, ConfigVersion, DurableSeq, Generation, OwnerEpoch, PartitionId, Seq,
    SnapshotHandle, TenantId,
};
use rdb_core::contracts::storage::{CapturedPrefix, Namespace, StorageFault, Write};
use rdb_core::contracts::trace::Version;
use rdb_core::contracts::txn::scoped_key;
use rdb_core::replication::append::PROGRESS_KEY;
use rdb_sim::storage::history::{canonical_history_from, CanonicalHistory};
use rdb_storage::keys::{cf_for, encode_key, CF_METADATA, COLUMN_FAMILIES};
use rdb_storage::{
    dump, verify_lineage, InheritError, Inherited, InjectedFault, LineageFault, Link, OpenError,
    RocksEngine,
};

const P: PartitionId = PartitionId(1);
const G0: Generation = Generation(0);
const G1: Generation = Generation(1);
const G2: Generation = Generation(2);
const G3: Generation = Generation(3);
const G5: Generation = Generation(5);

fn data_dir(name: &str) -> PathBuf {
    let root = std::env::var_os("RETCD_TEST_DATA_DIR")
        .map_or_else(|| PathBuf::from(env!("CARGO_TARGET_TMPDIR")), PathBuf::from);
    let dir = root.join(format!("rdb-storage-s1-{name}-{}", std::process::id()));
    if dir.exists() {
        std::fs::remove_dir_all(&dir).expect("remove a stale test directory");
    }
    std::fs::create_dir_all(&dir).expect("create the test directory");
    dir
}

/// The canonical records `start.0 + 1..=n` of `(partition, generation)`, chained from `start`.
fn chain(
    partition: PartitionId,
    generation: Generation,
    start: (Seq, Digest),
    n: u64,
) -> CanonicalHistory {
    let lineage = Lineage {
        partition,
        generation,
        owner_epoch: OwnerEpoch(1),
    };
    canonical_history_from(lineage, ConfigVersion(1), start, n).expect("canonical history")
}

/// Commit every batch of `history` above the lineage's applied seq, up to `last`.
fn commit_through(engine: &mut RocksEngine, history: &CanonicalHistory, last: u64) {
    for batch in &history.batches {
        let applied = engine.buffered_applied(batch.partition, batch.generation).0;
        if batch.seq.0 > applied && batch.seq.0 <= last {
            engine.commit(batch.clone()).expect("commit");
        }
    }
}

/// Commit the first `n` canonical batches of the root lineage `(P, generation)`.
fn write(engine: &mut RocksEngine, generation: Generation, n: u64) -> CanonicalHistory {
    let history = chain(P, generation, (Seq::ZERO, Digest::ROOT), n);
    commit_through(engine, &history, n);
    history
}

/// Commit `(P, generation)`'s records after `cutoff` up to `n`, chained from the parent's digest
/// at the cutoff, as a live T1 inherited at that cutoff would.
fn write_after(
    engine: &mut RocksEngine,
    generation: Generation,
    cutoff: (u64, &CanonicalHistory),
    n: u64,
) -> CanonicalHistory {
    let (at, parent) = cutoff;
    let history = chain(P, generation, (Seq(at), parent.digest(at)), n);
    commit_through(engine, &history, n);
    history
}

/// The canonical user key `k` as `(P, generation)` sees it: holder, version, and the value as
/// the u64 the canonical batch wrote. The canonical value of `k` at seq `s` is `s`.
fn user_k(
    engine: &RocksEngine,
    partition: PartitionId,
    generation: Generation,
) -> Option<(Generation, Version, u64)> {
    let key = scoped_key(TenantId(1), AffinityId(1), b"k");
    engine
        .get(partition, generation, Namespace::User, &key)
        .expect("get k")
        .map(|(from, version, value)| {
            let value = u64::from_be_bytes(value.as_ref().try_into().expect("8-byte value"));
            (from, version, value)
        })
}

/// Which generation holds the History record at `seq` as `(P, generation)` sees it.
fn history_from(engine: &RocksEngine, generation: Generation, seq: u64) -> Option<Generation> {
    engine
        .get(P, generation, Namespace::History, &seq.to_be_bytes())
        .expect("get history")
        .map(|(from, _, _)| from)
}

/// The Dedup key canonical batch `seq` wrote.
fn dedup_key_of(history: &CanonicalHistory, seq: u64) -> Bytes {
    history
        .batch(seq)
        .writes
        .iter()
        .find(|w| w.ns == Namespace::Dedup)
        .expect("a canonical batch writes one dedup row")
        .key
        .clone()
}

/// Every stored record, as `dump` prints it.
fn dumped(db: &Path) -> Vec<String> {
    dump(db)
        .expect("dump")
        .iter()
        .map(ToString::to_string)
        .collect()
}

/// Assert `result` is the lineage conflict whose reason prints as `reason`.
fn assert_conflict<T: std::fmt::Debug>(result: &Result<T, InheritError>, reason: &str) {
    assert!(
        matches!(result, Err(InheritError::LineageConflict { reason: r }) if r.as_str() == reason),
        "expected reason={reason}: {result:?}"
    );
}

/// g1 holds 10 records and g2 is linked to it at base 10 (the linked switch's setup).
fn linked_g2(engine: &mut RocksEngine) -> CanonicalHistory {
    let g1 = write(engine, G1, 10);
    assert_eq!(engine.inherit(P, G1, G2, Seq(10)), Ok(Inherited::Linked));
    g1
}

/// A linked switch is O(1): link, seal, `applied = base`, durable not inherited;
/// the child reads the parent's records through the chain, verify walks them, the same call
/// again is a no-op, and all of it survives a reopen.
#[retcd_test]
fn m8s_01_04_linked_switch_reads_through_and_a_rerun_is_a_noop() {
    let dir = data_dir("01-linked");
    let db = dir.join("db");
    let linked = Link {
        parent: G1,
        base: Seq(10),
        copied: false,
    };
    {
        let mut engine = RocksEngine::open(&db).expect("open");
        linked_g2(&mut engine);
        assert_eq!(engine.link(P, G2), Some(linked));
        assert_eq!(engine.sealed_by(P, G1), Some(G2));
        assert_eq!(engine.buffered_applied(P, G2), AppliedSeq(10));
        assert_eq!(
            engine.durable(P, G2),
            DurableSeq(0),
            "durable is not inherited"
        );
        assert_eq!(user_k(&engine, P, G2), Some((G1, 10, 10)));
        verify_lineage(&engine, P, G2).expect("g2 verifies through g1");

        let before = dumped(&db);
        assert_eq!(
            engine.inherit(P, G1, G2, Seq(10)),
            Ok(Inherited::AlreadyInherited)
        );
        assert_eq!(dumped(&db), before, "a no-op wrote something");
    }
    let engine = RocksEngine::open(&db).expect("reopen");
    assert_eq!(engine.link(P, G2), Some(linked));
    assert_eq!(engine.sealed_by(P, G1), Some(G2));
    assert_eq!(user_k(&engine, P, G2), Some((G1, 10, 10)));
    verify_lineage(&engine, P, G2).expect("g2 verifies after reopen");
}

/// The child's own writes chain on from the parent's digest at the cutoff and shadow
/// the parent; a late write to the sealed parent is refused and changes nothing.
#[retcd_test]
fn m8s_02_03_child_chains_on_and_a_sealed_parent_refuses_writes() {
    let dir = data_dir("02-child-writes");
    let mut engine = RocksEngine::open(dir.join("db")).expect("open");
    let g1 = linked_g2(&mut engine);
    write_after(&mut engine, G2, (10, &g1), 13);
    assert_eq!(engine.buffered_applied(P, G2), AppliedSeq(13));
    assert_eq!(user_k(&engine, P, G2), Some((G2, 13, 13)));
    verify_lineage(&engine, P, G2).expect("g2 verifies 1..=13 over two levels");

    let late = chain(P, G1, (Seq::ZERO, Digest::ROOT), 11)
        .batch(11)
        .clone();
    assert_eq!(
        engine.commit(late).expect_err("g1 is sealed"),
        StorageFault::WriteFailed
    );
    assert_eq!(engine.buffered_applied(P, G1), AppliedSeq(10));
    assert_eq!(user_k(&engine, P, G1), Some((G1, 10, 10)));
    assert_eq!(user_k(&engine, P, G2), Some((G2, 13, 13)));
}

/// Each inherit conflict is refused by its name and writes nothing, in memory or on disk.
#[retcd_test]
fn m8s_05_09_conflicts_are_refused_by_name_and_write_nothing() {
    let dir = data_dir("05-conflicts");
    let db = dir.join("db");
    let mut engine = RocksEngine::open(&db).expect("open");
    linked_g2(&mut engine);
    write(&mut engine, G5, 1);
    let marks = format!("{engine:?}");
    let records = dumped(&db);

    for (from, to, base, reason) in [
        (G1, G2, 9, "other_base"),
        (G0, G2, 10, "other_parent"),
        (G2, G2, 10, "child_not_newer"),
        (G3, G2, 10, "child_not_newer"),
        (G1, G5, 10, "child_has_history"),
        (G1, G3, 10, "parent_sealed_for_other"),
    ] {
        assert_conflict(&engine.inherit(P, from, to, Seq(base)), reason);
    }

    assert_eq!(format!("{engine:?}"), marks, "a refusal changed a lineage");
    assert_eq!(dumped(&db), records, "a refusal wrote a record");
}

/// A late write before the switch makes a full copy as of `base`. The child holds the
/// parent's state at `base`, not the late bytes; History `<= base` falls through, the late
/// History record does not; the parent keeps its quarantined late bytes.
#[retcd_test]
fn m8s_10_late_write_is_copied_as_of_base_and_history_falls_through() {
    let dir = data_dir("10-late-write");
    let mut engine = RocksEngine::open(dir.join("db")).expect("open");
    write(&mut engine, G1, 11);

    // k, the ten dedup rows of seq 1..=10, and Progress.
    assert_eq!(
        engine.inherit(P, G1, G2, Seq(10)),
        Ok(Inherited::Copied { keys: 12 })
    );

    assert_eq!(
        engine.link(P, G2),
        Some(Link {
            parent: G1,
            base: Seq(10),
            copied: true
        })
    );
    assert_eq!(user_k(&engine, P, G2), Some((G2, 10, 10)));
    assert_eq!(user_k(&engine, P, G1), Some((G1, 11, 11)));
    assert_eq!(history_from(&engine, G2, 5), Some(G1));
    assert_eq!(history_from(&engine, G2, 11), None);
    verify_lineage(&engine, P, G2).expect("g2 verifies");
    let seen = engine.snapshot(P, G2, SnapshotHandle(1)).expect("snapshot");
    for (ns, key, _, version, from) in seen.entries() {
        assert!(version <= 10, "{ns:?} {key:?} at v{version} is above base");
        assert!(
            ns == Namespace::History || from == G2,
            "{ns:?} {key:?} reads through a copied link from g{}",
            from.0
        );
    }
}

/// A parent held below `base` is refused `behind_cutoff` and nothing is written; once it
/// catches up, the same call links.
#[retcd_test]
fn m8s_12_behind_cutoff_writes_nothing_until_the_parent_catches_up() {
    let dir = data_dir("12-behind");
    let mut engine = RocksEngine::open(dir.join("db")).expect("open");
    let g1 = chain(P, G1, (Seq::ZERO, Digest::ROOT), 10);
    commit_through(&mut engine, &g1, 8);

    assert_eq!(
        engine.inherit(P, G1, G2, Seq(10)),
        Err(InheritError::BehindCutoff {
            held: AppliedSeq(8),
            base: Seq(10)
        })
    );
    assert!(!engine.lineages().contains(&(P, G2)), "the refusal made g2");
    assert_eq!(engine.sealed_by(P, G1), None);

    commit_through(&mut engine, &g1, 10);
    assert_eq!(engine.inherit(P, G1, G2, Seq(10)), Ok(Inherited::Linked));
}

/// A full copy that fails after its first key batch leaves g2 staging:
/// verify names it, writes to it are refused, a re-run with other arguments is refused and
/// leaves it alone, and the same-args re-run clears it and converges.
#[retcd_test]
fn m8s_15_16_18_34_a_failed_copy_is_staged_refused_and_a_rerun_converges() {
    let dir = data_dir("15-staged");
    let mut engine = RocksEngine::open(dir.join("db")).expect("open");
    // 1100 records so the copy (k, 1099 dedup rows, Progress) takes two 1024-key batches and
    // the injected fault on the last one leaves the first landed.
    let g1 = write(&mut engine, G1, 1100);
    engine.inject_fault(InjectedFault::CopyBatch);
    assert_eq!(
        engine.inherit(P, G1, G2, Seq(1099)),
        Err(InheritError::Storage(StorageFault::WriteFailed))
    );
    let staged = Some((G1, Seq(1099)));
    assert_eq!(engine.staging(P, G2), staged);
    assert_eq!(
        verify_lineage(&engine, P, G2),
        Err(LineageFault::CopyIncomplete)
    );

    let g2_write = chain(P, G2, (Seq(1099), g1.digest(1099)), 1100)
        .batch(1100)
        .clone();
    assert_eq!(
        engine.commit(g2_write).expect_err("g2 is staging"),
        StorageFault::WriteFailed
    );
    assert_conflict(&engine.inherit(P, G1, G2, Seq(1098)), "staging_other");
    assert_eq!(
        engine.staging(P, G2),
        staged,
        "staging_other touched the staging"
    );

    assert_eq!(
        engine.inherit(P, G1, G2, Seq(1099)),
        Ok(Inherited::Copied { keys: 1101 })
    );
    assert_eq!(engine.staging(P, G2), None);
    verify_lineage(&engine, P, G2).expect("g2 verifies after the re-run");
    assert_eq!(user_k(&engine, P, G2), Some((G2, 1099, 1099)));
}

/// A failed linked switch batch writes nothing and a re-run links; an inject point
/// a linked switch never reaches stays armed and does not fail it.
#[retcd_test]
fn m8s_19_20_a_failed_linked_switch_writes_nothing_and_an_unreached_point_stays_armed() {
    let dir = data_dir("19-switch");
    let mut engine = RocksEngine::open(dir.join("db")).expect("open");
    write(&mut engine, G1, 10);
    engine.inject_fault(InjectedFault::Switch);
    assert_eq!(
        engine.inherit(P, G1, G2, Seq(10)),
        Err(InheritError::Storage(StorageFault::WriteFailed))
    );
    assert!(
        !engine.lineages().contains(&(P, G2)),
        "the failed switch made g2"
    );
    assert_eq!(engine.sealed_by(P, G1), None, "the failed switch sealed g1");

    engine.inject_fault(InjectedFault::CopyBatch);
    assert_eq!(engine.inherit(P, G1, G2, Seq(10)), Ok(Inherited::Linked));
    assert_eq!(engine.armed_fault(), Some(InjectedFault::CopyBatch));
}

/// A batch with a `Meta` write is refused whole at commit: format 1 has no History
/// after-image for Meta, so a full copy could not rebuild it.
#[retcd_test]
fn m8s_25a_a_meta_write_is_refused_at_commit() {
    let dir = data_dir("25a-meta");
    let mut engine = RocksEngine::open(dir.join("db")).expect("open");
    let mut batch = chain(P, G1, (Seq::ZERO, Digest::ROOT), 1).batch(1).clone();
    batch.writes.push(Write {
        ns: Namespace::Meta,
        key: Bytes::from_static(b"m"),
        value: Some(Bytes::from_static(b"v")),
    });

    assert_eq!(
        engine.commit(batch).expect_err("meta is refused"),
        StorageFault::WriteFailed
    );
    assert_eq!(engine.buffered_applied(P, G1), AppliedSeq(0));
    assert_eq!(
        user_k(&engine, P, G1),
        None,
        "part of the batch was applied"
    );
    assert_eq!(
        engine.get(P, G1, Namespace::Meta, b"m").expect("get meta"),
        None
    );
}

/// Two linked levels: a row only g1 holds reads `from=1` in g3, and verify walks 1..=12
/// over three levels.
#[retcd_test]
fn m8s_26_two_linked_levels_read_through_to_the_root() {
    let dir = data_dir("26-two-linked");
    let mut engine = RocksEngine::open(dir.join("db")).expect("open");
    let g1 = linked_g2(&mut engine);
    write_after(&mut engine, G2, (10, &g1), 12);
    assert_eq!(engine.inherit(P, G2, G3, Seq(12)), Ok(Inherited::Linked));

    let dedup5 = dedup_key_of(&g1, 5);
    let row = engine.get(P, G3, Namespace::Dedup, &dedup5).expect("get");
    assert_eq!(row.map(|(from, version, _)| (from, version)), Some((G1, 5)));
    assert_eq!(user_k(&engine, P, G3), Some((G2, 12, 12)));
    verify_lineage(&engine, P, G3).expect("g3 verifies over three levels");
}

/// Syncing a child raises each ancestor's durable to min(through, base, its applied), as
/// `MemoryEngine::sync_ancestors` does.
#[retcd_test]
fn m8s_28_syncing_a_child_raises_its_ancestors_durable() {
    let dir = data_dir("28-ancestor-durable");
    let mut engine = RocksEngine::open(dir.join("db")).expect("open");
    let g1 = linked_g2(&mut engine);
    write_after(&mut engine, G2, (10, &g1), 13);

    engine
        .sync_wal_through(vec![CapturedPrefix {
            partition: P,
            generation: G2,
            through: AppliedSeq(13),
        }])
        .expect("sync g2");

    assert_eq!(engine.durable(P, G2), DurableSeq(13));
    assert_eq!(engine.durable(P, G1), DurableSeq(10));
}

/// The raw key of an engine-private record of `(P, generation)`: `P | generation | 0xFF | name`.
/// Spelled out here on purpose: these tests pin the layout they corrupt.
fn private_key(generation: u64, name: &[u8]) -> Vec<u8> {
    let mut key = P.0.to_be_bytes().to_vec();
    key.extend_from_slice(&generation.to_be_bytes());
    key.push(0xFF);
    key.extend_from_slice(name);
    key
}

/// A stored [`Link`]: `parent u64 BE | base u64 BE | flags u8`.
fn link_bytes(parent: u64, base: u64, flags: u8) -> Vec<u8> {
    let mut value = parent.to_be_bytes().to_vec();
    value.extend_from_slice(&base.to_be_bytes());
    value.push(flags);
    value
}

/// Put one malformed record into a valid linked directory (the linked switch's state), then
/// open must refuse it as a corrupt record, never repaired, with a message that `says` what is
/// wrong (a tester's paper cut: it used to call every case "undecodable").
fn assert_refused_at_open(case: &str, key: Vec<u8>, value: Vec<u8>, says: &str) {
    let dir = data_dir(&format!("30-{case}"));
    let db = dir.join("db");
    {
        let mut engine = RocksEngine::open(&db).expect("open");
        linked_g2(&mut engine);
    }
    {
        let raw = rocksdb::DB::open_cf(&rocksdb::Options::default(), &db, COLUMN_FAMILIES)
            .expect("raw open");
        let cf = raw.cf_handle(CF_METADATA).expect("metadata cf");
        raw.put_cf(cf, key, value).expect("raw put");
    }
    let refused = RocksEngine::open(&db).expect_err(case);
    assert!(
        matches!(refused, OpenError::CorruptRecord { .. }),
        "{case}: {refused:?}"
    );
    let message = refused.to_string();
    assert!(
        message.contains(says),
        "{case}: {message:?} does not say {says:?}"
    );
}

/// Every file in a RocksDB directory, by name, with its bytes.
fn files(db: &Path) -> std::collections::BTreeMap<String, Vec<u8>> {
    std::fs::read_dir(db)
        .expect("list the db")
        .map(|entry| {
            let entry = entry.expect("dir entry");
            let bytes = std::fs::read(entry.path()).expect("read a db file");
            (entry.file_name().to_string_lossy().into_owned(), bytes)
        })
        .collect()
}

/// A refused open changes no file. Each directory holds a record the open
/// refuses (format 0, a corrupt parent record, durable above applied) in an unflushed WAL, the
/// state a writable open would replay, flush and rotate before reading the marker.
#[retcd_test]
fn m8s_29_s1_p1_a_refused_open_changes_no_file() {
    use rdb_storage::keys::FORMAT_VERSION;

    let dir = data_dir("29-p1");
    let format = FORMAT_VERSION.to_be_bytes().to_vec();
    let one = 1u64.to_be_bytes().to_vec();
    type Records = Vec<(Vec<u8>, Vec<u8>)>;
    let cases: [(&str, Records); 3] = [
        (
            "format 0",
            vec![
                (b"format".to_vec(), 0u32.to_be_bytes().to_vec()),
                (private_key(1, b"applied"), one.clone()),
            ],
        ),
        (
            "parent record of 16 bytes",
            vec![
                (b"format".to_vec(), format.clone()),
                (private_key(2, b"parent"), vec![0; 16]),
            ],
        ),
        (
            "durable 2 above applied 1",
            vec![
                (b"format".to_vec(), format),
                (private_key(1, b"applied"), one),
                (private_key(1, b"durable"), 2u64.to_be_bytes().to_vec()),
            ],
        ),
    ];
    for (i, (case, records)) in cases.into_iter().enumerate() {
        let db = dir.join(format!("db-{i}"));
        {
            let mut opts = rocksdb::Options::default();
            opts.create_if_missing(true);
            opts.create_missing_column_families(true);
            let raw = rocksdb::DB::open_cf(&opts, &db, COLUMN_FAMILIES).expect("raw create");
            let meta = raw.cf_handle(CF_METADATA).expect("metadata cf");
            for (key, value) in records {
                raw.put_cf(meta, key, value).expect("raw put");
            }
        }
        let before = files(&db);
        RocksEngine::open(&db).expect_err(case);
        RocksEngine::open_existing(&db).expect_err(case);
        assert_eq!(
            files(&db).keys().collect::<Vec<_>>(),
            before.keys().collect::<Vec<_>>(),
            "{case}: a refused open added or removed a file"
        );
        for (name, bytes) in files(&db) {
            assert!(
                before[&name] == bytes,
                "{case}: a refused open rewrote {name}"
            );
        }
    }
}

/// A `parent` record that is not 17 bytes names the length it found.
#[retcd_test]
fn m8s_30_open_refuses_a_parent_record_of_16_bytes() {
    assert_refused_at_open(
        "parent-len",
        private_key(2, b"parent"),
        vec![0; 16],
        "parent record is 16 bytes, not 17",
    );
}

/// A `parent` record with a flag bit other than `copied` names the flag byte.
#[retcd_test]
fn m8s_30_open_refuses_a_parent_record_with_an_unknown_flag() {
    assert_refused_at_open(
        "parent-flag",
        private_key(2, b"parent"),
        link_bytes(1, 10, 2),
        "parent record has unknown flag bits in 0x02",
    );
}

/// A lineage that names itself as parent is refused (a chain could cycle).
#[retcd_test]
fn m8s_30_open_refuses_a_parent_not_older_than_its_child() {
    assert_refused_at_open(
        "parent-order",
        private_key(2, b"parent"),
        link_bytes(2, 10, 0),
        "parent record names generation 2, which is not older than 2",
    );
}

/// A parent newer than its child is refused.
#[retcd_test]
fn m8s_30_open_refuses_a_parent_newer_than_its_child() {
    assert_refused_at_open(
        "parent-newer",
        private_key(2, b"parent"),
        link_bytes(3, 10, 0),
        "parent record names generation 3, which is not older than 2",
    );
}

/// An inherited lineage whose applied mark is below its base names both numbers.
#[retcd_test]
fn m8s_30_open_refuses_applied_below_base() {
    let applied = 9u64.to_be_bytes().to_vec();
    assert_refused_at_open(
        "applied-below-base",
        private_key(2, b"applied"),
        applied,
        "applied 9 < base 10",
    );
}

/// A `sealed` record that is not 8 bytes names the length it found.
#[retcd_test]
fn m8s_30_open_refuses_a_sealed_record_of_7_bytes() {
    assert_refused_at_open(
        "sealed-len",
        private_key(1, b"sealed"),
        vec![0; 7],
        "sealed record is 7 bytes, not 8",
    );
}

/// A lineage sealed by itself is refused.
#[retcd_test]
fn m8s_30_open_refuses_a_self_seal() {
    assert_refused_at_open(
        "sealed-self",
        private_key(1, b"sealed"),
        1u64.to_be_bytes().to_vec(),
        "sealed record names child 1, which is not newer than 1",
    );
}

/// A `copying` record that is not 16 bytes names the length it found.
#[retcd_test]
fn m8s_30_open_refuses_a_copying_record_of_15_bytes() {
    assert_refused_at_open(
        "copying-len",
        private_key(3, b"copying"),
        vec![0; 15],
        "copying record is 15 bytes, not 16",
    );
}

/// A `copying` record whose parent is not older than the staging generation is refused.
#[retcd_test]
fn m8s_30_open_refuses_a_copying_record_from_a_newer_parent() {
    let mut value = 3u64.to_be_bytes().to_vec();
    value.extend_from_slice(&10u64.to_be_bytes());
    assert_refused_at_open(
        "copying-order",
        private_key(3, b"copying"),
        value,
        "copying record names parent 3, which is not older than 3",
    );
}

/// Any stored `Meta` key, however well framed, is refused; the message says it is a Meta key,
/// not an undecodable engine record.
#[retcd_test]
fn m8s_25b_open_refuses_a_stored_meta_key() {
    let mut value = vec![0];
    value.extend_from_slice(&1u64.to_be_bytes());
    let key = encode_key(P, G1, Namespace::Meta, b"m");
    assert_refused_at_open(
        "meta-key",
        key,
        value,
        "a Meta key, which format 1 never stores",
    );
}

/// Inheriting in one partition leaves another partition's lineages alone.
#[retcd_test]
fn m8s_31_inherit_leaves_other_partitions_alone() {
    let dir = data_dir("31-partitions");
    let mut engine = RocksEngine::open(dir.join("db")).expect("open");
    let p2 = PartitionId(2);
    let other = chain(p2, G1, (Seq::ZERO, Digest::ROOT), 5);
    commit_through(&mut engine, &other, 5);
    let before = (
        engine.buffered_applied(p2, G1),
        engine.durable(p2, G1),
        user_k(&engine, p2, G1),
    );

    linked_g2(&mut engine);

    let after = (
        engine.buffered_applied(p2, G1),
        engine.durable(p2, G1),
        user_k(&engine, p2, G1),
    );
    assert_eq!(after, before);
    assert_eq!(engine.sealed_by(p2, G1), None);
    assert!(!engine.lineages().contains(&(p2, G2)));
    verify_lineage(&engine, p2, G1).expect("p2 verifies");
}

/// A late write in a linked child makes the next inherit a full copy over two levels: a
/// row only g1 holds is copied into g3 at its g1 version, nothing from the late seq reaches g3,
/// and History reads through both levels up to the base.
#[retcd_test]
fn m8s_32_a_two_level_copy_takes_the_state_as_of_base() {
    let dir = data_dir("32-two-level-copy");
    let mut engine = RocksEngine::open(dir.join("db")).expect("open");
    let g1 = linked_g2(&mut engine);
    write_after(&mut engine, G2, (10, &g1), 13);
    assert!(matches!(
        engine.inherit(P, G2, G3, Seq(12)),
        Ok(Inherited::Copied { .. })
    ));

    let dedup5 = dedup_key_of(&g1, 5);
    let row = engine.get(P, G3, Namespace::Dedup, &dedup5).expect("get");
    assert_eq!(row.map(|(from, version, _)| (from, version)), Some((G3, 5)));
    assert_eq!(user_k(&engine, P, G3), Some((G3, 12, 12)));
    assert_eq!(history_from(&engine, G3, 5), Some(G1));
    assert_eq!(history_from(&engine, G3, 12), Some(G2));
    assert_eq!(history_from(&engine, G3, 13), None);
    let seen = engine.snapshot(P, G3, SnapshotHandle(1)).expect("snapshot");
    assert!(
        seen.entries().all(|(_, _, _, version, _)| version <= 12),
        "seq 13 reached g3"
    );
    verify_lineage(&engine, P, G3).expect("g3 verifies over three levels");
}

/// As of base 0 the state is empty: the copy writes no keys and no Progress, and the empty
/// child verifies.
#[retcd_test]
fn m8s_33_base_zero_copies_nothing() {
    let dir = data_dir("33-base-zero");
    let mut engine = RocksEngine::open(dir.join("db")).expect("open");
    write(&mut engine, G1, 1);

    assert_eq!(
        engine.inherit(P, G1, G2, Seq(0)),
        Ok(Inherited::Copied { keys: 0 })
    );

    let seen = engine.snapshot(P, G2, SnapshotHandle(1)).expect("snapshot");
    assert!(seen.is_empty(), "{:?}", seen.entries().collect::<Vec<_>>());
    assert_eq!(
        engine
            .get(P, G2, Namespace::Progress, PROGRESS_KEY)
            .expect("get"),
        None
    );
    assert_eq!(engine.buffered_applied(P, G2), AppliedSeq(0));
    verify_lineage(&engine, P, G2).expect("the empty child verifies");
}

/// Leave g2 staging a full copy of g1 as of seq 2 (g1 holds 3): the switch batch fails.
fn stage_g2_from_g1(engine: &mut RocksEngine) {
    write(engine, G1, 3);
    engine.inject_fault(InjectedFault::Switch);
    assert_eq!(
        engine.inherit(P, G1, G2, Seq(2)),
        Err(InheritError::Storage(StorageFault::WriteFailed)),
        "setup: the full copy stages g2 and its switch fails"
    );
    assert_eq!(engine.staging(P, G2), Some((G1, Seq(2))));
}

/// A defect found by hand (L-R183h): while g2 is staging a copy from g1, `inherit 1->3` is
/// refused by name and writes nothing — before the fix it sealed g1 for g3 and left g2 stuck
/// with `copying` forever. The way out is the same-args re-run of g2, after which g1 is sealed for
/// g2 and `1->3` is `parent_sealed_for_other`.
#[retcd_test]
fn m8s_35_t1_second_child_is_refused_while_a_sibling_is_staging() {
    let dir = data_dir("t1-staging-sibling");
    let mut engine = RocksEngine::open(dir.join("db")).expect("open");
    stage_g2_from_g1(&mut engine);

    let result = engine.inherit(P, G1, G3, Seq(2));

    assert_conflict(&result, "parent_has_staging_child");
    assert_eq!(engine.sealed_by(P, G1), None, "the refusal sealed g1");
    assert_eq!(engine.link(P, G3), None, "the refusal linked g3");
    assert_eq!(engine.staging(P, G3), None, "the refusal staged g3");
    let seen = engine
        .snapshot(P, G3, SnapshotHandle(1))
        .expect("snapshot g3");
    assert!(seen.is_empty(), "the refusal wrote g3 records");

    assert!(
        matches!(
            engine.inherit(P, G1, G2, Seq(2)),
            Ok(Inherited::Copied { .. })
        ),
        "the same-args re-run of g2 is the way out"
    );
    verify_lineage(&engine, P, G2).expect("g2 verifies after the re-run");
    assert_conflict(
        &engine.inherit(P, G1, G3, Seq(2)),
        "parent_sealed_for_other",
    );
}

/// A defect found by hand: a parent left staging by a failed full copy holds staged keys and no
/// applied mark. Inheriting from it must be refused by name, not linked at base 0 — linking let
/// the child read bytes that were never switched in (`read g4` -> `k version=2 from=2`).
#[retcd_test]
fn m8s_t2_inherit_from_a_staging_parent_is_refused() {
    let dir = data_dir("t2-staging-parent");
    let mut engine = RocksEngine::open(dir.join("db")).expect("open");
    stage_g2_from_g1(&mut engine);
    let g4 = Generation(4);

    let result = engine.inherit(P, G2, g4, Seq(0));

    assert_conflict(&result, "parent_staging");
    let seen = engine
        .snapshot(P, g4, SnapshotHandle(1))
        .expect("snapshot g4");
    assert!(
        seen.is_empty(),
        "g4 sees staged bytes: {:?}",
        seen.entries().collect::<Vec<_>>()
    );
    assert_eq!(engine.link(P, g4), None, "the refusal wrote a link");
    assert_eq!(engine.sealed_by(P, G2), None, "the refusal sealed g2");
}

/// Put one well-framed `(P, G1)` record straight into RocksDB, around the engine: the on-disk
/// result of a WAL patch that drops a History record, without the WAL tool.
fn raw_put_g1(db: &Path, ns: Namespace, key: &[u8], version: u64, bytes: &[u8]) {
    let raw =
        rocksdb::DB::open_cf(&rocksdb::Options::default(), db, COLUMN_FAMILIES).expect("raw open");
    let cf = raw.cf_handle(cf_for(ns)).expect("cf");
    // Frame: kind 0 (value) | version u64 BE | bytes.
    let mut value = vec![0];
    value.extend_from_slice(&version.to_be_bytes());
    value.extend_from_slice(bytes);
    raw.put_cf(cf, encode_key(P, G1, ns, key), value)
        .expect("raw put");
}

/// g1 holds 11 records, `patch` corrupts it on disk, and then a full copy at
/// base 10 is refused as `inherit_history_missing` (exit 12) naming `at`, and writes nothing.
fn assert_history_missing(row: &str, patch: impl FnOnce(&Path), at: &str) {
    let dir = data_dir(row);
    let db = dir.join("db");
    {
        let mut engine = RocksEngine::open(&db).expect("open");
        write(&mut engine, G1, 11);
    }
    patch(&db);
    let before = dumped(&db);
    {
        let mut engine = RocksEngine::open(&db).expect("reopen");
        assert_eq!(
            engine.inherit(P, G1, G2, Seq(10)),
            Err(InheritError::HistoryMissing { at: at.to_owned() }),
            "{row}"
        );
        assert!(!engine.lineages().contains(&(P, G2)), "{row}: g2 was made");
        assert_eq!(engine.sealed_by(P, G1), None, "{row}: g1 was sealed");
        assert_eq!(engine.staging(P, G2), None, "{row}: g2 was staged");
    }
    assert_eq!(dumped(&db), before, "{row}: the refusal wrote something");
}

/// An undecodable History record at `seq` of g1.
fn undecodable_history(seq: u64) -> impl FnOnce(&Path) {
    move |db| {
        raw_put_g1(
            db,
            Namespace::History,
            &seq.to_be_bytes(),
            seq,
            b"not an envelope",
        );
    }
}

/// A History record missing above base: seq 11, so the touched-key scan reads it first.
#[retcd_test]
fn m8s_23_history_missing_above_base_is_refused_at_that_seq() {
    assert_history_missing("23-missing-11", undecodable_history(11), "history:11");
}

/// A History record missing below base: seq 5; the backward after-image scan reaches it because
/// seq 11's dedup key is new above base and so stays open down to seq 1.
#[retcd_test]
fn m8s_24_history_missing_below_base_is_refused_at_that_seq() {
    assert_history_missing("24-missing-5", undecodable_history(5), "history:5");
}

/// Seq 5 holds a record that decodes but claims seq 6, so it is not seq 5's
/// record either; the copy refuses it like a missing one.
#[retcd_test]
fn m8s_24_a_history_record_claiming_another_seq_is_missing() {
    let patch = |db: &Path| {
        let raw = rocksdb::DB::open_cf(&rocksdb::Options::default(), db, COLUMN_FAMILIES)
            .expect("raw open");
        let cf = raw.cf_handle(cf_for(Namespace::History)).expect("cf");
        let key = |seq: u64| encode_key(P, G1, Namespace::History, &seq.to_be_bytes());
        let six = raw.get_cf(cf, key(6)).expect("raw get").expect("seq 6");
        raw.put_cf(cf, key(5), six).expect("raw put");
    };
    assert_history_missing("24-wrong-seq", patch, "history:5");
}

/// A value whose frame does not decode fails a snapshot taken through
/// the chain as `Corrupt`, logged as `snapshot_unframe_failed`; it is never skipped.
#[retcd_test]
fn m8s_26_c07_an_unframed_parent_value_fails_the_childs_snapshot() {
    let dir = data_dir("26-c07");
    let db = dir.join("db");
    {
        let mut engine = RocksEngine::open(&db).expect("open");
        linked_g2(&mut engine);
    }
    {
        let raw = rocksdb::DB::open_cf(&rocksdb::Options::default(), &db, COLUMN_FAMILIES)
            .expect("raw open");
        let cf = raw.cf_handle(cf_for(Namespace::User)).expect("cf");
        // 3 bytes: shorter than the 9-byte frame header.
        raw.put_cf(cf, encode_key(P, G1, Namespace::User, b"c07"), [0, 0, 0])
            .expect("raw put");
    }
    let engine = RocksEngine::open(&db).expect("reopen");
    assert_eq!(
        engine.snapshot(P, G2, SnapshotHandle(1)).err(),
        Some(StorageFault::Corrupt)
    );
    let lines = config_testkit::logs::lines_for_current_test(
        module_path!(),
        "m8s_26_c07_an_unframed_parent_value_fails_the_childs_snapshot",
    );
    assert!(
        lines
            .iter()
            .any(|row| row["@m"] == "snapshot_unframe_failed"),
        "no snapshot_unframe_failed line: {lines:?}"
    );
}

/// `dump` and `open_read_only` refuse a directory with no database
/// and create nothing, as `open_existing` does.
#[retcd_test]
fn m8s_d2_dump_and_read_only_refuse_a_missing_database() {
    let dir = data_dir("d2-read-only");
    let db = dir.join("not-there");
    assert!(matches!(dump(&db), Err(OpenError::NoDatabase { .. })));
    assert!(matches!(
        RocksEngine::open_read_only(&db),
        Err(OpenError::NoDatabase { .. })
    ));
    assert!(!db.exists(), "a refused open must not create the directory");
}

/// Tester row "step 5" (design R2 §2 step 5): a visible g1 record above base that no History
/// record names cannot be rebuilt as of base, so it is refused by its namespace and key.
#[retcd_test]
fn m8s_step5_a_record_above_base_that_no_history_names_is_refused() {
    let stray = |db: &Path| raw_put_g1(db, Namespace::Dedup, b"stray", 11, b"x");
    // "stray" in hex.
    assert_history_missing("step5-stray", stray, "dedup:7374726179");
}

/// Partition 0 and generation 0 are ordinary ids: a root g0 inherits to g1 like any other.
#[retcd_test]
fn m8s_27_partition_0_generation_0_inherits_like_any_other() {
    let p0 = PartitionId(0);
    let dir = data_dir("27-p0");
    let mut engine = RocksEngine::open(dir.join("db")).expect("open");
    let g0 = chain(p0, G0, (Seq::ZERO, Digest::ROOT), 5);
    commit_through(&mut engine, &g0, 5);
    assert_eq!(engine.inherit(p0, G0, G1, Seq(5)), Ok(Inherited::Linked));
    assert_eq!(engine.link(p0, G0), None, "g0 has no engine.parent");
    assert_eq!(
        engine.link(p0, G1),
        Some(Link {
            parent: G0,
            base: Seq(5),
            copied: false
        })
    );
    verify_lineage(&engine, p0, G1).expect("g1 verifies through g0");
}

/// Env var that turns [`zz_child_inherits_then_aborts`] from a no-op into the crashing child.
const CHILD_DIR: &str = "RDB_STORAGE_S1_CHILD_DIR";
/// Env var naming what the child does before it aborts: `write`, `linked` or `staged`.
const CHILD_MODE: &str = "RDB_STORAGE_S1_CHILD_MODE";

/// The crashing child for the three crash tests below. A no-op unless the parent set
/// [`CHILD_DIR`]. `write`: write 10 to g1, abort. `linked`: write 10, link g2 at 10, abort.
/// `staged`: write 1100, fail the copy's last batch as the failed-copy test does, abort with the
/// staging on disk. Never synced, so the reopen replays the WAL.
#[test]
fn zz_child_inherits_then_aborts() {
    let Some(db) = std::env::var_os(CHILD_DIR) else {
        return;
    };
    let mode = std::env::var(CHILD_MODE).expect("the parent names a mode");
    let mut engine = RocksEngine::open(PathBuf::from(db)).expect("child open");
    match mode.as_str() {
        "write" => {
            write(&mut engine, G1, 10);
        }
        "linked" => {
            linked_g2(&mut engine);
        }
        "staged" => {
            write(&mut engine, G1, 1100);
            engine.inject_fault(InjectedFault::CopyBatch);
            assert_eq!(
                engine.inherit(P, G1, G2, Seq(1099)),
                Err(InheritError::Storage(StorageFault::WriteFailed))
            );
        }
        other => panic!("unknown child mode {other}"),
    }
    std::process::abort();
}

/// Run [`zz_child_inherits_then_aborts`] in `mode` against `db` and wait for it to die.
fn crash_child(db: &Path, mode: &str) {
    let out = Command::new(std::env::current_exe().expect("current exe"))
        .args([
            "zz_child_inherits_then_aborts",
            "--exact",
            "--test-threads=1",
        ])
        .env(CHILD_DIR, db)
        .env(CHILD_MODE, mode)
        .output()
        .expect("spawn the child");
    assert!(
        !out.status.success(),
        "the child must die by abort(), got {:?}: {}",
        out.status,
        String::from_utf8_lossy(&out.stdout)
    );
}

/// A process crash after the root's commits (never synced), then a reopen, then the
/// inherit: links as a plain linked switch does, over the records the WAL replayed.
#[retcd_test]
fn m8s_21_inherit_after_a_crash_between_commit_and_inherit_links() {
    let dir = data_dir("21-crash-write");
    let db = dir.join("db");
    crash_child(&db, "write");
    let mut engine = RocksEngine::open(&db).expect("reopen after crash");
    assert_eq!(engine.buffered_applied(P, G1), AppliedSeq(10));
    assert_eq!(engine.inherit(P, G1, G2, Seq(10)), Ok(Inherited::Linked));
    let verified = verify_lineage(&engine, P, G2).expect("g2 verifies through g1");
    assert_eq!(verified.applied, AppliedSeq(10));
    assert_eq!(user_k(&engine, P, G2), Some((G1, 10, 10)));
}

/// A process crash right after a linked switch keeps the whole switch: link, seal and
/// `applied = base`; it verifies, and the same call again is a no-op.
#[retcd_test]
fn m8s_22_a_crash_after_a_linked_switch_keeps_it_whole() {
    let dir = data_dir("22-crash-linked");
    let db = dir.join("db");
    crash_child(&db, "linked");
    let mut engine = RocksEngine::open(&db).expect("reopen after crash");
    assert_eq!(
        engine.link(P, G2),
        Some(Link {
            parent: G1,
            base: Seq(10),
            copied: false
        })
    );
    assert_eq!(engine.sealed_by(P, G1), Some(G2));
    assert_eq!(engine.buffered_applied(P, G2), AppliedSeq(10));
    verify_lineage(&engine, P, G2).expect("g2 verifies after the crash");
    assert_eq!(
        engine.inherit(P, G1, G2, Seq(10)),
        Ok(Inherited::AlreadyInherited)
    );
}

/// A process crash with a copy staged leaves the staging and whole copy batches, never a
/// lineage record or a seal; the same call again converges.
#[retcd_test]
fn m8s_17_a_crash_with_a_copy_staged_converges_on_a_rerun() {
    let dir = data_dir("17-crash-staged");
    let db = dir.join("db");
    crash_child(&db, "staged");
    let mut engine = RocksEngine::open(&db).expect("reopen after crash");
    assert_eq!(engine.staging(P, G2), Some((G1, Seq(1099))));
    assert_eq!(
        engine.link(P, G2),
        None,
        "a lineage record without its switch"
    );
    assert_eq!(engine.sealed_by(P, G1), None, "a seal without its switch");
    assert_eq!(
        verify_lineage(&engine, P, G2),
        Err(LineageFault::CopyIncomplete)
    );
    assert_eq!(
        engine.inherit(P, G1, G2, Seq(1099)),
        Ok(Inherited::Copied { keys: 1101 })
    );
    verify_lineage(&engine, P, G2).expect("g2 verifies after the re-run");
    assert_eq!(user_k(&engine, P, G2), Some((G2, 1099, 1099)));
}

/// `fault_injected` names the point by its CLI spelling in a
/// `point` field, as `fault_not_reached` does, so one log query finds both.
#[retcd_test]
fn m8s_19_s1_p5_fault_injected_names_its_point() {
    let dir = data_dir("19-p5");
    let mut engine = RocksEngine::open(dir.join("db")).expect("open");
    write(&mut engine, G1, 10);
    engine.inject_fault(InjectedFault::Switch);
    assert!(engine.inherit(P, G1, G2, Seq(10)).is_err());
    let lines = config_testkit::logs::lines_for_current_test(
        module_path!(),
        "m8s_19_s1_p5_fault_injected_names_its_point",
    );
    let fired: Vec<_> = lines
        .iter()
        .filter(|row| row["@m"] == "fault_injected")
        .collect();
    assert_eq!(fired.len(), 1, "{lines:?}");
    assert_eq!(fired[0]["point"], "switch", "{:?}", fired[0]);
}
