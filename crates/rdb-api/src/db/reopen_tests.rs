//! dev-w2's reopen rows (`db.rs`'s own `tests` module is dev-w1's):
//! - M10 S1-8 (tester k151, A-R177): a refused reopen tells the app or operator the next step, and
//!   the `Db` logs the refusal as `db_open_refused` with its kind.
//! - M10 R3a case B (A-R171): a reopen whose grant record is gone heals with `NoGrantHeld` and
//!   serves what was written.

use super::*;

fn config(dir: &Path) -> DbConfig {
    DbConfig {
        dir: dir.to_path_buf(),
        hold: Vec::new(),
        timeouts: Timeouts::default(),
        dedup_cap: None,
        dedup_retention: None,
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime")
}

/// L1's waits cut short, as `tests::fast` does, so a fresh open is ready quickly.
fn fast() -> Budgets {
    Budgets {
        resume_hold_millis: 50,
        warn_age_millis: 200,
        pause_age_millis: 400,
        ..Budgets::SPEC_DEFAULTS
    }
}

fn open(
    rt: &tokio::runtime::Runtime,
    dir: &Path,
    store: &Arc<dyn ConfigStore>,
) -> Result<Db, OpenError> {
    rt.block_on(Db::open_with(
        config(dir),
        Arc::clone(store),
        rt.handle().clone(),
        fast(),
    ))
}

/// A fresh open, ready, with one write, then shut down: the copies hold generation 1.
fn written_and_closed(rt: &tokio::runtime::Runtime, dir: &Path, store: &Arc<dyn ConfigStore>) {
    let mut db = open(rt, dir, store).expect("the fresh open");
    db.wait_ready(crate::host::test_patience(Duration::from_secs(10)))
        .expect("the fresh open is ready");
    db.put(b"a", b"1", None).expect("a put");
    db.shutdown();
}

/// The `db_open_refused` events this test logged, as `(level, kind)`.
fn refusals(method: &str) -> Vec<(String, String)> {
    config_testkit::logs::lines_for_current_test(module_path!(), method)
        .into_iter()
        .filter(|line| line["@m"] == "db_open_refused")
        .map(|line| {
            (
                line["@l"].as_str().unwrap_or_default().to_owned(),
                line["kind"].as_str().unwrap_or_default().to_owned(),
            )
        })
        .collect()
}

/// A copy deleted after a write: the reopen is `CopyMissing`, its text says to restore the
/// copy, and the `Db` logs one `db_open_refused` naming the kind.
/// Integration (~1 s): one fresh open until ready, then one refused reopen.
#[config_log::retcd_test]
fn m10_a_reopen_missing_a_copy_says_to_restore_it_and_logs_the_refusal() {
    const METHOD: &str = "m10_a_reopen_missing_a_copy_says_to_restore_it_and_logs_the_refusal";
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    written_and_closed(&rt, dir.path(), &store);
    std::fs::remove_dir_all(dir.path().join("nodes").join("2")).expect("remove copy 2");

    let refused = match open(&rt, dir.path(), &store) {
        Err(error @ OpenError::CopyMissing(_)) => error,
        other => panic!(
            "a missing copy is CopyMissing, not {:?}",
            other.map(|_| "Db")
        ),
    };
    let text = refused.to_string();
    assert!(
        text.ends_with(
            "cannot be opened. Restore that copy from a backup of this directory, then open it again"
        ),
        "the text names the next step: {text}"
    );
    assert_eq!(
        refusals(METHOD),
        vec![("Warning".to_owned(), "CopyMissing".to_owned())],
        "one db_open_refused naming the kind"
    );
}

/// `partitions/1` names a generation no copy holds (this folder is an older copy): the reopen
/// is `Stale`, its text says to open the current folder, and the `Db` logs one
/// `db_open_refused` naming the kind.
/// Integration (~1 s): one fresh open until ready, a store edit, then one refused reopen.
#[config_log::retcd_test]
fn m10_a_reopen_of_an_older_copy_says_to_open_the_current_one_and_logs_the_refusal() {
    const METHOD: &str =
        "m10_a_reopen_of_an_older_copy_says_to_open_the_current_one_and_logs_the_refusal";
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    written_and_closed(&rt, dir.path(), &store);

    // Another folder moved the database on: partitions/1 now names a later generation.
    let key = Bytes::from(ControlKey::Partition(PARTITION).encode());
    let found = rt
        .block_on(store.get(config_core::GetRequest { key: key.clone() }))
        .expect("read partitions/1")
        .record
        .expect("partitions/1 exists");
    let mut record = PartitionRecord::decode(&found.value).expect("a record");
    let root = PartitionRoot::decode(&found.value).expect("a layout-3 root");
    record.generation = Generation(record.generation.0 + 5);
    rt.block_on(store.put(config_core::PutRequest {
        key,
        value: record.encode_with(&root),
        expected_mod_revision: Some(found.mod_revision),
        dedup: None,
    }))
    .expect("move partitions/1 on");

    let refused = match open(&rt, dir.path(), &store) {
        Err(error @ OpenError::Stale { .. }) => error,
        other => panic!("an older copy is Stale, not {:?}", other.map(|_| "Db")),
    };
    let text = refused.to_string();
    assert!(
        text.ends_with(
            ". This directory is an older copy of the database: open the folder that holds the \
             current copies instead"
        ),
        "the text names the next step: {text}"
    );
    assert_eq!(
        refusals(METHOD),
        vec![("Warning".to_owned(), "Stale".to_owned())],
        "one db_open_refused naming the kind"
    );
}

/// M10 R3a case B (A-R171), end to end through `Db`: node 1's grant record is gone when the
/// folder is reopened (an operator cleared it, or an earlier heal deleted it and stopped). The
/// heal reads it absent and hands node 1 a `NoGrantHeld` takeover, with nothing to wait out;
/// F1 commits the new generation `Active`, the write made before reads back, and a new one lands.
/// Integration (~2 s): one fresh open until ready, a store delete, one reopen until ready.
#[config_log::retcd_test]
fn m10_a_reopen_with_no_grant_record_heals_no_grant_held_and_reads_back() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    written_and_closed(&rt, dir.path(), &store);

    let deleted = rt
        .block_on(store.delete(config_core::DeleteRequest {
            key: Bytes::from(ControlKey::Grant(OWNER).encode()),
            expected_mod_revision: None,
            dedup: None,
        }))
        .expect("delete grants/1");
    assert_eq!(
        deleted.outcome,
        config_core::MutationOutcome::Applied,
        "the first open left a grant record"
    );

    let mut db = open(&rt, dir.path(), &store).expect("the reopen");
    db.wait_ready(crate::host::test_patience(Duration::from_secs(30)))
        .expect("the reopen is ready");
    match db.heal_state() {
        HealState::Handed(admin::Healed {
            cleared: admin::GrantCleared::Absent(absent),
            ..
        }) => assert!(
            absent.0 >= deleted.revision,
            "absent read at {absent:?}, before the delete at {}",
            deleted.revision
        ),
        other => panic!("case B hands a NoGrantHeld takeover, not {other:?}"),
    }
    let got = db.get(b"a").expect("read a back");
    assert_eq!(got.value.map(|(_, v)| v), Some(Bytes::from_static(b"1")));
    let served = got.generation;
    assert!(served > Generation(1), "{served:?}");
    db.put(b"b", b"2", None).expect("a put after the heal");
    let got = db.get(b"b").expect("read b");
    assert_eq!(got.value.map(|(_, v)| v), Some(Bytes::from_static(b"2")));
    // F1 committed the reopen's generation fully protected (`PartitionMode::Active`: every copy
    // answered), not below it and rebuilding.
    let owner = db.node_status().into_iter().next().expect("node 1");
    assert_eq!(
        (owner.recovery.as_deref(), owner.recovered),
        (Some("Committed"), Some(served)),
        "{owner:?}"
    );
    db.shutdown();
}
