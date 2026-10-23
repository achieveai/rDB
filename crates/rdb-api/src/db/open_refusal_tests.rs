//! M10 S1-8 (tester k151, A-R177): a refused reopen tells the app or operator the next step, and
//! the `Db` logs the refusal as `db_open_refused` with its kind. dev-w2's rows; `db.rs`'s own
//! `tests` module is dev-w1's.

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
