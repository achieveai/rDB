//! dev-w2's reopen rows (`db.rs`'s own `tests` module is dev-w1's):
//! - M10 S1-8 (tester k151, A-R177): a refused reopen tells the app or operator the next step, and
//!   the `Db` logs the refusal as `db_open_refused` with its kind.
//! - M10 R3a case B (A-R171): a reopen whose grant record is gone heals with `NoGrantHeld` and
//!   serves what was written.
//! - M10 S1-5 (#45, A-R182): a reopen whose recovery CAS answers "unknown" settles it by
//!   read-then-CAS, and a reopened `Db` takes a fresh grant in the same boot after a control store
//!   outage (A-R96).

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
/// is `Stale`, and its text says the acknowledged writes are intact, not to delete the folder,
/// and to open the folder or restore the backup that holds that generation (A-R184). The `Db`
/// logs one `db_open_refused` naming the kind. With the record put back, a reopen reads the
/// write made before: the refusal changed no data.
/// Integration (~2 s): one fresh open until ready, a store edit, one refused reopen, the edit
/// undone, one reopen until ready.
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
    let generation = record.generation.0;
    assert!(
        text.ends_with(&format!(
            ". This open changed no data, and the acknowledged writes these copies hold are \
             intact: do not delete this directory. If another folder, or a newer backup of this \
             one, holds generation {generation}, open that folder or restore those copies here"
        )),
        "the text says the data is intact, not to delete, and the next step: {text}"
    );
    // The refusal changed no data: the write made before still reads back once partitions/1
    // names the copies' generation again.
    let key = Bytes::from(ControlKey::Partition(PARTITION).encode());
    let moved = rt
        .block_on(store.get(config_core::GetRequest { key: key.clone() }))
        .expect("read partitions/1 again")
        .record
        .expect("partitions/1 still exists");
    rt.block_on(store.put(config_core::PutRequest {
        key,
        value: found.value.clone(),
        expected_mod_revision: Some(moved.mod_revision),
        dedup: None,
    }))
    .expect("put partitions/1 back");
    let mut db = open(&rt, dir.path(), &store).expect("the reopen after the record is back");
    db.wait_ready(crate::host::test_patience(Duration::from_secs(30)))
        .expect("the reopen is ready");
    let got = db.get(b"a").expect("read a back");
    assert_eq!(
        got.value.map(|(_, v)| v),
        Some(Bytes::from_static(b"1")),
        "the acknowledged write is intact after the refusal"
    );
    db.shutdown();
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

// ---- S1-5: the reopen's recovery CAS answered "unknown" (#45, A-R182) ----

/// What [`CasFault`] does to the puts on `partitions/1` while it is armed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fault {
    /// The next put applies, and its reply is lost: `DeadlineExceededUnknownOutcome`.
    LoseOne,
    /// The next put is never applied, and answers `DeadlineExceededUnknownOutcome`.
    DropOne,
    /// Every put applies and loses its reply, until disarmed: a store that keeps answering
    /// "unknown".
    LoseAll,
}

/// A `MemStore` whose puts on `partitions/1` can answer "unknown". Only F1's recovery CAS puts
/// that key on a reopen, so an armed fault reaches that CAS and nothing else. Hand-written, as
/// `test_store.rs` is, because `async-trait` is not a dependency of this crate.
struct CasFault {
    inner: config_testkit::MemStore,
    fault: Mutex<Option<Fault>>,
    /// Puts on `partitions/1` so far, while armed or not.
    puts: AtomicU64,
}

type Reply<'b, T> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<T, config_core::ConfigError>> + Send + 'b>,
>;

impl CasFault {
    fn new() -> Self {
        Self {
            inner: config_testkit::MemStore::new(),
            fault: Mutex::new(None),
            puts: AtomicU64::new(0),
        }
    }

    fn arm(&self, fault: Option<Fault>) {
        *lock(&self.fault) = fault;
    }

    fn partition_puts(&self) -> u64 {
        self.puts.load(Ordering::Relaxed)
    }

    /// The fault for this put on `partitions/1`; a one-shot fault disarms itself.
    fn take(&self) -> Option<Fault> {
        let mut fault = lock(&self.fault);
        let now = *fault;
        if matches!(now, Some(Fault::LoseOne | Fault::DropOne)) {
            *fault = None;
        }
        now
    }
}

impl ConfigStore for CasFault {
    fn get<'a, 'b>(
        &'a self,
        request: config_core::GetRequest,
    ) -> Reply<'b, config_core::GetResponse>
    where
        'a: 'b,
        Self: 'b,
    {
        self.inner.get(request)
    }

    fn list<'a, 'b>(
        &'a self,
        request: config_core::ListRequest,
    ) -> Reply<'b, config_core::ListResponse>
    where
        'a: 'b,
        Self: 'b,
    {
        self.inner.list(request)
    }

    fn put<'a, 'b>(
        &'a self,
        request: config_core::PutRequest,
    ) -> Reply<'b, config_core::MutationResponse>
    where
        'a: 'b,
        Self: 'b,
    {
        Box::pin(async move {
            if request.key.as_ref() != ControlKey::Partition(PARTITION).encode().as_bytes() {
                return self.inner.put(request).await;
            }
            self.puts.fetch_add(1, Ordering::Relaxed);
            let fault = self.take();
            if fault == Some(Fault::DropOne) {
                return Err(config_core::ConfigError::DeadlineExceededUnknownOutcome);
            }
            let response = self.inner.put(request).await?;
            if fault.is_some() {
                return Err(config_core::ConfigError::DeadlineExceededUnknownOutcome);
            }
            Ok(response)
        })
    }

    fn delete<'a, 'b>(
        &'a self,
        request: config_core::DeleteRequest,
    ) -> Reply<'b, config_core::MutationResponse>
    where
        'a: 'b,
        Self: 'b,
    {
        self.inner.delete(request)
    }

    fn capabilities(&self) -> config_core::Capabilities {
        self.inner.capabilities()
    }

    fn watch<'a, 'b>(
        &'a self,
        request: config_core::WatchRequest,
    ) -> Reply<'b, config_core::WatchStream>
    where
        'a: 'b,
        Self: 'b,
    {
        self.inner.watch(request)
    }
}

/// a=1 reads back, and a new write lands and reads back.
fn reads_back_and_writes(db: &Db, case: &str) {
    let got = db
        .get(b"a")
        .unwrap_or_else(|e| panic!("{case}: read a back: {e}"));
    assert_eq!(
        got.value.map(|(_, v)| v),
        Some(Bytes::from_static(b"1")),
        "{case}"
    );
    db.put(b"b", b"2", None)
        .unwrap_or_else(|e| panic!("{case}: a put after the heal: {e}"));
    let got = db
        .get(b"b")
        .unwrap_or_else(|e| panic!("{case}: read b: {e}"));
    assert_eq!(
        got.value.map(|(_, v)| v),
        Some(Bytes::from_static(b"2")),
        "{case}"
    );
}

/// S1-5 (#45): the store answers "unknown" to the reopen's recovery CAS. Whether it applied
/// (`LoseOne`) or not (`DropOne`), F1 reads `partitions/1` and sends the same CAS again at the
/// revision it read: its own bytes there, or the record it fenced, unchanged. That CAS lands,
/// the reopen is ready, the write made before reads back and a new one lands.
/// Integration (~4 s): per case, one fresh open until ready, one reopen until ready.
#[config_log::retcd_test]
fn m10_a_reopen_whose_recovery_cas_answers_unknown_settles_by_read_then_cas() {
    let rt = runtime();
    for (case, fault) in [
        ("applied, reply lost", Fault::LoseOne),
        ("never applied", Fault::DropOne),
    ] {
        let faulty = Arc::new(CasFault::new());
        let store: Arc<dyn ConfigStore> = faulty.clone();
        let dir = config_testkit::fs::temp_dir();
        written_and_closed(&rt, dir.path(), &store);
        let before = faulty.partition_puts();
        faulty.arm(Some(fault));

        let mut db = open(&rt, dir.path(), &store).expect("the reopen");
        db.wait_ready(crate::host::test_patience(Duration::from_secs(30)))
            .unwrap_or_else(|e| panic!("{case}: the reopen is ready: {e}"));
        assert_eq!(*lock(&faulty.fault), None, "{case}: the fault was used");
        assert!(
            faulty.partition_puts() >= before + 2,
            "{case}: the unknown CAS, then the settling one: {} puts",
            faulty.partition_puts() - before
        );
        reads_back_and_writes(&db, case);
        db.shutdown();
    }
}

/// S1-5 (#45), the store keeps answering "unknown": F1 cannot settle, and says so. `wait_ready`
/// times out naming `ProposalUnresolved`, never a block. Once the store answers again, the next
/// read-then-CAS lands and a retried `wait_ready` succeeds, with nothing reopened.
/// Integration (~6 s): one fresh open, a reopen held ~2 s, then one discovery window.
#[config_log::retcd_test]
fn m10_a_reopen_whose_store_keeps_answering_unknown_is_ready_once_it_answers() {
    let rt = runtime();
    let faulty = Arc::new(CasFault::new());
    let store: Arc<dyn ConfigStore> = faulty.clone();
    let dir = config_testkit::fs::temp_dir();
    written_and_closed(&rt, dir.path(), &store);
    faulty.arm(Some(Fault::LoseAll));

    let mut db = open(&rt, dir.path(), &store).expect("the reopen");
    // The heal first waits out the old grant; then F1 proposes, hears "unknown", and resolves.
    let patience = crate::host::test_patience(Duration::from_secs(30));
    let deadline = std::time::Instant::now() + patience;
    let phase = loop {
        let phase = db.node_status().into_iter().next().and_then(|n| n.recovery);
        let before_the_answer = [
            "Idle",
            "Fenced",
            "Collecting",
            "Synchronizing",
            "Barrier",
            "Proposing",
        ];
        let answered = phase
            .as_deref()
            .is_some_and(|p| !before_the_answer.contains(&p));
        if answered || std::time::Instant::now() >= deadline {
            break phase;
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(
        phase.as_deref(),
        Some("ProposalUnresolved"),
        "F1 is resolving, not blocked"
    );
    let puts = faulty.partition_puts();
    match db.wait_ready(Duration::from_secs(3)) {
        Err(ReadyError::TimedOut { phase, .. }) => {
            assert_eq!(phase, "ProposalUnresolved", "F1 is still resolving");
        }
        other => panic!("not ready while the store answers unknown: {other:?}"),
    }
    assert!(
        faulty.partition_puts() > puts,
        "F1 kept trying while unresolved: {} puts before, {} after",
        puts,
        faulty.partition_puts()
    );
    faulty.arm(None);
    db.wait_ready(crate::host::test_patience(Duration::from_secs(30)))
        .expect("ready once the store answers");
    reads_back_and_writes(&db, "after the outage");
    db.shutdown();
}

/// S1-5 regression (arch-m10 R8 "Control quorum loss", A-R96): a reopened `Db` whose control
/// store goes down loses its grant at expiry and fences; once the store is back, node 1 takes a
/// fresh grant in the same boot, with no restart, and a write lands. The reopen is the M10 path
/// (bound F1, layout-3 record), which M9's host row never ran.
/// Integration (~4 s): a fresh open and a reopen on a 600 ms grant, then one store outage.
#[config_log::retcd_test]
fn m10_a_reopened_db_after_a_control_store_outage_takes_a_fresh_grant_in_the_same_boot() {
    let rt = runtime();
    let mem = Arc::new(config_testkit::MemStore::new());
    let store: Arc<dyn ConfigStore> = mem.clone();
    let dir = config_testkit::fs::temp_dir();
    let budgets = Budgets {
        grant_millis: 600,
        renew_millis: 100,
        ..fast()
    };
    let open_short = || {
        rt.block_on(Db::open_with(
            config(dir.path()),
            Arc::clone(&store),
            rt.handle().clone(),
            budgets,
        ))
    };
    let mut db = open_short().expect("the fresh open");
    db.wait_ready(crate::host::test_patience(Duration::from_secs(10)))
        .expect("the fresh open is ready");
    db.put(b"a", b"1", None).expect("a put");
    db.shutdown();

    let mut db = open_short().expect("the reopen");
    db.wait_ready(crate::host::test_patience(Duration::from_secs(30)))
        .expect("the reopen is ready");
    let owner = |db: &Db| db.node_status().into_iter().next().expect("node 1 answers");
    let before = owner(&db);
    assert_eq!(before.admits, Some(true), "{before:?}");

    let patience = crate::host::test_patience(Duration::from_secs(30));
    let until = |what: &str, done: &dyn Fn(&crate::host::NodeStatus) -> bool| {
        let deadline = std::time::Instant::now() + patience;
        loop {
            let status = owner(&db);
            if done(&status) {
                return status;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{what}: last status {status:?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    };

    mem.failing_with(config_core::ConfigError::Unavailable {
        reason: "the control store is down".to_owned(),
    });
    let fenced = until("node 1 fenced", &|s| s.authority.starts_with("fenced"));
    assert_eq!(fenced.admits, Some(false), "{fenced:?}");
    mem.stop_failing();
    let after = until("node 1 admits again", &|s| s.admits == Some(true));

    assert_eq!(after.boot, before.boot, "the same boot: no restart");
    assert!(after.authority.starts_with("held("), "{after:?}");
    assert_ne!(
        after.authority, before.authority,
        "a fresh grant: {after:?}"
    );
    reads_back_and_writes(&db, "after the outage");
    db.shutdown();
}

// ---- S1-8, the tester's walk on 066ab6e: every refusal names a next step, under one event ----

/// A second open of a directory another open holds: `InUse`, whose text says to close that open
/// or stop its process, then open again; the `Db` logs one `db_open_refused` naming the kind. The
/// first open is untouched and still serves.
/// Integration (~1 s): one fresh open until ready, then one refused open beside it.
#[config_log::retcd_test]
fn m10_s1_8_a_second_open_of_a_held_directory_says_to_close_the_first() {
    const METHOD: &str = "m10_s1_8_a_second_open_of_a_held_directory_says_to_close_the_first";
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let mut first = open(&rt, dir.path(), &store).expect("the first open");
    first
        .wait_ready(crate::host::test_patience(Duration::from_secs(10)))
        .expect("the first open is ready");
    first.put(b"a", b"1", None).expect("a put");

    let path = match open(&rt, dir.path(), &store) {
        Err(OpenError::InUse(path)) => path,
        other => panic!("a held directory is InUse, not {:?}", other.map(|_| "Db")),
    };
    assert!(
        path.starts_with(dir.path().join("nodes")),
        "{}",
        path.display()
    );
    assert_eq!(
        OpenError::InUse(path.clone()).to_string(),
        format!(
            "{} is in use by another open. Close that open, or stop the process that holds it, \
             then open this directory again",
            path.display()
        ),
        "the text names the next step"
    );
    assert_eq!(
        refusals(METHOD),
        vec![("Warning".to_owned(), "InUse".to_owned())],
        "one db_open_refused naming the kind"
    );
    reads_back_and_writes(&first, "the first open, after the refusal");
    first.shutdown();
}

/// Copies that hold data but name no instance, as an M9 `rdb_dev --keep` folder does: `Unbound`,
/// whose text says to open it with the M9 build or start a new database in an empty folder, and
/// that this build does not migrate it; the `Db` logs one `db_open_refused` naming the kind.
/// Integration (~1 s): one fresh open until ready, the instance marker removed from every copy
/// with a raw RocksDB write, then one refused reopen.
#[config_log::retcd_test]
fn m10_s1_8_an_m9_directory_says_to_use_the_m9_build_or_a_new_folder() {
    const METHOD: &str = "m10_s1_8_an_m9_directory_says_to_use_the_m9_build_or_a_new_folder";
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    written_and_closed(&rt, dir.path(), &store);
    for n in 1..=3 {
        let path = dir.path().join("nodes").join(n.to_string());
        let raw = rocksdb::DB::open_cf(
            &rocksdb::Options::default(),
            &path,
            rdb_storage::keys::COLUMN_FAMILIES,
        )
        .expect("raw open");
        let metadata = raw
            .cf_handle(rdb_storage::keys::CF_METADATA)
            .expect("the metadata family");
        // `rdb_storage`'s instance marker key (crate-private there): absent means unbound.
        raw.delete_cf(metadata, b"instance")
            .expect("remove the marker");
    }

    let refused = match open(&rt, dir.path(), &store) {
        Err(error @ OpenError::Unbound(_)) => error,
        other => panic!("an M9 directory is Unbound, not {:?}", other.map(|_| "Db")),
    };
    assert_eq!(
        refused.to_string(),
        "nodes/1 holds data but names no rDB instance (an M9 directory); this build does not open \
         it. Open it with the M9 build that wrote it, or start a new database in an empty folder: \
         this build does not migrate M9 data",
        "the text names the next step"
    );
    assert_eq!(
        refusals(METHOD),
        vec![("Warning".to_owned(), "Unbound".to_owned())],
        "one db_open_refused naming the kind"
    );
}

/// `DbConfig::check`, which `rdb_dev` runs before it starts the control store, refuses a dedup cap
/// above the status cap and logs it as `db_open_refused` with `kind=DedupCap`, at the level of
/// every other refused open: one query finds them all. The text is pinned.
/// Unit (~1 ms): nothing starts.
#[config_log::retcd_test]
fn m10_s1_8_a_dedup_cap_refused_by_check_logs_db_open_refused() {
    const METHOD: &str = "m10_s1_8_a_dedup_cap_refused_by_check_logs_db_open_refused";
    let dir = config_testkit::fs::temp_dir();
    let above = DbConfig {
        dedup_cap: Some(RETENTION_CAP_ENTRIES + 1),
        ..config(dir.path())
    };
    let refused = above.check().expect_err("above the status cap");
    assert_eq!(
        refused.to_string(),
        format!(
            "dedup_cap {} is above the most P1's status index holds ({}); nothing was started. \
             Set DbConfig::dedup_cap to at most {}, or leave it unset for the default",
            RETENTION_CAP_ENTRIES + 1,
            RETENTION_CAP_ENTRIES,
            RETENTION_CAP_ENTRIES
        )
    );
    assert_eq!(
        refusals(METHOD),
        vec![("Warning".to_owned(), "DedupCap".to_owned())],
        "one db_open_refused naming the kind"
    );
    let lines = config_testkit::logs::lines_for_current_test(module_path!(), METHOD);
    assert!(
        lines.iter().all(|line| line["@m"] != "dedup_cap_refused"),
        "the old event name is gone: {lines:?}"
    );
}

/// M10 S1-6 (A-R183, critic-d6b candidate 3): a "not before" hint rides on the error as
/// `retry_after` and in its text, the seconds rounded up so the words never promise sooner than
/// the hint. The kind, the retry rule and the mutation proof are untouched, and an error built
/// without a hint carries none. Unit (~1 ms).
#[test]
fn m10_s1_6_an_api_error_carries_a_not_before_hint_in_its_field_and_its_text() {
    let plain = ApiError::new(ErrorKind::Overloaded, "dedup index full");
    assert_eq!(
        plain.retry_after(),
        None,
        "no hint unless the host sets one"
    );
    let hinted = plain.clone().with_retry_after(Duration::from_millis(1_500));
    assert_eq!(hinted.retry_after(), Some(Duration::from_millis(1_500)));
    assert_eq!(hinted.detail, "dedup index full; not before ~2s");
    assert_eq!(
        (hinted.kind, hinted.retry, hinted.no_mutation),
        (plain.kind, plain.retry, plain.no_mutation),
        "the hint changes nothing else"
    );
    assert_eq!(
        ApiError::new(ErrorKind::Overloaded, "x")
            .with_retry_after(Duration::from_secs(3))
            .detail,
        "x; not before ~3s",
        "a whole second is not rounded up past itself"
    );
}

// ---- Guard rows for the tester's surviving cuts at 066ab6e (lead, k-cuts) ----

/// Copy a closed copy's directory, file by file.
fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("create the copy's directory");
    for entry in std::fs::read_dir(from).expect("list the copy") {
        let entry = entry.expect("an entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("its type").is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("copy a file");
        }
    }
}

/// Set (`Some`) or remove (`None`) copy `node`'s instance marker with a raw RocksDB write.
fn set_marker(dir: &Path, node: u32, marker: Option<[u8; INSTANCE_LEN]>) {
    let path = dir.join("nodes").join(node.to_string());
    let raw = rocksdb::DB::open_cf(
        &rocksdb::Options::default(),
        &path,
        rdb_storage::keys::COLUMN_FAMILIES,
    )
    .expect("raw open");
    let metadata = raw
        .cf_handle(rdb_storage::keys::CF_METADATA)
        .expect("the metadata family");
    // `rdb_storage`'s instance marker key (crate-private there).
    match marker {
        Some(id) => raw
            .put_cf(metadata, b"instance", id)
            .expect("set the marker"),
        None => raw
            .delete_cf(metadata, b"instance")
            .expect("remove the marker"),
    }
}

/// The instance copy `node` names, read without taking it.
fn marker(dir: &Path, node: u32) -> Option<[u8; INSTANCE_LEN]> {
    RocksEngine::open_read_only(dir.join("nodes").join(node.to_string()))
        .expect("read-only open")
        .instance()
}

/// The open counter's record of `instance`, if any.
fn counter(
    rt: &tokio::runtime::Runtime,
    store: &Arc<dyn ConfigStore>,
    instance: &[u8; INSTANCE_LEN],
) -> Option<config_core::Record> {
    rt.block_on(store.get(config_core::GetRequest {
        key: Bytes::from(admin::instance_key(instance)),
    }))
    .expect("read the counter")
    .record
}

/// Survivor 1 (unwind needs `created`): a reopen refused while it holds node 1's lock never
/// removes a `nodes/` it did not create. Copy 2 is missing; the refusal keeps copies 1 and 3,
/// and once copy 2 is restored from a backup, as the text says, the reopen reads the write back.
/// Integration (~2 s): one fresh open until ready, one refused reopen, one reopen until ready.
#[config_log::retcd_test]
fn m10_cut_a_refused_reopen_keeps_the_nodes_it_did_not_create() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    written_and_closed(&rt, dir.path(), &store);
    let nodes = dir.path().join("nodes");
    let backup = dir.path().join("backup-2");
    copy_dir(&nodes.join("2"), &backup);
    std::fs::remove_dir_all(nodes.join("2")).expect("remove copy 2");

    match open(&rt, dir.path(), &store) {
        Err(OpenError::CopyMissing(_)) => {}
        other => panic!("CopyMissing, not {:?}", other.map(|_| "Db")),
    }
    for n in [1, 3] {
        assert!(
            nodes.join(n.to_string()).join("CURRENT").exists(),
            "copy {n} survives a refused reopen that did not create nodes/"
        );
    }

    copy_dir(&backup, &nodes.join("2"));
    let mut db = open(&rt, dir.path(), &store).expect("the reopen with copy 2 restored");
    db.wait_ready(crate::host::test_patience(Duration::from_secs(30)))
        .expect("ready");
    let got = db.get(b"a").expect("read a back");
    assert_eq!(got.value.map(|(_, v)| v), Some(Bytes::from_static(b"1")));
    db.shutdown();
}

/// Survivor 2 (`bound.len() > 1`): copies that name two instances are `Mismatch`, naming both,
/// and the refusal rebinds neither copy. Copy 3 is made to name another instance.
/// Integration (~1 s): one fresh open until ready, a raw marker write, one refused reopen.
#[config_log::retcd_test]
fn m10_cut_copies_naming_two_instances_are_refused_and_left_as_they_were() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    written_and_closed(&rt, dir.path(), &store);
    let ours = marker(dir.path(), 1).expect("the fresh open bound the copies");
    let other = [0x22; INSTANCE_LEN];
    assert_ne!(ours, other);
    set_marker(dir.path(), 3, Some(other));

    let refused = match open(&rt, dir.path(), &store) {
        Err(error @ OpenError::Mismatch(_)) => error,
        other => panic!("Mismatch, not {:?}", other.map(|_| "Db")),
    };
    let both: BTreeSet<[u8; INSTANCE_LEN]> = [ours, other].into_iter().collect();
    let named: Vec<String> = both.iter().map(admin::instance_hex).collect();
    assert_eq!(
        refused.to_string(),
        format!(
            "this directory and the control store do not belong together: the copies name \
             different instances: {}",
            named.join(", ")
        )
    );
    assert_eq!(
        [1, 2, 3].map(|n| marker(dir.path(), n)),
        [Some(ours), Some(ours), Some(other)],
        "the refusal bound nothing"
    );
}

/// Survivor 3 (counter absent): a reopen whose open counter is gone is `Mismatch` naming the
/// instance and the key, never counted again from N = 1, and the counter stays absent.
/// Integration (~1 s): one fresh open until ready, a store delete, one refused reopen.
#[config_log::retcd_test]
fn m10_cut_a_reopen_whose_open_counter_is_gone_is_a_mismatch_and_counts_nothing() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    written_and_closed(&rt, dir.path(), &store);
    let instance = marker(dir.path(), 1).expect("bound");
    let key = admin::instance_key(&instance);
    assert!(
        counter(&rt, &store, &instance).is_some(),
        "the first open counted"
    );
    rt.block_on(store.delete(config_core::DeleteRequest {
        key: Bytes::from(key.clone()),
        expected_mod_revision: None,
        dedup: None,
    }))
    .expect("delete the counter");

    let refused = match open(&rt, dir.path(), &store) {
        Err(error @ OpenError::Mismatch(_)) => error,
        other => panic!("Mismatch, not {:?}", other.map(|_| "Db")),
    };
    assert_eq!(
        refused.to_string(),
        format!(
            "this directory and the control store do not belong together: the copies name \
             instance {}, but {key} is absent",
            admin::instance_hex(&instance)
        )
    );
    assert!(
        counter(&rt, &store, &instance).is_none(),
        "the refused reopen wrote no counter"
    );
}

/// Survivor 4 (Unbound's `holds_data` guard): one copy with data that names no instance, beside
/// two that do (copy 3 put back from an M9 backup), is `Unbound` naming that copy, and the
/// refusal does not bind it. Without the guard the open binds it and goes on.
/// Integration (~1 s): one fresh open until ready, a raw marker delete, one refused reopen.
#[config_log::retcd_test]
fn m10_cut_one_unbound_copy_with_data_is_refused_and_stays_unbound() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    written_and_closed(&rt, dir.path(), &store);
    let ours = marker(dir.path(), 1).expect("bound");
    set_marker(dir.path(), 3, None);

    let refused = match open(&rt, dir.path(), &store) {
        Err(error @ OpenError::Unbound(_)) => error,
        other => panic!("Unbound, not {:?}", other.map(|_| "Db")),
    };
    assert!(
        refused.to_string().starts_with(
            "nodes/3 holds data but names no rDB instance (an M9 directory); this build does not \
             open it."
        ),
        "{refused}"
    );
    assert_eq!(
        [1, 2, 3].map(|n| marker(dir.path(), n)),
        [Some(ours), Some(ours), None],
        "the refusal bound nothing"
    );
}

/// A `MemStore` on which a second open counts in the gap between the first open's read of the
/// counter and its CAS: the first `get` of `key` reads, then runs [`admin::count_open`] for the
/// same instance on the inner store, then answers with what it read.
struct CountRace {
    inner: config_testkit::MemStore,
    key: Bytes,
    instance: [u8; INSTANCE_LEN],
    /// What the racing open was counted as, once it has run.
    raced: tokio::sync::Mutex<Option<u64>>,
}

impl ConfigStore for CountRace {
    fn get<'a, 'b>(
        &'a self,
        request: config_core::GetRequest,
    ) -> Reply<'b, config_core::GetResponse>
    where
        'a: 'b,
        Self: 'b,
    {
        Box::pin(async move {
            let racing = request.key == self.key;
            let read = self.inner.get(request).await?;
            let mut raced = self.raced.lock().await;
            if racing && raced.is_none() {
                let n = admin::count_open(&self.inner, &self.instance, false, admin::COUNT_WITHIN)
                    .await
                    .expect("the racing open counts");
                *raced = Some(n);
            }
            Ok(read)
        })
    }

    fn list<'a, 'b>(
        &'a self,
        request: config_core::ListRequest,
    ) -> Reply<'b, config_core::ListResponse>
    where
        'a: 'b,
        Self: 'b,
    {
        self.inner.list(request)
    }

    fn put<'a, 'b>(
        &'a self,
        request: config_core::PutRequest,
    ) -> Reply<'b, config_core::MutationResponse>
    where
        'a: 'b,
        Self: 'b,
    {
        self.inner.put(request)
    }

    fn delete<'a, 'b>(
        &'a self,
        request: config_core::DeleteRequest,
    ) -> Reply<'b, config_core::MutationResponse>
    where
        'a: 'b,
        Self: 'b,
    {
        self.inner.delete(request)
    }

    fn capabilities(&self) -> config_core::Capabilities {
        self.inner.capabilities()
    }

    fn watch<'a, 'b>(
        &'a self,
        request: config_core::WatchRequest,
    ) -> Reply<'b, config_core::WatchStream>
    where
        'a: 'b,
        Self: 'b,
    {
        self.inner.watch(request)
    }
}

/// Survivor 5 (the counter's CAS): open A reads N, open B counts N + 1 before A writes, and A's
/// write is conditioned on what it read, so A conflicts, reads again and is counted N + 2. Two
/// opens never get one number, so never one boot range or request floor. An unconditioned write
/// hands A the number B already has.
/// Unit (~1 ms): `count_open` against a store that races it.
#[test]
fn m10_cut_two_opens_counting_at_once_never_get_one_number() {
    let rt = runtime();
    let instance = [0x33; INSTANCE_LEN];
    let store = CountRace {
        inner: config_testkit::MemStore::new(),
        key: Bytes::from(admin::instance_key(&instance)),
        instance,
        raced: tokio::sync::Mutex::new(None),
    };
    let first = rt
        .block_on(admin::count_open(
            &store.inner,
            &instance,
            true,
            admin::COUNT_WITHIN,
        ))
        .expect("the first open");
    assert_eq!(first, 1);
    let a = rt
        .block_on(admin::count_open(
            &store,
            &instance,
            false,
            admin::COUNT_WITHIN,
        ))
        .expect("open A");
    let b = rt
        .block_on(store.raced.lock())
        .expect("open B ran in A's gap");
    assert_eq!(
        (b, a),
        (2, 3),
        "B counted in A's gap, so A is counted after it"
    );
    let next = rt
        .block_on(admin::count_open(
            &store.inner,
            &instance,
            false,
            admin::COUNT_WITHIN,
        ))
        .expect("the next open");
    assert_eq!(next, 4);
}

/// Survivor 6 (`MAX_OPENS`): the 2^24-th open is counted, and the one after it is refused
/// `Exhausted` with the counter left as it was. The counter is seeded with layout 1's bytes
/// (`[1]` then N as a little-endian u64), which this build reads.
/// Unit (~1 ms): `count_open` against a `MemStore`.
#[test]
fn m10_cut_the_open_after_max_opens_is_refused_and_counts_nothing() {
    let rt = runtime();
    let store = config_testkit::MemStore::new();
    let instance = [0x44; INSTANCE_LEN];
    let key = admin::instance_key(&instance);
    let mut value = vec![1_u8];
    value.extend_from_slice(&(admin::MAX_OPENS - 1).to_le_bytes());
    rt.block_on(store.put(config_core::PutRequest {
        key: Bytes::from(key.clone()),
        value: Bytes::from(value),
        expected_mod_revision: None,
        dedup: None,
    }))
    .expect("seed the counter");
    assert_eq!(
        rt.block_on(admin::count_open(
            &store,
            &instance,
            false,
            admin::COUNT_WITHIN
        )),
        Ok(admin::MAX_OPENS),
        "the last open in range is counted"
    );
    let read = || {
        rt.block_on(store.get(config_core::GetRequest {
            key: Bytes::from(key.clone()),
        }))
        .expect("read the counter")
        .record
        .expect("the counter")
    };
    let at_cap = read();
    let refused = rt.block_on(admin::count_open(
        &store,
        &instance,
        false,
        admin::COUNT_WITHIN,
    ));
    assert_eq!(
        refused,
        Err(admin::CounterError::Exhausted { key: key.clone() })
    );
    assert_eq!(
        refused.expect_err("refused").to_string(),
        format!("{key} has counted 16777216 opens, the most one directory may have")
    );
    let after = read();
    assert_eq!(
        (after.mod_revision, after.value),
        (at_cap.mod_revision, at_cap.value),
        "the refused open wrote nothing"
    );
}

/// Survivor 7 (`MAX_RESTARTS`): one open restarts a node at most `MAX_RESTARTS` times, so its
/// boots never reach the next open's range. The node's boot is set one restart short of the
/// cap; that restart runs, and the next is `INVALID_ARGUMENT` saying to reopen, with the node
/// left at its boot.
/// Integration (~2 s): one fresh open until ready, two restarts of node 2.
#[config_log::retcd_test]
fn m10_cut_a_node_restarted_max_restarts_times_is_not_restarted_again() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let mut db = open(&rt, dir.path(), &store).expect("the fresh open");
    db.wait_ready(crate::host::test_patience(Duration::from_secs(10)))
        .expect("ready");
    let node = NodeId(2);
    let base = db.opened.base_boot;
    let short = BootId(base.0 + MAX_RESTARTS - 1);
    lock(&db.nodes)
        .iter_mut()
        .find(|handle| handle.node == node)
        .expect("node 2")
        .boot = short;

    let last = db.restart_node(node).expect("the last restart in range");
    assert_eq!(last, BootId(base.0 + MAX_RESTARTS));
    let refused = db.restart_node(node).expect_err("one restart past the cap");
    assert_eq!(
        (refused.kind, refused.detail.as_str()),
        (
            ErrorKind::InvalidArgument,
            "node 2 not restarted: it has restarted 65534 times, the most one open allows; \
             reopen the directory"
        )
    );
    assert_eq!(
        db.boot_of(node),
        Some(last),
        "the refused restart left the node as it was"
    );
    db.shutdown();
}

/// R5 row 6, the heal's clear guard: `clear_until` counts node 1's grant as gone only when it
/// was deleted or read absent. A record naming the boot this open runs node 1 at is the live
/// process's grant (`Clearance::Current`), so the heal keeps asking until its time runs out,
/// never reads `partitions/1`, and sends node 1 nothing. Without the guard it fences over a
/// grant a live process holds. Unit (~300 ms): `MemStore`, the heal's own bound.
#[test]
fn m10_cut_a_heal_over_a_grant_naming_this_opens_boot_clears_nothing_and_sends_nothing() {
    use config_core::{GetRequest, PutRequest};
    use rdb_core::authority::grant::GrantRecord;
    use rdb_core::authority::partition::{PartitionRecord, PartitionRoot, INSTANCE_LEN};
    use rdb_core::contracts::control::ControlKey;
    use rdb_core::contracts::digest::Digest;
    use rdb_core::contracts::ids::{
        AuthorityGeneration, BootId, Generation, GrantId, OwnerEpoch, Revision, Seq,
    };

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("runtime");
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let open = BootId((1 << 16) + 1);
    let record = PartitionRecord {
        partition: crate::admin::PARTITION,
        owner: crate::admin::OWNER,
        generation: Generation(3),
        owner_epoch: OwnerEpoch(3),
        config_version: crate::admin::CONFIG_VERSION,
        lifecycle: rdb_core::authority::partition::PartitionLifecycle::Serving,
    };
    let root = PartitionRoot {
        mode: rdb_core::authority::partition::RecordMode::Active,
        owner_boot: BootId(5),
        base_seq: Seq(7),
        base_digest: Digest::ROOT,
        predecessor_generation: Generation(2),
        predecessor_cutoff: Seq(7),
        instance: [7; INSTANCE_LEN],
        run: Revision(1),
    };
    let partitions = Bytes::from(ControlKey::Partition(crate::admin::PARTITION).encode());
    rt.block_on(store.put(PutRequest {
        key: partitions.clone(),
        value: record.encode_with(&root),
        expected_mod_revision: Some(0),
        dedup: None,
    }))
    .expect("put partitions/1");
    let live = GrantRecord {
        grant: GrantId(4),
        node: crate::admin::OWNER,
        boot: open,
        authority_generation: AuthorityGeneration(0),
        expiry_utc_ms: 0,
        frozen: false,
    };
    let grants = Bytes::from(ControlKey::Grant(crate::admin::OWNER).encode());
    let granted = rt
        .block_on(store.put(PutRequest {
            key: grants.clone(),
            value: live.encode(),
            expected_mod_revision: Some(0),
            dedup: None,
        }))
        .expect("put grants/1")
        .revision;

    let (owner, mailbox) = std::sync::mpsc::channel();
    let healed = rt.block_on(crate::admin::heal(crate::admin::HealCtx {
        store: Arc::clone(&store),
        boot: open,
        budgets: rdb_core::contracts::event::Budgets::SPEC_DEFAULTS,
        clock: crate::clock::HostClock::start(),
        owner,
        within: Duration::from_millis(300),
    }));
    let sent: Vec<_> = mailbox.try_iter().collect();

    match &healed {
        Err(crate::admin::HealError::NotCleared { node: 1, last, .. }) => {
            assert_eq!(last, "Current", "{healed:?}");
        }
        other => panic!("expected NotCleared for node 1: {other:?}"),
    }
    assert!(sent.is_empty(), "{} messages sent to node 1", sent.len());
    let kept = rt
        .block_on(store.get(GetRequest { key: grants }))
        .expect("get grants/1")
        .record
        .expect("the live grant is still there");
    assert_eq!(kept.mod_revision, granted, "the live grant was not touched");
}
