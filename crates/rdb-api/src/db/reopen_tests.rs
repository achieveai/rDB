//! dev-w2's reopen rows (`db.rs`'s own `tests` module is dev-w1's):
//! - M10 S1-8 (tester k151, A-R177): a refused reopen tells the app or operator the next step, and
//!   the `Db` logs the refusal as `db_open_refused` with its kind.
//! - M10 R3a case B (A-R171): a reopen whose grant record is gone heals with `NoGrantHeld` and
//!   serves what was written.
//! - M10 S1-5 (#45, A-R182): a reopen whose recovery CAS answers "unknown" settles it by
//!   read-then-CAS, and a reopened `Db` takes a fresh grant in the same boot after a control store
//!   outage (A-R96).

use super::tests::{owner_status, owner_when};
use super::*;

fn config(dir: &Path) -> DbConfig {
    DbConfig {
        ack_floor: true,
        dir: dir.to_path_buf(),
        hold: Vec::new(),
        timeouts: Timeouts::default(),
        dedup_cap: None,
        dedup_retention: None,
        local: ALL_NODES.to_vec(),
        links: None,
        client: DEFAULT_CLIENT,
        boot: None,
        storage: crate::StorageOptions::default(),
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
///
/// Window kind: claims what is reported (A-R244, the A-R237 rule). Every row here claims an
/// outcome (what an open refuses, keeps or reads back), never how long F1's discovery window
/// is, so the window is the spec's 2 s times `deadline_scale()`. Unscaled, one 3.2 s mail step
/// on node 2 under gate load closed it and a reopen went `Blocked` (k435). The product default
/// stays the spec's.
fn fast() -> Budgets {
    Budgets {
        resume_hold_millis: 50,
        warn_age_millis: 200,
        pause_age_millis: 400,
        discovery_window_millis: Budgets::SPEC_DEFAULTS.discovery_window_millis
            * u64::from(config_testkit::poll::deadline_scale()),
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
    assert!(
        !(text.contains('/') && text.contains('\\')),
        "one separator style (A-R230): {text}"
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
    let owner = owner_status(&db, "node 1 after the heal");
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
/// Integration (~6 s at scale 1): one fresh open, a reopen held ~2 s, then one discovery window.
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
    // F1 retries once per discovery window, so the wait outlasts one window by 1 s (A-R244:
    // `fast()` scales the window, and a fixed 3 s saw no retry at scale 3).
    let one_window = Duration::from_millis(fast().discovery_window_millis);
    match db.wait_ready(one_window + Duration::from_secs(1)) {
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
    let before = owner_status(&db, "node 1 after the reopen");
    assert_eq!(before.admits, Some(true), "{before:?}");

    let patience = crate::host::test_patience(Duration::from_secs(30));
    let until = |what: &str, done: &dyn Fn(&crate::host::NodeStatus) -> bool| {
        owner_when(&db, what, patience, done)
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
             different instances: {}; next: the copies come from different folders, so restore \
             this folder's copies from one backup, then open it again",
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
             instance {}, but {key} is absent; next: point `Db::open` at the rEtcd this folder \
             was made with; for a new database, start a separate rEtcd",
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
                let n = admin::count_open(
                    &self.inner,
                    &self.instance,
                    false,
                    crate::host::test_patience(admin::COUNT_WITHIN),
                )
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
/// Unit (~1 ms): `count_open` against a store that races it. `COUNT_WITHIN` is a wait here, not
/// the claim, so it is scaled: on a starved host A's first read, which runs all of B, and its
/// conflicted CAS outlasted 10 s, and A gave up before reading again (A-R307).
#[test]
fn m10_cut_two_opens_counting_at_once_never_get_one_number() {
    let rt = runtime();
    let within = crate::host::test_patience(admin::COUNT_WITHIN);
    let instance = [0x33; INSTANCE_LEN];
    let store = CountRace {
        inner: config_testkit::MemStore::new(),
        key: Bytes::from(admin::instance_key(&instance)),
        instance,
        raced: tokio::sync::Mutex::new(None),
    };
    let first = rt
        .block_on(admin::count_open(&store.inner, &instance, true, within))
        .expect("the first open");
    assert_eq!(first, 1);
    let a = rt
        .block_on(admin::count_open(&store, &instance, false, within))
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
        .block_on(admin::count_open(&store.inner, &instance, false, within))
        .expect("the next open");
    assert_eq!(next, 4);
}

/// Survivor 6 (`MAX_OPENS`): the 2^23-th open is counted, and the one after it is refused
/// `Exhausted` with the counter left as it was. 2^23 is the cap because the last counted open
/// numbers its own writes below 2^63, the first id `app_request_id` builds (A-R195). The
/// counter is seeded with layout 1's bytes (`[1]` then N as a little-endian u64), which this
/// build reads.
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
        format!(
            "{key} has counted 8388608 opens, the most one directory may have; next: start a \
             new folder"
        )
    );
    let after = read();
    assert_eq!(
        (after.mod_revision, after.value),
        (at_cap.mod_revision, at_cap.value),
        "the refused open wrote nothing"
    );
    assert!(
        request_floor(admin::MAX_OPENS) + (1 << 40) <= 1 << 63,
        "the last counted open numbers its writes below every app id"
    );
    assert_eq!(app_request_id(0), Some(RequestId(1 << 63)));
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
        member_boot: Arc::new(move |_| Some(open)),
        owner_node: crate::admin::OWNER,
    }));
    let sent: Vec<_> = mailbox.try_iter().collect();

    match &healed {
        Err(crate::admin::HealError::NotCleared { node: 1, last, .. }) => {
            assert_eq!(
                last,
                "it names the boot this open runs node 1 at, so another open of this folder's \
                 data may be running now, for example a live copy of this folder: shut it down, \
                 then open again",
                "{healed:?}"
            );
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

/// A-R191b (tester w3a03 `heal/walk.out` c1): a reopen whose heal meets the last run's grant
/// still in force, because a live copy of the folder renews it, fails in plain words. The text
/// gives the wait in seconds, the time of day the grant may stay in use until and how far off
/// that is, and the next step: shut the other open down, then open again. Before, it was Rust
/// Debug: `Refused { refusal: NotProvenExpired, until_utc_ms: .. }`. Cut: `clear_until` keeps
/// `format!("{clearance:?}")`. Unit (~300 ms): `MemStore`, the heal's own bound.
#[test]
fn m10_r191b_a_heal_over_a_grant_still_in_use_says_when_and_what_to_do_in_words() {
    use config_core::PutRequest;
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
        owner_boot: BootId(1),
        base_seq: Seq(7),
        base_digest: Digest::ROOT,
        predecessor_generation: Generation(2),
        predecessor_cutoff: Seq(7),
        instance: [7; INSTANCE_LEN],
        run: Revision(1),
    };
    rt.block_on(store.put(PutRequest {
        key: Bytes::from(ControlKey::Partition(crate::admin::PARTITION).encode()),
        value: record.encode_with(&root),
        expected_mod_revision: Some(0),
        dedup: None,
    }))
    .expect("put partitions/1");
    // The last run's grant, renewed a moment ago by the live copy: in force for 90 s more.
    let clock = crate::clock::HostClock::start();
    let expiry_utc_ms = i64::try_from(clock.now().0).expect("a UTC time") + 90_000;
    let held = GrantRecord {
        grant: GrantId(4),
        node: crate::admin::OWNER,
        boot: BootId(1),
        authority_generation: AuthorityGeneration(0),
        expiry_utc_ms,
        frozen: false,
    };
    rt.block_on(store.put(PutRequest {
        key: Bytes::from(ControlKey::Grant(crate::admin::OWNER).encode()),
        value: held.encode(),
        expected_mod_revision: Some(0),
        dedup: None,
    }))
    .expect("put grants/1");

    let budgets = rdb_core::contracts::event::Budgets::SPEC_DEFAULTS;
    let (owner, _mailbox) = std::sync::mpsc::channel();
    let healed = rt.block_on(crate::admin::heal(crate::admin::HealCtx {
        store: Arc::clone(&store),
        boot: BootId((1 << 16) + 1),
        budgets,
        clock,
        owner,
        within: Duration::from_millis(300),
        member_boot: Arc::new(|_| Some(BootId((1 << 16) + 1))),
        owner_node: crate::admin::OWNER,
    }));
    let error = healed.expect_err("a grant still in use is not cleared");
    assert!(
        matches!(error, crate::admin::HealError::NotCleared { node: 1, .. }),
        "{error:?}"
    );

    // The guard's until-time: the expiry plus clock error and dispatch margin, as a time of day.
    let until = expiry_utc_ms
        + i64::try_from(budgets.clock_error_millis + budgets.dispatch_margin_millis)
            .expect("a margin");
    let of_day = until.div_euclid(1000).rem_euclid(86_400);
    let at = format!(
        "{:02}:{:02}:{:02}",
        of_day / 3600,
        of_day / 60 % 60,
        of_day % 60
    );
    let text = error.to_string();
    let head = format!(
        "the last run's grant for node 1 (grants/1) was still held after 0.3 seconds of trying: \
         it may still be in use until {at} UTC (about "
    );
    let tail = " seconds from now), and later if its holder keeps renewing it; another open may \
                hold this folder's data, for example a live copy of this folder: shut it down, \
                then open again";
    let seconds = text
        .strip_prefix(&head)
        .and_then(|rest| rest.strip_suffix(tail))
        .unwrap_or_else(|| panic!("the text differs:\n{text}\nexpected:\n{head}<seconds>{tail}"));
    let seconds: f64 = seconds
        .parse()
        .unwrap_or_else(|_| panic!("{seconds:?} is not a number of seconds: {text}"));
    let margin_s = f64::from(
        u32::try_from(budgets.clock_error_millis + budgets.dispatch_margin_millis)
            .expect("a margin"),
    ) / 1000.0;
    assert!(
        (85.0 + margin_s..=90.0 + margin_s).contains(&seconds),
        "about {seconds} s from now, not about {} s: {text}",
        90.0 + margin_s
    );
}

/// A-R188 (tester r8 `nr`): once node 1 has restarted, the owner stays out until the folder is
/// opened again.
/// `wait_ready` answers at once with `Stopped`, whose text names the restart and says to shut
/// the `Db` down and open the folder again, rather than waiting out its bound; a put and a get
/// refused after it carry the same next step. Taking that step works: the reopen reads the write
/// made before the restart.
/// Integration (~3 s): one fresh open until ready, a put, a restart of node 1, a refused put and
/// get, one reopen until ready.
#[config_log::retcd_test]
fn m10_r188_after_node_1_restarts_wait_ready_put_and_get_say_to_open_the_folder_again() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let mut db = open(&rt, dir.path(), &store).expect("the fresh open");
    db.wait_ready(crate::host::test_patience(Duration::from_secs(10)))
        .expect("ready");
    db.put(b"a", b"1", None).expect("a put");
    assert_eq!(db.restart_node(OWNER).expect("node 1 restarts"), BootId(2));

    let note =
        "the owner, node 1, restarted (now boot 2); the owner stays out until the folder is \
         opened again";
    let step = "nothing is served again in this open: to read or write again, shut this Db down \
                and open the folder again";
    let asked = std::time::Instant::now();
    let stopped = db
        .wait_ready(Duration::from_secs(30))
        .expect_err("node 1 does not admit writes again in this open");
    assert!(
        asked.elapsed() < Duration::from_secs(5),
        "answered at once, not at the bound: {:?}",
        asked.elapsed()
    );
    assert_eq!(
        stopped,
        ReadyError::Stopped {
            note: note.to_owned()
        }
    );
    assert_eq!(stopped.to_string(), format!("{note}; {step}"));

    let put = db
        .put(b"b", b"2", None)
        .expect_err("the put is refused")
        .error;
    let get = db.get(b"a").expect_err("the get is refused");
    for (call, error) in [("put", put), ("get", get)] {
        assert!(
            error.kind == ErrorKind::Unavailable
                && error.detail.ends_with(&format!("; {note}; {step}")),
            "{call}: {error:?}"
        );
    }
    db.shutdown();

    let mut db = open(&rt, dir.path(), &store).expect("the folder opened again");
    db.wait_ready(crate::host::test_patience(Duration::from_secs(10)))
        .expect("the reopen is ready");
    assert_eq!(
        db.get(b"a")
            .expect("the write made before the restart")
            .value
            .map(|(_, value)| value),
        Some(Bytes::from_static(b"1"))
    );
    db.shutdown();
}

/// A-R189: a `GENERATION_CHANGED` refusal names the action. A request sent under generation 1
/// and refused after a reopen moved the partition on says which request, under which
/// generation, to look it up with `Db::status`, and not to resend it as a new request.
/// Integration (~3 s): one fresh open until ready, a put, one reopen until ready, one resend.
#[config_log::retcd_test]
fn m10_r189_a_generation_changed_refusal_says_to_look_the_request_up_with_status() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let mut db = open(&rt, dir.path(), &store).expect("the fresh open");
    db.wait_ready(crate::host::test_patience(Duration::from_secs(10)))
        .expect("ready");
    let sent = *db.put(b"a", b"1", None).expect("a put").sent;
    db.shutdown();

    let mut db = open(&rt, dir.path(), &store).expect("the reopen");
    db.wait_ready(crate::host::test_patience(Duration::from_secs(10)))
        .expect("the reopen is ready");
    let refused = db
        .resend(sent.clone())
        .expect_err("a request sent under generation 1 is refused under generation 2");
    assert_eq!(
        (refused.error.kind, refused.error.detail.as_str()),
        (
            ErrorKind::GenerationChanged,
            "generation changed: expected Generation(1), current Generation(2); request 1 was \
             sent under generation 1 and this send wrote nothing; if it was sent before, look \
             it up with Db::status to see whether an earlier send applied it, and do not resend \
             it as a new request"
        )
    );
    assert_eq!(
        refused.request.as_deref(),
        Some(&sent),
        "handed back unchanged"
    );
    db.shutdown();
}

/// `partitions/1`'s generation now, `None` with no record.
fn stored_generation(rt: &tokio::runtime::Runtime, store: &Arc<dyn ConfigStore>) -> Option<u64> {
    rt.block_on(store.get(config_core::GetRequest {
        key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
    }))
    .expect("get partitions/1")
    .record
    .and_then(|found| PartitionRecord::decode(&found.value))
    .map(|record| record.generation.0)
}

/// review-r184 M1 (k220): a layout-1 open counter (`[1]` then N, written before A-R184) leaves
/// the activated generation unknown, and unknown never rolls forward. A reopen cut short right
/// after its commit leaves `partitions/1` at a generation no copy holds; with the counter put
/// back to layout-1 bytes, the next open is `Stale` naming the unknown mark, rolls nothing
/// forward, and counts nothing. Cut: both unknown arms (`roll_forward_copies`'s by-name guard and
/// `Count::may_have_admitted`'s) answer "never activated", and the open rolls forward.
/// Integration (~5 s): a fresh open, a reopen cut short at its commit, a refused open.
#[config_log::retcd_test]
fn m10_m1_a_layout_1_open_counter_never_rolls_forward() {
    const METHOD: &str = "m10_m1_a_layout_1_open_counter_never_rolls_forward";
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    written_and_closed(&rt, dir.path(), &store);

    crate::host::set_after_commit(dir.path(), Some(crate::host::AfterCommit::Fault));
    let mut db = open(&rt, dir.path(), &store).expect("the reopen cut short");
    let patience = crate::host::test_patience(Duration::from_secs(30));
    let deadline = std::time::Instant::now() + patience;
    while stored_generation(&rt, &store) != Some(2) {
        assert!(
            std::time::Instant::now() < deadline,
            "partitions/1 never named generation 2: {:?}",
            stored_generation(&rt, &store)
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    db.shutdown();
    drop(db);
    crate::host::set_after_commit(dir.path(), None);

    let instance = marker(dir.path(), 1).expect("the copies are bound");
    let key = admin::instance_key(&instance);
    let (count, revision) = rt
        .block_on(admin::read_count(&*store, &instance))
        .expect("the counter");
    let mut layout_1 = vec![1_u8];
    layout_1.extend_from_slice(&count.n.to_le_bytes());
    rt.block_on(store.put(config_core::PutRequest {
        key: Bytes::from(key.clone()),
        value: Bytes::from(layout_1),
        expected_mod_revision: Some(revision),
        dedup: None,
    }))
    .expect("the counter as layout 1");
    let (seeded, seeded_at) = rt
        .block_on(admin::read_count(&*store, &instance))
        .expect("layout 1 reads");
    assert_eq!(
        (seeded.n, seeded.activated),
        (count.n, admin::ACTIVATED_UNKNOWN)
    );

    let refused = match open(&rt, dir.path(), &store) {
        Err(error @ OpenError::Stale { generation: 2, .. }) => error,
        other => panic!("a layout-1 counter is Stale, not {:?}", other.map(|_| "Db")),
    };
    assert!(
        refused
            .to_string()
            .contains("the open counter predates activation marks, so it may have admitted writes"),
        "{refused}"
    );
    let rolled: Vec<_> = config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
        .into_iter()
        .filter(|line| line["@m"] == "db_open_rolled_forward")
        .collect();
    assert!(rolled.is_empty(), "{rolled:?}");
    assert_eq!(
        rt.block_on(admin::read_count(&*store, &instance))
            .expect("the counter"),
        (seeded, seeded_at),
        "the refused open counted nothing"
    );
}

/// review-r184 A3 (k220): open counter bytes this build does not write are `Unreadable`, never
/// a count. Layout 2 is exactly 17 bytes and layout 1 exactly 9; any other length, or any other
/// version byte, is refused by both the read a reopen decides on and the count an open makes,
/// and neither writes. An older build reads layout 2 by the same rule: its decoder knows only
/// version byte 1. Cuts: layout 2 accepts 16 or more bytes after its version byte; layout 1
/// accepts any version byte.
/// Unit (~1 ms): `MemStore`.
#[test]
fn m10_a3_open_counter_bytes_this_build_does_not_write_are_unreadable() {
    let rt = runtime();
    let n = 5_u64.to_le_bytes();
    let activated = 3_u64.to_le_bytes();
    let bytes = |version: u8, parts: &[&[u8]]| {
        let mut out = vec![version];
        for part in parts {
            out.extend_from_slice(part);
        }
        out
    };
    let cases = [
        ("layout 2, one byte short", bytes(2, &[&n, &activated[..7]])),
        ("layout 2, one byte long", bytes(2, &[&n, &activated, &[0]])),
        ("layout 2, N only", bytes(2, &[&n])),
        ("layout 1, one byte short", bytes(1, &[&n[..7]])),
        (
            "layout 1, with an activated generation",
            bytes(1, &[&n, &activated]),
        ),
        ("version 3, layout 2's length", bytes(3, &[&n, &activated])),
        ("version 3, layout 1's length", bytes(3, &[&n])),
        ("version 0", bytes(0, &[&n])),
        ("empty", Vec::new()),
    ];
    for (case, value) in cases {
        let store = config_testkit::MemStore::new();
        let instance = [0x55; INSTANCE_LEN];
        let key = admin::instance_key(&instance);
        let written = rt
            .block_on(store.put(config_core::PutRequest {
                key: Bytes::from(key.clone()),
                value: Bytes::from(value.clone()),
                expected_mod_revision: None,
                dedup: None,
            }))
            .expect("seed")
            .revision;
        let unreadable = admin::CounterError::Unreadable {
            key: key.clone(),
            revision: written,
        };
        assert_eq!(
            rt.block_on(admin::read_count(&store, &instance)),
            Err(unreadable.clone()),
            "{case}: read_count"
        );
        assert_eq!(
            rt.block_on(admin::count_open(
                &store,
                &instance,
                false,
                admin::COUNT_WITHIN
            )),
            Err(unreadable),
            "{case}: count_open"
        );
        let after = rt
            .block_on(store.get(config_core::GetRequest {
                key: Bytes::from(key),
            }))
            .expect("get")
            .record
            .expect("still there");
        assert_eq!(
            (after.mod_revision, after.value.to_vec()),
            (written, value),
            "{case}: nothing was written"
        );
    }
}

/// A fresh open, ready: the `Db` an `_as` row sends on.
fn ready_db(rt: &tokio::runtime::Runtime, dir: &Path, store: &Arc<dyn ConfigStore>) -> Db {
    let db = open(rt, dir, store).expect("the fresh open");
    db.wait_ready(crate::host::test_patience(Duration::from_secs(10)))
        .expect("ready");
    db
}

/// The id an app chose and kept before sending, built as an app builds it (A-R195).
const CHOSEN: RequestId = match app_request_id(0x00AB_CDEF) {
    Some(id) => id,
    None => panic!("below 2^63"),
};

/// A-R194: `txn_as` sends under the id the app chose, so the app can keep that id before it
/// sends and ask `status(id, gen)` afterwards, after a crash included. The answer names the
/// chosen id, status reports it applied at the same position, and both puts read back. Cut:
/// `txn_as` sends under `self.identity()`, and status of the chosen id is not `Resolved`.
/// Integration (~2 s): one fresh open until ready, one transaction.
#[config_log::retcd_test]
fn m10_r194_a_txn_sent_under_a_chosen_id_is_found_by_status_under_that_id() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let mut db = ready_db(&rt, dir.path(), &store);

    let puts = [
        TxnPut {
            group: GROUP,
            object: b"a",
            value: b"1",
            if_version: None,
        },
        TxnPut {
            group: GROUP,
            object: b"b",
            value: b"2",
            if_version: None,
        },
    ];
    let ok = db
        .txn_as(CHOSEN, &puts, Duration::from_secs(5))
        .expect("the txn");
    assert_eq!(ok.request, CHOSEN, "the answer names the chosen id");
    match db
        .status(CHOSEN, Some(ok.generation))
        .expect("status of the chosen id")
    {
        TxnStatus::Resolved(result) => assert_eq!(
            (result.generation, result.seq),
            (ok.generation, ok.seq),
            "{result:?}"
        ),
        other => panic!("status of the chosen id: {other:?}"),
    }
    for (object, value) in [(b"a", b"1"), (b"b", b"2")] {
        assert_eq!(
            db.get(object).expect("get").value.map(|(_, value)| value),
            Some(Bytes::copy_from_slice(value))
        );
    }
    db.shutdown();
}

/// A-R194: `write_as` sends under the id the app chose, as `txn_as` does. Cut: `write_as` sends
/// under `self.identity()`, and status of the chosen id is not `Resolved`.
/// Integration (~2 s): one fresh open until ready, one write.
#[config_log::retcd_test]
fn m10_r194_a_write_sent_under_a_chosen_id_is_found_by_status_under_that_id() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let mut db = ready_db(&rt, dir.path(), &store);

    let ok = db
        .write_as(CHOSEN, b"a", byte_doc(b"1"), Some(Expected::Absent))
        .expect("the write")
        .expect("a create is never unchanged");
    assert_eq!(ok.request, CHOSEN, "the answer names the chosen id");
    match db
        .status(CHOSEN, Some(ok.generation))
        .expect("status of the chosen id")
    {
        TxnStatus::Resolved(result) => assert_eq!(
            (result.generation, result.seq),
            (ok.generation, ok.seq),
            "{result:?}"
        ),
        other => panic!("status of the chosen id: {other:?}"),
    }
    assert_eq!(
        db.get(b"a").expect("get a").value.map(|(_, value)| value),
        Some(Bytes::from_static(b"1"))
    );
    db.shutdown();
}

/// Ruling A-R195 (D11): an app's id never names a write this `Db` numbered itself. A put draws
/// its own id k. `put_as` under the bare number k is refused `INVALID_ARGUMENT` with nothing
/// sent, its next step naming `app_request_id`; under `app_request_id(k)` it is a write of its
/// own, and status finds each id at its own position, with both values read back. Cut: the
/// `_as` check is dropped, and the bare k meets the put's own request (not `INVALID_ARGUMENT`).
/// Integration (~2 s): one fresh open until ready, three puts.
#[config_log::retcd_test]
fn m10_r195_an_app_id_never_names_a_write_this_db_numbered_itself() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let mut db = ready_db(&rt, dir.path(), &store);

    let auto = db.put(b"a", b"1", None).expect("put a=1");
    let k = auto.request.0;
    let bare = db
        .put_as(RequestId(k), b"b", b"2", None)
        .expect_err("the Db's own id");
    assert_eq!(
        (bare.error.kind, bare.request.is_none()),
        (ErrorKind::InvalidArgument, true),
        "{bare:?}"
    );
    assert!(
        bare.error
            .detail
            .ends_with("nothing was sent: build the id with rdb_api::app_request_id(n)"),
        "{bare:?}"
    );
    assert_eq!(db.get(b"b").expect("get b").value, None, "nothing was sent");

    let app = app_request_id(k).expect("k is below 2^63");
    assert_ne!(app, auto.request);
    let mine = db
        .put_as(app, b"b", b"2", None)
        .expect("put b=2 under the app's id");
    assert_eq!(mine.request, app);
    for (id, ok) in [(auto.request, &auto), (app, &mine)] {
        match db.status(id, Some(ok.generation)).expect("status") {
            TxnStatus::Resolved(result) => assert_eq!(
                (result.generation, result.seq),
                (ok.generation, ok.seq),
                "request {}: {result:?}",
                id.0
            ),
            other => panic!("status of request {}: {other:?}", id.0),
        }
    }
    assert_ne!(auto.seq, mine.seq, "two writes, not one replayed");
    for (object, value) in [(b"a", b"1"), (b"b", b"2")] {
        assert_eq!(
            db.get(object).expect("get").value.map(|(_, value)| value),
            Some(Bytes::copy_from_slice(value))
        );
    }
    db.shutdown();
}

// dev-w5's slice-2 rows (arch-s2 B1, rulings A-R226 h2, A-R231): a `Db` that runs no node, a
// node process that opens one copy, and an undecodable `partitions/1` (tester-s2).

/// An app-only config: no node of its own, `links` to reach them, and its own client.
fn app_config(dir: &Path, links: &Arc<Links>, client: ClientId) -> DbConfig {
    DbConfig {
        local: Vec::new(),
        links: Some(Arc::clone(links)),
        client,
        ..config(dir)
    }
}

fn open_app(
    rt: &tokio::runtime::Runtime,
    store: &Arc<dyn ConfigStore>,
    links: &Arc<Links>,
    client: ClientId,
) -> Db {
    let dir = config_testkit::fs::temp_dir();
    rt.block_on(Db::open_with(
        app_config(dir.path(), links, client),
        Arc::clone(store),
        rt.handle().clone(),
        fast(),
    ))
    .expect("the app-only open")
}

/// A config that runs only `nodes` of the folder at `dir`: a node process (A-R231(a)).
fn node_config(dir: &Path, nodes: &[u32]) -> DbConfig {
    DbConfig {
        local: nodes.iter().map(|node| NodeId(*node)).collect(),
        links: Some(Arc::new(Links::default())),
        ..config(dir)
    }
}

fn open_node(
    rt: &tokio::runtime::Runtime,
    dir: &Path,
    store: &Arc<dyn ConfigStore>,
    nodes: &[u32],
) -> Result<Db, OpenError> {
    rt.block_on(Db::open_with(
        node_config(dir, nodes),
        Arc::clone(store),
        rt.handle().clone(),
        fast(),
    ))
}

/// The messages this test logged, in order.
fn messages(method: &str) -> Vec<String> {
    config_testkit::logs::lines_for_current_test(module_path!(), method)
        .into_iter()
        .filter_map(|line| line["@m"].as_str().map(str::to_owned))
        .collect()
}

/// A-R226 h2: a `Db` with `local = []` runs no node, takes the owner from `partitions/1`,
/// counts one open, and numbers its writes above that open's floor. Its put goes over the
/// links to node 1 of the `Db` that runs the nodes, which reads it back, as does the app.
/// Integration (~1 s): one fresh open until ready.
#[config_log::retcd_test]
fn m10_s2_h2_an_app_only_db_writes_over_the_links_and_the_node_db_reads_it() {
    const METHOD: &str = "m10_s2_h2_an_app_only_db_writes_over_the_links_and_the_node_db_reads_it";
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let mut nodes = open(&rt, dir.path(), &store).expect("the fresh open");
    nodes
        .wait_ready(crate::host::test_patience(Duration::from_secs(10)))
        .expect("the fresh open is ready");

    let mut app = open_app(&rt, &store, nodes.links(), ClientId(2));
    assert!(app.node_status().is_empty(), "the app runs no node");
    let put = app.put(b"a", b"1", None).expect("the app's put");
    assert_eq!(
        put.request,
        RequestId(request_floor(2) + 1),
        "the app's open is the folder's second, so its ids start above floor(2)"
    );
    for (who, db) in [("node db", &nodes), ("app", &app)] {
        assert_eq!(
            db.get(b"a").expect("get a").value.map(|(_, v)| v),
            Some(Bytes::from_static(b"1")),
            "{who} reads the app's write"
        );
    }
    app.shutdown();
    nodes.shutdown();
    let logged = messages(METHOD);
    assert!(logged.iter().any(|m| m == "db_open_app"), "{logged:?}");
}

/// A-R231(b): the pilot restarts under the same client id. Its second open counts again, so
/// its first put has a fresh request id: it lands as a new write, never `REQUEST_ID_REUSE`
/// against the first open's put. Integration (~1 s): one fresh open until ready.
#[config_log::retcd_test]
fn m10_s2_h2_an_app_reopened_under_the_same_client_writes_with_fresh_ids() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let mut nodes = open(&rt, dir.path(), &store).expect("the fresh open");
    nodes
        .wait_ready(crate::host::test_patience(Duration::from_secs(10)))
        .expect("the fresh open is ready");

    let mut first = open_app(&rt, &store, nodes.links(), ClientId(2));
    let one = first.put(b"a", b"1", None).expect("the first open's put");
    first.shutdown();
    drop(first);
    let mut second = open_app(&rt, &store, nodes.links(), ClientId(2));
    let two = second
        .put(b"a", b"2", None)
        .unwrap_or_else(|e| panic!("the restarted app's first put is fresh: {e}"));
    assert!(two.request > one.request, "{one:?} then {two:?}");
    assert!(
        two.seq > one.seq,
        "two writes, not one replayed: {one:?} {two:?}"
    );
    assert_eq!(
        nodes.get(b"a").expect("get a").value.map(|(_, v)| v),
        Some(Bytes::from_static(b"2"))
    );
    second.shutdown();
    nodes.shutdown();
}

/// A-R226 h2, A-R233: an app `Db` routed to node 2, a live secondary of the trio, is refused
/// `NOT_PRIMARY` (rule `RefreshRoute`, nothing sent), re-reads the owner from `partitions/1`,
/// and its retry lands on node 1. The `Db` does not retry by itself: the app does.
/// Integration (~1 s): one fresh open until ready.
#[config_log::retcd_test]
fn m10_s2_h2_not_primary_refreshes_the_app_route() {
    const METHOD: &str = "m10_s2_h2_not_primary_refreshes_the_app_route";
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let mut nodes = open(&rt, dir.path(), &store).expect("the fresh open");
    nodes
        .wait_ready(crate::host::test_patience(Duration::from_secs(10)))
        .expect("the fresh open is ready");

    let mut app = open_app(&rt, &store, nodes.links(), ClientId(2));
    app.route.owner.store(2, Ordering::Relaxed);
    let refused = app
        .put(b"a", b"1", None)
        .expect_err("node 2 is not the owner");
    assert_eq!(
        (
            refused.error.kind,
            refused.error.retry,
            refused.error.no_mutation
        ),
        (ErrorKind::NotPrimary, RetryRule::RefreshRoute, true),
        "{refused}"
    );
    assert_eq!(app.route.owner(), NodeId(1), "the route was refreshed");
    let landed = app
        .put(b"a", b"1", None)
        .expect("the retry lands on node 1");
    assert_eq!(
        nodes.get(b"a").expect("get a").value,
        Some((landed.seq.0, Bytes::from_static(b"1"))),
        "{landed:?}"
    );
    app.shutdown();
    nodes.shutdown();
    let logged = messages(METHOD);
    for event in ["client_compile_not_primary", "db_owner_refreshed"] {
        assert!(logged.iter().any(|m| m == event), "{event}: {logged:?}");
    }
}

/// Defect tester-s2: `partitions/1` holding bytes this build cannot decode is its own
/// refusal naming the key, never "the store has no partitions/1". Both a folder's reopen and
/// an app-only open say so; the reopen changed no data, so with the record put back the
/// write made before reads back. Integration (~4 s): one fresh open until ready, two refused
/// opens, one reopen until ready.
#[config_log::retcd_test]
fn m10_s2_an_undecodable_partition_record_is_refused_by_its_key_and_changes_nothing() {
    const METHOD: &str =
        "m10_s2_an_undecodable_partition_record_is_refused_by_its_key_and_changes_nothing";
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    written_and_closed(&rt, dir.path(), &store);

    let key = Bytes::from(ControlKey::Partition(PARTITION).encode());
    let found = rt
        .block_on(store.get(config_core::GetRequest { key: key.clone() }))
        .expect("read partitions/1")
        .record
        .expect("partitions/1 exists");
    let damaged = rt
        .block_on(store.put(config_core::PutRequest {
            key: key.clone(),
            value: Bytes::from_static(b"not a record"),
            expected_mod_revision: Some(found.mod_revision),
            dedup: None,
        }))
        .expect("damage partitions/1");

    let reopen = match open(&rt, dir.path(), &store) {
        Err(error @ OpenError::Undecodable { .. }) => error,
        other => panic!(
            "damaged bytes are Undecodable, not {:?}",
            other.map(|_| "Db")
        ),
    };
    let links = Arc::new(Links::default());
    let app = match rt.block_on(Db::open_with(
        app_config(dir.path(), &links, ClientId(2)),
        Arc::clone(&store),
        rt.handle().clone(),
        fast(),
    )) {
        Err(error @ OpenError::Undecodable { .. }) => error,
        other => panic!(
            "damaged bytes are Undecodable, not {:?}",
            other.map(|_| "Db")
        ),
    };
    for (who, refused) in [("reopen", &reopen), ("app", &app)] {
        let text = refused.to_string();
        assert!(
            text.starts_with("partitions/1 in the control store does not decode: ")
                && text.contains("This open changed nothing"),
            "{who}: the text names the key and says nothing changed: {text}"
        );
        assert!(!text.contains("no partitions/1"), "{who}: {text}");
    }

    rt.block_on(store.put(config_core::PutRequest {
        key,
        value: found.value.clone(),
        expected_mod_revision: Some(damaged.revision),
        dedup: None,
    }))
    .expect("put partitions/1 back");
    let mut db = open(&rt, dir.path(), &store).expect("the reopen after the record is back");
    db.wait_ready(crate::host::test_patience(Duration::from_secs(30)))
        .expect("the reopen is ready");
    assert_eq!(
        db.get(b"a").expect("read a back").value.map(|(_, v)| v),
        Some(Bytes::from_static(b"1")),
        "the acknowledged write is intact after the refusal"
    );
    db.shutdown();
    assert_eq!(
        refusals(METHOD),
        vec![
            ("Warning".to_owned(), "Undecodable".to_owned()),
            ("Warning".to_owned(), "Undecodable".to_owned())
        ],
        "one db_open_refused per refused open, naming the kind"
    );
}

/// #59 (tester-s2), A-R230 Q3: a folder opened against a store that does not hold its records
/// is refused with what was found and the step: point the open at the rEtcd it was made with.
/// Folder A against an empty store finds no `partitions/1`; against folder B's store it finds
/// B's instance. Both leave every store as it was. Cut: `check` keeps the old texts, which end
/// at what was found. Integration (~3 s): two fresh opens until ready, two refused opens.
#[test]
fn m10_s2_a_folder_opened_against_another_store_says_to_open_it_against_its_own() {
    let rt = runtime();
    let ours: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let theirs: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let empty: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let a = config_testkit::fs::temp_dir();
    let b = config_testkit::fs::temp_dir();
    written_and_closed(&rt, a.path(), &ours);
    written_and_closed(&rt, b.path(), &theirs);
    let instance_a = marker(a.path(), 1).expect("folder A names its instance");
    let instance_b = marker(b.path(), 1).expect("folder B names its instance");
    let step = "; next: point `Db::open` at the rEtcd this folder was made with; for a new \
                database, start a separate rEtcd";
    let prefix = "this directory and the control store do not belong together: ";

    for (store, found) in [
        (
            &empty,
            "the copies hold data, but the store has no partitions/1".to_owned(),
        ),
        (
            &theirs,
            format!(
                "the copies name instance {}, partitions/1 names {}",
                admin::instance_hex(&instance_a),
                admin::instance_hex(&instance_b)
            ),
        ),
    ] {
        let revision = rt
            .block_on(store.get(config_core::GetRequest {
                key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
            }))
            .expect("read partitions/1")
            .read_revision;
        match open(&rt, a.path(), store) {
            Err(error @ OpenError::Mismatch(_)) => {
                assert_eq!(error.to_string(), format!("{prefix}{found}{step}"));
            }
            other => panic!("Mismatch, not {:?}", other.map(|_| "Db")),
        }
        let after = rt
            .block_on(store.get(config_core::GetRequest {
                key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
            }))
            .expect("read partitions/1 again")
            .read_revision;
        assert_eq!(
            after, revision,
            "the refused open wrote nothing to the store: {found}"
        );
    }
    assert_eq!(
        marker(a.path(), 1),
        Some(instance_a),
        "folder A is as it was"
    );
}

/// A-R231(a), narrowed by A-R328: a node process started on a folder while the control store
/// holds no `partitions/1` is refused before it creates anything, and says the database is not
/// formed and to run lr up. Unit (~20 ms): nothing opens.
#[config_log::retcd_test]
fn m10_s2_a_node_process_on_an_unformed_folder_says_to_form_it_first() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let refused = match open_node(&rt, dir.path(), &store, &[2]) {
        Err(error @ OpenError::Config(_)) => error,
        other => panic!(
            "an unformed folder is Config, not {:?}",
            other.map(|_| "Db")
        ),
    };
    let text = refused.to_string();
    assert!(
        text.contains(
            "database not formed: the control store holds no partitions/1, so node 2 has no \
             database to rejoin. Run lr up to form it"
        ),
        "{text}"
    );
    assert!(
        !(text.contains('/') && text.contains('\\')),
        "one separator style (A-R230): {text}"
    );
    assert!(
        !dir.path().join("nodes").join("2").exists(),
        "no copy was created"
    );
}

/// A-R231(a), narrowed by A-R328: a node process whose copy exists but holds no data, while
/// the control store holds no `partitions/1`, is refused before it binds or bootstraps, and
/// says the database is not formed. Unit (~100 ms): one empty RocksDB copy.
#[config_log::retcd_test]
fn m10_s2_a_node_process_on_an_empty_copy_says_to_form_it_first() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let copy = dir.path().join("nodes").join("2");
    drop(RocksEngine::open(&copy).expect("an empty copy"));
    let refused = match open_node(&rt, dir.path(), &store, &[2]) {
        Err(error @ OpenError::Config(_)) => error,
        other => panic!("an empty copy is Config, not {:?}", other.map(|_| "Db")),
    };
    let text = refused.to_string();
    assert!(
        text.contains("database not formed: the control store holds no partitions/1, so node 2"),
        "{text}"
    );
    let partitions = rt
        .block_on(store.get(config_core::GetRequest {
            key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
        }))
        .expect("read partitions/1");
    assert!(partitions.record.is_none(), "nothing was bootstrapped");
}

/// `partitions/1`'s raw record in `store`, with its revisions.
fn partitions_record(
    rt: &tokio::runtime::Runtime,
    store: &Arc<dyn ConfigStore>,
) -> Option<config_core::Record> {
    rt.block_on(store.get(config_core::GetRequest {
        key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
    }))
    .expect("read partitions/1")
    .record
}

/// A-R328 (tester D6): after formation, node 2's copy is wiped (`lr start 2 --wipe`). Its
/// process creates an empty copy, binds the formed database's instance, and starts it as a
/// behind copy that holds no lineage until F1 rebuilds it. It never forms or bootstraps: the
/// open writes nothing to `partitions/1`, and starts no heal. Cut: the old A-R231(a) refusal
/// ("holds no copy of node 2: form the database first"), and this open is refused `Config`.
/// Integration (~1 s): one fresh open until ready, one node-process open.
#[config_log::retcd_test]
fn m10_s2_a_wiped_member_copy_rejoins_the_formed_database_as_a_behind_copy() {
    const METHOD: &str = "m10_s2_a_wiped_member_copy_rejoins_the_formed_database_as_a_behind_copy";
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    written_and_closed(&rt, dir.path(), &store);
    let instance = marker(dir.path(), 1).expect("the formed database's instance");
    let before = partitions_record(&rt, &store).expect("partitions/1 after formation");
    std::fs::remove_dir_all(dir.path().join("nodes").join("2")).expect("wipe node 2's copy");

    let mut db = open_node(&rt, dir.path(), &store, &[2]).expect("node 2's process opens");
    let running: Vec<NodeId> = lock(&db.nodes).iter().map(|handle| handle.node).collect();
    assert_eq!(running, vec![NodeId(2)], "it runs node 2 only");
    let status = db.inspect(NodeId(2)).expect("node 2 answers");
    assert_eq!(
        status.holds, None,
        "the copy holds no lineage until F1 rebuilds it: {status:?}"
    );
    db.shutdown();
    assert_eq!(
        marker(dir.path(), 2),
        Some(instance),
        "the empty copy is bound to the formed database's instance"
    );
    let after = partitions_record(&rt, &store).expect("partitions/1 after the join");
    assert_eq!(
        (after.mod_revision, after.value),
        (before.mod_revision, before.value),
        "the join wrote nothing to partitions/1"
    );
    let logged = messages(METHOD);
    assert!(
        logged.iter().any(|m| m == "db_open_empty_copy_joining"),
        "{logged:?}"
    );
    assert!(
        !logged.iter().any(|m| m == "db_heal_started"),
        "node 2 is not the owner, so its process heals nothing: {logged:?}"
    );
}

/// A-R328: `partitions/1` exists but names no instance yet (the first open has not finished).
/// A node process on an empty folder is refused before it creates anything, and is told to wait
/// for that open. Unit (~20 ms): nothing opens.
#[config_log::retcd_test]
fn m10_s2_a_node_process_before_the_first_open_finishes_is_told_to_wait() {
    use rdb_core::contracts::ids::{Generation, OwnerEpoch};
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let record = PartitionRecord {
        partition: PARTITION,
        owner: OWNER,
        generation: Generation(0),
        owner_epoch: OwnerEpoch(0),
        config_version: admin::CONFIG_VERSION,
        lifecycle: rdb_core::authority::partition::PartitionLifecycle::Serving,
    };
    rt.block_on(store.put(config_core::PutRequest {
        key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
        value: record.encode(),
        expected_mod_revision: Some(0),
        dedup: None,
    }))
    .expect("put partitions/1 with no root");
    let dir = config_testkit::fs::temp_dir();
    let refused = match open_node(&rt, dir.path(), &store, &[3]) {
        Err(error @ OpenError::Config(_)) => error,
        other => panic!("Config, not {:?}", other.map(|_| "Db")),
    };
    assert!(
        refused.to_string().contains(
            "database not formed yet: partitions/1 names no instance. Wait for its first open \
             (lr up) to finish, then start node 3 again"
        ),
        "{refused}"
    );
    assert!(
        !dir.path().join("nodes").join("3").exists(),
        "no copy was created"
    );
}

/// A-R328: a node that is not a member of partition 1 has no copy to rebuild, and the refusal
/// names the members. `local_nodes` refuses such a node before any open reaches the check, so
/// this calls the check itself. Integration (~1 s): one fresh open until ready.
#[config_log::retcd_test]
fn m10_s2_a_node_that_is_not_a_member_is_refused_with_the_members() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    written_and_closed(&rt, dir.path(), &store);
    match rt.block_on(formed_member(&store, &[NodeId(2), NodeId(4)])) {
        Err(OpenError::Config(text)) => assert_eq!(
            text,
            "node 4 is not a member of partition 1 (its members are nodes 1, 2, 3), so it holds \
             no copy to rebuild"
        ),
        other => panic!("Config, not {other:?}"),
    }
    let (record, instance) = rt
        .block_on(formed_member(&store, &[NodeId(2)]))
        .expect("node 2 is a member");
    assert_eq!(
        (record.owner, Some(instance)),
        (OWNER, marker(dir.path(), 1)),
        "a member's check returns the owner and the formed instance"
    );
}

/// A-R328: an empty copy already bound to another database's instance does not join this one.
/// The open is refused `Mismatch` and the copy keeps its own binding. Integration (~1 s): one
/// fresh open until ready, one refused node-process open.
#[config_log::retcd_test]
fn m10_s2_an_empty_copy_bound_to_another_instance_is_refused() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    written_and_closed(&rt, dir.path(), &store);
    let formed = marker(dir.path(), 1).expect("the formed database's instance");
    let copy = dir.path().join("nodes").join("2");
    std::fs::remove_dir_all(&copy).expect("wipe node 2's copy");
    let other = [9; INSTANCE_LEN];
    {
        let mut engine = RocksEngine::open(&copy).expect("an empty copy");
        engine.bind_instance(other).expect("bind another instance");
    }
    match open_node(&rt, dir.path(), &store, &[2]) {
        Err(OpenError::Mismatch(text)) => assert_eq!(
            text,
            another_database(
                &other,
                &format!("instance {}", admin::instance_hex(&formed))
            )
        ),
        other => panic!("Mismatch, not {:?}", other.map(|_| "Db")),
    }
    assert_eq!(
        marker(dir.path(), 2),
        Some(other),
        "the copy keeps its binding"
    );
}

/// A-R328: the owner's own copy, wiped, is not rejoined as a behind copy: nothing could rebuild
/// it while `partitions/1` names it owner. The refusal says to promote another node first, and
/// the empty copy stays unbound. Integration (~1 s): one fresh open until ready, one refused
/// node-process open.
#[config_log::retcd_test]
fn m10_s2_the_owners_wiped_copy_is_refused_with_a_promote_hint() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    written_and_closed(&rt, dir.path(), &store);
    std::fs::remove_dir_all(dir.path().join("nodes").join("1")).expect("wipe node 1's copy");
    match open_node(&rt, dir.path(), &store, &[1]) {
        Err(OpenError::Config(text)) => assert_eq!(
            text,
            "the copy of node 1 holds no data, and partition 1 names it owner: promote another \
             node first (lr promote), then start node 1 again"
        ),
        other => panic!("Config, not {:?}", other.map(|_| "Db")),
    }
    assert_eq!(marker(dir.path(), 1), None, "the empty copy stays unbound");
}

/// A-R231(a), B1 case 1: after formation, node 2's process opens its own copy only. It is not
/// the owner `partitions/1` names, so it starts no heal; node 1's process heals.
/// Integration (~1 s): one fresh open until ready, one node-process open.
#[config_log::retcd_test]
fn m10_s2_a_non_owner_node_process_opens_its_copy_without_a_heal() {
    const METHOD: &str = "m10_s2_a_non_owner_node_process_opens_its_copy_without_a_heal";
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    written_and_closed(&rt, dir.path(), &store);

    let mut db = open_node(&rt, dir.path(), &store, &[2]).expect("node 2's process opens");
    let running: Vec<NodeId> = lock(&db.nodes).iter().map(|handle| handle.node).collect();
    assert_eq!(running, vec![NodeId(2)], "it runs node 2 only");
    db.shutdown();
    let logged = messages(METHOD);
    assert!(
        logged.iter().any(|m| m == "db_open_not_owner"),
        "{logged:?}"
    );
    assert!(
        !logged.iter().any(|m| m == "db_heal_started"),
        "neither open heals: the first is fresh, the second is not the owner: {logged:?}"
    );
}

/// The lr up race (arch-s2 B1 case 2): node 3's process starts after the owner's reopen has
/// moved the database to generation 2 and the open counter marks it activated. Its copy still
/// holds only what came before, so it is behind, not stale: the open passes as joining, runs
/// node 3 without a heal, and logs `db_open_joining`. Cut: no joining branch, and this open is
/// refused `Stale` ("the open counter marks generation 2 activated").
/// Integration (~2 s): one fresh open until ready, one reopen until ready, one node-process
/// open.
#[config_log::retcd_test]
fn m10_s2_a_node_process_whose_copy_is_behind_an_activated_generation_joins() {
    const METHOD: &str = "m10_s2_a_node_process_whose_copy_is_behind_an_activated_generation_joins";
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    written_and_closed(&rt, dir.path(), &store);
    let third = dir.path().join("nodes").join("3");
    let behind = config_testkit::fs::temp_dir();
    copy_dir(&third, behind.path());

    let mut db = open(&rt, dir.path(), &store).expect("the reopen");
    db.wait_ready(crate::host::test_patience(Duration::from_secs(10)))
        .expect("the reopen is ready");
    db.put(b"b", b"2", None).expect("a put at generation 2");
    db.shutdown();
    drop(db);
    let instance = marker(dir.path(), 1).expect("the copies are bound");
    let (count, _) = rt
        .block_on(admin::read_count(&*store, &instance))
        .expect("the counter");
    assert_eq!(
        (stored_generation(&rt, &store), count.activated),
        (Some(2), Generation(2)),
        "partitions/1 names generation 2 and the counter marks it activated"
    );

    std::fs::remove_dir_all(&third).expect("remove node 3's copy");
    copy_dir(behind.path(), &third);
    let mut db = open_node(&rt, dir.path(), &store, &[3]).expect("node 3's process joins");
    let running: Vec<NodeId> = lock(&db.nodes).iter().map(|handle| handle.node).collect();
    assert_eq!(running, vec![NodeId(3)], "it runs node 3 only");
    db.shutdown();
    let joined: Vec<_> = config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
        .into_iter()
        .filter(|line| line["@m"] == "db_open_joining")
        .collect();
    assert_eq!(joined.len(), 1, "{joined:?}");
    assert_eq!(joined[0]["generation"], 2, "{joined:?}");
    let logged = messages(METHOD);
    assert!(
        !logged.iter().any(|m| m == "db_open_rolled_forward"),
        "a joining copy is not rolled forward: {logged:?}"
    );
}

/// A-R231(a): `DbConfig::local` names each of nodes 1-3 at most once, and a `Db` that runs no
/// node needs `links`. Each is refused before anything opens. Unit (~10 ms).
#[config_log::retcd_test]
fn m10_s2_a_repeated_or_unknown_local_node_and_an_app_open_without_links_are_refused() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    for nodes in [&[2, 2][..], &[4][..]] {
        match open_node(&rt, dir.path(), &store, nodes) {
            Err(OpenError::Config(text)) => assert!(
                text.contains("name each of nodes 1, 2 and 3 at most once"),
                "{nodes:?}: {text}"
            ),
            other => panic!("{nodes:?} is Config, not {:?}", other.map(|_| "Db")),
        }
    }
    let no_links = DbConfig {
        local: Vec::new(),
        ..config(dir.path())
    };
    match rt.block_on(Db::open_with(
        no_links,
        Arc::clone(&store),
        rt.handle().clone(),
        fast(),
    )) {
        Err(OpenError::Config(text)) => assert!(text.contains("set links"), "{text}"),
        other => panic!("no links is Config, not {:?}", other.map(|_| "Db")),
    }
    assert!(!dir.path().join("nodes").exists(), "nothing was created");
}

/// arch-s2 B1 case 1, A-R231(a), A-R232: after formation, three node processes, here three
/// `Db`s on one `Links`, each open only their own copy. Node 1's process is the owner and
/// heals; its heal pins nodes 2 and 3 at the boots their opens registered, so the trio is
/// ready, the write made before reads back, and new writes land, also from an app-only `Db`.
/// Integration (~2 s): one fresh open until ready, three node opens until ready.
#[config_log::retcd_test]
fn m10_s2_three_node_processes_on_one_folder_heal_and_serve() {
    const METHOD: &str = "m10_s2_three_node_processes_on_one_folder_heal_and_serve";
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    written_and_closed(&rt, dir.path(), &store);

    let links = Links::new();
    let mut nodes: Vec<Db> = [1, 2, 3]
        .into_iter()
        .map(|node| {
            rt.block_on(Db::open_with(
                DbConfig {
                    links: Some(Arc::clone(&links)),
                    ..node_config(dir.path(), &[node])
                },
                Arc::clone(&store),
                rt.handle().clone(),
                fast(),
            ))
            .unwrap_or_else(|e| panic!("node {node}'s process opens: {e}"))
        })
        .collect();
    nodes[0]
        .wait_ready(crate::host::test_patience(Duration::from_secs(30)))
        .expect("node 1's process heals and is ready");
    reads_back_and_writes(&nodes[0], "node 1's process");

    // The boots node 1's heal pinned: each process registered its node at its own open's boot.
    for (at, db) in nodes.iter().enumerate() {
        let node = NodeId(u32::try_from(at + 1).expect("three"));
        assert_eq!(
            links.boot_of(node),
            Some(db.opened.base_boot),
            "node {}",
            node.0
        );
    }
    let mut app = open_app(&rt, &store, &links, ClientId(2));
    app.put(b"c", b"3", None).expect("the app's put");
    assert_eq!(
        app.get(b"c").expect("get c").value.map(|(_, v)| v),
        Some(Bytes::from_static(b"3"))
    );
    app.shutdown();
    for db in &mut nodes {
        db.shutdown();
    }

    let heal = lock(&nodes[0].opened.heal).clone();
    assert!(
        matches!(heal, HealState::Handed(_)),
        "node 1's heal handed over: {heal:?}"
    );
    let started = messages(METHOD)
        .iter()
        .filter(|m| *m == "db_heal_started")
        .count();
    assert_eq!(started, 1, "one heal, by node 1's process");
    let logged = messages(METHOD);
    assert_eq!(
        logged.iter().filter(|m| *m == "db_open_not_owner").count(),
        2,
        "nodes 2 and 3 do not heal: {logged:?}"
    );
}

/// tester-s2 `lr up` race (ruling A-R271): node 1's process opens alone and its heal finds no
/// boot for nodes 2 and 3 within its window. It does not give up: it logs `heal_retry` and
/// tries again (that line is logged by the heal task, outside this row's log span), so when
/// nodes 2 and 3 open after the window, node 1 heals and is ready in the same open, with no
/// restart. Cut: one attempt only, and node 1's heal fails for good at the window.
/// Integration (~15 s): the heal's window is the grant budget plus a fixed 10 s
/// (`heal_within`), and the first attempt must run it out.
#[config_log::retcd_test]
fn m10_s2_a_node_whose_peers_start_after_the_heal_window_heals_without_a_restart() {
    const METHOD: &str =
        "m10_s2_a_node_whose_peers_start_after_the_heal_window_heals_without_a_restart";
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    written_and_closed(&rt, dir.path(), &store);

    let links = Links::new();
    let open_one = |node: u32| {
        rt.block_on(Db::open_with(
            DbConfig {
                links: Some(Arc::clone(&links)),
                ..node_config(dir.path(), &[node])
            },
            Arc::clone(&store),
            rt.handle().clone(),
            fast(),
        ))
        .unwrap_or_else(|e| panic!("node {node}'s process opens: {e}"))
    };
    let mut one = open_one(1);
    // The heal starts at the end of the open, so the clock starts when the open returns. Past
    // the first attempt's window (the product's, not scaled) that attempt has found no boot
    // for nodes 2 and 3 and ended. The heal must still be running.
    let opened = std::time::Instant::now();
    let window = heal_within(&fast());
    while opened.elapsed() < window + Duration::from_secs(2) {
        let heal = lock(&one.opened.heal).clone();
        assert!(
            matches!(heal, HealState::Running),
            "node 1's heal gave up at {:?} (window {window:?}): {heal:?}",
            opened.elapsed()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    let mut late: Vec<Db> = [2, 3].into_iter().map(open_one).collect();
    one.wait_ready(crate::host::test_patience(Duration::from_secs(30)))
        .expect("node 1 heals once nodes 2 and 3 are up, without a restart");
    reads_back_and_writes(&one, "node 1's process");
    for db in &mut late {
        db.shutdown();
    }
    one.shutdown();

    let heal = lock(&one.opened.heal).clone();
    assert!(matches!(heal, HealState::Handed(_)), "{heal:?}");
    let logged = messages(METHOD);
    assert_eq!(
        logged.iter().filter(|m| *m == "db_open_not_owner").count(),
        2,
        "nodes 2 and 3 opened as copies: {logged:?}"
    );
}

// ---- S2-3 then a reopen (A-R256 follow-up, W5): the heal after a promote ----

/// A-R256 follow-up, arch-s2 B1/B2: after `admin promote 2`, the folder is closed and opened
/// again. `partitions/1` names node 2, so the reopen's heal clears node 2's grant alone, plans
/// node 2 primary and node 2 commits the next generation. Node 1's grant, frozen by the
/// promote, is left at the revision the promote wrote it: clearing it is refused (frozen), and
/// before this fix that refusal failed every reopen after a promote with `NotCleared` after
/// 13 s. The write made before the promote reads back and a new one lands.
/// Cuts: the heal clears node 1's grant first, as before: the reopen fails `NotCleared`. The plan
/// keeps node 1 primary: node 2 commits but never admits, and the reopen times out.
/// Integration (~5 s): a fresh open, a promote (waits out node 1's grant), one reopen.
#[config_log::retcd_test]
fn m10_s2_a_reopen_after_a_promote_heals_node_2_and_leaves_node_1s_grant_frozen() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let grant_1 = || {
        let found = rt
            .block_on(store.get(config_core::GetRequest {
                key: Bytes::from(ControlKey::Grant(crate::admin::OWNER).encode()),
            }))
            .expect("get grants/1")
            .record
            .expect("grants/1 is present");
        let record =
            rdb_core::authority::grant::GrantRecord::decode(&found.value).expect("a grant record");
        (found.mod_revision, record.frozen)
    };
    let mut db = open(&rt, dir.path(), &store).expect("the fresh open");
    db.wait_ready(crate::host::test_patience(Duration::from_secs(10)))
        .expect("the fresh open is ready");
    db.put(b"a", b"1", None).expect("a put");
    let promoted = db.promote(NodeId(2), None).expect("promote");
    let (frozen_at, frozen) = grant_1();
    assert!(frozen, "the promote froze grants/1");
    db.shutdown();

    let mut db = open(&rt, dir.path(), &store).expect("the reopen");
    db.wait_ready(crate::host::test_patience(Duration::from_secs(30)))
        .expect("the reopen after a promote is ready");
    assert!(
        matches!(db.heal_state(), HealState::Handed(_)),
        "{:?}",
        db.heal_state()
    );
    assert_eq!(
        db.route.owner(),
        NodeId(2),
        "the reopen's calls go to node 2"
    );
    let owner = db
        .node_status()
        .into_iter()
        .find(|status| status.node == NodeId(2))
        .expect("node 2 answers a status look");
    assert_eq!(owner.role, "primary", "{owner:?}");
    assert!(
        owner
            .recovered
            .is_some_and(|generation| generation > promoted.generation),
        "node 2 committed past the promote's {:?}: {owner:?}",
        promoted.generation
    );
    assert_eq!(
        grant_1(),
        (frozen_at, true),
        "the heal left node 1's frozen grant as the promote wrote it"
    );
    reads_back_and_writes(&db, "after the promote and a reopen");
    db.shutdown();
}

/// A-R253: a node that `Db::restart_node` starts again opens with the `DbConfig::storage` this
/// `Db` was opened with, not the engine's default. Each engine open logs the memtable budget it
/// asked RocksDB for, as `write_buffer_bytes_asked` on its `storage_open` line, so node 3's line
/// from the restart names the asked value, as its line from the first open did.
/// Integration (~1 s): one fresh open until ready, a put, one restart of node 3.
#[config_log::retcd_test]
fn m10_s2_a_restarted_node_keeps_the_write_buffer_this_db_was_opened_with() {
    const METHOD: &str = "m10_s2_a_restarted_node_keeps_the_write_buffer_this_db_was_opened_with";
    const ASKED: usize = 1 << 20;
    let node_3_opens = |dir: &Path| -> Vec<u64> {
        let node_3 = dir.join("nodes").join("3").display().to_string();
        config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
            .into_iter()
            .filter(|line| line["@m"] == "storage_open" && line["path"] == node_3.as_str())
            .map(|line| {
                line["write_buffer_bytes_asked"]
                    .as_u64()
                    .expect("storage_open names the budget it asked for")
            })
            .collect()
    };
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let mut db = rt
        .block_on(Db::open_with(
            DbConfig {
                storage: crate::StorageOptions {
                    write_buffer_bytes: Some(ASKED),
                    memtable_budget_bytes: None,
                    block_cache_bytes: None,
                    sync_stuck_millis: None,
                },
                ..config(dir.path())
            },
            Arc::clone(&store),
            rt.handle().clone(),
            fast(),
        ))
        .expect("the fresh open");
    db.wait_ready(crate::host::test_patience(Duration::from_secs(10)))
        .expect("ready");
    db.put(b"a", b"1", None).expect("a put");
    let before = node_3_opens(dir.path());
    assert_eq!(before, vec![ASKED as u64], "node 3's first open");

    assert_eq!(
        db.restart_node(NodeId(3)).expect("node 3 restarts"),
        BootId(2)
    );
    db.shutdown();
    assert_eq!(
        node_3_opens(dir.path()),
        vec![ASKED as u64, ASKED as u64],
        "node 3's restart asks for the budget this Db was opened with, not the 64 MiB default"
    );
}

/// A-R261 (scenario G, W7's walk `lr kill 1; lr promote 2`): an app-only `Db` promotes. Node 1
/// of the `Db` that runs the nodes is stopped; the app, which runs no node and reaches nodes 2
/// and 3 over the links, runs `promote 2`. It is answered: node 2 owns partition 1 at a newer
/// generation, node 1 is the one dropped, and the app's calls go to node 2.
/// Cut: `open_app` keeps no control store; the promote is refused before it starts.
/// Integration (~5 s): one fresh open until ready, a promote that waits node 1's grant out.
#[config_log::retcd_test]
fn m10_s2_g_an_app_only_db_promotes_node_2_while_node_1_is_stopped() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let mut nodes = open(&rt, dir.path(), &store).expect("the fresh open");
    nodes
        .wait_ready(crate::host::test_patience(Duration::from_secs(10)))
        .expect("the fresh open is ready");
    nodes.put(b"a", b"1", None).expect("a put");
    lock(&nodes.nodes)
        .iter_mut()
        .find(|handle| handle.node == crate::admin::OWNER)
        .expect("node 1 runs here")
        .stop();

    let mut app = open_app(&rt, &store, nodes.links(), ClientId(2));
    let promoted = app
        .promote(NodeId(2), None)
        .unwrap_or_else(|error| panic!("the app's promote is answered: {error}"));
    assert_eq!(
        (promoted.owner, promoted.prior_owner, promoted.dropped),
        (NodeId(2), crate::admin::OWNER, crate::admin::OWNER),
        "{promoted:?}"
    );
    assert!(promoted.generation > Generation(1), "{promoted:?}");
    assert_eq!(app.route.owner(), NodeId(2), "the app's calls go to node 2");
    app.shutdown();
    nodes.shutdown();
}

use rdb_core::contracts::event::KernelEvent;
use rdb_core::contracts::ids::{CorrelationId, PartitionId};

/// A promote's re-run, as the rows below drive it: `admin::promote` over `links` with this
/// `Db`'s budgets and clock, every `Routed` it sends kept in `sent` and posted on. Once the
/// fence has gone, both are posted again, as a re-run after a lost answer would send them.
fn promote_redelivering(
    rt: &tokio::runtime::Runtime,
    db: &Db,
    store: &Arc<dyn ConfigStore>,
    links: &Arc<Links>,
    sent: &Arc<Mutex<Vec<KernelEvent>>>,
) -> Result<admin::Promoted, admin::PromoteError> {
    let (send_links, boot_links, seen) = (Arc::clone(links), Arc::clone(links), Arc::clone(sent));
    let send = move |node: NodeId, msg: Msg| -> Result<(), NodeStopped> {
        let Msg::Routed {
            partition,
            correlation,
            kind,
        } = msg
        else {
            panic!("a promote sends Routed events only: {msg:?}");
        };
        let post = |kind: KernelEvent| {
            send_links
                .post(
                    node,
                    Msg::Routed {
                        partition,
                        correlation,
                        kind,
                    },
                )
                .map_err(|_| NodeStopped(node))
        };
        let mut seen = lock(&seen);
        seen.push(kind.clone());
        post(kind)?;
        if seen.len() == 2 {
            for kind in seen.clone() {
                post(kind)?;
            }
        }
        Ok(())
    };
    let ctx = admin::PromoteCtx {
        store: Arc::clone(store),
        budgets: fast(),
        clock: db.clock,
        to: NodeId(2),
        drop: None,
        within: heal_within(&fast()),
        members: admin::members(),
        member_boot: Arc::new(move |node| boot_links.boot_of(node)),
        send: Arc::new(send),
    };
    rt.block_on(admin::promote(ctx))
}

/// `partitions/1`'s owner and generation, read now.
fn partition_owner(
    rt: &tokio::runtime::Runtime,
    store: &Arc<dyn ConfigStore>,
) -> (NodeId, Generation) {
    let read = rt
        .block_on(store.get(config_core::GetRequest {
            key: Bytes::from(ControlKey::Partition(PartitionId(1)).encode()),
        }))
        .expect("get partitions/1")
        .record
        .expect("partitions/1");
    let record = PartitionRecord::decode(&read.value).expect("a partition record");
    (record.owner, record.generation)
}

/// A-R265/A-R266 (W7's walk G, condition 3): a promote that cannot reach node 2 is refused
/// `UNAVAILABLE` after it wrote, and says to promote again. Node 2 then runs again, and the re-run
/// finishes: node 2 owns partition 1 one generation on. Its plan and fence reach node 2 twice in
/// the re-run, as a re-run after a lost answer sends them, and once more after the commit. Node 2
/// steps all six without a fault and commits one generation, not two, and a put lands on it.
/// Cut: the re-run posts each event once (no re-delivery); node 2 logs four, not six, red.
/// Integration (~19 s): one fresh open, two promotes that each wait node 1's grant out.
#[config_log::retcd_test]
fn m10_s2_a_promote_that_could_not_reach_node_2_finishes_when_run_again_and_redelivery_is_a_no_op()
{
    const METHOD: &str = "m10_s2_a_promote_that_could_not_reach_node_2_finishes_when_run_again_and_redelivery_is_a_no_op";
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let mut nodes = open(&rt, dir.path(), &store).expect("the fresh open");
    let patience = crate::host::test_patience(Duration::from_secs(10));
    nodes.wait_ready(patience).expect("the fresh open is ready");
    nodes.put(b"a", b"1", None).expect("a put");
    let (_, before) = partition_owner(&rt, &store);
    for node in [crate::admin::OWNER, NodeId(2)] {
        lock(&nodes.nodes)
            .iter_mut()
            .find(|handle| handle.node == node)
            .expect("runs here")
            .stop();
    }

    let mut app = open_app(&rt, &store, nodes.links(), ClientId(2));
    let refused = app.promote(NodeId(2), None).expect_err("node 2 is stopped");
    assert_eq!(
        (refused.kind, refused.retry, refused.no_mutation),
        (ErrorKind::Unavailable, RetryRule::BoundedJitter, false),
        "{refused:?}"
    );
    assert!(
        refused
            .detail
            .starts_with("node 2 could not be sent the new plan;")
            && refused.detail.ends_with("promote again"),
        "{refused:?}"
    );
    assert_eq!(partition_owner(&rt, &store), (crate::admin::OWNER, before));

    assert_eq!(
        nodes.restart_node(NodeId(2)).expect("node 2 runs again"),
        BootId(2)
    );
    let sent: Arc<Mutex<Vec<KernelEvent>>> = Arc::default();
    let promoted = promote_redelivering(&rt, &app, &store, nodes.links(), &sent)
        .unwrap_or_else(|error| panic!("the re-run finishes: {error}"));
    assert_eq!(
        (promoted.owner, promoted.generation),
        (NodeId(2), Generation(before.0 + 1)),
        "{promoted:?}"
    );
    for kind in lock(&sent).clone() {
        nodes
            .links()
            .post(
                NodeId(2),
                Msg::Routed {
                    partition: PartitionId(1),
                    correlation: CorrelationId(4),
                    kind,
                },
            )
            .expect("node 2 runs");
    }

    app.route.owner.store(2, Ordering::Relaxed);
    let deadline = std::time::Instant::now() + patience;
    let put = loop {
        match app.put(b"b", b"2", None) {
            Ok(put) => break put,
            Err(refused) => assert!(
                std::time::Instant::now() < deadline,
                "no put landed on node 2 within {patience:?}: {refused:?}"
            ),
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(put.generation, promoted.generation, "{put:?}");
    let events: Vec<String> = config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
        .into_iter()
        .filter(|line| line["@m"] == "routed_admin_event" && line["node"] == 2)
        .map(|line| line["event"].as_str().unwrap_or_default().to_owned())
        .collect();
    assert_eq!(
        events,
        [
            "plan",
            "fence_proven",
            "plan",
            "fence_proven",
            "plan",
            "fence_proven"
        ],
        "node 2 took each, in order"
    );
    let two = nodes
        .node_status()
        .into_iter()
        .find(|status| status.node == NodeId(2))
        .expect("node 2 answers a look");
    assert_eq!(two.fault, None, "{two:?}");
    assert_eq!(
        partition_owner(&rt, &store),
        (NodeId(2), promoted.generation),
        "one generation, not two"
    );
    app.shutdown();
    nodes.shutdown();
}

/// A-R265/A-R266 (W7's walk G, condition 5): a promote over a real `Net`. Each node runs in its
/// own `Db` with its own links and a TLS link to the others, as three `rdb_node` processes do,
/// and node 1's is shut down. An app reaching nodes 2 and 3 only over its own `Net` runs
/// `promote 2` and is answered: node 2 owns partition 1, took the plan and then the fence over
/// the wire with no fault, and a put from the app lands on it.
/// Cut: the promote sends `Inject` again; `Net` refuses it (`net_send_refused`) and the promote
/// is `UNAVAILABLE`, red.
/// Integration (~8 s): one fresh open, three node opens until node 1 is ready, a promote that
/// waits node 1's grant out.
#[config_log::retcd_test]
fn m10_s2_a_promote_over_the_net_reaches_node_2_in_another_process_and_writes_land_on_it() {
    use crate::net::{certs, faults::Faults, wire::Label, Net, NetConfig, PROTOCOL_VERSION};
    const METHOD: &str =
        "m10_s2_a_promote_over_the_net_reaches_node_2_in_another_process_and_writes_land_on_it";
    let rt = runtime();
    let _entered = rt.enter();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let cert_dir = config_testkit::fs::temp_dir();
    let app_label = Label::App("w5-promote".to_owned());
    let labels: Vec<Label> = ALL_NODES
        .iter()
        .map(|node| Label::Node(*node))
        .chain([app_label.clone()])
        .collect();
    certs::generate(cert_dir.path(), &labels).expect("certs");
    written_and_closed(&rt, dir.path(), &store);

    let listeners: Vec<tokio::net::TcpListener> = ALL_NODES
        .iter()
        .map(|_| rt.block_on(config_testkit::ports::ephemeral_listener()).0)
        .collect();
    let addrs: BTreeMap<NodeId, String> = ALL_NODES
        .iter()
        .zip(&listeners)
        .map(|(node, listener)| (*node, listener.local_addr().expect("bound").to_string()))
        .collect();
    let mut processes: Vec<(Db, Arc<Net>, Arc<Links>)> = ALL_NODES
        .iter()
        .zip(listeners)
        .map(|(node, listener)| {
            let node = *node;
            let boot = next_boot(dir.path()).expect("a boot");
            let label = Label::Node(node);
            let links = Links::new();
            let net = Net::start(
                NetConfig {
                    tls: certs::load(cert_dir.path(), &label).expect("node certs"),
                    label,
                    boot,
                    peers: ALL_NODES.iter().copied().filter(|n| *n != node).collect(),
                    dial: NetConfig::dial_lower(node, &addrs),
                    protocol_version: PROTOCOL_VERSION,
                    faults: Faults::new(),
                },
                &links,
                Some(listener),
            )
            .expect("the node's net starts");
            let db = rt
                .block_on(Db::open_with(
                    DbConfig {
                        local: vec![node],
                        links: Some(Arc::clone(&links)),
                        boot: Some(boot),
                        ..config(dir.path())
                    },
                    Arc::clone(&store),
                    rt.handle().clone(),
                    fast(),
                ))
                .unwrap_or_else(|e| panic!("node {}'s process opens: {e}", node.0));
            (db, net, links)
        })
        .collect();
    let patience = crate::host::test_patience(Duration::from_secs(30));
    processes[0]
        .0
        .wait_ready(patience)
        .expect("node 1's process heals over the net and is ready");
    processes[0]
        .0
        .put(b"b", b"2", None)
        .expect("a put on node 1");

    // `lr kill 1`: node 1's process goes, with its links.
    let (mut one, one_net, one_links) = processes.remove(0);
    one.shutdown();
    drop((one_net, one_links));

    let app_links = Links::new();
    let app_net = Net::start(
        NetConfig {
            tls: certs::load(cert_dir.path(), &app_label).expect("app certs"),
            label: app_label,
            boot: BootId(0),
            peers: ALL_NODES.iter().copied().collect(),
            dial: addrs.clone(),
            protocol_version: PROTOCOL_VERSION,
            faults: Faults::new(),
        },
        &app_links,
        None,
    )
    .expect("the app's net starts");
    rt.block_on(app_net.wait_up(&[NodeId(2), NodeId(3)], patience))
        .expect("the app reaches nodes 2 and 3");
    let mut app = open_app(&rt, &store, &app_links, ClientId(2));
    let promoted = app
        .promote(NodeId(2), None)
        .unwrap_or_else(|error| panic!("the promote over the net is answered: {error}"));
    assert_eq!(
        (promoted.owner, promoted.prior_owner),
        (NodeId(2), crate::admin::OWNER),
        "{promoted:?}"
    );
    assert_eq!(app.route.owner(), NodeId(2), "the app's calls go to node 2");
    let deadline = std::time::Instant::now() + patience;
    let put = loop {
        match app.put(b"c", b"3", None) {
            Ok(put) => break put,
            Err(refused) => assert!(
                std::time::Instant::now() < deadline,
                "no put landed on node 2 within {patience:?}: {refused:?}"
            ),
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(put.generation, promoted.generation, "{put:?}");
    assert_eq!(
        app.get(b"a").expect("get a").value.map(|(_, v)| v),
        Some(Bytes::from_static(b"1")),
        "the write made before reads back from node 2"
    );

    let events: Vec<String> = config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
        .into_iter()
        .filter(|line| line["@m"] == "routed_admin_event" && line["node"] == 2)
        .map(|line| line["event"].as_str().unwrap_or_default().to_owned())
        .collect();
    assert_eq!(
        events,
        ["plan", "fence_proven"],
        "node 2 took each over the wire, in order"
    );
    let two = processes[0]
        .0
        .node_status()
        .into_iter()
        .find(|status| status.node == NodeId(2))
        .expect("node 2 answers a look");
    assert_eq!(two.fault, None, "{two:?}");
    assert_eq!(
        partition_owner(&rt, &store),
        (NodeId(2), promoted.generation)
    );
    app.shutdown();
    drop((app_net, app_links));
    for (db, _, _) in &mut processes {
        db.shutdown();
    }
}
