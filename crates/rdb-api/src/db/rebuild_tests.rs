//! bb #65 (critic-bb §3) rows for `Db::rebuild` with every node in this process: mode B under
//! writes, the moved-aside folder (G7), the owner refused (G2) and the wipe fence (M5).

use super::*;
use rdb_core::contracts::ids::ReplicaRole;

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

/// A fresh `Db`, ready, on `host::test_budgets`.
fn ready(rt: &tokio::runtime::Runtime, dir: &Path, store: &Arc<dyn ConfigStore>) -> Db {
    let db = rt
        .block_on(Db::open_with(
            config(dir),
            Arc::clone(store),
            rt.handle().clone(),
            crate::host::test_budgets(),
        ))
        .expect("a fresh open");
    db.wait_ready(crate::host::test_patience(Duration::from_secs(10)))
        .expect("the fresh open is ready");
    db
}

/// Put `key` until it is acked or `patience` runs out; refused sends are counted, any answer
/// but an ack is retried (a pause, a re-plan's refusal), since only an ack is claimed.
fn put_acked(db: &Db, key: &str, patience: Duration) -> Option<Duration> {
    let started = Instant::now();
    while started.elapsed() < patience {
        if db.put(key.as_bytes(), key.as_bytes(), None).is_ok() {
            return Some(started.elapsed());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    None
}

fn files_under(dir: &Path) -> usize {
    std::fs::read_dir(dir).map_or(0, |entries| entries.count())
}

/// Under writes, `admin rebuild 3 --wipe` with 3 pinned (mode B): the owner drops 3, waits for
/// that commit, 3 restarts on an empty folder and is pinned regular again. Every acked write
/// reads back, the longest wait for an ack is bounded, and 3's old folder is moved aside with
/// its files (G7), never deleted.
/// Cut G7 (`move_aside` deleting): the aside folder is missing. Cut G5/B2 (no drop before the
/// wipe): the fence refuses, since `partitions/1` still pins 3, and the row goes red.
/// Integration (~5-15 s): one open, a drop re-plan, a restart, a re-add and its promote.
#[config_log::retcd_test]
fn b3_rebuild_under_writes_brings_node_3_back_regular_and_keeps_every_acked_write() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let db = ready(&rt, dir.path(), &store);
    let patience = crate::host::test_patience(Duration::from_secs(20));
    for i in 0..5 {
        assert!(put_acked(&db, &format!("pre{i}"), patience).is_some(), "pre{i}");
    }
    let stop = AtomicBool::new(false);
    let (rebuilt, acked, longest) = std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            let mut acked = Vec::new();
            let mut longest = Duration::ZERO;
            let mut i = 0_u32;
            while !stop.load(Ordering::Relaxed) {
                let key = format!("w{i}");
                if let Some(took) = put_acked(&db, &key, patience) {
                    longest = longest.max(took);
                    acked.push(key);
                }
                i += 1;
            }
            (acked, longest)
        });
        let rebuilt = db.rebuild_with(NodeId(3), true);
        stop.store(true, Ordering::Relaxed);
        let (acked, longest) = writer.join().expect("the writer");
        (rebuilt, acked, longest)
    });
    let rebuilt = rebuilt.expect("the rebuild");
    tracing::info!(line = %rebuilt.line(), acked = acked.len(), longest_ms = longest.as_millis(), "b3_rebuilt");
    assert_eq!(rebuilt.mode, admin::rebuild::RebuildMode::Pinned);
    assert!(rebuilt.kept.contains(&NodeId(1)) && rebuilt.kept.contains(&NodeId(2)), "{rebuilt:?}");
    assert!(!acked.is_empty(), "writes went on during the rebuild");
    assert!(
        longest < crate::host::test_patience(Duration::from_secs(15)),
        "the longest wait for an ack was {longest:?}"
    );
    let aside = rebuilt.aside.clone().expect("wiped");
    assert!(aside.join("CURRENT").exists(), "G7: {} holds 3's old copy", aside.display());
    assert!(dir.path().join("nodes").join("3").join("CURRENT").exists(), "3 runs again");
    let owner = db.inspect(NodeId(1)).expect("the owner answers");
    assert!(
        owner
            .pinned
            .iter()
            .any(|m| m.node == NodeId(3) && m.role == ReplicaRole::RegularSecondary),
        "{:?}",
        owner.pinned
    );
    for key in (0..5).map(|i| format!("pre{i}")).chain(acked) {
        let got = db.get(key.as_bytes()).expect("a read");
        assert_eq!(
            got.value.map(|(_, value)| value),
            Some(Bytes::from(key.clone())),
            "acked {key} reads back"
        );
    }
    assert!(files_under(&aside) > 0);
}

/// G2 at the `Db`: rebuilding the owner is refused `INVALID_ARGUMENT`, names `admin promote`,
/// and changes nothing (node 1 keeps its boot). Cut G2: node 1 is asked to drop itself.
/// Integration (~1 s): one open.
#[config_log::retcd_test]
fn b3_rebuild_of_the_owner_is_refused_and_names_promote() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let db = ready(&rt, dir.path(), &store);
    let boot = db.boot_of(NodeId(1));
    let refused = db.rebuild_with(NodeId(1), true).expect_err("the owner");
    assert_eq!(refused.kind, ErrorKind::InvalidArgument, "{refused:?}");
    assert!(refused.no_mutation);
    assert!(refused.detail.contains("admin promote"), "{}", refused.detail);
    assert_eq!(db.boot_of(NodeId(1)), boot, "nothing restarted");
}

/// M5 at the `Db`: a wipe whose fence no longer holds moves nothing. Node 3 is still pinned,
/// so `partitions/1` names it: the wipe is refused after the stop, and 3 restarts on its own
/// folder. A wipe naming a boot 3 no longer runs (3 restarted since the drop) is refused
/// before anything stops. Cut M5 (no record check): 3's folder is moved aside while pinned.
/// Integration (~2 s): one open, one restart.
#[config_log::retcd_test]
fn b3_a_stale_wipe_is_refused_and_leaves_the_folder_in_place() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let db = ready(&rt, dir.path(), &store);
    let folder = dir.path().join("nodes").join("3");
    let boot = db.boot_of(NodeId(3)).expect("3 runs");
    let to = dir.path().join("aside").join("stale");

    let restarted = Aside {
        boot: BootId(boot.0 - 1),
        generation: Generation(1),
        to: to.clone(),
    };
    let refused = db
        .restart_node_with(NodeId(3), Some(restarted))
        .expect_err("a boot 3 no longer runs");
    assert!(refused.no_mutation, "{refused:?}");
    assert_eq!(db.boot_of(NodeId(3)), Some(boot), "nothing stopped");

    let pinned = Aside {
        boot,
        generation: Generation(1),
        to: to.clone(),
    };
    let refused = db
        .restart_node_with(NodeId(3), Some(pinned))
        .expect_err("the record still pins 3");
    assert!(refused.detail.contains("pins node 3 again"), "{}", refused.detail);
    assert!(!to.exists(), "nothing moved aside");
    assert!(folder.join("CURRENT").exists(), "3's folder is in place");
    assert_ne!(db.boot_of(NodeId(3)), Some(boot), "3 restarted on its own folder");
}
