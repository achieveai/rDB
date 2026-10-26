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
        assert!(
            put_acked(&db, &format!("pre{i}"), patience).is_some(),
            "pre{i}"
        );
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
    assert!(
        rebuilt.kept.contains(&NodeId(1)) && rebuilt.kept.contains(&NodeId(2)),
        "{rebuilt:?}"
    );
    assert!(!acked.is_empty(), "writes went on during the rebuild");
    assert!(
        longest < crate::host::test_patience(Duration::from_secs(15)),
        "the longest wait for an ack was {longest:?}"
    );
    let aside = rebuilt.aside.clone().expect("wiped");
    let line = rebuilt.line();
    assert!(
        line.starts_with("rebuilt: node 3 (mode B) is pinned regular again")
            && line.contains("kept nodes 1 and 2")
            && line.ends_with(&format!(
                "its old folder was moved aside to {}",
                aside.display()
            )),
        "{line}"
    );
    // Bug bash #65 paper cut: "rebuilt" used to return before writes were admitted again.
    assert!(
        rebuilt.writes_wait.is_none() && line.contains("writes are admitted again"),
        "{line}"
    );
    assert!(
        aside.join("CURRENT").exists(),
        "G7: {} holds 3's old copy",
        aside.display()
    );
    assert!(
        dir.path().join("nodes").join("3").join("CURRENT").exists(),
        "3 runs again"
    );
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
    assert!(
        refused.detail.contains("admin promote"),
        "{}",
        refused.detail
    );
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
    assert!(
        refused.detail.contains("pins node 3 again"),
        "{}",
        refused.detail
    );
    // Bug bash #65 paper cut: the refusal ends with its own step, not the generic NotWired
    // "retrying will not help" that contradicted it.
    assert!(
        refused.next_step().is_none() && !refused.explain().contains("retrying will not help"),
        "{}",
        refused.explain()
    );
    assert!(!to.exists(), "nothing moved aside");
    assert!(folder.join("CURRENT").exists(), "3's folder is in place");
    assert_ne!(
        db.boot_of(NodeId(3)),
        Some(boot),
        "3 restarted on its own folder"
    );
}

/// Fork `nodes`' copies (each differently), restart them, and wait until the owner's F1 is
/// `Quarantined`. Returns whether it got there within `patience`.
fn quarantine_by_fork(db: &Db, nodes: &[NodeId], patience: Duration) -> bool {
    for &node in nodes {
        let generation = db
            .inspect(node)
            .and_then(|status| status.holds)
            .expect("the copy holds a lineage")
            .generation;
        let seq = db.fork_record(node, generation).expect("forked");
        tracing::info!(node = node.0, generation = generation.0, seq, "b3_forked");
    }
    for &node in nodes {
        db.restart_node(node)
            .expect("restarted on its forked folder");
    }
    let started = Instant::now();
    while started.elapsed() < patience {
        let phase = db.inspect(NodeId(1)).and_then(|status| status.recovery);
        if phase.as_deref() == Some("Quarantined") {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

/// Mode C (critic-bb §3): node 3's copy diverges (a validly chained different head), its
/// restart's rejoin quarantines the owner's F1, and `admin rebuild 3` replaces that F1 with a
/// fresh run without 3, always wipes 3 (moved aside), and 3 is pinned regular again. Every
/// write acked before reads back, and a new write is acked after.
/// Cut: keep the quarantined F1 (no fresh run): the rebuild is refused, red.
/// Integration (~5-15 s): one open, a fork, a quarantine, a re-plan, a restart, a re-add.
#[config_log::retcd_test]
fn b3_mode_c_takes_a_quarantined_partition_out_and_back() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let db = ready(&rt, dir.path(), &store);
    let patience = crate::host::test_patience(Duration::from_secs(20));
    for i in 0..5 {
        assert!(
            put_acked(&db, &format!("pre{i}"), patience).is_some(),
            "pre{i}"
        );
    }
    assert!(
        quarantine_by_fork(&db, &[NodeId(3)], patience),
        "the owner quarantines once 3's forked copy rejoins: {:?}",
        db.inspect(NodeId(1)).map(|status| status.recovery)
    );
    let rebuilt = db.rebuild_with(NodeId(3), false).expect("the rebuild");
    tracing::info!(line = %rebuilt.line(), "b3_rebuilt");
    assert_eq!(rebuilt.mode, admin::rebuild::RebuildMode::Quarantined);
    let aside = rebuilt.aside.clone().expect("mode C always wipes");
    assert!(aside.join("CURRENT").exists(), "{}", aside.display());
    for key in (0..5).map(|i| format!("pre{i}")) {
        let got = db.get(key.as_bytes()).expect("a read");
        assert_eq!(
            got.value.map(|(_, value)| value),
            Some(Bytes::from(key.clone())),
            "acked {key} reads back"
        );
    }
    assert!(
        put_acked(&db, "after", patience).is_some(),
        "writes land again"
    );
}

/// Every node's boot and the owner's landed generation: what a refused rebuild must not move.
fn untouched(db: &Db) -> (Vec<Option<BootId>>, Option<Generation>) {
    (
        ALL_NODES.iter().map(|&node| db.boot_of(node)).collect(),
        db.inspect(NodeId(1)).and_then(|status| status.recovered),
    )
}

/// G4 through the owner's own `current` check (critic-65 F4): with link 1-2 held, node 2 is
/// pinned but not answering, so rebuilding 3 would leave no other regular copy. Refused
/// `UNAVAILABLE` with `no_mutation`, every boot and the owner's generation unchanged.
/// Cut: the `current` closure in `rebuild_asked` to `|_| true`: the owner re-plans, red.
/// Integration (~1 s): one open, one hold.
#[config_log::retcd_test]
fn b3_g4_a_rebuild_with_the_other_copy_unreachable_is_refused_untouched() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let db = ready(&rt, dir.path(), &store);
    let before = untouched(&db);
    db.links().hold(NodeId(1), NodeId(2));
    let refused = db.rebuild_with(NodeId(3), true).expect_err("G4");
    let after = untouched(&db);
    db.links().heal(NodeId(1), NodeId(2));
    assert_eq!(refused.kind, ErrorKind::Unavailable, "{refused:?}");
    assert!(refused.no_mutation, "{refused:?}");
    assert!(refused.detail.contains("(G4)"), "{}", refused.detail);
    assert_eq!(after, before, "nothing restarted, nothing re-planned");
}

/// G3's second half (critic-65 A2): while the owner works on one rebuild, a second is refused,
/// and the first still completes. Node 3's rebuild (mode B) is asked on another thread; once
/// the owner logged it, node 2's is refused G3 and nothing of 2 changes.
/// Cut: G3's `rejoin` term to `false`: 2's ask is let through and times out, red. Dropping only its
/// `rebuild_asks` half stays green: by the time 2 is asked, the drop re-plan is in `rejoins`.
/// Integration (~5-15 s): one open, a drop re-plan, a restart, a re-add.
#[config_log::retcd_test]
fn b3_g3_a_second_rebuild_while_one_runs_is_refused() {
    const METHOD: &str = "b3_g3_a_second_rebuild_while_one_runs_is_refused";
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let db = ready(&rt, dir.path(), &store);
    let boot_2 = db.boot_of(NodeId(2));
    let asked = || {
        config_log::layer::flush_test_logs();
        config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
            .iter()
            .any(|line| line["@m"] == "rebuild_asked")
    };
    let (first, second) = std::thread::scope(|scope| {
        let first = scope.spawn(|| db.rebuild_with(NodeId(3), false));
        let patience = crate::host::test_patience(Duration::from_secs(5));
        let started = Instant::now();
        while !asked() && started.elapsed() < patience {
            std::thread::sleep(Duration::from_millis(10));
        }
        let second = db.rebuild_with(NodeId(2), false);
        (first.join().expect("the first rebuild"), second)
    });
    let refused = second.expect_err("a second rebuild while one runs");
    assert!(refused.detail.contains("(G3)"), "{}", refused.detail);
    assert!(refused.no_mutation, "{refused:?}");
    assert_eq!(db.boot_of(NodeId(2)), boot_2, "2 untouched");
    let first = first.expect("the first rebuild completes");
    assert_eq!(first.node, NodeId(3));
}

/// G6 through the owner (critic-65 F3): node 3's fork quarantines with evidence naming copies 0
/// and 2 (nodes 1 and 3). Rebuilding node 2, whose copy the evidence does not name, is refused
/// G6 with `no_mutation`, and nothing restarts.
/// Cut: `names` always true: red, but through G4 (3 is a shadow, so dropping 2 leaves 1 alone).
/// Integration (~1-3 s): one open, a fork, a quarantine.
#[config_log::retcd_test]
fn b3_g6_a_quarantined_rebuild_of_an_unnamed_copy_is_refused() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let db = ready(&rt, dir.path(), &store);
    let patience = crate::host::test_patience(Duration::from_secs(20));
    assert!(put_acked(&db, "pre", patience).is_some(), "pre");
    assert!(
        quarantine_by_fork(&db, &[NodeId(3)], patience),
        "quarantined: {:?}",
        db.inspect(NodeId(1)).map(|status| status.recovery)
    );
    let before = untouched(&db);
    let refused = db.rebuild_with(NodeId(2), true).expect_err("G6");
    assert!(refused.detail.contains("(G6)"), "{}", refused.detail);
    assert!(refused.no_mutation, "{refused:?}");
    assert_eq!(untouched(&db), before, "nothing restarted");
}

/// The kept copies still diverge (critic-bb §3 mode C): node 3's fork quarantines with evidence
/// naming copies 0 and 2, so node 3's rebuild passes G4 and G6. Node 2 is then forked in place
/// (still pinned regular, not restarted), so dropping 3 keeps copies 0 and 1, which disagree.
/// The fresh F1 quarantines again and `rebuild_duty` refuses "quarantined again": node 3 keeps
/// its boot and nothing is moved aside.
/// Cut: `rebuild_duty` ignores a re-quarantine: the rebuild waits out and says timeout, red.
/// Integration (~1-5 s): one open, two forks, two quarantines.
#[config_log::retcd_test]
fn b3_mode_c_refuses_when_the_kept_copies_still_diverge() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let db = ready(&rt, dir.path(), &store);
    let patience = crate::host::test_patience(Duration::from_secs(20));
    assert!(put_acked(&db, "pre", patience).is_some(), "pre");
    assert!(
        quarantine_by_fork(&db, &[NodeId(3)], patience),
        "quarantined: {:?}",
        db.inspect(NodeId(1)).map(|status| status.recovery)
    );
    let generation = db
        .inspect(NodeId(2))
        .and_then(|status| status.holds)
        .expect("2 holds a lineage")
        .generation;
    db.fork_record(NodeId(2), generation)
        .expect("2 forked in place");
    let boot_3 = db.boot_of(NodeId(3));
    let refused = db
        .rebuild_with(NodeId(3), true)
        .expect_err("still diverged");
    tracing::info!(detail = %refused.detail, "b3_refused");
    assert!(
        refused.detail.contains("quarantined again"),
        "{}",
        refused.detail
    );
    assert_eq!(db.boot_of(NodeId(3)), boot_3, "3 untouched");
    assert_eq!(
        files_under(&dir.path().join("aside")),
        0,
        "nothing moved aside"
    );
}

/// Revobot #73 finding 1: node 3's copy is forked, then the whole `Db` shuts down and opens
/// again. The reopen's F1 quarantines before any recovery commits, so the owner holds no
/// committed result. `admin rebuild 3` still runs mode C from the quarantined run's plan: 3 is
/// moved aside, pinned regular again, every acked write reads back and a new one is acked.
/// Cut: `rebuild_asked` reads only `committed`: refused "has committed no recovery", red.
/// Integration (~5-15 s): two opens, a fork, a quarantine, a re-plan, a restart, a re-add.
#[config_log::retcd_test]
fn b3_mode_c_after_a_reopen_rebuilds_from_the_quarantined_plan() {
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let patience = crate::host::test_patience(Duration::from_secs(20));
    let mut db = ready(&rt, dir.path(), &store);
    for i in 0..5 {
        assert!(
            put_acked(&db, &format!("pre{i}"), patience).is_some(),
            "pre{i}"
        );
    }
    let generation = db
        .inspect(NodeId(3))
        .and_then(|status| status.holds)
        .expect("3 holds a lineage")
        .generation;
    db.fork_record(NodeId(3), generation).expect("3 forked");
    db.shutdown();
    drop(db);
    let db = rt
        .block_on(Db::open_with(
            config(dir.path()),
            Arc::clone(&store),
            rt.handle().clone(),
            crate::host::test_budgets(),
        ))
        .expect("the reopen");
    let started = Instant::now();
    let mut phase = None;
    while started.elapsed() < patience {
        phase = db.inspect(NodeId(1)).and_then(|status| status.recovery);
        if phase.as_deref() == Some("Quarantined") {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(
        phase.as_deref(),
        Some("Quarantined"),
        "the reopen quarantines"
    );
    assert_eq!(
        db.inspect(NodeId(1)).and_then(|status| status.recovered),
        None,
        "premise: nothing committed since the reopen"
    );
    let rebuilt = db.rebuild_with(NodeId(3), false).expect("the rebuild");
    tracing::info!(line = %rebuilt.line(), "b3_rebuilt");
    assert_eq!(rebuilt.mode, admin::rebuild::RebuildMode::Quarantined);
    assert!(
        rebuilt.kept.contains(&NodeId(1)) && rebuilt.kept.contains(&NodeId(2)),
        "{rebuilt:?}"
    );
    let aside = rebuilt.aside.clone().expect("mode C always wipes");
    assert!(aside.join("CURRENT").exists(), "{}", aside.display());
    for key in (0..5).map(|i| format!("pre{i}")) {
        let got = db.get(key.as_bytes()).expect("a read");
        assert_eq!(
            got.value.map(|(_, value)| value),
            Some(Bytes::from(key.clone())),
            "acked {key} reads back"
        );
    }
    assert!(
        put_acked(&db, "after", patience).is_some(),
        "writes land again"
    );
}
