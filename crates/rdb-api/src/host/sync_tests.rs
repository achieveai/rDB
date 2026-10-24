//! M10 slice 2, F4 (rulings A-R317, A-R326): a slow WAL fsync runs on the node's syncer, never
//! on its node thread, so a slow but working disk costs the owner neither its steps nor its
//! grant; a stuck one still loses the grant.
//!
//! The rows run three node threads on wall-clock time through [`Db`], as the product does, and
//! slow the disk with the crate-only [`slow_sync`] hook: it sleeps where the fsync runs, before
//! it, so a held sync shows which thread a slow disk holds. Each row is integration-length
//! because it holds real syncs for seconds; each says how long.

use super::*;
use crate::db::{Db, DbConfig, Timeouts, ALL_NODES, DEFAULT_CLIENT};
use config_core::ConfigStore;

type Line = serde_json::Value;

/// A node thread's step longer than this while a sync is held is the stall F4 removes.
const STEP_LIMIT_MILLIS: u64 = 500;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime")
}

fn config(dir: &std::path::Path) -> DbConfig {
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

/// The spec's budgets with L1's waits cut short, as the `Db` rows' `fast()`.
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
    config: DbConfig,
    store: &Arc<dyn ConfigStore>,
    budgets: Budgets,
) -> Db {
    rt.block_on(Db::open_with(
        config,
        Arc::clone(store),
        rt.handle().clone(),
        budgets,
    ))
    .expect("open")
}

fn node_dir(dir: &std::path::Path, node: u32) -> PathBuf {
    dir.join("nodes").join(node.to_string())
}

/// Node 1's status once `done` holds for it, within the scaled `patience`.
fn owner_when(
    db: &Db,
    what: &str,
    patience: Duration,
    done: &dyn Fn(&NodeStatus) -> bool,
) -> NodeStatus {
    let patience = test_patience(patience);
    let deadline = Instant::now() + patience;
    loop {
        let owner = db
            .node_status()
            .into_iter()
            .find(|status| status.node == NodeId(1));
        match owner {
            Some(owner) if done(&owner) => return owner,
            owner => assert!(
                Instant::now() < deadline,
                "{what}: not within {patience:?}: {owner:?}"
            ),
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn admits_within(db: &Db, what: &str, patience: Duration) {
    owner_when(db, what, patience, &|node| node.admits == Some(true));
}

fn is(line: &Line, msg: &str) -> bool {
    line["@m"] == msg
}

fn num(line: &Line, field: &str) -> u64 {
    line[field]
        .as_u64()
        .unwrap_or_else(|| panic!("{field} on {line}"))
}

/// The index of the first line at or after `from` that is `msg` and matches `also`.
fn next(lines: &[Line], from: usize, msg: &str, also: &dyn Fn(&Line) -> bool) -> Option<usize> {
    (from..lines.len()).find(|&at| is(&lines[at], msg) && also(&lines[at]))
}

/// The engine's `synced_write_during_sync` and `sync_began_during_sync` lines at or after
/// `from`: each is a write or a sync that waited inside RocksDB on the fsync in flight.
fn waited_on_a_sync(lines: &[Line], from: usize) -> Vec<&Line> {
    lines[from..]
        .iter()
        .filter(|line| is(line, "synced_write_during_sync") || is(line, "sync_began_during_sync"))
        .collect()
}

/// Every node-thread step over [`STEP_LIMIT_MILLIS`] logged at or after `from`.
fn slow_steps(lines: &[Line], from: usize) -> Vec<&Line> {
    lines[from..]
        .iter()
        .filter(|line| is(line, "host_slow_step") && num(line, "millis") > STEP_LIMIT_MILLIS)
        .collect()
}

/// A-R326: with every sync held 1 s on all three nodes, a reopen's link syncs and its holders'
/// recovery `Sync` answers go through the syncer. Each continuation runs only after a sync that
/// began after its write: a `link_synced` (and the acquire it releases, node 1's `activate`)
/// follows a held sync begun after its `link_sync_requested`, and each pre-commit
/// `sync_proven` follows a held sync begun after its `holder_sync_requested`. Meanwhile no
/// node thread takes a step over 500 ms, and the reopen admits writes and reads back.
/// Cut: `land` schedules the acquire without waiting on `link_pending`, red (node 1 wrote `b`
/// with no `link_synced`). Cut: the holder answers before `Msg::Synced`, red (`sync_proven` before the
/// held sync). Cut: the link or holder sync inline on the node thread, red (a step over 500 ms).
/// Integration (~10 s): a fresh open, then a reopen whose every sync is held 1 s.
#[config_log::retcd_test]
fn m10_f4_link_and_holder_syncs_answer_only_after_a_held_sync_and_the_node_keeps_stepping() {
    const METHOD: &str =
        "m10_f4_link_and_holder_syncs_answer_only_after_a_held_sync_and_the_node_keeps_stepping";
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    {
        let mut db = open(&rt, config(dir.path()), &store, fast());
        admits_within(&db, "the first open", Duration::from_secs(10));
        db.put(b"a", b"1", None).expect("put a");
        db.shutdown();
    }
    for node in 1..=3 {
        slow_sync(&node_dir(dir.path(), node), Duration::from_secs(1), true);
    }
    tracing::info!("f4_slow_armed");
    let mut db = open(&rt, config(dir.path()), &store, fast());
    admits_within(&db, "the reopen", Duration::from_secs(30));
    let read = db.get(b"a").expect("get a");
    assert_eq!(
        read.value.map(|(_, value)| value),
        Some(Bytes::from_static(b"1"))
    );
    // A write needs the new generation activated, which waits on node 1's link sync.
    db.put(b"b", b"2", None).expect("put b after the reopen");
    db.shutdown();
    for node in 1..=3 {
        clear_slow_sync(&node_dir(dir.path(), node));
    }

    let lines = config_testkit::logs::lines_for_current_test(module_path!(), METHOD);
    let armed = next(&lines, 0, "f4_slow_armed", &|_| true).expect("the marker");
    let slow = slow_steps(&lines, armed);
    assert!(
        slow.is_empty(),
        "node-thread steps over {STEP_LIMIT_MILLIS} ms: {slow:?}"
    );
    let waited = waited_on_a_sync(&lines, armed);
    assert!(
        waited.is_empty(),
        "a write or sync waited on a sync in flight: {waited:?}"
    );

    let mut links = 0;
    for at in (armed..lines.len()).filter(|&at| is(&lines[at], "link_sync_requested")) {
        let asked = &lines[at];
        let (node, generation) = (num(asked, "node"), num(asked, "generation"));
        let same = |line: &Line| num(line, "node") == node && num(line, "generation") == generation;
        // A copy's link sync may still be in flight when the row shuts down.
        let Some(synced) = next(&lines, at, "link_synced", &same) else {
            assert!(
                node != 1,
                "node 1 g{generation}: no link_synced, yet b was written"
            );
            continue;
        };
        let held = next(&lines, at, "test_sync_slowed", &|line| {
            num(line, "node") == node
        });
        assert!(
            held.is_some_and(|held| held < synced),
            "node {node} g{generation}: link_synced without a held sync begun after its write"
        );
        if node == 1 {
            let activate = next(&lines, at, "host_duty", &|line| {
                line["kind"] == "activate" && same(line)
            });
            assert!(
                activate.is_none_or(|activate| activate > synced),
                "node 1 g{generation}: the acquire ran before its link synced"
            );
        }
        links += 1;
    }
    assert!(links > 0, "the reopen requested no link sync");

    let mut answers = 0;
    for at in (armed..lines.len())
        .filter(|&at| is(&lines[at], "holder_sync_requested") && lines[at]["post_commit"] == false)
    {
        let asked = &lines[at];
        let node = num(asked, "node");
        let same = |line: &Line| {
            num(line, "node") == node
                && num(line, "copy") == num(asked, "copy")
                && num(line, "cutoff") == num(asked, "cutoff")
                && line["post_commit"] == false
        };
        let Some(proven) = next(&lines, at, "sync_proven", &same) else {
            continue;
        };
        let held = next(&lines, at, "test_sync_slowed", &|line| {
            num(line, "node") == node
        });
        assert!(
            held.is_some_and(|held| held < proven),
            "node {node}: sync_proven without a held sync begun after its ask: {asked}"
        );
        answers += 1;
    }
    assert!(answers > 0, "the reopen proved no holder sync");
    tracing::info!(links, answers, "f4_continuations_checked");
}

/// Wait, within the scaled `patience`, until this test's log holds a line at or after `from`
/// that is `msg` and matches `also`; its index.
fn wait_for_line(
    method: &str,
    from: usize,
    msg: &str,
    also: &dyn Fn(&Line) -> bool,
    patience: Duration,
) -> usize {
    let patience = test_patience(patience);
    let deadline = Instant::now() + patience;
    loop {
        let lines = config_testkit::logs::lines_for_current_test(module_path!(), method);
        if let Some(at) = next(&lines, from, msg, also) {
            return at;
        }
        assert!(Instant::now() < deadline, "no {msg} within {patience:?}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Defect seen in this file's first row: a stop with a sync in flight joined the syncer and
/// dropped its `Msg::Synced`, so the engine still counted the sync in flight and the clean
/// stop's synced final marks logged `synced_write_during_sync`. Now the stop finishes that sync
/// first (`sync_finished_at_stop`), and nothing logs a write that waited on a sync.
/// Cut: drop `finish_sync_at_stop` from the stop, red (no `sync_finished_at_stop`, and the final
/// marks log `synced_write_during_sync`).
/// Integration (~2 s): a fresh open, then a stop while node 1's sync is held 1 s.
#[config_log::retcd_test]
fn m10_f4_a_stop_with_a_sync_in_flight_finishes_it_before_the_final_marks() {
    const METHOD: &str = "m10_f4_a_stop_with_a_sync_in_flight_finishes_it_before_the_final_marks";
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let mut db = open(&rt, config(dir.path()), &store, fast());
    admits_within(&db, "the open", Duration::from_secs(10));
    db.put(b"a", b"1", None).expect("put a");
    let one = node_dir(dir.path(), 1);
    tracing::info!("f4_slow_armed");
    slow_sync(&one, Duration::from_secs(1), true);
    let armed = config_testkit::logs::lines_for_current_test(module_path!(), METHOD).len();
    wait_for_line(
        METHOD,
        armed,
        "test_sync_slowed",
        &|line| num(line, "node") == 1,
        Duration::from_secs(5),
    );
    db.shutdown();
    clear_slow_sync(&one);

    let lines = config_testkit::logs::lines_for_current_test(module_path!(), METHOD);
    let finished = next(&lines, armed, "sync_finished_at_stop", &|line| {
        num(line, "node") == 1 && line["ok"] == true
    });
    let marks = next(&lines, armed, "host_duty", &|line| {
        num(line, "node") == 1 && line["kind"] == "final_marks"
    });
    assert!(
        finished.is_some_and(|finished| marks.is_some_and(|marks| finished < marks)),
        "node 1 stopped with a sync in flight: finished {finished:?}, final marks {marks:?}"
    );
    let waited = waited_on_a_sync(&lines, armed);
    assert!(
        waited.is_empty(),
        "a write or sync waited on a sync in flight: {waited:?}"
    );
}
