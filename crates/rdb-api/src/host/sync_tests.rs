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

/// The index ranges `[slowed, returned)` of node `node`'s held syncs at or after `from`: each
/// opens at its `test_sync_slowed` and closes at the node thread's `wal_sync_slow` for it, or
/// at the end of `lines` when it never returned.
fn held_syncs(lines: &[Line], from: usize, node: u64) -> Vec<(usize, usize)> {
    let of = |line: &Line| num(line, "node") == node;
    let mut held = Vec::new();
    let mut at = from;
    while let Some(slowed) = next(lines, at, "test_sync_slowed", &of) {
        let returned = next(lines, slowed, "wal_sync_slow", &of).unwrap_or(lines.len());
        held.push((slowed, returned));
        at = returned;
    }
    held
}

/// The `host_duty` lines of `kind` node `node` logged inside one of `held`.
fn duties_in(lines: &[Line], held: &[(usize, usize)], node: u64, kind: &str) -> usize {
    held.iter()
        .map(|&(slowed, returned)| {
            lines[slowed..returned]
                .iter()
                .filter(|line| {
                    is(line, "host_duty") && line["kind"] == kind && num(line, "node") == node
                })
                .count()
        })
        .sum()
}

/// A-R317 (2), A-R322 (b3): the owner's periodic disk duties, publish marks and the dedup trim
/// delete, run while a sync is held and neither waits on it. Each stages its write, and the next
/// sync carries it: no `synced_write_during_sync` or `sync_began_during_sync`, and no node-thread
/// step over 500 ms. A 2 s retention makes both duties due every 500 ms; node 1's every sync is
/// held 700 ms while puts go on at 20/s, so the duties land inside held syncs.
/// Cut: the periodic marks written synced, red (`synced_write_during_sync`). Cut: the trim
/// delete synced (`delete_dedup_trimmed`), red (the same line).
/// Integration (~9 s): a fresh open, then 7 s of puts with node 1's syncs held. The first trims
/// come one retention (2 s) after the first puts, so 7 s gives several trim deletes.
#[config_log::retcd_test]
fn m10_f4_mark_and_trim_duties_run_during_a_held_sync_without_waiting_on_it() {
    const METHOD: &str = "m10_f4_mark_and_trim_duties_run_during_a_held_sync_without_waiting_on_it";
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let mut config = config(dir.path());
    config.dedup_retention = Some(Duration::from_secs(2));
    let mut db = open(&rt, config, &store, Budgets::SPEC_DEFAULTS);
    admits_within(&db, "the open", Duration::from_secs(10));
    let one = node_dir(dir.path(), 1);
    tracing::info!("f4_slow_armed");
    slow_sync(&one, Duration::from_millis(700), true);
    let start = Instant::now();
    let mut refused = Vec::new();
    for at in 0u32.. {
        if start.elapsed() > Duration::from_secs(7) {
            break;
        }
        if let Err(error) = db.put(format!("k{at}").as_bytes(), b"v", None) {
            refused.push(error.to_string());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    clear_slow_sync(&one);
    db.shutdown();

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
    let held = held_syncs(&lines, armed, 1);
    let marks = duties_in(&lines, &held, 1, "marks");
    let trims = duties_in(&lines, &held, 1, "trim_delete");
    assert!(
        marks > 0 && trims > 0,
        "node 1's duties inside its {} held syncs: marks {marks}, trim deletes {trims}; \
         refused puts {refused:?}",
        held.len()
    );
    tracing::info!(
        held = held.len(),
        marks,
        trims,
        refused = refused.len(),
        "f4_b3_checked"
    );
}

/// One paced put: when it returned, from the arm, and its refusal, if any.
#[derive(Debug)]
struct Sent {
    at: Duration,
    refused: Option<String>,
}

/// Puts at 50/s from `start` until `stop` says so; every put's [`Sent`].
fn puts_at_50_per_s(db: &Db, start: Instant, stop: &dyn Fn(&[Sent]) -> bool) -> Vec<Sent> {
    let mut sent = Vec::new();
    for at in 0u32.. {
        if stop(&sent) {
            break;
        }
        let refused = db
            .put(format!("k{at}").as_bytes(), b"v", None)
            .err()
            .map(|error| error.to_string());
        sent.push(Sent {
            at: start.elapsed(),
            refused,
        });
        let due = start + Duration::from_millis(20) * (at + 1);
        if let Some(wait) = due.checked_duration_since(Instant::now()) {
            std::thread::sleep(wait);
        }
    }
    sent
}

fn status_of(db: &Db, node: u32) -> NodeStatus {
    db.node_status()
        .into_iter()
        .find(|status| status.node == NodeId(node))
        .unwrap_or_else(|| panic!("node {node} answers a status look"))
}

/// The `authority_fence` lines at or after `from`, any node.
fn fences(lines: &[Line], from: usize) -> Vec<&Line> {
    lines[from..]
        .iter()
        .filter(|line| is(line, "authority_fence"))
        .collect()
}

/// A-R317 (4), b1, once: one 3 s sync on all three nodes, under 50 puts/s on product budgets.
/// The owner keeps its grant (the same grant id, no `authority_fence`), no node-thread step
/// goes over 500 ms, and nothing waits on the held sync. L1 may pause while the sync is held
/// (durability ages past the 2 s pause age); it resumes, and puts publish again, within the
/// sync (3 s) plus L1's resume hold (5 s) plus 1 s. Durable never runs ahead of a returned
/// sync: the native rows in `rdb-storage/tests/m10_wal_sync.rs` hold that claim.
/// Cut: every sync inline on the node thread, red (node 1 lost its grant: grant 1, then 2).
/// Integration (~12 s): a fresh open, then puts until 1 s of them publish after the resume.
#[config_log::retcd_test]
fn m10_f4_one_slow_sync_on_every_node_keeps_the_grant_and_l1_resumes_after_its_hold() {
    const METHOD: &str =
        "m10_f4_one_slow_sync_on_every_node_keeps_the_grant_and_l1_resumes_after_its_hold";
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let mut db = open(&rt, config(dir.path()), &store, Budgets::SPEC_DEFAULTS);
    admits_within(&db, "the open", Duration::from_secs(10));
    db.put(b"a", b"1", None).expect("put a");
    let before = status_of(&db, 1).authority;
    assert!(before.starts_with("held"), "node 1 before: {before}");
    tracing::info!("f4_slow_armed");
    for node in 1..=3 {
        slow_sync(&node_dir(dir.path(), node), Duration::from_secs(3), false);
    }
    let start = Instant::now();
    let bound = test_patience(Duration::from_secs(3 + 5 + 1));
    let sent = puts_at_50_per_s(&db, start, &|sent| {
        let calm = sent.len() >= 50 && sent[sent.len() - 50..].iter().all(|s| s.refused.is_none());
        (calm && start.elapsed() > Duration::from_secs(4))
            || start.elapsed() > bound + Duration::from_secs(2)
    });
    let after = status_of(&db, 1);
    db.shutdown();
    for node in 1..=3 {
        clear_slow_sync(&node_dir(dir.path(), node));
    }

    let refused: Vec<&Sent> = sent.iter().filter(|s| s.refused.is_some()).collect();
    let last_refused = refused.last().map(|s| s.at);
    let resumed = sent
        .iter()
        .find(|s| s.refused.is_none() && last_refused.is_none_or(|last| s.at > last))
        .map(|s| s.at);
    tracing::info!(
        puts = sent.len(),
        refused = refused.len(),
        first_refused = ?refused.first(),
        last_refused = ?last_refused,
        resumed = ?resumed,
        "f4_b1_once_puts"
    );
    assert_eq!(
        (
            after.authority.as_str(),
            after.fence,
            after.fault.as_deref()
        ),
        (before.as_str(), None, None),
        "node 1 after the held sync: {after:?}"
    );
    assert!(
        resumed.is_some_and(|resumed| resumed <= bound),
        "puts did not publish again within {bound:?}: resumed {resumed:?}, {} refused, last {:?}",
        refused.len(),
        refused.last()
    );
    let tail = &sent[sent.len().saturating_sub(50)..];
    assert!(
        tail.iter().all(|s| s.refused.is_none()),
        "the last second of puts: {tail:?}"
    );

    let lines = config_testkit::logs::lines_for_current_test(module_path!(), METHOD);
    let armed = next(&lines, 0, "f4_slow_armed", &|_| true).expect("the marker");
    let fenced = fences(&lines, armed);
    assert!(
        fenced.is_empty(),
        "fences while the sync was held: {fenced:?}"
    );
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
    let held: usize = (1..=3)
        .map(|node| held_syncs(&lines, armed, node).len())
        .sum();
    assert_eq!(held, 3, "one held sync per node");
}

/// A-R317 (4), b1, every: every sync held 3 s on all three nodes for 12 s, under 50 puts/s on
/// product budgets. Durability then ages past L1's pause age again and again, so puts may be
/// refused `PROTECTION_PAUSED` for most of the run; that is L1 doing its job, not a stall. What
/// must hold: the owner keeps its grant (same grant id, no fence), no node faults (3 s is under
/// the 5 s stuck limit), no node-thread step goes over 500 ms, and nothing waits on a held sync.
/// Cut: every sync inline on the node thread, red (node 1 lost its grant: grant 1, then 2).
/// Integration (~15 s): a fresh open, 12 s of held syncs, then a stop that waits the last out.
#[config_log::retcd_test]
fn m10_f4_every_sync_slow_on_every_node_never_stalls_a_node_thread_or_costs_the_grant() {
    const METHOD: &str =
        "m10_f4_every_sync_slow_on_every_node_never_stalls_a_node_thread_or_costs_the_grant";
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let mut db = open(&rt, config(dir.path()), &store, Budgets::SPEC_DEFAULTS);
    admits_within(&db, "the open", Duration::from_secs(10));
    db.put(b"a", b"1", None).expect("put a");
    let before = status_of(&db, 1).authority;
    assert!(before.starts_with("held"), "node 1 before: {before}");
    tracing::info!("f4_slow_armed");
    for node in 1..=3 {
        slow_sync(&node_dir(dir.path(), node), Duration::from_secs(3), true);
    }
    let start = Instant::now();
    let sent = puts_at_50_per_s(&db, start, &|_| start.elapsed() > Duration::from_secs(12));
    let statuses: Vec<NodeStatus> = (1..=3).map(|node| status_of(&db, node)).collect();
    for node in 1..=3 {
        clear_slow_sync(&node_dir(dir.path(), node));
    }
    db.shutdown();

    let refused = sent.iter().filter(|s| s.refused.is_some()).count();
    tracing::info!(puts = sent.len(), refused, "f4_b1_every_puts");
    assert_eq!(
        (statuses[0].authority.as_str(), statuses[0].fence),
        (before.as_str(), None),
        "node 1 after 12 s of held syncs: {:?}",
        statuses[0]
    );
    let faulted: Vec<&NodeStatus> = statuses.iter().filter(|s| s.fault.is_some()).collect();
    assert!(
        faulted.is_empty(),
        "a node faulted on a 3 s sync: {faulted:?}"
    );

    let lines = config_testkit::logs::lines_for_current_test(module_path!(), METHOD);
    let armed = next(&lines, 0, "f4_slow_armed", &|_| true).expect("the marker");
    let fenced = fences(&lines, armed);
    assert!(
        fenced.is_empty(),
        "fences while syncs were held: {fenced:?}"
    );
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
    let held: Vec<usize> = (1..=3)
        .map(|node| held_syncs(&lines, armed, node).len())
        .collect();
    assert!(
        held.iter().all(|&held| held >= 3),
        "held syncs per node: {held:?}"
    );
}

/// A-R317 (5), b2: a sync stuck on node 1 (held 8 s, past the 5 s stuck limit) faults node 1
/// node-wide (`wal_sync_stuck`, then `host_fault`) no sooner than the limit, so A1 renews no
/// more and the grant lapses. A promote to node 2 then lands writes there within the limit plus
/// the grant's 2.9 s plus 3 s to find and promote. Node 1 is still faulted after its sync
/// returns: a late sync does not unfault a node.
/// Cut: no stuck check (`stuck_sync` never faults), red (node 1 never faults).
/// Integration (~12 s): a fresh open, an 8 s held sync, a promote and a put.
#[config_log::retcd_test]
fn m10_f4_a_stuck_sync_faults_the_node_and_a_promote_takes_over_in_time() {
    const METHOD: &str = "m10_f4_a_stuck_sync_faults_the_node_and_a_promote_takes_over_in_time";
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let mut db = open(&rt, config(dir.path()), &store, Budgets::SPEC_DEFAULTS);
    admits_within(&db, "the open", Duration::from_secs(10));
    db.put(b"a", b"1", None).expect("put a");
    let one = node_dir(dir.path(), 1);
    tracing::info!("f4_slow_armed");
    let armed = config_testkit::logs::lines_for_current_test(module_path!(), METHOD).len();
    slow_sync(&one, Duration::from_secs(8), false);
    wait_for_line(
        METHOD,
        armed,
        "test_sync_slowed",
        &|line| num(line, "node") == 1,
        Duration::from_secs(5),
    );
    let held = Instant::now();
    let faulted = owner_when(&db, "node 1 faults", Duration::from_secs(5 + 2), &|node| {
        node.fault.is_some()
    });
    let fault_after = held.elapsed();
    assert!(
        fault_after >= Duration::from_millis(5_000 - 300),
        "node 1 faulted {fault_after:?} after its sync was held, before the 5 s limit: {faulted:?}"
    );

    let promoted = db.promote(NodeId(2), None).expect("promote node 2");
    let patience = test_patience(Duration::from_secs(10));
    let deadline = Instant::now() + patience;
    let put = loop {
        match db.put(b"b", b"2", None) {
            Ok(put) => break put,
            Err(refused) => assert!(
                Instant::now() < deadline,
                "no put landed on node 2 within {patience:?}: {refused:?}"
            ),
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let took_over = held.elapsed();
    assert_eq!(put.generation, promoted.generation, "{put:?}");
    let bound = test_patience(Duration::from_millis(5_000 + 2_900 + 3_000));
    assert!(
        took_over <= bound,
        "node 2 took writes {took_over:?} after node 1's sync stuck; bound {bound:?}"
    );

    // Past the held sync's return: node 1 stays faulted.
    if let Some(left) = Duration::from_millis(8_500).checked_sub(held.elapsed()) {
        std::thread::sleep(left);
    }
    let late = status_of(&db, 1);
    assert!(
        late.fault.is_some(),
        "node 1 after its sync returned: {late:?}"
    );
    clear_slow_sync(&one);
    db.shutdown();

    let lines = config_testkit::logs::lines_for_current_test(module_path!(), METHOD);
    let stuck = next(&lines, armed, "wal_sync_stuck", &|line| {
        num(line, "node") == 1
    });
    let fault = next(&lines, armed, "host_fault", &|line| num(line, "node") == 1);
    assert!(
        stuck.is_some_and(|stuck| fault.is_some_and(|fault| stuck < fault)),
        "node 1: wal_sync_stuck {stuck:?}, host_fault {fault:?}"
    );
    tracing::info!(?fault_after, ?took_over, "f4_b2_checked");
}
