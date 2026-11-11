//! Issue #90 (#80 S5d): the durability barrier through [`Db`], on wall-clock time. Rows 40's
//! drop under writes, 41, 42 and 47; the rows on three stepped hosts are in `host::barrier_tests`.

use super::reopen_tests::{config, fast, runtime};
use super::*;
use rdb_core::contracts::recovery::TailRelease;
use std::sync::atomic::{AtomicBool, Ordering};

const GROUP_OF: AffinityId = DEFAULT_GROUP;
const LONG: Duration = Duration::from_secs(90);

fn store() -> Arc<dyn ConfigStore> {
    Arc::new(config_testkit::MemStore::new())
}

fn opened(rt: &tokio::runtime::Runtime, dir: &Path, store: &Arc<dyn ConfigStore>) -> Db {
    let db = rt
        .block_on(Db::open_with(
            config(dir),
            Arc::clone(store),
            rt.handle().clone(),
            fast(),
        ))
        .expect("the open");
    db.wait_ready(crate::host::test_patience(Duration::from_secs(10)))
        .expect("ready");
    db
}

fn position(put: &PutOk) -> (Generation, Seq) {
    (put.generation, put.seq)
}

/// Row 41: a barrier held on a slow fsync blocks no write, and two barriers overlap. The second
/// put returning while the barrier is still waiting is the claim; its order is observed, not
/// its duration, so a loaded host cannot pass or fail it on time alone.
/// Integration (~3 s): the fsync is held for 1.5 s.
#[config_log::retcd_test]
fn s5d_41_a_barrier_on_a_slow_sync_blocks_no_write_and_barriers_overlap() {
    const METHOD: &str = "s5d_41_a_barrier_on_a_slow_sync_blocks_no_write_and_barriers_overlap";
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let db = opened(&rt, dir.path(), &store);
    let node_dir = dir.path().join("nodes").join("3");
    crate::host::slow_sync(&node_dir, Duration::from_millis(1_500), true);
    let first = db.put(b"a", b"1", None).expect("the first put");
    let answered = AtomicBool::new(false);
    std::thread::scope(|scope| {
        let waits: Vec<_> = (0..2)
            .map(|_| {
                scope.spawn(|| {
                    let got = db.wait_durable(GROUP_OF, position(&first), LONG);
                    answered.store(true, Ordering::SeqCst);
                    got
                })
            })
            .collect();
        let deadline = Instant::now() + crate::host::test_patience(Duration::from_secs(5));
        while config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
            .iter()
            .filter(|line| line["@m"] == "durable_barrier_started")
            .count()
            < 2
        {
            assert!(Instant::now() < deadline, "no barrier started");
            std::thread::yield_now();
        }

        db.put(b"b", b"2", None).expect("the later put");
        assert!(
            !answered.load(Ordering::SeqCst),
            "the barrier answered before the later put returned, so this run held nothing"
        );
        for wait in waits {
            wait.join().expect("the barrier").expect("Ok");
        }
    });
    crate::host::clear_slow_sync(&node_dir);
}

/// Row 40, the drop: a copy killed under continuous writes ends the barrier in flight in
/// `GENERATION_CHANGED` through `Host::recovered`, and a barrier on a write in the new generation
/// is `Ok` from the two copies left. Prints the seconds from the kill to the refusal.
/// Integration (~20 s): the drop clock.
#[config_log::retcd_test]
fn s5d_40_a_killed_copy_ends_the_barrier_in_flight_and_the_new_generation_answers() {
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let db = opened(&rt, dir.path(), &store);
    let stop = AtomicBool::new(false);
    std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            let mut wrote = 0_u64;
            while !stop.load(Ordering::SeqCst) {
                wrote += 1;
                if db.put(b"w", wrote.to_string().as_bytes(), None).is_err() {
                    std::thread::yield_now();
                }
            }
            wrote
        });
        let before = db
            .put(b"mark", b"1", None)
            .expect("a write before the kill");
        lock(&db.nodes)
            .iter_mut()
            .find(|handle| handle.node == NodeId(3))
            .expect("node 3")
            .stop();
        let killed = Instant::now();

        let error = db
            .wait_durable(GROUP_OF, position(&before), LONG)
            .expect_err("no barrier can finish with a copy gone");
        let seconds = killed.elapsed().as_secs_f64();
        assert_eq!(error.kind, ErrorKind::GenerationChanged, "{error:?}");
        assert!(!error.no_mutation, "{error:?}");
        tracing::info!(seconds, "s5d_40_kill_to_generation_changed");
        println!("s5d_40 kill to GenerationChanged: {seconds:.1} s");

        let after = loop {
            match db.put(b"mark", b"2", None) {
                Ok(put) if put.generation > before.generation => break put,
                _ => std::thread::yield_now(),
            }
        };
        db.wait_durable(GROUP_OF, position(&after), LONG)
            .expect("the two copies left answer");
        stop.store(true, Ordering::SeqCst);
        let wrote = writer.join().expect("the writer");
        assert!(wrote > 0, "the writer ran");
    });
}

/// The most passes the journal helper takes before it gives up loudly.
const MAX_PASSES: u32 = 400;

/// How long the helper waits after a refusal before its next pass.
const PASS_BACKOFF: Duration = Duration::from_millis(50);

/// The journal contract (design 5.6), written as a test helper over rDB's answers: read back
/// every key, resend what is missing or different, write the marker, barrier on the marker, and
/// finish only when every key's generation, every resend's and the marker's are the same. Any
/// refusal, or a generation mismatch, starts again. `before_marker` runs before each marker.
fn journal_sync(
    db: &Db,
    keys: &[(&[u8], &[u8])],
    marker: &[u8],
    mut before_marker: impl FnMut(u32),
) -> (u32, Generation, Vec<usize>) {
    let mut seen = Vec::new();
    for pass in 1..=MAX_PASSES {
        let mut generations = BTreeSet::new();
        let mut refused = false;
        for (key, value) in keys {
            let got = match db.get(key) {
                Ok(got) if !got.recovery_mode => got,
                _ => {
                    refused = true;
                    break;
                }
            };
            let held = got
                .value
                .as_ref()
                .is_some_and(|(_, bytes)| bytes.as_ref() == *value);
            if held {
                generations.insert(got.generation);
            } else {
                match db.put(key, value, None) {
                    Ok(put) => {
                        generations.insert(put.generation);
                    }
                    Err(_) => {
                        refused = true;
                        break;
                    }
                }
            }
        }
        if refused {
            std::thread::sleep(PASS_BACKOFF);
            continue;
        }
        before_marker(pass);
        let Ok(mark) = db.put(marker, pass.to_string().as_bytes(), None) else {
            std::thread::sleep(PASS_BACKOFF);
            continue;
        };
        generations.insert(mark.generation);
        if db.wait_durable(GROUP_OF, position(&mark), LONG).is_err() {
            continue;
        }
        tracing::info!(pass, generations = ?generations, "journal_pass");
        seen.push(generations.len());
        if generations.len() == 1 {
            return (pass, mark.generation, seen);
        }
    }
    panic!("the journal did not settle in {MAX_PASSES} passes");
}

/// Each WAL file under `node_dir` and its length now.
fn wal_lengths(node_dir: &Path) -> BTreeMap<PathBuf, u64> {
    let mut lengths = BTreeMap::new();
    for entry in std::fs::read_dir(node_dir).expect("the node directory") {
        let entry = entry.expect("a directory entry");
        if entry.path().extension().is_some_and(|ext| ext == "log") {
            lengths.insert(entry.path(), entry.metadata().expect("metadata").len());
        }
    }
    lengths
}

/// Back to `lengths`: a machine that lost everything after that moment.
fn cut_wal(node_dir: &Path, lengths: &BTreeMap<PathBuf, u64>) {
    for entry in std::fs::read_dir(node_dir).expect("the node directory") {
        let path = entry.expect("a directory entry").path();
        if path.extension().is_none_or(|ext| ext != "log") {
            continue;
        }
        match lengths.get(&path) {
            Some(length) => std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .expect("the WAL opens")
                .set_len(*length)
                .expect("the WAL is cut"),
            None => std::fs::remove_file(&path).expect("the later WAL is removed"),
        }
    }
}

fn open_process(
    rt: &tokio::runtime::Runtime,
    dir: &Path,
    store: &Arc<dyn ConfigStore>,
    node: u32,
    links: &Arc<Links>,
) -> Db {
    rt.block_on(Db::open_with(
        DbConfig {
            local: vec![NodeId(node)],
            links: Some(Arc::clone(links)),
            ..config(dir)
        },
        Arc::clone(store),
        rt.handle().clone(),
        fast(),
    ))
    .unwrap_or_else(|error| panic!("node {node} opens: {error}"))
}

/// Row 47: chunks K1 and K2 are acknowledged in G and read back in G. K1 is on every disk and K2
/// on no secondary's. Before the marker the owner is cut off, the one secondary that acknowledged
/// K2 loses power and returns without it, and a promote commits G+1 with its cutoff below K2. The
/// marker's generation differs, so the helper starts again, finds K2 missing, resends it, and
/// finishes with every generation equal and K2 present. Cut: remove the equality check, and it
/// finishes after one pass with K2 missing.
/// Integration (~15 s): node processes, a grant wait and a reopen.
#[config_log::retcd_test]
fn s5d_47_a_generation_change_before_the_marker_forces_another_pass() {
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let patience = crate::host::test_patience(Duration::from_secs(20));
    let mut formed = opened(&rt, dir.path(), &store);
    formed.shutdown();
    drop(formed);

    let links = Links::new();
    let mut nodes: Vec<Option<Db>> = [1, 2, 3]
        .map(|node| Some(open_process(&rt, dir.path(), &store, node, &links)))
        .into();
    for db in nodes.iter().flatten() {
        db.wait_ready(patience).expect("each node is ready");
    }
    let app_dir = config_testkit::fs::temp_dir();
    let app = rt
        .block_on(Db::open_with(
            DbConfig {
                local: Vec::new(),
                links: Some(Arc::clone(&links)),
                client: ClientId(2),
                ..config(app_dir.path())
            },
            Arc::clone(&store),
            rt.handle().clone(),
            fast(),
        ))
        .expect("the app opens");
    app.wait_ready(patience).expect("the app sees the owner");

    let k1 = app.put(b"k1", b"one", None).expect("k1 in G");
    app.wait_durable(GROUP_OF, position(&k1), LONG)
        .expect("k1 is on every disk");
    let third = dir.path().join("nodes").join("3");
    let before_k2 = wal_lengths(&third);
    links.hold(NodeId(1), NodeId(2));
    links.hold(NodeId(2), NodeId(3));
    let k2 = app.put(b"k2", b"two", None).expect("k2 in G");
    assert_eq!(k1.generation, k2.generation);

    let mut recovered = false;
    let (passes, settled, _) =
        journal_sync(&app, &[(b"k1", b"one"), (b"k2", b"two")], b"marker", |_| {
            if recovered {
                return;
            }
            recovered = true;
            links.hold(NodeId(1), NodeId(3));
            drop(nodes[2].take());
            cut_wal(&third, &before_k2);
            links.heal(NodeId(2), NodeId(3));
            nodes[2] = Some(open_process(&rt, dir.path(), &store, 3, &links));
            app.promote(NodeId(2), None).expect("the promote commits");
            let deadline = Instant::now() + crate::host::test_patience(Duration::from_secs(30));
            while app.put(b"probe", b"1", None).is_err() {
                assert!(Instant::now() < deadline, "no write was admitted");
                std::thread::yield_now();
            }
        });

    let got = app.get(b"k2").expect("the read");
    let (_, value) = got.value.expect("k2 is present");
    assert_eq!(value.as_ref(), b"two");
    assert!(passes >= 2, "the recovery forced another pass: {passes}");
    assert!(settled > k2.generation, "{settled:?}");
}

/// Row 47, the seq-0 clause, as far as it can be reached. `GetOk::generation`'s doc says a read
/// right after a cutoff can name the predecessor generation. Polled from the promote's return
/// with nothing written (S5d-B4), every read named the new generation at the cutoff, so the
/// journal took no extra pass. If a read ever does name the predecessor, the first assert fails
/// and this row should then expect the one extra pass the design describes.
/// Integration (~5 s): a grant wait.
#[config_log::retcd_test]
fn s5d_47_a_read_right_after_a_cutoff_names_the_new_generation_and_the_journal_ends() {
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let db = opened(&rt, dir.path(), &store);
    let k1 = db.put(b"k1", b"one", None).expect("k1");
    db.wait_durable(GROUP_OF, position(&k1), LONG)
        .expect("k1 is on every disk");
    db.links().hold(NodeId(1), NodeId(2));
    db.links().hold(NodeId(1), NodeId(3));
    db.promote(NodeId(2), None).expect("the promote commits");

    // Reads are refused for a while after the promote returns, for longer on a loaded host, so
    // the window starts at the first read that answers: the one nearest the cutoff.
    let deadline = Instant::now() + crate::host::test_patience(Duration::from_secs(30));
    let mut views: Vec<Generation> = Vec::new();
    while views.len() < 200 {
        match db.get(b"k1") {
            Ok(got) => views.push(got.generation),
            Err(error) => assert!(Instant::now() < deadline, "no read answered: {error:?}"),
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(
        views.iter().all(|generation| *generation > k1.generation),
        "a read named the predecessor: {views:?}"
    );

    let (_, settled, seen) = journal_sync(&db, &[(b"k1", b"one")], b"marker", |_| {});
    assert!(settled > k1.generation, "{settled:?}");
    assert_eq!(
        seen.last(),
        Some(&1),
        "it ended with one generation: {seen:?}"
    );
}

/// Row 42's measurement. Not a pass/fail on this host: the 250 ms target is on a dedicated SSD.
/// Run alone with `--ignored --nocapture`. 256 partitions, each with a copy on all three nodes, so
/// 256 on every node's disk (design 5.6, "one sync covers every partition on the node"). Each has
/// one write and barrier per second, all firing on the same tick as one timer per node would, on
/// one thread per partition so a slow barrier cannot delay the next, under pipelined writes.
/// Prints the rate achieved: a run that fell behind its ticks did not offer this load.
#[config_log::retcd_test]
#[ignore = "perf: run alone by hand on the disk being measured"]
fn s5d_42_barrier_latency_at_256_partitions_per_node() {
    const PARTITIONS: u32 = 256;
    const SECONDS: u64 = 30;
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let db = rt
        .block_on(Db::open_with(
            DbConfig {
                partitions: Some(PARTITIONS),
                ..config(dir.path())
            },
            Arc::clone(&store),
            rt.handle().clone(),
            fast(),
        ))
        .expect("the open");
    db.wait_ready(crate::host::test_patience(Duration::from_secs(120)))
        .expect("ready");

    let groups: Vec<AffinityId> = (1..=PARTITIONS)
        .map(|p| {
            (1..1_000_000)
                .map(AffinityId)
                .find(|group| db.partition_for(*group) == PartitionId(p))
                .expect("some group maps to every partition")
        })
        .collect();
    let syncs_before: Vec<u64> = (1..=3)
        .map(|n| crate::host::wal_sync_count(NodeId(n)))
        .collect();
    let started = Instant::now();
    let stop = AtomicBool::new(false);
    let (mut millis, failed): (Vec<f64>, Vec<String>) = std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            let mut object = 0_u64;
            while !stop.load(Ordering::SeqCst) {
                for group in groups.iter().step_by(8) {
                    object += 1;
                    let _ = db.put_in(*group, object.to_string().as_bytes(), b"w", None);
                }
            }
        });
        let slices: Vec<_> = groups
            .iter()
            .map(|group| {
                let db = &db;
                scope.spawn(move || {
                    let mut measured = Vec::new();
                    let mut failed = Vec::new();
                    for second in 0..SECONDS {
                        let tick = started + Duration::from_secs(second + 1);
                        std::thread::sleep(tick.saturating_duration_since(Instant::now()));
                        let put = match db.put_in(*group, b"timer", b"t", None) {
                            Ok(put) => put,
                            Err(error) => {
                                failed.push(format!("put {:?}", error.error.kind));
                                continue;
                            }
                        };
                        let at = Instant::now();
                        match db.wait_durable(*group, position(&put), Duration::from_secs(5)) {
                            Ok(()) => measured.push(at.elapsed().as_secs_f64() * 1000.0),
                            Err(error) => failed.push(format!("barrier {:?}", error.kind)),
                        }
                    }
                    (measured, failed)
                })
            })
            .collect();
        let mut out = (Vec::new(), Vec::new());
        for slice in slices {
            let (measured, failed) = slice.join().expect("a slice");
            out.0.extend(measured);
            out.1.extend(failed);
        }
        stop.store(true, Ordering::SeqCst);
        writer.join().expect("the writer");
        out
    });
    let elapsed = started.elapsed().as_secs_f64();
    millis.sort_by(f64::total_cmp);
    let mut kinds = BTreeMap::new();
    for kind in &failed {
        *kinds.entry(kind.as_str()).or_insert(0_u32) += 1;
    }
    println!("s5d_42 failed_ticks={} {kinds:?}", failed.len());
    let at = |q: f64| millis[((millis.len() - 1) as f64 * q) as usize];
    let over = |limit: f64| millis.iter().filter(|m| **m > limit).count();
    let per_node: Vec<f64> = (1..=3)
        .map(|n| {
            (crate::host::wal_sync_count(NodeId(n)) - syncs_before[n as usize - 1]) as f64 / elapsed
        })
        .collect();
    println!(
        "s5d_42 elapsed={elapsed:.1}s ok_per_s={:.1} (asked {PARTITIONS}) ok={} p50={:.1}ms p99={:.1}ms max={:.1}ms over250ms={} over1s={} syncs_per_s_per_node={per_node:?}",
        millis.len() as f64 / elapsed,
        millis.len(),
        at(0.50),
        at(0.99),
        at(1.0),
        over(250.0),
        over(1000.0),
    );
}

/// Row "loop ends" on the original schedule (#79 S4d, S5d-B1, row 47's first form): K2 is on node
/// 3 only, the owner and node 3 are cut from node 2, and a promote commits `ReadOnly` at a
/// cutoff below K2. Node 3 is linked again and its landing refuses `held_past_base`. Before S4d
/// this loop committed `ReadOnly` every 5 to 60 s for good. Now node 2 holds the repair and names
/// the copy and reason once, and for 30 s nothing recommits.
/// Issue #105 unit 2: K2 is stamped by the last writable generation, so with its proofs holding
/// node 3 adopts it and the hold ends (W-C's outcome). Node 3's proofs are fixed to fail
/// (`tail_not_contiguous`, `crate::host::refuse_adoption`) and its status says so, so the row's
/// subject stays the hold.
/// Cut: remove the hold in `repair_duty`, red (more repairs, generation moves within 30 s).
/// Integration (~50 s): the grant wait, the promote, and the 30 s window.
#[config_log::retcd_test]
fn s4d_a_lone_survivor_with_a_tail_holds_its_repair_and_stops_recommitting() {
    const METHOD: &str = "s4d_a_lone_survivor_with_a_tail_holds_its_repair_and_stops_recommitting";
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let patience = crate::host::test_patience(Duration::from_secs(60));
    crate::host::refuse_adoption(&dir.path().join("nodes").join("3"), "tail_not_contiguous");
    let mut formed = opened(&rt, dir.path(), &store);
    formed.shutdown();
    drop(formed);

    let links = Links::new();
    let nodes: Vec<Db> = [1, 2, 3]
        .map(|node| open_process(&rt, dir.path(), &store, node, &links))
        .into();
    for db in &nodes {
        db.wait_ready(patience).expect("each node is ready");
    }
    let app_dir = config_testkit::fs::temp_dir();
    let app = rt
        .block_on(Db::open_with(
            DbConfig {
                local: Vec::new(),
                links: Some(Arc::clone(&links)),
                client: ClientId(2),
                ..config(app_dir.path())
            },
            Arc::clone(&store),
            rt.handle().clone(),
            fast(),
        ))
        .expect("the app opens");
    app.wait_ready(patience).expect("the app sees the owner");

    let k1 = app.put(b"k1", b"one", None).expect("k1 in G");
    app.wait_durable(GROUP_OF, position(&k1), LONG)
        .expect("k1 is on every disk");
    links.hold(NodeId(1), NodeId(2));
    links.hold(NodeId(2), NodeId(3));
    let k2 = app
        .put(b"k2", b"two", None)
        .expect("k2 in G, on node 3 only");
    links.hold(NodeId(1), NodeId(3));
    app.promote(NodeId(2), None).expect("the promote commits");
    links.heal(NodeId(2), NodeId(3));

    let status_of = |db: &Db| db.node_status().into_iter().next().expect("its own status");
    let deadline = Instant::now() + patience;
    let tail = loop {
        if let Some(tail) = status_of(&nodes[2]).tail_refused {
            break tail;
        }
        assert!(Instant::now() < deadline, "node 3 reported no refusal");
        std::thread::sleep(Duration::from_millis(20));
    };
    assert!(tail.head.0 >= k2.seq.0, "{tail:?} {k2:?}");
    assert!(tail.base.0 < tail.head.0, "{tail:?}");
    assert_eq!(
        tail.adoption,
        crate::host::TailAdoption::Refused("tail_not_contiguous"),
        "{tail:?}"
    );
    let held = |reason: &str| {
        config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
            .into_iter()
            .filter(|line| {
                line["@m"] == "membership_repair_held" && line["reason"].as_str() == Some(reason)
            })
            .collect::<Vec<_>>()
    };
    let deadline = Instant::now() + patience;
    let lines = loop {
        let lines = held(tail.reason);
        if !lines.is_empty() {
            break lines;
        }
        assert!(Instant::now() < deadline, "no membership_repair_held");
        std::thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(lines[0]["copy_node"], 3, "{lines:?}");
    assert_eq!(lines[0]["node"], 2, "{lines:?}");
    assert_eq!(lines[0]["head"], tail.head.0, "{lines:?}");

    let settled = status_of(&nodes[1]).recovered;
    let repairs = || {
        config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
            .into_iter()
            .filter(|line| line["@m"] == "membership_repair_planned" && line["node"] == 2)
            .count()
    };
    let repairs_after_hold = repairs();
    let window = Instant::now() + Duration::from_secs(30);
    while Instant::now() < window {
        assert_eq!(
            status_of(&nodes[1]).recovered,
            settled,
            "the repair recommitted"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    assert!(repairs() <= 1, "more than one repair: {}", repairs());
    assert_eq!(repairs(), repairs_after_hold, "a repair ran after the hold");
    assert_eq!(held(tail.reason).len(), 1, "logged once");

    let refused = app.put(b"k3", b"three", None).expect_err("still read-only");
    assert_eq!(
        refused.error.kind,
        ErrorKind::RecoveryReadOnly,
        "{refused:?}"
    );
    let text = refused.error.explain();
    assert!(text.contains("on node 3"), "{text}");
    assert!(text.contains("held_past_base"), "{text}");
}

/// #105 W-C, walk C's schedule: K2 is acknowledged `BufferedOnTwo` on nodes 1 and 3 only, the
/// operator promotes node 2 (which lacks it), node 3 returns live and still holds K2, and node 1
/// comes back with its data gone. Before the guard the partition went writable with cutoff below
/// K2: K2 read absent, and the next write took K2's seq. Unit 1 held it `ReadOnly` for good while
/// node 3 answered. Unit 2 (design section 9.2, "After unit 2"): node 3 adopts K2 automatically,
/// the commit is writable at cutoff K2, K2 reads `two`, the next write takes the seq after K2,
/// and K2's own request sent again replays its answer instead of applying twice (A9).
/// Critic-s15 M3: K2's resend answers `GENERATION_CHANGED`, and the next put lands at the seq
/// after the first put. That is neither a lost nor a duplicated write. Not lost: K2 reads `two`
/// at its own seq, adopted with the record its client was acknowledged for. Not duplicated: the
/// pinned resend is stopped by the generation fence, and the unpinned one by Dedup, which still
/// holds K2's row under K2's generation and answers it as spec section 8.1 says for an older
/// generation's row, never executing it again; no seq is taken, which the next put's seq proves.
/// The client learns K2's outcome by reading it, as `GENERATION_CHANGED` tells it to.
/// Cut: the holder never adopts (`Host::adoption` answers its refusal), red (no put).
/// Integration (~45 s): the grant wait, the promote, a reopen and the repair's window.
#[config_log::retcd_test]
fn w_c_a_write_acked_on_two_copies_comes_back_from_the_live_copy_that_holds_it() {
    const METHOD: &str =
        "w_c_a_write_acked_on_two_copies_comes_back_from_the_live_copy_that_holds_it";
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let patience = crate::host::test_patience(Duration::from_secs(60));
    let mut formed = opened(&rt, dir.path(), &store);
    formed.shutdown();
    drop(formed);

    let links = Links::new();
    let mut nodes: Vec<Option<Db>> = [1, 2, 3]
        .map(|node| Some(open_process(&rt, dir.path(), &store, node, &links)))
        .into();
    for db in nodes.iter().flatten() {
        db.wait_ready(patience).expect("each node is ready");
    }
    let app_dir = config_testkit::fs::temp_dir();
    let app = rt
        .block_on(Db::open_with(
            DbConfig {
                local: Vec::new(),
                links: Some(Arc::clone(&links)),
                client: ClientId(2),
                ..config(app_dir.path())
            },
            Arc::clone(&store),
            rt.handle().clone(),
            fast(),
        ))
        .expect("the app opens");
    app.wait_ready(patience).expect("the app sees the owner");

    let k1 = app.put(b"k1", b"one", None).expect("k1 in G");
    app.wait_durable(GROUP_OF, position(&k1), LONG)
        .expect("k1 is on every disk");
    links.hold(NodeId(1), NodeId(2));
    links.hold(NodeId(2), NodeId(3));
    let k2 = app
        .put(b"k2", b"two", None)
        .expect("k2 in G, on nodes 1 and 3");
    assert_eq!(k2.durability, Durability::BufferedOnTwo, "{k2:?}");
    links.hold(NodeId(1), NodeId(3));
    app.promote(NodeId(2), None).expect("the promote commits");
    links.heal(NodeId(2), NodeId(3));

    // Node 3 has heard of the promote: it refused the landing, or it already adopted K2.
    let deadline = Instant::now() + patience;
    let three = nodes[2].as_ref().expect("node 3");
    let adopted_on = |node: u64| {
        config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
            .into_iter()
            .filter(|line| line["@m"] == "tail_adopted" && line["node"] == node)
            .count()
    };
    while three
        .node_status()
        .into_iter()
        .next()
        .expect("its own status")
        .tail_refused
        .is_none()
        && adopted_on(3) == 0
    {
        assert!(
            Instant::now() < deadline,
            "node 3 heard nothing of the promote"
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    // Node 1 loses its data: stopped, folder moved aside, started empty on healed links.
    let mut one = nodes[0].take().expect("node 1");
    one.shutdown();
    drop(one);
    let node_dir = dir.path().join("nodes").join("1");
    std::fs::rename(&node_dir, dir.path().join("nodes").join("1.aside")).expect("moved aside");
    links.heal(NodeId(1), NodeId(2));
    links.heal(NodeId(1), NodeId(3));
    nodes[0] = Some(open_process(&rt, dir.path(), &store, 1, &links));
    // Separate `Db`s share no handshake, so nodes 2 and 3 hear node 1's new boot as a net
    // handshake tells a node process (A-R249): the owner then re-plans to add it back.
    let boot = links
        .boot_of(NodeId(1))
        .expect("node 1 registered its boot");
    for (at, node) in [(1, NodeId(2)), (2, NodeId(3))] {
        let db = nodes[at].as_ref().expect("a running node");
        db.send_to(
            node,
            Msg::PeerBooted {
                node: NodeId(1),
                boot,
            },
        )
        .expect("the node hears the boot");
    }
    // Node 1 is re-added in two commits: a re-plan without it (`DegradedRf2`), then, once its
    // shadow cursor has caught it up, `shadow_promote` re-plans again to `Active`. A put in flight
    // across that second re-plan answers `UnknownOutcome` although it is durable, and a send
    // across either answers `GENERATION_CHANGED` or `LEASE_EXPIRED`; under load each of this
    // row's three sends below landed inside one (gate on dc1d03d, 2026-11-10). So each waits for
    // the re-add to settle: the owner's latest commit `Active`, and the owner admitting.
    let owner = nodes[1].as_ref().expect("node 2");
    let settled = || {
        recovered_modes(METHOD, 2).last().map(String::as_str) == Some("Active")
            && owner
                .node_status()
                .first()
                .is_some_and(|status| status.admits == Some(true))
    };
    wait_for("node 1's re-add settled", patience, settled);

    let put = first_put(&app, crate::host::test_patience(Duration::from_secs(30)))
        .expect("writes come back once node 3 adopts k2");
    assert_eq!(
        put.seq,
        Seq(k2.seq.0 + 1),
        "the next write follows k2: {put:?}"
    );
    assert!(put.generation > k2.generation, "{put:?}");
    assert_eq!(
        app.get(b"k2")
            .expect("k2 reads")
            .value
            .map(|(_, value)| value),
        Some(Bytes::from_static(b"two")),
        "k2 came back"
    );
    assert_eq!(
        app.get(b"k1")
            .expect("k1 reads")
            .value
            .map(|(_, value)| value),
        Some(Bytes::from_static(b"one")),
        "k1 kept"
    );
    assert!(adopted_on(3) >= 1, "node 3 logged tail_adopted");
    // A9 and critic-s15 M3: k2's own request, sent again, writes no second seq. Pinned to the
    // generation it was sent under, the generation fence may answer first, with the documented
    // `GENERATION_CHANGED` ("this send wrote nothing"); otherwise Dedup replays k2's answer.
    match app.resend((*k2.sent).clone()) {
        Ok(again) => assert_eq!(again.seq, k2.seq, "replayed, not applied again: {again:?}"),
        Err(refused) => assert_eq!(
            refused.error.kind,
            ErrorKind::GenerationChanged,
            "{refused:?}"
        ),
    }
    // Unpinned (the pin is not in the request digest), the same request passes the generation
    // fence and reaches Dedup, which must hold k2's row across the adoption. A row retained by
    // an older generation is never re-executed and never replayed transparently (spec §8.1,
    // `TransactionQueue::head` step 11): it answers `GENERATION_CHANGED` naming the generation
    // that retained it, k2's. Without the row this send would be applied again, at a new seq.
    let unpinned = TxnRequest {
        expected_generation: None,
        ..(*k2.sent).clone()
    };
    wait_for("node 1's re-add still settled", patience, settled);
    let retained = app
        .resend(unpinned)
        .expect_err("Dedup answers k2's unpinned resend from its older generation");
    assert_eq!(
        retained.error.kind,
        ErrorKind::GenerationChanged,
        "{retained:?}"
    );
    assert!(
        retained
            .error
            .detail
            .contains(&format!("expected {:?}", k2.generation)),
        "Dedup's older-generation answer names k2's generation: {retained:?}"
    );
    wait_for("node 1's re-add still settled", patience, settled);
    let next = app.put(b"later", b"v", None).expect("a later write");
    assert_eq!(next.seq, Seq(put.seq.0 + 1), "the resend took no seq");
}

/// Walk C's held state, as the W-C row builds it, for the issue #105 section 13 rows: K2
/// acknowledged on nodes 1 and 3 only, node 2 promoted without it, node 3 live and refusing its
/// tail, node 1 cut off from both. Answers the app, the nodes, the links and K2.
///
/// Issue #105 unit 2: K2 is a sound tail stamped by the last writable generation, so node 3
/// would adopt it and the held state would end before a row could release it. Every holder's
/// proofs are fixed to fail (`tail_not_contiguous`, `crate::host::refuse_adoption`): nodes 1 and
/// 3, the two that hold K2.
fn walk_c_held(
    rt: &tokio::runtime::Runtime,
    store: &Arc<dyn ConfigStore>,
    dir: &Path,
    app_dir: &Path,
    patience: Duration,
) -> (Db, Vec<Option<Db>>, Arc<Links>, PutOk) {
    for node in ["1", "3"] {
        crate::host::refuse_adoption(&dir.join("nodes").join(node), "tail_not_contiguous");
    }
    let mut formed = opened(rt, dir, store);
    formed.shutdown();
    drop(formed);
    let links = Links::new();
    let nodes: Vec<Option<Db>> = [1, 2, 3]
        .map(|node| Some(open_process(rt, dir, store, node, &links)))
        .into();
    for db in nodes.iter().flatten() {
        db.wait_ready(patience).expect("each node is ready");
    }
    let app = rt
        .block_on(Db::open_with(
            DbConfig {
                local: Vec::new(),
                links: Some(Arc::clone(&links)),
                client: ClientId(2),
                ..config(app_dir)
            },
            Arc::clone(store),
            rt.handle().clone(),
            fast(),
        ))
        .expect("the app opens");
    app.wait_ready(patience).expect("the app sees the owner");
    let k1 = app.put(b"k1", b"one", None).expect("k1 in G");
    app.wait_durable(GROUP_OF, position(&k1), LONG)
        .expect("k1 is on every disk");
    links.hold(NodeId(1), NodeId(2));
    links.hold(NodeId(2), NodeId(3));
    let k2 = app
        .put(b"k2", b"two", None)
        .expect("k2 in G, on nodes 1 and 3");
    assert_eq!(k2.durability, Durability::BufferedOnTwo, "{k2:?}");
    links.hold(NodeId(1), NodeId(3));
    app.promote(NodeId(2), None).expect("the promote commits");
    links.heal(NodeId(2), NodeId(3));
    let deadline = Instant::now() + patience;
    let three = nodes[2].as_ref().expect("node 3");
    while three
        .node_status()
        .into_iter()
        .next()
        .expect("its own status")
        .tail_refused
        .is_none()
    {
        assert!(Instant::now() < deadline, "node 3 reported no refusal");
        std::thread::sleep(Duration::from_millis(20));
    }
    (app, nodes, links, k2)
}

/// `forget-tail --through through --give-up-live-copy`, sent again until the owner accepts it:
/// while the app's route still names the old owner (`NotPrimary`), and while the owner has not
/// yet heard a holder's tail in its status probes ("nothing for --through to give up"), which
/// cuts c5 and c6 hit at random on 2026-11-09.
fn forget_k2(app: &Db, through: Seq, patience: Duration) {
    let deadline = Instant::now() + patience;
    loop {
        let release = TailRelease {
            through: Some(through),
            no_intact_copy: false,
        };
        match app.forget_tail(release, true) {
            Ok(_) => return,
            Err(error)
                if (error.kind == ErrorKind::NotPrimary
                    || error.detail.contains("nothing for --through to give up"))
                    && Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(error) => panic!("forget-tail refused: {error:?}"),
        }
    }
}

/// Puts until one succeeds or the window ends: the first success, or the last error.
///
/// Only a refusal that claims `no_mutation` gets a fresh put. Any other error resends the same
/// request (`Db::resend_within`), as a client must: a put a re-plan raced is answered
/// `UNKNOWN_OUTCOME` although it may have applied, and its resend replays that answer. A fresh
/// put there applied a second time at the next seq (gate3 at `65bf9cb`, 2026-11-10: dispatched at
/// seq 3 in G5, carried into G6 at cutoff 3, then a second put at seq 4).
fn first_put(app: &Db, window: Duration) -> Result<PutOk, PutError> {
    let end = Instant::now() + window;
    let mut unsettled: Option<TxnRequest> = None;
    loop {
        let sent = match unsettled.take() {
            Some(request) => app.resend_within(request, Duration::from_secs(5)),
            None => app.put(b"after", b"v", None),
        };
        match sent {
            Ok(put) => return Ok(put),
            Err(error) if Instant::now() >= end => return Err(error),
            Err(error) => {
                if !error.error.no_mutation {
                    unsettled = error.request.map(|request| *request);
                }
                std::thread::sleep(Duration::from_millis(250));
            }
        }
    }
}

/// The modes node `node` logged `recovered` with, in order.
fn recovered_modes(method: &str, node: u32) -> Vec<String> {
    config_testkit::logs::lines_for_current_test(module_path!(), method)
        .into_iter()
        .filter(|line| line["@m"] == "recovered" && line["node"] == node)
        .map(|line| line["mode"].as_str().expect("a mode").to_owned())
        .collect()
}

/// Issue #105 (design section 13), walk u1ft's acceptance: walk C's held state, the owner
/// (node 2) and node 3 the only copies that answer, node 3 refusing K2's tail. `forget-tail
/// --through K2 --give-up-live-copy` re-plans; node 3 is told the release, copies at the base
/// and reports, so the commit is writable on two copies, L moves, and the next write takes K2's
/// seq. K1 is kept. Node 3's adoption proofs are fixed to fail (`walk_c_held`), so it does not
/// bring K2 back before the release. Cut: the owner sends no `release_through`, red (ReadOnly by
/// count, no put).
/// Integration (~45 s): the grant wait, the promote and the re-plan.
#[config_log::retcd_test]
fn issue105_forget_tail_with_the_holder_the_only_other_copy_resumes_writes() {
    const METHOD: &str = "issue105_forget_tail_with_the_holder_the_only_other_copy_resumes_writes";
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let app_dir = config_testkit::fs::temp_dir();
    let patience = crate::host::test_patience(Duration::from_secs(60));
    let (app, _nodes, _links, k2) = walk_c_held(&rt, &store, dir.path(), app_dir.path(), patience);
    forget_k2(&app, k2.seq, patience);
    let put = first_put(&app, crate::host::test_patience(Duration::from_secs(20)))
        .expect("writes resume after the release");
    assert_eq!(put.seq, k2.seq, "the next write takes the given-up seq");
    assert!(put.generation > k2.generation, "{put:?}");
    let modes = recovered_modes(METHOD, 2);
    assert!(
        modes
            .iter()
            .any(|mode| mode == "DegradedRf2" || mode == "Active"),
        "a writable commit: {modes:?}"
    );
    let released = config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
        .into_iter()
        .filter(|line| line["@m"] == "tail_released" && line["node"] == 3)
        .count();
    assert!(released >= 1, "node 3 was told the release");
    assert_eq!(
        app.get(b"k1")
            .expect("k1 reads")
            .value
            .map(|(_, value)| value),
        Some(Bytes::from_static(b"one")),
        "k1 kept"
    );
}

/// Issue #105 (design section 13, section 7's M5 row): two live holders released together.
/// Walk C's held state, then walk A: node 1, the old owner, comes back on its own data and
/// lands holding K2 on an older lineage beside the anchor, while node 3 still holds K2 on the
/// generation it missed. One `forget-tail --through K2 --give-up-live-copy` releases both:
/// node 1 reports its anchor lineage with nothing written, node 3 copies at the base, both log
/// `tail_released`, and the commit is writable. Both holders' adoption proofs are fixed to fail
/// (`walk_c_held`), so neither brings K2 back before the release.
/// Cut: the owner sends no `release_through`, red (no put).
/// Integration (~60 s): the grant wait, the promote, a restart and the re-plan.
#[config_log::retcd_test]
fn issue105_forget_tail_tells_both_live_holders_and_writes_resume() {
    const METHOD: &str = "issue105_forget_tail_tells_both_live_holders_and_writes_resume";
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let app_dir = config_testkit::fs::temp_dir();
    let patience = crate::host::test_patience(Duration::from_secs(60));
    let (app, mut nodes, links, k2) =
        walk_c_held(&rt, &store, dir.path(), app_dir.path(), patience);
    let mut one = nodes[0].take().expect("node 1");
    one.shutdown();
    drop(one);
    links.heal(NodeId(1), NodeId(2));
    links.heal(NodeId(1), NodeId(3));
    nodes[0] = Some(open_process(&rt, dir.path(), &store, 1, &links));
    let boot = links
        .boot_of(NodeId(1))
        .expect("node 1 registered its boot");
    for (at, node) in [(1, NodeId(2)), (2, NodeId(3))] {
        let db = nodes[at].as_ref().expect("a running node");
        db.send_to(
            node,
            Msg::PeerBooted {
                node: NodeId(1),
                boot,
            },
        )
        .expect("the node hears the boot");
    }
    let deadline = Instant::now() + patience;
    let one = nodes[0].as_ref().expect("node 1");
    while one
        .node_status()
        .into_iter()
        .next()
        .is_none_or(|status| status.tail_refused.is_none())
    {
        assert!(Instant::now() < deadline, "node 1 reported no tail");
        std::thread::sleep(Duration::from_millis(50));
    }
    forget_k2(&app, k2.seq, patience);
    let put = first_put(&app, crate::host::test_patience(Duration::from_secs(30)))
        .expect("writes resume after the release");
    assert_eq!(put.seq, k2.seq, "the next write takes the given-up seq");
    let released: BTreeSet<u64> =
        config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
            .into_iter()
            .filter(|line| line["@m"] == "tail_released")
            .filter_map(|line| line["node"].as_u64())
            .collect();
    assert_eq!(released, BTreeSet::from([1, 3]), "both holders were told");
    let modes = recovered_modes(METHOD, 2);
    assert!(
        modes
            .iter()
            .any(|mode| mode == "DegradedRf2" || mode == "Active"),
        "a writable commit: {modes:?}"
    );
}

/// Section 14 (d) row 10, walk WA-5 as a row: walk A's schedule, node 3 stopped, and the old
/// owner (node 1) back on its own data. K2 is acknowledged on nodes 1 and 3 only; node 2 is
/// promoted without it; node 3 then stops, so node 1 is the only copy that holds K2, on the
/// older lineage beside the anchor generation it also holds. Node 1 adopts K2 by replay onto the
/// anchor's lineage (`tail_adopted` with `how = replay`), the commit is writable at K2, K2 reads
/// `two`, and the next write takes the seq after K2.
/// Cut: `Host::adoption` answers `replay_not_ready` where it would replay, red (no put).
/// Integration (~45 s): the grant wait, the promote, a reopen and the re-plan.
#[config_log::retcd_test]
fn u2_10_the_old_owner_back_on_its_own_data_replays_k2_onto_the_anchor_lineage() {
    const METHOD: &str =
        "u2_10_the_old_owner_back_on_its_own_data_replays_k2_onto_the_anchor_lineage";
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let patience = crate::host::test_patience(Duration::from_secs(60));
    let mut formed = opened(&rt, dir.path(), &store);
    formed.shutdown();
    drop(formed);
    let links = Links::new();
    let mut nodes: Vec<Option<Db>> = [1, 2, 3]
        .map(|node| Some(open_process(&rt, dir.path(), &store, node, &links)))
        .into();
    for db in nodes.iter().flatten() {
        db.wait_ready(patience).expect("each node is ready");
    }
    let app_dir = config_testkit::fs::temp_dir();
    let app = rt
        .block_on(Db::open_with(
            DbConfig {
                local: Vec::new(),
                links: Some(Arc::clone(&links)),
                client: ClientId(2),
                ..config(app_dir.path())
            },
            Arc::clone(&store),
            rt.handle().clone(),
            fast(),
        ))
        .expect("the app opens");
    app.wait_ready(patience).expect("the app sees the owner");
    let k1 = app.put(b"k1", b"one", None).expect("k1 in G");
    app.wait_durable(GROUP_OF, position(&k1), LONG)
        .expect("k1 is on every disk");
    links.hold(NodeId(1), NodeId(2));
    links.hold(NodeId(2), NodeId(3));
    let k2 = app
        .put(b"k2", b"two", None)
        .expect("k2 in G, on nodes 1 and 3");
    assert_eq!(k2.durability, Durability::BufferedOnTwo, "{k2:?}");
    links.hold(NodeId(1), NodeId(3));
    app.promote(NodeId(2), None).expect("the promote commits");

    // Node 3 stops, so only node 1 can bring K2 back.
    let mut three = nodes[2].take().expect("node 3");
    three.shutdown();
    drop(three);
    let mut one = nodes[0].take().expect("node 1");
    one.shutdown();
    drop(one);
    links.heal(NodeId(1), NodeId(2));
    links.heal(NodeId(1), NodeId(3));
    nodes[0] = Some(open_process(&rt, dir.path(), &store, 1, &links));
    let boot = links
        .boot_of(NodeId(1))
        .expect("node 1 registered its boot");
    nodes[1]
        .as_ref()
        .expect("node 2")
        .send_to(
            NodeId(2),
            Msg::PeerBooted {
                node: NodeId(1),
                boot,
            },
        )
        .expect("node 2 hears the boot");

    let put = first_put(&app, crate::host::test_patience(Duration::from_secs(30)))
        .expect("writes come back once node 1 replays k2");
    assert_eq!(
        put.seq,
        Seq(k2.seq.0 + 1),
        "the next write follows k2: {put:?}"
    );
    assert!(put.generation > k2.generation, "{put:?}");
    assert_eq!(
        app.get(b"k2")
            .expect("k2 reads")
            .value
            .map(|(_, value)| value),
        Some(Bytes::from_static(b"two")),
        "k2 came back"
    );
    let replays = config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
        .into_iter()
        .filter(|line| line["@m"] == "tail_adopted" && line["node"] == 1 && line["how"] == "replay")
        .count();
    assert_eq!(replays, 1, "node 1 replayed k2 once");
}

/// What [`w_c_promoted`] answers: the app, the nodes, the links, the control store and the
/// tail's puts.
type WcPromoted = (
    Db,
    Vec<Option<Db>>,
    Arc<Links>,
    Arc<config_testkit::MemStore>,
    Vec<PutOk>,
);

/// Issue #105 unit 2's owner-crash rows (section 14 (d)): W-C's schedule up to the promote,
/// with no holder's proofs fixed. `tail` is written while links 1-2 and 2-3 are held, so it is
/// acknowledged on nodes 1 and 3 only; node 2 is promoted without it and node 3, healed to node
/// 2, adopts it. Node 1 stays cut off. Answers the app, the nodes, the links, the control store
/// and the tail's puts.
fn w_c_promoted(
    rt: &tokio::runtime::Runtime,
    dir: &Path,
    app_dir: &Path,
    patience: Duration,
    tail: &[&[u8]],
) -> WcPromoted {
    let mem = Arc::new(config_testkit::MemStore::new());
    let store: Arc<dyn ConfigStore> = Arc::clone(&mem) as Arc<dyn ConfigStore>;
    let mut formed = opened(rt, dir, &store);
    formed.shutdown();
    drop(formed);
    let links = Links::new();
    let nodes: Vec<Option<Db>> = [1, 2, 3]
        .map(|node| Some(open_process(rt, dir, &store, node, &links)))
        .into();
    for db in nodes.iter().flatten() {
        db.wait_ready(patience).expect("each node is ready");
    }
    let app = rt
        .block_on(Db::open_with(
            DbConfig {
                local: Vec::new(),
                links: Some(Arc::clone(&links)),
                client: ClientId(2),
                ..config(app_dir)
            },
            Arc::clone(&store),
            rt.handle().clone(),
            fast(),
        ))
        .expect("the app opens");
    app.wait_ready(patience).expect("the app sees the owner");
    let k1 = app.put(b"k1", b"one", None).expect("k1 in G");
    app.wait_durable(GROUP_OF, position(&k1), LONG)
        .expect("k1 is on every disk");
    links.hold(NodeId(1), NodeId(2));
    links.hold(NodeId(2), NodeId(3));
    let puts: Vec<PutOk> = tail
        .iter()
        .map(|key| {
            let put = app.put(key, b"two", None).expect("in G, on nodes 1 and 3");
            assert_eq!(put.durability, Durability::BufferedOnTwo, "{put:?}");
            put
        })
        .collect();
    links.hold(NodeId(1), NodeId(3));
    app.promote(NodeId(2), None).expect("the promote commits");
    (app, nodes, links, mem, puts)
}

/// This test's log lines with message `message`.
fn lines_of(method: &str, message: &str) -> Vec<serde_json::Value> {
    config_testkit::logs::lines_for_current_test(module_path!(), method)
        .into_iter()
        .filter(|line| line["@m"] == message)
        .collect()
}

/// Waits until `done` holds, polling every 20 ms.
fn wait_for(what: &str, patience: Duration, done: impl Fn() -> bool) {
    let deadline = Instant::now() + patience;
    while !done() {
        assert!(Instant::now() < deadline, "not within {patience:?}: {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Node `node` stopped and started again on its own data, and node 3 told of its new boot.
fn reopen(
    rt: &tokio::runtime::Runtime,
    dir: &Path,
    store: &Arc<dyn ConfigStore>,
    nodes: &mut [Option<Db>],
    links: &Arc<Links>,
    node: u32,
    before_open: impl FnOnce(),
) {
    let at = usize::try_from(node - 1).expect("a node index");
    let mut old = nodes[at].take().expect("the node runs");
    old.shutdown();
    drop(old);
    before_open();
    nodes[at] = Some(open_process(rt, dir, store, node, links));
    let boot = links.boot_of(NodeId(node)).expect("its boot registered");
    for (other, db) in nodes.iter().enumerate() {
        let other = u32::try_from(other).expect("a node index") + 1;
        if other != node {
            if let Some(db) = db {
                db.send_to(
                    NodeId(other),
                    Msg::PeerBooted {
                        node: NodeId(node),
                        boot,
                    },
                )
                .expect("the node hears the boot");
            }
        }
    }
}

/// The way back after an owner crash: a put lands at the seq after the tail, in a newer
/// generation, and every tail key reads `two`.
fn writes_follow_the_tail(app: &Db, tail: &[PutOk], keys: &[&[u8]]) -> PutOk {
    let last = tail.last().expect("a tail");
    let put = first_put(app, crate::host::test_patience(Duration::from_secs(30)))
        .expect("writes come back after the owner's restart");
    assert_eq!(
        put.seq,
        Seq(last.seq.0 + 1),
        "the next write follows the tail: {put:?}"
    );
    assert!(put.generation > last.generation, "{put:?}");
    for key in keys {
        assert_eq!(
            app.get(key).expect("reads").value.map(|(_, value)| value),
            Some(Bytes::from_static(b"two")),
            "{} came back",
            String::from_utf8_lossy(key)
        );
    }
    put
}

/// Whether node 2 committed past `generation` (a `recovered` line above it).
fn committed_past(method: &str, generation: u64) -> bool {
    lines_of(method, "recovered")
        .iter()
        .any(|line| line["node"] == 2 && line["generation"].as_u64() > Some(generation))
}

/// The generation node 2 stood down in.
fn stood_down_in(method: &str) -> u64 {
    lines_of(method, "primary_stood_down")[0]["generation"]
        .as_u64()
        .expect("a generation")
}

/// Node 3, healed to node 2, has landed the promote's commit and marked its tail, and node 2's
/// repair has not yet asked its inventory. Then node 3 pauses `pause` before it answers that
/// inventory, once, so a row can act while node 2 has stood down and is not yet caught up.
fn heal_and_pause_the_holder(
    dir: &Path,
    nodes: &[Option<Db>],
    links: &Arc<Links>,
    method: &str,
    patience: Duration,
    pause: Duration,
) {
    links.heal(NodeId(2), NodeId(3));
    let three = nodes[2].as_ref().expect("node 3");
    wait_for("node 3 marked its tail", patience, || {
        three
            .node_status()
            .into_iter()
            .next()
            .is_some_and(|status| status.tail_refused.is_some())
    });
    crate::host::pause_before_inventory(&dir.join("nodes").join("3"), pause);
    assert!(
        lines_of(method, "tail_adopted").is_empty(),
        "paused before node 3 adopted"
    );
}

/// Node 2's applied head.
fn applied_on_two(nodes: &[Option<Db>]) -> u64 {
    nodes[1]
        .as_ref()
        .expect("node 2")
        .node_status()
        .into_iter()
        .next()
        .and_then(|status| status.holds)
        .map_or(0, |held| held.applied)
}

/// Section 14 (d), crash "node 2 after the catch-up records land, before the CAS": node 2, the
/// owner, stands down for node 3's adopted K2, and the control store goes down before the plan's
/// commit. Node 2 is caught up to K2 from node 3, its CAS cannot land, and it restarts. Its
/// anchor lineage holds K2's seq, which is not a hit, so after the restart its scans find no
/// tail; the next run commits at K2, K2 reads `two`, and the next write takes the seq after K2.
/// Cuts: `Host::tail_scan` drops its same-record check on the anchor lineage, red (observed:
/// node 2 is never caught up to K2, so the row fails before the restart, not at its scan
/// assertion); `Primary::on_view` reads the trigger after the tracker adopts the view, red (node
/// 2 never stands down).
/// Integration (~45 s): the grant wait, the promote, the store outage and a reopen.
#[config_log::retcd_test]
fn u2_c1_an_owner_restarted_after_its_catch_up_and_before_the_cas_commits_at_the_tail() {
    const METHOD: &str =
        "u2_c1_an_owner_restarted_after_its_catch_up_and_before_the_cas_commits_at_the_tail";
    let rt = runtime();
    let dir = config_testkit::fs::temp_dir();
    let app_dir = config_testkit::fs::temp_dir();
    let patience = crate::host::test_patience(Duration::from_secs(60));
    let keys: [&[u8]; 1] = [b"k2"];
    let (app, mut nodes, links, mem, tail) =
        w_c_promoted(&rt, dir.path(), app_dir.path(), patience, &keys);
    let store: Arc<dyn ConfigStore> = Arc::clone(&mem) as Arc<dyn ConfigStore>;
    let k2 = tail[0].seq;
    heal_and_pause_the_holder(
        dir.path(),
        &nodes,
        &links,
        METHOD,
        patience,
        Duration::from_secs(1),
    );
    wait_for("node 2 stood down", patience, || {
        !lines_of(METHOD, "primary_stood_down").is_empty()
    });
    let stood_down = stood_down_in(METHOD);
    mem.failing_with(config_core::ConfigError::Unavailable {
        reason: "the control store is down".to_owned(),
    });
    // Node 2 holds K2 once its sync of the catch-up proves K2 durable. Not `NodeStatus::holds`:
    // under full-gate load node 2's repair can stand it down before it adopts G3's authority, and
    // with the control store down it never adopts one, so `holds` stays `None` while its engine
    // has K2 (observed 2026-11-10: `sync_proven` node 2 durable 3, then 180 s of `holds` None).
    wait_for("node 2 holds K2 durably", patience, || {
        lines_of(METHOD, "sync_proven")
            .iter()
            .any(|line| line["node"] == 2 && line["durable"].as_u64() >= Some(k2.0))
    });
    assert!(
        !committed_past(METHOD, stood_down),
        "no commit before the restart"
    );
    let restarted_at = config_testkit::logs::lines_for_current_test(module_path!(), METHOD).len();
    reopen(&rt, dir.path(), &store, &mut nodes, &links, 2, || {
        mem.stop_failing()
    });
    writes_follow_the_tail(&app, &tail, &keys);
    let lines = config_testkit::logs::lines_for_current_test(module_path!(), METHOD);
    let scans: Vec<_> = lines[restarted_at..]
        .iter()
        .filter(|line| line["@m"] == "tail_scan" && line["node"] == 2)
        .map(|line| line["hit"].clone())
        .collect();
    assert!(!scans.is_empty(), "node 2 scanned after the restart");
    assert!(
        scans.iter().all(|hit| *hit == false),
        "after the restart node 2's scans find no tail: {scans:?}"
    );
}

/// Section 14 (d), crash "node 2 restarts while stood down": node 2, the owner, stands down for
/// node 3's tail, and restarts before node 3 answers the plan's inventory, so it was never caught
/// up. No primary of the stood-down generation serves again: the next write lands in a newer
/// generation, at the seq after K2, and K2 reads `two`.
/// Cut: `Primary::on_view` reads the trigger after the tracker adopts the view, red (node 2 never
/// stands down).
/// Integration (~45 s): the grant wait, the promote, the stand-down and a reopen.
#[config_log::retcd_test]
fn u2_c2_an_owner_restarted_while_stood_down_serves_no_old_generation() {
    const METHOD: &str = "u2_c2_an_owner_restarted_while_stood_down_serves_no_old_generation";
    let rt = runtime();
    let dir = config_testkit::fs::temp_dir();
    let app_dir = config_testkit::fs::temp_dir();
    let patience = crate::host::test_patience(Duration::from_secs(60));
    let keys: [&[u8]; 1] = [b"k2"];
    let (app, mut nodes, links, mem, tail) =
        w_c_promoted(&rt, dir.path(), app_dir.path(), patience, &keys);
    let store: Arc<dyn ConfigStore> = Arc::clone(&mem) as Arc<dyn ConfigStore>;
    heal_and_pause_the_holder(
        dir.path(),
        &nodes,
        &links,
        METHOD,
        patience,
        Duration::from_secs(1),
    );
    wait_for("node 2 stood down", patience, || {
        !lines_of(METHOD, "primary_stood_down").is_empty()
    });
    let stood_down = stood_down_in(METHOD);
    assert!(applied_on_two(&nodes) < tail[0].seq.0, "not yet caught up");
    assert!(
        !committed_past(METHOD, stood_down),
        "restarted while stood down"
    );
    reopen(&rt, dir.path(), &store, &mut nodes, &links, 2, || {});
    let put = writes_follow_the_tail(&app, &tail, &keys);
    assert!(
        put.generation.0 > stood_down,
        "no primary of the stood-down generation served: {put:?}"
    );
}

/// Section 14 (d), crash "between the CAS and `Recovered` on node 2": node 2, the owner, stands
/// down for node 3's adopted K2, is caught up, and its commit lands in the control store; node 2
/// then faults before anything after the commit runs (`crate::host::fault_next_commit`).
/// Reopened, it rolls the commit forward as today: K2 reads `two` and the next write takes the
/// seq after K2.
/// Cut: none of its own. This is today's roll-forward; the stand-down and adoption it follows are
/// cut in rows 9 and 10 and C1 to C4.
/// Integration (~45 s): the grant wait, the promote, the fault and a reopen.
#[config_log::retcd_test]
fn u2_c5_an_owner_faulted_between_the_cas_and_recovered_rolls_forward() {
    const METHOD: &str = "u2_c5_an_owner_faulted_between_the_cas_and_recovered_rolls_forward";
    let rt = runtime();
    let dir = config_testkit::fs::temp_dir();
    let app_dir = config_testkit::fs::temp_dir();
    let patience = crate::host::test_patience(Duration::from_secs(60));
    let keys: [&[u8]; 1] = [b"k2"];
    let (app, mut nodes, links, mem, tail) =
        w_c_promoted(&rt, dir.path(), app_dir.path(), patience, &keys);
    let store: Arc<dyn ConfigStore> = Arc::clone(&mem) as Arc<dyn ConfigStore>;
    let node_dir = dir.path().join("nodes").join("2");
    heal_and_pause_the_holder(dir.path(), &nodes, &links, METHOD, patience, Duration::ZERO);
    crate::host::fault_next_commit(&node_dir);
    let two = nodes[1].as_ref().expect("node 2");
    wait_for("node 2 faulted after the commit", patience, || {
        two.node_status()
            .into_iter()
            .next()
            .is_some_and(|status| status.fault.is_some())
    });
    let hooked = lines_of(METHOD, "after_commit_hook");
    assert_eq!(hooked.len(), 1, "{hooked:?}");
    assert!(
        !lines_of(METHOD, "primary_stood_down").is_empty(),
        "node 2 stood down before its commit"
    );
    assert!(
        hooked[0]["generation"].as_u64() > Some(stood_down_in(METHOD)),
        "the faulted commit is the adopting one: {hooked:?}"
    );
    reopen(&rt, dir.path(), &store, &mut nodes, &links, 2, || {});
    writes_follow_the_tail(&app, &tail, &keys);
}

/// critic-s15 M2 (2): node 2, the owner, restarts part-way through a multi-record catch-up from
/// node 3. Its plan was memory only and is gone; its anchor lineage keeps the records it staged.
/// Node 3 sends no catch-up record past four records into the tail (`host::cap_recovery_send`),
/// as a link cut part-way through would; link 2-3 is then held with node 2 strictly inside the
/// tail, the cap cleared, and node 2 restarts there. Polling the catch-up instead
/// raced it: under full-lib load all 40 records landed before the hold (observed, 2026-11-10). The
/// next run reports node 2's partial head, keeps its plan again on the receiver it installs
/// (B1), and finishes the tail from there: every tail key reads `two` and the next write follows
/// the tail.
/// Cut: `Host::restore_dormant` keeps the plan before it installs the receiver (critic-s15 B1),
/// red. Observed: the first catch-up is already refused `StaleGeneration`, node 2 reaches the
/// tail's head later by another path, and the row fails at its mid-tail precondition. The
/// restart's re-keep is the same `restore_dormant`, so no cut separates the two.
/// Integration (~45 s): the grant wait, a 40-record tail, the promote and a reopen.
#[config_log::retcd_test]
fn u2_m2_2_an_owner_restarted_part_way_through_its_catch_up_finishes_from_its_partial_head() {
    const METHOD: &str =
        "u2_m2_2_an_owner_restarted_part_way_through_its_catch_up_finishes_from_its_partial_head";
    let rt = runtime();
    let dir = config_testkit::fs::temp_dir();
    let app_dir = config_testkit::fs::temp_dir();
    let patience = crate::host::test_patience(Duration::from_secs(60));
    let names: Vec<Vec<u8>> = (0..40)
        .map(|at| format!("k2-{at:02}").into_bytes())
        .collect();
    let keys: Vec<&[u8]> = names.iter().map(Vec::as_slice).collect();
    let (app, mut nodes, links, mem, tail) =
        w_c_promoted(&rt, dir.path(), app_dir.path(), patience, &keys);
    let store: Arc<dyn ConfigStore> = Arc::clone(&mem) as Arc<dyn ConfigStore>;
    let (first, last) = (tail[0].seq.0, tail[tail.len() - 1].seq.0);
    let base = applied_on_two(&nodes);
    assert!(base < first, "node 2 was promoted without the tail: {base}");
    let cap = first + 4;
    let holder = dir.path().join("nodes").join("3");
    crate::host::cap_recovery_send(&holder, Seq(cap));
    links.heal(NodeId(2), NodeId(3));
    // Node 2 took the catch-up through the cap once node 3 opens a window past it: node 3 moves
    // its window on node 2's acknowledgements. Not `NodeStatus::holds`: under load node 2's repair
    // can stand it down before it adopts G3's authority, and `holds` then reads the adopted
    // generation, not the receiver taking the catch-up (observed 2026-11-11: node 3 sending from
    // seq 8 for 180 s while `holds` read 2, the same race as C1's).
    wait_for("node 2 took the catch-up up to the cap", patience, || {
        lines_of(METHOD, "recovery_envelopes_sent")
            .iter()
            .any(|line| line["node"] == 3 && line["to"] == 2 && line["from"].as_u64() > Some(cap))
    });
    links.hold(NodeId(2), NodeId(3));
    crate::host::clear_recovery_send_cap(&holder);
    // Node 3 sent nothing past the cap but the head record, which stays above the gap.
    let partial = cap;
    assert!(
        first <= partial && partial < last,
        "held strictly inside the tail {first}..={last}: {partial}"
    );
    assert!(
        !lines_of(METHOD, "primary_stood_down").is_empty(),
        "stood down"
    );
    let restarted_at = config_testkit::logs::lines_for_current_test(module_path!(), METHOD).len();
    reopen(&rt, dir.path(), &store, &mut nodes, &links, 2, || {});
    links.heal(NodeId(2), NodeId(3));
    writes_follow_the_tail(&app, &tail, &keys);
    let lines = config_testkit::logs::lines_for_current_test(module_path!(), METHOD);
    let heads: Vec<u64> = lines[restarted_at..]
        .iter()
        .filter(|line| line["@m"] == "inventory_reported" && line["node"] == 2)
        .filter_map(|line| line["head"].as_u64())
        .collect();
    assert!(
        heads.first().is_some_and(|head| *head >= partial),
        "the next run starts from node 2's partial head {partial}: {heads:?}"
    );
}
