//! Issue #90 (#80 S5d): the durability barrier through [`Db`], on wall-clock time. Rows 40's
//! drop under writes, 41, 42 and 47; the rows on three stepped hosts are in `host::barrier_tests`.

use super::reopen_tests::{config, fast, runtime};
use super::*;
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
