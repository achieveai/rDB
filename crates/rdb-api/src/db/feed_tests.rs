//! Issue #87 (ADR-rdb-0020) slice 1: the change feed through [`Db`], on wall-clock time. The
//! pure resume and page rules are tested in `feed`.

use super::reopen_tests::{config, fast, runtime};
use super::*;
use crate::feed::{FeedRecord, FEED_PAGE_RECORDS};
use crate::net::faults::{CutStore, Faults};
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::envelope::ReplicationEnvelope;

fn store() -> Arc<dyn ConfigStore> {
    Arc::new(config_testkit::MemStore::new())
}

fn opened(rt: &tokio::runtime::Runtime, dir: &Path, store: &Arc<dyn ConfigStore>) -> Db {
    opened_with(rt, dir, store, fast())
}

fn opened_with(
    rt: &tokio::runtime::Runtime,
    dir: &Path,
    store: &Arc<dyn ConfigStore>,
    budgets: Budgets,
) -> Db {
    let db = rt
        .block_on(Db::open_with(
            config(dir),
            Arc::clone(store),
            rt.handle().clone(),
            budgets,
        ))
        .expect("the open");
    db.wait_ready(crate::host::test_patience(Duration::from_secs(10)))
        .expect("ready");
    db
}

/// A grant short enough for a row to wait several of: the R2 rows' bound.
fn short_grant() -> Budgets {
    Budgets {
        grant_millis: 1_500,
        renew_millis: 250,
        ..fast()
    }
}

fn patience() -> Duration {
    crate::host::test_patience(Duration::from_secs(30))
}

/// One feed call, `UNAVAILABLE` and `LEASE_EXPIRED` answers retried until `patience` ends. The
/// first call to a secondary starts its control read and is refused; a new owner refuses until
/// it holds its grant.
fn page_from(db: &Db, from: FeedCursor, source: Source) -> FeedPage {
    let deadline = Instant::now() + patience();
    loop {
        match db.changes(PARTITION, from, FEED_PAGE_RECORDS, source) {
            Ok(page) => return page,
            Err(error)
                if matches!(error.kind, ErrorKind::Unavailable | ErrorKind::LeaseExpired)
                    && Instant::now() < deadline =>
            {
                assert!(error.no_mutation, "{error:?}");
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(error) => panic!("feed call from {source:?}: {error:?}"),
        }
    }
}

/// Every record after `from` up to `through`, read from `source`, and the cursor after them.
fn tail(db: &Db, from: FeedCursor, source: Source, through: Seq) -> (Vec<FeedRecord>, FeedCursor) {
    let deadline = Instant::now() + patience();
    let (mut got, mut at) = (Vec::new(), from);
    while at.seq < through {
        match page_from(db, at, source) {
            FeedPage::Records {
                records,
                next,
                more,
            } => {
                assert!(
                    more || records.len() < FEED_PAGE_RECORDS as usize,
                    "a full page says more"
                );
                got.extend(records);
                at = next;
                if !more {
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
            other => panic!("tailing {source:?} from {at:?}: {other:?}"),
        }
        assert!(
            Instant::now() < deadline,
            "{source:?} never reached {through:?}: at {at:?}"
        );
    }
    (got, at)
}

fn seqs(records: &[FeedRecord]) -> Vec<u64> {
    records.iter().map(|record| record.at.seq.0).collect()
}

fn objects(records: &[FeedRecord]) -> Vec<Vec<u8>> {
    records
        .iter()
        .flat_map(|record| record.writes.iter().map(|write| write.object.to_vec()))
        .collect()
}

/// The record digest node `node` sees at `seq` through lineage `generation`, read from a
/// read-only open of its engine while the node runs.
fn digest_on(
    dir: &Path,
    node: u32,
    generation: Generation,
    seq: Seq,
) -> Option<(Generation, Digest)> {
    let engine = RocksEngine::open_read_only(dir.join("nodes").join(node.to_string()))
        .expect("a read-only open");
    let (record, _) = engine
        .history_at(PARTITION, generation, seq)
        .expect("a history read")?;
    let envelope = ReplicationEnvelope::decode(&record).expect("the record decodes");
    Some((envelope.header.generation, envelope.record_digest))
}

/// The design's two early assumptions (design-87 section 6), checked before any feed code:
/// after a promote to a secondary that holds every write, (1) the record at the cutoff read
/// through the new generation's chain has the digest and stamp it had in the old one, and (2)
/// the new owner's published position is at or above the cutoff before any new write.
/// Integration (~5 s): a grant wait.
#[config_log::retcd_test]
fn f87_1_assumptions_digest_kept_across_a_promote_and_published_at_the_cutoff() {
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let db = opened(&rt, dir.path(), &store);
    db.put(b"a", b"1", None).expect("put a");
    let b = db.put(b"b", b"2", None).expect("put b");
    db.wait_durable(
        DEFAULT_GROUP,
        (b.generation, b.seq),
        Duration::from_secs(30),
    )
    .expect("b on every disk");
    let before = digest_on(dir.path(), 1, b.generation, b.seq).expect("b on node 1");
    db.links().hold(NodeId(1), NodeId(2));
    db.links().hold(NodeId(1), NodeId(3));
    let promoted = db.promote(NodeId(2), None).expect("the promote commits");
    assert!(promoted.generation > b.generation, "{promoted:?}");
    let deadline = Instant::now() + crate::host::test_patience(Duration::from_secs(30));
    let published = loop {
        let two = db
            .node_status()
            .into_iter()
            .find(|status| status.node == NodeId(2))
            .expect("node 2's status");
        if let Some((generation, seq)) = two.published {
            if generation == promoted.generation {
                break seq;
            }
        }
        assert!(
            Instant::now() < deadline,
            "node 2 never published in G': {two:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    tracing::info!(
        published = published.0,
        cutoff = b.seq.0,
        "f87_assumption_published"
    );
    assert!(
        published >= b.seq,
        "published {published:?} below the cutoff {:?}",
        b.seq
    );
    let after = digest_on(dir.path(), 2, promoted.generation, b.seq).expect("b through G'");
    assert_eq!(after, before, "stamp and digest at the cutoff");
}

/// The acceptance row (issue #87 S-C): a consumer tails partition 1 on node 2, a secondary, and
/// sees every write in order. The owner is cut off and node 2 promoted. While node 2 lands the
/// new generation a call is `UNAVAILABLE` and the cursor does not move; then, before any new
/// write, the same cursor answers an empty page at once (not `Behind`), and the next write is
/// the only record after it: no gap, no repeat. Node 2's prefix equals the owner's.
/// Cut: `feed::resume` rewinds a cursor at the exit itself (`>=` for `>`), red (`Rewound` to
/// itself where the empty page is due).
/// Integration (~6 s): a grant wait.
#[config_log::retcd_test]
fn f87_1_promote_to_tailing_secondary_resumes_without_gap_or_repeat() {
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let db = opened(&rt, dir.path(), &store);
    let a = db.put(b"a", b"1", None).expect("put a");
    let b = db.put(b"b", b"2", None).expect("put b");
    let (two, at_b) = tail(&db, FeedCursor::START, Source::Node(NodeId(2)), b.seq);
    let (one, _) = tail(&db, FeedCursor::START, Source::Owner, b.seq);
    assert_eq!(two, one, "node 2's feed is the owner's prefix");
    assert_eq!(
        seqs(&two),
        vec![a.seq.0, b.seq.0],
        "every write once, in order"
    );
    assert_eq!(&objects(&two), &[b"a".to_vec(), b"b".to_vec()]);
    assert_eq!(at_b.seq, b.seq);
    assert_eq!(at_b.generation, b.generation);

    db.links().hold(NodeId(1), NodeId(2));
    db.links().hold(NodeId(1), NodeId(3));
    let promoted = db.promote(NodeId(2), None).expect("the promote commits");
    assert!(promoted.generation > b.generation, "{promoted:?}");
    // No write yet: the cursor at the cutoff answers a page, empty and final.
    let first = page_from(&db, at_b, Source::Node(NodeId(2)));
    assert_eq!(
        first,
        FeedPage::Records {
            records: Vec::new(),
            next: at_b,
            more: false
        },
        "before any write in the new generation"
    );
    let unavailable = config_testkit::logs::lines_for_current_test(
        module_path!(),
        "f87_1_promote_to_tailing_secondary_resumes_without_gap_or_repeat",
    )
    .into_iter()
    .filter(|line| line["@m"] == "feed_secondary_refused" || line["@m"] == "feed_answered")
    .count();
    tracing::info!(unavailable, "f87_1_refusals_seen");

    let c = loop {
        match db.put(b"c", b"3", None) {
            Ok(put) => break put,
            Err(error) => {
                assert!(error.error.no_mutation, "{error:?}");
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    };
    assert_eq!(c.generation, promoted.generation, "{c:?}");
    let (after, at_c) = tail(&db, at_b, Source::Node(NodeId(2)), c.seq);
    assert_eq!(seqs(&after), vec![b.seq.0 + 1], "no gap, no repeat");
    assert_eq!(c.seq.0, b.seq.0 + 1);
    assert_eq!(&objects(&after), &[b"c".to_vec()]);
    assert_eq!(
        at_c.generation, promoted.generation,
        "stamped by the new generation"
    );
}

/// The owner's feed is every published write in order with its object ids (the open's own
/// record, with no `User` write, is skipped), and a cursor whose digest is not the record's is
/// `CORRUPT_HISTORY`, claiming no mutation.
/// Cut: `feed::resume` skips the digest comparison, red (a page, not the error).
/// Integration (~2 s): a fresh open.
#[config_log::retcd_test]
fn f87_1_owner_feed_matches_puts_in_order_and_a_wrong_digest_is_corrupt_history() {
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let db = opened(&rt, dir.path(), &store);
    let puts: Vec<PutOk> = [b"x", b"y", b"z"]
        .iter()
        .map(|object| db.put(*object, b"v", None).expect("a put"))
        .collect();
    let last = puts.last().expect("three puts").seq;
    let (records, _) = tail(&db, FeedCursor::START, Source::Owner, last);
    let written: Vec<u64> = puts.iter().map(|put| put.seq.0).collect();
    assert_eq!(seqs(&records), written, "the puts, in order");
    assert_eq!(
        &objects(&records),
        &[b"x".to_vec(), b"y".to_vec(), b"z".to_vec()]
    );
    for (record, put) in records.iter().zip(&puts) {
        assert_eq!(record.request.request, put.request, "{record:?}");
        assert_eq!(record.at.generation, put.generation);
    }
    let forged = FeedCursor {
        digest: records[0].at.digest,
        ..records[1].at
    };
    let error = db
        .changes(PARTITION, forged, 10, Source::Owner)
        .expect_err("another digest at the same position");
    assert_eq!(error.kind, ErrorKind::CorruptHistory, "{error:?}");
    assert!(error.no_mutation, "{error:?}");
}

/// B1 through a real promote: the owner's feed reaches k2, written while node 2 was cut off, so
/// k2 is on nodes 1 and 3 only. With node 3 cut off too, node 2 is promoted at a cutoff of k1.
/// With no write after it, node 2 answers the cursor at k2 `Rewound` at once, to k1's cursor
/// (stamp, seq and digest).
/// Cut: `feed::resume` ignores the exit, red (`Behind`: the cursor is past the new cap).
/// Integration (~6 s): a grant wait.
#[config_log::retcd_test]
fn f87_1_cut_then_idle_is_rewound_at_once() {
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let db = opened(&rt, dir.path(), &store);
    let k1 = db.put(b"k1", b"one", None).expect("k1");
    db.wait_durable(
        DEFAULT_GROUP,
        (k1.generation, k1.seq),
        Duration::from_secs(30),
    )
    .expect("k1 on every disk");
    let (upto_k1, at_k1) = tail(&db, FeedCursor::START, Source::Owner, k1.seq);
    db.links().hold(NodeId(1), NodeId(2));
    db.links().hold(NodeId(2), NodeId(3));
    let k2 = db.put(b"k2", b"two", None).expect("k2 on nodes 1 and 3");
    assert_eq!(k2.durability, Durability::BufferedOnTwo, "{k2:?}");
    let (with_k2, at_k2) = tail(&db, at_k1, Source::Owner, k2.seq);
    assert_eq!(seqs(&with_k2), vec![k2.seq.0]);
    db.links().hold(NodeId(1), NodeId(3));
    let promoted = db.promote(NodeId(2), None).expect("the promote commits");
    let page = page_from(&db, at_k2, Source::Node(NodeId(2)));
    assert_eq!(
        page,
        FeedPage::Rewound { to: at_k1 },
        "promoted at {promoted:?}"
    );
    assert_eq!(upto_k1.last().map(|record| record.at), Some(at_k1));
}

/// One process per node, each reaching the control store through its own cut, as
/// `lr cut-control <n>` cuts one, and an app process with no node.
struct Procs {
    nodes: Vec<Db>,
    faults: Vec<Arc<Faults>>,
    links: Arc<Links>,
    app: Db,
}

fn procs(
    rt: &tokio::runtime::Runtime,
    dir: &Path,
    app_dir: &Path,
    store: &Arc<dyn ConfigStore>,
    budgets: Budgets,
) -> Procs {
    let mut formed = opened_with(rt, dir, store, budgets);
    formed.shutdown();
    drop(formed);
    let links = Links::new();
    let mut nodes = Vec::new();
    let mut faults = Vec::new();
    for node in 1..=3 {
        let cut = Faults::new();
        let reached: Arc<dyn ConfigStore> = CutStore::new(Arc::clone(store), Arc::clone(&cut));
        let db = rt
            .block_on(Db::open_with(
                DbConfig {
                    local: vec![NodeId(node)],
                    links: Some(Arc::clone(&links)),
                    ..config(dir)
                },
                reached,
                rt.handle().clone(),
                budgets,
            ))
            .unwrap_or_else(|error| panic!("node {node} opens: {error}"));
        nodes.push(db);
        faults.push(cut);
    }
    for db in &nodes {
        db.wait_ready(patience()).expect("each node is ready");
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
            budgets,
        ))
        .expect("the app opens");
    app.wait_ready(patience()).expect("the app sees the owner");
    Procs {
        nodes,
        faults,
        links,
        app,
    }
}

/// R2: the old owner (node 1) and the consumer's secondary (node 2) are cut off together, from
/// rEtcd and from node 3, and still hear each other. Node 3 is promoted. Node 2 stops serving
/// within the grant plus one control read of the cut, though its owner keeps talking to it:
/// it answers `UNAVAILABLE` naming the missing control read.
/// Cut: `Host::feed_fresh` answers `None` (fresh) always, red (node 2 keeps serving).
/// Integration (~8 s): processes, a grant wait.
#[config_log::retcd_test]
fn f87_1_owner_and_secondary_partitioned_together_secondary_stops_within_bound() {
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let app_dir = config_testkit::fs::temp_dir();
    let budgets = short_grant();
    let procs = procs(&rt, dir.path(), app_dir.path(), &store, budgets);
    let k1 = procs.app.put(b"k1", b"one", None).expect("k1");
    procs
        .app
        .wait_durable(
            DEFAULT_GROUP,
            (k1.generation, k1.seq),
            Duration::from_secs(30),
        )
        .expect("k1 on every disk");
    let two = Source::Node(NodeId(2));
    let (_, at_k1) = tail(&procs.app, FeedCursor::START, two, k1.seq);

    procs.faults[0].apply("cut-control");
    procs.faults[1].apply("cut-control");
    procs.links.hold(NodeId(1), NodeId(3));
    procs.links.hold(NodeId(2), NodeId(3));
    let cut = Instant::now();
    let bound = Duration::from_millis(budgets.grant_millis) + procs.app.timeouts.read;
    // Node 2 stops within the bound, measured from the cut, with no promote yet.
    let stopped = loop {
        match procs.app.changes(PARTITION, at_k1, 10, two) {
            Ok(FeedPage::Records { .. }) => {}
            Ok(other) => panic!("node 2 while cut: {other:?}"),
            Err(error) => {
                assert_eq!(error.kind, ErrorKind::Unavailable, "{error:?}");
                assert!(
                    error.detail.contains("no control read")
                        && error.detail.contains("one is under way, so call again"),
                    "{error:?}"
                );
                break cut.elapsed();
            }
        }
        assert!(
            cut.elapsed() < crate::host::test_patience(bound),
            "node 2 still serves {:?} after the cut",
            cut.elapsed()
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    tracing::info!(
        stopped_millis = stopped.as_millis() as u64,
        "f87_1_secondary_stopped"
    );
    let promoted = procs
        .app
        .promote(NodeId(3), Some(NodeId(1)))
        .expect("node 3 is promoted");
    let refused = procs
        .app
        .changes(PARTITION, at_k1, 10, two)
        .expect_err("node 2 stays out while cut");
    assert_eq!(
        refused.kind,
        ErrorKind::Unavailable,
        "{refused:?} after {promoted:?}"
    );
    drop(procs.nodes);
}

/// An idle partition: no write for three grants, and the secondary still serves, because its
/// freshness comes from its own control reads, started while feed calls arrive, never from
/// owner traffic.
/// Cut: start the next read only once the last is older than the grant (not half of it), red
/// (`UNAVAILABLE` at the first call past a grant).
/// Integration (~6 s): three 1.5 s grants.
#[config_log::retcd_test]
fn f87_1_idle_secondary_keeps_serving_past_grant() {
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let budgets = short_grant();
    let db = opened_with(&rt, dir.path(), &store, budgets);
    let k1 = db.put(b"k1", b"one", None).expect("k1");
    let two = Source::Node(NodeId(2));
    let (_, at) = tail(&db, FeedCursor::START, two, k1.seq);
    let idle = Duration::from_millis(3 * budgets.grant_millis);
    let started = Instant::now();
    let mut calls = 0_u32;
    while started.elapsed() < idle {
        match db.changes(PARTITION, at, 10, two) {
            Ok(FeedPage::Records { records, .. }) => assert!(records.is_empty()),
            other => panic!("call {calls} at {:?} idle: {other:?}", started.elapsed()),
        }
        calls += 1;
        std::thread::sleep(Duration::from_millis(budgets.grant_millis / 5));
    }
    assert!(calls >= 15, "{calls} calls");
}

/// A secondary's feed takes no work from the owner: with node 1's thread stalled, node 2 still
/// answers.
/// Cut: `Db::changes` sends `Source::Node` calls to the owner, red (no answer).
/// Integration (~2 s): a fresh open.
#[config_log::retcd_test]
fn f87_1_secondary_call_never_reaches_owner() {
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let db = opened(&rt, dir.path(), &store);
    let k1 = db.put(b"k1", b"one", None).expect("k1");
    let two = Source::Node(NodeId(2));
    let (_, at) = tail(&db, FeedCursor::START, two, k1.seq);
    let (release, stall) = mpsc::channel::<()>();
    db.send_to(NodeId(1), Msg::Stall(stall)).expect("a stall");
    let page = db.changes(PARTITION, FeedCursor::START, 10, two);
    drop(release);
    let FeedPage::Records { next, .. } = page.expect("node 2 answers alone") else {
        panic!("not a page");
    };
    assert_eq!(next, at);
}

/// Q3: the feed returns only the caller's tenant's writes. A tenant-2 write between two of
/// tenant 1's in the same partition is never returned, and the cursor still moves past it.
/// Cut: `feed::record_of` keeps writes of every tenant, red (three records).
/// Integration (~2 s): a fresh open.
#[config_log::retcd_test]
fn f87_1_other_tenant_writes_never_returned() {
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let db = opened(&rt, dir.path(), &store);
    let one = db.put(b"mine-1", b"1", None).expect("tenant 1");
    let other = RequestIdentity {
        tenant: TenantId(2),
        client: db.client,
        request: RequestId(1 << 50),
    };
    let theirs = db
        .put_identified(other, GROUP, b"theirs", b"2", None)
        .expect("tenant 2");
    let two = db.put(b"mine-2", b"3", None).expect("tenant 1 again");
    assert!(
        one.seq < theirs.seq && theirs.seq < two.seq,
        "{one:?} {theirs:?} {two:?}"
    );
    for source in [Source::Owner, Source::Node(NodeId(2))] {
        let (records, at) = tail(&db, FeedCursor::START, source, two.seq);
        assert_eq!(seqs(&records), vec![one.seq.0, two.seq.0], "{source:?}");
        assert_eq!(
            &objects(&records),
            &[b"mine-1".to_vec(), b"mine-2".to_vec()]
        );
        assert_eq!(at.seq, two.seq);
    }
    // A cursor at the record before theirs answers past it, with no record of theirs.
    let (mine, _) = tail(&db, FeedCursor::START, Source::Owner, two.seq);
    let at_one = mine[0].at;
    assert_eq!(at_one.seq, one.seq);
    let FeedPage::Records { records, next, .. } = page_from(&db, at_one, Source::Owner) else {
        panic!("not a page");
    };
    assert_eq!(seqs(&records), vec![two.seq.0]);
    assert_eq!(next.seq, two.seq);
}

/// Spec §5.3: the owner never serves applied-but-unpublished. With both secondaries held, a
/// write applies on the owner and waits for an ack; while it is in flight the owner's feed
/// stays at the last published record. Once the links heal it publishes, and the feed has it.
/// Cut: `Host::changes` caps the owner at the higher of published and `buffered_applied`, red
/// (the unpublished write is returned).
/// Integration (~3 s): a held write.
#[config_log::retcd_test]
fn f87_1_feed_never_passes_published_seq() {
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let db = opened(&rt, dir.path(), &store);
    let k1 = db.put(b"k1", b"one", None).expect("k1");
    let (_, at_k1) = tail(&db, FeedCursor::START, Source::Owner, k1.seq);
    db.links().hold(NodeId(1), NodeId(2));
    db.links().hold(NodeId(1), NodeId(3));
    std::thread::scope(|scope| {
        let held = scope.spawn(|| db.put(b"k2", b"two", None));
        let deadline = Instant::now() + patience();
        while db.node_status()[0].in_flight.is_none() {
            assert!(Instant::now() < deadline, "k2 never applied on the owner");
            std::thread::sleep(Duration::from_millis(10));
        }
        for _ in 0..5 {
            let page = db
                .changes(PARTITION, at_k1, 10, Source::Owner)
                .expect("a page");
            assert_eq!(
                page,
                FeedPage::Records {
                    records: Vec::new(),
                    next: at_k1,
                    more: false
                },
                "k2 is applied, not published"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        db.links().heal(NodeId(1), NodeId(2));
        db.links().heal(NodeId(1), NodeId(3));
        let k2 = held.join().expect("the put thread").expect("k2 publishes");
        let (records, _) = tail(&db, at_k1, Source::Owner, k2.seq);
        assert_eq!(seqs(&records), vec![k2.seq.0]);
    });
}

/// A copy behind the cursor answers `Behind` with its head, never a page or `Trimmed`: node 2
/// is held from the owner, a write publishes through node 3, and node 2 is asked with the
/// owner's cursor after it.
/// Cut: `feed::resume` skips the cap check before the lookup, red (`Trimmed`).
/// Integration (~2 s): a fresh open.
#[config_log::retcd_test]
fn f87_1_lagging_copy_answers_behind() {
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let db = opened(&rt, dir.path(), &store);
    let k1 = db.put(b"k1", b"one", None).expect("k1");
    let two = Source::Node(NodeId(2));
    tail(&db, FeedCursor::START, two, k1.seq);
    db.links().hold(NodeId(1), NodeId(2));
    let k2 = db.put(b"k2", b"two", None).expect("k2 through node 3");
    let (_, at_k2) = tail(&db, FeedCursor::START, Source::Owner, k2.seq);
    let page = page_from(&db, at_k2, two);
    assert_eq!(
        page,
        FeedPage::Behind { head: k1.seq },
        "node 2 holds up to k1"
    );
}

/// A shadow copy refuses the feed: it holds what catch-up gave it, which no regular copy has
/// vouched for. As in `m11_t7`, node 4's frames to node 1 are lost, so `move 3 4` pins node 4
/// as a shadow that never catches up; asked directly, node 4 answers `UNAVAILABLE` naming its
/// role, and never a page.
/// Cut: `Host::changes` lets a shadow past the role check, red (refused for another reason).
/// Integration (~3 s): a fresh open, a second process, the move's re-plan.
#[config_log::retcd_test]
fn f87_1_shadow_copy_refuses() {
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let links = Arc::new(Links::default());
    let db = rt
        .block_on(Db::open_with(
            DbConfig {
                links: Some(Arc::clone(&links)),
                ..config(dir.path())
            },
            Arc::clone(&store),
            rt.handle().clone(),
            fast(),
        ))
        .expect("the open of nodes 1 to 3");
    db.wait_ready(patience()).expect("ready");
    let spare = rt
        .block_on(Db::open_with(
            DbConfig {
                local: vec![NodeId(4)],
                links: Some(Arc::clone(&links)),
                ..config(dir.path())
            },
            Arc::clone(&store),
            rt.handle().clone(),
            fast(),
        ))
        .expect("node 4's process");
    db.put(b"k1", b"one", None).expect("k1");
    links.drop_direction(NodeId(4), NodeId(1));
    let moved = db.move_copy(NodeId(3), NodeId(4)).expect("move 3 4");
    assert!(!moved.done, "{moved:?}");
    let four = Source::Node(NodeId(4));
    let deadline = Instant::now() + patience();
    let refused = loop {
        match spare.changes(PARTITION, FeedCursor::START, 10, four) {
            Ok(page) => panic!("a shadow served {page:?}"),
            Err(error) => {
                assert_eq!(error.kind, ErrorKind::Unavailable, "{error:?}");
                assert!(error.no_mutation, "{error:?}");
                if error.detail.contains("no regular copy") && error.detail.contains("Shadow") {
                    break error;
                }
                assert!(
                    Instant::now() < deadline,
                    "node 4 never refused as a shadow: {error:?}"
                );
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    tracing::info!(detail = %refused.detail, "f87_1_shadow_refused");
}

/// The old owner a promote left out refuses the feed, as a write would: its grant no longer
/// admits, so it cannot serve its published prefix, which may hold records the new generation
/// cut. Node 1 is cut off, node 2 promoted, node 1 asked directly.
/// Cut: `Host::changes` logs the owner's denial and serves on, red (a page, or `UNAVAILABLE`).
/// Integration (~6 s): a grant wait.
#[config_log::retcd_test]
fn f87_1_left_out_owner_refuses() {
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let db = opened(&rt, dir.path(), &store);
    let k1 = db.put(b"k1", b"one", None).expect("k1");
    let (_, at_k1) = tail(&db, FeedCursor::START, Source::Node(NodeId(1)), k1.seq);
    db.links().hold(NodeId(1), NodeId(2));
    db.links().hold(NodeId(1), NodeId(3));
    let promoted = db.promote(NodeId(2), None).expect("the promote commits");
    assert_eq!(promoted.dropped, NodeId(1), "{promoted:?}");
    for from in [FeedCursor::START, at_k1] {
        let error = db
            .changes(PARTITION, from, 10, Source::Node(NodeId(1)))
            .expect_err("node 1 is left out");
        assert!(
            matches!(error.kind, ErrorKind::LeaseExpired | ErrorKind::NotPrimary),
            "{error:?}"
        );
        assert!(error.no_mutation, "{error:?}");
    }
}

/// M6, measured not asserted: write latency at 100 writes/s with no consumer, then with one
/// consumer tailing each copy (owner, node 2, node 3) every 10 ms. Prints p50/p99/max for each.
/// Run by hand: `cargo test -p rdb-api --lib -- --ignored f87_1_measure --nocapture`.
#[config_log::retcd_test]
#[ignore = "a measurement, run by hand; timing depends on the host's load"]
fn f87_1_measure_write_p99_with_tailing_consumers() {
    const WRITES: usize = 600;
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let db = opened(&rt, dir.path(), &store);
    let run = |label: &str, consumers: &[Source]| {
        let stop = AtomicBool::new(false);
        let mut millis = std::thread::scope(|scope| {
            for source in consumers {
                let (db, stop) = (&db, &stop);
                scope.spawn(move || {
                    let (mut at, mut pages) = (FeedCursor::START, 0_u64);
                    while !stop.load(Ordering::Relaxed) {
                        match db.changes(PARTITION, at, FEED_PAGE_RECORDS, *source) {
                            Ok(FeedPage::Records { next, .. }) => at = next,
                            Ok(other) => panic!("{source:?}: {other:?}"),
                            Err(error) => assert!(
                                matches!(
                                    error.kind,
                                    ErrorKind::Unavailable | ErrorKind::LeaseExpired
                                ),
                                "{error:?}"
                            ),
                        }
                        pages += 1;
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    tracing::info!(?source, pages, seq = at.seq.0, "f87_measure_consumer");
                });
            }
            let started = Instant::now();
            let mut millis = Vec::with_capacity(WRITES);
            for i in 0..WRITES {
                let due = started + Duration::from_millis(10 * i as u64);
                if let Some(wait) = due.checked_duration_since(Instant::now()) {
                    std::thread::sleep(wait);
                }
                let sent = Instant::now();
                db.put(format!("m{}", i % 64).as_bytes(), b"v", None)
                    .expect("a measured put");
                millis.push(sent.elapsed().as_secs_f64() * 1000.0);
            }
            stop.store(true, Ordering::Relaxed);
            millis
        });
        millis.sort_by(f64::total_cmp);
        let at = |q: f64| millis[((millis.len() as f64 - 1.0) * q).round() as usize];
        let line = format!(
            "f87 p99 {label}: n={} p50={:.2}ms p99={:.2}ms max={:.2}ms",
            millis.len(),
            at(0.50),
            at(0.99),
            at(1.0)
        );
        eprintln!("{line}");
        tracing::info!(
            label,
            p50 = at(0.5),
            p99 = at(0.99),
            max = at(1.0),
            "f87_measure"
        );
    };
    // Warm up, then alternate so drift in the host's load falls on both.
    run("warmup", &[]);
    for round in 1..=2 {
        run(&format!("none r{round}"), &[]);
        run(
            &format!("tailing r{round}"),
            &[
                Source::Owner,
                Source::Node(NodeId(2)),
                Source::Node(NodeId(3)),
            ],
        );
    }
}

/// Slice 1 keeps the feed off the wire: a `Changes` call does not convert to a `WireCall`, and
/// the refusal names slice 2, so a remote caller is told why rather than "unreachable".
/// Cut: convert `Changes` to a `WaitDurable` frame, red.
#[test]
fn f87_1_changes_refuses_the_wire_until_slice_2() {
    let call = ClientCall::Changes {
        identity: RequestIdentity {
            tenant: TENANT,
            client: ClientId(1),
            request: RequestId(1),
        },
        from: FeedCursor::START,
        max: 10,
    };
    match crate::net::wire::WireCall::try_from(&call) {
        Err(crate::net::wire::WireError::Value(detail)) => {
            assert!(detail.contains("slice 2"), "{detail}");
        }
        other => panic!("{other:?}"),
    }
}
