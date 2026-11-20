//! Issue #87 (ADR-rdb-0020) slice 1: the change feed through [`Db`], on wall-clock time. The
//! pure resume and page rules are tested in `feed`.

use super::reopen_tests::{config, fast, runtime};
use super::*;
use crate::feed::{FeedRecord, FEED_PAGE_RECORDS};
use crate::net::faults::{CutStore, Faults};
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::envelope::ReplicationEnvelope;
use rdb_core::contracts::storage::Namespace;

use super::error::FEED_HISTORY_STEP;

fn store() -> Arc<dyn ConfigStore> {
    Arc::new(config_testkit::MemStore::new())
}

/// Review r1: a store whose reads of `partitions/1` wait while it is closed, so a secondary's
/// feed control read can be held as long as a row needs. It counts the reads it held. Every
/// other call goes straight through.
struct GatedStore {
    inner: Arc<dyn ConfigStore>,
    key: Bytes,
    closed: AtomicBool,
    held: AtomicU64,
}

impl GatedStore {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: store(),
            key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
            closed: AtomicBool::new(false),
            held: AtomicU64::new(0),
        })
    }

    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }

    fn open(&self) {
        self.closed.store(false, Ordering::SeqCst);
    }

    /// Reads of `partitions/1` that met the gate closed.
    fn held(&self) -> u64 {
        self.held.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl ConfigStore for GatedStore {
    async fn get(
        &self,
        request: config_core::GetRequest,
    ) -> Result<config_core::GetResponse, config_core::ConfigError> {
        if request.key == self.key && self.closed.load(Ordering::SeqCst) {
            self.held.fetch_add(1, Ordering::SeqCst);
            while self.closed.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
        self.inner.get(request).await
    }

    async fn list(
        &self,
        request: config_core::ListRequest,
    ) -> Result<config_core::ListResponse, config_core::ConfigError> {
        self.inner.list(request).await
    }

    async fn list_page(
        &self,
        request: config_core::PageRequest,
    ) -> Result<config_core::ListPage, config_core::ConfigError> {
        self.inner.list_page(request).await
    }

    async fn put(
        &self,
        request: config_core::PutRequest,
    ) -> Result<config_core::MutationResponse, config_core::ConfigError> {
        self.inner.put(request).await
    }

    async fn delete(
        &self,
        request: config_core::DeleteRequest,
    ) -> Result<config_core::MutationResponse, config_core::ConfigError> {
        self.inner.delete(request).await
    }

    fn capabilities(&self) -> config_core::Capabilities {
        self.inner.capabilities()
    }

    async fn watch(
        &self,
        request: config_core::WatchRequest,
    ) -> Result<config_core::WatchStream, config_core::ConfigError> {
        self.inner.watch(request).await
    }
}

/// `n` feed calls to `node` from `START`, each on its own thread, started together.
fn calls_together(
    db: &Arc<Db>,
    node: NodeId,
    n: usize,
) -> Vec<std::thread::JoinHandle<(Result<FeedPage, ApiError>, Duration)>> {
    (0..n)
        .map(|_| {
            let db = Arc::clone(db);
            std::thread::spawn(move || {
                let started = Instant::now();
                let answer = db.changes(PARTITION, FeedCursor::START, 10, Source::Node(node));
                (answer, started.elapsed())
            })
        })
        .collect()
}

/// Wait until `store` has held `n` reads.
fn until_held(store: &GatedStore, n: u64) {
    let deadline = Instant::now() + patience();
    while store.held() < n {
        assert!(
            Instant::now() < deadline,
            "{} reads held, not {n}",
            store.held()
        );
        std::thread::sleep(Duration::from_millis(5));
    }
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
/// `CORRUPT_HISTORY`, claiming no mutation. That cursor is the caller's input, so the node logs
/// it at warn and says it may not belong to this partition's history (review r1).
/// `max` 0 is `INVALID_ARGUMENT` with `no_mutation`, from the owner and from a secondary.
/// Cuts: `feed::resume` skips the digest comparison, red (a page, not the error);
/// `feed_cursor_diverged` back at error, red (level `Error`); `Host::changes` without its
/// `max == 0` refusal, red (an empty page).
/// Integration (~2 s): a fresh open.
#[config_log::retcd_test]
fn f87_1_owner_feed_matches_puts_in_order_and_a_wrong_digest_is_corrupt_history() {
    const METHOD: &str =
        "f87_1_owner_feed_matches_puts_in_order_and_a_wrong_digest_is_corrupt_history";
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
    assert!(
        error
            .detail
            .contains("may not belong to partition 1's history"),
        "{error:?}"
    );
    // Review r1: `max` 0 is refused before any copy is read, from the owner and a secondary.
    for source in [Source::Owner, Source::Node(NodeId(2))] {
        let error = db
            .changes(PARTITION, FeedCursor::START, 0, source)
            .expect_err("max 0");
        assert_eq!(
            error.kind,
            ErrorKind::InvalidArgument,
            "{source:?}: {error:?}"
        );
        assert!(error.no_mutation, "{source:?}: {error:?}");
    }
    let lines: Vec<_> = config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
        .into_iter()
        .filter(|line| line["@m"] == "feed_cursor_diverged")
        .collect();
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert_eq!(lines[0]["@l"], "Warning", "{lines:?}");
    assert!(
        lines[0]["why"]
            .as_str()
            .is_some_and(|why| why.contains("may not belong to this partition's history")),
        "{lines:?}"
    );
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

/// A consumer that polls slower than the grant, as a warm standby does. Every call is answered
/// with a page, the first one included: the call waits for the control read it starts.
/// Before the fix each call started a read and was refused "call again" at once, and the read
/// it started was older than the grant by the next call, so it never served (#87 lr walk, s2d).
/// Cut: `Host::changes` answers a call whose read is in flight at once ("call again") instead of
/// parking it in `FeedFreshness::waiting`, red (the first call is refused).
/// Integration (~8 s): three 2.25 s gaps.
#[config_log::retcd_test]
fn f87_2_a_slow_consumer_at_a_fresh_member_is_served_every_call() {
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let budgets = short_grant();
    let db = opened_with(&rt, dir.path(), &store, budgets);
    let k1 = db.put(b"k1", b"one", None).expect("k1");
    db.wait_durable(
        DEFAULT_GROUP,
        (k1.generation, k1.seq),
        Duration::from_secs(30),
    )
    .expect("k1 on every disk");
    let two = Source::Node(NodeId(2));
    for call in 0..4 {
        match db.changes(PARTITION, FeedCursor::START, 10, two) {
            Ok(FeedPage::Records { records, .. }) => {
                assert_eq!(seqs(&records).last(), Some(&k1.seq.0), "call {call}");
            }
            other => panic!("call {call} to node 2, a fresh member: {other:?}"),
        }
        std::thread::sleep(Duration::from_millis(budgets.grant_millis * 3 / 2));
    }
}

/// #87 lr walk (s2d): a regular copy cut off from its owner past the drop clock is left out of
/// the next generation. Polled slower than the grant, its feed serves while `partitions/1` still
/// pins it, then answers a definitive refusal: `UNAVAILABLE` with `Definitive`, naming the
/// generation that left it out and the owner, with the step to read another copy. It never
/// answers "call again", and the refusal comes within a few grants of the drop.
/// Cut: `Host::feed_fresh` treats a record that leaves this node out of a newer generation as
/// "still landing" (`BoundedJitter`), red (no definitive answer).
/// Integration (~12 s): a 2 s drop clock, 2.25 s gaps.
#[config_log::retcd_test]
fn f87_2_a_copy_left_out_while_held_answers_definitively_within_a_bound() {
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let budgets = short_grant();
    let db = rt
        .block_on(Db::open_with(
            DbConfig {
                timeouts: Timeouts {
                    unreachable_drop_after: Duration::from_secs(2),
                    ..Timeouts::default()
                },
                ..config(dir.path())
            },
            Arc::clone(&store),
            rt.handle().clone(),
            budgets,
        ))
        .expect("the open");
    db.wait_ready(patience()).expect("ready");
    let k1 = db.put(b"k1", b"one", None).expect("k1");
    db.wait_durable(
        DEFAULT_GROUP,
        (k1.generation, k1.seq),
        Duration::from_secs(30),
    )
    .expect("k1 on every disk");
    db.links().hold(NodeId(1), NodeId(3));
    db.links().hold(NodeId(2), NodeId(3));
    let held = Instant::now();
    let three = Source::Node(NodeId(3));
    let gap = Duration::from_millis(budgets.grant_millis * 3 / 2);
    let (mut served, mut dropped_at) = (0_u32, None);
    let refused = loop {
        if dropped_at.is_none()
            && db
                .pinned_members()
                .is_ok_and(|(_, pinned)| pinned.iter().all(|member| member.node != NodeId(3)))
        {
            dropped_at = Some(Instant::now());
        }
        match db.changes(PARTITION, FeedCursor::START, 10, three) {
            Ok(FeedPage::Records { .. }) => served += 1,
            Ok(other) => panic!("node 3 while held: {other:?}"),
            Err(error) if error.retry == RetryRule::Definitive => break error,
            Err(error) => panic!(
                "node 3 after {:?} held, {served} served: not definitive: {error}",
                held.elapsed()
            ),
        }
        assert!(
            held.elapsed() < patience(),
            "node 3 never refused definitively; {served} served"
        );
        std::thread::sleep(gap);
    };
    let dropped_at = dropped_at.unwrap_or_else(Instant::now);
    let bound = crate::host::test_patience(Duration::from_millis(3 * budgets.grant_millis) + gap);
    assert!(
        dropped_at.elapsed() < bound,
        "refused {:?} after the drop was seen",
        dropped_at.elapsed()
    );
    assert_eq!(refused.kind, ErrorKind::Unavailable, "{refused:?}");
    assert!(refused.no_mutation, "{refused:?}");
    let text = refused.to_string();
    assert!(
        text.contains("node 3 holds no current copy of partition 1")
            && text.contains("left out at generation")
            && text.contains("owner node 1")
            && text.contains("read from the owner or another member"),
        "{text}"
    );
    assert!(
        !text.contains("call again") && !text.contains("wait a few seconds"),
        "{text}"
    );
    // Definitive means it stays so: the next call, a gap later, answers the same.
    std::thread::sleep(gap);
    let again = db
        .changes(PARTITION, FeedCursor::START, 10, three)
        .expect_err("still left out");
    assert_eq!(again.retry, RetryRule::Definitive, "{again:?}");
    db.links().heal(NodeId(1), NodeId(3));
    db.links().heal(NodeId(2), NodeId(3));
}

/// A feed cursor whose digest is not the copy's record is `CORRUPT_HISTORY`, and its step is
/// the feed's own: read another copy or rebuild from your own source, never the map-entry step
/// "write over its key", which a feed caller cannot act on.
/// Cut: `Host::changes` refuses a diverged cursor without `FEED_HISTORY_STEP`, red (the kind's
/// generic step).
/// Integration (~2 s): a fresh open.
#[config_log::retcd_test]
fn f87_2_a_diverged_feed_cursor_says_to_read_another_copy() {
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let db = opened(&rt, dir.path(), &store);
    let one = db.put(b"x", b"v", None).expect("x");
    let two = db.put(b"y", b"v", None).expect("y");
    let (records, _) = tail(&db, FeedCursor::START, Source::Owner, two.seq);
    assert_eq!(seqs(&records), vec![one.seq.0, two.seq.0]);
    let forged = FeedCursor {
        digest: records[0].at.digest,
        ..records[1].at
    };
    for source in [Source::Owner, Source::Node(NodeId(2))] {
        let deadline = Instant::now() + patience();
        let error = loop {
            match db.changes(PARTITION, forged, 10, source) {
                Err(error) if error.kind == ErrorKind::CorruptHistory => break error,
                other => assert!(Instant::now() < deadline, "{source:?}: {other:?}"),
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        let text = error.to_string();
        assert!(
            text.contains("read from another copy (the owner or another member)")
                && text.contains("rebuild from your own source"),
            "{source:?}: {text}"
        );
        assert!(!text.contains("write over its key"), "{source:?}: {text}");
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

/// Warm standby (ADR-rdb-0020, design S-E): a consumer replays node 2's feed from `START`, two
/// records a page, decodes each after-image with rdb-value (the envelope, then the CBOR
/// document) and keeps the last value per object. The map it builds equals what the owner
/// reads for every object, including one written four times across two pages.
/// Cut: `feed::page` returns its records in reverse order, red (`a` rebuilds as `v3`, not
/// `v4`).
/// Integration (~3 s): a fresh open and node 2's first fresh read.
#[config_log::retcd_test]
fn f87_2_replay_from_start_rebuilds_user_state() {
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let db = opened(&rt, dir.path(), &store);
    let written: [(&[u8], &[u8]); 6] = [
        (b"a", b"v1"),
        (b"a", b"v2"),
        (b"a", b"v3"),
        (b"a", b"v4"),
        (b"b", b"b1"),
        (b"c", b"c1"),
    ];
    let mut last = Seq(0);
    for (object, value) in written {
        last = db.put(object, value, None).expect("a put").seq;
    }
    let node2 = Source::Node(NodeId(2));
    // Node 2 has applied every put, so each page below is two records and the pages fall
    // [a v1, a v2], [a v3, a v4], [b, c].
    tail(&db, FeedCursor::START, node2, last);

    let mut state: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let (mut at, mut pages) = (FeedCursor::START, 0);
    let deadline = Instant::now() + patience();
    while at.seq < last {
        assert!(Instant::now() < deadline, "replay stuck at {at:?}");
        let page = match db.changes(PARTITION, at, 2, node2) {
            Ok(page) => page,
            Err(error) if error.kind == ErrorKind::Unavailable => {
                std::thread::sleep(Duration::from_millis(20));
                continue;
            }
            Err(error) => panic!("replay from {at:?}: {error:?}"),
        };
        let FeedPage::Records { records, next, .. } = page else {
            panic!("replay from {at:?}: {page:?}");
        };
        for write in records.iter().flat_map(|record| &record.writes) {
            let stored = write.value.as_ref().expect("a put stores a value");
            let opened = rdb_value::envelope::open(stored).expect("a sealed after-image");
            assert_eq!(opened.kind, rdb_value::envelope::Kind::Document);
            let rdb_value::value::Value::Bytes(bytes) =
                rdb_value::cbor::decode(opened.payload).expect("a CBOR document")
            else {
                panic!("a put stores a byte string: {write:?}");
            };
            state.insert(write.object.to_vec(), bytes);
        }
        at = next;
        pages += 1;
    }
    assert_eq!(pages, 3, "two records a page");

    let mut owner: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for object in [b"a", b"b", b"c"] {
        let (_, value) = db
            .get(object)
            .expect("the owner reads")
            .value
            .expect("written");
        owner.insert(object.to_vec(), value.to_vec());
    }
    assert_eq!(state, owner, "the replayed state is the owner's");
    assert_eq!(state.get(b"a".as_slice()), Some(&b"v4".to_vec()));
}

/// Review r1: stored damage a feed read meets faults the node, as before, and every caller it
/// answers hears `CORRUPT_HISTORY` with the feed's step, never a generic host fault or a call
/// dropped unanswered. The owner's record with a broken frame (the read itself fails
/// `Corrupt`): typed. Then node 2's record cut one byte short (the envelope does not decode),
/// with two callers parked on one held control read: when the read lands, the first answer
/// faults the node, and the second caller hears the same typed fault.
/// Cuts: `feed_damage` returning `Fault::from` (untyped), red (caller 0: the generic host
/// fault, `UNAVAILABLE`); `answer_feed_waiters` dropping the rest at the fault (the old `?`),
/// red (caller 1: "node 2 dropped the feed call unanswered"); no `feed_reads` drain in
/// `Host::set_fault`, red (caller 1: the same).
/// Integration (~3 s): a fresh open.
#[config_log::retcd_test]
fn f87_2_damaged_history_answers_every_feed_caller_corrupt_history() {
    let rt = runtime();
    let gated = GatedStore::new();
    let store: Arc<dyn ConfigStore> = Arc::clone(&gated) as Arc<dyn ConfigStore>;
    let dir = config_testkit::fs::temp_dir();
    let db = Arc::new(opened(&rt, dir.path(), &store));
    let k1 = db.put(b"k1", b"one", None).expect("k1");
    db.wait_durable(
        DEFAULT_GROUP,
        (k1.generation, k1.seq),
        Duration::from_secs(30),
    )
    .expect("k1 on every disk");
    let typed = |who: &str, error: &ApiError| {
        assert_eq!(error.kind, ErrorKind::CorruptHistory, "{who}: {error:?}");
        assert!(
            error.detail.ends_with(FEED_HISTORY_STEP),
            "{who}: {error:?}"
        );
        assert_eq!(
            error.to_string().matches("next:").count(),
            1,
            "{who}: {error}"
        );
    };
    let key = k1.seq.0.to_be_bytes();
    db.damage_record_as(
        NodeId(2),
        k1.generation,
        Namespace::History,
        &key,
        rdb_storage::RecordDamage::Truncate,
    )
    .expect("node 2's record cut short");
    gated.close();
    let callers = calls_together(&db, NodeId(2), 2);
    until_held(&gated, 1);
    // Both calls reach node 2 within microseconds of their threads starting; the held read
    // keeps them parked (`feed_waits_for_control_read`, debug) for as long as this lasts.
    std::thread::sleep(Duration::from_millis(200));
    gated.open();
    for (index, caller) in callers.into_iter().enumerate() {
        let (answer, _) = caller.join().expect("the caller");
        typed(
            &format!("node 2, caller {index}"),
            &answer.expect_err("damaged"),
        );
    }
    assert_eq!(gated.held(), 1, "one shared read");
    db.damage_record_as(
        NodeId(1),
        k1.generation,
        Namespace::History,
        &key,
        rdb_storage::RecordDamage::Frame,
    )
    .expect("node 1's frame broken");
    let error = db
        .changes(PARTITION, FeedCursor::START, 10, Source::Owner)
        .expect_err("damaged");
    typed("node 1", &error);
}
