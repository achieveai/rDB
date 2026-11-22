//! Issue #87 (ADR-rdb-0020) slice 1: the change feed through [`Db`], on wall-clock time. The
//! pure resume and page rules are tested in `feed`.

use super::reopen_tests::{config, fast, runtime};
use super::*;
use crate::feed::{FeedRecord, FEED_PAGE_RECORDS};
use crate::net::faults::{CutStore, Faults};
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::envelope::ReplicationEnvelope;
#[cfg(debug_assertions)]
use rdb_core::contracts::storage::Namespace;

#[cfg(debug_assertions)]
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
    page_of(db, from, FEED_PAGE_RECORDS, source)
}

/// [`page_from`] asking for at most `max` records.
fn page_of(db: &Db, from: FeedCursor, max: u32, source: Source) -> FeedPage {
    let deadline = Instant::now() + patience();
    loop {
        match db.changes(PARTITION, from, max, source) {
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
/// owner traffic. Sixteen polls a fifth of a grant apart, so they span three grants however
/// slowly they run; every one is served (review r1: no minimum call count).
/// Cut: `Host::feed_fresh` starts a read only when none has answered yet (`age.is_none()`),
/// red (`UNAVAILABLE` "call again" at the first call past a grant: the parked call waits for a
/// read nobody started).
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
    for call in 0..16 {
        if call > 0 {
            std::thread::sleep(Duration::from_millis(budgets.grant_millis / 5));
        }
        match db.changes(PARTITION, at, 10, two) {
            Ok(FeedPage::Records { records, .. }) => assert!(records.is_empty()),
            other => panic!("call {call} at {:?} idle: {other:?}", started.elapsed()),
        }
    }
    assert!(started.elapsed() >= idle, "{:?}", started.elapsed());
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
/// "still landing" (`BoundedJitter`), red (no definitive answer). Review r1: with a restart
/// note (node 2 restarted), the answer keeps its one step and no retry of node 3.
/// Cut: `Db::explained` without its feed-step check, red (the note and the generic step after
/// the feed's).
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
    // Review r1: with a restart note to tell (node 2 restarted), the refusal still ends with
    // its one step, read another member, and says nothing of calling node 3 again.
    db.restart_node(NodeId(2)).expect("node 2 restarts");
    assert!(db.restart_note().is_some(), "a note to tell");
    let noted = db
        .changes(PARTITION, FeedCursor::START, 10, three)
        .expect_err("still left out");
    let text = noted.to_string();
    assert_eq!(noted.retry, RetryRule::Definitive, "{noted:?}");
    assert_eq!(text.matches("next:").count(), 1, "{text}");
    assert!(
        text.ends_with("calling this copy again will not help while it holds no current copy)"),
        "{text}"
    );
    assert!(
        !text.contains("restarted") && !text.contains("call again"),
        "{text}"
    );
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
/// with two callers parked on one held control read, counted from node 2's
/// `feed_waits_for_control_read` lines before the read is let go: when the read lands, the
/// first answer faults the node, and the second caller hears the same typed fault.
/// Cuts: `feed_damage` returning `Fault::from` (untyped), red (caller 0: the generic host
/// fault, `UNAVAILABLE`); `answer_feed_waiters` dropping the rest at the fault (the old `?`),
/// red (caller 1: "node 2 dropped the feed call unanswered"); no `feed_reads` drain in
/// `Host::set_fault`, red (caller 1: the same).
/// Integration (~3 s): a fresh open.
#[cfg(debug_assertions)]
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
    // Review r2: open the read only once node 2 says both calls are parked on it.
    let deadline = Instant::now() + patience();
    loop {
        let parked = config_testkit::logs::lines_for_current_test(
            module_path!(),
            "f87_2_damaged_history_answers_every_feed_caller_corrupt_history",
        )
        .into_iter()
        .filter(|line| line["@m"] == "feed_waits_for_control_read" && line["node"] == 2)
        .count();
        if parked >= 2 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "{parked} feed calls parked, not 2"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
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

/// One feed call to `node` from `START`, refused "call again" because its control read did not
/// land within the wait (`FEED_READ_WAIT_MILLIS`), not by the caller's own timeout.
fn refused_call_again(who: &str, answer: Result<FeedPage, ApiError>, took: Duration) {
    let error = answer.expect_err(who);
    assert_eq!(error.kind, ErrorKind::Unavailable, "{who}: {error:?}");
    assert!(error.no_mutation, "{who}: {error:?}");
    assert!(
        error.detail.contains("call again"),
        "{who} after {took:?}: {error:?}"
    );
}

/// Review r1: a secondary's control read of `partitions/1` held past the 500 ms wait, through
/// the real feed path.
/// (a) Two callers that arrive while it is held share one read and are each refused "call
/// again" when the wait ends, while the owner goes on writing.
/// (b) The callers abandoned the read; a later call starts no second one and is refused the same.
/// (c) Released, the abandoned read lands and the next call is served from it.
/// (d) After node 2 restarts, a read stamped with its old boot that lands late is dropped
/// (`control_stale_boot_dropped`, `feed_read`), so a call parked on the new boot's held read is
/// still refused, not served from the old process's answer.
/// Cuts: no `until` on parked calls (`Host::feed_duty` never due), red ((a): "no feed answer in
/// 1s"); `Host::feed_fresh` without `!entry.in_flight`, red ((a): 4 reads held); `Msg::FeedRead`
/// taken whatever its boot, red ((d): served from the old boot's read).
/// Integration (~10 s): a restart and its rejoin.
#[config_log::retcd_test]
fn f87_2_a_held_control_read_refuses_within_the_wait_shared_and_fenced_by_boot() {
    const METHOD: &str =
        "f87_2_a_held_control_read_refuses_within_the_wait_shared_and_fenced_by_boot";
    let rt = runtime();
    let gated = GatedStore::new();
    let store: Arc<dyn ConfigStore> = Arc::clone(&gated) as Arc<dyn ConfigStore>;
    let dir = config_testkit::fs::temp_dir();
    let budgets = short_grant();
    let db = Arc::new(opened_with(&rt, dir.path(), &store, budgets));
    let k1 = db.put(b"k1", b"one", None).expect("k1");
    db.wait_durable(
        DEFAULT_GROUP,
        (k1.generation, k1.seq),
        Duration::from_secs(30),
    )
    .expect("k1 on every disk");

    gated.close();
    let callers = calls_together(&db, NodeId(2), 2);
    until_held(&gated, 1);
    let k2 = db
        .put(b"k2", b"two", None)
        .expect("the owner writes while the read is held");
    for (index, caller) in callers.into_iter().enumerate() {
        let (answer, took) = caller.join().expect("the caller");
        refused_call_again(&format!("(a) caller {index}"), answer, took);
    }
    assert_eq!(gated.held(), 1, "(a) one shared read");

    let (answer, took) = calls_together(&db, NodeId(2), 1)
        .pop()
        .expect("one caller")
        .join()
        .expect("the caller");
    refused_call_again("(b) a later call", answer, took);
    assert_eq!(
        gated.held(),
        1,
        "(b) no second read while the first is held"
    );

    gated.open();
    let (records, _) = tail(&db, FeedCursor::START, Source::Node(NodeId(2)), k2.seq);
    assert_eq!(seqs(&records), vec![k1.seq.0, k2.seq.0], "(c)");

    let old = db.boot_of(NodeId(2)).expect("node 2's boot");
    db.restart_node(NodeId(2)).expect("node 2 restarts");
    tail(&db, FeedCursor::START, Source::Node(NodeId(2)), k2.seq);
    let landed = db
        .node_status()
        .into_iter()
        .find(|status| status.node == NodeId(2))
        .and_then(|status| status.holds)
        .expect("node 2 holds a generation")
        .generation;
    // Past the grant, so the next call needs a new read.
    std::thread::sleep(Duration::from_millis(budgets.grant_millis + 200));
    gated.close();
    let caller = calls_together(&db, NodeId(2), 1).pop().expect("one caller");
    until_held(&gated, 2);
    let late = Msg::FeedRead {
        boot: old,
        partition: PARTITION,
        started: rdb_core::contracts::time::Tick(u64::MAX / 2),
        read: Ok(crate::host::FeedControlRead {
            generation: landed,
            owner: NodeId(1),
            members: None,
        }),
    };
    lock(&db.nodes)
        .iter()
        .find(|handle| handle.node == NodeId(2))
        .expect("node 2")
        .send(late)
        .expect("node 2's mailbox");
    let (answer, took) = caller.join().expect("the caller");
    refused_call_again("(d) parked on the new boot's read", answer, took);
    gated.open();
    let dropped: Vec<_> = config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
        .into_iter()
        .filter(|line| line["@m"] == "control_stale_boot_dropped" && line["what"] == "feed_read")
        .collect();
    assert_eq!(dropped.len(), 1, "{dropped:?}");
    assert_eq!(dropped[0]["from_boot"], old.0, "{dropped:?}");
}

/// `source`'s answer from `from`, which must be `Trimmed`; its floor.
#[cfg(debug_assertions)]
fn trimmed_from(db: &Db, from: FeedCursor, source: Source, who: &str) -> Seq {
    match page_from(db, from, source) {
        FeedPage::Trimmed { floor } => floor,
        other => panic!("{who} from {from:?} on {source:?}: {other:?}"),
    }
}

/// Waits, bounded by [`patience`], until node 2's rejoin has settled, and returns the
/// generation the owner then serves. Settled means the owner (R1 role `primary`):
/// - pins node 2 as a regular secondary, and no member as a shadow;
/// - has adopted a generation newer than `than`;
/// - admits writes.
///
/// A restart's rejoin moves the generation twice, by design. The re-add commits one generation
/// that pins node 2 as a shadow. Once that shadow catches up, `shadow_duty`'s promote (A-R329)
/// re-plans and commits the next, which pins it regular. A put landed in between waited for
/// durability in a generation that the promote then replaced (`GENERATION_CHANGED`). Same
/// pattern as #191; #181's `owner_serves` is the shared form of this helper.
#[cfg(debug_assertions)]
fn owner_serves(db: &Db, than: Generation) -> Generation {
    let deadline = Instant::now() + patience();
    loop {
        let statuses = db
            .node_status_in(PARTITION)
            .expect("the partition's status");
        let served = statuses.iter().find_map(|status| {
            let held = status.holds.as_ref()?.generation;
            let settled = status.role == "primary"
                && held > than
                && status.admits == Some(true)
                && status.pinned.iter().any(|member| {
                    member.node == NodeId(2)
                        && member.role == rdb_core::contracts::ids::ReplicaRole::RegularSecondary
                })
                && status
                    .pinned
                    .iter()
                    .all(|member| member.role != rdb_core::contracts::ids::ReplicaRole::Shadow);
            settled.then_some(held)
        });
        if let Some(generation) = served {
            return generation;
        }
        assert!(
            Instant::now() < deadline,
            "node 2's rejoin never settled past {than:?}: {statuses:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Review r1/r2 (lead ruling 2026-11-20): a real `History` floor above seq 1, computed by
/// storage (`RocksEngine::history_floor`) and read through the Db feed path. The debug-only
/// `Db::drop_history` deletes one copy's records below a seq through its lineage's chain,
/// as a history trim (#61) would; `applied`, `Progress` and state stay.
/// Node 2 restarts first, so the newest generation is an inherited lineage linked to the level
/// that holds k1-k4; the row writes only once the rejoin's two designed generation moves
/// (re-add as shadow, then the promote) are done ([`owner_serves`]). Then records below k3 are
/// dropped on the owner and on node 3.
/// (a) Owner and secondary: `START` and a cursor at k1 answer exactly `Trimmed { floor: k3 }`,
/// found through the link; a retained cursor at k3 resumes across the link. Node 2, untouched,
/// still serves from `START`: the floor is per copy.
/// (a) also pages with `max` 1 above that floor, on the owner and on node 3: from k3 only k4,
/// `next` at k4 and `more`, then only k5 (`newer`) on the next call (round 3).
/// (b) An empty lineage: node 3 with every record dropped answers `Trimmed` at its applied head
/// plus one; after the next put that is its first record, and a cursor there resumes.
/// No re-plan follows a drop: recovery reads `History` a dropped copy no longer holds (tried
/// while writing this row: the partition stayed `ReadOnly`), which is #61's work.
/// Cuts: `Host::changes` ignoring `history_floor` (floor `Seq(1)`), red ((a): `START` meets the
/// hole, `CORRUPT_HISTORY`, not `Trimmed`); `RocksEngine::history_floor`'s empty-lineage
/// fallback answering `Seq(1)`, red ((b)); `feed::page` without its `records.len() < returned`
/// bound, red ((a) max 1 returns k4 and k5).
/// Integration (~10 s): one restart's rejoin.
#[cfg(debug_assertions)]
#[config_log::retcd_test]
fn f87_2_a_trimmed_history_answers_exactly_trimmed_below_its_floor() {
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let db = opened(&rt, dir.path(), &store);
    let mut acks = Vec::new();
    for key in [b"k1", b"k2", b"k3", b"k4"] {
        acks.push(db.put(key, b"v", None).expect("a put"));
    }
    let (k1, k3, k4) = (&acks[0], &acks[2], &acks[3]);
    db.restart_node(NodeId(2)).expect("node 2 restarts");
    let serves = owner_serves(&db, k4.generation);
    let newer = db
        .put(b"newer", b"v", None)
        .expect("a put once node 2's rejoin settled");
    assert_eq!(newer.generation, serves, "{newer:?}");
    db.wait_durable(DEFAULT_GROUP, (newer.generation, newer.seq), patience())
        .expect("the newer put on every disk");
    let (all, _) = tail(&db, FeedCursor::START, Source::Owner, newer.seq);
    let at = |seq: Seq| {
        all.iter()
            .find(|record| record.at.seq == seq)
            .expect("held")
            .at
    };
    let (owner, two, three) = (
        Source::Owner,
        Source::Node(NodeId(2)),
        Source::Node(NodeId(3)),
    );
    for node in [1, 3] {
        let dropped = db
            .drop_history(NodeId(node), newer.generation, 0..k3.seq.0)
            .expect("drop below k3");
        assert!(dropped >= 2, "node {node}: {dropped}");
    }

    for source in [owner, three] {
        assert_eq!(trimmed_from(&db, FeedCursor::START, source, "(a)"), k3.seq);
        assert_eq!(trimmed_from(&db, at(k1.seq), source, "(a)"), k3.seq);
        let (resumed, _) = tail(&db, at(k3.seq), source, newer.seq);
        let got = seqs(&resumed);
        assert!(
            got.first() == Some(&k4.seq.0)
                && got.last() == Some(&newer.seq.0)
                && got.windows(2).all(|pair| pair[0] < pair[1]),
            "(a) {source:?} across the link: {got:?}"
        );
    }
    // (a) max 1 above the real floor (round 3, r3 blocker 2): k4 and k5 (`newer`) are held
    // above the floor and cursor k3. One record per call, `more` while one is left.
    for source in [owner, three] {
        match page_of(&db, at(k3.seq), 1, source) {
            FeedPage::Records {
                records,
                next,
                more,
            } => {
                assert_eq!(seqs(&records), vec![k4.seq.0], "(a) max 1 {source:?}");
                assert_eq!(next, at(k4.seq), "(a) max 1 {source:?}: next is k4");
                assert!(more, "(a) max 1 {source:?}: k5 is left");
                match page_of(&db, next, 1, source) {
                    FeedPage::Records { records, .. } => assert_eq!(
                        seqs(&records),
                        vec![newer.seq.0],
                        "(a) max 1 {source:?}: k5 on the next call"
                    ),
                    other => panic!("(a) max 1 {source:?} from k4: {other:?}"),
                }
            }
            other => panic!("(a) max 1 {source:?} from k3: {other:?}"),
        }
    }
    let (untouched, _) = tail(&db, FeedCursor::START, two, newer.seq);
    assert_eq!(seqs(&untouched), seqs(&all), "(a) node 2 is untouched");

    let head = all.last().expect("records").at;
    db.drop_history(NodeId(3), newer.generation, 0..newer.seq.0 + 1)
        .expect("drop every record");
    let empty = trimmed_from(&db, FeedCursor::START, three, "(b) empty");
    assert_eq!(empty, Seq(newer.seq.0 + 1), "(b) the applied head plus one");
    let next = db.put(b"k-next", b"v", None).expect("the next put");
    assert_eq!(next.seq, empty, "no record between");
    db.wait_durable(DEFAULT_GROUP, (next.generation, next.seq), patience())
        .expect("next on every disk");
    assert_eq!(
        trimmed_from(&db, head, three, "(b) at the old head"),
        next.seq
    );
    let (to_next, _) = tail(&db, head, owner, next.seq);
    let after = db.put(b"k-after", b"v", None).expect("one more");
    let (resumed, _) = tail(&db, to_next[0].at, three, after.seq);
    assert_eq!(
        seqs(&resumed),
        vec![after.seq.0],
        "(b) resumed at its floor"
    );
}

/// One feed call's answer once it is not `UNAVAILABLE` or `LEASE_EXPIRED`, retried as in
/// [`page_from`] until `patience` ends.
#[cfg(debug_assertions)]
fn changes_settled(db: &Db, from: FeedCursor, source: Source) -> Result<FeedPage, ApiError> {
    let deadline = Instant::now() + patience();
    loop {
        match db.changes(PARTITION, from, FEED_PAGE_RECORDS, source) {
            Err(error)
                if matches!(error.kind, ErrorKind::Unavailable | ErrorKind::LeaseExpired)
                    && Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(20));
            }
            answer => return answer,
        }
    }
}

/// Review r2 (thread 4647111493): a stored `History` record that still decodes but is damaged
/// is damage, typed `CORRUPT_HISTORY` with the feed's step, never served as a page and never
/// read as the cursor's divergence. Three copies, three cases, each its own copy (the first
/// fault stops a node):
/// - node 3, START: k1's last byte flipped (it decodes; its digest no longer matches);
/// - node 2, a good cursor at k1 read before the damage: the same flip on k1;
/// - node 1, START: k1's record stored again at k2's key (decodes, matches its digest, names
///   seq k1).
///
/// Each answer names the damage ("does not match its digest", "names seq"), not "another
/// digest", and a second call to that node hears the same fault. Round 3 (thread
/// 4654331880): the stored and computed digests are only in the local
/// `feed_record_digest_mismatch` line. Neither is in the shared fault: not in the feed replies
/// of nodes 2 and 3, nor in their `node_status` fault. The over-the-links read is
/// `f87_3_a_digest_mismatch_fault_names_no_digest_over_the_links`.
/// Cuts: no digest check in `Host::changes`'s read, red (node 3 served, node 2 diverged); no
/// seq check, red (node 1 served k1 at k2); the digests back in the mismatch's fault detail,
/// red (a digest in the shared fault).
/// Integration (~3 s): a fresh open.
#[cfg(debug_assertions)]
#[config_log::retcd_test]
fn f87_2_r2_decodable_damage_is_damage_not_a_page_or_divergence() {
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let db = opened(&rt, dir.path(), &store);
    let k1 = db.put(b"k1", b"one", None).expect("k1");
    let k2 = db.put(b"k2", b"two", None).expect("k2");
    db.wait_durable(DEFAULT_GROUP, (k2.generation, k2.seq), patience())
        .expect("k2 on every disk");
    let (records, _) = tail(&db, FeedCursor::START, Source::Node(NodeId(2)), k1.seq);
    let good = records[0].at;
    assert_eq!(good.seq, k1.seq, "the cursor at k1");
    let damage = |node: u32, key: Seq, how: rdb_storage::RecordDamage| {
        db.damage_record_as(
            NodeId(node),
            k1.generation,
            Namespace::History,
            &key.0.to_be_bytes(),
            how,
        )
        .expect("damaged");
    };
    damage(3, k1.seq, rdb_storage::RecordDamage::LastByte);
    damage(2, k1.seq, rdb_storage::RecordDamage::LastByte);
    damage(1, k2.seq, rdb_storage::RecordDamage::ValueOf(k1.seq.0));
    let cases = [
        (
            "node 3 from START",
            FeedCursor::START,
            3,
            "does not match its digest",
        ),
        (
            "node 2 from a good cursor",
            good,
            2,
            "does not match its digest",
        ),
        ("node 1 from START", FeedCursor::START, 1, "names seq"),
    ];
    // Every case is judged before the row fails, so a cut shows each case's answer.
    let mut wrong = Vec::new();
    let mut replies = Vec::new();
    for (who, from, node, says) in cases {
        let source = if node == 1 {
            Source::Owner
        } else {
            Source::Node(NodeId(node))
        };
        for call in ["first", "again"] {
            let answer = changes_settled(&db, from, source);
            if let (Err(error), true) = (&answer, node != 1) {
                replies.push(error.detail.clone());
            }
            let ok = matches!(&answer, Err(error) if error.kind == ErrorKind::CorruptHistory
                && error.detail.contains(says)
                && !error.detail.contains("another digest")
                && error.detail.ends_with(FEED_HISTORY_STEP));
            if !ok {
                wrong.push(format!("{who} ({call}): {answer:?}"));
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");

    // Round 3 (thread 4654331880): the digests are in the local trace only, never in the shared
    // fault: not in a put's or a get's answer, nor in node status.
    let mismatches: Vec<_> = config_testkit::logs::lines_for_current_test(
        module_path!(),
        "f87_2_r2_decodable_damage_is_damage_not_a_page_or_divergence",
    )
    .into_iter()
    .filter(|line| line["@m"] == "feed_record_digest_mismatch")
    .collect();
    let mut digests = Vec::new();
    for node in [2_u64, 3] {
        let line = mismatches
            .iter()
            .find(|line| line["node"] == node)
            .unwrap_or_else(|| panic!("node {node} logged the mismatch: {mismatches:#?}"));
        for field in ["stored", "computed"] {
            let digest = line[field].as_str().expect("a digest").to_owned();
            assert_eq!(digest.len(), 64, "node {node} {field}: {digest}");
            digests.push(digest[..8].to_owned());
        }
    }
    let statuses = db.node_status();
    let mut shared: Vec<String> = statuses
        .iter()
        .filter(|status| status.node != NodeId(1))
        .map(|status| status.fault.clone().expect("nodes 2 and 3 faulted"))
        .collect();
    assert_eq!(shared.len(), 2, "{statuses:?}");
    shared.extend(replies);
    for detail in &shared {
        assert!(detail.contains("does not match its digest"), "{detail}");
        for digest in &digests {
            assert!(!detail.contains(digest.as_str()), "{digest} in {detail}");
        }
    }
}

/// Review r2 (threads 4647111507 and 4647111500): the owner's `History` keeps its floor, its
/// head and every other record, but one interior record is gone. A feed read from START meets
/// the hole below the cap: `CORRUPT_HISTORY` naming the seq, with the feed's step once, and the
/// node faults. Then a put and a get hear the fault without the feed's step (it is advice for a
/// cursor), and a later feed call to the faulted node hears it with the step. Both feed
/// answers claim no mutation.
/// Cuts: the hole mapped to a generic fault (`Fault::from`), red (the first feed answer is not
/// `CORRUPT_HISTORY`); `feed_damage` keeping the step in the node's fault, red (the put carries
/// it); `answer_faulted` answering a feed call as any other, red (the later feed call lacks it).
/// Integration (~2 s): a fresh open.
#[cfg(debug_assertions)]
#[config_log::retcd_test]
fn f87_2_r2_a_hole_in_history_faults_the_node_with_the_step_on_feed_replies_only() {
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let db = opened(&rt, dir.path(), &store);
    let k1 = db.put(b"k1", b"one", None).expect("k1");
    let k2 = db.put(b"k2", b"two", None).expect("k2");
    let k3 = db.put(b"k3", b"three", None).expect("k3");
    db.wait_durable(DEFAULT_GROUP, (k3.generation, k3.seq), patience())
        .expect("k3 on every disk");
    let dropped = db
        .drop_history(NodeId(1), k1.generation, k2.seq.0..k2.seq.0 + 1)
        .expect("k2's record dropped");
    assert_eq!(dropped, 1);
    let hole = format!("no record at seq {}", k2.seq.0);
    let feed = |who: &str| {
        let error = db
            .changes(PARTITION, FeedCursor::START, 10, Source::Owner)
            .expect_err("a hole below the cap");
        assert_eq!(error.kind, ErrorKind::CorruptHistory, "{who}: {error:?}");
        assert!(error.detail.contains(&hole), "{who}: {error:?}");
        assert!(
            error.detail.ends_with(FEED_HISTORY_STEP),
            "{who}: {error:?}"
        );
        assert_eq!(error.detail.matches("next:").count(), 1, "{who}: {error:?}");
        assert!(
            error.no_mutation,
            "{who}: a feed call writes nothing: {error:?}"
        );
    };
    feed("the first feed call");
    let put = db.put(b"k4", b"four", None).expect_err("the owner faulted");
    let put = format!("{put:?}");
    assert!(put.contains(&hole), "the put names the fault: {put}");
    assert!(!put.contains(FEED_HISTORY_STEP), "the put: {put}");
    let get = db.get(b"k1").expect_err("the owner faulted");
    assert!(
        get.detail.contains(&hole),
        "the get names the fault: {get:?}"
    );
    assert!(!get.detail.contains(FEED_HISTORY_STEP), "the get: {get:?}");
    feed("a later feed call");
}

/// Review r2 (thread 4647111514): node 2's first `History` key does not parse (the record at
/// k1 also stored under its key's first byte, which sorts before every seq). The floor lookup
/// in `Host::changes` meets it: `CORRUPT_HISTORY` from `RocksEngine::history_floor`'s `Corrupt`,
/// named "history floor", with the feed's step, and node 2 faults (a second call hears the
/// same).
/// Cut: the floor call site mapping to a generic fault (`Fault::from`), red (not
/// `CORRUPT_HISTORY`).
/// Integration (~2 s): a fresh open.
#[cfg(debug_assertions)]
#[config_log::retcd_test]
fn f87_2_r2_a_malformed_first_history_key_faults_the_node_typed() {
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let db = opened(&rt, dir.path(), &store);
    let k1 = db.put(b"k1", b"one", None).expect("k1");
    db.wait_durable(DEFAULT_GROUP, (k1.generation, k1.seq), patience())
        .expect("k1 on every disk");
    db.damage_record_as(
        NodeId(2),
        k1.generation,
        Namespace::History,
        &k1.seq.0.to_be_bytes(),
        rdb_storage::RecordDamage::ShortKey,
    )
    .expect("a short key planted");
    for call in ["first", "again"] {
        let answer = changes_settled(&db, FeedCursor::START, Source::Node(NodeId(2)));
        let error = answer.expect_err("a malformed floor key");
        assert_eq!(error.kind, ErrorKind::CorruptHistory, "{call}: {error:?}");
        assert!(error.detail.contains("history floor"), "{call}: {error:?}");
        assert!(
            error.detail.ends_with(FEED_HISTORY_STEP),
            "{call}: {error:?}"
        );
    }
}

/// Review r4 (blocker; threads 4647111500, 4654680328): a put admitted at the owner and still
/// pending when a feed read faults the owner on damaged history hears the owner-fault step:
/// ask `Db::status` with the same request id, never a new id, and nothing about writing over
/// a damaged entry (`CORRUPT_HISTORY`'s own step), which is the node's damage, not the app's.
/// Nodes 2 and 3 are cut off from node 1, so the put applies and cannot publish; the app hears
/// the fault over the links.
/// Cut: `ApiError::next_step` giving the owner-fault step only to `NotWired` host faults, red
/// ("write over its key").
/// Integration (~6 s): processes, one pending put.
#[cfg(debug_assertions)]
#[config_log::retcd_test]
fn f87_3_a_put_pending_when_feed_damage_faults_the_owner_asks_its_status() {
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let app_dir = config_testkit::fs::temp_dir();
    let procs = procs(&rt, dir.path(), app_dir.path(), &store, fast());
    let owner = &procs.nodes[0];
    let k1 = procs.app.put(b"k1", b"one", None).expect("k1");
    procs
        .app
        .wait_durable(DEFAULT_GROUP, (k1.generation, k1.seq), patience())
        .expect("k1 on every disk");
    procs.links.hold(NodeId(1), NodeId(2));
    procs.links.hold(NodeId(1), NodeId(3));

    let put = std::thread::scope(|scope| {
        let pending = scope.spawn(|| procs.app.put(b"k2", b"two", None));
        let deadline = Instant::now() + crate::host::test_patience(patience());
        while !owner
            .node_status()
            .iter()
            .any(|status| status.node == NodeId(1) && status.in_flight.is_some())
        {
            assert!(Instant::now() < deadline, "k2 never applied at node 1");
            assert!(!pending.is_finished(), "k2 answered before it applied");
            std::thread::sleep(Duration::from_millis(10));
        }
        owner
            .damage_record_as(
                NodeId(1),
                k1.generation,
                Namespace::History,
                &k1.seq.0.to_be_bytes(),
                rdb_storage::RecordDamage::LastByte,
            )
            .expect("damaged");
        let read = owner
            .changes(PARTITION, FeedCursor::START, 10, Source::Owner)
            .expect_err("damaged history faults node 1");
        assert_eq!(read.kind, ErrorKind::CorruptHistory, "{read:?}");
        pending.join().expect("the put thread")
    });
    let refused = put.expect_err("k2 hears node 1's fault");
    assert!(refused.request.is_some(), "k2 was sent: {refused:?}");
    let error = refused.error;
    let text = error.explain();
    assert!(error.is_host_fault(), "{error:?}");
    assert!(!error.no_mutation, "k2 applied, so it may have: {error:?}");
    assert!(text.contains("does not match its digest"), "{text}");
    assert!(
        text.contains("ask `Db::status` with its request id"),
        "{text}"
    );
    assert!(
        text.contains("never send it under a new request id"),
        "{text}"
    );
    assert!(!text.contains("write over"), "{text}");
    drop(procs.nodes);
}

/// Review r3/r4 (threads 4654331877, 4654675984): the debug seams on a faulted node say why,
/// at once, not after a timeout. Node 3 faults on a damaged record its feed read meets; then
/// `drop_history` on node 3 answers its fault ("nothing was dropped", naming the damage), and
/// `damage_record`, whose reply a faulted node drops, says it was dropped unanswered and not
/// that it timed out. Both well inside the read timeout.
/// Cuts: no `Msg::DropHistory` arm in `answer_faulted`, red ("dropped the call unanswered", not
/// the fault); `seam_unanswered` naming a dropped reply a timeout, red ("did not answer within").
/// Integration (~3 s): a fresh open.
#[cfg(debug_assertions)]
#[config_log::retcd_test]
fn f87_3_a_seam_on_a_faulted_node_says_why_not_that_it_timed_out() {
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let db = opened(&rt, dir.path(), &store);
    let k1 = db.put(b"k1", b"one", None).expect("k1");
    db.wait_durable(DEFAULT_GROUP, (k1.generation, k1.seq), patience())
        .expect("k1 on every disk");
    let three = Source::Node(NodeId(3));
    tail(&db, FeedCursor::START, three, k1.seq);
    let key = k1.seq.0.to_be_bytes();
    db.damage_record(NodeId(3), k1.generation, Namespace::History, &key)
        .expect("damaged");
    let read = changes_settled(&db, FeedCursor::START, three).expect_err("node 3 faults");
    assert_eq!(read.kind, ErrorKind::CorruptHistory, "{read:?}");

    let started = Instant::now();
    let dropped = db
        .drop_history(NodeId(3), k1.generation, 0..1)
        .expect_err("a faulted node drops nothing");
    assert!(
        dropped.starts_with("node 3: the node has faulted, so nothing was dropped"),
        "{dropped}"
    );
    assert!(dropped.contains("does not match its digest"), "{dropped}");
    let damaged = db
        .damage_record(NodeId(3), k1.generation, Namespace::History, &key)
        .expect_err("a faulted node damages nothing");
    assert!(
        damaged.starts_with("node 3 dropped the call unanswered"),
        "{damaged}"
    );
    assert!(!damaged.contains("did not answer within"), "{damaged}");
    assert!(
        started.elapsed() < db.timeouts.read,
        "{:?}: waited for a timeout",
        started.elapsed()
    );
}

/// Review r6 (thread 4655953458): a feed call node 2 takes and drops unread, as a thread that
/// stops with the call still queued does, is refused as a stopped node is: `UNAVAILABLE`,
/// `RetryAfterRecovery`, no mutation, ending with the stop step, not a host fault. Node 2 is a
/// threadless handle whose mailbox is read once and dropped.
/// Cut: the dropped reply a plain `ApiError::new` again, red (`BoundedJitter`).
/// Unit (~0.1 s): a `Db` built by hand.
#[config_log::retcd_test]
fn f87_3_a_feed_call_dropped_unanswered_is_a_stopped_node() {
    let rt = runtime();
    let (one, _one_mailbox) = NodeHandle::unanswered(OWNER);
    let (two, two_mailbox) = NodeHandle::unanswered(NodeId(2));
    let db = Db::by_hand(
        vec![one, two],
        store(),
        rt.handle(),
        Timeouts::default(),
        None,
    );
    let dropper = std::thread::spawn(move || {
        let taken = two_mailbox.recv_timeout(Duration::from_secs(5));
        assert!(
            matches!(taken, Ok(Msg::Client(_))),
            "node 2 takes the feed call"
        );
    });
    let error = db
        .changes(PARTITION, FeedCursor::START, 10, Source::Node(NodeId(2)))
        .expect_err("dropped unanswered");
    dropper.join().expect("the dropper");
    assert!(
        error
            .detail
            .contains("node 2 dropped the feed call unanswered"),
        "{error:?}"
    );
    assert!(!error.is_host_fault(), "{error:?}");
    assert_eq!(
        (error.kind, error.retry),
        (ErrorKind::Unavailable, RetryRule::RetryAfterRecovery),
        "{error:?}"
    );
    assert!(error.no_mutation, "{error:?}");
    assert!(
        error.detail.ends_with(crate::db::error::FEED_STOPPED_STEP),
        "{error:?}"
    );
    assert_eq!(error.next_step(), None, "its own step only: {error:?}");
}

/// Review r3/r4/r5 (thread 4654680317): a feed call to a member that cannot take it keeps the
/// cause, and gives a feed reader a step it can act on. Node 2's thread stops while its process
/// stays up. From node 2's own process: "node 2's node thread has stopped", `UNAVAILABLE`,
/// `RetryAfterRecovery`, ending with the stop step (another copy now, or node 2 once it has
/// restarted), not a host fault. From the app, another process, node 2 is unreachable: a retry
/// helps once it is back (`RetryAfterRecovery`). Nothing was read either way.
/// Cuts: the send failure never taken for a local node, red (node 2's own process hears the
/// remote "not reachable" reply, not the stopped thread); the local branch a host fault again,
/// red (`NotWired`, the owner-takeover step).
/// Integration (~5 s): processes, one stopped thread.
#[config_log::retcd_test]
fn f87_3_a_feed_call_to_a_stopped_member_names_why() {
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let app_dir = config_testkit::fs::temp_dir();
    let procs = procs(&rt, dir.path(), app_dir.path(), &store, fast());
    let two = Source::Node(NodeId(2));
    procs.nodes[1]
        .send_to(NodeId(2), Msg::Stop)
        .expect("node 2's host takes the stop");
    let refused = |db: &Db, says: &str| {
        let deadline = Instant::now() + crate::host::test_patience(patience());
        loop {
            let answer = db.changes(PARTITION, FeedCursor::START, 10, two);
            if let Err(error) = &answer {
                if error.detail.contains(says) {
                    return error.clone();
                }
            }
            assert!(Instant::now() < deadline, "never {says:?}: {answer:?}");
            std::thread::sleep(Duration::from_millis(20));
        }
    };
    let local = refused(&procs.nodes[1], "node 2's node thread has stopped");
    assert!(!local.is_host_fault(), "{local:?}");
    assert_eq!(
        (local.kind, local.retry),
        (ErrorKind::Unavailable, RetryRule::RetryAfterRecovery),
        "{local:?}"
    );
    assert!(local.no_mutation, "{local:?}");
    assert!(
        local.detail.ends_with(crate::db::error::FEED_STOPPED_STEP),
        "{local:?}"
    );
    assert_eq!(local.next_step(), None, "its own step only: {local:?}");
    let remote = refused(&procs.app, "node 2 is not reachable from this process");
    assert!(!remote.is_host_fault(), "{remote:?}");
    assert_eq!(
        (remote.kind, remote.retry),
        (ErrorKind::Unavailable, RetryRule::RetryAfterRecovery),
        "{remote:?}"
    );
    assert!(remote.no_mutation, "{remote:?}");
    drop(procs.nodes);
}

/// Review r4 (thread 4654680320): a secondary whose control read of `partitions/1` fails says
/// why in its refusal, bounded, and keeps the refusal's transient rule. Node 2 is cut from
/// rEtcd (`cut-control`); once its last good read is older than the grant, a feed call is
/// refused `UNAVAILABLE`, `BoundedJitter`, no mutation, "call again; the last read failed:"
/// naming the cut: its first 200 characters, and "..." when it is longer, so at most 203.
/// Cut: the refusal without the failure, red (never "the last read failed").
/// Integration (~5 s): processes, one short grant.
#[config_log::retcd_test]
fn f87_3_a_failed_control_read_is_named_in_the_refusal() {
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let app_dir = config_testkit::fs::temp_dir();
    let budgets = short_grant();
    let procs = procs(&rt, dir.path(), app_dir.path(), &store, budgets);
    let k1 = procs.app.put(b"k1", b"one", None).expect("k1");
    procs
        .app
        .wait_durable(DEFAULT_GROUP, (k1.generation, k1.seq), patience())
        .expect("k1 on every disk");
    let two = Source::Node(NodeId(2));
    let (_, at_k1) = tail(&procs.app, FeedCursor::START, two, k1.seq);

    procs.faults[1].apply("cut-control");
    let bound = Duration::from_millis(budgets.grant_millis) + procs.app.timeouts.read;
    let deadline = Instant::now() + crate::host::test_patience(bound);
    let error = loop {
        match procs.app.changes(PARTITION, at_k1, 10, two) {
            Err(error) if error.detail.contains("the last read failed: ") => break error,
            other => assert!(
                Instant::now() < deadline,
                "never named the failed read: {other:?}"
            ),
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(
        (error.kind, error.retry),
        (ErrorKind::Unavailable, RetryRule::BoundedJitter),
        "{error:?}"
    );
    assert!(error.no_mutation, "{error:?}");
    assert!(error.detail.contains("so call again"), "{error:?}");
    let failed = error
        .detail
        .split("the last read failed: ")
        .nth(1)
        .and_then(|rest| rest.split("; next: ").next())
        .expect("the failure");
    assert!(failed.contains("cut-control"), "{failed}");
    assert!(failed.chars().count() <= 203, "{failed}");
    drop(procs.nodes);
}

/// A fresh open whose calls wait `timeouts`.
fn opened_timed(
    rt: &tokio::runtime::Runtime,
    dir: &Path,
    store: &Arc<dyn ConfigStore>,
    timeouts: Timeouts,
) -> Db {
    let db = rt
        .block_on(Db::open_with(
            DbConfig {
                timeouts,
                ..config(dir)
            },
            Arc::clone(store),
            rt.handle().clone(),
            fast(),
        ))
        .expect("the open");
    db.wait_ready(crate::host::test_patience(Duration::from_secs(10)))
        .expect("ready");
    db
}

/// Wait until `done`, or fail naming `what` once `patience` has passed.
fn until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + patience();
    while !done() {
        assert!(Instant::now() < deadline, "never {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// A restart of node 2 held in its WAL join: node 2's next sync is held (`host::hold_sync`), a
/// put makes one, and node 2 is restarted on its own thread, in this row's span. Returns once
/// the restart logged `node_restart_stopping` (it waits on the join from then on), with the
/// put, the restart and the sync's release.
struct HeldRestart {
    put: std::thread::JoinHandle<Result<PutOk, PutError>>,
    restart: std::thread::JoinHandle<Result<BootId, ApiError>>,
    release: mpsc::Sender<()>,
}

fn restart_held_in_join(db: &Arc<Db>, dir: &Path, method: &'static str) -> HeldRestart {
    let (held, release) = crate::host::hold_sync(&dir.join("nodes").join("2"));
    let put = {
        let db = Arc::clone(db);
        std::thread::spawn(move || db.put(b"k2", b"two", None))
    };
    held.recv_timeout(patience())
        .expect("node 2's next WAL sync is held");
    let restart = {
        let db = Arc::clone(db);
        // In this row's span, so its `node_restart_stopping` line is in this row's log.
        let span = tracing::Span::current();
        std::thread::spawn(move || {
            let _span = span.enter();
            db.restart_node(NodeId(2))
        })
    };
    until("node 2's restart stopped it", || {
        config_testkit::logs::lines_for_current_test(module_path!(), method)
            .iter()
            .any(|line| line["@m"] == "node_restart_stopping" && line["node"] == 2)
    });
    HeldRestart {
        put,
        restart,
        release,
    }
}

/// A feed call to node 2 from `START` on its own thread.
fn call_two(db: &Arc<Db>) -> std::thread::JoinHandle<Result<FeedPage, ApiError>> {
    let db = Arc::clone(db);
    std::thread::spawn(move || {
        db.changes(PARTITION, FeedCursor::START, 10, Source::Node(NodeId(2)))
    })
}

/// Review r6/r7 (threads 4655953455, 4657364900): a restart stops and starts its node outside
/// the node lock. While node 2's restart waits in its WAL join ([`restart_held_in_join`]), a
/// feed call sent to node 2 is counted waiting at the restart's guard (`Db::restart_waiters`),
/// a feed read from node 3 in the same `Db` is served, and the restart state reads
/// (`Db::restart_note`, `Db::boot_of`) answer. The join is let go; the restart succeeds, and the
/// waiting call is answered by the restarted node: a page, or a retryable no-mutation refusal
/// of a node still catching up (`BoundedJitter`), after which a retry is served a page. The
/// read timeout is the row's patience, so the call never times out at the guard.
/// Cuts: `Db::nodes_for` waiting on every restart, red (node 3's read waits for the release);
/// `Db::restart_note` taking the settled lock, red (the note waits for the release).
/// Integration (~5 s): one open, one restart.
#[config_log::retcd_test]
fn f87_3_a_restart_held_in_its_wal_join_blocks_no_other_copy() {
    const METHOD: &str = "f87_3_a_restart_held_in_its_wal_join_blocks_no_other_copy";
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let timeouts = Timeouts {
        read: patience(),
        ..Timeouts::default()
    };
    let db = Arc::new(opened_timed(&rt, dir.path(), &store, timeouts));
    let k1 = db.put(b"k1", b"one", None).expect("k1");
    db.wait_durable(
        DEFAULT_GROUP,
        (k1.generation, k1.seq),
        Duration::from_secs(30),
    )
    .expect("k1 on every disk");
    let three = Source::Node(NodeId(3));
    let _ = tail(&db, FeedCursor::START, three, k1.seq);
    let old_boot = db.boot_of(NodeId(2)).expect("node 2 runs");
    let held = restart_held_in_join(&db, dir.path(), METHOD);
    let to_two = call_two(&db);
    until("the call to node 2 waits at the restart's guard", || {
        db.restart_waiters() == 1
    });
    let (read, answered) = mpsc::channel();
    {
        let db = Arc::clone(&db);
        std::thread::spawn(move || {
            let _ = read.send(page_from(&db, FeedCursor::START, three));
        });
    }
    let page = answered
        .recv_timeout(patience())
        .expect("node 3 serves a feed read while node 2's join is held");
    assert!(
        matches!(&page, FeedPage::Records { records, .. } if !records.is_empty()),
        "{page:?}"
    );
    let (noted, note) = mpsc::channel();
    {
        let db = Arc::clone(&db);
        std::thread::spawn(move || {
            let _ = noted.send((db.restart_note(), db.boot_of(NodeId(2))));
        });
    }
    let (_, boot) = note
        .recv_timeout(Duration::from_secs(5))
        .expect("the restart state reads answer while node 2's join is held");
    assert_eq!(
        boot,
        Some(old_boot),
        "mid-restart, node 2 is at its old boot"
    );
    assert!(
        !held.restart.is_finished(),
        "the restart still waits in its join"
    );
    assert_eq!(db.restart_waiters(), 1, "the call to node 2 still waits");
    assert!(
        !to_two.is_finished(),
        "a call to node 2 waits for its restart"
    );
    drop(held.release);
    let boot = held
        .restart
        .join()
        .expect("the restart thread")
        .expect("node 2 restarted");
    assert!(boot > old_boot, "{boot:?} after {old_boot:?}");
    match to_two.join().expect("the node 2 caller") {
        Ok(page) => assert!(matches!(page, FeedPage::Records { .. }), "{page:?}"),
        Err(error) => {
            assert!(
                matches!(error.kind, ErrorKind::Unavailable | ErrorKind::LeaseExpired)
                    && error.retry == RetryRule::BoundedJitter
                    && error.no_mutation,
                "the restarted node's refusal, retryable: {error:?}"
            );
            let page = page_from(&db, FeedCursor::START, Source::Node(NodeId(2)));
            assert!(
                matches!(&page, FeedPage::Records { records, .. } if !records.is_empty()),
                "{page:?}"
            );
        }
    }
    assert_eq!(db.restart_waiters(), 0);
    held.put.join().expect("the put thread").expect("k2");
}

/// Review r7 (thread 4655953455): a restart that has published its new handle clears only its
/// own mark, never the next restart's. Restart A of node 2 is held after it published, before
/// its mark is dropped (`RestartStage::Published`); restart B of node 2 then marks it and is
/// held with the old thread ended (`RestartStage::Stopped`). A is let go and ends: node 2 is
/// still marked by B. Then B is let go and ends. No sleep orders them: each stage is waited
/// for by its hold.
/// Cut: `RestartMark` clearing whatever mark the node holds, red (B's mark cleared by A).
/// Integration (~5 s): one open, two restarts.
#[config_log::retcd_test]
fn f87_3_a_finished_restart_leaves_the_next_restarts_mark() {
    let rt = runtime();
    let store = store();
    let dir = config_testkit::fs::temp_dir();
    let db = Arc::new(opened(&rt, dir.path(), &store));
    let marked = |db: &Db| {
        lock(&db.nodes)
            .iter()
            .find(|handle| handle.node == NodeId(2))
            .and_then(|handle| handle.restarting)
    };
    let restart = |db: &Arc<Db>| {
        let db = Arc::clone(db);
        std::thread::spawn(move || db.restart_node(NodeId(2)))
    };
    let (a_held, a_release) = db.hold_restart(crate::db::RestartStage::Published);
    let a = restart(&db);
    a_held
        .recv_timeout(patience())
        .expect("restart A published its handle");
    assert_eq!(marked(&db), None, "A's new handle carries no mark");
    let (b_held, b_release) = db.hold_restart(crate::db::RestartStage::Stopped);
    let b = restart(&db);
    b_held
        .recv_timeout(patience())
        .expect("restart B stopped node 2");
    let b_mark = marked(&db).expect("B marked node 2");
    drop(a_release);
    let a_boot = a.join().expect("restart A").expect("A restarted node 2");
    assert_eq!(marked(&db), Some(b_mark), "A's end left B's mark");
    drop(b_release);
    let b_boot = b.join().expect("restart B").expect("B restarted node 2");
    assert!(b_boot > a_boot, "{b_boot:?} after {a_boot:?}");
    assert_eq!(marked(&db), None, "B's end cleared its own mark");
}
