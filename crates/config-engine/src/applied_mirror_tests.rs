//! #186: the engine's cheap reads, and watch registration, while an apply is inside its synced
//! write.
//!
//! In-crate rather than under `tests/`, because a watch registered *during* an apply has to
//! bypass the linearizable barrier of [`ConfigNode::watch`]: on a node whose apply is parked
//! that barrier waits for the very apply the row is holding. [`WatchHub::open`] is the hub's
//! own entry point below the barrier, and it is `pub(crate)`.
//!
//! The apply is held with [`FaultInjector::before_state_write`]: the batch is built, the
//! state-machine lock is held, nothing is on disk.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use config_core::{
    AllowAll, ClusterId, ClusterIdentity, ConfigError, NoGossip, NodeId, Principal, PutRequest,
    RecoveryEpoch, WatchItem, WatchStream,
};
use config_storage::{
    AppliedBatchSink, Boundary, FaultAction, FaultInjector, RocksOptions, RocksStore,
};
use futures::StreamExt;

use crate::watch::WatchHub;
use crate::{
    AdminError, ConfigNode, FormationPlan, InProcTransport, NetFault, NodeConfig, StorageHandle,
};

const PATIENCE: Duration = Duration::from_secs(30);

/// A reader that has not answered in this long waited for the apply's write.
const PROMPT: Duration = Duration::from_secs(1);

/// Holds the first apply after [`ParkWrite::arm`] just before its synced write.
#[derive(Default)]
struct ParkWrite {
    armed: AtomicBool,
    reached: Mutex<Option<mpsc::Sender<()>>>,
    release: Mutex<Option<mpsc::Receiver<()>>>,
}

impl ParkWrite {
    /// Arm the next write; returns (reached, release).
    fn arm(&self) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
        let (reached_tx, reached_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        *self.reached.lock().unwrap() = Some(reached_tx);
        *self.release.lock().unwrap() = Some(release_rx);
        self.armed.store(true, Ordering::SeqCst);
        (reached_rx, release_tx)
    }
}

impl FaultInjector for ParkWrite {
    fn before(&self, _boundary: Boundary) -> FaultAction {
        FaultAction::Proceed
    }

    fn before_state_write(&self) {
        if !self.armed.swap(false, Ordering::SeqCst) {
            return;
        }
        if let Some(reached) = self.reached.lock().unwrap().take() {
            let _ = reached.send(());
        }
        if let Some(release) = self.release.lock().unwrap().take() {
            release
                .recv_timeout(PATIENCE)
                .expect("the test releases the write");
        }
    }
}

/// Run `f` on its own thread and fail unless it answers within [`PROMPT`].
///
/// Left behind on failure rather than joined: joining a reader stuck behind the parked apply
/// would hang the row instead of failing it.
fn promptly<T: Send + 'static>(what: &str, f: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(PROMPT)
        .unwrap_or_else(|_| panic!("{what} waited for the apply's write"))
}

async fn wait_reached(reached: mpsc::Receiver<()>) {
    tokio::task::spawn_blocking(move || reached.recv_timeout(PATIENCE))
        .await
        .expect("wait task joins")
        .expect("the apply reaches its write");
}

/// A formed single-voter node on a RocksStore that `park` can hold.
async fn start_node(dir: &Path, park: Arc<ParkWrite>) -> ConfigNode {
    let identity = ClusterIdentity {
        cluster_id: ClusterId::from_bytes([7u8; 16]),
        recovery_epoch: RecoveryEpoch(0),
        node_id: NodeId(1),
    };
    let watch = WatchHub::with_defaults(config_core::Limits::DEFAULT.watch);
    let store = RocksStore::open_with(
        dir,
        identity,
        config_core::Limits::DEFAULT,
        park,
        tracing::info_span!("store", node_id = 1u64),
        RocksOptions::DEFAULT,
        Arc::clone(&watch) as Arc<dyn AppliedBatchSink>,
    )
    .expect("store opens");
    let endpoint = InProcTransport::endpoint(identity.node_id);
    let cfg = NodeConfig::new(identity, endpoint.clone());
    let timers = cfg.raft;
    let transport = Arc::new(InProcTransport::new(NetFault::new()));
    let node = ConfigNode::start(
        cfg,
        StorageHandle::from(store),
        Arc::clone(&transport) as Arc<dyn crate::PeerTransport>,
        Arc::new(NoGossip),
        Arc::new(AllowAll),
        watch,
    )
    .await
    .expect("node starts");
    transport.register(identity.node_id, node.peer_handler());
    node.form_cluster(FormationPlan::new(
        &identity,
        [(identity.node_id, endpoint)],
    ))
    .await
    .expect("single-node formation");
    node.wait_for_leader(timers.election_timeout() * 8)
        .await
        .expect("a single voter leads");
    let started = std::time::Instant::now();
    while !node.committed_membership().is_formed() {
        assert!(started.elapsed() < PATIENCE, "membership never committed");
        tokio::time::sleep(Duration::from_millis(10)).await; // testkit:allow-sleep
    }
    node
}

fn put(key: &str) -> PutRequest {
    PutRequest {
        key: Bytes::copy_from_slice(key.as_bytes()),
        value: Bytes::from_static(b"v"),
        expected_mod_revision: None,
        dedup: None,
    }
}

/// The next event on `stream`, skipping progress frames.
async fn next_event_revision(stream: &mut WatchStream) -> u64 {
    tokio::time::timeout(PATIENCE, async {
        loop {
            match stream.next().await {
                Some(Ok(WatchItem::Event(event))) => return event.revision,
                Some(Ok(WatchItem::Progress { .. })) => {}
                other => panic!("unexpected watch item: {other:?}"),
            }
        }
    })
    .await
    .expect("a watch event arrives")
}

/// #186 row 6: `committed_membership`, `retired_nodes` and the peer plane's `is_retired` — the
/// last runs on every inbound heartbeat — answer while an apply is inside its write.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn i186_engine_cheap_reads_answer_during_an_apply_write() {
    let dir = tempfile::tempdir().unwrap();
    let park = Arc::new(ParkWrite::default());
    let node = start_node(dir.path(), Arc::clone(&park)).await;
    let principal = Principal::development();

    let (reached, release) = park.arm();
    let writing = {
        let node = node.clone();
        let principal = principal.clone();
        tokio::spawn(async move { node.put(&principal, put("/i186/a")).await })
    };
    wait_reached(reached).await;

    let n = node.clone();
    let membership = promptly("committed_membership", move || n.committed_membership());
    assert!(membership.is_formed());
    let n = node.clone();
    let retired = promptly("retired_nodes", move || n.retired_nodes());
    assert!(retired.is_empty());
    let handler = node.peer_handler();
    assert!(!promptly("is_retired", move || handler.is_retired(NodeId(9))));

    release.send(()).unwrap();
    writing
        .await
        .expect("put task joins")
        .expect("the parked put completes");
    node.stop().await.expect("stop");
}

/// #194: `journal_view` and one pass of the leader's retention tick answer while an apply is
/// inside its write. Both run on Tokio workers; before #194 each took the state-machine lock
/// through `journal_stats` (and the tick through `compact_revision` too), and a leader whose
/// tick waited out its own fsync stalled the worker its heartbeats were sent from.
///
/// The tick runs on its own thread under `block_on`: a pass that blocks on the lock never
/// yields, so a Tokio timeout could not fail it.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn i194_engine_journal_view_and_retention_answer_during_an_apply_write() {
    let dir = tempfile::tempdir().unwrap();
    let park = Arc::new(ParkWrite::default());
    let node = start_node(dir.path(), Arc::clone(&park)).await;
    let principal = Principal::development();
    node.put(&principal, put("/i194/a"))
        .await
        .expect("the seed put applies");
    let before = node.journal_view();
    assert_eq!(before.count, 1, "the seed put is journaled");

    let (reached, release) = park.arm();
    let writing = {
        let node = node.clone();
        let principal = principal.clone();
        tokio::spawn(async move { node.put(&principal, put("/i194/b")).await })
    };
    wait_reached(reached).await;

    let n = node.clone();
    let during = promptly("journal_view", move || n.journal_view());
    assert_eq!(
        during, before,
        "the parked batch is not on disk, so the journal view must not count it"
    );
    let n = node.clone();
    promptly("evaluate_retention", move || {
        futures::executor::block_on(n.evaluate_retention_once());
    });

    release.send(()).unwrap();
    writing
        .await
        .expect("put task joins")
        .expect("the parked put completes");
    assert_eq!(
        node.journal_view().count,
        2,
        "the released batch is counted"
    );
    node.stop().await.expect("stop");
}

/// #192 review blocker: a same-id `add_learner` issued while the `RetireNode` apply for that
/// id is parked inside its write is refused as `Retired`, never proposed.
///
/// Node ids are never reused: that is a safety rule, so the readmission check must not read
/// the mirror, which still says "not retired" until the write lands. With the check on the
/// mirror (1d0ad90) the learner was proposed and the call answered `Ok`.
///
/// The add is given 300 ms to reach its check before the write is released. On a host so
/// slow that it reaches the check only after the release, the row passes on either code; it
/// can be falsely green, never falsely red.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn i192_add_learner_during_a_parked_retire_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let park = Arc::new(ParkWrite::default());
    let node = start_node(dir.path(), Arc::clone(&park)).await;

    // Id 9 was never a member, so `remove_member` goes straight to step 3, `RetireNode`.
    let (reached, release) = park.arm();
    let retiring = {
        let node = node.clone();
        tokio::spawn(async move { node.remove_member(NodeId(9)).await })
    };
    wait_reached(reached).await;
    assert!(
        !node.retired_nodes().contains(&NodeId(9)),
        "the mirror does not yet show the parked retirement"
    );

    let adding = {
        let node = node.clone();
        tokio::spawn(async move {
            node.add_learner(
                NodeId(9),
                "inproc://9".to_string(),
                "inproc://9-client".to_string(),
            )
            .await
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await; // testkit:allow-sleep
    release.send(()).unwrap();

    retiring
        .await
        .expect("retire task joins")
        .expect("the retirement completes");
    let added = tokio::time::timeout(PATIENCE, adding)
        .await
        .expect("add_learner answers")
        .expect("add task joins");
    assert!(
        matches!(added, Err(AdminError::Retired { node_id }) if node_id == NodeId(9)),
        "a same-id add_learner during the parked retirement must be refused, got {added:?}"
    );
    let report = node.membership_report();
    assert!(
        !report.learners.contains(&NodeId(9)) && !report.membership.voters.contains(&NodeId(9)),
        "the retired id must not have re-entered membership"
    );
    node.stop().await.expect("stop");
}

/// #186 row 3: a watch registered while an apply is parked before its write reads the
/// pre-batch revision as its high-water mark, and after the release gets the batch's events
/// exactly once.
///
/// Why it matters: registration replays `(R, H]` from disk and drops live events `<= H`. If
/// `H` could be read ahead of the disk, the batch would be in neither half.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn i186_watch_registered_during_an_apply_write_gets_the_batch_once() {
    let dir = tempfile::tempdir().unwrap();
    let park = Arc::new(ParkWrite::default());
    let node = start_node(dir.path(), Arc::clone(&park)).await;
    let principal = Principal::development();
    let hub = Arc::clone(node.watch_hub());

    let before = node
        .put(&principal, put("/i186/seed"))
        .await
        .expect("seed put")
        .revision;

    let (reached, release) = park.arm();
    let writing = {
        let node = node.clone();
        let principal = principal.clone();
        tokio::spawn(async move { node.put(&principal, put("/i186/parked")).await })
    };
    wait_reached(reached).await;

    // The parked batch's revision is not yet on disk, so it is above the high-water mark.
    let refused = tokio::time::timeout(
        PROMPT,
        hub.open(
            &principal,
            Bytes::from_static(b"/i186/"),
            before + 1,
            Duration::from_secs(3600),
        ),
    )
    .await
    .expect("registration does not wait for the apply's write");
    match refused {
        Err(ConfigError::InvalidArgument { detail }) => assert!(
            detail.contains(&format!("applied revision {before}")),
            "high water must be the pre-batch revision {before}: {detail}"
        ),
        Err(other) => panic!("expected InvalidArgument, got {other:?}"),
        Ok(_) => panic!(
            "high water ran ahead of the disk: a cursor at {} was accepted",
            before + 1
        ),
    }

    let mut stream = tokio::time::timeout(
        PROMPT,
        hub.open(
            &principal,
            Bytes::from_static(b"/i186/"),
            before,
            Duration::from_secs(3600),
        ),
    )
    .await
    .expect("registration does not wait for the apply's write")
    .expect("a cursor at the pre-batch revision registers");

    release.send(()).unwrap();
    let parked = writing
        .await
        .expect("put task joins")
        .expect("the parked put completes")
        .revision;
    assert_eq!(parked, before + 1);
    assert_eq!(next_event_revision(&mut stream).await, parked);

    // Exactly once: the next event after it is the next write, not a second copy.
    let next = node
        .put(&principal, put("/i186/after"))
        .await
        .expect("follow-up put")
        .revision;
    assert_eq!(next_event_revision(&mut stream).await, next);

    drop(stream);
    node.stop().await.expect("stop");
}
