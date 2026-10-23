//! `Cluster::stop_node` and `Cluster::shutdown` are bounded (#31).
//!
//! On 2026-10-23 one `m4_88` run passed every assertion and then sat in `Cluster::shutdown`
//! for over ten minutes. Node 1 logged `node stopped`; nodes 2 and 3 never began to stop, so the
//! wait was in node 1's plane drain. tonic drains a plane by waiting for every connection it
//! accepted to close, with no timeout, and a connection that never finishes its HTTP/2
//! handshake never closes. These rows hold one such connection open on purpose.
//!
//! Cut: replace the `timeout_at` in `StopProgress::step` with a plain `.await` — both rows hang
//! in the drain instead of failing.

use std::sync::Arc;
use std::time::Duration;

use config_core::NodeId;
use config_testkit::cluster::Cluster;
use config_testkit::poll::TestTimers;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

/// Fast timers, so a row that waits out a whole stop deadline costs seconds, not tens of them.
const FAST: TestTimers = TestTimers {
    heartbeat: Duration::from_millis(50),
    election_timeout_min: Duration::from_millis(150),
    election_timeout_max: Duration::from_millis(300),
};

/// Connect to `endpoint` and say nothing.
///
/// The server speaks first: an HTTP/2 server sends its SETTINGS before it reads the client's
/// preface. Reading those bytes proves tonic accepted this connection and is waiting on it,
/// so a stop that follows cannot race the accept.
async fn silent_client(cluster: &Cluster, endpoint: &str) -> TcpStream {
    let mut stream = TcpStream::connect(endpoint)
        .await
        .unwrap_or_else(|e| panic!("connect to {endpoint}: {e}"));
    let mut first = [0u8; 9];
    let read = tokio::time::timeout(cluster.deadline(1), stream.read(&mut first))
        .await
        .unwrap_or_else(|_| panic!("{endpoint} sent nothing; tonic never accepted the socket"))
        .unwrap_or_else(|e| panic!("read from {endpoint}: {e}"));
    assert!(
        read > 0,
        "{endpoint} closed the socket instead of greeting it"
    );
    stream
}

fn panic_text(e: tokio::task::JoinError) -> String {
    let payload = e.into_panic();
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_owned()))
        .unwrap_or_else(|| "<non-string panic>".to_owned())
}

/// A node whose client plane cannot drain fails `stop_node` within the stop deadline, and the
/// failure names the node, its last lifecycle event and the step it is waiting on.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_node_fails_within_its_deadline_naming_the_stuck_node() {
    let cluster = Arc::new(Cluster::builder().nodes(1).timers(FAST).start().await);
    let id = NodeId(1);
    let _held = silent_client(&cluster, &cluster.client_endpoint(id)).await;
    let bound = cluster.stop_deadline();

    let started = tokio::time::Instant::now();
    let stopping = Arc::clone(&cluster);
    let outcome = tokio::spawn(async move { stopping.stop_node(id).await }).await;
    let took = started.elapsed();

    let text =
        panic_text(outcome.expect_err("stop_node returned while a connection held the drain"));
    assert!(
        text.contains("node 1 did not stop")
            && text.contains("last lifecycle event `node stopped`")
            && text.contains("still waiting on client plane drain"),
        "the failure must name the node, its last event and the stuck step: {text}"
    );
    assert!(
        took >= bound && took < bound + cluster.deadline(1),
        "stop_node failed after {took:?}; its deadline is {bound:?}"
    );
}

/// `Cluster::shutdown` with one stuck node still stops the others, then fails naming only the
/// stuck one, within one stop deadline per node.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_fails_within_its_deadline_and_stops_the_other_nodes() {
    let cluster = Cluster::builder().nodes(3).timers(FAST).start().await;
    cluster
        .wait_for_leader(cluster.deadline(10))
        .await
        .expect("a leader elects");
    let stuck = NodeId(2);
    let _held = silent_client(&cluster, &cluster.client_endpoint(stuck)).await;
    let bound = cluster.stop_deadline() * 3;
    let slack = cluster.deadline(1);

    let started = tokio::time::Instant::now();
    let outcome = tokio::spawn(cluster.shutdown()).await;
    let took = started.elapsed();

    let text =
        panic_text(outcome.expect_err("shutdown returned while a connection held node 2's drain"));
    assert!(
        text.contains("1 node(s) did not stop")
            && text.contains("node 2 did not stop")
            && text.contains("still waiting on client plane drain")
            && text.contains("Stopped: [NodeId(1), NodeId(3)]"),
        "shutdown must name node 2 alone and report nodes 1 and 3 stopped: {text}"
    );
    assert!(
        took < bound + slack,
        "shutdown failed after {took:?}; its bound is {bound:?}"
    );
}
