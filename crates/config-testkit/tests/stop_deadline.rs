//! `Cluster::stop_node` and `Cluster::shutdown` are bounded (#31), and a socket that never
//! speaks cannot hold a stop at all.
//!
//! On 2026-10-23 one `m4_88` run passed every assertion and then sat in `Cluster::shutdown`
//! for over ten minutes. Node 1 logged `node stopped`; nodes 2 and 3 never began to stop, so the
//! wait was in node 1's plane drain. tonic drains a plane by waiting for every connection it
//! accepted to close, with no timeout. The connection that held it was a peer's dial that
//! landed in node 1's backlog during a restart and never sent its HTTP/2 preface; hyper waits
//! for the preface before it will close. The plaintext listener now hands tonic a socket only
//! once its client has sent something.
//!
//! Cuts:
//! - Replace the `timeout_at` in `StopProgress::step` with a plain `.await`: the two deadline
//!   rows hang in the drain instead of failing.
//! - In `config_grpc::server::spawn_first_bytes`, make the `select!` yield `None` at once
//!   (forward every socket unread): the silent-socket row fails, its `stop_node` panicking
//!   "still waiting on peer plane drain".

use std::sync::Arc;
use std::time::Duration;

use config_core::NodeId;
use config_testkit::cluster::Cluster;
use config_testkit::poll::TestTimers;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Fast timers, so a row that waits out a whole stop deadline costs seconds, not tens of them.
const FAST: TestTimers = TestTimers {
    heartbeat: Duration::from_millis(50),
    election_timeout_min: Duration::from_millis(150),
    election_timeout_max: Duration::from_millis(300),
};

/// The HTTP/2 client preface, then an empty SETTINGS frame.
const PREFACE_AND_SETTINGS: &[u8] =
    b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\x00\x00\x00\x04\x00\x00\x00\x00\x00";

/// Open an HTTP/2 connection to `endpoint`, then go silent.
///
/// It sends the preface and its SETTINGS, so tonic takes the connection, and then never
/// acknowledges anything — in particular not the PING a graceful shutdown waits on, so the
/// connection never closes. Reading the server's own SETTINGS proves tonic accepted it, so a
/// stop that follows cannot race the accept.
async fn stalled_client(cluster: &Cluster, endpoint: &str) -> TcpStream {
    let mut stream = TcpStream::connect(endpoint)
        .await
        .unwrap_or_else(|e| panic!("connect to {endpoint}: {e}"));
    stream
        .write_all(PREFACE_AND_SETTINGS)
        .await
        .unwrap_or_else(|e| panic!("write the preface to {endpoint}: {e}"));
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
    let _held = stalled_client(&cluster, &cluster.client_endpoint(id)).await;
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
    let _held = stalled_client(&cluster, &cluster.client_endpoint(stuck)).await;
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

/// A socket that connects to a node's peer plane and never sends a byte does not hold that
/// node's stop: the listener never hands it to tonic, and shutdown drops it at once.
///
/// This is the #31 hang itself. There a peer's abandoned dial sat in a restarting node's
/// backlog, was accepted when the plane came up, and held the drain forever.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn a_socket_that_never_speaks_does_not_hold_a_stop() {
    const METHOD: &str = "a_socket_that_never_speaks_does_not_hold_a_stop";
    let cluster = Cluster::builder().nodes(1).timers(FAST).start().await;
    let id = NodeId(1);
    let endpoint = cluster.peer_endpoint(id);
    let silent = TcpStream::connect(&endpoint)
        .await
        .unwrap_or_else(|e| panic!("connect to {endpoint}: {e}"));
    let peer = silent
        .local_addr()
        .expect("a connected socket has a local address")
        .to_string();
    let named = |row: &serde_json::Value, message: &str| {
        row.get("@m").and_then(serde_json::Value::as_str) == Some(message)
            && row.get("peer").and_then(serde_json::Value::as_str) == Some(peer.as_str())
    };
    cluster
        .wait_for(
            "the peer plane to accept the silent socket",
            cluster.deadline(4),
            || {
                config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
                    .iter()
                    .any(|row| named(row, "grpc connection waiting for its first bytes"))
                    .then_some(())
            },
        )
        .await
        .unwrap_or_else(|t| panic!("{t}"));

    let started = tokio::time::Instant::now();
    cluster.stop_node(id).await;
    let took = started.elapsed();
    assert!(
        took < config_grpc::tls::DEFAULT_HANDSHAKE_TIMEOUT,
        "stop took {took:?}: the silent socket was held until its first-bytes timeout"
    );

    // The gate task sees the shutdown on its own schedule, so wait for its line.
    let dropped = cluster
        .wait_for(
            "the silent socket to be dropped",
            cluster.deadline(4),
            || {
                let rows: Vec<serde_json::Value> =
                    config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
                        .into_iter()
                        .filter(|row| named(row, "grpc connection dropped before it sent a byte"))
                        .collect();
                (!rows.is_empty()).then_some(rows)
            },
        )
        .await
        .unwrap_or_else(|t| panic!("{t}"));
    assert!(
        dropped.len() == 1
            && dropped[0].get("cause").and_then(serde_json::Value::as_str) == Some("shutdown")
            && dropped[0].get("plane").and_then(serde_json::Value::as_str) == Some("peer"),
        "the silent socket must be dropped once, by the shutdown, on the peer plane: {dropped:?}"
    );
    drop(silent);
    cluster.shutdown().await;
}
