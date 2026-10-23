//! Issue #57: `ConfigStore::watch_leader` opens a watch on the leader, wherever it is.
//!
//! rEtcd serves watches from the leader only (spec §11.1), and `watch` makes one attempt at
//! one endpoint (M4-106, M4-107, M4-108). So a control watch opened through `watch` worked
//! only while the first endpoint was the leader. `watch_leader` finds the leader at open the
//! way a unary call does: it moves past an endpoint it cannot connect to and follows a bounded
//! number of `NotLeader` hints. Once the stream is open it is the caller's, exactly as with
//! `watch`: nothing re-opens it.

mod support;

use std::time::{Duration, Instant};

use bytes::Bytes;
use config_client::{GrpcClient, GrpcClientOptions};
use config_core::{
    ConfigError, ConfigStore, GetRequest, LeaderHint, NodeId, PutRequest, WatchItem, WatchRequest,
};
use config_log::retcd_test;
use config_testkit::cluster::{Cluster, StorageKind};
use config_testkit::MemStore;
use futures::StreamExt;

use support::{start_nodes, start_spy, Node};

fn options() -> GrpcClientOptions {
    GrpcClientOptions {
        request_deadline: Duration::from_secs(5),
        ..Default::default()
    }
}

fn client(endpoints: Vec<String>) -> GrpcClient {
    GrpcClient::connect(endpoints, options()).expect("client connects")
}

fn endpoints(nodes: &[Node]) -> Vec<String> {
    nodes.iter().map(|n| n.endpoint.clone()).collect()
}

fn watch_request(prefix: &'static [u8], start_after_revision: u64) -> WatchRequest {
    WatchRequest {
        prefix: Bytes::from_static(prefix),
        start_after_revision,
        progress_interval: None,
    }
}

/// An address nothing listens on: bound, read and released.
async fn dead_endpoint() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    drop(listener);
    addr.to_string()
}

/// #57: the first endpoint answers `NotLeader` with a hint; the watch opens at the hinted
/// node. One call is one open, and draining the stream to its end re-opens nothing.
/// Cut: `watch_leader` calls `watch` — the hint comes back as `NotLeader`.
#[retcd_test]
async fn m10_57_watch_leader_follows_a_hint_at_open() {
    let nodes = start_nodes(2).await;
    nodes[0].store.set_error(Some(ConfigError::NotLeader {
        hint: Some(LeaderHint {
            node_id: NodeId(2),
            endpoint: nodes[1].endpoint.clone(),
        }),
    }));
    let client = client(endpoints(&nodes));

    let mut stream = client
        .watch_leader(watch_request(b"/app/", 7))
        .await
        .unwrap_or_else(|e| panic!("the watch opens at the hinted leader: {e}"));
    // The scripted store delivers nothing and ends. What ends a stream is the caller's to act
    // on; the client must not open another one behind it.
    assert!(stream.next().await.is_none(), "the scripted stream ends");

    let stats = client.stats();
    assert_eq!(
        (stats.watch_opens, stats.hint_follows, stats.sends),
        (1, 1, 2),
        "one call, one followed hint, one send per node: {stats:?}"
    );
    assert_eq!(
        (nodes[0].store.call_count(), nodes[1].store.call_count()),
        (1, 1),
        "each node saw exactly one open"
    );
}

/// #57: the first endpoint is down; the watch opens at the next one.
/// Cut: `watch_leader` calls `watch` — the dead endpoint is `Unavailable`.
#[retcd_test]
async fn m10_57_watch_leader_moves_past_a_dead_first_endpoint() {
    let dead = dead_endpoint().await;
    let live = Node::start().await;
    let client = client(vec![dead, live.endpoint.clone()]);

    let mut stream = client
        .watch_leader(watch_request(b"/app/", 0))
        .await
        .unwrap_or_else(|e| panic!("the watch opens at the live endpoint: {e}"));
    assert!(stream.next().await.is_none(), "the scripted stream ends");

    let stats = client.stats();
    assert_eq!(
        (stats.watch_opens, stats.reconnects, stats.sends),
        (1, 1, 1),
        "one failed dial, then one open at the live endpoint: {stats:?}"
    );
    assert_eq!(live.store.call_count(), 1);
}

/// #57: a server's refusal that is not a hint ends the call as it is, after one attempt.
/// `RevisionCompacted` is the caller's to recover from (spec §11.2); retrying it elsewhere
/// would only get the same answer.
#[retcd_test]
async fn m10_57_watch_leader_returns_a_refusal_without_retrying() {
    let nodes = start_nodes(2).await;
    nodes[0]
        .store
        .set_error(Some(ConfigError::RevisionCompacted {
            minimum_available_revision: 41,
        }));
    let client = client(endpoints(&nodes));

    let got = client.watch_leader(watch_request(b"/app/", 7)).await.err();
    assert_eq!(
        got,
        Some(ConfigError::RevisionCompacted {
            minimum_available_revision: 41
        })
    );
    let stats = client.stats();
    assert_eq!((stats.watch_opens, stats.sends), (1, 1), "{stats:?}");
    assert_eq!(nodes[1].store.call_count(), 0, "nothing moved to node 2");
}

/// #57: a watch carries no `grpc-timeout`, so the server never ends an idle stream on the
/// open's budget. The budget still bounds the open, on the client side.
/// Cut: send the remaining budget on a watch too — the spy sees a deadline.
#[retcd_test]
async fn m10_57_watch_leader_sends_no_deadline() {
    let server = start_spy().await;
    server.spy.answer_with(ConfigError::RevisionCompacted {
        minimum_available_revision: 3,
    });
    let client = client(vec![server.endpoint.clone()]);

    let got = client.watch_leader(watch_request(b"/app/", 1)).await.err();
    assert!(
        matches!(got, Some(ConfigError::RevisionCompacted { .. })),
        "the spy's answer comes back: {got:?}"
    );
    assert_eq!(
        server.spy.timeouts(),
        Vec::<String>::new(),
        "a watch must not carry a grpc-timeout"
    );
}

/// #57: a store that does not override `watch_leader` serves it as `watch` (the trait default).
/// Cut: a default that refuses — the two answers differ.
#[retcd_test]
async fn m10_57_the_default_watch_leader_is_watch() {
    let store = MemStore::new();
    let plain = store.watch(watch_request(b"/app/", 0)).await.err();
    let leader = store.watch_leader(watch_request(b"/app/", 0)).await.err();
    assert!(plain.is_some(), "MemStore serves no watches");
    assert_eq!(leader, plain);
}

/// Wait for the first event on `stream`.
async fn first_event(
    stream: &mut config_core::WatchStream,
    deadline: Duration,
) -> config_core::MutationEvent {
    tokio::time::timeout(deadline, async {
        loop {
            match stream.next().await {
                Some(Ok(WatchItem::Event(event))) => return event,
                Some(Ok(WatchItem::Progress { .. })) => continue,
                other => panic!("the watch ended before an event arrived: {other:?}"),
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no event within {deadline:?}"))
}

/// Write `key` through the cluster and return its revision.
async fn put(cluster: &Cluster, leader: NodeId, key: &'static [u8]) -> u64 {
    cluster
        .client(leader)
        .put(PutRequest {
            dedup: None,
            key: Bytes::from_static(key),
            value: Bytes::from_static(b"v"),
            expected_mod_revision: None,
        })
        .await
        .unwrap_or_else(|e| panic!("put through the leader: {e}"))
        .revision
}

/// #57, the issue's own row: node 1, the first configured endpoint, is stopped. A watch opened
/// through a client over all three endpoints reaches the leader among nodes 2 and 3, and a
/// change to a watched key arrives on it.
/// Cut: `watch_leader` calls `watch` — the open is `Unavailable` (node 1 is down).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m10_57_a_watch_opened_with_the_first_endpoint_down_delivers() {
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    cluster.leader().await;
    let first = NodeId(1);
    assert_eq!(
        cluster.client_endpoints()[0],
        cluster.client_endpoint(first),
        "node 1 is the first configured endpoint"
    );
    cluster.stop_node(first).await;
    let leader = cluster
        .wait_for("a leader among nodes 2 and 3", cluster.deadline(10), || {
            cluster.leader_now().filter(|leader| *leader != first)
        })
        .await
        .unwrap_or_else(|e| panic!("nodes 2 and 3 never elected a leader: {e}"));

    // The cursor is read on the leader directly, so the watch is the client's first call and
    // meets the stopped endpoint itself.
    let from = cluster
        .client(leader)
        .get(GetRequest {
            key: Bytes::from_static(b"/m10-57/a"),
        })
        .await
        .unwrap_or_else(|e| panic!("a read on the leader: {e}"))
        .read_revision;
    let client = cluster.grpc_client_multi();
    let mut stream = client
        .watch_leader(watch_request(b"/m10-57/", from))
        .await
        .unwrap_or_else(|e| panic!("the watch opens at the leader: {e} {:?}", client.stats()));

    let written = Instant::now();
    let revision = put(&cluster, leader, b"/m10-57/a").await;
    let event = first_event(&mut stream, cluster.deadline(5)).await;
    let took = written.elapsed();
    tracing::info!(
        leader = leader.0,
        took_ms = u64::try_from(took.as_millis()).unwrap_or(u64::MAX),
        stats = ?client.stats(),
        "m10_57_event_with_first_endpoint_down"
    );
    assert_eq!(
        (event.key.as_ref(), event.revision),
        (&b"/m10-57/a"[..], revision)
    );
    let stats = client.stats();
    assert_eq!(
        (stats.watch_opens, stats.reconnects),
        (1, 1),
        "one open, which moved past the stopped node once: {stats:?}"
    );

    cluster.shutdown().await;
}

/// #57: every node is up and the client's first endpoint is a follower. The watch follows the
/// follower's hint to the leader and delivers.
/// Cut: `watch_leader` calls `watch` — the open is `NotLeader`.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m10_57_a_watch_opened_at_a_follower_delivers() {
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let leader = cluster.leader().await;
    let follower = cluster
        .followers()
        .into_iter()
        .next()
        .expect("a three-node cluster has a follower");

    let client = cluster.grpc_client(follower);
    let mut stream = client
        .watch_leader(watch_request(b"/m10-57/", 0))
        .await
        .unwrap_or_else(|e| panic!("the watch opens at the leader: {e} {:?}", client.stats()));

    let written = Instant::now();
    let revision = put(&cluster, leader, b"/m10-57/b").await;
    let event = first_event(&mut stream, cluster.deadline(5)).await;
    let took = written.elapsed();
    tracing::info!(
        leader = leader.0,
        follower = follower.0,
        took_ms = u64::try_from(took.as_millis()).unwrap_or(u64::MAX),
        stats = ?client.stats(),
        "m10_57_event_from_a_follower_start"
    );
    assert_eq!(
        (event.key.as_ref(), event.revision),
        (&b"/m10-57/b"[..], revision)
    );
    let stats = client.stats();
    assert_eq!(
        (stats.watch_opens, stats.hint_follows),
        (1, 1),
        "one open, one followed hint: {stats:?}"
    );

    cluster.shutdown().await;
}
