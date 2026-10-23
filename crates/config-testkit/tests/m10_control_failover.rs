//! M10 D14: a client of a healthy rEtcd must not fail because the first endpoint it was given
//! is down (ruling A-R199).
//!
//! An rDB open reads its partition record through `config_client::GrpcClient`. Two of three
//! voters are a working cluster, so a control read with node 1 stopped has an answer to get.

use config_core::{ConfigError, ConfigStore, GetRequest, NodeId};
use config_testkit::cluster::{Cluster, StorageKind};

/// D14: node 1, the first configured endpoint, is stopped; nodes 2 and 3 elect a leader. A
/// strict `get` through a client over all three endpoints answers from that leader.
/// Red before the fix: the client dials only the first endpoint, so the read is `Unavailable`
/// although the cluster is healthy.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m10_d14_a_control_read_succeeds_with_the_first_endpoint_down() {
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
    tracing::info!(leader = leader.0, "d14_leader_without_node_1");

    let client = cluster.grpc_client_multi();
    let read = client
        .get(GetRequest {
            key: bytes::Bytes::from_static(b"/m10-d14"),
        })
        .await;
    tracing::info!(ok = read.is_ok(), stats = ?client.stats(), "d14_read");
    if let Err(error) = &read {
        tracing::warn!(%error, "d14_read_failed");
    }
    assert!(
        read.is_ok(),
        "a healthy two-voter cluster must answer a read: {read:?}, {:?}",
        client.stats()
    );

    // F4: the next read starts at the endpoint that answered, so it does not dial node 1.
    let failed_dials = client.stats().reconnects;
    client
        .get(GetRequest {
            key: bytes::Bytes::from_static(b"/m10-d14"),
        })
        .await
        .expect("the second read answers");
    assert_eq!(
        client.stats().reconnects,
        failed_dials,
        "the second read dialled the stopped node again: {:?}",
        client.stats()
    );
    cluster.shutdown().await;
}

/// D14 F5: the client already holds a channel to node 1 when node 1 stops. The next operation
/// goes down that channel, so it fails after submission: a put is an unknown outcome or
/// `Unavailable`, never moved to another node (ADR-0015). The operation after it dials afresh,
/// moves on from node 1, and answers.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m10_d14_a_node_that_stops_under_a_cached_channel_fails_one_call_then_moves_on() {
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    cluster.leader().await;
    let first = NodeId(1);
    let client = cluster.grpc_client_multi();
    let key = || bytes::Bytes::from_static(b"/m10-d14-f5");
    client
        .get(GetRequest { key: key() })
        .await
        .expect("a read with every node up");
    assert_eq!(client.stats().reconnects, 0, "{:?}", client.stats());

    cluster.stop_node(first).await;
    cluster
        .wait_for("a leader among nodes 2 and 3", cluster.deadline(10), || {
            cluster.leader_now().filter(|leader| *leader != first)
        })
        .await
        .unwrap_or_else(|e| panic!("nodes 2 and 3 never elected a leader: {e}"));

    let sends = client.stats().sends;
    let stale = client
        .put(config_core::PutRequest {
            dedup: None,
            key: key(),
            value: bytes::Bytes::from_static(b"v"),
            expected_mod_revision: None,
        })
        .await;
    tracing::info!(stale = ?stale, stats = ?client.stats(), "d14_f5_stale_channel");
    assert!(
        matches!(
            stale,
            Err(ConfigError::DeadlineExceededUnknownOutcome | ConfigError::Unavailable { .. })
        ),
        "a put down a dead channel is unknown or unavailable, never moved: {stale:?}"
    );
    assert_eq!(
        client.stats().sends,
        sends + 1,
        "sent once down the cached channel and not again: {:?}",
        client.stats()
    );

    let read = client.get(GetRequest { key: key() }).await;
    tracing::info!(ok = read.is_ok(), stats = ?client.stats(), "d14_f5_next_read");
    assert!(
        read.is_ok(),
        "the next call moves on from node 1: {read:?}, {:?}",
        client.stats()
    );
    cluster.shutdown().await;
}
