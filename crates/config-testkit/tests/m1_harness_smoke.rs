//! Smoke tests for the [`Cluster`] harness itself.
//!
//! The M1 rows assume the harness is honest: that formation really went over gRPC, that
//! `partition`/`heal` really move the switchboard the transport consults, and that `shutdown`
//! really frees the sockets. A harness bug in any of those would make a row pass for the wrong
//! reason, so each is asserted here directly rather than inferred.

use std::time::Duration;

use bytes::Bytes;
use config_core::{MutationOutcome, NodeId, PutRequest};
use config_testkit::cluster::{Cluster, ClusterConfig, StorageKind};
use config_testkit::poll::poll_until;

fn get_req(k: &str) -> config_core::GetRequest {
    config_core::GetRequest {
        key: Bytes::copy_from_slice(k.as_bytes()),
    }
}

fn put_req(k: &str, v: &str) -> PutRequest {
    PutRequest {
        key: Bytes::copy_from_slice(k.as_bytes()),
        value: Bytes::copy_from_slice(v.as_bytes()),
        expected_mod_revision: None,
    }
}

/// A cluster forms over the real gRPC peer plane, and the real gRPC client plane serves the
/// same data the embedded client wrote.
///
/// The gRPC half is what distinguishes this harness from the engine's in-process one: a write
/// through `DirectClient` on the leader must be readable through a `GrpcClient` that starts at
/// a *follower* and follows the leader hint (ADR-0009).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn harness_forms_over_real_grpc_and_serves_both_client_kinds() {
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let leader = cluster.leader().await;
    let follower = cluster.followers()[0];

    // Peer endpoints are real bound sockets, not placeholders, and committed membership names
    // exactly them — which is what makes a NotLeader hint dialable.
    let membership = cluster.membership();
    for id in cluster.ids() {
        assert_eq!(
            membership.endpoint_of(id),
            Some(cluster.peer_endpoint(id).as_str()),
            "committed endpoint for {id} is not the address it listens on"
        );
        assert_ne!(
            cluster.peer_endpoint(id),
            cluster.client_endpoint(id),
            "node {id} serves both planes on one port"
        );
    }

    let written = cluster
        .client(leader)
        .put(put_req("/smoke", "v1"))
        .await
        .expect("embedded write on the leader");
    assert_eq!(written.outcome, MutationOutcome::Applied);

    let at_leader = cluster.grpc_client_at_leader().await;
    assert_eq!(
        at_leader.pinned_endpoint(),
        cluster.client_endpoint(leader),
        "the client was not pinned where it was asked to be"
    );
    let got = config_core::ConfigStore::get(&at_leader, get_req("/smoke"))
        .await
        .expect("gRPC read on the leader's client plane");
    assert_eq!(
        got.record.as_ref().map(|r| r.value.clone()),
        Some(Bytes::copy_from_slice(b"v1")),
        "the gRPC plane served different data than the embedded client wrote"
    );

    // A client pinned to a follower is answered `NotLeader` carrying a hint that names the
    // leader's **client** endpoint — the address the refused client can actually dial. The
    // peer endpoint of the same node is still in committed membership, and the two must not be
    // the same address, or the assertion would pass for the wrong reason.
    let at_follower = cluster.grpc_client(follower);
    let hint = cluster
        .node(follower)
        .leader_hint()
        .expect("a follower knows who the leader is");
    assert_eq!(hint.node_id, leader, "the hint names the wrong node");
    assert_eq!(
        hint.endpoint,
        cluster.client_endpoint(leader),
        "the hint must name the client plane, which is what a client can dial (ADR-0009)"
    );
    assert_eq!(
        membership.endpoint_of(leader),
        Some(cluster.peer_endpoint(leader).as_str()),
        "committed membership must still carry the peer endpoint"
    );
    assert_ne!(
        hint.endpoint,
        cluster.peer_endpoint(leader),
        "the two planes share a port, so this test cannot tell them apart"
    );

    // And the hint is followable: the same pinned-to-a-follower client reaches the leader.
    let got_via_hint = config_core::ConfigStore::get(&at_follower, get_req("/smoke"))
        .await
        .expect("a follower-pinned client reaches the leader by following the hint");
    assert_eq!(
        got_via_hint.record.map(|r| r.value),
        Some(Bytes::copy_from_slice(b"v1"))
    );
    assert!(
        at_follower.stats().hint_follows >= 1,
        "the read succeeded without following a hint: {:?}",
        at_follower.stats()
    );

    cluster.shutdown().await;
}

/// `partition`, `isolate`, and `heal` move the very `NetFault` the peer transport consults.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn harness_partition_and_heal_drive_the_shared_netfault() {
    let cluster = Cluster::start_with(ClusterConfig {
        nodes: 3,
        form: false,
        ..ClusterConfig::default()
    })
    .await;
    let net = cluster.netfault();
    let (a, b, c) = (NodeId(1), NodeId(2), NodeId(3));

    assert!(!net.is_blocked(a, b), "a fresh cluster starts partitioned");

    cluster.partition(a, b);
    assert!(
        net.is_blocked(a, b) && net.is_blocked(b, a),
        "not symmetric"
    );
    assert!(!net.is_blocked(a, c), "an unrelated pair was blocked");

    cluster.heal();
    assert!(!net.is_blocked(a, b), "heal left a block behind");

    cluster.partition_one_way(a, c);
    assert!(
        net.is_blocked(a, c) && !net.is_blocked(c, a),
        "one-way block is not one-way"
    );
    cluster.heal();

    cluster.isolate(b);
    assert!(net.is_blocked(b, a) && net.is_blocked(a, b), "b<->a open");
    assert!(net.is_blocked(b, c) && net.is_blocked(c, b), "b<->c open");
    assert!(!net.is_blocked(a, c), "isolate blocked an unrelated pair");

    cluster.heal();
    assert!(!net.is_blocked(b, a) && !net.is_blocked(b, c));

    cluster.shutdown().await;
}

/// `shutdown` frees every listener, so the next test in the same process can bind.
///
/// Dropping the handles only *signals* a shutdown; awaiting it is what drains the servers.
/// The proof is a bind on the exact addresses the cluster used, polled rather than slept on.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn harness_shutdown_releases_every_port() {
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    cluster.leader().await;

    let addrs: Vec<String> = cluster
        .ids()
        .into_iter()
        .flat_map(|id| [cluster.peer_endpoint(id), cluster.client_endpoint(id)])
        .collect();
    assert_eq!(addrs.len(), 6, "a 3-node cluster binds six listeners");

    cluster.shutdown().await;

    for addr in &addrs {
        let parsed: std::net::SocketAddr = addr.parse().expect("harness addresses are literal");
        let bound = poll_until(Duration::from_secs(5), Duration::from_millis(20), || {
            std::net::TcpListener::bind(parsed).ok()
        })
        .await;
        assert!(
            bound.is_ok(),
            "port {addr} was still held after shutdown: {:?}",
            bound.err()
        );
    }
}

/// Real gossip: every node runs a `GossipNode`, and each one eventually observes the others.
///
/// The M1 gossip rows (M1-28..35) all start from "gossip is actually running", so a break in
/// this wiring would make every one of them vacuous. Injection is exercised too, because the
/// poison rows drive the same seam.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn harness_real_gossip_observes_peers_and_accepts_injection() {
    let cluster = Cluster::start_with(ClusterConfig {
        nodes: 3,
        gossip: config_testkit::cluster::GossipKind::Real,
        ..ClusterConfig::default()
    })
    .await;
    cluster.leader().await;

    // Advisory, so it converges on its own schedule: poll rather than assume.
    let observed = cluster
        .wait_for(
            "every node to observe the other two over real gossip",
            cluster.deadline(20),
            || {
                cluster
                    .ids()
                    .iter()
                    .all(|id| {
                        cluster
                            .gossip_peers(*id)
                            .iter()
                            .filter(|h| h.node_id != *id)
                            .count()
                            >= 2
                    })
                    .then_some(())
            },
        )
        .await;
    assert!(
        observed.is_ok(),
        "real gossip never converged: {:?}",
        observed.err()
    );

    for id in cluster.ids() {
        for hint in cluster.gossip_peers(id) {
            assert_eq!(
                hint.peer_endpoint,
                cluster.peer_endpoint(hint.node_id),
                "node {id} observed a wrong peer endpoint for {}",
                hint.node_id
            );
        }
    }

    // The injection seam the poison rows use is additive over whatever real gossip reports.
    let gossip = cluster.gossip();
    let injected =
        gossip.poisoned_hint(NodeId(2), config_testkit::cluster::PoisonSpec::WrongNodeId);
    gossip.inject(NodeId(1), injected.clone());
    assert!(
        cluster
            .gossip_peers(NodeId(1))
            .iter()
            .any(|h| h.node_id == injected.node_id && h.peer_endpoint == injected.peer_endpoint),
        "an injected hint was not visible to the observer"
    );

    cluster.shutdown().await;
}
