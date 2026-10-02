//! `RETCD_TEST_PORT_RANGE` end to end: with the range set, every listener a testkit cluster
//! binds for "any port" — Raft peer plane, client gRPC, gossip — lands inside it.
//!
//! The host this was written for (2026-10-01) had its whole dynamic pool held by other
//! processes, so a port-`0` bind failed with `os error 10055`. The range moves port-`0` binds
//! below the pool; this row proves the cluster's binds all went through it.

use config_gossip::ports::{port_range_from_env, PORT_RANGE_ENV};
use config_testkit::cluster::{Cluster, ClusterConfig, GossipKind};

fn port_of(endpoint: &str) -> u16 {
    endpoint
        .rsplit_once(':')
        .and_then(|(_, port)| port.trim_end_matches('/').parse().ok())
        .unwrap_or_else(|| panic!("endpoint {endpoint:?} ends in a port"))
}

#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn every_cluster_listener_lands_inside_the_test_port_range() {
    // This binary holds this one test, so setting the variable races no other reader. The gate
    // sets it already; a bare `cargo test` gets the gate's default.
    if port_range_from_env().expect("a valid range").is_none() {
        std::env::set_var(PORT_RANGE_ENV, "20000-26999");
    }
    let range = port_range_from_env()
        .expect("a valid range")
        .expect("the range is set");

    let cluster = Cluster::start_with(ClusterConfig {
        nodes: 3,
        gossip: GossipKind::Real,
        ..ClusterConfig::default()
    })
    .await;

    let mut seen = Vec::new();
    for id in cluster.ids() {
        let gossip = cluster
            .gossip_node(id)
            .unwrap_or_else(|| panic!("node {id} runs real gossip"));
        for (what, port) in [
            ("peer", port_of(&cluster.peer_endpoint(id))),
            ("client", port_of(&cluster.client_endpoint(id))),
            ("gossip", gossip.advertise_addr().port()),
        ] {
            seen.push((id, what, port));
        }
    }
    cluster.shutdown().await;

    let outside: Vec<_> = seen
        .iter()
        .filter(|(_, _, port)| !range.contains(port))
        .collect();
    assert!(
        outside.is_empty(),
        "listeners outside {range:?}: {outside:?} (all: {seen:?})"
    );
    assert_eq!(
        seen.len(),
        9,
        "three listeners on each of three nodes: {seen:?}"
    );
}
