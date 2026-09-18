//! `GrpcClient` retry policy: bounded hint following, and the one error that is never
//! retried (ADR-0009, ADR-0015).

mod support;

use std::time::Duration;

use bytes::Bytes;
use config_client::{GrpcClient, GrpcClientOptions};
use config_core::{
    Authz, Capabilities, ConfigError, ConfigStore, Dedup, Durability, LeaderHint, MutationOutcome,
    MutationResponse, NodeId, Pagination, PutRequest, TransportSecurity, WatchResumption,
};
use config_grpc::TlsMode;
use config_log::retcd_test;
use serde_json::Value;

use support::{start_nodes, Node};

fn options() -> GrpcClientOptions {
    GrpcClientOptions {
        request_deadline: Duration::from_secs(5),
        ..Default::default()
    }
}

fn put() -> PutRequest {
    PutRequest {
        key: Bytes::from_static(b"/app/a"),
        value: Bytes::from_static(b"v"),
        expected_mod_revision: None,
    }
}

/// Point `node` at `target` with a `NotLeader` hint.
fn hint_to(node: &Node, leader: u64, target: &Node) {
    node.store.set_error(Some(ConfigError::NotLeader {
        hint: Some(LeaderHint {
            node_id: NodeId(leader),
            endpoint: target.endpoint.clone(),
        }),
    }));
}

fn client(nodes: &[Node]) -> GrpcClient {
    let endpoints = nodes.iter().map(|n| n.endpoint.clone()).collect();
    GrpcClient::connect(endpoints, options()).expect("client connects")
}

#[retcd_test]
async fn a_hint_chain_reaches_the_leader_within_the_bound() {
    let nodes = start_nodes(3).await;
    hint_to(&nodes[0], 3, &nodes[1]);
    hint_to(&nodes[1], 3, &nodes[2]);
    nodes[2]
        .store
        .set_mutation(MutationResponse::applied_put(42));

    let client = client(&nodes);
    let response = client.put(put()).await.expect("the chase finds the leader");

    assert_eq!(response.outcome, MutationOutcome::Applied);
    assert_eq!(response.revision, 42);

    let stats = client.stats();
    assert_eq!(stats.sends, 3, "one initial send plus two hint follows");
    assert_eq!(stats.hint_follows, 2);
    assert_eq!(nodes[0].store.call_count(), 1);
    assert_eq!(nodes[1].store.call_count(), 1);
    assert_eq!(nodes[2].store.call_count(), 1);

    // ADR-0009: the whole chase is one operation, so every hop carries one request_id.
    let path = config_log::layer::test_file_path(
        &config_log::testing::test_log_dir(),
        module_path!(),
        "a_hint_chain_reaches_the_leader_within_the_bound",
    );
    let contents = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("test log {} is readable: {e}", path.display()));
    let test_run = config_log::testing::test_run_id();
    let request_ids: Vec<String> = contents
        .lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter(|v| v.get("testRun").and_then(Value::as_str) == Some(test_run))
        .filter(|v| v.get("@m").and_then(Value::as_str) == Some("rpc"))
        .filter_map(|v| {
            v.get("request_id")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .collect();
    assert_eq!(
        request_ids.len(),
        3,
        "each hop must log one rpc line in {}",
        path.display()
    );
    assert!(
        request_ids.windows(2).all(|w| w[0] == w[1]),
        "hops used different request ids: {request_ids:?}"
    );

    for node in nodes {
        node.shutdown().await;
    }
}

#[retcd_test]
async fn hint_following_stops_at_the_configured_bound() {
    let nodes = start_nodes(3).await;
    // A cycle: every node points at the next one and none is ever the leader.
    hint_to(&nodes[0], 2, &nodes[1]);
    hint_to(&nodes[1], 3, &nodes[2]);
    hint_to(&nodes[2], 1, &nodes[0]);

    let client = client(&nodes);
    let error = client.put(put()).await.expect_err("nobody is the leader");

    assert!(
        matches!(error, ConfigError::NotLeader { hint: Some(_) }),
        "the last hint is returned to the caller, got {error:?}"
    );
    let stats = client.stats();
    assert_eq!(
        stats.hint_follows, 3,
        "default max_hint_follows is 3 (ADR-0009)"
    );
    assert_eq!(stats.sends, 4, "the initial send plus three follows");

    for node in nodes {
        node.shutdown().await;
    }
}

#[retcd_test]
async fn a_hint_outside_the_configured_endpoints_is_not_followed() {
    let nodes = start_nodes(1).await;
    nodes[0].store.set_error(Some(ConfigError::NotLeader {
        hint: Some(LeaderHint {
            node_id: NodeId(9),
            // A hint is advisory. Dialing an endpoint the operator never configured would let
            // a compromised node redirect a client anywhere (ADR-0003, ADR-0009).
            endpoint: "203.0.113.9:2379".into(),
        }),
    }));

    let client = client(&nodes);
    let error = client.put(put()).await.expect_err("not leader");

    assert!(matches!(error, ConfigError::NotLeader { hint: Some(_) }));
    assert_eq!(client.stats().hint_follows, 0);
    assert_eq!(client.stats().sends, 1);

    for node in nodes {
        node.shutdown().await;
    }
}

#[retcd_test]
async fn a_deadline_on_a_mutation_is_never_retried() {
    let nodes = start_nodes(2).await;
    nodes[0].store.hang_forever();

    let endpoints = nodes.iter().map(|n| n.endpoint.clone()).collect();
    let client = GrpcClient::connect(
        endpoints,
        GrpcClientOptions {
            // Short on purpose: the server never answers, so this is the only thing that ends
            // the call. It is a budget, not a synchronization sleep.
            request_deadline: Duration::from_millis(250),
            ..Default::default()
        },
    )
    .expect("client connects");

    let error = client
        .put(put())
        .await
        .expect_err("the server never answers");

    assert!(
        matches!(error, ConfigError::DeadlineExceededUnknownOutcome),
        "expected an unknown-outcome error, got {error:?}"
    );
    assert!(
        !error.is_safe_to_resubmit(),
        "ADR-0015: this outcome must not be marked resubmittable"
    );
    assert_eq!(
        client.stats().sends,
        1,
        "ADR-0015: an unknown outcome is never replayed"
    );
    assert_eq!(client.stats().hint_follows, 0);

    // A read that times out has no outcome to be unsure about.
    let error = client
        .get(config_core::GetRequest {
            key: Bytes::from_static(b"/app/a"),
        })
        .await
        .expect_err("the server never answers");
    assert!(
        matches!(error, ConfigError::Unavailable { .. }),
        "a timed-out read is Unavailable, got {error:?}"
    );

    // The hung server would block a graceful drain, so the handles are dropped instead.
    drop(nodes);
}

#[retcd_test]
async fn a_conflict_outcome_survives_the_round_trip() {
    let nodes = start_nodes(1).await;
    nodes[0]
        .store
        .set_mutation(MutationResponse::conflict(20, true, 15));

    let client = client(&nodes);
    let response = client
        .put(PutRequest {
            expected_mod_revision: Some(14),
            ..put()
        })
        .await
        .expect("§7.3: a rejected CAS is an Ok outcome, not an error");

    assert_eq!(response.outcome, MutationOutcome::Conflict);
    assert_eq!(response.revision, 20);
    assert!(response.exists);
    assert_eq!(response.current_mod_revision, 15);
    assert_eq!(client.stats().sends, 1, "an outcome is never retried");

    for node in nodes {
        node.shutdown().await;
    }
}

#[retcd_test]
async fn pinning_chooses_the_first_endpoint() {
    let nodes = start_nodes(2).await;
    nodes[1]
        .store
        .set_mutation(MutationResponse::applied_put(5));

    let client = client(&nodes).pinned(&nodes[1].endpoint).expect("pin");
    assert_eq!(client.pinned_endpoint(), nodes[1].endpoint);
    client.put(put()).await.expect("put on the pinned node");

    assert_eq!(nodes[0].store.call_count(), 0);
    assert_eq!(nodes[1].store.call_count(), 1);

    assert!(
        client.pinned("127.0.0.1:1").is_err(),
        "pinning outside the configured set is a configuration error"
    );

    for node in nodes {
        node.shutdown().await;
    }
}

#[retcd_test]
async fn capabilities_are_configuration_not_discovery() {
    let nodes = start_nodes(1).await;
    let endpoints: Vec<String> = nodes.iter().map(|n| n.endpoint.clone()).collect();

    // Without an explicit expectation, every field except transport security is the weakest
    // legal value — the schema has no capabilities RPC to ask (ADR-0016).
    let default = GrpcClient::connect(endpoints.clone(), options()).expect("connect");
    assert_eq!(
        default.capabilities(),
        Capabilities {
            durability: Durability::Ephemeral,
            watch_resumption: WatchResumption::Unsupported,
            authz: Authz::Development,
            transport_security: TransportSecurity::Insecure,
            pagination: Pagination::Unsupported,
            dedup: Dedup::Unsupported,
        }
    );

    let expected = Capabilities {
        durability: Durability::Persistent,
        authz: Authz::StaticAllowlist,
        transport_security: TransportSecurity::MutualTls,
        ..Capabilities::EPHEMERAL_DEVELOPMENT
    };
    let configured = GrpcClient::connect(
        endpoints,
        GrpcClientOptions {
            expected_capabilities: Some(expected),
            ..options()
        },
    )
    .expect("connect");
    assert_eq!(configured.capabilities(), expected);

    // The scheme follows the TLS mode even before anything is dialed.
    assert_eq!(TlsMode::Insecure.scheme(), "http");

    for node in nodes {
        node.shutdown().await;
    }
}
