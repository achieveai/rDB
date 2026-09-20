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
        dedup: None,
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
async fn m1_client_01_a_hint_chain_reaches_the_leader_within_the_bound() {
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
        "m1_client_01_a_hint_chain_reaches_the_leader_within_the_bound",
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
async fn m1_client_02_hint_following_stops_at_the_configured_bound() {
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
async fn m1_client_03_a_hint_outside_the_configured_endpoints_is_not_followed() {
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
async fn m1_client_04_a_deadline_on_a_mutation_is_never_retried() {
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
async fn m1_client_05_a_conflict_outcome_survives_the_round_trip() {
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
async fn m1_client_06_pinning_chooses_the_first_endpoint() {
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
        // A fixture that is never dialed: `pinned` rejects it before any socket exists.
        client.pinned("127.0.0.1:1").is_err(), // testkit:allow-port
        "pinning outside the configured set is a configuration error"
    );

    for node in nodes {
        node.shutdown().await;
    }
}

#[retcd_test]
async fn m1_client_07_capabilities_are_configuration_not_discovery() {
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

/// ADR-0015, the case the whole no-replay rule exists for: the request reached the socket and
/// the connection then died.
///
/// A dropped connection arrives as an ordinary gRPC status — often `UNAVAILABLE`, the same
/// code a node uses to say "I am not formed yet". Reading it as a rejection would tell the
/// caller a mutation that may already be committed is safe to send again. The marker the
/// server puts on its own statuses is what separates the two.
#[retcd_test]
async fn m1_client_08_a_connection_reset_after_submission_is_an_unknown_outcome() {
    let server = support::ResetServer::start().await;
    let client = GrpcClient::connect(
        vec![server.endpoint.clone()],
        GrpcClientOptions {
            // Long on purpose: the reset must be what ends the call, not this budget.
            request_deadline: Duration::from_secs(5),
            ..Default::default()
        },
    )
    .expect("client connects");

    let started = std::time::Instant::now();
    let error = client
        .put(put())
        .await
        .expect_err("the connection died under the request");

    assert!(
        matches!(error, ConfigError::DeadlineExceededUnknownOutcome),
        "a reset after submission must be an unknown outcome, got {error:?}"
    );
    assert!(
        !error.is_safe_to_resubmit(),
        "ADR-0015: a mutation whose fate is unknown must never be marked resubmittable"
    );
    assert_eq!(
        client.stats().sends,
        1,
        "ADR-0015: an unknown outcome is never replayed"
    );
    assert_eq!(client.stats().hint_follows, 0);
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "the call ended on the deadline, not on the reset: {:?}",
        started.elapsed()
    );
    assert!(
        server.accepted() >= 1,
        "the request never reached the socket, so this proves nothing"
    );

    // A read has no outcome to be uncertain about, so the same failure is Unavailable.
    let error = client
        .get(config_core::GetRequest {
            key: Bytes::from_static(b"/app/a"),
        })
        .await
        .expect_err("the connection died under the request");
    assert!(
        matches!(error, ConfigError::Unavailable { .. }),
        "a reset read is Unavailable, got {error:?}"
    );
    assert!(error.is_safe_to_resubmit());
}

/// The other half of the same rule: a node that *decided* to answer `UNAVAILABLE` still means
/// "rejected, resubmit freely". Marking transport failures must not sweep up real rejections.
#[retcd_test]
async fn m1_client_09_a_server_generated_unavailable_is_still_safe_to_resubmit() {
    let nodes = start_nodes(1).await;
    nodes[0].store.set_error(Some(ConfigError::Unavailable {
        reason: "cluster is not formed".into(),
    }));

    let client = client(&nodes);
    let error = client.put(put()).await.expect_err("the node is not formed");

    assert!(
        matches!(error, ConfigError::Unavailable { .. }),
        "a server's own Unavailable must survive as Unavailable, got {error:?}"
    );
    assert!(
        error.is_safe_to_resubmit(),
        "the request never entered the log, so resubmitting is the documented recovery"
    );

    // And a server-generated INTERNAL still reads as a storage failure rather than an
    // unknown outcome, because the server told us what happened.
    nodes[0].store.set_error(Some(ConfigError::FatalStorage {
        detail: "rocksdb io".into(),
    }));
    let error = client.put(put()).await.expect_err("storage is broken");
    assert!(
        matches!(error, ConfigError::FatalStorage { .. }),
        "expected FatalStorage, got {error:?}"
    );

    for node in nodes {
        node.shutdown().await;
    }
}

/// ADR-0015: `request_deadline` is the budget for the whole operation, not for each hop.
///
/// Two things are asserted, and the second is the one that was missing entirely: the server is
/// *told* the deadline (`grpc-timeout`), so it can stop working on a call whose answer nobody
/// will read; and the budget shrinks across a hint follow instead of resetting, so a chase
/// cannot cost four times what the caller asked for.
#[retcd_test]
async fn m1_client_10_the_request_deadline_is_one_total_budget() {
    let follower = support::start_spy().await;
    let leader = support::start_spy().await;

    // The follower points at the leader; the leader never answers at all.
    follower.spy.answer_with(ConfigError::NotLeader {
        hint: Some(LeaderHint {
            node_id: NodeId(2),
            endpoint: leader.endpoint.clone(),
        }),
    });

    const BUDGET: Duration = Duration::from_millis(600);
    let client = GrpcClient::connect(
        vec![follower.endpoint.clone(), leader.endpoint.clone()],
        GrpcClientOptions {
            request_deadline: BUDGET,
            ..Default::default()
        },
    )
    .expect("client connects");

    let started = std::time::Instant::now();
    let error = client
        .put(put())
        .await
        .expect_err("the leader never answers");
    let elapsed = started.elapsed();

    assert!(
        matches!(error, ConfigError::DeadlineExceededUnknownOutcome),
        "a mutation that ran out of budget has an unknown outcome, got {error:?}"
    );
    assert_eq!(client.stats().hint_follows, 1, "the hint was followed once");
    assert_eq!(client.stats().sends, 2);

    let first = follower.spy.timeouts();
    let second = leader.spy.timeouts();
    // Anti-flake rule 11: presence before property.
    assert_eq!(first.len(), 1, "the follower saw no deadline: {first:?}");
    assert_eq!(second.len(), 1, "the leader saw no deadline: {second:?}");

    let first_ns = support::grpc_timeout_nanos(&first[0]);
    let second_ns = support::grpc_timeout_nanos(&second[0]);
    assert!(
        first_ns <= BUDGET.as_nanos(),
        "the first hop asked for more than the whole budget: {first_ns} ns"
    );
    assert!(
        second_ns < first_ns,
        "the budget reset on the hint follow: {first_ns} ns then {second_ns} ns"
    );

    // The whole chase fits in one budget, rather than one budget per hop.
    assert!(
        elapsed < BUDGET * 2,
        "the operation outlived its total budget: {elapsed:?} for a {BUDGET:?} budget"
    );
}
