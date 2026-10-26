//! M10 D14 (ruling A-R199): a connect failure moves on to another configured endpoint.
//!
//! A failure to connect wrote nothing (ADR-0015, note of 2026-09-18), so the client may try
//! the next configured endpoint, and later operations start at the one it moved to. A
//! failure after the request reached a socket is never moved: its outcome may be unknown.

mod support;

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use config_client::{GrpcClient, GrpcClientOptions, MtlsConfig, TlsMode};
use config_core::{
    ConfigError, ConfigStore, GetRequest, LeaderHint, MutationOutcome, NodeId, PutRequest,
};
use config_log::retcd_test;
use config_testkit::tls::TlsFixture;

use support::{Node, ResetServer};

const SEED: u64 = 0x5eed_d014;

fn options(budget: Duration) -> GrpcClientOptions {
    GrpcClientOptions {
        request_deadline: budget,
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

/// An address nothing listens on: bound, read and released.
async fn dead_endpoint() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    drop(listener);
    addr.to_string()
}

/// D14: the first endpoint is dead and the second is live. The put lands on the second,
/// once, and the next put starts there without dialling the dead one again (F4).
/// Cut: no move on a connect failure, and the first put is `Unavailable`. Cut: no start
/// index, and the second put dials the dead endpoint again (`reconnects` 2).
#[retcd_test]
async fn m10_d14_a_dead_first_endpoint_moves_on_and_the_next_call_starts_at_the_live_one() {
    let dead = dead_endpoint().await;
    let live = Node::start().await;
    let client = GrpcClient::connect(
        vec![dead.clone(), live.endpoint.clone()],
        options(Duration::from_secs(5)),
    )
    .expect("client connects");

    let first = client.put(put()).await.expect("the live endpoint answers");
    assert_eq!(first.outcome, MutationOutcome::Applied, "{first:?}");
    let stats = client.stats();
    assert_eq!(
        (stats.sends, stats.reconnects, live.store.call_count()),
        (1, 1, 1),
        "one failed dial, one send, applied once: {stats:?}"
    );

    client
        .put(put())
        .await
        .expect("the second put starts at the live endpoint");
    let stats = client.stats();
    assert_eq!(
        (stats.sends, stats.reconnects, live.store.call_count()),
        (2, 1, 2),
        "the second put did not dial the dead endpoint: {stats:?}"
    );
    assert_eq!(
        client.pinned_endpoint(),
        dead,
        "the pin is still reported as configured"
    );
    live.shutdown().await;
}

/// D14: every endpoint is dead. The error names each one with its reason, nothing was
/// sent, and the whole operation stays inside its one budget. Cut: return only the last
/// error, and the other two endpoints are missing from it.
#[retcd_test]
async fn m10_d14_with_every_endpoint_down_the_error_names_each_one() {
    const BUDGET: Duration = Duration::from_millis(1500);
    let dead = [
        dead_endpoint().await,
        dead_endpoint().await,
        dead_endpoint().await,
    ];
    let client = GrpcClient::connect(dead.to_vec(), options(BUDGET)).expect("client connects");

    let started = Instant::now();
    let error = client
        .get(GetRequest {
            key: Bytes::from_static(b"/app/a"),
        })
        .await
        .expect_err("nothing is listening");
    let elapsed = started.elapsed();

    let ConfigError::Unavailable { reason } = &error else {
        panic!("a read that never connected is Unavailable: {error:?}");
    };
    for endpoint in &dead {
        assert!(
            reason.contains(endpoint.as_str()),
            "{endpoint} is not named: {reason}"
        );
    }
    assert!(
        reason.starts_with("no rEtcd endpoint could be reached: "),
        "{reason}"
    );
    assert_eq!(client.stats().sends, 0);
    assert!(
        elapsed < BUDGET * 3,
        "{elapsed:?} for one {BUDGET:?} budget"
    );
}

/// D14 guard: the first endpoint takes the request and then drops the connection. That is
/// after submission, so the outcome is unknown and the client does not move on to the live
/// second endpoint (ADR-0015). Cut: move on for a failure after sending, and the live
/// endpoint applies it.
#[retcd_test]
async fn m10_d14_a_failure_after_sending_never_moves_on() {
    let reset = ResetServer::start().await;
    let live = Node::start().await;
    let client = GrpcClient::connect(
        vec![reset.endpoint.clone(), live.endpoint.clone()],
        options(Duration::from_secs(5)),
    )
    .expect("client connects");

    let error = client
        .put(put())
        .await
        .expect_err("the connection died under the request");
    assert!(
        matches!(error, ConfigError::DeadlineExceededUnknownOutcome),
        "the request was on the wire, so its outcome is unknown: {error:?}"
    );
    let stats = client.stats();
    assert_eq!((stats.sends, stats.reconnects), (1, 0), "{stats:?}");
    assert_eq!(live.store.call_count(), 0, "the live endpoint never saw it");
    assert!(
        reset.accepted() >= 1,
        "the request never reached the socket"
    );
    live.shutdown().await;
}

/// A-R238 (tester F-002: "last: unavailable: Unknown error: transport error"): a read whose
/// connection dies after the request was sent is `Unavailable`, and its reason names the
/// endpoint, keeping the transport's code and message. Cut: no endpoint in the reason, and
/// this row fails.
#[retcd_test]
async fn m10_a_read_cut_mid_request_names_the_endpoint() {
    let reset = ResetServer::start().await;
    let client = GrpcClient::connect(
        vec![reset.endpoint.clone()],
        options(Duration::from_secs(5)),
    )
    .expect("client connects");

    let error = client
        .get(GetRequest {
            key: Bytes::from_static(b"/app/a"),
        })
        .await
        .expect_err("the connection died under the request");
    let ConfigError::Unavailable { reason } = &error else {
        panic!("a read has no outcome to be unsure about, so it is Unavailable: {error:?}");
    };
    let named = format!(
        "the connection to {} failed during the request (",
        reset.endpoint
    );
    assert!(reason.starts_with(&named), "{reason}");
    assert!(
        reason.ends_with(')'),
        "the code and message are kept: {reason}"
    );
    assert!(
        reset.accepted() >= 1,
        "the request never reached the socket"
    );
}

/// D14 floor row 4 (review A2): a TLS refusal is a connect failure too, so it moves on.
/// Two strangers, each ahead of a node this client trusts:
/// - signed by another CA, which the client refuses during `connect()`;
/// - signed by our CA but trusting only another, so it refuses the client's certificate
///   and only the probe sees it (ADR-0015, fix-round note).
///
/// Each put lands once, on the trusted node, and nothing reaches the stranger.
/// Cut: move only on a plain `connect()` error, and the probe case answers `Unavailable`.
#[retcd_test]
async fn m10_d14_a_tls_refusal_at_the_first_endpoint_moves_on_to_a_trusted_one() {
    let cluster = support::cluster();
    let ours = Arc::new(TlsFixture::new(cluster, SEED));
    let theirs = Arc::new(TlsFixture::other_ca(cluster, SEED));
    let picky = MtlsConfig {
        ca_pem: theirs.ca_pem().as_bytes().to_vec(),
        ..ours.node_mtls(NodeId(1))
    };
    let cases = [
        ("signed by another CA", theirs.node_mtls(NodeId(1))),
        ("refuses our client certificate", picky),
    ];
    for (case, stranger_tls) in cases {
        let stranger = Node::start_with(TlsMode::MutualTls(stranger_tls), cluster).await;
        let trusted =
            Node::start_with(TlsMode::MutualTls(ours.node_mtls(NodeId(2))), cluster).await;
        let client = GrpcClient::connect(
            vec![stranger.endpoint.clone(), trusted.endpoint.clone()],
            GrpcClientOptions {
                tls: TlsMode::MutualTls(ours.client_mtls("svc-a")),
                ..options(Duration::from_secs(5))
            },
        )
        .expect("client connects");

        let response = client
            .put(put())
            .await
            .unwrap_or_else(|e| panic!("{case}: the trusted node answers: {e:?}"));
        assert_eq!(response.outcome, MutationOutcome::Applied, "{case}");
        let stats = client.stats();
        assert_eq!(
            (stats.sends, stats.reconnects),
            (1, 1),
            "{case}: one refused dial, one send: {stats:?}"
        );
        assert_eq!(
            (stranger.store.call_count(), trusted.store.call_count()),
            (0, 1),
            "{case}: nothing reached the stranger"
        );
        stranger.shutdown().await;
        trusted.shutdown().await;
    }
}

/// Review A4: the first endpoint answers `NotLeader` with a hint to the second, which is
/// down. Nothing was applied, and the error says no leader could be reached. It names the
/// down endpoint with its reason and the one that answered, rather than claiming no endpoint
/// could be reached.
/// Cut: drop the "reached" half, and the text says no endpoint could be reached.
#[retcd_test]
async fn m10_d14_a_follower_that_answers_with_its_leader_down_says_no_leader_was_reached() {
    const BUDGET: Duration = Duration::from_millis(1500);
    let follower = Node::start().await;
    let down = dead_endpoint().await;
    follower.store.set_error(Some(ConfigError::NotLeader {
        hint: Some(LeaderHint {
            node_id: NodeId(2),
            endpoint: down.clone(),
        }),
    }));
    let client = GrpcClient::connect(
        vec![follower.endpoint.clone(), down.clone()],
        options(BUDGET),
    )
    .expect("client connects");

    let error = client.put(put()).await.expect_err("the leader is down");
    let ConfigError::Unavailable { reason } = &error else {
        panic!("a leader that never connected is Unavailable: {error:?}");
    };
    assert!(
        reason.starts_with("no rEtcd leader could be reached: "),
        "{reason}"
    );
    let (unreached, answered) = reason
        .split_once("; reached, but not the leader: ")
        .unwrap_or_else(|| panic!("the endpoint that answered is named: {reason}"));
    assert!(
        unreached.contains(down.as_str()) && !unreached.contains(follower.endpoint.as_str()),
        "the down leader, with its reason, and only it: {reason}"
    );
    assert_eq!(answered, follower.endpoint, "{reason}");
    let stats = client.stats();
    assert_eq!(
        (stats.sends, stats.hint_follows, follower.store.call_count()),
        (1, 1, 1),
        "{stats:?}"
    );
    follower.shutdown().await;
}

/// A-R208: control's nodes 1 and 2 are down, and follower 3, having lost quorum, answers
/// `Unavailable` "leader unknown". The answer is kept, it names the endpoint that gave it, and
/// it lists both endpoints that could not be reached, each with its reason. The class stays
/// `Unavailable`, and "unavailable:" is said once.
/// Cut: drop the could-not-reach list, and the two down endpoints are missing.
#[retcd_test]
async fn m10_r208_a_follower_that_answers_leader_unknown_names_itself_and_each_unreached_endpoint()
{
    const BUDGET: Duration = Duration::from_millis(3000);
    let down = [dead_endpoint().await, dead_endpoint().await];
    let follower = Node::start().await;
    follower.store.set_error(Some(ConfigError::Unavailable {
        reason: "leader unknown".to_owned(),
    }));
    let client = GrpcClient::connect(
        vec![down[0].clone(), down[1].clone(), follower.endpoint.clone()],
        options(BUDGET),
    )
    .expect("client connects");

    let error = client
        .get(GetRequest {
            key: Bytes::from_static(b"/app/a"),
        })
        .await
        .expect_err("no leader is known");
    let ConfigError::Unavailable { reason } = &error else {
        panic!("the follower's answer keeps its class: {error:?}");
    };
    let (answer, rest) = reason
        .split_once(" (answered by ")
        .unwrap_or_else(|| panic!("the endpoint that answered is named: {reason}"));
    assert_eq!(
        answer, "leader unknown",
        "the server's answer comes first: {reason}"
    );
    let (answered_by, unreached) = rest
        .split_once("; could not reach: ")
        .unwrap_or_else(|| panic!("the endpoints that could not be reached are listed: {reason}"));
    assert_eq!(answered_by, follower.endpoint, "{reason}");
    for endpoint in &down {
        assert!(
            unreached.contains(&format!("cannot connect to {endpoint}"))
                || unreached.contains(&format!("connecting to {endpoint} ")),
            "{endpoint} is listed with its reason: {reason}"
        );
    }
    let text = error.to_string();
    assert_eq!(
        text.matches("unavailable:").count(),
        1,
        "\"unavailable:\" once: {text}"
    );
    let stats = client.stats();
    assert_eq!(
        (stats.sends, stats.reconnects, follower.store.call_count()),
        (1, 2, 1),
        "{stats:?}"
    );
    follower.shutdown().await;
}
