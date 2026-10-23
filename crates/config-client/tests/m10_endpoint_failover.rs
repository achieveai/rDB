//! M10 D14 (ruling A-R199): a connect failure moves on to another configured endpoint.
//!
//! A failure to connect wrote nothing (ADR-0015, note of 2026-09-18), so the client may try
//! the next configured endpoint, and later operations start at the one that answered. A
//! failure after the request reached a socket is never moved: its outcome may be unknown.

mod support;

use std::time::{Duration, Instant};

use bytes::Bytes;
use config_client::{GrpcClient, GrpcClientOptions};
use config_core::{ConfigError, ConfigStore, GetRequest, MutationOutcome, PutRequest};
use config_log::retcd_test;

use support::{Node, ResetServer};

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
