//! Failing to connect is not an unknown outcome (ADR-0015, note of 2026-09-18).
//!
//! ADR-0015's no-replay rule exists for one shape: the request reached a socket, so a server
//! may have applied it, and the answer never came back. Getting a connection at all is an
//! earlier, separate phase, and lumping it in cost real precision — a `put` that never left
//! the process was reported as "outcome unknown", sending the caller off to run the read-back
//! recovery recipe for a mutation that demonstrably did not happen.
//!
//! These rows hold the two halves side by side, against doubles that differ in exactly one
//! respect: whether the connection was ever usable.

mod support;

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use config_client::{GrpcClient, GrpcClientOptions, TlsMode};
use config_core::{ConfigError, ConfigStore, GetRequest, MutationOutcome, NodeId, PutRequest};
use config_log::retcd_test;
use config_testkit::tls::TlsFixture;

use support::{Node, ResetServer};

const SEED: u64 = 0x5eed_0064;

fn put() -> PutRequest {
    PutRequest {
        dedup: None,
        key: Bytes::from_static(b"/app/a"),
        value: Bytes::from_static(b"v"),
        expected_mod_revision: None,
    }
}

fn get() -> GetRequest {
    GetRequest {
        key: Bytes::from_static(b"/app/a"),
    }
}

fn options() -> GrpcClientOptions {
    GrpcClientOptions {
        request_deadline: Duration::from_secs(5),
        ..Default::default()
    }
}

/// M3-62 / M3-64: a handshake the server cannot satisfy is `Unavailable` — for a mutation
/// exactly as for a read — and the reconnects it triggers are bounded.
///
/// "Wrong certificate" is the connect failure an operator actually meets, and it is the one
/// that used to be reported as an unknown mutation outcome: a `put` that TLS refused to carry
/// sent the caller off to run the read-back-then-CAS recovery recipe for a write that
/// provably never happened. The fixture here makes the difference exactly one fact — a
/// different trust anchor, same cluster, same names, same SANs.
#[retcd_test]
async fn m3_client_62_a_refused_tls_handshake_is_unavailable_and_bounded() {
    let cluster = support::cluster();
    let ours = Arc::new(TlsFixture::new(cluster, SEED));
    let theirs = Arc::new(TlsFixture::other_ca(cluster, SEED));

    let stranger = Node::start_with(TlsMode::MutualTls(theirs.node_mtls(NodeId(1))), cluster).await;
    let client = GrpcClient::connect(
        vec![stranger.endpoint.clone()],
        GrpcClientOptions {
            tls: TlsMode::MutualTls(ours.client_mtls("svc-a")),
            ..options()
        },
    )
    .expect("client connects");

    let error = client
        .put(put())
        .await
        .expect_err("the server is signed by a CA we do not trust");

    assert!(
        matches!(error, ConfigError::Unavailable { .. }),
        "a refused handshake happens before submission, so it is Unavailable: got {error:?}"
    );
    assert!(
        error.is_safe_to_resubmit(),
        "ADR-0015 note: nothing was submitted, so resubmitting duplicates nothing"
    );
    assert_eq!(
        stranger.store.call_count(),
        0,
        "the mutation reached a store behind an untrusted certificate"
    );

    let stats = client.stats();
    assert_eq!(
        stats.sends, 0,
        "nothing reached the wire, so nothing may be counted as sent"
    );
    assert_eq!(
        stats.reconnects, 3,
        "the first attempt plus max_hint_follows reconnects, and no more"
    );
    assert_eq!(stats.hint_follows, 0, "a reconnect is not a hint follow");

    // A read reports the same thing, because the two were never different.
    let read_error = client.get(get()).await.expect_err("still untrusted");
    assert!(
        matches!(read_error, ConfigError::Unavailable { .. }),
        "got {read_error:?}"
    );

    // The control: the same client shape against a node this fixture really did sign. Without
    // it the row above would pass just as well if the client had stopped working entirely.
    let ourself = Node::start_with(TlsMode::MutualTls(ours.node_mtls(NodeId(1))), cluster).await;
    let trusting = GrpcClient::connect(
        vec![ourself.endpoint.clone()],
        GrpcClientOptions {
            tls: TlsMode::MutualTls(ours.client_mtls("svc-a")),
            ..options()
        },
    )
    .expect("client connects");
    let response = trusting.put(put()).await.expect("our own CA is trusted");
    assert_eq!(response.outcome, MutationOutcome::Applied);

    stranger.shutdown().await;
    ourself.shutdown().await;
}

/// M3-64, the plainest form: the node is simply not there any more.
///
/// Naming a closed port means binding an ephemeral one and releasing it, and another test in
/// this binary can be handed that exact port in between; the probe therefore re-binds
/// afterwards to prove the port stayed free, and retries otherwise — the same dance
/// `config-grpc`'s peer-transport row does.
///
/// The budget is short because a refused connect is not instant on every platform (Windows
/// loopback takes about two seconds to give up), and how long the operating system takes to
/// say "no" is not what this row is about. What it is about is that the whole thing stays
/// inside the caller's budget and comes back resubmittable.
#[retcd_test]
async fn m3_client_64_a_stopped_node_is_unavailable_before_submission() {
    const BUDGET: Duration = Duration::from_millis(750);
    let mut stolen = Vec::new();

    for _ in 0..5 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        drop(listener);

        let client = GrpcClient::connect(
            vec![addr.to_string()],
            GrpcClientOptions {
                request_deadline: BUDGET,
                ..Default::default()
            },
        )
        .expect("client connects");

        let started = Instant::now();
        let error = client.put(put()).await.expect_err("nothing is listening");
        let elapsed = started.elapsed();
        let stats = client.stats();

        match tokio::net::TcpListener::bind(addr).await {
            Err(_) => stolen.push(error),
            Ok(reclaimed) => {
                drop(reclaimed);
                assert!(
                    matches!(error, ConfigError::Unavailable { .. }),
                    "a mutation that never reached a socket is Unavailable, not unknown: \
                     got {error:?}"
                );
                assert!(error.is_safe_to_resubmit());
                assert_eq!(stats.sends, 0, "nothing was ever put on the wire");
                assert!(
                    (1..=3).contains(&stats.reconnects),
                    "reconnects must be bounded by max_hint_follows: {stats:?}"
                );
                assert_eq!(stats.hint_follows, 0);
                assert!(
                    elapsed < BUDGET * 3,
                    "the reconnects outlived the request budget: {elapsed:?} for {BUDGET:?}"
                );
                return;
            }
        }
    }
    panic!("no port stayed closed for a whole dial; observed {stolen:?}");
}

/// The other half, unchanged: a connection that accepted the request and then died leaves a
/// mutation genuinely unknown.
///
/// [`ResetServer`] differs from the rows above in one respect — it completes the HTTP/2
/// handshake before destroying the connection — and that one difference is the whole rule.
#[retcd_test]
async fn m3_client_63_a_failure_after_submission_is_still_an_unknown_outcome() {
    let reset = ResetServer::start().await;
    let client =
        GrpcClient::connect(vec![reset.endpoint.clone()], options()).expect("client connects");

    let error = client
        .put(put())
        .await
        .expect_err("the connection died under the request");

    assert!(
        matches!(error, ConfigError::DeadlineExceededUnknownOutcome),
        "the request was on the wire, so its outcome is unknown: got {error:?}"
    );
    assert!(!error.is_safe_to_resubmit());
    assert_eq!(
        client.stats().sends,
        1,
        "ADR-0015: an unknown outcome is never replayed, and a reconnect must not sneak one in"
    );
    assert_eq!(client.stats().reconnects, 0);
    assert!(
        reset.accepted() >= 1,
        "the request never reached the socket, so this proves nothing"
    );
}

/// L1 (critic M1): the server refusing *our* client certificate is a connect-phase failure.
///
/// This is the shape TLS 1.3 hides. The client finishes its half of the handshake before the
/// server has looked at the certificate, so `Endpoint::connect()` returns `Ok` and the refusal
/// arrives only as the first RPC dying — an unmarked `CANCELLED`, which the mutation table
/// reads as "may have been applied". Nothing was applied: no request ever reached a store.
#[retcd_test]
async fn m3_client_65_a_rejected_client_certificate_is_unavailable_before_submission() {
    let cluster = support::cluster();
    let ours = Arc::new(TlsFixture::new(cluster, SEED));
    let server = Node::start_with(TlsMode::MutualTls(ours.node_mtls(NodeId(1))), cluster).await;

    // The one thing wrong with this client: its certificate expired in 2021. The CA is ours,
    // so the *server's* certificate still verifies and the dial itself succeeds.
    let expired = ours.issue_with(
        config_testkit::tls::CertProfile::client("svc-a"),
        config_testkit::tls::CertOverrides::expired(),
    );
    let client = GrpcClient::connect(
        vec![server.endpoint.clone()],
        GrpcClientOptions {
            tls: TlsMode::MutualTls(expired.mtls()),
            ..options()
        },
    )
    .expect("client connects");

    let error = client
        .put(put())
        .await
        .expect_err("the server refuses an expired client certificate");
    assert!(
        matches!(error, ConfigError::Unavailable { .. }),
        "nothing was submitted, so this is Unavailable and not an unknown outcome: got {error:?}"
    );
    assert!(error.is_safe_to_resubmit());
    assert_eq!(
        client.stats().sends,
        0,
        "the probe is not a send, and the put never got one"
    );
    assert_eq!(
        server.store.call_count(),
        0,
        "the mutation reached a store behind a refused certificate"
    );
    assert!(
        (1..=3).contains(&client.stats().reconnects),
        "the failure is in the connect phase, so it is retried and bounded: {:?}",
        client.stats()
    );

    // The connect phase says so in the log, not only in the returned error.
    let lines = support::log_lines(
        module_path!(),
        "m3_client_65_a_rejected_client_certificate_is_unavailable_before_submission",
    );
    let failures: Vec<_> = lines
        .iter()
        .filter(|l| {
            l.get("@m").and_then(serde_json::Value::as_str) == Some("client connect attempt failed")
        })
        .collect();
    assert!(
        !failures.is_empty(),
        "no connect-phase failure line was written; saw {} lines",
        lines.len()
    );

    server.shutdown().await;
}
