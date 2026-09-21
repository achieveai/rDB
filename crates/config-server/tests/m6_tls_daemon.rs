//! G-01 — `[tls] handshake_timeout_ms` as a running daemon honours it (M6-45's daemon half,
//! ADR-0028).
//!
//! # Why this row needs a process
//!
//! `config-grpc/tests/mtls.rs`'s `m6_45_a_stalled_handshake_is_bounded_and_counted` already
//! proves the listener ends a handshake that never arrives. It does so by handing
//! `MtlsConfig::with_handshake_timeout` to a listener it constructs itself, which is exactly
//! the seam this row exists to remove: it proves the *bound* works and says nothing about
//! whether an operator can reach it. Between the TOML key and that setter lie
//! `config::validate`, `ServerConfig::tls_material` and `run::tls_mode`, and a knob nothing
//! drives end to end is not better than the constant it replaced.
//!
//! So the claim here is narrower and different: a value written in a configuration file is the
//! value a real client's stalled handshake is actually cut off at.
//!
//! # Nothing here sleeps
//!
//! The row waits by reading the stalled socket to EOF and by polling `/metrics`, both under
//! deadlines derived from [`support::deadline`]. The one timing assertion that is not a
//! deadline is the *lower* bound — the listener must not have given up sooner than it was told
//! to — and a lower bound cannot be made flaky by a slow host, only by a fast one, which is not
//! a thing that happens.

mod support;

use std::time::Duration;

use config_grpc::DEFAULT_HANDSHAKE_TIMEOUT;
use config_log::retcd_test;
use config_testkit::poll::{poll_until_async, Timeout};
use tokio::io::AsyncReadExt as _;

use support::{deadline, startup_deadline, Harness, NodeOptions};

// =====================================================================================
// Helpers
// =====================================================================================

/// The `[tls] handshake_timeout_ms` this row configures.
///
/// Two orders of magnitude under [`DEFAULT_HANDSHAKE_TIMEOUT`], which is what lets the row tell
/// "the configured bound was honoured" from "the daemon fell back to its default": a run that
/// ignored the key would take forty times longer to close the socket than the assertion below
/// allows.
const HANDSHAKE_TIMEOUT_MS: u64 = 250;

/// `retcd_authn_rejected_total{plane="client",reason="handshake_failed"}` for node `node_id`.
///
/// The family is published for every `(plane, reason)` pair including the ones at zero
/// (ADR-0026), so an absent line is a defect rather than a zero and is reported as one instead
/// of being rounded down.
async fn handshake_failures(health_endpoint: &str, node_id: u64) -> u64 {
    let needle = format!(
        "retcd_authn_rejected_total{{node_id=\"{node_id}\",plane=\"client\",\
         reason=\"handshake_failed\"}} "
    );
    let body = support::http_get(health_endpoint, "/metrics").await;
    body.lines()
        .find_map(|line| {
            line.strip_prefix(needle.as_str())
                .and_then(|rest| rest.trim().parse::<u64>().ok())
        })
        .unwrap_or_else(|| {
            panic!("no {needle:?} series in /metrics; ADR-0026 publishes it even at zero:\n{body}")
        })
}

/// Poll `/metrics` until the client plane's handshake-failure counter reaches `want`.
async fn wait_for_handshake_failures(health_endpoint: &str, node_id: u64, want: u64) {
    let result = poll_until_async(deadline(10), Duration::from_millis(25), || async {
        let count = handshake_failures(health_endpoint, node_id).await;
        (count >= want).then_some(count)
    })
    .await;
    if let Err(Timeout { elapsed, .. }) = result {
        let count = handshake_failures(health_endpoint, node_id).await;
        panic!(
            "node {node_id}'s client-plane handshake_failed counter never reached {want} within \
             {elapsed:?} (last observed: {count})"
        );
    }
}

// =====================================================================================
// G-01
// =====================================================================================

/// G-01: a handshake that never progresses is cut off at the bound `[tls]` configured, and
/// counted.
///
/// Drives the shape M6-45 describes — complete TCP, then say nothing — against a **real daemon
/// configured from a file**, and asserts three things that together mean the key is wired:
///
/// * the socket is closed rather than left parked, so the bound fired at all;
/// * it was closed no sooner than the configured 250 ms, so the daemon did not simply refuse
///   the connection for some other reason that happens to look like a timeout;
/// * it was closed well inside [`DEFAULT_HANDSHAKE_TIMEOUT`], so the configured value — not the
///   start-up constant it used to be — is what the listener ran on.
///
/// The third is the assertion that fails if the key is parsed and then dropped on the floor,
/// which is the realistic way this wiring breaks.
///
/// Finally the expiry must reach the surface an operator reads: the same
/// `retcd_authn_rejected_total{reason="handshake_failed"}` series the listener-level row
/// asserts on, here scraped over HTTP from outside the process.
#[retcd_test]
async fn g01_a_stalled_handshake_is_cut_off_at_the_configured_bound() {
    const METHOD: &str = "g01_a_stalled_handshake_is_cut_off_at_the_configured_bound";
    let harness = Harness::with_nodes(METHOD, &[1]).await;
    let node_layout = &harness.nodes[0];
    let node_id = node_layout.node_id;
    harness.write_node_files(
        node_layout,
        &NodeOptions {
            tls_handshake_timeout_ms: Some(HANDSHAKE_TIMEOUT_MS),
            ..harness.node_options()
        },
    );

    let mut node = harness.start(0, true);
    let client_endpoint = node.client_endpoint().to_string();
    let health_endpoint = node.health_endpoint().to_string();

    assert_eq!(
        handshake_failures(&health_endpoint, node_id).await,
        0,
        "nothing has failed a handshake yet, so the row's increment below is its own"
    );

    // Connect and then say nothing at all. The listener accepts the TCP before it can know who
    // is calling, which is precisely the unauthenticated window the bound exists to close.
    let mut stalled = tokio::net::TcpStream::connect(&client_endpoint)
        .await
        .unwrap_or_else(|e| panic!("connect to the client plane at {client_endpoint}: {e}"));
    let opened = std::time::Instant::now();
    let mut byte = [0u8; 1];
    let read = tokio::time::timeout(deadline(20), stalled.read(&mut byte))
        .await
        .expect("a stalled handshake must be dropped well inside the test deadline")
        .expect("read the closed socket");
    let elapsed = opened.elapsed();

    assert_eq!(
        read, 0,
        "the listener must close a handshake that never arrived, not answer on it"
    );
    let configured = Duration::from_millis(HANDSHAKE_TIMEOUT_MS);
    assert!(
        elapsed >= configured,
        "the socket closed after {elapsed:?}, sooner than the configured {configured:?}; \
         something other than the handshake bound ended it"
    );
    assert!(
        elapsed < DEFAULT_HANDSHAKE_TIMEOUT,
        "the socket closed after {elapsed:?}, at or beyond the {DEFAULT_HANDSHAKE_TIMEOUT:?} \
         start-up default; tls.handshake_timeout_ms = {HANDSHAKE_TIMEOUT_MS} did not reach the \
         listener"
    );

    wait_for_handshake_failures(&health_endpoint, node_id, 1).await;

    node.stop_gracefully(startup_deadline()).await;
}

/// G-01's compatibility half: a `[tls]` section with no `handshake_timeout_ms` still starts.
///
/// The key is new, and `TlsSection` is `deny_unknown_fields` in both directions — a document
/// that omits an added field must parse exactly as it did before, or every deployed
/// configuration file becomes an exit code 2 on upgrade. This is the cheap daemon-level guard
/// for that; the value the omission resolves to is asserted in `config::tests`, where it can be
/// compared against [`DEFAULT_HANDSHAKE_TIMEOUT`] without waiting ten seconds for it.
#[retcd_test]
async fn g01_a_document_without_the_key_starts_unchanged() {
    const METHOD: &str = "g01_a_document_without_the_key_starts_unchanged";
    let harness = Harness::with_nodes(METHOD, &[1]).await;

    // `harness.node_options()` leaves `tls_handshake_timeout_ms` at `None`, which writes no key
    // at all — the byte-identical pre-G-01 document.
    let mut node = harness.start(0, true);

    let health = support::health(node.health_endpoint()).await;
    assert_eq!(health.node_id, harness.nodes[0].node_id);
    assert!(
        health.ready,
        "a document that predates the key must still serve: {health:#?}"
    );
    assert_eq!(
        health.transport_security, "MutualTls",
        "the node still serves the mutual profile the document asked for: {health:#?}"
    );

    node.stop_gracefully(startup_deadline()).await;
}
