//! `/health` names the node's client-plane address (ADR-0018 §3, note of 2026-10-01).
//!
//! An operator holding only a health port could not tell which gRPC address that node serves
//! on: the ready line said so once, on stdout, at startup. The payload now carries the same
//! string under the same key, `client`, so a health poller can hand a client the address of the
//! node it is looking at without a second source of truth.

mod support;

use config_log::retcd_test;

use support::{startup_deadline, DaemonProcess, Harness};

/// `client` is present and is exactly the bound client address — the string the ready line
/// printed, and the address the harness reserved for this node — not a configured or
/// advertised value that could differ from it.
#[retcd_test]
async fn health_reports_the_bound_client_address() {
    const METHOD: &str = "health_reports_the_bound_client_address";
    let harness = Harness::with_nodes(METHOD, &[1]).await;
    let mut spec = harness.spec(0);
    spec.form = true;
    let mut process = DaemonProcess::spawn(spec);
    let ready = process
        .wait_ready(startup_deadline())
        .unwrap_or_else(|e| panic!("the node never became ready: {e}"));

    let body = support::http_get(process.health_endpoint(), "/health").await;
    let payload: serde_json::Value = serde_json::from_str(&body)
        .unwrap_or_else(|e| panic!("/health did not answer JSON ({e}): {body}"));

    assert_eq!(
        payload.get("client").and_then(serde_json::Value::as_str),
        Some(ready.client.as_str()),
        "/health must carry the ready line's client address under the same key: {body}"
    );
    assert_eq!(
        ready.client,
        harness.nodes[0].client.to_string(),
        "the ready line's client address is the one this node actually bound"
    );
    // Additive: the fields that were there before keep their names.
    for key in ["node_id", "role", "current_leader", "term", "ready"] {
        assert!(
            payload.get(key).is_some(),
            "existing field {key:?} must still be present: {body}"
        );
    }
}
