//! Authenticated leader hints under mutual TLS (OQ-21, ADR-0010, m3-architecture §3).
//!
//! A `NotLeader` hint is a claim made by one node about another. Restricting follows to the
//! configured endpoint set proves the operator trusts the *address*; it says nothing about who
//! answers there. These rows prove the missing half: the hinted node id is checked, in TLS, by
//! pinning the dial to `node-<id>.<cluster>.retcd` — the DNS SAN only that node's certificate
//! carries — so a member cannot accept a mutation that was redirected to a different member.
//!
//! And when the check is impossible — mutual TLS with no cluster id configured — the hint is
//! not followed at all. That is the fail-closed direction: a deployment that bothered to issue
//! certificates plainly cares who it talks to.

mod support;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use config_client::{GrpcClient, GrpcClientOptions, TlsMode};
use config_core::{ConfigError, ConfigStore, LeaderHint, MutationOutcome, NodeId, PutRequest};
use config_log::retcd_test;
use config_testkit::tls::TlsFixture;
use serde_json::Value;

use support::{log_lines, Node};

/// Seed is fixed, so a failing row replays byte-for-byte (anti-flake rule 7).
const SEED: u64 = 0x5eed_0054;

fn put() -> PutRequest {
    PutRequest {
        dedup: None,
        key: Bytes::from_static(b"/app/a"),
        value: Bytes::from_static(b"v"),
        expected_mod_revision: None,
    }
}

fn options(tls: TlsMode) -> GrpcClientOptions {
    GrpcClientOptions {
        request_deadline: Duration::from_secs(5),
        tls,
        ..Default::default()
    }
}

/// `follower` answers every call with a `NotLeader` hint naming `leader_node_id` at `target`.
fn hint_to(follower: &Node, leader_node_id: u64, target: &Node) {
    follower.store.set_error(Some(ConfigError::NotLeader {
        hint: Some(LeaderHint {
            node_id: NodeId(leader_node_id),
            endpoint: target.endpoint.clone(),
        }),
    }));
}

/// A follower (node 1) and a leader (node 2), both serving mutual TLS from one fixture.
async fn two_nodes(fixture: &TlsFixture) -> (Node, Node) {
    let cluster = fixture.cluster_id();
    let follower =
        Node::start_with(TlsMode::MutualTls(fixture.node_mtls(NodeId(1))), cluster).await;
    let leader = Node::start_with(TlsMode::MutualTls(fixture.node_mtls(NodeId(2))), cluster).await;
    (follower, leader)
}

fn client(fixture: &TlsFixture, nodes: [&Node; 2]) -> GrpcClient {
    let endpoints = nodes.iter().map(|n| n.endpoint.clone()).collect();
    GrpcClient::connect(
        endpoints,
        options(TlsMode::MutualTls(fixture.client_mtls("svc-a"))),
    )
    .expect("client connects")
    .pinned(&nodes[0].endpoint)
    .expect("pin the follower")
}

/// M3-51 / M3-54: the hinted *node id* is what the target has to prove, not the address.
///
/// One endpoint, two readings. Hinted as node 2 — which is who that certificate is for — the
/// follow works and the mutation lands. Hinted as node 3 at the very same address, the dial is
/// pinned to a name node 2's certificate does not carry, TLS refuses, and the mutation is never
/// written. Without the pin both halves would succeed identically, which is the whole hazard:
/// every check that runs *after* the handshake is satisfied by a certificate that is entirely
/// valid — for somebody else.
#[retcd_test]
async fn m3_client_54_a_hint_is_followed_only_to_the_node_id_it_names() {
    let fixture = Arc::new(TlsFixture::new(support::cluster(), SEED));
    let (follower, leader) = two_nodes(&fixture).await;

    // The honest reading: node 2 really is at `leader.endpoint`.
    hint_to(&follower, 2, &leader);
    let honest = client(&fixture, [&follower, &leader]).with_cluster_id(fixture.cluster_id());
    let response = honest
        .put(put())
        .await
        .expect("a hint whose target proves its node id is followed");
    assert_eq!(response.outcome, MutationOutcome::Applied);
    assert_eq!(honest.stats().hint_follows, 1);
    assert_eq!(leader.store.call_count(), 1);

    // The impostor reading: the same address, claimed to be node 3.
    hint_to(&follower, 3, &leader);
    let fooled = client(&fixture, [&follower, &leader]).with_cluster_id(fixture.cluster_id());
    let error = fooled
        .put(put())
        .await
        .expect_err("node 2's certificate must not satisfy a dial addressed to node 3");

    assert!(
        matches!(error, ConfigError::Unavailable { .. }),
        "a refused handshake happens before submission, so it is Unavailable (R3), got {error:?}"
    );
    assert!(
        error.is_safe_to_resubmit(),
        "nothing was written, so the caller may resubmit"
    );
    assert_eq!(
        leader.store.call_count(),
        1,
        "the mutation was sent to the impostor: only the honest follow above should appear"
    );
    assert_eq!(
        fooled.stats().hint_follows,
        1,
        "the hint was followed as far as the handshake and no further"
    );

    follower.shutdown().await;
    leader.shutdown().await;
}

/// OQ-21, fail-closed: mutual TLS with no cluster id cannot check a hint, so it does not
/// follow one — and says so exactly once.
///
/// The alternative would be to follow it anyway, which is worse than the insecure mode: the
/// deployment paid for certificates and would still be redirected by an unverified claim. One
/// line per client, not per request, because this is a configuration mistake and a
/// per-request line would bury the log it is meant to warn in.
#[retcd_test]
async fn m3_client_55_without_a_cluster_id_a_mutual_tls_hint_is_not_followed() {
    const METHOD: &str = "m3_client_55_without_a_cluster_id_a_mutual_tls_hint_is_not_followed";
    let fixture = Arc::new(TlsFixture::new(support::cluster(), SEED));
    let (follower, leader) = two_nodes(&fixture).await;
    hint_to(&follower, 2, &leader);

    // No `with_cluster_id`.
    let client = client(&fixture, [&follower, &leader]);

    for attempt in 1..=2 {
        let error = client
            .put(put())
            .await
            .expect_err("the hint cannot be verified, so it is returned to the caller");
        assert!(
            matches!(error, ConfigError::NotLeader { hint: Some(_) }),
            "attempt {attempt}: the caller still gets the hint, got {error:?}"
        );
    }

    assert_eq!(
        client.stats().hint_follows,
        0,
        "an unverifiable hint must not be followed"
    );
    assert_eq!(client.stats().sends, 2, "one send per put, no follows");
    assert_eq!(
        leader.store.call_count(),
        0,
        "the mutation reached the hinted node without its identity being checked"
    );

    follower.shutdown().await;
    leader.shutdown().await;

    let warnings: Vec<Value> = log_lines(module_path!(), METHOD)
        .into_iter()
        .filter(|v| v.get("msg").and_then(Value::as_str) == Some("hint_identity_unverified"))
        .collect();
    // Anti-flake rule 11: presence before property.
    assert_eq!(
        warnings.len(),
        1,
        "expected exactly one warning for two puts, got {warnings:#?}"
    );
    assert_eq!(
        warnings[0].get("@l").and_then(Value::as_str),
        Some("Warning")
    );
}
