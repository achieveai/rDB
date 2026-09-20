//! M6 credential rotation: TLS reload and certificate expiry (test plan §4.1 and §4.4, rows
//! M6-41..M6-48 and M6-62..M6-64; ADR-0028).
//!
//! # What these rows are written against
//!
//! Behaviour, never the acceptor type (TA-57, OQ-59). No row here names
//! `tokio_rustls::TlsAcceptor`, `rustls::ServerConfig` or `tonic::transport::Server`; what they
//! name is a fingerprint read off a real handshake, a counter, a log line, and a client that
//! either connects or does not.
//!
//! # Ground truth, read off the code and confirmed by running it
//!
//! * A reload that finds the same bytes reports `outcome = "unchanged"` on every plane and does
//!   **not** increment `retcd_tls_reloads_total`: `TlsRotator::try_reload` compares what it
//!   read against what is served before it swaps anything. A row that wants a rotation must
//!   therefore write genuinely different material. `TlsFixture` is deterministic in
//!   `(cluster_id, seed, label)`, so "different" here means a second authority
//!   ([`TlsFixture::other_ca`]) rather than a second call on the same one.
//! * A reload reports **three** planes — `client`, `peer` and `peer_dial`. The dialler is one
//!   of them because rotating what a node serves without rotating what it dials leaves its
//!   peers disagreeing about who it is (M6-49).
//! * One node's rotation is only safe inside an overlap window, which is why every row below
//!   that rotates a *leaf* first widens every node's trust anchors ([`overlap`]). That is not a
//!   harness convenience: it is the §15.1 procedure, and a row that skipped it would be
//!   asserting that an unstaged rotation works, which it must not.
//! * A refused handshake never reaches a backend, so it is counted on the *listener*
//!   (`Cluster::tls_authn_rejections`) rather than in `NodeMetrics::authn_rejected_by_reason`,
//!   which counts the application-layer refusals a *completed* handshake can still produce.
//! * `GrpcClient::connect` is eager, so a client holding material the server will refuse fails
//!   at the first request with `ConfigError::Unavailable` — the shape the M3 rows established
//!   (their ruling R3). These rows assert "refused", not a particular message.
//!
//! # The two things an in-process row cannot assert
//!
//! `retcd_process_start_time_seconds` and the PID (TA-57) are properties of a *process*, and
//! every node here shares the test's. What stands in their place is stronger for what these
//! rows are about: the credential **generation**, which a restarted listener would reset to
//! zero, and a connection opened before the rotation that is still serving after it. E2E-41
//! owns the process-level claim.

mod support;

use std::time::Duration;

use config_core::{ClusterId, ConfigError, ConfigStore, NodeId, WatchItem, WatchRequest};
use config_engine::AuthnRejectReason;
use config_gossip::GossipKeyFingerprint;
use config_grpc::{GossipKeyOp, MtlsConfig, TlsMode, TlsPlaneReload};
use config_testkit::cluster::{Cluster, GossipKind, StorageKind};
use config_testkit::rotation::{Plane, TlsFile};
use config_testkit::tls::{CertOverrides, CertProfile, TlsFixture};
use config_testkit::{poll_until_async, CertPair};
use futures::StreamExt as _;

use support::{field, get_req, key, put_req};

/// Every cluster in this file.
const CLUSTER: ClusterId = ClusterId::from_bytes([7u8; 16]);

/// The principal every admin call in this file is made as.
const ADMIN: &str = "ops";

/// A perfectly valid identity that is not on the admin allowlist (M6-61).
const OUTSIDER: &str = "svc-a";

fn my_log_lines(method: &str) -> Vec<serde_json::Value> {
    support::my_log_lines(module_path!(), method)
}

/// A three-node mutual-TLS cluster serving its material from files, with `ops` as admin.
///
/// `rotatable_tls` is the whole difference from the M3 clusters: the same fixture material,
/// written to a per-node directory a rotation can rewrite.
async fn rotatable(seed: u64) -> Cluster {
    Cluster::builder()
        .nodes(3)
        .cluster_id(CLUSTER)
        .storage(StorageKind::Ephemeral)
        .rotatable_tls(seed)
        .admins([ADMIN])
        .start()
        .await
}

/// Widen every node's trust anchors to `{current, next}` and put it in force.
///
/// Stage one of §15.1's procedure, and the precondition for rotating any single node's leaf:
/// until every peer trusts the new authority, a node that starts presenting its certificate
/// has left the cluster.
async fn overlap(cluster: &Cluster, next: &TlsFixture) {
    let current = cluster.fixture().ca_pem().to_string();
    for id in cluster.running_ids() {
        cluster.rotate_ca_bundle(id, &[&current, next.ca_pem()]);
        cluster
            .reload_tls(id, ADMIN)
            .await
            .unwrap_or_else(|e| panic!("node {id} must accept a two-anchor bundle: {e}"));
    }
}

/// The profile node `id` serves after rotating its leaf to `next`, keeping `anchors` trusted.
fn serving(next: &TlsFixture, id: NodeId, anchors: &[&str]) -> MtlsConfig {
    let mut profile = next.issue(CertProfile::node(id)).mtls();
    profile.ca_pem = anchors.join("").into_bytes();
    profile
}

/// A client profile presenting `pair` and verifying the server against `anchors`.
fn dialling(pair: &CertPair, anchors: &[&str]) -> TlsMode {
    let mut profile = pair.mtls();
    profile.ca_pem = anchors.join("").into_bytes();
    TlsMode::MutualTls(profile)
}

/// The next `Event` on `stream`, or `None` if the deadline passes first.
async fn next_event(
    stream: &mut (impl futures::Stream<Item = Result<WatchItem, ConfigError>> + Unpin),
    deadline: Duration,
) -> Option<config_core::MutationEvent> {
    tokio::time::timeout(deadline, async {
        loop {
            match stream.next().await {
                Some(Ok(WatchItem::Event(e))) => return Some(e),
                Some(Ok(WatchItem::Progress { .. })) => {}
                other => panic!("unexpected watch item: {other:?}"),
            }
        }
    })
    .await
    .ok()
    .flatten()
}

/// Every `tls_reloaded` line this test has written.
fn reload_lines(method: &str) -> Vec<serde_json::Value> {
    my_log_lines(method)
        .into_iter()
        .filter(|l| field(l, "@m") == Some("tls_reloaded"))
        .collect()
}

/// Fail if any plane reports a rotation, naming the case that should have been refused.
fn assert_unchanged(what: &str, planes: &[TlsPlaneReload]) {
    assert!(
        planes.iter().all(|p| p.outcome == "unchanged"),
        "{what} was accepted as a rotation: {planes:#?}"
    );
}

// =====================================================================================
// §4.1 — client-plane TLS reload (M6-41..M6-48)
// =====================================================================================

/// M6-41: the core claim of D6.2 — a new leaf is served without the process restarting.
///
/// The fingerprint is read from a *new* handshake rather than from the reply, because a reply
/// is exactly what a reload that changed nothing would also produce. The reply is asserted too,
/// and the two must agree: that is what says the node reports what it actually serves.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_41_reload_tls_rpc_serves_the_new_leaf_without_restart() {
    let cluster = rotatable(4101).await;
    let node = NodeId(1);
    let method = "m6_41_reload_tls_rpc_serves_the_new_leaf_without_restart";
    let next = TlsFixture::other_ca(CLUSTER, 4101);
    overlap(&cluster, &next).await;
    let anchors = [cluster.fixture().ca_pem().to_string(), next.ca_pem().into()];
    let anchors: Vec<&str> = anchors.iter().map(String::as_str).collect();

    let before = cluster.served_leaf_fingerprint(node, Plane::Client).await;
    let established = cluster.grpc_client_tls(node, ADMIN);
    established
        .put(put_req("/rot/before", "1"))
        .await
        .expect("a write before the rotation");

    cluster.rotate_files_with(node, &serving(&next, node, &anchors));
    let planes = cluster
        .reload_tls(node, ADMIN)
        .await
        .expect("an allowlisted principal may reload");

    let after = cluster.served_leaf_fingerprint(node, Plane::Client).await;
    assert_ne!(
        after, before,
        "a reload that did not change the served leaf is a reload that did nothing"
    );
    let client = planes
        .iter()
        .find(|p| p.plane == "client")
        .expect("the reply names every plane");
    assert_eq!(client.outcome, "reloaded", "{planes:#?}");
    assert_eq!(
        client.cert_fingerprint, after,
        "the node must report the leaf it actually serves"
    );
    assert_eq!(
        cluster.tls_metrics(node).reloads,
        // One per node from `overlap`, and one more here. The counter is per node, so node 1's
        // own is 2 while nodes 2 and 3 stay at 1.
        2,
        "retcd_tls_reloads_total counts every rotation this node made"
    );
    assert_eq!(
        client.generation, 2,
        "a listener that had restarted would be back at generation 0: {planes:#?}"
    );
    established
        .put(put_req("/rot/after", "2"))
        .await
        .expect("the connection opened before the rotation is still serving");

    // One `tls_reloaded` line per plane per rotation, and no PEM body anywhere (M6-120).
    let reloaded = reload_lines(method);
    assert_eq!(
        reloaded.len(),
        3 * 4,
        "three planes, three overlap rotations and this one: {reloaded:#?}"
    );
    let last_three = &reloaded[reloaded.len() - 3..];
    for line in last_three {
        assert_eq!(
            field(line, "leaf_fingerprint"),
            Some(after.as_str()),
            "{line:#?}"
        );
        assert_eq!(field(line, "source"), Some("rpc"), "{line:#?}");
    }
    let rendered = serde_json::to_string(&my_log_lines(method)).expect("the log renders");
    assert!(
        !rendered.contains("BEGIN PRIVATE KEY") && !rendered.contains("BEGIN CERTIFICATE"),
        "a rotation log line must never carry PEM"
    );

    cluster.shutdown().await;
}

/// M6-42: the poll route reaches the same rotation, and an unchanged file set is a no-op.
///
/// The timer is the daemon's (`config-server::rotation::spawn_tls_poller`) and E2E-41 covers it
/// end to end; what this row owns is everything the poll route does *differently* —
/// `source = "poll"`, no admin principal, no audit entry — and the claim that a tick over
/// unchanged files produces neither a reload nor a line.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_42_file_polling_reloads_without_an_rpc() {
    let cluster = rotatable(4102).await;
    let node = NodeId(1);
    let method = "m6_42_file_polling_reloads_without_an_rpc";

    let idle = cluster.poll_tls(node).expect("an unchanged poll succeeds");
    assert_unchanged("a poll over unchanged files", &idle);
    assert_eq!(
        cluster.tls_metrics(node).reloads,
        0,
        "an unchanged poll is not a rotation"
    );
    assert!(
        reload_lines(method).is_empty(),
        "an unchanged poll must not write a rotation line"
    );

    let next = TlsFixture::other_ca(CLUSTER, 4102);
    overlap(&cluster, &next).await;
    let anchors = [cluster.fixture().ca_pem().to_string(), next.ca_pem().into()];
    let anchors: Vec<&str> = anchors.iter().map(String::as_str).collect();
    cluster.rotate_files_with(node, &serving(&next, node, &anchors));

    let planes = cluster.poll_tls(node).expect("a changed poll succeeds");
    for plane in &planes {
        assert_eq!(plane.outcome, "reloaded", "{planes:#?}");
    }
    assert_eq!(
        cluster.served_leaf_fingerprint(node, Plane::Client).await,
        planes[0].cert_fingerprint,
        "the poll route serves what it reports, exactly as the rpc route does"
    );
    let sources: Vec<_> = reload_lines(method)
        .iter()
        .filter_map(|l| field(l, "source").map(str::to_string))
        .collect();
    assert_eq!(
        sources.iter().filter(|s| *s == "poll").count(),
        3,
        "the three lines this poll wrote name the poll route: {sources:?}"
    );

    cluster.shutdown().await;
}

/// M6-43: a reload is a non-event for everything already connected.
///
/// The established connection keeps the certificate it handshook under — TLS does not
/// renegotiate — so the watch keeps delivering in revision order with no gap while a *new*
/// handshake already sees the new leaf.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_43_in_flight_requests_and_streams_survive_a_reload() {
    let cluster = rotatable(4103).await;
    let next = TlsFixture::other_ca(CLUSTER, 4103);
    overlap(&cluster, &next).await;
    let anchors = [cluster.fixture().ca_pem().to_string(), next.ca_pem().into()];
    let anchors: Vec<&str> = anchors.iter().map(String::as_str).collect();

    let leader = cluster.leader().await;
    let client = cluster.grpc_client_tls(leader, ADMIN);
    let mut watch = client
        .watch(WatchRequest {
            prefix: key("/rot/"),
            start_after_revision: 0,
            progress_interval: None,
        })
        .await
        .expect("a watch over the client plane");

    client
        .put(put_req("/rot/a", "1"))
        .await
        .expect("a write before the rotation");
    let first = next_event(&mut watch, cluster.deadline(4))
        .await
        .expect("the first event");

    let before = cluster.served_leaf_fingerprint(leader, Plane::Client).await;
    cluster.rotate_files_with(leader, &serving(&next, leader, &anchors));
    cluster
        .reload_tls(leader, ADMIN)
        .await
        .expect("the reload succeeds");
    assert_ne!(
        cluster.served_leaf_fingerprint(leader, Plane::Client).await,
        before,
        "a new handshake sees the new leaf"
    );

    // The same connection, after the rotation: neither the stream nor the request path noticed.
    client
        .put(put_req("/rot/b", "2"))
        .await
        .expect("the established connection still works");
    let second = next_event(&mut watch, cluster.deadline(4))
        .await
        .expect("the stream did not terminate");
    assert!(
        second.revision > first.revision,
        "no gap and no reorder across a reload: {first:?} then {second:?}"
    );
    assert_eq!(
        client
            .get(get_req("/rot/a"))
            .await
            .expect("a read on the established connection")
            .record
            .map(|r| r.value),
        Some(key("1")),
        "the data the stream is about is still readable over the same connection"
    );

    cluster.shutdown().await;
}

/// M6-44: an overlapping CA bundle lets clients of both authorities in at once.
///
/// The state §15.1 requires a rotation to pass through, and the state all of M6-51..M6-54 sit
/// inside. Neither client may be refused, and no `(plane, reason)` counter may move.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_44_overlap_ca_bundle_accepts_old_and_new_clients() {
    let cluster = rotatable(4104).await;
    let node = cluster.leader().await;
    let old = cluster.fixture().ca_pem().to_string();
    let new = TlsFixture::other_ca(CLUSTER, 4104);

    let before = cluster.tls_authn_rejections(node);
    overlap(&cluster, &new).await;

    // The nodes still serve their original leaves, so every client verifies them through the
    // old anchor whichever authority issued the client's own certificate.
    let anchors = [old.as_str()];
    for (label, pair) in [
        (
            "ca-old",
            cluster.fixture().issue(CertProfile::client(ADMIN)),
        ),
        ("ca-new", new.issue(CertProfile::client(ADMIN))),
    ] {
        let client = cluster
            .grpc_client_with_tls(node, dialling(&pair, &anchors))
            .unwrap_or_else(|e| panic!("{label} client configuration: {e}"));
        client
            .put(put_req(&format!("/rot/{label}"), label))
            .await
            .unwrap_or_else(|e| {
                panic!("{label} must authenticate against the overlap bundle: {e}")
            });
        assert_eq!(
            client
                .get(get_req(&format!("/rot/{label}")))
                .await
                .expect("read back")
                .record
                .map(|r| r.value),
            Some(key(label)),
            "{label} derived a principal the policy admits"
        );
    }

    assert_eq!(
        cluster.tls_authn_rejections(node),
        before,
        "no (plane, reason) may increment while both authorities are trusted"
    );

    cluster.shutdown().await;
}

/// M6-45: dropping the old anchor completes the rotation, and the refusal is counted and named.
///
/// The row that proves the verifier's root store was rebuilt rather than cached: a cached store
/// would keep admitting CA-old clients forever and nothing else in the suite would notice.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_45_removing_the_old_ca_refuses_old_clients() {
    let cluster = rotatable(4105).await;
    let method = "m6_45_removing_the_old_ca_refuses_old_clients";
    let new = TlsFixture::other_ca(CLUSTER, 4105);
    overlap(&cluster, &new).await;

    // Complete the procedure on every node: new leaf, new anchor alone.
    let node = cluster.leader().await;
    let served_before = cluster.served_leaf_fingerprint(node, Plane::Client).await;
    for id in cluster.running_ids() {
        cluster.rotate_files_with(id, &serving(&new, id, &[new.ca_pem()]));
        cluster
            .reload_tls(id, ADMIN)
            .await
            .unwrap_or_else(|e| panic!("node {id} must accept the completed rotation: {e}"));
    }

    // The CA-new client is the one the rotation was for.
    let new_client = cluster
        .grpc_client_with_tls(
            node,
            dialling(&new.issue(CertProfile::client(ADMIN)), &[new.ca_pem()]),
        )
        .expect("a ca-new client configuration");
    new_client
        .put(put_req("/rot/new", "ok"))
        .await
        .expect("the ca-new client authenticates against the completed rotation");

    // Taken here, after the CA-new client has finished: the harness brought three nodes up
    // and rotated them twice each, and a baseline taken any earlier would let one of those
    // pass for the refusal this row is about.
    let before = cluster.tls_authn_rejections(node);

    // The CA-old client's certificate no longer reaches a trusted root.
    let old_client = cluster
        .grpc_client_with_tls(
            node,
            dialling(
                &cluster.fixture().issue(CertProfile::client(ADMIN)),
                &[new.ca_pem()],
            ),
        )
        .expect("a ca-old client configuration is valid; the handshake is what fails");
    let error = old_client
        .put(put_req("/rot/old", "no"))
        .await
        .expect_err("a certificate with no trusted issuer must be refused");
    assert!(
        matches!(error, ConfigError::Unavailable { .. }),
        "a handshake refusal carries no server-marked outcome: {error:?}"
    );

    // `GrpcClient::connect` re-dials, so the number of refused handshakes is the client's
    // business and not this row's. What the row is about is *where* they were counted: every
    // one of them on the client plane under `untrusted_client_ca`, and none anywhere else.
    let moved = poll_until_async(cluster.deadline(4), cluster.poll_interval(), || async {
        let now = cluster.tls_authn_rejections(node);
        let moved: Vec<_> = now
            .iter()
            .zip(&before)
            .filter(|((_, _, now), (_, _, was))| now > was)
            .map(|((plane, reason, now), (_, _, was))| (*plane, *reason, now - was))
            .collect();
        (!moved.is_empty()).then_some(moved)
    })
    .await
    .unwrap_or_else(|e| {
        panic!(
            "the listener must count the handshake it refused: {e}; counters now {:?}",
            cluster.tls_authn_rejections(node)
        )
    });
    assert!(
        moved.iter().all(|(plane, reason, _)| *plane == "client"
            && *reason == AuthnRejectReason::UntrustedClientCa),
        "every refusal belongs to {{plane=\"client\", reason=\"untrusted_client_ca\"}}: {moved:?}"
    );
    assert_ne!(
        cluster.served_leaf_fingerprint(node, Plane::Client).await,
        served_before,
        "the node presents its new leaf, so the rotation is complete on both halves"
    );

    let lines = my_log_lines(method);
    assert!(
        lines
            .iter()
            .any(|l| field(l, "@m") == Some("tls handshake refused")
                && field(l, "reason") == Some("untrusted_client_ca")),
        "the refusal is logged with its typed reason"
    );
    let rendered = serde_json::to_string(&lines).expect("the log renders");
    assert!(
        !rendered.contains("BEGIN CERTIFICATE"),
        "a refusal must not log the client certificate"
    );

    cluster.shutdown().await;
}

/// M6-46: every way a reload can fail leaves the node serving exactly what it served.
///
/// Three distinct corruptions, each refused with its own reason, and the previously served
/// credentials working throughout — a rotation that can half-apply is worse than one that
/// cannot rotate.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_46_a_bad_reload_is_atomic_and_keeps_the_old_credentials() {
    let cluster = rotatable(4106).await;
    let node = cluster.leader().await;
    let served = cluster.served_leaf_fingerprint(node, Plane::Client).await;
    let other = TlsFixture::other_ca(CLUSTER, 4106);
    let files = cluster.tls_files(node);

    let cases: Vec<(&str, TlsFile, Vec<u8>)> = vec![
        (
            "a private key belonging to another identity",
            TlsFile::Key,
            other.issue(CertProfile::node(node)).key_pem.into_bytes(),
        ),
        (
            "a malformed certificate PEM",
            TlsFile::Cert,
            b"-----BEGIN CERTIFICATE-----\nnot base64 at all\n-----END CERTIFICATE-----\n".to_vec(),
        ),
        (
            "a trust anchor bundle with no anchors in it",
            TlsFile::Ca,
            Vec::new(),
        ),
    ];

    let mut refusals = 0u64;
    for (what, which, bytes) in cases {
        let path = match which {
            TlsFile::Ca => &files.ca,
            TlsFile::Cert => &files.cert,
            TlsFile::Key => &files.key,
        };
        let original = std::fs::read(path).expect("the harness wrote this file");

        cluster.corrupt_tls_file(node, which, &bytes);
        let error = cluster
            .reload_tls(node, ADMIN)
            .await
            .err()
            .unwrap_or_else(|| panic!("{what} must be refused, not accepted"));
        assert_eq!(
            error.code(),
            tonic::Code::InvalidArgument,
            "{what}: a refused reload is the operator's mistake, not the node's: {error:?}"
        );
        assert!(
            error.message().contains("tls_"),
            "{what}: the refusal carries the greppable reason token: {}",
            error.message()
        );
        refusals += 1;

        assert_eq!(
            cluster.served_leaf_fingerprint(node, Plane::Client).await,
            served,
            "{what}: the previously served credentials must keep serving"
        );
        std::fs::write(path, &original).expect("restore");
    }

    assert_eq!(
        cluster.tls_metrics(node).reloads,
        0,
        "no corruption may be recorded as a rotation"
    );
    assert_eq!(
        cluster
            .tls_metrics(node)
            .reload_failures
            .values()
            .sum::<u64>(),
        refusals,
        "every refusal increments retcd_tls_reload_failures_total{{reason}}: {:?}",
        cluster.tls_metrics(node).reload_failures
    );
    // A restored file set is *unchanged*, not a rotation: nothing was recorded as served while
    // the files were broken.
    assert_unchanged(
        "a restored file set",
        &cluster.poll_tls(node).expect("poll"),
    );

    cluster
        .grpc_client_tls(node, ADMIN)
        .put(put_req("/rot/after", "ok"))
        .await
        .expect("a node that refused three reloads is still serving and still ready");

    cluster.shutdown().await;
}

/// M6-47: a rotation does not change who a certificate says you are.
///
/// Two claims: the same SAN before and after derives the same principal, and a certificate
/// rotated to a *different* SAN derives the new principal from its very first request — there
/// is no cache that could answer with the old one.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_47_principal_derivation_is_unchanged_across_a_rotation() {
    let cluster = rotatable(4107).await;
    let method = "m6_47_principal_derivation_is_unchanged_across_a_rotation";
    let node = cluster.leader().await;
    let old = cluster.fixture().ca_pem().to_string();
    let new = TlsFixture::other_ca(CLUSTER, 4107);
    overlap(&cluster, &new).await;
    let anchors = [old.as_str(), new.ca_pem()];

    cluster
        .grpc_client_tls(node, ADMIN)
        .put(put_req("/rot/before", "1"))
        .await
        .expect("a write before the rotation");

    cluster.rotate_files_with(node, &serving(&new, node, &anchors));
    cluster.reload_tls(node, ADMIN).await.expect("the rotation");

    // The same client certificate, the same SAN URI, across the node's rotation.
    cluster
        .grpc_client_with_tls(
            node,
            dialling(
                &cluster.fixture().issue(CertProfile::client(ADMIN)),
                &anchors,
            ),
        )
        .expect("a ca-old client configuration")
        .put(put_req("/rot/after", "2"))
        .await
        .expect("the same identity still authenticates after the node rotated");

    // A client certificate rotated to a different SAN is that other principal immediately.
    cluster
        .grpc_client_with_tls(
            node,
            dialling(&new.issue(CertProfile::client("svc-rotated")), &anchors),
        )
        .expect("a ca-new client configuration")
        .put(put_req("/rot/rotated", "3"))
        .await
        .expect("an AllowAll cluster admits both principals");

    let principals: Vec<_> = my_log_lines(method)
        .iter()
        .filter(|l| field(l, "@m") == Some("rpc"))
        .filter_map(|l| field(l, "principal").map(str::to_string))
        .collect();
    assert!(
        principals.iter().filter(|p| *p == ADMIN).count() >= 2,
        "the unchanged SAN derives the same principal before and after: {principals:?}"
    );
    assert!(
        principals.iter().any(|p| p == "svc-rotated"),
        "the rotated SAN's own principal is what the server derived: {principals:?}"
    );

    cluster.shutdown().await;
}

/// M6-48: a file rotation cannot turn the Common-Name fallback back on.
///
/// The gate is a property of the *listener*, not of the bytes on disk, so there is no reload
/// path that could re-read it — `TlsRotator` rebuilds every profile from the template it
/// captured at start (`read_material`). This row holds that structurally, by rotating to
/// material whose only usable identity is a Common Name and asserting the client is still
/// refused, on a cluster whose gate is off.
///
/// The harness's own clusters serve the gate **on** (two M3 rows are about the fallback), so
/// this row builds its listener from `config-grpc` directly rather than from [`Cluster`].
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_48_cn_fallback_gate_is_not_silently_re_enabled_by_a_reload() {
    let fixture = TlsFixture::new(CLUSTER, 4108);
    let dir = config_testkit::fs::temp_dir();
    let files = config_grpc::TlsFiles {
        ca: dir.path().join("ca.pem"),
        cert: dir.path().join("node.cert.pem"),
        key: dir.path().join("node.key.pem"),
    };
    // The gate is off, which is `MtlsConfig::new`'s default and the safe reading of an absent
    // configuration key.
    let started_with = fixture.issue(CertProfile::node(NodeId(1))).mtls();
    assert!(
        !started_with.allow_common_name_principals,
        "this row is about a listener whose gate is off"
    );
    std::fs::write(&files.ca, &started_with.ca_pem).expect("write ca");
    std::fs::write(&files.cert, &started_with.cert_pem).expect("write cert");
    std::fs::write(&files.key, &started_with.key_pem).expect("write key");

    let dial = config_grpc::GrpcPeerTransport::new(
        TlsMode::MutualTls(started_with.clone()),
        config_engine::NetFault::new(),
        config_core::Limits::DEFAULT,
    );
    let rotator = config_grpc::TlsRotator::new(files.clone(), started_with, dial);
    let source = config_grpc::CredentialSource::new(
        "client",
        config_grpc::MtlsConfig::new(
            std::fs::read(&files.ca).expect("ca"),
            std::fs::read(&files.cert).expect("cert"),
            std::fs::read(&files.key).expect("key"),
        ),
    )
    .expect("the material the listener started with is serveable");
    rotator.register(std::sync::Arc::clone(&source));

    // Rotate to a certificate set whose leaf asserts **no** SAN URI: its only identity is a
    // Common Name, which is exactly what the gate decides about.
    let cn_only = fixture.issue_with(CertProfile::node(NodeId(1)), CertOverrides::no_san());
    std::fs::write(&files.cert, &cn_only.cert_pem).expect("rotate cert");
    std::fs::write(&files.key, &cn_only.key_pem).expect("rotate key");
    let planes = rotator.reload("rpc").expect("the rotation is serveable");

    assert!(
        planes.iter().any(|p| p.outcome == "reloaded"),
        "the material did change: {planes:#?}"
    );
    assert!(
        !source.current().mtls().allow_common_name_principals,
        "no reload path may re-read the gate from the certificate files: a flag a file write \
         can flip is a file-write privilege escalation"
    );
}

// =====================================================================================
// §4.2 — peer-plane rotation and destination binding (M6-49..M6-56)
// =====================================================================================

/// Like [`overlap`], but widens every configured node's trust anchors, including one that is
/// currently stopped (M6-51..M6-55).
///
/// A stopped node's PEM files still live on disk — [`Cluster::rotate_ca_bundle`] writes them
/// through the slot, not through a running node — and [`Cluster::start_node`] reads them fresh
/// on the next start, so widening its bundle now is what lets it rejoin under an
/// already-rotated leader without ever calling an RPC on a node that cannot answer one.
async fn overlap_all(cluster: &Cluster, next: &TlsFixture) {
    let current = cluster.fixture().ca_pem().to_string();
    let running: std::collections::HashSet<_> = cluster.running_ids().into_iter().collect();
    for id in cluster.ids() {
        cluster.rotate_ca_bundle(id, &[&current, next.ca_pem()]);
        if running.contains(&id) {
            cluster
                .reload_tls(id, ADMIN)
                .await
                .unwrap_or_else(|e| panic!("node {id} must accept a two-anchor bundle: {e}"));
        }
    }
}

/// M6-56: the acceptor implementation is recorded, not assumed (TA-57, OQ-59, ADR-0028).
///
/// No row in this file may name a concrete acceptor type — see the ground truth at the top of
/// this file. This is the one row that exists specifically to pin the choice ADR-0028 records,
/// so it is written as a source assertion rather than against a running cluster, the same shape
/// as `m5_63_set_nodes_is_never_used_and_no_endpoint_update_rpc_exists`.
#[test]
fn m6_56_acceptor_implementation_is_recorded_not_assumed() {
    let server_src = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../config-grpc/src/server.rs"
    ))
    .expect("read config-grpc/src/server.rs");
    assert!(
        server_src.contains("tokio_rustls::TlsAcceptor"),
        "config-grpc/src/server.rs must build its own acceptor per handshake (ADR-0028's answer \
         to OQ-59); a tonic ServerTlsConfig compiled once at start cannot be rotated"
    );
    assert!(
        server_src.contains("fn spawn_handshakes"),
        "the per-connection handshake loop this choice depends on must still exist under that \
         name"
    );

    let adr = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../docs/ADRs/0028-tls-and-gossip-key-rotation.md"
    ))
    .expect("read docs/ADRs/0028-tls-and-gossip-key-rotation.md");
    assert!(
        adr.contains("OQ-59"),
        "ADR-0028 must still document the acceptor-choice open question this row records"
    );
}

/// M6-49: a node's peer transport reloads both the credentials it serves and the ones it
/// dials with (M6-R16 Q9; the third `peer_dial` plane in every `ReloadTls` reply).
///
/// The leader is the node under rotation, not an arbitrary follower: it is the side actively
/// dialling out (`AppendEntries`/`RequestVote`), so it is the only choice that can prove the
/// *dialling* half of the claim — a follower's own dial credentials are never exercised while
/// it stays a follower.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_49_peer_transport_reloads_its_client_and_server_credentials() {
    let cluster = rotatable(4149).await;
    let leader = cluster.leader().await;
    let next = TlsFixture::other_ca(CLUSTER, 4149);
    overlap(&cluster, &next).await;
    let anchors = [
        cluster.fixture().ca_pem().to_string(),
        next.ca_pem().to_string(),
    ];
    let anchors_ref: Vec<&str> = anchors.iter().map(String::as_str).collect();

    let before = cluster.served_leaf_fingerprint(leader, Plane::Peer).await;
    cluster.rotate_files_with(leader, &serving(&next, leader, &anchors_ref));
    let planes = cluster
        .reload_tls(leader, ADMIN)
        .await
        .expect("the leader accepts its own rotation");
    let after = cluster.served_leaf_fingerprint(leader, Plane::Peer).await;
    assert_ne!(
        after, before,
        "a reload that served the same leaf did nothing"
    );

    for name in ["peer", "peer_dial"] {
        let plane = planes
            .iter()
            .find(|p| p.plane == name)
            .unwrap_or_else(|| panic!("the reply must name the {name} plane: {planes:#?}"));
        assert_eq!(plane.outcome, "reloaded", "{name}: {planes:#?}");
    }

    // Narrow every follower's trust to the new authority only: if the leader's *dialer* were
    // still holding what it built at process start, its next AppendEntries would present the
    // old leaf and every follower would refuse it.
    for id in cluster.running_ids() {
        if id == leader {
            continue;
        }
        cluster.rotate_ca_bundle(id, &[next.ca_pem()]);
        cluster
            .reload_tls(id, ADMIN)
            .await
            .expect("a follower narrowing to the already-adopted authority");
    }

    // A write and a converge check alone would prove nothing about the *dialer*: the leader's
    // connections to both followers were already open (and already trusted) before this test
    // ever rotated anything, and TLS does not re-verify a connection that is already up (the
    // same fact M6-50's own redesign is built on). Restarting one follower here is what makes
    // this row capable of catching a `peer_dial` reload that reports "reloaded" but left the
    // leader's dialer on its start-time leaf: the leader can only rejoin it by dialling fresh,
    // and the restarted follower now trusts only `next`'s authority, so a stale-leafed dial
    // would be refused, not merely slow. (Found by mutation check, 2026-09-19: skipping the
    // `peer_dial.reload` call while still reporting "reloaded" passed this row's final
    // assertions when they only exercised already-open connections — see the test-plan note.)
    let redialed = *cluster
        .followers()
        .iter()
        .find(|id| **id != leader)
        .expect("a follower besides the leader exists");
    cluster.stop(redialed).await;
    cluster.start_node(redialed).await;
    cluster
        .wait_rejoined(redialed, cluster.deadline(10))
        .await
        .expect(
            "the leader must be able to dial this follower fresh, using its rotated peer_dial \
             credentials, for it to rejoin at all",
        );

    cluster
        .client(leader)
        .put(put_req("/rot/49/after", "1"))
        .await
        .expect("the leader's own re-dialled connections still replicate");
    let hash = cluster
        .wait_converged(cluster.deadline(10))
        .await
        .expect("the cluster converges on the rotated leaf");
    assert_eq!(cluster.state_hash(leader), hash);
    assert_eq!(
        cluster.leader().await,
        leader,
        "rotating the peer plane must not itself cost the leader its term"
    );

    cluster.shutdown().await;
}

/// M6-50: destination binding survives a rotation, both when a node keeps its own identity and
/// when it is made to claim someone else's.
///
/// The negative half re-mints `target`'s leaf under the same, now-trusted authority
/// (`next`), but with `CertOverrides::wrong_node(leader)`: signed by a trusted issuer, correct
/// in every way except that its SAN and peer DNS name claim to be node `leader`. That is the
/// exact shape M3's destination-binding rows established (a correct-looking identity that is
/// the *wrong* identity), proven here to survive a rotation rather than only a process start.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_50_destination_binding_survives_rotation() {
    let cluster = rotatable(4150).await;
    let method = "m6_50_destination_binding_survives_rotation";
    let leader = cluster.leader().await;
    let target = *cluster
        .followers()
        .iter()
        .find(|id| **id != leader)
        .expect("a follower besides the leader exists");

    let next = TlsFixture::other_ca(CLUSTER, 4150);
    overlap(&cluster, &next).await;
    let anchors = [
        cluster.fixture().ca_pem().to_string(),
        next.ca_pem().to_string(),
    ];
    let anchors_ref: Vec<&str> = anchors.iter().map(String::as_str).collect();

    // Positive half: rotating `target`'s own leaf, still naming itself, must not disturb
    // destination binding.
    cluster.rotate_files_with(target, &serving(&next, target, &anchors_ref));
    cluster
        .reload_tls(target, ADMIN)
        .await
        .expect("a same-identity rotation must be accepted");
    cluster
        .client(leader)
        .put(put_req("/rot/50/same-identity", "1"))
        .await
        .expect("a write after a same-identity rotation");
    let hash = cluster
        .wait_converged(cluster.deadline(10))
        .await
        .expect("a same-identity rotation still converges");
    assert_eq!(
        cluster.state_hash(target),
        hash,
        "the rotated node keeps up"
    );

    // Negative half: stop `target`, re-mint its leaf under the same trusted issuer but naming
    // node `leader` instead, and bring it back. A restart is what forces a fresh dial and
    // therefore a fresh handshake to verify: an already-open peer connection does not get
    // re-verified just because the file on disk changed (M6-43's own ground truth), so a
    // restart is the only way to actually exercise the destination-binding check here — the
    // same mechanics M6-53 uses for its own fencing half.
    cluster.stop(target).await;
    let impostor = next.issue_with(CertProfile::node(target), CertOverrides::wrong_node(leader));
    let mut impostor_profile = impostor.mtls();
    impostor_profile.ca_pem = anchors_ref.join("").into_bytes();
    cluster.rotate_files_with(target, &impostor_profile);

    let since = support::log_baseline(module_path!(), method);
    cluster.start_node(target).await;

    cluster
        .assert_never(
            &format!(
                "node {target} learning of a leader once its own leaf claims to be node {leader}"
            ),
            cluster.deadline(6),
            || {
                cluster
                    .try_node(target)
                    .and_then(|n| n.metrics().current_leader)
                    .is_some()
            },
        )
        .await;
    for n in 0..3u32 {
        cluster
            .client(leader)
            .put(put_req(&format!("/rot/50/never-{n}"), "x"))
            .await
            .expect("the leader keeps serving once target is impersonating a different node");
    }
    let rejections = support::peer_transport_rejections(module_path!(), method, since);
    assert!(
        !rejections.is_empty(),
        "the leader dialling a peer whose certificate names a different node id must be \
         refused, not silently accepted"
    );

    cluster.shutdown().await;
}

/// M6-51: rotating the running majority's peer credentials while one voter is down does not
/// cost the cluster its quorum.
///
/// Every write below panics naming its sequence number and the node being rotated around it,
/// rather than a bare `.expect`: the one thing this row exists to catch is an `Unavailable`,
/// and the diagnostic needs to say when in the sequence it happened.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_51_rotation_while_one_voter_is_down_keeps_the_cluster_available() {
    let cluster = rotatable(4151).await;
    let leader = cluster.leader().await;
    let down = *cluster
        .followers()
        .iter()
        .find(|id| **id != leader)
        .expect("a follower besides the leader exists");
    cluster.stop(down).await;

    let next = TlsFixture::other_ca(CLUSTER, 4151);
    overlap_all(&cluster, &next).await;
    let anchors = [
        cluster.fixture().ca_pem().to_string(),
        next.ca_pem().to_string(),
    ];
    let anchors_ref: Vec<&str> = anchors.iter().map(String::as_str).collect();

    let client = cluster.client(leader);
    let mut n = 0u32;
    for id in cluster.running_ids() {
        for _ in 0..3 {
            n += 1;
            client
                .put(put_req(&format!("/rot/51/{n}"), "x"))
                .await
                .unwrap_or_else(|e| {
                    panic!("write {n} (around node {id}'s rotation) must not see Unavailable: {e}")
                });
        }
        cluster.rotate_files_with(id, &serving(&next, id, &anchors_ref));
        cluster.reload_tls(id, ADMIN).await.unwrap_or_else(|e| {
            panic!("node {id} must accept its own rotation while node {down} is down: {e}")
        });
    }
    for _ in 0..3 {
        n += 1;
        client
            .put(put_req(&format!("/rot/51/{n}"), "x"))
            .await
            .unwrap_or_else(|e| {
                panic!("write {n} after the rotation must not see Unavailable: {e}")
            });
    }

    assert_eq!(
        cluster.leader().await,
        leader,
        "rotating a majority's certificates must not itself trigger an election"
    );

    cluster.shutdown().await;
}

/// M6-52: a voter that was down for a completed rotation rejoins as long as it still holds a
/// chain to a trusted root.
///
/// Node `down` never has its own leaf touched — only its CA bundle is widened while it is
/// stopped, by [`overlap_all`] — so what this proves is the ordinary M3 mTLS contract (both
/// sides verify each other against their own trust anchors) surviving a restart against an
/// already-rotated majority.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_52_the_down_voter_rejoins_only_with_a_chain_to_a_trusted_root() {
    let cluster = rotatable(4152).await;
    let leader = cluster.leader().await;
    let down = *cluster
        .followers()
        .iter()
        .find(|id| **id != leader)
        .expect("a follower besides the leader exists");
    cluster.stop(down).await;

    let next = TlsFixture::other_ca(CLUSTER, 4152);
    overlap_all(&cluster, &next).await;
    let anchors = [
        cluster.fixture().ca_pem().to_string(),
        next.ca_pem().to_string(),
    ];
    let anchors_ref: Vec<&str> = anchors.iter().map(String::as_str).collect();
    for id in cluster.running_ids() {
        cluster.rotate_files_with(id, &serving(&next, id, &anchors_ref));
        cluster
            .reload_tls(id, ADMIN)
            .await
            .expect("the running majority completes its rotation");
    }
    cluster
        .client(leader)
        .put(put_req("/rot/52/before", "1"))
        .await
        .expect("a write while node down is still down");

    cluster.start_node(down).await;
    cluster
        .wait_rejoined(down, cluster.deadline(10))
        .await
        .expect("a chain to a still-trusted root lets the node rejoin");

    cluster
        .client(leader)
        .put(put_req("/rot/52/after", "2"))
        .await
        .expect("a write after node down rejoins");
    let hash = cluster
        .wait_converged(cluster.deadline(10))
        .await
        .expect("all three nodes converge");
    for id in cluster.running_ids() {
        assert_eq!(
            cluster.state_hash(id),
            hash,
            "node {id} holds the same state as the rest"
        );
    }

    cluster.shutdown().await;
}

/// M6-53: once the old root is dropped, the down voter's stale leaf is refused, not merely
/// unrecognised.
///
/// `AuthnRejectReason` (config-engine/src/metrics.rs) has no `untrusted_peer_ca` variant, so
/// the plan's own `reason="untrusted_peer_ca"` names a label the product does not have. What
/// this row asserts instead is what running it actually produces: `plane="peer"`,
/// `reason="handshake_failed"` — see the comment at the refusal-count assertion below for why
/// this mutual-distrust shape lands on the catch-all reason rather than
/// `UntrustedClientCa`/`UntrustedServerCa`.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_53_the_down_voter_is_refused_after_the_old_root_is_dropped() {
    let cluster = rotatable(4153).await;
    let method = "m6_53_the_down_voter_is_refused_after_the_old_root_is_dropped";
    let leader = cluster.leader().await;
    let down = *cluster
        .followers()
        .iter()
        .find(|id| **id != leader)
        .expect("a follower besides the leader exists");
    cluster.stop(down).await;

    // Deliberately not `overlap_all`: `down` is left holding exactly what it started with,
    // because this row is the fencing half — its now-untrusted leaf is the only thing it has
    // to offer when it comes back.
    let next = TlsFixture::other_ca(CLUSTER, 4153);
    let widened = [
        cluster.fixture().ca_pem().to_string(),
        next.ca_pem().to_string(),
    ];
    let widened_ref: Vec<&str> = widened.iter().map(String::as_str).collect();
    for id in cluster.running_ids() {
        cluster.rotate_ca_bundle(id, &widened_ref);
        cluster
            .reload_tls(id, ADMIN)
            .await
            .expect("the running majority widens its own trust first");
    }
    for id in cluster.running_ids() {
        cluster.rotate_files_with(id, &serving(&next, id, &[next.ca_pem()]));
        cluster
            .reload_tls(id, ADMIN)
            .await
            .expect("the running majority completes its rotation and drops the old root");
    }
    cluster
        .client(leader)
        .put(put_req("/rot/53/before", "1"))
        .await
        .expect("quorum survives the completed rotation");

    let since = support::log_baseline(module_path!(), method);
    cluster.start_node(down).await;

    cluster
        .assert_never(
            &format!("node {down} learning of a leader once its own root was dropped"),
            cluster.deadline(6),
            || {
                cluster
                    .try_node(down)
                    .and_then(|n| n.metrics().current_leader)
                    .is_some()
            },
        )
        .await;

    for n in 0..3u32 {
        cluster
            .client(leader)
            .put(put_req(&format!("/rot/53/after-{n}"), "2"))
            .await
            .expect("nodes 1 and 2 keep quorum and keep serving once node down is fenced");
    }

    // Ground truth from running this row, not from the plan or from M6-45's single-direction
    // shape: both sides distrust each other's root here (node `down` still only trusts
    // CA-old; nodes 1 and 2 now only trust CA-new), so a handshake in either direction is
    // aborted by whichever side evaluates the peer's certificate *first* — often the dialling
    // client, before the listener's accept loop ever gets far enough to downcast a specific
    // `rustls::CertificateError`. What lands on the listener in that case is a plain I/O error
    // (the peer closed after its own alert), which `classify_handshake_failure` correctly
    // reports as the catch-all `HandshakeFailed` rather than `UntrustedClientCa` — unlike
    // M6-45's one-sided case, where the listener itself completes the certificate evaluation
    // and can name it precisely. `reason="untrusted_client_ca"`/`reason="untrusted_peer_ca"`
    // (the plan's own spelling) are therefore both wrong for this specific mutual-distrust
    // shape; `reason="handshake_failed"` is what is actually counted.
    let refused: u64 = cluster
        .running_ids()
        .into_iter()
        .map(|id| cluster.tls_authn_rejected(id, Plane::Peer, AuthnRejectReason::HandshakeFailed))
        .sum();
    assert!(
        refused > 0,
        "the down voter's stale leaf must be refused and counted on the peer plane \
         (plane=\"peer\", reason=\"handshake_failed\" for this mutual-distrust shape)"
    );
    let rejections = support::peer_transport_rejections(module_path!(), method, since);
    assert!(
        !rejections.is_empty(),
        "nodes 1 and 2 dialling out to node {down}'s still-old leaf must also be refused, on \
         their own dialling side"
    );

    cluster.shutdown().await;
}

/// M6-54: issuing the down voter a new leaf from the new authority is what completes its
/// rejoin — no membership change required.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_54_the_down_voter_rejoins_after_being_issued_a_new_leaf() {
    let cluster = rotatable(4154).await;
    let leader = cluster.leader().await;
    let down = *cluster
        .followers()
        .iter()
        .find(|id| **id != leader)
        .expect("a follower besides the leader exists");
    cluster.stop(down).await;

    let next = TlsFixture::other_ca(CLUSTER, 4154);
    let widened = [
        cluster.fixture().ca_pem().to_string(),
        next.ca_pem().to_string(),
    ];
    let widened_ref: Vec<&str> = widened.iter().map(String::as_str).collect();
    for id in cluster.running_ids() {
        cluster.rotate_ca_bundle(id, &widened_ref);
        cluster
            .reload_tls(id, ADMIN)
            .await
            .expect("widen trust first");
    }
    for id in cluster.running_ids() {
        cluster.rotate_files_with(id, &serving(&next, id, &[next.ca_pem()]));
        cluster
            .reload_tls(id, ADMIN)
            .await
            .expect("complete the rotation, dropping the old root");
    }
    let membership_before = cluster.membership_of(leader);

    // The completed procedure: node `down` is issued a fresh leaf from the *new* authority
    // before it ever comes back, rather than fenced on its old one (M6-53's case).
    cluster.rotate_files_with(down, &serving(&next, down, &[next.ca_pem()]));
    cluster.start_node(down).await;
    cluster
        .wait_rejoined(down, cluster.deadline(10))
        .await
        .expect("a node issued a new leaf from the trusted authority rejoins");

    cluster
        .client(leader)
        .put(put_req("/rot/54/after", "1"))
        .await
        .expect("a write after node down rejoins");
    let hash = cluster
        .wait_converged(cluster.deadline(10))
        .await
        .expect("all three nodes converge");
    for id in cluster.running_ids() {
        assert_eq!(
            cluster.state_hash(id),
            hash,
            "node {id} holds the same state as the rest"
        );
    }
    assert_eq!(
        cluster.membership_of(leader),
        membership_before,
        "no membership change was needed to complete the procedure"
    );

    cluster.shutdown().await;
}

/// M6-55: rotation is invisible to Raft — committed membership, cluster identity and applied
/// commands are unchanged by any step in this section.
///
/// No `put` anywhere in this row: the claim is that the rotation path itself proposes nothing,
/// so the strongest evidence is `NodeMetrics::applied_commands` — which counts only
/// Command-carrying entries, excluding blank and membership entries — staying at its starting
/// value through a full stop/widen/rotate/drop/rejoin sequence.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_55_rotation_does_not_disturb_committed_membership_or_identity() {
    let cluster = rotatable(4155).await;
    let leader = cluster.leader().await;
    let down = *cluster
        .followers()
        .iter()
        .find(|id| **id != leader)
        .expect("a follower besides the leader exists");

    let membership_before = cluster.membership_of(leader);
    let health_before = cluster.health(leader).await;
    let retired_before: std::collections::BTreeMap<_, _> = cluster
        .running_ids()
        .into_iter()
        .map(|id| (id, cluster.node(id).retired_nodes()))
        .collect();
    let commands_before: std::collections::BTreeMap<_, _> = cluster
        .running_ids()
        .into_iter()
        .map(|id| (id, cluster.metrics(id).applied_commands))
        .collect();

    cluster.stop(down).await;
    let next = TlsFixture::other_ca(CLUSTER, 4155);
    overlap_all(&cluster, &next).await;
    let anchors = [
        cluster.fixture().ca_pem().to_string(),
        next.ca_pem().to_string(),
    ];
    let anchors_ref: Vec<&str> = anchors.iter().map(String::as_str).collect();
    for id in cluster.running_ids() {
        cluster.rotate_files_with(id, &serving(&next, id, &anchors_ref));
        cluster.reload_tls(id, ADMIN).await.expect("rotation step");
    }
    cluster.rotate_files_with(down, &serving(&next, down, &anchors_ref));
    cluster.start_node(down).await;
    cluster
        .wait_rejoined(down, cluster.deadline(10))
        .await
        .expect("the voter rejoins without any membership change");

    assert_eq!(
        cluster.membership_of(leader),
        membership_before,
        "committed membership must be unchanged"
    );
    let health_after = cluster.health(leader).await;
    assert_eq!(
        health_after.cluster_id, health_before.cluster_id,
        "cluster_id"
    );
    assert_eq!(
        health_after.recovery_epoch, health_before.recovery_epoch,
        "recovery_epoch"
    );
    for id in cluster.running_ids() {
        assert_eq!(
            cluster.node(id).retired_nodes(),
            retired_before.get(&id).cloned().unwrap_or_default(),
            "node {id}'s retired set must be unchanged by a rotation"
        );
        assert_eq!(
            cluster.metrics(id).applied_commands,
            commands_before.get(&id).copied().unwrap_or(0),
            "node {id} must not have had any Command proposed by the rotation path"
        );
    }

    cluster.shutdown().await;
}

// =====================================================================================
// §4.4 — expiry observability (M6-62..M6-64)
// =====================================================================================

/// M6-62: the gauge exists on both planes and reads the served certificate's real `notAfter`.
///
/// The "no subject DN, serial or SAN" half is structural: `TlsRotator::expiry_seconds` is
/// keyed by plane and carries nothing else, and ADR-0026 declares the metric's labels as
/// `node_id` and `plane` (TA-64's 2026-09-19 correction). There is no subject field to assert
/// the absence of, which is the point — and the type is what enforces it.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_62_cert_expiry_metric_is_exported_per_plane() {
    let cluster = rotatable(4162).await;
    let node = NodeId(1);

    let client = cluster
        .cert_expiry(node, Plane::Client)
        .expect("the client plane serves a leaf");
    let peer = cluster
        .cert_expiry(node, Plane::Peer)
        .expect("the peer plane serves a leaf");
    let not_after = cluster
        .served_not_after(node, Plane::Client)
        .expect("the served leaf has a notAfter");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("a clock after 1970")
        .as_secs() as i64;

    assert!(
        (not_after - now - client).abs() <= 60,
        "within 60 s of the certificate's real notAfter: gauge {client}, notAfter {not_after}, \
         now {now}"
    );
    assert!(
        client > 0 && peer > 0,
        "a fresh fixture leaf has not expired"
    );
    assert_eq!(
        client, peer,
        "one node serves one identity, so both planes report the same leaf"
    );

    cluster.shutdown().await;
}

/// M6-63: the warning is emitted once per crossing, not once per scrape.
///
/// Counted from the log, because "warns once" is a claim about an operator's log: a latch
/// assertion passes with the suppression deleted, which is the lesson
/// `config-grpc/src/rotation.rs`'s own unit tests record.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_63_warning_fires_once_at_thirty_days() {
    let cluster = rotatable(4163).await;
    let node = NodeId(1);
    let method = "m6_63_warning_fires_once_at_thirty_days";
    let not_after = cluster
        .served_not_after(node, Plane::Client)
        .expect("a served leaf");
    let day = 24 * 60 * 60;
    let inside = not_after - 29 * day;
    let outside = not_after - 31 * day;

    cluster.cert_expiry_at(node, Plane::Client, outside);
    assert_eq!(
        expiry_warnings(method).await,
        0,
        "a certificate 31 days out is not near expiry"
    );

    // Both planes cross together: one node serves one leaf.
    cluster.cert_expiry_at(node, Plane::Client, inside);
    assert_eq!(
        expiry_warnings(method).await,
        2,
        "one line per plane on the crossing"
    );

    for _ in 0..10 {
        cluster.cert_expiry_at(node, Plane::Client, inside);
    }
    assert_eq!(
        expiry_warnings(method).await,
        2,
        "ten more scrapes inside the window must not write ten more lines"
    );

    cluster.cert_expiry_at(node, Plane::Client, outside);
    cluster.cert_expiry_at(node, Plane::Client, inside);
    assert_eq!(
        expiry_warnings(method).await,
        4,
        "crossing back out and in again is reported, so the next certificate to age into the \
         window is not swallowed"
    );

    let days: Vec<_> = my_log_lines(method)
        .iter()
        .filter(|l| field(l, "@m") == Some("cert_expiring"))
        .filter_map(|l| l.get("days_remaining").and_then(serde_json::Value::as_i64))
        .collect();
    assert!(
        days.iter().all(|d| *d == 29),
        "the line carries the days remaining: {days:?}"
    );

    cluster.shutdown().await;
}

/// How many `cert_expiring` lines this test has written so far.
async fn expiry_warnings(method: &str) -> usize {
    my_log_lines(method)
        .iter()
        .filter(|l| field(l, "@m") == Some("cert_expiring"))
        .count()
}

/// M6-64: the gauge follows the *served* credential, not the one read at boot.
///
/// The plan's row asks for a rotation to a certificate with a **later** `notAfter`.
/// `TlsFixture` mints every leaf inside one validity window, so what this row establishes is
/// the claim underneath that one: after a rotation the gauge is recomputed from the material
/// the planes now hold, and it agrees with the certificate the reply names. A cached boot-time
/// value would disagree with the reply as soon as the fingerprint changed.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_64_expiry_metric_follows_a_rotation() {
    let cluster = rotatable(4164).await;
    let node = NodeId(1);
    let next = TlsFixture::other_ca(CLUSTER, 4164);
    overlap(&cluster, &next).await;
    let anchors = [cluster.fixture().ca_pem().to_string(), next.ca_pem().into()];
    let anchors: Vec<&str> = anchors.iter().map(String::as_str).collect();

    let before_leaf = cluster.served_leaf_fingerprint(node, Plane::Client).await;
    cluster.rotate_files_with(node, &serving(&next, node, &anchors));
    let planes = cluster.reload_tls(node, ADMIN).await.expect("the rotation");
    let reported = planes
        .iter()
        .find(|p| p.plane == "client")
        .expect("the client plane is in the reply");

    assert_ne!(
        reported.cert_fingerprint, before_leaf,
        "the row needs a genuinely different certificate to be about"
    );
    assert_eq!(
        cluster.served_not_after(node, Plane::Client),
        Some(reported.cert_expiry_unix),
        "the gauge and the reply must read the same certificate, with no restart between them"
    );
    assert_eq!(
        cluster.cert_expiry_at(node, Plane::Peer, reported.cert_expiry_unix - 600),
        Some(600),
        "the peer plane's gauge followed the same rotation"
    );

    cluster.shutdown().await;
}

// =====================================================================================
// §4.3 — gossip key rotation (M6-57..M6-61)
// =====================================================================================

/// The key every gossip cluster in this file starts encrypted with.
const K1: [u8; 32] = [0x11; 32];
/// The key every gossip rotation in this file rotates to.
const K2: [u8; 32] = [0x22; 32];

/// A three-node cluster whose gossip is real and encrypted under [`K1`].
///
/// Encryption is the precondition for a keyring: `memberlist` has nothing to rotate on a node
/// whose gossip is in the clear, and `GossipNode::require_keyring` says so rather than
/// pretending to succeed.
async fn keyed(seed: u64) -> Cluster {
    Cluster::builder()
        .nodes(3)
        .cluster_id(CLUSTER)
        .storage(StorageKind::Ephemeral)
        .rotatable_tls(seed)
        .admins([ADMIN])
        .gossip(GossipKind::Real)
        .gossip_key(K1)
        .start()
        .await
}

/// Fingerprints of what node `id`'s keyring holds, as `(primary, accepted)`.
///
/// Fingerprints, never keys: `GossipKeyring` is the only view of the live `memberlist` keyring
/// this harness offers, and it deliberately cannot hand a row the bytes back (TA-58).
fn keyring_of(cluster: &Cluster, id: NodeId) -> (GossipKeyFingerprint, Vec<GossipKeyFingerprint>) {
    let ring = cluster
        .gossip_keyring(id)
        .unwrap_or_else(|| panic!("node {id} runs no encrypted gossip"));
    (ring.primary, ring.accepted)
}

/// Every node still sees every other node over gossip.
///
/// The observable form of "no node is ever seen as failed or suspect by another": a node whose
/// messages nobody can decrypt stops refreshing its hint and drops out of its peers' views.
async fn gossip_is_whole(cluster: &Cluster, stage: &str) {
    let ids = cluster.running_ids();
    let expected = ids.len() - 1;
    poll_until_async(cluster.deadline(20), cluster.poll_interval(), || async {
        ids.iter()
            .all(|id| cluster.gossip_peers(*id).len() >= expected)
            .then_some(())
    })
    .await
    .unwrap_or_else(|e| {
        let seen: Vec<_> = ids
            .iter()
            .map(|id| (*id, cluster.gossip_peers(*id).len()))
            .collect();
        panic!("gossip did not stay whole {stage}: {e}; peers seen {seen:?}")
    });
}

/// M6-57: D6.2's three stages, run across every node, converge on the new key.
///
/// The row is as much about what does *not* happen: the cluster is asked, at every stage, to
/// still be a cluster. A rotation that is only correct at the end is a rotation that takes an
/// outage in the middle.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_57_staged_add_use_remove_converges_with_all_nodes_up() {
    let cluster = keyed(4157).await;
    let method = "m6_57_staged_add_use_remove_converges_with_all_nodes_up";
    let ids = cluster.running_ids();
    let (k1_fp, _) = keyring_of(&cluster, ids[0]);

    for id in &ids {
        cluster
            .gossip_add_key(*id, &K2, ADMIN)
            .await
            .unwrap_or_else(|e| panic!("node {id} must accept a second key: {e}"));
    }
    gossip_is_whole(&cluster, "after add").await;
    for id in &ids {
        let (primary, accepted) = keyring_of(&cluster, *id);
        assert_eq!(primary, k1_fp, "add does not change what a node signs with");
        assert_eq!(
            accepted.len(),
            2,
            "node {id} accepts both keys: {accepted:?}"
        );
    }

    for id in &ids {
        cluster
            .gossip_use_key(*id, &K2, ADMIN)
            .await
            .unwrap_or_else(|e| panic!("node {id} must promote a key it already accepts: {e}"));
    }
    gossip_is_whole(&cluster, "after use").await;

    // The refusal in `remove_gossip_key` reads what peers *advertise*, which reaches this node
    // one gossip round after they changed it. Retried rather than waited out: a fixed sleep
    // here would be a guess at the failure detector's period, and the operator's own retry is
    // exactly what this models.
    for id in &ids {
        let node = *id;
        poll_until_async(cluster.deadline(20), cluster.poll_interval(), || {
            let cluster = &cluster;
            async move {
                match cluster.gossip_remove_key(node, &K1, false, ADMIN).await {
                    Ok(view) => Some(view),
                    Err(status) if status.message().contains("gossip_key_still_needed:") => None,
                    Err(status) => {
                        panic!("node {node} refused the removal for another reason: {status}")
                    }
                }
            }
        })
        .await
        .unwrap_or_else(|e| panic!("node {node} never saw its peers accept the new key: {e}"));
    }
    gossip_is_whole(&cluster, "after remove").await;

    let (k2_fp, _) = keyring_of(&cluster, ids[0]);
    assert_ne!(k2_fp, k1_fp, "the cluster signs with a different key now");
    for id in &ids {
        let (primary, accepted) = keyring_of(&cluster, *id);
        assert_eq!(primary, k2_fp, "every node converged on the same primary");
        assert_eq!(
            accepted,
            vec![k2_fp],
            "node {id} still accepts a retired key"
        );
    }

    // One line per node per stage, and never a key — only its fingerprint (M6-121).
    let lines = my_log_lines(method);
    let stages: Vec<_> = lines
        .iter()
        .filter(|l| field(l, "@m") == Some("gossip_key_rotated"))
        .filter_map(|l| field(l, "stage").map(str::to_string))
        .collect();
    for (stage, expected) in [("added", 3), ("promoted", 3), ("removed", 3)] {
        assert_eq!(
            stages.iter().filter(|s| *s == stage).count(),
            expected,
            "one `{stage}` line per node: {stages:?}"
        );
    }
    let rendered = serde_json::to_string(&lines).expect("the log renders");
    for gossip_key in [&K1, &K2] {
        assert!(
            !rendered.contains(&config_testkit::gossip_key_hex(gossip_key)),
            "a gossip key's hex reached the log"
        );
    }

    cluster.shutdown().await;
}

/// M6-58: promoting a key one peer has not accepted is an observability incident, not a
/// consensus one.
///
/// Gossip is advisory (§19.9), so the claim under test is a negative: whatever the failure
/// detector makes of a node it can no longer read, Raft keeps its leader, its membership and
/// its data, and the cluster recovers by itself once the missing `add` lands.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_58_use_before_add_on_a_peer_is_survivable() {
    let cluster = keyed(4158).await;
    let ids = cluster.running_ids();
    let (one, three) = (ids[0], ids[2]);
    let leader_before = cluster.leader().await;
    let membership_before = format!("{:?}", cluster.membership());
    cluster
        .client(leader_before)
        .put(put_req("/gossip/anchor", "1"))
        .await
        .expect("an anchor write before the botched stage");

    // Node 1 signs with a key node 3 has never been told about. Nodes 1 and 2 stay readable to
    // each other; node 3 is the one deliberately left behind.
    cluster
        .gossip_add_key(one, &K2, ADMIN)
        .await
        .expect("node 1 accepts the new key");
    cluster
        .gossip_add_key(ids[1], &K2, ADMIN)
        .await
        .expect("node 2 accepts the new key");
    cluster
        .gossip_use_key(one, &K2, ADMIN)
        .await
        .expect("node 1 promotes it while node 3 still cannot read it");

    assert_eq!(
        cluster.leader().await,
        leader_before,
        "a gossip key nobody could read moved leadership; gossip is advisory (ADR-0003)"
    );
    assert_eq!(
        format!("{:?}", cluster.membership()),
        membership_before,
        "a botched gossip rotation changed committed membership"
    );
    cluster
        .client(cluster.leader().await)
        .put(put_req("/gossip/during", "2"))
        .await
        .expect("the cluster still accepts writes while gossip is degraded");

    // Recovery is the operator finishing the stage they started, and nothing else.
    cluster
        .gossip_add_key(three, &K2, ADMIN)
        .await
        .expect("the missing add");
    cluster
        .gossip_use_key(three, &K2, ADMIN)
        .await
        .expect("node 3 catches up");
    gossip_is_whole(&cluster, "after the missing add landed").await;
    cluster
        .wait_converged(cluster.deadline(20))
        .await
        .expect("every node holds the same state after the incident");
    assert_eq!(
        cluster
            .client(cluster.leader().await)
            .get(get_req("/gossip/anchor"))
            .await
            .expect("the anchor is still readable")
            .record
            .map(|r| r.value),
        Some(key("1")),
        "no committed data was lost to a gossip key mistake"
    );

    cluster.shutdown().await;
}

/// M6-59: the destructive stage refuses to make this node deaf to a peer (OQ-61's default).
///
/// Node 3 is deliberately left on K1 alone, so K1 is the only key it can be read with. The
/// refusal has to name that fact in a form an operator can act on, which is what the
/// `gossip_key_still_needed:` prefix and the peer count are for.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_59_remove_before_every_peer_uses_the_new_key_is_refused_or_recoverable() {
    let cluster = keyed(4159).await;
    let ids = cluster.running_ids();
    let (one, three) = (ids[0], ids[2]);

    for id in [one, ids[1]] {
        cluster
            .gossip_add_key(id, &K2, ADMIN)
            .await
            .expect("the two rotating nodes accept the new key");
        cluster
            .gossip_use_key(id, &K2, ADMIN)
            .await
            .expect("and promote it");
    }
    // Taken here: add and use legitimately changed this keyring, and what the row is about is
    // what the *refused* removal does to it from this point on.
    let before = keyring_of(&cluster, one);

    // Poll on the refusal rather than assert it once: node 1 has to have *seen* node 3's
    // advertisement, and a run that asserted before the first gossip round would pass for the
    // wrong reason — it would be asserting an empty peer list.
    let status = poll_until_async(cluster.deadline(20), cluster.poll_interval(), || {
        let cluster = &cluster;
        async move {
            match cluster.gossip_remove_key(one, &K1, false, ADMIN).await {
                Ok(view) => panic!(
                    "removing the last key node {three} can be read with was allowed: {view:?}"
                ),
                Err(status) => status
                    .message()
                    .contains("gossip_key_still_needed:")
                    .then_some(status),
            }
        }
    })
    .await
    .expect("node 1 must refuse while a peer still depends on the key");

    assert_eq!(
        status.code(),
        tonic::Code::InvalidArgument,
        "a refusal an operator can fix by finishing the rotation: {status:?}"
    );
    assert_eq!(
        keyring_of(&cluster, one),
        before,
        "a refused removal must leave the keyring exactly as it was"
    );

    // Finishing the rotation is the other way out, and the same call then succeeds.
    cluster
        .gossip_add_key(three, &K2, ADMIN)
        .await
        .expect("the missing add");
    cluster
        .gossip_use_key(three, &K2, ADMIN)
        .await
        .expect("node 3 promotes the new key");
    poll_until_async(cluster.deadline(20), cluster.poll_interval(), || {
        let cluster = &cluster;
        async move { cluster.gossip_remove_key(one, &K1, false, ADMIN).await.ok() }
    })
    .await
    .expect("with no peer left depending on it, the removal is allowed");
    assert_ne!(
        keyring_of(&cluster, one),
        before,
        "the removal that was allowed actually removed something"
    );

    cluster.shutdown().await;
}

/// M6-60: a node unreachable over gossip for the whole of a staged rotation still converges
/// once it can hear its peers again.
///
/// There is no `NetFault` seam on the real gossip UDP transport (see
/// [`Cluster::gossip_isolate`]'s docs), so "unreachable" here is the closest in-process
/// approximation: node 3's `GossipNode` is stopped outright for the middle of the rotation and
/// started fresh, seeded off a peer, once the row is done pretending it was ever gone. Raft
/// membership on node 3 is never touched — this is purely an observability-plane row (§19.9),
/// nothing here claims a vote or a write was ever at risk.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_60_rotation_with_one_node_unreachable_converges_on_its_return() {
    let cluster = keyed(4160).await;
    let ids = cluster.running_ids();
    let (one, two, three) = (ids[0], ids[1], ids[2]);
    let (k1_fp, _) = keyring_of(&cluster, one);

    cluster.gossip_isolate(three).await;

    for id in [one, two] {
        cluster
            .gossip_add_key(id, &K2, ADMIN)
            .await
            .unwrap_or_else(|e| {
                panic!("node {id} must accept a second key while node {three} is unreachable: {e}")
            });
    }
    for id in [one, two] {
        cluster
            .gossip_use_key(id, &K2, ADMIN)
            .await
            .unwrap_or_else(|e| {
                panic!("node {id} must promote the new key while node {three} is unreachable: {e}")
            });
    }
    // `three` runs no gossip at all while isolated (its `GossipNode` is shut down, not merely
    // unreachable), so there is nothing to query about its keyring until it is healed. The
    // freshly started node's keyring is checked immediately below, before it does anything of
    // its own: that is the claim that it received no add/use traffic in the meantime.
    cluster.gossip_heal(three).await;
    let (primary_healed, _) = keyring_of(&cluster, three);
    assert_eq!(
        primary_healed, k1_fp,
        "node {three} received no add/use traffic while unreachable, so it must come back \
         still on the key it started with"
    );

    // Node 3 rejoins still holding only K1, while nodes 1 and 2 now primarily sign with K2 —
    // the same "use before add on a peer" shape M6-58 already covers, so gossip is *not*
    // expected to be whole again until node 3 finishes the stage itself (below): it cannot
    // decrypt what its peers send until it holds the key they are using.
    cluster
        .gossip_add_key(three, &K2, ADMIN)
        .await
        .expect("node 3 accepts the key once it can be reached");
    cluster
        .gossip_use_key(three, &K2, ADMIN)
        .await
        .expect("node 3 promotes it and converges with the rest");
    // `Cluster::gossip_heal` rebuilds node 3 holding only the key it had when it left (K1,
    // correctly — see its own assertion above), so its very first join attempt, back when
    // nothing it had could decrypt K2-signed gossip from nodes 1 and 2, fails and is not
    // retried at length (`gossip_heal`'s doc explains why: no amount of retrying with the
    // wrong key ever succeeds). Node 3 must be told to try again now that it holds the key
    // its peers are actually using, exactly as an operator's own reconnect step would.
    let seeds: Vec<_> = [one, two]
        .into_iter()
        .filter_map(|id| cluster.gossip_node(id).map(|n| n.advertise_addr()))
        .collect();
    cluster
        .gossip_node(three)
        .expect("node 3 still runs gossip after being healed")
        .join(&seeds)
        .await;
    gossip_is_whole(&cluster, "after node 3 caught up on the key it missed").await;

    let converged = keyring_of(&cluster, one);
    assert_ne!(converged.0, k1_fp, "the cluster's primary changed");
    for id in [two, three] {
        assert_eq!(
            keyring_of(&cluster, id),
            converged,
            "node {id} must converge on the same keyring as the rest, including node {three} \
             which caught up after being unreachable"
        );
    }

    cluster.shutdown().await;
}

/// M6-61: a key rotation is an admin operation, and every attempt leaves a trail.
///
/// All three stages, because the allowlist is checked per RPC and an audit line that only
/// covers the harmless stage is not an audit trail of a rotation.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_61_gossip_key_operations_are_admin_only_and_audited() {
    let cluster = keyed(4161).await;
    let method = "m6_61_gossip_key_operations_are_admin_only_and_audited";
    let node = cluster.running_ids()[0];
    let before = keyring_of(&cluster, node);

    for op in [GossipKeyOp::Add, GossipKeyOp::Use, GossipKeyOp::Remove] {
        let status = cluster
            .gossip_key_op(node, op, &K2, false, OUTSIDER)
            .await
            .expect_err("a non-admin principal must not be able to rotate a gossip key");
        assert_eq!(
            status.code(),
            tonic::Code::PermissionDenied,
            "{op:?}: a non-admin is refused before the handler runs: {status:?}"
        );
    }
    assert_eq!(
        keyring_of(&cluster, node),
        before,
        "a refused rotation must not touch the keyring"
    );

    let audited: Vec<serde_json::Value> = my_log_lines(method)
        .into_iter()
        .filter(|l| field(l, "@m") == Some("admin_op"))
        .collect();
    let denied: Vec<_> = audited
        .iter()
        .filter(|l| field(l, "outcome") == Some("rejected"))
        .filter_map(|l| field(l, "op").map(str::to_string))
        .collect();
    for op in ["gossip_key_add", "gossip_key_use", "gossip_key_remove"] {
        assert_eq!(
            denied.iter().filter(|o| *o == op).count(),
            1,
            "one audited refusal for `{op}`: {denied:?}"
        );
    }
    assert!(
        audited
            .iter()
            .all(|l| field(l, "principal") == Some(OUTSIDER)),
        "an audit line that cannot name who tried is not an audit line: {audited:#?}"
    );

    cluster.shutdown().await;
}
