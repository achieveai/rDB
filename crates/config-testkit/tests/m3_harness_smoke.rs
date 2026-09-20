//! Smoke tests for the M3 half of the [`Cluster`] harness: mutual TLS on both planes,
//! certificate-derived principals, and the static allowlist.
//!
//! The M3 rows (M3-01..M3-81) all assume the harness really speaks mTLS — that a node serves
//! its own certificate, that a peer verifies the DNS SAN, and that the principal a request is
//! authorized as comes from the *certificate* rather than from the call. If any of that were
//! a fiction, a negative row could pass while proving nothing, so each half is asserted here.

use bytes::Bytes;
use config_core::{ClusterId, ConfigError, ConfigStore, GetRequest, NodeId, PutRequest};
use config_testkit::cluster::{AuthzKind, Cluster, StorageKind};
use config_testkit::tls::{CertOverrides, CertProfile, TlsFixture};

const CLUSTER: ClusterId = ClusterId::from_bytes([7u8; 16]);

fn put_req(k: &str, v: &str) -> PutRequest {
    PutRequest {
        dedup: None,
        key: Bytes::copy_from_slice(k.as_bytes()),
        value: Bytes::copy_from_slice(v.as_bytes()),
        expected_mod_revision: None,
    }
}

fn get_req(k: &str) -> GetRequest {
    GetRequest {
        key: Bytes::copy_from_slice(k.as_bytes()),
    }
}

/// `[[grant]]` document used by the authorization rows: `svc-a` may read and write under
/// `/app/a/`, and nobody else is named at all.
const POLICY: &str = r#"
[[grant]]
principal = "svc-a"
prefix = "/app/a/"
access = ["read", "write"]
"#;

/// An mTLS cluster forms over the real gRPC peer plane and serves a certificate-authenticated
/// client on the real client plane (test plan §4.1/§4.2).
///
/// Formation is the peer-plane proof: OpenRaft cannot elect a leader without a quorum of
/// successful mutual handshakes, each verifying `node-<id>.<cluster>.retcd`.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn mtls_cluster_forms_and_serves_a_certificate_client() {
    let cluster = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Ephemeral)
        .mutual_tls(1)
        .authz(AuthzKind::Static(POLICY.to_string()))
        .start()
        .await;

    let leader = cluster.leader().await;
    let client = cluster.grpc_client_tls(leader, "svc-a");

    let written = client
        .put(put_req("/app/a/k", "v1"))
        .await
        .expect("a listed principal may write inside its granted prefix");
    assert!(written.revision >= 1, "a put advances the revision");

    let read = client
        .get(get_req("/app/a/k"))
        .await
        .expect("the same principal may read it back");
    assert_eq!(
        read.record.expect("the key it just wrote").value.as_ref(),
        b"v1",
        "the value survives the round trip over TLS"
    );
}

/// A client whose certificate chains to a different CA never gets a usable connection: the
/// handshake fails, so the request fails rather than being served as some principal.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wrong_ca_client_is_refused() {
    let cluster = Cluster::builder().nodes(3).mutual_tls(2).start().await;
    let leader = cluster.leader().await;

    // A whole separate CA for the same cluster id: the SAN is well formed, the signature is
    // not one this cluster trusts, and the CA it pins is not the one the nodes present.
    let foreign = TlsFixture::other_ca(CLUSTER, 99);
    let client = cluster
        .grpc_client_with_tls(
            leader,
            config_grpc::TlsMode::MutualTls(foreign.client_mtls("svc-a")),
        )
        .expect("the client configuration itself is valid; only the handshake is not");

    let err = client
        .put(put_req("/app/a/k", "v1"))
        .await
        .expect_err("a foreign CA must not be served");
    // The client does an explicit connect under mTLS, so a refused handshake is a
    // connect-phase failure and reaches the caller as `Unavailable` — not as a spent request
    // budget. §4.2 rows assert exactly this shape.
    assert!(
        matches!(err, ConfigError::Unavailable { .. }),
        "a refused handshake must surface as Unavailable, not as {err:?}"
    );

    // The cluster is unharmed: its own clients still work.
    cluster
        .grpc_client_tls(leader, "svc-a")
        .put(put_req("/app/a/k", "v1"))
        .await
        .expect("the rejection was the client's, not the cluster's");
}

/// The static allowlist denies a principal it does not name, and the principal comes from the
/// certificate — the caller never states who it is (ADR-0012, M3-26).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn a_static_policy_denies_an_unlisted_principal() {
    let cluster = Cluster::builder()
        .nodes(3)
        .mutual_tls(3)
        .authz(AuthzKind::Static(POLICY.to_string()))
        .start()
        .await;
    let leader = cluster.leader().await;

    let err = cluster
        .grpc_client_tls(leader, "svc-z")
        .get(get_req("/app/a/k"))
        .await
        .expect_err("svc-z is in no grant");
    assert!(
        matches!(err, ConfigError::PermissionDenied { .. }),
        "an unlisted principal is denied, not served: {err:?}"
    );

    // Same key, same cluster, different certificate: the only thing that changed is the SAN.
    cluster
        .grpc_client_tls(leader, "svc-a")
        .put(put_req("/app/a/k", "v1"))
        .await
        .expect("svc-a is granted write on /app/a/");
}

/// One node holding a certificate minted for a foreign cluster cannot be reached by its
/// peers, and the other two elect a leader and commit writes anyway (M3-13): two of three is
/// a quorum, and an unreachable third voter only costs replication, not availability.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn one_bad_cert_node_does_not_stop_the_quorum() {
    let broken = NodeId(3);
    let cluster = Cluster::builder()
        .nodes(3)
        .mutual_tls(4)
        .node_cert_override(
            broken,
            CertOverrides::wrong_cluster(ClusterId::from_bytes([0xAB; 16])),
        )
        .form(false)
        .start()
        .await;

    // All three are voters, so the broken node is genuinely in the way rather than merely
    // left out of the plan: every quorum decision has to route around it.
    cluster
        .form()
        .await
        .expect("the plan names all three nodes");
    let leader = cluster
        .wait_for_leader(cluster.deadline(12))
        .await
        .expect("two reachable nodes are a quorum of three");
    assert_ne!(
        leader, broken,
        "a node no peer can complete a handshake with cannot win an election"
    );

    let written = cluster
        .grpc_client_tls(leader, "svc-a")
        .put(put_req("/app/a/k", "v1"))
        .await
        .expect("the surviving quorum still commits writes");

    cluster
        .wait_revision_on(
            &[NodeId(1), NodeId(2)],
            written.revision,
            cluster.deadline(8),
        )
        .await
        .expect("both reachable nodes apply the write");

    // The broken node is still up — it is its certificate, not its process, that is wrong —
    // and it is still shut out, which is the whole point of the row.
    assert!(
        cluster.running_ids().contains(&broken),
        "the row proves an identity rejection, so the node must still be running"
    );
    let stranded = cluster.node(broken).metrics();
    assert!(
        stranded.current_leader.is_none(),
        "no peer can reach the broken node, so it never learns of a leader: {stranded:?}"
    );
}

/// A certificate with no rEtcd SAN URI still completes the handshake (its DNS names are
/// intact) and is resolved through the Common Name — the CN-fallback path of §4.2.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn a_certificate_without_a_san_uri_is_still_usable() {
    let cluster = Cluster::builder()
        .nodes(3)
        .mutual_tls(5)
        .authz(AuthzKind::Static(POLICY.to_string()))
        .start()
        .await;
    let leader = cluster.leader().await;

    let client = cluster
        .grpc_client_with_cert(
            leader,
            CertProfile::client("svc-a"),
            CertOverrides::no_san(),
        )
        .expect("a SAN-less certificate is still a valid client configuration");

    // One outcome, asserted. The earlier revision accepted `Ok(_)` *or* a denial, which made
    // the row unable to fail: a server that silently granted an unnamed caller and a server
    // that refused every SAN-less certificate both passed it, and those are opposite
    // behaviours. ADR-0012 says the client plane falls back to the Common Name, the fixture
    // sets the CN to the principal name, and the policy grants `svc-a` write on `/app/a/` —
    // so the one correct outcome is that the write is served *as svc-a*.
    client
        .put(put_req("/app/a/k", "v1"))
        .await
        .expect("the CN fallback resolves this certificate to svc-a, which is granted write");

    // ...and the fallback resolved it to `svc-a` specifically, not to some unnamed default:
    // a key outside svc-a's prefix must still be denied, which only holds if a real principal
    // was derived. A blanket grant would serve this too.
    let denied = client.put(put_req("/app/b/k", "v1")).await;
    assert!(
        matches!(denied, Err(ConfigError::PermissionDenied { .. })),
        "the CN fallback must yield the real principal svc-a, whose grant stops at /app/a/:          {denied:?}"
    );
    cluster.shutdown().await;
}

/// A harness mTLS client follows a leader hint (M3 hint rows, ADR-0009).
///
/// Under mutual TLS a client that does not know the cluster id cannot name the node a hint
/// points at (`node-<id>.<cluster>.retcd`) and refuses to follow hints at all. Every harness
/// client carries the id, and this row is what proves it: a client pinned to a *follower*
/// still reads the leader's data, and says it got there by following a hint.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn an_mtls_client_follows_a_leader_hint() {
    let cluster = Cluster::builder()
        .nodes(3)
        .mutual_tls(6)
        .authz(AuthzKind::Static(POLICY.to_string()))
        .start()
        .await;
    let leader = cluster.leader().await;
    let follower = cluster
        .followers()
        .into_iter()
        .next()
        .expect("a 3-node cluster has followers");

    let written = cluster
        .grpc_client_tls(leader, "svc-a")
        .put(put_req("/app/a/hinted", "v1"))
        .await
        .expect("a write at the leader");
    cluster
        .wait_revision_all(written.revision, cluster.deadline(8))
        .await
        .expect("every node applies it");

    let at_follower = cluster.grpc_client_tls(follower, "svc-a");
    assert_eq!(
        at_follower.cluster_id(),
        Some(cluster.config().cluster_id),
        "a harness mTLS client that does not know its cluster cannot follow a hint at all"
    );
    assert_eq!(
        at_follower.pinned_endpoint(),
        cluster.client_endpoint(follower),
        "the client must start at the follower, or the hint is never issued"
    );

    let got = at_follower
        .get(get_req("/app/a/hinted"))
        .await
        .expect("the follower-pinned client reaches the leader by following the hint");
    assert_eq!(
        got.record.expect("the hinted key").value.as_ref(),
        b"v1",
        "the hint led somewhere that did not have the data"
    );
    assert!(
        at_follower.stats().hint_follows >= 1,
        "the read succeeded without following a hint: {:?}",
        at_follower.stats()
    );
}
