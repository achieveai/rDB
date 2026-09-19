//! M3 client-plane mutual TLS and principal derivation (test plan §4.2, rows M3-15..M3-25).
//!
//! Ground truth from reading `config_grpc::client_plane::ConfigSvc`:
//!
//! * A successful RPC logs `@m = "rpc"` with fields `rpc`, `principal`, `status`, `latency_ms`
//!   — `principal` is always the SAN-/CN-derived name, never anything the caller stated.
//! * `principal()` failing (no cert presented, or `principal_from_certs` refusing a foreign
//!   cluster or a non-client SAN) logs `@m = "rpc rejected"` with `reason = "unauthenticated"`
//!   and `detail = status.message()`, and returns a *server*-marked `Unauthenticated` status —
//!   this is a post-handshake, application-layer refusal.
//! * A raw TLS-handshake failure (foreign CA, expired cert presented where the chain itself is
//!   what's untrusted/invalid, no client cert at all, plaintext to a TLS listener) never
//!   reaches `principal()` and produces no server-side status at all; per the lead's ruling
//!   (R3) the client surfaces these as `ConfigError::Unavailable` because `GrpcClient` connects
//!   eagerly and a connect-phase failure has no server-marked outcome to trust.
//! * A wrong-cluster SAN (M3-17) is **not** one of these handshake failures: the TLS chain
//!   itself is perfectly valid (signed by the right CA, unexpired), so the handshake completes
//!   and `principal_from_certs` is reached and does the rejecting — confirmed by running it:
//!   the server logs a genuine `Unauthenticated` status with
//!   `detail = "client certificate asserts a retcd identity that is not a client of this
//!   cluster"`, which `GrpcClient` surfaces as `ConfigError::Unauthenticated`, not
//!   `Unavailable`. An earlier draft of M3-17 asserted `Unavailable` by over-applying ruling R3
//!   to every §4.2 negative row; this file's own bullet above already had the correct ground
//!   truth (`principal_from_certs` refusing a foreign cluster is the post-handshake case), the
//!   assertion just hadn't been reconciled with it.

mod support;

use bytes::Bytes;
use config_core::{ClusterId, ConfigError, ConfigStore, NodeId, Principal, PrincipalKind};
use config_testkit::cluster::{AuthzKind, Cluster, StorageKind};
use config_testkit::tls::{CertOverrides, CertProfile};
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};

use support::{field, get_req, put_req};

const CLUSTER: ClusterId = ClusterId::from_bytes([7u8; 16]);

const POLICY: &str = r#"
[[grant]]
principal = "svc-a"
prefix = "/app/a/"
access = ["read", "write"]

[[grant]]
principal = "svc-b"
prefix = "/app/a/"
access = ["read", "write"]
"#;

fn my_log_lines(method: &str) -> Vec<serde_json::Value> {
    support::my_log_lines(module_path!(), method)
}

async fn cluster_with_policy(seed: u64) -> Cluster {
    Cluster::builder()
        .nodes(3)
        .cluster_id(CLUSTER)
        .storage(StorageKind::Ephemeral)
        .mutual_tls(seed)
        .authz(AuthzKind::Static(POLICY.to_string()))
        .start()
        .await
}

// =====================================================================================
// M3-15/16 — principal derivation: SAN URI, and CN fallback
// =====================================================================================

/// M3-15: a client cert SAN `retcd://<cid>/client/svc-a` derives `Principal{name:"svc-a"}`.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_15_principal_from_san_uri() {
    const METHOD: &str = "m3_15_principal_from_san_uri";
    let cluster = cluster_with_policy(215).await;
    let leader = cluster.leader().await;

    cluster
        .grpc_client_tls(leader, "svc-a")
        .get(get_req("/app/a/k"))
        .await
        .expect("svc-a is granted read");

    let rows = my_log_lines(METHOD);
    let seen: Vec<_> = rows
        .iter()
        .filter(|r| {
            field(r, "@m") == Some("rpc")
                && field(r, "rpc") == Some("get")
                && field(r, "principal") == Some("svc-a")
        })
        .cloned()
        .collect();
    config_testkit::logs::assert_nonempty(&seen, "an rpc line naming principal=svc-a");
    cluster.shutdown().await;
}

/// M3-16: a client cert with no SAN URI, CN `svc-b`, still derives `Principal{name:"svc-b"}`
/// via the CN-fallback path (client-plane only, ADR-0012).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_16_principal_cn_fallback() {
    const METHOD: &str = "m3_16_principal_cn_fallback";
    let cluster = cluster_with_policy(216).await;
    let leader = cluster.leader().await;

    let client = cluster
        .grpc_client_with_cert(
            leader,
            CertProfile::client("svc-b"),
            CertOverrides::no_san(),
        )
        .expect("a SAN-less client certificate is still a valid configuration");
    client
        .get(get_req("/app/a/k"))
        .await
        .expect("svc-b is granted read via CN fallback");

    let rows = my_log_lines(METHOD);
    let seen: Vec<_> = rows
        .iter()
        .filter(|r| {
            field(r, "@m") == Some("rpc")
                && field(r, "rpc") == Some("get")
                && field(r, "principal") == Some("svc-b")
        })
        .cloned()
        .collect();
    config_testkit::logs::assert_nonempty(
        &seen,
        "an rpc line naming principal=svc-b (CN fallback)",
    );
    cluster.shutdown().await;
}

// =====================================================================================
// M3-17..M3-21 — handshake- and identity-level rejections (R3: assert Unavailable)
// =====================================================================================

/// M3-17: a client SAN naming a foreign cluster. The certificate chain itself is valid, so the
/// handshake completes and `principal_from_certs` does the rejecting post-handshake — this is
/// `ConfigError::Unauthenticated`, not the handshake-failure `Unavailable` of M3-18..21 (see the
/// module doc comment for the full reasoning, corrected after an earlier draft got this wrong).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_17_client_cert_wrong_cluster_id_rejected() {
    let cluster = cluster_with_policy(217).await;
    let leader = cluster.leader().await;
    let foreign = ClusterId::from_bytes([0xEE; 16]);
    let client = cluster
        .grpc_client_with_cert(
            leader,
            CertProfile::client("svc-a"),
            CertOverrides::wrong_cluster(foreign),
        )
        .expect("the client configuration itself is valid");
    let err = client
        .get(get_req("/app/a/k"))
        .await
        .expect_err("a foreign cluster id must be refused");
    assert!(
        matches!(err, ConfigError::Unauthenticated { .. }),
        "expected Unauthenticated, got {err:?}"
    );
    cluster.shutdown().await;
}

/// M3-18: a client cert signed by an unrelated CA never completes the handshake.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_18_client_cert_other_ca_rejected() {
    let cluster = cluster_with_policy(218).await;
    let leader = cluster.leader().await;
    let foreign = config_testkit::tls::TlsFixture::other_ca(CLUSTER, 918);
    let client = cluster
        .grpc_client_with_tls(
            leader,
            config_grpc::TlsMode::MutualTls(foreign.client_mtls("svc-a")),
        )
        .expect("the client configuration itself is valid; only the handshake is not");
    let err = client
        .get(get_req("/app/a/k"))
        .await
        .expect_err("a foreign CA must not be served");
    assert!(
        matches!(err, ConfigError::Unavailable { .. }),
        "expected Unavailable, got {err:?}"
    );
    cluster.shutdown().await;
}

/// M3-19: an expired client cert.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_19_client_cert_expired_rejected() {
    let cluster = cluster_with_policy(219).await;
    let leader = cluster.leader().await;
    let client = cluster
        .grpc_client_with_cert(
            leader,
            CertProfile::client("svc-a"),
            CertOverrides::expired(),
        )
        .expect("the client configuration itself is valid");
    let err = client
        .get(get_req("/app/a/k"))
        .await
        .expect_err("an expired certificate must not be served");
    assert!(
        matches!(err, ConfigError::Unavailable { .. }),
        "expected Unavailable, got {err:?}"
    );
    cluster.shutdown().await;
}

/// M3-20: plain TLS with no client certificate at all — never a default/anonymous principal.
///
/// `Endpoint::connect().await` resolving to `Ok` does **not** prove the server accepted the
/// handshake: per RFC 8446 the client sends its (here, empty) Certificate/Finished flight and
/// tonic's h2 client considers its side of the handshake done and spawns the connection-driving
/// task in the background (`MakeSendRequestService::call` in tonic 0.12, confirmed by reading
/// tonic's source: `builder.handshake(io).await?` returns as soon as the *local* H2 preface is
/// written, then `conn` — where an async-arriving fatal TLS alert, such as rustls's mandatory
/// `WebPkiClientVerifier`'s `CertificateRequired`, would actually surface — is
/// `Executor::execute`d, not awaited). So `connect()` can race ahead of the server's rejection
/// and return `Ok(Channel)` even against a listener that rejects the connection outright; the
/// existing `m1_grpc_22_a_client_from_another_ca_cannot_connect` (`crates/config-grpc/tests/mtls.rs`)
/// already treats a successful `connect()` as one legitimate outcome for exactly this reason and
/// defers the actual assertion to the first RPC. This test follows the same, proven pattern
/// instead of asserting on `connect()` alone (confirmed empirically: this test flaked exactly
/// this way, `connect()` returning `Ok` with no client cert presented, before this fix).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_20_client_no_cert_rejected() {
    let cluster = cluster_with_policy(220).await;
    let leader = cluster.leader().await;
    let endpoint = cluster.client_endpoint(leader);
    let ca_pem = cluster.fixture().ca_pem().to_string();
    let uri = format!("https://{endpoint}");
    // `MtlsConfig`-based client constructors always attach an identity; only a hand-built
    // `ClientTlsConfig` with no `.identity(..)` can dial with no client certificate at all.
    // The server's certificate also carries `DNS:localhost` (`config_testkit::tls` module doc),
    // so this is a genuine name match, not a confound that would fail the RPC for a different
    // reason than the missing client certificate.
    let channel = Endpoint::from_shared(uri)
        .expect("valid uri")
        .tls_config(
            ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(&ca_pem))
                .domain_name("localhost"),
        )
        .expect("tls config accepted")
        .connect()
        .await;
    // The typed error is kept rather than collapsed to `()`. What is asserted is that the
    // refusal came from the transport and never reached the application — see
    // `support::assert_transport_refusal` for why the exact `tonic::Code` is not the assertable
    // fact here (it alternates between `Unknown` and `Cancelled` with the interleaving).
    match channel {
        // `connect()` failed outright: the handshake refusal won the race. Already transport.
        Err(e) => {
            let text = format!("{e:?}");
            assert!(
                text.contains("Transport"),
                "an mTLS listener must refuse a certificate-less dial at the transport: {text}"
            );
        }
        Ok(channel) => {
            let status = config_grpc::pb::config_service_client::ConfigServiceClient::new(channel)
                .get(config_grpc::pb::GetRequest {
                    key: Bytes::from_static(b"/app/a/k"),
                })
                .await
                .err()
                .unwrap_or_else(|| panic!("an mTLS listener must require a client certificate"));
            support::assert_transport_refusal(&status, "a dial with no client certificate");
        }
    }
    cluster.shutdown().await;
}

/// M3-21: raw plaintext to the client port.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_21_client_plaintext_rejected() {
    let cluster = cluster_with_policy(221).await;
    let leader = cluster.leader().await;
    let endpoint = cluster.client_endpoint(leader);
    let client = cluster
        .grpc_client_plaintext(leader)
        .expect("the client configuration itself is valid; only the handshake is not");
    let err = client
        .get(get_req("/app/a/k"))
        .await
        .expect_err("plaintext to a TLS listener must fail");
    assert!(
        matches!(err, ConfigError::Unavailable { .. }),
        "expected Unavailable, got {err:?}"
    );
    let _ = endpoint;
    cluster.shutdown().await;
}

// =====================================================================================
// M3-22 — the effective principal cannot be forged via metadata
// =====================================================================================

/// M3-22: a valid `svc-a` certificate with a forged `retcd-principal: admin` metadata header
/// attached is still evaluated as `svc-a` — the header is simply never read.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_22_principal_not_forgeable_from_metadata() {
    const METHOD: &str = "m3_22_principal_not_forgeable_from_metadata";
    let cluster = cluster_with_policy(222).await;
    let leader = cluster.leader().await;
    let endpoint = cluster.client_endpoint(leader);
    let domain = cluster.fixture().node_mtls(leader).clone(); // unused; endpoint verified by name below
    let _ = domain;
    let pair = cluster.fixture().client_mtls("svc-a");

    let channel = Endpoint::from_shared(format!("https://{endpoint}"))
        .expect("valid uri")
        .tls_config(pair.client_tls_config())
        .expect("tls config accepted")
        .connect()
        .await
        .expect("a genuine svc-a certificate connects");

    let mut client = config_grpc::pb::config_service_client::ConfigServiceClient::new(channel);
    let mut request = tonic::Request::new(config_grpc::pb::GetRequest {
        key: Bytes::from_static(b"/app/a/k"),
    });
    request.metadata_mut().insert(
        "retcd-principal",
        "admin".parse().expect("valid header value"),
    );
    let _ = client.get(request).await; // key need not exist; only the principal matters here.

    let rows = my_log_lines(METHOD);
    let seen: Vec<_> = rows
        .iter()
        .filter(|r| field(r, "@m") == Some("rpc") && field(r, "principal") == Some("svc-a"))
        .cloned()
        .collect();
    config_testkit::logs::assert_nonempty(
        &seen,
        "the rpc line naming the real principal svc-a, not admin",
    );
    let admin_seen = rows.iter().any(|r| field(r, "principal") == Some("admin"));
    assert!(
        !admin_seen,
        "the forged metadata principal must never appear in a log line"
    );
    cluster.shutdown().await;
}

// =====================================================================================
// M3-23 — a direct client's principal is fixed at construction
// =====================================================================================

/// M3-23: `cluster.client_as(id, Principal{name:"embedder-x"})` evaluates every request from
/// that handle as `embedder-x`; a second handle with a different principal gets a different
/// decision.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_23_direct_client_principal_is_scoped_at_construction() {
    let cluster = cluster_with_policy(223).await;
    let leader = cluster.leader().await;

    let embedder_x = cluster.client_as(
        leader,
        Principal::new("embedder-x", PrincipalKind::Embedded),
    );
    let err = embedder_x
        .put(put_req("/app/a/k", "v"))
        .await
        .expect_err("embedder-x is granted nothing");
    assert!(
        matches!(err, ConfigError::PermissionDenied { .. }),
        "expected PermissionDenied, got {err:?}"
    );

    let svc_a = cluster.client_as(leader, Principal::new("svc-a", PrincipalKind::Embedded));
    svc_a
        .put(put_req("/app/a/k", "v"))
        .await
        .expect("svc-a is granted write on /app/a/");
    cluster.shutdown().await;
}

// =====================================================================================
// M3-24/25 — separate identity profiles: a cert minted for one plane is refused on the other
// =====================================================================================

/// M3-24: a peer (node) certificate presented on the client plane is refused.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_24_peer_cert_cannot_be_used_on_client_plane() {
    let cluster = cluster_with_policy(224).await;
    let leader = cluster.leader().await;
    let client = cluster
        .grpc_client_with_cert(leader, CertProfile::node(NodeId(1)), CertOverrides::none())
        .expect("the client configuration itself is valid");
    let err = client
        .get(get_req("/app/a/k"))
        .await
        .expect_err("a node certificate must not be served as a client");
    // Exactly one variant, determined by running it rather than assumed. The handshake itself
    // succeeds — a node certificate chains to the same CA and carries `DNS:localhost`, so
    // rustls has nothing to object to — and the refusal happens one layer up, when the client
    // plane looks for a `retcd://<cluster>/client/<name>` SAN and finds a node SAN instead.
    // That is precisely the case `Unauthenticated` is reserved for: an accepted session whose
    // certificate yields no principal. `Unavailable` would mean the connection never formed,
    // which would make this row a duplicate of M3-20 rather than a statement about identity
    // profiles; admitting both (as an earlier draft did) let either outcome pass and so
    // asserted neither.
    assert!(
        matches!(err, ConfigError::Unauthenticated { .. }),
        "expected Unauthenticated, got {err:?}"
    );
    cluster.shutdown().await;
}

/// M3-25: a client certificate presented on the peer plane is refused — the peer plane has no
/// path to accept a non-node identity, so this always fails the transport-identity check.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_25_client_cert_cannot_be_used_on_peer_plane() {
    let cluster = cluster_with_policy(225).await;
    let target = cluster.followers()[0];
    let peer_endpoint = cluster.peer_endpoint(target);
    let domain = config_grpc::peer_server_domain(&CLUSTER, target);
    let pair = cluster.fixture().issue(CertProfile::client("svc-a"));
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(&pair.ca_pem))
        .identity(tonic::transport::Identity::from_pem(
            &pair.cert_pem,
            &pair.key_pem,
        ))
        .domain_name(domain);

    let connect = Endpoint::from_shared(format!("https://{peer_endpoint}"))
        .expect("valid uri")
        .tls_config(tls)
        .expect("tls config accepted")
        .connect()
        .await;
    match connect {
        Err(_) => {} // refused at the handshake — acceptable
        Ok(channel) => {
            let mut client = config_grpc::pb::peer_service_client::PeerServiceClient::new(channel);
            let envelope = config_grpc::pb::PeerEnvelope {
                cluster_id: CLUSTER.to_string(),
                recovery_epoch: 1,
                from_node_id: 1,
                to_node_id: target.0,
                payload_encoding: 1,
                payload: b"{}".to_vec().into(),
                schema: None,
            };
            let result = client.vote(tonic::Request::new(envelope)).await;
            assert!(
                result.is_err(),
                "a client certificate must not be usable on the peer plane"
            );
        }
    }
    cluster.shutdown().await;
}
