//! Mutual TLS: identity really comes from the certificate (M3 plumbing, exercised now).
//!
//! These prove four claims that are easy to assert falsely: a client certificate's SAN URI
//! becomes the [`Principal`], a certificate from a foreign CA cannot connect at all, the peer
//! plane refuses an envelope whose `from_node_id` disagrees with the certificate, and a peer
//! *dial* is verified against the node the envelope addresses rather than against whoever
//! answers at that address.

mod support;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use config_core::{NodeId, PrincipalKind, RecoveryEpoch};
use config_engine::netfault::NetFault;
use config_engine::transport::{PeerEnvelopeMeta, PeerRequest, PeerTransport, TransportError};
use config_grpc::pb::config_service_client::ConfigServiceClient;
use config_grpc::{
    pb, peer_server_domain, serve_peer_plane, GrpcPeerTransport, MtlsConfig, TlsMode,
};
use config_log::{retcd_test, TraceContext};
use openraft::raft::VoteRequest;
use openraft::Vote;
use rcgen::{
    BasicConstraints, CertificateParams, DnType, Ia5String, IsCa, KeyPair, KeyUsagePurpose, SanType,
};
use serde_json::Value;
use tokio::net::TcpListener;
use tonic::transport::{Channel, ClientTlsConfig};

use support::{cluster, log_lines, start_client_plane, FakeSink, FakeStore, CLUSTER};

const DEADLINE: Duration = Duration::from_secs(5);
/// A cluster this listener does not serve.
const OTHER_CLUSTER: &str = "ffffffffffffffffffffffffffffffff";
/// Certificates name the server by DNS; the listener is reached at 127.0.0.1.
const SERVER_DNS: &str = "retcd.test";

struct Ca {
    pem: String,
    cert: rcgen::Certificate,
    key: KeyPair,
}

fn new_ca(common_name: &str) -> Ca {
    let key = KeyPair::generate().expect("ca key");
    let mut params = CertificateParams::new(Vec::<String>::new()).expect("ca params");
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let cert = params.self_signed(&key).expect("self-signed ca");
    Ca {
        pem: cert.pem(),
        cert,
        key,
    }
}

/// An end-entity certificate signed by `ca`, naming `san_uri` plus the server DNS name.
fn issue(ca: &Ca, common_name: &str, san_uri: Option<&str>) -> (String, String) {
    issue_named(ca, common_name, san_uri, &[SERVER_DNS.to_string()])
}

/// A node certificate shaped exactly like the one `config-testkit` issues: the peer URI SAN
/// *and* the DNS SAN [`peer_server_domain`] pins. `dns_node` is the identity the DNS name
/// claims, which a negative row sets to somebody else.
fn issue_node(ca: &Ca, uri_node: u64, dns_node: u64) -> (String, String) {
    issue_named(
        ca,
        &format!("retcd-node-{uri_node}"),
        Some(&format!("retcd://{CLUSTER}/node/{uri_node}")),
        &[
            peer_server_domain(&cluster(), NodeId(dns_node)),
            SERVER_DNS.to_string(),
        ],
    )
}

fn issue_named(
    ca: &Ca,
    common_name: &str,
    san_uri: Option<&str>,
    dns_names: &[String],
) -> (String, String) {
    let key = KeyPair::generate().expect("leaf key");
    let mut params = CertificateParams::new(dns_names.to_vec()).expect("leaf params");
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    if let Some(uri) = san_uri {
        params
            .subject_alt_names
            .push(SanType::URI(Ia5String::try_from(uri).expect("ascii uri")));
    }
    let cert = params
        .signed_by(&key, &ca.cert, &ca.key)
        .expect("ca signs leaf");
    (cert.pem(), key.serialize_pem())
}

fn mtls(ca: &Ca, cert_pem: String, key_pem: String) -> MtlsConfig {
    MtlsConfig::new(
        ca.pem.clone().into_bytes(),
        cert_pem.into_bytes(),
        key_pem.into_bytes(),
    )
    .with_server_domain(SERVER_DNS)
}

/// Dial `endpoint` over TLS while verifying the certificate against `SERVER_DNS`.
async fn tls_channel(endpoint: &str, tls: &MtlsConfig) -> Result<Channel, tonic::transport::Error> {
    let config: ClientTlsConfig = tls.client_tls_config();
    Channel::from_shared(format!("https://{endpoint}"))
        .expect("valid authority")
        .tls_config(config)?
        .connect()
        .await
}

#[retcd_test]
async fn m1_grpc_21_client_principal_comes_from_the_certificate_san() {
    let ca = new_ca("retcd-test-ca");
    let (server_cert, server_key) = issue(&ca, "server", None);
    let (client_cert, client_key) = issue(
        &ca,
        "fallback-cn",
        Some(&format!("retcd://{CLUSTER}/client/svc-a")),
    );

    let store = FakeStore::new();
    let server = start_client_plane(
        store.clone(),
        TlsMode::MutualTls(mtls(&ca, server_cert, server_key)),
    )
    .await;

    let channel = tls_channel(&server.endpoint, &mtls(&ca, client_cert, client_key))
        .await
        .expect("mutual TLS handshake succeeds");
    let mut client = ConfigServiceClient::new(channel);
    client
        .get(pb::GetRequest {
            key: Bytes::from_static(b"/app/a"),
        })
        .await
        .expect("get over mTLS");

    let principals = store.seen_principals();
    assert_eq!(principals.len(), 1);
    assert_eq!(
        principals[0].name, "svc-a",
        "the SAN URI wins over the common name"
    );
    assert_eq!(principals[0].kind, PrincipalKind::Certificate);
}

#[retcd_test]
async fn m1_grpc_22_a_client_from_another_ca_cannot_connect() {
    let ca = new_ca("retcd-test-ca");
    let rogue = new_ca("rogue-ca");
    let (server_cert, server_key) = issue(&ca, "server", None);
    let (rogue_cert, rogue_key) = issue(
        &rogue,
        "rogue",
        Some(&format!("retcd://{CLUSTER}/client/svc-a")),
    );

    let store = FakeStore::new();
    let server = start_client_plane(
        store.clone(),
        TlsMode::MutualTls(mtls(&ca, server_cert, server_key)),
    )
    .await;

    // The rogue presents its own CA's chain; the server trusts only `ca`.
    let rogue_tls = MtlsConfig::new(
        rogue.pem.clone().into_bytes(),
        rogue_cert.into_bytes(),
        rogue_key.into_bytes(),
    )
    .with_server_domain(SERVER_DNS);

    let outcome = match tls_channel(&server.endpoint, &rogue_tls).await {
        Err(_) => Err(()),
        Ok(channel) => ConfigServiceClient::new(channel)
            .get(pb::GetRequest {
                key: Bytes::from_static(b"/app/a"),
            })
            .await
            .map(|_| ())
            .map_err(|_| ()),
    };
    assert!(outcome.is_err(), "a foreign CA must not be served");
    assert_eq!(
        store.call_count(),
        0,
        "the store must never see a request from an untrusted client"
    );
}

#[retcd_test]
async fn m1_grpc_23_peer_certificate_identity_must_match_the_envelope() {
    let ca = new_ca("retcd-test-ca");
    // A node certificate now has to carry the DNS SAN the transport pins (ADR-0010 R1), or
    // the handshake ends before the envelope check this row is about can run.
    let (server_cert, server_key) = issue_node(&ca, 2, 2);
    let (node1_cert, node1_key) = issue_node(&ca, 1, 1);

    let sink = FakeSink::new(cluster(), NodeId(2));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let server_tls = TlsMode::MutualTls(mtls(&ca, server_cert, server_key));
    let handle = support::node_span(sink.node_id.0).in_scope(|| {
        serve_peer_plane(sink.handler(), listener, server_tls, sink.identity())
            .expect("serve peer plane")
    });
    let endpoint = handle.local_addr().to_string();

    let client_tls = TlsMode::MutualTls(mtls(&ca, node1_cert, node1_key));
    let transport: Arc<GrpcPeerTransport> = GrpcPeerTransport::new(client_tls, NetFault::new());

    let vote = || PeerRequest::Vote(VoteRequest::new(Vote::new(3, 1), None));
    let meta = |from: u64| PeerEnvelopeMeta {
        cluster_id: cluster(),
        recovery_epoch: RecoveryEpoch(0),
        from: NodeId(from),
        to: NodeId(2),
        trace: TraceContext::new_root(),
    };

    // The certificate says node 1 and so does the envelope: served.
    transport
        .send(meta(1), &endpoint, vote(), DEADLINE)
        .await
        .expect("matching identity is served");
    assert_eq!(sink.seen_metas().len(), 1);

    // The same certificate claiming to be node 3: refused before the handler sees it.
    let error = transport
        .send(meta(3), &endpoint, vote(), DEADLINE)
        .await
        .expect_err("a mismatched from_node_id must be refused");
    assert!(
        matches!(error, TransportError::IdentityRejected(_)),
        "expected IdentityRejected, got {error:?}"
    );
    assert_eq!(
        sink.seen_metas().len(),
        1,
        "the impersonation never reached the engine"
    );

    handle.shutdown().await.expect("clean shutdown");
}

/// ADR-0011: a CA is usually shared across an organisation, so "signed by our CA" is not
/// "minted for our cluster". A certificate naming a neighbouring cluster is refused.
#[retcd_test]
async fn m1_grpc_24_a_client_certificate_from_another_cluster_is_refused() {
    let ca = new_ca("retcd-test-ca");
    let (server_cert, server_key) = issue(&ca, "server", None);
    let (foreign_cert, foreign_key) = issue(
        &ca,
        "svc-a",
        Some(&format!("retcd://{OTHER_CLUSTER}/client/svc-a")),
    );

    let store = FakeStore::new();
    let server = start_client_plane(
        store.clone(),
        TlsMode::MutualTls(mtls(&ca, server_cert, server_key)),
    )
    .await;

    let channel = tls_channel(&server.endpoint, &mtls(&ca, foreign_cert, foreign_key))
        .await
        .expect("the CA is trusted, so the handshake succeeds");
    let status = ConfigServiceClient::new(channel)
        .get(pb::GetRequest {
            key: Bytes::from_static(b"/app/a"),
        })
        .await
        .expect_err("a certificate for another cluster must not be served");

    assert_eq!(status.code(), tonic::Code::Unauthenticated);
    assert_eq!(
        store.call_count(),
        0,
        "the store must never see a request from a foreign cluster's client"
    );
    // The refusal must not tell a prober which cluster this listener belongs to.
    assert!(
        !status.message().contains(CLUSTER),
        "the refusal leaked this listener's cluster id: {}",
        status.message()
    );
}

/// ADR-0012: the Common Name fallback exists for CAs that cannot mint URI SANs, and stops
/// exactly there. A node's peer certificate asserts a `retcd://` identity, so it must never
/// fall through to its CN and be served as a client — that would undo the separation of the
/// two planes with a certificate the cluster itself issued.
#[retcd_test]
async fn m1_grpc_25_a_node_certificate_is_not_a_client_principal() {
    let ca = new_ca("retcd-test-ca");
    let (server_cert, server_key) = issue(&ca, "server", None);
    let (node_cert, node_key) = issue(&ca, "node-1", Some(&format!("retcd://{CLUSTER}/node/1")));
    let (bad_uri_cert, bad_uri_key) = issue(
        &ca,
        "svc-b",
        // A well-formed URI our grammar refuses: an extra path segment.
        Some(&format!("retcd://{CLUSTER}/client/svc-b/extra")),
    );
    let (cn_only_cert, cn_only_key) = issue(&ca, "legacy-svc", None);

    let store = FakeStore::new();
    let server = start_client_plane(
        store.clone(),
        TlsMode::MutualTls(mtls(&ca, server_cert, server_key)),
    )
    .await;

    for (label, cert, key) in [
        ("a node identity", node_cert, node_key),
        ("a malformed retcd uri", bad_uri_cert, bad_uri_key),
    ] {
        let channel = tls_channel(&server.endpoint, &mtls(&ca, cert, key))
            .await
            .expect("the CA is trusted, so the handshake succeeds");
        let status = ConfigServiceClient::new(channel)
            .get(pb::GetRequest {
                key: Bytes::from_static(b"/app/a"),
            })
            .await
            .unwrap_err();
        assert_eq!(
            status.code(),
            tonic::Code::Unauthenticated,
            "{label} must not fall back to the common name"
        );
    }
    assert_eq!(store.call_count(), 0);

    // A certificate that asserts no retcd URI at all still gets the fallback, so the check is
    // narrow rather than a blanket refusal.
    let channel = tls_channel(&server.endpoint, &mtls(&ca, cn_only_cert, cn_only_key))
        .await
        .expect("handshake");
    ConfigServiceClient::new(channel)
        .get(pb::GetRequest {
            key: Bytes::from_static(b"/app/a"),
        })
        .await
        .expect("a SAN-less certificate is still served under its common name");
    let principals = store.seen_principals();
    assert_eq!(principals.len(), 1);
    assert_eq!(principals[0].name, "legacy-svc");
    assert_eq!(principals[0].kind, PrincipalKind::Certificate);
}

/// ADR-0013: a refusal before the handler must leave exactly one warn line inside the RPC
/// span. A rejection that logs nothing is a rejection nobody can diagnose from the log.
#[retcd_test]
async fn m1_grpc_26_an_unauthenticated_rejection_is_logged() {
    let ca = new_ca("retcd-test-ca");
    let (server_cert, server_key) = issue(&ca, "server", None);
    let (node_cert, node_key) = issue(&ca, "node-1", Some(&format!("retcd://{CLUSTER}/node/1")));

    let store = FakeStore::new();
    let server = start_client_plane(
        store.clone(),
        TlsMode::MutualTls(mtls(&ca, server_cert, server_key)),
    )
    .await;

    let channel = tls_channel(&server.endpoint, &mtls(&ca, node_cert, node_key))
        .await
        .expect("handshake");
    let request_id = "req-unauthenticated-is-logged";
    let mut request = tonic::Request::new(pb::GetRequest {
        key: Bytes::from_static(b"/app/a"),
    });
    request
        .metadata_mut()
        .insert(config_log::HEADER_REQUEST_ID, request_id.parse().unwrap());
    let status = ConfigServiceClient::new(channel)
        .get(request)
        .await
        .expect_err("a node certificate is not a client");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);
    assert!(
        config_grpc::is_server_rejection(&status),
        "a refusal the server decided on must be marked, or the client reads it as an \
         unknown outcome (ADR-0015)"
    );

    server.handle.shutdown().await.expect("clean shutdown");

    let rejected: Vec<Value> = log_lines(
        module_path!(),
        "m1_grpc_26_an_unauthenticated_rejection_is_logged",
    )
    .into_iter()
    .filter(|v| v.get("@m").and_then(Value::as_str) == Some("rpc rejected"))
    .collect();

    // Anti-flake rule 11: assert presence before asserting a property.
    assert_eq!(
        rejected.len(),
        1,
        "expected one warn line, got {rejected:#?}"
    );
    let line = &rejected[0];
    assert_eq!(line.get("@l").and_then(Value::as_str), Some("Warning"));
    assert_eq!(line.get("rpc").and_then(Value::as_str), Some("get"));
    assert_eq!(
        line.get("reason").and_then(Value::as_str),
        Some("unauthenticated")
    );
    assert_eq!(
        line.get("request_id").and_then(Value::as_str),
        Some(request_id),
        "the refusal is not correlatable with the call that caused it"
    );
    assert!(line.get("latency_ms").is_some());
    assert_eq!(
        line.get("testMethod").and_then(Value::as_str),
        Some("m1_grpc_26_an_unauthenticated_rejection_is_logged")
    );
}

/// R1 (m3-architecture §3, ADR-0011): a peer dial is verified against the node the *envelope*
/// addresses, not against whatever answers at that address.
///
/// The impostor here is the interesting shape: a listener holding node 2's perfectly valid,
/// correctly-signed certificate, serving a sink that claims to be node 3 and stamps node 3 on
/// every answer. Every check that runs *after* the handshake passes — same CA, same cluster,
/// `from`/`to`/`cluster_id` all agree — so before this ruling the call succeeded and node 1
/// happily fed OpenRaft bytes from a node that was never node 3. The only thing that can catch
/// it is refusing the connection, which is what pinning the DNS SAN per target does.
#[retcd_test]
async fn m3_grpc_50_a_peer_dial_is_verified_against_the_node_the_envelope_addresses() {
    let ca = new_ca("retcd-test-ca");
    let (node1_cert, node1_key) = issue_node(&ca, 1, 1);

    // Honest node 3.
    let honest_sink = FakeSink::new(cluster(), NodeId(3));
    let (honest_cert, honest_key) = issue_node(&ca, 3, 3);
    let honest_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let honest = support::node_span(honest_sink.node_id.0).in_scope(|| {
        serve_peer_plane(
            honest_sink.handler(),
            honest_listener,
            TlsMode::MutualTls(mtls(&ca, honest_cert, honest_key)),
            honest_sink.identity(),
        )
        .expect("serve honest peer plane")
    });
    let honest_endpoint = honest.local_addr().to_string();

    // An impostor: node 2's real certificate, a sink that answers as node 3.
    let impostor_sink = FakeSink::new(cluster(), NodeId(3));
    let (impostor_cert, impostor_key) = issue_node(&ca, 2, 2);
    let impostor_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let impostor = support::node_span(impostor_sink.node_id.0).in_scope(|| {
        serve_peer_plane(
            impostor_sink.handler(),
            impostor_listener,
            TlsMode::MutualTls(mtls(&ca, impostor_cert, impostor_key)),
            impostor_sink.identity(),
        )
        .expect("serve impostor peer plane")
    });
    let impostor_endpoint = impostor.local_addr().to_string();

    // The dialler's own profile carries a `server_domain`; the peer plane must ignore it and
    // derive the name from the envelope instead (otherwise every peer would be verified as
    // `retcd.test`, which is to say not verified at all).
    let transport: Arc<GrpcPeerTransport> = GrpcPeerTransport::new(
        TlsMode::MutualTls(mtls(&ca, node1_cert, node1_key)),
        NetFault::new(),
    );

    let vote = || PeerRequest::Vote(VoteRequest::new(Vote::new(3, 1), None));
    let to_node_3 = || PeerEnvelopeMeta {
        cluster_id: cluster(),
        recovery_epoch: RecoveryEpoch(0),
        from: NodeId(1),
        to: NodeId(3),
        trace: TraceContext::new_root(),
    };

    transport
        .send(to_node_3(), &honest_endpoint, vote(), DEADLINE)
        .await
        .expect("the real node 3 presents the name the envelope addresses");
    assert_eq!(honest_sink.seen_metas().len(), 1);

    let error = transport
        .send(to_node_3(), &impostor_endpoint, vote(), DEADLINE)
        .await
        .expect_err("node 2's certificate must not satisfy a dial addressed to node 3");
    assert!(
        !matches!(error, TransportError::Remote(_)),
        "the impostor answered and was only rejected afterwards, got {error:?}"
    );
    assert!(
        impostor_sink.seen_metas().is_empty(),
        "the envelope reached the impostor's handler: {:?}",
        impostor_sink.seen_metas()
    );

    // Both targets are cached separately, which is what makes one endpoint safe to reuse for
    // one identity only.
    assert_eq!(transport.cached_endpoints(), 2);

    honest.shutdown().await.expect("clean shutdown");
    impostor.shutdown().await.expect("clean shutdown");
}

/// M3-81: a caller whose certificate yields no client identity is counted as an
/// *authentication* rejection on the node it reached.
///
/// The certificate here is well-formed, signed by the right CA, and asserts a `retcd://`
/// **node** identity — so the handshake succeeds and the CN fallback is (correctly) refused,
/// which is exactly the case that used to leave nothing behind but a log line. The counter is
/// read off a real `ConfigNode`, because the whole point is that the fact crosses from the
/// transport, which sees certificates, into the engine, which keeps the counters.
#[retcd_test]
async fn m3_81_a_certificate_with_no_client_identity_is_counted_as_an_authn_rejection() {
    let ca = new_ca("retcd-test-ca");
    let (server_cert, server_key) = issue(&ca, "server", None);
    let (node_cert, node_key) = issue(&ca, "node-1", Some(&format!("retcd://{CLUSTER}/node/1")));
    let (client_cert, client_key) = issue(
        &ca,
        "svc-a",
        Some(&format!("retcd://{CLUSTER}/client/svc-a")),
    );

    let node = support::start_idle_node(NodeId(1)).await;
    let store = FakeStore::new();
    let server = support::start_client_plane_with(
        Arc::new(support::NodeBackend {
            node: node.clone(),
            store: store.clone(),
        }),
        TlsMode::MutualTls(mtls(&ca, server_cert, server_key)),
        cluster(),
    )
    .await;

    assert_eq!(node.metrics().authn_rejected, 0);

    let channel = tls_channel(&server.endpoint, &mtls(&ca, node_cert, node_key))
        .await
        .expect("the CA is trusted, so the handshake succeeds");
    let status = ConfigServiceClient::new(channel)
        .get(pb::GetRequest {
            key: Bytes::from_static(b"/app/a"),
        })
        .await
        .expect_err("a node certificate is not a client identity");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);

    assert_eq!(
        node.metrics().authn_rejected,
        1,
        "the refusal must reach the node's counter, not only the log"
    );
    assert_eq!(
        node.metrics().authz_denied,
        0,
        "no principal existed, so no authorization decision was ever made"
    );
    assert_eq!(store.call_count(), 0, "the call reached a store");

    // The control: a real client certificate is served and moves neither counter. Without it
    // the row above would pass on a listener that refused everybody.
    let channel = tls_channel(&server.endpoint, &mtls(&ca, client_cert, client_key))
        .await
        .expect("handshake");
    ConfigServiceClient::new(channel)
        .get(pb::GetRequest {
            key: Bytes::from_static(b"/app/a"),
        })
        .await
        .expect("a client certificate is a client identity");
    assert_eq!(node.metrics().authn_rejected, 1);
    assert_eq!(node.health_payload().await.authn_rejected, 1);

    server.handle.shutdown().await.expect("clean shutdown");
    node.stop().await.expect("stop");
}
