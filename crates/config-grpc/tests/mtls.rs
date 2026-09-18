//! Mutual TLS: identity really comes from the certificate (M3 plumbing, exercised now).
//!
//! These prove three claims that are easy to assert falsely: a client certificate's SAN URI
//! becomes the [`Principal`], a certificate from a foreign CA cannot connect at all, and the
//! peer plane refuses an envelope whose `from_node_id` disagrees with the certificate.

mod support;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use config_core::{ClusterId, NodeId, PrincipalKind, RecoveryEpoch};
use config_engine::netfault::NetFault;
use config_engine::transport::{PeerEnvelopeMeta, PeerRequest, PeerTransport, TransportError};
use config_grpc::pb::config_service_client::ConfigServiceClient;
use config_grpc::{pb, serve_peer_plane, GrpcPeerTransport, MtlsConfig, TlsMode};
use config_log::{retcd_test, TraceContext};
use openraft::raft::VoteRequest;
use openraft::Vote;
use rcgen::{
    BasicConstraints, CertificateParams, DnType, Ia5String, IsCa, KeyPair, KeyUsagePurpose, SanType,
};
use tokio::net::TcpListener;
use tonic::transport::{Channel, ClientTlsConfig};

use support::{start_client_plane, FakeSink, FakeStore};

const DEADLINE: Duration = Duration::from_secs(5);
const CLUSTER: &str = "0123456789abcdef0123456789abcdef";
/// Certificates name the server by DNS; the listener is reached at 127.0.0.1.
const SERVER_DNS: &str = "retcd.test";

fn cluster() -> ClusterId {
    CLUSTER.parse().expect("valid hex cluster id")
}

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
    let key = KeyPair::generate().expect("leaf key");
    let mut params = CertificateParams::new(vec![SERVER_DNS.to_string()]).expect("leaf params");
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
async fn client_principal_comes_from_the_certificate_san() {
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
async fn a_client_from_another_ca_cannot_connect() {
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
async fn peer_certificate_identity_must_match_the_envelope() {
    let ca = new_ca("retcd-test-ca");
    let (server_cert, server_key) =
        issue(&ca, "node-2", Some(&format!("retcd://{CLUSTER}/node/2")));
    let (node1_cert, node1_key) = issue(&ca, "node-1", Some(&format!("retcd://{CLUSTER}/node/1")));

    let sink = FakeSink::new(cluster(), NodeId(2));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let server_tls = TlsMode::MutualTls(mtls(&ca, server_cert, server_key));
    let handle = serve_peer_plane(sink.handler(), listener, server_tls).expect("serve peer plane");
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
