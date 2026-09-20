//! M3 peer-plane mutual TLS and identity binding (test plan §4.1, rows M3-01..M3-14).
//!
//! `m3_harness_smoke.rs` already proves the harness genuinely speaks mTLS on both planes; this
//! file is the row-by-row backlog. Ground truth used throughout (established by reading the
//! source, not assumed from the plan's aspirational log names):
//!
//! * A certificate-level rejection is logged by `config_grpc::peer_plane::PeerSvc::call` as
//!   `@m = "peer rpc rejected"`. Two call sites share that message: an early one (payload /
//!   cluster-id-parse / transport-identity failure) with `reason` a short tag
//!   (`"bad_payload_encoding"`, `"bad_cluster_id"`, `"identity_mismatch"`) and `detail` the
//!   refusal's prose; and a later one, after the engine's own checks, with
//!   `reason = %PeerReject` — the `Display` of `WrongCluster{..}` / `WrongEpoch{..}` /
//!   `WrongDestination{..}` / `IdentityMismatch(_)`.
//! * A raw TLS-handshake failure (foreign CA, self-signed, no client cert) never reaches
//!   `PeerSvc::call` at all, so it produces **no** application log line — `config_grpc::tls`
//!   has no tracing calls. Those rows assert on the RPC's own error, not on a log line.
//! * `Cluster::node_cert_override` varies fields of the certificate a node serves with, and
//!   `CertOverrides::self_signed` additionally detaches it from the fixture CA while leaving
//!   every name correct — which is what M3-05 needs. A *foreign-CA* identity is still not
//!   expressible that way (the holder would also lose its own trust anchor and the row could no
//!   longer say which side did the rejecting), so M3-04 is driven by `raw_peer_call` below
//!   instead, dialling a real node directly.
//! * A TLS-layer refusal is only ever logged by the side that *dialled*: it never reaches a
//!   handler on the accepting side, and `config_grpc::tls` has no tracing calls. The rows that
//!   break a cluster member's certificate therefore assert `peer_transport_rejections` on the
//!   peers; the rows that dial from the test itself assert the RPC's own typed error.

mod support;

use config_core::{ClusterId, ConfigStore, NodeId};
use config_engine::transport::{PeerRequest, PAYLOAD_ENCODING_POSTCARD};
use config_testkit::cluster::{AuthzKind, Cluster, PoisonSpec, StorageKind};
use config_testkit::tls::{CertOverrides, CertProfile, TlsFixture};
use openraft::raft::VoteRequest;
use openraft::Vote;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};

use support::{
    field, field_u64, log_baseline, my_log_lines, my_log_lines_since, peer_transport_rejections,
    put_req,
};

const CLUSTER: ClusterId = ClusterId::from_bytes([7u8; 16]);

/// `[[grant]]` used by every row that needs authz configured at all: `svc-a` may read/write
/// under `/app/a/`. Most peer-plane rows use `AuthzKind::AllowAll` instead, because they are
/// about the Raft transport, not authorization.
const POLICY: &str = r#"
[[grant]]
principal = "svc-a"
prefix = "/app/a/"
access = ["read", "write"]
"#;

fn my_log_lines_for(method: &str) -> Vec<serde_json::Value> {
    my_log_lines(module_path!(), method)
}

fn baseline(method: &str) -> usize {
    log_baseline(module_path!(), method)
}

/// The dialling side's TLS rejections logged since `since`. See
/// [`support::peer_transport_rejections`] for why this is the only side that logs.
fn rejections_since(method: &str, since: usize) -> Vec<serde_json::Value> {
    peer_transport_rejections(module_path!(), method, since)
}

/// Assert that `broken` never joins, and that its peers logged the refusal.
///
/// Two separate claims, and both are needed. `assert_never` over a full deadline is the
/// liveness half: a single instantaneous `current_leader.is_none()` sample taken right after
/// `wait_for_leader` returned proves only that node 3 had not caught up *yet*, which is also
/// true of a node that joins a moment later. The log half is the mechanism: it says the node
/// stayed out because its certificate was refused, not because it was merely slow.
async fn assert_never_joins(cluster: &Cluster, broken: NodeId, method: &str, since: usize) {
    cluster
        .assert_never(
            &format!("node {broken} learning of a leader"),
            cluster.deadline(4),
            || {
                cluster
                    .try_node(broken)
                    .and_then(|n| n.metrics().current_leader)
                    .is_some()
            },
        )
        .await;

    let rejections = rejections_since(method, since);
    config_testkit::logs::assert_nonempty(
        &rejections,
        &format!("peer TLS rejections involving node {broken}"),
    );
}

/// A harmless, well-formed `Vote` payload: low term, no last log id. Real enough to decode and
/// reach the engine's own identity checks; never high enough a term to actually win an
/// election if it somehow got that far (it never does — every row here proves it is rejected
/// before OpenRaft sees it).
fn dummy_vote(from: u64) -> PeerRequest {
    PeerRequest::Vote(VoteRequest::new(Vote::new(1, from), None))
}

/// Dial `endpoint` directly with `PeerServiceClient::vote`, overriding the envelope's header
/// fields independently of the certificate presented — the only way to build the M3-09/10/11
/// "valid cert, forged envelope field" scenarios, since `GrpcPeerTransport` always stamps a
/// node's own real identity into the envelope it sends.
///
/// `tls` is the client TLS config to dial with (whatever certificate/CA the row needs);
/// `verify_domain` is the DNS name to verify the *server* against (the real target's domain,
/// even when the envelope's `to_node_id` lies about who that is).
#[allow(clippy::too_many_arguments)]
async fn raw_peer_call(
    endpoint: &str,
    verify_domain: &str,
    tls: ClientTlsConfig,
    cluster_id_hdr: &str,
    recovery_epoch_hdr: u32,
    from_hdr: u64,
    to_hdr: u64,
    req: PeerRequest,
) -> Result<(), tonic::Status> {
    let uri = format!("https://{endpoint}");
    let channel: Channel = Endpoint::from_shared(uri)
        .expect("a harness endpoint is a valid URI")
        .tls_config(tls.domain_name(verify_domain.to_string()))
        .expect("tls config accepted")
        .connect()
        .await
        .map_err(|e| tonic::Status::unavailable(format!("connect: {e}")))?;

    let payload = postcard::to_allocvec(&req).expect("a dummy vote request encodes");
    let envelope = config_grpc::pb::PeerEnvelope {
        cluster_id: cluster_id_hdr.to_string(),
        recovery_epoch: recovery_epoch_hdr,
        from_node_id: from_hdr,
        to_node_id: to_hdr,
        payload_encoding: PAYLOAD_ENCODING_POSTCARD,
        payload: payload.into(),
        schema: None,
    };
    let mut client = config_grpc::pb::peer_service_client::PeerServiceClient::new(channel);
    client.vote(tonic::Request::new(envelope)).await?;
    Ok(())
}

/// A `ClientTlsConfig` trusting the fixture's CA, presenting `pair`'s identity.
fn client_tls(
    fixture: &TlsFixture,
    pair_ca_pem: &str,
    cert_pem: &str,
    key_pem: &str,
) -> ClientTlsConfig {
    let _ = fixture;
    ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(pair_ca_pem))
        .identity(tonic::transport::Identity::from_pem(cert_pem, key_pem))
}

// =====================================================================================
// M3-01 — baseline
// =====================================================================================

/// M3-01: three real mTLS handshakes elect a leader and every write commits and converges.
/// Every negative row below differs from this one by exactly one variable.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_01_peer_mtls_happy_path() {
    let cluster = Cluster::builder()
        .nodes(3)
        .cluster_id(CLUSTER)
        .storage(StorageKind::Ephemeral)
        .mutual_tls(101)
        .authz(AuthzKind::Static(POLICY.to_string()))
        .start()
        .await;

    let leader = cluster.leader().await;
    for i in 0..5 {
        let put = cluster
            .grpc_client_tls(leader, "svc-a")
            .put(put_req(&format!("/app/a/k{i}"), "v"))
            .await
            .expect("a granted principal writes over mTLS");
        assert!(put.revision >= 1);
    }
    let hash = cluster
        .wait_converged(cluster.deadline(10))
        .await
        .expect("all three nodes converge on the same applied state");
    assert_ne!(hash, [0u8; 32]);
    assert_eq!(
        cluster.capabilities(leader).transport_security,
        config_core::TransportSecurity::MutualTls
    );
    cluster.shutdown().await;
}

// =====================================================================================
// M3-02/03 — wrong cluster id / wrong node id in the peer cert's SAN
// =====================================================================================

/// M3-02: node 3's peer cert claims a foreign cluster. Its peers refuse the connection; node 3
/// never joins; nodes 1/2 form and commit without it.
///
/// Ground truth, confirmed by running it: `CertOverrides::wrong_cluster` changes the *same*
/// SAN fields `config_grpc::peer_server_domain` uses for a dialer's TLS hostname check
/// (`node-3.<cluster>.retcd`), so every other node's *rustls client-side* verification of
/// node 3's presented server certificate fails on the domain name alone — a handshake failure
/// that (like M3-04/05/06) never reaches `PeerSvc::check_transport_identity`, which logs
/// `identity_mismatch`. The observed rejection is `config_engine::transport::TransportError`
/// ("invalid peer certificate: certificate not valid for name ...") on the *dialing* node's
/// replication/vote logs, not a `"peer rpc rejected"` line. An earlier draft assumed the
/// app-level identity check was reachable through `node_cert_override`; empirically it is not,
/// for the same structural reason M3-04/05/06 already document.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_02_peer_cert_wrong_cluster_id_rejected() {
    const METHOD: &str = "m3_02_peer_cert_wrong_cluster_id_rejected";
    let broken = NodeId(3);
    let since = baseline(METHOD);
    let cluster = Cluster::builder()
        .nodes(3)
        .cluster_id(CLUSTER)
        .mutual_tls(102)
        .node_cert_override(
            broken,
            CertOverrides::wrong_cluster(ClusterId::from_bytes([0xAB; 16])),
        )
        .form(false)
        .start()
        .await;

    cluster.form().await.expect("the plan names all three");
    let leader = cluster
        .wait_for_leader(cluster.deadline(12))
        .await
        .expect("two reachable nodes are a quorum of three");
    assert_ne!(leader, broken);

    assert_never_joins(&cluster, broken, METHOD, since).await;
    cluster.shutdown().await;
}

/// M3-03: node 3's cert claims node id 9. Same rejection path as M3-02 (a TLS-hostname
/// failure on the dialing node, not an app-level `identity_mismatch` log line), different SAN
/// field: `CertOverrides::wrong_node` changes the same `node-<id>.<cluster>.retcd` DNS name
/// `peer_server_domain` builds a dialer's hostname check from, so this also never reaches
/// `PeerSvc::check_transport_identity`. See M3-02's doc comment for the full ground truth.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_03_peer_cert_wrong_node_id_rejected() {
    const METHOD: &str = "m3_03_peer_cert_wrong_node_id_rejected";
    let broken = NodeId(3);
    let since = baseline(METHOD);
    let cluster = Cluster::builder()
        .nodes(3)
        .cluster_id(CLUSTER)
        .mutual_tls(103)
        .node_cert_override(broken, CertOverrides::wrong_node(NodeId(9)))
        .form(false)
        .start()
        .await;

    cluster.form().await.expect("the plan names all three");
    let leader = cluster
        .wait_for_leader(cluster.deadline(12))
        .await
        .expect("two reachable nodes are a quorum of three");
    assert_ne!(leader, broken);

    assert_never_joins(&cluster, broken, METHOD, since).await;
    cluster.shutdown().await;
}

// =====================================================================================
// M3-04/05 — a foreign or self-signed chain, correct SAN
// =====================================================================================

/// M3-04: a cert signed by an unrelated CA (correct SAN, untrusted chain) cannot complete a
/// handshake with a real node — the TLS layer itself refuses it, before any `PeerService`
/// handler runs, so there is no log line to assert on (`config_grpc::tls` never logs).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_04_peer_cert_other_ca_rejected() {
    let cluster = Cluster::builder()
        .nodes(3)
        .cluster_id(CLUSTER)
        .mutual_tls(104)
        .start()
        .await;
    let target = cluster.followers()[0];
    let endpoint = cluster.peer_endpoint(target);
    let domain = config_grpc::peer_server_domain(&CLUSTER, target);

    let foreign = TlsFixture::other_ca(CLUSTER, 999);
    let pair = foreign.issue(CertProfile::node(NodeId(3)));
    let tls = client_tls(&foreign, &pair.ca_pem, &pair.cert_pem, &pair.key_pem);

    let result = raw_peer_call(
        &endpoint,
        &domain,
        tls,
        &CLUSTER.to_string(),
        1,
        3,
        target.0,
        dummy_vote(3),
    )
    .await;
    // The dialling side here is the test itself, so the rejection is a `tonic::Status`, not a
    // log line: the accepting node refuses the chain inside rustls and `config_grpc::tls` has
    // no tracing calls, so there is nothing for it to write. `raw_peer_call` maps a connect
    // failure to `Unavailable` (R3: a pre-submission failure is not an unknown outcome), which
    // is exactly what a handshake refusal is.
    let status = result.expect_err("a foreign-CA certificate must not complete the handshake");
    assert_eq!(
        status.code(),
        tonic::Code::Unavailable,
        "a handshake refusal is a connect-phase failure: {status:?}"
    );

    // The cluster keeps serving valid peers — asserted, not `.ok()`-swallowed, and proved by
    // the write actually landing on every node rather than by the leader still being listed as
    // running (which stays true of a cluster that has stopped committing).
    let leader = cluster.leader().await;
    let put = cluster
        .grpc_client_multi()
        .put(put_req("/app/a/after", "v"))
        .await
        .expect("the cluster keeps committing for valid peers");
    cluster
        .wait_revision_all(put.revision, cluster.deadline(10))
        .await
        .unwrap_or_else(|t| panic!("the write never replicated after the refused dial: {t}"));
    assert_eq!(
        cluster.leader_now(),
        Some(leader),
        "leadership stays stable"
    );
    cluster.shutdown().await;
}

/// M3-05: a self-signed certificate with a *correct* SAN is still on an untrusted chain.
///
/// The distinction this row exists to make: M3-02/03 break the *name*, so a reader could
/// reasonably conclude that rEtcd only checks names. Here every name is right — the SAN URI is
/// `retcd://<cluster>/node/3` and the DNS name is `node-3.<cluster>.retcd` — and the only
/// defect is that nothing signed it but itself. It must still never join.
///
/// `CertOverrides::self_signed` leaves the trust anchor alone, so node 3 can still *verify*
/// its peers; what is broken is one-directional. Both directions of the handshake therefore
/// fail for the same underlying reason: nodes 1/2 cannot verify node 3's server certificate,
/// and node 3's client certificate is not one nodes 1/2 will accept.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_05_peer_cert_self_signed_rejected() {
    const METHOD: &str = "m3_05_peer_cert_self_signed_rejected";
    let broken = NodeId(3);
    let since = baseline(METHOD);
    let cluster = Cluster::builder()
        .nodes(3)
        .cluster_id(CLUSTER)
        .mutual_tls(105)
        .node_cert_override(broken, CertOverrides::self_signed())
        .form(false)
        .start()
        .await;

    // The SAN really is correct: this row would be indistinguishable from M3-03 otherwise.
    let pair = cluster
        .fixture()
        .issue_self_signed(CertProfile::node(broken));
    assert_eq!(
        pair.server_domain.as_deref(),
        Some(config_grpc::peer_server_domain(&CLUSTER, broken).as_str()),
        "the self-signed leaf must carry the right peer domain, or this row is just M3-03"
    );
    assert_ne!(
        pair.cert_pem,
        cluster.fixture().issue(CertProfile::node(broken)).cert_pem,
        "the self-signed leaf must differ from the CA-signed one"
    );

    cluster.form().await.expect("the plan names all three");
    let leader = cluster
        .wait_for_leader(cluster.deadline(12))
        .await
        .expect("two reachable nodes are a quorum of three");
    assert_ne!(leader, broken);

    assert_never_joins(&cluster, broken, METHOD, since).await;
    cluster.shutdown().await;
}

// =====================================================================================
// M3-06/07 — expired cert / missing SAN
// =====================================================================================

/// M3-06: node 3's cert `not_after` is already in the past. No sleeping — the fixture mints it
/// pre-expired.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_06_peer_cert_expired_rejected() {
    const METHOD: &str = "m3_06_peer_cert_expired_rejected";
    let broken = NodeId(3);
    let since = baseline(METHOD);
    let cluster = Cluster::builder()
        .nodes(3)
        .cluster_id(CLUSTER)
        .mutual_tls(106)
        .node_cert_override(broken, CertOverrides::expired())
        .form(false)
        .start()
        .await;

    cluster.form().await.expect("the plan names all three");
    let leader = cluster
        .wait_for_leader(cluster.deadline(12))
        .await
        .expect("two reachable nodes are a quorum of three");
    assert_ne!(leader, broken);
    assert_never_joins(&cluster, broken, METHOD, since).await;
    cluster.shutdown().await;
}

/// M3-07: node 3's cert has a CN only, no SAN URI. The peer plane has no CN-fallback (that is
/// a client-plane-only affordance, ADR-0012), so it is rejected exactly like a mismatched SAN.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_07_peer_cert_missing_san_rejected() {
    const METHOD: &str = "m3_07_peer_cert_missing_san_rejected";
    // Not built via `node_cert_override`: `CertOverrides::no_san` only drops the `retcd://`
    // *URI* SAN (see `crates/config-testkit/src/tls.rs`'s cert-minting code) — the DNS-name
    // SANs a dialler's hostname check verifies (`node-<id>.<cluster>.retcd`, `localhost`) are a
    // separate, unconditional block, so a SAN-less node still completes every handshake it is
    // dialled *into*. A passive follower never dials anyone with its own certificate (it only
    // ever answers RPCs another, unbroken node opened), so a `node_cert_override`-built cluster
    // never exercises this node's own broken identity at all — confirmed by running it: the
    // node reaches `Follower` and zero `"peer rpc rejected"` lines are ever logged. This needs
    // the M3-09/10/11 pattern instead: dial a real, running node directly with a hand-issued,
    // SAN-less "node 3" certificate, which *does* reach `PeerSvc::check_transport_identity` on
    // the receiving end (mTLS never hostname-checks the client's own certificate).
    let cluster = Cluster::builder()
        .nodes(3)
        .cluster_id(CLUSTER)
        .mutual_tls(107)
        .start()
        .await;
    let target = cluster.followers()[0];
    let endpoint = cluster.peer_endpoint(target);
    let domain = config_grpc::peer_server_domain(&CLUSTER, target);
    let broken = NodeId(3);
    let pair = cluster
        .fixture()
        .issue_with(CertProfile::node(broken), CertOverrides::no_san());
    let tls = client_tls(
        cluster.fixture(),
        &pair.ca_pem,
        &pair.cert_pem,
        &pair.key_pem,
    );

    let result = raw_peer_call(
        &endpoint,
        &domain,
        tls,
        &CLUSTER.to_string(),
        1,
        broken.0,
        target.0,
        dummy_vote(broken.0),
    )
    .await;
    assert!(result.is_err(), "a SAN-less certificate must be rejected");

    let rows = my_log_lines_for(METHOD);
    let rejected: Vec<_> = rows
        .iter()
        .filter(|r| {
            field(r, "@m") == Some("peer rpc rejected")
                && field(r, "reason") == Some("identity_mismatch")
                && field(r, "detail")
                    .map(|d| d.contains("no retcd node SAN URI"))
                    .unwrap_or(false)
        })
        .cloned()
        .collect();
    config_testkit::logs::assert_nonempty(&rejected, "SAN-less peer certificate rejections");
    cluster.shutdown().await;
}

// =====================================================================================
// M3-08 — plaintext to the peer port
// =====================================================================================

/// M3-08: a raw TCP connection to the peer port with no TLS at all is refused at the
/// handshake; the server keeps serving real peers afterward.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_08_peer_plaintext_connection_rejected() {
    const METHOD: &str = "m3_08_peer_plaintext_connection_rejected";
    let cluster = Cluster::builder()
        .nodes(3)
        .cluster_id(CLUSTER)
        .mutual_tls(108)
        .start()
        .await;
    let target = cluster.followers()[0];
    let endpoint = cluster.peer_endpoint(target);
    // Taken after the cluster is up so the peer plane's own legitimate traffic (elections,
    // heartbeats) is excluded; only what this connection provokes is counted below.
    let since = baseline(METHOD);

    let mut stream = tokio::net::TcpStream::connect(&endpoint)
        .await
        .unwrap_or_else(|e| panic!("TCP connect to the peer port itself must succeed: {e}"));
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    // A gRPC/h2 client would send a TLS ClientHello first; plain bytes are simply not that.
    let _ = stream.write_all(b"not a tls handshake").await;
    let mut buf = [0u8; 16];
    // Derived from the cluster's own timers, not a literal: anti-flake rule 3.
    let read = tokio::time::timeout(cluster.deadline(2), stream.read(&mut buf)).await;
    // Three outcomes are all "rejected, never answered": the read times out (the server
    // silently drops unrecognised bytes), it returns 0/err (connection closed with nothing
    // sent), or — confirmed by running it — rustls itself parses the garbage as a malformed
    // TLS record and writes back a fatal alert before closing: content type `0x15` (Alert),
    // here `[21, 3, 3, 0, 2, 2, 50]` = TLS 1.2 record layer, level `fatal`, description `50`
    // (`decode_error`). That is a record layer refusal, not `PeerService` answering anything;
    // only a `0x16` (Handshake, e.g. a real `ServerHello`) would mean the connection was
    // actually treated as a valid TLS session. An earlier draft asserted `n == 0` and did not
    // account for this legitimate alert, which is a handful of bytes, not zero.
    // Unconditional: the earlier `if let` silently passed whenever the read errored or timed
    // out, which are outcomes this row must name rather than skip. Every accepted outcome is
    // now enumerated in one place, and `outcome` carries it into the failure messages below.
    let outcome = match read {
        Err(_elapsed) => "the server never answered".to_string(),
        Ok(Err(e)) => format!("the connection was closed with a read error: {e}"),
        Ok(Ok(0)) => "the connection was closed with nothing sent".to_string(),
        Ok(Ok(n)) => {
            assert_eq!(
                buf[0],
                0x15,
                "a non-TLS connection must not get anything but a TLS-record-layer alert, \
                 got byte 0x{:02x}: {:?}",
                buf[0],
                &buf[..n]
            );
            format!("the server wrote a TLS record-layer alert {:?}", &buf[..n])
        }
    };

    // The plan's real oracle: whatever the socket did, `PeerService` never saw a request from
    // it. The three real members keep heartbeating throughout, so "zero peer-plane lines" is
    // not available as a statement — what is available, and is what the row actually claims,
    // is that every peer-plane line since the baseline is attributable to a genuine member and
    // that nothing was refused at the application layer. A connection that got as far as the
    // handler would show up as either a `from` outside the membership or a rejection line.
    let members: Vec<String> = cluster.ids().iter().map(|id| id.0.to_string()).collect();
    // The members keep heartbeating, but an acceptor that refuses the probe in a few
    // milliseconds leaves a window shorter than one heartbeat interval, so "what landed since
    // the baseline" can legitimately be nothing yet. Wait for the next legitimate peer-plane
    // line instead of asserting on that window (anti-flake rule 1; M6 gate, 2026-09-19).
    let peer_lines: Vec<serde_json::Value> = cluster
        .wait_for(
            "a peer-plane log line since the baseline",
            cluster.deadline(2),
            || {
                let lines: Vec<serde_json::Value> =
                    my_log_lines_since(module_path!(), METHOD, since)
                        .into_iter()
                        .filter(|row| {
                            field(row, "@logger")
                                .map(|l| l.starts_with("config_grpc::peer"))
                                .unwrap_or(false)
                        })
                        .collect();
                (!lines.is_empty()).then_some(lines)
            },
        )
        .await
        .unwrap_or_default();
    let foreign: Vec<_> = peer_lines
        .iter()
        .filter(|row| {
            field(row, "@m") == Some("peer rpc rejected")
                || !field(row, "from").is_some_and(|f| members.iter().any(|m| m == f))
        })
        .collect();
    assert!(
        foreign.is_empty(),
        "a plaintext connection reached the peer plane ({outcome}); lines: {foreign:#?}"
    );
    config_testkit::logs::assert_nonempty(
        &peer_lines,
        "peer-plane traffic between the real members (an empty log would make the check above vacuous)",
    );

    // The server keeps working for real peers — proved by a write that actually replicates,
    // not by the leader still appearing in `running_ids()`, which stays true of a cluster that
    // has stopped committing entirely.
    let leader = cluster.leader().await;
    let put = cluster
        .grpc_client_multi()
        .put(put_req("/m3-08/after", "v"))
        .await
        .unwrap_or_else(|e| panic!("the cluster stopped committing after {outcome}: {e:?}"));
    cluster
        .wait_revision_all(put.revision, cluster.deadline(10))
        .await
        .unwrap_or_else(|t| panic!("the write never replicated after {outcome}: {t}"));
    assert_eq!(
        cluster.leader_now(),
        Some(leader),
        "leadership stays stable"
    );
    cluster.shutdown().await;
}

// =====================================================================================
// M3-09/10/11 — envelope header binding, independent of the certificate presented
// =====================================================================================

/// M3-09: a valid cert for node 2 dials node 1, but the envelope's `to_node_id` names node 2.
/// The engine's own destination check rejects it — the RPC is never handed to Raft.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_09_peer_destination_binding_enforced() {
    const METHOD: &str = "m3_09_peer_destination_binding_enforced";
    let cluster = Cluster::builder()
        .nodes(3)
        .cluster_id(CLUSTER)
        .mutual_tls(109)
        .start()
        .await;
    let n1 = NodeId(1);
    let n2 = NodeId(2);
    let endpoint = cluster.peer_endpoint(n1);
    let domain = config_grpc::peer_server_domain(&CLUSTER, n1);
    let pair = cluster.fixture().issue(CertProfile::node(n2));
    let tls = client_tls(
        cluster.fixture(),
        &pair.ca_pem,
        &pair.cert_pem,
        &pair.key_pem,
    );

    let result = raw_peer_call(
        &endpoint,
        &domain,
        tls,
        &CLUSTER.to_string(),
        1,
        n2.0,
        n2.0, // wrong: physically dialling node 1, envelope claims `to = 2`
        dummy_vote(n2.0),
    )
    .await;
    assert!(result.is_err(), "a misaddressed envelope must be rejected");

    let rows = my_log_lines_for(METHOD);
    let rejected: Vec<_> = rows
        .iter()
        .filter(|r| {
            field(r, "@m") == Some("peer rpc rejected")
                && field(r, "reason")
                    .map(|s| s.contains("wrong destination"))
                    .unwrap_or(false)
        })
        .cloned()
        .collect();
    config_testkit::logs::assert_nonempty(&rejected, "wrong-destination peer rejections");
    cluster.shutdown().await;
}

/// M3-10: a valid cert for node 2 dials node 1, but the envelope's `from_node_id` claims node
/// 3 — the transport-identity check (cert vs envelope) rejects it before the engine is asked.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_10_peer_from_node_id_must_match_cert() {
    const METHOD: &str = "m3_10_peer_from_node_id_must_match_cert";
    let cluster = Cluster::builder()
        .nodes(3)
        .cluster_id(CLUSTER)
        .mutual_tls(110)
        .start()
        .await;
    let n1 = NodeId(1);
    let n2 = NodeId(2);
    let endpoint = cluster.peer_endpoint(n1);
    let domain = config_grpc::peer_server_domain(&CLUSTER, n1);
    let pair = cluster.fixture().issue(CertProfile::node(n2));
    let tls = client_tls(
        cluster.fixture(),
        &pair.ca_pem,
        &pair.cert_pem,
        &pair.key_pem,
    );

    let result = raw_peer_call(
        &endpoint,
        &domain,
        tls,
        &CLUSTER.to_string(),
        1,
        3, // wrong: the cert says node 2, the envelope claims from = 3
        n1.0,
        dummy_vote(3),
    )
    .await;
    assert!(result.is_err(), "a forged from_node_id must be rejected");

    let rows = my_log_lines_for(METHOD);
    let rejected: Vec<_> = rows
        .iter()
        .filter(|r| {
            field(r, "@m") == Some("peer rpc rejected")
                && field(r, "reason") == Some("identity_mismatch")
                && field(r, "detail")
                    .map(|d| d.contains("certificate claims"))
                    .unwrap_or(false)
        })
        .cloned()
        .collect();
    config_testkit::logs::assert_nonempty(&rejected, "from_node_id-vs-cert rejections");
    cluster.shutdown().await;
}

/// M3-11: a cert whose SAN and the envelope's `cluster_id` agree with each other (so the
/// transport-identity check passes) but disagree with the receiving node's real cluster — the
/// engine's own cluster check rejects it.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_11_peer_cluster_id_header_must_match() {
    const METHOD: &str = "m3_11_peer_cluster_id_header_must_match";
    let cluster = Cluster::builder()
        .nodes(3)
        .cluster_id(CLUSTER)
        .mutual_tls(111)
        .start()
        .await;
    let n1 = NodeId(1);
    let n2 = NodeId(2);
    let foreign = ClusterId::from_bytes([0xCD; 16]);
    let endpoint = cluster.peer_endpoint(n1);
    // Verify the *real* server's domain (it is still node 1's genuine listener); only the
    // envelope and the presented cert's SAN claim the foreign cluster.
    let domain = config_grpc::peer_server_domain(&CLUSTER, n1);
    let pair = cluster
        .fixture()
        .issue_with(CertProfile::node(n2), CertOverrides::wrong_cluster(foreign));
    let tls = client_tls(
        cluster.fixture(),
        &pair.ca_pem,
        &pair.cert_pem,
        &pair.key_pem,
    );

    let result = raw_peer_call(
        &endpoint,
        &domain,
        tls,
        &foreign.to_string(),
        1,
        n2.0,
        n1.0,
        dummy_vote(n2.0),
    )
    .await;
    assert!(
        result.is_err(),
        "a foreign cluster_id header must be rejected"
    );

    let rows = my_log_lines_for(METHOD);
    let rejected: Vec<_> = rows
        .iter()
        .filter(|r| {
            field(r, "@m") == Some("peer rpc rejected")
                && field(r, "reason")
                    .map(|s| s.starts_with("wrong cluster"))
                    .unwrap_or(false)
        })
        .cloned()
        .collect();
    config_testkit::logs::assert_nonempty(&rejected, "wrong-cluster engine rejections");
    cluster.shutdown().await;
}

// =====================================================================================
// M3-12 — client cert required
// =====================================================================================

/// M3-12: the peer listener is mTLS; a dialler that presents no client certificate never
/// completes the handshake.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_12_peer_client_cert_required() {
    let cluster = Cluster::builder()
        .nodes(3)
        .cluster_id(CLUSTER)
        .mutual_tls(112)
        .start()
        .await;
    let target = cluster.followers()[0];
    let endpoint = cluster.peer_endpoint(target);
    let domain = config_grpc::peer_server_domain(&CLUSTER, target);
    let ca_pem = cluster.fixture().ca_pem().to_string();

    // `connect()` resolving `Ok` does not by itself prove the server accepted the handshake:
    // tonic 0.12.3 drives the HTTP/2 connection on a background-spawned task rather than
    // awaiting it, so a rejecting server's fatal alert (`CertificateRequired`, mandatory
    // client auth with no cert presented) can arrive *after* `connect()` already returned —
    // confirmed by running it, mirroring the identical finding already documented for M3-20 in
    // m3_client_mtls.rs. Only an actual RPC reliably surfaces the rejection.
    let uri = format!("https://{endpoint}");
    let channel = Endpoint::from_shared(uri)
        .expect("valid uri")
        .tls_config(
            ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(&ca_pem))
                .domain_name(domain),
        )
        .expect("tls config accepted")
        .connect()
        .await;
    // The typed error is kept, not collapsed to `()`. The refusal is a TLS-handshake refusal,
    // so it is pre-submission and can never carry an application-level code — above all never
    // `Unauthenticated`, which is reserved for an accepted session whose certificate yields no
    // principal (the client plane's M3-24). The exact `tonic::Code` is *not* the assertable
    // fact: see `support::assert_transport_refusal`.
    match channel {
        // `connect()` failed outright: the handshake refusal won the race.
        Err(e) => {
            let text = format!("{e:?}");
            assert!(
                text.contains("Transport"),
                "a certificate-less dial must be refused at the transport: {text}"
            );
        }
        Ok(channel) => {
            let mut client = config_grpc::pb::peer_service_client::PeerServiceClient::new(channel);
            let payload = postcard::to_allocvec(&dummy_vote(3)).expect("a dummy vote encodes");
            let status = client
                .vote(tonic::Request::new(config_grpc::pb::PeerEnvelope {
                    cluster_id: CLUSTER.to_string(),
                    recovery_epoch: 1,
                    from_node_id: 3,
                    to_node_id: target.0,
                    payload_encoding: PAYLOAD_ENCODING_POSTCARD,
                    payload: payload.into(),
                    schema: None,
                }))
                .await
                .err()
                .unwrap_or_else(|| {
                    panic!("mTLS must require a client certificate, not just server trust")
                });
            support::assert_transport_refusal(&status, "a peer dial with no client certificate");
        }
    }

    // The cluster keeps committing — asserted, and proved by replication rather than by the
    // leader still being listed as running.
    let leader = cluster.leader().await;
    let put = cluster
        .grpc_client_multi()
        .put(put_req("/m3-12/after", "v"))
        .await
        .expect("the cluster keeps committing after a certificate-less dial");
    cluster
        .wait_revision_all(put.revision, cluster.deadline(10))
        .await
        .unwrap_or_else(|t| panic!("the write never replicated: {t}"));
    assert_eq!(
        cluster.leader_now(),
        Some(leader),
        "leadership stays stable"
    );
    cluster.shutdown().await;
}

// =====================================================================================
// M3-13 — a rejected peer never affects the surviving quorum
// =====================================================================================

/// M3-13: while a node with a bad certificate keeps hammering the peer port, the other two
/// keep a stable leader and keep committing; `current_term` does not churn (§19.12).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_13_rejected_peer_does_not_affect_quorum() {
    let broken = NodeId(3);
    let cluster = Cluster::builder()
        .nodes(3)
        .cluster_id(CLUSTER)
        .mutual_tls(113)
        .node_cert_override(
            broken,
            CertOverrides::wrong_cluster(ClusterId::from_bytes([0xAB; 16])),
        )
        .form(false)
        .start()
        .await;
    cluster.form().await.expect("the plan names all three");
    let leader = cluster
        .wait_for_leader(cluster.deadline(12))
        .await
        .expect("two reachable nodes are a quorum of three");
    let term_before = cluster.metrics(leader).current_term;

    for i in 0..3 {
        let put = cluster
            .grpc_client_multi()
            .put(put_req(&format!("/m3-13/{i}"), "v"))
            .await;
        assert!(
            put.is_ok(),
            "the surviving quorum keeps committing: {put:?}"
        );
    }
    let term_after = cluster.metrics(leader).current_term;
    assert_eq!(
        term_before, term_after,
        "an unreachable third voter must not churn the term"
    );
    assert_eq!(
        cluster.leader_now(),
        Some(leader),
        "leadership stays stable"
    );
    cluster.shutdown().await;
}

// =====================================================================================
// M3-14 — a poisoned gossip hint cannot redirect the Raft peer transport
// =====================================================================================

/// M3-14: a hijacked gossip hint for node 2, pointing at node 3's endpoint, is rejected by
/// gossip validation and never reaches the Raft peer transport — replication to node 2 (over
/// its real, mTLS-verified endpoint) never breaks. M1-32 re-run with real mTLS.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_14_gossip_hint_cannot_bypass_peer_mtls() {
    const METHOD: &str = "m3_14_gossip_hint_cannot_bypass_peer_mtls";
    let cluster = Cluster::builder()
        .nodes(3)
        .cluster_id(CLUSTER)
        .mutual_tls(114)
        .start()
        .await;
    let leader = cluster.leader().await;

    let bad = cluster
        .gossip()
        .poisoned_hint(NodeId(2), PoisonSpec::HijackedEndpoint);
    for observer in cluster.ids() {
        if observer != NodeId(2) {
            cluster.gossip().inject(observer, bad.clone());
        }
    }

    let deadline = cluster.deadline(10);
    let found = cluster
        .wait_for(
            "the hijacked-endpoint hint to be rejected",
            deadline,
            move || {
                my_log_lines_for(METHOD).into_iter().find(|row| {
                    field(row, "@m") == Some("gossip_hint_rejected")
                        && field(row, "reason") == Some("endpoint_mismatch")
                        && field_u64(row, "peer_node_id") == Some(2)
                })
            },
        )
        .await;
    assert!(
        found.is_ok(),
        "the poisoned hint about node 2 was never rejected: {found:?}"
    );

    // The cluster still uses AllowAll here (no authz configured), so "dev" is accepted.
    let put = cluster
        .grpc_client_tls(leader, "dev")
        .put(put_req("/m3-14", "v"))
        .await
        .expect("the leader still commits after the poisoned hint");
    // `put.is_ok()` only proved the *leader* committed, which a quorum of {1,3} satisfies
    // without node 2 ever hearing about it — exactly the state a successful hijack would
    // produce. The claim is about node 2 specifically, so wait on node 2's applied index.
    let index = cluster
        .metrics(leader)
        .last_log_index
        .expect("the leader has a log after a committed write");
    cluster
        .wait_applied_on(&[NodeId(2)], index, cluster.deadline(10))
        .await
        .unwrap_or_else(|t| {
            panic!(
                "node 2 stopped replicating after the hijacked hint (revision {}): {t}",
                put.revision
            )
        });
    cluster.shutdown().await;
}
