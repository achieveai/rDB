//! `Watch` on the wire, part 2 (test plan §3.9/§3.10, rows M4-50, M4-61, M4-97, M4-102,
//! M4-109, M4-110).
//!
//! `m4_watch_transport.rs` proves the frame shapes and the two new statuses against a scripted
//! store over plain TCP. This file adds the rows that need mTLS itself (a missing client
//! certificate), the rows that compare the direct and gRPC paths side by side, and the two rows
//! that pin the wire against reuse or drift: the new `Watch*` messages' tags (M4-109) and the
//! pre-existing M1-M3 messages' tags (M4-110).
//!
//! One row is **not** in this file, with the precise gap recorded rather than silently skipped:
//!
//! * M4-107 (`client_watch_open_count_is_exact`) is a `config_client::ClientStats` claim, and
//!   `config-grpc` has no dependency — even a dev one — on `config-client` (and adding one was
//!   out of this file's scope; see the handoff notes). `config-client/tests/m4_watch_client.rs`
//!   already carries adjacent coverage (`m4_106_no_termination_triggers_an_automatic_resume`,
//!   which asserts one open per call across three termination shapes, and
//!   `the_trait_method_and_the_inherent_method_agree`, which asserts two calls open twice) but
//!   not the row's exact "3 calls, 2 terminate" fixture.
//!
//! M4-103 (`m4_103_mtls_principal_is_per_stream`) **is** in this file (dated note 2026-09-19,
//! tester-m6a). An earlier attempt recorded a reproducible hang inside `tls_channel`'s
//! `.connect()` on a second TLS handshake that a `tokio::time::timeout` could not interrupt.
//! dev-rotation's bounded investigation (`.claude/scratchpad/conversation_memories/
//! retcd-m4-m6-implementation/dev-rotation-notes.md` §7-8) disproved that as a transport defect:
//! three independent experiments — a standalone tonic/rustls crate dialing two client
//! certificates, this crate's own shipped mTLS suite as a positive control, and the exact M4-103
//! shape (two raw `Channel::connect()` calls against a real formed mTLS cluster, each under
//! `#[retcd_test(flavor = "multi_thread", worker_threads = 2)]`) — all completed in well under a
//! second, the last one in ~10 ms per connect. The abandoned fixture that hung was never
//! committed, so the actual defect cannot be named, but the two remaining candidates are both
//! fixture bugs rather than transport bugs: a wrong CA / wrong `domain_name` pairing (a real
//! cluster's node certificate carries `peer_server_domain(cluster_id, node)`, not an
//! independently chosen local DNS name), or a lock held across the `.await` on a two-worker
//! runtime (the only mechanism consistent with "near-zero CPU and the timeout never fires",
//! because it would stop the timer wheel itself). See the test below.

mod support;

use std::time::Duration;

use bytes::Bytes;
use config_core::{
    ConfigError, ConfigStore, LeaderHint, MutationEvent, MutationEventKind, NodeId, WatchItem,
    WatchRequest,
};
use config_grpc::pb::config_service_client::ConfigServiceClient;
use config_grpc::{
    error_from_status, pb, watch_item_from_pb, MtlsConfig, TlsMode, HEADER_LEADER_ENDPOINT,
    HEADER_LEADER_NODE_ID, HEADER_MIN_REVISION,
};
use config_log::retcd_test;
use prost::Message;
use rcgen::{
    BasicConstraints, CertificateParams, DnType, Ia5String, IsCa, KeyPair, KeyUsagePurpose, SanType,
};
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tonic::Code;

use support::{start_client_plane, FakeStore};

const CALL_DEADLINE: Duration = Duration::from_secs(10);
const SERVER_DNS: &str = "retcd.test";

async fn dial(endpoint: &str) -> ConfigServiceClient<Channel> {
    let channel = Channel::from_shared(format!("http://{endpoint}"))
        .expect("endpoint is a valid authority")
        .connect_timeout(CALL_DEADLINE)
        .connect()
        .await
        .expect("client plane accepts connections");
    ConfigServiceClient::new(channel)
}

fn put_event(revision: u64, key: &'static str, value: &'static str) -> MutationEvent {
    MutationEvent {
        revision,
        key: Bytes::from_static(key.as_bytes()),
        kind: MutationEventKind::Put {
            value: Bytes::from_static(value.as_bytes()),
            create_revision: revision,
        },
    }
}

// --------------------------------------------------------------------------------------------
// mTLS fixtures (M4-50, M4-103) — the same shapes `mtls.rs` uses, kept local to this file so it
// does not have to reach into that file's private helpers.
// --------------------------------------------------------------------------------------------

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

async fn tls_channel(endpoint: &str, tls: &MtlsConfig) -> Result<Channel, tonic::transport::Error> {
    Channel::from_shared(format!("https://{endpoint}"))
        .expect("valid authority")
        .tls_config(tls.client_tls_config())?
        .connect()
        .await
}

/// M4-50: unauthenticated watch is rejected — a client presenting no certificate at all.
///
/// This is the strictest form of "M3 parity for the new RPC": a caller without a certificate
/// never completes the mutual-TLS handshake, so `Watch` is never even dispatched. That is a
/// stronger guarantee than a status code, and it is the same guarantee every unary RPC on this
/// listener already gets from requiring a client certificate — `Watch` gets it "for free"
/// because it shares the one listener and the one TLS profile (ADR-0010).
#[retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn m4_50_unauthenticated_watch_rejected() {
    let ca = new_ca("retcd-test-ca");
    let (server_cert, server_key) = issue(&ca, "server", None);
    let store = FakeStore::new();
    store.set_watch(vec![Ok(WatchItem::Event(put_event(1, "/app/a", "v")))]);
    let server = start_client_plane(
        store.clone(),
        TlsMode::MutualTls(mtls(&ca, server_cert, server_key)),
    )
    .await;

    // No `.identity(..)`: this dialler presents the CA's trust anchor and nothing that proves
    // who it is, which is exactly "a client without a cert".
    let bare = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca.pem.clone()))
        .domain_name(SERVER_DNS);
    let outcome = match Channel::from_shared(format!("https://{}", server.endpoint))
        .expect("valid authority")
        .tls_config(bare)
        .expect("tls config accepted")
        .connect()
        .await
    {
        Err(_) => Err(()),
        Ok(channel) => ConfigServiceClient::new(channel)
            .watch(pb::WatchRequest {
                prefix: Bytes::new(),
                start_after_revision: 0,
                progress_interval_ms: None,
            })
            .await
            .map(|_| ())
            .map_err(|_| ()),
    };
    assert!(
        outcome.is_err(),
        "a client with no certificate must never open a watch stream"
    );
    assert_eq!(
        store.call_count(),
        0,
        "the store must never see a request from an unauthenticated caller"
    );

    server.handle.shutdown().await.expect("clean shutdown");
}

/// M4-61: a compacted cursor maps to the same typed error over the direct call and over gRPC.
///
/// `m4_100_compacted_cursor_maps_to_out_of_range_with_the_minimum` (in `m4_watch_transport.rs`)
/// already proves the gRPC half; this row's job is to show the *direct* call — no transport at
/// all — produces the identical `ConfigError`, so "the client decodes back to the same typed
/// error" is provable rather than assumed.
#[retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn m4_61_compacted_error_maps_to_the_documented_status() {
    let expected = ConfigError::RevisionCompacted {
        minimum_available_revision: 41,
    };

    // The direct path: no gRPC, no transport, just the `ConfigStore` trait.
    let direct_store = FakeStore::new();
    direct_store.set_error(Some(expected.clone()));
    let direct_error = ConfigStore::watch(
        direct_store.as_ref(),
        WatchRequest {
            prefix: Bytes::new(),
            start_after_revision: 10,
            progress_interval: None,
        },
    )
    .await
    .err()
    .expect("the direct call must fail the same way"); // WatchStream is not Debug, so
                                                       // `expect_err` cannot be used here.
    assert_eq!(direct_error, expected);

    // The gRPC path.
    let grpc_store = FakeStore::new();
    grpc_store.set_error(Some(expected.clone()));
    let server = start_client_plane(grpc_store.clone(), TlsMode::Insecure).await;
    let status = dial(&server.endpoint)
        .await
        .watch(pb::WatchRequest {
            prefix: Bytes::new(),
            start_after_revision: 10,
            progress_interval_ms: None,
        })
        .await
        .expect_err("a compacted cursor must be refused over gRPC too");
    assert_eq!(status.code(), Code::OutOfRange);
    assert_eq!(
        status
            .metadata()
            .get(HEADER_MIN_REVISION)
            .and_then(|v| v.to_str().ok()),
        Some("41")
    );
    let grpc_error = error_from_status(&status);
    assert_eq!(
        grpc_error, expected,
        "gRPC must decode back to exactly the direct call's error"
    );
    assert_eq!(
        direct_error, grpc_error,
        "direct and gRPC must agree on the typed error for the same trigger"
    );

    server.handle.shutdown().await.expect("clean shutdown");
}

/// M4-97: `Watch` over mTLS streams the same events, in the same order, as the direct call —
/// the smoke test that the RPC exists and works end to end on the secured listener, the profile
/// every production deployment actually uses.
#[retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn m4_97_grpc_watch_streams_events() {
    let script = || {
        vec![
            Ok(WatchItem::Event(put_event(1, "/app/a", "v1"))),
            Ok(WatchItem::Event(put_event(2, "/app/b", "v2"))),
            Ok(WatchItem::Progress { revision: 2 }),
        ]
    };

    // The direct call: exactly the script, unmodified by any transport.
    let direct_store = FakeStore::new();
    direct_store.set_watch(script());
    let mut direct_items = Vec::new();
    let mut direct_stream = ConfigStore::watch(
        direct_store.as_ref(),
        WatchRequest {
            prefix: Bytes::new(),
            start_after_revision: 0,
            progress_interval: None,
        },
    )
    .await
    .expect("direct watch opens");
    while let Some(item) = tokio_stream::StreamExt::next(&mut direct_stream).await {
        direct_items.push(item.expect("no error in the script"));
    }

    // The gRPC call, over mTLS.
    let ca = new_ca("retcd-test-ca");
    let (server_cert, server_key) = issue(&ca, "server", None);
    let (client_cert, client_key) = issue(
        &ca,
        "svc-a",
        Some(&format!("retcd://{}/client/svc-a", support::CLUSTER)),
    );
    let grpc_store = FakeStore::new();
    grpc_store.set_watch(script());
    let server = start_client_plane(
        grpc_store.clone(),
        TlsMode::MutualTls(mtls(&ca, server_cert, server_key)),
    )
    .await;
    let channel = tls_channel(&server.endpoint, &mtls(&ca, client_cert, client_key))
        .await
        .expect("mutual TLS handshake succeeds");
    let mut stream = ConfigServiceClient::new(channel)
        .watch(pb::WatchRequest {
            prefix: Bytes::new(),
            start_after_revision: 0,
            progress_interval_ms: None,
        })
        .await
        .expect("watch opens over mTLS")
        .into_inner();
    let mut grpc_items = Vec::new();
    while let Some(frame) = stream.message().await.expect("no terminal status") {
        grpc_items.push(watch_item_from_pb(frame).expect("every frame decodes"));
    }

    assert_eq!(
        grpc_items, direct_items,
        "gRPC over mTLS must deliver exactly what the direct call delivers, in order"
    );
    assert_eq!(grpc_items.len(), 3);

    server.handle.shutdown().await.expect("clean shutdown");
}

/// M4-102: `NotLeader`'s trailers survive a *stream* termination, not just a call refusal.
///
/// The leader-change causation itself is a cluster-level claim (M4-79, already covered in
/// `m4_watch_cluster.rs`); this row's job is narrower and belongs at the transport: a
/// `NotLeader` arriving as the last item of an already-open stream must carry the same status
/// and the same trailers a `NotLeader` arriving as the call's own refusal would.
#[retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn m4_102_not_leader_trailers_on_a_stream() {
    let store = FakeStore::new();
    store.set_watch(vec![
        Ok(WatchItem::Event(put_event(1, "/app/a", "v"))),
        Err(ConfigError::NotLeader {
            hint: Some(LeaderHint {
                node_id: NodeId(2),
                endpoint: "127.0.0.1:7002".into(), // testkit:allow-port
            }),
        }),
    ]);
    let server = start_client_plane(store.clone(), TlsMode::Insecure).await;

    let mut stream = dial(&server.endpoint)
        .await
        .watch(pb::WatchRequest {
            prefix: Bytes::new(),
            start_after_revision: 0,
            progress_interval_ms: None,
        })
        .await
        .expect("the stream opens")
        .into_inner();

    let first = stream
        .message()
        .await
        .expect("the first frame is not an error")
        .expect("a frame arrives");
    assert!(matches!(
        watch_item_from_pb(first).expect("decodes"),
        WatchItem::Event(_)
    ));

    let status = stream
        .message()
        .await
        .expect_err("the stream must end with the M3 NotLeader mapping, not silently");
    assert_eq!(
        status.code(),
        Code::FailedPrecondition,
        "no new mapping is invented for streams"
    );
    assert_eq!(
        status
            .metadata()
            .get(HEADER_LEADER_NODE_ID)
            .and_then(|v| v.to_str().ok()),
        Some("2")
    );
    assert_eq!(
        status
            .metadata()
            .get(HEADER_LEADER_ENDPOINT)
            .and_then(|v| v.to_str().ok()),
        Some("127.0.0.1:7002") // testkit:allow-port
    );
    assert_eq!(
        error_from_status(&status),
        ConfigError::NotLeader {
            hint: Some(LeaderHint {
                node_id: NodeId(2),
                endpoint: "127.0.0.1:7002".into(), // testkit:allow-port
            }),
        }
    );

    server.handle.shutdown().await.expect("clean shutdown");
}

/// M4-103: the principal on a `Watch` stream is derived from *that stream's own* certificate,
/// not cached or shared across connections on the same server.
///
/// Two client certificates, two channels, two watch streams, opened one after the other against
/// the same `start_client_plane` server this file's other mTLS rows use. `FakeBackend::store_for`
/// (`support::mod.rs`) records the principal every stream is served under, in call order —
/// exactly the seam `ConfigSvc::dispatch` derives a fresh `Principal` from on every RPC
/// (`client_plane.rs`, "The principal is derived per *stream*, from that connection's
/// certificate"). If a principal were ever memoized per connection, cached per server, or
/// confused between concurrent streams, the two recorded names would collide or reorder.
#[retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn m4_103_mtls_principal_is_per_stream() {
    let ca = new_ca("retcd-test-ca");
    let (server_cert, server_key) = issue(&ca, "server", None);
    let (a_cert, a_key) = issue(
        &ca,
        "svc-a",
        Some(&format!("retcd://{}/client/svc-a", support::CLUSTER)),
    );
    let (b_cert, b_key) = issue(
        &ca,
        "svc-b",
        Some(&format!("retcd://{}/client/svc-b", support::CLUSTER)),
    );

    let store = FakeStore::new();
    let server = start_client_plane(
        store.clone(),
        TlsMode::MutualTls(mtls(&ca, server_cert, server_key)),
    )
    .await;

    // First stream, presenting svc-a's own certificate.
    store.set_watch(vec![Ok(WatchItem::Progress { revision: 1 })]);
    let channel_a = tls_channel(&server.endpoint, &mtls(&ca, a_cert, a_key))
        .await
        .expect("svc-a's mutual TLS handshake succeeds");
    let mut stream_a = ConfigServiceClient::new(channel_a)
        .watch(pb::WatchRequest {
            prefix: Bytes::new(),
            start_after_revision: 0,
            progress_interval_ms: None,
        })
        .await
        .expect("svc-a opens a watch")
        .into_inner();
    let item_a = stream_a
        .message()
        .await
        .expect("no error on svc-a's stream")
        .expect("one frame arrives");
    assert!(matches!(
        watch_item_from_pb(item_a).expect("decodes"),
        WatchItem::Progress { revision: 1 }
    ));

    // Second stream, on a distinct connection, presenting svc-b's own, distinct certificate.
    store.set_watch(vec![Ok(WatchItem::Progress { revision: 2 })]);
    let channel_b = tls_channel(&server.endpoint, &mtls(&ca, b_cert, b_key))
        .await
        .expect("svc-b's mutual TLS handshake succeeds");
    let mut stream_b = ConfigServiceClient::new(channel_b)
        .watch(pb::WatchRequest {
            prefix: Bytes::new(),
            start_after_revision: 0,
            progress_interval_ms: None,
        })
        .await
        .expect("svc-b opens a watch")
        .into_inner();
    let item_b = stream_b
        .message()
        .await
        .expect("no error on svc-b's stream")
        .expect("one frame arrives");
    assert!(matches!(
        watch_item_from_pb(item_b).expect("decodes"),
        WatchItem::Progress { revision: 2 }
    ));

    let seen = store.seen_principals();
    assert_eq!(
        seen.len(),
        2,
        "each of the two streams derives and serves exactly one principal: {seen:?}"
    );
    assert_eq!(seen[0].name, "svc-a", "the first stream's own certificate");
    assert_eq!(seen[1].name, "svc-b", "the second stream's own certificate");
    assert_ne!(
        seen[0].name, seen[1].name,
        "two distinct client certificates must never collapse to one principal"
    );

    server.handle.shutdown().await.expect("clean shutdown");
}

// --------------------------------------------------------------------------------------------
// M4-109 / M4-110: golden bytes.
//
// No M3-era golden-bytes fixture exists anywhere in the tree to decode against (grepped for
// `golden` across the workspace before writing this; the only hits are the `config-core`
// command-envelope and `config-gossip` hint golden vectors, neither of which is this wire).
// The M3 messages here were never pinned at the byte level before M4, so both rows' golden
// vectors are derived from the `.proto`'s own field numbers (`proto/retcd/v1/config.proto`,
// read and confirmed unchanged for every M0-M3 message) and computed once by encoding through
// the generated types, exactly as `config-gossip`'s `GOLDEN_HINT_V1` was: a change to either
// array is a wire-format change, not a typo to "fix" back into agreement.
// --------------------------------------------------------------------------------------------

/// M4-109: the `Watch*` messages' own tags, pinned. Every value below is chosen to stay a
/// single-byte varint so the array is checkable by hand against
/// `proto/retcd/v1/config.proto`'s field numbers (`WatchRequest` 1..3, `WatchResponse`'s
/// `oneof body` 1..2, `Event` 1..2 plus `oneof change` 3..4, `Progress` 1) rather than only by
/// trusting the encoder.
#[test]
fn m4_109_proto_tags_are_from_the_reserved_block() {
    // WatchRequest{ prefix: "a", start_after_revision: 6, progress_interval_ms: Some(100) }
    let request = pb::WatchRequest {
        prefix: Bytes::from_static(b"a"),
        start_after_revision: 6,
        progress_interval_ms: Some(100),
    };
    const GOLDEN_REQUEST: [u8; 7] = [0x0A, 0x01, 0x61, 0x10, 0x06, 0x18, 0x64];
    assert_eq!(request.encode_to_vec(), GOLDEN_REQUEST);
    assert_eq!(
        pb::WatchRequest::decode(&GOLDEN_REQUEST[..]).expect("decodes"),
        request
    );

    // Progress{ revision: 9 }
    let progress = pb::Progress { revision: 9 };
    const GOLDEN_PROGRESS: [u8; 2] = [0x08, 0x09];
    assert_eq!(progress.encode_to_vec(), GOLDEN_PROGRESS);

    // WatchResponse{ progress: Progress{ revision: 9 } } — the oneof's tag 2.
    let response_progress = pb::WatchResponse {
        body: Some(pb::watch_response::Body::Progress(progress)),
    };
    const GOLDEN_RESPONSE_PROGRESS: [u8; 4] = [0x12, 0x02, 0x08, 0x09];
    assert_eq!(response_progress.encode_to_vec(), GOLDEN_RESPONSE_PROGRESS);

    // Event{ revision: 9, key: "z", delete: Deleted{} } — the oneof's tag 4, an empty embedded
    // message (a tombstone truly carries nothing).
    let delete_event = pb::Event {
        revision: 9,
        key: Bytes::from_static(b"z"),
        change: Some(pb::event::Change::Delete(pb::Deleted {})),
    };
    const GOLDEN_DELETE_EVENT: [u8; 7] = [0x08, 0x09, 0x12, 0x01, 0x7A, 0x22, 0x00];
    assert_eq!(delete_event.encode_to_vec(), GOLDEN_DELETE_EVENT);
    assert_eq!(
        pb::Event::decode(&GOLDEN_DELETE_EVENT[..]).expect("decodes"),
        delete_event
    );

    // Event{ revision: 5, key: "k", put: Record{ key: "k", value: "v", create_revision: 5,
    // mod_revision: 5 } } — the oneof's tag 3, and `Record`'s own four tags (already shipped at
    // M0-M3, reused unchanged inside the new message per §17 "never reuse a tag" — reused as a
    // *type*, not as a field number of `Event` itself).
    let put_event = pb::Event {
        revision: 5,
        key: Bytes::from_static(b"k"),
        change: Some(pb::event::Change::Put(pb::Record {
            key: Bytes::from_static(b"k"),
            value: Bytes::from_static(b"v"),
            create_revision: 5,
            mod_revision: 5,
        })),
    };
    const GOLDEN_PUT_EVENT: [u8; 17] = [
        0x08, 0x05, 0x12, 0x01, 0x6B, 0x1A, 0x0A, 0x0A, 0x01, 0x6B, 0x12, 0x01, 0x76, 0x18, 0x05,
        0x20, 0x05,
    ];
    assert_eq!(put_event.encode_to_vec(), GOLDEN_PUT_EVENT);
    assert_eq!(
        pb::Event::decode(&GOLDEN_PUT_EVENT[..]).expect("decodes"),
        put_event
    );

    let response_event = pb::WatchResponse {
        body: Some(pb::watch_response::Body::Event(put_event)),
    };
    assert_eq!(
        response_event.encode_to_vec()[0],
        0x0A,
        "event is body tag 1"
    );
}

/// M4-110: the M0-M3 messages this milestone did not touch still encode and decode exactly as
/// they did before `Watch` existed. A representative get (read) and put (write), plus the
/// response shape a mutation returns, all built with only the fields M0-M3 ever set (the M5
/// `dedup` field and the M5 `dedup_hit`/`dedup_recorded` response fields are absent/false,
/// which proto3 never puts on the wire).
#[test]
fn m4_110_m3_wire_compatibility_unbroken() {
    let get = pb::GetRequest {
        key: Bytes::from_static(b"k"),
    };
    const GOLDEN_GET: [u8; 3] = [0x0A, 0x01, 0x6B];
    assert_eq!(get.encode_to_vec(), GOLDEN_GET);
    assert_eq!(
        pb::GetRequest::decode(&GOLDEN_GET[..]).expect("an M3 GetRequest still decodes"),
        get
    );

    let put = pb::PutRequest {
        key: Bytes::from_static(b"k"),
        value: Bytes::from_static(b"v"),
        expected_mod_revision: Some(3),
        dedup: None,
    };
    const GOLDEN_PUT: [u8; 8] = [0x0A, 0x01, 0x6B, 0x12, 0x01, 0x76, 0x18, 0x03];
    assert_eq!(
        put.encode_to_vec(),
        GOLDEN_PUT,
        "an M5 field that is absent must not appear on an M3-shaped message"
    );
    let decoded_put =
        pb::PutRequest::decode(&GOLDEN_PUT[..]).expect("an M3 PutRequest still decodes");
    assert_eq!(decoded_put, put);
    assert_eq!(decoded_put.dedup, None, "M3 never set the M5 field");
    // Round trip: re-encoding what M3 sent must reproduce exactly what M3 sent — nothing new
    // silently attaches itself to an old-shaped message under the current build.
    assert_eq!(decoded_put.encode_to_vec(), GOLDEN_PUT);

    let response = pb::MutationResponse {
        outcome: pb::MutationOutcome::Applied as i32,
        revision: 7,
        exists: true,
        current_mod_revision: 9,
        dedup_hit: false,
        dedup_recorded: false,
    };
    const GOLDEN_RESPONSE: [u8; 8] = [0x08, 0x01, 0x10, 0x07, 0x18, 0x01, 0x20, 0x09];
    assert_eq!(response.encode_to_vec(), GOLDEN_RESPONSE);
    let decoded_response = pb::MutationResponse::decode(&GOLDEN_RESPONSE[..])
        .expect("an M3 MutationResponse still decodes");
    assert_eq!(decoded_response, response);
    assert!(!decoded_response.dedup_hit);
    assert!(!decoded_response.dedup_recorded);
    assert_eq!(decoded_response.encode_to_vec(), GOLDEN_RESPONSE);
}
