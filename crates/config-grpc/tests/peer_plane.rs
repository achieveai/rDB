//! Peer-plane transport behaviour: envelope round trip, identity refusal, and the fault
//! classification OpenRaft's retry policy depends on.

mod support;

use std::time::Duration;

use config_core::{ClusterId, Limits, NodeId, RecoveryEpoch};
use config_engine::netfault::NetFault;
use config_engine::transport::{
    PeerEnvelopeMeta, PeerRequest, PeerResponse, PeerTransport, TransportError,
    PAYLOAD_ENCODING_POSTCARD,
};
use config_grpc::pb;
use config_grpc::pb::peer_service_client::PeerServiceClient;
use config_grpc::{serve_peer_plane, GrpcPeerTransport, PeerIdentity, ServerHandle, TlsMode};
use config_log::{retcd_test, TraceContext};
use openraft::raft::VoteRequest;
use openraft::Vote;
use serde_json::Value;
use tokio::net::TcpListener;

use support::{cluster, FakeSink, CLUSTER};

const DEADLINE: Duration = Duration::from_secs(5);
/// The `payload_encoding` tag of the retired serde-JSON encoding (ADR-0010, fix-round note).
const RETIRED_PAYLOAD_ENCODING_JSON: u32 = 1;
const OTHER_CLUSTER: &str = "ffffffffffffffffffffffffffffffff";

fn meta(cluster_id: ClusterId, from: u64, to: u64) -> PeerEnvelopeMeta {
    PeerEnvelopeMeta {
        cluster_id,
        recovery_epoch: RecoveryEpoch(0),
        from: NodeId(from),
        to: NodeId(to),
        trace: TraceContext::new_root(),
    }
}

fn vote() -> PeerRequest {
    PeerRequest::Vote(VoteRequest::new(Vote::new(3, 1), None))
}

async fn start_peer_plane(sink: &std::sync::Arc<FakeSink>) -> (ServerHandle, String) {
    start_peer_plane_as(sink, sink.identity()).await
}

/// Serve `sink` while stamping `identity` on the answers, so a test can make the responder
/// lie about who it is.
async fn start_peer_plane_as(
    sink: &std::sync::Arc<FakeSink>,
    identity: PeerIdentity,
) -> (ServerHandle, String) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral peer-plane port");
    // ADR-0013: the plane's lines name the node it serves for (see `support::node_span`).
    let handle = support::node_span(sink.node_id.0).in_scope(|| {
        serve_peer_plane(
            sink.handler(),
            listener,
            TlsMode::Insecure,
            identity,
            Limits::DEFAULT,
        )
        .expect("serve peer plane")
    });
    let endpoint = handle.local_addr().to_string();
    (handle, endpoint)
}

#[retcd_test]
async fn m1_grpc_10_vote_round_trips_through_the_peer_plane() {
    let sink = FakeSink::new(cluster(), NodeId(2));
    let (handle, endpoint) = start_peer_plane(&sink).await;
    let transport = GrpcPeerTransport::new(TlsMode::Insecure, NetFault::new(), Limits::DEFAULT);

    let sent = meta(cluster(), 1, 2);
    let response = transport
        .send(sent.clone(), &endpoint, vote(), DEADLINE)
        .await
        .expect("vote round trip");

    match response {
        PeerResponse::Vote(v) => {
            assert!(v.vote_granted, "fake sink grants every vote");
            assert_eq!(v.vote, Vote::new(3, 1), "the candidate's vote is echoed");
        }
        other => panic!("expected a Vote response, got {other:?}"),
    }

    let seen = sink.seen_metas();
    assert_eq!(seen.len(), 1, "the sink saw exactly one call");
    assert_eq!(seen[0].cluster_id, cluster());
    assert_eq!(seen[0].from, NodeId(1));
    assert_eq!(seen[0].to, NodeId(2));
    // ADR-0013: the trace survives the hop, with the caller's span recorded as the parent.
    assert_eq!(seen[0].trace.trace_id, sent.trace.trace_id);
    assert_eq!(seen[0].trace.request_id, sent.trace.request_id);
    assert_eq!(
        seen[0].trace.parent_span_id.as_deref(),
        Some(sent.trace.span_id.as_str())
    );

    handle.shutdown().await.expect("clean shutdown");
}

#[retcd_test]
async fn m1_grpc_11_wrong_cluster_id_is_rejected_as_identity() {
    let sink = FakeSink::new(cluster(), NodeId(2));
    let (_handle, endpoint) = start_peer_plane(&sink).await;
    let transport = GrpcPeerTransport::new(TlsMode::Insecure, NetFault::new(), Limits::DEFAULT);

    let foreign: ClusterId = OTHER_CLUSTER.parse().unwrap();
    let error = transport
        .send(meta(foreign, 1, 2), &endpoint, vote(), DEADLINE)
        .await
        .expect_err("a foreign cluster must be refused");

    assert!(
        matches!(error, TransportError::IdentityRejected(_)),
        "expected IdentityRejected, got {error:?}"
    );
    assert!(
        sink.seen_metas().is_empty(),
        "a refused call must not be recorded as handled"
    );
}

#[retcd_test]
async fn m1_grpc_12_wrong_destination_node_is_rejected_as_identity() {
    let sink = FakeSink::new(cluster(), NodeId(2));
    let (_handle, endpoint) = start_peer_plane(&sink).await;
    let transport = GrpcPeerTransport::new(TlsMode::Insecure, NetFault::new(), Limits::DEFAULT);

    let error = transport
        .send(meta(cluster(), 1, 3), &endpoint, vote(), DEADLINE)
        .await
        .expect_err("an envelope addressed elsewhere must be refused");

    assert!(
        matches!(error, TransportError::IdentityRejected(_)),
        "expected IdentityRejected, got {error:?}"
    );
}

#[retcd_test]
async fn m1_grpc_13_a_closed_port_is_unreachable_not_a_remote_error() {
    let transport = GrpcPeerTransport::new(TlsMode::Insecure, NetFault::new(), Limits::DEFAULT);
    let mut stolen = Vec::new();

    // Naming a closed port means binding an ephemeral one and releasing it — but a test
    // running concurrently in this binary can be handed that exact port by the OS between the
    // release and the dial, and then answers with an h2 error instead of a refusal. So the
    // probe re-binds afterwards to prove the port stayed free, and retries otherwise.
    for _ in 0..5 {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        drop(listener);

        let error = transport
            .send(meta(cluster(), 1, 2), &addr.to_string(), vote(), DEADLINE)
            .await
            .expect_err("nothing is listening");

        match TcpListener::bind(addr).await {
            Ok(reclaimed) => {
                drop(reclaimed);
                assert!(
                    matches!(error, TransportError::Unreachable(_)),
                    "expected Unreachable so OpenRaft backs off, got {error:?}"
                );
                return;
            }
            Err(_) => stolen.push(error),
        }
    }
    panic!("no port stayed closed for a whole dial; observed {stolen:?}");
}

#[retcd_test]
async fn m1_grpc_14_a_blocked_pair_fails_without_dialing() {
    let sink = FakeSink::new(cluster(), NodeId(2));
    let (_handle, endpoint) = start_peer_plane(&sink).await;

    let faults = NetFault::new();
    faults.block(NodeId(1), NodeId(2));
    let transport = GrpcPeerTransport::new(TlsMode::Insecure, faults.clone(), Limits::DEFAULT);

    let error = transport
        .send(meta(cluster(), 1, 2), &endpoint, vote(), DEADLINE)
        .await
        .expect_err("a blocked pair is a cut cable");
    assert!(
        matches!(error, TransportError::Unreachable(_)),
        "expected Unreachable, got {error:?}"
    );
    assert_eq!(
        transport.cached_endpoints(),
        0,
        "a blocked send must not even construct a channel"
    );
    assert!(sink.seen_metas().is_empty(), "the peer must not be reached");

    // The reverse direction is unaffected, and healing restores service.
    faults.unblock_all();
    transport
        .send(meta(cluster(), 1, 2), &endpoint, vote(), DEADLINE)
        .await
        .expect("healed link works");
    assert_eq!(sink.seen_metas().len(), 1);
}

#[retcd_test]
async fn m1_grpc_15_a_dropped_response_is_a_network_error() {
    let sink = FakeSink::new(cluster(), NodeId(2));
    let (_handle, endpoint) = start_peer_plane(&sink).await;

    let faults = NetFault::new();
    faults.drop_response(NodeId(1), NodeId(2));
    let transport = GrpcPeerTransport::new(TlsMode::Insecure, faults, Limits::DEFAULT);

    let error = transport
        .send(meta(cluster(), 1, 2), &endpoint, vote(), DEADLINE)
        .await
        .expect_err("the response was discarded");

    assert!(
        matches!(error, TransportError::Network(_)),
        "expected Network (the call happened, the answer did not), got {error:?}"
    );
    // The peer really did process it — that is the point of drop_response versus block.
    assert_eq!(sink.seen_metas().len(), 1);
}

#[retcd_test]
async fn m1_grpc_16_unknown_payload_encoding_is_invalid_argument() {
    let sink = FakeSink::new(cluster(), NodeId(2));
    let (_handle, endpoint) = start_peer_plane(&sink).await;

    let mut raw = PeerServiceClient::connect(format!("http://{endpoint}"))
        .await
        .expect("peer plane accepts connections");

    let status = raw
        .vote(pb::PeerEnvelope {
            cluster_id: CLUSTER.to_string(),
            recovery_epoch: 0,
            from_node_id: 1,
            to_node_id: 2,
            // The serde-JSON tag this protocol spoke before it moved to postcard: a peer built
            // against the retired encoding must be refused, not silently mis-decoded.
            payload_encoding: RETIRED_PAYLOAD_ENCODING_JSON,
            payload: Default::default(),
        })
        .await
        .expect_err("an unknown encoding must be refused before decoding");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);

    // A well-formed envelope carrying the wrong request kind is refused too.
    let payload = postcard::to_allocvec(&vote()).unwrap();
    let status = raw
        .append_entries(pb::PeerEnvelope {
            cluster_id: CLUSTER.to_string(),
            recovery_epoch: 0,
            from_node_id: 1,
            to_node_id: 2,
            payload_encoding: PAYLOAD_ENCODING_POSTCARD,
            payload: payload.into(),
        })
        .await
        .expect_err("a vote payload on the append_entries rpc is a protocol error");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);

    assert!(sink.seen_metas().is_empty());

    // Every refusal before the handler leaves one warn line inside the rpc span, or a peer
    // that is being refused is invisible to the operator watching the log (ADR-0013).
    let rejected: Vec<_> = support::log_lines(
        module_path!(),
        "m1_grpc_16_unknown_payload_encoding_is_invalid_argument",
    )
    .into_iter()
    .filter(|v| v.get("@m").and_then(Value::as_str) == Some("peer rpc rejected"))
    .collect();
    assert_eq!(
        rejected.len(),
        2,
        "expected one warn line per refusal, got {rejected:#?}"
    );
    for line in &rejected {
        assert_eq!(line.get("@l").and_then(Value::as_str), Some("Warning"));
        assert_eq!(
            line.get("reason").and_then(Value::as_str),
            Some("bad_payload_encoding")
        );
        assert!(line.get("latency_ms").is_some(), "no latency on {line:#?}");
    }
}

/// M1-GRPC-17, restated by M5: `InstallSnapshot` is **served**, on the same terms as the
/// other two RPCs.
///
/// Until M5 this RPC answered `UNIMPLEMENTED` without decoding anything, because the release
/// triggered no snapshots (ADR-0008). ADR-0022 made snapshot install the only way to catch up
/// a learner whose leader has already purged the log it needs, so the claim inverts. The row
/// asserts it through the refusals rather than by round-tripping a snapshot: what matters
/// here is that the RPC runs the shared `PeerSvc::call` pipeline — decode, kind check,
/// identity — and not a shortcut of its own.
#[retcd_test]
async fn m1_grpc_17_install_snapshot_is_served_from_m5() {
    let sink = FakeSink::new(cluster(), NodeId(2));
    let (_handle, endpoint) = start_peer_plane(&sink).await;

    let mut raw = PeerServiceClient::connect(format!("http://{endpoint}"))
        .await
        .expect("peer plane accepts connections");

    let envelope = |payload: bytes::Bytes| pb::PeerEnvelope {
        cluster_id: CLUSTER.to_string(),
        recovery_epoch: 0,
        from_node_id: 1,
        to_node_id: 2,
        payload_encoding: PAYLOAD_ENCODING_POSTCARD,
        payload,
    };

    // An undecodable payload is a payload problem now, not an unimplemented method.
    let status = raw
        .install_snapshot(envelope(Default::default()))
        .await
        .expect_err("an empty payload is not a snapshot");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);

    // A *vote* payload on this RPC trips the kind check, which only exists downstream of
    // decoding — so reaching it proves the envelope was decoded rather than short-circuited.
    let status = raw
        .install_snapshot(envelope(postcard::to_allocvec(&vote()).unwrap().into()))
        .await
        .expect_err("a vote payload on install_snapshot is a protocol error");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);

    assert!(
        sink.seen_metas().is_empty(),
        "neither refusal should have reached the handler"
    );
}

/// ADR-0011: the responder stamps its **own** identity, and the caller checks it.
///
/// Echoing the request's identity fields back would make them self-confirming — whoever
/// answers gets to agree with whatever the caller already believed — so an impostor on the
/// endpoint would be indistinguishable from the node we meant to reach.
#[retcd_test]
async fn m1_grpc_18_an_answer_from_the_wrong_identity_is_rejected() {
    let sink = FakeSink::new(cluster(), NodeId(2));
    let honest = sink.identity();

    // Same sink, but the plane answers as node 9.
    let impostor = PeerIdentity {
        node_id: NodeId(9),
        ..honest
    };
    let (_handle, endpoint) = start_peer_plane_as(&sink, impostor).await;
    let transport = GrpcPeerTransport::new(TlsMode::Insecure, NetFault::new(), Limits::DEFAULT);

    let error = transport
        .send(meta(cluster(), 1, 2), &endpoint, vote(), DEADLINE)
        .await
        .expect_err("an answer from another node must be refused");
    assert!(
        matches!(error, TransportError::IdentityRejected(_)),
        "expected IdentityRejected, got {error:?}"
    );
    assert_eq!(
        sink.seen_metas().len(),
        1,
        "the call itself was served; it is the answer that is refused"
    );

    // Answering as another cluster is refused the same way.
    let foreign = PeerIdentity {
        cluster_id: OTHER_CLUSTER.parse().expect("valid hex"),
        ..honest
    };
    let (_handle, endpoint) = start_peer_plane_as(&sink, foreign).await;
    let error = transport
        .send(meta(cluster(), 1, 2), &endpoint, vote(), DEADLINE)
        .await
        .expect_err("an answer from another cluster must be refused");
    assert!(
        matches!(error, TransportError::IdentityRejected(_)),
        "expected IdentityRejected, got {error:?}"
    );

    // And the truthful case still works, so the check is not simply refusing everything.
    let (handle, endpoint) = start_peer_plane_as(&sink, honest).await;
    transport
        .send(meta(cluster(), 1, 2), &endpoint, vote(), DEADLINE)
        .await
        .expect("a truthful answer is accepted");
    handle.shutdown().await.expect("clean shutdown");
}

/// An injected latency is spent *inside* the caller's budget.
///
/// Applying it outside would make `NetFault::delay` unable to express the failure it exists
/// for — a link slow enough to blow a deadline — because every call would still complete in
/// `delay + deadline`.
#[retcd_test]
async fn m1_grpc_19_an_injected_delay_is_charged_to_the_call_deadline() {
    let sink = FakeSink::new(cluster(), NodeId(2));
    let (_handle, endpoint) = start_peer_plane(&sink).await;

    let faults = NetFault::new();
    // The subject of the test, not a synchronization sleep: the deadline is a tenth of it, so
    // the call must fail on time however fast the machine is.
    faults.delay(NodeId(1), NodeId(2), Duration::from_secs(30));
    let transport = GrpcPeerTransport::new(TlsMode::Insecure, faults, Limits::DEFAULT);

    let started = std::time::Instant::now();
    let error = transport
        .send(
            meta(cluster(), 1, 2),
            &endpoint,
            vote(),
            Duration::from_millis(200),
        )
        .await
        .expect_err("the injected latency exceeds the deadline");

    assert!(
        matches!(error, TransportError::Network(_)),
        "a call that ran out of time is a Network error, got {error:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the deadline did not bound the injected delay: {:?}",
        started.elapsed()
    );
    assert!(
        sink.seen_metas().is_empty(),
        "the call never got past the delay, so the peer never saw it"
    );
}

/// M5/TA-51: the peer plane refuses a retired sender before it decodes the payload.
///
/// The envelope carries bytes that are not a valid postcard `PeerRequest`. A plane that
/// decoded first would answer `bad_payload_encoding`, which would still look like a refusal
/// in a log — while having already handed a fenced node's bytes to a deserializer. The whole
/// value of the fence is that it sits above that line, so this row asserts the *reason*, not
/// merely that something was refused.
#[retcd_test]
async fn m5_grpc_retired_sender_is_refused_before_the_payload_is_decoded() {
    let sink = FakeSink::new(cluster(), NodeId(2));
    sink.retire(NodeId(1));
    let (_handle, endpoint) = start_peer_plane(&sink).await;

    let mut raw = PeerServiceClient::connect(format!("http://{endpoint}"))
        .await
        .expect("peer plane accepts connections");

    let status = raw
        .vote(pb::PeerEnvelope {
            cluster_id: CLUSTER.to_string(),
            recovery_epoch: 0,
            from_node_id: 1,
            to_node_id: 2,
            payload_encoding: PAYLOAD_ENCODING_POSTCARD,
            payload: bytes::Bytes::from_static(b"not a postcard PeerRequest"),
        })
        .await
        .expect_err("a retired sender must be refused");

    // `Unauthenticated`, the same status every other identity refusal carries, and
    // deliberately not `InvalidArgument`: `GrpcPeerTransport` maps this status to
    // `TransportError::IdentityRejected`, which OpenRaft treats as unreachable and backs off
    // from. A retired node must back off — and it must not conclude that its *payload* was the
    // problem and rebuild it. The `reason` field below is what separates the fence from a PKI
    // fault for the operator reading the log; the status code is what shapes the sender.
    assert_eq!(status.code(), tonic::Code::Unauthenticated);
    assert!(
        sink.seen_metas().is_empty(),
        "a retired sender must never reach the handler"
    );

    let rejected: Vec<_> = support::log_lines(
        module_path!(),
        "m5_grpc_retired_sender_is_refused_before_the_payload_is_decoded",
    )
    .into_iter()
    .filter(|v| v.get("@m").and_then(Value::as_str) == Some("peer rpc rejected"))
    .collect();
    assert_eq!(
        rejected.len(),
        1,
        "expected one refusal line: {rejected:#?}"
    );
    assert_eq!(
        rejected[0].get("reason").and_then(Value::as_str),
        Some("identity_retired"),
        "the refusal must name the fence, not the payload: {:#?}",
        rejected[0]
    );

    // And a sender that was never retired still gets through the same plane, so the row is
    // about the fence rather than about the plane being broken.
    let transport = GrpcPeerTransport::new(TlsMode::Insecure, NetFault::new(), Limits::DEFAULT);
    transport
        .send(meta(cluster(), 4, 2), &endpoint, vote(), DEADLINE)
        .await
        .expect("an un-retired sender round trips");
}
