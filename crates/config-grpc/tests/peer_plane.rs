//! Peer-plane transport behaviour: envelope round trip, identity refusal, and the fault
//! classification OpenRaft's retry policy depends on.

mod support;

use std::time::Duration;

use config_core::{ClusterId, NodeId, RecoveryEpoch};
use config_engine::netfault::NetFault;
use config_engine::transport::{
    PeerEnvelopeMeta, PeerRequest, PeerResponse, PeerTransport, TransportError,
    PAYLOAD_ENCODING_JSON,
};
use config_grpc::pb;
use config_grpc::pb::peer_service_client::PeerServiceClient;
use config_grpc::{serve_peer_plane, GrpcPeerTransport, ServerHandle, TlsMode};
use config_log::{retcd_test, TraceContext};
use openraft::raft::VoteRequest;
use openraft::Vote;
use tokio::net::TcpListener;

use support::FakeSink;

const DEADLINE: Duration = Duration::from_secs(5);
const CLUSTER: &str = "0123456789abcdef0123456789abcdef";
const OTHER_CLUSTER: &str = "ffffffffffffffffffffffffffffffff";

fn cluster() -> ClusterId {
    CLUSTER.parse().expect("test cluster id is valid hex")
}

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
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral peer-plane port");
    let handle =
        serve_peer_plane(sink.handler(), listener, TlsMode::Insecure).expect("serve peer plane");
    let endpoint = handle.local_addr().to_string();
    (handle, endpoint)
}

#[retcd_test]
async fn vote_round_trips_through_the_peer_plane() {
    let sink = FakeSink::new(cluster(), NodeId(2));
    let (handle, endpoint) = start_peer_plane(&sink).await;
    let transport = GrpcPeerTransport::new(TlsMode::Insecure, NetFault::new());

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
async fn wrong_cluster_id_is_rejected_as_identity() {
    let sink = FakeSink::new(cluster(), NodeId(2));
    let (_handle, endpoint) = start_peer_plane(&sink).await;
    let transport = GrpcPeerTransport::new(TlsMode::Insecure, NetFault::new());

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
async fn wrong_destination_node_is_rejected_as_identity() {
    let sink = FakeSink::new(cluster(), NodeId(2));
    let (_handle, endpoint) = start_peer_plane(&sink).await;
    let transport = GrpcPeerTransport::new(TlsMode::Insecure, NetFault::new());

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
async fn a_closed_port_is_unreachable_not_a_remote_error() {
    let transport = GrpcPeerTransport::new(TlsMode::Insecure, NetFault::new());
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
async fn a_blocked_pair_fails_without_dialing() {
    let sink = FakeSink::new(cluster(), NodeId(2));
    let (_handle, endpoint) = start_peer_plane(&sink).await;

    let faults = NetFault::new();
    faults.block(NodeId(1), NodeId(2));
    let transport = GrpcPeerTransport::new(TlsMode::Insecure, faults.clone());

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
async fn a_dropped_response_is_a_network_error() {
    let sink = FakeSink::new(cluster(), NodeId(2));
    let (_handle, endpoint) = start_peer_plane(&sink).await;

    let faults = NetFault::new();
    faults.drop_response(NodeId(1), NodeId(2));
    let transport = GrpcPeerTransport::new(TlsMode::Insecure, faults);

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
async fn unknown_payload_encoding_is_invalid_argument() {
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
            payload_encoding: PAYLOAD_ENCODING_JSON + 1,
            payload: Default::default(),
        })
        .await
        .expect_err("an unknown encoding must be refused before decoding");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);

    // A well-formed envelope carrying the wrong request kind is refused too.
    let payload = serde_json::to_vec(&vote()).unwrap();
    let status = raw
        .append_entries(pb::PeerEnvelope {
            cluster_id: CLUSTER.to_string(),
            recovery_epoch: 0,
            from_node_id: 1,
            to_node_id: 2,
            payload_encoding: PAYLOAD_ENCODING_JSON,
            payload: payload.into(),
        })
        .await
        .expect_err("a vote payload on the append_entries rpc is a protocol error");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);

    assert!(sink.seen_metas().is_empty());
}

#[retcd_test]
async fn install_snapshot_is_unimplemented_in_this_release() {
    let sink = FakeSink::new(cluster(), NodeId(2));
    let (_handle, endpoint) = start_peer_plane(&sink).await;

    let mut raw = PeerServiceClient::connect(format!("http://{endpoint}"))
        .await
        .expect("peer plane accepts connections");

    let status = raw
        .install_snapshot(pb::PeerEnvelope {
            cluster_id: CLUSTER.to_string(),
            recovery_epoch: 0,
            from_node_id: 1,
            to_node_id: 2,
            payload_encoding: PAYLOAD_ENCODING_JSON,
            payload: Default::default(),
        })
        .await
        .expect_err("snapshots are never served in this release (ADR-0008)");
    assert_eq!(status.code(), tonic::Code::Unimplemented);
}
