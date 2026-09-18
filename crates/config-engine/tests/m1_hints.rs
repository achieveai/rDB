//! Gossip hint validation, as a table (test plan TA-7, M1-19..M1-22; ADR-0003).
//!
//! No sockets and no cluster: [`validate_hint`] is pure, so the whole rejection matrix can be
//! stated as data. The end-to-end proof that gossip cannot influence routing lives in
//! `m1_cluster.rs`; this file proves the decision function itself is right.

mod common;

use std::collections::{BTreeMap, BTreeSet};

use common::{cluster_id, identity};
use config_core::{ClusterId, Liveness, NodeId, ObservedPeerHint};
use config_engine::{
    validate_hint, HintVerdict, InProcTransport, MembershipView, REASON_CLUSTER_MISMATCH,
    REASON_ENDPOINT_MISMATCH, REASON_NOT_FORMED, REASON_SELF_CLAIM, REASON_UNKNOWN_NODE,
};

/// A committed 3-voter membership at the in-process endpoints.
fn membership() -> MembershipView {
    let voters: BTreeSet<NodeId> = (1..=3).map(NodeId).collect();
    MembershipView {
        endpoints: voters
            .iter()
            .map(|id| (*id, InProcTransport::endpoint(*id)))
            .collect::<BTreeMap<_, _>>(),
        voters,
        membership_log_id: Some((1, 1)),
    }
}

fn hint(node_id: u64, cluster: ClusterId, endpoint: &str) -> ObservedPeerHint {
    ObservedPeerHint {
        cluster_id: cluster,
        node_id: NodeId(node_id),
        peer_endpoint: endpoint.to_string(),
        client_endpoint: None,
        software_version: "0.1.0".to_string(),
        protocol_version: 1,
        zone: None,
        liveness: Liveness::Alive,
    }
}

#[config_log::retcd_test]
fn ta7_validate_hint_rejection_matrix() {
    let formed = membership();
    let me = identity(1);
    let other_cluster = ClusterId::from_bytes([42u8; 16]);

    let cases: Vec<(&str, ObservedPeerHint, MembershipView, HintVerdict)> = vec![
        (
            "a voter advertising its committed endpoint is coherent",
            hint(2, cluster_id(), &InProcTransport::endpoint(NodeId(2))),
            formed.clone(),
            HintVerdict::Accepted,
        ),
        (
            "a peer from another cluster is refused before anything else is looked at",
            hint(2, other_cluster, &InProcTransport::endpoint(NodeId(2))),
            formed.clone(),
            HintVerdict::Rejected {
                reason: REASON_CLUSTER_MISMATCH,
            },
        ),
        (
            "a hint claiming to be us is refused even when it is otherwise correct",
            hint(1, cluster_id(), &InProcTransport::endpoint(NodeId(1))),
            formed.clone(),
            HintVerdict::Rejected {
                reason: REASON_SELF_CLAIM,
            },
        ),
        (
            "nothing can be corroborated before membership is committed",
            hint(2, cluster_id(), &InProcTransport::endpoint(NodeId(2))),
            MembershipView::default(),
            HintVerdict::Rejected {
                reason: REASON_NOT_FORMED,
            },
        ),
        (
            "a node id that is not a committed voter cannot be admitted by gossip",
            hint(9, cluster_id(), "inproc://9"),
            formed.clone(),
            HintVerdict::Rejected {
                reason: REASON_UNKNOWN_NODE,
            },
        ),
        (
            "a voter advertising a different endpoint is the hijack ADR-0003 exists to refuse",
            hint(2, cluster_id(), "inproc://attacker"),
            formed.clone(),
            HintVerdict::Rejected {
                reason: REASON_ENDPOINT_MISMATCH,
            },
        ),
        (
            "a voter with no committed address cannot be corroborated either",
            hint(2, cluster_id(), &InProcTransport::endpoint(NodeId(2))),
            MembershipView {
                voters: (1..=3).map(NodeId).collect(),
                endpoints: BTreeMap::new(),
                membership_log_id: Some((1, 1)),
            },
            HintVerdict::Rejected {
                reason: REASON_ENDPOINT_MISMATCH,
            },
        ),
    ];

    for (why, h, view, expected) in cases {
        assert_eq!(
            validate_hint(&h, &view, &me),
            expected,
            "case failed: {why}"
        );
    }
}

/// An accepted hint is still only telemetry: it never adds a voter and never supplies an
/// address. The verdict type says so by carrying no payload at all.
#[config_log::retcd_test]
fn ta7_an_accepted_hint_carries_nothing_a_caller_could_route_on() {
    let accepted = validate_hint(
        &hint(2, cluster_id(), &InProcTransport::endpoint(NodeId(2))),
        &membership(),
        &identity(1),
    );
    assert!(accepted.is_accepted());
    assert_eq!(accepted.reason(), None);
    // `HintVerdict::Accepted` is a unit variant: there is no endpoint to be tempted by.
    assert_eq!(
        std::mem::size_of_val(&accepted),
        std::mem::size_of::<HintVerdict>()
    );
}
