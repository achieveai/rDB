//! Gossip hint validation, as a table (test plan TA-7, M1-19..M1-22; ADR-0003).
//!
//! No sockets and no cluster: [`validate_hint`] is pure, so the whole rejection matrix can be
//! stated as data. The end-to-end proof that gossip cannot influence routing lives in
//! `m1_cluster.rs`; this file proves the decision function itself is right.

mod common;

use std::collections::{BTreeMap, BTreeSet};

use common::{cluster_id, identity, recovery_epoch};
use config_core::{ClusterId, Liveness, NodeId, ObservedPeerHint, RecoveryEpoch};
use config_engine::{
    validate_hint, HintVerdict, InProcTransport, MembershipView, REASON_CLUSTER_MISMATCH,
    REASON_ENDPOINT_MISMATCH, REASON_EPOCH_MISMATCH, REASON_NOT_FORMED, REASON_SELF_CLAIM,
    REASON_UNKNOWN_NODE,
};

/// A committed 3-voter membership at the in-process endpoints.
fn membership() -> MembershipView {
    let voters: BTreeSet<NodeId> = (1..=3).map(NodeId).collect();
    let endpoints = voters
        .iter()
        .map(|id| (*id, InProcTransport::endpoint(*id)))
        .collect::<BTreeMap<_, _>>();
    MembershipView {
        // In-process nodes serve both planes from one listener, so the client endpoint is the
        // peer endpoint. Hint validation only ever looks at the peer plane.
        client_endpoints: endpoints.clone(),
        endpoints,
        voters,
        membership_log_id: Some((1, 1)),
    }
}

fn hint(node_id: u64, cluster: ClusterId, endpoint: &str) -> ObservedPeerHint {
    hint_at(node_id, cluster, recovery_epoch(), endpoint)
}

/// Like [`hint`] but names the recovery epoch the peer claims (ADR-0011).
fn hint_at(
    node_id: u64,
    cluster: ClusterId,
    epoch: RecoveryEpoch,
    endpoint: &str,
) -> ObservedPeerHint {
    ObservedPeerHint {
        cluster_id: cluster,
        recovery_epoch: epoch,
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
            "the right cluster at the wrong recovery epoch is a fenced-off peer, not a peer",
            hint_at(
                2,
                cluster_id(),
                RecoveryEpoch(recovery_epoch().0 + 1),
                &InProcTransport::endpoint(NodeId(2)),
            ),
            formed.clone(),
            HintVerdict::Rejected {
                reason: REASON_EPOCH_MISMATCH,
            },
        ),
        (
            "an epoch behind ours is refused for the same reason as one ahead",
            hint_at(
                2,
                cluster_id(),
                RecoveryEpoch(recovery_epoch().0 - 1),
                &InProcTransport::endpoint(NodeId(2)),
            ),
            formed.clone(),
            HintVerdict::Rejected {
                reason: REASON_EPOCH_MISMATCH,
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
                client_endpoints: BTreeMap::new(),
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
    // The whole verdict is an optional reason string and nothing else, so an accepted one has
    // no room for an endpoint. Until 2026-09-21 this line read
    // `size_of_val(&accepted) == size_of::<HintVerdict>()`, which is true of every sized value
    // in Rust whatever it holds — the row asserted nothing and would have passed with a
    // routable field added (M0/M1 manual tester, candidate 1). Comparing against
    // `Option<&'static str>` instead is a claim that can fail: a payload on either variant
    // makes the enum wider than the reason it exists to carry. `HintVerdict::Accepted` gaining
    // a field is also a compile error in `a10_...` below, which names the variant by path.
    assert_eq!(
        std::mem::size_of::<HintVerdict>(),
        std::mem::size_of::<Option<&'static str>>(),
        "an accepted hint must carry no payload: the verdict is a reason or nothing"
    );
}

/// A10 (ADR-0011): identity is the *pair* `(cluster_id, recovery_epoch)`. After an unsafe
/// recovery the cluster id does not change, so a peer that was fenced off during the recovery
/// still advertises a hint whose `cluster_id` matches ours exactly. Only the epoch tells them
/// apart, which is why `ObservedPeerHint` carries one.
#[config_log::retcd_test]
fn a10_a_hint_at_the_wrong_recovery_epoch_is_rejected_though_the_cluster_matches() {
    let me = identity(1);
    let view = membership();
    let endpoint = InProcTransport::endpoint(NodeId(2));

    let right = hint_at(2, cluster_id(), me.recovery_epoch, &endpoint);
    assert_eq!(right.cluster_id, me.cluster_id);
    assert_eq!(
        validate_hint(&right, &view, &me),
        HintVerdict::Accepted,
        "the same hint at our epoch must still be accepted, or the test proves nothing"
    );

    let stale = hint_at(
        2,
        cluster_id(),
        RecoveryEpoch(me.recovery_epoch.0 - 1),
        &endpoint,
    );
    // Everything a pre-A10 validator could see is identical between the two hints.
    assert_eq!(stale.cluster_id, right.cluster_id);
    assert_eq!(stale.node_id, right.node_id);
    assert_eq!(stale.peer_endpoint, right.peer_endpoint);

    assert_eq!(
        validate_hint(&stale, &view, &me),
        HintVerdict::Rejected {
            reason: REASON_EPOCH_MISMATCH,
        }
    );
    assert_eq!(REASON_EPOCH_MISMATCH, "recovery_epoch mismatch");
}
