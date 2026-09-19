//! M5 learner lifecycle, the promotion lag bound, removal, and the retirement fence
//! (spec §13.2, §19.8, §21; ADR-0023; architecture A5; test plan TA-45, TA-51).
//!
//! These rows exercise the engine directly rather than the gRPC admin plane, because the
//! properties under test are decisions the *leader* and *every peer* make, not transport
//! details: the catch-up predicate is evaluated on the leader against live replication
//! metrics, and the fence has to hold on a follower that was not involved in the removal.
//! `config-grpc` adds a wire format on top of these and nothing else.
//!
//! Every wait is a multiple of the configured election timeout (test plan §6 rule 3).

mod common;

use std::collections::BTreeSet;

use common::{identity, Cluster};
use config_core::NodeId;
use config_engine::transport::{PeerEnvelopeMeta, PeerRequest};
use config_engine::{AdminError, FormationPlan, InProcTransport, RaftTimers};
use config_log::retcd_test;
use config_storage::types::RaftNodeId;
use openraft::raft::VoteRequest;
use openraft::Vote;

/// The node that joins after formation in every row here.
const JOINER: NodeId = NodeId(3);

/// Start three nodes but form the cluster from only nodes 1 and 2.
///
/// Node 3 is running and registered on the transport the whole time — it simply is not a
/// member. That is the honest starting point for a learner row: a node that does not exist
/// yet could not be caught up, so "the learner caught up" would prove nothing.
async fn two_voters_and_a_spare(promote_max_lag: u64) -> Cluster {
    let cluster = Cluster::start_with_gossip_and_tweak(
        3,
        RaftTimers::default(),
        |_| std::sync::Arc::new(config_core::NoGossip),
        move |cfg| {
            cfg.promote_max_lag = promote_max_lag;
        },
    )
    .await;
    let voters = [NodeId(1), NodeId(2)]
        .into_iter()
        .map(|id| (id, InProcTransport::endpoint(id)))
        .collect::<Vec<_>>();
    cluster
        .node(1)
        .form_cluster(FormationPlan::new(&identity(1), voters))
        .await
        .expect("formation from a fresh two-voter cluster");
    cluster.wait_leader().await;
    // Committed membership, not just "a leader exists". OpenRaft refuses a second
    // configuration change while the first is uncommitted, so a row that called `add_learner`
    // straight after `wait_leader` would be racing formation and would fail as
    // `Raft("already undergoing a configuration change")` — a harness artefact wearing the
    // costume of a real refusal. `Cluster::wait_formed` cannot be used here because it waits
    // on *every* started node, and node 3 is deliberately not a member.
    cluster
        .wait_for(
            "committed membership on both voters",
            cluster.elections(8),
            |c| {
                [NodeId(1), NodeId(2)]
                    .iter()
                    .all(|id| {
                        c.get_node(*id)
                            .membership_report()
                            .membership
                            .membership_log_id
                            .is_some()
                    })
                    .then_some(())
            },
        )
        .await;
    cluster
}

/// The leader, which after forming from node 1 with two voters is whichever of 1 and 2 won.
fn leader_id(cluster: &Cluster) -> NodeId {
    cluster.leader()
}

/// A voter that is not the leader, for the leader-only rows.
fn a_follower(cluster: &Cluster) -> NodeId {
    *cluster
        .followers()
        .iter()
        .find(|id| **id != JOINER)
        .expect("a two-voter cluster has one follower")
}

/// Wait until `node_id` is a committed voter as seen by the leader.
async fn wait_voter(cluster: &Cluster, node_id: NodeId) {
    let leader = leader_id(cluster);
    cluster
        .wait_for(
            "the joiner is a committed voter",
            cluster.elections(8),
            |c| {
                c.get_node(leader)
                    .membership_report()
                    .membership
                    .voters
                    .contains(&node_id)
                    .then_some(())
            },
        )
        .await;
}

/// Wait until the leader has heard `node_id` acknowledge a log index at all.
async fn wait_replicating(cluster: &Cluster, node_id: NodeId) {
    let leader = leader_id(cluster);
    cluster
        .wait_for("the learner is replicating", cluster.elections(8), |c| {
            let report = c.get_node(leader).membership_report();
            report
                .replication
                .get(&node_id)
                .and_then(|p| p.matched_index)
                .map(|_| ())
        })
        .await;
}

// -------------------------------------------------------------------------------------------
// the learner lifecycle
// -------------------------------------------------------------------------------------------

/// A learner joins, replicates, and is promoted; at no point is it counted toward quorum
/// before its promotion commits.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_learner_joins_replicates_and_is_promoted() {
    let cluster = two_voters_and_a_spare(config_engine::DEFAULT_PROMOTE_MAX_LAG).await;
    let leader = leader_id(&cluster);
    let node = cluster.get_node(leader);

    cluster
        .put(leader, "/m5/a", "1")
        .await
        .expect("a write before the learner joins");

    node.add_learner(
        JOINER,
        InProcTransport::endpoint(JOINER),
        InProcTransport::endpoint(JOINER),
    )
    .await
    .expect("adding a fresh node as a learner");

    // A learner is a member that is not a voter. The distinction is the whole safety claim:
    // quorum is still 2 of {1, 2} while node 3 is catching up, so a learner that fell over
    // could not stall a write.
    let report = node.membership_report();
    assert!(
        report.learners.contains(&JOINER),
        "the joiner must appear as a learner: {report:?}"
    );
    assert!(
        !report.membership.voters.contains(&JOINER),
        "a learner must not be a committed voter: {report:?}"
    );
    assert_eq!(
        report.joint_config_len, 1,
        "adding a learner is not a joint change: {report:?}"
    );
    assert!(
        report.authoritative,
        "the leader's own report is authoritative"
    );

    wait_replicating(&cluster, JOINER).await;

    node.promote_voter(JOINER)
        .await
        .expect("promoting a caught-up learner");
    wait_voter(&cluster, JOINER).await;

    let report = node.membership_report();
    assert_eq!(
        report.membership.voters,
        BTreeSet::from([NodeId(1), NodeId(2), JOINER]),
        "the promoted learner must be a committed voter"
    );
    assert!(
        !report.learners.contains(&JOINER),
        "a promoted node is no longer a learner: {report:?}"
    );
    assert_eq!(
        report.joint_config_len, 1,
        "the joint change must have completed, not stalled: {report:?}"
    );

    // The new voter carries the state it replicated, not an empty one. Asserted against its
    // *own* store rather than through a read: reads are leader-linearizable (ADR-0009), so
    // asking node 3 for the key would answer `NotLeader` and prove nothing about what node 3
    // holds.
    cluster
        .wait_converged(&cluster.ids(), cluster.elections(8))
        .await;
    let mut held = false;
    cluster
        .store(JOINER)
        .reader()
        .with_state(&mut |state| held = state.get(&common::key("/m5/a")).is_some());
    assert!(
        held,
        "the promoted voter must hold the write that preceded it"
    );
}

/// Promotion is refused while the learner is behind, and allowed once it is not.
///
/// The predicate is evaluated on the leader against `RaftMetrics.replication` at the instant
/// the call arrives (A5, OQ-50). That is the only honest oracle openraft offers: its own
/// `add_learner(blocking = true)` waits for catch-up and then discards the wait's result
/// (research trap T7), so an `Ok` from it would be an acknowledgement dressed as a promise.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_promotion_refuses_a_lagging_learner() {
    // A threshold of zero: the learner must match the leader's last log index exactly. Legal,
    // and the only value that makes "lagging" reproducible without racing a busy cluster.
    let cluster = two_voters_and_a_spare(0).await;
    let leader = leader_id(&cluster);
    let node = cluster.get_node(leader);

    // Block the leader's replication to the joiner *before* it is added, so it is added into
    // a state where it cannot possibly catch up.
    cluster.faults().block(leader, JOINER);

    node.add_learner(
        JOINER,
        InProcTransport::endpoint(JOINER),
        InProcTransport::endpoint(JOINER),
    )
    .await
    .expect("adding a learner does not require it to be reachable");

    for i in 0..5 {
        cluster
            .put(leader, &format!("/m5/b{i}"), "1")
            .await
            .expect("the two remaining voters still form a quorum");
    }

    match node.promote_voter(JOINER).await {
        Err(AdminError::Lagging { node_id, lag, max }) => {
            assert_eq!(node_id, JOINER);
            assert_eq!(max, 0, "the refusal must quote the configured threshold");
            assert!(lag > 0, "a blocked learner lags by more than zero");
        }
        other => panic!("a blocked learner must not be promotable, got {other:?}"),
    }

    // And it did not become a voter as a side effect of being asked about.
    assert!(
        !node.membership_report().membership.voters.contains(&JOINER),
        "a refused promotion must change nothing"
    );

    cluster.faults().unblock(leader, JOINER);
    cluster
        .wait_for("the learner reaches lag 0", cluster.elections(10), |c| {
            (c.get_node(leader).replication_lag(JOINER) == Some(0)).then_some(())
        })
        .await;

    node.promote_voter(JOINER)
        .await
        .expect("a caught-up learner is promotable");
    wait_voter(&cluster, JOINER).await;
}

/// Promoting a node that was never added is refused, and says so specifically.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_promotion_refuses_a_non_member() {
    let cluster = two_voters_and_a_spare(config_engine::DEFAULT_PROMOTE_MAX_LAG).await;
    let node = cluster.get_node(leader_id(&cluster));

    match node.promote_voter(JOINER).await {
        Err(e @ AdminError::NotAMember { node_id }) => {
            assert_eq!(node_id, JOINER);
            assert_eq!(e.reason(), "not_a_member");
        }
        other => panic!("promoting a non-member must be refused, got {other:?}"),
    }
}

// -------------------------------------------------------------------------------------------
// removal and the fence
// -------------------------------------------------------------------------------------------

/// Removal takes a voter out of membership, retires its id everywhere, and the fence then
/// refuses that id on **every** node — including one that was not the leader.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_removal_retires_the_identity_and_fences_it() {
    let cluster = two_voters_and_a_spare(config_engine::DEFAULT_PROMOTE_MAX_LAG).await;
    let leader = leader_id(&cluster);
    let node = cluster.get_node(leader);

    node.add_learner(
        JOINER,
        InProcTransport::endpoint(JOINER),
        InProcTransport::endpoint(JOINER),
    )
    .await
    .expect("add the learner");
    wait_replicating(&cluster, JOINER).await;
    node.promote_voter(JOINER).await.expect("promote it");
    wait_voter(&cluster, JOINER).await;

    node.remove_member(JOINER)
        .await
        .expect("removing a voter from a three-voter cluster");

    cluster
        .wait_for("the removal is committed", cluster.elections(8), |c| {
            let report = c.get_node(leader).membership_report();
            (!report.membership.voters.contains(&JOINER)
                && !report.learners.contains(&JOINER)
                && report.joint_config_len == 1)
                .then_some(())
        })
        .await;

    // The retirement is replicated state, not a leader-local note. A node that was partitioned
    // while the removal happened learns it by applying the log, which is why every surviving
    // node — not only the leader — must show it.
    let survivors = [NodeId(1), NodeId(2)];
    cluster
        .wait_for(
            "every survivor knows the id is retired",
            cluster.elections(8),
            |c| {
                survivors
                    .iter()
                    .all(|id| c.get_node(*id).retired_nodes().contains(&JOINER))
                    .then_some(())
            },
        )
        .await;

    // Readmission half of the fence: the leader refuses to invite it back.
    match node
        .add_learner(
            JOINER,
            InProcTransport::endpoint(JOINER),
            InProcTransport::endpoint(JOINER),
        )
        .await
    {
        Err(e @ AdminError::Retired { node_id }) => {
            assert_eq!(node_id, JOINER);
            assert_eq!(e.reason(), "node_retired");
        }
        other => panic!("a retired id must never be re-addable, got {other:?}"),
    }

    // Network half of the fence, asserted on a **follower**: the retired node holds a genuine
    // certificate and will dial whichever peer answers first, so a check that lived only on
    // the leader would fence nothing (TA-51).
    let follower = *survivors
        .iter()
        .find(|id| **id != leader)
        .expect("a two-voter cluster has one follower");
    let handler = cluster.get_node(follower).peer_handler();
    let meta = PeerEnvelopeMeta {
        cluster_id: common::cluster_id(),
        recovery_epoch: common::recovery_epoch(),
        from: JOINER,
        to: follower,
        trace: config_log::TraceContext::new_root(),
    };
    let before = authn_rejected_by_plane(&cluster, follower).await;

    let vote: VoteRequest<RaftNodeId> = VoteRequest::new(Vote::new(99, JOINER.0), None);
    let rejection = handler
        .handle(meta, PeerRequest::Vote(vote))
        .await
        .expect_err("a retired sender must be refused");
    assert!(
        matches!(rejection, config_engine::transport::PeerReject::Retired { node_id } if node_id == JOINER),
        "the refusal must name the fence, not a generic identity mismatch: {rejection:?}"
    );

    // E6 / ADR-0026: the scrape has to say which plane refused. A single undivided counter was
    // published entirely as `plane="client"`, so a fenced peer — a membership event — showed up
    // in an operator's dashboard as a client authentication failure and sent them to look at
    // certificates that were never the problem.
    let after = authn_rejected_by_plane(&cluster, follower).await;
    assert_eq!(
        after.peer,
        before.peer + 1.0,
        "the peer-plane sample must account for the fence"
    );
    assert_eq!(
        after.client, before.client,
        "and the client plane must be untouched by it"
    );

    // A term-99 vote from a fenced node is exactly the danger: had it been processed, it would
    // have moved the surviving cluster's term. It did not.
    assert!(
        cluster.try_leader().is_some(),
        "the fenced vote must not have disturbed the surviving cluster"
    );
}

/// Re-issuing a removal against an already-retired id is a no-op, not an error.
///
/// An operator whose first call timed out has no way to know whether it committed, so the
/// only safe contract is that repeating it is harmless.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_removal_is_idempotent() {
    let cluster = two_voters_and_a_spare(config_engine::DEFAULT_PROMOTE_MAX_LAG).await;
    let leader = leader_id(&cluster);
    let node = cluster.get_node(leader);

    node.add_learner(
        JOINER,
        InProcTransport::endpoint(JOINER),
        InProcTransport::endpoint(JOINER),
    )
    .await
    .expect("add the learner");
    wait_replicating(&cluster, JOINER).await;
    node.promote_voter(JOINER).await.expect("promote it");
    wait_voter(&cluster, JOINER).await;

    node.remove_member(JOINER).await.expect("first removal");
    let retired_after_first = node.retired_nodes();
    node.remove_member(JOINER)
        .await
        .expect("a repeated removal is a no-op, not a refusal");
    assert_eq!(
        node.retired_nodes(),
        retired_after_first,
        "a repeated removal must not change the retired set"
    );
    assert_eq!(
        node.membership_report().joint_config_len,
        1,
        "a repeated removal must not leave the cluster in a joint config"
    );
}

// -------------------------------------------------------------------------------------------
// leader-only
// -------------------------------------------------------------------------------------------

/// Every mutating admin operation is leader-only, and refuses with a followable hint.
///
/// The hint names the *client* endpoint, because a hint exists to tell a caller where to
/// retry and the peer plane is not somewhere a client can go (ADR-0009).
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_admin_mutations_are_leader_only() {
    let cluster = two_voters_and_a_spare(config_engine::DEFAULT_PROMOTE_MAX_LAG).await;
    let leader = leader_id(&cluster);
    let follower = a_follower(&cluster);
    let node = cluster.get_node(follower);

    let refusals = vec![
        (
            "add_learner",
            node.add_learner(
                JOINER,
                InProcTransport::endpoint(JOINER),
                InProcTransport::endpoint(JOINER),
            )
            .await
            .err(),
        ),
        ("promote_voter", node.promote_voter(JOINER).await.err()),
        ("remove_member", node.remove_member(leader).await.err()),
    ];

    for (op, refusal) in refusals {
        match refusal {
            Some(AdminError::NotLeader { hint }) => {
                let hint = hint.unwrap_or_else(|| {
                    panic!("{op}: a follower that knows the leader must hand back a hint")
                });
                assert_eq!(hint.node_id, leader, "{op}: the hint must name the leader");
                assert_eq!(
                    hint.endpoint,
                    cluster
                        .get_node(follower)
                        .membership_report()
                        .membership
                        .client_endpoint_of(leader)
                        .expect("the leader's client endpoint is committed"),
                    "{op}: the hint must name a committed client endpoint"
                );
            }
            other => panic!("{op} on a follower must be NotLeader, got {other:?}"),
        }
    }

    // A follower still *reports*: reads are served everywhere, and the report says plainly
    // that it is not authoritative rather than leaving an empty replication map to be
    // misread as "nothing is replicating".
    let report = node.membership_report();
    assert!(!report.authoritative, "a follower is not authoritative");
    assert!(
        report.replication.is_empty(),
        "openraft populates the replication map only on a leader: {report:?}"
    );
    assert_eq!(report.current_leader, Some(leader));
    assert!(
        report.membership.is_formed(),
        "a follower still reports committed membership"
    );
}

/// An id that is out of membership and **not** retired is fenced, not refused (C5-02).
///
/// This is the state a leader is left in when `RemoveMember`'s step 2 committed and step 3 did
/// not: the node is out of the configuration and its identity is still live. `propose_retire`
/// tells the operator to re-issue `RemoveMember` to finish the job, so a re-issue that answered
/// `NotAMember` made the documented recovery impossible and left the removed node able to vote.
///
/// The row names an id the cluster never had, because after step 2 commits the leader's view of
/// the two situations is byte-for-byte identical — the membership entry that would have told
/// them apart is exactly the one step 2 removed. That indistinguishability is *why* the
/// fallthrough is the right behaviour, so it is also the honest way to test it.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_removal_fences_an_id_that_is_already_out_of_membership() {
    let cluster = two_voters_and_a_spare(config_engine::DEFAULT_PROMOTE_MAX_LAG).await;
    let leader = leader_id(&cluster);
    let node = cluster.get_node(leader);

    assert!(
        !node.membership_report().membership.voters.contains(&JOINER)
            && !node.membership_report().learners.contains(&JOINER),
        "the spare is deliberately not a member"
    );
    assert!(
        !node.retired_nodes().contains(&JOINER),
        "and it has not been fenced yet"
    );

    node.remove_member(JOINER)
        .await
        .expect("finishing an interrupted removal must not be refused");

    let survivors = [NodeId(1), NodeId(2)];
    cluster
        .wait_for(
            "every survivor knows the id is retired",
            cluster.elections(8),
            |c| {
                survivors
                    .iter()
                    .all(|id| c.get_node(*id).retired_nodes().contains(&JOINER))
                    .then_some(())
            },
        )
        .await;

    // The fence is the whole point of finishing the sequence, so assert the network half of it
    // on a follower rather than trusting the retired set alone.
    let follower = *survivors
        .iter()
        .find(|id| **id != leader)
        .expect("a two-voter cluster has one follower");
    let handler = cluster.get_node(follower).peer_handler();
    let meta = PeerEnvelopeMeta {
        cluster_id: common::cluster_id(),
        recovery_epoch: common::recovery_epoch(),
        from: JOINER,
        to: follower,
        trace: config_log::TraceContext::new_root(),
    };
    let vote: VoteRequest<RaftNodeId> = VoteRequest::new(Vote::new(99, JOINER.0), None);
    let rejection = handler
        .handle(meta, PeerRequest::Vote(vote))
        .await
        .expect_err("the id must be fenced once the sequence finished");
    assert!(
        matches!(rejection, config_engine::transport::PeerReject::Retired { node_id } if node_id == JOINER),
        "the refusal must name the fence: {rejection:?}"
    );

    // Still idempotent afterwards, which is the property M5-67 asserts from the other side.
    node.remove_member(JOINER)
        .await
        .expect("a second re-issue is a no-op");
    assert!(
        cluster.try_leader().is_some(),
        "none of this disturbed the surviving cluster"
    );
}

/// The two `retcd_authn_rejected_total` samples on one node, read out of a real scrape.
struct AuthnByPlane {
    client: f64,
    peer: f64,
}

/// Scrape `node_id` and pull both planes' samples out of the exposition text.
///
/// Parsed from the rendered text rather than read off `NodeMetrics`, because the defect E6
/// describes was in the *exporter*: the counters were fine and the label was a lie.
async fn authn_rejected_by_plane(cluster: &Cluster, node_id: NodeId) -> AuthnByPlane {
    let text = cluster
        .get_node(node_id)
        .metrics_report()
        .await
        .render_prometheus();
    let sample = |plane: &str| -> f64 {
        let needle = format!("plane=\"{plane}\"");
        let line = text
            .lines()
            .find(|l| l.starts_with("retcd_authn_rejected_total{") && l.contains(&needle))
            .unwrap_or_else(|| {
                panic!("no retcd_authn_rejected_total sample for plane {plane:?} in:\n{text}")
            });
        line.rsplit(' ')
            .next()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or_else(|| panic!("unparsable sample line {line:?}"))
    };
    AuthnByPlane {
        client: sample("client"),
        peer: sample("peer"),
    }
}
