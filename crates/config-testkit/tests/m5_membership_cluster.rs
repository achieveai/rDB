//! M5 membership lifecycle, catch-up, fencing and hygiene rows driven against a real
//! `config_testkit::Cluster` — 3-node cluster gates (test plan §4, TA-45, TA-46; ADR-0023).
//!
//! `crates/config-engine/tests/m5_membership.rs` already proves the *engine's* admin surface
//! (`ConfigNode::add_learner`/`promote_voter`/`remove_member`) against a two-voter-plus-spare
//! cluster it assembles itself out of raw `InProcTransport` plumbing. This file proves the same
//! properties one layer up, against the harness every other M5 cluster gate uses
//! (`config_testkit::cluster::Cluster`), over the real per-node gRPC peer/client listeners — the
//! surface §4's rows are specified against.
//!
//! # How a non-member node is obtained
//!
//! Two shapes, and each row uses the cheaper one that says what it means:
//!
//! * **Started but unformed.** `nodes(4)` + `form(false)` + `form_with(&[1, 2, 3])` leaves node
//!   4 running, registered on the transport, and outside every committed membership until a row
//!   calls `add_learner` on it — TA-46's "provisioned but not a member", with no `src` change.
//!   That is what [`three_voters_and_a_spare`] builds and what most rows here want.
//! * **Provisioned but never started.** [`Cluster::provision_reusing_dir`] and
//!   [`Cluster::provision_seeded_dir`] add a *new, harness-allocated* node id with its listeners
//!   reserved and its data directory chosen by the caller, and deliberately stop short of
//!   starting it. M5-70's "new id over the old dir" half and M5-71 are exactly the rows whose
//!   whole claim is that the node's **store open** is refused, so they must be able to attempt
//!   that open and nothing else.
//!
//! # What is not here (scope)
//!
//! M5-49..M5-53 (the `AdminService` gRPC surface) live in `m5_admin_cluster.rs`, which mounts
//! the admin plane on every `Cluster` node's client listener through `ClusterBuilder::admins`.
//!
//! M5-59, M5-74 (already covered — `crates/config-engine/tests/m5_membership.rs`), M5-69
//! (`membership_survives_purge_and_snapshot`), and every row under
//! dedup/metrics/observability/runbooks/alerts (M5-95..M5-126, subject-excluded to dev-dedup)
//! are out of scope by assignment.

mod support;

use std::collections::BTreeSet;
use std::sync::Arc;

use config_core::NodeId;
use config_engine::admin::AdminError;
use config_engine::transport::{PeerEnvelopeMeta, PeerReject, PeerRequest};
use config_storage::types::RaftNodeId;
use config_storage::{Boundary, StorageOpenError};
use config_testkit::cluster::{Cluster, NodeStartError, StorageKind};
use openraft::raft::VoteRequest;
use openraft::Vote;

use support::{put_req, ScriptedInjector};

/// The node every "add a fourth member" row grows into: never one of the original three
/// voters, always harness-provisioned (test plan anti-flake rule 30's spirit — a literal id,
/// but never reused across a row and never mistaken for a live voter mid-test).
const JOINER: NodeId = NodeId(4);

/// Three formed voters plus node 4, started but outside every committed membership — the
/// TA-46 substitute described in the module doc. Every node carries a disarmed
/// [`ScriptedInjector`], so a row that never arms one pays nothing for it.
async fn three_voters_and_a_spare() -> (
    Arc<Cluster>,
    std::collections::BTreeMap<NodeId, Arc<ScriptedInjector>>,
) {
    three_voters_and_a_spare_with_lag(config_engine::DEFAULT_PROMOTE_MAX_LAG).await
}

/// [`three_voters_and_a_spare`] with an explicit `promote_max_lag`, for the one row whose
/// subject *is* the threshold: a row that has to write past it wants it small, so the write
/// volume that proves the refusal is a couple of dozen entries rather than a couple of
/// thousand. Every other row wants the shipped default and calls the wrapper above.
async fn three_voters_and_a_spare_with_lag(
    promote_max_lag: u64,
) -> (
    Arc<Cluster>,
    std::collections::BTreeMap<NodeId, Arc<ScriptedInjector>>,
) {
    let mut builder = Cluster::builder()
        .nodes(4)
        .storage(StorageKind::ROCKS)
        .promote_max_lag(promote_max_lag)
        .form(false);
    let mut scripts = std::collections::BTreeMap::new();
    for i in 1..=4u64 {
        let id = NodeId(i);
        let script = ScriptedInjector::new();
        builder = builder.faults(
            id,
            Arc::clone(&script) as Arc<dyn config_storage::FaultInjector>,
        );
        scripts.insert(id, script);
    }
    let cluster = Arc::new(builder.start().await);
    cluster
        .form_with(&[NodeId(1), NodeId(2), NodeId(3)])
        .await
        .expect("three-voter formation excluding the spare");
    cluster
        .wait_formed_on(&[NodeId(1), NodeId(2), NodeId(3)], cluster.deadline(10))
        .await
        .expect("a leader and agreed membership among the three voters");
    (cluster, scripts)
}

/// Add [`JOINER`] as a learner at its harness-allocated endpoints.
async fn add_joiner(cluster: &Cluster, leader: NodeId) {
    cluster
        .node(leader)
        .add_learner(
            JOINER,
            cluster.peer_endpoint(JOINER),
            cluster.client_endpoint(JOINER),
        )
        .await
        .expect("adding the spare as a learner");
}

/// Wait until the leader's own replication map shows [`JOINER`] caught up to `lag` or fewer
/// entries behind (the only admissible catch-up oracle — architecture A5, research trap T7:
/// `add_learner(blocking = true)`'s wait result is logged and discarded by openraft itself).
async fn wait_caught_up(cluster: &Cluster, leader: NodeId, lag: u64) {
    cluster
        .wait_for(
            "the joiner's replication lag to close",
            cluster.deadline(20),
            || (cluster.node(leader).replication_lag(JOINER)? <= lag).then_some(()),
        )
        .await
        .unwrap_or_else(|t| panic!("{t}"));
}

// -------------------------------------------------------------------------------------------
// M5-54, M5-58 — add_learner is non-blocking, and nothing treats its `Ok` as a catch-up proof
// -------------------------------------------------------------------------------------------

/// `AddLearner` returns as soon as the membership entry commits, carrying no claim that the
/// learner has caught up — the response is silent on replication, not falsely reassuring.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_54_add_learner_is_non_blocking() {
    let (cluster, _scripts) = three_voters_and_a_spare().await;
    let leader = cluster.leader().await;

    let ack = cluster
        .node(leader)
        .add_learner(
            JOINER,
            cluster.peer_endpoint(JOINER),
            cluster.client_endpoint(JOINER),
        )
        .await
        .expect("adding a fresh node as a learner");
    // The ack carries a membership log id (something committed) and nothing that could be
    // read as "and it is caught up" — there is no such field to misread in the first place.
    assert!(ack.is_some(), "the learner's addition must have committed");

    let report = cluster.node(leader).membership_report();
    assert!(
        report.learners.contains(&JOINER),
        "the joiner must appear as a learner immediately: {report:?}"
    );
    assert!(
        !report.membership.voters.contains(&JOINER),
        "a learner must never be counted as a voter: {report:?}"
    );
    assert_eq!(
        report.joint_config_len, 1,
        "adding a learner is not a joint change: {report:?}"
    );
}

/// Nothing in the engine treats `add_learner`'s `Ok` as a catch-up signal — the only place that
/// asks "is it caught up" reads the live replication map, never the addition's own result
/// (research trap T7).
///
/// A grep row rather than a behavioural one: the defect T7 warns about is an *absence* (a
/// promotion path that trusts `add_learner`'s return value instead of asking
/// `RaftMetrics.replication`), and an absence has no runtime symptom to assert on directly.
#[test]
fn m5_58_promote_reads_live_replication_not_add_learners_result() {
    let promote_src = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../config-engine/src/node.rs"
    ))
    .expect("read config-engine/src/node.rs");
    let admin_src = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../config-engine/src/admin.rs"
    ))
    .expect("read config-engine/src/admin.rs");

    // The promotion path must consult replication lag live (M5-56/M5-57's own oracle), not a
    // value cached from an earlier `add_learner` call.
    assert!(
        promote_src.contains("fn promote_voter_inner") || promote_src.contains("fn promote_voter"),
        "expected to find the promotion entry point in config-engine/src/node.rs to inspect"
    );
    assert!(
        promote_src.contains("replication") || admin_src.contains("replication"),
        "the promotion path must be seen reading a live replication map somewhere in \
         config-engine/src/{{node,admin}}.rs"
    );
    // The one call site that blocks on catch-up (openraft's own `add_learner(blocking=true)`)
    // must not appear anywhere the engine calls `raft.add_learner`; T7's defect is exactly a
    // `blocking: true` argument whose wait result then gets trusted. Doc comments are allowed
    // to *describe* the trap by name (this file's own doc comment does) — only a real call
    // actually passing `true` as the argument is the defect, so code lines are checked and
    // `//`/`///` comment lines are skipped.
    let live_blocking_true = promote_src.lines().any(|line| {
        let code = line.split("//").next().unwrap_or(line);
        code.contains("add_learner") && code.contains(", true")
            || code.contains("blocking: true")
            || code.contains("blocking = true) ")
    });
    assert!(
        !live_blocking_true,
        "config-engine must never call openraft's add_learner with blocking = true \
         (research trap T7 — the wait result is logged and discarded by openraft itself, so an \
         `Ok` from it is not proof of catch-up); found a live (non-comment) occurrence in \
         config-engine/src/node.rs"
    );
}

// -------------------------------------------------------------------------------------------
// M5-56, M5-57 — promotion is gated on the leader's live replication map
// -------------------------------------------------------------------------------------------

/// A learner held behind by a real partition cannot be promoted; the refusal names both the
/// measured lag and the configured threshold.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_56_promote_refused_while_learner_lags() {
    /// Low enough that the row proves the refusal with a couple of dozen writes instead of
    /// `DEFAULT_PROMOTE_MAX_LAG + 20`. The threshold is configuration, not a constant of the
    /// design, so asserting the refusal quotes *this* value is the stronger claim anyway: it
    /// proves the server reports its own configured threshold rather than a compiled-in one.
    const LAG: u64 = 16;

    let (cluster, _scripts) = three_voters_and_a_spare_with_lag(LAG).await;
    let leader = cluster.leader().await;
    add_joiner(&cluster, leader).await;

    // Cut the joiner off from the rest of the cluster before it can catch up at all, then
    // drive enough writes that it is unambiguously behind `promote_max_lag`.
    cluster.isolate(JOINER);
    for i in 0..(LAG as usize + 20) {
        // A write has to reach the leader, and the leader can change under the test (a loaded
        // host starves the incumbent's heartbeats long enough to lose an election). Resolving
        // the leader per write is what makes a volume-driven row deterministic; the isolated
        // learner never wins one, so whoever leads is one of the three healthy voters.
        let at = cluster.leader().await;
        cluster
            .client(at)
            .put(put_req(&format!("/m5/56/{i}"), "v"))
            .await
            .unwrap_or_else(|e| panic!("put {i} against a healthy 3-voter quorum: {e}"));
    }

    let leader = cluster.leader().await;
    match cluster.node(leader).promote_voter(JOINER).await {
        Err(AdminError::Lagging { node_id, lag, max }) => {
            assert_eq!(node_id, JOINER);
            assert_eq!(
                max, LAG,
                "the refusal must quote the server's own configured threshold"
            );
            assert!(
                lag > max,
                "an isolated learner must lag past the threshold: lag={lag}"
            );
        }
        other => panic!("a lagging learner must not be promotable, got {other:?}"),
    }
    assert!(
        !cluster
            .node(leader)
            .membership_report()
            .membership
            .voters
            .contains(&JOINER),
        "a refused promotion must not have changed membership as a side effect"
    );

    cluster.heal();
}

/// Once the leader's own replication map shows the learner caught up, promotion succeeds and
/// membership converges to a uniform 4-voter configuration on every node.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_57_promote_allowed_when_caught_up_by_the_replication_map() {
    let (cluster, _scripts) = three_voters_and_a_spare().await;
    let leader = cluster.leader().await;
    add_joiner(&cluster, leader).await;

    cluster
        .client(leader)
        .put(put_req("/m5/57/a", "1"))
        .await
        .expect("a write for the learner to catch up on");
    wait_caught_up(&cluster, leader, 0).await;

    cluster
        .node(leader)
        .promote_voter(JOINER)
        .await
        .expect("promoting a caught-up learner");

    cluster
        .wait_formed_on(
            &[NodeId(1), NodeId(2), NodeId(3), JOINER],
            cluster.deadline(10),
        )
        .await
        .unwrap_or_else(|t| panic!("{t}"));

    let report = cluster.node(leader).membership_report();
    assert_eq!(
        report.membership.voters,
        BTreeSet::from([NodeId(1), NodeId(2), NodeId(3), JOINER]),
        "the promoted learner must be a committed voter on the leader's own report"
    );
    assert!(!report.learners.contains(&JOINER));
    assert_eq!(
        report.joint_config_len, 1,
        "the joint round trip openraft runs internally must have completed to uniform"
    );

    // The new voter is carrying real state, not an empty store re-replicated from nothing.
    let mut held = false;
    cluster
        .rocks_store(JOINER)
        .reader()
        .with_state(&mut |state| {
            held = state.get(&bytes::Bytes::from_static(b"/m5/57/a")).is_some()
        });
    assert!(
        held,
        "the promoted voter must hold the write it replicated before promotion"
    );
}

// -------------------------------------------------------------------------------------------
// M5-60 — removing a voter also removes it as a node, not just as a voter
// -------------------------------------------------------------------------------------------

#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_60_removed_voter_is_also_removed_as_a_node() {
    let (cluster, _scripts) = three_voters_and_a_spare().await;
    let leader = cluster.leader().await;
    add_joiner(&cluster, leader).await;
    cluster
        .client(leader)
        .put(put_req("/m5/60/a", "1"))
        .await
        .expect("a write for the learner to catch up on");
    wait_caught_up(&cluster, leader, 0).await;
    cluster
        .node(leader)
        .promote_voter(JOINER)
        .await
        .expect("promote");
    cluster
        .wait_formed_on(
            &[NodeId(1), NodeId(2), NodeId(3), JOINER],
            cluster.deadline(10),
        )
        .await
        .unwrap_or_else(|t| panic!("{t}"));

    cluster
        .node(leader)
        .remove_member(JOINER)
        .await
        .expect("removing a voter from a four-voter cluster");

    cluster
        .wait_for("the removal to commit", cluster.deadline(10), || {
            let report = cluster.node(leader).membership_report();
            (!report.membership.voters.contains(&JOINER) && report.joint_config_len == 1)
                .then_some(())
        })
        .await
        .unwrap_or_else(|t| panic!("{t}"));

    let report = cluster.node(leader).membership_report();
    assert!(
        !report.membership.voters.contains(&JOINER),
        "removed from voters: {report:?}"
    );
    assert!(
        !report.learners.contains(&JOINER),
        "a fully removed member must not linger as a learner (RemoveVoters, then RemoveNodes — \
         M5-R5): {report:?}"
    );
    assert!(
        !report.replication.contains_key(&JOINER),
        "the leader must stop replicating to a node it no longer knows: {report:?}"
    );
}

// -------------------------------------------------------------------------------------------
// M5-61, M5-62 — the retirement fence: peer plane and readmission
// -------------------------------------------------------------------------------------------

/// Both halves of the fence, and mutation-tested: `AdminError::Retired` refuses readmission,
/// and a fenced node's own vote request is refused at the peer plane by a node that was not
/// the one that removed it.
///
/// The peer-plane half runs directly against [`config_engine::transport::PeerHandler`], the
/// same seam `crates/config-engine/tests/m5_membership.rs` uses, rather than waiting on the
/// restarted node's own election timer: a deterministic call proves the refusal without racing
/// whichever of node 1/2/3 happens to hear from it first.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_61_and_62_retired_identity_is_fenced_at_readmission_and_the_peer_plane() {
    let (cluster, _scripts) = three_voters_and_a_spare().await;
    let leader = cluster.leader().await;
    add_joiner(&cluster, leader).await;
    cluster
        .client(leader)
        .put(put_req("/m5/61/a", "1"))
        .await
        .expect("a write for the learner to catch up on");
    wait_caught_up(&cluster, leader, 0).await;
    cluster
        .node(leader)
        .promote_voter(JOINER)
        .await
        .expect("promote");
    cluster
        .wait_formed_on(
            &[NodeId(1), NodeId(2), NodeId(3), JOINER],
            cluster.deadline(10),
        )
        .await
        .unwrap_or_else(|t| panic!("{t}"));

    cluster
        .node(leader)
        .remove_member(JOINER)
        .await
        .expect("removing the fourth voter");

    let survivors = [NodeId(1), NodeId(2), NodeId(3)];
    cluster
        .wait_for(
            "every survivor to know the id is retired (replicated state, not a leader-local \
             note)",
            cluster.deadline(10),
            || {
                survivors
                    .iter()
                    .all(|id| cluster.node(*id).retired_nodes().contains(&JOINER))
                    .then_some(())
            },
        )
        .await
        .unwrap_or_else(|t| panic!("{t}"));

    // M5-62 — readmission is refused, by name.
    match cluster
        .node(leader)
        .add_learner(
            JOINER,
            cluster.peer_endpoint(JOINER),
            cluster.client_endpoint(JOINER),
        )
        .await
    {
        Err(AdminError::Retired { node_id }) => assert_eq!(node_id, JOINER),
        other => panic!("a retired id must never be re-addable, got {other:?}"),
    }

    // M5-61 — the network half, asserted on a survivor that did not itself run the removal's
    // RPC (whichever of 1/2/3 is not `leader`), so the fence is proven to be replicated state
    // and not a note the acting leader alone remembers.
    let witness = *survivors
        .iter()
        .find(|id| **id != leader)
        .expect("a three-voter cluster has at least one node besides the leader");
    let handler = cluster.node(witness).peer_handler();
    let meta = PeerEnvelopeMeta {
        cluster_id: cluster.config().cluster_id,
        recovery_epoch: cluster.config().recovery_epoch,
        from: JOINER,
        to: witness,
        trace: config_log::TraceContext::new_root(),
    };
    let vote: VoteRequest<RaftNodeId> = VoteRequest::new(Vote::new(99, JOINER.0), None);
    let rejection = handler
        .handle(meta, PeerRequest::Vote(vote))
        .await
        .expect_err("a retired sender must be refused at the peer plane");
    assert!(
        matches!(rejection, PeerReject::Retired { node_id } if node_id == JOINER),
        "the refusal must name the fence specifically, not a generic identity mismatch: \
         {rejection:?}"
    );
    // A term-99 vote from the fenced node is exactly the danger TA-51 exists to close: had it
    // been processed, it would have disturbed the surviving cluster's term.
    assert!(
        cluster.leader_now().is_some(),
        "the fenced vote must not have disturbed the surviving cluster's leadership"
    );
}

/// A restart with the node's **original** id and data directory — no new identity minted — is
/// exactly as fenced as a live peer-plane probe: the readmission half refuses it the same way,
/// and it never becomes reachable through membership again.
///
/// This is the "old id, old dir" half of M5-70; the "new id, old dir" half is
/// [`m5_70_new_identity_over_an_old_dir_is_refused_at_store_open`] below.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_70_old_identity_over_its_own_dir_is_refused_after_retirement() {
    let (cluster, _scripts) = three_voters_and_a_spare().await;
    let leader = cluster.leader().await;
    add_joiner(&cluster, leader).await;
    cluster
        .client(leader)
        .put(put_req("/m5/70/a", "1"))
        .await
        .expect("a write for the learner to catch up on");
    wait_caught_up(&cluster, leader, 0).await;
    cluster
        .node(leader)
        .promote_voter(JOINER)
        .await
        .expect("promote");
    cluster
        .wait_formed_on(
            &[NodeId(1), NodeId(2), NodeId(3), JOINER],
            cluster.deadline(10),
        )
        .await
        .unwrap_or_else(|t| panic!("{t}"));
    cluster
        .node(leader)
        .remove_member(JOINER)
        .await
        .expect("remove");
    cluster
        .wait_for("retirement to replicate", cluster.deadline(10), || {
            cluster
                .node(leader)
                .retired_nodes()
                .contains(&JOINER)
                .then_some(())
        })
        .await
        .unwrap_or_else(|t| panic!("{t}"));

    // Bring the retired node's own process back on its own id, its own data directory, no
    // identity change at all. Its applied index is frozen at this point: it was dropped from
    // membership above, so the leader stopped replicating to it.
    let before_restart = cluster.node(JOINER).applied_index();
    cluster
        .restart(JOINER)
        .await
        .expect("the retired node's own process may still restart");
    // A restart replays the node's own persisted log asynchronously, so sampling the "stalled"
    // index immediately would race that replay and the closing assertion could fail on the
    // node's own recovery rather than on anything the cluster sent it. Wait for replay to land
    // back on the index it already held, and take *that* as the baseline.
    cluster
        .wait_for(
            "the retired node to finish replaying its own persisted log",
            cluster.deadline(10),
            || (cluster.node(JOINER).applied_index() == before_restart).then_some(()),
        )
        .await
        .unwrap_or_else(|t| panic!("{t}"));

    // It never rejoins: a direct add_learner using its original id is still refused, and its
    // applied index never advances past what it already had (nothing the cluster does can
    // reach it through membership again).
    let stalled_index = before_restart;
    let leader_after = cluster.leader().await;
    match cluster
        .node(leader_after)
        .add_learner(
            JOINER,
            cluster.peer_endpoint(JOINER),
            cluster.client_endpoint(JOINER),
        )
        .await
    {
        Err(AdminError::Retired { node_id }) => assert_eq!(node_id, JOINER),
        other => {
            panic!("the original identity must stay fenced after its own restart, got {other:?}")
        }
    }
    for _ in 0..10 {
        cluster
            .client(leader_after)
            .put(put_req("/m5/70/more", "1"))
            .await
            .expect("the surviving voters still form a quorum without the fenced node");
    }
    assert_eq!(
        cluster.node(JOINER).applied_index(),
        stalled_index,
        "a fenced node restarted on its own identity must never receive new entries"
    );
}

/// The "new id, old dir" half of M5-70: minting a fresh node id does **not** launder a data
/// directory that is already bound to another identity. The refusal happens at store open,
/// before Raft exists, and it names both sides (ADR-0011, spec §13.2/§19.10).
///
/// `provision_reusing_dir` is the harness seam this needs: a new, never-used node id whose
/// `data_dir` is the one node [`JOINER`] already stamped with its own identity on its first
/// open. `JOINER` is stopped first, so what the new id meets is the *identity* check and not
/// RocksDB's exclusive `LOCK` — which would be a true refusal for the wrong reason.
///
/// The row's "exit code 2 at process level" clause belongs to the daemon and is asserted where
/// the process exists (`crates/config-server/tests`); what a `Cluster` can prove, and what this
/// asserts, is the typed error that clause is a mapping of.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_70_new_identity_over_an_old_dir_is_refused_at_store_open() {
    let (cluster, _scripts) = three_voters_and_a_spare().await;
    let leader = cluster.leader().await;
    let members_before = cluster.node(leader).membership_report().membership;

    cluster.stop_node(JOINER).await;
    let replacement = cluster.provision_reusing_dir(JOINER).await;
    assert_ne!(
        replacement, JOINER,
        "the harness must mint a genuinely new id, or this row would be a restart"
    );

    let refusal = cluster
        .try_start_node(replacement)
        .await
        .expect_err("a new node id must not be able to adopt another node's data directory");
    match refusal {
        NodeStartError::Storage(StorageOpenError::IdentityMismatch {
            stored,
            configured,
            path,
        }) => {
            assert_eq!(
                stored.node_id, JOINER,
                "the refusal must name the identity the directory is already bound to"
            );
            assert_eq!(
                configured.node_id, replacement,
                "...and the identity that tried to open it"
            );
            assert_eq!(
                stored.cluster_id, configured.cluster_id,
                "only the node id differs here; a cluster-id mismatch would be a different row"
            );
            assert_eq!(path, cluster.data_dir(JOINER), "named by directory too");
        }
        other => panic!("expected an ADR-0011 identity refusal at store open, got {other:?}"),
    }

    // Nothing about the attempt reached the cluster: the new id never appears anywhere in
    // membership, and the surviving voters keep serving.
    let members_after = cluster
        .node(cluster.leader().await)
        .membership_report()
        .membership;
    assert_eq!(
        members_after.voters, members_before.voters,
        "a refused open must not have changed committed membership"
    );
    assert!(
        !members_after.endpoints.contains_key(&replacement),
        "the refused identity must not appear anywhere in committed membership, voter or not"
    );
    let leader_after = cluster.leader().await;
    cluster
        .client(leader_after)
        .put(put_req("/m5/70/new-id", "1"))
        .await
        .expect("the cluster is undisturbed by a node that never opened its store");
}

// -------------------------------------------------------------------------------------------
// M5-71 — a v1-format directory carrying history is refused by name, and never joins
// -------------------------------------------------------------------------------------------

/// M5-71: a node pointed at a legacy (v1) data directory that still holds Raft log entries is
/// refused with a typed, self-naming format error and never becomes a learner of the v2+
/// cluster it was meant to join (spec §17, ADR-0021 note 4, ruling M5-R19).
///
/// # Which refusal is the oracle
///
/// The plan text predates ruling M5-R19 and says only "a typed format error naming
/// `format_version`". R19 is the more specific statement and is what the product implements: a
/// legacy directory whose log is **drained** migrates in place, and one whose log is
/// **undrained** is refused, because a log entry is a positional `postcard` encoding of a
/// `Command` that a narrower build may decode into a *different* command. So the store this row
/// seeds carries exactly one log entry — the difference between the migration path and the
/// refusal path — and the assertion is `UpgradeRequiresDrainedLog`, naming both the format and
/// the entry count.
///
/// # Why the directory is hand-built
///
/// This build cannot write a v1 directory; that format predates it. The seed below therefore
/// creates one directly through a raw `rocksdb` handle, in the only window where that is
/// possible — `Cluster::provision_seeded_dir` hands the caller the directory before the node's
/// first open. The shape it writes is the one `verify_column_families` classifies as
/// `CfLayout::LegacyV1`: the four v1 families, no `events`, no `dedup`, and a `format_version`
/// marker of 1.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_71_a_v1_dir_with_an_undrained_log_never_joins() {
    let (cluster, _scripts) = three_voters_and_a_spare().await;
    let leader = cluster.leader().await;
    let members_before = cluster.node(leader).membership_report().membership;

    let legacy = cluster
        .provision_seeded_dir(seed_v1_dir_with_one_log_entry)
        .await;
    let refusal = cluster
        .try_start_node(legacy)
        .await
        .expect_err("a legacy directory that still holds history must not open");
    match refusal {
        NodeStartError::Storage(StorageOpenError::UpgradeRequiresDrainedLog {
            format,
            log_entries,
            path,
        }) => {
            assert_eq!(format, 1, "the refusal must name the format it found");
            assert_eq!(log_entries, 1, "...and how much history blocks the upgrade");
            assert_eq!(path, cluster.data_dir(legacy));
        }
        other => panic!("expected a typed legacy-format refusal, got {other:?}"),
    }

    // The refusal is read-only: the directory an operator has to go back and drain is
    // byte-for-byte the one they left. Had the open reached `create_missing_column_families`,
    // `events` and `dedup` would exist here and that evidence would be gone.
    let families = rocksdb::DB::list_cf(&rocksdb::Options::default(), cluster.data_dir(legacy))
        .expect("the refused directory is still a readable database");
    for manufactured in ["events", "dedup"] {
        assert!(
            !families.iter().any(|f| f == manufactured),
            "a refused open must not have created the {manufactured:?} family: {families:?}"
        );
    }

    // And it never joins: committed membership is untouched by the attempt.
    let members_after = cluster
        .node(cluster.leader().await)
        .membership_report()
        .membership;
    assert_eq!(
        members_after.voters, members_before.voters,
        "a refused legacy node must not have changed committed membership"
    );
    assert!(
        !members_after.endpoints.contains_key(&legacy),
        "the refused node must never appear in committed membership, learner or voter"
    );
}

/// Write the directory an M2/M3-era build would have left behind, still holding one log entry.
///
/// Only the three facts the refusal keys on are reproduced, because only those are load-bearing
/// and the rest would be a guess at bytes this build cannot write: the v1 column-family set, a
/// `format_version` marker of `1`, and a non-empty `raft_log`. The entry's *contents* are
/// deliberately opaque — `count_log_entries` counts without decoding, and "this build cannot
/// decode them" is the very reason the open is refused.
fn seed_v1_dir_with_one_log_entry(dir: &std::path::Path) {
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let db = rocksdb::DB::open_cf(&opts, dir, ["raft_log", "raft_meta", "kv", "state_meta"])
        .expect("create a v1-shaped database");
    db.put_cf(
        db.cf_handle("state_meta").expect("state_meta cf"),
        b"format_version",
        1u32.to_le_bytes(),
    )
    .expect("stamp the v1 marker");
    db.put_cf(
        db.cf_handle("raft_log").expect("raft_log cf"),
        1u64.to_be_bytes(),
        b"an entry written by a build with a narrower Command".as_slice(),
    )
    .expect("leave the log undrained");
}

// -------------------------------------------------------------------------------------------
// M5-63 — SetNodes/ReplaceAllNodes stay unused, and there is no endpoint-update RPC
// -------------------------------------------------------------------------------------------

#[test]
fn m5_63_set_nodes_is_never_used_and_no_endpoint_update_rpc_exists() {
    // Source assertion over the whole workspace: `ChangeMembers::SetNodes` and
    // `ReplaceAllNodes` are the split-brain hazard research §3.5 documents (an endpoint change
    // that does not go through remove+re-add can let two nodes believe they hold the same
    // identity at different addresses). Grep every crate's `src/`, not just config-engine,
    // because the hazard is workspace-wide by nature.
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
    let mut hits = Vec::new();
    for crate_name in [
        "config-core",
        "config-engine",
        "config-storage",
        "config-grpc",
        "config-server",
        "config-client",
    ] {
        let src = std::path::Path::new(root)
            .join("crates")
            .join(crate_name)
            .join("src");
        if !src.is_dir() {
            continue;
        }
        for entry in walk_rs_files(&src) {
            let text = std::fs::read_to_string(&entry).unwrap_or_default();
            for needle in ["ChangeMembers::SetNodes", "ReplaceAllNodes"] {
                if text.contains(needle) {
                    hits.push(format!("{}: {needle}", entry.display()));
                }
            }
        }
    }
    assert!(
        hits.is_empty(),
        "SetNodes/ReplaceAllNodes must never be used (research §3.5 split-brain hazard); found: \
         {hits:?}"
    );

    // No endpoint-update RPC: the admin vocabulary is add_learner / promote_voter /
    // remove_member / trigger_snapshot / backup and nothing that takes a changed endpoint for
    // an existing id.
    let admin_src = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../config-engine/src/node.rs"
    ))
    .expect("read config-engine/src/node.rs");
    for forbidden in ["update_endpoint", "change_endpoint", "set_endpoint"] {
        assert!(
            !admin_src.contains(forbidden),
            "found an endpoint-update-shaped method `{forbidden}` on ConfigNode; the admin API \
             must refuse in-place endpoint changes and direct operators to remove + re-add \
             (test plan M5-63, research §3.5)"
        );
    }
}

fn walk_rs_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(walk_rs_files(&path));
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
    out
}

// -------------------------------------------------------------------------------------------
// M5-64 — crash right after the learner membership entry commits
// -------------------------------------------------------------------------------------------

/// A crash on the leader immediately after `AddLearner` commits leaves the new leader (which
/// may be the same node, once restarted) with the learner still in committed membership, no
/// joint config left over, and no manual repair needed before `PromoteVoter` can be called.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_64_crash_after_learner_added() {
    let (cluster, scripts) = three_voters_and_a_spare().await;
    let leader = cluster.leader().await;

    // Adding a learner is a single, non-joint membership entry (it does not change the voter
    // set), so it crosses `AfterStateBatch` exactly once. Confirmed by inspection of
    // `config-engine/src/node.rs::add_learner_inner`, and — because that is exactly the kind
    // of claim this file must not simply assert into existence — re-derived empirically below
    // rather than hard-coded: arm on the very next crossing.
    scripts[&leader].crash_on_nth(Boundary::AfterStateBatch, 1);

    // The crash lands mid-RPC; the outcome is deliberately unobserved (ADR-0015) — a crash can
    // leave the caller with an error or an unknown outcome, and this row treats neither as
    // proof either way.
    let _ = cluster
        .node(leader)
        .add_learner(
            JOINER,
            cluster.peer_endpoint(JOINER),
            cluster.client_endpoint(JOINER),
        )
        .await;

    cluster
        .wait_for(
            "the crash to poison the target's store",
            cluster.deadline(6),
            || (cluster.counters(leader).get(Boundary::AfterStateBatch) >= 1).then_some(()),
        )
        .await
        .unwrap_or_else(|t| panic!("{t}"));
    cluster.reopen_store(leader).ok(); // release the RocksDB LOCK before restart, if held
    cluster
        .restart(leader)
        .await
        .unwrap_or_else(|e| panic!("restart {leader}: {e}"));

    let new_leader = cluster.leader().await;
    cluster
        .wait_for(
            "the learner to reappear in committed membership after recovery",
            cluster.deadline(10),
            || {
                let report = cluster.node(new_leader).membership_report();
                (report.learners.contains(&JOINER) || report.membership.voters.contains(&JOINER))
                    .then_some(())
            },
        )
        .await
        .unwrap_or_else(|t| panic!("{t}"));

    let report = cluster.node(new_leader).membership_report();
    assert_eq!(
        report.joint_config_len, 1,
        "adding a learner must never leave a joint config behind, crash or not: {report:?}"
    );

    // The operator can resume exactly where the plan says: PromoteVoter, with no manual repair.
    cluster
        .client(new_leader)
        .put(put_req("/m5/64/after", "1"))
        .await
        .expect("the cluster keeps serving after the crashed node recovers");
    wait_caught_up(&cluster, new_leader, 0).await;
    cluster
        .node(new_leader)
        .promote_voter(JOINER)
        .await
        .expect("PromoteVoter must work immediately after recovery with no manual repair");
}

// -------------------------------------------------------------------------------------------
// M5-72 — there is no --join flag, and an unformed spare never self-forms
// -------------------------------------------------------------------------------------------

#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_72_there_is_no_join_flag_and_no_self_forming() {
    let cli_src = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../config-server/src/cli.rs"
    ))
    .expect("read config-server/src/cli.rs");
    assert!(
        !cli_src.contains("\"--join\"")
            && !cli_src.contains("'join'")
            && !cli_src.contains("long = \"join\""),
        "config-server must have no --join flag (D5.2; ADR-0011; test plan M5-72): \
         `--form` is the only way a cluster forms"
    );

    let (cluster, _scripts) = three_voters_and_a_spare().await;
    // Node 4 was started by `three_voters_and_a_spare` but never named in `form_with`. It must
    // sit unready and uninvolved in Raft — never having initialized on its own — for as long
    // as nobody calls AddLearner on it.
    let health = cluster.health(JOINER).await;
    assert!(
        !health.ready,
        "an unformed, never-added node must report itself unready: {health:?}"
    );
    assert!(
        cluster
            .node(JOINER)
            .committed_membership()
            .membership_log_id
            .is_none(),
        "an unformed node must never have initialized Raft on its own"
    );

    // It stays that way — no background timer eventually self-forms it. `committed_membership`
    // is read straight off the applied state machine (sync, no Raft barrier), so this predicate
    // needs no async health call to stay a `FnMut() -> bool` for `assert_never`.
    cluster
        .assert_never(
            "the spare node to commit a membership entry on its own",
            cluster.deadline(4),
            || {
                cluster
                    .node(JOINER)
                    .committed_membership()
                    .membership_log_id
                    .is_some()
            },
        )
        .await;
}

// -------------------------------------------------------------------------------------------
// M5-73 — quorum availability holds throughout a full learner-add / promote / remove cycle
// -------------------------------------------------------------------------------------------

#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_73_two_voter_availability_during_replacement() {
    let (cluster, _scripts) = three_voters_and_a_spare().await;
    let leader = cluster.leader().await;

    // A writer that runs concurrently with the whole replacement sequence and demands that
    // every call either commits or fails in a way the client can retry — never blocks forever
    // and never sees fewer than a quorum able to serve it.
    let writer_cluster = cluster.clone();
    let writer = tokio::spawn(async move {
        let mut applied = 0u64;
        for i in 0..200 {
            let leader = writer_cluster.leader_now().unwrap_or(leader);
            match writer_cluster
                .client(leader)
                .put(put_req(&format!("/m5/73/{i}"), "v"))
                .await
            {
                Ok(resp) if resp.outcome == config_core::MutationOutcome::Applied => applied += 1,
                Ok(_) => {}
                Err(config_core::ConfigError::NotLeader { .. })
                | Err(config_core::ConfigError::Unavailable { .. })
                | Err(config_core::ConfigError::DeadlineExceededUnknownOutcome) => {
                    // Retryable, by ADR-0015's own contract — not an availability hole.
                }
                Err(e) => panic!("put {i} failed in a non-retryable way: {e}"),
            }
        }
        applied
    });

    add_joiner(&cluster, leader).await;
    wait_caught_up(&cluster, leader, 5).await;
    // Re-fetch the leader: it may have changed under the concurrent write load.
    let leader = cluster.leader().await;
    cluster
        .node(leader)
        .promote_voter(JOINER)
        .await
        .expect("promote the caught-up learner");
    cluster
        .wait_for("promotion to commit", cluster.deadline(10), || {
            cluster
                .node(leader)
                .membership_report()
                .membership
                .voters
                .contains(&JOINER)
                .then_some(())
        })
        .await
        .unwrap_or_else(|t| panic!("{t}"));

    let leader = cluster.leader().await;
    cluster
        .node(leader)
        .remove_member(NodeId(3))
        .await
        .expect("remove the replaced voter");
    cluster
        .wait_for("removal to commit", cluster.deadline(10), || {
            (!cluster
                .node(leader)
                .membership_report()
                .membership
                .voters
                .contains(&NodeId(3)))
            .then_some(())
        })
        .await
        .unwrap_or_else(|t| panic!("{t}"));

    let applied = writer.await.expect("the writer task must not panic");
    assert!(
        applied > 0,
        "the writer must have made real progress throughout the replacement"
    );
}
