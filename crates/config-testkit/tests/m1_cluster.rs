//! M1 acceptance rows M1-01..M1-16 over the **real** gRPC peer and client planes
//! (test plan §4.2).
//!
//! Every test builds its own [`Cluster`] — one cluster per test, no reuse (anti-flake rule 8)
//! — and every wait is a deadline-bounded poll over observable state whose deadline is a
//! multiple of the harness's election timeout (rules 1-3, 10). Test names begin with their
//! plan id so the §5 log queries and the ADR-0014 gate mapping both work by string match
//! (rule 13).

use std::collections::BTreeSet;
use std::time::Duration;

use bytes::Bytes;
use config_core::{
    ClusterId, ConfigError, DeleteRequest, GetRequest, ListRequest, MutationOutcome, NodeId,
    PutRequest, RecoveryEpoch,
};
use config_engine::{FormationError, FormationPlan, NodeRole};
use config_testkit::cluster::{Cluster, ClusterConfig, GossipKind, StorageKind};
use config_testkit::poll::TestTimers;

/// The fastest legal timers, used by the tests whose claim is *negative* ("no leader ever").
///
/// A faster election is a stronger claim — ten genuine opportunities to misbehave instead of
/// ten slow ones — and it costs three seconds instead of fifteen.
const FAST: TestTimers = TestTimers {
    heartbeat: Duration::from_millis(50),
    election_timeout_min: Duration::from_millis(150),
    election_timeout_max: Duration::from_millis(300),
};

/// Timers for the partition matrix, which pays for six elections in one test.
const BRISK: TestTimers = TestTimers {
    heartbeat: Duration::from_millis(100),
    election_timeout_min: Duration::from_millis(300),
    election_timeout_max: Duration::from_millis(600),
};

fn key(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

fn put_req(k: &str, v: &str) -> PutRequest {
    PutRequest {
        dedup: None,
        key: key(k),
        value: key(v),
        expected_mod_revision: None,
    }
}

fn get_req(k: &str) -> GetRequest {
    GetRequest { key: key(k) }
}

fn list_req(prefix: &str) -> ListRequest {
    ListRequest {
        prefix: key(prefix),
        ..Default::default()
    }
}

fn delete_req(k: &str) -> DeleteRequest {
    DeleteRequest {
        dedup: None,
        key: key(k),
        expected_mod_revision: None,
    }
}

/// An unformed cluster: nodes started, no `form_cluster` call, fastest timers.
async fn unformed(nodes: u64) -> Cluster {
    Cluster::start_with(ClusterConfig {
        nodes,
        form: false,
        timers: FAST,
        gossip: GossipKind::Disabled,
        ..ClusterConfig::default()
    })
    .await
}

/// The JSONL lines this test itself produced, filtered to this process's `testRun`.
fn my_log_lines(method: &str) -> Vec<serde_json::Value> {
    config_testkit::logs::lines_for_current_test(module_path!(), method)
}

fn field<'a>(row: &'a serde_json::Value, name: &str) -> Option<&'a str> {
    row.get(name).and_then(serde_json::Value::as_str)
}

/// Test plan §5 Q4, evaluated over this test's own log file: a node that never formed must
/// never have logged a transition into leadership.
fn assert_no_leadership_evidence(method: &str) {
    let rows = my_log_lines(method);
    config_testkit::logs::assert_nonempty(&rows, "log lines from an unformed cluster");
    let offending: Vec<&serde_json::Value> = rows
        .iter()
        .filter(|row| match field(row, "@m") {
            Some("raft role changed") => field(row, "new") == Some("leader"),
            Some("raft leader changed") => field(row, "new").is_some_and(|v| v != "None"),
            _ => false,
        })
        .collect();
    assert!(
        offending.is_empty(),
        "an unformed cluster logged leadership evidence (test plan §5 Q4): {offending:#?}"
    );
}

// =====================================================================================
// No self-form
// =====================================================================================

/// M1-01: a single empty node polled for ten election timeouts never elects itself.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_01_empty_node_never_self_forms() {
    let cluster = unformed(1).await;

    cluster
        .assert_never(
            "a leader on a single unformed node",
            cluster.deadline(10),
            || cluster.leader_now().is_some(),
        )
        .await;

    let m = cluster.metrics(NodeId(1));
    assert_eq!(m.role, NodeRole::Learner, "left Learner without formation");
    assert_eq!(m.current_leader, None, "believes in a leader");
    assert_eq!(m.last_log_index, None, "appended an entry");
    assert!(m.membership_voter_ids.is_empty(), "has voters");

    cluster.shutdown().await;
    assert_no_leadership_evidence("m1_01_empty_node_never_self_forms");
}

/// M1-02: three empty nodes that can reach each other still elect nobody.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_02_three_empty_nodes_never_self_form() {
    let cluster = unformed(3).await;

    cluster
        .assert_never(
            "a leader on three unformed nodes",
            cluster.deadline(10),
            || cluster.leader_now().is_some(),
        )
        .await;

    for m in cluster.running_metrics() {
        assert_eq!(m.current_leader, None, "node {} saw a leader", m.node_id);
        assert_eq!(m.last_log_index, None, "node {} has log entries", m.node_id);
        assert!(
            m.membership_voter_ids.is_empty(),
            "node {} has voters without formation",
            m.node_id
        );
    }

    cluster.shutdown().await;
    assert_no_leadership_evidence("m1_02_three_empty_nodes_never_self_form");
}

/// M1-03: an unformed node answers `Unavailable` — not `NotLeader`, not a hang, not success.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_03_unformed_node_serves_unavailable() {
    let cluster = unformed(3).await;
    let client = cluster.client(NodeId(1));

    let read = client.get(get_req("/a")).await;
    let write = client.put(put_req("/a", "1")).await;
    let scan = client.list(list_req("/")).await;

    // `Unavailable` and not `NotLeader`: there is no leader to point at, and a hint we cannot
    // justify would send the client somewhere that cannot serve it either (ADR-0009).
    assert!(
        matches!(read, Err(ConfigError::Unavailable { .. })),
        "get on an unformed node: {read:?}"
    );
    assert!(
        matches!(write, Err(ConfigError::Unavailable { .. })),
        "put on an unformed node: {write:?}"
    );
    assert!(
        matches!(scan, Err(ConfigError::Unavailable { .. })),
        "list on an unformed node: {scan:?}"
    );

    cluster.shutdown().await;
}

// =====================================================================================
// Formation
// =====================================================================================

/// M1-04: one `form_cluster` on node 1 gives every node the same voter set and the same
/// membership log id, and a leader appears.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_04_explicit_formation_succeeds() {
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let ids: BTreeSet<NodeId> = cluster.ids().into_iter().collect();

    let leader = cluster.leader().await;
    assert!(ids.contains(&leader), "leader {leader} is not a member");

    let views: Vec<_> = ids.iter().map(|id| cluster.membership_of(*id)).collect();
    for (id, view) in ids.iter().zip(&views) {
        assert_eq!(view.voters, ids, "node {id} disagrees about the voter set");
        assert_eq!(
            view.endpoint_of(*id),
            Some(cluster.peer_endpoint(*id).as_str()),
            "node {id}'s committed endpoint is not the address it actually listens on"
        );
    }
    assert!(
        views
            .windows(2)
            .all(|w| w[0].membership_log_id == w[1].membership_log_id),
        "membership log ids differ: {views:#?}"
    );
    assert!(
        views[0].membership_log_id.is_some(),
        "membership was never committed"
    );

    cluster.shutdown().await;
}

/// M1-05: a second formation is refused, on the forming node and on a node that learned
/// membership by replication — and nothing churns.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_05_double_formation_is_rejected() {
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let before_leader = cluster.leader().await;
    let before = cluster.membership();
    let before_term = cluster.metrics(before_leader).current_term;

    let plan = cluster.formation_plan();
    assert_eq!(
        cluster.node(NodeId(1)).form_cluster(plan.clone()).await,
        Err(FormationError::AlreadyFormed),
        "the forming node accepted a second plan"
    );
    assert_eq!(
        cluster.node(NodeId(2)).form_cluster(plan).await,
        Err(FormationError::AlreadyFormed),
        "a replicated-into node accepted a formation plan"
    );

    assert_eq!(cluster.membership(), before, "membership changed");
    assert_eq!(
        cluster.leader_now(),
        Some(before_leader),
        "the leader changed"
    );
    assert_eq!(
        cluster.metrics(before_leader).current_term,
        before_term,
        "a refused formation caused term churn"
    );

    cluster.shutdown().await;
}

/// M1-06: a plan naming a different cluster is refused before Raft is touched.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_06_formation_requires_matching_identity() {
    let cluster = unformed(3).await;

    let mut foreign = cluster.identity(NodeId(1));
    foreign.cluster_id = ClusterId::from_bytes([0xEE; 16]);
    let voters: Vec<(NodeId, String)> = cluster
        .ids()
        .into_iter()
        .map(|id| (id, cluster.peer_endpoint(id)))
        .collect();

    let wrong_cluster = cluster
        .node(NodeId(1))
        .form_cluster(FormationPlan::new(&foreign, voters.clone()))
        .await;
    assert!(
        matches!(wrong_cluster, Err(FormationError::IdentityMismatch(_))),
        "a foreign cluster id was accepted: {wrong_cluster:?}"
    );

    let mut wrong_epoch_identity = cluster.identity(NodeId(1));
    wrong_epoch_identity.recovery_epoch = RecoveryEpoch(42);
    let wrong_epoch = cluster
        .node(NodeId(1))
        .form_cluster(FormationPlan::new(&wrong_epoch_identity, voters))
        .await;
    assert!(
        matches!(wrong_epoch, Err(FormationError::IdentityMismatch(_))),
        "a foreign recovery epoch was accepted: {wrong_epoch:?}"
    );

    for m in cluster.running_metrics() {
        assert_eq!(
            m.last_log_index, None,
            "node {} appended an entry for a refused plan",
            m.node_id
        );
        assert!(
            m.membership_voter_ids.is_empty(),
            "node {} gained voters from a refused plan",
            m.node_id
        );
    }

    cluster.shutdown().await;
}

// =====================================================================================
// One stopped voter still commits
// =====================================================================================

/// Write `/k{1..=n}` through `id`'s embedded client, asserting each is `Applied` with the
/// expected revision, and return the last revision.
async fn put_series(cluster: &Cluster, id: NodeId, n: u64) -> u64 {
    let client = cluster.client(id);
    let mut last = 0;
    for i in 1..=n {
        let response = client
            .put(put_req(&format!("/k{i}"), &format!("v{i}")))
            .await
            .unwrap_or_else(|e| panic!("put {i} failed: {e}; {}", cluster.diagnostic()));
        assert_eq!(
            response.outcome,
            MutationOutcome::Applied,
            "put {i} was not applied"
        );
        assert_eq!(response.revision, i, "put {i} allocated the wrong revision");
        last = response.revision;
    }
    last
}

/// M1-07: with one voter stopped, the remaining two still commit every write.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_07_write_commits_with_one_voter_stopped() {
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let leader = cluster.leader().await;
    let stopped = cluster.followers()[0];
    cluster.stop_node(stopped).await;

    assert_eq!(put_series(&cluster, leader, 5).await, 5);

    let target = cluster
        .metrics(leader)
        .last_applied
        .expect("the leader applied its own writes")
        .index;
    cluster
        .wait_applied_all(target, cluster.deadline(8))
        .await
        .unwrap_or_else(|t| panic!("the live nodes did not catch up: {t}"));

    assert_eq!(
        cluster.running_ids().len(),
        2,
        "the stopped voter was not required for the quorum, but it is still running"
    );

    cluster.shutdown().await;
}

/// M1-08: reads on the leader still work with one voter stopped, at the right revision.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_08_read_succeeds_with_one_voter_stopped() {
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let leader = cluster.leader().await;
    cluster.stop_node(cluster.followers()[0]).await;

    put_series(&cluster, leader, 5).await;

    let client = cluster.client(leader);
    let got = client.get(get_req("/k5")).await.expect("get on the leader");
    assert_eq!(got.read_revision, 5, "read at the wrong revision");
    assert_eq!(
        got.record.as_ref().map(|r| r.value.clone()),
        Some(key("v5")),
        "the last write is not readable"
    );

    let listed = client
        .list(list_req("/k"))
        .await
        .expect("list on the leader");
    assert_eq!(listed.read_revision, 5, "list at the wrong revision");
    assert_eq!(listed.records.len(), 5, "list lost records: {listed:?}");

    cluster.shutdown().await;
}

/// M1-09: a stopped voter restarted on a **fresh** Ephemeral store is re-replicated from the
/// leader and converges to the same state (test plan M1-43: an Ephemeral restart is empty by
/// design, and the recovery path is replication, not local replay).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_09_stopped_voter_catches_up_on_restart() {
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let leader = cluster.leader().await;
    let stopped = cluster.followers()[0];
    cluster.stop_node(stopped).await;

    put_series(&cluster, leader, 5).await;
    let target = cluster
        .metrics(leader)
        .last_applied
        .expect("leader applied state")
        .index;

    cluster.start_node(stopped).await;
    assert_eq!(
        cluster.running_ids().len(),
        3,
        "the restarted node is not running"
    );

    cluster
        .wait_applied_all(target, cluster.deadline(10))
        .await
        .unwrap_or_else(|t| panic!("the restarted voter never caught up: {t}"));
    let hash = cluster
        .wait_converged(cluster.deadline(10))
        .await
        .unwrap_or_else(|t| panic!("the cluster never converged after restart: {t}"));

    let hashes = cluster.state_hashes();
    assert_eq!(hashes.len(), 3, "not every node reported a state hash");
    assert!(
        hashes.values().all(|h| *h == hash),
        "state hashes diverged after a restart: {hashes:?}"
    );

    cluster.shutdown().await;
}

/// M1-10: stopping the leader elects a survivor, and acknowledged revisions survive.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_10_leader_stop_elects_new_leader() {
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let old_leader = cluster.leader().await;
    let last_revision = put_series(&cluster, old_leader, 3).await;
    let survivors = cluster.followers();

    cluster.stop_node(old_leader).await;

    let new_leader = cluster
        .wait_for_leader(cluster.deadline(10))
        .await
        .unwrap_or_else(|t| panic!("no leader after the leader stopped: {t}"));
    assert_ne!(new_leader, old_leader, "the stopped node is still leader");
    assert!(
        survivors.contains(&new_leader),
        "leader {new_leader} is not one of the survivors {survivors:?}"
    );

    let got = cluster
        .client(new_leader)
        .get(get_req("/k3"))
        .await
        .expect("read on the new leader");
    assert_eq!(
        got.record.as_ref().map(|r| r.mod_revision),
        Some(last_revision),
        "an acknowledged revision was lost or reused"
    );
    assert!(
        got.read_revision >= last_revision,
        "the cluster revision went backwards: {} < {last_revision}",
        got.read_revision
    );

    cluster.shutdown().await;
}

// =====================================================================================
// Isolated / former leader
// =====================================================================================

/// Set up a 3-node cluster with one committed write, then isolate the leader.
/// Returns `(cluster, isolated_leader, baseline_revision)`.
async fn isolated_leader(seed_key: &str) -> (Cluster, NodeId, u64) {
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let leader = cluster.leader().await;
    let revision = cluster
        .client(leader)
        .put(put_req(seed_key, "seed"))
        .await
        .expect("the seed write commits on a healthy cluster")
        .revision;
    cluster.isolate(leader);
    (cluster, leader, revision)
}

/// M1-11: an isolated former leader refuses a strict read rather than serving stale state.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_11_isolated_former_leader_rejects_strict_read() {
    let (cluster, leader, _) = isolated_leader("/iso").await;

    // Strictly `Unavailable`: the row and `ConfigNode::read`'s doc comment both say an
    // isolated leader whose lease cannot be renewed reports `Unavailable`. `NotLeader` would
    // mean it knows a different leader, which an isolated node cannot learn.
    let read = cluster.client(leader).get(get_req("/iso")).await;
    assert!(
        matches!(read, Err(ConfigError::Unavailable { .. })),
        "an isolated leader served a read, or refused it with the wrong error: {read:?}"
    );

    cluster.shutdown().await;

    // Test plan §5 Q6: the isolated node logged a failed read, and never a successful one.
    let rows = my_log_lines("m1_11_isolated_former_leader_rejects_strict_read");
    let gets: Vec<&serde_json::Value> = rows
        .iter()
        .filter(|r| field(r, "op") == Some("get") && field(r, "outcome").is_some())
        .collect();
    assert!(
        !gets.is_empty(),
        "no `get` outcome line was logged at all — the query would have passed vacuously"
    );
    let served_by_isolated = gets.iter().any(|r| {
        field(r, "outcome") == Some("ok")
            && r.get("node_id").and_then(serde_json::Value::as_u64) == Some(leader.0)
    });
    assert!(
        !served_by_isolated,
        "the isolated node logged a successful read: {gets:#?}"
    );
    let failed_by_isolated = gets.iter().any(|r| {
        field(r, "outcome") == Some("error")
            && r.get("node_id").and_then(serde_json::Value::as_u64) == Some(leader.0)
    });
    assert!(
        failed_by_isolated,
        "the isolated node never logged the refusal: {gets:#?}"
    );
}

/// M1-12: an isolated former leader never applies a write, and healing proves it.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_12_isolated_former_leader_rejects_write() {
    let (cluster, leader, baseline) = isolated_leader("/iso").await;

    // `DeadlineExceededUnknownOutcome` stays admissible: a write that was already appended
    // when the partition landed legitimately has an unknown outcome. `NotLeader` does not —
    // an isolated node has no way to learn who took over.
    let write = cluster.client(leader).put(put_req("/iso", "ghost")).await;
    match &write {
        Err(ConfigError::Unavailable { .. }) | Err(ConfigError::DeadlineExceededUnknownOutcome) => {
        }
        other => panic!("an isolated leader accepted a write, or refused it wrongly: {other:?}"),
    }

    cluster.heal();
    cluster
        .wait_converged(cluster.deadline(15))
        .await
        .unwrap_or_else(|t| panic!("the cluster never reconverged after heal: {t}"));

    let survivor = cluster
        .wait_for_leader(cluster.deadline(10))
        .await
        .expect("a leader after heal");
    let got = cluster
        .client(survivor)
        .get(get_req("/iso"))
        .await
        .expect("read after heal");
    assert_eq!(
        got.record.as_ref().map(|r| r.value.clone()),
        Some(key("seed")),
        "the ghost write reached the state machine"
    );
    assert_eq!(
        got.read_revision, baseline,
        "a rejected write allocated a revision"
    );

    cluster.shutdown().await;
}

/// M1-13: `List` is leader-linearized exactly like `Get`, so an isolated leader refuses it
/// rather than serving a partial scan of stale state.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_13_isolated_former_leader_list_rejected() {
    let (cluster, leader, _) = isolated_leader("/iso/a").await;

    // Strictly `Unavailable`, exactly as M1-11: `List` takes the same lease check as `Get`.
    let scan = cluster.client(leader).list(list_req("/iso")).await;
    match &scan {
        Err(ConfigError::Unavailable { .. }) => {}
        other => panic!("an isolated leader served a list, or refused it wrongly: {other:?}"),
    }

    let removed = cluster.client(leader).delete(delete_req("/iso/a")).await;
    assert!(
        removed.is_err(),
        "an isolated leader accepted a delete: {removed:?}"
    );

    cluster.shutdown().await;
}

/// M1-14: the surviving majority elects a leader and keeps serving, with revisions
/// continuing from the last committed value — no gap and no reuse.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_14_surviving_majority_still_serves() {
    let (cluster, isolated, baseline) = isolated_leader("/iso").await;
    let survivors: Vec<NodeId> = cluster
        .ids()
        .into_iter()
        .filter(|id| *id != isolated)
        .collect();

    let new_leader = cluster
        .wait_for("a leader among the survivors", cluster.deadline(15), || {
            survivors
                .iter()
                .copied()
                .find(|id| cluster.metrics(*id).role == NodeRole::Leader)
        })
        .await
        .unwrap_or_else(|t| panic!("the majority never elected a leader: {t}"));

    let written = cluster
        .client(new_leader)
        .put(put_req("/majority", "1"))
        .await
        .unwrap_or_else(|e| panic!("the majority could not commit: {e}"));
    assert_eq!(written.outcome, MutationOutcome::Applied);
    assert_eq!(
        written.revision,
        baseline + 1,
        "the revision sequence has a gap or reuse (baseline {baseline})"
    );

    let got = cluster
        .client(new_leader)
        .get(get_req("/majority"))
        .await
        .expect("read on the new leader");
    assert_eq!(got.read_revision, baseline + 1);

    cluster.shutdown().await;
}

/// M1-15: healing makes the old leader step down, catch up, and agree — and it never rolls
/// its term backwards.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_15_heal_reconverges_old_leader() {
    let (cluster, isolated, baseline) = isolated_leader("/iso").await;
    let term_before = cluster.metrics(isolated).current_term;
    let survivors: Vec<NodeId> = cluster
        .ids()
        .into_iter()
        .filter(|id| *id != isolated)
        .collect();

    let new_leader = cluster
        .wait_for("a leader among the survivors", cluster.deadline(15), || {
            survivors
                .iter()
                .copied()
                .find(|id| cluster.metrics(*id).role == NodeRole::Leader)
        })
        .await
        .expect("the majority elects a leader");
    let latest = cluster
        .client(new_leader)
        .put(put_req("/after", "1"))
        .await
        .expect("the majority commits")
        .revision;
    assert_eq!(latest, baseline + 1);

    cluster.heal();

    cluster
        .wait_for("the old leader to step down", cluster.deadline(15), || {
            (cluster.metrics(isolated).role != NodeRole::Leader).then_some(())
        })
        .await
        .unwrap_or_else(|t| panic!("the old leader never stepped down: {t}"));

    let target = cluster
        .metrics(new_leader)
        .last_applied
        .expect("the new leader applied its write")
        .index;
    cluster
        .wait_applied_all(target, cluster.deadline(15))
        .await
        .unwrap_or_else(|t| panic!("the old leader never caught up: {t}"));
    cluster
        .wait_converged(cluster.deadline(15))
        .await
        .unwrap_or_else(|t| panic!("the cluster never converged after heal: {t}"));

    assert!(
        cluster.metrics(isolated).current_term >= term_before,
        "the old leader's term went backwards"
    );
    assert_eq!(
        cluster.state_hashes().len(),
        3,
        "a node dropped out during heal"
    );

    cluster.shutdown().await;
}

/// M1-16: every three-node partition arrangement leaves at most one writable authority, and
/// healing always reconverges to one identical state (§20, §19.11).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_16_every_pair_partition_matrix() {
    /// A partition arrangement: either a symmetric pair block or a full isolation.
    enum Arrangement {
        Pair(NodeId, NodeId),
        Isolate(NodeId),
    }

    let cluster = Cluster::start_with(ClusterConfig {
        nodes: 3,
        timers: BRISK,
        // Short enough that a call into a lost quorum fails inside this test's budget; the
        // cases that matter here are refusals, not slow successes.
        read_timeout: Duration::from_millis(500),
        write_timeout: Duration::from_millis(500),
        ..ClusterConfig::default()
    })
    .await;
    cluster.leader().await;

    let n = |i: u64| NodeId(i);
    let arrangements = [
        Arrangement::Pair(n(1), n(2)),
        Arrangement::Pair(n(1), n(3)),
        Arrangement::Pair(n(2), n(3)),
        Arrangement::Isolate(n(1)),
        Arrangement::Isolate(n(2)),
        Arrangement::Isolate(n(3)),
    ];

    let mut round = 0u64;
    for arrangement in arrangements {
        round += 1;
        // The nodes that cannot possibly be part of a quorum in this arrangement. A pair
        // block still leaves every node in some majority (the third node reaches both), so
        // only a full isolation produces a minority.
        let minority: Vec<NodeId> = match arrangement {
            Arrangement::Pair(a, b) => {
                cluster.partition(a, b);
                Vec::new()
            }
            Arrangement::Isolate(id) => {
                cluster.isolate(id);
                vec![id]
            }
        };
        let majority: Vec<NodeId> = cluster
            .ids()
            .into_iter()
            .filter(|id| !minority.contains(id))
            .collect();

        // Exactly one authority: a leader inside the majority, which commits.
        let leader = cluster
            .wait_for(
                &format!("round {round}: a leader in the majority {majority:?}"),
                cluster.deadline(10),
                || {
                    majority
                        .iter()
                        .copied()
                        .find(|id| cluster.metrics(*id).role == NodeRole::Leader)
                },
            )
            .await
            .unwrap_or_else(|t| panic!("round {round}: {t}"));

        let response = cluster
            .client(leader)
            .put(put_req(&format!("/round{round}"), "1"))
            .await
            .unwrap_or_else(|e| panic!("round {round}: the majority could not commit: {e}"));
        assert_eq!(response.outcome, MutationOutcome::Applied);

        // The minority side refuses: never a successful strict read.
        for id in &minority {
            let read = cluster
                .client(*id)
                .get(get_req(&format!("/round{round}")))
                .await;
            assert!(
                matches!(
                    read,
                    Err(ConfigError::Unavailable { .. }) | Err(ConfigError::NotLeader { .. })
                ),
                "round {round}: minority node {id} served a read: {read:?}"
            );
        }

        cluster.heal();
        cluster
            .wait_converged(cluster.deadline(15))
            .await
            .unwrap_or_else(|t| panic!("round {round}: no reconvergence after heal: {t}"));
    }

    let hashes = cluster.state_hashes();
    assert_eq!(hashes.len(), 3, "a node was lost during the matrix");
    let first = *hashes.values().next().expect("three nodes");
    assert!(
        hashes.values().all(|h| *h == first),
        "the matrix left the cluster divergent: {hashes:?}"
    );

    cluster.shutdown().await;
}

// =====================================================================================
// Follower NotLeader + hint
// =====================================================================================

/// M1-21: a gRPC client addressed at a follower reaches the leader by following the hint,
/// within the bounded follow count.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_21_grpc_client_follows_hint_bounded() {
    use config_core::ConfigStore;

    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let follower = cluster.followers()[0];
    let client = cluster.grpc_client(follower);

    let written = client
        .put(put_req("/hinted", "v"))
        .await
        .expect("the client reaches the leader by following the hint");
    assert_eq!(written.outcome, MutationOutcome::Applied);

    let stats = client.stats();
    assert!(
        stats.hint_follows >= 1,
        "the write succeeded without following a hint: {stats:?}"
    );
    assert!(
        stats.hint_follows <= 3,
        "hint following is not bounded at the default N=3: {stats:?}"
    );

    cluster.shutdown().await;
}

/// M1-24: a write made through the embedded client on the leader is visible through the gRPC
/// plane of a *different* node, reached by following the leader hint.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_24_direct_write_visible_via_grpc_on_other_nodes() {
    use config_core::ConfigStore;

    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let leader = cluster.leader().await;
    let other = cluster.followers()[0];

    let written = cluster
        .client(leader)
        .put(put_req("/shared", "v"))
        .await
        .expect("embedded write on the leader");

    let got = cluster
        .grpc_client(other)
        .get(get_req("/shared"))
        .await
        .expect("gRPC read from another node, via the hint");
    assert_eq!(
        got.record.as_ref().map(|r| r.mod_revision),
        Some(written.revision),
        "the gRPC plane reported a different mod_revision than the embedded write allocated"
    );

    cluster.shutdown().await;
}
