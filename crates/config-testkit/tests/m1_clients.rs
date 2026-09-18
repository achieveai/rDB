//! M1 client/hint-following and conformance rows (test plan §4.2): M1-17, M1-18, M1-20, M1-22,
//! M1-23, M1-26, M1-27, M1-44, M1-45, M1-46.
//!
//! M1-21 and M1-24 (the other two rows in this neighborhood of the plan) are owned by
//! `m1_cluster.rs`, not this file, per the assignment for this pass.

mod support;

use std::sync::Arc;

use config_core::{ConfigError, ConfigStore, Limits, MutationOutcome};
use config_testkit::cluster::{Cluster, StorageKind};
use config_testkit::conformance::{self, ConformanceConfig};

use support::{delete_req, get_req, put_req};

// =====================================================================================
// M1-17/M1-18 — a follower answers NotLeader with the committed-membership hint
// =====================================================================================

/// M1-17: a follower's strict read returns `NotLeader` naming the real leader at its
/// committed **client**-plane endpoint (ADR-0009; the peer endpoint is a different, non-dialable
/// address a client must never be told to connect to).
///
/// `LeaderHint.endpoint` is the client-plane endpoint (engine fix round); the committed
/// membership behind it comes from whatever `Cluster::form`'s plan submitted, so the harness's
/// own `cluster.client_endpoint(leader)` — the address actually bound for that node's client
/// plane — is the source of truth this test checks against, not a re-derivation through
/// `MembershipView`.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_17_follower_get_returns_not_leader_with_hint() {
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let leader = cluster.leader().await;
    let f = cluster.followers()[0];

    let result = cluster.client(f).get(get_req("/m1-17")).await;
    match result {
        Err(ConfigError::NotLeader { hint: Some(hint) }) => {
            assert_eq!(hint.node_id, leader, "the hint names the wrong node");
            assert_eq!(
                hint.endpoint,
                cluster.client_endpoint(leader),
                "the hint is not the leader's client-plane endpoint"
            );
        }
        other => panic!("a follower answered a strict read with {other:?}"),
    }

    cluster.shutdown().await;
}

/// M1-18: a follower's write also returns `NotLeader` with the same hint, and — unlike a
/// rejected mutation that at least reached the API edge — never even attempts to enter the log
/// on either the follower or the leader.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_18_follower_put_returns_not_leader_with_hint() {
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let leader = cluster.leader().await;
    let f = cluster.followers()[0];

    let before_follower_log_index = cluster.metrics(f).last_log_index;
    let before_leader_log_len = cluster.metrics(leader).raft_log_len;

    let result = cluster.client(f).put(put_req("/m1-18", "v1")).await;
    match result {
        Err(ConfigError::NotLeader { hint: Some(hint) }) => {
            assert_eq!(hint.node_id, leader);
            assert_eq!(hint.endpoint, cluster.client_endpoint(leader));
        }
        other => panic!("a follower answered a write with {other:?}"),
    }

    assert_eq!(
        cluster.metrics(f).last_log_index,
        before_follower_log_index,
        "a rejected follower write appended to the follower's own log"
    );
    assert_eq!(
        cluster.metrics(leader).raft_log_len,
        before_leader_log_len,
        "a rejected follower write reached the leader's log"
    );

    cluster.shutdown().await;
}

// =====================================================================================
// M1-20 — an unknown leader is Unavailable, never a hint-shaped guess
// =====================================================================================

/// M1-20: once a follower has lost contact with everyone and no longer believes any node is
/// leader, a strict read returns `Unavailable`, not `NotLeader` carrying a garbage/absent hint.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_20_unknown_leader_returns_unavailable() {
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let f = cluster.followers()[0];
    cluster.isolate(f);

    let lost_leader = cluster
        .wait_for(
            "the isolated follower to stop believing anyone is leader",
            cluster.deadline(10),
            || (cluster.metrics(f).current_leader.is_none()).then_some(()),
        )
        .await;
    assert!(
        lost_leader.is_ok(),
        "the isolated follower never lost its leader belief: {:?}",
        lost_leader.err()
    );

    match cluster.client(f).get(get_req("/m1-20")).await {
        Err(ConfigError::Unavailable { .. }) => {}
        other => panic!(
            "an isolated follower with no known leader must answer Unavailable, not {other:?}"
        ),
    }

    cluster.shutdown().await;
}

// =====================================================================================
// M1-22 — DirectClient never forwards on a follower's behalf
// =====================================================================================

/// M1-22: `DirectClient::put` on a follower returns `NotLeader` straight to the embedder. No
/// internal retry or forwarding happens — an `Applied` outcome here would mean the client
/// silently redirected the call, which `DirectClient` must never do (ADR-0009).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_22_direct_client_does_not_follow_hints() {
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let f = cluster.followers()[0];

    let result = cluster.client(f).put(put_req("/m1-22", "v1")).await;
    assert!(
        matches!(result, Err(ConfigError::NotLeader { .. })),
        "DirectClient on a follower did not return NotLeader to the embedder: {result:?}"
    );

    cluster.shutdown().await;
}

// =====================================================================================
// M1-23 — a direct write advances the applied index on every node via Raft
// =====================================================================================

/// M1-23: a write submitted directly on the leader is not a local shortcut — it advances
/// `last_applied` and `raft_log_len` on *every* node, and each node's `applied_commands`
/// (immune to any spurious-election blank/membership noise) advances by exactly one.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_23_direct_write_advances_applied_index_on_all_nodes() {
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let leader = cluster.leader().await;
    let ids = cluster.ids();

    let before: Vec<(u64, u64, u64)> = ids
        .iter()
        .map(|id| {
            let m = cluster.metrics(*id);
            (
                m.last_applied.map(|l| l.index).unwrap_or(0),
                m.raft_log_len,
                m.applied_commands,
            )
        })
        .collect();

    let written = cluster
        .client(leader)
        .put(put_req("/m1-23", "v1"))
        .await
        .expect("put");
    assert_eq!(written.outcome, MutationOutcome::Applied);

    let target = cluster
        .metrics(leader)
        .last_applied
        .expect("the leader applied its own write")
        .index;
    cluster
        .wait_applied_all(target, cluster.deadline(10))
        .await
        .unwrap_or_else(|e| panic!("not every node caught up: {e:?}"));

    // `applied_commands` increments inside the same apply critical section that moves
    // `last_applied` (engine fix round), so an observer that has already seen `last_applied`
    // reach `target` above should see the count too — but poll for it explicitly rather than
    // reading it inline, so this row does not depend on that ordering guarantee holding exactly.
    let counted = cluster
        .wait_for(
            "every node's applied_commands to advance by exactly one",
            cluster.deadline(10),
            || {
                ids.iter()
                    .enumerate()
                    .all(|(i, id)| cluster.metrics(*id).applied_commands - before[i].2 >= 1)
                    .then_some(())
            },
        )
        .await;
    assert!(
        counted.is_ok(),
        "not every node's applied_commands advanced: {:?}",
        counted.err()
    );

    for (i, id) in ids.iter().enumerate() {
        let m = cluster.metrics(*id);
        let (before_applied, before_log_len, before_commands) = before[i];
        assert!(
            m.last_applied.map(|l| l.index).unwrap_or(0) > before_applied,
            "node {id} did not advance last_applied"
        );
        assert!(
            m.raft_log_len > before_log_len,
            "node {id} did not grow its log"
        );
        assert_eq!(
            m.applied_commands - before_commands,
            1,
            "node {id} applied a different number of real commands than exactly one"
        );
    }

    cluster.shutdown().await;
}

// =====================================================================================
// M1-26/M1-27 — log growth per mutation, and rejected mutations create none
// =====================================================================================

/// M1-26: ten sequential puts grow the leader's log by at least ten entries (tolerant of a
/// spurious election adding a blank/membership entry) and the public `cluster_revision` — the
/// oracle immune to that noise — by exactly ten.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_26_direct_write_log_growth_is_one_entry_per_mutation() {
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let leader = cluster.leader().await;

    let before_log_len = cluster.metrics(leader).raft_log_len;
    let before_revision = cluster.metrics(leader).cluster_revision;

    for i in 1..=10u64 {
        let response = cluster
            .client(leader)
            .put(put_req(&format!("/m1-26/{i}"), "v"))
            .await
            .unwrap_or_else(|e| panic!("put {i} failed: {e}"));
        assert_eq!(response.outcome, MutationOutcome::Applied, "put {i}");
    }

    let after_log_len = cluster.metrics(leader).raft_log_len;
    let after_revision = cluster.metrics(leader).cluster_revision;

    assert!(
        after_log_len - before_log_len >= 10,
        "raft_log_len grew by {} (< 10) for 10 puts",
        after_log_len - before_log_len
    );
    assert_eq!(
        after_revision - before_revision,
        10,
        "cluster_revision must grow by exactly one per mutation, immune to blank/election noise"
    );

    cluster.shutdown().await;
}

/// M1-27: a structurally invalid delete and an over-cap put are both rejected at the API edge
/// and never enter the log or allocate a revision, on any node.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_27_rejected_mutation_creates_no_log_entry() {
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let leader = cluster.leader().await;
    let ids = cluster.ids();

    let before: Vec<u64> = ids
        .iter()
        .map(|id| cluster.metrics(*id).raft_log_len)
        .collect();
    let before_revision = cluster.metrics(leader).cluster_revision;

    let mut invalid_delete = delete_req("/m1-27/edge-invalid");
    invalid_delete.expected_mod_revision = Some(0);
    match cluster.client(leader).delete(invalid_delete).await {
        Err(ConfigError::InvalidArgument { .. }) => {}
        other => panic!("Delete{{expected: Some(0)}} must be InvalidArgument, got {other:?}"),
    }

    let oversize_value = vec![0u8; Limits::DEFAULT.max_value_bytes + 1];
    let mut oversize_put = put_req("/m1-27/oversize", "");
    oversize_put.value = bytes::Bytes::from(oversize_value);
    match cluster.client(leader).put(oversize_put).await {
        Err(ConfigError::ResourceExhausted { .. }) => {}
        other => panic!("an over-cap value must be ResourceExhausted, got {other:?}"),
    }

    for (i, id) in ids.iter().enumerate() {
        assert_eq!(
            cluster.metrics(*id).raft_log_len,
            before[i],
            "node {id}'s log grew from a rejected mutation"
        );
    }
    assert_eq!(
        cluster.metrics(leader).cluster_revision,
        before_revision,
        "cluster_revision moved from a rejected mutation"
    );

    cluster.shutdown().await;
}

// =====================================================================================
// M1-44/M1-45/M1-46 — conformance parity between DirectClient and GrpcClient
// =====================================================================================

/// M1-44: the full C-01..C-15 conformance suite passes against `DirectClient`.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_44_conformance_direct_client() {
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let leader = cluster.leader().await;

    let report =
        conformance::run_all(cluster.client(leader), ConformanceConfig::unique("m1-44")).await;
    report.assert_all_passed();

    cluster.shutdown().await;
}

/// M1-45: the same suite, with the same expected values, passes against `GrpcClient` over the
/// (M1) insecure transport.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_45_conformance_grpc_client() {
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    cluster.leader().await;

    let store: Arc<dyn ConfigStore> = Arc::new(cluster.grpc_client_multi());
    let report = conformance::run_all(store, ConformanceConfig::unique("m1-45")).await;
    report.assert_all_passed();

    cluster.shutdown().await;
}

/// M1-46: running the same scenarios through `DirectClient` and `GrpcClient` produces
/// scenario-identical reports (modulo transport-only fields) — the actual proof that direct and
/// gRPC access share semantics, not just that each independently passes.
///
/// Uses two separate, freshly-formed clusters (one per client kind) rather than one shared
/// cluster: `cluster_revision` is a single cluster-wide counter (test plan §7.2), so running
/// both suites back-to-back against one cluster would give the second run an inherited offset
/// baseline and produce a spurious, meaningless diff on every revision-bearing field. Two
/// independent clusters both start at revision 0, so their absolute revision sequences line up
/// exactly for scenarios run in the same order — this does not conflict with anti-flake rule 8
/// ("one cluster per test"), whose "no cross-test reuse" concern is about state leaking between
/// separate `#[retcd_test]` functions, not about how many `Cluster::start` calls one function
/// makes.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_46_conformance_reports_are_identical() {
    let direct_cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let direct_leader = direct_cluster.leader().await;
    let direct_report = conformance::run_all(
        direct_cluster.client(direct_leader),
        ConformanceConfig::unique("m1-46-direct"),
    )
    .await;
    direct_cluster.shutdown().await;

    let grpc_cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    grpc_cluster.leader().await;
    let grpc_store: Arc<dyn ConfigStore> = Arc::new(grpc_cluster.grpc_client_multi());
    let grpc_report =
        conformance::run_all(grpc_store, ConformanceConfig::unique("m1-46-grpc")).await;
    grpc_cluster.shutdown().await;

    direct_report.assert_all_passed();
    grpc_report.assert_all_passed();

    let diff = direct_report.diff(&grpc_report);
    assert!(
        diff.is_empty(),
        "direct and gRPC conformance reports disagree: {diff:#?}"
    );
}
