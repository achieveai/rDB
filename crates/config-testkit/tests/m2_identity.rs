//! M2 acceptance rows M2-41..M2-48 (test plan §3.5): a data directory is permanently bound to
//! its [`ClusterIdentity`] (ADR-0011) — written once on first open, refused on any mismatch,
//! and untouched by every crash in the §3.3 matrix.
//!
//! `crates/config-testkit/tests/m2_harness_smoke.rs::reopen_with_a_foreign_identity_is_an_identity_mismatch`
//! already exercises the same `RocksStore::open` seam this file drives from; these rows extend
//! it to every mismatched field individually, to the log/oracle checks §3.5's table asks for,
//! and to `form_cluster` (M2-48). M2-46 (no panic, `config-server` exits with code 2) is a
//! daemon-process row — the exit-code half belongs to `crates/config-server/tests/e2e_daemon.rs`
//! (out of this crate's scope); the "typed error, not a panic" half is what every M2-42..M2-45
//! test below already proves by pattern-matching a normal `Err`, so M2-46 is folded into them
//! rather than duplicated as its own test.

mod support;

use std::sync::Arc;

use config_core::{ClusterId, NodeId, RecoveryEpoch};
use config_engine::{FormationError, FormationPlan};
use config_storage::{Boundary, NoFaults, RocksStore, StorageOpenError};
use config_testkit::cluster::{Cluster, StorageKind};
use support::{
    field, field_u64, my_log_lines, rocks_cluster_with_scripts, settled_leader, unformed_rocks,
};

/// How many lines this test's own JSONL file holds right now — a baseline to filter *out* the
/// legitimate node startup (and, for M2-42..M2-44, the `stop_all` shutdown) every one of these
/// tests performs *before* attempting its mismatched open. Without this, every row below is a
/// false positive: `Cluster::start`/`unformed_rocks` necessarily logs real `openraft::*` lines
/// (`get_initial_state`, membership load, …) for the node(s) it legitimately brings up, and
/// [`assert_no_openraft_lines`] must prove nothing reached openraft *as a result of the
/// mismatched attempt*, not that the test's log is empty of openraft activity altogether.
fn log_baseline(method: &str) -> usize {
    my_log_lines(module_path!(), method).len()
}

/// Every `identity_mismatch` line logged since `since` (see [`log_baseline`]).
fn identity_mismatch_lines(method: &str, since: usize) -> Vec<serde_json::Value> {
    my_log_lines(module_path!(), method)
        .into_iter()
        .skip(since)
        .filter(|r| field(r, "@m") == Some("identity_mismatch"))
        .collect()
}

/// Q7's other half: no `openraft`-targeted line was logged since `since` — the mismatch must be
/// caught before `Raft::new`/`Raft::initialize` is ever reached, not merely somewhere in a log
/// that also contains the test's own legitimate setup.
fn assert_no_openraft_lines(method: &str, since: usize) {
    let rows = my_log_lines(module_path!(), method);
    let openraft_rows: Vec<_> = rows
        .iter()
        .skip(since)
        .filter(|r| {
            field(r, "@logger")
                .map(|l| l.starts_with("openraft"))
                .unwrap_or(false)
        })
        .collect();
    assert!(
        openraft_rows.is_empty(),
        "an identity-mismatched open reached openraft: {openraft_rows:#?}"
    );
}

/// M2-41: `state_meta/identity` is written on first open, and is readable — unchanged — after
/// a close and reopen.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_41_identity_written_on_first_open() {
    let cluster = Cluster::start(1, StorageKind::ROCKS).await;
    let id = NodeId(1);
    cluster.leader().await;

    let configured = cluster.identity(id);
    assert_eq!(configured.node_id, id);

    cluster.stop_node(id).await;
    let reopened = cluster
        .reopen_store(id)
        .expect("the directory reopens once the node is stopped");
    assert_eq!(
        reopened.identity(),
        configured,
        "state_meta/identity did not round-trip exactly what was configured on first open"
    );
    assert!(
        !reopened.is_fresh(),
        "a directory that has already bound an identity must not report itself fresh"
    );
    drop(reopened);

    cluster.shutdown().await;
}

/// M2-42: a reopen with a different `cluster_id` is refused before `Raft::new` runs.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_42_wrong_cluster_id_blocks_startup() {
    let cluster = Cluster::start(1, StorageKind::ROCKS).await;
    let id = NodeId(1);
    cluster.leader().await;
    let stored = cluster.identity(id);
    let dir = cluster.data_dir(id);
    let limits = cluster.config().limits;
    cluster.stop_all().await;
    let since = log_baseline("m2_42_wrong_cluster_id_blocks_startup");

    let mut configured = stored;
    configured.cluster_id = ClusterId::from_bytes([0xAA; 16]);
    let opened = RocksStore::open(
        &dir,
        configured,
        limits,
        Arc::new(NoFaults),
        tracing::Span::current(),
    );
    match opened {
        Err(StorageOpenError::IdentityMismatch {
            stored: got_stored,
            configured: got_configured,
            ..
        }) => {
            assert_eq!(got_stored, stored);
            assert_eq!(got_configured, configured);
        }
        other => panic!("a mismatched cluster_id was not refused as IdentityMismatch: {other:?}"),
    }

    let rows = identity_mismatch_lines("m2_42_wrong_cluster_id_blocks_startup", since);
    assert_eq!(
        rows.len(),
        1,
        "expected exactly one identity_mismatch line: {rows:#?}"
    );
    let row = &rows[0];
    assert_eq!(field(row, "@l"), Some("Error"));
    assert_eq!(
        field(row, "stored_cluster_id"),
        Some(stored.cluster_id.to_string().as_str())
    );
    assert_eq!(
        field(row, "configured_cluster_id"),
        Some(configured.cluster_id.to_string().as_str())
    );
    assert_eq!(field_u64(row, "node_id"), Some(id.0));
    assert_no_openraft_lines("m2_42_wrong_cluster_id_blocks_startup", since);
}

/// M2-43: a reopen with a different `node_id` is refused the same way.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_43_wrong_node_id_blocks_startup() {
    let cluster = Cluster::start(1, StorageKind::ROCKS).await;
    let id = NodeId(1);
    cluster.leader().await;
    let stored = cluster.identity(id);
    let dir = cluster.data_dir(id);
    let limits = cluster.config().limits;
    cluster.stop_all().await;
    let since = log_baseline("m2_43_wrong_node_id_blocks_startup");

    let mut configured = stored;
    configured.node_id = NodeId(99);
    let opened = RocksStore::open(
        &dir,
        configured,
        limits,
        Arc::new(NoFaults),
        tracing::Span::current(),
    );
    assert!(
        matches!(opened, Err(StorageOpenError::IdentityMismatch { .. })),
        "a mismatched node_id was not refused as IdentityMismatch: {opened:?}"
    );

    let rows = identity_mismatch_lines("m2_43_wrong_node_id_blocks_startup", since);
    assert_eq!(
        rows.len(),
        1,
        "expected exactly one identity_mismatch line: {rows:#?}"
    );
    assert_eq!(
        field_u64(&rows[0], "stored_node_id"),
        Some(stored.node_id.0)
    );
    assert_eq!(field_u64(&rows[0], "configured_node_id"), Some(99));
    assert_no_openraft_lines("m2_43_wrong_node_id_blocks_startup", since);
}

/// M2-44: a reopen with `recovery_epoch + 1` is refused the same way.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_44_wrong_recovery_epoch_blocks_startup() {
    let cluster = Cluster::start(1, StorageKind::ROCKS).await;
    let id = NodeId(1);
    cluster.leader().await;
    let stored = cluster.identity(id);
    let dir = cluster.data_dir(id);
    let limits = cluster.config().limits;
    cluster.stop_all().await;
    let since = log_baseline("m2_44_wrong_recovery_epoch_blocks_startup");

    let mut configured = stored;
    configured.recovery_epoch = RecoveryEpoch(stored.recovery_epoch.0 + 1);
    let opened = RocksStore::open(
        &dir,
        configured,
        limits,
        Arc::new(NoFaults),
        tracing::Span::current(),
    );
    assert!(
        matches!(opened, Err(StorageOpenError::IdentityMismatch { .. })),
        "a mismatched recovery_epoch was not refused as IdentityMismatch: {opened:?}"
    );

    let rows = identity_mismatch_lines("m2_44_wrong_recovery_epoch_blocks_startup", since);
    assert_eq!(
        rows.len(),
        1,
        "expected exactly one identity_mismatch line: {rows:#?}"
    );
    assert_eq!(
        field_u64(&rows[0], "stored_recovery_epoch"),
        Some(stored.recovery_epoch.0 as u64)
    );
    assert_eq!(
        field_u64(&rows[0], "configured_recovery_epoch"),
        Some((stored.recovery_epoch.0 + 1) as u64)
    );
    assert_no_openraft_lines("m2_44_wrong_recovery_epoch_blocks_startup", since);
}

/// M2-45: a byte-for-byte copy of one node's directory, started under a different node's
/// identity, is refused exactly like a hand-built mismatch — cloning a data directory is
/// indistinguishable from (and forbidden for the same reason as) reusing one under the wrong
/// identity (§4.2).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_45_cloned_data_dir_rejected() {
    let cluster = Cluster::start(2, StorageKind::ROCKS).await;
    cluster.leader().await;
    let node1_identity = cluster.identity(NodeId(1));
    let node2_identity = cluster.identity(NodeId(2));
    assert_ne!(
        node1_identity, node2_identity,
        "the two nodes must have distinct identities to clone across"
    );
    let node1_dir = cluster.data_dir(NodeId(1));
    let limits = cluster.config().limits;
    cluster.stop_all().await;

    let clone_dir = node1_dir
        .parent()
        .expect("a data dir has a parent")
        .join("node1-clone-for-m2-45");
    copy_dir_recursive(&node1_dir, &clone_dir).expect("copy node 1's directory");

    // Opening the clone under node 1's own identity works (it is byte-for-byte the same data).
    let same_identity = RocksStore::open(
        &clone_dir,
        node1_identity,
        limits,
        Arc::new(NoFaults),
        tracing::Span::current(),
    );
    assert!(
        same_identity.is_ok(),
        "the clone did not even open under its own original identity: {same_identity:?}"
    );
    drop(same_identity);

    // Opening it configured as node 2 is exactly M2-45: the clone is refused.
    let as_node2 = RocksStore::open(
        &clone_dir,
        node2_identity,
        limits,
        Arc::new(NoFaults),
        tracing::Span::current(),
    );
    assert!(
        matches!(as_node2, Err(StorageOpenError::IdentityMismatch { .. })),
        "a cloned data directory was accepted under a different node's identity: {as_node2:?}"
    );

    std::fs::remove_dir_all(&clone_dir).ok();
    cluster.shutdown().await;
}

fn copy_dir_recursive(from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let dest = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir_recursive(&entry.path(), &dest)?;
        } else {
            std::fs::copy(entry.path(), &dest)?;
        }
    }
    Ok(())
}

/// M2-47: identity survives a crash at every one of the 8 boundaries — written once on first
/// open, never rewritten by any apply, vote, or log write afterward.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_47_identity_survives_crash_at_every_boundary() {
    let (cluster, scripts) = rocks_cluster_with_scripts(3).await;
    let id = NodeId(1);
    cluster.leader().await;
    let identity_before = cluster.identity(id);

    for boundary in Boundary::ALL {
        // Target node 1 specifically regardless of its current role: `crash_on_nth` only fires
        // when this node itself crosses the boundary, and a plain put always crosses the
        // append/flush/state-batch boundaries on the leader, so pointing every put at node 1
        // (stepping it up via a write if it is not already leader) reliably exercises it.
        //
        // The vote boundaries need the opposite role. Isolating a *leader* never crosses
        // `BeforeVoteSync`/`AfterVoteSync`: nothing ever tells an isolated leader about a higher
        // term, so it just keeps reporting `Leader` at its old term forever (this openraft
        // build has no check-quorum/lease step-down to force the issue) — confirmed empirically
        // (an earlier revision of this test isolated node 1 unconditionally and hung for the
        // full 20-election-timeout deadline whenever node 1 happened to be leader). Only a
        // *follower*, cut off from the real leader's heartbeats, times out and campaigns for
        // itself, which is what actually crosses the boundary — the same driver M2-19/M2-20
        // already use. So if node 1 currently holds leadership, step it down first: isolate it
        // with no crash armed, wait for the other two to elect a replacement, heal, and let
        // node 1 rejoin (and catch up) as an ordinary follower before arming and isolating it
        // for real.
        if matches!(boundary, Boundary::BeforeVoteSync | Boundary::AfterVoteSync) {
            if settled_leader(&cluster, cluster.deadline(10)).await == id {
                cluster.isolate(id);
                let new_leader = cluster
                    .wait_for(
                        &format!("{boundary}: a replacement leader while stepping node 1 down"),
                        cluster.deadline(20),
                        || cluster.leaders_now().into_iter().find(|l| *l != id),
                    )
                    .await
                    .unwrap_or_else(|t| panic!("{boundary}: node 1 never lost leadership: {t}"));
                cluster.heal();
                // Not `wait_converged`: nothing has been written since the step-down began, so
                // every node's KV state already matches trivially and that check would return
                // immediately — before node 1 has actually received a heartbeat from
                // `new_leader` and stepped down. Re-isolating node 1 at that point would leave
                // it exactly as stuck as before. Waiting for node 1's own metrics to name
                // `new_leader` proves it processed at least one RPC from the new term.
                cluster
                    .wait_for(
                        &format!("{boundary}: node 1 to learn of {new_leader}'s leadership"),
                        cluster.deadline(20),
                        || (cluster.metrics(id).current_leader == Some(new_leader)).then_some(()),
                    )
                    .await
                    .unwrap_or_else(|t| {
                        panic!("{boundary}: node 1 never learned of the new leader: {t}")
                    });
            }
            scripts[&id].crash_on_nth(boundary, 1);
            cluster.isolate(id);
        } else {
            scripts[&id].crash_on_nth(boundary, 1);
            // `settled_leader`, not `cluster.leader()`: a prior vote-boundary iteration may
            // have isolated whichever node was leader at the time, and a still-settling ex-
            // leader reporting stale `Leader` state must not be allowed to mask the real one.
            let leader = settled_leader(&cluster, cluster.deadline(10)).await;
            let _ = cluster
                .client(leader)
                .put(config_core::PutRequest {
                    key: bytes::Bytes::copy_from_slice(format!("/m2/47/{boundary}").as_bytes()),
                    value: bytes::Bytes::copy_from_slice(b"v"),
                    expected_mod_revision: None,
                })
                .await;
        }

        cluster
            .wait_for(
                &format!("node 1 to crash at {boundary}"),
                cluster.deadline(20),
                || cluster.store(id).is_poisoned().then_some(()),
            )
            .await
            .unwrap_or_else(|t| panic!("{boundary}: node 1 never crashed: {t}"));
        let crossed = cluster.counters(id).get(boundary);
        assert!(crossed >= 1, "{boundary}: never actually crossed on node 1");

        cluster.heal();
        cluster
            .restart(id)
            .await
            .unwrap_or_else(|e| panic!("{boundary}: restart: {e}"));
        cluster
            .wait_rejoined(id, cluster.deadline(20))
            .await
            .unwrap_or_else(|t| panic!("{boundary}: rejoin: {t}"));

        assert_eq!(
            cluster.identity(id),
            identity_before,
            "{boundary}: identity changed after a crash+restart"
        );
    }

    cluster.shutdown().await;
}

/// M2-48: `form_cluster` against a manifest for one cluster, run against a node whose plan
/// names a different one, is refused as a typed `IdentityMismatch` before `Raft::initialize` —
/// extending M1-06's in-memory version to a real Rocks data directory.
///
/// Unlike M2-42..M2-44, this does *not* also assert "no `openraft`-targeted log line at all":
/// `unformed_rocks` brings up three genuinely live (if unformed) Raft cores, each already
/// ticking its own periodic `report_metrics`/`get current_leader` debug logging in the
/// background — chatter that continues regardless of what `form_cluster` does and cannot be
/// isolated from it by a log-line baseline the way M2-42..M2-44 isolate their fully-stopped
/// `RocksStore::open` attempt. `form_cluster`'s identity check is a pure in-memory field
/// comparison before any RPC or storage touch, so the two assertions below — no log entry was
/// appended, no voter was gained — are what actually proves `Raft::initialize` never ran; a
/// blanket "zero openraft lines" oracle would just be testing this suite's own background noise.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_48_formation_identity_must_match_manifest() {
    let cluster = unformed_rocks(3).await;

    let mut foreign = cluster.identity(NodeId(1));
    foreign.cluster_id = ClusterId::from_bytes([0xBB; 16]);
    let voters: Vec<(NodeId, String)> = cluster
        .ids()
        .into_iter()
        .map(|id| (id, cluster.peer_endpoint(id)))
        .collect();

    let result = cluster
        .node(NodeId(1))
        .form_cluster(FormationPlan::new(&foreign, voters))
        .await;
    assert!(
        matches!(result, Err(FormationError::IdentityMismatch(_))),
        "a plan for a foreign cluster id was accepted by form_cluster: {result:?}"
    );

    for m in cluster.running_metrics() {
        assert_eq!(
            m.last_log_index, None,
            "node {} appended a log entry for a refused formation plan",
            m.node_id
        );
        assert!(
            m.membership_voter_ids.is_empty(),
            "node {} gained voters from a refused formation plan",
            m.node_id
        );
    }

    cluster.shutdown().await;
}
