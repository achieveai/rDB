//! M2 acceptance rows M2-36 and M2-37 (test plan §3.4): under an ordinary cluster workload,
//! M0-M3's `SnapshotPolicy::Never` design (research §3.6, ADR-0008 OQ-25) is never contradicted
//! by what the store actually does. Both rows are measured directly off `RocksStore`'s own
//! counters and trait methods on every node, not assumed from the configured policy value —
//! that is the whole point of "measured, not assumed" in M2-36's oracle text.
//!
//! `config-storage` had no purge/snapshot-build counters before this file: [`RocksStore`] now
//! exposes `purge_calls()` and `snapshot_build_calls()` (see `RocksShared`), and `RocksSm`'s
//! `SnapshotBuilder` is the counted `RocksSnapshotBuilder` rather than the uncounted
//! `NoSnapshots` `EphemeralSm` still uses — both are product edits made to unblock this row, not
//! pre-existing surface.

mod support;

use config_core::{ConfigStore, NodeId};
use config_testkit::cluster::{Cluster, StorageKind};
use openraft::storage::{RaftLogStorage, RaftStateMachine};
use support::put_req;

/// Drive `n` unconditional puts through `client` (the plan's "200-mutation cluster workload").
async fn put_n(client: &dyn ConfigStore, n: u64, prefix: &str) {
    for i in 0..n {
        client
            .put(put_req(&format!("{prefix}{i}"), "v"))
            .await
            .unwrap_or_else(|e| panic!("put {prefix}{i}: {e}"));
    }
}

/// M2-36: a 200-mutation workload never causes a purge, on any node — `purge_calls()` stays 0
/// and the persisted `last_purged_log_id` stays `None` throughout.
///
/// Checked on every node (not just the leader): a follower's log storage sees the same append
/// stream replicated to it, so it is just as able to purge as the leader is, and the row's
/// claim is about the storage layer in general, not about one role.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_36_purge_is_never_invoked() {
    let cluster = Cluster::start(3, StorageKind::ROCKS).await;
    let leader = cluster.leader().await;
    put_n(&*cluster.client(leader), 200, "/m2/36/k").await;
    cluster
        .wait_revision_all(200, cluster.deadline(20))
        .await
        .expect("every node applies all 200 writes");

    for id in [NodeId(1), NodeId(2), NodeId(3)] {
        let store = cluster.rocks_store(id);
        assert_eq!(
            store.purge_calls(),
            0,
            "node {id}: purge was called {} times after a 200-mutation workload",
            store.purge_calls()
        );
        let log_state = store
            .log_store()
            .get_log_state()
            .await
            .unwrap_or_else(|e| panic!("node {id}: get_log_state: {e}"));
        assert_eq!(
            log_state.last_purged_log_id, None,
            "node {id}: last_purged_log_id is {:?}, expected None",
            log_state.last_purged_log_id
        );
    }

    cluster.shutdown().await;
}

/// M2-37: the same 200-mutation workload never causes a snapshot to be built, on any node —
/// `snapshot_build_calls()` stays 0 and `get_current_snapshot()` stays `Ok(None)` throughout,
/// which is `SnapshotPolicy::Never` actually holding rather than merely being configured.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_37_snapshot_never_built() {
    let cluster = Cluster::start(3, StorageKind::ROCKS).await;
    let leader = cluster.leader().await;
    put_n(&*cluster.client(leader), 200, "/m2/37/k").await;
    cluster
        .wait_revision_all(200, cluster.deadline(20))
        .await
        .expect("every node applies all 200 writes");

    for id in [NodeId(1), NodeId(2), NodeId(3)] {
        let store = cluster.rocks_store(id);
        assert_eq!(
            store.snapshot_build_calls(),
            0,
            "node {id}: build_snapshot was called {} times after a 200-mutation workload",
            store.snapshot_build_calls()
        );
        let snapshot = store
            .state_machine()
            .get_current_snapshot()
            .await
            .unwrap_or_else(|e| panic!("node {id}: get_current_snapshot: {e}"));
        assert!(
            snapshot.is_none(),
            "node {id}: get_current_snapshot() returned Some(..), expected Ok(None)"
        );
    }

    cluster.shutdown().await;
}
