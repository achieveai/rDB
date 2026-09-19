//! Smoke tests for the M2 half of the [`Cluster`] harness: persistent storage, restart, and
//! the RocksDB lock invariant every restart row depends on (test plan TA-16).
//!
//! The M2 rows (M2-01..M2-64) all assume `restart` really closes the store and really reopens
//! the same directory. If either half were a fiction, a row could pass while proving nothing,
//! so each half is asserted here directly.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use config_core::{
    ClusterId, ClusterIdentity, Durability, GetRequest, MutationOutcome, NodeId, PutRequest,
};
use config_storage::{
    Boundary, FaultAction, FaultInjector, NoFaults, RocksStore, StorageOpenError,
};
use config_testkit::cluster::{Cluster, ClusterConfig, NodeStartError, RocksSpec, StorageKind};

fn put_req(k: &str, v: &str) -> PutRequest {
    PutRequest {
        key: Bytes::copy_from_slice(k.as_bytes()),
        value: Bytes::copy_from_slice(v.as_bytes()),
        expected_mod_revision: None,
    }
}

fn hex(h: [u8; 32]) -> String {
    h.iter().map(|b| format!("{b:02x}")).collect()
}

fn get_req(k: &str) -> GetRequest {
    GetRequest {
        key: Bytes::copy_from_slice(k.as_bytes()),
    }
}

/// A Rocks cluster forms over the real gRPC planes, each node owns its own directory, and the
/// reported durability is the honest one for the configured sync mode (ADR-0016, TA-27).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn rocks_cluster_forms_with_one_data_dir_per_node() {
    let cluster = Cluster::start(3, StorageKind::ROCKS).await;
    let leader = cluster.leader().await;

    let mut dirs: Vec<std::path::PathBuf> = Vec::new();
    for id in cluster.ids() {
        let dir = cluster.data_dir(id);
        assert!(dir.is_dir(), "node {id} has no data directory at {dir:?}");
        assert!(
            !dirs.contains(&dir),
            "node {id} shares a data directory with another node: {dir:?}"
        );
        dirs.push(dir);
        assert_eq!(
            cluster.durability(id),
            Durability::Persistent,
            "a syncing Rocks store must not claim less than Persistent"
        );
    }

    let written = cluster
        .client(leader)
        .put(put_req("/rocks", "v1"))
        .await
        .expect("write on a Rocks leader");
    assert_eq!(written.outcome, MutationOutcome::Applied);
    cluster
        .wait_revision_all(written.revision, cluster.deadline(10))
        .await
        .expect("every node applies the write");

    cluster.shutdown().await;
}

/// `sync_writes: false` downgrades the advertised durability instead of lying about it.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn rocks_without_sync_reports_persistent_unverified() {
    let cluster = Cluster::start_with(ClusterConfig {
        nodes: 1,
        storage: StorageKind::Rocks(RocksSpec::NO_SYNC),
        ..ClusterConfig::default()
    })
    .await;
    cluster.leader().await;

    assert_eq!(
        cluster.durability(NodeId(1)),
        Durability::PersistentUnverified,
        "a non-syncing store must not be reported as Persistent"
    );
    assert_eq!(
        cluster.capabilities(NodeId(1)).durability,
        Durability::PersistentUnverified,
        "the advertised capability must match the store's own claim"
    );

    cluster.shutdown().await;
}

/// `restart` on Rocks reopens the **same** directory, so a value written before the stop is
/// still there afterwards — with the same revision, on the node's own state machine.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn rocks_restart_reopens_the_same_dir_and_keeps_state() {
    let cluster = Cluster::start(3, StorageKind::ROCKS).await;
    let leader = cluster.leader().await;

    let written = cluster
        .client(leader)
        .put(put_req("/durable", "v1"))
        .await
        .expect("write before the restart");
    // Converged, not merely applied: the hash captured here is the thing the restart has to
    // reproduce, so it must be the settled one on every node rather than a snapshot of a node
    // that has not caught up yet.
    let hash_before = cluster
        .wait_converged(cluster.deadline(10))
        .await
        .expect("every node converges before anything is stopped");

    let victim = cluster.followers()[0];
    let dir_before = cluster.data_dir(victim);
    assert_eq!(
        cluster.state_hash(victim),
        hash_before,
        "the node picked to restart was not on the converged state"
    );

    cluster
        .restart(victim)
        .await
        .expect("a restart must not be blocked by a lock the harness itself holds");

    assert_eq!(
        cluster.data_dir(victim),
        dir_before,
        "the restart moved the node to another directory, which would make every M2 row vacuous"
    );

    // The restarted node's own applied state, with no Raft round trip: the value has to have
    // come off disk. Polled, because the node re-enters the cluster asynchronously.
    let recovered = cluster
        .wait_for(
            "the restarted node to serve its persisted applied state",
            cluster.deadline(10),
            || (cluster.state_hash(victim) == hash_before).then_some(()),
        )
        .await;
    assert!(
        recovered.is_ok(),
        "the restarted node came back on a different applied state: before={} after={}          leader={}; {:?}",
        hex(hash_before),
        hex(cluster.state_hash(victim)),
        hex(cluster.state_hash(cluster.leader_now().expect("a leader"))),
        recovered.err()
    );
    cluster
        .wait_rejoined(victim, cluster.deadline(10))
        .await
        .expect("the restarted node rejoins the cluster");
    cluster
        .wait_revision_all(written.revision, cluster.deadline(10))
        .await
        .expect("the restarted node reports the cluster revision");
    cluster.assert_crash_invariants(victim);

    let value = cluster
        .client(cluster.leader().await)
        .get(get_req("/durable"))
        .await
        .expect("read after the restart");
    assert_eq!(
        value.record.as_ref().map(|r| r.mod_revision),
        Some(written.revision),
        "the revision changed across a restart"
    );

    cluster.shutdown().await;
}

/// The lock invariant, asserted rather than assumed: while a store clone is alive, a second
/// open of that directory is refused as `Locked` — and once it is dropped, the same open works.
///
/// This is the failure mode TA-16.1 warns about. Proving the error is reachable is also what
/// proves the restart above is doing real work rather than passing by luck.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn reopening_a_live_rocks_dir_is_refused_as_locked() {
    let cluster = Cluster::start(1, StorageKind::ROCKS).await;
    cluster.leader().await;

    match cluster.reopen_store(NodeId(1)) {
        Err(StorageOpenError::Locked { path, .. }) => {
            assert_eq!(
                path,
                cluster.data_dir(NodeId(1)),
                "wrong directory reported"
            );
        }
        other => panic!("a second open of a live RocksDB directory was not refused: {other:?}"),
    }

    // A partial drop is the real hazard: one store clone outliving the node keeps the lock.
    let held = cluster.rocks_store(NodeId(1));
    cluster.stop_node(NodeId(1)).await;
    assert!(
        matches!(
            cluster.reopen_store(NodeId(1)),
            Err(StorageOpenError::Locked { .. })
        ),
        "a store clone held past the stop did not keep the lock, so the invariant is untested"
    );
    drop(held);

    let reopened = cluster
        .reopen_store(NodeId(1))
        .expect("a fully closed directory reopens");
    assert_eq!(reopened.identity(), cluster.identity(NodeId(1)));
    assert!(
        !reopened.is_fresh(),
        "a directory that served a formed cluster must not look fresh"
    );
    drop(reopened);

    cluster.shutdown().await;
}

/// `stop_all` then `start_all` is a cold restart of the whole cluster from disk: a leader comes
/// back from the persisted membership, with no second formation.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_all_then_start_all_recovers_from_disk() {
    let cluster = Cluster::start(3, StorageKind::ROCKS).await;
    let leader = cluster.leader().await;

    let written = cluster
        .client(leader)
        .put(put_req("/cold", "v1"))
        .await
        .expect("write before the cold restart");
    cluster
        .wait_revision_all(written.revision, cluster.deadline(10))
        .await
        .expect("every node applies the write");
    let membership_before = cluster.membership();

    cluster.stop_all().await;
    assert!(
        cluster.running_ids().is_empty(),
        "stop_all left nodes running"
    );

    cluster
        .start_all()
        .await
        .expect("every directory reopens once stop_all has released its lock");
    assert_eq!(cluster.running_ids().len(), 3);

    // There is no `form` call anywhere in this test: the membership has to come off disk.
    let leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader from persisted membership, without re-forming");
    for id in cluster.ids() {
        cluster
            .wait_rejoined(id, cluster.deadline(20))
            .await
            .unwrap_or_else(|t| panic!("node {id} never rejoined after the cold start: {t}"));
    }
    assert_eq!(
        cluster.membership_of(leader).voters,
        membership_before.voters,
        "the cold-started cluster committed a different voter set"
    );
    assert_eq!(
        cluster.membership_of(leader).membership_log_id,
        membership_before.membership_log_id,
        "a new membership entry was written, so the cluster re-formed instead of recovering"
    );

    let got = cluster
        .client(leader)
        .get(get_req("/cold"))
        .await
        .expect("read after the cold restart");
    assert_eq!(
        got.record.as_ref().map(|r| r.mod_revision),
        Some(written.revision),
        "the acknowledged mutation did not survive a cold restart"
    );

    cluster.shutdown().await;
}

/// The Ephemeral half of M2-10: a restart there is documented to come back empty, and the
/// harness must not quietly make it persistent.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn ephemeral_restart_keeps_its_documented_amnesia() {
    let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
    let leader = cluster.leader().await;
    assert_eq!(cluster.durability(leader), Durability::Ephemeral);

    let written = cluster
        .client(leader)
        .put(put_req("/volatile", "v1"))
        .await
        .expect("write before the restart");
    cluster
        .wait_revision_all(written.revision, cluster.deadline(10))
        .await
        .expect("every node applies the write");

    let victim = cluster.followers()[0];
    cluster.stop_node(victim).await;
    cluster.start_node(victim).await;

    // It comes back empty and is re-replicated, so it converges again — by copying, not by
    // remembering. Both halves matter, and the second is what M2-10 contrasts against Rocks.
    let converged = cluster
        .wait_for(
            "the re-replicated node to match the leader again",
            cluster.deadline(20),
            || {
                let hashes = cluster.state_hashes();
                let distinct: BTreeSet<_> = hashes.values().collect();
                (hashes.len() == 3 && distinct.len() == 1).then_some(())
            },
        )
        .await;
    assert!(
        converged.is_ok(),
        "an ephemeral node never re-replicated after a restart: {:?}",
        converged.err()
    );

    cluster.shutdown().await;
}

/// A data directory refuses a foreign identity (ADR-0011).
///
/// `data_dir` is the seam the M2 identity rows drive, so the harness has to hand out a path
/// that a test can reopen under its own identity and get a typed error from.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn reopen_with_a_foreign_identity_is_an_identity_mismatch() {
    let cluster = Cluster::start(1, StorageKind::ROCKS).await;
    cluster.leader().await;
    let dir = cluster.data_dir(NodeId(1));
    cluster.stop_all().await;

    let foreign = ClusterIdentity {
        cluster_id: ClusterId::from_bytes([9u8; 16]),
        recovery_epoch: cluster.identity(NodeId(1)).recovery_epoch,
        node_id: NodeId(1),
    };
    let opened = RocksStore::open(
        &dir,
        foreign,
        cluster.config().limits,
        Arc::new(NoFaults),
        tracing::Span::current(),
    );
    assert!(
        matches!(opened, Err(StorageOpenError::IdentityMismatch { .. })),
        "a directory accepted a foreign cluster id: {opened:?}"
    );

    cluster.shutdown().await;
}

/// Crashes the `nth` crossing of one boundary and then gets out of the way.
///
/// Local to this file on purpose: it exists to prove the *harness* reports an engine start
/// failure, so it must not depend on a row file's own scripting helpers.
#[derive(Debug)]
struct CrashOnce {
    boundary: Boundary,
    armed: AtomicBool,
}

impl CrashOnce {
    fn new(boundary: Boundary) -> Self {
        Self {
            boundary,
            armed: AtomicBool::new(false),
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }
}

impl FaultInjector for CrashOnce {
    fn before(&self, boundary: Boundary) -> FaultAction {
        if boundary == self.boundary && self.armed.swap(false, Ordering::SeqCst) {
            FaultAction::Crash
        } else {
            FaultAction::Proceed
        }
    }
}

/// A replay that crashes fails the *start*, and the harness says so instead of panicking.
///
/// OpenRaft reports an injected apply-boundary crash as `EngineError::Raft("...storage is
/// poisoned")`, so the harness surfaces it as [`NodeStartError::Engine`], never as a
/// [`StorageOpenError`] — the store opened perfectly well. M2-17 depends on being able to tell
/// the two apart.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn an_engine_start_failure_is_returned_not_panicked() {
    let crasher = Arc::new(CrashOnce::new(Boundary::BeforeStateBatch));
    let cluster = Cluster::builder()
        .nodes(1)
        .storage(StorageKind::ROCKS)
        .faults(NodeId(1), Arc::clone(&crasher) as Arc<dyn FaultInjector>)
        .start()
        .await;
    let leader = cluster.leader().await;
    let written = cluster
        .client(leader)
        .put(put_req("/start/k", "v"))
        .await
        .expect("a clean write before anything is armed");
    cluster
        .wait_revision_all(written.revision, cluster.deadline(8))
        .await
        .expect("the write applies");

    // Crash one apply so the log ends up ahead of the state machine. Without that gap a replay
    // has nothing to apply and never reaches the boundary at all.
    crasher.arm();
    let _ = cluster.client(leader).put(put_req("/start/k2", "v2")).await;
    cluster
        .wait_for(
            "the injected crash to poison the store",
            cluster.deadline(10),
            || cluster.store(leader).is_poisoned().then_some(()),
        )
        .await
        .expect("the injected crash never poisoned the store");

    // Arm again before the restart: the reopened store's counters start at zero, so the
    // replay's own first apply is the crossing that crashes.
    cluster.stop_all().await;
    crasher.arm();
    let started = cluster.start_all().await;
    let err = started.expect_err("the replay crashed, so the node did not start");
    assert!(
        matches!(err, NodeStartError::Engine(_)),
        "a crashed replay is an engine failure, not a storage-open failure: {err:?}"
    );
    assert!(
        err.to_string().contains("poisoned"),
        "the harness must pass the engine's own text through: {err}"
    );

    // Nothing is armed now, so the same directory starts cleanly and keeps its state.
    cluster
        .start_all()
        .await
        .expect("the retry replays to the end");
    cluster
        .wait_rejoined(NodeId(1), cluster.deadline(10))
        .await
        .expect("the node comes back once the replay completes");
    assert_eq!(
        cluster
            .client(NodeId(1))
            .get(get_req("/start/k"))
            .await
            .expect("read after the successful restart")
            .record
            .expect("the key written before the crash")
            .value
            .as_ref(),
        b"v",
        "the failed start cost nothing that was already acknowledged"
    );

    cluster.shutdown().await;
}
