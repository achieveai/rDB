//! M5-88 / M5-89 / M5-92 — the fence a `restore` mints, seen from both sides: an old cluster
//! and a restored one can never talk to each other, and a restored store still forms the new
//! cluster it was restored into (spec §19.11; ADR-0011, ADR-0024; OQ-45).
//!
//! # M5-88 / M5-89 — what they prove, and what they approximate
//!
//! The test plan's setup for both rows is "restore a backup, keep the original cluster running,
//! point one at the other." What `restore` *does* to make that refusal happen is mint a new
//! `cluster_id`/`recovery_epoch` pair unrelated to the source's — proven directly, at the store
//! level, by `crates/config-server/tests/m5_backup_cli.rs::m5_94_restore_does_not_reuse_the_source_identity_anywhere`
//! (the restored directory refuses to open under the source identity at all). What these two
//! rows prove is the other half: that two live clusters holding **different** `cluster_id`s —
//! which is exactly what "the old cluster" and "a cluster grown from a restored directory" are,
//! from the peer plane's point of view — reject each other's Raft traffic symmetrically, in both
//! directions, and that neither side's leadership is disturbed by the attempt.
//!
//! For those two rows that is an approximation rather than a literal backup-then-restore round
//! trip, and a sound one: fencing is keyed entirely on the identity tuple in the envelope, never
//! on cluster history, so two independently formed clusters given distinct `cluster_id`s through
//! `ClusterBuilder::cluster_id` are indistinguishable from "old cluster" and "restored cluster"
//! as far as `PeerReject::WrongCluster` is concerned.
//!
//! # M5-92 — the round trip itself
//!
//! M5-92 does take the literal route, because formation is the one part an approximation cannot
//! stand in for. It exports a real artifact from a stopped node, restores it into a fenced
//! identity through `config_storage::restore_into_fresh_store`, and stands that directory up as
//! node 1 of a new `Cluster` via `ClusterBuilder::data_dir` — the harness seam whose absence
//! this module doc previously recorded as the gap that blocked the row.

mod support;

use config_core::{ClusterId, ClusterIdentity, NodeId, RecoveryEpoch};
use config_engine::transport::{PeerEnvelopeMeta, PeerReject, PeerRequest};
use config_storage::types::RaftNodeId;
use config_testkit::cluster::{Cluster, StorageKind};
use openraft::raft::VoteRequest;
use openraft::Vote;

use support::{get_req, put_req};

/// M5-88 and M5-89 together: the refusal is proven in both directions from a single pair of
/// clusters, which is what makes it a symmetry claim rather than two coincidentally similar
/// tests.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_88_and_89_distinct_clusters_refuse_each_others_peer_traffic_both_ways() {
    let old = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::ROCKS)
        .cluster_id(ClusterId::from_bytes([0xA1; 16]))
        .start()
        .await;
    old.wait_formed(old.deadline(10))
        .await
        .unwrap_or_else(|t| panic!("old cluster: {t}"));

    let restored = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::ROCKS)
        .cluster_id(ClusterId::from_bytes([0xB2; 16]))
        .start()
        .await;
    restored
        .wait_formed(restored.deadline(10))
        .await
        .unwrap_or_else(|t| panic!("restored cluster: {t}"));

    let old_leader_before = old.leader().await;
    let restored_leader_before = restored.leader().await;

    // M5-88: a node from the old cluster dials a node in the restored cluster.
    let target = restored
        .followers()
        .first()
        .copied()
        .unwrap_or(restored.leader().await);
    let handler = restored.node(target).peer_handler();
    let sender = old.leader().await;
    let meta = PeerEnvelopeMeta {
        cluster_id: old.config().cluster_id,
        recovery_epoch: old.config().recovery_epoch,
        from: sender,
        to: target,
        trace: config_log::TraceContext::new_root(),
    };
    let vote: VoteRequest<RaftNodeId> = VoteRequest::new(Vote::new(99, sender.0), None);
    let rejection = handler
        .handle(meta, PeerRequest::Vote(vote))
        .await
        .expect_err("a node from an unrelated cluster must be refused");
    match rejection {
        PeerReject::WrongCluster { expected, got } => {
            assert_eq!(expected, restored.config().cluster_id);
            assert_eq!(got, old.config().cluster_id);
        }
        other => panic!("expected WrongCluster, got {other:?}"),
    }

    // M5-89: the mirror direction — a node from the restored cluster dials the old one.
    let target = old
        .followers()
        .first()
        .copied()
        .unwrap_or(old.leader().await);
    let handler = old.node(target).peer_handler();
    let sender = restored.leader().await;
    let meta = PeerEnvelopeMeta {
        cluster_id: restored.config().cluster_id,
        recovery_epoch: restored.config().recovery_epoch,
        from: sender,
        to: target,
        trace: config_log::TraceContext::new_root(),
    };
    let vote: VoteRequest<RaftNodeId> = VoteRequest::new(Vote::new(99, sender.0), None);
    let rejection = handler
        .handle(meta, PeerRequest::Vote(vote))
        .await
        .expect_err("a node from the restored cluster must be refused by the old cluster too");
    match rejection {
        PeerReject::WrongCluster { expected, got } => {
            assert_eq!(expected, old.config().cluster_id);
            assert_eq!(got, restored.config().cluster_id);
        }
        other => panic!("expected WrongCluster, got {other:?}"),
    }

    // Neither refusal disturbed either cluster's own leadership — both clusters keep their own
    // leaders and revisions, exactly as M5-88's expectation states.
    assert_eq!(
        old.leader_now(),
        Some(old_leader_before),
        "a refused cross-cluster vote must not have changed the old cluster's leader"
    );
    assert_eq!(
        restored.leader_now(),
        Some(restored_leader_before),
        "a refused cross-cluster vote must not have changed the restored cluster's leader"
    );

    old.shutdown().await;
    restored.shutdown().await;
}

// -------------------------------------------------------------------------------------------
// M5-92 — a restored store forms the new cluster as its genesis member (OQ-45, ADR-0011)
// -------------------------------------------------------------------------------------------

/// M5-92: the directory `restore` wrote carries data, and ADR-0011 only lets a **fresh** store
/// form a cluster. OQ-45 rules that the restored store is fresh *for formation* — and this row
/// is what makes that ruling observable end to end: a real `export_snapshot` artifact, a real
/// `restore_into_fresh_store` into a fenced identity, and then a real three-node formation with
/// the restored directory as node 1 and two brand-new peers.
///
/// Four claims, in the order the row states them:
///
/// 1. the restored store is **not** treated as fresh for *identity binding* — reopening it
///    under the source identity is refused, which is the fence the new cluster id exists for;
/// 2. it **is** accepted as the genesis member — `form_cluster` succeeds on it although
///    `last_applied` is absent only because restore deliberately wrote no Raft position;
/// 3. formation is ordinary ADR-0011 formation: one plan carrying the new identity, no
///    self-forming, and the two fresh peers commit the same membership;
/// 4. a second formation attempt is refused.
///
/// # Deviation, recorded
///
/// The row names a `formation_completed` event. No such event exists in this build — the
/// engine logs `"forming cluster"` on the way in and reports success through
/// `form_cluster`'s `Result` — so "exactly one formation" is asserted the way the harness can
/// actually observe it: the second call is refused with `AlreadyFormed`, and every node's
/// committed membership is the one plan's voter set.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_92_a_restored_store_forms_the_new_cluster_as_its_genesis_member() {
    // --- the cluster that is about to be lost -----------------------------------------------
    let source = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::ROCKS)
        .start()
        .await;
    source
        .wait_formed(source.deadline(10))
        .await
        .unwrap_or_else(|t| panic!("source cluster: {t}"));
    let writer = source.leader().await;
    for i in 0..5 {
        source
            .client(writer)
            .put(put_req(&format!("/m5/92/{i}"), "before-the-disaster"))
            .await
            .unwrap_or_else(|e| panic!("seed put {i}: {e}"));
    }
    let backup_revision = source.metrics(writer).cluster_revision;

    // Taken from a stopped node, exactly as `config-server backup` requires: `export_snapshot`
    // opens the directory itself, so the node holding RocksDB's LOCK must be down first.
    let donor = source.followers().first().copied().unwrap_or(writer);
    source
        .wait_revision_all(backup_revision, source.deadline(20))
        .await
        .expect("every voter reaches the backup revision before the backup is taken");
    let source_identity = source.identity(donor);
    source.stop(donor).await;
    let artifacts = config_testkit::fs::temp_dir();
    let snap = artifacts.path().join("m5-92.snap");
    let header = config_storage::export_snapshot(&source.data_dir(donor), &snap)
        .expect("export a backup artifact from the stopped node");
    source.shutdown().await; // the disaster

    // --- restore into one fenced, empty directory -------------------------------------------
    let fresh = config_testkit::fs::temp_dir();
    let data_dir = fresh.path().join("restored");
    let new_cluster_id = ClusterId::from_bytes([0x92; 16]);
    let new_epoch = RecoveryEpoch(source_identity.recovery_epoch.0 + 1);
    let new_identity = ClusterIdentity {
        cluster_id: new_cluster_id,
        recovery_epoch: new_epoch,
        node_id: NodeId(1),
    };
    config_storage::restore_into_fresh_store(
        &data_dir,
        &new_identity,
        &snap,
        &config_core::RestoredFrom {
            cluster_id: source_identity.cluster_id,
            recovery_epoch: source_identity.recovery_epoch.0,
            revision: header.cluster_revision,
        },
    )
    .expect("restore into a fresh, fenced store");

    // Claim 1. Formation-fresh is not identity-fresh: the restored directory is bound to the
    // new identity and refuses the old one outright. Asserted here, while nothing holds the
    // directory's lock, and before the cluster below takes it.
    match config_storage::RocksStore::open(
        &data_dir,
        source_identity,
        config_core::Limits::DEFAULT,
        std::sync::Arc::new(config_storage::NoFaults),
        tracing::Span::current(),
    ) {
        Err(config_storage::StorageOpenError::IdentityMismatch { stored, .. }) => {
            assert_eq!(
                stored, new_identity,
                "the restored directory must be bound to the identity restore gave it"
            );
        }
        Ok(_) => panic!("the restored store must not open under the identity it was taken from"),
        Err(other) => panic!("expected an ADR-0011 identity refusal, got {other}"),
    }

    // --- the new cluster: restored directory as node 1, two brand-new peers -----------------
    // `recovery_epoch` is set on the assembled config because `ClusterBuilder` has no setter
    // for it; it must equal the epoch `restore` bound the directory to or node 1 would not
    // open at all.
    let mut cfg = Cluster::builder()
        .nodes(3)
        .storage(StorageKind::ROCKS)
        .cluster_id(new_cluster_id)
        .data_dir(NodeId(1), &data_dir)
        .form(false)
        .config();
    cfg.recovery_epoch = new_epoch;
    let restored = Cluster::start_with(cfg).await;
    assert_eq!(
        restored.identity(NodeId(1)),
        new_identity,
        "node 1 must be configured with exactly the identity restore wrote"
    );

    // Claim 2 and 3: ordinary ADR-0011 formation, on a store that carries data.
    restored.form().await.expect(
        "OQ-45: a restored store is the genesis member of the cluster it was restored into",
    );
    restored
        .wait_formed(restored.deadline(10))
        .await
        .unwrap_or_else(|t| panic!("restored cluster: {t}"));
    let leader = restored.leader().await;
    let committed = restored
        .node(leader)
        .membership_report()
        .membership
        .voters
        .clone();
    assert_eq!(
        committed,
        [NodeId(1), NodeId(2), NodeId(3)].into_iter().collect(),
        "the one formation plan's voter set is what every node committed"
    );

    // The restored data really did come with it, and the revision continues rather than
    // restarting (ADR-0024's `cluster_revision` is preserved).
    let record = restored
        .client(leader)
        .get(get_req("/m5/92/0"))
        .await
        .expect("read the restored key")
        .record
        .expect("a key written before the disaster must have survived the restore");
    assert_eq!(&record.value[..], b"before-the-disaster");
    assert!(
        restored.metrics(leader).cluster_revision >= backup_revision,
        "the restored cluster continues the source's revision line rather than restarting it"
    );

    // Claim 4: the cluster cannot be formed twice.
    match restored.form().await {
        Err(config_engine::FormationError::AlreadyFormed) => {}
        other => panic!("a second formation attempt must be refused, got {other:?}"),
    }

    restored.shutdown().await;
}
