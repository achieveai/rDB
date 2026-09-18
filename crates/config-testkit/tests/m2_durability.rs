//! M2 acceptance rows M2-01..M2-18 (test plan §3.1, §3.2): ordinary restart preserves every
//! acknowledged mutation, and a leader that crashes after a commit but before its own apply
//! replays that entry exactly once on restart.
//!
//! Every test builds its own [`Cluster`] on [`StorageKind::ROCKS`] (anti-flake rule 8), and
//! every wait is a deadline-bounded poll derived from the harness's election timeout (rules
//! 1-3, 10). `restart` on Rocks reopens the **same** directory (TA-16); every row here follows
//! it with [`Cluster::wait_rejoined`], never trusting the instant `restart` returns (the M1
//! tester's notes call this out as the harness's second trap: a persistent node's applied
//! state reads correct before its Raft core has published anything).

mod support;

use std::collections::BTreeSet;

use config_core::{Durability, MutationOutcome, NodeId};
use config_storage::Boundary;
use config_testkit::cluster::{Cluster, NodeStartError, StorageKind};
use support::{field_u64, get_req, list_req, my_log_lines, put_req, rocks_cluster_with_scripts};

/// Put `n` distinct keys (`/m2/k{0..n}`) through `client`, waiting for nothing in between.
/// Returns the revision of the last write, which — since every write is unconditional and
/// state-changing — is also the number of writes (ADR-0005: one revision per applied
/// mutation).
async fn put_n(client: &dyn config_core::ConfigStore, n: u64, prefix: &str) -> u64 {
    let mut last = 0;
    for i in 0..n {
        let resp = client
            .put(put_req(&format!("{prefix}{i}"), "v"))
            .await
            .unwrap_or_else(|e| panic!("put {prefix}{i}: {e}"));
        assert_eq!(resp.outcome, MutationOutcome::Applied);
        last = resp.revision;
    }
    last
}

// =====================================================================================
// §3.1 Ordinary restart preserves every acknowledged mutation
// =====================================================================================

/// M2-01: a restarted follower comes back on the same applied state as the leader.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_01_restart_follower_preserves_state() {
    let cluster = Cluster::start(3, StorageKind::ROCKS).await;
    let leader = cluster.leader().await;
    put_n(&*cluster.client(leader), 20, "/m2/01/k").await;
    cluster
        .wait_revision_all(20, cluster.deadline(10))
        .await
        .expect("every node applies all 20 writes");
    let converged_before = cluster
        .wait_converged(cluster.deadline(10))
        .await
        .expect("cluster settles before anything is stopped");

    let follower = cluster.followers()[0];
    let before_applied = cluster.metrics(follower).last_applied.map(|l| l.index);

    cluster
        .restart(follower)
        .await
        .expect("a plain restart must succeed");
    cluster
        .wait_rejoined(follower, cluster.deadline(10))
        .await
        .expect("the restarted follower rejoins");
    cluster
        .wait_revision_all(20, cluster.deadline(10))
        .await
        .expect("the restarted follower catches back up to revision 20");

    let after_applied = cluster.metrics(follower).last_applied.map(|l| l.index);
    assert!(
        after_applied >= before_applied,
        "last_applied went backwards across a plain restart: before={before_applied:?} after={after_applied:?}"
    );
    assert_eq!(cluster.metrics(follower).cluster_revision, 20);
    let converged_after = cluster
        .wait_converged(cluster.deadline(10))
        .await
        .expect("cluster reconverges after the restart");
    assert_eq!(
        converged_after, converged_before,
        "the restarted follower's applied state differs from the pre-restart hash"
    );

    cluster.shutdown().await;
}

/// M2-02: a restarted leader steps down, a new leader is elected among the survivors, and once
/// the old leader rejoins it converges on the new leader's state.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_02_restart_leader_preserves_state() {
    let cluster = Cluster::start(3, StorageKind::ROCKS).await;
    let leader = cluster.leader().await;
    put_n(&*cluster.client(leader), 20, "/m2/02/k").await;
    cluster
        .wait_revision_all(20, cluster.deadline(10))
        .await
        .expect("every node applies all 20 writes");

    cluster
        .restart(leader)
        .await
        .expect("restarting the leader must succeed");

    let new_leader = cluster
        .wait_for_leader(cluster.deadline(10))
        .await
        .expect("the survivors elect a new leader");
    cluster
        .wait_rejoined(leader, cluster.deadline(10))
        .await
        .expect("the old leader rejoins as a follower");
    cluster
        .wait_revision_all(20, cluster.deadline(10))
        .await
        .expect("every node, including the rejoined old leader, is back at revision 20");

    let hash = cluster
        .wait_converged(cluster.deadline(10))
        .await
        .expect("all three nodes converge");
    assert_eq!(cluster.state_hash(new_leader), hash);
    for id in cluster.ids() {
        assert_eq!(
            cluster.metrics(id).cluster_revision,
            20,
            "node {id} lagging"
        );
    }

    cluster.shutdown().await;
}

/// M2-03: restarting each node in turn, one at a time, never loses or duplicates a revision.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_03_restart_each_node_in_turn() {
    let cluster = Cluster::start(3, StorageKind::ROCKS).await;
    let leader = cluster.leader().await;
    put_n(&*cluster.client(leader), 20, "/m2/03/k").await;
    cluster
        .wait_revision_all(20, cluster.deadline(10))
        .await
        .expect("every node applies all 20 writes");

    for i in 1..=3u64 {
        let id = NodeId(i);
        cluster
            .restart(id)
            .await
            .unwrap_or_else(|e| panic!("restarting node {id} in turn {i}: {e}"));
        cluster
            .wait_rejoined(id, cluster.deadline(10))
            .await
            .unwrap_or_else(|t| panic!("node {id} never rejoined: {t}"));
        cluster
            .wait_revision_all(20, cluster.deadline(10))
            .await
            .unwrap_or_else(|t| {
                panic!("cluster never got back to revision 20 after restarting {id}: {t}")
            });
    }

    let hash = cluster
        .wait_converged(cluster.deadline(10))
        .await
        .expect("all three converge after the full rotation");
    for id in cluster.ids() {
        assert_eq!(cluster.state_hash(id), hash, "node {id} diverged");
    }

    let dump = cluster
        .client(cluster.leader().await)
        .list(list_req(""))
        .await
        .expect("full dump after the rotation");
    let mut revisions: Vec<u64> = dump.records.iter().map(|r| r.mod_revision).collect();
    revisions.sort_unstable();
    assert_eq!(
        revisions,
        (1..=20).collect::<Vec<_>>(),
        "revisions were lost or reused across the restart rotation: {revisions:?}"
    );

    cluster.shutdown().await;
}

/// M2-04: revisions keep climbing strictly after every restart in M2-03's rotation, with no gap
/// and no reuse, and an untouched key's revisions never move.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_04_revision_monotonic_across_restart() {
    let cluster = Cluster::start(3, StorageKind::ROCKS).await;
    let leader = cluster.leader().await;
    put_n(&*cluster.client(leader), 20, "/m2/04/k").await;
    cluster
        .wait_revision_all(20, cluster.deadline(10))
        .await
        .expect("every node applies all 20 writes");

    let sentinel_before = cluster
        .client(leader)
        .get(get_req("/m2/04/k0"))
        .await
        .expect("read the sentinel before any restart")
        .record
        .expect("sentinel exists");

    let mut expected = 20u64;
    for i in 1..=3u64 {
        let id = NodeId(i);
        cluster
            .restart(id)
            .await
            .unwrap_or_else(|e| panic!("restarting {id}: {e}"));
        cluster
            .wait_rejoined(id, cluster.deadline(10))
            .await
            .unwrap_or_else(|t| panic!("{id} never rejoined: {t}"));

        expected += 1;
        let leader_now = cluster.leader().await;
        let resp = cluster
            .client(leader_now)
            .put(put_req(&format!("/m2/04/after{i}"), "v"))
            .await
            .unwrap_or_else(|e| panic!("put after restarting {id}: {e}"));
        assert_eq!(
            resp.revision, expected,
            "revision was not the next strictly-increasing one after restarting {id}"
        );
        cluster
            .wait_revision_all(expected, cluster.deadline(10))
            .await
            .unwrap_or_else(|t| panic!("cluster never caught up to revision {expected}: {t}"));
    }

    let sentinel_after = cluster
        .client(cluster.leader().await)
        .get(get_req("/m2/04/k0"))
        .await
        .expect("read the sentinel after the rotation")
        .record
        .expect("sentinel still exists");
    assert_eq!(
        sentinel_after.create_revision, sentinel_before.create_revision,
        "an untouched key's create_revision moved"
    );
    assert_eq!(
        sentinel_after.mod_revision, sentinel_before.mod_revision,
        "an untouched key's mod_revision moved"
    );

    cluster.shutdown().await;
}

/// M2-05: the strictest form of §21 M2 line 1 — a mutation whose `APPLIED` response the client
/// already holds survives every node stopping and starting again from disk.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_05_acknowledged_mutation_survives_immediate_restart() {
    let cluster = Cluster::start(3, StorageKind::ROCKS).await;
    let leader = cluster.leader().await;
    let written = cluster
        .client(leader)
        .put(put_req("/m2/05/k", "v1"))
        .await
        .expect("put before the cold restart");
    assert_eq!(written.outcome, MutationOutcome::Applied);

    cluster.stop_all().await;
    cluster
        .start_all()
        .await
        .expect("every directory reopens after stop_all");

    let new_leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader from persisted membership");
    for id in cluster.ids() {
        cluster
            .wait_rejoined(id, cluster.deadline(20))
            .await
            .unwrap_or_else(|t| panic!("node {id} never rejoined the cold-started cluster: {t}"));
    }

    let got = cluster
        .client(new_leader)
        .get(get_req("/m2/05/k"))
        .await
        .expect("read after the cold restart");
    let record = got.record.expect("the acknowledged mutation is gone");
    assert_eq!(record.value, config_core_value("v1"));
    assert_eq!(record.mod_revision, written.revision);
    assert_eq!(
        cluster.metrics(new_leader).cluster_revision,
        written.revision
    );

    cluster.shutdown().await;
}

fn config_core_value(s: &str) -> bytes::Bytes {
    bytes::Bytes::copy_from_slice(s.as_bytes())
}

/// M2-06: a cold cluster restart recovers a leader from persisted membership without ever
/// re-forming — checked both structurally (`membership_log_id` unchanged) and against the
/// real `"forming cluster"` log line node.rs emits exactly once, on the original formation.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_06_cold_cluster_restart_does_not_reform() {
    let cluster = Cluster::start(3, StorageKind::ROCKS).await;
    let leader = cluster.leader().await;
    cluster
        .client(leader)
        .put(put_req("/m2/06/k", "v1"))
        .await
        .expect("put before the cold restart");
    let membership_before = cluster.membership();

    cluster.stop_all().await;
    cluster.start_all().await.expect("cold start from disk");
    cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader without re-forming");
    for id in cluster.ids() {
        cluster
            .wait_rejoined(id, cluster.deadline(20))
            .await
            .unwrap_or_else(|t| panic!("node {id} never rejoined: {t}"));
    }

    let membership_after = cluster.membership();
    assert_eq!(
        membership_after.membership_log_id, membership_before.membership_log_id,
        "a new membership entry was committed: the cluster re-formed instead of recovering"
    );
    assert_eq!(membership_after.voters, membership_before.voters);

    // The real message node.rs logs exactly once, from `form_cluster` (node.rs:311-314): a
    // cold restart must never produce a second one.
    let rows = my_log_lines(module_path!(), "m2_06_cold_cluster_restart_does_not_reform");
    let formations = rows
        .iter()
        .filter(|r| r.get("@m").and_then(|v| v.as_str()) == Some("forming cluster"))
        .count();
    assert_eq!(
        formations, 1,
        "expected exactly one \"forming cluster\" line (the original formation); a cold \
         restart must never log a second one: {rows:#?}"
    );

    cluster.shutdown().await;
}

/// M2-07: the same directory survives ten restart cycles, one put between each, with no
/// `LOCK` failure — and the store really was reopened ten times, proved by the `"store_opened"`
/// line `rocks.rs::open_inner` logs on every open, filtered to this node.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_07_data_dir_reuse_ten_cycles() {
    let cluster = Cluster::start(3, StorageKind::ROCKS).await;
    let leader = cluster.leader().await;
    put_n(&*cluster.client(leader), 5, "/m2/07/seed").await;
    cluster
        .wait_revision_all(5, cluster.deadline(10))
        .await
        .expect("seed writes applied");

    let victim = NodeId(2);
    for cycle in 0..10u64 {
        cluster
            .restart(victim)
            .await
            .unwrap_or_else(|e| panic!("restart cycle {cycle}: {e}"));
        cluster
            .wait_rejoined(victim, cluster.deadline(10))
            .await
            .unwrap_or_else(|t| panic!("node {victim} never rejoined on cycle {cycle}: {t}"));
        let leader_now = cluster.leader().await;
        cluster
            .client(leader_now)
            .put(put_req(&format!("/m2/07/cycle{cycle}"), "v"))
            .await
            .unwrap_or_else(|e| panic!("put after cycle {cycle}: {e}"));
    }
    cluster
        .wait_revision_all(15, cluster.deadline(10))
        .await
        .expect("5 seed + 10 cycle writes all applied everywhere");

    let rows = my_log_lines(module_path!(), "m2_07_data_dir_reuse_ten_cycles");
    let opens_for_victim = rows
        .iter()
        .filter(|r| {
            r.get("@m").and_then(|v| v.as_str()) == Some("store_opened")
                && field_u64(r, "node_id") == Some(victim.0)
        })
        .count();
    // The very first open (cluster formation) plus the ten restart cycles.
    assert_eq!(
        opens_for_victim, 11,
        "expected 11 store_opened lines for node {victim} (1 initial + 10 restarts): {rows:#?}"
    );

    cluster.shutdown().await;
}

/// M2-08: a second `form_cluster` after a cold restart is refused as `AlreadyFormed`, with no
/// membership churn and no new log entry.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_08_second_formation_after_restart_rejected() {
    let cluster = Cluster::start(3, StorageKind::ROCKS).await;
    let leader = cluster.leader().await;
    cluster
        .client(leader)
        .put(put_req("/m2/08/k", "v1"))
        .await
        .expect("put before the cold restart");

    cluster.stop_all().await;
    cluster.start_all().await.expect("cold start");
    let new_leader = cluster
        .wait_for_leader(cluster.deadline(20))
        .await
        .expect("a leader without re-forming");
    for id in cluster.ids() {
        cluster
            .wait_rejoined(id, cluster.deadline(20))
            .await
            .unwrap_or_else(|t| panic!("node {id} never rejoined: {t}"));
    }

    let before_term = cluster.metrics(new_leader).current_term;
    let before_len = cluster.store(new_leader).raft_log_len();
    let plan = cluster.formation_plan();

    let result = cluster.node(new_leader).form_cluster(plan).await;
    assert!(
        matches!(result, Err(config_engine::FormationError::AlreadyFormed)),
        "a second formation on a restarted cluster was not refused as AlreadyFormed: {result:?}"
    );
    assert_eq!(
        cluster.metrics(new_leader).current_term,
        before_term,
        "term churned"
    );
    assert_eq!(
        cluster.store(new_leader).raft_log_len(),
        before_len,
        "a refused formation still appended a log entry"
    );

    cluster.shutdown().await;
}

/// M2-09: a node stopped for a while (quorum kept by the other two) catches all the writes it
/// missed once it comes back, and everyone converges.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_09_restart_with_stopped_peer() {
    let cluster = Cluster::start(3, StorageKind::ROCKS).await;
    let leader = cluster.leader().await;
    put_n(&*cluster.client(leader), 5, "/m2/09/a").await;
    cluster
        .wait_revision_all(5, cluster.deadline(10))
        .await
        .expect("first 5 writes applied");

    cluster.stop_node(NodeId(3)).await;

    let leader2 = cluster.leader().await;
    put_n(&*cluster.client(leader2), 5, "/m2/09/b").await;
    cluster
        .wait_revision_on(&[NodeId(1), NodeId(2)], 10, cluster.deadline(10))
        .await
        .expect("the surviving quorum applies the next 5 writes");

    cluster
        .restart(NodeId(1))
        .await
        .expect("restarting a node while the third is stopped");
    cluster
        .wait_rejoined(NodeId(1), cluster.deadline(10))
        .await
        .expect("node 1 rejoins");

    cluster.start_node(NodeId(3)).await;
    cluster
        .wait_rejoined(NodeId(3), cluster.deadline(20))
        .await
        .expect("node 3 catches up after being stopped for two rounds of writes");
    cluster
        .wait_revision_all(10, cluster.deadline(20))
        .await
        .expect("every node reaches revision 10");

    let hash = cluster
        .wait_converged(cluster.deadline(10))
        .await
        .expect("all three converge");
    for id in cluster.ids() {
        assert_eq!(cluster.state_hash(id), hash);
    }

    cluster.shutdown().await;
}

/// M2-10: the documented Rocks/Ephemeral divergence on restart, side by side in one table —
/// Rocks keeps its state, Ephemeral starts empty and is re-replicated (M1-43).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_10_ephemeral_vs_rocks_divergence_documented() {
    struct Row {
        name: &'static str,
        storage: StorageKind,
    }
    for row in [
        Row {
            name: "rocks",
            storage: StorageKind::ROCKS,
        },
        Row {
            name: "ephemeral",
            storage: StorageKind::Ephemeral,
        },
    ] {
        let cluster = Cluster::start(3, row.storage).await;
        let leader = cluster.leader().await;
        let written = cluster
            .client(leader)
            .put(put_req("/m2/10/k", "v1"))
            .await
            .unwrap_or_else(|e| panic!("[{}] put: {e}", row.name));
        cluster
            .wait_revision_all(written.revision, cluster.deadline(10))
            .await
            .unwrap_or_else(|t| panic!("[{}] initial convergence: {t}", row.name));

        let victim = cluster.followers()[0];
        let before_hash = cluster.state_hash(victim);
        cluster.stop_node(victim).await;
        cluster.start_node(victim).await;

        match row.storage {
            StorageKind::Rocks(_) => {
                cluster
                    .wait_rejoined(victim, cluster.deadline(10))
                    .await
                    .unwrap_or_else(|t| panic!("[rocks] victim never rejoined: {t}"));
                assert_eq!(
                    cluster.state_hash(victim),
                    before_hash,
                    "[rocks] a restarted node forgot its applied state"
                );
                assert_eq!(cluster.durability(victim), Durability::Persistent);
            }
            StorageKind::Ephemeral => {
                let converged = cluster
                    .wait_for(
                        "[ephemeral] the re-replicated node to match the others",
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
                    "[ephemeral] never re-replicated after a restart: {:?}",
                    converged.err()
                );
                assert_eq!(cluster.durability(victim), Durability::Ephemeral);
            }
        }

        cluster.shutdown().await;
    }
}

// =====================================================================================
// §3.2 Committed-but-unapplied replay
// =====================================================================================

/// M2-11: a leader whose own apply crashes right after its followers committed the entry
/// replays that entry on restart — no acknowledged mutation from before the crash is lost, and
/// the crashed entry itself lands exactly once.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_11_crash_after_commit_before_apply_replays() {
    let (cluster, scripts) = rocks_cluster_with_scripts(3).await;
    let leader = cluster.leader().await;
    put_n(&*cluster.client(leader), 5, "/m2/11/k").await;
    cluster
        .wait_revision_all(5, cluster.deadline(10))
        .await
        .expect("5 seed writes applied");

    let target = leader;
    scripts[&target].crash_on_nth(Boundary::BeforeStateBatch, 1);
    // The write may return an error, a stale-hint retry, or nothing — its outcome is
    // deliberately unobserved (ADR-0015: never treat a submission as safe to interpret past
    // the deadline). What matters is that it crashes the leader's own apply.
    let _ = cluster.client(target).put(put_req("/m2/11/k5", "v5")).await;

    let poisoned = cluster
        .wait_for(
            "the leader's store to poison from the injected apply crash",
            cluster.deadline(10),
            || cluster.store(target).is_poisoned().then_some(()),
        )
        .await;
    assert!(
        poisoned.is_ok(),
        "the leader's apply never crashed: {poisoned:?}"
    );
    // Anti-flake rule 11: prove the boundary was actually crossed before trusting anything
    // that follows from "it crashed there".
    let crossed = cluster
        .store(target)
        .counters()
        .get(Boundary::BeforeStateBatch);
    assert!(
        crossed >= 1,
        "BeforeStateBatch was never crossed on the leader"
    );

    cluster
        .restart(target)
        .await
        .expect("restarting the crashed leader");
    cluster
        .wait_rejoined(target, cluster.deadline(10))
        .await
        .expect("the crashed leader rejoins and replays");
    cluster
        .wait_revision_all(6, cluster.deadline(10))
        .await
        .expect("the whole cluster reaches revision 6 (5 seed + the replayed entry)");

    let hash = cluster
        .wait_converged(cluster.deadline(10))
        .await
        .expect("all three converge on the replayed state");
    for id in cluster.ids() {
        assert_eq!(
            cluster.state_hash(id),
            hash,
            "node {id} diverged after the replay"
        );
    }
    cluster.assert_crash_invariants(target);

    let got = cluster
        .client(cluster.leader().await)
        .get(get_req("/m2/11/k5"))
        .await
        .expect("read the replayed key");
    assert_eq!(
        got.record.map(|r| r.mod_revision),
        Some(6),
        "the replayed mutation did not land at revision 6"
    );

    cluster.shutdown().await;
}

/// M2-12: the replay in M2-11 allocates no duplicate revision — exactly six records, six
/// revisions, `cluster_revision == 6`. (`applied_commands` is deliberately **not** asserted at
/// "6": it counts applies since the store's current *open*, and a restart opens a fresh store
/// — see the M2 storage notes' "applied_commands is per open". The durable oracle across a
/// restart is `cluster_revision`, not that counter.)
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_12_replay_allocates_no_duplicate_revision() {
    let (cluster, scripts) = rocks_cluster_with_scripts(3).await;
    let leader = cluster.leader().await;
    put_n(&*cluster.client(leader), 5, "/m2/12/k").await;
    cluster
        .wait_revision_all(5, cluster.deadline(10))
        .await
        .expect("5 seed writes applied");

    let target = leader;
    scripts[&target].crash_on_nth(Boundary::BeforeStateBatch, 1);
    let _ = cluster.client(target).put(put_req("/m2/12/k5", "v5")).await;
    cluster
        .wait_for("the leader's store to poison", cluster.deadline(10), || {
            cluster.store(target).is_poisoned().then_some(())
        })
        .await
        .expect("the injected crash never poisoned the store");

    cluster.restart(target).await.expect("restart after crash");
    cluster
        .wait_rejoined(target, cluster.deadline(10))
        .await
        .expect("rejoin after replay");
    cluster
        .wait_revision_all(6, cluster.deadline(10))
        .await
        .expect("cluster reaches revision 6");

    let dump = cluster
        .client(cluster.leader().await)
        .list(list_req(""))
        .await
        .expect("full dump after the replay");
    assert_eq!(
        dump.records.len(),
        6,
        "expected exactly 6 records: {dump:#?}"
    );
    let mut revisions: Vec<u64> = dump.records.iter().map(|r| r.mod_revision).collect();
    revisions.sort_unstable();
    assert_eq!(
        revisions,
        (1..=6).collect::<Vec<_>>(),
        "duplicate or missing revision"
    );
    for id in cluster.ids() {
        assert_eq!(
            cluster.metrics(id).cluster_revision,
            6,
            "node {id} reports a revision other than 6 (not 7 — the replay must not double-apply)"
        );
    }

    cluster.shutdown().await;
}

/// M2-13: `raft_meta/committed` is actually implemented (not defaulted to a no-op, which would
/// silently disable replay — research §8.2): after ordinary writes, it exists, and its index is
/// at least `last_applied`.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_13_committed_is_persisted() {
    let cluster = Cluster::start(3, StorageKind::ROCKS).await;
    let leader = cluster.leader().await;
    put_n(&*cluster.client(leader), 5, "/m2/13/k").await;
    cluster
        .wait_revision_all(5, cluster.deadline(10))
        .await
        .expect("5 writes applied");

    let victim = cluster.followers()[0];
    let last_applied = cluster
        .metrics(victim)
        .last_applied
        .map(|l| l.index)
        .expect("the follower has applied something");
    cluster.stop_node(victim).await;

    let inspect = cluster
        .reopen_store(victim)
        .expect("reopen the stopped follower's directory for inspection");
    let mut log = inspect.log_store();
    let committed = {
        use openraft::storage::RaftLogStorage;
        log.read_committed()
            .await
            .expect("read_committed must not error")
    };
    let committed = committed.expect("raft_meta/committed must exist after ordinary writes");
    assert!(
        committed.index >= last_applied,
        "committed index {} is behind last_applied {last_applied}",
        committed.index
    );
    drop(inspect);

    cluster.shutdown().await;
}

// M2-14/M2-15 (`read_committed_drives_replay_window`, `replay_chunked_over_64_entries`) are
// store-level rows against a directly constructed `RocksStore` with `committed` pre-set ahead
// of `last_applied` (test plan §3.2, file mapping `crates/config-storage/tests/m2_store_*.rs`)
// — not this harness's `Cluster`. Reproducing a multi-entry replay window through
// `Cluster`/`ScriptedInjector` does not work: `FaultAction::Crash` poisons the **whole** store
// (TA-14), so the crashed node's log stops growing at the very entry that triggered it — the
// window this harness can produce is always exactly 1 (which is what M2-11/M2-12 exercise),
// never a pre-seeded multi-entry backlog. That row belongs in `config-storage`'s own test
// suite, out of this crate's ownership; left unimplemented here rather than watered down.

/// M2-16: one put crosses `BeforeStateBatch`/`AfterStateBatch` exactly once each — the apply
/// really is one atomic crossing, proven by counting rather than by reading the source.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_16_apply_batch_is_one_atomic_crossing() {
    let cluster = Cluster::start(3, StorageKind::ROCKS).await;
    let leader = cluster.leader().await;
    cluster
        .client(leader)
        .put(put_req("/m2/16/warm", "v"))
        .await
        .expect("a warm-up write so formation's own applies are not counted");
    cluster
        .wait_revision_all(1, cluster.deadline(10))
        .await
        .expect("warm-up applied");

    let before = cluster.counters(leader).snapshot();
    cluster
        .client(leader)
        .put(put_req("/m2/16/k", "v"))
        .await
        .expect("the put under test");
    cluster
        .wait_revision_all(2, cluster.deadline(10))
        .await
        .expect("the put under test applied everywhere");
    let after = cluster.counters(leader).snapshot();

    for boundary in [Boundary::BeforeStateBatch, Boundary::AfterStateBatch] {
        let delta = after[&boundary] - before[&boundary];
        assert_eq!(
            delta, 1,
            "{boundary} crossed {delta} times for one put (expected exactly 1): before={before:?} after={after:?}"
        );
    }

    cluster.shutdown().await;
}

/// M2-17: if the replay itself crashes, the second restart still completes it — the replay path
/// is as idempotent as the ordinary apply path.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_17_crash_during_replay_is_idempotent() {
    let (cluster, scripts) = rocks_cluster_with_scripts(3).await;
    let leader = cluster.leader().await;
    put_n(&*cluster.client(leader), 5, "/m2/17/k").await;
    cluster
        .wait_revision_all(5, cluster.deadline(10))
        .await
        .expect("5 seed writes applied");

    let target = leader;
    scripts[&target].crash_on_nth(Boundary::BeforeStateBatch, 1);
    let _ = cluster.client(target).put(put_req("/m2/17/k5", "v5")).await;
    cluster
        .wait_for(
            "the first crash to poison the store",
            cluster.deadline(10),
            || cluster.store(target).is_poisoned().then_some(()),
        )
        .await
        .expect("the first injected crash never poisoned the store");

    // Arm again *before* the first restart: the fresh store's boundary counters start at zero,
    // so "crash on the 1st BeforeStateBatch crossing" fires on the replay's own first apply.
    //
    // The store opens fine (identity matches) but the engine's replay crashes on its own first
    // apply, so `try_start_node` never reaches the point where it records a running slot: the
    // harness only attaches `slot.running` (and therefore `cluster.store`/`cluster.counters`)
    // once startup fully succeeds. A replay crash is thus observable only as `restart`'s `Err`,
    // not as a poll on a store handle that was never retained.
    scripts[&target].crash_on_nth(Boundary::BeforeStateBatch, 1);
    let replay_restart = cluster.restart(target).await;
    assert!(
        matches!(replay_restart, Err(NodeStartError::Engine(_))),
        "expected the replay's own crash to surface as NodeStartError::Engine, not a panic or a \
         quiet success: {replay_restart:?}"
    );

    // No third arm: this restart's replay must complete.
    cluster.restart(target).await.expect("the second restart");
    cluster
        .wait_rejoined(target, cluster.deadline(10))
        .await
        .expect("the node rejoins once the replay finally completes");
    cluster
        .wait_revision_all(6, cluster.deadline(10))
        .await
        .expect("cluster reaches revision 6 after the replay finally lands");

    let dump = cluster
        .client(cluster.leader().await)
        .list(list_req(""))
        .await
        .expect("full dump");
    assert_eq!(dump.records.len(), 6, "still exactly 6 records: {dump:#?}");
    let hash = cluster
        .wait_converged(cluster.deadline(10))
        .await
        .expect("convergence after the twice-crashed replay");
    for id in cluster.ids() {
        assert_eq!(cluster.state_hash(id), hash);
    }

    cluster.shutdown().await;
}

/// M2-18: a follower whose apply crashes converges by replay once restarted, and the leader
/// never noticed — it kept committing on the surviving quorum, and its term never churned.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_18_follower_committed_unapplied_replay() {
    let (cluster, scripts) = rocks_cluster_with_scripts(3).await;
    let leader = cluster.leader().await;
    let target = cluster.followers()[0];
    let leader_term_before = cluster.metrics(leader).current_term;

    scripts[&target].crash_on_nth(Boundary::BeforeStateBatch, 1);
    put_n(&*cluster.client(leader), 3, "/m2/18/k").await;

    cluster
        .wait_for(
            "the follower's store to poison",
            cluster.deadline(10),
            || cluster.store(target).is_poisoned().then_some(()),
        )
        .await
        .expect("the follower's apply never crashed");
    // The leader kept committing on the other follower's quorum the whole time.
    cluster
        .wait_revision_on(
            &cluster
                .ids()
                .into_iter()
                .filter(|id| *id != target)
                .collect::<Vec<_>>(),
            3,
            cluster.deadline(10),
        )
        .await
        .expect("the surviving quorum reached revision 3 without the crashed follower");
    assert_eq!(
        cluster.metrics(leader).current_term,
        leader_term_before,
        "the leader's term churned even though only a follower died"
    );

    cluster
        .restart(target)
        .await
        .expect("restart the crashed follower");
    cluster
        .wait_rejoined(target, cluster.deadline(20))
        .await
        .expect("the follower rejoins and catches up (replay plus ordinary replication)");
    cluster
        .wait_revision_all(3, cluster.deadline(20))
        .await
        .expect("every node, including the restarted follower, reaches revision 3");

    let hash = cluster
        .wait_converged(cluster.deadline(10))
        .await
        .expect("all three converge");
    for id in cluster.ids() {
        assert_eq!(cluster.state_hash(id), hash, "node {id} diverged");
    }
    assert_eq!(
        cluster.metrics(leader).current_term,
        leader_term_before,
        "the leader's term churned across the whole episode"
    );

    cluster.shutdown().await;
}
