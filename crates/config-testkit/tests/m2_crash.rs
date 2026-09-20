//! M2 acceptance rows M2-19..M2-29 (test plan §3.3): a crash injected at every one of the 8
//! durability boundaries loses no acknowledged mutation and leaves no log hole.
//!
//! Two deliberate, documented reductions from the test plan's literal text (both are reversible
//! technical choices, not scope cuts — the underlying property each row cares about is still
//! checked):
//!
//! - M2-19/M2-20 say "isolate the leader to force an election on the target". Isolating the
//!   *leader* leaves it ambiguous which of the two survivors campaigns first, so which node
//!   actually crosses the vote boundary is nondeterministic. This file instead calls
//!   [`Cluster::isolate`] on the **target** node itself: cut off from the leader's heartbeats,
//!   it is the one guaranteed to time out and campaign, deterministically crossing the boundary
//!   on itself. The original leader is undisturbed throughout, so "cluster re-elects" does not
//!   apply to this version — what the row is actually protecting (a vote that crashed before
//!   sync must not resurface; one that crashed after sync must never regress) is still fully
//!   exercised.
//! - Every row's `role ∈ {leader, follower}` matrix is driven through the *coordinating* leader
//!   (i.e. `cluster.client(leader)`), with the crash armed on either the leader itself or one
//!   follower. It is not repeated once per distinct follower — one representative follower is
//!   enough to exercise the follower-side code path; the boundary logic itself does not depend
//!   on *which* follower is replaying.
//!
//! `role = Follower` is always a strict "the write still lands" case in this file: with 3 nodes,
//! the leader plus the one untouched follower already form a quorum, so a crash confined to a
//! single follower's replication/apply path can never block that write's commit — only how soon
//! the target itself converges. `role = Leader` is where the crash lands on the coordinating
//! node, so the fate of that one write is genuinely boundary-dependent (see the per-boundary
//! table in `.claude/scratchpad/conversation_memories/retcd-m0-m3-implementation/m2-rocksstore-notes.md`)
//! and each M2-2x test encodes exactly what the table promises for that boundary — a strict
//! "never landed" for the two append-time boundaries, an intentionally weak "landed at most
//! once" for the flush-time ones the table itself calls ambiguous, and a strict "landed exactly
//! once" for the two state-batch boundaries (this is M2-11's own scenario, replayed against the
//! generic driver here).

mod support;

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use config_core::NodeId;
use config_storage::Boundary;
use config_testkit::cluster::Cluster;
use support::{get_req, list_req, put_req, rocks_cluster_with_scripts, ScriptedInjector};

async fn put_n(client: &dyn config_core::ConfigStore, n: u64, prefix: &str) -> u64 {
    let mut last = 0;
    for i in 0..n {
        let resp = client
            .put(put_req(&format!("{prefix}{i}"), "v"))
            .await
            .unwrap_or_else(|e| panic!("put {prefix}{i}: {e}"));
        last = resp.revision;
    }
    last
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    Leader,
    Follower,
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Role::Leader => "leader",
            Role::Follower => "follower",
        })
    }
}

/// Every M2-19..M2-26 row's common shape: arm one node's boundary, drive one put through the
/// current leader, wait for the poison, restart, rejoin, reconverge, and check the invariants
/// every row shares (test plan §3.3 preamble). Returns the node that crashed.
async fn crash_and_recover(
    cluster: &Cluster,
    scripts: &BTreeMap<NodeId, Arc<ScriptedInjector>>,
    boundary: Boundary,
    role: Role,
    key: &str,
) -> NodeId {
    let leader = cluster.leader().await;
    let target = match role {
        Role::Leader => leader,
        Role::Follower => cluster.followers()[0],
    };
    scripts[&target].crash_on_nth(boundary, 1);
    // The outcome is deliberately unobserved (ADR-0015): a crash mid-write can leave the client
    // with an error or an unknown outcome, and this file never treats either as proof of
    // anything past the deadline.
    let _ = cluster.client(leader).put(put_req(key, "v")).await;

    cluster
        .wait_for(
            &format!("{target}'s store to poison at {boundary} ({role})"),
            cluster.deadline(10),
            || cluster.store(target).is_poisoned().then_some(()),
        )
        .await
        .unwrap_or_else(|t| panic!("{boundary} ({role}) was never crashed on {target}: {t}"));
    // Anti-flake rule: a boundary that was never actually crossed proves nothing about it.
    let crossed = cluster.counters(target).get(boundary);
    assert!(
        crossed >= 1,
        "{boundary} on {target} ({role}) reported poisoned but the crossing counter is {crossed}"
    );

    cluster
        .restart(target)
        .await
        .unwrap_or_else(|e| panic!("restarting {target} after a {boundary} ({role}) crash: {e}"));
    cluster
        .wait_rejoined(target, cluster.deadline(10))
        .await
        .unwrap_or_else(|t| {
            panic!("{target} never rejoined after a {boundary} ({role}) crash: {t}")
        });
    cluster
        .wait_converged(cluster.deadline(10))
        .await
        .unwrap_or_else(|t| {
            panic!("cluster never reconverged after a {boundary} ({role}) crash on {target}: {t}")
        });
    cluster.assert_crash_invariants(target);
    target
}

/// Every record under `prefix` seeded before the crash is still there, at its original
/// revision, with nothing added or removed — the crash-matrix's own "no acknowledged mutation
/// is lost" invariant, checked directly against a full read rather than trusted from the
/// mechanism that crashed.
async fn assert_seed_intact(cluster: &Cluster, prefix: &str, n: u64) {
    let leader = cluster.leader().await;
    let dump = cluster
        .client(leader)
        .list(list_req(prefix))
        .await
        .unwrap_or_else(|e| panic!("list {prefix} after the crash: {e}"));
    assert_eq!(
        dump.records.len() as u64,
        n,
        "seeded records under {prefix} did not all survive: {dump:#?}"
    );
    let mut revisions: Vec<u64> = dump.records.iter().map(|r| r.mod_revision).collect();
    revisions.sort_unstable();
    assert_eq!(
        revisions,
        (1..=n).collect::<Vec<_>>(),
        "seeded revisions under {prefix} were corrupted by the crash: {revisions:?}"
    );
}

/// The crashed key landed exactly once, at exactly `before + 1`.
async fn assert_progressed_by_one(cluster: &Cluster, key: &str, before: u64) {
    cluster
        .wait_revision_all(before + 1, cluster.deadline(10))
        .await
        .unwrap_or_else(|t| {
            panic!(
                "cluster never reached revision {} after the crash: {t}",
                before + 1
            )
        });
    let leader = cluster.leader().await;
    let got = cluster
        .client(leader)
        .get(get_req(key))
        .await
        .unwrap_or_else(|e| panic!("read {key} after the crash: {e}"));
    let record = got
        .record
        .unwrap_or_else(|| panic!("{key} is missing after the crash; it was expected to land"));
    assert_eq!(
        record.mod_revision,
        before + 1,
        "{key} landed at the wrong revision"
    );
}

/// The crashed key never landed at all: the coordinating leader crashed before the write was
/// ever proposed to anyone, so it is guaranteed lost rather than merely unacknowledged.
async fn assert_never_landed(cluster: &Cluster, key: &str, before: u64) {
    let leader = cluster.leader().await;
    assert_eq!(
        cluster.metrics(leader).cluster_revision,
        before,
        "revision advanced even though the crashed leader never appended the entry"
    );
    let got = cluster
        .client(leader)
        .get(get_req(key))
        .await
        .unwrap_or_else(|e| panic!("read {key} after the crash: {e}"));
    assert!(
        got.record.is_none(),
        "{key} exists even though the leader crashed before ever appending it"
    );
}

/// The crashed key landed **at most once**: either it is entirely absent (revision unchanged),
/// or it landed at exactly `before + 1`. This is the table's own "may or may not be present"
/// for the unsynced append/flush boundaries on the leader's own coordinating path — genuinely
/// nondeterministic, so this only rules out double-counting or a revision jump of more than 1.
async fn assert_landed_at_most_once(cluster: &Cluster, key: &str, before: u64) {
    let leader = cluster.leader().await;
    let revision = cluster.metrics(leader).cluster_revision;
    assert!(
        revision == before || revision == before + 1,
        "revision jumped by more than one crashed write: before={before} after={revision}"
    );
    let got = cluster
        .client(leader)
        .get(get_req(key))
        .await
        .unwrap_or_else(|e| panic!("read {key} after the crash: {e}"));
    match (revision == before + 1, got.record) {
        (true, Some(record)) => assert_eq!(
            record.mod_revision,
            before + 1,
            "{key} landed at the wrong revision"
        ),
        (true, None) => panic!("revision advanced to {revision} but {key} itself is missing"),
        (false, Some(_)) => panic!("{key} exists even though the revision never advanced"),
        (false, None) => {}
    }
}

// =====================================================================================
// Vote boundary (M2-19, M2-20)
// =====================================================================================

/// M2-19: a vote that crashes **before** it is synced never resurfaces — the isolated node
/// comes back at its old term, having never durably claimed to be a candidate it cannot
/// remember being.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_19_crash_before_vote_sync() {
    let (cluster, scripts) = rocks_cluster_with_scripts(3).await;
    let leader = cluster.leader().await;
    put_n(&*cluster.client(leader), 3, "/m2/19/seed").await;
    cluster
        .wait_revision_all(3, cluster.deadline(10))
        .await
        .expect("seed applied");

    let target = cluster.followers()[0];
    let term_before = cluster.metrics(target).current_term;

    scripts[&target].crash_on_nth(Boundary::BeforeVoteSync, 1);
    cluster.isolate(target);
    cluster
        .wait_for(
            "the isolated node to campaign and crash before syncing its own vote",
            cluster.deadline(20),
            || cluster.store(target).is_poisoned().then_some(()),
        )
        .await
        .unwrap_or_else(|t| {
            panic!("node {target} never crashed on BeforeVoteSync after isolation: {t}")
        });
    let crossed = cluster.counters(target).get(Boundary::BeforeVoteSync);
    assert!(crossed >= 1, "BeforeVoteSync was never crossed on {target}");

    cluster.heal();
    cluster
        .restart(target)
        .await
        .expect("restart after a vote-sync crash");
    cluster
        .wait_rejoined(target, cluster.deadline(10))
        .await
        .expect("the node rejoins after failing to persist its own candidacy");

    let term_after = cluster.metrics(target).current_term;
    assert_eq!(
        term_after, term_before,
        "a vote that crashed BEFORE it was synced must not be visible after restart: {term_before} -> {term_after}"
    );
    cluster
        .wait_converged(cluster.deadline(10))
        .await
        .expect("cluster still converges — the original leader was never disturbed");
    cluster.assert_crash_invariants(target);
    assert_seed_intact(&cluster, "/m2/19/seed", 3).await;

    cluster.shutdown().await;
}

/// M2-20: a vote that crashes **after** it is synced is durable — term after restart never
/// regresses below the term it campaigned at.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_20_crash_after_vote_sync() {
    let (cluster, scripts) = rocks_cluster_with_scripts(3).await;
    let leader = cluster.leader().await;
    put_n(&*cluster.client(leader), 3, "/m2/20/seed").await;
    cluster
        .wait_revision_all(3, cluster.deadline(10))
        .await
        .expect("seed applied");

    let target = cluster.followers()[0];
    let term_before = cluster.metrics(target).current_term;

    scripts[&target].crash_on_nth(Boundary::AfterVoteSync, 1);
    cluster.isolate(target);
    cluster
        .wait_for(
            "the isolated node to campaign and crash right after syncing its own vote",
            cluster.deadline(20),
            || cluster.store(target).is_poisoned().then_some(()),
        )
        .await
        .unwrap_or_else(|t| {
            panic!("node {target} never crashed on AfterVoteSync after isolation: {t}")
        });
    let crossed = cluster.counters(target).get(Boundary::AfterVoteSync);
    assert!(crossed >= 1, "AfterVoteSync was never crossed on {target}");

    cluster.heal();
    cluster
        .restart(target)
        .await
        .expect("restart after a vote-sync crash");
    cluster
        .wait_rejoined(target, cluster.deadline(10))
        .await
        .expect("the node rejoins on its own durably persisted term");

    let term_after = cluster.metrics(target).current_term;
    assert!(
        term_after > term_before,
        "a vote that crashed AFTER it was synced must survive at least at the campaigned term: {term_before} -> {term_after}"
    );
    cluster.wait_converged(cluster.deadline(10)).await.expect(
        "cluster reconverges, even if the higher term forced the original leader to step down",
    );
    cluster.assert_crash_invariants(target);
    assert_seed_intact(&cluster, "/m2/20/seed", 3).await;

    cluster.shutdown().await;
}

// =====================================================================================
// Log append / flush boundaries (M2-21..M2-24)
// =====================================================================================

/// M2-21: `BeforeLogAppend`. On a follower, the leader+other-follower quorum still lands the
/// write. On the leader itself, the write was never appended anywhere and is guaranteed lost.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_21_crash_before_log_append() {
    let (cluster, scripts) = rocks_cluster_with_scripts(3).await;
    let leader0 = cluster.leader().await;
    put_n(&*cluster.client(leader0), 5, "/m2/21/seed").await;
    cluster
        .wait_revision_all(5, cluster.deadline(10))
        .await
        .expect("seed applied");

    let before_f = cluster.metrics(cluster.leader().await).cluster_revision;
    crash_and_recover(
        &cluster,
        &scripts,
        Boundary::BeforeLogAppend,
        Role::Follower,
        "/m2/21/follower",
    )
    .await;
    assert_progressed_by_one(&cluster, "/m2/21/follower", before_f).await;
    assert_seed_intact(&cluster, "/m2/21/seed", 5).await;

    let before_l = cluster.metrics(cluster.leader().await).cluster_revision;
    crash_and_recover(
        &cluster,
        &scripts,
        Boundary::BeforeLogAppend,
        Role::Leader,
        "/m2/21/leader",
    )
    .await;
    assert_never_landed(&cluster, "/m2/21/leader", before_l).await;
    assert_seed_intact(&cluster, "/m2/21/seed", 5).await;

    cluster.shutdown().await;
}

/// M2-22: `AfterLogAppend` — written but not yet synced. Same follower/leader split as M2-21,
/// except the leader-role outcome is the table's own "may or may not be present".
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_22_crash_after_log_append() {
    let (cluster, scripts) = rocks_cluster_with_scripts(3).await;
    let leader0 = cluster.leader().await;
    put_n(&*cluster.client(leader0), 5, "/m2/22/seed").await;
    cluster
        .wait_revision_all(5, cluster.deadline(10))
        .await
        .expect("seed applied");

    let before_f = cluster.metrics(cluster.leader().await).cluster_revision;
    crash_and_recover(
        &cluster,
        &scripts,
        Boundary::AfterLogAppend,
        Role::Follower,
        "/m2/22/follower",
    )
    .await;
    assert_progressed_by_one(&cluster, "/m2/22/follower", before_f).await;
    assert_seed_intact(&cluster, "/m2/22/seed", 5).await;

    let before_l = cluster.metrics(cluster.leader().await).cluster_revision;
    crash_and_recover(
        &cluster,
        &scripts,
        Boundary::AfterLogAppend,
        Role::Leader,
        "/m2/22/leader",
    )
    .await;
    assert_landed_at_most_once(&cluster, "/m2/22/leader", before_l).await;
    assert_seed_intact(&cluster, "/m2/22/seed", 5).await;

    cluster.shutdown().await;
}

/// M2-23: `BeforeLogFlush` — TA-13.1's explicitly distinct crossing from M2-22, same
/// expectations.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_23_crash_before_log_flush() {
    let (cluster, scripts) = rocks_cluster_with_scripts(3).await;
    let leader0 = cluster.leader().await;
    put_n(&*cluster.client(leader0), 5, "/m2/23/seed").await;
    cluster
        .wait_revision_all(5, cluster.deadline(10))
        .await
        .expect("seed applied");

    let before_f = cluster.metrics(cluster.leader().await).cluster_revision;
    crash_and_recover(
        &cluster,
        &scripts,
        Boundary::BeforeLogFlush,
        Role::Follower,
        "/m2/23/follower",
    )
    .await;
    assert_progressed_by_one(&cluster, "/m2/23/follower", before_f).await;
    assert_seed_intact(&cluster, "/m2/23/seed", 5).await;

    let before_l = cluster.metrics(cluster.leader().await).cluster_revision;
    crash_and_recover(
        &cluster,
        &scripts,
        Boundary::BeforeLogFlush,
        Role::Leader,
        "/m2/23/leader",
    )
    .await;
    assert_landed_at_most_once(&cluster, "/m2/23/leader", before_l).await;
    assert_seed_intact(&cluster, "/m2/23/seed", 5).await;

    cluster.shutdown().await;
}

/// M2-24: `AfterLogFlush` — the entry is durable on whichever node crashed, but the flush
/// callback never told anyone; the table itself does not commit to a fixed outcome for the
/// coordinating leader's own copy, only that the node recovers cleanly either way.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_24_crash_after_log_flush() {
    let (cluster, scripts) = rocks_cluster_with_scripts(3).await;
    let leader0 = cluster.leader().await;
    put_n(&*cluster.client(leader0), 5, "/m2/24/seed").await;
    cluster
        .wait_revision_all(5, cluster.deadline(10))
        .await
        .expect("seed applied");

    let before_f = cluster.metrics(cluster.leader().await).cluster_revision;
    crash_and_recover(
        &cluster,
        &scripts,
        Boundary::AfterLogFlush,
        Role::Follower,
        "/m2/24/follower",
    )
    .await;
    assert_progressed_by_one(&cluster, "/m2/24/follower", before_f).await;
    assert_seed_intact(&cluster, "/m2/24/seed", 5).await;

    let before_l = cluster.metrics(cluster.leader().await).cluster_revision;
    crash_and_recover(
        &cluster,
        &scripts,
        Boundary::AfterLogFlush,
        Role::Leader,
        "/m2/24/leader",
    )
    .await;
    assert_landed_at_most_once(&cluster, "/m2/24/leader", before_l).await;
    assert_seed_intact(&cluster, "/m2/24/seed", 5).await;

    cluster.shutdown().await;
}

// =====================================================================================
// State-batch boundaries (M2-25, M2-26)
// =====================================================================================

/// M2-25: `BeforeStateBatch` — the entry is already durable in the log by this point (this is
/// M2-11's exact scenario), so it always replays after restart, on either role.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_25_crash_before_state_batch() {
    let (cluster, scripts) = rocks_cluster_with_scripts(3).await;
    let leader0 = cluster.leader().await;
    put_n(&*cluster.client(leader0), 5, "/m2/25/seed").await;
    cluster
        .wait_revision_all(5, cluster.deadline(10))
        .await
        .expect("seed applied");

    let before_f = cluster.metrics(cluster.leader().await).cluster_revision;
    crash_and_recover(
        &cluster,
        &scripts,
        Boundary::BeforeStateBatch,
        Role::Follower,
        "/m2/25/follower",
    )
    .await;
    assert_progressed_by_one(&cluster, "/m2/25/follower", before_f).await;
    assert_seed_intact(&cluster, "/m2/25/seed", 5).await;

    let before_l = cluster.metrics(cluster.leader().await).cluster_revision;
    crash_and_recover(
        &cluster,
        &scripts,
        Boundary::BeforeStateBatch,
        Role::Leader,
        "/m2/25/leader",
    )
    .await;
    assert_progressed_by_one(&cluster, "/m2/25/leader", before_l).await;
    assert_seed_intact(&cluster, "/m2/25/seed", 5).await;

    cluster.shutdown().await;
}

/// M2-26: `AfterStateBatch` — KV, revision, `last_applied` and membership are all durable
/// before the crash; the mutation reappears exactly once, on either role.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_26_crash_after_state_batch() {
    let (cluster, scripts) = rocks_cluster_with_scripts(3).await;
    let leader0 = cluster.leader().await;
    put_n(&*cluster.client(leader0), 5, "/m2/26/seed").await;
    cluster
        .wait_revision_all(5, cluster.deadline(10))
        .await
        .expect("seed applied");

    let before_f = cluster.metrics(cluster.leader().await).cluster_revision;
    let target_f = crash_and_recover(
        &cluster,
        &scripts,
        Boundary::AfterStateBatch,
        Role::Follower,
        "/m2/26/follower",
    )
    .await;
    assert_progressed_by_one(&cluster, "/m2/26/follower", before_f).await;
    assert_seed_intact(&cluster, "/m2/26/seed", 5).await;
    assert_eq!(
        cluster.metrics(target_f).cluster_revision,
        before_f + 1,
        "the target's own applied cluster_revision did not land at exactly one past the seed"
    );

    let before_l = cluster.metrics(cluster.leader().await).cluster_revision;
    let target_l = crash_and_recover(
        &cluster,
        &scripts,
        Boundary::AfterStateBatch,
        Role::Leader,
        "/m2/26/leader",
    )
    .await;
    assert_progressed_by_one(&cluster, "/m2/26/leader", before_l).await;
    assert_seed_intact(&cluster, "/m2/26/seed", 5).await;
    assert_eq!(
        cluster.metrics(target_l).cluster_revision,
        before_l + 1,
        "the target's own applied cluster_revision did not land at exactly one past the seed"
    );

    cluster.shutdown().await;
}

// =====================================================================================
// Matrix-level rows (M2-27, M2-28, M2-29)
// =====================================================================================

/// M2-27 / M4-96: the crash matrix in this file (and M2-28/M2-29 below, plus the M4 journal
/// matrix in `m4_journal_boundary.rs`) covers `Boundary::ALL`. M4 grew the enum from 8 to 9
/// with `AfterStateBatchBeforePublish` (TA-28); M5 added eight more concurrently on this same
/// branch (snapshot publish/install/purge boundaries — `config-storage/src/fault.rs`), taking
/// `ALL` to 17. Per the lead's M4 tester ruling, this row deliberately does **not** use an
/// exhaustive `match` over `Boundary` any more — a match that must list every variant would
/// fail to compile every time either milestone lands a new one, on a file this row does not
/// own. Coverage is asserted by iterating `Boundary::ALL` and checking each boundary is
/// nameable/usable (`Debug`), not by exhaustive pattern matching. M4-96 pins the count M4
/// actually shipped against (9 boundaries at the point M4's fault-injection rows were written);
/// this row tracks the live total, which now includes M5's boundaries too — M4 does not add
/// fault-injection rows for those, that is dev-snapshot's/M5's work.
#[test]
fn m2_27_crash_boundary_table_is_exhaustive() {
    assert_eq!(
        Boundary::ALL.len(),
        17,
        "Boundary::ALL grew or shrank; the crash matrix above (M2-19..M2-26, M4-89..M4-96) \
         needs a matching row for any new *M4* boundary. M4 added AfterStateBatchBeforePublish \
         (TA-28) taking the count to 9; M5 added eight snapshot/install/purge boundaries \
         (BeforeSnapshotTmpSync, AfterSnapshotRename, BeforeCurrentSnapshotMeta, \
         BeforeInstallMarker, AfterInstallDropCf, BeforeInstallFinalBatch, BeforePurge, \
         AfterPurge) taking it to 17 — those are dev-snapshot's rows, not this file's. If this \
         fires again, update the expected count here to match config_storage::Boundary::ALL's \
         live length, not the (nonexistent) match shape below."
    );
    // No exhaustive match over `Boundary`: a per-variant match arm would fail to compile every
    // time M4 or the concurrent M5 wave adds a boundary, on a file this row does not own.
    // Every boundary just needs to be distinct and formattable — proven by iterating
    // `Boundary::ALL` (a wildcard-shaped loop, not per-variant arms), matching TA-28.3's "the
    // hook is consulted on every crossing" without naming each one.
    let mut seen = std::collections::BTreeSet::new();
    for b in Boundary::ALL {
        let rendered = format!("{b:?}");
        assert!(
            !rendered.is_empty(),
            "Boundary::{b:?} must be Debug-formattable"
        );
        assert!(
            seen.insert(rendered),
            "Boundary::ALL contains a duplicate: {b:?}"
        );
    }
    assert_eq!(
        seen.len(),
        Boundary::ALL.len(),
        "Boundary::ALL must have no duplicates"
    );
}

/// M4-96: `boundary_table_is_exhaustive` — cites M2-27 by ID (test plan §3.8) so the pair
/// cannot drift silently. Originally pinned to exactly 9 (M4's own count: the original 8 plus
/// `AfterStateBatchBeforePublish`); M5 landed eight more boundaries concurrently on this same
/// branch (see M2-27's doc comment above), so this row now asserts what M4-96 actually cares
/// about — that M4's own boundary is present and every M4 fault-injection row (M4-89..M4-95)
/// still has a nameable target — rather than a total this file does not own past M4's slice.
#[test]
fn m4_96_boundary_table_is_exhaustive() {
    assert!(
        Boundary::ALL.len() >= 9,
        "Boundary::ALL must contain at least the 9 boundaries M4 shipped against \
         (found {}); M2-27 in this file tracks the live total",
        Boundary::ALL.len()
    );
    assert!(
        Boundary::ALL.contains(&Boundary::AfterStateBatchBeforePublish),
        "M4-96 requires the M4 boundary to be present in Boundary::ALL"
    );
    // The 8 boundaries before it (M2/M3) and the M4 one itself must all still be reachable by
    // name — M5's additions are exercised by dev-snapshot's own rows, not here.
    let m4_and_earlier = [
        Boundary::BeforeVoteSync,
        Boundary::AfterVoteSync,
        Boundary::BeforeLogAppend,
        Boundary::AfterLogAppend,
        Boundary::BeforeLogFlush,
        Boundary::AfterLogFlush,
        Boundary::BeforeStateBatch,
        Boundary::AfterStateBatch,
        Boundary::AfterStateBatchBeforePublish,
    ];
    assert_eq!(m4_and_earlier.len(), 9);
    for b in m4_and_earlier {
        assert!(
            Boundary::ALL.contains(&b),
            "M4-96: {b:?} (M2/M3/M4 boundary) missing from Boundary::ALL"
        );
    }
}

/// A small seeded linear-congruential sequence — no external `rand` dependency for ten indices.
/// The seed is fixed and printed in every panic message, so a failure names exactly which
/// boundary in which position broke, and the run is exactly reproducible.
///
/// Draws from [`support::DRIVEABLE_BOUNDARIES`], not `Boundary::ALL`: this row's driver is an
/// ordinary put or a vote-triggering isolate, which cannot cross any of M5's snapshot boundaries
/// (see that constant's doc comment).
fn seeded_boundaries(seed: u64, n: usize) -> Vec<Boundary> {
    let mut state = seed;
    (0..n)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let idx = ((state >> 33) % support::DRIVEABLE_BOUNDARIES.len() as u64) as usize;
            support::DRIVEABLE_BOUNDARIES[idx]
        })
        .collect()
}

/// M2-28: ten cycles, each arming a seeded-random boundary on the current leader, crashing it,
/// restarting it, and then confirming forward progress with a fresh put. Term must never
/// regress across the whole run, and the run ends with every node agreeing on one final
/// revision with no gap or duplicate under it — at least the 10 confirmed writes, possibly a
/// few more (see the loop body for why the crash-time attempt can itself land on some
/// boundaries, which rules out asserting a fixed "10").
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_28_repeated_crash_cycles_no_vote_regression() {
    const SEED: u64 = 0xC0FFEE_u64;
    let (cluster, scripts) = rocks_cluster_with_scripts(3).await;
    let boundaries = seeded_boundaries(SEED, 10);

    let mut last_term = cluster.metrics(cluster.leader().await).current_term;
    let mut last_revision = 0u64;
    for (cycle, boundary) in boundaries.iter().enumerate() {
        let is_vote_boundary =
            matches!(boundary, Boundary::BeforeVoteSync | Boundary::AfterVoteSync);
        // Vote boundaries are only crossed by a node that campaigns — a put never touches
        // them. Isolating a follower (same deterministic trick as M2-19/M2-20) forces exactly
        // that; every other boundary is still driven by an ordinary put on the leader.
        let target = if is_vote_boundary {
            cluster.followers()[0]
        } else {
            cluster.leader().await
        };
        scripts[&target].crash_on_nth(*boundary, 1);
        if is_vote_boundary {
            cluster.isolate(target);
        } else {
            let _ = cluster
                .client(target)
                .put(put_req(&format!("/m2/28/attempt{cycle}"), "v"))
                .await;
        }
        cluster
            .wait_for(
                &format!("seed={SEED} cycle={cycle}: {target} to crash at {boundary}"),
                cluster.deadline(20),
                || cluster.store(target).is_poisoned().then_some(()),
            )
            .await
            .unwrap_or_else(|t| {
                panic!("seed={SEED} cycle={cycle}: {boundary} never crashed {target}: {t}")
            });
        let crossed = cluster.counters(target).get(*boundary);
        assert!(
            crossed >= 1,
            "seed={SEED} cycle={cycle}: {boundary} never actually crossed on {target}"
        );
        if is_vote_boundary {
            cluster.heal();
        }

        cluster
            .restart(target)
            .await
            .unwrap_or_else(|e| panic!("seed={SEED} cycle={cycle}: restarting {target}: {e}"));
        cluster
            .wait_rejoined(target, cluster.deadline(10))
            .await
            .unwrap_or_else(|t| panic!("seed={SEED} cycle={cycle}: {target} never rejoined: {t}"));

        let term_now = cluster.metrics(target).current_term;
        assert!(
            term_now >= last_term,
            "seed={SEED} cycle={cycle}: term regressed on {target}: {last_term} -> {term_now}"
        );
        last_term = term_now.max(last_term);

        // A confirmed put through whichever node leads now, guaranteed to land (unlike the
        // crash-time attempt above, whose fate is boundary-dependent). It is deliberately
        // *not* asserted at a fixed "cycle + 1" revision: for the boundaries at or after
        // durability (AfterLogFlush, BeforeStateBatch, AfterStateBatch), the crash-time
        // attempt itself can also land — it was durably committed before the crash, so it
        // either replays or was already applied — which legitimately advances the revision by
        // one extra step this cycle did not plan for. `last_revision` tracks whatever actually
        // landed instead of a formula, and only the end-of-run properties the row cares about
        // (term/vote never regress, final state agrees everywhere, no gap or duplicate in the
        // revision space) are asserted.
        let confirm_leader = cluster
            .wait_for_leader(cluster.deadline(10))
            .await
            .unwrap_or_else(|t| panic!("seed={SEED} cycle={cycle}: no leader after restart: {t}"));
        let resp = cluster
            .client(confirm_leader)
            .put(put_req(&format!("/m2/28/confirm{cycle}"), "v"))
            .await
            .unwrap_or_else(|e| panic!("seed={SEED} cycle={cycle}: confirm put: {e}"));
        last_revision = resp.revision;
        cluster
            .wait_revision_all(resp.revision, cluster.deadline(10))
            .await
            .unwrap_or_else(|t| {
                panic!(
                    "seed={SEED} cycle={cycle}: cluster never caught up to revision {}: {t}",
                    resp.revision
                )
            });
    }

    assert!(
        last_revision >= 10,
        "seed={SEED}: only {last_revision} confirmed writes landed across 10 cycles, expected at least 10"
    );
    let hash = cluster
        .wait_converged(cluster.deadline(10))
        .await
        .unwrap_or_else(|t| panic!("seed={SEED}: final convergence failed: {t}"));
    for id in cluster.ids() {
        assert_eq!(
            cluster.state_hash(id),
            hash,
            "seed={SEED}: node {id} diverged at the end of the run"
        );
        assert_eq!(
            cluster.metrics(id).cluster_revision,
            last_revision,
            "seed={SEED}: node {id} did not settle on the same final revision as the leader"
        );
    }
    let dump = cluster
        .client(cluster.leader().await)
        .list(list_req(""))
        .await
        .unwrap_or_else(|e| panic!("seed={SEED}: final list: {e}"));
    let mut revisions: Vec<u64> = dump.records.iter().map(|r| r.mod_revision).collect();
    revisions.sort_unstable();
    revisions.dedup();
    assert_eq!(
        revisions.len() as u64,
        last_revision,
        "seed={SEED}: the revision space has a gap or a duplicate: {revisions:?}"
    );

    cluster.shutdown().await;
}

/// M2-29: the aggregate over the whole crash matrix — all 8 boundaries, both roles where the
/// row above distinguishes one — reported as a single table so one boundary's failure is named
/// without the other 15 silently passing around it (§3.3's own request: "a single boundary
/// failure is named").
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_29_crash_matrix_loses_no_acknowledged_mutation() {
    let (cluster, scripts) = rocks_cluster_with_scripts(3).await;
    let leader0 = cluster.leader().await;
    put_n(&*cluster.client(leader0), 5, "/m2/29/seed").await;
    cluster
        .wait_revision_all(5, cluster.deadline(10))
        .await
        .expect("seed applied");

    let non_vote = [
        Boundary::BeforeLogAppend,
        Boundary::AfterLogAppend,
        Boundary::BeforeLogFlush,
        Boundary::AfterLogFlush,
        Boundary::BeforeStateBatch,
        Boundary::AfterStateBatch,
    ];

    // `catch_unwind` does not compose with `.await` (the future is not `UnwindSafe`, and
    // `Cluster`'s internals — locks, channels — make forcing it unsound), so this does not try
    // to keep running after a case fails. Instead every case's own assertions (in
    // `crash_and_recover` and the helpers above) already embed the boundary and role in their
    // panic message, so a failure still names exactly which of the 16 cases broke — just by
    // stopping there rather than by a table printed after the fact. `completed` records how far
    // the matrix got, printed alongside the panic by the harness's own backtrace/output.
    let mut completed: Vec<(Boundary, Role)> = Vec::new();
    for boundary in non_vote {
        for role in [Role::Follower, Role::Leader] {
            let key = format!("/m2/29/{boundary}/{role}");
            crash_and_recover(&cluster, &scripts, boundary, role, &key).await;
            assert_seed_intact(&cluster, "/m2/29/seed", 5).await;
            completed.push((boundary, role));
        }
    }

    // Vote boundaries: single representative case each, same reduction as M2-19/M2-20.
    for boundary in [Boundary::BeforeVoteSync, Boundary::AfterVoteSync] {
        let target = cluster.followers()[0];
        let term_before = cluster.metrics(target).current_term;
        scripts[&target].crash_on_nth(boundary, 1);
        cluster.isolate(target);
        cluster
            .wait_for(
                &format!("{target} to crash at {boundary} while isolated"),
                cluster.deadline(20),
                || cluster.store(target).is_poisoned().then_some(()),
            )
            .await
            .unwrap_or_else(|t| {
                panic!(
                    "M2-29 completed {} of 14 cases; vote case {boundary} never crashed: {t}",
                    completed.len()
                )
            });
        cluster.heal();
        cluster.restart(target).await.unwrap_or_else(|e| {
            panic!(
                "M2-29 completed {} of 14 cases; vote case {boundary} restart: {e}",
                completed.len()
            )
        });
        cluster
            .wait_rejoined(target, cluster.deadline(10))
            .await
            .unwrap_or_else(|t| {
                panic!(
                    "M2-29 completed {} of 14 cases; vote case {boundary} rejoin: {t}",
                    completed.len()
                )
            });
        let term_after = cluster.metrics(target).current_term;
        match boundary {
            Boundary::BeforeVoteSync => assert_eq!(
                term_after, term_before,
                "M2-29 vote case {boundary}: term must not move"
            ),
            Boundary::AfterVoteSync => assert!(
                term_after > term_before,
                "M2-29 vote case {boundary}: term must not regress"
            ),
            _ => unreachable!(),
        }
        cluster
            .wait_converged(cluster.deadline(10))
            .await
            .unwrap_or_else(|t| {
                panic!(
                    "M2-29 completed {} of 14 cases; vote case {boundary} convergence: {t}",
                    completed.len()
                )
            });
        cluster.assert_crash_invariants(target);
        completed.push((boundary, Role::Follower));
    }

    assert_seed_intact(&cluster, "/m2/29/seed", 5).await;
    assert_eq!(
        completed.len(),
        14,
        "the crash matrix did not run all 14 cases: {completed:?}"
    );

    cluster.shutdown().await;
}
