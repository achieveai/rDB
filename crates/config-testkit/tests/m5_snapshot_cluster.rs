//! M5 snapshot publication rows driven against a real `config_testkit::Cluster` (test plan
//! §3.1-3.2; ADR-0022).
//!
//! `crates/config-storage/tests/m5_snapshot.rs` already proves the publication/crash-injection
//! properties exhaustively at the **store** level, against a bare `RocksStore` opened directly
//! (no Raft, no cluster) — that file is DO-NOT-EDIT and is the idiom reference this one follows
//! (`s.counters().get(Boundary::X)`, `s.metrics()` / [`StorageMetrics`]). This file proves the
//! handful of rows that specifically need a *live cluster* around the store — a real leader
//! driving real applies, a real restart through [`Cluster::restart`], a real read of what
//! survives — rather than duplicating what the store-level suite already covers node-locally.
//!
//! # How a build is driven, and when the policy is used instead
//!
//! Most rows here drive a build with `ConfigNode::trigger_snapshot`, which calls
//! `raft.trigger().snapshot()` directly and does not consult `SnapshotPolicy` at all. That is
//! deliberate: a trigger is deterministic where write volume is not, and every row that only
//! needs *a build* prefers it.
//!
//! [`config_testkit::cluster::ClusterConfig`] does now carry a `snapshot: SnapshotConfig`
//! (defaulting to `SnapshotConfig::DISABLED`, so no existing caller changed behaviour), set
//! through `ClusterBuilder::snapshot`. M5-17 is the row that needs it: **purge** is policy-gated
//! — `max_in_snapshot_log_to_keep`/`purge_batch_size` both come from `self.snapshot` in
//! `NodeConfig::openraft_config` — and a manual trigger can never produce the purged store that
//! row starts from.
//!
//! # The pause hook the paused-build rows use
//!
//! M5-01, M5-02 and M5-14 need a build held *open* while the test drives the cluster, which
//! crash/fail/delay injection cannot express. `support::ScriptedInjector` therefore gained
//! `pause_on_nth(Boundary, n) -> Arc<PauseHandle>`: the crossing task signals `reached` and then
//! blocks until the test calls `release`. It is the same `sync_channel(1)` handshake
//! `crates/config-storage/tests/m5_snapshot.rs`'s `PauseAt` fixture uses, and it is safe for the
//! same reason — `RocksShared::run` consults boundaries inside `spawn_blocking`, so the parked
//! thread is a blocking-pool thread and never a runtime worker.
//!
//! # What is not here
//!
//! M5-19/M5-20's live-cluster purge shape stays where it already has full coverage
//! (`crates/config-storage/tests/m5_snapshot.rs::m5_19_purge_actually_happens`); M5-06..M5-09,
//! M5-12, M5-13, M5-15, M5-21..M5-25 are likewise already covered at the store level (confirmed
//! by grepping that file's function names), and M5-03/M5-04 partially so, as the plan itself
//! acknowledges. M5-28 (`AlreadyInProgress`) appears below as a side effect of M5-05's own
//! assertions rather than as a separate row.

mod support;

use std::sync::Arc;

use config_core::NodeId;
use config_engine::admin::SnapshotTriggered;
use config_storage::Boundary;
use config_testkit::cluster::{Cluster, StorageKind};

use support::{put_req, ScriptedInjector};

/// A formed 3-node Rocks cluster with a disarmed [`ScriptedInjector`] per node, seeded with a
/// handful of keys so a build has something to capture.
async fn seeded_cluster() -> (
    Arc<Cluster>,
    std::collections::BTreeMap<NodeId, Arc<ScriptedInjector>>,
) {
    let mut builder = Cluster::builder().nodes(3).storage(StorageKind::ROCKS);
    let mut scripts = std::collections::BTreeMap::new();
    for i in 1..=3u64 {
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
        .wait_formed(cluster.deadline(10))
        .await
        .unwrap_or_else(|t| panic!("{t}"));
    let leader = cluster.leader().await;
    for i in 0..20 {
        cluster
            .client(leader)
            .put(put_req(&format!("/m5/snap/{i}"), "v"))
            .await
            .unwrap_or_else(|e| panic!("seed put {i}: {e}"));
    }
    (cluster, scripts)
}

// -------------------------------------------------------------------------------------------
// M5-05 — snapshot_id is unique per build, even with nothing written in between
// -------------------------------------------------------------------------------------------

#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_05_snapshot_id_unique_per_build_at_the_same_last_log_id() {
    let (cluster, _scripts) = seeded_cluster().await;
    let leader = cluster.leader().await;

    let first = cluster
        .node(leader)
        .trigger_snapshot()
        .await
        .expect("the first build must not be refused");
    let (first_id, first_log_id) = match first {
        SnapshotTriggered::Started {
            snapshot_id: Some(id),
            last_log_id: Some(log_id),
        } => (id, log_id),
        other => panic!("the first build on an idle cluster must publish something: {other:?}"),
    };

    // No writes at all between the two triggers, matching the row's setup exactly. Whatever
    // the second call reports, it must never claim the same snapshot_id as the first at that
    // same last_log_id (research §1.2).
    let second = cluster
        .node(leader)
        .trigger_snapshot()
        .await
        .expect("a second trigger is a legitimate admin call, not a protocol error");
    match second {
        SnapshotTriggered::Started {
            snapshot_id: Some(second_id),
            last_log_id: Some(second_log_id),
        } => {
            assert_eq!(
                second_log_id, first_log_id,
                "no writes happened between the two triggers, so both builds cover the same log id"
            );
            assert_ne!(
                second_id, first_id,
                "two builds must never share a snapshot_id, even at an unchanged last_log_id \
                 (research §1.2 — a derived-from-last_log_id id breaks in-flight equality)"
            );
        }
        SnapshotTriggered::AlreadyInProgress => {
            // Also admissible: the two calls raced against the same in-flight build.
        }
        SnapshotTriggered::Started { snapshot_id, .. } => {
            // Every other shape (no id published at all, or an id with no log id attached) is
            // openraft judging there was nothing newer to build and declining to publish again
            // — exactly the row's other admissible outcome ("refused as not newer"). Whatever
            // was reported, it must not coincide with the first build's id.
            if let Some(id) = snapshot_id {
                assert_ne!(id, first_id, "must not reuse the first build's snapshot_id");
            }
        }
    }
}

// -------------------------------------------------------------------------------------------
// M5-11 — a crash before the tmp-file sync leaves no publication at all
// -------------------------------------------------------------------------------------------

#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_11_crash_before_snapshot_tmp_sync_leaves_no_publication() {
    let (cluster, scripts) = seeded_cluster().await;
    let leader = cluster.leader().await;

    let previous = cluster.rocks_store(leader).snapshot_meta();
    assert!(
        previous.is_none(),
        "a freshly seeded node must not already have a published snapshot to confuse this row"
    );

    scripts[&leader].crash_on_nth(Boundary::BeforeSnapshotTmpSync, 1);
    let _ = cluster.node(leader).trigger_snapshot().await;

    cluster
        .wait_for(
            "the crash to poison the target's store",
            cluster.deadline(6),
            || {
                (cluster
                    .counters(leader)
                    .get(Boundary::BeforeSnapshotTmpSync)
                    >= 1)
                    .then_some(())
            },
        )
        .await
        .unwrap_or_else(|t| panic!("{t}"));
    cluster.reopen_store(leader).ok();
    cluster
        .restart(leader)
        .await
        .unwrap_or_else(|e| panic!("restart {leader}: {e}"));

    // No new .snap file, and no `.tmp` left over either (M5-11's own words: "any .tmp left
    // behind is removed on open").
    let after = cluster.rocks_store(leader).snapshot_meta();
    assert!(
        after.is_none(),
        "a crash before the tmp file is even synced must leave no publication: {after:?}"
    );
    let snap_dir = cluster.data_dir(leader).join("snapshots");
    if snap_dir.is_dir() {
        let leftovers: Vec<_> = std::fs::read_dir(&snap_dir)
            .into_iter()
            .flatten()
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|e| e == "tmp"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "no .tmp file must survive an open after this crash: {leftovers:?}"
        );
    }

    // The node rejoins and keeps serving — the crash was recoverable, not fatal to the cluster.
    let leader_after = cluster.leader().await;
    cluster
        .client(leader_after)
        .put(put_req("/m5/11/after", "v"))
        .await
        .expect("the cluster keeps serving after the crashed node recovers");
}

// -------------------------------------------------------------------------------------------
// M5-16 — a published snapshot survives a restart, readable from the durable record alone
// -------------------------------------------------------------------------------------------

#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_16_published_snapshot_survives_restart() {
    let (cluster, _scripts) = seeded_cluster().await;
    let leader = cluster.leader().await;

    let triggered = cluster
        .node(leader)
        .trigger_snapshot()
        .await
        .expect("triggering a build on an idle, unfaulted node must succeed");
    let (published_id, published_log_id) = match triggered {
        SnapshotTriggered::Started {
            snapshot_id: Some(id),
            last_log_id: Some(log_id),
        } => (id, log_id),
        other => panic!("expected a real publication on a clean node, got {other:?}"),
    };

    // Best-effort: the running node still holds RocksDB's LOCK, so this is expected to fail
    // and is only here in case a previous row's handle leaked. `restart` below is what actually
    // matters.
    cluster.reopen_store(leader).ok();
    cluster
        .restart(leader)
        .await
        .unwrap_or_else(|e| panic!("restart {leader}: {e}"));
    cluster
        .wait_for("the restarted node to rejoin", cluster.deadline(10), || {
            cluster
                .running_metrics()
                .iter()
                .any(|m| m.node_id == leader)
                .then_some(())
        })
        .await
        .unwrap_or_else(|t| panic!("{t}"));

    let after_restart = cluster
        .rocks_store(leader)
        .snapshot_meta()
        .expect("the durably recorded current_snapshot must still be there after a restart");
    assert_eq!(
        after_restart.snapshot_id, published_id,
        "restart must not have changed which snapshot is current"
    );
    assert_eq!(
        after_restart.last_log_id.map(|l| l.index),
        Some(published_log_id.index),
        "the recorded last_log_id must match what was published before the restart: {:?} vs {:?}",
        after_restart.last_log_id,
        published_log_id
    );
}

// -------------------------------------------------------------------------------------------
// M5-18 — SnapshotData is a file, not an in-memory cursor (source half; behavioural half
// deferred to M6's precise RSS measurement per the plan's own note)
// -------------------------------------------------------------------------------------------

#[test]
fn m5_18_snapshot_data_resolves_to_a_real_file() {
    let src = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../config-storage/src/lib.rs"
    ))
    .unwrap_or_default();
    let mut hit =
        src.contains("type SnapshotData = tokio::fs::File") || src.contains("SnapshotData = File");
    if !hit {
        // The associated type may be defined in a different module of the same crate; search
        // every source file rather than assuming lib.rs is where `TypeConfig` lives.
        let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../config-storage/src");
        for entry in walk_rs_files(std::path::Path::new(root)) {
            let text = std::fs::read_to_string(&entry).unwrap_or_default();
            if text.contains("SnapshotData") && text.contains("tokio::fs::File") {
                hit = true;
                break;
            }
        }
    }
    assert!(
        hit,
        "TypeConfig::SnapshotData must resolve to tokio::fs::File (A1 overrides D5.1; research \
         trap T4/U4) — grepped every crates/config-storage/src/*.rs for the association and \
         found none"
    );
}

// -------------------------------------------------------------------------------------------
// Fixtures for the paused-build rows (M5-01, M5-02, M5-14)
// -------------------------------------------------------------------------------------------

/// Node `id`'s published `.snap` file names, sorted.
fn snap_files(cluster: &Cluster, id: NodeId) -> Vec<String> {
    let dir = cluster.data_dir(id).join("snapshots");
    let mut out: Vec<String> = std::fs::read_dir(&dir)
        .map(|entries| {
            entries
                .flatten()
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| n.ends_with(".snap"))
                .collect()
        })
        .unwrap_or_default();
    out.sort();
    out
}

/// Every key byte-string the published snapshot `snapshot_id` actually carries, across every
/// exported column family.
///
/// Deliberately *all* families rather than only `kv`: a key written after the view was captured
/// must be absent from the journal export too, and a row that only scanned `kv` would pass on a
/// build that leaked newer events.
fn snapshot_keys(cluster: &Cluster, id: NodeId, snapshot_id: &str) -> Vec<Vec<u8>> {
    let path = cluster
        .data_dir(id)
        .join("snapshots")
        .join(format!("{snapshot_id}.snap"));
    let mut reader = config_storage::SnapshotReader::open(&path)
        .unwrap_or_else(|e| panic!("the published snapshot must be readable: {e}"));
    let mut keys = Vec::new();
    while let Some(record) = reader
        .next_record()
        .unwrap_or_else(|e| panic!("the published snapshot must decode: {e}"))
    {
        keys.push(record.key.to_vec());
    }
    keys
}

/// Whether any key in `keys` contains `needle`.
fn any_key_contains(keys: &[Vec<u8>], needle: &str) -> bool {
    keys.iter()
        .any(|k| k.windows(needle.len()).any(|w| w == needle.as_bytes()))
}

/// Trigger a build on `id` in the background, so the caller can drive the cluster while the
/// build sits on a paused boundary.
///
/// `trigger_snapshot` only returns once the build has published, so a paused build would
/// otherwise deadlock the test's own task.
fn trigger_in_background(
    cluster: &Arc<Cluster>,
    id: NodeId,
) -> tokio::task::JoinHandle<Result<SnapshotTriggered, config_engine::AdminError>> {
    let node = cluster.node(id);
    tokio::spawn(config_log::testing::in_current_span(async move {
        node.trigger_snapshot().await
    }))
}

/// The `snapshot_id` a completed background trigger published.
async fn published_id(
    handle: tokio::task::JoinHandle<Result<SnapshotTriggered, config_engine::AdminError>>,
) -> String {
    match handle
        .await
        .expect("the build task must not panic")
        .expect("the build must not be refused")
    {
        SnapshotTriggered::Started {
            snapshot_id: Some(id),
            ..
        } => id,
        other => panic!("the build must publish a snapshot id: {other:?}"),
    }
}

// -------------------------------------------------------------------------------------------
// M5-01 — the exported body is the view captured before the build, not live state
// -------------------------------------------------------------------------------------------

/// M5-01: writes that land *while a build is held open* appear neither in the published
/// snapshot's body nor in its header.
///
/// The pause sits on `BeforeSnapshotTmpSync`, which is inside `export_snapshot` and after the
/// records have been written, so the 40 puts below are applied strictly between the view
/// capture and the publication. The row then asserts the two halves of A2 that are observable
/// from outside the store: the header's `cluster_revision`/`last_applied` are the capture
/// instant, and the record stream contains exactly the pre-capture keys.
///
/// **Residual limitation, recorded rather than hidden.** This discriminates a build that reads
/// *live* state during the export (the corruption A2 exists to prevent: contents newer than
/// `meta.last_log_id`, silent follower corruption). It cannot discriminate a view captured at
/// the top of `build_snapshot` from one captured in `get_snapshot_builder`, because no
/// `Boundary` exists between those two points — every crossing the store offers is already
/// past the capture. Closing that would need a new product boundary; the exact patch is in
/// `.claude/scratchpad/conversation_memories/retcd-m4-m6-implementation/dev-harness-notes.md`.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_01_builder_view_captured_before_the_build() {
    let (cluster, scripts) = seeded_cluster().await;
    let leader = cluster.leader().await;

    for i in 0..30 {
        cluster
            .client(leader)
            .put(put_req(&format!("/m5/01/before/{i}"), "v"))
            .await
            .unwrap_or_else(|e| panic!("pre-capture put {i}: {e}"));
    }
    let at_capture = cluster.node(leader).metrics();

    let pause = scripts[&leader].pause_on_nth(Boundary::BeforeSnapshotTmpSync, 1);
    let build = trigger_in_background(&cluster, leader);
    pause.reached().await;
    // The capture happened somewhere between the two metric reads, so the header's own
    // `last_applied` must land in that closed interval. Pinning it to `at_capture` exactly
    // would be asserting that no blank or membership entry was applied in between, which is
    // the leader's business and not this row's claim.
    let at_pause = cluster.node(leader).metrics();

    // Applied strictly inside the build window.
    for i in 0..40 {
        cluster
            .client(leader)
            .put(put_req(&format!("/m5/01/after/{i}"), "v"))
            .await
            .unwrap_or_else(|e| panic!("in-flight put {i}: {e}"));
    }
    let during = cluster.node(leader).metrics();
    assert!(
        during.cluster_revision > at_capture.cluster_revision,
        "the row is vacuous unless the 40 puts really landed while the build was paused: \
         {} vs {}",
        during.cluster_revision,
        at_capture.cluster_revision
    );

    pause.release();
    let id = published_id(build).await;

    let path = cluster
        .data_dir(leader)
        .join("snapshots")
        .join(format!("{id}.snap"));
    let reader = config_storage::SnapshotReader::open(&path).expect("the published snapshot opens");
    let header = reader.header().clone();
    assert_eq!(
        header.cluster_revision, at_capture.cluster_revision,
        "the header must describe the capture instant, not the publication instant"
    );
    let header_index = header
        .last_applied
        .map(|l| l.index)
        .expect("a snapshot of a formed cluster covers at least one entry");
    let lower = at_capture.last_applied.map(|l| l.index).unwrap_or(0);
    let upper = at_pause.last_applied.map(|l| l.index).unwrap_or(0);
    assert!(
        (lower..=upper).contains(&header_index),
        "the header's last_applied must be the instant the view was captured, which lies \
         between the pre-trigger read ({lower}) and the paused read ({upper}); got \
         {header_index}"
    );
    assert!(
        header_index < during.last_applied.map(|l| l.index).unwrap_or(0),
        "the header must be older than the writes that landed while the build was paused: \
         {header_index} vs {:?}",
        during.last_applied
    );

    let keys = snapshot_keys(&cluster, leader, &id);
    assert!(
        any_key_contains(&keys, "/m5/01/before/29"),
        "every key applied before the capture must be in the export"
    );
    assert!(
        !any_key_contains(&keys, "/m5/01/after/"),
        "no key applied after the capture may appear anywhere in the export — a body newer \
         than its own header is silent follower corruption (A2, research trap T3)"
    );
}

// -------------------------------------------------------------------------------------------
// M5-02 — a running build does not block apply
// -------------------------------------------------------------------------------------------

/// M5-02: while a build is held open on a durability boundary, ordinary writes keep being
/// applied.
///
/// Asserts *progress*, never wall-clock latency (anti-flake rule 1): the claim under test is
/// that the builder does not share a mutex with `apply`, and the observable form of that is
/// `last_applied` and `applied_commands` advancing while the build cannot proceed. A
/// latency-based oracle would be measuring the test machine.
///
/// The build is confirmed to still be in flight *after* the writes, not merely before them:
/// otherwise a build that finished early would let the row pass without ever having overlapped
/// anything.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_02_apply_is_not_blocked_by_a_running_build() {
    let (cluster, scripts) = seeded_cluster().await;
    let leader = cluster.leader().await;

    let pause = scripts[&leader].pause_on_nth(Boundary::BeforeSnapshotTmpSync, 1);
    let build = trigger_in_background(&cluster, leader);
    pause.reached().await;

    let before = cluster.node(leader).metrics();
    for i in 0..20 {
        cluster
            .client(leader)
            .put(put_req(&format!("/m5/02/{i}"), "v"))
            .await
            .unwrap_or_else(|e| panic!("put {i} must be APPLIED while a build is open: {e}"));
    }
    let during = cluster.node(leader).metrics();

    assert!(
        !build.is_finished(),
        "the build must still be parked on its boundary, or the row proved nothing about \
         overlap"
    );
    assert!(
        during.applied_commands >= before.applied_commands + 20,
        "every one of the 20 writes must have been applied while the build was open: {} -> {}",
        before.applied_commands,
        during.applied_commands
    );
    assert!(
        during.last_applied.map(|l| l.index) > before.last_applied.map(|l| l.index),
        "last_applied must advance while a build is open: {:?} -> {:?}",
        before.last_applied,
        during.last_applied
    );

    pause.release();
    let _ = published_id(build).await;
}

// -------------------------------------------------------------------------------------------
// M5-14 — the previous snapshot stays current until the new one is published
// -------------------------------------------------------------------------------------------

/// M5-14: with build B parked immediately before its `current_snapshot` batch, A is still the
/// current snapshot and A's file is still on disk; only after B's batch commits does B take
/// over.
///
/// `BeforeCurrentSnapshotMeta` is the publication point in ADR-0022's ordering: B's `.snap` is
/// complete, renamed and directory-synced by then, so this is exactly the window in which a
/// store that published early — or pruned early — would be caught.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_14_old_snapshot_retained_until_publication() {
    let (cluster, scripts) = seeded_cluster().await;
    let leader = cluster.leader().await;

    let a = published_id(trigger_in_background(&cluster, leader)).await;
    assert_eq!(
        cluster
            .rocks_store(leader)
            .snapshot_meta()
            .map(|m| m.snapshot_id),
        Some(a.clone()),
        "A must be current before B starts"
    );

    // Something for B to cover that A does not, so B is a genuinely newer build.
    for i in 0..10 {
        cluster
            .client(leader)
            .put(put_req(&format!("/m5/14/{i}"), "v"))
            .await
            .unwrap_or_else(|e| panic!("put {i}: {e}"));
    }

    let pause = scripts[&leader].pause_on_nth(Boundary::BeforeCurrentSnapshotMeta, 1);
    let build_b = trigger_in_background(&cluster, leader);
    pause.reached().await;

    let current = cluster
        .rocks_store(leader)
        .snapshot_meta()
        .map(|m| m.snapshot_id);
    assert_eq!(
        current,
        Some(a.clone()),
        "while B is unpublished the current snapshot must still be A (spec §12.1)"
    );
    let files = snap_files(&cluster, leader);
    assert!(
        files.contains(&format!("{a}.snap")),
        "A's file must still exist while B is unpublished: {files:?}"
    );

    pause.release();
    let b = published_id(build_b).await;
    assert_ne!(b, a, "B is a different build");
    assert_eq!(
        cluster
            .rocks_store(leader)
            .snapshot_meta()
            .map(|m| m.snapshot_id),
        Some(b),
        "once B's meta batch commits, B is current"
    );
}

// -------------------------------------------------------------------------------------------
// M5-17 — a store that purged and then lost its snapshot rebuilds one on the startup path
// -------------------------------------------------------------------------------------------

/// The production snapshot shape shrunk so a few dozen writes cross both thresholds: build
/// once committed is 8 entries past the last snapshot, keep only 2 snapshot-covered entries.
///
/// Both knobs have to be finite together — [`SnapshotConfig::validate`] refuses the mismatched
/// pair — which is exactly why this row needs the policy and cannot fake it with
/// `trigger_snapshot`: a manual trigger builds, but only the policy *purges*, and it is the
/// purge pointer that makes the startup path rebuild.
const REPAIR_PROFILE: config_storage::SnapshotConfig = config_storage::SnapshotConfig {
    logs_since_last: 8,
    logs_to_keep: 2,
    purge_batch_size: 1,
    retain_snapshots: 2,
};

/// M5-17: when a store holds `last_purged = Some(..)` but no `current_snapshot`, OpenRaft's
/// `StorageHelper::get_initial_state` calls `get_snapshot_builder().build_snapshot()`
/// *synchronously*, inside `Raft::new` — so the node cannot answer anything until a snapshot
/// exists again (openraft 0.9.25 `storage/helper.rs`: "If there is not a snapshot and there
/// are logs purged ... we just rebuild it so that replication can use it"; research §4.3,
/// trap T3).
///
/// That is the whole point of A3's "replace every stub in the same change that enables purge":
/// with purge on and `build_snapshot` stubbed, this path is a start-up failure rather than a
/// repair.
///
/// # How the state is produced, and why it is produced this way
///
/// The plan's setup says "crash at `BeforeCurrentSnapshotMeta` after a purge had already
/// happened in an earlier cycle". That crash cannot actually produce the state the row names:
/// M5-14 proves the *previous* snapshot stays current until the new meta batch commits, so a
/// build interrupted there leaves `current_snapshot` populated, not absent. The state this row
/// is about is the one an operator reaches by losing snapshot files while the log stays purged
/// — a half-restored directory, a partially-synced volume — so it is produced directly: run
/// the real policy until it has genuinely built *and* purged, stop the node, delete
/// `state_meta/current_snapshot` and the `.snap` files, restart. Every byte the repair path
/// consumes is one the product wrote.
///
/// Single-node on purpose: with no peer, nothing but the startup path can publish or install a
/// snapshot, so "the snapshot present after the restart was built by the startup path" needs
/// no exclusion argument.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_17_startup_rebuilds_the_snapshot_a_purged_store_lost() {
    let cluster = Cluster::builder()
        .nodes(1)
        .storage(StorageKind::ROCKS)
        .snapshot(REPAIR_PROFILE)
        .start()
        .await;
    cluster
        .wait_formed(cluster.deadline(10))
        .await
        .unwrap_or_else(|t| panic!("{t}"));
    let node = NodeId(1);

    for i in 0..40 {
        cluster
            .client(node)
            .put(put_req(&format!("/m5/17/{i}"), "v"))
            .await
            .unwrap_or_else(|e| panic!("seed put {i}: {e}"));
    }
    cluster
        .wait_for(
            "the policy to both publish a snapshot and purge behind it",
            cluster.deadline(20),
            || {
                let m = cluster.rocks_store(node).metrics();
                (m.snapshot_publications > 0 && m.purges > 0).then_some(())
            },
        )
        .await
        .unwrap_or_else(|t| panic!("{t}"));
    let purged_index = cluster.rocks_store(node).metrics().purged_index;
    let applied_before = cluster.node(node).applied_index();

    // Stop first: RocksDB's LOCK is exclusive, and the tamper below needs a raw handle.
    cluster.stop_node(node).await;
    drop_the_published_snapshot(&cluster, node).await;
    assert!(
        snap_files(&cluster, node).is_empty(),
        "the row starts from a store with no snapshot file at all"
    );

    cluster.start_node(node).await;

    // No wait, deliberately: `start_node` returns after `Raft::new`, and the rebuild happens
    // inside it. A snapshot that only appeared after a poll would be a *later* build, which is
    // a different claim.
    let meta = cluster
        .rocks_store(node)
        .snapshot_meta()
        .expect("startup must republish a snapshot before the node can serve anything");
    assert!(
        meta.last_log_id.is_some_and(|id| id.index >= purged_index),
        "the rebuilt snapshot must cover at least everything the log no longer holds: \
         rebuilt at {:?}, purged through {purged_index}",
        meta.last_log_id
    );
    assert_eq!(
        snap_files(&cluster, node).len(),
        1,
        "the rebuild must leave exactly the one file it just wrote"
    );
    assert!(
        !snapshot_keys(&cluster, node, &meta.snapshot_id).is_empty(),
        "a rebuilt snapshot with no records would satisfy every pointer check and still be a stub"
    );

    // And the node really is serving on the repaired store, with the state it had before.
    assert_eq!(
        cluster.node(node).applied_index(),
        applied_before,
        "the repair must not replay or lose applied state"
    );
    cluster
        .client(node)
        .put(put_req("/m5/17/after", "v"))
        .await
        .expect("the repaired node accepts writes");
}

/// Remove every trace of the published snapshot while `dir`'s store is closed: the pointer in
/// `state_meta` and the files it named.
///
/// Deleting the pointer alone would leave the rebuilt file racing the old one for the same
/// name; deleting the files alone would leave a pointer to nothing, which is a *different*
/// failure (M5-18's). The row is about the pair being gone together.
///
/// The raw open is retried against a deadline rather than attempted once: `Cluster::stop_node`
/// awaits the node's shutdown, but the last clone of the store handle is dropped by the tasks
/// that shutdown releases, so RocksDB's exclusive `LOCK` outlives the `await` by however long
/// that takes. Polling the open *is* the "the store is really closed now" condition — there is
/// nothing else to observe — so this is a bounded wait on a real signal, not a sleep.
async fn drop_the_published_snapshot(cluster: &Cluster, id: NodeId) {
    let dir = cluster.data_dir(id);
    {
        let db = cluster
            .wait_for(
                "the stopped node to release RocksDB's exclusive LOCK",
                cluster.deadline(10),
                || {
                    rocksdb::DB::open_cf(
                        &rocksdb::Options::default(),
                        &dir,
                        config_storage::COLUMN_FAMILIES,
                    )
                    .ok()
                },
            )
            .await
            .unwrap_or_else(|t| panic!("{t}"));
        let state_meta = db.cf_handle("state_meta").expect("state_meta cf");
        assert!(
            db.get_cf(state_meta, b"current_snapshot")
                .expect("read the publication pointer")
                .is_some(),
            "the row's premise is a store that *had* published a snapshot"
        );
        assert!(
            db.cf_handle("raft_meta")
                .and_then(|cf| db
                    .get_cf(cf, b"last_purged")
                    .expect("read the purge pointer"))
                .is_some(),
            "the row's premise is a store that had also purged behind that snapshot"
        );
        db.delete_cf(state_meta, b"current_snapshot")
            .expect("drop the publication pointer");
    }
    for file in std::fs::read_dir(dir.join(config_storage::SNAPSHOT_DIR))
        .expect("the snapshot directory")
        .flatten()
    {
        std::fs::remove_file(file.path()).expect("drop a published snapshot file");
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
