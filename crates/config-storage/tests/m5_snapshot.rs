//! `RocksStore` snapshot, install and purge tests (M5, ADR-0022, test plan M5-01..M5-48).
//!
//! These drive the store directly, without OpenRaft and without a cluster, because that is the
//! only way to crash *inside* a publish or an install: the eight boundaries M5 adds all sit in
//! code paths OpenRaft would otherwise enter and leave in one call.
//!
//! Every test owns a `TempDir`. On Windows a directory whose RocksDB is still open cannot be
//! removed, so every store handle is dropped before the directory is — that is what the
//! explicit scopes are for.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use config_core::{ClusterId, ClusterIdentity, Command, Limits, NodeId, RecoveryEpoch};
use config_log::retcd_test;
use config_storage::{
    Boundary, FaultAction, FaultInjector, NoFaults, RaftNode, RaftNodeId, RocksStore,
    SnapshotConfig, SnapshotHeader, SnapshotReader, TypeConfig, CF_EVENTS, CF_KV,
};
use openraft::storage::{RaftLogStorage, RaftLogStorageExt, RaftSnapshotBuilder, RaftStateMachine};
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId, SnapshotMeta};
use tokio::io::AsyncWriteExt;
use tracing::Span;

// --- fixtures ------------------------------------------------------------------------------

/// The eight boundaries M5 adds. Kept here rather than derived from `Boundary::ALL` so that a
/// boundary added to the enum and forgotten here shows up as a gap in this file's coverage row
/// rather than as silence.
const SNAPSHOT_BOUNDARIES: [Boundary; 8] = [
    Boundary::BeforeSnapshotTmpSync,
    Boundary::AfterSnapshotRename,
    Boundary::BeforeCurrentSnapshotMeta,
    Boundary::BeforeInstallMarker,
    Boundary::AfterInstallDropCf,
    Boundary::BeforeInstallFinalBatch,
    Boundary::BeforePurge,
    Boundary::AfterPurge,
];

fn identity_for(cluster: u8, node: u64) -> ClusterIdentity {
    ClusterIdentity {
        cluster_id: ClusterId::from_bytes([cluster; 16]),
        recovery_epoch: RecoveryEpoch(0),
        node_id: NodeId(node),
    }
}

fn open_as(dir: &Path, id: ClusterIdentity, faults: Arc<dyn FaultInjector>) -> RocksStore {
    RocksStore::open(dir, id, Limits::DEFAULT, faults, Span::none()).expect("store opens")
}

fn open_at(dir: &Path, faults: Arc<dyn FaultInjector>) -> RocksStore {
    open_as(dir, identity_for(7, 1), faults)
}

fn open_plain(dir: &Path) -> RocksStore {
    open_at(dir, Arc::new(NoFaults))
}

fn log_id(term: u64, index: u64) -> LogId<RaftNodeId> {
    LogId::new(CommittedLeaderId::new(term, 1), index)
}

fn put_cmd(key: &str, value: &str) -> Command {
    Command::Put {
        key: Bytes::copy_from_slice(key.as_bytes()),
        value: Bytes::copy_from_slice(value.as_bytes()),
        expected_mod_revision: None,
        dedup: None,
    }
}

fn put(term: u64, index: u64, key: &str, value: &str) -> Entry<TypeConfig> {
    Entry {
        log_id: log_id(term, index),
        payload: EntryPayload::Normal(put_cmd(key, value)),
    }
}

/// Append and apply `count` puts, so the store has both a log and applied state to snapshot.
async fn seed(s: &RocksStore, count: u64) {
    for i in 1..=count {
        let entry = put(1, i, &format!("/m5/{i}"), &format!("v{i}"));
        s.log_store()
            .blocking_append(vec![entry.clone()])
            .await
            .expect("append");
        s.state_machine().apply(vec![entry]).await.expect("apply");
    }
}

/// Build one snapshot and return its metadata.
async fn build(s: &RocksStore) -> Result<SnapshotMeta<RaftNodeId, RaftNode>, String> {
    let mut sm = s.state_machine();
    let mut builder = sm.get_snapshot_builder().await;
    builder
        .build_snapshot()
        .await
        .map(|snap| snap.meta)
        .map_err(|e| e.to_string())
}

fn snap_path(s: &RocksStore, meta: &SnapshotMeta<RaftNodeId, RaftNode>) -> std::path::PathBuf {
    s.path()
        .join("snapshots")
        .join(format!("{}.snap", meta.snapshot_id))
}

/// Ship `src`'s published snapshot into `dst` exactly as OpenRaft would: open a receive slot,
/// stream the bytes, then install. `mutate` gets to corrupt them in flight.
async fn transfer(
    src: &RocksStore,
    dst: &RocksStore,
    meta: &SnapshotMeta<RaftNodeId, RaftNode>,
    mutate: impl FnOnce(&mut Vec<u8>),
) -> Result<(), String> {
    let mut bytes = std::fs::read(snap_path(src, meta)).expect("published snapshot is readable");
    mutate(&mut bytes);
    let mut sm = dst.state_machine();
    let mut file = sm
        .begin_receiving_snapshot()
        .await
        .map_err(|e| e.to_string())?;
    file.write_all(&bytes).await.expect("write received bytes");
    sm.install_snapshot(meta, file)
        .await
        .map_err(|e| e.to_string())
}

/// Fires `action` on the `nth` crossing of `boundary`, counting from 1.
struct FailAt {
    boundary: Boundary,
    nth: u64,
    action: FaultAction,
    seen: AtomicU64,
}

impl FailAt {
    fn new(boundary: Boundary, action: FaultAction) -> Arc<Self> {
        Arc::new(Self {
            boundary,
            nth: 1,
            action,
            seen: AtomicU64::new(0),
        })
    }
}

impl FaultInjector for FailAt {
    fn before(&self, boundary: Boundary) -> FaultAction {
        if boundary != self.boundary {
            return FaultAction::Proceed;
        }
        let n = self.seen.fetch_add(1, Ordering::SeqCst) + 1;
        if n == self.nth {
            self.action
        } else {
            FaultAction::Proceed
        }
    }
}

/// Fails **every** crossing of one boundary, which is what a retry loop cannot absorb.
struct AlwaysFail(Boundary);

impl FaultInjector for AlwaysFail {
    fn before(&self, boundary: Boundary) -> FaultAction {
        if boundary == self.0 {
            FaultAction::Fail
        } else {
            FaultAction::Proceed
        }
    }
}

/// Records every boundary it is asked about, in order.
#[derive(Default)]
struct Recorder(Mutex<Vec<Boundary>>);

impl FaultInjector for Recorder {
    fn before(&self, boundary: Boundary) -> FaultAction {
        self.0.lock().unwrap().push(boundary);
        FaultAction::Proceed
    }
}

fn read_header(path: &Path) -> SnapshotHeader {
    SnapshotReader::open(path)
        .expect("snapshot opens")
        .header()
        .clone()
}

// --- build and publish ---------------------------------------------------------------------

/// M5-06/M5-16: a build publishes one file, records it as the current snapshot, and describes
/// exactly the state that was applied when the view was captured.
#[retcd_test]
async fn m5_06_snapshot_header_carries_every_required_field() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let s = open_plain(tmp.path());
        seed(&s, 5).await;

        let meta = build(&s).await.expect("build succeeds");
        assert_eq!(meta.last_log_id, Some(log_id(1, 5)));

        let path = snap_path(&s, &meta);
        assert!(path.exists(), "the published file exists");
        let header = read_header(&path);
        assert_eq!(header.snapshot_id, meta.snapshot_id);
        assert_eq!(header.last_applied, Some(log_id(1, 5)));
        assert_eq!(header.cluster_revision, 5);
        assert_eq!(header.built_by, 1);
        assert_eq!(header.cluster_id, identity_for(7, 1).cluster_id);

        // The `.tmp` is gone: a published snapshot is one file, not two.
        assert!(!s
            .path()
            .join("snapshots")
            .join(format!("{}.tmp", meta.snapshot_id))
            .exists());

        assert_eq!(
            s.snapshot_meta().map(|m| m.snapshot_id),
            Some(meta.snapshot_id.clone())
        );
        assert_eq!(
            s.reader().snapshot_meta().map(|m| m.last_log_id),
            Some(meta.last_log_id)
        );
        let metrics = s.metrics();
        assert_eq!(metrics.snapshot_builds, 1);
        assert_eq!(metrics.snapshot_build_failures, 0);
        assert_eq!(metrics.snapshot_publications, 1);
        assert_eq!(metrics.snapshot_files, 1);
        assert_eq!(metrics.snapshot_last_log_index, 5);
        assert!(metrics.snapshot_size_bytes > 0);
    }
    // Survives the restart, which is the whole point of writing it to `state_meta`.
    {
        let s = open_plain(tmp.path());
        let meta = s.snapshot_meta().expect("current snapshot is durable");
        assert_eq!(meta.last_log_id, Some(log_id(1, 5)));
    }
}

/// M5-09: the body is column-family generic. Every state-machine column family is exported and
/// named in the header; the log and `state_meta` are not.
#[retcd_test]
async fn m5_09_counts_match_payload_and_body_is_cf_generic() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let s = open_plain(tmp.path());
        seed(&s, 3).await;
        let meta = build(&s).await.expect("build succeeds");
        let header = read_header(&snap_path(&s, &meta));

        assert!(header.cfs.contains(&CF_KV.to_string()));
        assert!(header.cfs.contains(&CF_EVENTS.to_string()));
        assert!(
            !header
                .cfs
                .iter()
                .any(|c| c == "raft_log" || c == "state_meta"),
            "the log and node-local metadata are never in a snapshot body: {:?}",
            header.cfs
        );
        assert_eq!(header.counts.get(CF_KV), Some(&3));
        assert_eq!(header.counts.get(CF_EVENTS), Some(&3));
        assert_eq!(header.total_records(), 6);
        assert!(header.bytes > 0);
    }
}

/// M5-03: a transient failure during a build is absorbed by a retry, not turned into the fatal
/// error OpenRaft would shut the node down for.
#[retcd_test]
async fn m5_03_transient_build_error_is_absorbed_not_returned() {
    for boundary in [
        Boundary::BeforeSnapshotTmpSync,
        Boundary::AfterSnapshotRename,
        Boundary::BeforeCurrentSnapshotMeta,
    ] {
        let tmp = tempfile::tempdir().unwrap();
        {
            let s = open_at(tmp.path(), FailAt::new(boundary, FaultAction::Fail));
            seed(&s, 2).await;
            let meta = build(&s)
                .await
                .unwrap_or_else(|e| panic!("{boundary}: {e}"));
            assert_eq!(
                s.snapshot_meta().map(|m| m.snapshot_id),
                Some(meta.snapshot_id)
            );
            let metrics = s.metrics();
            assert_eq!(metrics.snapshot_build_failures, 0, "{boundary}");
            assert!(metrics.snapshot_build_retries >= 1, "{boundary}: retried");
        }
    }
}

/// M5-04: a build that cannot succeed reports one failure and publishes nothing — and
/// crucially leaves no `current_snapshot`, so no purge can claim cover from it.
#[retcd_test]
async fn m5_04_unrecoverable_build_error_leaves_no_partial_publication() {
    for boundary in [
        Boundary::BeforeSnapshotTmpSync,
        Boundary::BeforeCurrentSnapshotMeta,
    ] {
        let tmp = tempfile::tempdir().unwrap();
        {
            let s = open_at(tmp.path(), Arc::new(AlwaysFail(boundary)));
            seed(&s, 2).await;
            let err = build(&s).await.expect_err("build cannot succeed");
            assert!(err.contains("injected"), "{boundary}: {err}");
            assert_eq!(s.metrics().snapshot_build_failures, 1, "{boundary}");
            assert!(s.snapshot_meta().is_none(), "{boundary}: nothing published");
        }
        // And the refusal survives the restart: a snapshot nobody recorded is not a snapshot.
        {
            let s = open_plain(tmp.path());
            assert!(s.snapshot_meta().is_none(), "{boundary}: still unpublished");
        }
    }
}

/// M5-13, §19.7 in its sharpest form: crash after the file is durable and before the metadata
/// batch. The reopened node must have no current snapshot and must refuse a purge — a node that
/// purged here would be deleting a log it cannot reconstruct.
#[retcd_test]
async fn m5_13_crash_before_current_snapshot_meta() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let s = open_at(
            tmp.path(),
            FailAt::new(Boundary::BeforeCurrentSnapshotMeta, FaultAction::Crash),
        );
        seed(&s, 4).await;
        assert!(build(&s).await.is_err(), "the crash aborts the build");
        assert!(s.is_poisoned());
    }
    {
        let s = open_plain(tmp.path());
        assert!(
            s.snapshot_meta().is_none(),
            "a file nothing points at is not a published snapshot"
        );
        // `last_applied` is 4, so a purge up to 4 is still covered by applied state; a purge
        // beyond it has nothing to justify it and must be refused, not deferred.
        let err = s
            .log_store()
            .purge(log_id(1, 9))
            .await
            .expect_err("uncovered purge with no transfer in flight");
        assert!(err.to_string().contains("not covered"), "{err}");
        assert_eq!(s.metrics().purge_refusals, 1);
    }
}

/// M5-12: a crash after the rename leaves a complete file and no publication. The next build
/// succeeds and the orphan does not become the current snapshot.
#[retcd_test]
async fn m5_12_crash_after_snapshot_rename() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let s = open_at(
            tmp.path(),
            FailAt::new(Boundary::AfterSnapshotRename, FaultAction::Crash),
        );
        seed(&s, 3).await;
        assert!(build(&s).await.is_err());
    }
    {
        let s = open_plain(tmp.path());
        assert!(s.snapshot_meta().is_none());
        let meta = build(&s).await.expect("a later build succeeds");
        assert_eq!(
            s.snapshot_meta().map(|m| m.snapshot_id),
            Some(meta.snapshot_id)
        );
    }
}

/// M5-15: retention keeps the configured number of published files, and never the current one.
#[retcd_test]
async fn m5_15_retain_last_two_snapshots() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let s = open_plain(tmp.path());
        s.configure_snapshots(&SnapshotConfig {
            retain_snapshots: 2,
            ..SnapshotConfig::DEFAULT
        });
        let mut latest = None;
        for round in 1..=4u64 {
            // Each build gets its own id: the applied index advances every round, and the
            // timestamp would distinguish them even if it did not.
            let entry = put(1, round, &format!("/m5/{round}"), "v");
            s.log_store()
                .blocking_append(vec![entry.clone()])
                .await
                .expect("append");
            s.state_machine().apply(vec![entry]).await.expect("apply");
            latest = Some(build(&s).await.expect("build succeeds"));
        }
        let files: Vec<_> = std::fs::read_dir(s.path().join("snapshots"))
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|x| x == "snap"))
            .collect();
        assert_eq!(files.len(), 2, "two published snapshots are retained");
        assert!(snap_path(&s, latest.as_ref().unwrap()).exists());
        assert_eq!(s.metrics().snapshot_files, 2);
    }
}

// --- install ---------------------------------------------------------------------------------

/// M5-39/M5-40/M5-44: a snapshot built on one node installs on another and reproduces its state
/// exactly, including the retained journal and the compaction watermark.
#[retcd_test]
async fn m5_39_applied_state_matches_meta_after_install() {
    let src_dir = tempfile::tempdir().unwrap();
    let dst_dir = tempfile::tempdir().unwrap();
    {
        let src = open_as(src_dir.path(), identity_for(7, 1), Arc::new(NoFaults));
        let dst = open_as(dst_dir.path(), identity_for(7, 2), Arc::new(NoFaults));
        seed(&src, 6).await;
        let meta = build(&src).await.expect("build succeeds");

        transfer(&src, &dst, &meta, |_| {}).await.expect("install");

        assert_eq!(dst.reader().state_hash(), src.reader().state_hash());
        assert_eq!(dst.reader().last_applied(), src.reader().last_applied());
        assert_eq!(
            dst.reader().cluster_revision(),
            src.reader().cluster_revision()
        );
        assert_eq!(
            dst.reader().journal_stats().unwrap(),
            src.reader().journal_stats().unwrap()
        );
        assert_eq!(
            dst.snapshot_meta().map(|m| m.last_log_id),
            Some(meta.last_log_id)
        );
        assert_eq!(dst.metrics().snapshot_installs, 1);
        assert_eq!(dst.metrics().snapshot_install_failures, 0);
    }
    // And it is durable: the installed state is on disk, not only in the mirror.
    {
        let dst = open_as(dst_dir.path(), identity_for(7, 2), Arc::new(NoFaults));
        assert_eq!(dst.reader().last_applied(), Some(log_id(1, 6)));
        assert_eq!(dst.reader().cluster_revision(), 6);
    }
}

/// M5-34/M5-35/M5-36: a corrupted or foreign snapshot is refused **before** anything is
/// overwritten.
#[retcd_test]
async fn m5_34_install_refuses_foreign_or_corrupt_snapshot() {
    // Corruption in the body: caught by the sha256 trailer, before the destructive phase.
    {
        let src_dir = tempfile::tempdir().unwrap();
        let dst_dir = tempfile::tempdir().unwrap();
        let before;
        {
            let src = open_as(src_dir.path(), identity_for(7, 1), Arc::new(NoFaults));
            let dst = open_as(dst_dir.path(), identity_for(7, 2), Arc::new(NoFaults));
            seed(&src, 4).await;
            seed(&dst, 2).await;
            before = dst.reader().state_hash();
            let meta = build(&src).await.expect("build succeeds");

            let err = transfer(&src, &dst, &meta, |bytes| {
                let last = bytes.len() - 40;
                bytes[last] ^= 0xff;
            })
            .await
            .expect_err("a corrupted snapshot is refused");
            assert!(
                err.contains("checksum") || err.contains("malformed"),
                "{err}"
            );
            assert_eq!(dst.reader().state_hash(), before, "state is untouched");
            assert!(dst.snapshot_meta().is_none());
            assert_eq!(dst.metrics().snapshot_install_failures, 1);
        }
        // The in-memory mirror is only rebuilt by the final batch, so it would look clean even
        // if the column families had already been cleared. Reopening is what actually proves
        // the refusal happened before anything on disk was touched.
        {
            let dst = open_as(dst_dir.path(), identity_for(7, 2), Arc::new(NoFaults));
            assert_eq!(
                dst.reader().state_hash(),
                before,
                "the durable state was damaged by a snapshot that was refused"
            );
            assert_eq!(dst.reader().last_applied(), Some(log_id(1, 2)));
            assert_eq!(
                dst.metrics().snapshot_install_redos,
                0,
                "no marker was written"
            );
        }
    }
    // A different cluster: refused on identity, whatever the bytes say.
    {
        let src_dir = tempfile::tempdir().unwrap();
        let dst_dir = tempfile::tempdir().unwrap();
        let src = open_as(src_dir.path(), identity_for(7, 1), Arc::new(NoFaults));
        let dst = open_as(dst_dir.path(), identity_for(9, 1), Arc::new(NoFaults));
        seed(&src, 3).await;
        seed(&dst, 1).await;
        let before = dst.reader().state_hash();
        let meta = build(&src).await.expect("build succeeds");

        let err = transfer(&src, &dst, &meta, |_| {})
            .await
            .expect_err("a foreign snapshot is refused");
        assert!(err.contains("identity mismatch"), "{err}");
        assert_eq!(dst.reader().state_hash(), before, "state is untouched");
    }
    // M5-35: same cluster, earlier recovery epoch. A restored cluster's snapshot must never
    // install into the cluster it replaced (§14.4).
    {
        let src_dir = tempfile::tempdir().unwrap();
        let dst_dir = tempfile::tempdir().unwrap();
        let mut newer = identity_for(7, 1);
        newer.recovery_epoch = RecoveryEpoch(1);
        let src = open_as(src_dir.path(), newer, Arc::new(NoFaults));
        let dst = open_as(dst_dir.path(), identity_for(7, 2), Arc::new(NoFaults));
        seed(&src, 3).await;
        seed(&dst, 1).await;
        let before = dst.reader().state_hash();
        let meta = build(&src).await.expect("build succeeds");

        let err = transfer(&src, &dst, &meta, |_| {})
            .await
            .expect_err("a snapshot from another recovery epoch is refused");
        assert!(err.contains("identity mismatch"), "{err}");
        assert_eq!(dst.reader().state_hash(), before, "state is untouched");
    }
}

/// M5-31/M5-32/M5-33: a crash in the destructive middle of an install is redone at open,
/// not served.
#[retcd_test]
async fn m5_31_crash_mid_install_is_redone_from_the_marker() {
    for boundary in [
        Boundary::AfterInstallDropCf,
        Boundary::BeforeInstallFinalBatch,
    ] {
        let src_dir = tempfile::tempdir().unwrap();
        let dst_dir = tempfile::tempdir().unwrap();
        let expected_hash;
        let expected_revision;
        {
            let src = open_as(src_dir.path(), identity_for(7, 1), Arc::new(NoFaults));
            seed(&src, 5).await;
            let meta = build(&src).await.expect("build succeeds");
            expected_hash = src.reader().state_hash();
            expected_revision = src.reader().cluster_revision();

            let dst = open_as(
                dst_dir.path(),
                identity_for(7, 2),
                FailAt::new(boundary, FaultAction::Crash),
            );
            seed(&dst, 2).await;
            assert!(
                transfer(&src, &dst, &meta, |_| {}).await.is_err(),
                "{boundary}: the crash aborts the install"
            );
            assert!(dst.is_poisoned(), "{boundary}");
        }
        {
            let dst = open_as(dst_dir.path(), identity_for(7, 2), Arc::new(NoFaults));
            assert_eq!(
                dst.reader().state_hash(),
                expected_hash,
                "{boundary}: the redo finished the install"
            );
            assert_eq!(dst.reader().cluster_revision(), expected_revision);
            assert_eq!(dst.metrics().snapshot_install_redos, 1, "{boundary}");
            assert!(dst.snapshot_meta().is_some(), "{boundary}");
        }
        // Idempotent: opening again has nothing left to redo.
        {
            let dst = open_as(dst_dir.path(), identity_for(7, 2), Arc::new(NoFaults));
            assert_eq!(dst.metrics().snapshot_install_redos, 0, "{boundary}");
            assert_eq!(dst.reader().state_hash(), expected_hash, "{boundary}");
        }
    }
}

/// M5-30: a failure before the marker leaves the destination exactly as it was.
#[retcd_test]
async fn m5_30_crash_before_install_marker() {
    let src_dir = tempfile::tempdir().unwrap();
    let dst_dir = tempfile::tempdir().unwrap();
    {
        let src = open_as(src_dir.path(), identity_for(7, 1), Arc::new(NoFaults));
        let dst = open_as(
            dst_dir.path(),
            identity_for(7, 2),
            Arc::new(AlwaysFail(Boundary::BeforeInstallMarker)),
        );
        seed(&src, 4).await;
        seed(&dst, 2).await;
        let before = dst.reader().state_hash();
        let meta = build(&src).await.expect("build succeeds");

        assert!(transfer(&src, &dst, &meta, |_| {}).await.is_err());
        assert_eq!(dst.reader().state_hash(), before);
        assert_eq!(dst.reader().last_applied(), Some(log_id(1, 2)));
    }
    {
        let dst = open_as(dst_dir.path(), identity_for(7, 2), Arc::new(NoFaults));
        assert_eq!(dst.reader().last_applied(), Some(log_id(1, 2)));
        assert_eq!(
            dst.metrics().snapshot_install_redos,
            0,
            "no marker, no redo"
        );
    }
}

// --- purge ------------------------------------------------------------------------------------

/// M5-19/M5-23: a purge covered by applied state deletes the prefix, records `last_purged`, and
/// crosses the purge boundaries rather than the log-flush ones (ruling M5-R2).
#[retcd_test]
async fn m5_19_purge_actually_happens() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let s = open_plain(tmp.path());
        seed(&s, 5).await;
        build(&s).await.expect("build succeeds");
        let flushes_before = s.counters().get(Boundary::BeforeLogFlush);

        s.log_store().purge(log_id(1, 3)).await.expect("purge");

        assert_eq!(s.raft_log_len(), 2, "entries 4 and 5 survive");
        assert_eq!(s.counters().get(Boundary::BeforePurge), 1);
        assert_eq!(s.counters().get(Boundary::AfterPurge), 1);
        assert_eq!(
            s.counters().get(Boundary::BeforeLogFlush),
            flushes_before,
            "a purge does not cross the append path's boundaries (M5-R2)"
        );
        let metrics = s.metrics();
        assert_eq!(metrics.purges, 1);
        assert_eq!(metrics.purged_index, 3);
        assert_eq!(metrics.purge_deferrals, 0);
        assert_eq!(metrics.purge_refusals, 0);
    }
    {
        let s = open_plain(tmp.path());
        let state = s.log_store().get_log_state().await.expect("log state");
        assert_eq!(state.last_purged_log_id, Some(log_id(1, 3)));
    }
}

/// M5-24: an uncovered purge with nothing in flight is an error, not a deferral. This is the
/// rule-3 witness of ruling M5-R11 — a logic error has to surface.
#[retcd_test]
async fn m5_24_uncovered_purge_with_no_transfer_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let s = open_plain(tmp.path());
        seed(&s, 3).await;
        let err = s
            .log_store()
            .purge(log_id(1, 9))
            .await
            .expect_err("nothing covers index 9");
        assert!(err.to_string().contains("not covered"), "{err}");
        assert_eq!(s.raft_log_len(), 3, "nothing was deleted");
        assert_eq!(s.metrics().purge_refusals, 1);
        assert_eq!(s.metrics().purges, 0);
    }
}

/// M5-24a (ruling M5-R11, rule 2): a purge OpenRaft issues while a snapshot is being received
/// is deferred — nothing deleted, nothing persisted, no error — and then executed by the
/// install that justifies it, in the same synced batch as `current_snapshot`.
#[retcd_test]
async fn m5_24a_purge_during_install_is_deferred_then_executed() {
    let src_dir = tempfile::tempdir().unwrap();
    let dst_dir = tempfile::tempdir().unwrap();
    {
        let src = open_as(src_dir.path(), identity_for(7, 1), Arc::new(NoFaults));
        let dst = open_as(dst_dir.path(), identity_for(7, 2), Arc::new(NoFaults));
        seed(&src, 6).await;
        let meta = build(&src).await.expect("build succeeds");
        seed(&dst, 6).await;

        // Exactly OpenRaft's follower order: the file is received first, then the install and
        // the purge are pushed together — so at this instant `last_applied` is still the old
        // value and the purge is not yet covered.
        let bytes = std::fs::read(snap_path(&src, &meta)).unwrap();
        let mut sm = dst.state_machine();
        let mut file = sm.begin_receiving_snapshot().await.expect("receive slot");
        file.write_all(&bytes).await.unwrap();

        // Make the request genuinely uncovered: ask to purge beyond what is applied.
        dst.log_store()
            .purge(log_id(1, 10))
            .await
            .expect("a purge during a transfer is deferred, not refused");
        assert_eq!(dst.raft_log_len(), 6, "nothing was deleted yet");
        assert_eq!(dst.metrics().purge_deferrals, 1);
        assert_eq!(dst.metrics().purges, 0);
        assert_eq!(dst.metrics().purged_index, 0);

        sm.install_snapshot(&meta, file).await.expect("install");

        assert_eq!(
            dst.raft_log_len(),
            0,
            "the deferred purge ran with the install"
        );
        assert_eq!(dst.metrics().purged_index, 10);
        assert_eq!(dst.metrics().purges, 1);
    }
    {
        let dst = open_as(dst_dir.path(), identity_for(7, 2), Arc::new(NoFaults));
        let state = dst.log_store().get_log_state().await.expect("log state");
        assert_eq!(state.last_purged_log_id, Some(log_id(1, 10)));
    }
}

/// M5-24b (ruling M5-R11): an install that aborts **drops** the deferred purge rather than
/// executing it later. The proof is that the next identical purge is refused — if the request
/// had survived, it would have been executed by something.
#[retcd_test]
async fn m5_24b_aborted_install_drops_the_pending_purge() {
    let src_dir = tempfile::tempdir().unwrap();
    let dst_dir = tempfile::tempdir().unwrap();
    {
        let src = open_as(src_dir.path(), identity_for(7, 1), Arc::new(NoFaults));
        let dst = open_as(dst_dir.path(), identity_for(7, 2), Arc::new(NoFaults));
        seed(&src, 5).await;
        let meta = build(&src).await.expect("build succeeds");
        seed(&dst, 5).await;

        let mut bytes = std::fs::read(snap_path(&src, &meta)).unwrap();
        let last = bytes.len() - 20;
        bytes[last] ^= 0xff;

        let mut sm = dst.state_machine();
        let mut file = sm.begin_receiving_snapshot().await.expect("receive slot");
        file.write_all(&bytes).await.unwrap();
        dst.log_store()
            .purge(log_id(1, 9))
            .await
            .expect("deferred while the transfer is in flight");
        assert_eq!(dst.metrics().purge_deferrals, 1);

        assert!(
            sm.install_snapshot(&meta, file).await.is_err(),
            "a corrupt snapshot is refused"
        );
        assert_eq!(dst.raft_log_len(), 5, "the deferred purge did not run");

        // The slot is closed and the request is gone, so the same purge is now a refusal.
        let err = dst
            .log_store()
            .purge(log_id(1, 9))
            .await
            .expect_err("no transfer is in flight any more");
        assert!(err.to_string().contains("not covered"), "{err}");
        assert_eq!(dst.metrics().purge_refusals, 1);
        assert_eq!(dst.metrics().purged_index, 0);
    }
}

/// M5-24c (ruling M5-R11): a restart in the middle of a deferral reports the **lower**
/// `last_purged`, because the deferred request was never persisted. OpenRaft re-issues the
/// purge cleanly afterwards.
#[retcd_test]
async fn m5_24c_restart_mid_defer_reports_the_lower_last_purged() {
    let src_dir = tempfile::tempdir().unwrap();
    let dst_dir = tempfile::tempdir().unwrap();
    {
        let src = open_as(src_dir.path(), identity_for(7, 1), Arc::new(NoFaults));
        let dst = open_as(dst_dir.path(), identity_for(7, 2), Arc::new(NoFaults));
        seed(&src, 4).await;
        let _ = build(&src).await.expect("build succeeds");
        seed(&dst, 4).await;
        dst.log_store()
            .purge(log_id(1, 2))
            .await
            .expect("covered by applied state");

        let mut sm = dst.state_machine();
        let mut file = sm.begin_receiving_snapshot().await.expect("receive slot");
        file.write_all(b"partial").await.unwrap();
        dst.log_store().purge(log_id(1, 8)).await.expect("deferred");
        assert_eq!(dst.metrics().purge_deferrals, 1);
        // The process dies here: no install, no final batch, nothing persisted for index 8.
    }
    {
        let dst = open_as(dst_dir.path(), identity_for(7, 2), Arc::new(NoFaults));
        let state = dst.log_store().get_log_state().await.expect("log state");
        assert_eq!(
            state.last_purged_log_id,
            Some(log_id(1, 2)),
            "the deferred purge was never persisted"
        );
        // The partial transfer went with it, so a repeat of the deferred request is now a
        // refusal — which is exactly what OpenRaft needs to see rather than a silent success.
        assert!(dst.log_store().purge(log_id(1, 8)).await.is_err());
        // And the covered part is still purgeable, so recovery is not blocked.
        dst.log_store().purge(log_id(1, 4)).await.expect("covered");
        assert_eq!(dst.metrics().purged_index, 4);
    }
}

/// M5-24d: a purge covered by the snapshot but ahead of `last_applied` is legal — that is the
/// case the `max` in the cover test exists for.
#[retcd_test]
async fn m5_24d_purge_covered_by_the_snapshot_is_allowed() {
    let src_dir = tempfile::tempdir().unwrap();
    let dst_dir = tempfile::tempdir().unwrap();
    {
        let src = open_as(src_dir.path(), identity_for(7, 1), Arc::new(NoFaults));
        let dst = open_as(dst_dir.path(), identity_for(7, 2), Arc::new(NoFaults));
        seed(&src, 6).await;
        let meta = build(&src).await.expect("build succeeds");
        seed(&dst, 3).await;
        transfer(&src, &dst, &meta, |_| {}).await.expect("install");

        // The destination applied only 3 of its own entries, but the installed snapshot covers
        // 6, so purging up to 6 is justified by the snapshot rather than by apply.
        dst.log_store().purge(log_id(1, 6)).await.expect("purge");
        assert_eq!(dst.metrics().purged_index, 6);
    }
}

/// M5-23a: a failure at `BeforePurge` deletes nothing; a crash at `AfterPurge` leaves the
/// deletion durable, because the batch is written before that boundary is crossed.
#[retcd_test]
async fn m5_23a_purge_boundaries_fail_and_crash_cleanly() {
    // Fail before: nothing deleted, store still usable.
    {
        let tmp = tempfile::tempdir().unwrap();
        let s = open_at(
            tmp.path(),
            FailAt::new(Boundary::BeforePurge, FaultAction::Fail),
        );
        seed(&s, 4).await;
        assert!(s.log_store().purge(log_id(1, 2)).await.is_err());
        assert_eq!(s.raft_log_len(), 4, "nothing was deleted");
        assert!(!s.is_poisoned(), "Fail must not poison");
        s.log_store()
            .purge(log_id(1, 2))
            .await
            .expect("retry works");
        assert_eq!(s.raft_log_len(), 2);
    }
    // Crash after: poisoned, but the deletion and its `last_purged` are both durable.
    let tmp = tempfile::tempdir().unwrap();
    {
        let s = open_at(
            tmp.path(),
            FailAt::new(Boundary::AfterPurge, FaultAction::Crash),
        );
        seed(&s, 4).await;
        assert!(s.log_store().purge(log_id(1, 3)).await.is_err());
        assert!(s.is_poisoned());
    }
    {
        let s = open_plain(tmp.path());
        let state = s.log_store().get_log_state().await.expect("log state");
        assert_eq!(state.last_purged_log_id, Some(log_id(1, 3)));
        assert_eq!(s.raft_log_len(), 1);
    }
}

// --- coverage -----------------------------------------------------------------------------

/// M5-48a: every boundary M5 added is actually crossed by a real operation. A boundary nothing
/// reaches is a fault-injection point that proves nothing, and the counters are the only way to
/// tell the difference.
#[retcd_test]
async fn m5_48a_every_snapshot_boundary_is_crossed_by_a_real_operation() {
    let src_dir = tempfile::tempdir().unwrap();
    let dst_dir = tempfile::tempdir().unwrap();
    let recorder = Arc::new(Recorder::default());
    {
        let src = open_as(src_dir.path(), identity_for(7, 1), Arc::new(NoFaults));
        let dst = open_as(dst_dir.path(), identity_for(7, 2), recorder.clone());
        seed(&src, 4).await;
        let meta = build(&src).await.expect("build succeeds");
        seed(&dst, 4).await;
        let _ = build(&dst).await.expect("build succeeds");
        transfer(&src, &dst, &meta, |_| {}).await.expect("install");
        dst.log_store().purge(log_id(1, 4)).await.expect("purge");

        let counters = dst.counters();
        for b in SNAPSHOT_BOUNDARIES {
            assert!(counters.get(b) >= 1, "{b} was never crossed");
        }
    }
    let seen = recorder.0.lock().unwrap().clone();
    for b in SNAPSHOT_BOUNDARIES {
        assert!(seen.contains(&b), "{b} was never offered to the injector");
    }
}

// --- fix round 1 (critic-m5a) ----------------------------------------------------------------

/// Blocks the **first** crossing of one boundary until the test releases it, so a test can hold
/// an operation open at a known point and drive a second operation against it.
///
/// A deterministic handshake rather than a `FaultAction::Delay`: a duration is a sleep, and a
/// sleep long enough to be reliable is long enough to be slow. Blocking here is safe because
/// `RocksShared::run` consults every boundary inside `spawn_blocking`, so this parks a
/// blocking-pool thread, not a runtime worker.
struct PauseAt {
    boundary: Boundary,
    seen: AtomicU64,
    reached_tx: std::sync::mpsc::SyncSender<()>,
    reached_rx: Mutex<std::sync::mpsc::Receiver<()>>,
    release_tx: std::sync::mpsc::SyncSender<()>,
    release_rx: Mutex<std::sync::mpsc::Receiver<()>>,
}

impl PauseAt {
    fn new(boundary: Boundary) -> Arc<Self> {
        let (reached_tx, reached_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        Arc::new(Self {
            boundary,
            seen: AtomicU64::new(0),
            reached_tx,
            reached_rx: Mutex::new(reached_rx),
            release_tx,
            release_rx: Mutex::new(release_rx),
        })
    }

    /// Wait until the paused operation is sitting on the boundary.
    async fn reached(self: &Arc<Self>) {
        let me = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            me.reached_rx
                .lock()
                .unwrap()
                .recv()
                .expect("the paused operation reaches the boundary")
        })
        .await
        .expect("wait task joins");
    }

    /// Let it continue.
    fn release(&self) {
        self.release_tx
            .send(())
            .expect("the pause is still waiting");
    }
}

impl FaultInjector for PauseAt {
    fn before(&self, boundary: Boundary) -> FaultAction {
        if boundary == self.boundary && self.seen.fetch_add(1, Ordering::SeqCst) == 0 {
            self.reached_tx.send(()).expect("the test is still waiting");
            self.release_rx
                .lock()
                .unwrap()
                .recv()
                .expect("the test releases the pause");
        }
        FaultAction::Proceed
    }
}

/// M5-24e (finding C5-05): the install window between "the bytes are accepted" and "the marker
/// is durable" still counts as activity.
///
/// The receive slot is *claimed*, not taken, for the whole validate/rename/fsync stretch. If it
/// were emptied on entry, `snapshot_activity()` would be false right there while the marker does
/// not exist yet, and OpenRaft pushes `install_full_snapshot` and `PurgeLog` back to back with no
/// condition between them — so a purge landing in that window would be **refused**, which is a
/// fatal `StorageError`, instead of deferred.
#[retcd_test]
async fn m5_24e_purge_inside_the_install_window_is_deferred() {
    let src_dir = tempfile::tempdir().unwrap();
    let dst_dir = tempfile::tempdir().unwrap();
    {
        let src = open_as(src_dir.path(), identity_for(7, 1), Arc::new(NoFaults));
        seed(&src, 6).await;
        let meta = build(&src).await.expect("build succeeds");
        let expected_hash = src.reader().state_hash();

        let pause = PauseAt::new(Boundary::BeforeInstallMarker);
        let dst = open_as(dst_dir.path(), identity_for(7, 2), pause.clone());
        seed(&dst, 6).await;

        let bytes = std::fs::read(snap_path(&src, &meta)).unwrap();
        let mut sm = dst.state_machine();
        let file = {
            let mut file = sm.begin_receiving_snapshot().await.expect("receive slot");
            file.write_all(&bytes).await.unwrap();
            file
        };

        // The install is in flight and parked *after* the rename, *before* the marker.
        let installing = {
            let dst = dst.clone();
            let meta = meta.clone();
            tokio::spawn(async move { dst.state_machine().install_snapshot(&meta, file).await })
        };
        pause.reached().await;

        // Nothing is asserted until the pause is released and the install has joined: a panic
        // here would leave the injector parked on a blocking-pool thread, and dropping the
        // runtime waits for blocking tasks, so the failure would surface as a hang.
        let purged = dst.log_store().purge(log_id(1, 10)).await;
        let deferrals = dst.metrics().purge_deferrals;
        let refusals = dst.metrics().purge_refusals;
        let len_inside_the_window = dst.raft_log_len();

        pause.release();
        installing
            .await
            .expect("the install task joins")
            .expect("the install succeeds");

        purged.expect("a purge inside the install window is deferred, not refused");
        assert_eq!(deferrals, 1);
        assert_eq!(refusals, 0, "the window is activity, not a logic error");
        assert_eq!(len_inside_the_window, 6, "nothing was deleted yet");

        assert_eq!(
            dst.raft_log_len(),
            0,
            "the deferred purge ran with the install"
        );
        assert_eq!(dst.metrics().purged_index, 10);
        assert_eq!(dst.metrics().purges, 1);
        assert_eq!(dst.reader().state_hash(), expected_hash);
    }
}

/// M5-15a (finding C5-06): retention never unlinks the file an install marker points at.
///
/// Snapshots sort by build time, and a leader's snapshot is always older than a build this node
/// makes afterwards — so a local build during an install can push the incoming file past the
/// retention window. Deleting it turns the next open's redo into a `Corrupt`, and the node cannot
/// start at all.
///
/// The marker is held open by failing the install at its final batch rather than by pausing a
/// concurrent one: Windows refuses to unlink a file the installing thread still has open, which
/// would make the row pass for the wrong reason.
///
/// Since F-001 a build never publishes, and so never prunes, while the install marker is set,
/// and a failed install poisons the store (critic-f A-1). So the local build that used to prune
/// here is now refused, and refused **at once**: before the poison, the marker mirror stayed set
/// and the build waited `INSTALL_PATIENCE` (300 s) for an install that would never commit.
#[retcd_test]
async fn m5_15a_prune_never_removes_the_in_progress_install_file() {
    let src_dir = tempfile::tempdir().unwrap();
    let dst_dir = tempfile::tempdir().unwrap();
    let expected_hash;
    {
        let src = open_as(src_dir.path(), identity_for(7, 1), Arc::new(NoFaults));
        seed(&src, 6).await;
        let meta = build(&src).await.expect("build succeeds");
        expected_hash = src.reader().state_hash();

        let dst = open_as(
            dst_dir.path(),
            identity_for(7, 2),
            FailAt::new(Boundary::BeforeInstallFinalBatch, FaultAction::Fail),
        );
        dst.configure_snapshots(&SnapshotConfig {
            retain_snapshots: 1,
            ..SnapshotConfig::DEFAULT
        });
        // Ahead of the incoming snapshot, so a local build sorts newer on index even if both
        // land in the same millisecond.
        seed(&dst, 9).await;

        assert!(
            transfer(&src, &dst, &meta, |_| {}).await.is_err(),
            "the install stops at its final batch, leaving the marker durable"
        );
        let installed_file = snap_path(&dst, &meta);
        assert!(
            installed_file.exists(),
            "the marker's file was published before the failure"
        );

        assert!(
            dst.is_poisoned(),
            "an install that failed mid-replacement poisons the store"
        );
        let local = tokio::time::timeout(CAPTURE_PATIENCE, build(&dst))
            .await
            .expect("a build after a failed install ends at once, not after the install wait");
        assert!(local.is_err(), "a poisoned store publishes nothing");
        assert!(
            installed_file.exists(),
            "retention must skip the snapshot the install marker points at"
        );
    }
    // The consequence the row is really about: the node still starts.
    {
        let dst = open_as(dst_dir.path(), identity_for(7, 2), Arc::new(NoFaults));
        assert_eq!(dst.metrics().snapshot_install_redos, 1);
        assert_eq!(dst.reader().state_hash(), expected_hash);
    }
}

/// M5-11a (finding C5-13): open sweeps both kinds of partial file, not just received ones.
///
/// A `.tmp` is an export that died before its rename, so it was never a publication and never
/// will be. `list_snapshots` only sees `.snap`, so no retention policy would ever count it.
#[retcd_test]
async fn m5_11a_open_sweeps_partial_builds_and_receives() {
    let tmp = tempfile::tempdir().unwrap();
    let published;
    let leftover_build;
    let leftover_receive;
    {
        let s = open_plain(tmp.path());
        seed(&s, 3).await;
        let meta = build(&s).await.expect("build succeeds");
        published = snap_path(&s, &meta);

        let dir = s.path().join("snapshots");
        leftover_build = dir.join("9-1-1.tmp");
        leftover_receive = dir.join("incoming-2-1.recv.tmp");
        std::fs::write(&leftover_build, b"half an export").unwrap();
        std::fs::write(&leftover_receive, b"half a transfer").unwrap();
    }
    {
        let s = open_plain(tmp.path());
        assert!(
            !leftover_build.exists(),
            "an export that never reached its rename is swept"
        );
        assert!(
            !leftover_receive.exists(),
            "a transfer that never reached its install is swept"
        );
        assert!(published.exists(), "the published snapshot survives");
        assert!(s.snapshot_meta().is_some());
    }
}

/// M5-82a (finding C5-07): the offline restore streams in bounded batches.
///
/// A `WriteBatch` is held entirely in memory, so one batch for the whole snapshot would put the
/// whole state machine there — the exact cost `SnapshotData = tokio::fs::File` exists to avoid.
/// The observable proof is a snapshot with more than two full batches restoring with counts that
/// match the header exactly: nothing is lost at a chunk edge, and nothing is written twice.
#[retcd_test]
async fn m5_82a_restore_streams_a_large_snapshot_in_bounded_batches() {
    /// Comfortably more than two `INSTALL_BATCH_RECORDS` (4096) in `kv` alone, which is the
    /// only large family a restore writes — `events` is dropped.
    const KEYS: u64 = 8_500;

    let tmp = tempfile::tempdir().unwrap();
    let fresh = tempfile::tempdir().unwrap();
    let dest = fresh.path().join("restored");
    let new_identity = ClusterIdentity {
        cluster_id: ClusterId::from_bytes([0x5a; 16]),
        recovery_epoch: RecoveryEpoch(1),
        node_id: NodeId(1),
    };
    let header;
    let report;
    {
        let s = open_plain(tmp.path());
        let entries: Vec<_> = (1..=KEYS)
            .map(|i| put(1, i, &format!("/m5/big/{i:06}"), "v"))
            .collect();
        s.log_store()
            .blocking_append(entries.clone())
            .await
            .expect("append");
        s.state_machine().apply(entries).await.expect("apply");

        let meta = build(&s).await.expect("build succeeds");
        let path = snap_path(&s, &meta);
        header = read_header(&path);
        assert!(
            header.counts[CF_KV] > 2 * 4_096,
            "the row is only meaningful past two full batches, got {}",
            header.counts[CF_KV]
        );

        let restored_from = config_core::RestoredFrom {
            cluster_id: identity_for(7, 1).cluster_id,
            recovery_epoch: 0,
            revision: header.cluster_revision,
        };
        report =
            config_storage::restore_into_fresh_store(&dest, &new_identity, &path, &restored_from)
                .expect("restore into a fresh store");
    }
    assert_eq!(
        report.written[CF_KV], header.counts[CF_KV],
        "every record crossed exactly one chunk boundary"
    );
    assert_eq!(report.revision, header.cluster_revision);
    {
        let s = open_as(&dest, new_identity, Arc::new(NoFaults));
        let mut present = 0usize;
        s.reader().with_state(&mut |state| present = state.len());
        assert_eq!(present as u64, KEYS, "the restored store holds every key");
        assert_eq!(s.reader().cluster_revision(), header.cluster_revision);
    }
}

/// F-015 `a_pinned_node_refuses_a_snapshot_built_by_a_newer_generation`.
///
/// The install path's companion to the apply-path fence in `m6_compat_open.rs`, and the answer
/// to the question ADR-0030's snapshot paragraph left open: the refusal it describes did not
/// exist. `validate_snapshot_file` compared the header against this *build's* constants, which
/// a pinned node meets by definition, so the snapshot installed — and `apply_snapshot_records`
/// then unioned the header's `max_applied_command_schema` into the local one. That made
/// install the second route by which a node advertising schema 1 could come back claiming to
/// have applied schema 2 without ever having decoded a schema-2 command, and unlike the apply
/// path it needed no gate bug to reach: catching a lagging node up is the ordinary path.
///
/// The check is on `command_schema` rather than on `format_version` because that is the axis
/// the watermark rides on, and it covers the format axis anyway: every snapshot this build
/// writes stamps the header from `COMMAND_ENVELOPE_VERSION`, so a pinned node turns away the
/// whole generation.
#[retcd_test]
async fn f015_a_pinned_node_refuses_a_snapshot_built_by_a_newer_generation() {
    let src_dir = tempfile::tempdir().expect("temp dir");
    let dst_dir = tempfile::tempdir().expect("temp dir");

    let src = open_plain(src_dir.path());
    seed(&src, 3).await;
    let meta = build(&src).await.expect("the newer node publishes");

    let dst = RocksStore::open_with(
        dst_dir.path(),
        identity_for(7, 2),
        Limits::DEFAULT,
        Arc::new(NoFaults),
        Span::none(),
        config_storage::RocksOptions {
            max_format_version: 1,
            command_schema: config_core::COMMAND_SCHEMA_V1,
            ..config_storage::RocksOptions::DEFAULT
        },
        Arc::new(config_storage::NoopSink),
    )
    .expect("a pinned node opens its own fresh directory");

    let refused = transfer(&src, &dst, &meta, |_| {})
        .await
        .expect_err("a pinned node must refuse a newer generation's snapshot");
    assert!(
        refused.contains("command schema") || refused.contains("command_schema"),
        "the refusal must name the axis it refused on, got {refused:?}"
    );

    // The install left nothing behind: no state, and above all no raised watermark. A node
    // that accepted the header's claim here would go on advertising schema 1 while reporting
    // a durable maximum of 2, and every gate decision the cluster made afterwards would rest
    // on that contradiction.
    let mut watermark = 0u16;
    dst.reader()
        .with_state(&mut |s| watermark = s.max_applied_command_schema());
    assert_eq!(
        watermark,
        config_core::COMMAND_SCHEMA_V1,
        "a refused install must not raise the durable activation watermark"
    );
    assert_eq!(
        dst.reader().cluster_revision(),
        0,
        "and none of the refused snapshot's records landed"
    );
    drop(src);
    drop(dst);
}

// --- capture does not block the state machine (A-R279) -------------------------------------

/// How long a read or an apply may take while a capture is held open. Generous on purpose: with
/// the capture held, the wrong answer is "never", so the bound only has to tell "now" from that.
const CAPTURE_PATIENCE: std::time::Duration = std::time::Duration::from_secs(5);

/// Holds the first snapshot capture open at [`FaultInjector::during_snapshot_capture`]: after its
/// checkpoint exists and before it reads what that checkpoint describes.
///
/// The same handshake as [`PauseAt`], on the capture hook rather than a boundary. The release
/// wait is bounded so a row that fails before releasing ends as a failure rather than a hang.
struct HoldCapture {
    seen: AtomicU64,
    reached_tx: std::sync::mpsc::SyncSender<()>,
    reached_rx: Mutex<std::sync::mpsc::Receiver<()>>,
    release_tx: std::sync::mpsc::SyncSender<()>,
    release_rx: Mutex<std::sync::mpsc::Receiver<()>>,
}

impl HoldCapture {
    fn new() -> Arc<Self> {
        let (reached_tx, reached_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        Arc::new(Self {
            seen: AtomicU64::new(0),
            reached_tx,
            reached_rx: Mutex::new(reached_rx),
            release_tx,
            release_rx: Mutex::new(release_rx),
        })
    }

    /// Wait until the capture is being held.
    async fn reached(self: &Arc<Self>) {
        let me = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            me.reached_rx
                .lock()
                .unwrap()
                .recv()
                .expect("the capture reaches its hook")
        })
        .await
        .expect("wait task joins");
    }

    fn release(&self) {
        self.release_tx.send(()).expect("the capture is still held");
    }
}

impl FaultInjector for HoldCapture {
    fn before(&self, _boundary: Boundary) -> FaultAction {
        FaultAction::Proceed
    }

    fn during_snapshot_capture(&self) {
        if self.seen.fetch_add(1, Ordering::SeqCst) == 0 {
            self.reached_tx.send(()).expect("the test is still waiting");
            let _ = self
                .release_rx
                .lock()
                .unwrap()
                .recv_timeout(std::time::Duration::from_secs(60));
        }
    }
}

/// M5-01a (A-R279): a state read does not wait for a snapshot capture.
///
/// Readers take the state-machine lock synchronously, from runtime workers. When the capture held
/// that lock across its RocksDB checkpoint — which flushes the memtable, and queues behind every
/// other store's flush on the shared flush thread — a read waited the whole checkpoint: 2.7-3.7 s
/// per read in `m5_snapshot_cluster` under load, long enough to stall a cluster's runtime.
#[retcd_test]
async fn m5_01a_state_reads_do_not_wait_for_a_snapshot_capture() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let hold = HoldCapture::new();
        let s = open_at(tmp.path(), hold.clone());
        seed(&s, 6).await;

        let building = {
            let s = s.clone();
            tokio::spawn(async move { build(&s).await })
        };
        hold.reached().await;

        // A plain thread, so a read that blocks parks only itself. Nothing is asserted until the
        // capture is released and the build has joined, so a red row fails instead of hanging.
        let (tx, rx) = std::sync::mpsc::channel();
        let probe = {
            let reader = s.reader();
            let s = s.clone();
            std::thread::spawn(move || {
                let mut revision = 0;
                reader.with_state(&mut |k| revision = k.cluster_revision());
                let current = s.snapshot_meta();
                let _ = tx.send((revision, current.is_none()));
            })
        };
        let answered = tokio::task::spawn_blocking(move || rx.recv_timeout(CAPTURE_PATIENCE))
            .await
            .expect("wait task joins");

        hold.release();
        let meta = building
            .await
            .expect("the build task joins")
            .expect("the build succeeds");
        probe.join().expect("the probe thread joins");

        let (revision, no_current_yet) =
            answered.expect("a state read answers while a snapshot capture is held open");
        assert_eq!(revision, 6, "the read sees the applied state");
        assert!(
            no_current_yet,
            "nothing was published while the capture was held"
        );
        assert_eq!(meta.last_log_id, Some(log_id(1, 6)));
    }
}

/// M5-01b (A-R279): applies proceed while a capture is held, and the snapshot still describes
/// exactly its checkpoint.
///
/// This is the row that keeps M5-01's guarantee once the capture stops holding the lock: entries
/// 7..9 apply *after* the checkpoint, so a header that read the live state machine instead of the
/// checkpoint would claim index 9 and revision 9 over six records.
#[retcd_test]
async fn m5_01b_a_capture_describes_its_checkpoint_not_later_applies() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let hold = HoldCapture::new();
        let s = open_at(tmp.path(), hold.clone());
        seed(&s, 6).await;
        let later: Vec<_> = (7..=9)
            .map(|i| put(1, i, &format!("/m5/{i}"), &format!("v{i}")))
            .collect();
        s.log_store()
            .blocking_append(later.clone())
            .await
            .expect("append");

        let building = {
            let s = s.clone();
            tokio::spawn(async move { build(&s).await })
        };
        hold.reached().await;

        let mut applying = {
            let s = s.clone();
            tokio::spawn(async move { s.state_machine().apply(later).await.map(|_| ()) })
        };
        let applied_inside = tokio::time::timeout(CAPTURE_PATIENCE, &mut applying).await;

        hold.release();
        let meta = building
            .await
            .expect("the build task joins")
            .expect("the build succeeds");
        let finished_inside = applied_inside.is_ok();
        let joined = match applied_inside {
            Ok(joined) => joined,
            Err(_) => applying.await,
        };
        joined.expect("the apply task joins").expect("apply");

        assert!(
            finished_inside,
            "an apply finishes while a snapshot capture is held open"
        );
        assert_eq!(
            s.reader().cluster_revision(),
            9,
            "the later entries applied"
        );

        assert_eq!(
            meta.last_log_id,
            Some(log_id(1, 6)),
            "the snapshot covers the checkpoint"
        );
        let header = read_header(&snap_path(&s, &meta));
        assert_eq!(header.last_applied, Some(log_id(1, 6)));
        assert_eq!(header.cluster_revision, 6);
        assert_eq!(
            header.counts.get(CF_KV).copied(),
            Some(6),
            "six records, as the header says"
        );
    }
}

/// M5-01c (A-R286): the view is captured when the build runs, not when OpenRaft takes the
/// builder, and the snapshot describes exactly that view.
///
/// OpenRaft takes the builder on its state-machine worker and then keeps applying while the
/// build runs in a spawned task, which is the order this row follows: builder, apply 7, build
/// (held right after its checkpoint), apply 8..9. The snapshot must cover index 7 — the
/// checkpoint's own applied id, with seven records — and stay at or below what is applied.
/// A capture taken with the builder would say 6; metadata read from the live state machine
/// would say 9 over seven records.
#[retcd_test]
async fn m5_01c_snapshot_describes_the_view_taken_when_the_build_runs() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let hold = HoldCapture::new();
        let s = open_at(tmp.path(), hold.clone());
        seed(&s, 6).await;
        let later: Vec<_> = (7..=9)
            .map(|i| put(1, i, &format!("/m5/{i}"), &format!("v{i}")))
            .collect();
        s.log_store()
            .blocking_append(later.clone())
            .await
            .expect("append");

        let mut builder = s.state_machine().get_snapshot_builder().await;
        s.state_machine()
            .apply(later[..1].to_vec())
            .await
            .expect("apply 7");

        let building =
            tokio::spawn(async move { builder.build_snapshot().await.map(|snap| snap.meta) });
        hold.reached().await;

        let mut applying = {
            let s = s.clone();
            let rest = later[1..].to_vec();
            tokio::spawn(async move { s.state_machine().apply(rest).await.map(|_| ()) })
        };
        let applied_inside = tokio::time::timeout(CAPTURE_PATIENCE, &mut applying).await;

        hold.release();
        let meta = building
            .await
            .expect("the build task joins")
            .expect("the build succeeds");
        let finished_inside = applied_inside.is_ok();
        let joined = match applied_inside {
            Ok(joined) => joined,
            Err(_) => applying.await,
        };
        joined.expect("the apply task joins").expect("apply 8..9");
        assert!(
            finished_inside,
            "an apply finishes while a snapshot capture is held open"
        );

        let applied = s.reader().last_applied();
        assert_eq!(applied, Some(log_id(1, 9)), "every entry applied");
        assert_eq!(
            meta.last_log_id,
            Some(log_id(1, 7)),
            "the snapshot covers the view taken when the build ran"
        );
        assert!(
            meta.last_log_id <= applied,
            "a snapshot never runs ahead of apply"
        );
        let header = read_header(&snap_path(&s, &meta));
        assert_eq!(
            header.last_applied, meta.last_log_id,
            "the checkpoint's applied id"
        );
        assert_eq!(header.cluster_revision, 7);
        assert_eq!(
            header.counts.get(CF_KV).copied(),
            Some(7),
            "seven records, as the header says"
        );
    }
}

/// A-R286: a store opens with RocksDB `max_background_jobs=16` (4 flush threads), not the
/// default 2, so one slow flush does not hold every other store's snapshot checkpoint.
/// Read from the OPTIONS file RocksDB writes at open, which records what it actually applied.
#[retcd_test]
async fn a_r286_store_opens_with_more_background_jobs() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let s = open_plain(tmp.path());
        let options = std::fs::read_dir(s.path())
            .expect("data dir lists")
            .filter_map(Result::ok)
            .filter(|e| e.file_name().to_string_lossy().starts_with("OPTIONS-"))
            .max_by_key(|e| e.file_name())
            .expect("RocksDB writes an OPTIONS file at open");
        let text = std::fs::read_to_string(options.path()).expect("OPTIONS reads");
        assert!(
            text.lines().any(|l| l.trim() == "max_background_jobs=16"),
            "the store's RocksDB options must carry max_background_jobs=16"
        );
    }
}

// --- F-001: a build that overlaps an install --------------------------------------------------

/// Bounds every wait in the F-001 rows, so a red row fails instead of hanging.
const RACE_PATIENCE: std::time::Duration = std::time::Duration::from_secs(30);

/// One handshake: the store's thread says "reached" and may wait for "release".
struct Handshake {
    reached_tx: std::sync::mpsc::SyncSender<()>,
    reached_rx: Mutex<std::sync::mpsc::Receiver<()>>,
    release_tx: std::sync::mpsc::SyncSender<()>,
    release_rx: Mutex<std::sync::mpsc::Receiver<()>>,
}

impl Handshake {
    fn new() -> Self {
        let (reached_tx, reached_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        Self {
            reached_tx,
            reached_rx: Mutex::new(reached_rx),
            release_tx,
            release_rx: Mutex::new(release_rx),
        }
    }

    /// Called on the store's thread. `hold` decides whether it also waits for the release.
    fn arrive(&self, hold: bool) {
        self.reached_tx.send(()).expect("the test is still waiting");
        if hold {
            let _ = self.release_rx.lock().unwrap().recv_timeout(RACE_PATIENCE);
        }
    }

    fn release(&self) {
        let _ = self.release_tx.try_send(());
    }
}

/// Drives an install and a build against each other (F-001). Optionally parks the first
/// crossing of `pause` (an install boundary), and signals — optionally holding — the first
/// snapshot capture right after its checkpoint exists.
struct InstallRace {
    pause: Option<Boundary>,
    /// Fails every crossing of this boundary (with `FaultAction::Fail`, which does not poison).
    fail: Option<Boundary>,
    hold_capture: bool,
    boundary_seen: AtomicU64,
    capture_seen: AtomicU64,
    install: Handshake,
    capture: Handshake,
}

impl InstallRace {
    fn new(pause: Option<Boundary>, fail: Option<Boundary>, hold_capture: bool) -> Arc<Self> {
        Arc::new(Self {
            pause,
            fail,
            hold_capture,
            boundary_seen: AtomicU64::new(0),
            capture_seen: AtomicU64::new(0),
            install: Handshake::new(),
            capture: Handshake::new(),
        })
    }

    /// Wait until the install (`capture == false`) or the capture has reached its hook.
    async fn reached(self: &Arc<Self>, capture: bool) {
        let me = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let which = if capture { &me.capture } else { &me.install };
            which
                .reached_rx
                .lock()
                .unwrap()
                .recv_timeout(RACE_PATIENCE)
                .expect("the operation reaches its hook")
        })
        .await
        .expect("wait task joins");
    }

    fn release_all(&self) {
        self.install.release();
        self.capture.release();
    }
}

impl FaultInjector for InstallRace {
    fn before(&self, boundary: Boundary) -> FaultAction {
        if Some(boundary) == self.pause && self.boundary_seen.fetch_add(1, Ordering::SeqCst) == 0 {
            self.install.arrive(true);
        }
        if Some(boundary) == self.fail {
            return FaultAction::Fail;
        }
        FaultAction::Proceed
    }

    fn during_snapshot_capture(&self) {
        if self.capture_seen.fetch_add(1, Ordering::SeqCst) == 0 {
            self.capture.arrive(self.hold_capture);
        }
    }
}

/// The KV keys `/m5/1..=n`, which is what `seed(n)` leaves applied at index `n`.
fn seeded_keys(n: u64) -> std::collections::BTreeSet<Vec<u8>> {
    (1..=n).map(|i| format!("/m5/{i}").into_bytes()).collect()
}

/// One published `.snap`: its path, its header's applied id, and the KV keys its body holds.
type PublishedBody = (
    String,
    Option<LogId<RaftNodeId>>,
    std::collections::BTreeSet<Vec<u8>>,
);

/// Every published `.snap` under `dir/snapshots`.
fn published_bodies(dir: &Path) -> Vec<PublishedBody> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir.join("snapshots")).expect("snapshot dir lists") {
        let path = entry.expect("dir entry").path();
        if path.extension().and_then(|e| e.to_str()) != Some("snap") {
            continue;
        }
        let mut reader = SnapshotReader::open(&path).expect("published snapshot opens");
        let header = reader.header().clone();
        let mut keys = std::collections::BTreeSet::new();
        while let Some(record) = reader.next_record().expect("record reads") {
            if header.cfs[usize::from(record.cf)] == CF_KV {
                keys.insert(record.key);
            }
        }
        reader.verify_to_end().expect("counts and digest match");
        out.push((path.display().to_string(), header.last_applied, keys));
    }
    out
}

/// "Every published snapshot's header describes exactly its body": each `.snap` whose header
/// says index `n` holds exactly the records `seed` left applied at `n`. A header and a body
/// taken from the same mid-install checkpoint pass the counts and the digest, so this compares
/// the body with the state the header names, not with itself.
fn assert_headers_match_bodies(dir: &Path) {
    let published = published_bodies(dir);
    assert!(!published.is_empty(), "the row published nothing to check");
    for (file, last_applied, keys) in published {
        let index = last_applied.map_or(0, |l| l.index);
        assert_eq!(
            keys,
            seeded_keys(index),
            "{file}: the header says index {index}, the body holds another state"
        );
    }
}

/// Whether a build's checkpoint directory still exists under `dir/snapshots`.
fn build_view_exists(dir: &Path) -> bool {
    std::fs::read_dir(dir.join("snapshots"))
        .map(|entries| {
            entries.flatten().any(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with(config_storage::snapshot::BUILD_VIEW_PREFIX)
            })
        })
        .unwrap_or(false)
}

/// F-001 (mid-install half): a build whose checkpoint lands inside an install never publishes
/// it.
///
/// The install is parked after it cleared the data column families, so the checkpoint holds the
/// old header (index 3) over an empty body. The row waits until the build is done with that
/// checkpoint (its directory is gone) while the install is still parked, and reads what was
/// published by then. Nothing may be: a build that exported the view published a snapshot that
/// says index 3 and holds no records, and a lagging follower would install it.
#[retcd_test]
async fn f001_a_build_inside_an_install_never_publishes_the_mid_install_view() {
    let src_dir = tempfile::tempdir().unwrap();
    let dst_dir = tempfile::tempdir().unwrap();
    {
        let src = open_as(src_dir.path(), identity_for(7, 1), Arc::new(NoFaults));
        seed(&src, 6).await;
        let meta = build(&src).await.expect("source build succeeds");

        let race = InstallRace::new(Some(Boundary::AfterInstallDropCf), None, false);
        let dst = open_as(dst_dir.path(), identity_for(7, 2), race.clone());
        seed(&dst, 3).await;

        let bytes = std::fs::read(snap_path(&src, &meta)).unwrap();
        let file = {
            let mut file = dst
                .state_machine()
                .begin_receiving_snapshot()
                .await
                .expect("receive slot");
            file.write_all(&bytes).await.unwrap();
            file
        };
        let installing = {
            let dst = dst.clone();
            let meta = meta.clone();
            tokio::spawn(async move { dst.state_machine().install_snapshot(&meta, file).await })
        };
        race.reached(false).await;

        let building = {
            let dst = dst.clone();
            tokio::spawn(async move { build(&dst).await })
        };
        race.reached(true).await;

        // The checkpoint is mid-install. Wait until the build is done with it.
        let deadline = std::time::Instant::now() + RACE_PATIENCE;
        while build_view_exists(dst_dir.path()) && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await; // testkit:allow-sleep: bounded poll for the build to release its view
        }
        let view_released = !build_view_exists(dst_dir.path());
        let published_mid_install = dst.metrics().snapshot_publications;
        let current_mid_install = dst.snapshot_meta().map(|m| m.last_log_id);

        // Nothing is asserted until both tasks have joined, so a red row fails, not hangs.
        race.release_all();
        installing
            .await
            .expect("the install task joins")
            .expect("the install succeeds");
        let built = building
            .await
            .expect("the build task joins")
            .expect("the build succeeds");

        assert!(view_released, "the build finished with its checkpoint");
        assert_eq!(
            (published_mid_install, current_mid_install),
            (0, None),
            "nothing is published from a checkpoint taken inside an install"
        );
        assert_eq!(
            built.last_log_id, meta.last_log_id,
            "the build captured again after the install"
        );
        assert_eq!(
            dst.snapshot_meta().map(|m| m.last_log_id),
            Some(meta.last_log_id),
            "the current snapshot covers the installed state"
        );
        assert_eq!(dst.reader().last_applied(), meta.last_log_id);
        assert_eq!(
            dst.metrics().snapshot_build_retries,
            0,
            "waiting for an install spends no build attempt"
        );
        assert_headers_match_bodies(dst_dir.path());
    }
}

/// F-001 (newer-snapshot half): a build never replaces a newer `current_snapshot` with an older
/// view.
///
/// The build's checkpoint is taken before the install starts — a consistent image of index 3 —
/// and held while an install of index 6 runs to completion. Publishing the view afterwards would
/// move this node's current snapshot back to index 3 under an applied state of 6: it would serve
/// lagging followers the old state, and OpenRaft, whose own snapshot meta never moves backwards,
/// may already have purged the log entries the older snapshot needs.
#[retcd_test]
async fn f001_a_build_never_replaces_a_newer_installed_snapshot() {
    let src_dir = tempfile::tempdir().unwrap();
    let dst_dir = tempfile::tempdir().unwrap();
    {
        let src = open_as(src_dir.path(), identity_for(7, 1), Arc::new(NoFaults));
        seed(&src, 6).await;
        let meta = build(&src).await.expect("source build succeeds");

        let race = InstallRace::new(None, None, true);
        let dst = open_as(dst_dir.path(), identity_for(7, 2), race.clone());
        seed(&dst, 3).await;

        let building = {
            let dst = dst.clone();
            tokio::spawn(async move { build(&dst).await })
        };
        race.reached(true).await;

        // The checkpoint holds index 3. Install index 6 while it is held.
        let installed = transfer(&src, &dst, &meta, |_| {}).await;

        race.release_all();
        let built = building
            .await
            .expect("the build task joins")
            .expect("the build succeeds");
        installed.expect("the install succeeds");

        assert_eq!(
            dst.snapshot_meta().map(|m| (m.snapshot_id, m.last_log_id)),
            Some((meta.snapshot_id.clone(), meta.last_log_id)),
            "the installed snapshot stays current"
        );
        assert_eq!(
            built.last_log_id, meta.last_log_id,
            "the build hands OpenRaft the current snapshot, not the older view"
        );
        assert_eq!(
            dst.metrics().snapshot_publications,
            1,
            "only the install published; the older view did not"
        );
        assert_headers_match_bodies(dst_dir.path());
    }
}

/// F-001 (offline backup): `export_snapshot` refuses a directory whose install stopped half-way.
///
/// The install crashes after clearing the data column families, so the directory holds the old
/// header fields over an empty body and a durable marker. An export of it would be a backup of
/// a state no node had, with counts and a digest that agree. After one open redoes the install,
/// the same export succeeds and covers the installed state.
#[retcd_test]
async fn f001_offline_export_refuses_a_half_installed_store() {
    let src_dir = tempfile::tempdir().unwrap();
    let dst_dir = tempfile::tempdir().unwrap();
    let out_dir = tempfile::tempdir().unwrap();
    let out = out_dir.path().join("backup.snap");
    let meta;
    {
        let src = open_as(src_dir.path(), identity_for(7, 1), Arc::new(NoFaults));
        seed(&src, 6).await;
        meta = build(&src).await.expect("source build succeeds");
        let dst = open_as(
            dst_dir.path(),
            identity_for(7, 2),
            FailAt::new(Boundary::AfterInstallDropCf, FaultAction::Crash),
        );
        seed(&dst, 3).await;
        assert!(
            transfer(&src, &dst, &meta, |_| {}).await.is_err(),
            "the crash stops the install half-way"
        );
    }

    let refused = config_storage::export_snapshot(dst_dir.path(), &out);
    let detail = refused
        .expect_err("a half-installed store is not backed up")
        .to_string();
    assert!(
        detail.contains("install"),
        "the refusal names the interrupted install: {detail}"
    );
    assert!(!out.exists(), "no backup file is written");

    // One open redoes the install; then the backup covers the installed state.
    drop(open_as(
        dst_dir.path(),
        identity_for(7, 2),
        Arc::new(NoFaults),
    ));
    let header = config_storage::export_snapshot(dst_dir.path(), &out)
        .expect("the finished store is backed up");
    assert_eq!(header.last_applied, meta.last_log_id);
    assert_eq!(header.counts.get(CF_KV).copied(), Some(6));
}

/// F-001 / critic-f A-1: a build that is already waiting on an install stops waiting when that
/// install fails.
///
/// The install is parked after its clear; the build captures mid-install and waits. Then the
/// install is released and fails at its final batch, which poisons the store and leaves the
/// marker mirror set. The waiting build must end at once with an error, not wait out
/// `INSTALL_PATIENCE` (300 s) for an install that will never commit.
#[retcd_test]
async fn f001_a_waiting_build_ends_when_the_install_fails() {
    let src_dir = tempfile::tempdir().unwrap();
    let dst_dir = tempfile::tempdir().unwrap();
    {
        let src = open_as(src_dir.path(), identity_for(7, 1), Arc::new(NoFaults));
        seed(&src, 6).await;
        let meta = build(&src).await.expect("source build succeeds");

        let race = InstallRace::new(
            Some(Boundary::AfterInstallDropCf),
            Some(Boundary::BeforeInstallFinalBatch),
            false,
        );
        let dst = open_as(dst_dir.path(), identity_for(7, 2), race.clone());
        seed(&dst, 3).await;

        let bytes = std::fs::read(snap_path(&src, &meta)).unwrap();
        let file = {
            let mut file = dst
                .state_machine()
                .begin_receiving_snapshot()
                .await
                .expect("receive slot");
            file.write_all(&bytes).await.unwrap();
            file
        };
        let installing = {
            let dst = dst.clone();
            let meta = meta.clone();
            tokio::spawn(async move { dst.state_machine().install_snapshot(&meta, file).await })
        };
        race.reached(false).await;

        let mut building = {
            let dst = dst.clone();
            tokio::spawn(async move { build(&dst).await })
        };
        race.reached(true).await;
        // The build is done with its mid-install checkpoint, so it is waiting on the install.
        let deadline = std::time::Instant::now() + RACE_PATIENCE;
        while build_view_exists(dst_dir.path()) && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await; // testkit:allow-sleep: bounded poll for the build to release its view
        }

        race.release_all();
        let install = installing.await.expect("the install task joins");
        let ended = tokio::time::timeout(CAPTURE_PATIENCE, &mut building).await;
        if ended.is_err() {
            building.abort();
        }

        assert!(install.is_err(), "the install fails at its final batch");
        assert!(dst.is_poisoned(), "a failed install poisons the store");
        let built = ended
            .expect("the waiting build ends when the install fails")
            .expect("the build task joins");
        assert!(
            built.is_err(),
            "nothing is built from a half-replaced store"
        );
        assert!(dst.snapshot_meta().is_none(), "nothing was published");
    }
}

// --- #68: C5-06 rename window, A-2 superseded prune, Q-R1 install clock -----------------------

/// Seed `s` with puts `from..=to` on top of what it holds.
async fn seed_range(s: &RocksStore, from: u64, to: u64) {
    for i in from..=to {
        let entry = put(1, i, &format!("/m5/{i}"), &format!("v{i}"));
        s.log_store()
            .blocking_append(vec![entry.clone()])
            .await
            .expect("append");
        s.state_machine().apply(vec![entry]).await.expect("apply");
    }
}

/// [`transfer`] for bytes already read, so the source's file may be gone by now.
async fn install_bytes(
    dst: &RocksStore,
    meta: &SnapshotMeta<RaftNodeId, RaftNode>,
    bytes: &[u8],
) -> Result<(), String> {
    let mut sm = dst.state_machine();
    let mut file = sm
        .begin_receiving_snapshot()
        .await
        .map_err(|e| e.to_string())?;
    file.write_all(bytes).await.expect("write received bytes");
    sm.install_snapshot(meta, file)
        .await
        .map_err(|e| e.to_string())
}

/// #68 C5-06: a local build that publishes and prunes while an install sits between renaming
/// its file into `snapshots/` and writing its marker must not prune that file.
///
/// The received file was built on the leader, so it sorts older than the local build, and with
/// `retain_snapshots = 1` it is the first to go. Before the fix the install set its in-memory
/// mirror only after the marker, so prune saw nothing to spare; the install then wrote a marker
/// naming a deleted file and failed, poisoning the store. The row waits until the build is done
/// with its checkpoint (published, or refused because an install is in flight) before letting
/// the install go on.
#[retcd_test]
async fn c5_06_a_build_inside_the_rename_window_never_prunes_the_installing_file() {
    let src_dir = tempfile::tempdir().unwrap();
    let dst_dir = tempfile::tempdir().unwrap();
    {
        let src = open_as(src_dir.path(), identity_for(7, 1), Arc::new(NoFaults));
        seed(&src, 6).await;
        let meta = build(&src).await.expect("source build succeeds");
        let expected_hash = src.reader().state_hash();

        let race = InstallRace::new(Some(Boundary::BeforeInstallMarker), None, false);
        let dst = open_as(dst_dir.path(), identity_for(7, 2), race.clone());
        dst.configure_snapshots(&SnapshotConfig {
            retain_snapshots: 1,
            ..SnapshotConfig::DEFAULT
        });
        seed(&dst, 9).await;

        let installing = {
            let (src, dst, meta) = (src.clone(), dst.clone(), meta.clone());
            tokio::spawn(async move { transfer(&src, &dst, &meta, |_| {}).await })
        };
        race.reached(false).await;
        let building = {
            let dst = dst.clone();
            tokio::spawn(async move { build(&dst).await })
        };
        race.reached(true).await;
        let deadline = std::time::Instant::now() + RACE_PATIENCE;
        while build_view_exists(dst_dir.path()) && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        race.release_all();
        let installed = installing.await.expect("the install task joins");
        let built = building.await.expect("the build task joins");

        installed.expect("the install finds the file it renamed in");
        assert!(
            !dst.is_poisoned(),
            "the install did not fail mid-replacement"
        );
        built.expect("the build publishes once the install has committed");
        assert_eq!(dst.reader().state_hash(), expected_hash);
    }
}

/// Holds the first snapshot capture open, and removes `victim` (when set) at the first
/// [`FaultInjector::after_snapshot_export`], as a prune would between the publish check and the
/// open (#68 A-2).
struct PruneBeforeOpen {
    capture_seen: AtomicU64,
    capture: Handshake,
    victim: Mutex<Option<std::path::PathBuf>>,
    removed: AtomicU64,
}

impl FaultInjector for PruneBeforeOpen {
    fn before(&self, _boundary: Boundary) -> FaultAction {
        FaultAction::Proceed
    }

    fn during_snapshot_capture(&self) {
        if self.capture_seen.fetch_add(1, Ordering::SeqCst) == 0 {
            self.capture.arrive(true);
        }
    }

    fn after_snapshot_export(&self) {
        if let Some(victim) = self.victim.lock().unwrap().take() {
            std::fs::remove_file(&victim).expect("the current snapshot's file exists");
            self.removed.fetch_add(1, Ordering::SeqCst);
        }
    }
}

/// Wait on the store-side half of `handshake` from a blocking thread.
async fn wait_reached<T: Send + Sync + 'static>(owner: Arc<T>, pick: fn(&T) -> &Handshake) {
    tokio::task::spawn_blocking(move || {
        pick(&owner)
            .reached_rx
            .lock()
            .unwrap()
            .recv_timeout(RACE_PATIENCE)
            .expect("the operation reaches its hook")
    })
    .await
    .expect("wait task joins");
}

/// #68 A-2: a build whose view an install superseded hands OpenRaft the current snapshot. If
/// that file is pruned between the publish check and the open, the build captures again rather
/// than failing: an `Err` from `build_snapshot` is fatal to the node, and nothing here failed.
#[retcd_test]
async fn a2_a_superseded_build_whose_file_was_pruned_captures_again() {
    let src_dir = tempfile::tempdir().unwrap();
    let dst_dir = tempfile::tempdir().unwrap();
    {
        let src = open_as(src_dir.path(), identity_for(7, 1), Arc::new(NoFaults));
        seed(&src, 6).await;
        let meta = build(&src).await.expect("source build succeeds");

        let hook = Arc::new(PruneBeforeOpen {
            capture_seen: AtomicU64::new(0),
            capture: Handshake::new(),
            victim: Mutex::new(None),
            removed: AtomicU64::new(0),
        });
        let dst = open_as(dst_dir.path(), identity_for(7, 2), hook.clone());
        seed(&dst, 3).await;

        let building = {
            let dst = dst.clone();
            tokio::spawn(async move { build(&dst).await })
        };
        wait_reached(hook.clone(), |h| &h.capture).await;

        // The checkpoint holds index 3. Install index 6 while it is held, so the build's view is
        // superseded, and arm the prune of the installed file.
        let installed = transfer(&src, &dst, &meta, |_| {}).await;
        *hook.victim.lock().unwrap() = Some(snap_path(&dst, &meta));
        hook.capture.release();
        let built = building.await.expect("the build task joins");
        installed.expect("the install succeeds");

        assert_eq!(
            hook.removed.load(Ordering::SeqCst),
            1,
            "the superseded snapshot's file was removed before the open"
        );
        let built = built.expect("a pruned superseded file costs a capture, not the node");
        assert_eq!(
            built.last_log_id, meta.last_log_id,
            "the new capture covers the installed state"
        );
        assert_eq!(
            dst.snapshot_meta().map(|m| m.snapshot_id),
            Some(built.snapshot_id),
            "the build published its own file"
        );
    }
}

/// Parks each of the first two crossings of [`Boundary::AfterInstallDropCf`] (two installs)
/// and holds the second snapshot capture, each on its own handshake (Q-R1).
struct TwoInstalls {
    boundary_seen: AtomicU64,
    capture_seen: AtomicU64,
    first: Handshake,
    second: Handshake,
    capture: Handshake,
}

impl FaultInjector for TwoInstalls {
    fn before(&self, boundary: Boundary) -> FaultAction {
        if boundary == Boundary::AfterInstallDropCf {
            match self.boundary_seen.fetch_add(1, Ordering::SeqCst) {
                0 => self.first.arrive(true),
                1 => self.second.arrive(true),
                _ => {}
            }
        }
        FaultAction::Proceed
    }

    fn during_snapshot_capture(&self) {
        if self.capture_seen.fetch_add(1, Ordering::SeqCst) == 1 {
            self.capture.arrive(true);
        }
    }
}

/// Q-R1: a build's patience for an install restarts when a **new** install begins.
///
/// The patience is shortened to 1.5 s. The build waits on install 1 for 1 s, then, after one
/// fresh capture, on install 2 for another 1 s. Neither wait reaches the patience, but the two
/// together pass it. Before the fix the clock counted from the build's start, so the patience ran
/// out during install 2 and the build spent an attempt; with every attempt spent it returns the
/// error OpenRaft turns into a fatal one. The row requires that **no** attempt is spent: whether
/// a clock that is still wrong spends all three depends on how long a capture takes. The holds
/// are real time because the patience is a clock: this row takes about 2 s by design.
#[retcd_test]
async fn q_r1_the_install_patience_restarts_for_each_new_install() {
    const PATIENCE: std::time::Duration = std::time::Duration::from_millis(1500);
    const HOLD: std::time::Duration = std::time::Duration::from_millis(1000);
    let src_dir = tempfile::tempdir().unwrap();
    let dst_dir = tempfile::tempdir().unwrap();
    {
        let src = open_as(src_dir.path(), identity_for(7, 1), Arc::new(NoFaults));
        seed(&src, 6).await;
        let first = build(&src).await.expect("first source build");
        let first_bytes = std::fs::read(snap_path(&src, &first)).unwrap();
        seed_range(&src, 7, 9).await;
        let second = build(&src).await.expect("second source build");
        let second_bytes = std::fs::read(snap_path(&src, &second)).unwrap();
        let expected_hash = src.reader().state_hash();

        let race = Arc::new(TwoInstalls {
            boundary_seen: AtomicU64::new(0),
            capture_seen: AtomicU64::new(0),
            first: Handshake::new(),
            second: Handshake::new(),
            capture: Handshake::new(),
        });
        let dst = open_as(dst_dir.path(), identity_for(7, 2), race.clone());
        dst.set_install_patience(PATIENCE);
        seed(&dst, 3).await;

        let install_one = {
            let (dst, meta) = (dst.clone(), first.clone());
            tokio::spawn(async move { install_bytes(&dst, &meta, &first_bytes).await })
        };
        wait_reached(race.clone(), |r| &r.first).await;
        let building = {
            let dst = dst.clone();
            tokio::spawn(async move { build(&dst).await })
        };
        tokio::time::sleep(HOLD).await;
        race.first.release();
        install_one
            .await
            .expect("install 1 joins")
            .expect("install 1 commits");

        // The build captures again; hold that capture until install 2 is parked mid-install, so
        // the capture's publish meets install 2 in flight.
        wait_reached(race.clone(), |r| &r.capture).await;
        let install_two = {
            let (dst, meta) = (dst.clone(), second.clone());
            tokio::spawn(async move { install_bytes(&dst, &meta, &second_bytes).await })
        };
        wait_reached(race.clone(), |r| &r.second).await;
        race.capture.release();
        tokio::time::sleep(HOLD).await;
        race.second.release();
        install_two
            .await
            .expect("install 2 joins")
            .expect("install 2 commits");

        let built = building.await.expect("the build task joins");
        assert_eq!(
            dst.metrics().snapshot_build_retries,
            0,
            "no install was waited on for the patience, so no attempt was spent"
        );
        let built = built
            .expect("each install was waited on for less than the patience, so the build succeeds");
        assert_eq!(built.last_log_id, second.last_log_id);
        assert_eq!(dst.reader().state_hash(), expected_hash);
    }
}

/// #68 C5-06, the abort half: the mirror now goes up before the rename, so an install that
/// fails after that point must take it down again. Left up, it names an install that will never
/// commit, and every later build waits on it until the patience runs out and then fails, which
/// OpenRaft treats as fatal. The install here fails at its marker, after the rename.
#[retcd_test]
async fn c5_06b_an_install_that_fails_after_the_rename_leaves_no_install_behind() {
    let src_dir = tempfile::tempdir().unwrap();
    let dst_dir = tempfile::tempdir().unwrap();
    {
        let src = open_as(src_dir.path(), identity_for(7, 1), Arc::new(NoFaults));
        seed(&src, 6).await;
        let meta = build(&src).await.expect("source build succeeds");

        let dst = open_as(
            dst_dir.path(),
            identity_for(7, 2),
            FailAt::new(Boundary::BeforeInstallMarker, FaultAction::Fail),
        );
        // Short, so a build that waits on a dead install fails in well under a second.
        dst.set_install_patience(std::time::Duration::from_millis(300));
        seed(&dst, 3).await;

        transfer(&src, &dst, &meta, |_| {})
            .await
            .expect_err("the install fails at its marker");
        assert!(!dst.is_poisoned(), "a Fail at the marker does not poison");

        let built = build(&dst)
            .await
            .expect("no install is in flight after the failed one, so the build publishes");
        assert_eq!(
            dst.snapshot_meta().map(|m| m.snapshot_id),
            Some(built.snapshot_id)
        );
    }
}
