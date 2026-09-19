//! `RocksStore` unit tests (test plan TA-13..TA-16, ADR-0008, ADR-0011, ADR-0016).
//!
//! These exercise the store directly, without OpenRaft and without a cluster: identity
//! binding, restart correctness, the eight fault boundaries, explicit sync accounting, and
//! crash poisoning. The cluster-level M2-xx rows live in the workspace test crate and are
//! driven through the testkit, which is why these are named `m2_storage_NN_…`.
//!
//! Every test owns a `TempDir`. On Windows a directory whose RocksDB is still open cannot be
//! removed, so every store (and every `RocksLog`/`RocksSm` handle cloned from it) is dropped
//! before the directory is — that is what the explicit scopes below are for.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use config_core::{
    ClusterId, ClusterIdentity, Command, Durability, KvState, Limits, NodeId, RecoveryEpoch,
};
use config_log::retcd_test;
use config_storage::{
    Boundary, FaultAction, FaultInjector, NoFaults, NoopSink, RaftNodeId, RocksOptions, RocksStore,
    StorageOpenError, TypeConfig, CF_RAFT_LOG, CF_STATE_META, FORMAT_VERSION,
};
use openraft::storage::{RaftLogStorage, RaftLogStorageExt, RaftStateMachine};
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId, RaftLogReader, Vote};
use tracing::Span;

// --- fixtures ---------------------------------------------------------------------------

fn identity() -> ClusterIdentity {
    ClusterIdentity {
        cluster_id: ClusterId::from_bytes([7u8; 16]),
        recovery_epoch: RecoveryEpoch(0),
        node_id: NodeId(1),
    }
}

fn open_at(dir: &Path, faults: Arc<dyn FaultInjector>) -> RocksStore {
    RocksStore::open(dir, identity(), Limits::DEFAULT, faults, Span::none()).expect("store opens")
}

fn open_plain(dir: &Path) -> RocksStore {
    open_at(dir, Arc::new(NoFaults))
}

fn log_id(term: u64, index: u64) -> LogId<RaftNodeId> {
    LogId::new(CommittedLeaderId::new(term, 1), index)
}

fn blank(term: u64, index: u64) -> Entry<TypeConfig> {
    Entry {
        log_id: log_id(term, index),
        payload: EntryPayload::Blank,
    }
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

/// Records every boundary it is asked about, in order.
#[derive(Default)]
struct Recorder(Mutex<Vec<Boundary>>);

impl FaultInjector for Recorder {
    fn before(&self, boundary: Boundary) -> FaultAction {
        self.0.lock().unwrap().push(boundary);
        FaultAction::Proceed
    }
}

// --- open, identity, freshness ----------------------------------------------------------

#[retcd_test]
async fn m2_storage_01_fresh_open_reports_fresh_and_persistent() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let s = open_plain(tmp.path());
        assert!(s.is_fresh(), "an empty directory opens fresh");
        assert_eq!(s.identity(), identity());
        assert_eq!(s.durability(), Durability::Persistent);
        assert_eq!(s.applied_commands(), 0);
        assert_eq!(s.raft_log_len(), 0);
        assert_eq!(s.sync_count(), 0);
        assert!(!s.is_poisoned());
        assert_eq!(s.path(), tmp.path());
    }
}

#[retcd_test]
async fn m2_storage_02_identity_round_trips_and_binds_the_directory() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let s = open_plain(tmp.path());
        assert!(s.is_fresh());
    }
    {
        // The identity written on the first open is itself enough to make the directory
        // non-fresh: `form_cluster` must not run twice on the same data dir (ADR-0011).
        let s = open_plain(tmp.path());
        assert_eq!(s.identity(), identity());
        assert!(!s.is_fresh(), "a bound directory is no longer fresh");
    }
}

#[retcd_test]
async fn m2_storage_03_identity_mismatch_blocks_reopen() {
    let base = identity();
    let variants = [
        (
            "cluster_id",
            ClusterIdentity {
                cluster_id: ClusterId::from_bytes([9u8; 16]),
                ..base
            },
        ),
        (
            "node_id",
            ClusterIdentity {
                node_id: NodeId(2),
                ..base
            },
        ),
        (
            "recovery_epoch",
            ClusterIdentity {
                recovery_epoch: RecoveryEpoch(1),
                ..base
            },
        ),
    ];

    for (field, wrong) in variants {
        let tmp = tempfile::tempdir().unwrap();
        drop(open_plain(tmp.path()));

        let err = RocksStore::open(
            tmp.path(),
            wrong,
            Limits::DEFAULT,
            Arc::new(NoFaults),
            Span::none(),
        )
        .expect_err("a different identity must not open the directory");

        match err {
            StorageOpenError::IdentityMismatch {
                stored, configured, ..
            } => {
                assert_eq!(stored, base, "{field}: stored identity is reported");
                assert_eq!(
                    configured, wrong,
                    "{field}: configured identity is reported"
                );
            }
            other => panic!("{field}: expected IdentityMismatch, got {other:?}"),
        }

        // The refused open released the lock, so the rightful owner can still start.
        let s = open_plain(tmp.path());
        assert_eq!(s.identity(), base);
    }
}

#[retcd_test]
async fn m2_storage_04_missing_or_unexpected_column_family_is_typed() {
    // A directory holding only `raft_log` must not have the other three auto-created over it.
    let missing = tempfile::tempdir().unwrap();
    {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let db = rocksdb::DB::open_cf_descriptors(
            &opts,
            missing.path(),
            vec![rocksdb::ColumnFamilyDescriptor::new(
                CF_RAFT_LOG,
                rocksdb::Options::default(),
            )],
        )
        .expect("bare db");
        drop(db);
    }
    let err = RocksStore::open(
        missing.path(),
        identity(),
        Limits::DEFAULT,
        Arc::new(NoFaults),
        Span::none(),
    )
    .expect_err("a partial schema must be refused");
    match &err {
        StorageOpenError::MissingColumnFamily { name, .. } => {
            assert!(
                ["raft_meta", "kv", "state_meta"].contains(&name.as_str()),
                "unexpected name {name}"
            );
        }
        other => panic!("expected MissingColumnFamily, got {other:?}"),
    }

    // An unallocated CF means the directory belongs to a later schema version (spec §17).
    // `events` played this role until M4 and `dedup` until M5; both are real families now, so
    // the assertion moves on to a name no milestone has claimed.
    let extra = tempfile::tempdir().unwrap();
    drop(open_plain(extra.path()));
    {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let descriptors: Vec<rocksdb::ColumnFamilyDescriptor> = config_storage::COLUMN_FAMILIES
            .iter()
            .chain(std::iter::once(&"future"))
            .map(|n| rocksdb::ColumnFamilyDescriptor::new(*n, rocksdb::Options::default()))
            .collect();
        let db = rocksdb::DB::open_cf_descriptors(&opts, extra.path(), descriptors)
            .expect("db with an extra cf");
        drop(db);
    }
    let err = RocksStore::open(
        extra.path(),
        identity(),
        Limits::DEFAULT,
        Arc::new(NoFaults),
        Span::none(),
    )
    .expect_err("a later schema version must be refused");
    match &err {
        StorageOpenError::UnexpectedColumnFamily { name, .. } => assert_eq!(name, "future"),
        other => panic!("expected UnexpectedColumnFamily, got {other:?}"),
    }
}

#[retcd_test]
async fn m2_storage_05_locked_directory_fails_fast_and_typed() {
    let tmp = tempfile::tempdir().unwrap();
    let held = open_plain(tmp.path());

    let err = RocksStore::open(
        tmp.path(),
        identity(),
        Limits::DEFAULT,
        Arc::new(NoFaults),
        Span::none(),
    )
    .expect_err("a second open of a live directory must fail");
    assert!(
        matches!(err, StorageOpenError::Locked { .. }),
        "expected Locked, got {err:?}"
    );
    assert!(
        err.to_string().contains(&tmp.path().display().to_string()),
        "the error names the directory: {err}"
    );

    drop(held);
    // Once the holder is gone the directory reopens, which is what `Cluster::restart` relies on.
    drop(open_plain(tmp.path()));
}

// --- restart correctness ----------------------------------------------------------------

#[retcd_test]
async fn m2_storage_06_vote_and_committed_persist_across_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let vote = Vote::new(7, 1);
    {
        let s = open_plain(tmp.path());
        let mut log = s.log_store();
        assert_eq!(log.read_vote().await.unwrap(), None);
        assert_eq!(log.read_committed().await.unwrap(), None);

        log.save_vote(&vote).await.unwrap();
        log.save_committed(Some(log_id(7, 42))).await.unwrap();
        assert!(!s.is_fresh(), "a saved vote makes the store non-fresh");
    }
    {
        let s = open_plain(tmp.path());
        let mut log = s.log_store();
        assert_eq!(log.read_vote().await.unwrap(), Some(vote));
        assert_eq!(log.read_committed().await.unwrap(), Some(log_id(7, 42)));
    }
}

#[retcd_test]
async fn m2_storage_07_appended_entries_reload_identically() {
    let tmp = tempfile::tempdir().unwrap();
    let entries: Vec<Entry<TypeConfig>> = (1..=8)
        .map(|i| put(1, i, &format!("/k{i}"), &format!("v{i}")))
        .collect();

    {
        let s = open_plain(tmp.path());
        let mut log = s.log_store();
        log.blocking_append(entries.clone()).await.unwrap();
        assert_eq!(s.raft_log_len(), 8);
        assert_eq!(
            log.get_log_state().await.unwrap().last_log_id,
            Some(log_id(1, 8))
        );
    }
    {
        let s = open_plain(tmp.path());
        let mut log = s.log_store();
        assert_eq!(s.raft_log_len(), 8);
        let state = log.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id, Some(log_id(1, 8)));
        assert_eq!(state.last_purged_log_id, None);

        let reloaded = log.try_get_log_entries(1..9).await.unwrap();
        assert_eq!(reloaded.len(), 8);
        for (want, got) in entries.iter().zip(reloaded.iter()) {
            assert_eq!(want.log_id, got.log_id);
            assert_eq!(payload_bytes(want), payload_bytes(got));
        }

        // A sub-range must not over- or under-read.
        let middle = log.try_get_log_entries(3..6).await.unwrap();
        assert_eq!(
            middle.iter().map(|e| e.log_id.index).collect::<Vec<_>>(),
            vec![3, 4, 5]
        );
    }
}

/// Compare entries by the canonical command bytes (ADR-0007), not by a serde shape.
fn payload_bytes(entry: &Entry<TypeConfig>) -> Option<Vec<u8>> {
    match &entry.payload {
        EntryPayload::Normal(cmd) => Some(cmd.encode()),
        _ => None,
    }
}

#[retcd_test]
async fn m2_storage_08_truncate_and_purge_persist_across_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let s = open_plain(tmp.path());
        let mut log = s.log_store();
        log.blocking_append((1..=10).map(|i| blank(1, i)).collect::<Vec<_>>())
            .await
            .unwrap();
        // Truncate is inclusive of the given index.
        log.truncate(log_id(1, 6)).await.unwrap();
        assert_eq!(s.raft_log_len(), 5);
        assert_eq!(
            log.get_log_state().await.unwrap().last_log_id,
            Some(log_id(1, 5))
        );
        // Truncating then appending at the freed index must be accepted, with no hole.
        log.blocking_append(vec![blank(2, 6)]).await.unwrap();
        assert_eq!(s.raft_log_len(), 6);
    }
    {
        let s = open_plain(tmp.path());
        let mut log = s.log_store();
        assert_eq!(s.raft_log_len(), 6);
        assert_eq!(
            log.get_log_state().await.unwrap().last_log_id,
            Some(log_id(2, 6))
        );

        // Purge is refused above `last_applied`, because it would destroy the replay window.
        let err = log
            .purge(log_id(2, 6))
            .await
            .expect_err("purge above last_applied");
        assert!(err.to_string().contains("last_applied"), "{err}");
        assert_eq!(s.raft_log_len(), 6, "a refused purge removes nothing");

        // With entries applied, a purge below `last_applied` is accepted and persists.
        s.state_machine()
            .apply((1..=4).map(|i| blank(1, i)).collect::<Vec<_>>())
            .await
            .unwrap();
        log.purge(log_id(1, 3)).await.unwrap();
        assert_eq!(s.raft_log_len(), 3);
    }
    {
        let s = open_plain(tmp.path());
        let state = s.log_store().get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(log_id(1, 3)));
        assert_eq!(state.last_log_id, Some(log_id(2, 6)));
        assert_eq!(s.raft_log_len(), 3);
    }
}

#[retcd_test]
async fn m2_storage_09_applied_state_reloads_with_the_same_state_hash() {
    let tmp = tempfile::tempdir().unwrap();
    let commands = [
        put_cmd("/z", "9"),
        put_cmd("/a", "1"),
        Command::Delete {
            key: Bytes::from_static(b"/z"),
            expected_mod_revision: None,
            dedup: None,
        },
        put_cmd("/a", "2"),
        put_cmd("/b", ""),
    ];

    let (hash_before, revision_before) = {
        let s = open_plain(tmp.path());
        let mut entries = vec![blank(1, 1)];
        for (i, cmd) in commands.iter().enumerate() {
            entries.push(Entry {
                log_id: log_id(1, 2 + i as u64),
                payload: EntryPayload::Normal(cmd.clone()),
            });
        }
        let responses = s.state_machine().apply(entries).await.unwrap();
        assert_eq!(
            responses.len(),
            commands.len() + 1,
            "one response per entry"
        );
        assert_eq!(s.applied_commands(), commands.len() as u64);
        (s.reader().state_hash(), s.reader().cluster_revision())
    };

    // The oracle: a plain `KvState` fed the same commands.
    let mut expected = KvState::with_limits(Limits::DEFAULT);
    for cmd in &commands {
        expected.apply(cmd);
    }
    assert_eq!(hash_before, expected.state_hash());
    assert_eq!(revision_before, expected.cluster_revision());

    {
        let s = open_plain(tmp.path());
        assert_eq!(
            s.reader().state_hash(),
            expected.state_hash(),
            "a reopened store hashes identically to a replayed one"
        );
        assert_eq!(s.reader().cluster_revision(), expected.cluster_revision());
        assert_eq!(
            s.state_machine().applied_state().await.unwrap().0,
            Some(log_id(1, 1 + commands.len() as u64))
        );
        assert_eq!(
            s.applied_commands(),
            0,
            "the counter is per open, and nothing was applied since"
        );
    }
}

#[retcd_test]
async fn m2_storage_10_membership_and_revision_survive_reopen() {
    use config_storage::RaftNode;
    use openraft::Membership;
    use std::collections::BTreeSet;

    let tmp = tempfile::tempdir().unwrap();
    let voters: BTreeSet<u64> = [1, 2, 3].into_iter().collect();
    let nodes: BTreeMap<u64, RaftNode> = voters
        .iter()
        .map(|id| (*id, RaftNode::same(format!("inproc://{id}"))))
        .collect();

    {
        let s = open_plain(tmp.path());
        s.state_machine()
            .apply(vec![
                Entry {
                    log_id: log_id(1, 1),
                    payload: EntryPayload::Membership(Membership::new(
                        vec![voters.clone()],
                        nodes.clone(),
                    )),
                },
                put(1, 2, "/a", "1"),
            ])
            .await
            .unwrap();
    }
    {
        let s = open_plain(tmp.path());
        let (last_applied, membership) = s.state_machine().applied_state().await.unwrap();
        assert_eq!(last_applied, Some(log_id(1, 2)));
        assert_eq!(membership.voter_ids().collect::<BTreeSet<_>>(), voters);
        assert_eq!(membership.log_id(), &Some(log_id(1, 1)));
        assert_eq!(s.reader().cluster_revision(), 1);
        assert_eq!(s.reader().membership().log_id(), &Some(log_id(1, 1)));
    }
}

#[retcd_test]
async fn m2_storage_11_committed_above_last_applied_survives_reopen() {
    // The committed-but-unapplied window is what makes §9.3.6 replay possible. OpenRaft's
    // default `save_committed` is a no-op, which would silently disable it (research §8.2).
    let tmp = tempfile::tempdir().unwrap();
    {
        let s = open_plain(tmp.path());
        let mut log = s.log_store();
        log.blocking_append((1..=5).map(|i| put(1, i, "/k", "v")).collect::<Vec<_>>())
            .await
            .unwrap();
        log.save_committed(Some(log_id(1, 5))).await.unwrap();
        s.state_machine()
            .apply(vec![put(1, 1, "/k", "v"), put(1, 2, "/k", "v")])
            .await
            .unwrap();
    }
    {
        let s = open_plain(tmp.path());
        let committed = s.log_store().read_committed().await.unwrap();
        let (last_applied, _) = s.state_machine().applied_state().await.unwrap();

        assert_eq!(committed, Some(log_id(1, 5)));
        assert_eq!(last_applied, Some(log_id(1, 2)));
        assert!(
            last_applied.unwrap().index < committed.unwrap().index,
            "the store reports both, so OpenRaft can replay (last_applied, committed]"
        );
        // The replay window is readable, contiguous, and in order.
        let window = s.log_store().try_get_log_entries(3..=5).await.unwrap();
        assert_eq!(
            window.iter().map(|e| e.log_id.index).collect::<Vec<_>>(),
            vec![3, 4, 5]
        );
        assert!(
            s.log_store()
                .get_log_state()
                .await
                .unwrap()
                .last_log_id
                .unwrap()
                .index
                >= last_applied.unwrap().index,
            "last_log_id must never under-report last_applied, or OpenRaft deletes entries"
        );
    }
}

#[retcd_test]
async fn m2_storage_12_append_refuses_to_leave_a_hole() {
    let tmp = tempfile::tempdir().unwrap();
    let s = open_plain(tmp.path());
    let mut log = s.log_store();

    log.blocking_append(vec![blank(1, 1), blank(1, 2)])
        .await
        .unwrap();

    let err = log
        .blocking_append(vec![blank(1, 4)])
        .await
        .expect_err("an index gap must be refused, not silently written");
    assert!(err.to_string().contains("hole"), "{err}");
    assert_eq!(s.raft_log_len(), 2, "the refused append wrote nothing");
    assert!(!s.is_poisoned(), "a rejected append is not a crash");

    // A gap inside one batch is refused too.
    let err = log
        .blocking_append(vec![blank(1, 3), blank(1, 5)])
        .await
        .expect_err("an intra-batch gap must be refused");
    assert!(err.to_string().contains("hole"), "{err}");

    log.blocking_append(vec![blank(1, 3)]).await.unwrap();
    assert_eq!(s.raft_log_len(), 3);
}

// --- fault boundaries --------------------------------------------------------------------

#[retcd_test]
async fn m2_storage_13_every_boundary_is_crossed_in_order() {
    let tmp = tempfile::tempdir().unwrap();
    let recorder = Arc::new(Recorder::default());
    let s = open_at(tmp.path(), recorder.clone());

    s.log_store().save_vote(&Vote::new(1, 1)).await.unwrap();
    s.log_store()
        .blocking_append(vec![put(1, 1, "/a", "1")])
        .await
        .unwrap();
    s.state_machine()
        .apply(vec![put(1, 1, "/a", "1")])
        .await
        .unwrap();

    assert_eq!(
        recorder.0.lock().unwrap().clone(),
        WRITE_PATH_BOUNDARIES.to_vec()
    );
    let counters = s.counters();
    for b in WRITE_PATH_BOUNDARIES {
        assert_eq!(counters.get(b), 1, "{b} crossed exactly once");
    }
    assert_eq!(counters.total(), 9);
}

#[retcd_test]
async fn m2_storage_14_fail_at_each_boundary_leaves_the_store_usable() {
    for boundary in WRITE_PATH_BOUNDARIES {
        let tmp = tempfile::tempdir().unwrap();
        let s = open_at(tmp.path(), FailAt::new(boundary, FaultAction::Fail));

        let vote = s.log_store().save_vote(&Vote::new(1, 1)).await;
        let appended = s
            .log_store()
            .blocking_append(vec![put(1, 1, "/a", "1")])
            .await;
        let applied = s.state_machine().apply(vec![put(1, 1, "/a", "1")]).await;

        let failed = [vote.is_err(), appended.is_err(), applied.is_err()];
        assert_eq!(
            failed.iter().filter(|f| **f).count(),
            1,
            "{boundary}: exactly one operation should fail, got {failed:?}"
        );
        assert!(!s.is_poisoned(), "{boundary}: Fail must not poison");
        assert_eq!(s.counters().get(boundary), 1, "{boundary}: was reached");

        // The store still works: a second attempt succeeds.
        s.log_store().save_vote(&Vote::new(2, 1)).await.unwrap();
        let responses = s
            .state_machine()
            .apply(vec![put(2, 9, "/later", "ok")])
            .await
            .unwrap();
        assert_eq!(responses.len(), 1);
        assert!(
            responses[0].mutation().is_some_and(|m| m.is_applied()),
            "{boundary}: store still applies"
        );
    }
}

/// What must be true after a crash at each boundary, once the directory is reopened.
///
/// `entry_present == None` means "either" — an unsynced `write_opt` has already handed the
/// bytes to the operating system, so a same-process crash cannot take them back (M2-22).
struct CrashExpectation {
    vote_term: u64,
    entry_present: Option<bool>,
    last_applied: u64,
    cluster_revision: u64,
}

/// The boundaries the vote / append / apply workload below actually crosses.
///
/// The M5 snapshot, install and purge boundaries are not in this list because this workload
/// never builds, receives or purges anything — arming a crash on one of them would assert that
/// a boundary "was reached" when nothing could have reached it. They are driven by
/// `m5_snapshot.rs` instead, which performs the operations that cross them.
const WRITE_PATH_BOUNDARIES: [Boundary; 9] = [
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

fn crash_expectation(boundary: Boundary) -> CrashExpectation {
    match boundary {
        // The new vote never reached the disk: the node keeps the vote it can remember.
        Boundary::BeforeVoteSync => CrashExpectation {
            vote_term: 1,
            entry_present: Some(false),
            last_applied: 2,
            cluster_revision: 2,
        },
        Boundary::AfterVoteSync => CrashExpectation {
            vote_term: 5,
            entry_present: Some(false),
            last_applied: 2,
            cluster_revision: 2,
        },
        Boundary::BeforeLogAppend => CrashExpectation {
            vote_term: 5,
            entry_present: Some(false),
            last_applied: 2,
            cluster_revision: 2,
        },
        // Written, not synced: presence is not promised either way, but the flush callback
        // never fired so nothing was acknowledged.
        Boundary::AfterLogAppend | Boundary::BeforeLogFlush => CrashExpectation {
            vote_term: 5,
            entry_present: None,
            last_applied: 2,
            cluster_revision: 2,
        },
        // Synced: the entry is durable even though the callback never fired.
        Boundary::AfterLogFlush | Boundary::BeforeStateBatch => CrashExpectation {
            vote_term: 5,
            entry_present: Some(true),
            last_applied: 2,
            cluster_revision: 2,
        },
        // The whole batch — record, revision, last_applied, journal — is durable. The publish
        // boundary sits after the write, so crashing at either leaves the same disk.
        // Not crossed by this workload; `WRITE_PATH_BOUNDARIES` is what the matrix iterates.
        // Listed rather than folded into a wildcard so that adding a boundary to the enum
        // forces a decision here instead of silently inheriting someone else's expectation.
        Boundary::BeforeSnapshotTmpSync
        | Boundary::AfterSnapshotRename
        | Boundary::BeforeCurrentSnapshotMeta
        | Boundary::BeforeInstallMarker
        | Boundary::AfterInstallDropCf
        | Boundary::BeforeInstallFinalBatch
        | Boundary::BeforePurge
        | Boundary::AfterPurge => {
            panic!("{boundary} is an M5 boundary; see m5_snapshot.rs")
        }
        Boundary::AfterStateBatch | Boundary::AfterStateBatchBeforePublish => CrashExpectation {
            vote_term: 5,
            entry_present: Some(true),
            last_applied: 3,
            cluster_revision: 3,
        },
    }
}

/// Call each fallible `RaftLogReader` / `RaftLogStorage` / `RaftStateMachine` method once and
/// report its outcome by name, so a failure says which entry point leaked.
async fn poisoned_call_results(s: &RocksStore) -> Vec<(&'static str, Result<(), String>)> {
    async fn r<T, E: std::fmt::Display>(v: Result<T, E>) -> Result<(), String> {
        v.map(|_| ()).map_err(|e| e.to_string())
    }
    vec![
        (
            "try_get_log_entries",
            r(s.log_store().try_get_log_entries(0..10).await).await,
        ),
        (
            "get_log_state",
            r(s.log_store().get_log_state().await).await,
        ),
        (
            "save_vote",
            r(s.log_store().save_vote(&Vote::new(9, 1)).await).await,
        ),
        ("read_vote", r(s.log_store().read_vote().await).await),
        (
            "save_committed",
            r(s.log_store().save_committed(Some(log_id(1, 1))).await).await,
        ),
        (
            "read_committed",
            r(s.log_store().read_committed().await).await,
        ),
        (
            "append",
            r(s.log_store()
                .blocking_append(vec![put(1, 9, "/k9", "v9")])
                .await)
            .await,
        ),
        (
            "truncate",
            r(s.log_store().truncate(log_id(1, 1)).await).await,
        ),
        ("purge", r(s.log_store().purge(log_id(1, 1)).await).await),
        (
            "applied_state",
            r(s.state_machine().applied_state().await).await,
        ),
        (
            "apply",
            r(s.state_machine().apply(vec![put(1, 8, "/k8", "v8")]).await).await,
        ),
        (
            "build_snapshot",
            r({
                use openraft::RaftSnapshotBuilder;
                s.state_machine()
                    .get_snapshot_builder()
                    .await
                    .build_snapshot()
                    .await
            })
            .await,
        ),
        (
            "begin_receiving_snapshot",
            r(s.state_machine().begin_receiving_snapshot().await).await,
        ),
        (
            "install_snapshot",
            r(s.state_machine()
                .install_snapshot(
                    &openraft::SnapshotMeta::default(),
                    Box::new(tokio::fs::File::from_std(tempfile::tempfile().unwrap())),
                )
                .await)
            .await,
        ),
        (
            "get_current_snapshot",
            r(s.state_machine().get_current_snapshot().await).await,
        ),
    ]
}

#[retcd_test]
async fn m2_storage_15_crash_at_each_boundary_poisons_then_reopens_consistently() {
    for boundary in WRITE_PATH_BOUNDARIES {
        let tmp = tempfile::tempdir().unwrap();

        // Seed: vote at term 1, entries 1..=2 appended and applied.
        {
            let s = open_plain(tmp.path());
            s.log_store().save_vote(&Vote::new(1, 1)).await.unwrap();
            s.log_store()
                .blocking_append(vec![put(1, 1, "/k1", "v1"), put(1, 2, "/k2", "v2")])
                .await
                .unwrap();
            s.state_machine()
                .apply(vec![put(1, 1, "/k1", "v1"), put(1, 2, "/k2", "v2")])
                .await
                .unwrap();
        }

        // Crash: drive all eight boundaries with a one-shot crash armed on `boundary`.
        {
            let s = open_at(tmp.path(), FailAt::new(boundary, FaultAction::Crash));
            let _ = s.log_store().save_vote(&Vote::new(5, 1)).await;
            let _ = s
                .log_store()
                .blocking_append(vec![put(1, 3, "/k3", "v3")])
                .await;
            let _ = s.state_machine().apply(vec![put(1, 3, "/k3", "v3")]).await;

            assert!(s.is_poisoned(), "{boundary}: Crash must poison the store");
            assert_eq!(s.counters().get(boundary), 1, "{boundary}: was reached");

            // *Every* fallible trait entry point, not a sample of them: a poisoned store that
            // still answered one call would let OpenRaft believe some part of it survived the
            // crash. `truncate`, `purge` and the two snapshot readers are included.
            for (name, result) in poisoned_call_results(&s).await {
                assert!(
                    result.is_err(),
                    "{boundary}: {name} answered on a poisoned store"
                );
            }
            let err = s
                .state_machine()
                .apply(vec![put(1, 4, "/k4", "v4")])
                .await
                .expect_err("poisoned");
            assert!(err.to_string().contains("poisoned"), "{boundary}: {err}");
        }

        // Reopen: the directory is consistent, and `Drop` flushed nothing of its own.
        let expect = crash_expectation(boundary);
        let s = open_plain(tmp.path());
        let mut log = s.log_store();

        let vote = log.read_vote().await.unwrap().expect("a vote is persisted");
        assert_eq!(
            vote.leader_id.term, expect.vote_term,
            "{boundary}: vote term after reopen"
        );

        let entries = log.try_get_log_entries(1..100).await.unwrap();
        let indexes: Vec<u64> = entries.iter().map(|e| e.log_id.index).collect();
        assert_eq!(
            indexes,
            (1..=indexes.len() as u64).collect::<Vec<_>>(),
            "{boundary}: log indexes are contiguous from 1, got {indexes:?}"
        );
        let has_three = indexes.contains(&3);
        if let Some(want) = expect.entry_present {
            assert_eq!(has_three, want, "{boundary}: visibility of entry 3");
        }

        let (last_applied, _) = s.state_machine().applied_state().await.unwrap();
        assert_eq!(
            last_applied.map(|l| l.index),
            Some(expect.last_applied),
            "{boundary}: last_applied after reopen"
        );
        assert_eq!(
            s.reader().cluster_revision(),
            expect.cluster_revision,
            "{boundary}: cluster_revision after reopen"
        );

        // Research §8.2 startup trap: OpenRaft deletes log entries when the log under-reports.
        let last_log = log.get_log_state().await.unwrap().last_log_id;
        assert!(
            last_log.map(|l| l.index).unwrap_or(0) >= expect.last_applied,
            "{boundary}: last_log_id {last_log:?} must be >= last_applied"
        );

        // Acknowledged mutations from the seed phase are all still readable.
        let hash = s.reader().state_hash();
        let mut oracle = KvState::with_limits(Limits::DEFAULT);
        oracle.apply(&put_cmd("/k1", "v1"));
        oracle.apply(&put_cmd("/k2", "v2"));
        if expect.cluster_revision == 3 {
            oracle.apply(&put_cmd("/k3", "v3"));
        }
        assert_eq!(hash, oracle.state_hash(), "{boundary}: applied state");
    }
}

#[retcd_test]
async fn m2_storage_16_crash_matrix_covers_every_boundary() {
    // Guards against a silently-dropped boundary when the enum grows (test plan M2-27). The
    // enum reached seventeen at M5; nine of them are on the write path this file drives, and
    // the other eight are driven by `m5_snapshot.rs`. Both halves are asserted, so a new
    // variant cannot be absorbed into either set unnoticed.
    assert_eq!(Boundary::ALL.len(), 17);
    assert_eq!(WRITE_PATH_BOUNDARIES.len(), 9);
    for b in WRITE_PATH_BOUNDARIES {
        assert!(Boundary::ALL.contains(&b), "{b} is missing from ALL");
        let _ = crash_expectation(b);
    }
}

// --- sync accounting and capability ------------------------------------------------------

#[retcd_test]
async fn m2_storage_17_sync_count_tracks_vote_flush_and_state_batch() {
    let tmp = tempfile::tempdir().unwrap();
    let s = open_plain(tmp.path());
    assert_eq!(s.sync_count(), 0);

    s.log_store().save_vote(&Vote::new(1, 1)).await.unwrap();
    assert_eq!(s.sync_count(), 1, "one fsync per vote");

    s.log_store()
        .blocking_append(vec![put(1, 1, "/a", "1")])
        .await
        .unwrap();
    assert_eq!(s.sync_count(), 2, "one WAL sync per append batch");

    s.log_store()
        .blocking_append(vec![put(1, 2, "/b", "2")])
        .await
        .unwrap();
    assert_eq!(s.sync_count(), 3);

    s.state_machine()
        .apply(vec![put(1, 1, "/a", "1"), put(1, 2, "/b", "2")])
        .await
        .unwrap();
    assert_eq!(
        s.sync_count(),
        4,
        "one fsync per apply batch, not per entry"
    );

    // The counters are the same oracle, read the other way round (TA-15).
    let counters = s.counters();
    assert_eq!(counters.get(Boundary::AfterVoteSync), 1);
    assert_eq!(counters.get(Boundary::AfterLogFlush), 2);
    assert_eq!(counters.get(Boundary::AfterStateBatch), 1);
    assert_eq!(
        counters.get(Boundary::AfterVoteSync)
            + counters.get(Boundary::AfterLogFlush)
            + counters.get(Boundary::AfterStateBatch),
        s.sync_count()
    );
}

#[retcd_test]
async fn m2_storage_18_sync_disabled_downgrades_the_capability() {
    let tmp = tempfile::tempdir().unwrap();
    let s = RocksStore::open_with(
        tmp.path(),
        identity(),
        Limits::DEFAULT,
        Arc::new(NoFaults),
        Span::none(),
        RocksOptions {
            sync_writes: false,
            ..RocksOptions::DEFAULT
        },
        Arc::new(NoopSink),
    )
    .expect("store opens without sync");

    assert_eq!(
        s.durability(),
        Durability::PersistentUnverified,
        "a node that is not fsyncing must not claim Persistent"
    );
    s.log_store().save_vote(&Vote::new(1, 1)).await.unwrap();
    s.log_store()
        .blocking_append(vec![put(1, 1, "/a", "1")])
        .await
        .unwrap();
    s.state_machine()
        .apply(vec![put(1, 1, "/a", "1")])
        .await
        .unwrap();
    assert_eq!(s.sync_count(), 0, "no fsync was performed");
    // The boundaries still exist, so an injector behaves identically in either mode.
    assert_eq!(s.counters().total(), 9);
}

/// M2-37 as amended at M5: snapshots are no longer "unsupported", so what this row now asserts
/// is the part that never changed — a store with no snapshot answers `Ok(None)` rather than an
/// error, and an `install_snapshot` that nothing opened a receive slot for is refused instead
/// of guessing which file it meant. The build path itself is exercised by `m5_snapshot.rs`.
#[retcd_test]
async fn m2_storage_19_snapshots_are_unsupported_but_never_panic() {
    let tmp = tempfile::tempdir().unwrap();
    let s = open_plain(tmp.path());
    let mut sm = s.state_machine();
    assert!(sm.get_current_snapshot().await.unwrap().is_none());
    assert!(
        sm.install_snapshot(
            &openraft::SnapshotMeta::default(),
            Box::new(tokio::fs::File::from_std(tempfile::tempfile().unwrap()))
        )
        .await
        .is_err(),
        "an install with no receive slot has no file to install"
    );
}

// --- logging -----------------------------------------------------------------------------

#[retcd_test]
async fn m2_storage_20_boundary_lines_carry_test_method_and_node_id() {
    let tmp = tempfile::tempdir().unwrap();
    let node_span = tracing::info_span!("node", node_id = 1u64);
    let s = RocksStore::open(
        tmp.path(),
        identity(),
        Limits::DEFAULT,
        Arc::new(NoFaults),
        node_span,
    )
    .expect("store opens");

    s.log_store().save_vote(&Vote::new(1, 1)).await.unwrap();
    s.log_store()
        .blocking_append(vec![put(1, 1, "/a", "1")])
        .await
        .unwrap();
    s.state_machine()
        .apply(vec![put(1, 1, "/a", "1")])
        .await
        .unwrap();
    drop(s);

    let path = config_log::layer::test_file_path(
        &config_log::testing::test_log_dir(),
        module_path!(),
        "m2_storage_20_boundary_lines_carry_test_method_and_node_id",
    );
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
    // The file accumulates across `cargo test` invocations, so scope to this process run.
    let run = config_log::testing::test_run_id();
    let lines: Vec<serde_json::Value> = text
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter(|l| l["testRun"] == run)
        .collect();
    assert!(!lines.is_empty(), "no lines in {}", path.display());

    let opened: Vec<&serde_json::Value> =
        lines.iter().filter(|l| l["@m"] == "store_opened").collect();
    assert_eq!(opened.len(), 1, "exactly one store_opened line");
    assert_eq!(opened[0]["node_id"], 1);
    assert_eq!(opened[0]["fresh"], true);
    assert!(opened[0]["identity"].is_string(), "identity is rendered");

    let boundaries: Vec<&serde_json::Value> = lines
        .iter()
        .filter(|l| l["@m"] == "storage boundary")
        .collect();
    assert_eq!(
        boundaries.len(),
        5,
        "one line per After* boundary crossed (vote, append, flush, state batch, publish)"
    );
    for line in &boundaries {
        assert_eq!(
            line["testMethod"],
            "m2_storage_20_boundary_lines_carry_test_method_and_node_id"
        );
        assert_eq!(line["node_id"], 1);
        assert!(line["boundary"].is_string(), "boundary field is present");
        assert!(line["synced"].is_boolean(), "synced field is present");
    }
    let names: Vec<&str> = boundaries
        .iter()
        .filter_map(|l| l["boundary"].as_str())
        .collect();
    assert_eq!(
        names,
        vec![
            "after_vote_sync",
            "after_log_append",
            "after_log_flush",
            "after_state_batch",
            "after_state_batch_before_publish"
        ]
    );

    // §15.2 redaction: a log line carries a key digest, never a value.
    for line in &lines {
        assert!(
            line.get("value").is_none(),
            "no line may carry a stored value: {line}"
        );
    }
}

// --- log integrity (test plan §3.4) ------------------------------------------------------

/// M2-31 `append_entries_readable_on_return`: entries written by `append` must be readable
/// (openraft §9.3.3: "when this method returns, the entries must be readable") even when the
/// call ultimately fails — because the write to `raft_log` happens, unsynced, *before*
/// `BeforeLogFlush` is even consulted (`RocksLog::append`, `config-storage/src/rocks.rs`: the
/// `s.write(batch, false, ..)` call precedes the `BeforeLogFlush` boundary by several lines).
/// Arming `Fail` at `BeforeLogFlush` isolates exactly that ordering: the batch is already on
/// disk and readable through `try_get_log_entries` by the time the call reports failure, so
/// visibility never depended on the flush (or its callback) succeeding.
#[retcd_test]
async fn m2_storage_21_append_entries_readable_before_flush_callback() {
    let tmp = tempfile::tempdir().unwrap();
    let s = open_at(
        tmp.path(),
        FailAt::new(Boundary::BeforeLogFlush, FaultAction::Fail),
    );
    let mut log = s.log_store();

    let err = log
        .blocking_append(vec![put(1, 1, "/m2/31/k", "v")])
        .await
        .expect_err("BeforeLogFlush is armed to fail this call");
    assert!(
        !s.is_poisoned(),
        "a Fail (not Crash) at BeforeLogFlush must leave the store usable: {err}"
    );

    let reloaded = log
        .try_get_log_entries(1..2)
        .await
        .expect("the store is not poisoned; reads still work");
    assert_eq!(
        reloaded.len(),
        1,
        "the entry must be readable even though the flush that would confirm it failed"
    );
    assert_eq!(reloaded[0].log_id, log_id(1, 1));
}

/// M2-32 `flush_callback_only_after_sync`: arming `Fail` on `BeforeLogFlush` must deliver
/// `log_io_completed(Err(..))`, never `Ok(())`, and no code path may call `log_io_completed(Ok)`
/// before `AfterLogFlush` is crossed.
///
/// `openraft::storage::callback::LogFlushed::new` is `pub(crate)` to `openraft`, so a
/// downstream test cannot build its own callback and inspect what it received — and
/// `RaftLogStorageExt::blocking_append` (the only public entry point that drives `append`)
/// makes a direct capture moot for the failure case anyway: its body is
/// `self.append(entries, callback).await?; rx.await..`, so the `?` returns as soon as
/// `append`'s own `Result` is `Err`, before the channel the callback feeds is ever polled.
/// What *is* provable, and is the row's actual claim: `RocksLog::append`'s callback call is a
/// single `match &result { Ok(()) => ..Ok(()), Err(e) => ..Err(..) }` on the exact `Result`
/// this function itself returns (`config-storage/src/rocks.rs`), so the callback's payload and
/// `append`'s return value can never diverge by construction — and `AfterLogFlush` is recorded
/// (via `after_boundary`) only *after* `flush_wal` has already run, strictly before that
/// `match`, so an `Ok` callback structurally cannot fire before `AfterLogFlush` is crossed.
/// This test exercises the two runtime-observable halves: `append` never returns a stray `Ok`
/// while `BeforeLogFlush` is failing, and the number of successful (`Ok`) calls exactly equals
/// the number of `AfterLogFlush` crossings recorded — the evidence the row itself names
/// ("counter: `AfterLogFlush` count == count of `Ok` callbacks").
#[retcd_test]
async fn m2_storage_22_flush_callback_mirrors_sync_result_never_ok_on_fail() {
    let tmp = tempfile::tempdir().unwrap();
    let s = open_at(
        tmp.path(),
        FailAt::new(Boundary::BeforeLogFlush, FaultAction::Fail),
    );
    let mut log = s.log_store();
    let counters = s.counters();

    let mut ok_calls: u64 = 0;
    for i in 1..=4u64 {
        match log
            .blocking_append(vec![put(1, i, &format!("/m2/32/{i}"), "v")])
            .await
        {
            Ok(()) => ok_calls += 1,
            Err(e) => assert_eq!(i, 1, "only the first (armed) crossing may fail: {e}"),
        }
    }

    assert_eq!(
        ok_calls, 3,
        "3 of the 4 appends must succeed once the one-shot fault has fired"
    );
    assert_eq!(
        counters.get(Boundary::BeforeLogFlush),
        4,
        "every attempt crosses BeforeLogFlush, whatever the injector decided"
    );
    assert_eq!(
        counters.get(Boundary::AfterLogFlush),
        ok_calls,
        "AfterLogFlush is only reached after a successful flush, one crossing per Ok callback"
    );
}

/// M2-39 `log_state_never_under_reports`: after 50 appends, a crash at `AfterLogFlush`, and a
/// reopen, `get_log_state().last_log_id` must be `>=` the reopened store's applied index — the
/// startup trap this guards against (research §8.2) is openraft deleting log entries whenever
/// `last_log_id < last_applied` on `get_initial_state`, which would happen if a store's log
/// reader under-reported the true on-disk last index after a crash.
///
/// Mutation-test argument (why this row cannot pass by accident): if `RocksLog::get_log_state`
/// or `load_state`'s `last_log_id` scan were changed to read a cached/stale value instead of
/// re-scanning the `raft_log` CF `IteratorMode::End` on open, this test fails — the crash lands
/// at `AfterLogFlush`, i.e. *after* the 50th entry's `write(batch, false, ..)` already put it in
/// the CF (§9.3.3) but before any in-memory `last_log_id` field the crash's `Drop` might have
/// skipped updating could be trusted; only a fresh on-disk scan at reopen sees index 50.
#[retcd_test]
async fn m2_storage_23_log_state_never_under_reports_after_crash_and_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    // Phase 1: 49 plain appends, no injector — `FailAt` fires on the *first* crossing it ever
    // sees, so getting the crash to land on entry 50 specifically means entries 1..=49 must be
    // written by a store that never had the fault armed at all, then a second store (its own
    // fresh `FailAt`, so its own "first crossing" is entry 50's) is opened just for entry 50.
    {
        let s = open_plain(tmp.path());
        let mut log = s.log_store();
        for i in 1..50u64 {
            log.blocking_append(vec![put(1, i, &format!("/m2/39/{i}"), "v")])
                .await
                .expect("warm-up appends run against a plain store");
        }
    }
    // Phase 2: reopen, append entry 50 with `AfterLogFlush` armed to crash on its first (only)
    // crossing in this store's lifetime.
    {
        let s = open_at(
            tmp.path(),
            FailAt::new(Boundary::AfterLogFlush, FaultAction::Crash),
        );
        let mut log = s.log_store();
        let crashed = log
            .blocking_append(vec![put(1, 50, "/m2/39/50", "v")])
            .await;
        assert!(
            crashed.is_err(),
            "the 50th append's AfterLogFlush crossing is armed to crash"
        );
        assert!(s.is_poisoned(), "Crash must poison the store");
        // Drop without any extra write: TA-14 says Drop performs no flush of its own, so the
        // next reopen only ever sees what `write(batch, false, ..)` already committed for entry
        // 50 — never anything Drop might have added on top.
    }
    // Phase 3: reopen plain and check the log state is not under-reporting.
    {
        let s = open_plain(tmp.path());
        let mut log = s.log_store();
        let log_state = log.get_log_state().await.expect("log state reads back");
        let last_log_id = log_state.last_log_id;
        let last_applied = s.reader().last_applied();
        assert_eq!(
            last_log_id.map(|l| l.index),
            Some(50),
            "the crashed entry was already durably written before the crash; the log state \
             must not under-report it after reopen"
        );
        assert!(
            last_log_id.map(|l| l.index) >= last_applied.map(|l| l.index),
            "get_log_state ({last_log_id:?}) must never fall behind applied_state \
             ({last_applied:?}) — openraft deletes log entries on this exact condition"
        );
    }
}

/// Bytes that are invalid as *any* `postcard`-encoded value: `0xFF` sets the varint
/// continuation bit, so the decoder always expects another byte to follow, and running out of
/// buffer produces `DeserializeUnexpectedEnd` regardless of the target type's shape.
///
/// Plain ASCII text (e.g. `b"not a valid postcard-encoded Entry"`) does **not** reliably work
/// for this: every ASCII byte is `< 0x80`, so `postcard`'s varint decoder reads each one as a
/// complete, valid single-byte value, and `postcard::from_bytes` silently ignores trailing
/// unused bytes rather than erroring on them (`postcard::de::from_bytes` doc: "the unused
/// portion (if any) of the byte slice is not returned") — a small struct like `LogId` can
/// decode "successfully" from the first few letters of an English sentence.
const NOT_POSTCARD: &[u8] = &[0xFFu8; 8];

/// M2-60 `corrupt_log_entry_detected_on_open`: garbage bytes in the last `raft_log` CF value
/// must be refused as a typed [`StorageOpenError::Corrupt`] naming the index, never a panic.
/// `load_state` only decodes the *last* entry on open (`IteratorMode::End`, one `postcard`
/// decode), so the corruption must land on that specific key to be caught at open rather than
/// lazily on a later read.
#[retcd_test]
async fn m2_storage_24_corrupt_log_entry_detected_on_open() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let s = open_plain(tmp.path());
        s.log_store()
            .blocking_append(vec![put(1, 1, "/m2/60/a", "v"), put(1, 2, "/m2/60/b", "v")])
            .await
            .unwrap();
    }
    {
        let db = rocksdb::DB::open_cf(
            &rocksdb::Options::default(),
            tmp.path(),
            config_storage::COLUMN_FAMILIES,
        )
        .expect("reopen raw to corrupt a value");
        let cf = db.cf_handle(CF_RAFT_LOG).expect("raft_log cf");
        // The *last* key (index 2) is the one `load_state` actually decodes on open.
        db.put_cf(&cf, 2u64.to_be_bytes(), NOT_POSTCARD)
            .expect("write garbage over index 2's value");
    }

    let err = RocksStore::open(
        tmp.path(),
        identity(),
        Limits::DEFAULT,
        Arc::new(NoFaults),
        Span::none(),
    )
    .expect_err("a garbage log entry must refuse startup, not panic");
    match &err {
        StorageOpenError::Corrupt { what, detail, .. } => {
            assert!(
                what.contains('2'),
                "the error should name the corrupted index (2): {what}"
            );
            assert!(!detail.is_empty(), "the decoder's own complaint is kept");
        }
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

/// M2-61 `corrupt_state_meta_detected_on_open`: garbage bytes in `state_meta/last_applied`
/// must refuse startup with a typed [`StorageOpenError::Corrupt`], never a panic.
#[retcd_test]
async fn m2_storage_25_corrupt_state_meta_last_applied_detected_on_open() {
    let tmp = tempfile::tempdir().unwrap();
    drop(open_plain(tmp.path()));
    {
        let db = rocksdb::DB::open_cf(
            &rocksdb::Options::default(),
            tmp.path(),
            config_storage::COLUMN_FAMILIES,
        )
        .expect("reopen raw to corrupt state_meta");
        let cf = db.cf_handle(CF_STATE_META).expect("state_meta cf");
        db.put_cf(&cf, b"last_applied", NOT_POSTCARD)
            .expect("write garbage over last_applied");
    }

    let err = RocksStore::open(
        tmp.path(),
        identity(),
        Limits::DEFAULT,
        Arc::new(NoFaults),
        Span::none(),
    )
    .expect_err("a garbage state_meta/last_applied must refuse startup, not panic");
    match &err {
        StorageOpenError::Corrupt { what, detail, .. } => {
            assert_eq!(what, "state_meta/last_applied");
            assert!(!detail.is_empty(), "the decoder's own complaint is kept");
        }
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

/// M2-40 `log_indexes_contiguous_after_every_crash`: a direct scan of the `raft_log` CF's keys
/// (big-endian `u64`) must be contiguous — `k[i+1] == k[i] + 1` — after a crash at every one of
/// the 8 boundaries and a reopen. `Cluster::assert_crash_invariants` in `config-testkit` only
/// checks a length bound against `last_log_index` (see
/// `.claude/scratchpad/conversation_memories/retcd-m0-m3-implementation/m1-testkit-cluster-notes.md`,
/// "Vote non-regression and full log-hole detection need a store-level API that does not exist
/// yet"), not a full key-contiguity scan, so this row belongs here rather than being exercised
/// through the harness.
#[retcd_test]
async fn m2_storage_26_log_cf_keys_contiguous_after_crash_at_every_boundary() {
    for boundary in WRITE_PATH_BOUNDARIES {
        let tmp = tempfile::tempdir().unwrap();
        {
            let s = open_at(tmp.path(), FailAt::new(boundary, FaultAction::Crash));
            let mut log = s.log_store();
            log.save_vote(&Vote::new(1, 1)).await.ok();
            for i in 1..=5u64 {
                let _ = log
                    .blocking_append(vec![put(1, i, &format!("/m2/40/{i}"), "v")])
                    .await;
                if s.is_poisoned() {
                    break;
                }
            }
        }
        // Reopen raw (not through `RocksStore::open`, which only decodes the *last* entry) and
        // scan every key in `raft_log` directly — the assertion this row actually names.
        let db = rocksdb::DB::open_cf(
            &rocksdb::Options::default(),
            tmp.path(),
            config_storage::COLUMN_FAMILIES,
        )
        .unwrap_or_else(|e| panic!("reopen raw after crash at {boundary}: {e}"));
        let cf = db.cf_handle(CF_RAFT_LOG).expect("raft_log cf");
        let keys: Vec<u64> = db
            .iterator_cf(&cf, rocksdb::IteratorMode::Start)
            .map(|item| {
                let (key, _) =
                    item.unwrap_or_else(|e| panic!("scan raft_log after {boundary}: {e}"));
                let bytes: [u8; 8] = key.as_ref().try_into().expect("8-byte big-endian key");
                u64::from_be_bytes(bytes)
            })
            .collect();
        for w in keys.windows(2) {
            assert_eq!(
                w[1],
                w[0] + 1,
                "raft_log keys must be contiguous after a crash at {boundary}: {keys:?}"
            );
        }
    }
}

// --- on-disk format version (M2-66..M2-68) ----------------------------------------------

/// Reopen `dir` raw, run `f` against the `state_meta` CF, and drop the handle again.
///
/// Every persisted value except `state_meta/format_version` is a `postcard` encoding of a type
/// `config-storage` does not own, so the marker is the only thing standing between an OpenRaft
/// upgrade and a silently misread store. These rows tamper with it directly, which means
/// bypassing `RocksStore` — and on Windows the raw handle must be gone before the next open.
fn with_raw_state_meta(dir: &Path, f: impl FnOnce(&rocksdb::DB, &rocksdb::ColumnFamily)) {
    let db = rocksdb::DB::open_cf(
        &rocksdb::Options::default(),
        dir,
        config_storage::COLUMN_FAMILIES,
    )
    .expect("reopen raw to touch state_meta");
    let cf = db.cf_handle(CF_STATE_META).expect("state_meta cf");
    f(&db, cf);
}

fn read_format_marker(dir: &Path) -> Option<Vec<u8>> {
    let mut found = None;
    with_raw_state_meta(dir, |db, cf| {
        found = db
            .get_cf(cf, b"format_version")
            .expect("read state_meta/format_version");
    });
    found
}

/// M2-66 `format_version_stamped_on_first_open`: a fresh directory is stamped with
/// [`FORMAT_VERSION`] as a bare little-endian `u32`, and reopening the stamped directory is an
/// ordinary success — the marker is a gate, not a one-shot.
#[retcd_test]
async fn m2_storage_27_format_version_stamped_on_first_open_and_reopen_succeeds() {
    let tmp = tempfile::tempdir().unwrap();
    drop(open_plain(tmp.path()));

    assert_eq!(
        read_format_marker(tmp.path()).as_deref(),
        Some(&FORMAT_VERSION.to_le_bytes()[..]),
        "a first open must stamp state_meta/format_version = {FORMAT_VERSION} (LE u32)"
    );

    let store = open_plain(tmp.path());
    assert_eq!(store.identity(), identity());
    drop(store);

    assert_eq!(
        read_format_marker(tmp.path()).as_deref(),
        Some(&FORMAT_VERSION.to_le_bytes()[..]),
        "reopening must not rewrite or drop the marker"
    );
}

/// M2-67 `unsupported_format_version_refused`: a directory stamped with a version this build
/// does not write is refused by a typed [`StorageOpenError::UnsupportedFormat`] naming both
/// versions — never a best-effort decode of bytes whose layout is unknown.
#[retcd_test]
async fn m2_storage_28_unsupported_format_version_refused() {
    let tmp = tempfile::tempdir().unwrap();
    drop(open_plain(tmp.path()));
    with_raw_state_meta(tmp.path(), |db, cf| {
        db.put_cf(cf, b"format_version", 4u32.to_le_bytes())
            .expect("stamp a future format version");
    });

    let err = RocksStore::open(
        tmp.path(),
        identity(),
        Limits::DEFAULT,
        Arc::new(NoFaults),
        Span::none(),
    )
    .expect_err("a directory in another on-disk format must refuse startup");
    match &err {
        StorageOpenError::UnsupportedFormat {
            found, supported, ..
        } => {
            assert_eq!(*found, 4, "the stamped version is reported verbatim");
            assert_eq!(*supported, FORMAT_VERSION);
        }
        other => panic!("expected UnsupportedFormat, got {other:?}"),
    }
    let text = err.to_string();
    assert!(
        text.contains('4') && text.contains(&FORMAT_VERSION.to_string()),
        "the message must name both versions for the operator: {text}"
    );
}

/// M2-68 `missing_format_version_on_non_empty_store_refused`: a store that already holds data
/// but carries no marker was written before the marker existed, so its layout cannot be
/// established. It is refused with `found: 0` rather than being adopted into the current
/// format — adopting it is exactly the silent misread the marker exists to prevent.
#[retcd_test]
async fn m2_storage_29_missing_format_version_on_non_empty_store_refused() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let s = open_plain(tmp.path());
        s.log_store()
            .blocking_append(vec![put(1, 1, "/m2/68/a", "v")])
            .await
            .unwrap();
    }
    with_raw_state_meta(tmp.path(), |db, cf| {
        db.delete_cf(cf, b"format_version")
            .expect("simulate a pre-marker store");
    });

    let err = RocksStore::open(
        tmp.path(),
        identity(),
        Limits::DEFAULT,
        Arc::new(NoFaults),
        Span::none(),
    )
    .expect_err("an unstamped store holding data must refuse startup");
    match &err {
        StorageOpenError::UnsupportedFormat {
            found, supported, ..
        } => {
            assert_eq!(*found, 0, "a pre-marker store reports version 0");
            assert_eq!(*supported, FORMAT_VERSION);
        }
        other => panic!("expected UnsupportedFormat, got {other:?}"),
    }
}
