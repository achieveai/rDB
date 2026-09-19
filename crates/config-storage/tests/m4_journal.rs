//! M4 storage rows: the event journal, the v1 -> v2 migration, replicated compaction and the
//! storage half of the boundary matrix (test plan `docs/testing/test-plan-m4.md` §3.1, §3.2,
//! §3.3, §3.8).
//!
//! Everything here is single-store. The cluster-level rows of the same sections — three-node
//! journal equality, rolling-upgrade divergence, the leader's proposal triggers — belong to the
//! testkit suite and are deliberately absent.
//!
//! The migration rows need a *v1* directory, which this build can no longer create: its
//! `COLUMN_FAMILIES` carries `events` and its `FORMAT_VERSION` is 2. They are built by taking a
//! real v2 directory and demoting it through the raw RocksDB handle ([`downgrade_to_v1`]) —
//! dropping `events`, restamping the marker and removing the v2-only metadata keys. That leaves
//! a directory byte-identical to one an M2/M3 build would have written, rather than a
//! hand-assembled approximation whose realism nobody can check.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use config_core::{
    ClusterId, ClusterIdentity, Command, KvState, Limits, MutationEvent, MutationEventKind, NodeId,
    RecoveryEpoch,
};
use config_log::retcd_test;
use config_storage::{
    AppliedBatch, AppliedBatchSink, Boundary, EphemeralStore, FaultAction, FaultInjector,
    JournalStats, NoFaults, NoopSink, RaftNodeId, RocksOptions, RocksStore, StateReader,
    StorageOpenError, StorageReadError, TypeConfig, CF_DEDUP, CF_EVENTS, CF_STATE_META,
    COLUMN_FAMILIES, FORMAT_VERSION,
};
use openraft::storage::{RaftLogStorage, RaftStateMachine};
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId, Vote};
use tracing::Span;

// --- fixtures -----------------------------------------------------------------------------

fn identity() -> ClusterIdentity {
    ClusterIdentity {
        cluster_id: ClusterId::from_bytes([7u8; 16]),
        recovery_epoch: RecoveryEpoch(0),
        node_id: NodeId(1),
    }
}

fn open_at(dir: &Path, faults: Arc<dyn FaultInjector>) -> RocksStore {
    open_full(dir, faults, Arc::new(NoopSink))
}

fn open_plain(dir: &Path) -> RocksStore {
    open_at(dir, Arc::new(NoFaults))
}

fn open_full(
    dir: &Path,
    faults: Arc<dyn FaultInjector>,
    sink: Arc<dyn AppliedBatchSink>,
) -> RocksStore {
    RocksStore::open_with(
        dir,
        identity(),
        Limits::DEFAULT,
        faults,
        Span::none(),
        RocksOptions::DEFAULT,
        sink,
    )
    .expect("store opens")
}

fn log_id(term: u64, index: u64) -> LogId<RaftNodeId> {
    LogId::new(CommittedLeaderId::new(term, 1), index)
}

fn entry(index: u64, cmd: Command) -> Entry<TypeConfig> {
    Entry {
        log_id: log_id(1, index),
        payload: EntryPayload::Normal(cmd),
    }
}

fn blank(index: u64) -> Entry<TypeConfig> {
    Entry {
        log_id: log_id(1, index),
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

fn put(index: u64, key: &str, value: &str) -> Entry<TypeConfig> {
    entry(index, put_cmd(key, value))
}

fn delete(index: u64, key: &str) -> Entry<TypeConfig> {
    entry(
        index,
        Command::Delete {
            key: Bytes::copy_from_slice(key.as_bytes()),
            expected_mod_revision: None,
            dedup: None,
        },
    )
}

fn compact(index: u64, up_to_revision: u64) -> Entry<TypeConfig> {
    entry(
        index,
        Command::Compact {
            up_to_revision,
            dedup_trim_below: None,
        },
    )
}

/// All retained events, in revision order.
fn all_events(reader: &Arc<dyn StateReader>) -> Vec<MutationEvent> {
    reader
        .read_events(0, u64::MAX, &[], usize::MAX)
        .expect("journal readable")
}

/// Fires `action` on the `nth` crossing of `boundary`, counting from 1.
struct FailAt {
    boundary: Boundary,
    nth: u64,
    action: FaultAction,
    seen: AtomicU64,
}

impl FailAt {
    fn nth(boundary: Boundary, nth: u64, action: FaultAction) -> Arc<Self> {
        Arc::new(Self {
            boundary,
            nth,
            action,
            seen: AtomicU64::new(0),
        })
    }

    fn new(boundary: Boundary, action: FaultAction) -> Arc<Self> {
        Self::nth(boundary, 1, action)
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

/// Captures what the publish seam was told, in order.
#[derive(Default)]
struct RecordingSink {
    batches: Mutex<Vec<AppliedBatch>>,
    bracket: Mutex<Vec<(&'static str, u64)>>,
}

impl RecordingSink {
    fn batches(&self) -> Vec<AppliedBatch> {
        self.batches.lock().unwrap().clone()
    }

    fn bracket(&self) -> Vec<(&'static str, u64)> {
        self.bracket.lock().unwrap().clone()
    }
}

impl AppliedBatchSink for RecordingSink {
    fn on_applied(&self, batch: AppliedBatch) {
        self.batches.lock().unwrap().push(batch);
    }

    fn before_compact(&self, up_to_revision: u64) {
        self.bracket
            .lock()
            .unwrap()
            .push(("before", up_to_revision));
    }

    fn after_compact(&self, up_to_revision: u64) {
        self.bracket.lock().unwrap().push(("after", up_to_revision));
    }
}

/// Reach past `RocksStore` into the raw handle. Every helper below scopes the handle tightly:
/// on Windows the directory lock must be released before the next open.
fn with_raw(dir: &Path, cfs: &[&str], f: impl FnOnce(&rocksdb::DB)) {
    let db = rocksdb::DB::open_cf(&rocksdb::Options::default(), dir, cfs.iter().copied())
        .expect("reopen raw");
    f(&db);
}

/// Turn a current directory into the directory an M2/M3 build would have written.
fn downgrade_to_v1(dir: &Path) {
    let mut db = rocksdb::DB::open_cf(&rocksdb::Options::default(), dir, COLUMN_FAMILIES)
        .expect("reopen raw to demote");
    {
        let cf = db.cf_handle(CF_STATE_META).expect("state_meta cf");
        db.put_cf(cf, b"format_version", 1u32.to_le_bytes())
            .expect("restamp v1");
        db.delete_cf(cf, b"compact_revision").expect("drop v2 key");
        db.delete_cf(cf, b"journal_stats").expect("drop v2 key");
        db.delete_cf(cf, b"retired_nodes").expect("drop v3 key");
    }
    db.drop_cf(CF_EVENTS).expect("drop the journal family");
    db.drop_cf(CF_DEDUP).expect("drop the dedup family");
}

fn raw_format_version(dir: &Path, cfs: &[&str]) -> Option<u32> {
    let mut found = None;
    with_raw(dir, cfs, |db| {
        let cf = db.cf_handle(CF_STATE_META).expect("state_meta cf");
        found = db
            .get_cf(cf, b"format_version")
            .expect("read marker")
            .map(|b| u32::from_le_bytes(b.as_slice().try_into().expect("4 bytes")));
    });
    found
}

fn list_cfs(dir: &Path) -> Vec<String> {
    rocksdb::DB::list_cf(&rocksdb::Options::default(), dir).expect("list column families")
}

/// Seed a directory with `/a=1`, `/b=2`, `/c=3` at revisions 1..=3.
async fn seed_three(dir: &Path) {
    let s = open_plain(dir);
    s.log_store().save_vote(&Vote::new(1, 1)).await.unwrap();
    s.state_machine()
        .apply(vec![
            put(1, "/a", "1"),
            put(2, "/b", "2"),
            put(3, "/c", "3"),
        ])
        .await
        .unwrap();
    // Explicit: on Windows the directory lock outlives an implicit drop just long enough for
    // the next raw open in the same test to fail.
    drop(s);
}

// --- §3.1 the event journal ---------------------------------------------------------------

/// M4-01: one mutation produces exactly one journal event, inside the *same* state batch — no
/// second batch and no second fsync. Counted rather than read out of the source, so a future
/// refactor that splits the write is caught by the numbers.
#[retcd_test]
async fn m4_01_journal_written_in_same_state_batch() {
    let tmp = tempfile::tempdir().unwrap();
    let s = open_plain(tmp.path());
    let reader = s.reader();

    let before_batches = s.counters().get(Boundary::BeforeStateBatch);
    let after_batches = s.counters().get(Boundary::AfterStateBatch);
    let before_syncs = s.sync_count();
    let before_count = reader.journal_stats().unwrap().count;

    s.state_machine()
        .apply(vec![put(1, "/a", "1")])
        .await
        .unwrap();

    assert_eq!(
        s.counters().get(Boundary::BeforeStateBatch) - before_batches,
        1
    );
    assert_eq!(
        s.counters().get(Boundary::AfterStateBatch) - after_batches,
        1
    );
    assert_eq!(
        s.sync_count() - before_syncs,
        1,
        "the journal must not cost a second fsync"
    );
    assert_eq!(
        reader.journal_stats().unwrap().count - before_count,
        1,
        "exactly one event for one mutation"
    );
    assert_eq!(
        reader.journal_stats().unwrap().newest_revision,
        Some(1),
        "keyed by the revision the mutation allocated"
    );
}

/// M4-02: the event is the record delta, not a pointer to it — key, value, both revisions.
#[retcd_test]
async fn m4_02_journal_event_matches_mutation() {
    let tmp = tempfile::tempdir().unwrap();
    let s = open_plain(tmp.path());
    s.state_machine()
        .apply(vec![put(1, "/k", "v"), put(2, "/k", "v2"), delete(3, "/k")])
        .await
        .unwrap();

    let events = all_events(&s.reader());
    assert_eq!(events.len(), 3);
    assert_eq!(
        events.iter().map(|e| e.revision).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    for event in &events {
        assert_eq!(event.key, Bytes::from_static(b"/k"));
    }
    match &events[0].kind {
        MutationEventKind::Put {
            value,
            create_revision,
        } => {
            assert_eq!(value, &Bytes::from_static(b"v"));
            assert_eq!(*create_revision, 1, "a create names itself");
        }
        other => panic!("expected Put, got {other:?}"),
    }
    match &events[1].kind {
        MutationEventKind::Put {
            value,
            create_revision,
        } => {
            assert_eq!(value, &Bytes::from_static(b"v2"));
            assert_eq!(*create_revision, 1, "an update keeps the create revision");
        }
        other => panic!("expected Put, got {other:?}"),
    }
    assert!(matches!(events[2].kind, MutationEventKind::Delete));
}

/// M4-03 (§19.3): a command that allocates no revision produces no event. A journal that
/// recorded rejections would hand watchers revisions the cluster never issued.
#[retcd_test]
async fn m4_03_no_event_for_conflict_or_missing_delete() {
    let tmp = tempfile::tempdir().unwrap();
    let s = open_plain(tmp.path());
    s.state_machine()
        .apply(vec![put(1, "/a", "1")])
        .await
        .unwrap();

    let reader = s.reader();
    let before = reader.journal_stats().unwrap();
    let revision_before = {
        let mut seen = 0;
        reader.with_state(&mut |kv: &KvState| seen = kv.cluster_revision());
        seen
    };

    s.state_machine()
        .apply(vec![
            entry(
                2,
                Command::Put {
                    key: Bytes::from_static(b"/a"),
                    value: Bytes::from_static(b"nope"),
                    expected_mod_revision: Some(99),
                    dedup: None,
                },
            ),
            entry(
                3,
                Command::Delete {
                    key: Bytes::from_static(b"/missing"),
                    expected_mod_revision: None,
                    dedup: None,
                },
            ),
            blank(4),
        ])
        .await
        .unwrap();

    let after = reader.journal_stats().unwrap();
    assert_eq!(after, before, "no event, no bytes, no bounds moved");
    let revision_after = {
        let mut seen = 0;
        reader.with_state(&mut |kv: &KvState| seen = kv.cluster_revision());
        seen
    };
    assert_eq!(revision_after, revision_before);
}

/// M4-05 (TA-28.2): the publish boundary is crossed once per applied batch, after the state
/// batch, never inside it — and an empty batch still publishes, because "applied up to R with
/// nothing to report" is what advances an idle stream's cursor.
#[retcd_test]
async fn m4_05_publish_boundary_crossed_once_per_batch() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(RecordingSink::default());
    let s = open_full(tmp.path(), Arc::new(NoFaults), sink.clone());

    for i in 1..=10 {
        s.state_machine()
            .apply(vec![put(i, &format!("/k{i}"), "v")])
            .await
            .unwrap();
    }
    s.state_machine().apply(vec![blank(11)]).await.unwrap();

    let counters = s.counters();
    assert_eq!(counters.get(Boundary::AfterStateBatchBeforePublish), 11);
    assert_eq!(
        counters.get(Boundary::AfterStateBatchBeforePublish),
        counters.get(Boundary::AfterStateBatch),
        "the publish boundary never outruns the batch it publishes"
    );

    let batches = sink.batches();
    assert_eq!(batches.len(), 11, "one publish per batch, empty or not");
    assert_eq!(batches[9].events.len(), 1);
    assert_eq!(batches[9].applied_revision, 10);
    assert!(
        batches[10].events.is_empty(),
        "the blank batch publishes nothing"
    );
    assert_eq!(
        batches[10].applied_revision, 10,
        "but still carries the applied revision"
    );
    assert!(batches.iter().all(|b| b.compacted_to.is_none()));
}

/// M4-07: the journal is durable, not a cache. Reopening reloads every event byte-for-byte and
/// rebuilds the stats from the column family rather than trusting the stored marker.
#[retcd_test]
async fn m4_07_journal_survives_ordinary_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let before = {
        seed_three(tmp.path()).await;
        let s = open_plain(tmp.path());
        let reader = s.reader();
        let events = all_events(&reader);
        let hash = reader.journal_hash(0).unwrap();
        let stats = reader.journal_stats().unwrap();
        (events, hash, stats)
    };

    let s = open_plain(tmp.path());
    let reader = s.reader();
    assert_eq!(all_events(&reader), before.0);
    assert_eq!(reader.journal_hash(0).unwrap(), before.1);
    assert_eq!(reader.journal_stats().unwrap(), before.2);
}

/// M4-09: the stats are the numbers retention arithmetic is expressed in, so they must track
/// the *serialized* bytes and both bounds, and must survive a reopen unchanged.
#[retcd_test]
async fn m4_09_journal_stats_track_bytes_and_oldest() {
    let tmp = tempfile::tempdir().unwrap();
    let s = open_plain(tmp.path());
    let reader = s.reader();
    assert_eq!(reader.journal_stats().unwrap(), JournalStats::default());

    s.state_machine()
        .apply(vec![put(1, "/a", "1"), put(2, "/b", "a much longer value")])
        .await
        .unwrap();

    let stats = reader.journal_stats().unwrap();
    assert_eq!(stats.count, 2);
    assert_eq!(stats.oldest_revision, Some(1));
    assert_eq!(stats.newest_revision, Some(2));
    let expected: u64 = all_events(&reader)
        .iter()
        .map(config_storage::event_bytes)
        .sum();
    assert_eq!(stats.bytes, expected, "serialized bytes, not an estimate");
    assert!(stats.bytes > 0);

    // The reader holds its own handle on the store, so both must go before the reopen.
    drop(reader);
    drop(s);
    let s = open_plain(tmp.path());
    assert_eq!(
        s.reader().journal_stats().unwrap(),
        stats,
        "rebuilt from the column family, identical"
    );
}

/// M4-10: `read_events` is half-open below and inclusive above, ordered, prefix-filtered, and
/// applies its limit *after* the prefix filter — otherwise a narrow watch behind a wide batch
/// would spend its whole budget on events it discards and never make progress.
#[retcd_test]
async fn m4_10_journal_range_is_ordered_and_half_open() {
    let tmp = tempfile::tempdir().unwrap();
    let s = open_plain(tmp.path());
    s.state_machine()
        .apply(vec![
            put(1, "/a/1", "v"),
            put(2, "/b/1", "v"),
            put(3, "/a/2", "v"),
            put(4, "/b/2", "v"),
            put(5, "/a/3", "v"),
        ])
        .await
        .unwrap();
    let reader = s.reader();

    let all = all_events(&reader);
    assert_eq!(
        all.iter().map(|e| e.revision).collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5],
        "ascending, always"
    );

    let mid = reader.read_events(2, 4, &[], usize::MAX).unwrap();
    assert_eq!(
        mid.iter().map(|e| e.revision).collect::<Vec<_>>(),
        vec![3, 4],
        "from_exclusive is excluded, to_inclusive is included"
    );

    assert!(
        reader
            .read_events(3, 3, &[], usize::MAX)
            .unwrap()
            .is_empty(),
        "an empty range is empty, not an error"
    );
    assert!(reader
        .read_events(9, 1, &[], usize::MAX)
        .unwrap()
        .is_empty());
    assert!(reader.read_events(0, 5, &[], 0).unwrap().is_empty());

    let prefixed = reader.read_events(0, u64::MAX, b"/a", usize::MAX).unwrap();
    assert_eq!(
        prefixed.iter().map(|e| e.revision).collect::<Vec<_>>(),
        vec![1, 3, 5]
    );

    let limited = reader.read_events(0, u64::MAX, b"/a", 2).unwrap();
    assert_eq!(
        limited.iter().map(|e| e.revision).collect::<Vec<_>>(),
        vec![1, 3],
        "the limit counts matches, not candidates"
    );
}

/// M4-11: the ephemeral store is a full journal implementation, not a stub. Same events, same
/// stats, same digest as RocksDB for the same command sequence — which is what lets a test
/// choose the cheap store without weakening what it proves.
#[retcd_test]
async fn m4_11_ephemeral_journal_parity() {
    let commands = vec![
        put(1, "/a", "1"),
        put(2, "/b", "2"),
        put(3, "/a", "1b"),
        delete(4, "/b"),
        blank(5),
    ];

    let tmp = tempfile::tempdir().unwrap();
    let rocks = open_plain(tmp.path());
    rocks.state_machine().apply(commands.clone()).await.unwrap();

    let eph = EphemeralStore::new_without_sink(
        identity(),
        Limits::DEFAULT,
        Arc::new(NoFaults),
        Span::none(),
    );
    eph.state_machine().apply(commands).await.unwrap();

    let (r, e) = (rocks.reader(), eph.reader());
    assert_eq!(all_events(&r), all_events(&e));
    assert_eq!(r.journal_stats().unwrap(), e.journal_stats().unwrap());
    assert_eq!(r.journal_hash(0).unwrap(), e.journal_hash(0).unwrap());
    assert_eq!(r.journal_hash(2).unwrap(), e.journal_hash(2).unwrap());
    assert_eq!(r.compact_revision().unwrap(), e.compact_revision().unwrap());
    assert_eq!(
        r.read_events(1, 3, b"/a", 10).unwrap(),
        e.read_events(1, 3, b"/a", 10).unwrap()
    );
}

/// M4-12: the ephemeral journal dies with the process, by construction. Recorded as a test so
/// the limitation is a checked property rather than a sentence in a doc comment that could
/// quietly stop being true.
#[retcd_test]
async fn m4_12_ephemeral_journal_lost_on_restart_documented() {
    let store = || {
        EphemeralStore::new_without_sink(
            identity(),
            Limits::DEFAULT,
            Arc::new(NoFaults),
            Span::none(),
        )
    };

    let first = store();
    first
        .state_machine()
        .apply(vec![put(1, "/a", "1")])
        .await
        .unwrap();
    assert_eq!(first.reader().journal_stats().unwrap().count, 1);

    // A "restart" of an ephemeral node is a new store: nothing is read back from anywhere.
    let second = store();
    assert_eq!(
        second.reader().journal_stats().unwrap(),
        JournalStats::default(),
        "an ephemeral node comes back with no history at all"
    );
    assert_eq!(second.reader().compact_revision().unwrap(), 0);
}

// --- §3.2 the v1 -> v2 migration ----------------------------------------------------------

/// M4-13: a v1 directory opens, is stamped v2, gains the journal family, and keeps its data.
#[retcd_test]
async fn m4_13_v1_dir_migrates_to_v2() {
    let tmp = tempfile::tempdir().unwrap();
    seed_three(tmp.path()).await;
    downgrade_to_v1(tmp.path());

    assert_eq!(raw_format_version(tmp.path(), &COLUMN_FAMILIES_V1), Some(1));
    assert!(!list_cfs(tmp.path()).iter().any(|c| c == CF_EVENTS));

    let s = open_plain(tmp.path());
    let reader = s.reader();
    let mut revision = 0;
    let mut records = 0;
    reader.with_state(&mut |kv: &KvState| {
        revision = kv.cluster_revision();
        records = kv.len();
    });
    assert_eq!(revision, 3, "the revision survives the migration");
    assert_eq!(records, 3, "so do the records");
    assert_eq!(
        reader.compact_revision().unwrap(),
        3,
        "ruling R1: the watermark is stamped from this node's own cluster_revision"
    );
    drop(reader);
    drop(s);

    assert_eq!(
        raw_format_version(tmp.path(), &COLUMN_FAMILIES),
        Some(FORMAT_VERSION)
    );
    assert!(list_cfs(tmp.path()).iter().any(|c| c == CF_EVENTS));
    assert!(
        list_cfs(tmp.path()).iter().any(|c| c == CF_DEDUP),
        "the same migration creates every family this build knows, not just the v2 one"
    );
}

/// M4-14: the migration is one synced batch. A fault at either boundary of that batch is
/// reported as a typed open error rather than leaving a half-migrated directory behind.
#[retcd_test]
async fn m4_14_migration_is_one_synced_batch() {
    for boundary in [Boundary::BeforeStateBatch, Boundary::AfterStateBatch] {
        let tmp = tempfile::tempdir().unwrap();
        seed_three(tmp.path()).await;
        downgrade_to_v1(tmp.path());

        let err = RocksStore::open(
            tmp.path(),
            identity(),
            Limits::DEFAULT,
            FailAt::new(boundary, FaultAction::Fail),
            Span::none(),
        )
        .expect_err("{boundary}: an injected fault must refuse the open");
        assert!(
            matches!(err, StorageOpenError::Backend { .. }),
            "{boundary}: expected Backend, got {err:?}"
        );
        assert!(
            err.to_string().contains("format migration"),
            "{boundary}: the error names what was being done: {err}"
        );

        // `BeforeStateBatch` fires ahead of the write, `AfterStateBatch` behind it. Either way
        // the directory is reopenable, which is the only property an operator can act on.
        let s = open_plain(tmp.path());
        assert_eq!(s.reader().compact_revision().unwrap(), 3);
    }
}

/// M4-15: the second open of a migrated directory is an ordinary open. The marker is a gate,
/// not a one-shot, and the watermark is not re-stamped on top of real compaction progress.
#[retcd_test]
async fn m4_15_migration_is_idempotent() {
    let tmp = tempfile::tempdir().unwrap();
    seed_three(tmp.path()).await;
    downgrade_to_v1(tmp.path());

    {
        let s = open_plain(tmp.path());
        s.state_machine()
            .apply(vec![put(4, "/d", "4"), put(5, "/e", "5")])
            .await
            .unwrap();
        assert_eq!(s.reader().compact_revision().unwrap(), 3);
    }

    let s = open_plain(tmp.path());
    assert_eq!(
        s.reader().compact_revision().unwrap(),
        3,
        "a second open must not re-stamp the watermark to the new cluster_revision"
    );
    assert_eq!(
        s.reader().journal_stats().unwrap().count,
        2,
        "the post-migration events are still there"
    );
}

/// M4-16: the migration touches metadata only. Every record keeps its bytes and both revisions,
/// which the `state_hash` oracle checks in one comparison.
#[retcd_test]
async fn m4_16_migration_does_not_rewrite_kv() {
    let tmp = tempfile::tempdir().unwrap();
    seed_three(tmp.path()).await;

    let before = {
        let s = open_plain(tmp.path());
        let mut hash = [0u8; 32];
        s.reader()
            .with_state(&mut |kv: &KvState| hash = kv.state_hash());
        hash
    };

    downgrade_to_v1(tmp.path());
    let s = open_plain(tmp.path());
    let mut after = [0u8; 32];
    s.reader()
        .with_state(&mut |kv: &KvState| after = kv.state_hash());
    assert_eq!(
        after, before,
        "ruling R1 keeps compact_revision out of state_hash, so a migration cannot move it"
    );
}

/// M4-17: a migrated directory has no retained history. The pre-v2 revisions were never
/// journalled, so claiming them resumable would be the silent gap the journal exists to close.
#[retcd_test]
async fn m4_17_migrated_dir_has_no_retained_history() {
    let tmp = tempfile::tempdir().unwrap();
    seed_three(tmp.path()).await;
    downgrade_to_v1(tmp.path());

    let s = open_plain(tmp.path());
    let reader = s.reader();
    assert_eq!(
        reader.journal_stats().unwrap(),
        JournalStats::default(),
        "no events, and none invented"
    );
    assert_eq!(
        reader.compact_revision().unwrap(),
        3,
        "everything at or below the pre-migration revision is unresumable"
    );
    assert!(all_events(&reader).is_empty());
}

/// M4-18: crashing during the migration leaves a directory that still opens. The marker is only
/// advanced by the synced batch, so a crash before it lands means the next open migrates again.
#[retcd_test]
async fn m4_18_migration_crash_leaves_a_reopenable_dir() {
    let tmp = tempfile::tempdir().unwrap();
    seed_three(tmp.path()).await;
    downgrade_to_v1(tmp.path());

    let err = RocksStore::open(
        tmp.path(),
        identity(),
        Limits::DEFAULT,
        FailAt::new(Boundary::BeforeStateBatch, FaultAction::Crash),
        Span::none(),
    )
    .expect_err("a crash during migration must refuse the open");
    assert!(matches!(err, StorageOpenError::Backend { .. }), "{err:?}");

    // The crash landed before the batch, so the marker is still v1 and the retry migrates.
    let s = open_plain(tmp.path());
    assert_eq!(s.reader().compact_revision().unwrap(), 3);
    let mut records = 0;
    s.reader()
        .with_state(&mut |kv: &KvState| records = kv.len());
    assert_eq!(records, 3, "no data was lost by the failed attempt");
    drop(s);
    assert_eq!(
        raw_format_version(tmp.path(), &COLUMN_FAMILIES),
        Some(FORMAT_VERSION)
    );
}

/// M4-19 (ruling R2): a v1 build meeting a v2 directory refuses on the *unexpected* `events`
/// family, before it ever reads the format marker.
///
/// A v1 build cannot be instantiated from inside this one, so the row asserts the two facts
/// that make that refusal certain: the directory carries a family outside the v1 set, and a
/// v1-shaped open — the exact descriptor list and options an M2/M3 build used — fails rather
/// than quietly proceeding on a subset of the families.
#[retcd_test]
async fn m4_19_v1_build_refuses_a_v2_dir() {
    let tmp = tempfile::tempdir().unwrap();
    seed_three(tmp.path()).await;

    let found = list_cfs(tmp.path());
    assert!(found.iter().any(|c| c == CF_EVENTS));
    let mut unexpected: Vec<String> = found
        .iter()
        .filter(|c| c.as_str() != "default" && !COLUMN_FAMILIES_V1.contains(&c.as_str()))
        .cloned()
        .collect();
    unexpected.sort();
    assert_eq!(
        unexpected,
        vec![CF_DEDUP.to_string(), CF_EVENTS.to_string()],
        "a v1 build's column-family check sees every family it does not know"
    );

    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(false);
    opts.create_missing_column_families(false);
    let opened = rocksdb::DB::open_cf(&opts, tmp.path(), COLUMN_FAMILIES_V1);
    assert!(
        opened.is_err(),
        "a v1 descriptor list must not open a directory holding an extra family"
    );
}

// --- §3.3 replicated compaction (storage half) ---------------------------------------------

/// M4-21: `Compact` deletes the journal range at or below the watermark, keeps everything
/// above it, and moves the watermark — in one batch, bracketed by the sink's compaction pair.
#[retcd_test]
async fn m4_21_compact_command_deletes_the_range() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = Arc::new(RecordingSink::default());
    let s = open_full(tmp.path(), Arc::new(NoFaults), sink.clone());
    s.state_machine()
        .apply(vec![
            put(1, "/a", "1"),
            put(2, "/b", "2"),
            put(3, "/c", "3"),
            put(4, "/d", "4"),
        ])
        .await
        .unwrap();

    s.state_machine().apply(vec![compact(5, 2)]).await.unwrap();

    let reader = s.reader();
    assert_eq!(reader.compact_revision().unwrap(), 2);
    assert_eq!(
        all_events(&reader)
            .iter()
            .map(|e| e.revision)
            .collect::<Vec<_>>(),
        vec![3, 4],
        "everything at or below the watermark is gone, everything above survives"
    );
    let stats = reader.journal_stats().unwrap();
    assert_eq!(stats.count, 2);
    assert_eq!(stats.oldest_revision, Some(3));
    assert_eq!(stats.newest_revision, Some(4));

    assert_eq!(
        sink.bracket(),
        vec![("before", 2), ("after", 2)],
        "the sink is told before any event is deleted and after the watermark is visible"
    );
    let last = sink
        .batches()
        .pop()
        .expect("a publish for the compact batch");
    assert_eq!(last.compacted_to, Some(2));
    assert!(last.events.is_empty(), "M4-23: compaction emits no event");

    // Durable, not just in memory.
    drop(reader);
    drop(s);
    let s = open_plain(tmp.path());
    assert_eq!(s.reader().compact_revision().unwrap(), 2);
    assert_eq!(s.reader().journal_stats().unwrap(), stats);
}

/// M4-22 / M4-23: `Compact` allocates no revision, changes no record, and produces no event —
/// it is maintenance, not a mutation, and must not shift the sequence watchers are following.
#[retcd_test]
async fn m4_22_compact_allocates_no_revision() {
    let tmp = tempfile::tempdir().unwrap();
    let s = open_plain(tmp.path());
    s.state_machine()
        .apply(vec![put(1, "/a", "1"), put(2, "/b", "2")])
        .await
        .unwrap();

    let reader = s.reader();
    let mut before = [0u8; 32];
    reader.with_state(&mut |kv: &KvState| before = kv.state_hash());

    s.state_machine().apply(vec![compact(3, 1)]).await.unwrap();

    let mut revision = 0;
    let mut after = [0u8; 32];
    reader.with_state(&mut |kv: &KvState| {
        revision = kv.cluster_revision();
        after = kv.state_hash();
    });
    assert_eq!(revision, 2, "no revision allocated");
    assert_eq!(after, before, "no record touched");
    assert_eq!(
        all_events(&reader)
            .iter()
            .map(|e| e.revision)
            .collect::<Vec<_>>(),
        vec![2],
        "and no event of its own"
    );
}

/// M4-25 / M4-26 (OQ-26 as ruled): the watermark only ever rises, and a request above the
/// applied revision is clamped rather than refused — a follower replaying the same entry at a
/// lower revision must reach the same place as the leader did.
#[retcd_test]
async fn m4_25_compact_is_monotonic() {
    let tmp = tempfile::tempdir().unwrap();
    let s = open_plain(tmp.path());
    s.state_machine()
        .apply(vec![
            put(1, "/a", "1"),
            put(2, "/b", "2"),
            put(3, "/c", "3"),
        ])
        .await
        .unwrap();
    let reader = s.reader();

    s.state_machine().apply(vec![compact(4, 2)]).await.unwrap();
    assert_eq!(reader.compact_revision().unwrap(), 2);

    s.state_machine().apply(vec![compact(5, 1)]).await.unwrap();
    assert_eq!(
        reader.compact_revision().unwrap(),
        2,
        "a watermark below the current one is a no-op, never a rewind"
    );
    assert_eq!(all_events(&reader).len(), 1, "and deletes nothing more");

    s.state_machine()
        .apply(vec![compact(6, 9_999)])
        .await
        .unwrap();
    assert_eq!(
        reader.compact_revision().unwrap(),
        3,
        "clamped to cluster_revision, so every node lands on the same watermark"
    );
    assert!(all_events(&reader).is_empty());
    let stats = reader.journal_stats().unwrap();
    assert_eq!(
        stats,
        JournalStats::default(),
        "an emptied journal is empty"
    );
}

// --- §3.8 crash boundaries and corruption --------------------------------------------------

/// M4-89: a crash before the state batch leaves neither the record nor its event.
#[retcd_test]
async fn m4_89_crash_before_state_batch_leaves_no_event() {
    let tmp = tempfile::tempdir().unwrap();
    seed_three(tmp.path()).await;

    {
        let s = open_at(
            tmp.path(),
            FailAt::new(Boundary::BeforeStateBatch, FaultAction::Crash),
        );
        let _ = s.state_machine().apply(vec![put(4, "/d", "4")]).await;
        assert!(s.is_poisoned());
    }

    let s = open_plain(tmp.path());
    let reader = s.reader();
    let mut revision = 0;
    reader.with_state(&mut |kv: &KvState| revision = kv.cluster_revision());
    assert_eq!(revision, 3);
    assert_eq!(
        all_events(&reader)
            .iter()
            .map(|e| e.revision)
            .collect::<Vec<_>>(),
        vec![1, 2, 3],
        "the crashed mutation left no trace on either side"
    );
}

/// M4-90 / M4-91: crashing at the state batch or in the durable-but-unpublished window leaves
/// the record *and* its event on disk. The two must never come apart — a record without its
/// event is exactly the gap a resuming watch would step over in silence.
#[retcd_test]
async fn m4_90_crash_after_state_batch_has_kv_and_event() {
    for boundary in [
        Boundary::AfterStateBatch,
        Boundary::AfterStateBatchBeforePublish,
    ] {
        let tmp = tempfile::tempdir().unwrap();
        seed_three(tmp.path()).await;

        let sink = Arc::new(RecordingSink::default());
        {
            let s = open_full(
                tmp.path(),
                FailAt::new(boundary, FaultAction::Crash),
                sink.clone(),
            );
            let _ = s.state_machine().apply(vec![put(4, "/d", "4")]).await;
            assert!(s.is_poisoned(), "{boundary}: Crash must poison the store");
        }
        assert!(
            sink.batches().is_empty(),
            "{boundary}: nothing may be published out of a crashed batch"
        );

        let s = open_plain(tmp.path());
        let reader = s.reader();
        let mut revision = 0;
        let mut records = 0;
        reader.with_state(&mut |kv: &KvState| {
            revision = kv.cluster_revision();
            records = kv.len();
        });
        assert_eq!(revision, 4, "{boundary}: the batch was durable");
        assert_eq!(records, 4);
        assert_eq!(
            all_events(&reader)
                .iter()
                .map(|e| e.revision)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4],
            "{boundary}: the event is as durable as the record"
        );
    }
}

/// M4-91 (second half): the compaction bracket is closed on the crash path too. Left open, the
/// engine's journal gate would be held forever and every later registration would hang instead
/// of reporting the storage error.
#[retcd_test]
async fn m4_91_crash_after_state_batch_before_publish() {
    let tmp = tempfile::tempdir().unwrap();
    seed_three(tmp.path()).await;

    let sink = Arc::new(RecordingSink::default());
    {
        let s = open_full(
            tmp.path(),
            FailAt::new(Boundary::AfterStateBatchBeforePublish, FaultAction::Crash),
            sink.clone(),
        );
        let _ = s.state_machine().apply(vec![compact(4, 2)]).await;
        assert!(s.is_poisoned());
    }

    assert_eq!(
        sink.bracket(),
        vec![("before", 2), ("after", 2)],
        "a crash in the publish window still closes the bracket"
    );
    assert!(sink.batches().is_empty(), "and publishes nothing");
}

/// M4-92: a crash while a `Compact` applies leaves a directory whose watermark and journal
/// agree — never a watermark claiming deletions that did not happen, or events below a
/// watermark that says they are gone.
#[retcd_test]
async fn m4_92_crash_during_compact_apply() {
    for (boundary, expected_watermark, expected) in [
        (Boundary::BeforeStateBatch, 0u64, vec![1u64, 2, 3]),
        (Boundary::AfterStateBatch, 2, vec![3]),
    ] {
        let tmp = tempfile::tempdir().unwrap();
        seed_three(tmp.path()).await;

        {
            let s = open_at(tmp.path(), FailAt::new(boundary, FaultAction::Crash));
            let _ = s.state_machine().apply(vec![compact(4, 2)]).await;
            assert!(s.is_poisoned(), "{boundary}");
        }

        let s = open_plain(tmp.path());
        let reader = s.reader();
        assert_eq!(
            reader.compact_revision().unwrap(),
            expected_watermark,
            "{boundary}: watermark"
        );
        assert_eq!(
            all_events(&reader)
                .iter()
                .map(|e| e.revision)
                .collect::<Vec<_>>(),
            expected,
            "{boundary}: retained events agree with the watermark"
        );
        let stats = reader.journal_stats().unwrap();
        assert_eq!(
            stats.count,
            expected.len() as u64,
            "{boundary}: stats agree"
        );
        assert_eq!(
            stats.oldest_revision,
            expected.first().copied(),
            "{boundary}"
        );
    }
}

/// M4-93: a poisoned store refuses every journal read rather than answering short. A partial
/// answer is indistinguishable from a complete one, and a watch resuming off it would skip
/// revisions with nothing to report the loss.
#[retcd_test]
async fn m4_93_io_error_on_journal_write_is_fatal_not_silent() {
    let tmp = tempfile::tempdir().unwrap();
    seed_three(tmp.path()).await;

    let s = open_at(
        tmp.path(),
        FailAt::new(Boundary::AfterStateBatch, FaultAction::Crash),
    );
    let _ = s.state_machine().apply(vec![put(4, "/d", "4")]).await;
    assert!(s.is_poisoned());

    let reader = s.reader();
    assert_eq!(reader.compact_revision(), Err(StorageReadError::Poisoned));
    assert_eq!(reader.journal_stats(), Err(StorageReadError::Poisoned));
    assert_eq!(
        reader.read_events(0, u64::MAX, &[], 10),
        Err(StorageReadError::Poisoned)
    );
    assert_eq!(reader.journal_hash(0), Err(StorageReadError::Poisoned));
}

/// M4-94: a journal record that cannot be decoded is reported at open, typed and naming the
/// revision — not at the first watch, and never skipped.
#[retcd_test]
async fn m4_94_journal_corruption_detected_on_open() {
    let tmp = tempfile::tempdir().unwrap();
    seed_three(tmp.path()).await;

    with_raw(tmp.path(), &COLUMN_FAMILIES, |db| {
        let cf = db.cf_handle(CF_EVENTS).expect("events cf");
        db.put_cf(cf, 2u64.to_be_bytes(), b"not a postcard event")
            .expect("corrupt one record");
    });

    let err = RocksStore::open(
        tmp.path(),
        identity(),
        Limits::DEFAULT,
        Arc::new(NoFaults),
        Span::none(),
    )
    .expect_err("a corrupt journal record must refuse the open");
    match &err {
        StorageOpenError::Corrupt { what, .. } => {
            assert!(
                what.contains("events"),
                "the error names the family: {what}"
            );
            assert!(what.contains('2'), "and the revision: {what}");
        }
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

/// M4-95: a v2 directory whose `events` family was removed is refused — and, critically, the
/// family is *not* silently recreated on the way to that refusal. RocksDB is opened with
/// `create_missing_column_families`, so the check has to happen through a read-only probe
/// before the writable open.
#[retcd_test]
async fn m4_95_missing_events_cf_on_v2_dir_refused() {
    let tmp = tempfile::tempdir().unwrap();
    seed_three(tmp.path()).await;

    // Demote the family without demoting the marker: a v2 directory missing its journal.
    {
        let mut db =
            rocksdb::DB::open_cf(&rocksdb::Options::default(), tmp.path(), COLUMN_FAMILIES)
                .expect("reopen raw");
        db.drop_cf(CF_EVENTS).expect("drop the journal family");
        db.drop_cf(CF_DEDUP).expect("drop the dedup family");
    }
    assert_eq!(
        raw_format_version(tmp.path(), &COLUMN_FAMILIES_V1),
        Some(FORMAT_VERSION),
        "the marker still says the current version"
    );

    let err = RocksStore::open(
        tmp.path(),
        identity(),
        Limits::DEFAULT,
        Arc::new(NoFaults),
        Span::none(),
    )
    .expect_err("a v2 directory without its journal must be refused");
    match &err {
        StorageOpenError::MissingColumnFamily { name, .. } => assert_eq!(name, CF_EVENTS),
        other => panic!("expected MissingColumnFamily, got {other:?}"),
    }
    assert!(
        !list_cfs(tmp.path()).iter().any(|c| c == CF_EVENTS),
        "the refusal must not have created the family it refused over"
    );
}

/// M4-96: the boundary table is complete and self-consistent. Guards against a boundary being
/// added to the enum and silently dropped from the tables that drive the fault matrix.
///
/// The count moved from nine to seventeen at M5 — ADR-0022 adds three publish boundaries, three
/// install boundaries and two purge boundaries. It is asserted as a literal deliberately:
/// `Boundary::ALL` is the single source of truth every table in the workspace iterates, so a
/// variant added to the enum but not to `ALL` has to fail somewhere, and this is that somewhere.
#[retcd_test]
async fn m4_96_boundary_table_is_exhaustive_at_nine() {
    assert_eq!(Boundary::ALL.len(), 17);
    assert_eq!(
        Boundary::ALL[8],
        Boundary::AfterStateBatchBeforePublish,
        "the publish boundary ends the apply path: it is crossed after everything else"
    );
    assert_eq!(
        Boundary::ALL[16],
        Boundary::AfterPurge,
        "purge is last: nothing is deleted before everything that justifies it is durable"
    );
    let mut names: Vec<&str> = Boundary::ALL.iter().map(|b| b.as_str()).collect();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), 17, "every boundary has a distinct name");
    let mut indexes: Vec<usize> = Boundary::ALL.iter().map(|b| b.index()).collect();
    indexes.sort_unstable();
    assert_eq!(indexes, (0..17).collect::<Vec<_>>(), "indexes are dense");
}

/// The v1 column-family set, as an M2/M3 build spelled it.
const COLUMN_FAMILIES_V1: [&str; 4] = ["raft_log", "raft_meta", "kv", "state_meta"];
