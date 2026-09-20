//! M5 storage rows for bounded request deduplication and node retirement (test plan
//! `docs/testing/test-plan-m5.md` §6; ADR-0025, ADR-0023, ADR-0021).
//!
//! Single-store, no OpenRaft cluster: these rows prove the properties a cluster row would then
//! be entitled to assume — that a dedup record is written in the *same* synced batch as the
//! mutation it describes, that both it and the retired set come back after a restart, that the
//! `dedup` column family is exported by the snapshot format, and that a v2 directory migrates
//! forward into one that has the family.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use config_core::{
    ClusterId, ClusterIdentity, Command, CommandResponse, DedupKey, DedupLimits, KvState, Limits,
    MutationOutcome, NodeId, RecoveryEpoch,
};
use config_log::retcd_test;
use config_storage::{
    is_snapshot_data_cf, Boundary, FaultAction, FaultInjector, NoFaults, NoopSink, RaftNodeId,
    RocksOptions, RocksStore, SnapshotReader, StorageOpenError, TypeConfig, CF_DEDUP,
    COLUMN_FAMILIES,
};
use openraft::storage::{RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine};
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId, Vote};
use tracing::Span;

const ALICE: [u8; 32] = [0x11; 32];
const CLIENT: [u8; 16] = [0xab; 16];

fn identity() -> ClusterIdentity {
    ClusterIdentity {
        cluster_id: ClusterId::from_bytes([7u8; 16]),
        recovery_epoch: RecoveryEpoch(0),
        node_id: NodeId(1),
    }
}

/// Dedup is off in [`Limits::DEFAULT`] (M5-108), so every row here opts in explicitly.
fn dedup_limits() -> Limits {
    Limits {
        dedup: DedupLimits {
            enabled: true,
            window_requests: 8,
            max_records: 1_000_000,
        },
        ..Limits::DEFAULT
    }
}

fn open_at(dir: &Path, faults: Arc<dyn FaultInjector>) -> RocksStore {
    RocksStore::open_with(
        dir,
        identity(),
        dedup_limits(),
        faults,
        Span::none(),
        RocksOptions::DEFAULT,
        Arc::new(NoopSink),
    )
    .expect("store opens")
}

fn open_plain(dir: &Path) -> RocksStore {
    open_at(dir, Arc::new(NoFaults))
}

fn log_id(term: u64, index: u64) -> LogId<RaftNodeId> {
    LogId::new(CommittedLeaderId::new(term, 1), index)
}

/// A `Put` already bound to [`ALICE`] — the shape a follower sees in the log.
fn put_cmd(key: &str, value: &str, request_id: u64) -> Command {
    Command::Put {
        key: Bytes::copy_from_slice(key.as_bytes()),
        value: Bytes::copy_from_slice(value.as_bytes()),
        expected_mod_revision: None,
        dedup: Some(DedupKey::new(CLIENT, request_id).stamp(ALICE)),
    }
}

fn put(index: u64, key: &str, value: &str, request_id: u64) -> Entry<TypeConfig> {
    Entry {
        log_id: log_id(1, index),
        payload: EntryPayload::Normal(put_cmd(key, value, request_id)),
    }
}

fn entry(index: u64, cmd: Command) -> Entry<TypeConfig> {
    Entry {
        log_id: log_id(1, index),
        payload: EntryPayload::Normal(cmd),
    }
}

/// Fires `action` on the `nth` crossing of `boundary`, counting from 1.
struct FailAt {
    boundary: Boundary,
    nth: u64,
    seen: AtomicU64,
}

impl FaultInjector for FailAt {
    fn before(&self, boundary: Boundary) -> FaultAction {
        if boundary != self.boundary {
            return FaultAction::Proceed;
        }
        if self.seen.fetch_add(1, Ordering::SeqCst) + 1 == self.nth {
            FaultAction::Crash
        } else {
            FaultAction::Proceed
        }
    }
}

fn raw_dedup_count(dir: &Path) -> usize {
    let db = rocksdb::DB::open_cf(
        &rocksdb::Options::default(),
        dir,
        COLUMN_FAMILIES.iter().copied(),
    )
    .expect("reopen raw");
    let cf = db.cf_handle(CF_DEDUP).expect("dedup cf");
    // Counted in a loop rather than `.map(…).count()`: the `expect` is the point — every row
    // must actually read back — and an adaptor whose closure `count()` is free to skip would
    // let an unreadable row be counted as a readable one.
    let mut n = 0;
    for item in db.iterator_cf(cf, rocksdb::IteratorMode::Start) {
        item.expect("every dedup row reads back");
        n += 1;
    }
    drop(db);
    n
}

/// M5-102 (durability half): the dedup record is written in the **same** synced batch as the
/// KV change, so the two can never be observed apart.
///
/// The `Before` arm proves the pair is atomic in one direction (neither survives), and the
/// reopened store proves it in the other: after the crash the duplicate is *not* a hit, which
/// is the only honest answer when the original application did not survive either.
#[retcd_test]
async fn m5_102_dedup_record_is_lost_with_its_mutation_at_before_state_batch() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let s = open_at(
            tmp.path(),
            Arc::new(FailAt {
                boundary: Boundary::BeforeStateBatch,
                nth: 1,
                seen: AtomicU64::new(0),
            }),
        );
        let result = s.state_machine().apply(vec![put(1, "/a", "1", 7)]).await;
        assert!(result.is_err(), "the armed boundary crashes the batch");
        drop(s);
    }

    assert_eq!(
        raw_dedup_count(tmp.path()),
        0,
        "no dedup record survived a batch that did not commit"
    );

    let s = open_plain(tmp.path());
    let reader = s.reader();
    assert_eq!(reader.cluster_revision(), 0, "nor did the mutation");
    assert_eq!(reader.dedup_stats().records, 0);
    drop(reader);

    // Replay applies it exactly once, and only *then* is a duplicate a hit.
    let responses = s
        .state_machine()
        .apply(vec![put(1, "/a", "1", 7)])
        .await
        .unwrap();
    assert!(matches!(
        responses[0],
        CommandResponse::Mutation {
            dedup_hit: false,
            dedup_recorded: true,
            ..
        }
    ));
    let responses = s
        .state_machine()
        .apply(vec![put(2, "/a", "1", 7)])
        .await
        .unwrap();
    let CommandResponse::Mutation {
        response,
        dedup_hit,
        ..
    } = &responses[0]
    else {
        panic!("mutation");
    };
    assert!(
        dedup_hit,
        "the replayed record now recognizes the duplicate"
    );
    assert_eq!(response.revision, 1);
    assert_eq!(s.reader().cluster_revision(), 1, "and allocated nothing");
    drop(s);
}

/// M5-102 (the other half): a crash *after* the state batch keeps both, and the duplicate
/// hits. A dedup record in a batch of its own would show up here as a phantom hit or a lost
/// one; it cannot, because it rides the same batch.
#[retcd_test]
async fn m5_102_dedup_record_survives_with_its_mutation_at_after_state_batch() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let s = open_at(
            tmp.path(),
            Arc::new(FailAt {
                boundary: Boundary::AfterStateBatch,
                nth: 1,
                seen: AtomicU64::new(0),
            }),
        );
        let result = s.state_machine().apply(vec![put(1, "/a", "1", 7)]).await;
        assert!(result.is_err(), "the crash lands after the synced write");
        drop(s);
    }

    assert_eq!(raw_dedup_count(tmp.path()), 1);

    let s = open_plain(tmp.path());
    assert_eq!(s.reader().cluster_revision(), 1);
    assert_eq!(s.reader().dedup_stats().records, 1);

    let responses = s
        .state_machine()
        .apply(vec![put(2, "/a", "1", 7)])
        .await
        .unwrap();
    let CommandResponse::Mutation {
        response,
        dedup_hit,
        ..
    } = &responses[0]
    else {
        panic!("mutation");
    };
    assert!(dedup_hit);
    assert_eq!(response.outcome, MutationOutcome::Applied);
    assert_eq!(response.revision, 1, "the original revision, not a new one");
    assert_eq!(s.reader().cluster_revision(), 1);
    drop(s);
}

/// The dedup index and the retired set are replicated state, so a restart must rebuild both
/// exactly — a store that came back with a partial window would answer duplicates differently
/// from its peers.
#[retcd_test]
async fn m5_dedup_index_and_retired_set_survive_a_restart() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let s = open_plain(tmp.path());
        s.log_store().save_vote(&Vote::new(1, 1)).await.unwrap();
        s.state_machine()
            .apply(vec![
                put(1, "/a", "1", 1),
                put(2, "/b", "2", 2),
                entry(3, Command::RetireNode { node_id: NodeId(3) }),
            ])
            .await
            .unwrap();
        drop(s);
    }

    let s = open_plain(tmp.path());
    let reader = s.reader();
    assert_eq!(reader.dedup_stats().records, 2);
    assert_eq!(reader.dedup_stats().max_records, 1_000_000);
    assert_eq!(
        reader.retired_nodes(),
        [NodeId(3)].into_iter().collect(),
        "a node that forgot a retirement would re-admit an expelled identity"
    );
    assert_eq!(
        reader.restored_from(),
        None,
        "this directory grew its own state; it was not restored into"
    );
    drop(reader);

    // The rebuilt index is the *same* index: a resubmission is still a hit after the restart.
    let responses = s
        .state_machine()
        .apply(vec![put(4, "/a", "1", 1)])
        .await
        .unwrap();
    assert!(matches!(
        responses[0],
        CommandResponse::Mutation {
            dedup_hit: true,
            ..
        }
    ));
    drop(s);
}

/// M5-100 (storage half): `Compact { dedup_trim_below }` deletes the records from the family,
/// not just from the in-memory map — otherwise a restart would resurrect them.
#[retcd_test]
async fn m5_100_compact_trims_the_dedup_family() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let s = open_plain(tmp.path());
        s.state_machine()
            .apply(vec![
                put(1, "/a", "1", 1),
                put(2, "/b", "2", 2),
                put(3, "/c", "3", 3),
            ])
            .await
            .unwrap();
        s.state_machine()
            .apply(vec![entry(
                4,
                Command::Compact {
                    up_to_revision: 2,
                    dedup_trim_below: Some(3),
                },
            )])
            .await
            .unwrap();
        assert_eq!(s.reader().dedup_stats().records, 1);
        drop(s);
    }

    assert_eq!(
        raw_dedup_count(tmp.path()),
        1,
        "the trim reached the column family, not only the map"
    );
    let s = open_plain(tmp.path());
    assert_eq!(s.reader().dedup_stats().records, 1);
    drop(s);
}

/// Spec §12.1: retained deduplication records are part of a snapshot. The M5 snapshot format
/// is column-family generic, so this holds without a format change — which is exactly the
/// claim worth pinning, because it is the reason nobody had to remember to add `dedup`.
#[retcd_test]
async fn m5_103_dedup_family_is_snapshot_data() {
    assert!(is_snapshot_data_cf(CF_DEDUP));

    let tmp = tempfile::tempdir().unwrap();
    let s = open_plain(tmp.path());
    s.state_machine()
        .apply(vec![put(1, "/a", "1", 1), put(2, "/b", "2", 2)])
        .await
        .unwrap();

    let meta = {
        let mut sm = s.state_machine();
        let mut builder = sm.get_snapshot_builder().await;
        builder
            .build_snapshot()
            .await
            .expect("snapshot builds")
            .meta
    };
    let path = s
        .path()
        .join("snapshots")
        .join(format!("{}.snap", meta.snapshot_id));
    let header = SnapshotReader::open(&path)
        .expect("snapshot opens")
        .header()
        .clone();
    assert_eq!(
        header.counts.get(CF_DEDUP).copied(),
        Some(2),
        "the header counts the dedup records it carries"
    );
    drop(s);
}

/// ADR-0021 v2 -> v3: an M4 directory gains the `dedup` family on open and keeps its data. The
/// row is built by demoting a real current directory rather than hand-assembling one, so what
/// it migrates is what an M4 build actually wrote.
///
/// Note what "an M4 directory" means here, because the row's first version read as if it meant
/// more: the directory's **log is empty**. `apply` writes state without appending, so nothing
/// ever reached `raft_log`. That is not incidental coverage, it is the only case migration
/// supports — ADR-0021 note 4 (ruling M5-R19) refuses an undrained legacy log outright, since
/// the log payload is a positional `postcard` encoding of a `Command` this build has widened.
/// The refusal is the row below; this row is the drained half of the same contract.
#[retcd_test]
async fn m5_v2_directory_migrates_forward_and_gains_the_dedup_family() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let s = open_plain(tmp.path());
        s.state_machine()
            .apply(vec![put(1, "/a", "1", 1)])
            .await
            .unwrap();
        drop(s);
    }
    {
        let mut db = rocksdb::DB::open_cf(
            &rocksdb::Options::default(),
            tmp.path(),
            COLUMN_FAMILIES.iter().copied(),
        )
        .expect("reopen raw to demote");
        {
            let cf = db
                .cf_handle(config_storage::CF_STATE_META)
                .expect("state_meta cf");
            db.put_cf(cf, b"format_version", 2u32.to_le_bytes())
                .expect("restamp v2");
            db.delete_cf(cf, b"retired_nodes").expect("drop the v3 key");
        }
        db.drop_cf(CF_DEDUP).expect("drop the dedup family");
    }
    assert!(
        !rocksdb::DB::list_cf(&rocksdb::Options::default(), tmp.path())
            .expect("list")
            .iter()
            .any(|c| c == CF_DEDUP)
    );

    let s = open_plain(tmp.path());
    let reader = s.reader();
    let mut records = 0;
    reader.with_state(&mut |kv: &KvState| records = kv.len());
    assert_eq!(records, 1, "the record survives the migration");
    assert_eq!(reader.cluster_revision(), 1);
    assert_eq!(
        reader.dedup_stats().records,
        0,
        "and starts an empty window"
    );
    drop(reader);
    drop(s);

    assert!(
        rocksdb::DB::list_cf(&rocksdb::Options::default(), tmp.path())
            .expect("list")
            .iter()
            .any(|c| c == CF_DEDUP)
    );
    assert_eq!(raw_dedup_count(tmp.path()), 0);
}

/// A second store in the same cluster, so a snapshot built by one installs into the other.
fn open_as_node(dir: &Path, node: u64) -> RocksStore {
    RocksStore::open_with(
        dir,
        ClusterIdentity {
            node_id: NodeId(node),
            ..identity()
        },
        dedup_limits(),
        Arc::new(NoFaults),
        Span::none(),
        RocksOptions::DEFAULT,
        Arc::new(NoopSink),
    )
    .expect("store opens")
}

/// Ship `src`'s published snapshot into `dst` exactly as OpenRaft would.
async fn transfer(
    src: &RocksStore,
    dst: &RocksStore,
    meta: &openraft::SnapshotMeta<RaftNodeId, config_storage::RaftNode>,
) {
    use tokio::io::AsyncWriteExt;

    let path = src
        .path()
        .join("snapshots")
        .join(format!("{}.snap", meta.snapshot_id));
    let bytes = std::fs::read(path).expect("the published snapshot is readable");
    let mut sm = dst.state_machine();
    let mut file = sm
        .begin_receiving_snapshot()
        .await
        .expect("the receive slot opens");
    file.write_all(&bytes).await.expect("write received bytes");
    sm.install_snapshot(meta, file)
        .await
        .expect("the install succeeds");
}

/// M5-103 (install half, finding C5B-01): after a **live** install the in-memory state machine
/// agrees with the column families the install just wrote.
///
/// The header-count row above proves the `dedup` family reaches the file. That is only half the
/// claim, and the weaker half: the install also has to put those records back into the
/// in-memory index, because that index — not the column family — is what answers the next
/// resubmission. Before this row the live install rebuilt `KvState` with `from_parts` alone, so
/// a freshly installed follower silently re-applied every request id the snapshot had retained
/// and re-admitted every node the cluster had retired, and an unrelated restart healed both.
///
/// The retired set is the sharper half. A snapshot deliberately does not carry `state_meta`
/// (`NON_DATA_CFS`), so the authority for it is the receiving node's own column family, which
/// the install does not clear — meaning the bug was not "the snapshot lacked it" but "the
/// install threw away state the node still had on disk".
#[retcd_test]
async fn m5_103_install_restores_the_dedup_index_and_retired_set() {
    let leader_dir = tempfile::tempdir().unwrap();
    let follower_dir = tempfile::tempdir().unwrap();

    let leader = open_as_node(leader_dir.path(), 1);
    leader
        .state_machine()
        .apply(vec![
            put(1, "/a", "1", 1),
            put(2, "/b", "2", 2),
            put(3, "/c", "3", 3),
        ])
        .await
        .unwrap();
    let meta = {
        let mut sm = leader.state_machine();
        let mut builder = sm.get_snapshot_builder().await;
        builder
            .build_snapshot()
            .await
            .expect("snapshot builds")
            .meta
    };
    let expected_dedup = leader.reader().dedup_stats().records;
    let expected_hash = leader.reader().state_hash();
    assert_eq!(expected_dedup, 3, "the snapshot carries three records");

    // The follower retires a node from its own log first. That retirement is not in the
    // snapshot and must survive the install.
    let follower = open_as_node(follower_dir.path(), 2);
    follower
        .state_machine()
        .apply(vec![entry(1, Command::RetireNode { node_id: NodeId(9) })])
        .await
        .unwrap();
    assert_eq!(
        follower.reader().retired_nodes(),
        [NodeId(9)].into_iter().collect()
    );

    transfer(&leader, &follower, &meta).await;

    // 1. The index equals the installed column family, by count and by behaviour.
    let stats = follower.reader().dedup_stats();
    assert_eq!(
        stats.records, expected_dedup,
        "the in-memory index must hold every record the install wrote"
    );
    // 2. TA-50's oracle: the applied state digest matches the node the snapshot came from.
    assert_eq!(
        follower.reader().state_hash(),
        expected_hash,
        "an installed follower must hash identically to the node that built the snapshot"
    );

    // 3. A request id the snapshot retained is answered as a duplicate, not applied again.
    let responses = follower
        .state_machine()
        .apply(vec![put(10, "/a", "1", 1)])
        .await
        .unwrap();
    assert!(
        matches!(
            responses[0],
            CommandResponse::Mutation {
                dedup_hit: true,
                ..
            }
        ),
        "a request id present in the installed snapshot must not apply a second time: {:?}",
        responses[0]
    );

    // 4. And the retirement the install did not carry is still in force.
    assert_eq!(
        follower.reader().retired_nodes(),
        [NodeId(9)]
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>(),
        "an install must not re-admit a node the receiving state machine had retired"
    );

    drop(follower);
    drop(leader);

    // Independently of the in-memory view: the column family on disk holds the same three
    // records, each of them readable. The duplicate above allocated nothing.
    assert_eq!(
        raw_dedup_count(follower_dir.path()),
        expected_dedup as usize
    );
}

/// M5-133 (finding C5B-18, ruling M5-R21): the retired-node fence converges *through* a
/// snapshot install, in both directions.
///
/// M5-103 above proves the half that was already true — an install does not throw away a
/// retirement the receiver already had. This row proves the half that was not: a node which was
/// down when the cluster retired an identity, and is caught up by `InstallSnapshot` rather than
/// by `AppendEntries`, must learn that retirement from the snapshot. It has no other source.
/// The `RetireNode` entry is gone from the leader's log (purged here, deliberately, so the test
/// cannot accidentally pass through a path that does not exist in the field), the set lives in
/// `state_meta`, and `state_meta` is not in the snapshot *body* (`NON_DATA_CFS`). Before
/// M5-R21 the receiving node's `is_retired` answered `false` for that identity permanently,
/// which ADR-0023 says can never happen — and it would have re-admitted it at the peer plane
/// and through `AddLearner`, both of them silently.
///
/// Union, not replacement, is the contract, so the row arranges for the two sets to differ in
/// both directions at once: the receiver holds `NodeId(7)` that the snapshot has never heard
/// of, and the snapshot carries `NodeId(9)` that the receiver has never heard of. Afterwards
/// both must be retired. A replacing install passes the "9 is retired" assertion and fails
/// this one, which is exactly why both are here.
#[retcd_test]
async fn m5_133_retired_set_converges_through_snapshot_install() {
    use openraft::storage::RaftLogStorageExt;

    let leader_dir = tempfile::tempdir().unwrap();
    let follower_dir = tempfile::tempdir().unwrap();

    // The leader retires NodeId(9) as a replicated command, through the log, exactly as a real
    // `RemoveMember` step 3 does.
    let leader = open_as_node(leader_dir.path(), 1);
    let entries = vec![
        put(1, "/a", "1", 1),
        put(2, "/b", "2", 2),
        entry(3, Command::RetireNode { node_id: NodeId(9) }),
    ];
    leader
        .log_store()
        .blocking_append(entries.clone())
        .await
        .expect("append");
    leader.state_machine().apply(entries).await.unwrap();
    assert_eq!(
        leader.reader().retired_nodes(),
        [NodeId(9)]
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>()
    );

    let meta = {
        let mut sm = leader.state_machine();
        let mut builder = sm.get_snapshot_builder().await;
        builder
            .build_snapshot()
            .await
            .expect("snapshot builds")
            .meta
    };

    // The header is the carrier, so assert on the file itself rather than only on the effect:
    // a build that stopped writing the set would otherwise be indistinguishable from an
    // install that stopped reading it.
    let snap = leader_dir
        .path()
        .join("snapshots")
        .join(format!("{}.snap", meta.snapshot_id));
    assert_eq!(
        SnapshotReader::open(&snap)
            .expect("the published snapshot opens")
            .header()
            .retired_nodes,
        [NodeId(9)]
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>(),
        "the snapshot header must carry the retired set"
    );

    // And the log that could have taught it the other way is gone.
    leader
        .log_store()
        .purge(log_id(1, 3))
        .await
        .expect("the applied prefix is purgeable");
    assert_eq!(
        leader.raft_log_len(),
        0,
        "the RetireNode entry must not be recoverable from the log"
    );

    // The follower never applied that entry. It has a retirement of its own, which the snapshot
    // knows nothing about.
    let follower = open_as_node(follower_dir.path(), 2);
    follower
        .state_machine()
        .apply(vec![entry(1, Command::RetireNode { node_id: NodeId(7) })])
        .await
        .unwrap();

    transfer(&leader, &follower, &meta).await;

    let expected = [NodeId(7), NodeId(9)]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        follower.reader().retired_nodes(),
        expected,
        "the install must carry the leader's retirement onto a node that never applied it, \
         and must not drop the one the receiver already held"
    );

    drop(leader);
    drop(follower);

    // The union rode the install's final synced batch, so a restart reconstructs it from
    // `state_meta` rather than from anything the live install left in memory.
    let reopened = open_as_node(follower_dir.path(), 2);
    assert_eq!(
        reopened.reader().retired_nodes(),
        expected,
        "the widened fence must be durable, not an in-memory artefact of the install"
    );
    drop(reopened);
}

/// The `Command` shape an **M4** build wrote into its Raft log: no dedup stamp on `Put`, no
/// dedup stamp on `Delete`, no trim watermark on `Compact`, and no `RetireNode` variant at all.
///
/// Mirrored here rather than recovered from git so the row keeps testing M4's layout after M4's
/// source is gone. Only `Serialize` is derived: the point is to *write* these bytes.
#[derive(serde::Serialize)]
enum M4Command {
    Put {
        key: Bytes,
        value: Bytes,
        expected_mod_revision: Option<u64>,
    },
    #[allow(dead_code)]
    Delete {
        key: Bytes,
        expected_mod_revision: Option<u64>,
    },
    #[allow(dead_code)]
    Compact { up_to_revision: u64 },
}

/// `openraft::EntryPayload` mirrored for serialization. The variant order is load-bearing:
/// `postcard` writes a varint index, so `Normal` must stay second. `Membership` carries the
/// real openraft type because it is unchanged between M4 and M5 — only `Command` moved.
#[derive(serde::Serialize)]
enum M4Payload {
    #[allow(dead_code)]
    Blank,
    Normal(M4Command),
    #[allow(dead_code)]
    Membership(openraft::Membership<RaftNodeId, config_storage::RaftNode>),
}

/// `openraft::Entry` mirrored for serialization, with the real `LogId` (also unchanged).
#[derive(serde::Serialize)]
struct M4Entry {
    log_id: LogId<RaftNodeId>,
    payload: M4Payload,
}

/// The column families a v2 (M4) directory has, for the raw reopens this row does.
const V2_FAMILIES: [&str; 6] = [
    "default",
    "raft_log",
    "raft_meta",
    "kv",
    "state_meta",
    "events",
];

/// M5-127 (finding C5B-03, ruling M5-R19): a legacy directory whose Raft log was never drained
/// is refused **by name**, and the refusal leaves the directory exactly as it was found.
///
/// The row above proves the state half of ADR-0021 migrates. This row is about the half that
/// cannot: a log entry is `postcard::to_stdvec(&Entry<TypeConfig>)`, and `postcard` is
/// positional with no per-field tag and no payload version. M5 widened `Command` — a dedup
/// stamp on `Put`/`Delete`, a trim watermark on `Compact`, `RetireNode` added — so M4's bytes
/// are not an older dialect of something this build reads; they are a different grammar. The
/// first assertion makes that concrete rather than assuming it.
///
/// So the refusal is the feature. It happens before the open batch, which matters more than it
/// looks: the operator's fix is to go *back* to the old build and drain the log there, and that
/// is only possible while the directory still says v2.
#[retcd_test]
async fn m5_127_v2_directory_with_an_undrained_log_is_refused_by_name() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let s = open_plain(tmp.path());
        s.state_machine()
            .apply(vec![put(1, "/a", "1", 1)])
            .await
            .unwrap();
        drop(s);
    }

    // One genuine M4 log entry: an unconditional `Put` with no dedup stamp, which is exactly
    // what an M4 leader appended.
    let m4_bytes = postcard::to_stdvec(&M4Entry {
        log_id: log_id(1, 1),
        payload: M4Payload::Normal(M4Command::Put {
            key: Bytes::from_static(b"/m4"),
            value: Bytes::from_static(b"written-by-m4"),
            expected_mod_revision: None,
        }),
    })
    .expect("the M4 entry encodes");

    // The premise of the ruling, asserted rather than assumed: this build cannot read those
    // bytes back. A *silent success* would be the dangerous outcome, so it is checked here.
    if let Ok(entry) = postcard::from_bytes::<Entry<TypeConfig>>(&m4_bytes) {
        panic!(
            "an M4 log entry decoded under the M5 `Command` layout, which means an in-place \
             upgrade would replay a silently different command: {:?}",
            entry.payload
        );
    }

    // Demote the directory to what an M4 build left behind, log included.
    {
        let mut db = rocksdb::DB::open_cf(
            &rocksdb::Options::default(),
            tmp.path(),
            COLUMN_FAMILIES.iter().copied(),
        )
        .expect("reopen raw to demote");
        {
            let meta = db
                .cf_handle(config_storage::CF_STATE_META)
                .expect("state_meta cf");
            db.put_cf(meta, b"format_version", 2u32.to_le_bytes())
                .expect("restamp v2");
            db.delete_cf(meta, b"retired_nodes")
                .expect("drop the v3 key");
            let log = db.cf_handle("raft_log").expect("raft_log cf");
            db.put_cf(log, 1u64.to_be_bytes(), &m4_bytes)
                .expect("seed the undrained entry");
        }
        db.drop_cf(CF_DEDUP).expect("drop the dedup family");
    }

    let refused = RocksStore::open_with(
        tmp.path(),
        identity(),
        dedup_limits(),
        Arc::new(NoFaults),
        Span::none(),
        RocksOptions::DEFAULT,
        Arc::new(NoopSink),
    );
    match refused {
        Err(StorageOpenError::UpgradeRequiresDrainedLog {
            format,
            log_entries,
            ref path,
        }) => {
            assert_eq!(format, 2, "the refusal names the version it found");
            assert_eq!(log_entries, 1, "and how much history is in the way");
            assert_eq!(path.as_path(), tmp.path());
            // The operator has to be able to act on this without reading the source.
            let message = refused.as_ref().unwrap_err().to_string();
            for needle in ["snapshot", "purge", "drained"] {
                assert!(
                    message.contains(needle),
                    "the refusal must name the fix; {needle:?} missing from {message:?}"
                );
            }
        }
        Err(other) => panic!("expected a typed drained-log refusal, got {other:?}"),
        Ok(_) => panic!("a v2 directory with an undrained log must not open"),
    }

    // Nothing was migrated on the way to that error: the marker still reads v2 and the dedup
    // family is still absent, so the previous build can still open this directory and drain it.
    {
        let db = rocksdb::DB::open_cf(&rocksdb::Options::default(), tmp.path(), V2_FAMILIES)
            .expect("the refused directory still opens as v2");
        let meta = db
            .cf_handle(config_storage::CF_STATE_META)
            .expect("state_meta cf");
        assert_eq!(
            db.get_cf(meta, b"format_version").expect("read marker"),
            Some(2u32.to_le_bytes().to_vec()),
            "a refused open must not have stamped the new format version"
        );
    }
    assert!(
        !rocksdb::DB::list_cf(&rocksdb::Options::default(), tmp.path())
            .expect("list")
            .iter()
            .any(|c| c == CF_DEDUP),
        "a refused open must not have created the dedup family either"
    );

    // And the documented fix works: drain the log on the old build, then upgrade.
    {
        let db = rocksdb::DB::open_cf(&rocksdb::Options::default(), tmp.path(), V2_FAMILIES)
            .expect("reopen raw to drain");
        let log = db.cf_handle("raft_log").expect("raft_log cf");
        db.delete_cf(log, 1u64.to_be_bytes()).expect("drain");
    }
    let s = open_plain(tmp.path());
    let reader = s.reader();
    let mut records = 0;
    reader.with_state(&mut |kv: &KvState| records = kv.len());
    assert_eq!(records, 1, "the drained directory migrates as usual");
    drop(reader);
    drop(s);
    assert!(
        rocksdb::DB::list_cf(&rocksdb::Options::default(), tmp.path())
            .expect("list")
            .iter()
            .any(|c| c == CF_DEDUP),
        "and gains the dedup family, exactly as the drained-from-the-start row does"
    );
}
