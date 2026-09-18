//! `EphemeralStore` unit tests (test plan TA-4, ADR-0008).
//!
//! These exercise the store directly, without OpenRaft: every fault boundary, the log and
//! vote round trips, and the one-response-per-entry apply contract.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use config_core::{
    ClusterId, ClusterIdentity, Command, CommandResponse, Durability, KvState, Limits,
    MutationOutcome, NodeId, RecoveryEpoch,
};
use config_log::retcd_test;
use config_storage::{
    Boundary, EphemeralStore, FaultAction, FaultInjector, NoFaults, RaftNodeId, TypeConfig,
};
use openraft::storage::{RaftLogStorage, RaftLogStorageExt, RaftStateMachine};
use openraft::{
    BasicNode, CommittedLeaderId, Entry, EntryPayload, LogId, Membership, RaftLogReader, Vote,
};
use tracing::Span;

fn identity() -> ClusterIdentity {
    ClusterIdentity {
        cluster_id: ClusterId::from_bytes([7u8; 16]),
        recovery_epoch: RecoveryEpoch(0),
        node_id: NodeId(1),
    }
}

fn store(faults: Arc<dyn FaultInjector>) -> EphemeralStore {
    EphemeralStore::new(identity(), Limits::DEFAULT, faults, Span::none())
}

fn plain_store() -> EphemeralStore {
    store(Arc::new(NoFaults))
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

fn membership_entry(term: u64, index: u64, voters: [u64; 3]) -> Entry<TypeConfig> {
    let voters: BTreeSet<u64> = voters.into_iter().collect();
    let nodes: BTreeMap<u64, BasicNode> = voters
        .iter()
        .map(|id| (*id, BasicNode::new(format!("inproc://{id}"))))
        .collect();
    Entry {
        log_id: log_id(term, index),
        payload: EntryPayload::Membership(Membership::new(vec![voters], nodes)),
    }
}

fn put(term: u64, index: u64, key: &str, value: &str) -> Entry<TypeConfig> {
    Entry {
        log_id: log_id(term, index),
        payload: EntryPayload::Normal(Command::Put {
            key: Bytes::copy_from_slice(key.as_bytes()),
            value: Bytes::copy_from_slice(value.as_bytes()),
            expected_mod_revision: None,
        }),
    }
}

/// Fails (or crashes) on the `nth` crossing of `boundary`, counting from 1.
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

#[retcd_test]
async fn ephemeral_reports_identity_and_ephemeral_durability() {
    let s = plain_store();
    assert_eq!(s.identity(), identity());
    assert_eq!(s.durability(), Durability::Ephemeral);
    assert_eq!(s.applied_commands(), 0);
    assert_eq!(s.raft_log_len(), 0);
}

#[retcd_test]
async fn log_append_truncate_purge_and_get_log_state() {
    let s = plain_store();
    let mut log = s.log_store();

    let state = log.get_log_state().await.unwrap();
    assert_eq!(state.last_log_id, None);
    assert_eq!(state.last_purged_log_id, None);

    log.blocking_append(vec![blank(1, 1), blank(1, 2), blank(1, 3)])
        .await
        .unwrap();
    assert_eq!(s.raft_log_len(), 3);
    assert_eq!(
        log.get_log_state().await.unwrap().last_log_id,
        Some(log_id(1, 3))
    );

    let entries = log.try_get_log_entries(1..4).await.unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[2].log_id, log_id(1, 3));

    // Truncate is inclusive of the given index.
    log.truncate(log_id(1, 3)).await.unwrap();
    assert_eq!(s.raft_log_len(), 2);
    assert_eq!(
        log.get_log_state().await.unwrap().last_log_id,
        Some(log_id(1, 2))
    );

    // Purge is inclusive too, and leaves `last_purged` behind as the floor.
    log.purge(log_id(1, 2)).await.unwrap();
    assert_eq!(s.raft_log_len(), 0);
    let state = log.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(log_id(1, 2)));
    assert_eq!(state.last_log_id, Some(log_id(1, 2)));
}

#[retcd_test]
async fn vote_and_committed_round_trip() {
    let s = plain_store();
    let mut log = s.log_store();

    assert_eq!(log.read_vote().await.unwrap(), None);
    assert_eq!(log.read_committed().await.unwrap(), None);

    let vote = Vote::new(7, 2);
    log.save_vote(&vote).await.unwrap();
    assert_eq!(log.read_vote().await.unwrap(), Some(vote));

    log.save_committed(Some(log_id(7, 42))).await.unwrap();
    assert_eq!(log.read_committed().await.unwrap(), Some(log_id(7, 42)));
}

#[retcd_test]
async fn is_fresh_flips_after_first_vote_or_append() {
    let s = plain_store();
    assert!(s.is_fresh());
    s.log_store().save_vote(&Vote::new(1, 1)).await.unwrap();
    assert!(!s.is_fresh(), "a saved vote makes the store non-fresh");

    let s2 = plain_store();
    assert!(s2.is_fresh());
    s2.log_store()
        .blocking_append(vec![blank(1, 1)])
        .await
        .unwrap();
    assert!(
        !s2.is_fresh(),
        "an appended entry makes the store non-fresh"
    );
}

#[retcd_test]
async fn apply_returns_one_response_per_entry_including_noop() {
    let s = plain_store();
    let mut sm = s.state_machine();

    let responses = sm
        .apply(vec![
            blank(1, 1),
            membership_entry(1, 2, [1, 2, 3]),
            put(1, 3, "/a", "1"),
            put(1, 4, "/b", "2"),
        ])
        .await
        .unwrap();

    assert_eq!(responses.len(), 4, "exactly one response per entry");
    assert!(matches!(responses[0], CommandResponse::Noop));
    assert!(matches!(responses[1], CommandResponse::Noop));
    assert_eq!(
        responses[2].mutation().unwrap().outcome,
        MutationOutcome::Applied
    );
    assert_eq!(responses[2].mutation().unwrap().revision, 1);
    assert_eq!(responses[3].mutation().unwrap().revision, 2);

    // Blank and membership entries advance `last_applied` but allocate no revision, and are
    // not counted as commands.
    let (last_applied, membership) = sm.applied_state().await.unwrap();
    assert_eq!(last_applied, Some(log_id(1, 4)));
    assert_eq!(
        membership.voter_ids().collect::<BTreeSet<_>>(),
        BTreeSet::from([1, 2, 3])
    );
    assert_eq!(membership.log_id(), &Some(log_id(1, 2)));
    assert_eq!(s.applied_commands(), 2);
    assert_eq!(s.reader().cluster_revision(), 2);
}

#[retcd_test]
async fn state_hash_matches_a_standalone_kv_state() {
    let commands = [
        Command::Put {
            key: Bytes::from_static(b"/z"),
            value: Bytes::from_static(b"9"),
            expected_mod_revision: None,
        },
        Command::Put {
            key: Bytes::from_static(b"/a"),
            value: Bytes::from_static(b"1"),
            expected_mod_revision: Some(0),
        },
        Command::Delete {
            key: Bytes::from_static(b"/z"),
            expected_mod_revision: None,
        },
        Command::Put {
            key: Bytes::from_static(b"/a"),
            value: Bytes::from_static(b"2"),
            expected_mod_revision: None,
        },
    ];

    let s = plain_store();
    let mut sm = s.state_machine();
    let mut entries = vec![blank(1, 1), membership_entry(1, 2, [1, 2, 3])];
    for (i, cmd) in commands.iter().enumerate() {
        entries.push(Entry {
            log_id: log_id(1, 3 + i as u64),
            payload: EntryPayload::Normal(cmd.clone()),
        });
    }
    sm.apply(entries).await.unwrap();

    let mut expected = KvState::with_limits(Limits::DEFAULT);
    for cmd in &commands {
        expected.apply(cmd);
    }

    assert_eq!(s.reader().state_hash(), expected.state_hash());
    assert_eq!(s.reader().cluster_revision(), expected.cluster_revision());
}

#[retcd_test]
async fn every_boundary_is_crossed_in_order() {
    let recorder = Arc::new(Recorder::default());
    let s = store(recorder.clone());

    s.log_store().save_vote(&Vote::new(1, 1)).await.unwrap();
    s.log_store()
        .blocking_append(vec![put(1, 1, "/a", "1")])
        .await
        .unwrap();
    s.state_machine()
        .apply(vec![put(1, 1, "/a", "1")])
        .await
        .unwrap();

    let seen = recorder.0.lock().unwrap().clone();
    assert_eq!(seen, Boundary::ALL.to_vec());

    let counters = s.counters();
    for b in Boundary::ALL {
        assert_eq!(counters.get(b), 1, "{b} crossed exactly once");
    }
    assert_eq!(counters.total(), 8);
}

#[retcd_test]
async fn fail_at_each_boundary_returns_an_error_and_the_store_keeps_working() {
    for boundary in Boundary::ALL {
        let s = store(FailAt::new(boundary, FaultAction::Fail));

        // Drive all eight boundaries; exactly the operation owning `boundary` must fail.
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

        // The store is still usable afterwards: a second attempt succeeds.
        s.log_store().save_vote(&Vote::new(2, 1)).await.unwrap();
        let responses = s
            .state_machine()
            .apply(vec![put(2, 9, "/later", "ok")])
            .await
            .unwrap();
        assert_eq!(responses.len(), 1);
        assert!(responses[0].is_applied(), "{boundary}: store still applies");
    }
}

#[retcd_test]
async fn crash_poisons_the_store_for_every_later_call() {
    let s = store(FailAt::new(Boundary::BeforeStateBatch, FaultAction::Crash));

    // Reachable boundaries before the crash still work.
    s.log_store().save_vote(&Vote::new(1, 1)).await.unwrap();

    let err = s
        .state_machine()
        .apply(vec![put(1, 1, "/a", "1")])
        .await
        .expect_err("crash must surface as a storage error");
    assert!(
        err.to_string().contains("poisoned"),
        "crash error should name poisoning: {err}"
    );
    assert!(s.is_poisoned());

    // Everything afterwards fails, including reads and boundaries the injector never names.
    assert!(s.log_store().read_vote().await.is_err());
    assert!(s.log_store().get_log_state().await.is_err());
    assert!(s.log_store().read_committed().await.is_err());
    assert!(s.log_store().try_get_log_entries(0..10).await.is_err());
    assert!(s.log_store().save_vote(&Vote::new(2, 1)).await.is_err());
    assert!(s
        .log_store()
        .blocking_append(vec![blank(1, 1)])
        .await
        .is_err());
    assert!(s.state_machine().applied_state().await.is_err());
    assert!(s
        .state_machine()
        .apply(vec![put(1, 2, "/b", "2")])
        .await
        .is_err());
}

#[retcd_test]
async fn snapshots_are_unsupported_but_never_panic() {
    let s = plain_store();
    let mut sm = s.state_machine();
    assert!(sm.get_current_snapshot().await.unwrap().is_none());
    assert!(sm.begin_receiving_snapshot().await.is_ok());
    let mut builder = sm.get_snapshot_builder().await;
    use openraft::RaftSnapshotBuilder;
    assert!(builder.build_snapshot().await.is_err());
}
