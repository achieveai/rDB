//! The engine on persistent storage (spec §19.1, ADR-0008, ADR-0011, ADR-0016).
//!
//! `m1_cluster.rs` proves an Ephemeral node comes back empty *by design*. This is the other
//! half of that sentence: the same engine, the same client calls, on a [`RocksStore`] — and
//! the value is still there after the process that wrote it is gone.
//!
//! On Windows a RocksDB directory cannot be reopened while any handle to it is alive, so the
//! first incarnation is confined to its own scope and dropped before the second opens. That
//! constraint is also the point: an operator restarting a daemon gets exactly this sequence.

mod common;

use std::path::Path;
use std::sync::Arc;

use common::{get_request, identity, key, principal, put_request};
use config_core::{ConfigStore, Durability, NoGossip};
use config_engine::{ConfigNode, FormationPlan, InProcTransport, RaftTimers, StorageHandle};
use config_storage::{NoFaults, RocksStore, TypeConfig};
use openraft::storage::{RaftLogStorage, RaftLogStorageExt, RaftStateMachine};
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};

/// Open the store at `dir` the way a daemon does: **before** the node exists, so an open
/// failure is the daemon's to report and never becomes a half-started node (ADR-0011).
fn open_store(dir: &Path) -> RocksStore {
    RocksStore::open(
        dir,
        identity(1),
        config_core::Limits::DEFAULT,
        Arc::new(NoFaults),
        tracing::info_span!("store", node_id = 1u64),
    )
    .unwrap_or_else(|e| panic!("open {}: {e}", dir.display()))
}

/// Start a single-voter node on `store`, forming it only if it is not already formed.
///
/// A restart must **not** re-form: the membership is in the store, and forming again would be
/// the wiped-node-overwrites-the-cluster failure ADR-0011 exists to prevent.
async fn start_node(store: RocksStore, transport: Arc<InProcTransport>) -> ConfigNode {
    let identity = identity(1);
    let timers = RaftTimers::default();
    let mut cfg = common::node_config(identity, timers);
    cfg.raft = timers;
    let was_fresh = store.is_fresh();
    let node = ConfigNode::start(
        cfg,
        StorageHandle::from(store),
        Arc::clone(&transport) as Arc<dyn config_engine::PeerTransport>,
        Arc::new(NoGossip),
        Arc::new(config_core::AllowAll),
    )
    .await
    .expect("node start on rocks");
    transport.register(identity.node_id, node.peer_handler());

    if was_fresh {
        node.form_cluster(FormationPlan::new(
            &identity,
            [(
                identity.node_id,
                InProcTransport::endpoint(identity.node_id),
            )],
        ))
        .await
        .expect("single-node formation");
    }
    node.wait_for_leader(timers.election_timeout() * 8)
        .await
        .expect("a single voter leads");
    common::poll_until(
        "committed membership on the single voter",
        timers.election_timeout() * 8,
        || node.committed_membership().is_formed().then_some(()),
    )
    .await;
    node
}

/// Start a node over `store` and return immediately after `Raft::new` completes — no
/// formation, no `wait_for_leader`. M2-14/M2-15 only care about the replay `Raft::new` itself
/// performs (openraft's `StorageHelper::get_initial_state`/`reapply_committed`, which run
/// unconditionally on storage state before membership is ever read); the stores these two rows
/// build have logs and a committed pointer but no membership, so waiting for a leader here
/// would hang forever (an unformed single voter never elects itself).
async fn start_node_for_replay_only(
    store: RocksStore,
    transport: Arc<InProcTransport>,
) -> ConfigNode {
    let identity = identity(1);
    let timers = RaftTimers::default();
    let mut cfg = common::node_config(identity, timers);
    cfg.raft = timers;
    let node = ConfigNode::start(
        cfg,
        StorageHandle::from(store),
        Arc::clone(&transport) as Arc<dyn config_engine::PeerTransport>,
        Arc::new(NoGossip),
        Arc::new(config_core::AllowAll),
    )
    .await
    .expect("node start on rocks");
    transport.register(identity.node_id, node.peer_handler());
    node
}

/// A blank (no-op payload) entry at `index`, term 1 — enough to drive replay without a
/// `config_core::Command` to encode/apply.
fn blank_entry(index: u64) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Blank,
    }
}

/// This test's own `"applied non-command entry"` lines (test plan §5: scoped to `testRun`),
/// carrying the `log_index` field `RocksSm::apply` logs for every `Blank`/`Membership` entry.
fn replayed_blank_indexes(method: &str) -> Vec<u64> {
    let path = config_log::layer::test_file_path(
        &config_log::testing::test_log_dir(),
        module_path!(),
        method,
    );
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
    let run = config_log::testing::test_run_id();
    text.lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter(|l| l["testRun"] == run && l["@m"] == "applied non-command entry")
        .filter_map(|l| l["log_index"].as_u64())
        .collect()
}

/// A write acknowledged on RocksDB is still there after a full restart.
///
/// Everything between the two incarnations is dropped — node, store, transport — so the
/// second `open` genuinely reads the directory rather than a cached handle. What must come
/// back is not only the value but the *cluster*: membership, revision, and the applied
/// command count, because a node that reloaded the data and forgot it was formed would be
/// unready and could not serve it anyway.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_engine_01_rocks_restart_reads_back() {
    let dir = tempfile::tempdir().expect("temp dir");

    let (revision, state_hash) = {
        let store = open_store(dir.path());
        assert!(store.is_fresh(), "a new directory must open fresh");
        let transport = Arc::new(InProcTransport::new(config_engine::NetFault::new()));
        let node = start_node(store, transport).await;

        assert_eq!(
            node.capabilities().durability,
            Durability::Persistent,
            "a synced RocksStore must report Persistent (ADR-0016)"
        );

        let client = node.direct_client(principal());
        let put = client.put(put_request("/a/k", "v1")).await.expect("put");
        let read = client.get(get_request("/a/k")).await.expect("get");
        assert_eq!(read.record.expect("record").value, key("v1"));

        node.stop().await.expect("stop");
        (put.revision, node.state_hash())
    };

    // Second incarnation: same directory, nothing carried over but the bytes on disk.
    let store = open_store(dir.path());
    assert!(
        !store.is_fresh(),
        "the reopened store lost its vote, log and applied state"
    );
    // `applied_commands` counts applies *since this open* (TA-24), so it is 0 here by design.
    // What has to have survived is the applied state itself, before any node is started on it.
    assert_eq!(store.applied_commands(), 0);
    assert!(
        store.reader().last_applied().is_some(),
        "the reopened store forgot how far it had applied"
    );
    assert_eq!(
        store.reader().cluster_revision(),
        revision,
        "the reopened store lost the cluster revision"
    );

    let transport = Arc::new(InProcTransport::new(config_engine::NetFault::new()));
    let node = start_node(store, transport).await;

    assert!(
        node.committed_membership().is_formed(),
        "a restarted node must not need re-forming"
    );
    assert_eq!(
        node.state_hash(),
        state_hash,
        "applied state differs after the restart"
    );

    let client = node.direct_client(principal());
    let got = client
        .get(get_request("/a/k"))
        .await
        .expect("get after restart");
    assert_eq!(got.read_revision, revision);
    let record = got.record.expect("the value survived the restart");
    assert_eq!(record.value, key("v1"));
    assert_eq!(record.mod_revision, revision);

    // And the restarted node keeps writing where the old one left off.
    let next = client.put(put_request("/a/k2", "v2")).await.expect("put");
    assert_eq!(next.revision, revision + 1);

    node.stop().await.expect("stop");
    drop(node);
    drop(client);
    // `dir` drops last: on Windows the directory cannot be removed while a handle is open.
}

/// M2-14 `read_committed_drives_replay_window` (test plan §3.2): a store with `last_applied`
/// 3 entries in, `committed` 7 entries in, logs `0..=6` present (7 entries), replays exactly
/// the remaining 4 indexes, contiguous, once each, in order, when a `Raft` starts over it.
///
/// The vendored `openraft` source (`storage/helper.rs`, `StorageHelper::get_initial_state`)
/// confirms this window is driven purely by storage state, unconditionally, and runs *before*
/// membership is ever read — so the window can be built directly against the store's own
/// `RaftLogStorage`/`RaftStateMachine` handles (no client writes, no leadership, no formation)
/// and observed through `Raft::new`'s replay alone.
///
/// Logs start at index **0**, not 1: `openraft::engine::LogIdList::load_log_ids` (called from
/// `get_initial_state` right after replay) does `sto.get_log_id(0)` whenever
/// `last_purged_log_id` is `None` — i.e. it hard-requires a real entry at index 0 on a store
/// that has never purged, discovered empirically (a 1-based log here fails `Raft::new` with
/// `LogIndexNotFound { want: 0, got: None }`, not a replay-window error at all). The row's own
/// numbers (`last_applied = 3`, `committed = 7`) are preserved as *counts*; only the starting
/// index shifts down by one to satisfy that constraint.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_engine_02_read_committed_drives_replay_window() {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = open_store(dir.path());

    let entries: Vec<Entry<TypeConfig>> = (0..7u64).map(blank_entry).collect();
    store
        .log_store()
        .blocking_append(entries.clone())
        .await
        .expect("seed logs 0..=6 (7 entries)");
    store
        .state_machine()
        .apply(entries[..3].to_vec())
        .await
        .expect("pre-apply the first 3 entries (indexes 0..=2)");
    store
        .log_store()
        .save_committed(Some(entries[6].log_id))
        .await
        .expect("committed = the 7th entry (index 6)");

    let transport = Arc::new(InProcTransport::new(config_engine::NetFault::new()));
    let node = start_node_for_replay_only(store, transport).await;

    let indexes = replayed_blank_indexes("m2_engine_02_read_committed_drives_replay_window");
    // The phase-1 `apply(entries[..3])` call above also logs 3 "applied non-command entry"
    // lines (indexes 0..=2); `last_applied` after that is exactly the fence that separates
    // "already applied before Raft::new" from "replayed by Raft::new", so filtering `> 2`
    // leaves only what the replay itself produced.
    let replayed: Vec<u64> = indexes.into_iter().filter(|&i| i > 2).collect();
    assert_eq!(
        replayed,
        vec![3, 4, 5, 6],
        "replay must apply exactly the 4 remaining indexes, once each, in order"
    );

    node.stop().await.expect("stop");
}

/// M2-15 `replay_chunked_over_64_entries` (test plan §3.2): `last_applied = 0` (nothing
/// pre-applied), `committed` = the 200th entry, logs `0..=199` present (200 entries) — replay
/// must apply all 200, contiguous, no gap, no duplicate, no reorder, even though openraft
/// internally re-applies them in chunks of 64 (`reapply_committed`, vendored `openraft`
/// `storage/helper.rs`: `let chunk_size = 64;`). The chunking is an internal implementation
/// detail this row deliberately does not assert on directly (per the test plan's own evidence
/// column) — only the end-to-end index sequence. Logs start at index 0 for the same reason as
/// M2-14 (`LogIdList::load_log_ids` requires a real entry at index 0 when nothing was ever
/// purged); this row needed no shift on the "nothing applied" side, since `None` already means
/// index 0 is where replay starts.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_engine_03_replay_chunked_over_64_entries() {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = open_store(dir.path());

    let entries: Vec<Entry<TypeConfig>> = (0..200u64).map(blank_entry).collect();
    store
        .log_store()
        .blocking_append(entries.clone())
        .await
        .expect("seed logs 0..=199 (200 entries)");
    store
        .log_store()
        .save_committed(Some(entries[199].log_id))
        .await
        .expect("committed = the 200th entry (index 199)");

    let transport = Arc::new(InProcTransport::new(config_engine::NetFault::new()));
    let node = start_node_for_replay_only(store, transport).await;

    let replayed = replayed_blank_indexes("m2_engine_03_replay_chunked_over_64_entries");
    let expected: Vec<u64> = (0..200u64).collect();
    assert_eq!(
        replayed, expected,
        "replay must apply exactly indexes 1..=200, once each, contiguous, in order, whatever \
         chunk size openraft uses internally"
    );

    node.stop().await.expect("stop");
}
