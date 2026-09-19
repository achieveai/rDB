//! Snapshots and log purge driven by OpenRaft itself (spec §19.7, ADR-0022, M5).
//!
//! `config-storage`'s `m5_snapshot.rs` proves the store does the right thing when *told* to
//! build, install and purge. This file proves the other half: that [`NodeConfig::snapshot`]
//! actually reaches OpenRaft's `SnapshotPolicy`/`max_in_snapshot_log_to_keep`/`purge_batch_size`
//! latch, that OpenRaft then issues a build and a purge on its own, and that the node keeps
//! serving through both — and comes back from a restart whose log has been purged out from
//! under it.
//!
//! On Windows a RocksDB directory cannot be reopened while any handle to it is alive, so the
//! first incarnation is confined to its own scope and dropped before the second opens.

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use common::{get_request, identity, key, principal, put_request};
use config_core::{ConfigStore, NoGossip};
use config_engine::{
    ConfigNode, FormationPlan, InProcTransport, RaftTimers, StorageHandle, WatchHub,
};
use config_storage::{NoFaults, RocksStore, SnapshotConfig};
use openraft::storage::RaftLogStorage;

/// Snapshot early and purge everything the snapshot covers.
///
/// Production defaults (5 000 entries) would need a five-thousand-write test; the latch being
/// exercised is the same one either way, and `logs_to_keep: 0` makes the purge observable as
/// an empty log rather than as an arithmetic argument.
const EAGER: SnapshotConfig = SnapshotConfig {
    logs_since_last: 8,
    logs_to_keep: 0,
    purge_batch_size: 1,
    retain_snapshots: 2,
};

fn try_open_store(
    dir: &Path,
    sink: Arc<WatchHub>,
) -> Result<RocksStore, config_storage::StorageOpenError> {
    RocksStore::open_with(
        dir,
        identity(1),
        config_core::Limits::DEFAULT,
        Arc::new(NoFaults),
        tracing::info_span!("store", node_id = 1u64),
        config_storage::RocksOptions::DEFAULT,
        sink as Arc<dyn config_storage::AppliedBatchSink>,
    )
}

fn open_store(dir: &Path, sink: Arc<WatchHub>) -> RocksStore {
    try_open_store(dir, sink).unwrap_or_else(|e| panic!("open {}: {e}", dir.display()))
}

/// Reopen `dir`, waiting out a RocksDB `LOCK` an in-flight snapshot build is still holding.
///
/// OpenRaft builds snapshots in a task it spawns and never joins
/// (`core/sm/worker.rs:186`), and that task owns the `RaftSnapshotBuilder` — so `Raft::shutdown`
/// can return while a builder still holds the database open. In production that is invisible:
/// a restart is a new process and the OS drops the lock with the old one. In-process it is
/// visible, so this retries within a deadline rather than asserting on a race.
async fn reopen_store(dir: &Path, sink: Arc<WatchHub>) -> RocksStore {
    common::poll_until(
        "the stopped node to release the rocksdb lock",
        Duration::from_secs(10),
        || try_open_store(dir, Arc::clone(&sink)).ok(),
    )
    .await
}

fn watch_hub() -> Arc<WatchHub> {
    WatchHub::with_defaults(config_core::Limits::DEFAULT.watch)
}

/// Start a single voter on `store` with snapshots enabled, forming only if it is fresh.
async fn start_node(
    store: RocksStore,
    transport: Arc<InProcTransport>,
    watch: Arc<WatchHub>,
) -> ConfigNode {
    let identity = identity(1);
    let timers = RaftTimers::default();
    let cfg = common::node_config(identity, timers)
        .with_snapshots(EAGER)
        .expect("the eager snapshot policy is a legal latch");
    let was_fresh = store.is_fresh();
    let node = ConfigNode::start(
        cfg,
        StorageHandle::from(store),
        Arc::clone(&transport) as Arc<dyn config_engine::PeerTransport>,
        Arc::new(NoGossip),
        Arc::new(config_core::AllowAll),
        watch,
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

/// M5-19/M5-16 (engine level): with the policy latched on, OpenRaft builds a snapshot and
/// purges the log it covers — and the node answers reads throughout and after.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_engine_01_openraft_builds_a_snapshot_and_purges_the_log() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (revision, state_hash, purged) = {
        let watch = watch_hub();
        let store = open_store(dir.path(), watch.clone());
        let probe = store.clone();
        let transport = Arc::new(InProcTransport::new(config_engine::NetFault::new()));
        let node = start_node(store, transport, watch).await;
        let client = node.direct_client(principal());

        let mut last = None;
        for i in 0..30u64 {
            last = Some(
                client
                    .put(put_request(&format!("/m5/{i}"), &format!("v{i}")))
                    .await
                    .expect("put"),
            );
        }

        // OpenRaft decides *when* to build; the test only asserts that it does, inside a
        // deadline derived from the timers rather than after a fixed sleep.
        let meta = common::poll_until(
            "openraft to build a snapshot on its own",
            Duration::from_secs(10),
            || probe.snapshot_meta(),
        )
        .await;
        // `LogsSinceLast(n)` counts entries from index 0, so the first build lands at index
        // `n - 1`: an eight-entry window is `0..=7`, not `1..=8`.
        assert!(
            meta.last_log_id
                .is_some_and(|l| l.index + 1 >= EAGER.logs_since_last),
            "the first snapshot must cover at least one policy window: {meta:?}"
        );

        let purged = common::poll_until(
            "openraft to purge the log the snapshot covers",
            Duration::from_secs(10),
            || {
                let m = probe.metrics();
                (m.purges > 0).then_some(m.purged_index)
            },
        )
        .await;
        assert!(purged > 0);
        assert_eq!(
            probe.metrics().purge_refusals,
            0,
            "a purge OpenRaft issues on its own must never be refused"
        );

        // Still a working cluster: the entries backing this read were purged.
        let read = client.get(get_request("/m5/0")).await.expect("get");
        assert_eq!(read.record.expect("record").value, key("v0"));
        let after = client.put(put_request("/after", "x")).await.expect("put");
        assert!(after.revision > last.expect("a put happened").revision);

        node.stop().await.expect("stop");
        (after.revision, node.state_hash(), purged)
    };

    // Restart over a purged log: the state comes back from the snapshot plus whatever log
    // survived, and the published snapshot is still the one on disk.
    let watch = watch_hub();
    let store = reopen_store(dir.path(), watch.clone()).await;
    assert!(!store.is_fresh());
    assert!(
        store.snapshot_meta().is_some(),
        "the published snapshot did not survive the restart"
    );
    let state = store
        .log_store()
        .get_log_state()
        .await
        .expect("reopened log state");
    // At least what was observed purged before the stop: OpenRaft may have purged further
    // between the observation and the shutdown, and that is still a correct log start.
    assert!(
        state.last_purged_log_id.is_some_and(|l| l.index >= purged),
        "the reopened node forgot where its log starts: {:?} < {purged}",
        state.last_purged_log_id
    );

    let transport = Arc::new(InProcTransport::new(config_engine::NetFault::new()));
    let node = start_node(store, transport, watch).await;
    assert_eq!(
        node.state_hash(),
        state_hash,
        "state changed across restart"
    );
    let client = node.direct_client(principal());
    let read = client.get(get_request("/m5/0")).await.expect("get");
    assert_eq!(read.record.expect("record").value, key("v0"));
    assert_eq!(
        client
            .get(get_request("/after"))
            .await
            .expect("get")
            .record
            .expect("record")
            .value,
        key("x")
    );
    let _ = revision;
    node.stop().await.expect("stop");
}

/// M5-21: the policy is a latch, not three independent knobs. A half-applied change would let
/// OpenRaft purge a log no snapshot covers, so it is refused at config time, before a node
/// exists to be damaged by it.
#[config_log::retcd_test]
async fn m5_engine_02_half_applied_snapshot_policy_is_refused() {
    let cfg = common::node_config(identity(1), RaftTimers::default());
    let err = cfg
        .clone()
        .with_snapshots(SnapshotConfig {
            logs_since_last: 0,
            logs_to_keep: 100,
            purge_batch_size: 1,
            retain_snapshots: 2,
        })
        .expect_err("policy off but retention on is not a legal latch");
    assert!(err.to_string().contains("logs_since_last"), "{err}");

    assert!(cfg
        .clone()
        .with_snapshots(SnapshotConfig {
            purge_batch_size: 0,
            ..SnapshotConfig::DEFAULT
        })
        .is_err());
    assert!(cfg.clone().with_snapshots(SnapshotConfig::DEFAULT).is_ok());
    assert!(cfg.with_snapshots(SnapshotConfig::DISABLED).is_ok());
}

/// M5-21a: an ephemeral node never snapshots, whatever the config says.
///
/// `EphemeralStore` cannot build one, and in OpenRaft a `build_snapshot` error is fatal — so
/// honouring an enabled policy there would shut the node down. The engine downgrades to
/// `Never` instead, and says so.
#[config_log::retcd_test]
async fn m5_engine_03_ephemeral_storage_forces_the_policy_off() {
    let cfg = common::node_config(identity(1), RaftTimers::default())
        .with_snapshots(EAGER)
        .expect("legal latch");
    let ephemeral = StorageHandle::from(config_storage::EphemeralStore::new_without_sink(
        identity(1),
        config_core::Limits::DEFAULT,
        Arc::new(NoFaults),
        tracing::Span::none(),
    ));
    assert_eq!(cfg.effective_snapshot(&ephemeral), SnapshotConfig::DISABLED);
    assert!(!cfg.effective_snapshot(&ephemeral).enabled());

    let dir = tempfile::tempdir().expect("temp dir");
    let rocks = StorageHandle::from(open_store(dir.path(), watch_hub()));
    assert_eq!(cfg.effective_snapshot(&rocks), EAGER);
}
