//! The engine's "is a snapshot build running" answer after a build that ended without publishing
//! (#68 finding, ruling A-R440).
//!
//! `ConfigNode::trigger_snapshot` answers `AlreadyInProgress` while
//! [`StorageHandle::snapshot_builds_in_flight`] is above zero. That number was derived from event
//! counters: builds started, minus those that published, minus those that failed. A superseded
//! build does neither, so it stayed "in flight" for ever and every later trigger was refused:
//! snapshots stopped and the log grew. The store is driven directly here, because a superseded
//! build needs an install to land inside the build's capture, and only the store's own fault
//! hook can hold a capture open.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use config_core::{ClusterId, ClusterIdentity, Command, Limits, NodeId, RecoveryEpoch};
use config_engine::StorageHandle;
use config_storage::{
    Boundary, FaultAction, FaultInjector, NoFaults, RaftNode, RaftNodeId, RocksStore, TypeConfig,
};
use openraft::storage::{RaftLogStorage, RaftLogStorageExt, RaftSnapshotBuilder, RaftStateMachine};
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId, SnapshotMeta};
use tokio::io::AsyncWriteExt;
use tracing::Span;

const PATIENCE: Duration = Duration::from_secs(30);

fn open_as(dir: &Path, node: u64, faults: Arc<dyn FaultInjector>) -> RocksStore {
    let id = ClusterIdentity {
        cluster_id: ClusterId::from_bytes([7; 16]),
        recovery_epoch: RecoveryEpoch(0),
        node_id: NodeId(node),
    };
    RocksStore::open(dir, id, Limits::DEFAULT, faults, Span::none()).expect("store opens")
}

/// Append and apply puts `1..=count`.
async fn seed(s: &RocksStore, count: u64) {
    for i in 1..=count {
        let entry: Entry<TypeConfig> = Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), i),
            payload: EntryPayload::Normal(Command::Put {
                key: Bytes::from(format!("/b68/{i}")),
                value: Bytes::from_static(b"v"),
                expected_mod_revision: None,
                dedup: None,
            }),
        };
        s.log_store()
            .blocking_append(vec![entry.clone()])
            .await
            .expect("append");
        s.state_machine().apply(vec![entry]).await.expect("apply");
    }
}

async fn build(s: &RocksStore) -> Result<SnapshotMeta<RaftNodeId, RaftNode>, String> {
    let mut sm = s.state_machine();
    let mut builder = sm.get_snapshot_builder().await;
    builder
        .build_snapshot()
        .await
        .map(|snap| snap.meta)
        .map_err(|e| e.to_string())
}

/// Holds the first snapshot capture until the test releases it.
struct HoldFirstCapture {
    seen: AtomicU64,
    reached: Mutex<mpsc::Sender<()>>,
    release: Mutex<mpsc::Receiver<()>>,
}

impl FaultInjector for HoldFirstCapture {
    fn before(&self, _boundary: Boundary) -> FaultAction {
        FaultAction::Proceed
    }

    fn during_snapshot_capture(&self) {
        if self.seen.fetch_add(1, Ordering::SeqCst) == 0 {
            self.reached
                .lock()
                .unwrap()
                .send(())
                .expect("test is waiting");
            self.release
                .lock()
                .unwrap()
                .recv_timeout(PATIENCE)
                .expect("the test releases the capture");
        }
    }
}

#[config_log::retcd_test]
async fn b68_a_superseded_build_is_not_left_in_flight() {
    let src_dir = tempfile::tempdir().unwrap();
    let dst_dir = tempfile::tempdir().unwrap();
    {
        let src = open_as(src_dir.path(), 1, Arc::new(NoFaults));
        seed(&src, 6).await;
        let meta = build(&src).await.expect("source build");
        let bytes = std::fs::read(
            src.path()
                .join("snapshots")
                .join(format!("{}.snap", meta.snapshot_id)),
        )
        .expect("published snapshot is readable");

        let (reached_tx, reached_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let hold = Arc::new(HoldFirstCapture {
            seen: AtomicU64::new(0),
            reached: Mutex::new(reached_tx),
            release: Mutex::new(release_rx),
        });
        let dst = open_as(dst_dir.path(), 2, hold);
        seed(&dst, 3).await;
        let handle = StorageHandle::from(dst.clone());

        // The build captures index 3 and is held there.
        let building = {
            let dst = dst.clone();
            tokio::spawn(async move { build(&dst).await })
        };
        tokio::task::spawn_blocking(move || reached_rx.recv_timeout(PATIENCE))
            .await
            .expect("wait task joins")
            .expect("the build reaches its capture");
        assert_eq!(handle.snapshot_builds_in_flight(), 1, "the held build runs");

        // An install of index 6 lands meanwhile, so the build's view is superseded.
        {
            let mut sm = dst.state_machine();
            let mut file = sm.begin_receiving_snapshot().await.expect("receive slot");
            file.write_all(&bytes).await.expect("write received bytes");
            sm.install_snapshot(&meta, file).await.expect("install");
        }
        release_tx.send(()).expect("the build is held");
        let built = building
            .await
            .expect("the build task joins")
            .expect("a superseded build hands OpenRaft the current snapshot");
        assert_eq!(
            built.snapshot_id, meta.snapshot_id,
            "the build was superseded: it returned the installed snapshot, not its own"
        );

        assert_eq!(
            handle.snapshot_builds_in_flight(),
            0,
            "a superseded build has ended, so a trigger may start the next one"
        );
        let calls = dst.snapshot_build_calls();
        build(&dst).await.expect("the next build runs");
        assert_eq!(dst.snapshot_build_calls(), calls + 1);
        assert_eq!(handle.snapshot_builds_in_flight(), 0);
    }
}
