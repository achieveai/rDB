//! A Rocks cluster's data root is gone once its test is over (TA-11, TA-16.3).
//!
//! On 2026-09-27 a host held 839 `retcd-testkit-*` directories, 5.7 GB, from one week of runs.
//! Each was a cluster data root with `node-1` half deleted: `000004.log`, `CURRENT` and
//! `IDENTITY` gone, `LOCK` and everything after it still there. `TempDir`'s `Drop` had run
//! while RocksDB still held `LOCK` open, Windows refused that one file, and `Drop` swallows the
//! error. Two ways reach that state, and each has a row here.

use std::path::{Path, PathBuf};
use std::time::Duration;

use bytes::Bytes;
use config_core::PutRequest;
use config_testkit::cluster::{Cluster, StorageKind};
use config_testkit::poll::TestTimers;

fn put_req(k: &str, v: &str) -> PutRequest {
    PutRequest {
        dedup: None,
        key: Bytes::copy_from_slice(k.as_bytes()),
        value: Bytes::copy_from_slice(v.as_bytes()),
        expected_mod_revision: None,
    }
}

/// Start a Rocks cluster, write through it so every store has files open, and return the
/// directory that holds every node's data.
async fn written_rocks_cluster() -> (Cluster, PathBuf) {
    let cluster = Cluster::start(3, StorageKind::ROCKS).await;
    let leader = cluster.leader().await;
    let written = cluster
        .client(leader)
        .put(put_req("/cleanup", "v1"))
        .await
        .expect("write on a Rocks leader");
    cluster
        .wait_revision_all(written.revision, cluster.deadline(10))
        .await
        .expect("every node applies the write");
    let root = cluster
        .data_dir(leader)
        .parent()
        .expect("a node directory sits under the cluster's data root")
        .to_path_buf();
    assert!(root.is_dir(), "the data root exists while the cluster runs");
    (cluster, root)
}

fn describe(root: &Path) -> String {
    let left: Vec<String> = walk(root)
        .into_iter()
        .map(|p| p.strip_prefix(root).unwrap_or(&p).display().to_string())
        .collect();
    format!("{root:?} still holds {left:?}")
}

fn walk(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(walk(&path));
        }
        out.push(path);
    }
    out
}

/// `shutdown` returning means the data root is gone, even when the last store handle is
/// released a moment *after* the node's own shutdown completes.
///
/// That moment is real: the last clone of a node's store belongs to tasks that shutdown only
/// signals, so RocksDB's `LOCK` outlives the `await` (see `m5_snapshot_cluster`'s
/// `drop_the_published_snapshot`). The row stands in for those tasks with one of its own that
/// holds a clone briefly, so the race is reproduced every run instead of on a loaded host.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_removes_the_data_root_after_the_last_store_handle_closes() {
    let (cluster, root) = written_rocks_cluster().await;
    let leader = cluster.leader().await;

    let late = cluster.rocks_store(leader);
    let released = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await; // testkit:allow-sleep: stands in for a task that releases the store late
        drop(late);
    });

    cluster.shutdown().await;
    assert!(!root.exists(), "after shutdown, {}", describe(&root));
    released.await.expect("the late holder ran to completion");
}

/// A cluster dropped without `shutdown` — a row that never calls it, or one that panicked —
/// still has its data root removed once the test's runtime is gone.
///
/// The runtime is built by hand, the same way `#[tokio::test]` builds it, because the stores
/// close only when that runtime drops the tasks that hold them, and the check has to happen
/// after that.
#[config_log::retcd_test]
fn a_cluster_dropped_without_shutdown_still_removes_its_data_root() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    let root = runtime.block_on(async {
        let (cluster, root) = written_rocks_cluster().await;
        drop(cluster);
        root
    });
    drop(runtime);

    let deadline = TestTimers::DEFAULT.multiple(4);
    let start = std::time::Instant::now();
    while root.exists() && start.elapsed() < deadline {
        std::thread::sleep(Duration::from_millis(50)); // testkit:allow-sleep: bounded poll for a removal on another thread
    }
    assert!(
        !root.exists(),
        "{:?} after the runtime dropped, {}",
        start.elapsed(),
        describe(&root)
    );
}
