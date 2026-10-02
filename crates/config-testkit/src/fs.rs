//! Per-test filesystem isolation (TA-11; test plan §6 anti-flake rule 5).
//!
//! Any on-disk state a test needs goes under a directory created here, never under a shared
//! `target/tmp`, so parallel test runs cannot collide and a panic still cleans up.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::poll::TestTimers;

/// A fresh, uniquely named temp directory for one test's on-disk state.
///
/// Deleted on `Drop`, including when the caller's test panics while it is in scope. Callers
/// that need the path to outlive a scope keep the returned [`tempfile::TempDir`] alive rather
/// than extracting the path.
///
/// Created under `RETCD_TEST_DATA_DIR` when it is set, and the system temp directory
/// otherwise. The gate scripts point it at a per-run directory in their target directory and
/// remove that after a passing run, so nothing a run leaves behind piles up in `%TEMP%`.
pub fn temp_dir() -> tempfile::TempDir {
    let mut builder = tempfile::Builder::new();
    builder.prefix("retcd-testkit-");
    match std::env::var_os("RETCD_TEST_DATA_DIR") {
        Some(root) => {
            std::fs::create_dir_all(&root).expect("create RETCD_TEST_DATA_DIR");
            builder.tempdir_in(root)
        }
        None => builder.tempdir(),
    }
    .expect("create per-test temp directory")
}

/// Remove `dir` if nothing has a file open under it any more, and report whether it is gone.
///
/// A plain `TempDir` drop is not enough for a directory RocksDB used. Windows will not delete
/// a file another handle holds without delete sharing, which is how RocksDB holds `LOCK`, so
/// the drop deletes the files it can, stops at the first it cannot, and swallows the error.
/// The result is a store with its `CURRENT` gone and its `LOCK` still there, left for good.
///
/// Windows also refuses to rename a directory while *any* file under it is open, whatever the
/// sharing. So the rename goes first: it succeeds only once the last handle has closed, and a
/// refusal leaves every file in place for the next attempt. Elsewhere the rename always
/// succeeds and the removal works on open files, as it always did.
pub fn try_remove(dir: &Path) -> bool {
    let mut grave = dir.as_os_str().to_owned();
    grave.push(".reap");
    let grave = PathBuf::from(grave);
    if dir.exists() && std::fs::rename(dir, &grave).is_err() {
        return false;
    }
    match std::fs::remove_dir_all(&grave) {
        Ok(()) => true,
        Err(e) => e.kind() == std::io::ErrorKind::NotFound,
    }
}

/// Remove `dir` on a background thread once the files under it are closed.
///
/// For a directory whose last holder will go away only after the caller returns, as with a
/// cluster dropped inside a test: its stores close when the test's runtime drops the tasks
/// holding them. Gives up after a bounded wait and leaves the directory whole, never half
/// deleted. The thread does not hold the process open: when the last test of a binary drops a
/// cluster, the process can exit first and the directory stays, whole.
pub fn remove_when_closed(dir: tempfile::TempDir) {
    // From here on only `try_remove` touches it: the `TempDir`'s own drop would half delete it.
    let path = dir.keep();
    let patience = TestTimers::DEFAULT.multiple(40);
    let reaper = std::thread::Builder::new()
        .name("retcd-testkit-reaper".into())
        .spawn(move || {
            let start = Instant::now();
            while !try_remove(&path) {
                if start.elapsed() >= patience {
                    tracing::warn!(
                        path = %path.display(),
                        waited = ?start.elapsed(),
                        "test data directory still in use; left in place"
                    );
                    return;
                }
                std::thread::sleep(Duration::from_millis(20)); // testkit:allow-sleep: bounded retry of a removal, not a synchronization sleep
            }
        });
    if let Err(e) = reaper {
        tracing::warn!(error = %e, "could not start the test data reaper");
    }
}
