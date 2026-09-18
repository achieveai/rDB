//! Per-test filesystem isolation (TA-11; test plan §6 anti-flake rule 5).
//!
//! Any on-disk state a test needs goes under a directory created here, never under a shared
//! `target/tmp`, so parallel test runs cannot collide and a panic still cleans up.

/// A fresh, uniquely named temp directory for one test's on-disk state.
///
/// Deleted on `Drop`, including when the caller's test panics while it is in scope. Callers
/// that need the path to outlive a scope keep the returned [`tempfile::TempDir`] alive rather
/// than extracting the path.
pub fn temp_dir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("retcd-testkit-")
        .tempdir()
        .expect("create per-test temp directory")
}
