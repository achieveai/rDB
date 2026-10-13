//! Per-test lines go through a background writer thread. Nothing logged may be missing when a
//! test reads its own file, and nothing may be missing from the file once the process exits.

use std::path::{Path, PathBuf};
use std::process::Command;

use config_log::retcd_test;
use config_log::testing::{test_log_dir, test_run_id};
use serde_json::Value;

const THREADS: usize = 8;
const PER_THREAD: usize = 5_000;
const LINE: &str = "writer row line";

/// Log `per_thread` lines from each of `threads` threads, all inside the current test span.
fn log_lines(threads: usize, per_thread: usize) {
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let span = tracing::Span::current();
            std::thread::spawn(move || {
                let _entered = span.enter();
                for seq in 0..per_thread {
                    tracing::info!(t, seq, "writer row line");
                }
            })
        })
        .collect();
    for h in handles {
        h.join().expect("logging thread");
    }
}

/// `(thread, seq)` of every `LINE` in `path`, in file order.
fn lines_in(path: &Path, run: Option<&str>) -> Vec<(u64, u64)> {
    let text = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
    text.lines()
        .map(|l| serde_json::from_str::<Value>(l).expect("one JSON object per line"))
        .filter(|l| l["@m"] == LINE)
        .filter(|l| run.is_none_or(|run| l["testRun"] == run))
        .map(|l| (l["t"].as_u64().unwrap(), l["seq"].as_u64().unwrap()))
        .collect()
}

/// Every thread's lines are all there, and in the order that thread logged them.
fn assert_complete(what: &str, lines: &[(u64, u64)], threads: usize, per_thread: usize) {
    assert_eq!(lines.len(), threads * per_thread, "lines in {what}'s file");
    let mut next = vec![0u64; threads];
    for &(t, seq) in lines {
        let t = usize::try_from(t).unwrap();
        assert_eq!(seq, next[t], "{what}: thread {t}'s lines out of order");
        next[t] += 1;
    }
}

#[retcd_test]
fn n_lines_logged_are_n_lines_read() {
    log_lines(THREADS, PER_THREAD);
    let path = config_log::layer::test_file_path(
        &test_log_dir(),
        module_path!(),
        "n_lines_logged_are_n_lines_read",
    );
    assert_complete(
        "n_lines_logged_are_n_lines_read",
        &lines_in(&path, Some(test_run_id())),
        THREADS,
        PER_THREAD,
    );
}

/// Run by [`lines_queued_at_process_exit_are_not_lost`] as the only test of a child process,
/// which exits as soon as this returns. Nothing here reads the file, so nothing flushes it
/// except the exit path under test.
#[retcd_test]
#[ignore = "run in a child process by lines_queued_at_process_exit_are_not_lost"]
fn child_logs_then_returns() {
    log_lines(THREADS, PER_THREAD);
}

/// Logs every line while its runtime shuts down, after the test body has returned.
struct LogOnDrop(tracing::Span);

impl Drop for LogOnDrop {
    fn drop(&mut self) {
        let _entered = self.0.enter();
        log_lines(THREADS, PER_THREAD);
    }
}

/// The async twin of [`child_logs_then_returns`]: a task that never finishes holds a
/// [`LogOnDrop`], so its lines are logged only when the runtime drops the task.
#[retcd_test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "run in a child process by lines_queued_at_process_exit_are_not_lost"]
async fn child_async_logs_at_runtime_shutdown() {
    let guard = LogOnDrop(tracing::Span::current());
    tokio::spawn(async move {
        let _guard = guard;
        std::future::pending::<()>().await;
    });
}

#[test]
fn lines_queued_at_process_exit_are_not_lost() {
    for child in [
        "child_logs_then_returns",
        "child_async_logs_at_runtime_shutdown",
    ] {
        child_writes_every_line(child);
    }
}

fn child_writes_every_line(child: &str) {
    let root = tempfile::tempdir().expect("temp log root");
    let out = Command::new(std::env::current_exe().expect("this test binary"))
        .args([child, "--exact", "--ignored", "--test-threads=1"])
        .env("RETCD_TEST_LOG_DIR", root.path())
        .output()
        .expect("run the child");
    assert!(
        out.status.success(),
        "{child} failed: {}\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("1 passed"),
        "{child} ran no test:\n{stdout}"
    );

    // `<root>/<child run id>/<module>/<method>.jsonl`; the child is the only run under `root`.
    let runs: Vec<PathBuf> = std::fs::read_dir(root.path())
        .expect("read the temp log root")
        .map(|e| e.expect("entry").path())
        .filter(|p| p.is_dir())
        .collect();
    assert_eq!(runs.len(), 1, "one child run directory: {runs:?}");
    let path = runs[0]
        .join("test_log_writer")
        .join(format!("{child}.jsonl"));
    assert_complete(child, &lines_in(&path, None), THREADS, PER_THREAD);
}
