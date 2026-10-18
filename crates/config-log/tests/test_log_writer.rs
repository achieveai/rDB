//! Per-test lines go through a background writer thread. Nothing logged may be missing when a
//! test reads its own file, nor once a test has ended normally and the process exits.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::mpsc;
use std::time::{Duration, Instant};

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
    // `--nocapture`, so a flush error the test guard prints reaches `out.stderr`.
    let out = Command::new(std::env::current_exe().expect("this test binary"))
        .args([
            child,
            "--exact",
            "--ignored",
            "--test-threads=1",
            "--nocapture",
        ])
        .env("RETCD_TEST_LOG_DIR", root.path())
        .output()
        .expect("run the child");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success() && stdout.contains("1 passed"),
        "{child} failed or ran no test: {}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        out.status
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
    let lines = lines_in(&path, None);
    assert!(
        lines.len() == THREADS * PER_THREAD,
        "{child}: {} of {} lines in its file\nchild stderr:\n{stderr}",
        lines.len(),
        THREADS * PER_THREAD
    );
    assert_complete(child, &lines, THREADS, PER_THREAD);
}

/// A per-test file that cannot be opened loses its own lines and nothing else: the shared
/// writer keeps going, and the reader of that file is told the path and the cause.
#[retcd_test]
fn an_unopenable_log_file_loses_only_its_own_lines() {
    const BLOCKED: &str = "an_unopenable_log_file_blocked_twin";
    // A directory where the twin's `<method>.jsonl` should go, so opening it fails.
    let blocker = config_log::layer::test_file_path(&test_log_dir(), module_path!(), BLOCKED);
    std::fs::create_dir_all(&blocker).expect("create the blocking directory");

    let twin = tracing::info_span!("twin", testMethod = BLOCKED);
    twin.in_scope(|| tracing::info!("to the blocked file"));
    tracing::info!("to my own file");

    let read_blocked = std::panic::catch_unwind(|| {
        config_log::layer::test_file_path(&test_log_dir(), module_path!(), BLOCKED)
    });
    std::fs::remove_dir(&blocker).expect("remove the blocking directory");
    let message = read_blocked
        .expect_err("reading the blocked file must fail")
        .downcast::<String>()
        .map(|m| *m)
        .unwrap_or_default();
    assert!(
        message.contains(&blocker.display().to_string()),
        "the panic names the path: {message}"
    );

    let own = config_log::layer::test_file_path(
        &test_log_dir(),
        module_path!(),
        "an_unopenable_log_file_loses_only_its_own_lines",
    );
    let text = std::fs::read_to_string(&own).expect("own log file");
    assert!(
        text.contains("to my own file"),
        "the writer kept going after the failed open"
    );
}

/// A stalled file write must not stall logging. With every per-test write held, other threads
/// still log all their lines and return; the lines reach the file once the write resumes.
/// Before 2026-10-12 the layer wrote under one process-wide lock, and this row would wait out
/// its bound.
#[retcd_test]
fn logging_returns_while_the_file_write_is_stalled() {
    const METHOD: &str = "logging_returns_while_the_file_write_is_stalled";
    const PER_THREAD_HELD: usize = 250;
    // Far above what queueing these lines costs, and the hold never ends on its own.
    const BOUND: Duration = Duration::from_secs(2);
    // The path without the flush `test_file_path` does, which would wait out the hold.
    let unflushed = test_log_dir()
        .join("test_log_writer")
        .join(format!("{METHOD}.jsonl"));

    // Flushed first, so nothing of this file is mid-write when it is read under the hold.
    config_log::layer::flush_test_logs();
    let hold = config_log::layer::hold_test_writes();
    let (done_tx, done_rx) = mpsc::channel();
    let span = tracing::Span::current();
    let started = Instant::now();
    std::thread::spawn(move || {
        let _entered = span.enter();
        log_lines(THREADS, PER_THREAD_HELD);
        let _ = done_tx.send(());
    });
    let logged = done_rx.recv_timeout(BOUND);
    let elapsed = started.elapsed();
    let on_disk_while_held = lines_if_any(&unflushed).len();
    drop(hold);
    assert!(
        logged.is_ok(),
        "logging waited for a stalled file write: {THREADS} threads not done after {elapsed:?}"
    );
    assert_eq!(
        on_disk_while_held, 0,
        "lines written while the write was held"
    );

    let path = config_log::layer::test_file_path(&test_log_dir(), module_path!(), METHOD);
    assert_complete(
        METHOD,
        &lines_in(&path, Some(test_run_id())),
        THREADS,
        PER_THREAD_HELD,
    );
}

/// [`lines_in`], or none when the file does not exist yet.
fn lines_if_any(path: &Path) -> Vec<(u64, u64)> {
    if path.exists() {
        lines_in(path, None)
    } else {
        Vec::new()
    }
}
