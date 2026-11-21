//! Proves `logs::query` actually round-trips through the `duckdb` CLI over this test's own
//! JSONL output, and that the assertion helpers panic (not skip) on the conditions they name
//! (test plan §5, §6 rule 11).

#[config_log::retcd_test]
fn logs_query_reads_this_tests_own_jsonl_file() {
    tracing::info!(
        marker = "logs_query_reads_this_tests_own_jsonl_file",
        "did something"
    );

    // Read the file directly first (no DuckDB round trip needed for this check).
    let direct = config_testkit::logs::lines_for_current_test(
        module_path!(),
        "logs_query_reads_this_tests_own_jsonl_file",
    );
    config_testkit::logs::assert_nonempty(&direct, "this test's own JSONL lines");
    config_testkit::logs::assert_no_value_fields(&direct);
    assert!(
        direct.iter().any(|row| row["@m"] == "did something"),
        "the logged line must be present: {direct:?}"
    );

    // Then the same thing through DuckDB, which is what real M1 log assertions use.
    let lines = config_testkit::logs::relation_for_current_test(
        module_path!(),
        "logs_query_reads_this_tests_own_jsonl_file",
    );
    let filter = config_testkit::logs::current_run_filter();
    let sql = format!(
        "SELECT count(*) AS n FROM {lines} \
         WHERE testMethod = 'logs_query_reads_this_tests_own_jsonl_file' AND {filter}"
    );
    let rows = config_testkit::logs::query(&sql);
    config_testkit::logs::assert_nonempty(&rows, "duckdb query result");
    let n = rows[0]["n"].as_i64().expect("n is an integer column");
    assert!(
        n >= 1,
        "expected at least one matching row from duckdb, got {n}"
    );
}

#[config_log::retcd_test]
fn logs_query_panics_loudly_on_a_bad_query_rather_than_skipping() {
    let result = std::panic::catch_unwind(|| {
        config_testkit::logs::query("SELECT * FROM this_table_does_not_exist")
    });
    assert!(
        result.is_err(),
        "an invalid query must panic, never silently skip"
    );
}

#[config_log::retcd_test]
fn assert_nonempty_panics_on_an_empty_slice() {
    let empty: Vec<serde_json::Value> = Vec::new();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        config_testkit::logs::assert_nonempty(&empty, "synthetic empty rows")
    }));
    assert!(
        result.is_err(),
        "assert_nonempty must panic on empty input — an empty result must not read as a pass"
    );
}

#[config_log::retcd_test]
fn assert_no_value_fields_panics_when_a_value_field_is_present() {
    let offending = vec![serde_json::json!({"value": "leaked"})];
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        config_testkit::logs::assert_no_value_fields(&offending)
    }));
    assert!(
        result.is_err(),
        "assert_no_value_fields must panic when a row carries a `value` field, even redacted"
    );

    let clean = vec![serde_json::json!({"key_hex": "aa"})];
    config_testkit::logs::assert_no_value_fields(&clean);
}

/// The `map_inference_threshold` option in [`logs::test_logs_relation`] is load-bearing, and a
/// row that only checked a query against its own narrow log file would never notice its removal.
/// So this builds the condition that breaks it.
///
/// The condition is the *union* of field names across the files a glob matches, not the width of
/// any one object: a single object with 300 keys still binds, which is what the first draft of
/// this row asserted and why it failed. What breaks is many files whose key sets differ, because
/// `union_by_name` then infers one very wide object and DuckDB types it as a `MAP` past the
/// 200-key default — collapsing the whole relation to a single `json` column, so every named
/// column fails to bind:
///
/// ```text
/// Binder Error: Referenced column "testMethod" not found in FROM clause!
/// Candidate bindings: "json"
/// ```
///
/// That is exactly the shape of a whole-workspace gate run: 1310 files under one log root on
/// 2026-09-21, each suite logging its own fields. The bare arm below is the positive control —
/// if a future DuckDB stops collapsing wide unions, this row goes red and says so rather than
/// quietly guarding nothing.
#[config_log::retcd_test]
fn a_wide_log_relation_needs_the_map_inference_threshold_option() {
    let dir = config_log::testing::test_log_dir().join("wide_relation_probe");
    std::fs::create_dir_all(&dir).expect("probe dir");

    // 300 files, each contributing one distinct key, so the union is past the 200-key default
    // while no single object is anywhere near it.
    for i in 0..300 {
        let line = serde_json::json!({ "testMethod": "probe", format!("k{i}"): i });
        std::fs::write(dir.join(format!("f{i}.jsonl")), format!("{line}\n")).expect("write probe");
    }

    let glob = format!("{}/*.jsonl", dir.to_string_lossy().replace('\\', "/"));
    let bin = std::env::var("RETCD_DUCKDB").unwrap_or_else(|_| "duckdb".to_string());
    let run = |from: &str| {
        std::process::Command::new(&bin)
            .arg("-c")
            .arg(format!("SELECT testMethod FROM {from};"))
            .output()
            .expect("launch duckdb")
    };

    let bare = run(&format!("read_json_auto('{glob}', union_by_name=true)"));
    assert!(
        !bare.status.success(),
        "a 300-file union is supposed to collapse to a single `json` column without \
         map_inference_threshold. It bound instead, so the option this row guards may no \
         longer be doing anything — re-derive it before deleting it.\nstdout: {}",
        String::from_utf8_lossy(&bare.stdout)
    );

    let guarded = run(&format!(
        "read_json_auto('{glob}', union_by_name=true, map_inference_threshold=-1)"
    ));
    assert!(
        guarded.status.success(),
        "map_inference_threshold=-1 must make the named column bind: {}",
        String::from_utf8_lossy(&guarded.stderr)
    );
    assert!(
        String::from_utf8_lossy(&guarded.stdout).contains("probe"),
        "the guarded query must return the row it selected"
    );

    // The helper the rest of the workspace calls must carry the option it was added for.
    assert!(
        config_testkit::logs::test_logs_relation()
            .relation()
            .contains("map_inference_threshold=-1"),
        "test_logs_relation must carry the option; the two arms above are why"
    );
}

/// Issue #132: [`config_testkit::logs::lines_for_current_test`] must never hand back a line that
/// is still being written. A test commonly polls its own log while threads it started keep
/// logging into the same file (the `rdb-api` trio rows poll `logged!` inside `until`), so a read
/// races the append of a line logged after its flush. That line is the writer's to finish, not
/// the reader's to see half of.
///
/// On ext4 one `write` becomes visible a page at a time, so a reader that stops at the end of
/// the file can stop inside a line. XFS takes the inode lock for a buffered read as well as for
/// a write, so there a read never sees part of one `write`, and on XFS this row cannot go red.
/// The gate's log root is under the target directory, which is on XFS on the build host. So the
/// body runs in a child process ([`child_reads_its_log_while_long_lines_are_written`]) whose log
/// root is a fresh directory in the system temp dir (`/tmp`, ext4, on the build host). Where the
/// temp dir is also on a filesystem like XFS, this row passes without having raced anything.
///
/// Cut: `lines_for_current_test` reads the file to its current end, red (`malformed JSONL line`)
/// with the child's log root on ext4.
#[test]
fn lines_for_current_test_never_reads_a_line_still_being_written() {
    const CHILD: &str = "child_reads_its_log_while_long_lines_are_written";
    let run = run_bounded_child(CHILD, child_bound(READ_PATIENCE * scale()));
    assert!(
        run.success && run.stdout.contains("1 passed"),
        "{CHILD} failed or ran no test: {run}"
    );
}

/// Each reader's patience for the long lines, before [`scale`]. One round takes milliseconds on
/// an idle host; a reader that is still waiting after this is waiting for a line that is not
/// coming.
const READ_PATIENCE: std::time::Duration = std::time::Duration::from_secs(30);

/// The patience of [`child_misses_the_long_line`]: short, because no line it waits for is ever
/// written, so the row spends all of it.
const MISSING_PATIENCE: std::time::Duration = std::time::Duration::from_secs(1);

fn scale() -> u32 {
    config_testkit::poll::deadline_scale()
}

/// How long the parent lets a child run whose readers give up after `patience`: that, plus
/// [`READ_PATIENCE`] times [`scale`] for the process start and the writer.
fn child_bound(patience: std::time::Duration) -> std::time::Duration {
    patience + READ_PATIENCE * scale()
}

/// The reader loop must end when the long line never comes, and say which line it waited for.
/// Before it had a deadline, a reader whose `LONG` never arrived read its markers forever and
/// the child never exited.
///
/// The child ([`child_misses_the_long_line`]) runs the same body as
/// [`child_reads_its_log_while_long_lines_are_written`], but its writer logs a line with a
/// different message, so no reader ever sees `LONG`.
///
/// Cut: drop the reader's deadline check, and the child outlives [`child_bound`] and is killed.
#[test]
fn a_reader_whose_long_line_never_comes_fails_within_its_patience() {
    const CHILD: &str = "child_misses_the_long_line";
    let bound = child_bound(MISSING_PATIENCE);
    let run = run_bounded_child(CHILD, bound);
    assert!(
        !run.killed,
        "{CHILD} was still running after {bound:?} and was killed: {run}"
    );
    assert!(!run.success, "{CHILD} passed with no long line: {run}");
    let diagnostic = format!("no `{LONG}` line within {MISSING_PATIENCE:?}");
    assert!(
        run.stdout.contains(&diagnostic) || run.stderr.contains(&diagnostic),
        "{CHILD} failed without naming the missing line ({diagnostic}): {run}"
    );
}

/// [`run_bounded_child`]'s kill: a child that never exits is killed at its bound, reaped, and
/// reported as a failure, and what it wrote before the kill is kept.
///
/// The bound here is short ([`MISSING_PATIENCE`] times [`scale`]) because the whole of it is
/// spent; the rows above derive theirs the same way, from a patience, with [`child_bound`].
///
/// Cut: drop the kill branch, and the parent polls a child that never ends, so this row never
/// ends either.
#[test]
fn a_child_that_never_exits_is_killed_at_its_bound() {
    const CHILD: &str = "child_never_exits";
    let bound = MISSING_PATIENCE * scale();
    let run = run_bounded_child(CHILD, bound);
    assert!(run.killed, "{CHILD} was not killed: {run}");
    assert!(!run.success, "a killed child is a failure: {run}");
    assert!(
        run.elapsed >= bound,
        "{CHILD} was killed before its bound {bound:?}: {run}"
    );
    assert!(
        run.stderr.contains(STUCK_MARKER),
        "the output {CHILD} wrote before the kill was lost: {run}"
    );
}

/// What [`child_never_exits`] writes before it blocks.
const STUCK_MARKER: &str = "stuck child is up";

/// The body of [`a_child_that_never_exits_is_killed_at_its_bound`]: write a line, then block
/// for ever.
#[test]
#[ignore = "run in a child process by a_child_that_never_exits_is_killed_at_its_bound"]
fn child_never_exits() {
    use std::io::Write;
    // Straight to the process's stderr, past libtest's capture, which a kill would discard.
    let mut stderr = std::io::stderr();
    writeln!(stderr, "{STUCK_MARKER}").expect("write the marker");
    stderr.flush().expect("flush the marker");
    loop {
        std::thread::park();
    }
}

/// What a bounded child run ended with.
struct ChildRun {
    success: bool,
    /// The child outlived its bound and was killed, then reaped.
    killed: bool,
    elapsed: std::time::Duration,
    /// Removed when the run is dropped, after the row's asserts have printed it.
    root: tempfile::TempDir,
    stdout: String,
    stderr: String,
}

impl std::fmt::Display for ChildRun {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "success={} killed={} after {:?}, log root {}\nstdout:\n{}\nstderr:\n{}",
            self.success,
            self.killed,
            self.elapsed,
            self.root.path().display(),
            self.stdout,
            self.stderr
        )
    }
}

/// Run the ignored test `child` alone in a child process of this binary, with its log root a
/// fresh directory in the system temp dir. If it is still running after `bound`, kill it. It is
/// always reaped before this returns, so no row leaves a child behind.
fn run_bounded_child(child: &str, bound: std::time::Duration) -> ChildRun {
    let root = tempfile::Builder::new()
        .prefix("retcd-132-")
        .tempdir_in(std::env::temp_dir())
        .expect("temp log root");
    // Files, not pipes: nobody reads a pipe while the parent polls, and a full one would stall
    // the child. The log glob only reaches `<root>/<run>/*/*.jsonl`, so these are not in it.
    let stdout_path = root.path().join("child.stdout");
    let stderr_path = root.path().join("child.stderr");
    let started = std::time::Instant::now();
    let mut process =
        std::process::Command::new(std::env::current_exe().expect("this test binary"))
            .args([child, "--exact", "--ignored", "--test-threads=1"])
            .env("RETCD_TEST_LOG_DIR", root.path())
            .stdin(std::process::Stdio::null())
            .stdout(std::fs::File::create(&stdout_path).expect("child stdout file"))
            .stderr(std::fs::File::create(&stderr_path).expect("child stderr file"))
            .spawn()
            .expect("spawn the child");
    let (status, killed) = loop {
        match process.try_wait().expect("try_wait on the child") {
            Some(status) => break (status, false),
            None if started.elapsed() >= bound => {
                process.kill().expect("kill the child");
                break (process.wait().expect("reap the killed child"), true);
            }
            // A bounded poll on a child process, not synchronization: the loop ends on the
            // child's exit or on `bound`, never on the sleep itself.
            None => std::thread::sleep(std::time::Duration::from_millis(20)), // testkit:allow-sleep
        }
    };
    let read = |path: &std::path::Path| {
        std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("read the child's output {}: {e}", path.display()))
    };
    ChildRun {
        success: status.success(),
        killed,
        elapsed: started.elapsed(),
        stdout: read(&stdout_path),
        stderr: read(&stderr_path),
        root,
    }
}

/// The message of the long line each round writes, and the one each reader waits for.
const LONG: &str = "concurrent long line";

/// The body of [`lines_for_current_test_never_reads_a_line_still_being_written`], run in a child
/// process with its own log root.
#[config_log::retcd_test]
#[ignore = "run in a child process by lines_for_current_test_never_reads_a_line_still_being_written"]
fn child_reads_its_log_while_long_lines_are_written() {
    read_while_long_lines_are_written(
        "child_reads_its_log_while_long_lines_are_written",
        LONG,
        READ_PATIENCE * scale(),
    );
}

/// The body of [`a_reader_whose_long_line_never_comes_fails_within_its_patience`]: the writer
/// logs its long lines under another message, so every reader runs out its patience.
#[config_log::retcd_test]
#[ignore = "run in a child process by a_reader_whose_long_line_never_comes_fails_within_its_patience"]
fn child_misses_the_long_line() {
    read_while_long_lines_are_written(
        "child_misses_the_long_line",
        "not the long line",
        MISSING_PATIENCE,
    );
}

/// A line queued before a read's flush cannot be seen cut, because the flush waits for it; only
/// one written while a read that has already flushed is still reaching the end of the file can.
/// So each round gives one line of `PAD` bytes, logged with message `writes`, to its own file,
/// through a twin span with its own `testMethod`, while `READERS` threads keep reading that file,
/// still small and cheap to read, until each has read a [`LONG`] line whole.
///
/// Each reader gives up `patience` after it starts, checked before every read, and fails naming
/// the line it waited for. Without that, a reader whose line never comes reads forever.
///
/// Also checks the flush still holds: the marker each reader logs just before a read is in it.
fn read_while_long_lines_are_written(
    method: &str,
    writes: &'static str,
    patience: std::time::Duration,
) {
    use std::sync::{Condvar, Mutex};

    const ROUNDS: usize = 12;
    const READERS: usize = 6;
    const PAD: usize = 4 << 20;
    let twin = |round: usize| format!("{method}_r{round}");

    // Readers started so far, over all rounds. The writer logs round `r`'s long line once
    // every reader has started round `r`.
    let started = (Mutex::new(0usize), Condvar::new());
    let start = |n: usize| {
        *started.0.lock().unwrap_or_else(|e| e.into_inner()) += n;
        started.1.notify_all();
    };
    let span = tracing::Span::current();
    let failure = std::thread::scope(|scope| {
        scope.spawn(|| {
            let _entered = span.enter();
            let pad = "x".repeat(PAD);
            for round in 0..ROUNDS {
                let count = started.0.lock().unwrap_or_else(|e| e.into_inner());
                drop(
                    started
                        .1
                        .wait_while(count, |n| *n < (round + 1) * READERS)
                        .unwrap_or_else(|e| e.into_inner()),
                );
                let method = twin(round);
                tracing::info_span!("twin", testMethod = method.as_str()).in_scope(|| {
                    tracing::info!(round, pad = pad.as_str(), "{writes}");
                });
            }
        });
        let readers: Vec<_> = (0..READERS)
            .map(|reader| {
                let (span, start, twin) = (span.clone(), &start, &twin);
                scope.spawn(move || {
                    let _entered = span.enter();
                    let deadline = std::time::Instant::now() + patience;
                    let rounds = std::panic::catch_unwind(|| {
                        let mut reads = 0u64;
                        for round in 0..ROUNDS {
                            let method = twin(round);
                            let in_twin = tracing::info_span!("twin", testMethod = method.as_str());
                            start(1);
                            loop {
                                assert!(
                                    std::time::Instant::now() < deadline,
                                    "reader {reader} of {method}: no `{LONG}` line within \
                                     {patience:?}, after {reads} reads"
                                );
                                in_twin.in_scope(|| tracing::info!(reader, reads, "read marker"));
                                let lines = config_testkit::logs::lines_for_current_test(
                                    module_path!(),
                                    &method,
                                );
                                assert!(
                                    lines.iter().any(|l| l["@m"] == "read marker"
                                        && l["reader"] == reader
                                        && l["reads"] == reads),
                                    "read {reads} by reader {reader} of {method} is missing \
                                     the marker logged just before it"
                                );
                                reads += 1;
                                if lines.iter().any(|l| l["@m"] == LONG) {
                                    break;
                                }
                            }
                        }
                    });
                    // A failed reader stops early; count its rounds started so the writer
                    // and the other readers can finish.
                    if rounds.is_err() {
                        start(ROUNDS * READERS);
                    }
                    rounds
                })
            })
            .collect();
        readers
            .into_iter()
            .map(|r| r.join().expect("reader thread"))
            .find_map(Result::err)
    });
    if let Some(panic) = failure {
        std::panic::resume_unwind(panic);
    }
}
