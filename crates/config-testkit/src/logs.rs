//! DuckDB-backed assertions over `target/test-logs/**/*.jsonl` (test plan §5; ADR-0013).
//!
//! # Why a subprocess, not `duckdb-rs`
//!
//! ADR-0014's Clarifications call for `duckdb` as an in-process dev-dependency. In this
//! workspace that pulls in `duckdb-rs`'s bundled DuckDB C++ build, which took over ten minutes
//! on the authoring machine — paid by every `cargo build`/`cargo test` in the workspace, not
//! just the log-assertion tests. This module instead shells out to the `duckdb` CLI (present
//! on `PATH` on this machine; override with `RETCD_DUCKDB`, read once via `std::env::var` and
//! never set by a test — anti-flake rule 6 bans mutating process env from a test). A missing
//! or failing CLI panics with its stderr; it never skips (test plan §6 rule 11, "empty
//! evidence is a failure").

use std::fmt;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

fn duckdb_bin() -> String {
    std::env::var("RETCD_DUCKDB").unwrap_or_else(|_| "duckdb".to_string())
}

/// Run `sql` against the `duckdb` CLI in JSON mode and return the rows.
///
/// Panics with the CLI's stderr if the binary cannot be launched or the query fails. A DuckDB
/// query returning zero rows is not a failure of this function — pass the result to
/// [`assert_nonempty`] when the caller expects at least one row.
pub fn query(sql: &str) -> Vec<serde_json::Value> {
    let bin = duckdb_bin();
    let output = Command::new(&bin)
        .arg("-json")
        .arg("-c")
        .arg(sql)
        .output()
        .unwrap_or_else(|e| {
            panic!(
                "failed to launch the duckdb CLI (`{bin}`): {e}. Install DuckDB or set \
                 RETCD_DUCKDB to its path — a missing CLI must fail the test, not skip it."
            )
        });

    if !output.status.success() {
        panic!(
            "duckdb query failed (exit {:?}):\n{}\nsql:\n{sql}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let trimmed = stdout.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    serde_json::from_str(trimmed).unwrap_or_else(|e| {
        panic!("duckdb returned non-JSON stdout: {e}\nstdout:\n{trimmed}\nsql:\n{sql}")
    })
}

/// The `read_json_auto(...)` options every query against a test log root needs.
///
/// `map_inference_threshold=-1` is the load-bearing one. DuckDB infers an object with more than
/// 200 distinct keys as a `MAP` rather than a `STRUCT`, and at the top level that collapses the
/// whole relation to a single `json` column. Every named column then fails to bind:
///
/// ```text
/// Binder Error: Referenced column "testMethod" not found in FROM clause!
/// Candidate bindings: "json"
/// ```
///
/// The threshold is on the *union* of field names across every file the glob matches, so a
/// query is fine against one suite's logs and fails against a whole-workspace gate run — which
/// is the run where a log query is actually wanted. Observed 2026-09-21 at 1310 files under one
/// root. The failure names a column, so it reads like a typo in the query rather than a limit.
const READ_OPTIONS: &str = "union_by_name=true, map_inference_threshold=-1";

/// A private copy of the JSONL files a query needs, plus the `read_json_auto(...)` relation
/// over that copy. Put it straight in a `FROM` clause — it renders as the relation — and keep
/// it alive for as long as the query runs; dropping it deletes the copy.
///
/// # Why a copy and not the live files
///
/// A test's own JSONL file is still being appended to while the test queries it: the nodes it
/// started keep logging inside its span, and so do the other tests running in parallel in the
/// same binary. Handing DuckDB a path that is open for writing is where log assertions went
/// intermittently red under a whole-workspace `scripts/gate.sh`, with an IO error that names
/// the file and a byte offset:
///
/// ```text
/// IO Error: Could not read file ".../m1_47_trace_id_spans_leader_and_both_followers.jsonl"
/// (error in ReadFile(location: 218862478, nr_bytes: 16777212)): Reached the end of the file.
/// ```
///
/// Read that offset carefully before chasing it: it is what DuckDB *attempted*, not the file's
/// size. The file in that failure finished at 862_679 bytes — 253x smaller than the offset, and
/// 50x smaller than every file the glob matched put together — so DuckDB was not simply
/// overrunning a file that grew under it. Reading a growing file is fine on its own: three
/// files appended to a gigabyte each, queried 20 times through the same CLI (v1.3.2) and the
/// same options, came back right 20 times out of 20. Whatever the CLI does wrong here, it needs
/// a file somebody still has open, and none of it can happen to a file nobody is writing.
///
/// So the mechanism is not the fix's warrant; the absence of the ingredient is. A snapshot is a
/// closed file of fixed length, and the copy is taken by this process, which already owns the
/// writer — no retry loop, no sleep, and no assertion weakened to tolerate a bad read.
pub struct LogSnapshot {
    dir: PathBuf,
    relation: String,
}

impl LogSnapshot {
    /// The `read_json_auto(...)` call to put in a `FROM` clause.
    #[must_use]
    pub fn relation(&self) -> &str {
        &self.relation
    }

    fn new(dir: PathBuf, target: &Path) -> Self {
        let relation = format!(
            "read_json_auto('{}', {READ_OPTIONS})",
            target.to_string_lossy().replace('\\', "/")
        );
        Self { dir, relation }
    }
}

impl fmt::Display for LogSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.relation)
    }
}

impl Drop for LogSnapshot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// A fresh, empty directory to snapshot into.
///
/// Deliberately outside the test log root rather than a dot-directory inside it. The root is
/// what the globs in this module and in `AGENTS.md` are pointed at, including `**/*.jsonl`
/// forms that reach any depth, and a snapshot that a later glob can match would be counted
/// twice by the very queries it exists to make honest.
fn snapshot_dir() -> PathBuf {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "retcd-log-snapshot-{}-{}-{}",
        std::process::id(),
        config_log::testing::test_run_id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir)
        .unwrap_or_else(|e| panic!("cannot create log snapshot dir {}: {e}", dir.display()));
    dir
}

/// Copy `src` to `dest`, stopping at the last complete line the file held when the copy began.
///
/// The length is read once and never re-read, so anything appended while the copy runs is
/// simply not in the snapshot — the point is a file of fixed length, not a current one. The
/// trailing-newline trim is insurance: `config_log`'s layer writes one whole line per
/// `write_all` under a mutex, so a torn line should not be reachable, but a snapshot that
/// cannot end mid-object costs one `set_len` to guarantee.
fn freeze(src: &Path, dest: &Path) {
    let mut reader = File::open(src)
        .unwrap_or_else(|e| panic!("cannot open test log {} to snapshot it: {e}", src.display()));
    let len = reader
        .metadata()
        .unwrap_or_else(|e| panic!("cannot stat test log {}: {e}", src.display()))
        .len();
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)
            .unwrap_or_else(|e| panic!("cannot create {}: {e}", parent.display()));
    }
    let mut writer = File::create(dest)
        .unwrap_or_else(|e| panic!("cannot create log snapshot {}: {e}", dest.display()));

    let mut buf = vec![0u8; 1 << 20];
    let mut left = len;
    let mut written = 0u64;
    let mut complete = 0u64;
    while left > 0 {
        let want = usize::try_from(left.min(buf.len() as u64)).unwrap_or(buf.len());
        let n = reader
            .read(&mut buf[..want])
            .unwrap_or_else(|e| panic!("cannot read test log {}: {e}", src.display()));
        if n == 0 {
            break;
        }
        if let Some(i) = buf[..n].iter().rposition(|b| *b == b'\n') {
            complete = written + i as u64 + 1;
        }
        writer
            .write_all(&buf[..n])
            .unwrap_or_else(|e| panic!("cannot write log snapshot {}: {e}", dest.display()));
        written += n as u64;
        left -= n as u64;
    }
    if complete < written {
        writer
            .set_len(complete)
            .unwrap_or_else(|e| panic!("cannot trim log snapshot {}: {e}", dest.display()));
    }
}

/// A snapshot of every per-test JSONL file this run has written so far, and the relation over
/// it. Use it for a query that is *about* the run as a whole — one that has to see lines other
/// tests wrote, such as "no line anywhere escaped its test context".
///
/// A query that names one `testMethod` wants [`relation_for_current_test`] instead: it reads
/// the same rows from one file, and copies one file to get them.
///
/// Panics if the run has written no per-test files yet — an empty relation would make every
/// assertion over it vacuously true (test plan §6 rule 11).
#[must_use]
pub fn test_logs_relation() -> LogSnapshot {
    let root = config_log::testing::test_log_dir();
    let dir = snapshot_dir();
    let snapshot = LogSnapshot::new(dir.clone(), &dir.join("*").join("*.jsonl"));

    // `<run>/<module>/<method>.jsonl`, the two levels the live glob matched. Files directly
    // under the run root (`_untagged-<pid>.jsonl`) stay out of it, as they were before.
    let mut found = 0usize;
    let modules = std::fs::read_dir(&root)
        .unwrap_or_else(|e| panic!("cannot read test log dir {}: {e}", root.display()));
    for module in modules.flatten() {
        if !module.file_type().is_ok_and(|t| t.is_dir()) {
            continue;
        }
        let Ok(entries) = std::fs::read_dir(module.path()) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "jsonl") {
                freeze(&path, &dir.join(module.file_name()).join(entry.file_name()));
                found += 1;
            }
        }
    }

    assert!(
        found > 0,
        "no per-test JSONL files under {} to query — an empty relation must not read as a pass",
        root.display()
    );
    snapshot
}

/// A snapshot of just the calling test's own JSONL file, and the relation over it.
///
/// `config_log`'s layer routes a line by its own `testModule`/`testMethod`, so every row a
/// `WHERE testMethod = '<method>'` could match is in this one file and every row in this file
/// matches it. Naming the file rather than globbing the run therefore returns the same rows,
/// off one copy instead of the suite's.
///
/// Panics if the file does not exist — i.e. the test never opened `#[retcd_test]`, or has not
/// logged anything yet.
#[must_use]
pub fn relation_for_current_test(module: &str, method: &str) -> LogSnapshot {
    let src =
        config_log::layer::test_file_path(&config_log::testing::test_log_dir(), module, method);
    assert!(
        src.is_file(),
        "no test log at {} to query. Did the test open #[config_log::retcd_test], and has it \
         logged anything yet?",
        src.display()
    );
    let dir = snapshot_dir();
    let dest = dir.join("own.jsonl");
    freeze(&src, &dest);
    LogSnapshot::new(dir, &dest)
}

/// A `WHERE` clause fragment restricting rows to this process's test run:
/// `testRun = '<id>'`.
pub fn current_run_filter() -> String {
    format!("testRun = '{}'", config_log::testing::test_run_id())
}

/// Read the current test's own per-test JSONL file directly (no DuckDB needed) and filter to
/// this process's `testRun`, so a test does not need the glob-and-DuckDB round trip just to
/// look at its own output.
///
/// Panics if the file cannot be read (i.e. the test never opened `#[retcd_test]`, or nothing
/// was logged yet) or if a line is not valid JSON — a malformed log file is itself a bug the
/// test should surface, not silently ignore.
pub fn lines_for_current_test(module: &str, method: &str) -> Vec<serde_json::Value> {
    let dir = config_log::testing::test_log_dir();
    let path = config_log::layer::test_file_path(&dir, module, method);
    let content = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "cannot read test log {}: {e}. Did the test open #[config_log::retcd_test]?",
            path.display()
        )
    });
    let run_id = config_log::testing::test_run_id();
    content
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str::<serde_json::Value>(line).unwrap_or_else(|e| {
                panic!("malformed JSONL line in {}: {e}\n{line}", path.display())
            })
        })
        .filter(|value| value.get("testRun").and_then(serde_json::Value::as_str) == Some(run_id))
        .collect()
}

/// Fail with the offending rows if any carries a `"value"` field at all (redacted or not).
///
/// [`config_log`]'s layer redacts a field literally named `value` to `"<redacted>"` as a
/// safety net, but this assertion is stricter: production code must never even attempt to log
/// one (test plan §5 Q3, ADR-0013 §15.2).
pub fn assert_no_value_fields(rows: &[serde_json::Value]) {
    let offending: Vec<&serde_json::Value> = rows
        .iter()
        .filter(|row| row.get("value").is_some())
        .collect();
    assert!(
        offending.is_empty(),
        "log rows carry a raw `value` field, which must never be logged even redacted: {offending:?}"
    );
}

/// Fail if `rows` is empty, naming what was expected.
///
/// An empty result over logs, metrics, or a conformance report must never read as a pass
/// (test plan §6 rule 11): `assert!(rows.iter().all(...))` over zero rows is vacuously true
/// and is a banned pattern this function exists to replace.
pub fn assert_nonempty(rows: &[serde_json::Value], what: &str) {
    assert!(
        !rows.is_empty(),
        "expected at least one row for {what}, got zero — an empty result must not read as a pass"
    );
}
