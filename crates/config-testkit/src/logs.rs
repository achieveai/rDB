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
/// intermittently red under `scripts/gate.sh`, with an IO error that names the file and a byte
/// offset:
///
/// ```text
/// IO Error: Could not read file ".../m1_47_trace_id_spans_leader_and_both_followers.jsonl"
/// (error in ReadFile(location: 218862478, nr_bytes: 16777212)): Reached the end of the file.
/// ```
///
/// ## What the glob actually matches
///
/// Settle this first, because two earlier readings of this fault both got it wrong and both
/// conclusions followed from it. The glob is **run-scoped**: `<log root>/<test_run_id>/*/*.jsonl`,
/// and `test_run_id` is one fresh uuid per test *binary*. For `m1_observability` it matched
/// **6 files, 4_096_523 bytes**, largest 874_323. A whole-workspace gate root does hold ~1050
/// files and ~3.0 GB — but spread over **107 sibling run directories**, one per binary, and the
/// glob never reaches a single one of them. So the 533 MB and 480 MB files that live in that
/// root are not "files in the same glob": they belong to `m4_watch_faults_cluster` and
/// `m6_evidence`, and nothing `m1_observability` runs can see them.
///
/// The offset is therefore genuinely outside everything the scan could address — 53x the whole
/// glob — and it is not evidence about any file's size in either direction. Do not go looking
/// for a huge log; there isn't one.
///
/// ## The mechanism, measured
///
/// It reproduces with no Rust, no cluster and no openraft: plain writers appending ~750-byte
/// JSON lines to `<dir>/<module>/*.jsonl`, then the same `duckdb` CLI (v1.3.2) with the same
/// options, querying while they append. The discriminator is **per-file size**, and the cliff
/// sits at DuckDB's JSON read buffer — note `nr_bytes: 16777212`, which is 16 MiB minus the 4
/// bytes of yyjson padding. Three files, one constant append rate, queried repeatedly as they
/// grew past it:
///
/// ```text
///   0- 9 MB   16/16 queries failed
///  10-19 MB    8/15 failed
///  20-59 MB    0/54 failed
/// ```
///
/// File *count* is not the mechanism, only more chances per query: at ~2.6 MB each, 1 file
/// failed 5/30, 2 files 27/30, 3 files 29/30, 6 files 29/30. That is also why appending to
/// gigabyte-scale files never reproduced it (20/20 and 25/25 clean in two earlier attempts) —
/// every file in those runs was far above the buffer, so the run had no exposed file in it.
///
/// Two shapes appear. Sometimes the requested offset is exactly the file's length at that
/// instant: the scan consumed the file to the EOF it had measured, then asked for one more full
/// buffer at that offset instead of stopping. Sometimes it runs away entirely — `location:
/// 86109658` against a 5.6 MB file. Either way the file is smaller than one buffer, which is the
/// case for **every per-test log this workspace writes**. This is the normal regime here, not an
/// exotic one, and that is why the row was flaky rather than rare.
///
/// What is deliberately *not* claimed: why the CLI's bookkeeping goes wrong. The size cliff and
/// the ingredient are measured; the cause inside DuckDB is not, and nothing here depends on it.
/// The control is direct — the same harness, copying each file to its length-at-open before
/// querying, ran **60/60 clean**.
///
/// So the fix removes the ingredient. A snapshot is a closed file of fixed length, and the copy
/// is taken by this process, which already owns the writer — no retry loop, no sleep, and no
/// assertion weakened to tolerate a bad read.
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

/// A snapshot of every `*.jsonl` under `root`, at any depth, and the relation over it.
///
/// For logs that are **not** under the test log root and so have no `testRun` scoping of their
/// own: `config-server`'s E2E rows start real daemon child processes and read the files those
/// processes write into a per-test temp tree. Those daemons are still running when the row
/// queries them, which is the same hazard [`LogSnapshot`] documents — and a daemon log is a
/// few hundred KB, far below the 16 MiB buffer, so it sits in the exposed regime.
///
/// Relative paths are preserved inside the snapshot, so a `<node>/<file>.jsonl` layout still
/// reads as one relation. Panics if `root` holds no `.jsonl` file at all: an empty relation
/// would make every assertion over it vacuously true (test plan §6 rule 11).
#[must_use]
pub fn relation_for_tree(root: &Path) -> LogSnapshot {
    let dir = snapshot_dir();
    let snapshot = LogSnapshot::new(dir.clone(), &dir.join("**").join("*.jsonl"));
    let mut found = 0usize;
    let mut stack = vec![root.to_path_buf()];
    while let Some(next) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&next) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if entry.file_type().is_ok_and(|t| t.is_dir()) {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "jsonl") {
                let rel = path.strip_prefix(root).unwrap_or(&path);
                freeze(&path, &dir.join(rel));
                found += 1;
            }
        }
    }
    assert!(
        found > 0,
        "no .jsonl files under {} to query — an empty relation must not read as a pass",
        root.display()
    );
    snapshot
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
