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

use std::process::Command;

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

/// Forward-slashed glob over every test JSONL file, suitable for
/// `read_json_auto('<glob>', union_by_name=true)` regardless of host path separator.
pub fn test_logs_glob() -> String {
    let mut dir = config_log::testing::test_log_dir()
        .to_string_lossy()
        .replace('\\', "/");
    if !dir.ends_with('/') {
        dir.push('/');
    }
    dir.push_str("**/*.jsonl");
    dir
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
