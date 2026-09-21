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
    let lines = config_testkit::logs::test_logs_relation();
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
        config_testkit::logs::test_logs_relation().contains("map_inference_threshold=-1"),
        "test_logs_relation must carry the option; the two arms above are why"
    );
}
