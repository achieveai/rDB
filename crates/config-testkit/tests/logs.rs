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
    let glob = config_testkit::logs::test_logs_glob();
    let filter = config_testkit::logs::current_run_filter();
    let sql = format!(
        "SELECT count(*) AS n FROM read_json_auto('{glob}', union_by_name=true) \
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
