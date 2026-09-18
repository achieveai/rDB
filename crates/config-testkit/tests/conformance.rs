//! Proves the conformance suite itself: it runs to 15 unique results against a real
//! `ConfigStore`, it actually detects failure rather than always reporting green, and
//! `ConformanceReport::diff` compares two reports meaningfully.

use std::collections::HashSet;
use std::sync::Arc;

use config_core::{ConfigError, ConfigStore};
use config_testkit::{ConformanceConfig, MemStore};

#[config_log::retcd_test]
async fn conformance_suite_passes_against_memstore_with_15_unique_ids() {
    let store: Arc<dyn ConfigStore> = Arc::new(MemStore::new());
    let cfg = ConformanceConfig::unique("passes");

    let report = config_testkit::conformance::run_all(store, cfg).await;

    assert_eq!(report.results.len(), 15, "C-01..C-15 must all run");
    let ids: HashSet<&str> = report.results.iter().map(|r| r.id).collect();
    assert_eq!(ids.len(), 15, "scenario ids must be unique");

    report.assert_all_passed();
}

#[config_log::retcd_test]
async fn conformance_suite_reports_failures_when_store_is_failing() {
    let store = Arc::new(MemStore::new());
    store.failing_with(ConfigError::Unavailable {
        reason: "injected for conformance_suite_reports_failures_when_store_is_failing".into(),
    });
    let dyn_store: Arc<dyn ConfigStore> = store;
    let cfg = ConformanceConfig::unique("failing");

    let report = config_testkit::conformance::run_all(dyn_store, cfg).await;

    assert!(
        !report.passed(),
        "every scenario must fail when the store always errors"
    );
    assert_eq!(
        report.failures().len(),
        15,
        "all 15 scenarios must be caught as failures, not aborts"
    );
    for failure in report.failures() {
        assert!(
            !failure.detail.is_empty(),
            "{} must carry a non-empty failure detail",
            failure.id
        );
    }
}

#[config_log::retcd_test]
#[should_panic(expected = "conformance suite failed")]
async fn assert_all_passed_panics_and_lists_every_failure() {
    let store = Arc::new(MemStore::new());
    store.failing_with(ConfigError::Unavailable {
        reason: "injected for assert_all_passed_panics_and_lists_every_failure".into(),
    });
    let dyn_store: Arc<dyn ConfigStore> = store;
    let cfg = ConformanceConfig::unique("assert-panics");

    let report = config_testkit::conformance::run_all(dyn_store, cfg).await;
    report.assert_all_passed();
}

#[config_log::retcd_test]
async fn diff_of_identical_reports_is_empty_and_detects_a_changed_revision() {
    let store: Arc<dyn ConfigStore> = Arc::new(MemStore::new());
    let cfg = ConformanceConfig::unique("diff");
    let report = config_testkit::conformance::run_all(store, cfg).await;
    report.assert_all_passed();

    let identical = report.clone();
    assert!(
        report.diff(&identical).is_empty(),
        "a report diffed against a clone of itself must show no differences"
    );

    let mut mutated = report.clone();
    let c02 = mutated
        .results
        .iter_mut()
        .find(|r| r.id == "C-02")
        .expect("C-02 present");
    let original_revision = c02.observed["revision"].clone();
    c02.observed["revision"] = serde_json::json!(original_revision.as_u64().unwrap_or(0) + 1000);

    let diffs = report.diff(&mutated);
    assert!(
        !diffs.is_empty(),
        "a changed observed revision must be detected"
    );
    assert!(
        diffs.iter().any(|d| d.starts_with("C-02")),
        "the diff must name the scenario that changed: {diffs:?}"
    );
}
