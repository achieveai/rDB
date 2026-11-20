//! Issue #98 (lead option B): an app that fills its dedup cap with ids it chose (`put_as`) is
//! warned before and at the cap, and node status reports the fill against the cap. Admission
//! still answers `OVERLOADED` at the cap, unchanged.

use super::reopen_tests::{config, fast, runtime};
use super::*;

/// The cap under test: small enough to fill in a few writes.
const CAP: usize = 10;

/// The `dedup_cap_high` and `dedup_cap_full` lines this row's nodes logged, in order.
fn cap_lines(method: &str) -> Vec<serde_json::Value> {
    config_testkit::logs::lines_for_current_test(module_path!(), method)
        .into_iter()
        .filter(|line| line["@m"] == "dedup_cap_high" || line["@m"] == "dedup_cap_full")
        .collect()
}

/// Issue #98 scenario: `put_as` writes on the default 24 h retention fill a cap of 10. Node 1,
/// the owner, logs `dedup_cap_high` once at 8 entries and `dedup_cap_full` once at 10, both at
/// warn with node, partition, fill, cap and retention; no other node logs either, since only the
/// owner holds T1's index. Node status then reports the fill and the cap side by side, and the
/// next write is `OVERLOADED` as before, which logs neither line again.
/// Integration (~1 s): three node threads, about ten puts.
#[config_log::retcd_test]
fn i98_3_a_filled_cap_warns_once_high_once_full_and_status_reports_the_cap() {
    const METHOD: &str = "i98_3_a_filled_cap_warns_once_high_once_full_and_status_reports_the_cap";
    let rt = runtime();
    let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let dir = config_testkit::fs::temp_dir();
    let budgets = fast();
    let db_config = DbConfig {
        dedup_cap: Some(CAP),
        ..config(dir.path())
    };
    let mut db = rt
        .block_on(Db::open_with(
            db_config,
            store,
            rt.handle().clone(),
            budgets,
        ))
        .expect("open");
    db.wait_ready(crate::host::test_patience(Duration::from_secs(10)))
        .expect("ready");

    let mut written = 0_u64;
    let refused = loop {
        let id = app_request_id(written + 1).expect("an app id");
        match db.put_as(id, b"a", &written.to_le_bytes(), None) {
            Ok(_) => written += 1,
            Err(refused) => break refused,
        }
        assert!(
            written <= CAP as u64,
            "{written} writes and still no refusal"
        );
    };
    assert_eq!(refused.error.name(), "OVERLOADED", "{refused:?}");
    tracing::info!(written, "i98_cap_filled");

    let owner = db
        .node_status()
        .into_iter()
        .find(|status| status.node == NodeId(1))
        .expect("node 1's status");
    let lengths = owner.lengths.expect("node 1 reports its index lengths");
    assert_eq!(
        (lengths.dedup, lengths.dedup_cap),
        (CAP, CAP),
        "status reports the fill against the cap: {lengths:?}"
    );

    // One more refused write logs nothing more.
    let again = db
        .put_as(
            app_request_id(written + 1).expect("an app id"),
            b"a",
            b"x",
            None,
        )
        .expect_err("still full");
    assert_eq!(again.error.name(), "OVERLOADED", "{again:?}");
    db.shutdown();
    drop(db);

    let lines = cap_lines(METHOD);
    let seen: Vec<(String, String, u64, u64, u64, u64)> = lines
        .iter()
        .map(|line| {
            (
                line["@m"].as_str().unwrap_or_default().to_owned(),
                line["@l"].as_str().unwrap_or_default().to_owned(),
                line["node"].as_u64().unwrap_or(u64::MAX),
                line["partition"].as_u64().unwrap_or(u64::MAX),
                line["len"].as_u64().unwrap_or(u64::MAX),
                line["cap"].as_u64().unwrap_or(u64::MAX),
            )
        })
        .collect();
    let warn = || "Warning".to_owned();
    assert_eq!(
        seen,
        vec![
            ("dedup_cap_high".to_owned(), warn(), 1, 1, 8, CAP as u64),
            (
                "dedup_cap_full".to_owned(),
                warn(),
                1,
                1,
                CAP as u64,
                CAP as u64
            ),
        ],
        "{lines:?}"
    );
    for line in &lines {
        assert_eq!(
            line["retention_ms"].as_u64(),
            Some(budgets.dedup_retention_millis),
            "each line names the retention: {line:?}"
        );
    }
}
