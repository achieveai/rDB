//! The stall watchdog announces itself (A-R309).
//!
//! It logs `process_stall` or `runtime_stall` only when a clock wakes late, so a run with no
//! stall lines could mean no stalls or no watchdog. `stall_watchdog_started` at startup tells
//! the two apart from the log alone.

mod support;

use config_log::retcd_test;

use support::{startup_deadline, DaemonProcess, Harness};

/// A started daemon logs `stall_watchdog_started` once, with the tick and the threshold its
/// clocks use, inside the process span (so it carries `node_id`).
#[retcd_test]
async fn a_started_daemon_logs_that_its_watchdog_runs() {
    const METHOD: &str = "a_started_daemon_logs_that_its_watchdog_runs";
    let harness = Harness::with_nodes(METHOD, &[1]).await;
    let mut spec = harness.spec(0);
    spec.form = true;
    let mut process = DaemonProcess::spawn(spec);
    process
        .wait_ready(startup_deadline())
        .unwrap_or_else(|e| panic!("the node never became ready: {e}"));

    let is_started =
        |l: &serde_json::Value| support::log_field(l, "@m") == Some("stall_watchdog_started");
    let lines =
        support::log_lines_when(&process.log_file(), |lines| lines.iter().any(is_started)).await;
    let started: Vec<&serde_json::Value> = lines.iter().filter(|l| is_started(l)).collect();
    assert_eq!(
        started.len(),
        1,
        "a daemon must say once that its stall watchdog runs: {started:#?}"
    );
    let line = started[0];
    assert_eq!(
        line.get("tick_ms").and_then(serde_json::Value::as_u64),
        Some(100),
        "{line:#?}"
    );
    assert_eq!(
        line.get("threshold_ms").and_then(serde_json::Value::as_u64),
        Some(500),
        "{line:#?}"
    );
    assert_eq!(
        line.get("node_id").and_then(serde_json::Value::as_u64),
        Some(1),
        "the line carries the process fields: {line:#?}"
    );
}
