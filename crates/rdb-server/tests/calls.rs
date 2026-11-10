//! The gateway's call line (cs-tester D2, 548d78a): every call ends in one `rdb_server_call`
//! line, including a call whose client gave up first. That is the unknown-outcome case, where
//! an operator settles an id-less write from the gateway's log.
#![allow(clippy::result_large_err)] // `tonic::Status`, as in the library.

use std::time::{Duration, Instant};

use tonic::Status;

const MODULE: &str = module_path!();

/// The `rdb_server_call` lines this test has logged so far.
fn call_lines(method: &str) -> Vec<serde_json::Value> {
    config_log::layer::flush_test_logs();
    config_testkit::logs::lines_for_current_test(MODULE, method)
        .into_iter()
        .filter(|line| line["@m"] == "rdb_server_call")
        .collect()
}

/// Wait up to 5 s for a call line: it is written when the work ends, after the caller left.
async fn wait_for_call_line(method: &str) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(line) = call_lines(method).into_iter().next() {
            return line;
        }
        assert!(
            Instant::now() < deadline,
            "no rdb_server_call line within 5 s of the work ending"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A put whose client deadline passes mid-call: tonic drops the handler's future. The work
/// still finishes, and its line names the method, the final kind, the request id, and that
/// the caller had gone.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn d2_an_abandoned_call_still_writes_its_call_line() {
    const TEST: &str = "d2_an_abandoned_call_still_writes_its_call_line";
    tracing::info!("test_started");
    let (release, held) = std::sync::mpsc::channel::<()>();
    let call = rdb_server::logged_call(
        "Put",
        |id: &u64| Some(*id),
        move || {
            held.recv()
                .map_err(|_| Status::internal("test dropped the release"))?;
            Ok(4_398_046_511_122_u64)
        },
    );
    let abandoned = tokio::time::timeout(Duration::from_millis(50), call).await;
    assert!(
        abandoned.is_err(),
        "the call answered before the caller gave up"
    );
    release.send(()).expect("the work is still waiting");

    let line = wait_for_call_line(TEST).await;
    assert_eq!(line["method"], "Put", "{line}");
    assert_eq!(line["kind"], "OK", "{line}");
    assert_eq!(line["request_id"], 4_398_046_511_122_u64, "{line}");
    assert_eq!(line["caller_gone"], true, "{line}");
}

/// A call the caller waited for says so: `caller_gone=false`.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn d2_an_answered_call_says_the_caller_waited() {
    const TEST: &str = "d2_an_answered_call_says_the_caller_waited";
    let answer = rdb_server::logged_call("Get", |_: &u64| None, || Ok(1_u64)).await;
    assert_eq!(answer.expect("the work answered"), 1);

    let line = wait_for_call_line(TEST).await;
    assert_eq!(line["method"], "Get", "{line}");
    assert_eq!(line["kind"], "OK", "{line}");
    assert_eq!(line["caller_gone"], false, "{line}");
}

/// F-008: a failed write's line carries the request id and the mutation proof from its
/// `RdbError`. An unknown outcome names the id to settle and says it may have applied.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn f008_a_failed_write_line_names_its_request_and_that_it_may_have_applied() {
    const TEST: &str = "f008_a_failed_write_line_names_its_request_and_that_it_may_have_applied";
    let error = rdb_api::ApiError::new(rdb_api::ErrorKind::UnknownOutcome, "no answer in time");
    let status = rdb_server::to_status(
        &error,
        Some((
            rdb_api::RequestId(4_398_046_511_133),
            Some(rdb_api::Generation(2)),
        )),
    );
    let answer = rdb_server::logged_call("Txn", |_: &u64| None, move || Err(status)).await;
    assert!(answer.is_err(), "the work failed");

    let line = wait_for_call_line(TEST).await;
    assert_eq!(line["method"], "Txn", "{line}");
    assert_eq!(line["kind"], "UNKNOWN_OUTCOME", "{line}");
    assert_eq!(line["request_id"], 4_398_046_511_133_u64, "{line}");
    assert_eq!(line["no_mutation"], false, "{line}");
    assert_eq!(line["caller_gone"], false, "{line}");
}

/// F-008: a refusal that changed nothing says so on its line, and names no request when none
/// was sent.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn f008_a_refusal_line_says_it_changed_nothing() {
    const TEST: &str = "f008_a_refusal_line_says_it_changed_nothing";
    let mut error = rdb_api::ApiError::new(rdb_api::ErrorKind::ProtectionPaused, "paused");
    error.no_mutation = true;
    let status = rdb_server::to_status(&error, None);
    let answer = rdb_server::logged_call("Put", |_: &u64| None, move || Err(status)).await;
    assert!(answer.is_err(), "the work failed");

    let line = wait_for_call_line(TEST).await;
    assert_eq!(line["method"], "Put", "{line}");
    assert_eq!(line["kind"], "PROTECTION_PAUSED", "{line}");
    assert_eq!(line["no_mutation"], true, "{line}");
    assert!(line["request_id"].is_null(), "{line}");
}
