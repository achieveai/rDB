//! Proves `poll::poll_until`/`poll_until_async` actually observe state rather than guessing a
//! duration, and that `election_timeout_multiple` derives from `TestTimers` rather than being
//! a literal (test plan §6 rules 2-3).

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use config_testkit::election_timeout_multiple;
use config_testkit::poll::{poll_until, poll_until_async, TestTimers};

#[config_log::retcd_test]
async fn poll_until_returns_as_soon_as_the_predicate_succeeds() {
    let attempts = Arc::new(AtomicU32::new(0));
    let a = attempts.clone();

    let result = poll_until(
        Duration::from_millis(500),
        Duration::from_millis(5),
        move || {
            let n = a.fetch_add(1, Ordering::SeqCst) + 1;
            (n >= 3).then_some(n)
        },
    )
    .await;

    let value = result.expect("predicate succeeds well before the deadline");
    assert_eq!(value, 3);
    assert!(
        attempts.load(Ordering::SeqCst) >= 3,
        "must have actually polled at least until success"
    );
}

#[config_log::retcd_test]
async fn poll_until_times_out_with_a_diagnostic() {
    let started = tokio::time::Instant::now();
    let result: Result<(), _> =
        poll_until(Duration::from_millis(40), Duration::from_millis(10), || {
            None
        })
        .await;

    let timeout = result.expect_err("a predicate that never succeeds must time out");
    assert!(
        timeout.elapsed >= Duration::from_millis(40),
        "elapsed ({:?}) must be at least the requested deadline",
        timeout.elapsed
    );
    assert!(
        !timeout.last_diagnostic.is_empty(),
        "a timeout without a diagnostic is undiagnosable"
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the test itself must not have blocked far past the deadline"
    );
}

#[config_log::retcd_test]
async fn poll_until_async_polls_an_async_predicate() {
    let attempts = Arc::new(AtomicU32::new(0));
    let a = attempts.clone();

    let result = poll_until_async(
        Duration::from_millis(500),
        Duration::from_millis(5),
        move || {
            let a = a.clone();
            async move {
                let n = a.fetch_add(1, Ordering::SeqCst) + 1;
                tokio::task::yield_now().await;
                (n >= 2).then_some(n)
            }
        },
    )
    .await;

    assert_eq!(result.expect("async predicate eventually succeeds"), 2);
}

#[config_log::retcd_test]
fn election_timeout_multiple_is_derived_not_literal() {
    let n = 10;
    assert_eq!(
        election_timeout_multiple(n),
        TestTimers::DEFAULT.election_timeout_max * n,
        "the free function must derive from TestTimers, not hardcode a duration"
    );

    let custom = TestTimers {
        heartbeat: Duration::from_millis(50),
        election_timeout_min: Duration::from_millis(100),
        election_timeout_max: Duration::from_millis(200),
    };
    assert_eq!(custom.multiple(3), Duration::from_millis(600));
}

/// The crate root re-exports `poll_until` for convenience; prove it is callable from there.
#[config_log::retcd_test]
async fn poll_until_is_reexported_at_crate_root() {
    let result =
        config_testkit::poll_until(Duration::from_millis(50), Duration::from_millis(5), || {
            Some(())
        })
        .await;
    assert!(result.is_ok());
}
