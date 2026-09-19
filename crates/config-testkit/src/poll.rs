//! Deadline-bounded polling (TA-6; test plan §6 anti-flake rules 2-3).
//!
//! No test may sleep a fixed duration and assume consensus happened. Instead a test polls a
//! predicate until it succeeds or a deadline passes, and the deadline itself is derived from
//! the harness's election timeout rather than written as a literal, so a slower CI machine is
//! handled by one config change rather than by editing every wait.

use std::future::Future;
use std::time::Duration;

/// A poll deadline expired before the predicate returned `Some`.
///
/// Carries enough to make the failure diagnosable without re-running: how long was actually
/// waited, and a caller-supplied description of the last observed state.
#[derive(Debug, Clone)]
pub struct Timeout {
    /// Wall time actually spent polling before giving up.
    pub elapsed: Duration,
    /// Description of the last observed state, so a timeout failure names what it saw instead
    /// of just "gave up" (test plan §6 rule 2: "a timeout must fail with the last observed
    /// state... or the failure is undiagnosable").
    pub last_diagnostic: String,
}

impl std::fmt::Display for Timeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "timed out after {:?}: {}",
            self.elapsed, self.last_diagnostic
        )
    }
}

impl std::error::Error for Timeout {}

/// Poll a synchronous predicate until it returns `Some(value)` or `deadline` elapses.
///
/// `pred` is called immediately, then again every `interval` until it succeeds or the
/// deadline passes. This is the seam every `wait_for_leader`/`wait_applied`-style API in the
/// eventual `Cluster` harness is built from.
pub async fn poll_until<T>(
    deadline: Duration,
    interval: Duration,
    mut pred: impl FnMut() -> Option<T>,
) -> Result<T, Timeout> {
    poll_until_async(deadline, interval, move || {
        let value = pred();
        async move { value }
    })
    .await
}

/// Poll an asynchronous predicate until it returns `Some(value)` or `deadline` elapses.
///
/// Use this when producing a diagnostic (or the check itself) requires an `await`, e.g.
/// reading a node's metrics over a channel. The sleep between attempts is the sanctioned use
/// of `tokio::time::sleep` under anti-flake rule 1: it is bounded by `deadline` and only ever
/// gates the *next attempt*, never used as a proxy for "the operation finished".
pub async fn poll_until_async<T, F, Fut>(
    deadline: Duration,
    interval: Duration,
    mut pred: F,
) -> Result<T, Timeout>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let start = tokio::time::Instant::now();
    let deadline_at = start + deadline;
    loop {
        if let Some(value) = pred().await {
            return Ok(value);
        }
        let now = tokio::time::Instant::now();
        if now >= deadline_at {
            return Err(Timeout {
                elapsed: now.saturating_duration_since(start),
                last_diagnostic: format!(
                    "predicate did not return Some(_) within {deadline:?} (interval {interval:?})"
                ),
            });
        }
        let remaining = deadline_at.saturating_duration_since(now);
        tokio::time::sleep(interval.min(remaining)).await; // testkit:allow-sleep: bounded poll wait, not a synchronization sleep
    }
}

/// Heartbeat and election timers a harness derives its poll deadlines from (test plan §6 rule
/// 3: "deadlines are derived, not literal").
///
/// The eventual `Cluster` harness configures the real OpenRaft timers from the same values, so
/// a test's deadline and the cluster's actual timeout move together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TestTimers {
    /// Interval between leader heartbeats.
    pub heartbeat: Duration,
    /// Lower bound of the randomized election timeout range.
    pub election_timeout_min: Duration,
    /// Upper bound of the randomized election timeout range. Deadlines are derived from this
    /// bound, since it is the worst case a correct implementation may take.
    pub election_timeout_max: Duration,
}

impl TestTimers {
    /// The harness default: 250 ms heartbeat, 750-1500 ms randomized election timeout.
    pub const DEFAULT: Self = Self {
        heartbeat: Duration::from_millis(250),
        election_timeout_min: Duration::from_millis(750),
        election_timeout_max: Duration::from_millis(1500),
    };

    /// `n` times the worst-case election timeout, e.g. `10 * election_timeout` for "no leader
    /// was ever elected" (test plan §6 rule 3 example), stretched by [`deadline_scale`].
    pub fn multiple(&self, n: u32) -> Duration {
        self.election_timeout_max * n * deadline_scale()
    }
}

/// Wall-clock deadlines assume the host can actually run the cluster. `RETCD_TEST_DEADLINE_SCALE`
/// (an integer, default `1`) stretches every derived deadline for an oversubscribed or
/// instrumented host without touching the Raft timers, which keep their real values so the rows
/// still test the real thing. A deadline is a bound on how long a poll may wait for an observed
/// state, never a sleep, so stretching it changes only how patient a row is, not what it asserts.
/// The gate scripts set it; an idle developer host needs nothing.
pub fn deadline_scale() -> u32 {
    std::env::var("RETCD_TEST_DEADLINE_SCALE")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(1)
        .max(1)
}

impl Default for TestTimers {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// `n` times the default [`TestTimers::election_timeout_max`].
///
/// Free-function convenience for the common case; a harness that configures non-default
/// timers uses [`TestTimers::multiple`] directly.
pub fn election_timeout_multiple(n: u32) -> Duration {
    TestTimers::DEFAULT.multiple(n)
}
