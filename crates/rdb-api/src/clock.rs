//! The host clock: a [`Tick`] is UTC milliseconds, advanced by a monotonic clock.
//!
//! One clock for the whole process (Decision 1). It is sampled once at start, so a wall-clock
//! step after start never moves a tick backwards. A1 reads the same value as its UTC estimate
//! (`StepCtx::control_time`), with the budget's clock error as the bound (ADR-0007 §5
//! assumption 3; M12 qualifies it).

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use rdb_core::contracts::event::Budgets;
use rdb_core::contracts::time::{ControlTime, Tick};

/// UTC milliseconds at start plus monotonic time since.
#[derive(Debug, Clone, Copy)]
pub struct HostClock {
    start_utc_ms: u64,
    start: Instant,
}

impl HostClock {
    /// Sample the wall clock once.
    ///
    /// # Panics
    ///
    /// When the system clock reads before 1970: no tick could be honest then.
    #[must_use]
    pub fn start() -> Self {
        let since_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the system clock reads after 1970");
        Self {
            start_utc_ms: u64::try_from(since_epoch.as_millis()).unwrap_or(u64::MAX),
            start: Instant::now(),
        }
    }

    /// Now, as a tick.
    #[must_use]
    pub fn now(&self) -> Tick {
        let elapsed = u64::try_from(self.start.elapsed().as_millis()).unwrap_or(u64::MAX);
        Tick(self.start_utc_ms.saturating_add(elapsed))
    }

    /// The control-time sample A1 sees at `now`: the estimate is the tick itself.
    #[must_use]
    pub const fn control_time(now: Tick, budgets: &Budgets) -> ControlTime {
        ControlTime {
            estimate: now,
            error_millis: budgets.clock_error_millis,
            bound_established: true,
            sampled_at: now,
        }
    }

    /// Milliseconds from `now` until `at`, zero when `at` has passed.
    #[must_use]
    pub const fn millis_until(now: Tick, at: Tick) -> u64 {
        at.0.saturating_sub(now.0)
    }
}
