//! The host clock: a [`Tick`] is UTC milliseconds, advanced by a monotonic clock.
//!
//! One clock for the whole process (Decision 1). It is sampled once at start, so a wall-clock
//! step after start never moves a tick backwards. A1 reads the same value as its UTC estimate
//! (`StepCtx::control_time`), with the budget's clock error as the bound (ADR-0007 §5
//! assumption 3; M12 qualifies it).
//!
//! Crate tests have a second mode, [`HostClock::capped`] (#50): the same clock, except that
//! one reading moves at most a set step past the last. A stall of the thread that steps the
//! hosts then costs them one step of their time, not the whole stall.

#[cfg(test)]
use std::sync::Mutex;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use rdb_core::contracts::event::Budgets;
use rdb_core::contracts::time::{ControlTime, Tick};

/// UTC milliseconds at start plus monotonic time since.
#[derive(Debug, Clone, Copy)]
pub struct HostClock {
    start_utc_ms: u64,
    start: Instant,
    /// The capped mode's state, when this clock is one (crate tests only).
    #[cfg(test)]
    capped: Option<&'static Capped>,
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
            #[cfg(test)]
            capped: None,
        }
    }

    /// A clock whose readings each move at most `max_step_millis` past the last one (#50).
    /// Between readings closer than that it is [`Self::start`]'s clock. A longer gap, a stall
    /// of the reading thread, counts `max_step_millis` and is logged
    /// `test_clock_capped{real_millis, counted_millis}`. Copies share one state, so every host
    /// in a row reads one time, and it never moves backwards.
    ///
    /// The state is leaked, once per call, so the clock stays `Copy` as its production callers
    /// take it: a few bytes per test cluster.
    #[cfg(test)]
    pub(crate) fn capped(max_step_millis: u64) -> Self {
        let clock = Self::start();
        let capped: &'static Capped = Box::leak(Box::new(Capped {
            max_step_millis,
            read: Mutex::new((clock.start, 0)),
        }));
        Self {
            capped: Some(capped),
            ..clock
        }
    }

    /// Now, as a tick.
    #[must_use]
    pub fn now(&self) -> Tick {
        #[cfg(test)]
        if let Some(capped) = self.capped {
            return Tick(self.start_utc_ms.saturating_add(capped.elapsed()));
        }
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

/// [`HostClock::capped`]'s shared state: the instant counted up to, and the milliseconds
/// counted since start.
#[cfg(test)]
#[derive(Debug)]
struct Capped {
    max_step_millis: u64,
    read: Mutex<(Instant, u64)>,
}

#[cfg(test)]
impl Capped {
    /// Milliseconds counted since start, this reading included. Within the cap the count moves
    /// by whole milliseconds and keeps the remainder for the next reading, so readings closer
    /// than a millisecond still add up. Past the cap it moves by the cap and drops the rest.
    fn elapsed(&self) -> u64 {
        let mut read = self
            .read
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (counted_to, counted) = *read;
        let now = Instant::now();
        let real = u64::try_from(now.saturating_duration_since(counted_to).as_millis())
            .unwrap_or(u64::MAX);
        let (to, step) = if real <= self.max_step_millis {
            (counted_to + std::time::Duration::from_millis(real), real)
        } else {
            tracing::info!(
                real_millis = real,
                counted_millis = self.max_step_millis,
                "test_clock_capped"
            );
            (now, self.max_step_millis)
        };
        *read = (to, counted.saturating_add(step));
        read.1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// #50: a stall between two readings counts the cap, and readings far closer together than
    /// a millisecond still add up to the time that passed. Cuts: count the whole gap, red on
    /// the stall; drop the sub-millisecond remainder, red on the close readings.
    /// Unit, ~0.2 s.
    #[test]
    fn a_capped_clock_counts_a_stall_as_one_step_and_close_readings_in_full() {
        let clock = HostClock::capped(20);
        let before = clock.now();
        std::thread::sleep(Duration::from_millis(120));
        let after = clock.now();
        assert_eq!(
            after.0 - before.0,
            20,
            "a 120 ms stall counts the 20 ms cap"
        );

        // A copy reads the same state. Half the real time is the floor, so a scheduler stall
        // inside the loop (clipped to the cap) cannot fail the row; the cut counts ~0.
        let copy = clock;
        let start = clock.now();
        let began = Instant::now();
        let mut last = start;
        while began.elapsed() < Duration::from_millis(60) {
            let now = copy.now();
            assert!(now >= last, "never backwards");
            last = now;
        }
        let counted = clock.now().0 - start.0;
        let real = u64::try_from(began.elapsed().as_millis()).unwrap_or(u64::MAX);
        assert!(
            counted >= real / 2 && counted <= real + 1,
            "{real} ms of close readings count {counted} ms"
        );
    }
}
