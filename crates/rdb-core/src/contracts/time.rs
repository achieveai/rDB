//! The time seam: logical ticks, deadlines, timers, and the bounded-clock estimate.
//!
//! No type here can be constructed from a real clock. `Instant::now` and `SystemTime` never
//! appear in `rdb-core`; a tick arrives in [`crate::contracts::event::StepCtx`] or as a
//! [`TimerFired`] event, and that is the only way a kernel module learns what time it is.
//!
//! Two different notions of time live here and must not be confused:
//!
//! * [`Tick`] — the simulator's monotonic logical clock. Ordering only.
//! * [`ControlTime`] — an *estimate* of the shared authority clock, with an explicit error
//!   bound. Spec §7.2's bounded-clock mode compares grant expiry against this, never against
//!   [`Tick`], and a comparison that the error bound makes ambiguous must fail closed.

use serde::{Deserialize, Serialize};

use crate::contracts::ids::{TimerId, TimerVersion};

/// One millisecond of logical time. The spec states its thresholds in milliseconds
/// (1 s warn, 2 s pause, 3 s grant, 500 ms renewal, ±100 ms clock error), so the tick is a
/// millisecond and no threshold needs a conversion.
pub const TICK_MILLIS: u64 = 1;

/// Monotonic logical time. Ordering is the only meaning it has.
///
/// The scheduler jumps straight to the next deadline rather than ticking through idle
/// milliseconds (spike §6), so the gap between consecutive ticks in a run carries no
/// information about work done.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Tick(
    /// Milliseconds since the start of the run.
    pub u64,
);

impl Tick {
    /// The start of a run.
    pub const ZERO: Self = Self(0);

    /// The tick `millis` later. Saturates rather than wrapping: a wrap would silently reorder
    /// the event queue.
    #[must_use]
    pub const fn plus_millis(self, millis: u64) -> Self {
        Self(self.0.saturating_add(millis))
    }

    /// Milliseconds from `self` to `later`, or zero when `later` is not after `self`.
    #[must_use]
    pub const fn millis_until(self, later: Self) -> u64 {
        later.0.saturating_sub(self.0)
    }
}

/// A point by which something must happen.
///
/// Transmitted between nodes as a *remaining duration*, never as an absolute instant: spec §5.1
/// forbids trusting a client's wall clock. [`Deadline::remaining_millis`] is what crosses the
/// transport seam; the absolute [`Tick`] is local.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Deadline {
    /// The local tick at which the deadline expires.
    pub at: Tick,
}

impl Deadline {
    /// The remaining duration to transmit, measured from `now`.
    #[must_use]
    pub const fn remaining_millis(self, now: Tick) -> u64 {
        now.millis_until(self.at)
    }

    /// Whether the deadline has already passed at `now`.
    #[must_use]
    pub const fn elapsed(self, now: Tick) -> bool {
        self.at.0 <= now.0
    }
}

/// An estimate of the shared authority clock, with the error bound it is only valid within.
///
/// Spec §7.2: a configured maximum UTC error `epsilon` and a dispatch margin `delta`. An old
/// owner may admit only while its clock is below `expiry - epsilon - delta`; a new owner may
/// activate only above `expiry + epsilon + delta`. Between those points the answer is
/// *unknown*, and unknown must deny.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ControlTime {
    /// The best estimate of the authority clock.
    pub estimate: Tick,
    /// Maximum error of `estimate`, in milliseconds. This is spec §7.2's `epsilon`.
    pub error_millis: u64,
    /// Whether the error bound is currently established. A backward jump, a process resume, a
    /// reboot or an authority-generation change clears it, and a node with no established bound
    /// must stop accepting requests (spec §7.2).
    pub bound_established: bool,
}

/// How a bounded-clock comparison came out.
///
/// Three-valued on purpose. A boolean would force every caller to pick a side for the ambiguous
/// case, and the safe side differs between "may I still admit?" and "may I activate?".
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ClockVerdict {
    /// Provably before the compared instant, even at the worst end of the error bound.
    DefinitelyBefore,
    /// Provably after it, even at the worst end of the error bound.
    DefinitelyAfter,
    /// The error bound spans the instant, or no bound is established. Callers must fail closed.
    Uncertain,
}

impl ControlTime {
    /// Compare this estimate against `instant`, widened by `margin_millis` on both sides.
    ///
    /// `margin_millis` is spec §7.2's dispatch margin `delta`. Lives here rather than in the
    /// authority module because publication, recovery and the protection timer all need the same
    /// comparison, and one of them getting the sign wrong is a fencing violation.
    #[must_use]
    pub const fn compare(self, instant: Tick, margin_millis: u64) -> ClockVerdict {
        if !self.bound_established {
            return ClockVerdict::Uncertain;
        }
        let slack = self.error_millis.saturating_add(margin_millis);
        if self.estimate.0.saturating_add(slack) < instant.0 {
            ClockVerdict::DefinitelyBefore
        } else if self.estimate.0 > instant.0.saturating_add(slack) {
            ClockVerdict::DefinitelyAfter
        } else {
            ClockVerdict::Uncertain
        }
    }
}

/// Arm or cancel a timer. Emitted by a kernel module as an effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum TimerEffect {
    /// Arm `id` at version `version` to fire at `at`. Re-arming an existing `id` with a higher
    /// version supersedes the old arm; the old fire, if already in flight, is ignored on arrival.
    Arm {
        /// The timer being armed.
        id: TimerId,
        /// The version this arm establishes.
        version: TimerVersion,
        /// When it should fire.
        at: Tick,
    },
    /// Cancel `id`. A fire already in flight still arrives and is still ignored by version.
    Cancel {
        /// The timer being cancelled.
        id: TimerId,
        /// The version being cancelled.
        version: TimerVersion,
    },
}

/// A timer fired. Delivered as an event like any other completion.
///
/// The expiry/cancel race is deliverable on purpose (spike §4): the environment may hand the
/// kernel a fire for a timer that was cancelled or re-armed, and the kernel must ignore it by
/// comparing `version` to what it currently holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TimerFired {
    /// The timer that fired.
    pub id: TimerId,
    /// The version that was armed when this fire was scheduled.
    pub version: TimerVersion,
    /// The tick the fire was scheduled for. May be earlier than the current tick.
    pub scheduled_at: Tick,
}
