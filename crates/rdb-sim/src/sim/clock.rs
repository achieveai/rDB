//! Manual time: the tick, the timer table, and the bounded-clock estimate.
//!
//! Nothing here reads a real clock. Time moves only when the scheduler takes an event and the
//! harness tells the clock so through [`Clock::advance`], and a timer fires only because the
//! scheduler delivered its fire event.
//!
//! Two races are deliverable on purpose (spike §4, §5):
//!
//! * **Expiry versus cancel.** Cancelling a timer does not unschedule a fire that is already in
//!   flight. The fire still arrives, carrying the version that was armed when it was scheduled,
//!   and the kernel ignores it because that version is stale. A simulator that quietly dropped
//!   the fire would hide the bug in every kernel that forgot the version check.
//! * **Clock skew.** [`Clock::set_skew`] moves one node's authority-clock estimate relative to
//!   the others, inside and outside spec §7.2's `epsilon`. Outside the bound, the estimate is
//!   reported with [`ControlTime::bound_established`] false and every comparison must fail
//!   closed.

use std::collections::BTreeMap;

use rdb_core::contracts::ids::{NodeId, TimerId, TimerVersion};
use rdb_core::contracts::time::{ControlTime, Tick, TimerFired};

use crate::error::SimError;

/// One node's deviation from the authority clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Skew {
    millis: i64,
    bound_established: bool,
}

/// The manual clock and its timer table.
///
/// Holds its state and is not `Copy` (finding K-F-29).
#[derive(Debug)]
pub struct Clock {
    now: Tick,
    /// Spec §7.2's `epsilon`, reported on every sample.
    error_millis: u64,
    skew: BTreeMap<NodeId, Skew>,
    timers: BTreeMap<(NodeId, TimerId), (TimerVersion, Tick)>,
}

impl Default for Clock {
    fn default() -> Self {
        Self::new(100)
    }
}

impl Clock {
    /// A clock at [`Tick::ZERO`] with no timers, no skew, and `error_millis` as every node's
    /// reported clock error.
    #[must_use]
    pub fn new(error_millis: u64) -> Self {
        Self {
            now: Tick::ZERO,
            error_millis,
            skew: BTreeMap::new(),
            timers: BTreeMap::new(),
        }
    }

    /// Current logical time, as last told by [`Clock::advance`].
    #[must_use]
    pub const fn now(&self) -> Tick {
        self.now
    }

    /// Move logical time to `now`. The harness calls this with the scheduler's tick.
    ///
    /// # Errors
    ///
    /// [`SimError::Config`] naming `now` when it is earlier than the current tick. Logical time
    /// never runs backwards; a backward jump of the *authority* clock is [`Clock::set_skew`].
    pub fn advance(&mut self, now: Tick) -> Result<(), SimError> {
        if now < self.now {
            return Err(SimError::Config { field: "now" });
        }
        self.now = now;
        Ok(())
    }

    /// The authority-clock estimate as `node` currently sees it, sampled at [`Clock::now`].
    ///
    /// The estimate is the logical tick plus the node's skew; the error bound is the clock's
    /// configured `epsilon`; `bound_established` is whatever [`Clock::set_skew`] last said for
    /// the node, `true` for a node never skewed. `sampled_at` is the current tick, so a caller
    /// that holds the sample and asks later is what
    /// [`ControlTime::is_stale`] exists for.
    #[must_use]
    pub fn control_time(&self, node: NodeId) -> ControlTime {
        let skew = self.skew.get(&node).copied().unwrap_or(Skew {
            millis: 0,
            bound_established: true,
        });
        let estimate = if skew.millis >= 0 {
            self.now.plus_millis(skew.millis.unsigned_abs())
        } else {
            Tick(self.now.0.saturating_sub(skew.millis.unsigned_abs()))
        };
        ControlTime {
            estimate,
            error_millis: self.error_millis,
            bound_established: skew.bound_established,
            sampled_at: self.now,
        }
    }

    /// Give `node` a clock error of `skew_millis`, and say whether its bound is still
    /// established.
    pub fn set_skew(&mut self, node: NodeId, skew_millis: i64, bound_established: bool) {
        self.skew.insert(
            node,
            Skew {
                millis: skew_millis,
                bound_established,
            },
        );
    }

    /// Arm a timer. A higher version for an existing id supersedes the old arm without
    /// unscheduling its fire.
    ///
    /// # Errors
    ///
    /// [`SimError::Config`] naming `version` when `version` is not above the version already
    /// armed for this id: a re-arm at the same or a lower version would let a stale fire pass
    /// the kernel's version check.
    pub fn arm(
        &mut self,
        node: NodeId,
        id: TimerId,
        version: TimerVersion,
        at: Tick,
    ) -> Result<(), SimError> {
        if let Some((armed, _)) = self.timers.get(&(node, id)) {
            if version <= *armed {
                return Err(SimError::Config { field: "version" });
            }
        }
        self.timers.insert((node, id), (version, at));
        Ok(())
    }

    /// Cancel a timer. Any fire already in flight still arrives and is still stale.
    ///
    /// A cancel for a version that is not the armed one changes nothing: the kernel may cancel
    /// a timer it has already re-armed, and the newer arm must survive.
    pub fn cancel(&mut self, node: NodeId, id: TimerId, version: TimerVersion) {
        if self.timers.get(&(node, id)).map(|(armed, _)| *armed) == Some(version) {
            self.timers.remove(&(node, id));
        }
    }

    /// The next tick at which any timer is due, or `None`.
    ///
    /// What lets the scheduler jump to the next deadline instead of ticking through idle
    /// milliseconds.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Tick> {
        self.timers.values().map(|(_, at)| *at).min()
    }

    /// Every timer due at or before `now`, removed from the table, in `(node, id)` order.
    ///
    /// The harness turns each into a [`rdb_core::contracts::event::EventKind::Timer`] event.
    /// The fire carries the version that was armed, which is what makes the expiry/cancel race
    /// deliverable: a cancel that lands after this call has nothing to cancel, and the fire is
    /// already on its way.
    pub fn due(&mut self, now: Tick) -> Vec<(NodeId, TimerFired)> {
        let due: Vec<(NodeId, TimerId)> = self
            .timers
            .iter()
            .filter(|(_, (_, at))| *at <= now)
            .map(|(key, _)| *key)
            .collect();
        due.into_iter()
            .filter_map(|key| {
                let (version, scheduled_at) = self.timers.remove(&key)?;
                Some((
                    key.0,
                    TimerFired {
                        id: key.1,
                        version,
                        scheduled_at,
                    },
                ))
            })
            .collect()
    }
}
