//! Manual time: the tick, the timer table, and the bounded-clock estimate.
//!
//! Nothing here reads a real clock. Time moves only when the scheduler takes an event, and a
//! timer fires only because the scheduler delivered its fire event.
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
//!
//! # Seed state
//!
//! Signatures only; package H1 lands the timer table.

use rdb_core::contracts::ids::{NodeId, TimerId, TimerVersion};
use rdb_core::contracts::time::{ControlTime, Tick};

use crate::error::SimError;

/// The manual clock and its timer table.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Clock;

impl Clock {
    /// A clock at [`Tick::ZERO`] with no timers and no skew.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// The authority-clock estimate as `node` currently sees it.
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] until package H1 lands the timer table.
    pub const fn control_time(&self, _node: NodeId) -> Result<ControlTime, SimError> {
        Err(SimError::unavailable("sim::clock::Clock::control_time"))
    }

    /// Give `node` a clock error of `skew_millis`, and say whether its bound is still
    /// established.
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] until package H1 lands the timer table.
    pub const fn set_skew(
        &mut self,
        _node: NodeId,
        _skew_millis: i64,
        _bound_established: bool,
    ) -> Result<(), SimError> {
        Err(SimError::unavailable("sim::clock::Clock::set_skew"))
    }

    /// Arm a timer. A higher version for an existing id supersedes the old arm without
    /// unscheduling its fire.
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] until package H1 lands the timer table.
    pub const fn arm(
        &mut self,
        _node: NodeId,
        _id: TimerId,
        _version: TimerVersion,
        _at: Tick,
    ) -> Result<(), SimError> {
        Err(SimError::unavailable("sim::clock::Clock::arm"))
    }

    /// Cancel a timer. Any fire already in flight still arrives and is still stale.
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] until package H1 lands the timer table.
    pub const fn cancel(
        &mut self,
        _node: NodeId,
        _id: TimerId,
        _version: TimerVersion,
    ) -> Result<(), SimError> {
        Err(SimError::unavailable("sim::clock::Clock::cancel"))
    }

    /// The next tick at which any timer is due, or `None`.
    ///
    /// What lets the scheduler jump to the next deadline instead of ticking through idle
    /// milliseconds.
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] until package H1 lands the timer table.
    pub const fn next_deadline(&self) -> Result<Option<Tick>, SimError> {
        Err(SimError::unavailable("sim::clock::Clock::next_deadline"))
    }
}
