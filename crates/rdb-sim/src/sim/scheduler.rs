//! The event queue: one total order over everything that happens.
//!
//! Ordered by `(tick, event_id)`. The id is not decoration — spike §6 requires that equal-time
//! events have stable ids and that generated schedules vary their order *explicitly*, rather
//! than inheriting whatever order a hash map or a thread pool happened to produce.
//!
//! The queue jumps to the next scheduled tick. It never advances through idle milliseconds, so a
//! twenty-four-hour dedup expiry costs the same as a one-millisecond timer.
//!
//! # Seed state
//!
//! Signatures only; every method returns [`SimError::Unavailable`]. Package H1 lands the queue.
//! It will be a `BTreeMap<(Tick, EventId), Event>` — never a binary heap, whose tie order among
//! equal keys is unspecified, and never a `HashMap`.

use rdb_core::contracts::event::Event;
use rdb_core::contracts::ids::EventId;
use rdb_core::contracts::time::Tick;

use crate::error::SimError;

/// The ordered event queue.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Scheduler;

impl Scheduler {
    /// An empty queue at [`Tick::ZERO`].
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Current logical time: the tick of the last event taken, or [`Tick::ZERO`].
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] until package H1 lands the queue.
    pub const fn now(&self) -> Result<Tick, SimError> {
        Err(SimError::unavailable("sim::scheduler::Scheduler::now"))
    }

    /// Allocate the next event id. Strictly increasing for the life of the run.
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] until package H1 lands the queue.
    pub const fn next_event_id(&mut self) -> Result<EventId, SimError> {
        Err(SimError::unavailable(
            "sim::scheduler::Scheduler::next_event_id",
        ))
    }

    /// Queue an event.
    ///
    /// # Errors
    ///
    /// [`SimError::Config`] when the event is scheduled before [`Scheduler::now`], which would
    /// reorder the past. [`SimError::Unavailable`] until package H1 lands the queue.
    pub fn schedule(&mut self, _event: Event) -> Result<(), SimError> {
        Err(SimError::unavailable("sim::scheduler::Scheduler::schedule"))
    }

    /// Take the next event, advancing logical time to its tick.
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] until package H1 lands the queue.
    pub const fn next(&mut self) -> Result<Option<Event>, SimError> {
        Err(SimError::unavailable("sim::scheduler::Scheduler::next"))
    }

    /// How many events are queued.
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] until package H1 lands the queue.
    pub const fn queued(&self) -> Result<usize, SimError> {
        Err(SimError::unavailable("sim::scheduler::Scheduler::queued"))
    }
}
