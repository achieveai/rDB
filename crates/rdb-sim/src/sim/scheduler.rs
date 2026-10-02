//! The event queue: one total order over everything that happens.
//!
//! Ordered by `(tick, event_id)`. The id is not decoration — spike §6 requires that equal-time
//! events have stable ids and that generated schedules vary their order *explicitly*, rather
//! than inheriting whatever order a hash map or a thread pool happened to produce.
//!
//! The queue jumps to the next scheduled tick. It never advances through idle milliseconds, so a
//! twenty-four-hour dedup expiry costs the same as a one-millisecond timer.
//!
//! A `BTreeMap<(Tick, EventId), Event>` — never a binary heap, whose tie order among equal keys
//! is unspecified, and never a `HashMap`.

use std::collections::BTreeMap;

use rdb_core::contracts::event::Event;
use rdb_core::contracts::ids::EventId;
use rdb_core::contracts::time::Tick;

use crate::error::SimError;

/// The ordered event queue.
///
/// Holds its state and is not `Copy` (finding K-F-29): a silent copy of a scheduler would
/// duplicate the queue rather than alias it, invisibly at the call site.
#[derive(Debug)]
pub struct Scheduler {
    now: Tick,
    next_id: EventId,
    queue: BTreeMap<(Tick, EventId), Event>,
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl Scheduler {
    /// An empty queue at [`Tick::ZERO`].
    #[must_use]
    pub const fn new() -> Self {
        Self {
            now: Tick::ZERO,
            next_id: EventId(0),
            queue: BTreeMap::new(),
        }
    }

    /// Current logical time: the tick of the last event taken, or [`Tick::ZERO`].
    #[must_use]
    pub const fn now(&self) -> Tick {
        self.now
    }

    /// Allocate the next event id. Strictly increasing for the life of the run.
    pub const fn next_event_id(&mut self) -> EventId {
        let id = self.next_id;
        self.next_id = EventId(id.0 + 1);
        id
    }

    /// Queue an event.
    ///
    /// # Errors
    ///
    /// [`SimError::Config`] naming `at` when the event is scheduled before [`Scheduler::now`],
    /// which would reorder the past, and naming `event_id` when an event with the same tick and
    /// id is already queued — one id is one event, and overwriting it would lose one silently.
    pub fn schedule(&mut self, event: Event) -> Result<(), SimError> {
        if event.at < self.now {
            return Err(SimError::Config { field: "at" });
        }
        let key = (event.at, event.id);
        if self.queue.contains_key(&key) {
            return Err(SimError::Config { field: "event_id" });
        }
        self.queue.insert(key, event);
        Ok(())
    }

    /// Take the next event, advancing logical time to its tick: the jump to the next deadline
    /// (spike §6). Idle milliseconds are never ticked through.
    pub fn pop(&mut self) -> Option<Event> {
        let ((at, _), event) = self.queue.pop_first()?;
        self.now = at;
        Some(event)
    }

    /// How many events are queued.
    #[must_use]
    pub fn queued(&self) -> usize {
        self.queue.len()
    }

    /// The tick of the next queued event, or `None` when the queue is empty.
    #[must_use]
    pub fn next_tick(&self) -> Option<Tick> {
        self.queue.keys().next().map(|(at, _)| *at)
    }
}
