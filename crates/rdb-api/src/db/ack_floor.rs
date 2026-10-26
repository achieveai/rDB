//! The client half of the ack floor (M10 S2, lead ruling A-R252): which of this open's request
//! ids the app holds a definitive answer for, so each write can tell the owner it may forget
//! them.
//!
//! - An id is **held** once its write is answered `Ok`, or refused with `no_mutation = true`. A
//!   write that compiled to nothing, or was refused before it was sent, is held too: nothing of
//!   it reached a dedup index.
//! - Any other answer makes it a **pin**: `UNKNOWN_OUTCOME`, or a refusal that cannot prove
//!   nothing happened. A pin stays in the owner's index, so a later status query or resend still
//!   settles it. At most [`MAX_PINS`] are carried. The ninth evicts the oldest, which is logged
//!   `ack_floor_pin_evicted` and from then on answers `STATUS_EXPIRED` (A5). A pin also ends
//!   when the app settles it, or `dedup_retention` after it was made, when the owner's 24 h
//!   aging drops it anyway.
//! - The floor is the lowest id of this open still in flight: every id below it is held or
//!   pinned.
//!
//! Ids are allocated under this tracker's lock ([`AckFloor::begin`]), so no thread computes a
//! floor above an id another thread has taken but not yet recorded. The tracker also bounds the
//! counter (A-R252 A2): a write never takes its open's reserved floor record id, nor an id of the
//! next open. With the floor off only that bound applies: nothing is tracked, and no request
//! carries a floor.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rdb_core::contracts::ids::RequestId;
use rdb_core::contracts::txn::MAX_PINS;
use rdb_core::transaction::dedup::{is_floor_record_id, open_base};

/// One open's held ids and pins.
#[derive(Debug)]
pub(crate) struct AckFloor {
    /// The open's base: every id this `Db` numbers a write with is in `[base, base | OPEN_MASK)`.
    base: u64,
    /// `DbConfig::ack_floor`: whether ids are tracked and requests carry a floor.
    enabled: bool,
    /// Ids taken and not yet answered.
    in_flight: BTreeSet<u64>,
    /// Ids answered without a definitive outcome, oldest first, with when.
    pins: Vec<(u64, Instant)>,
    /// How long a pin is carried: the owner's dedup retention.
    retention: Duration,
}

/// What a request carries: its `acked_below` and `pinned`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Carried {
    pub(crate) acked_below: u64,
    pub(crate) pinned: Vec<u64>,
}

impl AckFloor {
    /// The tracker of the open whose first id is `first`.
    pub(crate) const fn new(first: u64, enabled: bool, retention: Duration) -> Self {
        Self {
            base: open_base(RequestId(first)),
            enabled,
            in_flight: BTreeSet::new(),
            pins: Vec::new(),
            retention,
        }
    }

    /// Take the next id from `next` and record it in flight, in one step. `None` when the open's
    /// ids are used up: the next one is the reserved floor record id or past it (A-R252 A2).
    pub(crate) fn begin(&mut self, next: &AtomicU64) -> Option<u64> {
        let id = next.fetch_add(1, Ordering::Relaxed);
        if open_base(RequestId(id)) != self.base || is_floor_record_id(RequestId(id)) {
            tracing::error!(id, base = self.base, "ack_floor_ids_used_up");
            return None;
        }
        if self.enabled {
            self.in_flight.insert(id);
        }
        Some(id)
    }

    /// What the request `id` carries at `now`: the floor and the pins below it. Drops pins older
    /// than the retention first. `None` for an id this tracker is not holding in flight: an
    /// app-chosen id, or any id with the floor off.
    pub(crate) fn carried(&mut self, id: u64, now: Instant) -> Option<Carried> {
        if !self.in_flight.contains(&id) {
            return None;
        }
        let retention = self.retention;
        self.pins
            .retain(|(_, made)| now.saturating_duration_since(*made) < retention);
        let acked_below = self.in_flight.first().copied().unwrap_or(id).min(id);
        let pinned = self
            .pins
            .iter()
            .map(|(pin, _)| *pin)
            .filter(|pin| *pin < acked_below)
            .collect();
        Some(Carried {
            acked_below,
            pinned,
        })
    }

    /// `id` was answered at `now`: held, or a pin. A no-op for an id not in flight.
    pub(crate) fn finish(&mut self, id: u64, held: bool, now: Instant) {
        if !self.in_flight.remove(&id) {
            return;
        }
        if held {
            return;
        }
        self.pins.push((id, now));
        if self.pins.len() > MAX_PINS {
            let (evicted, made) = self.pins.remove(0);
            tracing::warn!(
                evicted,
                age_ms = u64::try_from(now.saturating_duration_since(made).as_millis())
                    .unwrap_or(u64::MAX),
                pins = MAX_PINS,
                "ack_floor_pin_evicted"
            );
        }
    }

    /// The app got a definitive answer for `id`, by a resend or a status query: it is no longer
    /// a pin.
    pub(crate) fn settle(&mut self, id: u64) {
        self.pins.retain(|(pin, _)| *pin != id);
    }

    /// Whether this open numbered `id` (A-R357): its `STATUS_EXPIRED` then means this `Db` was
    /// answered for it, not an earlier open of the same client.
    pub(crate) const fn numbered_here(&self, id: u64) -> bool {
        open_base(RequestId(id)) == self.base
    }

    /// The pins held, oldest first: what a row checks a settle against.
    #[cfg(test)]
    pub(crate) fn pins(&self) -> Vec<u64> {
        self.pins.iter().map(|(pin, _)| *pin).collect()
    }
}

#[cfg(test)]
mod tests {
    //! Unit checks of the tracker alone. The floor through a running `Db` is pinned by the
    //! `m10_s2_d*` rows in `db.rs`.

    use std::sync::atomic::AtomicU64;
    use std::time::{Duration, Instant};

    use super::{AckFloor, Carried, MAX_PINS};

    const DAY: Duration = Duration::from_secs(86_400);
    const MASK: u64 = (1 << 40) - 1;

    fn begun(floor: &mut AckFloor, next: &AtomicU64) -> u64 {
        floor.begin(next).expect("an id of this open")
    }

    #[test]
    fn the_floor_is_the_lowest_id_in_flight() {
        let next = AtomicU64::new(1);
        let mut floor = AckFloor::new(1, true, DAY);
        let now = Instant::now();
        let ids: Vec<_> = (0..4).map(|_| begun(&mut floor, &next)).collect();
        assert_eq!(ids, [1, 2, 3, 4]);
        floor.finish(1, true, now);
        floor.finish(3, true, now);
        let below = |floor: &mut AckFloor, id| floor.carried(id, now).expect("in flight");
        assert_eq!(below(&mut floor, 4).acked_below, 2, "2 is still in flight");
        floor.finish(2, true, now);
        assert_eq!(below(&mut floor, 4).acked_below, 4, "its own id at most");
        let id = begun(&mut floor, &next);
        assert_eq!(below(&mut floor, id).acked_below, 4);
        floor.finish(4, true, now);
        assert_eq!(floor.carried(4, now), None, "a finished id carries nothing");
    }

    #[test]
    fn a_pin_rides_below_the_floor_until_settled() {
        let next = AtomicU64::new(1);
        let mut floor = AckFloor::new(1, true, DAY);
        let now = Instant::now();
        for _ in 0..3 {
            begun(&mut floor, &next);
        }
        floor.finish(1, true, now);
        floor.finish(2, false, now);
        floor.finish(3, true, now);
        let id = begun(&mut floor, &next);
        let carried = Carried {
            acked_below: 4,
            pinned: vec![2],
        };
        assert_eq!(floor.carried(id, now), Some(carried));
        floor.settle(2);
        assert_eq!(floor.carried(id, now).expect("in flight").pinned, [0; 0]);
    }

    #[test]
    fn the_ninth_pin_evicts_the_oldest() {
        let next = AtomicU64::new(1);
        let mut floor = AckFloor::new(1, true, DAY);
        let now = Instant::now();
        let count = MAX_PINS as u64 + 1;
        for _ in 0..count {
            let id = begun(&mut floor, &next);
            floor.finish(id, false, now);
        }
        let id = begun(&mut floor, &next);
        let carried = floor.carried(id, now).expect("in flight");
        assert_eq!(carried.pinned.len(), MAX_PINS);
        assert!(!carried.pinned.contains(&1), "the oldest pin went");
        assert_eq!(carried.acked_below, count + 1);
    }

    #[test]
    fn a_pin_ages_out_with_the_retention() {
        let next = AtomicU64::new(1);
        let mut floor = AckFloor::new(1, true, Duration::from_secs(10));
        let start = Instant::now();
        let id = begun(&mut floor, &next);
        floor.finish(id, false, start);
        let id = begun(&mut floor, &next);
        let at = |floor: &mut AckFloor, secs| {
            floor
                .carried(id, start + Duration::from_secs(secs))
                .expect("in flight")
                .pinned
        };
        assert_eq!(at(&mut floor, 9), [1]);
        assert!(at(&mut floor, 10).is_empty());
    }

    #[test]
    fn with_the_floor_off_nothing_is_tracked_and_nothing_carried() {
        let next = AtomicU64::new(1);
        let mut floor = AckFloor::new(1, false, DAY);
        let now = Instant::now();
        let id = begun(&mut floor, &next);
        assert_eq!(floor.carried(id, now), None);
        floor.finish(id, false, now);
        let id = begun(&mut floor, &next);
        assert_eq!(floor.carried(id, now), None, "no pin either");
    }

    /// A2: the counter never hands out the open's reserved floor record id, nor the next open's
    /// first ids, whether the floor is on or off.
    #[test]
    fn the_open_s_ids_end_before_its_reserved_floor_record_id() {
        for enabled in [true, false] {
            let base = 2 << 40;
            let next = AtomicU64::new(base | (MASK - 1));
            let mut floor = AckFloor::new(base + 1, enabled, DAY);
            assert_eq!(floor.begin(&next), Some(base | (MASK - 1)), "the last id");
            assert_eq!(floor.begin(&next), None, "the reserved id");
            assert_eq!(floor.begin(&next), None, "the next open's base");
            assert_eq!(floor.begin(&next), None, "and every id after");
        }
    }
}
