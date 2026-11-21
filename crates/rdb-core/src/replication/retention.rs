//! Issue #61 S3 (design §3.1): the History retention floor `F`, computed by one pure function
//! that the host's trim duty and the simulator's twin both call.
//!
//! ```text
//! need = min( own_durable,
//!             transfer pins (S of each outbound transfer in flight),
//!             primary only: known(copy).durable over the counted copies )
//! keep = newest seq applied at or before now − history_keep        (RetentionSamples)
//! cap  = lowest seq with bytes(cap ..= head) ≤ history_budget_bytes (cap_seq)
//! F    = max( F_prev, min( max( min(need, keep), min(cap, own_durable) ), applied ) )
//! ```
//!
//! * **Counted copy:** a copy whose `known(copy).durable ≥ F_prev`. One below is uncounted and
//!   reported in [`RetentionFloor::behind`], for the caller to log once as
//!   [`HISTORY_COPY_BEHIND_FLOOR`]. It is then rebuilt by a snapshot (design §3.3).
//! * **The cap is hard.** It overrides a lagging member and a transfer pin. A pin `F` passes is
//!   reported in [`RetentionFloor::overran_pins`], for the caller to log as
//!   [`HISTORY_BUDGET_OVERRAN_PIN`] and restart that transfer.
//! * **No trim** while any [`RetentionHold`] but [`RetentionHold::Grace`] is true: the floor in
//!   force stays.
//! * **Grace stops the window, not the cap** (lead ruling R23). Under it `need` and `keep` move
//!   nothing and only `min(cap, own_durable)` can raise `F`. When the cap does, every copy whose
//!   durable it passed is reported in [`RetentionFloor::behind`] in that round, since a copy in
//!   grace has not acked yet and its `known` durable says nothing; when it does not, the round
//!   is [`RetentionDecision::Held`] with [`RetentionHold::Grace`].
//! * A secondary passes no copies: its `need` is `min(own_durable, pins)`, and it keeps the same
//!   window.
//!
//! Pure: no clock, no storage. The caller samples time into [`RetentionSamples`] and asks its
//! engine for the bytes [`cap_seq`] searches.

use std::collections::VecDeque;

use crate::contracts::ids::{DurableSeq, NodeId, Seq};
use crate::contracts::membership::CopyId;
use crate::contracts::time::Tick;

/// Logged once per copy that drops below the floor in force: it no longer holds `F` back.
pub const HISTORY_COPY_BEHIND_FLOOR: &str = "history_copy_behind_floor";

/// Logged when the budget cap moves `F` past a transfer pin: that transfer restarts, until
/// [`overrun_action`] gives up on the copy.
pub const HISTORY_BUDGET_OVERRAN_PIN: &str = "history_budget_overran_pin";

/// How many overruns in a row of one copy's transfer the source tries before it stops
/// re-offering (issue #61 S3-R4 part (b), owner ruling 2026-11-20, option 1).
pub const HISTORY_BUDGET_OVERRUN_ATTEMPTS: u32 = 3;

/// What a source does after the `consecutive`-th overrun in a row of one copy's transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverrunAction {
    /// End the transfer; the copy's next ask opens a new one.
    ReOffer,
    /// End the transfer and offer that copy no more: warn with `gave_up = true` and name it in
    /// `needs_operator` ([`needs_history_budget`]) until a caught-up release resets the count, or
    /// a raised budget or a new generation clears it (lead ruling R27).
    GiveUp,
}

/// The one seam for repeated overruns (owner ruling 2026-11-20, option 1): re-offer until
/// [`HISTORY_BUDGET_OVERRUN_ATTEMPTS`] overruns in a row, then give up. `consecutive` counts
/// the overrun just seen, so the first is 1. The caller keeps the count per (source, partition,
/// copy) and resets it on any round for that copy that is not an overrun: a caught-up release,
/// or a retention round that keeps the copy's open transfer within budget (lead ruling
/// 2026-11-21).
#[must_use]
pub const fn overrun_action(consecutive: u32) -> OverrunAction {
    if consecutive >= HISTORY_BUDGET_OVERRUN_ATTEMPTS {
        OverrunAction::GiveUp
    } else {
        OverrunAction::ReOffer
    }
}

/// `needs_operator`'s text for a copy whose transfers the budget overran
/// [`HISTORY_BUDGET_OVERRUN_ATTEMPTS`] times in a row: the copy, the source node, and the
/// remedy. The give-up clears itself when the budget in force rises above the one it was
/// recorded at, or the partition moves to a new generation (lead ruling R27), so the text says
/// the copy is retried then.
#[must_use]
pub fn needs_history_budget(copy: CopyId, node: NodeId, attempts: u32) -> String {
    format!(
        "copy {} cannot be rebuilt from node {}: the history budget overran its snapshot \
         transfer {attempts} times in a row; raise history budget or reduce writes, then the \
         copy is retried automatically",
        copy.0, node.0
    )
}

/// Logged when a source clears a give-up ([`OverrunAction::GiveUp`]) for a copy, with
/// `why` = `budget_raised` or `new_generation` (lead ruling R27): the copy's next ask is
/// offered again.
pub const HISTORY_BUDGET_GIVE_UP_CLEARED: &str = "history_budget_give_up_cleared";

/// Why the duty trims nothing this round (design §3.1, "No trim while"). The floor in force
/// stays.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionHold {
    /// The newest lineage is mid copy (`staging`).
    Staging,
    /// The newest lineage is mid snapshot install (`installing`).
    Installing,
    /// F1 on this node runs a recovery for the partition.
    Recovering,
    /// A landing is pending.
    LandingPending,
    /// This node answered an inventory and holds its pin (design §3.5).
    InventoryPinned,
    /// A primary built by `Recovered` or by an install, before every pinned copy has an admitted
    /// ack and before `max(history_keep, inventory_pin_millis)` has passed since the build
    /// (design §3.1, m2; lead ruling R23). The only hold the cap passes.
    Grace,
}

impl RetentionHold {
    /// The word logged for this hold.
    #[must_use]
    pub const fn reason(self) -> &'static str {
        match self {
            Self::Staging => "staging",
            Self::Installing => "installing",
            Self::Recovering => "recovering",
            Self::LandingPending => "landing_pending",
            Self::InventoryPinned => "inventory_pinned",
            Self::Grace => "grace",
        }
    }
}

/// What the trim duty knows about one partition on this node, as [`retention_floor`] reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionInput {
    /// `F_prev`: the floor in force (the engine's trim floor, 0 when never trimmed).
    pub prev: Seq,
    /// This copy's applied head. `F` never passes it.
    pub applied: Seq,
    /// This copy's durable head.
    pub own_durable: DurableSeq,
    /// `S` of each snapshot transfer this node serves for the partition.
    pub transfer_pins: Vec<Seq>,
    /// On a primary, `known(copy).durable` of each other copy in the tracker's active
    /// predicates. `None` on a secondary.
    pub copies: Option<Vec<(CopyId, DurableSeq)>>,
    /// The newest seq applied at or before `now − history_keep` ([`RetentionSamples::keep_seq`]).
    pub keep: Seq,
    /// The lowest seq whose History through the head fits the budget ([`cap_seq`]).
    pub cap: Seq,
    /// A reason to trim nothing this round, if the caller observed one.
    pub hold: Option<RetentionHold>,
}

/// The floor [`retention_floor`] computed, and what the caller owes for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionFloor {
    /// `F`: trim through it. Equal to `prev` when nothing moves.
    pub floor: Seq,
    /// Copies below `prev`, not counted ([`HISTORY_COPY_BEHIND_FLOOR`]).
    pub behind: Vec<CopyId>,
    /// Transfer pins below `floor`: the cap passed them ([`HISTORY_BUDGET_OVERRAN_PIN`]).
    pub overran_pins: Vec<Seq>,
}

/// What the duty does with one partition this round.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetentionDecision {
    /// Trim nothing; the floor in force stays.
    Held(RetentionHold),
    /// Trim through [`RetentionFloor::floor`] when it is above `prev`.
    Floor(RetentionFloor),
}

/// `F` for one partition on this node (module docs).
#[must_use]
pub fn retention_floor(input: &RetentionInput) -> RetentionDecision {
    let grace = input.hold == Some(RetentionHold::Grace);
    if let Some(hold) = input.hold.filter(|_| !grace) {
        return RetentionDecision::Held(hold);
    }
    let own = Seq(input.own_durable.0);
    if grace {
        let floor = input.prev.max(input.cap.min(own).min(input.applied));
        if floor == input.prev {
            return RetentionDecision::Held(RetentionHold::Grace);
        }
        return RetentionDecision::Floor(RetentionFloor {
            floor,
            behind: input
                .copies
                .iter()
                .flatten()
                .filter(|(_, durable)| Seq(durable.0) < floor)
                .map(|entry| entry.0)
                .collect(),
            overran_pins: input
                .transfer_pins
                .iter()
                .copied()
                .filter(|pin| *pin < floor)
                .collect(),
        });
    }
    let (counted, behind): (Vec<_>, Vec<_>) = input
        .copies
        .iter()
        .flatten()
        .partition(|(_, durable)| durable.0 >= input.prev.0);
    let need = counted
        .iter()
        .map(|(_, durable)| Seq(durable.0))
        .chain(input.transfer_pins.iter().copied())
        .fold(own, Seq::min);
    let window = need.min(input.keep);
    let capped = input.cap.min(own);
    let floor = input.prev.max(window.max(capped).min(input.applied));
    RetentionDecision::Floor(RetentionFloor {
        floor,
        behind: behind.into_iter().map(|entry| entry.0).collect(),
        overran_pins: input
            .transfer_pins
            .iter()
            .copied()
            .filter(|pin| *pin < floor)
            .collect(),
    })
}

/// The lowest seq `s` in `floor..=head` whose History `s..=head` takes at most `budget` bytes,
/// by binary search over `bytes_from(s)`, the engine's bytes of the records from `s` through
/// the head. `bytes_from` must not increase as `s` grows. `floor` when everything held fits;
/// `head` when even the head record alone does not, so the head is always kept. A `head` below
/// `floor` answers `floor`.
pub fn cap_seq(floor: Seq, head: Seq, budget: u64, mut bytes_from: impl FnMut(Seq) -> u64) -> Seq {
    if head <= floor || bytes_from(floor) <= budget {
        return floor;
    }
    // Invariant: bytes_from(lo) > budget, and hi is a fit or the head.
    let (mut lo, mut hi) = (floor.0, head.0);
    while hi - lo > 1 {
        let mid = lo + (hi - lo) / 2;
        if bytes_from(Seq(mid)) <= budget {
            hi = mid;
        } else {
            lo = mid;
        }
    }
    Seq(hi)
}

/// The applied head over time, for `keep` (design §3.1, `RetentionSamples`): one sample per duty
/// round that saw the head move, pruned to the window. Bounded by `history_keep` over the duty
/// period, never by the write rate.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RetentionSamples {
    samples: VecDeque<(Tick, Seq)>,
}

impl RetentionSamples {
    /// No samples: `keep` is 0 until the window has passed once.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that `applied` was the head at `now`, then drop every sample older than the newest
    /// one at or before `now − keep_millis`: no later [`Self::keep_seq`] with the same window
    /// reads them. A head that did not move adds nothing.
    ///
    /// # Errors
    ///
    /// `"tick_backwards"` when `now` is before the newest sample, and `"applied_backwards"` when
    /// `applied` is below it. Nothing is recorded.
    pub fn record(
        &mut self,
        now: Tick,
        applied: Seq,
        keep_millis: u64,
    ) -> Result<(), &'static str> {
        if let Some(&(at, seq)) = self.samples.back() {
            if now < at {
                return Err("tick_backwards");
            }
            if applied < seq {
                return Err("applied_backwards");
            }
        }
        if self.samples.back().is_none_or(|&(_, seq)| seq < applied) {
            self.samples.push_back((now, applied));
        }
        if let Some(cutoff) = now.0.checked_sub(keep_millis) {
            while self.samples.get(1).is_some_and(|&(at, _)| at.0 <= cutoff) {
                self.samples.pop_front();
            }
        }
        Ok(())
    }

    /// The newest seq applied at or before `now − keep_millis`, or 0 when no sample is that old.
    #[must_use]
    pub fn keep_seq(&self, now: Tick, keep_millis: u64) -> Seq {
        let Some(cutoff) = now.0.checked_sub(keep_millis) else {
            return Seq::ZERO;
        };
        self.samples
            .iter()
            .take_while(|(at, _)| at.0 <= cutoff)
            .last()
            .map_or(Seq::ZERO, |&(_, seq)| seq)
    }

    /// How many samples are held.
    #[must_use]
    pub fn len(&self) -> usize {
        self.samples.len()
    }

    /// Whether no sample is held.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }
}

#[cfg(test)]
mod tests {
    //! Unit checks of the pure retention floor (issue #61 S3, design §3.1). Not `M7*` rows, so
    //! no `#[retcd_test]`, as in `contracts::ignore`. Each test names the mutation that fails it.

    use super::{
        cap_seq, retention_floor, RetentionDecision, RetentionFloor, RetentionHold, RetentionInput,
        RetentionSamples,
    };
    use crate::contracts::ids::{DurableSeq, Seq};
    use crate::contracts::membership::CopyId;
    use crate::contracts::time::Tick;

    /// A primary at head 100, durable 100, floor 10, nothing pinned or lagging, a keep and a cap
    /// that bound nothing.
    fn input() -> RetentionInput {
        RetentionInput {
            prev: Seq(10),
            applied: Seq(100),
            own_durable: DurableSeq(100),
            transfer_pins: Vec::new(),
            copies: Some(vec![
                (CopyId(1), DurableSeq(100)),
                (CopyId(2), DurableSeq(100)),
            ]),
            keep: Seq(100),
            cap: Seq(0),
            hold: None,
        }
    }

    fn floor_of(input: &RetentionInput) -> RetentionFloor {
        match retention_floor(input) {
            RetentionDecision::Floor(floor) => floor,
            held @ RetentionDecision::Held(_) => panic!("expected a floor, got {held:?}"),
        }
    }

    /// `need` is the least of own durable, every pin and every counted copy. Red if any one
    /// term is left out of the min.
    #[test]
    fn need_is_the_least_of_own_durable_pins_and_counted_copies() {
        let mut lagging = input();
        lagging.copies = Some(vec![
            (CopyId(1), DurableSeq(40)),
            (CopyId(2), DurableSeq(90)),
        ]);
        assert_eq!(
            floor_of(&lagging).floor,
            Seq(40),
            "a lagging member holds F"
        );

        let mut pinned = input();
        pinned.transfer_pins = vec![Seq(70), Seq(30)];
        assert_eq!(floor_of(&pinned).floor, Seq(30), "the lowest pin holds F");

        let mut own = input();
        own.own_durable = DurableSeq(55);
        assert_eq!(floor_of(&own).floor, Seq(55), "own durable holds F");
    }

    /// `keep` bounds `F` below `need`. Red if `keep` is ignored.
    #[test]
    fn keep_bounds_the_floor() {
        let mut window = input();
        window.keep = Seq(60);
        assert_eq!(floor_of(&window).floor, Seq(60));
    }

    /// The cap overrides a lagging member and a pin, and names each pin it passed. Red if the
    /// cap is folded into `need`, or the overrun is not reported.
    #[test]
    fn the_cap_overrides_a_lagging_member_and_a_pin() {
        let mut over = input();
        over.copies = Some(vec![(CopyId(1), DurableSeq(20))]);
        over.transfer_pins = vec![Seq(30), Seq(90)];
        over.cap = Seq(80);
        assert_eq!(
            floor_of(&over),
            RetentionFloor {
                floor: Seq(80),
                behind: Vec::new(),
                overran_pins: vec![Seq(30)],
            }
        );

        let mut beyond = input();
        beyond.own_durable = DurableSeq(50);
        beyond.cap = Seq(80);
        assert_eq!(
            floor_of(&beyond).floor,
            Seq(50),
            "the cap never passes own durable"
        );
    }

    /// `F` never moves down, and never passes `applied`. Red if `prev` is dropped from the
    /// max, or the `applied` bound is.
    #[test]
    fn the_floor_never_moves_down_or_past_applied() {
        let mut low = input();
        low.prev = Seq(70);
        low.keep = Seq(20);
        assert_eq!(
            floor_of(&low).floor,
            Seq(70),
            "never below the floor in force"
        );

        let mut ahead = input();
        ahead.own_durable = DurableSeq(120);
        ahead.keep = Seq(120);
        ahead.copies = Some(Vec::new());
        assert_eq!(floor_of(&ahead).floor, Seq(100), "never past applied");
    }

    /// A copy below the floor in force is not counted and is named behind. Red if a behind copy
    /// still holds `F`, or is not reported.
    #[test]
    fn a_copy_below_the_floor_is_uncounted_and_named() {
        let mut behind = input();
        behind.prev = Seq(50);
        behind.copies = Some(vec![
            (CopyId(1), DurableSeq(5)),
            (CopyId(2), DurableSeq(80)),
        ]);
        assert_eq!(
            floor_of(&behind),
            RetentionFloor {
                floor: Seq(80),
                behind: vec![CopyId(1)],
                overran_pins: Vec::new(),
            }
        );
    }

    /// A secondary counts no copies: `need` is `min(own durable, pins)`. Red if `None` is read
    /// as "every copy at 0".
    #[test]
    fn a_secondary_counts_no_copies() {
        let mut secondary = input();
        secondary.copies = None;
        secondary.transfer_pins = vec![Seq(75)];
        assert_eq!(floor_of(&secondary).floor, Seq(75));
    }

    /// Every hold trims nothing while the cap binds nothing. Red if a hold is ignored.
    #[test]
    fn every_hold_trims_nothing() {
        for hold in [
            RetentionHold::Staging,
            RetentionHold::Installing,
            RetentionHold::Recovering,
            RetentionHold::LandingPending,
            RetentionHold::InventoryPinned,
            RetentionHold::Grace,
        ] {
            let mut held = input();
            held.hold = Some(hold);
            assert_eq!(retention_floor(&held), RetentionDecision::Held(hold));
        }
    }

    /// Grace freezes `need` and `keep`, but the cap still raises `F` (lead ruling R23), naming
    /// every copy and pin it passed in that round. Red if grace stops the cap, or if it lets the
    /// window move, or if the passed copies are not named.
    #[test]
    fn grace_stops_the_window_but_not_the_cap() {
        let mut grace = input();
        grace.hold = Some(RetentionHold::Grace);
        grace.copies = Some(vec![
            (CopyId(1), DurableSeq(0)),
            (CopyId(2), DurableSeq(90)),
        ]);
        assert_eq!(
            retention_floor(&grace),
            RetentionDecision::Held(RetentionHold::Grace),
            "the window would reach 90; grace holds it"
        );

        grace.transfer_pins = vec![Seq(50)];
        grace.cap = Seq(80);
        assert_eq!(
            retention_floor(&grace),
            RetentionDecision::Floor(RetentionFloor {
                floor: Seq(80),
                behind: vec![CopyId(1)],
                overran_pins: vec![Seq(50)],
            }),
            "the cap passes copy 1 and the pin, not copy 2"
        );
    }

    /// `cap_seq` finds the lowest seq whose bytes through the head fit, keeps everything that
    /// fits, and always keeps the head. 10 bytes per record, head 100. Red on an off-by-one in
    /// the search.
    #[test]
    fn cap_seq_finds_the_lowest_seq_that_fits() {
        let bytes = |from: Seq| (101 - from.0) * 10;
        assert_eq!(
            cap_seq(Seq(1), Seq(100), 205, bytes),
            Seq(81),
            "21 records = 210 > 205"
        );
        assert_eq!(cap_seq(Seq(1), Seq(100), 210, bytes), Seq(80));
        assert_eq!(
            cap_seq(Seq(1), Seq(100), 10_000, bytes),
            Seq(1),
            "everything fits"
        );
        assert_eq!(
            cap_seq(Seq(1), Seq(100), 5, bytes),
            Seq(100),
            "the head is kept"
        );
        assert_eq!(
            cap_seq(Seq(40), Seq(30), 0, bytes),
            Seq(40),
            "head below floor"
        );
    }

    /// `keep_seq` answers the newest head at or before `now − keep`, and `record` prunes to the
    /// window, adds nothing for a head that did not move, and refuses time or a head going
    /// back. Red if pruning drops the sample `keep_seq` reads, or never prunes.
    #[test]
    fn samples_answer_keep_and_stay_bounded() {
        let keep = 1_000;
        let mut samples = RetentionSamples::new();
        assert_eq!(
            samples.keep_seq(Tick(500), keep),
            Seq::ZERO,
            "no window yet"
        );
        for step in 0..100_u64 {
            samples
                .record(Tick(step * 100), Seq(step * 10), keep)
                .expect("forward");
        }
        // now 9_900: the newest sample at or before 8_900 is step 89.
        assert_eq!(samples.keep_seq(Tick(9_900), keep), Seq(890));
        assert!(
            samples.len() <= 12,
            "pruned to the window: {}",
            samples.len()
        );
        let held = samples.len();
        samples
            .record(Tick(9_950), Seq(990), keep)
            .expect("same head");
        assert_eq!(samples.len(), held, "a head that did not move adds nothing");
        assert_eq!(
            samples.record(Tick(9_000), Seq(995), keep),
            Err("tick_backwards")
        );
        assert_eq!(
            samples.record(Tick(9_960), Seq(5), keep),
            Err("applied_backwards")
        );
    }
}
