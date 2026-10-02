//! A source that is still sending its advertised prefix (lead ruling B-R55, row M7B-96).
//!
//! F1 keeps its discovery window open while a source is making progress (team kernel-b
//! `design.md` §5.5), and learns of that progress only through
//! [`rdb_core::contracts::recovery::RecoveryEvent::TransferProgress`]. No landed component
//! produces one, so the scenario declares a [`TransferPlan`] and the harness plays it out:
//! F1's `QueryInventory` for that copy starts the transfer instead of answering with an
//! inventory, and each later step is an event the run loop pops like a timer fire. Progress
//! therefore comes from the simulated transfer, never from seeded events.
//!
//! The arithmetic lives here, apart from the dispatcher, so a row can read exactly what a plan
//! produces at each step without running anything.

use rdb_core::contracts::ids::{NodeId, Seq};
use rdb_core::contracts::membership::CopyId;
use rdb_core::contracts::time::Tick;

/// A scenario's transfer from one source copy, declared before the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransferPlan {
    /// The source copy F1 asks about.
    pub copy: CopyId,
    /// The node holding the source. The transfer stalls if it is crashed or not registered.
    pub holder: NodeId,
    /// The head the source advertises.
    pub advertised: Seq,
    /// What F1 has received when the query starts the transfer: the first report's
    /// `received_seq`.
    pub from: Seq,
    /// Records moved per step.
    pub per_step: u64,
    /// Logical milliseconds between steps. The first report goes out at the query's tick.
    pub step_millis: u64,
    /// No step at or after this tick moves anything: the source goes silent (it "stops at").
    pub stop_at: Option<Tick>,
    /// Nothing moves past this position: the source stalls once it has sent this far.
    pub stall_at: Option<Seq>,
}

/// What one step of a transfer does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// Report `received`, and step again `step_millis` later.
    Progress(Seq),
    /// Report `received`, which is the advertised head: the transfer is complete, and the
    /// source's inventory is answered now.
    Complete(Seq),
    /// Nothing moved: the source is silent from here on, and F1 sees no more progress.
    Stalled,
}

/// A transfer in flight.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Transfer {
    plan: TransferPlan,
    received: Seq,
    started: bool,
}

impl Transfer {
    /// A transfer that has not reported yet.
    #[must_use]
    pub const fn new(plan: TransferPlan) -> Self {
        Self {
            plan,
            received: plan.from,
            started: false,
        }
    }

    /// The plan this transfer plays out.
    #[must_use]
    pub const fn plan(&self) -> TransferPlan {
        self.plan
    }

    /// Where no step moves past: the advertised head, or the stall position if lower.
    fn cap(&self) -> Seq {
        self.plan.stall_at.map_or(self.plan.advertised, |stall| {
            stall.min(self.plan.advertised)
        })
    }

    /// Take the step due at `now`. The first step reports `from` unmoved: it is the
    /// advertisement. Every later one moves `per_step` records, up to the cap, unless the source
    /// has stopped or has nothing left it will send.
    pub fn step(&mut self, now: Tick) -> Step {
        if self.plan.stop_at.is_some_and(|stop| now >= stop) {
            return Step::Stalled;
        }
        if self.started {
            let moved = Seq(self
                .received
                .0
                .saturating_add(self.plan.per_step)
                .min(self.cap().0));
            if moved <= self.received {
                return Step::Stalled;
            }
            self.received = moved;
        }
        self.started = true;
        if self.received >= self.plan.advertised {
            Step::Complete(self.received)
        } else {
            Step::Progress(self.received)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan() -> TransferPlan {
        TransferPlan {
            copy: CopyId(2),
            holder: NodeId(3),
            advertised: Seq(150),
            from: Seq(100),
            per_step: 10,
            step_millis: 1_000,
            stop_at: None,
            stall_at: None,
        }
    }

    #[test]
    fn a_transfer_advertises_then_moves_per_step_until_complete() {
        let mut transfer = Transfer::new(plan());
        let steps: Vec<Step> = (0..6).map(|at| transfer.step(Tick(at * 1_000))).collect();
        assert_eq!(
            steps,
            vec![
                Step::Progress(Seq(100)),
                Step::Progress(Seq(110)),
                Step::Progress(Seq(120)),
                Step::Progress(Seq(130)),
                Step::Progress(Seq(140)),
                Step::Complete(Seq(150)),
            ]
        );
    }

    #[test]
    fn a_transfer_stops_at_its_tick_and_stalls_at_its_position() {
        let mut stopped = Transfer::new(TransferPlan {
            stop_at: Some(Tick(3_000)),
            ..plan()
        });
        let steps: Vec<Step> = [0, 1_000, 2_000, 3_000]
            .map(|at| stopped.step(Tick(at)))
            .to_vec();
        assert_eq!(
            steps,
            vec![
                Step::Progress(Seq(100)),
                Step::Progress(Seq(110)),
                Step::Progress(Seq(120)),
                Step::Stalled,
            ]
        );

        let mut stalled = Transfer::new(TransferPlan {
            stall_at: Some(Seq(115)),
            ..plan()
        });
        let steps: Vec<Step> = [0, 1_000, 2_000, 3_000]
            .map(|at| stalled.step(Tick(at)))
            .to_vec();
        assert_eq!(
            steps,
            vec![
                Step::Progress(Seq(100)),
                Step::Progress(Seq(110)),
                Step::Progress(Seq(115)),
                Step::Stalled,
            ]
        );
    }
}
