//! Planned delays on one kernel-to-kernel hop (P-3).
//!
//! A routed kernel fact is scheduled at the tick it was emitted (ruling B-R23: a hop is zero
//! ticks, plus only what a fault plan adds). This is that fault plan, for one hop: an
//! `AuthorityCheck` at one checkpoint, emitted on one node, reaches A1 `by_millis` later.
//!
//! It exists because P1 asks for its `Reply` recheck in the very step it publishes, so with
//! zero-tick hops no scenario can put anything between publication and the reply decision. A
//! delay on that hop is the smallest thing that can. Everything else about routing is unchanged:
//! the event is still marked routed, still offered to its named consumers first, and a decline
//! still stops the run.

use rdb_core::contracts::authority::Checkpoint;
use rdb_core::contracts::event::KernelEffect;
use rdb_core::contracts::ids::NodeId;

/// Hold every `AuthorityCheck` at `checkpoint` emitted on `node` back by `by_millis`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HopDelay {
    /// The node whose module asks.
    pub node: NodeId,
    /// Which checkpoint's check.
    pub checkpoint: Checkpoint,
    /// How long it spends on the hop.
    pub by_millis: u64,
}

/// How long `effect`, emitted on `node`, spends on its hop under `plan`: the largest matching
/// delay, or zero.
#[must_use]
pub fn delay(plan: &[HopDelay], node: NodeId, effect: &KernelEffect) -> u64 {
    let KernelEffect::AuthorityCheck { checkpoint, .. } = effect else {
        return 0;
    };
    plan.iter()
        .filter(|hop| hop.node == node && hop.checkpoint == *checkpoint)
        .map(|hop| hop.by_millis)
        .max()
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use rdb_core::contracts::authority::Lineage;
    use rdb_core::contracts::ids::{CorrelationId, Generation, OwnerEpoch, PartitionId};

    use super::*;

    fn check(checkpoint: Checkpoint) -> KernelEffect {
        KernelEffect::AuthorityCheck {
            checkpoint,
            lineage: Lineage {
                partition: PartitionId(1),
                generation: Generation(1),
                owner_epoch: OwnerEpoch(1),
            },
            correlation: CorrelationId(1),
        }
    }

    #[test]
    fn only_the_named_node_and_checkpoint_are_held() {
        let plan = [HopDelay {
            node: NodeId(2),
            checkpoint: Checkpoint::Reply,
            by_millis: 4_000,
        }];
        assert_eq!(delay(&plan, NodeId(2), &check(Checkpoint::Reply)), 4_000);
        assert_eq!(delay(&plan, NodeId(3), &check(Checkpoint::Reply)), 0);
        assert_eq!(delay(&plan, NodeId(2), &check(Checkpoint::Publication)), 0);
        assert_eq!(delay(&[], NodeId(2), &check(Checkpoint::Reply)), 0);
    }
}
