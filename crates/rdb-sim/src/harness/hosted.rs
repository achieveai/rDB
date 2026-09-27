//! One kernel instance per node, or per node and partition, behind one [`Module`].
//!
//! A1 and F1 are written as the state of **one** node: `Authority` holds one grant and one
//! served lineage, and `Recovery` runs one partition's recovery. Until 2026-09-26 the dispatcher
//! held one of each for the whole cluster, so the second node's first event stepped the first
//! node's state. Nothing noticed because no run had two nodes; the spine does. L1 already had
//! its own table ([`crate::harness::protection::ProtectionTable`]) for the same reason.
//!
//! An instance is made the first time its key is offered an event, and kept. Keys are in a
//! `BTreeMap`, so iteration order is part of nothing but is still deterministic.

use std::collections::BTreeMap;

use rdb_core::contracts::errors::RdbError;
use rdb_core::contracts::event::{Effect, Event, Module, ModuleName, StepCtx};
use rdb_core::contracts::ids::{NodeId, PartitionId};
use rdb_core::contracts::trace::CapabilityState;

/// What one instance serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    /// One instance per node, for every partition: A1, whose grant is the node's.
    Node,
    /// One instance per `(node, partition)`: F1, whose recovery is one partition's.
    Partition,
}

/// A table of `M`, keyed by [`Scope`].
#[derive(Debug)]
pub struct Hosted<M> {
    scope: Scope,
    hosted: BTreeMap<(NodeId, PartitionId), M>,
}

impl<M: Module + Default> Hosted<M> {
    /// An empty table.
    #[must_use]
    pub const fn new(scope: Scope) -> Self {
        Self {
            scope,
            hosted: BTreeMap::new(),
        }
    }

    const fn key(&self, node: NodeId, partition: PartitionId) -> (NodeId, PartitionId) {
        match self.scope {
            Scope::Node => (node, PartitionId(0)),
            Scope::Partition => (node, partition),
        }
    }

    /// The instance serving `(node, partition)`, if it has been offered anything.
    #[must_use]
    pub fn get(&self, node: NodeId, partition: PartitionId) -> Option<&M> {
        self.hosted.get(&self.key(node, partition))
    }
}

impl<M: Module + Default> Module for Hosted<M> {
    fn name(&self) -> ModuleName {
        M::default().name()
    }

    /// The package's own answer, from a fresh instance: capability is a property of the build,
    /// not of any one node's state.
    fn capability(&self) -> CapabilityState {
        M::default().capability()
    }

    fn step(&mut self, ctx: &StepCtx<'_>, event: &Event) -> Result<Vec<Effect>, RdbError> {
        let key = self.key(ctx.node, ctx.partition);
        self.hosted.entry(key).or_default().step(ctx, event)
    }
}
