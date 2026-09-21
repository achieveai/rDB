//! The module registry, and the one honest answer a seed can give.
//!
//! Every kernel module is an [`rdb_core::contracts::event::Module`]. The dispatcher owns the six,
//! routes an event to them and collects their effects. Until a package is wired its `step`
//! returns [`rdb_core::contracts::errors::RdbError::unavailable`], and the dispatcher reports
//! that as [`CapabilityState::Unavailable`] rather than swallowing it.
//!
//! This is the spike §8 rule made concrete. An unwired seam is explicitly unavailable; it is
//! never `todo!()`, because a panic would take the campaign runner down with it and turn "not
//! built yet" into "the run crashed", and it is never a fake success, because a fake success is
//! indistinguishable from a passing implementation exactly when it matters most.

use rdb_core::contracts::errors::{ErrorKind, RdbError};
use rdb_core::contracts::event::{Effect, Event, Module, ModuleName, StepCtx};
use rdb_core::contracts::trace::CapabilityState;
use rdb_core::{
    authority::Authority, protection::Protection, publication::Publication, recovery::Recovery,
    replication::Replication, transaction::Transaction,
};

/// The six kernel modules, in a fixed order.
///
/// A struct of six named fields rather than a `Vec<Box<dyn Module>>`: the set is closed, the
/// order is part of the contract, and a fixed struct cannot be iterated in a surprising order.
/// It also costs no allocation and no dynamic dispatch.
#[derive(Debug, Default)]
pub struct Dispatcher {
    authority: Authority,
    transaction: Transaction,
    replication: Replication,
    publication: Publication,
    protection: Protection,
    recovery: Recovery,
}

impl Dispatcher {
    /// A dispatcher holding the six kernel modules.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            authority: Authority::new(),
            transaction: Transaction::new(),
            replication: Replication::new(),
            publication: Publication::new(),
            protection: Protection::new(),
            recovery: Recovery::new(),
        }
    }

    /// The modules in dispatch order.
    ///
    /// [`ModuleName::ALL`] is the same order, and is what the capability report walks.
    fn modules(&mut self) -> [&mut dyn Module; 6] {
        [
            &mut self.authority,
            &mut self.transaction,
            &mut self.replication,
            &mut self.publication,
            &mut self.protection,
            &mut self.recovery,
        ]
    }

    /// Offer `event` to `module` and return its effects.
    ///
    /// Routing — which module sees which event — belongs to package I1 and is not decided here.
    /// This method is the plumbing under it, and it is real today so that a module can be stepped
    /// the moment its package lands.
    ///
    /// # Errors
    ///
    /// Whatever the module returns, unchanged. An unwired module returns
    /// [`RdbError::unavailable`], and that error is propagated rather than absorbed.
    pub fn step(
        &mut self,
        module: ModuleName,
        ctx: &StepCtx<'_>,
        event: &Event,
    ) -> Result<Vec<Effect>, RdbError> {
        let index = ModuleName::ALL
            .iter()
            .position(|candidate| *candidate == module)
            .expect("ModuleName::ALL is exhaustive");
        self.modules()[index].step(ctx, event)
    }

    /// What each module reports about itself, in [`ModuleName::ALL`] order.
    ///
    /// Determined by stepping, not by a hand-maintained table: a module is `Wired` when its
    /// `step` stops answering `Unavailable`, so this report cannot drift from the code. The
    /// harness emits it as [`rdb_core::contracts::trace::TraceKind::Capability`] at trace start,
    /// which is what stops a green campaign over six unimplemented packages from looking like a
    /// passing one.
    #[must_use]
    pub fn capability_report(&mut self, ctx: &StepCtx<'_>, probe: &Event) -> [CapabilityState; 6] {
        let mut report = [CapabilityState::Wired; 6];
        for (slot, module) in report.iter_mut().zip(ModuleName::ALL) {
            *slot = match self.step(module, ctx, probe) {
                Err(error) if error.kind() == ErrorKind::Unavailable => {
                    CapabilityState::Unavailable
                }
                _ => CapabilityState::Wired,
            };
        }
        report
    }
}
