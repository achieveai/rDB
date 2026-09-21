//! Fenced grants, epochs and coherent watch resync.
//!
//! Holds the grant state machine and answers the four revalidation gates of spec §5.2 — admission, storage dispatch, publication and reply. Uncertainty is not a tie: when the bounded-clock comparison in [`crate::contracts::time::ControlTime::compare`] returns [`crate::contracts::time::ClockVerdict::Uncertain`], the answer is deny.
//!
//! **Owner:** team kernel-a, package A1. Specification: spec §7.2, §7.3.
//!
//! # Seed state
//!
//! [`Authority::step`] returns [`RdbError::Unavailable`] for every event. That is the contract for
//! an unwired capability (spike §8): explicit, never a fake success, never a panic. The owning
//! team replaces the body and adds its submodules; nothing outside this file needs to change,
//! because `lib.rs` already exports the module (spike §3: "C0 alone updates manifest features
//! and module exports").

use crate::contracts::errors::{Capability, RdbError};
use crate::contracts::event::{Effect, Event, Module, ModuleName, StepCtx};

/// Fenced grants, epochs and coherent watch resync.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Authority;

impl Authority {
    /// A module holding no state yet.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Module for Authority {
    fn name(&self) -> ModuleName {
        ModuleName::Authority
    }

    fn step(&mut self, _ctx: &StepCtx<'_>, _event: &Event) -> Result<Vec<Effect>, RdbError> {
        Err(RdbError::unavailable(
            Capability::Authority,
            "package A1 is not wired yet",
        ))
    }
}
