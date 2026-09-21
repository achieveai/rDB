//! Canonical append, ancestry validation, per-copy progress and catch-up.
//!
//! Validates epoch, configuration membership, predecessor digest and exact sequence before any mutation. Same sequence and same digest is idempotent; same sequence and a different digest quarantines the stream; a gap returns `NEED_PREFIX` and never a speculative out-of-order apply.
//!
//! **Owner:** team kernel-b, package R1. Specification: spec §6.1, §6.3.
//!
//! # Seed state
//!
//! [`Replication::step`] returns [`RdbError::Unavailable`] for every event. That is the contract for
//! an unwired capability (spike §8): explicit, never a fake success, never a panic. The owning
//! team replaces the body and adds its submodules; nothing outside this file needs to change,
//! because `lib.rs` already exports the module (spike §3: "C0 alone updates manifest features
//! and module exports").

use crate::contracts::errors::{Capability, RdbError};
use crate::contracts::event::{Effect, Event, Module, ModuleName, StepCtx};

/// Canonical append, ancestry validation, per-copy progress and catch-up.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Replication;

impl Replication {
    /// A module holding no state yet.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Module for Replication {
    fn name(&self) -> ModuleName {
        ModuleName::Replication
    }

    fn step(&mut self, _ctx: &StepCtx<'_>, _event: &Event) -> Result<Vec<Effect>, RdbError> {
        Err(RdbError::unavailable(
            Capability::Replication,
            "package R1 is not wired yet",
        ))
    }
}
