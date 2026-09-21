//! Conditions, mutations, atomic batches and retained request outcomes.
//!
//! Evaluates conditions serially against the published snapshot, assigns the next sequence, and emits **one** atomic batch carrying the user writes, the history record, the dedup record and the progress update together. Local application is never client success (spec §5.2 step 6).
//!
//! **Owner:** team kernel-a, package T1. Specification: spec §5.1, §5.2, §5.3.
//!
//! # Seed state
//!
//! [`Transaction::step`] returns [`RdbError::Unavailable`] for every event. That is the contract for
//! an unwired capability (spike §8): explicit, never a fake success, never a panic. The owning
//! team replaces the body and adds its submodules; nothing outside this file needs to change,
//! because `lib.rs` already exports the module (spike §3: "C0 alone updates manifest features
//! and module exports").

use crate::contracts::errors::{Capability, RdbError};
use crate::contracts::event::{Effect, Event, Module, ModuleName, StepCtx};

/// Conditions, mutations, atomic batches and retained request outcomes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Transaction;

impl Transaction {
    /// A module holding no state yet.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Module for Transaction {
    fn name(&self) -> ModuleName {
        ModuleName::Transaction
    }

    fn step(&mut self, _ctx: &StepCtx<'_>, _event: &Event) -> Result<Vec<Effect>, RdbError> {
        Err(RdbError::unavailable(
            Capability::Transaction,
            "package T1 is not wired yet",
        ))
    }
}
