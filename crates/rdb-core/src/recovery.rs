//! Survivor inventory, compatible-longest-prefix selection and rebuild.
//!
//! Queries every reachable eligible survivor within the discovery window and records the unreachable ones before choosing a shorter prefix. Selection is by validated hash ancestry from the committed lineage root — never by sequence length, and never by combining independent key changes from two histories.
//!
//! **Owner:** team kernel-b, package F1. Specification: spec §8.
//!
//! # Seed state
//!
//! [`Recovery::step`] returns [`RdbError::Unavailable`] for every event. That is the contract for
//! an unwired capability (spike §8): explicit, never a fake success, never a panic. The owning
//! team replaces the body and adds its submodules; nothing outside this file needs to change,
//! because `lib.rs` already exports the module (spike §3: "C0 alone updates manifest features
//! and module exports").

use crate::contracts::errors::{Capability, RdbError};
use crate::contracts::event::{Effect, Event, Module, ModuleName, StepCtx};

/// Survivor inventory, compatible-longest-prefix selection and rebuild.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Recovery;

impl Recovery {
    /// A module holding no state yet.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Module for Recovery {
    fn name(&self) -> ModuleName {
        ModuleName::Recovery
    }

    fn step(&mut self, _ctx: &StepCtx<'_>, _event: &Event) -> Result<Vec<Effect>, RdbError> {
        Err(RdbError::unavailable(
            Capability::Recovery,
            "package F1 is not wired yet",
        ))
    }
}
