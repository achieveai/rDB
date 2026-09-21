//! The publication barrier, reads, status and uncertain outcomes.
//!
//! Publication is a distinct local state transition: the required regular acknowledgement plus an authority recheck, and nothing else, may publish an applied prefix. Every reader — API, export, actor, diagnostic — acquires the barrier and sees the declared published prefix, never the raw applied one.
//!
//! **Owner:** team kernel-a, package P1. Specification: spec §5.3, §5.4.
//!
//! # Seed state
//!
//! [`Publication::step`] returns [`RdbError::Unavailable`] for every event. That is the contract for
//! an unwired capability (spike §8): explicit, never a fake success, never a panic. The owning
//! team replaces the body and adds its submodules; nothing outside this file needs to change,
//! because `lib.rs` already exports the module (spike §3: "C0 alone updates manifest features
//! and module exports").

use crate::contracts::errors::{Capability, RdbError};
use crate::contracts::event::{Effect, Event, Module, ModuleName, StepCtx};

/// The publication barrier, reads, status and uncertain outcomes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Publication;

impl Publication {
    /// A module holding no state yet.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Module for Publication {
    fn name(&self) -> ModuleName {
        ModuleName::Publication
    }

    fn step(&mut self, _ctx: &StepCtx<'_>, _event: &Event) -> Result<Vec<Effect>, RdbError> {
        Err(RdbError::unavailable(
            Capability::Publication,
            "package P1 is not wired yet",
        ))
    }
}
