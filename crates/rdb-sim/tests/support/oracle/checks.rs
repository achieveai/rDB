//! The ten checkers (design §2.3).
//!
//! Each is `observe` plus `finish` plus `armed`, and each reads the [`super::model::Model`] for
//! facts declared by **earlier** events only. None of them derives a protocol decision: every
//! clause compares one declared fact against another, or against the environment's own
//! declarations (topology, flushes).
//!
//! `armed()` is the checker's state **at the end of the fold**, not a latch (design §2.4, ruling
//! V-R20 (6)). A checker that armed and then disarmed reports `false`, and its per-seed verdict
//! is `Unavailable(NotArmed)`.

pub mod atomicity;
pub mod authority;
pub mod dedup;
pub mod lag;
pub mod lineage;
pub mod liveness;
pub mod loss;
pub mod publication;
pub mod version;

use rdb_core::contracts::ids::PartitionId;
use rdb_core::contracts::trace::TraceEvent;

use super::model::Model;
use super::{Invariant, Violation};

/// One invariant, folded left to right.
pub trait Checker {
    /// Which invariant this checker carries.
    fn invariant(&self) -> Invariant;

    /// Judge `event` against the facts of every event before it.
    ///
    /// # Errors
    ///
    /// The named clause, when it fires.
    fn observe(&mut self, model: &Model, event: &TraceEvent) -> Result<(), Violation>;

    /// End-of-trace obligations: a conflict that was never quarantined, a request that never
    /// reached a terminal outcome. Returns the partition the obligation is about.
    fn finish(&mut self, model: &Model) -> Option<(PartitionId, Violation)> {
        let _ = model;
        None
    }

    /// Whether the checker's arming situation holds **at the end of the fold**.
    fn armed(&self) -> bool;
}

/// Every checker, in [`Invariant::ALL`] order. The one place the registry is built, so a
/// checker added without an [`Invariant`] member fails to compile.
#[must_use]
pub fn registry() -> Vec<Box<dyn Checker>> {
    vec![
        Box::new(atomicity::Atomicity::default()),
        Box::new(publication::Publication::default()),
        Box::new(authority::Authority::default()),
        Box::new(lineage::Lineage::default()),
        Box::new(dedup::Dedup::default()),
        Box::new(loss::Loss::default()),
        Box::new(liveness::Liveness::default()),
        Box::new(liveness::Isolation::default()),
        Box::new(version::VersionCompat::default()),
        Box::new(lag::Lag::default()),
    ]
}
