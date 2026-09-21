//! The fake single-record CAS control store, with a watch that is allowed to end badly.
//!
//! This stands in for rEtcd. It is a *fake*, not a certification: passing against it says nothing
//! about the real `ConfigStore` (spike §9). Its value is the opposite — it must be **no
//! friendlier** than the real surface, because a kernel that only works against a polite store is
//! a kernel that does not work.
//!
//! ADR-rdb-0008 §7, as amended by lead ruling A-R15 (2026-09-20), lists the hostile behaviours the
//! fake must be able to produce under scenario control:
//!
//! | Requirement | Where it lives |
//! |---|---|
//! | 1. a conflict hides the winning value | [`rdb_core::contracts::control::CasOutcome::Conflict`] carries `exists` and `current`, never a value |
//! | 2. `Unknown` is distinct from `Unavailable` and from `Conflict` | three separate `CasOutcome` variants; [`ControlOp::PlanCas`] forces any of them |
//! | 3. all five typed watch terminations, plus a progress tick | [`ControlOp::TerminateWatch`], [`ControlOp::EmitProgress`] |
//! | 4. ~~no silent gap~~ | **kernel-side assertion, not a fake property** (A-R15). [`ControlOp::EmitWatch`] delivers contiguously and a gap is only ever a termination, so "the kernel reloaded without being told to" is an assertion the oracle makes against the trace — the fake cannot enforce it and does not try |
//! | 5. ~~`Unavailable` inside a generous deadline~~ | **deleted** (A-R15) |
//! | 6. a coherent family read with a resumable `snapshot_revision` | [`ControlStore::snapshot_family`] |
//! | 7. a completion delivered arbitrarily late, after the grant expired | [`ControlOp::DelayCompletion`] |
//! | 8. a control effect that never completes at all | [`ControlOp::DropCompletion`] |
//!
//! 7 and 8 are the two that catch a kernel which treats "I asked" as "I have it". A grant renewal
//! whose CAS lands after the grant expired must not revive the grant, and an effect with no
//! completion must not leave a partition waiting for one — spec §7.2 fails closed on both.
//!
//! # Seed state
//!
//! Signatures only; package H1 lands the store.

use bytes::Bytes;
use rdb_core::contracts::control::{CasOutcome, ControlKey, ReadOutcome, WatchTermination};
use rdb_core::contracts::ids::{NodeId, Revision};

use crate::error::SimError;

/// An injectable control-plane fault, as a scenario writes it.
///
/// One enum, like [`crate::sim::network::NetworkOp`] and [`crate::storage::StorageOp`], so the
/// generator, the shrinker and the coverage matrix can enumerate and compare operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ControlOp {
    /// Force the outcome of the next CAS, whatever the record actually holds.
    ///
    /// The only way a scenario produces `Unknown`, which by construction cannot be derived from
    /// the store's own state — that is exactly what makes it unknown.
    PlanCas {
        /// What the next CAS will report.
        outcome: CasOutcome,
    },
    /// Deliver the pending watch changes to `node`, contiguously.
    EmitWatch {
        /// The watcher.
        node: NodeId,
    },
    /// Emit a progress tick carrying only the current revision: a cache-freshness watermark that
    /// conveys no authority and no record content.
    EmitProgress {
        /// The watcher.
        node: NodeId,
    },
    /// End `node`'s watch with a specific termination.
    TerminateWatch {
        /// The watcher.
        node: NodeId,
        /// Why it ended.
        termination: WatchTermination,
    },
    /// Compact history below `up_to`, so a watcher resuming from before it must terminate with
    /// [`WatchTermination::RevisionCompacted`].
    Compact {
        /// The lowest revision that will remain available.
        up_to: Revision,
    },
    /// Hold `node`'s next control completion back by `by_millis` of logical time.
    ///
    /// The completion still arrives, and it still reports what really happened — arbitrarily
    /// late. Long enough and the grant it belongs to has expired, which is the case spec §7.2
    /// fails closed on: the CAS committed, and the right it would have granted is gone.
    DelayCompletion {
        /// The waiting node.
        node: NodeId,
        /// How long to hold it.
        by_millis: u64,
    },
    /// Drop `node`'s next control completion entirely. It never arrives.
    ///
    /// Distinct from [`Self::PlanCas`] with `CasOutcome::Unknown`: there, the caller is *told*
    /// nothing is known. Here the caller is told nothing at all, and must still make progress off
    /// its own deadline rather than waiting forever.
    DropCompletion {
        /// The waiting node.
        node: NodeId,
    },
}

/// The fake control store.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ControlStore;

impl ControlStore {
    /// An empty store at revision zero.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Apply one scenario operation.
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] until package H1 lands the store.
    pub const fn inject(&mut self, _op: ControlOp) -> Result<(), SimError> {
        Err(SimError::unavailable("sim::control::ControlStore::inject"))
    }

    /// Compare and swap one record. `expected: None` means create-only.
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] until package H1 lands the store.
    pub fn cas(
        &mut self,
        _key: ControlKey,
        _expected: Option<Revision>,
        _value: Option<Bytes>,
    ) -> Result<CasOutcome, SimError> {
        Err(SimError::unavailable("sim::control::ControlStore::cas"))
    }

    /// Linearizable read of one record.
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] until package H1 lands the store.
    pub const fn get(&self, _key: ControlKey) -> Result<ReadOutcome, SimError> {
        Err(SimError::unavailable("sim::control::ControlStore::get"))
    }

    /// A coherent snapshot of one key family, with the revision a watch may resume after.
    ///
    /// The only sanctioned answer to a gap (spec §7.1): one operation, never a diff against
    /// remembered state. The returned revision is what makes reload-then-rewatch a closed loop
    /// rather than a race.
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] until package H1 lands the store.
    pub const fn snapshot_family(&self, _family: ControlKey) -> Result<Revision, SimError> {
        Err(SimError::unavailable(
            "sim::control::ControlStore::snapshot_family",
        ))
    }
}
