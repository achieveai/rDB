//! The in-memory engine.
//!
//! Holds three watermarks and never derives one from another (spec §6.1, and team kernel-b's
//! stop condition):
//!
//! | Watermark | Meaning | May a publication rest on it? |
//! |---|---|---|
//! | `received` | the highest sequence handed to the engine | no — diagnostic only |
//! | `buffered_applied` | applied to state, not yet synced | only for `BufferedOnTwo` |
//! | `durable` | synced, and only ever moved by a real sync | yes |
//!
//! `durable` is private and is written in exactly one place: a successful sync. That is what
//! makes [`StorageOp::FalseDurable`] a fault the engine cannot accidentally honour.
//!
//! # Seed state
//!
//! Signatures only; package M1 lands the engine.

use rdb_core::contracts::ids::{AppliedSeq, DurableSeq, Generation, PartitionId, ReceivedSeq};
use rdb_core::contracts::storage::{Batch, CapturedPrefix, DurablePrefix};

use crate::error::SimError;
use crate::storage::StorageOp;

/// One core set's data engine.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MemoryEngine;

impl MemoryEngine {
    /// An empty engine at sequence zero.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Apply a batch atomically and advance `buffered_applied`.
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] until package M1 lands the engine.
    pub fn commit(&mut self, _batch: Batch) -> Result<AppliedSeq, SimError> {
        Err(SimError::unavailable(
            "storage::memory::MemoryEngine::commit",
        ))
    }

    /// Sync the captured prefixes and report which are now durable.
    ///
    /// This is the **only** place `durable` moves, and the only place a [`DurableSeq`] is
    /// constructed. Spec §6.1's write-order rule lives here: the engine holds a
    /// per-engine write-order mutex across capture-then-sync, so two concurrent syncs cannot
    /// interleave and publish a prefix neither of them actually covered.
    ///
    /// A partial or failed sync moves nothing and returns nothing
    /// ([`rdb_core::contracts::trace::SyncOutcome`]).
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] until package M1 lands the engine.
    pub fn sync_wal_through(
        &mut self,
        _captured: Vec<CapturedPrefix>,
    ) -> Result<Vec<DurablePrefix>, SimError> {
        Err(SimError::unavailable(
            "storage::memory::MemoryEngine::sync_wal_through",
        ))
    }

    /// The highest sequence handed to the engine. Diagnostic; never an input to any decision.
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] until package M1 lands the engine.
    pub const fn received(&self, _partition: PartitionId) -> Result<ReceivedSeq, SimError> {
        Err(SimError::unavailable(
            "storage::memory::MemoryEngine::received",
        ))
    }

    /// The highest sequence applied to state but not necessarily synced.
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] until package M1 lands the engine.
    pub const fn buffered_applied(&self, _partition: PartitionId) -> Result<AppliedSeq, SimError> {
        Err(SimError::unavailable(
            "storage::memory::MemoryEngine::buffered_applied",
        ))
    }

    /// The highest synced sequence, as the engine knows it.
    ///
    /// Deliberately not the value a kernel module is allowed to publish on: a module publishes on
    /// a [`DurablePrefix`] it was handed in [`rdb_core::contracts::storage::StorageEvent::Flushed`].
    /// This accessor exists for the oracle and for [`crate::storage::crash_image::CrashImage`],
    /// which must compute what a host crash keeps.
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] until package M1 lands the engine.
    pub const fn durable(&self, _partition: PartitionId) -> Result<DurableSeq, SimError> {
        Err(SimError::unavailable(
            "storage::memory::MemoryEngine::durable",
        ))
    }

    /// Schedule a fault for this engine.
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] until package M1 lands the engine.
    pub const fn inject(&mut self, _op: StorageOp) -> Result<(), SimError> {
        Err(SimError::unavailable(
            "storage::memory::MemoryEngine::inject",
        ))
    }

    /// Read one record's current version, for condition evaluation.
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] until package M1 lands the engine.
    pub const fn version_of(
        &self,
        _partition: PartitionId,
        _generation: Generation,
        _key: &[u8],
    ) -> Result<Option<u64>, SimError> {
        Err(SimError::unavailable(
            "storage::memory::MemoryEngine::version_of",
        ))
    }
}
