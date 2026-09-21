//! What survives a crash, as a value.
//!
//! Spike §6 requires the crash-before-commit and crash-after-commit boundaries to be *exact*, and
//! an exact boundary needs an inspectable answer to "what is still there". A crash that is only a
//! dropped process gives the oracle nothing to check.
//!
//! The rule, from spec §6.1:
//!
//! - [`rdb_core::contracts::storage::StorageFault::ProcessCrash`] keeps everything applied,
//!   buffered or synced. The page cache lives in the operating system, not the process.
//! - [`rdb_core::contracts::storage::StorageFault::HostCrash`] keeps only what a real sync
//!   covered. Everything buffered-but-unsynced is gone — including anything a
//!   [`crate::storage::StorageOp::FalseDurable`] claimed.
//!
//! # Seed state
//!
//! Signatures only; package M1 lands the image.

use rdb_core::contracts::ids::{DurableSeq, Generation, PartitionId};
use rdb_core::contracts::storage::StorageFault;

use crate::error::SimError;
use crate::storage::memory::MemoryEngine;

/// The state an engine comes back with.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CrashImage {
    /// The partition, lineage and the highest sequence that survived, in ascending order.
    ///
    /// A `Vec` in a fixed order rather than a map: this is compared and digested, and no
    /// hash-ordered collection may sit on a trace path (team rules).
    pub surviving: Vec<(PartitionId, Generation, DurableSeq)>,
}

impl CrashImage {
    /// Compute what `engine` keeps across `fault`.
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] until package M1 lands the image.
    pub const fn of(_engine: &MemoryEngine, _fault: StorageFault) -> Result<Self, SimError> {
        Err(SimError::unavailable(
            "storage::crash_image::CrashImage::of",
        ))
    }

    /// Reopen an engine from this image. The engine that comes back has a `durable` watermark
    /// equal to what survived and a `buffered_applied` equal to it — never above it.
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] until package M1 lands the image.
    pub const fn reopen(&self) -> Result<MemoryEngine, SimError> {
        Err(SimError::unavailable(
            "storage::crash_image::CrashImage::reopen",
        ))
    }
}
