//! What survives a crash, as a value.
//!
//! Spike §6 requires the crash-before-commit and crash-after-commit boundaries to be *exact*, and
//! an exact boundary needs an inspectable answer to "what is still there". A crash that is only a
//! dropped process gives the oracle nothing to check.
//!
//! The rule, from spec §6.1, and the same rule [`CrashImage::reopen`] applies:
//!
//! - [`rdb_core::contracts::storage::StorageFault::ProcessCrash`] keeps everything applied,
//!   buffered or synced. The page cache lives in the operating system, not the process, so the
//!   reopened engine has the same `applied` and the same `durable` as before.
//! - [`rdb_core::contracts::storage::StorageFault::HostCrash`] keeps only what a real sync
//!   covered. Everything buffered-but-unsynced is gone — including anything a
//!   [`crate::storage::StorageOp::FalseDurable`] claimed — so the reopened engine has
//!   `applied == durable`.
//!
//! Both watermarks are carried (finding K-F-03). The seed carried only `durable`, and with the
//! watermarks given no conversion (ruling B-R13) the buffered state that survives a process crash
//! had no representation except by being relabelled durable — which turned every process crash
//! into a silent promotion and made an early acknowledgement pass.

use rdb_core::contracts::ids::{AppliedSeq, DurableSeq, Generation, NodeId, PartitionId};
use rdb_core::contracts::storage::{Batch, StorageFault};

use crate::error::SimError;
use crate::storage::memory::MemoryEngine;

/// What one lineage came back with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SurvivingPrefix {
    /// The partition.
    pub partition: PartitionId,
    /// The lineage.
    pub generation: Generation,
    /// The highest sequence a real sync covered. Never above what it was before the crash.
    pub durable: DurableSeq,
    /// The highest sequence still applied. Equal to what it was before a process crash; equal
    /// to `durable` after a host crash. Never above what it was before the crash.
    pub applied: AppliedSeq,
}

/// The state an engine comes back with.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CrashImage {
    /// Each lineage's surviving watermarks, in ascending `(partition, generation)` order.
    ///
    /// A `Vec` in a fixed order rather than a map: this is compared and digested, and no
    /// hash-ordered collection may sit on a trace path (team rules).
    pub surviving: Vec<SurvivingPrefix>,
    /// The batches that survived, in commit order: every batch at or below its lineage's
    /// surviving `applied`. What [`CrashImage::reopen`] replays.
    batches: Vec<Batch>,
}

impl CrashImage {
    /// Compute what `engine` keeps across `fault`.
    ///
    /// # Errors
    ///
    /// [`SimError::Config`] naming `fault` when `fault` is not a crash: what a `WriteFailed`
    /// keeps is not a question, and answering it would invent a crash the scenario did not ask
    /// for.
    pub fn of(engine: &MemoryEngine, fault: StorageFault) -> Result<Self, SimError> {
        let mut surviving = Vec::new();
        let mut batches = Vec::new();
        for ((partition, generation), lineage) in engine.lineages() {
            let applied = match fault {
                StorageFault::ProcessCrash => lineage.applied,
                // The safe direction: applied is *lowered* to what a real sync covered. This
                // is a truncation, not a promotion, and it is the only place it is spelled.
                StorageFault::HostCrash => AppliedSeq(lineage.durable.0),
                StorageFault::WriteFailed | StorageFault::FlushFailed | StorageFault::Corrupt => {
                    return Err(SimError::Config { field: "fault" });
                }
            };
            surviving.push(SurvivingPrefix {
                partition: *partition,
                generation: *generation,
                durable: lineage.durable,
                applied,
            });
            batches.extend(
                lineage
                    .batches
                    .iter()
                    .filter(|batch| batch.seq.0 <= applied.0)
                    .cloned(),
            );
        }
        Ok(Self { surviving, batches })
    }

    /// Reopen an engine from this image, for `node`.
    ///
    /// The engine that comes back has each lineage's `applied` and `durable` equal to what
    /// survived — never above either — and its records rebuilt by replaying the surviving
    /// batches in order. Nothing a lost batch wrote is visible.
    #[must_use]
    pub fn reopen(&self, node: NodeId) -> MemoryEngine {
        let mut engine = MemoryEngine::new(node);
        for batch in &self.batches {
            // Replaying a batch that committed before the crash cannot fail: no fault is
            // planned on a fresh engine, and `commit` only errs on a planned fault.
            let _ = engine.commit(batch.clone());
        }
        for prefix in &self.surviving {
            engine.restore_durable(prefix.partition, prefix.generation, prefix.durable);
        }
        engine
    }
}
