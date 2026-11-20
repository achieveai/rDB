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
//!
//! Snapshot installs (issue #61 S2) follow the host's sync points. An unfinished install comes
//! back as it was and [`CrashImage::reopen`] clears it, as the host's open does. A finished one
//! survives a host crash with `applied` at its barrier and `durable` untouched, because
//! `install_finish` is a synced write that moves no durable mark. A tail release is synced, so
//! every crash keeps it.
//!
//! Not modelled: the host's WAL is in order, and the sim's image is not. On the host an unsynced
//! `install_begin` is lost by a host crash **with its wipe**, so the old copy comes back; here the
//! wipe has already happened and the copy comes back empty. Likewise a surviving trim mark on the
//! host implies the commits before it survived, which the per-lineage cut here does not enforce.

use rdb_core::contracts::ids::{AppliedSeq, DurableSeq, Generation, NodeId, PartitionId, Seq};
use rdb_core::contracts::storage::{Batch, StorageFault};

use crate::error::SimError;
use crate::storage::memory::{Install, MemoryEngine, TrimFloor};

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
    /// Each inherited lineage's predecessor and surviving base (see [`MemoryEngine::inherit`]):
    /// the whole base after a process crash; after a host crash, no more of it than the
    /// predecessor had synced, because the base is the predecessor's batches and only those
    /// survive to be replayed.
    bases: Vec<(PartitionId, Generation, Generation, Seq)>,
    /// Each lineage's surviving trim mark (issue #61): the staged one after a process crash, and
    /// only the one a sync covered after a host crash, as the host's unsynced trim batch is
    /// lost with the WAL tail.
    floors: Vec<(PartitionId, Generation, TrimFloor)>,
    /// Each lineage's install, finished or not, and its tail release (issue #61).
    installs: Vec<(PartitionId, Generation, Option<Install>, Option<Seq>)>,
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
        let mut bases = Vec::new();
        let mut floors = Vec::new();
        let mut installs = Vec::new();
        for ((partition, generation), lineage) in engine.lineages() {
            let applied = match fault {
                StorageFault::ProcessCrash => lineage.applied,
                // The safe direction: applied is *lowered* to what a real sync covered. This
                // is a truncation, not a promotion, and it is the only place it is spelled.
                // A finished install was a synced write: its barrier survives though it moved
                // no durable mark.
                StorageFault::HostCrash => AppliedSeq(
                    lineage.durable.0.max(
                        lineage
                            .install
                            .as_ref()
                            .filter(|install| install.finished)
                            .map_or(0, |install| install.at.0),
                    ),
                ),
                StorageFault::WriteFailed | StorageFault::FlushFailed | StorageFault::Corrupt => {
                    return Err(SimError::Config { field: "fault" });
                }
            };
            if let Some(parent) = lineage.parent {
                let base = match fault {
                    StorageFault::HostCrash => {
                        Seq(lineage.base.0.min(engine.durable(*partition, parent).0))
                    }
                    _ => lineage.base,
                };
                bases.push((*partition, *generation, parent, base));
            }
            let floor = match fault {
                StorageFault::HostCrash => lineage.floor_synced,
                _ => lineage.floor,
            };
            if let Some(floor) = floor {
                floors.push((*partition, *generation, floor));
            }
            if lineage.install.is_some() || lineage.tail_release.is_some() {
                installs.push((
                    *partition,
                    *generation,
                    lineage.install.clone(),
                    lineage.tail_release,
                ));
            }
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
        Ok(Self {
            surviving,
            batches,
            bases,
            floors,
            installs,
        })
    }

    /// Reopen an engine from this image, for `node`.
    ///
    /// The engine that comes back has each lineage's `applied` and `durable` equal to what
    /// survived — never above either — and its records rebuilt by replaying the surviving
    /// batches in order. Nothing a lost batch wrote is visible. A lineage still installing is
    /// cleared, as the host's open clears it.
    #[must_use]
    pub fn reopen(&self, node: NodeId) -> MemoryEngine {
        let mut engine = MemoryEngine::new(node);
        for (partition, generation, install, tail_release) in &self.installs {
            engine.restore_install(*partition, *generation, install.as_ref(), *tail_release);
        }
        for batch in &self.batches {
            // Replaying a batch that committed before the crash cannot fail: no fault is
            // planned on a fresh engine, and `commit` only errs on a planned fault.
            let _ = engine.commit(batch.clone());
        }
        for (partition, generation, parent, base) in &self.bases {
            engine.restore_base(*partition, *generation, *parent, *base);
        }
        for prefix in &self.surviving {
            engine.restore_durable(prefix.partition, prefix.generation, prefix.durable);
        }
        for (partition, generation, floor) in &self.floors {
            engine.restore_floor(*partition, *generation, *floor);
        }
        engine.clear_installs();
        engine
    }
}
