//! The lineage check S0's `verify` runs: is the stored history a whole, chained prefix?
//!
//! Per record `1..=applied`: it exists, decodes as an envelope, claims its own sequence, carries
//! a digest that recomputes, and its `prev_digest` is the digest of the record before it, from
//! [`Digest::ROOT`] at sequence 0. Then: no `History` record exists above `applied`, and the
//! lineage's `Progress` value names the head — `seq` u64 LE, then the head digest
//! (`rdb_core::replication::append::PROGRESS_KEY`'s documented format).
//!
//! Progress is checked **at the head only** (critic F1, lead ruling L-R182j). RocksDB keeps only
//! the latest `PROGRESS_KEY` value, so a per-record progress check, as
//! `rdb_sim::storage::history::verified_record` does below the head, would fail every record
//! but the last on a correct engine.
//!
//! An inherited lineage (ADR-rdb-0010 decision 7) is checked the same way: its records at or
//! below `base` are read through the chain, so the walk still starts at sequence 0 and passes
//! through the parent's records, and its head `Progress` falls through to the parent's until the
//! lineage writes its own.

use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::envelope::ReplicationEnvelope;
use rdb_core::contracts::ids::{AppliedSeq, DurableSeq, Generation, PartitionId, Seq};
use rdb_core::contracts::storage::StorageFault;

use crate::engine::{RocksEngine, LOG_TARGET};

/// A lineage that passed [`verify_lineage`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VerifiedLineage {
    /// The partition.
    pub partition: PartitionId,
    /// The generation.
    pub generation: Generation,
    /// The applied watermark; every record `1..=applied` was checked.
    pub applied: AppliedSeq,
    /// The durable watermark, as the engine holds it. `open` already refused `durable > applied`.
    pub durable: DurableSeq,
    /// The head record's digest; [`Digest::ROOT`] for an empty lineage.
    pub head_digest: Digest,
}

/// Why a lineage failed [`verify_lineage`]. Each names the first sequence at fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LineageFault {
    /// No `History` record at a sequence at or below `applied`.
    #[error("no history record at {0:?}")]
    Missing(Seq),
    /// The record does not decode as an envelope.
    #[error("history record at {0:?} does not decode")]
    Undecodable(Seq),
    /// The record claims another sequence.
    #[error("history record at {at:?} claims {claims:?}")]
    WrongSeq {
        /// Where it is stored.
        at: Seq,
        /// What it claims.
        claims: Seq,
    },
    /// The carried digest does not recompute.
    #[error("history record at {0:?} carries a digest that does not recompute")]
    DigestMismatch(Seq),
    /// `prev_digest` is not the digest of the record before it.
    #[error("history record at {0:?} does not chain to the record before it")]
    BrokenChain(Seq),
    /// A `History` record exists above `applied`: a batch landed in part.
    #[error("history record at {0:?} is above applied")]
    AboveApplied(Seq),
    /// The `Progress` value does not name the head sequence and digest.
    #[error("progress does not name head {0:?} and its digest")]
    ProgressMismatch(Seq),
    /// A full copy is staged in this lineage and was never switched: its keys are partial
    /// (ADR-rdb-0010 decision 8; row #15).
    #[error("a full copy is staged here and was never switched")]
    CopyIncomplete,
    /// The engine could not read a record.
    #[error("read failed: {0:?}")]
    Read(StorageFault),
}

/// Check lineage `(partition, generation)` of `engine` as the module docs describe.
///
/// # Errors
///
/// The first [`LineageFault`] found, lowest sequence first.
pub fn verify_lineage(
    engine: &RocksEngine,
    partition: PartitionId,
    generation: Generation,
) -> Result<VerifiedLineage, LineageFault> {
    let applied = engine.buffered_applied(partition, generation);
    let result = check(engine, partition, generation, applied);
    match &result {
        Ok(verified) => tracing::info!(
            target: LOG_TARGET,
            partition = partition.0,
            generation = generation.0,
            applied = applied.0,
            durable = verified.durable.0,
            "lineage_verified"
        ),
        Err(fault) => tracing::error!(
            target: LOG_TARGET,
            partition = partition.0,
            generation = generation.0,
            applied = applied.0,
            fault = %fault,
            "lineage_verify_failed"
        ),
    }
    result
}

fn check(
    engine: &RocksEngine,
    partition: PartitionId,
    generation: Generation,
    applied: AppliedSeq,
) -> Result<VerifiedLineage, LineageFault> {
    if engine.staging(partition, generation).is_some() {
        return Err(LineageFault::CopyIncomplete);
    }
    let mut prev = Digest::ROOT;
    let mut head_progress = None;
    for seq in (1..=applied.0).map(Seq) {
        let (record, progress) = engine
            .history_at(partition, generation, seq)
            .map_err(LineageFault::Read)?
            .ok_or(LineageFault::Missing(seq))?;
        let envelope =
            ReplicationEnvelope::decode(&record).map_err(|_| LineageFault::Undecodable(seq))?;
        if envelope.header.seq != seq {
            return Err(LineageFault::WrongSeq {
                at: seq,
                claims: envelope.header.seq,
            });
        }
        match envelope.compute_record_digest() {
            Ok(digest) if digest == envelope.record_digest => {}
            _ => return Err(LineageFault::DigestMismatch(seq)),
        }
        if envelope.prev_digest != prev {
            return Err(LineageFault::BrokenChain(seq));
        }
        prev = envelope.record_digest;
        head_progress = progress;
    }
    let above = Seq(applied.0 + 1);
    if engine
        .history_at(partition, generation, above)
        .map_err(LineageFault::Read)?
        .is_some()
    {
        return Err(LineageFault::AboveApplied(above));
    }
    if applied.0 > 0 {
        let mut expected = applied.0.to_le_bytes().to_vec();
        expected.extend_from_slice(&prev.0);
        if head_progress.as_deref() != Some(expected.as_slice()) {
            return Err(LineageFault::ProgressMismatch(Seq(applied.0)));
        }
    }
    Ok(VerifiedLineage {
        partition,
        generation,
        applied,
        durable: engine.durable(partition, generation),
        head_digest: prev,
    })
}
