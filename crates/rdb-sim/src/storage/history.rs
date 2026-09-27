//! Canonical history: the records a lineage holds, as T1 commits them and R1 reads them back.
//!
//! T1's commit (and R1's receiver stage) writes one batch per record: the envelope's mutations
//! (the user writes, then the request's `Dedup` row), then
//! `History{key: seq BE, value: the encoded envelope}`, then
//! `Progress{PROGRESS_KEY: seq LE ++ record_digest}`. [`canonical_history_from`] builds that batch
//! from T1's own public pieces — [`scoped_key`], [`TxnRequest::request_digest`], [`dedup_key`],
//! [`dedup_value`] and [`BATCH_TAG`] — rather than a copy of their layout, so a scenario that
//! preloads a survivor's history preloads bytes the `SendEnvelopes` provider can read and send
//! unchanged (lead ruling B-R57, rule 3; B-R57a F6). The row
//! `canonical_history_writes_what_t1_commits` holds it equal to a batch a live T1 commits.
//! [`verified_record`] is the provider's
//! read check (rule 2): the bytes decode, sit at the sequence asked for, carry a digest that
//! recomputes, and agree with the progress record committed beside them.

use bytes::Bytes;
use rdb_core::contracts::authority::Lineage;
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::envelope::{EnvelopeHeader, ReplicationEnvelope};
use rdb_core::contracts::ids::{
    AffinityId, BatchId, ClientId, ConfigVersion, LeaseId, RequestId, RequestIdentity, Seq,
    TenantId,
};
use rdb_core::contracts::storage::{Batch, Namespace, Write};
use rdb_core::contracts::txn::{scoped_key, Mutation, Outcome, TxnRequest};
use rdb_core::contracts::version::{API_VERSION, ENVELOPE_VERSION};
use rdb_core::replication::append::PROGRESS_KEY;
use rdb_core::transaction::dedup::{dedup_key, dedup_value};
use rdb_core::transaction::BATCH_TAG;

use crate::error::SimError;

/// A lineage's records `start + 1..=n`, chained from the digest it holds at `start`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalHistory {
    /// Where the chain starts: [`Seq::ZERO`] for a lineage cut at the root, the inherited cutoff
    /// for one that is not. Nothing at or below it is built here.
    pub start: Seq,
    /// One batch per record `start + 1..=n`, in sequence order, each written as T1's commit
    /// writes it.
    pub batches: Vec<Batch>,
    /// `digests[i]` is the record digest at sequence `start + i`; `digests[0]` is the chain start
    /// (the digest passed in, [`Digest::ROOT`] at the root). What a survivor's ladder and head
    /// report.
    pub digests: Vec<Digest>,
}

impl CanonicalHistory {
    /// The digest at `seq`: the chain start at `start`, [`Digest::ROOT`] at zero for a root chain.
    ///
    /// # Panics
    ///
    /// When `seq` is below `start` or past the history's head: a fixture asking for a record it
    /// did not build.
    #[must_use]
    pub fn digest(&self, seq: u64) -> Digest {
        let index = seq
            .checked_sub(self.start.0)
            .and_then(|index| usize::try_from(index).ok())
            .expect("a sequence at or above the chain start");
        self.digests[index]
    }

    /// The batch that writes record `seq`.
    ///
    /// # Panics
    ///
    /// When `seq` is at or below `start` or past the history's head, as [`Self::digest`].
    #[must_use]
    pub fn batch(&self, seq: u64) -> &Batch {
        let index = seq
            .checked_sub(self.start.0 + 1)
            .and_then(|index| usize::try_from(index).ok())
            .expect("a sequence above the chain start");
        &self.batches[index]
    }
}

/// Records `1..=n` of a lineage cut at the root: [`canonical_history_from`] chained from
/// `(Seq::ZERO, Digest::ROOT)`. Valid only for a lineage whose chain really starts there — a
/// first generation, or one recovered at cutoff 0. A lineage inherited at a cutoff above 0 takes
/// [`canonical_history_from`] with the digest it holds at that cutoff (B-R57b).
///
/// # Errors
///
/// As [`canonical_history_from`].
pub fn canonical_history(
    lineage: Lineage,
    config_version: ConfigVersion,
    n: u64,
) -> Result<CanonicalHistory, SimError> {
    canonical_history_from(lineage, config_version, (Seq::ZERO, Digest::ROOT), n)
}

/// Records `start.0 + 1..=n` of `lineage`, written under `config_version`, record `start.0 + 1`
/// chained from `start.1`. Record `s` is the batch T1 commits for request `s` of tenant 1,
/// client 1, affinity 1 — one put of `s` (big-endian) at `scoped_key(1, 1, "k")`, no conditions,
/// admitted under grant 1 — chained from the record before it. Deterministic: the same arguments
/// give the same bytes.
///
/// `start` is the chain start a live T1 at this lineage would hold: its recovery cutoff and the
/// digest there, which is the **predecessor's** record digest, not one of this lineage's own. A
/// lineage inherited at `c` passes `(Seq(c), predecessor.digest(c))`. The chain start is a stored
/// byte R1 checks: each record's `prev_digest` is compared with the receiver's head, so a wrong
/// start is refused as `DivergentHistory` (B-R57b; rows
/// `canonical_history_from_an_inherited_cutoff_is_taken_by_a_receiver_at_that_cutoff` and
/// `canonical_history_from_the_wrong_digest_is_refused_by_a_receiver_at_that_cutoff`).
///
/// One thing differs from a live T1, and no reader checks it. The batch id is [`BATCH_TAG`] with
/// the sequence as its counter and no boot or generation bytes: T1's `id_base` is private and
/// needs a boot this helper does not have. The only reader of a batch id in this crate is the
/// dispatcher's report of a kernel commit, which a preloaded batch never reaches.
///
/// # Errors
///
/// Whatever [`ReplicationEnvelope::compute_record_digest`] or [`ReplicationEnvelope::encode`]
/// returns; neither fails for records this small.
pub fn canonical_history_from(
    lineage: Lineage,
    config_version: ConfigVersion,
    start: (Seq, Digest),
    n: u64,
) -> Result<CanonicalHistory, SimError> {
    let mut batches = Vec::new();
    let (cutoff, cutoff_digest) = start;
    let mut digests = vec![cutoff_digest];
    let (tenant, affinity) = (TenantId(1), AffinityId(1));
    for seq in cutoff.0 + 1..=n {
        let prev_digest = *digests.last().unwrap_or(&cutoff_digest);
        let key = scoped_key(tenant, affinity, b"k");
        let value = Bytes::copy_from_slice(&seq.to_be_bytes());
        let request = TxnRequest {
            api_version: API_VERSION,
            identity: RequestIdentity {
                tenant,
                client: ClientId(1),
                request: RequestId(seq),
            },
            affinity,
            expected_generation: None,
            remaining_millis: 1_000,
            conditions: Vec::new(),
            mutations: vec![Mutation::Put {
                key: key.clone(),
                value: value.clone(),
                expected_version: None,
            }],
        };
        let request_digest = request.request_digest();
        // As T1 reserves it: the put as a user write, then the request's dedup row.
        let mutations = vec![
            Write {
                ns: Namespace::User,
                key,
                value: Some(value),
            },
            Write {
                ns: Namespace::Dedup,
                key: dedup_key(lineage.generation, affinity, request.identity),
                value: Some(dedup_value(request_digest, Seq(seq), lineage.owner_epoch)),
            },
        ];
        let mut envelope = ReplicationEnvelope {
            header: EnvelopeHeader {
                protocol_version: ENVELOPE_VERSION,
                partition: lineage.partition,
                generation: lineage.generation,
                config_version,
                owner_epoch: lineage.owner_epoch,
                seq: Seq(seq),
                body_len: 0,
            },
            lease_id: LeaseId(1),
            prev_digest,
            request_identity: request.identity,
            request_digest,
            conditions_result: Vec::new(),
            mutations,
            result: Outcome::Published,
            record_digest: Digest::ROOT,
        };
        envelope.record_digest = envelope.compute_record_digest()?;
        let record = envelope.encode()?;
        let mut writes = envelope.mutations.clone();
        writes.push(Write {
            ns: Namespace::History,
            key: Bytes::copy_from_slice(&seq.to_be_bytes()),
            value: Some(record),
        });
        writes.push(Write {
            ns: Namespace::Progress,
            key: Bytes::from_static(PROGRESS_KEY),
            value: Some(progress_value(Seq(seq), envelope.record_digest)),
        });
        batches.push(Batch {
            id: BatchId(BATCH_TAG | seq),
            partition: lineage.partition,
            generation: lineage.generation,
            seq: Seq(seq),
            writes,
        });
        digests.push(envelope.record_digest);
    }
    Ok(CanonicalHistory {
        start: cutoff,
        batches,
        digests,
    })
}

/// The `History` record at `seq` in `batch`, and the batch's `Progress` value, if it wrote them.
#[must_use]
pub fn history_writes(batch: &Batch, seq: Seq) -> Option<(Bytes, Option<Bytes>)> {
    let key = seq.0.to_be_bytes();
    let record = batch
        .writes
        .iter()
        .find(|write| write.ns == Namespace::History && write.key.as_ref() == key)?
        .value
        .clone()?;
    let progress = batch
        .writes
        .iter()
        .find(|write| write.ns == Namespace::Progress && write.key.as_ref() == PROGRESS_KEY)
        .and_then(|write| write.value.clone());
    Some((record, progress))
}

/// Why a stored record was not sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordFault {
    /// The bytes do not decode as an envelope.
    Undecodable,
    /// The envelope claims another sequence.
    WrongSeq,
    /// The carried digest does not recompute.
    DigestMismatch,
    /// The batch's progress record is missing or names another `(seq, digest)`.
    ProgressMismatch,
}

/// The stored `record` at `seq`, checked against itself and the `progress` value committed in
/// the same batch (lead ruling B-R57, rule 2). The bytes are returned unchanged.
///
/// # Errors
///
/// The first [`RecordFault`] found.
pub fn verified_record(
    seq: Seq,
    record: &Bytes,
    progress: Option<&Bytes>,
) -> Result<ReplicationEnvelope, RecordFault> {
    let envelope = ReplicationEnvelope::decode(record).map_err(|_| RecordFault::Undecodable)?;
    if envelope.header.seq != seq {
        return Err(RecordFault::WrongSeq);
    }
    match envelope.compute_record_digest() {
        Ok(digest) if digest == envelope.record_digest => {}
        _ => return Err(RecordFault::DigestMismatch),
    }
    if progress != Some(&progress_value(seq, envelope.record_digest)) {
        return Err(RecordFault::ProgressMismatch);
    }
    Ok(envelope)
}

/// The progress value T1 and R1 write: `seq` u64 LE, then the digest.
fn progress_value(seq: Seq, digest: Digest) -> Bytes {
    let mut value = seq.0.to_le_bytes().to_vec();
    value.extend_from_slice(&digest.0);
    Bytes::from(value)
}
