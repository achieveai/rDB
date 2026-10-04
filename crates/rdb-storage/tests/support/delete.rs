//! The chained request batches shared by the `rocks_scenario` example and the S1 tests.
//!
//! Included by path (`#[path = ".../tests/support/delete.rs"] mod delete;`) from both, so there
//! is one copy. `rdb_sim::storage::history` builds only puts of one canonical value; this builds
//! the delete of the same canonical key the same way, and (for the S1 value differential) a
//! batch for any one user mutation, such as a compiled document `Put`.

use bytes::Bytes;
use rdb_core::contracts::authority::Lineage;
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::envelope::{EnvelopeHeader, ReplicationEnvelope};
use rdb_core::contracts::errors::RdbError;
use rdb_core::contracts::ids::{
    AffinityId, BatchId, ClientId, ConfigVersion, LeaseId, RequestId, RequestIdentity, Seq,
    TenantId,
};
use rdb_core::contracts::storage::{Batch, Namespace, Write};
use rdb_core::contracts::txn::{
    scoped_key, Condition, ConditionOutcome, Mutation, Outcome, TxnRequest,
};
use rdb_core::contracts::version::{API_VERSION, ENVELOPE_VERSION};
use rdb_core::replication::append::PROGRESS_KEY;
use rdb_core::transaction::dedup::{dedup_key, dedup_value};
use rdb_core::transaction::BATCH_TAG;

/// The batch T1 commits for a request that deletes k, chained from `prev_digest`: the user
/// delete, the request's dedup row, the History record and Progress. Built as
/// `rdb_sim::storage::history::canonical_history_from` builds its puts.
pub fn delete_batch(lineage: Lineage, seq: Seq, prev_digest: Digest) -> Result<Batch, RdbError> {
    let key = scoped_key(TenantId(1), AffinityId(1), b"k");
    request_batch(
        lineage,
        seq,
        prev_digest,
        Vec::new(),
        Mutation::Delete {
            key,
            expected_version: None,
        },
    )
}

/// The batch T1 commits for a request of tenant 1, affinity 1 with these `conditions` (all
/// met) and this one user `mutation`, chained from `prev_digest`: the user write, the request's
/// dedup row, the History record and Progress.
#[allow(dead_code)] // the example builds deletes only
pub fn request_batch(
    lineage: Lineage,
    seq: Seq,
    prev_digest: Digest,
    conditions: Vec<Condition>,
    mutation: Mutation,
) -> Result<Batch, RdbError> {
    let (tenant, affinity) = (TenantId(1), AffinityId(1));
    let user = match &mutation {
        Mutation::Put { key, value, .. } => Write {
            ns: Namespace::User,
            key: key.clone(),
            value: Some(value.clone()),
        },
        Mutation::Delete { key, .. } => Write {
            ns: Namespace::User,
            key: key.clone(),
            value: None,
        },
    };
    let request = TxnRequest {
        api_version: API_VERSION,
        identity: RequestIdentity {
            tenant,
            client: ClientId(1),
            request: RequestId(seq.0),
        },
        affinity,
        expected_generation: None,
        remaining_millis: 1_000,
        conditions,
        mutations: vec![mutation],
    };
    let request_digest = request.request_digest();
    let mutations = vec![
        user,
        Write {
            ns: Namespace::Dedup,
            key: dedup_key(lineage.generation, affinity, request.identity),
            value: Some(dedup_value(request_digest, seq, lineage.owner_epoch)),
        },
    ];
    let mut envelope = ReplicationEnvelope {
        header: EnvelopeHeader {
            protocol_version: ENVELOPE_VERSION,
            partition: lineage.partition,
            generation: lineage.generation,
            config_version: ConfigVersion(1),
            owner_epoch: lineage.owner_epoch,
            seq,
            body_len: 0,
        },
        lease_id: LeaseId(1),
        prev_digest,
        request_identity: request.identity,
        request_digest,
        conditions_result: vec![ConditionOutcome::Met; request.conditions.len()],
        mutations,
        result: Outcome::Published,
        record_digest: Digest::ROOT,
    };
    envelope.record_digest = envelope.compute_record_digest()?;
    let record = envelope.encode()?;
    let mut progress = seq.0.to_le_bytes().to_vec();
    progress.extend_from_slice(&envelope.record_digest.0);
    let mut writes = envelope.mutations.clone();
    writes.push(Write {
        ns: Namespace::History,
        key: Bytes::copy_from_slice(&seq.0.to_be_bytes()),
        value: Some(record),
    });
    writes.push(Write {
        ns: Namespace::Progress,
        key: Bytes::from_static(PROGRESS_KEY),
        value: Some(Bytes::from(progress)),
    });
    Ok(Batch {
        id: BatchId(BATCH_TAG | seq.0),
        partition: lineage.partition,
        generation: lineage.generation,
        seq,
        writes,
    })
}
