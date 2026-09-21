//! Rows M7F-02, M7F-03 and M7F-04: the known-answer vectors for package C0's four codec
//! functions.
//!
//! These are *known-answer* tests, not round-trip tests. A round trip proves that this build
//! agrees with itself; a fixed hex string proves that this build agrees with the one that wrote
//! the vector, which is the property two nodes of different ages need. Both are here, and the
//! golden strings are the ones that fail when an encoding drifts.
//!
//! | Row | Claim |
//! |---|---|
//! | M7F-02 | `record_digest` chains `prev_digest`, binds the partition, is invariant under `protocol_version` and `lease_id` (F-R6), separates field boundaries; `request_digest` ignores the remaining deadline |
//! | M7F-03 | `ControlKey::encode`/`decode` for every spec §7.1 key family |
//! | M7F-04 | envelope round trip, golden bytes, and an unknown mandatory version refused before any body decode |
//!
//! No key or value bytes reach a log field here (team rules): the tracing fields are digests and
//! lengths.

use bytes::Bytes;
use config_log::retcd_test;
use rdb_core::contracts::control::ControlKey;
use rdb_core::contracts::digest::{Digest, Domain};
use rdb_core::contracts::envelope::{EnvelopeHeader, ReplicationEnvelope, ENVELOPE_MAGIC};
use rdb_core::contracts::errors::{ErrorKind, RdbError};
use rdb_core::contracts::ids::{
    AffinityId, ClientId, ConfigVersion, Generation, LeaseId, NodeId, OperationId, OwnerEpoch,
    PartitionId, RangeId, RequestId, RequestIdentity, Seq, TenantId,
};
use rdb_core::contracts::storage::{Namespace, Write};
use rdb_core::contracts::txn::{Condition, ConditionOutcome, Mutation, Outcome, TxnRequest};
use rdb_core::contracts::version::{VersionedArtifact, API_VERSION, ENVELOPE_VERSION};

// ---------------------------------------------------------------------------------------------
// Fixtures. Small, explicit, and built by hand so a vector can be read without running anything.
// ---------------------------------------------------------------------------------------------

/// One envelope at `seq`, chained to `prev`, with one whole-value put.
fn envelope(
    seq: u64,
    prev: Digest,
    key: &'static [u8],
    value: &'static [u8],
) -> ReplicationEnvelope {
    ReplicationEnvelope {
        header: EnvelopeHeader {
            protocol_version: ENVELOPE_VERSION,
            partition: PartitionId(7),
            generation: Generation(3),
            config_version: ConfigVersion(11),
            owner_epoch: OwnerEpoch(5),
            seq: Seq(seq),
            body_len: 0,
        },
        lease_id: LeaseId(42),
        prev_digest: prev,
        request_identity: RequestIdentity {
            tenant: TenantId(1),
            client: ClientId(2),
            request: RequestId(3),
        },
        request_digest: Digest([0xAB; 32]),
        conditions_result: vec![ConditionOutcome::Met],
        mutations: vec![Write {
            ns: Namespace::User,
            key: Bytes::from_static(key),
            value: Some(Bytes::from_static(value)),
        }],
        result: Outcome::Published,
        record_digest: Digest::ROOT,
    }
}

/// A request with no conditions and one put, at `remaining_millis`.
fn request(remaining_millis: u64) -> TxnRequest {
    TxnRequest {
        api_version: API_VERSION,
        identity: RequestIdentity {
            tenant: TenantId(1),
            client: ClientId(2),
            request: RequestId(3),
        },
        affinity: AffinityId(9),
        expected_generation: Some(Generation(3)),
        remaining_millis,
        conditions: vec![Condition::Present {
            key: Bytes::from_static(b"k"),
        }],
        mutations: vec![Mutation::Put {
            key: Bytes::from_static(b"k"),
            value: Bytes::from_static(b"v"),
            expected_version: Some(4),
        }],
    }
}

// ---------------------------------------------------------------------------------------------
// M7F-02 — the digest vectors
// ---------------------------------------------------------------------------------------------

/// Ruling B-R9: equal digest at equal sequence implies equal prefix.
///
/// Flip one byte in entry 1 and entry 2's digest must move, because entry 2 hashes entry 1's
/// digest as its `prev_digest`. Without the chain, entry 2 would be unchanged and recovery could
/// select across a real divergence.
#[retcd_test]
fn m7f_02_record_digest_chains_prev_digest() {
    let first = envelope(1, Digest::ROOT, b"k", b"v0");
    let first_digest = first.compute_record_digest().expect("entry 1 digest");
    let second = envelope(2, first_digest, b"k", b"v1");
    let second_digest = second.compute_record_digest().expect("entry 2 digest");

    // One byte flipped in entry 1, and nothing else touched.
    let flipped = envelope(1, Digest::ROOT, b"k", b"w0");
    let flipped_digest = flipped.compute_record_digest().expect("flipped entry 1");
    let second_after = envelope(2, flipped_digest, b"k", b"v1");
    let second_after_digest = second_after.compute_record_digest().expect("entry 2 again");

    tracing::info!(
        first = %first_digest,
        flipped = %flipped_digest,
        second = %second_digest,
        second_after = %second_after_digest,
        "m7f_02 chain vector"
    );

    assert_ne!(
        first_digest, flipped_digest,
        "entry 1 must feel its own byte"
    );
    assert_ne!(
        second_digest, second_after_digest,
        "entry 2 must feel a change in entry 1 through prev_digest"
    );
}

/// Finding K-B-08: unprefixed concatenation is collidable.
///
/// `key=b"ab" value=b"c"` and `key=b"a" value=b"bc"` concatenate to the same bytes. Every part of
/// the preimage is length-prefixed, so the two digests must differ.
#[retcd_test]
fn m7f_02_record_digest_separates_field_boundaries() {
    let left = envelope(1, Digest::ROOT, b"ab", b"c");
    let right = envelope(1, Digest::ROOT, b"a", b"bc");

    let left_digest = left.compute_record_digest().expect("left digest");
    let right_digest = right.compute_record_digest().expect("right digest");

    assert_ne!(
        left_digest, right_digest,
        "a different field split must give a different digest"
    );
}

/// Finding K-B-07: the preimage binds the partition.
///
/// `ProbeDigestReply` and `InventoryReply` carry raw `(seq, digest)` pairs that never pass the
/// append ladder, so nothing but the digest itself ties a ladder rung to its partition.
#[retcd_test]
fn m7f_02_record_digest_binds_partition() {
    let base = envelope(1, Digest::ROOT, b"k", b"v");
    let base_digest = base.compute_record_digest().expect("base digest");

    let mut other_partition = envelope(1, Digest::ROOT, b"k", b"v");
    other_partition.header.partition = PartitionId(8);

    assert_ne!(
        base_digest,
        other_partition
            .compute_record_digest()
            .expect("other partition"),
        "partition_id is part of the preimage (K-B-07)"
    );
}

/// Ruling F-R6 (findings K-F-01, K-F-02): `protocol_version` is out of the preimage.
///
/// A digest chain is compared across node ages. If the envelope's wire version were hashed, a
/// node that re-encoded a record under a newer wire format would compute a different digest for
/// the same history, and recovery would call an upgrade a divergence (design §4.8).
#[retcd_test]
fn m7f_02_record_digest_is_invariant_under_protocol_version() {
    let base = envelope(1, Digest::ROOT, b"k", b"v");
    let base_digest = base.compute_record_digest().expect("base digest");

    let mut other_version = envelope(1, Digest::ROOT, b"k", b"v");
    other_version.header.protocol_version = ENVELOPE_VERSION + 1;

    assert_eq!(
        base_digest,
        other_version
            .compute_record_digest()
            .expect("other version"),
        "protocol_version is framing, not history (F-R6)"
    );
}

/// Ruling F-R6 again: `lease_id` is out of the preimage.
///
/// The grant that produced an entry is a fact about the primary, not about the history. Two
/// secondaries that received one entry under two grant renewals hold one history, and a chain
/// that hashed the lease would report a divergence at every renewal (design §4.8). The epoch
/// stays in: an entry's `owner_epoch` is what the fence checks.
#[retcd_test]
fn m7f_02_record_digest_is_invariant_under_lease_id() {
    let base = envelope(1, Digest::ROOT, b"k", b"v");
    let base_digest = base.compute_record_digest().expect("base digest");

    let mut other_lease = envelope(1, Digest::ROOT, b"k", b"v");
    other_lease.lease_id = LeaseId(43);

    assert_eq!(
        base_digest,
        other_lease.compute_record_digest().expect("other lease"),
        "lease_id is the grant, not the history (F-R6)"
    );

    let mut other_epoch = envelope(1, Digest::ROOT, b"k", b"v");
    other_epoch.header.owner_epoch = OwnerEpoch(6);
    assert_ne!(
        base_digest,
        other_epoch.compute_record_digest().expect("other epoch"),
        "owner_epoch stays in the preimage"
    );
}

/// The `record_digest` field of the envelope itself is excluded, so a record can carry its own
/// digest without the digest depending on what it carries.
#[retcd_test]
fn m7f_02_record_digest_excludes_itself_and_body_len() {
    let base = envelope(1, Digest::ROOT, b"k", b"v");
    let expected = base.compute_record_digest().expect("base digest");

    let mut sealed = envelope(1, Digest::ROOT, b"k", b"v");
    sealed.record_digest = expected;
    sealed.header.body_len = 4_096;

    assert_eq!(
        sealed.compute_record_digest().expect("sealed digest"),
        expected,
        "record_digest and body_len are framing, not content"
    );
}

/// Ruling A-R18: the request digest is over the *semantic* request.
///
/// Two retries of one request carry different remaining deadlines by construction — the duration
/// shrinks as the request travels. If the deadline were in the preimage, every retry would look
/// like a new payload and dedup would report `REQUEST_ID_REUSE` for a correct client.
#[retcd_test]
fn m7f_02_request_digest_ignores_remaining_deadline() {
    let early = request(30_000).request_digest();
    let late = request(12).request_digest();

    tracing::info!(early = %early, late = %late, "m7f_02 request vector");

    assert_eq!(
        early, late,
        "the remaining deadline is not part of the payload"
    );
}

/// Ruling A-R18 again, the other half: the preimage covers `tenant`, `affinity_id`,
/// `conditions[]`, `mutations[]` and `api_version`, and nothing else.
///
/// `client_id` and `request_id` are the *identity* the digest is compared under, so folding them
/// in would make the comparison vacuous.
#[retcd_test]
fn m7f_02_request_digest_covers_the_semantic_fields_only() {
    let base = request(1_000).request_digest();

    let mut other_client = request(1_000);
    other_client.identity.client = ClientId(99);
    other_client.identity.request = RequestId(99);
    assert_eq!(
        base,
        other_client.request_digest(),
        "client and request id are the identity, not the payload"
    );

    let mut other_generation = request(1_000);
    other_generation.expected_generation = None;
    assert_eq!(
        base,
        other_generation.request_digest(),
        "expected_generation is an admission check, not the payload"
    );

    let mut other_tenant = request(1_000);
    other_tenant.identity.tenant = TenantId(2);
    assert_ne!(base, other_tenant.request_digest(), "tenant is covered");

    let mut other_affinity = request(1_000);
    other_affinity.affinity = AffinityId(10);
    assert_ne!(base, other_affinity.request_digest(), "affinity is covered");

    let mut other_condition = request(1_000);
    other_condition.conditions = vec![Condition::Absent {
        key: Bytes::from_static(b"k"),
    }];
    assert_ne!(
        base,
        other_condition.request_digest(),
        "conditions are covered"
    );

    let mut other_mutation = request(1_000);
    other_mutation.mutations = vec![Mutation::Delete {
        key: Bytes::from_static(b"k"),
        expected_version: Some(4),
    }];
    assert_ne!(
        base,
        other_mutation.request_digest(),
        "mutations are covered"
    );

    let mut other_api = request(1_000);
    other_api.api_version = 9;
    assert_ne!(base, other_api.request_digest(), "api_version is covered");
}

/// Domain separation: the same bytes in two domains are two digests.
#[retcd_test]
fn m7f_02_domains_do_not_collide() {
    let parts: [&[u8]; 2] = [b"a", b"b"];

    assert_ne!(
        Digest::of(Domain::Record, &parts),
        Digest::of(Domain::Request, &parts)
    );
    assert_ne!(
        Digest::of(Domain::Lineage, &parts),
        Digest::of(Domain::Checkpoint, &parts)
    );
}

/// The known answer proper: a fixed envelope hashes to a fixed 32 bytes, for ever.
///
/// This is the vector a second implementation, or a later refactor, is checked against. A change
/// here is a protocol change and must move [`ENVELOPE_VERSION`]. Re-pinned once, in correction
/// round 1, when ruling F-R6 took `protocol_version` and `lease_id` out of the preimage; the
/// envelope's golden bytes did not move, because the wire format did not.
#[retcd_test]
fn m7f_02_record_digest_golden() {
    let first = envelope(1, Digest::ROOT, b"k", b"v0");
    let first_digest = first.compute_record_digest().expect("entry 1 digest");
    let second = envelope(2, first_digest, b"k", b"v1");
    let second_digest = second.compute_record_digest().expect("entry 2 digest");

    assert_eq!(
        first_digest.to_hex(),
        "0d31b22c883fd531d0b037044aebbf9f577d2b55a6633eee612fbfe75ee429f0",
        "entry 1 golden"
    );
    assert_eq!(
        second_digest.to_hex(),
        "cf81114353a593748af1e9d5eba6a445636912c2fe4f4de2828e4d9a761c3358",
        "entry 2 golden"
    );
}

/// The request digest has a golden too, for the same reason.
#[retcd_test]
fn m7f_02_request_digest_golden() {
    assert_eq!(
        request(30_000).request_digest().to_hex(),
        "be896f14c9b2711f1b67c24883a849f6966ab421560fa666fa95e159eb3e3c98",
    );
}

// ---------------------------------------------------------------------------------------------
// M7F-03 — the control key vectors
// ---------------------------------------------------------------------------------------------

/// Every spec §7.1 key family, spelled out. A typo here is a key family that silently moves.
#[retcd_test]
fn m7f_03_control_key_encode_vectors() {
    let vectors = [
        (ControlKey::ClusterSchema, "cluster/schema"),
        (ControlKey::Node(NodeId(0)), "nodes/0"),
        (ControlKey::Node(NodeId(4_294_967_295)), "nodes/4294967295"),
        (ControlKey::Grant(NodeId(7)), "grants/7"),
        (ControlKey::Partition(PartitionId(12)), "partitions/12"),
        (ControlKey::Route(RangeId(3)), "routes/3"),
        (ControlKey::Operation(OperationId(99)), "operations/99"),
        (ControlKey::PlannerGrant, "planner/grant"),
    ];

    for (key, expected) in vectors {
        assert_eq!(key.encode(), expected, "{key:?}");
    }
}

/// Encode then decode is the identity on every family.
#[retcd_test]
fn m7f_03_control_key_round_trips() {
    let keys = [
        ControlKey::ClusterSchema,
        ControlKey::Node(NodeId(1)),
        ControlKey::Grant(NodeId(2)),
        ControlKey::Partition(PartitionId(3)),
        ControlKey::Route(RangeId(4)),
        ControlKey::Operation(OperationId(5)),
        ControlKey::PlannerGrant,
    ];

    for key in keys {
        assert_eq!(ControlKey::decode(&key.encode()), Ok(key), "{key:?}");
    }
}

/// No two families share an encoding, and no family's encoding is a prefix trap for another.
#[retcd_test]
fn m7f_03_control_key_families_are_distinct() {
    let encoded = [
        ControlKey::ClusterSchema.encode(),
        ControlKey::Node(NodeId(1)).encode(),
        ControlKey::Grant(NodeId(1)).encode(),
        ControlKey::Partition(PartitionId(1)).encode(),
        ControlKey::Route(RangeId(1)).encode(),
        ControlKey::Operation(OperationId(1)).encode(),
        ControlKey::PlannerGrant.encode(),
    ];

    let mut sorted = encoded.to_vec();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), encoded.len(), "two families share a key");
}

/// A key this build does not recognise is a typed refusal, never a guess.
#[retcd_test]
fn m7f_03_control_key_decode_refuses_anything_else() {
    for bad in [
        "",
        "cluster/schema/extra",
        "nodes",
        "nodes/",
        "nodes/x",
        "nodes/-1",
        "nodes/4294967296",
        "nodes/1/2",
        "planner/grants",
        "unknown/1",
        " nodes/1",
    ] {
        let error = ControlKey::decode(bad).expect_err("must refuse {bad}");
        assert_eq!(error.kind(), ErrorKind::InvalidArgument, "{bad:?}");
    }
}

// ---------------------------------------------------------------------------------------------
// M7F-04 — the envelope vectors
// ---------------------------------------------------------------------------------------------

/// Encode, decode, and get the same envelope back — including the `body_len` the encoder
/// computed.
#[retcd_test]
fn m7f_04_envelope_round_trips() {
    let mut original = envelope(1, Digest::ROOT, b"k", b"v");
    original.conditions_result = vec![ConditionOutcome::Met, ConditionOutcome::NotMet];
    original.mutations = vec![
        Write {
            ns: Namespace::User,
            key: Bytes::from_static(b"k"),
            value: Some(Bytes::from_static(b"v")),
        },
        Write {
            ns: Namespace::History,
            key: Bytes::from_static(b"h"),
            value: None,
        },
    ];
    original.result = Outcome::RecoveredApplied;
    original.record_digest = original.compute_record_digest().expect("digest");

    let bytes = original.encode().expect("encode");
    let decoded = ReplicationEnvelope::decode(&bytes).expect("decode");

    // `body_len` is framing: the encoder computes it, so the fixture cannot carry it.
    assert_eq!(
        usize::try_from(decoded.header.body_len).expect("fits"),
        bytes.len() - 46
    );
    original.header.body_len = decoded.header.body_len;
    assert_eq!(decoded, original);
    assert_eq!(
        decoded.compute_record_digest().expect("recompute"),
        original.record_digest,
        "the digest survives the wire"
    );
}

/// An empty envelope — no conditions, no mutations — still encodes and decodes.
#[retcd_test]
fn m7f_04_envelope_round_trips_when_empty() {
    let mut original = envelope(1, Digest::ROOT, b"k", b"v");
    original.conditions_result = Vec::new();
    original.mutations = Vec::new();

    let bytes = original.encode().expect("encode");
    let decoded = ReplicationEnvelope::decode(&bytes).expect("decode");
    original.header.body_len = decoded.header.body_len;

    assert_eq!(decoded, original);
}

/// The golden bytes. Fixed layout, no floats, no maps, per rEtcd ADR-0007's discipline.
#[retcd_test]
fn m7f_04_envelope_golden_bytes() {
    let original = envelope(1, Digest::ROOT, b"k", b"v");
    let bytes = original.encode().expect("encode");

    assert_eq!(&bytes[..4], ENVELOPE_MAGIC);
    assert_eq!(
        hex::encode(&bytes),
        concat!(
            // header: magic, version 1, partition 7, generation 3, config 11, epoch 5, seq 1,
            // body_len 142
            "52444245",
            "0100",
            "07000000",
            "0300000000000000",
            "0b00000000000000",
            "0500000000000000",
            "0100000000000000",
            "8e000000",
            // body: lease 42, prev_digest ROOT
            "2a00000000000000",
            "0000000000000000000000000000000000000000000000000000000000000000",
            // identity: tenant 1, client 2, request 3; then request_digest
            "01000000",
            "02000000",
            "0300000000000000",
            "abababababababababababababababababababababababababababababababab",
            // conditions: one Met
            "01000000",
            "01",
            // mutations: one put, namespace User, key "k", value "v"
            "01000000",
            "01",
            "01000000",
            "6b",
            "01",
            "01000000",
            "76",
            // result Published, then record_digest as carried (ROOT in this fixture)
            "01",
            "0000000000000000000000000000000000000000000000000000000000000000",
        )
    );
}

/// The charter row: an unknown **mandatory** version is refused before any body decode.
///
/// The body here is deliberate rubbish — one byte where a 154-byte body belongs. If the decoder
/// touched the body first it would say `InvalidArgument`; refusing on the version is what proves
/// the ordering, and the error names the version it found.
#[retcd_test]
fn m7f_04_unknown_mandatory_version_is_refused_before_body_decode() {
    let good = envelope(1, Digest::ROOT, b"k", b"v")
        .encode()
        .expect("encode");

    let mut bumped = good[..46].to_vec();
    bumped[4..6].copy_from_slice(&(ENVELOPE_VERSION + 1).to_le_bytes());
    bumped.push(0xFF);

    for decoded in [
        ReplicationEnvelope::decode(&bumped).map(|_| ()),
        ReplicationEnvelope::decode_header(&bumped).map(|_| ()),
    ] {
        let error = decoded.expect_err("an unknown mandatory version must be refused");
        assert_eq!(
            error,
            RdbError::IncompatibleVersion {
                artifact: VersionedArtifact::Envelope,
                found: ENVELOPE_VERSION + 1,
                min: ENVELOPE_VERSION,
                max: ENVELOPE_VERSION,
            },
            "the refusal names the version"
        );
    }
}

/// The header is readable on its own, which is what makes "before the body" implementable.
#[retcd_test]
fn m7f_04_decode_header_reads_the_prefix_alone() {
    let original = envelope(9, Digest::ROOT, b"k", b"v");
    let bytes = original.encode().expect("encode");

    let header = ReplicationEnvelope::decode_header(&bytes[..46]).expect("header alone");

    assert_eq!(header.seq, Seq(9));
    assert_eq!(header.partition, PartitionId(7));
    assert_eq!(header.generation, Generation(3));
    assert_eq!(header.owner_epoch, OwnerEpoch(5));
    assert_eq!(header.config_version, ConfigVersion(11));
    assert_eq!(
        usize::try_from(header.body_len).expect("fits"),
        bytes.len() - 46
    );
}

/// No slack, and no foreign frame. Both are `InvalidArgument`, naming the field.
#[retcd_test]
fn m7f_04_decode_refuses_a_foreign_frame_and_trailing_bytes() {
    let good = envelope(1, Digest::ROOT, b"k", b"v")
        .encode()
        .expect("encode");

    let mut foreign = good.to_vec();
    foreign[0] = b'X';
    assert_eq!(
        ReplicationEnvelope::decode_header(&foreign)
            .expect_err("foreign magic")
            .kind(),
        ErrorKind::InvalidArgument
    );

    let mut trailing = good.to_vec();
    trailing.push(0);
    assert_eq!(
        ReplicationEnvelope::decode(&trailing)
            .expect_err("trailing bytes")
            .kind(),
        ErrorKind::InvalidArgument
    );

    let truncated = &good[..good.len() - 1];
    assert_eq!(
        ReplicationEnvelope::decode(truncated)
            .expect_err("truncated body")
            .kind(),
        ErrorKind::InvalidArgument
    );

    assert_eq!(
        ReplicationEnvelope::decode_header(&good[..10])
            .expect_err("truncated header")
            .kind(),
        ErrorKind::InvalidArgument
    );
}
