//! M5-74, M5-95..M5-108 (state-machine half) — bounded request deduplication and node
//! retirement in the pure core (ADR-0025, ADR-0023, spec §8.2, §19.5).
//!
//! Every row here runs against a bare [`KvState`]: no store, no transport, no clock. That is
//! deliberate. Deduplication decides whether a resubmission is a hit, a fresh application, or
//! a refusal, and that decision is made inside `apply` on *every* voter — so it has to be
//! provable without anything a node could hold locally.

mod common;

use config_core::{
    ApplyEffects, Command, CommandResponse, DecodeError, DedupKey, DedupLimits, DedupStamp,
    DeleteRequest, KvState, Limits, MutationOutcome, MutationResponse, NodeId, PutRequest,
    COMMAND_ENVELOPE_VERSION, COMMAND_MAGIC, OP_COMPACT, OP_RETIRE_NODE,
};

use common::b;

/// A principal hash that is visibly not the zero hash, so a test that forgets to bind one
/// fails instead of accidentally matching.
const ALICE: [u8; 32] = [0x11; 32];
const BOB: [u8; 32] = [0x22; 32];
const CLIENT: [u8; 16] = [0xab; 16];

fn limits_with_dedup(window_requests: u32, max_records: u64) -> Limits {
    Limits {
        dedup: DedupLimits {
            enabled: true,
            window_requests,
            max_records,
        },
        ..Limits::DEFAULT
    }
}

/// A `Put` carrying `request_id` under [`CLIENT`], unbound — as it leaves a client.
fn put_dedup(key: &[u8], value: &[u8], request_id: u64) -> Command {
    Command::Put {
        key: b(key),
        value: b(value),
        expected_mod_revision: None,
        dedup: Some(DedupKey::new(CLIENT, request_id).stamp([0u8; 32])),
    }
}

fn hex_of(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// M5-74 (encoder half): the three M5 envelope shapes, byte for byte.
///
/// Hand-written from the layout in `command`'s module docs rather than captured from the
/// encoder, so a refactor that changes the layout fails here instead of re-recording itself.
#[config_log::retcd_test]
fn m5_74_golden_bytes_for_the_m5_envelope_shapes() {
    let dedup_put = Command::Put {
        key: b(b"a"),
        value: b(b"b"),
        expected_mod_revision: None,
        dedup: Some(DedupKey::new([0x22; 16], 7).stamp([0x11; 32])),
    };
    let expected = format!(
        "52 43 4d 44 02 00 01 01 00 00 00 61 01 00 00 00 62 00 00 00 00 00 00 00 00 00 01 {} {} 07 00 00 00 00 00 00 00",
        vec!["11"; 32].join(" "),
        vec!["22"; 16].join(" "),
    );
    let bytes = dedup_put.encode();
    assert_eq!(hex_of(&bytes), expected);
    // 27 bytes of M4 envelope plus the 56 the dedup group costs when it is present.
    assert_eq!(bytes.len(), 83);
    assert_eq!(bytes.len(), dedup_put.encoded_len());
    assert_eq!(Command::decode(&bytes).unwrap(), dedup_put);
    assert_eq!(&bytes[0..4], &COMMAND_MAGIC);
    assert_eq!(COMMAND_ENVELOPE_VERSION, 2);

    let compact = Command::Compact {
        up_to_revision: 5,
        dedup_trim_below: Some(3),
    };
    let bytes = compact.encode();
    assert_eq!(
        hex_of(&bytes),
        "52 43 4d 44 02 00 03 05 00 00 00 00 00 00 00 01 03 00 00 00 00 00 00 00"
    );
    assert_eq!(bytes[6], OP_COMPACT);
    assert_eq!(Command::decode(&bytes).unwrap(), compact);

    // A `Compact` with no trim is the shape an M4-era compaction has, still expressible.
    let no_trim = Command::Compact {
        up_to_revision: 5,
        dedup_trim_below: None,
    };
    assert_eq!(
        hex_of(&no_trim.encode()),
        "52 43 4d 44 02 00 03 05 00 00 00 00 00 00 00 00",
        "an absent trim watermark is the flag byte and nothing else (M5-R17)"
    );

    let retire = Command::RetireNode { node_id: NodeId(9) };
    let bytes = retire.encode();
    assert_eq!(
        hex_of(&bytes),
        "52 43 4d 44 02 00 04 09 00 00 00 00 00 00 00"
    );
    assert_eq!(bytes[6], OP_RETIRE_NODE);
    assert_eq!(bytes.len(), 15);
    assert_eq!(Command::decode(&bytes).unwrap(), retire);
}

/// One logical command, one byte string: the dedup group and the trim watermark admit no
/// second spelling, or byte equality would stop being a determinism oracle.
#[config_log::retcd_test]
fn m5_74_non_canonical_dedup_and_trim_are_rejected() {
    // The flag is the last byte of a dedup-bearing `Put`'s envelope minus the 56 bytes it
    // introduces.
    let mut bad_flag = put_dedup(b"/a", b"1", 7).encode();
    let has_dedup = bad_flag.len() - 57;
    bad_flag[has_dedup] = 2;
    assert_eq!(
        Command::decode(&bad_flag),
        Err(DecodeError::InvalidHasDedup(2))
    );

    // Under M5-R17 the group is variable-width, so there is no padding to populate behind a
    // zero flag: canonical form is structural rather than checked. What has to be refused
    // instead is a *truncated* group behind `has_dedup = 1` — the shape a peer running an
    // envelope it only half understands would produce.
    let full = put_dedup(b"/a", b"1", 7).encode();
    for cut in [1usize, 20, 40, 56] {
        let truncated = &full[..full.len() - cut];
        assert!(
            matches!(
                Command::decode(truncated),
                Err(DecodeError::Truncated { .. })
            ),
            "a dedup group {cut} bytes short must be Truncated, not silently decoded"
        );
    }

    // And trailing bytes behind a complete command are still refused, which is what keeps
    // "one logical command, one byte string" true now that lengths vary.
    let mut trailing = Command::Put {
        key: b(b"/a"),
        value: b(b"1"),
        expected_mod_revision: None,
        dedup: None,
    }
    .encode();
    trailing.push(0);
    assert_eq!(
        Command::decode(&trailing),
        Err(DecodeError::TrailingBytes { extra: 1 })
    );

    let mut bad_trim = Command::Compact {
        up_to_revision: 5,
        dedup_trim_below: None,
    }
    .encode();
    let has_trim = bad_trim.len() - 1;
    bad_trim[has_trim] = 2;
    assert_eq!(
        Command::decode(&bad_trim),
        Err(DecodeError::InvalidHasTrim(2))
    );

    // `has_trim = 1` with no watermark behind it is the trim-side truncation.
    let mut truncated_trim = Command::Compact {
        up_to_revision: 5,
        dedup_trim_below: None,
    }
    .encode();
    let has_trim = truncated_trim.len() - 1;
    truncated_trim[has_trim] = 1;
    assert!(
        matches!(
            Command::decode(&truncated_trim),
            Err(DecodeError::Truncated { .. })
        ),
        "a has_trim flag with no watermark behind it must be Truncated"
    );
}

/// M5-95: a duplicate inside the window returns the original outcome, allocates nothing, and
/// produces no second event.
#[config_log::retcd_test]
fn m5_95_duplicate_within_window_returns_the_original_outcome() {
    let mut kv = KvState::with_limits(limits_with_dedup(1024, 1_000_000));

    let first = kv.apply_with_principal(&put_dedup(b"/a", b"1", 7), ALICE);
    let CommandResponse::Mutation {
        response: original,
        event,
        dedup_hit,
        dedup_recorded,
    } = first
    else {
        panic!("a dedup-bearing put applies as a mutation");
    };
    assert_eq!(original.outcome, MutationOutcome::Applied);
    assert_eq!(original.revision, 1);
    assert!(event.is_some(), "the first application publishes an event");
    assert!(!dedup_hit);
    assert!(dedup_recorded);

    let second = kv.apply_with_principal(&put_dedup(b"/a", b"1", 7), ALICE);
    let CommandResponse::Mutation {
        response,
        event,
        dedup_hit,
        ..
    } = second
    else {
        panic!("a duplicate is still a mutation response");
    };
    assert!(dedup_hit, "the resubmission is recognized");
    assert!(
        response.dedup_hit,
        "the response says so too, for a caller that never sees the apply-time flag"
    );
    // Every field except `dedup_hit` is the retained record, replayed. `dedup_hit` is the one
    // field that describes *this* submission rather than the original one, so it is set on the
    // way out and is expected to differ — comparing the whole struct would be asserting that
    // the flag never works.
    assert_eq!(
        MutationResponse {
            dedup_hit: false,
            ..response
        },
        original,
        "byte-for-byte the original outcome, apart from the flag naming this submission"
    );
    assert!(
        event.is_none(),
        "a hit must publish no second event, or every watcher sees a phantom write"
    );
    assert_eq!(
        kv.cluster_revision(),
        1,
        "and no revision is allocated for it"
    );
    assert_eq!(kv.dedup_len(), 1);
}

/// A duplicate of a *rejected* CAS replays the conflict, not a fresh evaluation: the stored
/// record is the whole response, so `exists` and `current_mod_revision` come back too.
#[config_log::retcd_test]
fn m5_95_duplicate_replays_a_conflict_unchanged() {
    let mut kv = KvState::with_limits(limits_with_dedup(1024, 1_000_000));
    kv.apply(&common::put(b"/a", b"1"));

    let cas = Command::Put {
        key: b(b"/a"),
        value: b(b"2"),
        expected_mod_revision: Some(99),
        dedup: Some(DedupKey::new(CLIENT, 1).stamp([0u8; 32])),
    };
    let CommandResponse::Mutation {
        response: first, ..
    } = kv.apply_with_principal(&cas, ALICE)
    else {
        panic!("mutation");
    };
    assert_eq!(first.outcome, MutationOutcome::Conflict);

    // Make the CAS *succeed* if it were evaluated again, so a re-evaluation would be visible.
    kv.apply(&common::put_cas(b"/a", b"9", 1));

    let CommandResponse::Mutation {
        response,
        dedup_hit,
        ..
    } = kv.apply_with_principal(&cas, ALICE)
    else {
        panic!("mutation");
    };
    assert!(dedup_hit);
    assert!(response.dedup_hit, "and the response carries the flag");
    assert_eq!(
        MutationResponse {
            dedup_hit: false,
            ..response
        },
        first,
        "the retained outcome is replayed, never recomputed against newer state"
    );
}

/// M5-99: the window is per `(principal, client_id)` and bounded, and an evicted id fails
/// **closed** into the monotonicity error rather than into a second application.
#[config_log::retcd_test]
fn m5_99_window_eviction_is_per_client_and_bounded() {
    let mut kv = KvState::with_limits(limits_with_dedup(8, 1_000_000));

    let mut evictions = 0;
    for id in 1..=12 {
        let mut effects = ApplyEffects::default();
        let cmd = put_dedup(b"/a", b"1", id).bind_principal(ALICE);
        kv.apply_with_effects(&cmd, &mut effects);
        evictions += effects.dedup_window_evictions;
    }
    assert_eq!(kv.dedup_len(), 8, "ids 5..=12 are retained");
    assert_eq!(evictions, 4, "ids 1..=4 were evicted, one at a time");

    let replayed = kv.apply_with_principal(&put_dedup(b"/a", b"1", 1), ALICE);
    let CommandResponse::Rejected { reason } = replayed else {
        panic!("an evicted id must not be applied a second time: {replayed:?}");
    };
    assert!(
        reason.contains("request_id_not_monotonic"),
        "the refusal names the rule: {reason}"
    );
    assert_eq!(kv.cluster_revision(), 12, "and allocated nothing");

    // A second client under the same principal keeps its own window.
    let other = Command::Put {
        key: b(b"/a"),
        value: b(b"1"),
        expected_mod_revision: None,
        dedup: Some(DedupKey::new([0xcd; 16], 1).stamp([0u8; 32])),
    };
    let CommandResponse::Mutation { dedup_hit, .. } = kv.apply_with_principal(&other, ALICE) else {
        panic!("a different client_id is a fresh namespace");
    };
    assert!(!dedup_hit);
}

/// M5-100 (core half): the global cap fails closed on *new* records — the mutation applies,
/// nothing is retained, and `Compact { dedup_trim_below }` is what releases the space (OQ-49).
#[config_log::retcd_test]
fn m5_100_global_cap_refuses_new_records_and_compact_trims() {
    let mut kv = KvState::with_limits(limits_with_dedup(1024, 2));

    for id in 1..=2 {
        let response = kv.apply_with_principal(&put_dedup(b"/a", b"1", id), ALICE);
        // Both copies of the flag, because only the inner one reaches the caller: the
        // envelope field is consumed by the engine, `MutationResponse.dedup_recorded` is what
        // crosses the wire and what a client decides to resubmit on (C5B-05).
        assert!(
            matches!(
                response,
                CommandResponse::Mutation {
                    dedup_recorded: true,
                    response: MutationResponse {
                        dedup_recorded: true,
                        ..
                    },
                    ..
                }
            ),
            "id {id} is retained, and the response must say so: {response:?}"
        );
    }

    let mut effects = ApplyEffects::default();
    let at_cap = put_dedup(b"/a", b"1", 3).bind_principal(ALICE);
    let response = kv.apply_with_effects(&at_cap, &mut effects);
    let CommandResponse::Mutation {
        response: mutation,
        dedup_recorded,
        ..
    } = response
    else {
        panic!("the write applies normally at the cap");
    };
    assert_eq!(
        mutation.outcome,
        MutationOutcome::Applied,
        "reaching the cap must never reject a write"
    );
    assert!(
        !dedup_recorded,
        "and must never silently claim to retain it"
    );
    assert!(
        !mutation.dedup_recorded,
        "least of all on the response the client reads and resubmits on"
    );
    assert_eq!(effects.dedup_cap_refusals, 1);
    assert_eq!(kv.dedup_len(), 2, "no other client's record was evicted");
    // The counter the exporter reads is the same event, and it is *not* an eviction: nothing
    // was dropped, an outcome was never retained (review finding C5B-04, ADR-0026).
    assert_eq!(kv.dedup_cap_refusals(), 1);
    assert_eq!(
        kv.dedup_window_evictions(),
        0,
        "cap pressure must not be reported as window pressure"
    );
    assert_eq!(kv.dedup_trim_evictions(), 0, "nor as trim pressure");

    // The trim rides the existing `Compact` (OQ-48) and releases records by the revision they
    // were applied at.
    let mut effects = ApplyEffects::default();
    kv.apply_with_effects(
        &Command::Compact {
            up_to_revision: 0,
            dedup_trim_below: Some(2),
        },
        &mut effects,
    );
    assert_eq!(kv.dedup_len(), 1, "records applied at or below 2 are gone");
    assert_eq!(effects.dedup_trim_evictions, 1);
    assert_eq!(kv.dedup_trim_evictions(), 1, "exported as reason=\"trim\"");
    assert_eq!(
        kv.dedup_cap_refusals(),
        1,
        "and the cap refusal is a separate series that the trim did not touch"
    );
    assert_eq!(
        effects.dedup_removed.len(),
        1,
        "the storage layer is told exactly which keys to delete"
    );
}

/// M5-101: the principal is the leader's, never the message's. A replay by another principal
/// of the same `(client_id, request_id)` is a miss under its own namespace, not a hit
/// returning the first principal's outcome.
#[config_log::retcd_test]
fn m5_101_principal_is_bound_by_the_leader_not_the_message() {
    let mut kv = KvState::with_limits(limits_with_dedup(1024, 1_000_000));

    kv.apply_with_principal(&put_dedup(b"/a", b"alice", 7), ALICE);

    // Bob's command *claims* Alice's principal on the wire.
    let forged = Command::Put {
        key: b(b"/a"),
        value: b(b"bob"),
        expected_mod_revision: None,
        dedup: Some(DedupKey::new(CLIENT, 7).stamp(ALICE)),
    };
    let CommandResponse::Mutation {
        response,
        dedup_hit,
        ..
    } = kv.apply_with_principal(&forged, BOB)
    else {
        panic!("mutation");
    };
    assert!(
        !dedup_hit,
        "a claimed principal must not address another principal's namespace"
    );
    assert_eq!(response.outcome, MutationOutcome::Applied);
    assert_eq!(kv.get(b"/a").unwrap().value, b(b"bob"));
    assert_eq!(kv.dedup_len(), 2, "two namespaces, two records");

    // Alice.s own namespace is untouched by Bob.s traffic: her resubmission still hits, and
    // still returns *her* outcome.
    let CommandResponse::Mutation {
        response,
        dedup_hit,
        ..
    } = kv.apply_with_principal(&put_dedup(b"/a", b"alice", 7), ALICE)
    else {
        panic!("mutation");
    };
    assert!(dedup_hit);
    assert_eq!(response.revision, 1, "Alice's original revision, not Bob's");

    // And the bind is unconditional: whatever the envelope carried is overwritten.
    let bound = forged.bind_principal(BOB);
    assert_eq!(
        bound.dedup(),
        Some(DedupStamp {
            principal_hash: BOB,
            key: DedupKey::new(CLIENT, 7),
        })
    );
}

/// M5-108: dedup is off by default, and an M5 build with it off behaves exactly as M4 did —
/// a resubmission applies a second time rather than being silently swallowed.
#[config_log::retcd_test]
fn m5_108_dedup_is_off_by_default_and_m4_behaviour_is_unchanged() {
    // `assert_eq!` rather than `assert!(!…)`: the expression is const-evaluable, and clippy's
    // `assertions_on_constants` refuses an assertion the compiler can fold away.
    assert_eq!(
        Limits::DEFAULT.dedup,
        DedupLimits::DISABLED,
        "the default limits must leave deduplication off"
    );
    assert_eq!(DedupLimits::default(), DedupLimits::DISABLED);

    let mut kv = KvState::new();
    let mut effects = ApplyEffects::default();
    for _ in 0..2 {
        kv.apply_with_effects(
            &put_dedup(b"/a", b"1", 7).bind_principal(ALICE),
            &mut effects,
        );
    }
    assert_eq!(
        kv.cluster_revision(),
        2,
        "with dedup off the resubmission applies again, as ADR-0015 documents"
    );
    assert_eq!(kv.dedup_len(), 0, "and the dedup index stays empty");
    assert!(effects.is_empty());
}

/// ADR-0023: retirement is replicated, idempotent, and has no inverse.
#[config_log::retcd_test]
fn m5_59_retire_node_is_replicated_idempotent_and_final() {
    let mut kv = KvState::new();

    let mut effects = ApplyEffects::default();
    let response = kv.apply_with_effects(&Command::RetireNode { node_id: NodeId(3) }, &mut effects);
    assert_eq!(response, CommandResponse::Retired { node_id: NodeId(3) });
    assert_eq!(effects.retired_node, Some(NodeId(3)));
    assert!(kv.is_retired(NodeId(3)));
    assert!(!kv.is_retired(NodeId(4)));
    assert_eq!(
        kv.cluster_revision(),
        0,
        "retirement is not a mutation and allocates no revision"
    );

    // Re-applying is a no-op that still answers identically, because a re-proposed command
    // must apply the same way on every voter.
    let mut again = ApplyEffects::default();
    let repeated = kv.apply_with_effects(&Command::RetireNode { node_id: NodeId(3) }, &mut again);
    assert_eq!(repeated, CommandResponse::Retired { node_id: NodeId(3) });
    assert_eq!(
        again.retired_node, None,
        "the storage layer is only asked to write when the set actually changed"
    );

    // The set is restorable, which is what makes it survive a restart and a snapshot install.
    let mut restored = KvState::new();
    restored.restore_retired_nodes(kv.retired_nodes().iter().copied());
    assert_eq!(restored.retired_nodes(), kv.retired_nodes());
}

/// The storage key is the ADR-0025 layout exactly, and it round-trips — the property that
/// makes a per-client window a range scan and a trim a range delete.
#[config_log::retcd_test]
fn m5_dedup_storage_key_is_the_adr_layout() {
    let key = (ALICE, CLIENT, 7u64);
    let raw = config_core::dedup_storage_key(&key);

    assert_eq!(raw.len(), 56);
    assert_eq!(&raw[0..32], &ALICE);
    assert_eq!(&raw[32..48], &CLIENT);
    assert_eq!(
        &raw[48..56],
        &7u64.to_be_bytes(),
        "big-endian, so byte order is request order"
    );
    assert_eq!(config_core::dedup_index_key_from_storage(&raw), Some(key));
    assert_eq!(config_core::dedup_index_key_from_storage(&raw[..55]), None);

    let lower = config_core::dedup_storage_key(&(ALICE, CLIENT, 6));
    assert!(lower < raw, "ascending request ids sort ascending");
}

/// M5-129 (finding C5B-06): the request budget is measured on the command that will actually
/// be replicated — dedup group included — and `encoded_len` is the length of the bytes
/// `encode` produces, not an estimate of them.
///
/// Two separate claims, and the second is why the first matters.
///
/// `encoded_len` is a hand-written sum of constants. `encode` is a hand-written sequence of
/// pushes. Nothing in the type system ties them together, so the M5-R17 widening — where the
/// dedup group became *variable* width, one flag byte when absent and 57 when present — was
/// exactly the kind of change that silently desynchronizes them. The first half of this row
/// pins them across the whole matrix rather than at the one shape a golden-bytes row happens
/// to use.
///
/// The second half is the budget. `validate_put` measures `Command::from(&req)`, whose dedup
/// group is stamped with a placeholder principal hash. A placeholder is correct *because* the
/// hash is fixed-width: the edge cannot know which principal the leader will bind, but it does
/// not need to — the length is the same for every principal, so the edge check and the
/// replicated bytes agree (`Limits::max_request_bytes` doc). If the edge instead measured the
/// unstamped command, a client could put 56 bytes past a cap by attaching a dedup key, and the
/// overflow would only be discovered after the entry was already in the log.
#[config_log::retcd_test]
fn m5_129_request_size_is_measured_on_the_stamped_command() {
    // 1. Every shape: the measurement equals the bytes.
    let shapes = [
        put_dedup(b"/k", b"v", 7),
        Command::Put {
            key: b(b"/k"),
            value: b(b"v"),
            expected_mod_revision: Some(3),
            dedup: None,
        },
        Command::Delete {
            key: b(b"/k"),
            expected_mod_revision: None,
            dedup: Some(DedupKey::new(CLIENT, 7).stamp(ALICE)),
        },
        Command::Delete {
            key: b(b"/k"),
            expected_mod_revision: Some(3),
            dedup: None,
        },
        Command::Compact {
            up_to_revision: 9,
            dedup_trim_below: Some(4),
        },
        Command::Compact {
            up_to_revision: 9,
            dedup_trim_below: None,
        },
        Command::RetireNode { node_id: NodeId(4) },
    ];
    for cmd in &shapes {
        assert_eq!(
            cmd.encoded_len(),
            cmd.encode().len(),
            "encoded_len must be the length of the bytes encode writes, for {cmd:?}"
        );
    }

    // The stamp is what makes the group variable-width, so its cost is pinned by name.
    let stamped = put_dedup(b"/k", b"v", 7);
    let bare = Command::Put {
        key: b(b"/k"),
        value: b(b"v"),
        expected_mod_revision: None,
        dedup: None,
    };
    assert_eq!(
        stamped.encoded_len() - bare.encoded_len(),
        32 + 16 + 8,
        "a present dedup group costs principal_hash || client_id || request_id beyond its flag"
    );

    // 2. The budget: a request that fits without a dedup key does not fit with one.
    let limits = Limits {
        max_request_bytes: 512,
        ..Limits::DEFAULT
    };
    let filler = vec![0u8; limits.max_request_bytes - 25 - 1];
    let at_cap = PutRequest {
        key: b(b"k"),
        value: b(&filler),
        expected_mod_revision: None,
        dedup: None,
    };
    assert_eq!(
        Command::from(&at_cap).encoded_len(),
        limits.max_request_bytes,
        "the fixture must sit exactly on the cap or the next assertion proves nothing"
    );
    config_core::validate_put(&at_cap, &limits).expect("exactly at the cap is admitted");

    let with_key = PutRequest {
        dedup: Some(DedupKey::new(CLIENT, 7)),
        ..at_cap.clone()
    };
    let err = config_core::validate_put(&with_key, &limits)
        .expect_err("attaching a dedup key must not buy 56 bytes of headroom");
    assert_eq!(err.kind(), config_core::StatusClass::ResourceExhausted);
    assert!(
        err.to_string()
            .contains(&(limits.max_request_bytes + 32 + 16 + 8).to_string()),
        "the refusal should report the stamped length, got {err}"
    );

    // 3. And the measurement does not depend on *which* principal the leader will bind, which
    // is the only reason measuring at the edge is sound at all.
    let alice = Command::Put {
        key: b(b"k"),
        value: b(&filler),
        expected_mod_revision: None,
        dedup: Some(DedupKey::new(CLIENT, 7).stamp(ALICE)),
    };
    let bob = Command::Put {
        key: b(b"k"),
        value: b(&filler),
        expected_mod_revision: None,
        dedup: Some(DedupKey::new(CLIENT, 7).stamp(BOB)),
    };
    assert_eq!(alice.encoded_len(), bob.encoded_len());
    assert_eq!(
        alice.encoded_len(),
        Command::from(&with_key).encoded_len(),
        "the placeholder hash the edge stamps must measure the same as a real one"
    );

    // 4. Delete is measured the same way, one byte of framing lighter.
    let delete_at_cap = DeleteRequest {
        key: b(&vec![0u8; limits.max_request_bytes - 21]),
        expected_mod_revision: None,
        dedup: None,
    };
    config_core::validate_delete(&delete_at_cap, &limits).expect("exactly at the cap");
    let delete_with_key = DeleteRequest {
        dedup: Some(DedupKey::new(CLIENT, 7)),
        ..delete_at_cap.clone()
    };
    assert_eq!(
        config_core::validate_delete(&delete_with_key, &limits)
            .expect_err("the stamp counts for Delete too")
            .kind(),
        config_core::StatusClass::ResourceExhausted
    );

    // 5. Apply-time (finding C5B-06): `validate_command` runs on every voter against the
    // entry as it sits in the log, and must measure the stamp that entry actually carries.
    // A crafted or replayed entry whose key and value fit but whose stamp pushes it past
    // the cap is refused here, not applied because the rebuilt request had `dedup: None`.
    config_core::validate_command(&Command::from(&at_cap), &limits)
        .expect("the bare entry at the cap is admitted at apply time too");
    assert_eq!(
        config_core::validate_command(&alice, &limits)
            .expect_err("a stamped over-cap Put must be refused at apply time")
            .kind(),
        config_core::StatusClass::ResourceExhausted
    );
    let stamped_delete = Command::Delete {
        key: delete_at_cap.key.clone(),
        expected_mod_revision: None,
        dedup: Some(DedupKey::new(CLIENT, 7).stamp(ALICE)),
    };
    assert_eq!(
        config_core::validate_command(&stamped_delete, &limits)
            .expect_err("a stamped over-cap Delete must be refused at apply time")
            .kind(),
        config_core::StatusClass::ResourceExhausted
    );
}

/// M5-131 (finding C5B-07): ids may arrive out of order inside the window, and the id the
/// window has already evicted still fails closed.
///
/// `GrpcClient` mints request ids with one atomic `fetch_add` and is `Clone`, so a client with
/// two requests in flight mints them in order and has no way to make them *arrive* in order.
/// Under a ceiling rule — "a request_id must exceed every id still retained" — whichever one
/// landed second was refused as non-monotonic. That did not make anything unsafe; it made
/// deduplication unusable by any concurrent caller, which is nearly all of them.
///
/// The floor rule admits the gap and gives up nothing, because eviction is strictly
/// oldest-first: any id above the oldest retained id that is *not* retained was never applied.
/// The second half of this row is the part that has to keep holding — an id at or below the
/// floor may have been evicted, so it is still refused rather than applied a second time.
#[config_log::retcd_test]
fn m5_131_out_of_order_ids_are_admitted_inside_the_window() {
    let mut kv = KvState::with_limits(limits_with_dedup(8, 1_000_000));

    // Three concurrent requests, landing 102, 100, 101 — one `fetch_add` sequence, reordered
    // by the network.
    for id in [102, 100, 101] {
        let response = kv.apply_with_principal(&put_dedup(b"/a", b"1", id), ALICE);
        let CommandResponse::Mutation {
            dedup_hit,
            dedup_recorded,
            ..
        } = response
        else {
            panic!("an in-window gap must apply, not be refused: id {id} gave {response:?}");
        };
        assert!(!dedup_hit, "id {id} is new, not a duplicate");
        assert!(
            dedup_recorded,
            "id {id} must be retained for a resubmission"
        );
    }
    assert_eq!(kv.dedup_len(), 3);
    assert_eq!(kv.cluster_revision(), 3, "each applied exactly once");

    // And each of them is still a duplicate on resubmission — admitting the gap must not have
    // cost the guarantee the gap was admitted for.
    for id in [100, 101, 102] {
        let CommandResponse::Mutation { dedup_hit, .. } =
            kv.apply_with_principal(&put_dedup(b"/a", b"1", id), ALICE)
        else {
            panic!("a retained id must replay");
        };
        assert!(dedup_hit, "id {id} must replay from its record");
    }
    assert_eq!(kv.cluster_revision(), 3, "and none of them allocated again");

    // Fill the window past 100 so the early ids are evicted, then resubmit one of them. Now
    // the outcome is genuinely unknowable, and the rule fails closed.
    for id in 200..=207 {
        kv.apply_with_principal(&put_dedup(b"/a", b"1", id), ALICE);
    }
    assert_eq!(kv.dedup_len(), 8, "the window holds 200..=207");
    let replayed = kv.apply_with_principal(&put_dedup(b"/a", b"1", 101), ALICE);
    let CommandResponse::Rejected { reason } = replayed else {
        panic!("an evicted id must not apply a second time: {replayed:?}");
    };
    assert!(
        reason.contains("request_id_not_monotonic") && reason.contains("200"),
        "the refusal names the rule and the floor it compared against: {reason}"
    );

    // Floor, not ceiling -- and this is the half that tells the two rules apart. Land a
    // far-ahead id so the full window holds a genuine gap, then submit an id inside it.
    kv.apply_with_principal(&put_dedup(b"/a", b"1", 300), ALICE);
    assert_eq!(
        kv.dedup_len(),
        8,
        "inserting 300 evicted 200: the window is 201..=207, 300"
    );

    // 250 is above the oldest retained id (201) and not retained, which by the eviction
    // discipline proves it was never applied, so it must apply. A ceiling rule sees
    // 250 <= 300 and refuses a request that has never been seen.
    let gap = kv.apply_with_principal(&put_dedup(b"/a", b"1", 250), ALICE);
    let CommandResponse::Mutation { dedup_hit, .. } = gap else {
        panic!("an id inside the window's gap was never applied, so it must apply: {gap:?}");
    };
    assert!(!dedup_hit, "250 is new");
    assert_eq!(kv.dedup_len(), 8, "and inserting it evicted 201 in turn");

    // Widening the rule cost nothing on either side of the new floor: 250 now replays...
    let replayed = kv.apply_with_principal(&put_dedup(b"/a", b"1", 250), ALICE);
    assert!(
        matches!(
            replayed,
            CommandResponse::Mutation {
                dedup_hit: true,
                ..
            }
        ),
        "250 is retained now and must replay, not apply again: {replayed:?}"
    );
    // ...and the id its insertion evicted still fails closed.
    let evicted = kv.apply_with_principal(&put_dedup(b"/a", b"1", 201), ALICE);
    let CommandResponse::Rejected { reason } = evicted else {
        panic!("201 was evicted and must not apply a second time: {evicted:?}");
    };
    assert!(reason.contains("request_id_not_monotonic"), "{reason}");
}
