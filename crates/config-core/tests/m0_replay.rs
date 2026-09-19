//! M0-52..M0-57 — replay determinism and the `state_hash` contract
//! (spec §21 M0 bullet 1, ADR-0007, TA-2).
//!
//! This is the M0 acceptance gate: an identical command sequence must yield byte-identical
//! state and an identical response sequence. The hash is the oracle, so the hash itself is
//! also tested — a digest too weak to notice a changed revision would make every other row
//! here vacuous.

mod common;

use std::collections::BTreeMap;

use common::{apply, del, del_cas, put, put_cas};
use config_core::{Command, CommandResponse, KvState, Limits, Record};
use proptest::prelude::*;

fn hex32(digest: [u8; 32]) -> String {
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// SHA-256 over `cluster_revision = 0` and `record count = 0` — sixteen zero bytes.
const EMPTY_STATE_HASH: &str = "374708fff7719dd5979ec875d56cd2286f6d3cf7ec317a3b25632aab28ec37bb";

#[config_log::retcd_test]
fn m0_55_empty_state_hash_golden() {
    assert_eq!(hex32(KvState::new().state_hash()), EMPTY_STATE_HASH);
    assert_eq!(
        KvState::new().state_hash(),
        KvState::default().state_hash(),
        "Default and new() must agree"
    );
    assert_eq!(
        KvState::with_limits(config_core::Limits {
            max_key_bytes: 8,
            ..config_core::Limits::DEFAULT
        })
        .state_hash(),
        KvState::new().state_hash(),
        "limits are node-local configuration and must not enter the hash"
    );
}

/// A hash that misses any of these differences could not detect divergence between replicas.
/// Insertion order is the one thing it must *not* see.
#[config_log::retcd_test]
fn m0_56_state_hash_sensitivity() {
    let base = {
        let mut s = KvState::new();
        apply(&mut s, &put(b"a", b"1"));
        apply(&mut s, &put(b"b", b"2"));
        s
    };

    let different_value = {
        let mut s = KvState::new();
        apply(&mut s, &put(b"a", b"1"));
        apply(&mut s, &put(b"b", b"X"));
        s
    };
    let different_key_set = {
        let mut s = KvState::new();
        apply(&mut s, &put(b"a", b"1"));
        apply(&mut s, &put(b"c", b"2"));
        s
    };
    let different_mod_revision = {
        let mut s = KvState::new();
        apply(&mut s, &put(b"a", b"1"));
        apply(&mut s, &put(b"b", b"2"));
        apply(&mut s, &put(b"b", b"2")); // same value, new mod_revision
        s
    };
    let different_create_revision = {
        let mut s = KvState::new();
        apply(&mut s, &put(b"b", b"2"));
        apply(&mut s, &put(b"a", b"1"));
        s
    };
    let different_cluster_revision = KvState::from_parts(Limits::DEFAULT, 99, collect(&base));

    for (label, other) in [
        ("value bytes", &different_value),
        ("key set", &different_key_set),
        ("mod_revision", &different_mod_revision),
        ("create_revision", &different_create_revision),
        ("cluster_revision", &different_cluster_revision),
    ] {
        assert_ne!(
            base.state_hash(),
            other.state_hash(),
            "the hash must distinguish a differing {label}"
        );
    }

    // Insertion order alone must not matter: same records, same revisions, same hash.
    let reordered = KvState::from_parts(Limits::DEFAULT, base.cluster_revision(), collect(&base));
    assert_eq!(base.state_hash(), reordered.state_hash());
}

fn collect(state: &KvState) -> BTreeMap<bytes::Bytes, Record> {
    state
        .iter()
        .map(|(key, record)| (key.clone(), record.clone()))
        .collect()
}

/// A hash insensitive to apply order would make the replay rows vacuous — two machines fed
/// different orders would "agree".
#[config_log::retcd_test]
fn m0_57_apply_is_order_sensitive() {
    let mut forward = KvState::new();
    apply(&mut forward, &put(b"k", b"v1"));
    apply(&mut forward, &put(b"k", b"v2"));

    let mut reverse = KvState::new();
    apply(&mut reverse, &put(b"k", b"v2"));
    apply(&mut reverse, &put(b"k", b"v1"));

    assert_eq!(forward.cluster_revision(), reverse.cluster_revision());
    assert_ne!(forward.state_hash(), reverse.state_hash());
}

/// Sequences over a deliberately small key space so CAS hits and misses both occur often;
/// a key space wide enough to avoid collisions would never exercise a conflict.
fn sequence_strategy() -> impl Strategy<Value = Vec<Command>> {
    let key = (0u8..8).prop_map(|i| bytes::Bytes::from(vec![b'k', b'0' + i]));
    let expected = prop_oneof![Just(None), (0u64..6).prop_map(Some)];
    let command = prop_oneof![
        3 => (key.clone(), prop::collection::vec(any::<u8>(), 0..8), expected.clone())
            .prop_map(|(key, value, expected_mod_revision)| Command::Put {
                key,
                value: bytes::Bytes::from(value),
                expected_mod_revision,
            }),
        1 => (key, expected).prop_map(|(key, expected_mod_revision)| Command::Delete {
            key,
            expected_mod_revision,
        }),
    ];
    // 100 commands over 8 keys already produces dozens of CAS hits and misses per case. The
    // ceiling is a runtime budget, not a coverage one: every apply emits a JSONL line
    // (ADR-0013), so the suite's cost is dominated by logging rather than by the state
    // machine, and the per-test budget is 5 s (test plan §2).
    prop::collection::vec(command, 1..100)
}

fn run(state: &mut KvState, commands: &[Command]) -> Vec<CommandResponse> {
    commands.iter().map(|cmd| state.apply(cmd)).collect()
}

#[config_log::retcd_test]
fn m0_52_replay_determinism_proptest() {
    proptest!(|(commands in sequence_strategy())| {
        let mut a = KvState::new();
        let mut c = KvState::new();

        let responses_a = run(&mut a, &commands);

        let mut b_state = KvState::new();
        let responses_b = run(&mut b_state, &commands);

        // A third run that clones the machine after every step, proving Clone carries the
        // whole state and not a shared or lazily-derived part of it.
        let mut responses_c = Vec::with_capacity(commands.len());
        for cmd in &commands {
            let mut forked = c.clone();
            responses_c.push(forked.apply(cmd));
            c = forked;
        }

        prop_assert_eq!(a.state_hash(), b_state.state_hash());
        prop_assert_eq!(a.state_hash(), c.state_hash());
        prop_assert_eq!(&responses_a, &responses_b);
        prop_assert_eq!(&responses_a, &responses_c);
    });
}

/// Determinism must survive the wire form. If `encode`/`decode` lost or normalized anything,
/// a follower replaying from its log would diverge from the leader that applied the struct.
#[config_log::retcd_test]
fn m0_53_replay_from_encoded_bytes() {
    proptest!(|(commands in sequence_strategy())| {
        let mut direct = KvState::new();
        let responses_direct = run(&mut direct, &commands);

        let round_tripped: Vec<Command> = commands
            .iter()
            .map(|cmd| Command::decode(&cmd.encode()).expect("round trip"))
            .collect();
        let mut wire = KvState::new();
        let responses_wire = run(&mut wire, &round_tripped);

        prop_assert_eq!(direct.state_hash(), wire.state_hash());
        prop_assert_eq!(&responses_direct, &responses_wire);
    });
}

/// Comparing only the final hash reports "they differ" and nothing more. Comparing the hash
/// after every step names the first divergent index and the command at it.
#[config_log::retcd_test]
fn m0_54_replay_prefix_hash_sequence() {
    let commands = vec![
        put(b"k0", b"a"),
        put_cas(b"k0", b"b", 1),
        put_cas(b"k0", b"c", 99),
        del_cas(b"k0", 2),
        del(b"k0"),
        put_cas(b"k1", b"z", 0),
        put_cas(b"k1", b"z", 0),
    ];

    let mut left = KvState::new();
    let mut right = KvState::new();
    let mut left_hashes = Vec::new();
    let mut right_hashes = Vec::new();
    let mut left_responses = Vec::new();
    let mut right_responses = Vec::new();

    for cmd in &commands {
        left_responses.push(left.apply(cmd));
        left_hashes.push(left.state_hash());
        right_responses.push(right.apply(cmd));
        right_hashes.push(right.state_hash());
    }

    if let Some(index) = left_hashes
        .iter()
        .zip(&right_hashes)
        .position(|(l, r)| l != r)
    {
        panic!(
            "state diverged at index {index} after {:?}\n  left  {}\n  right {}",
            commands[index],
            hex32(left_hashes[index]),
            hex32(right_hashes[index])
        );
    }
    assert_eq!(left_responses, right_responses);
    assert_eq!(left_hashes.len(), commands.len());

    // A restored machine must be indistinguishable from a replayed one — the seam M2 relies
    // on when it compares nodes after restart.
    let restored = KvState::from_parts(Limits::DEFAULT, left.cluster_revision(), collect(&left));
    assert_eq!(restored.state_hash(), left.state_hash());
}

fn record(key: &[u8], value: &[u8], create_revision: u64, mod_revision: u64) -> Record {
    Record {
        key: bytes::Bytes::copy_from_slice(key),
        value: bytes::Bytes::copy_from_slice(value),
        create_revision,
        mod_revision,
    }
}

/// TA-2 isolation: two states identical in every field except one `create_revision` must
/// hash differently. M0-56 perturbs `create_revision` and `mod_revision` together, so this
/// row is the one that fails if the hash silently stops covering `create_revision`.
#[config_log::retcd_test]
fn m0_66_state_hash_isolates_create_revision() {
    let mut a = BTreeMap::new();
    a.insert(bytes::Bytes::from_static(b"k"), record(b"k", b"v", 1, 3));
    let mut b = BTreeMap::new();
    b.insert(bytes::Bytes::from_static(b"k"), record(b"k", b"v", 2, 3));
    let left = KvState::from_parts(Limits::DEFAULT, 3, a);
    let right = KvState::from_parts(Limits::DEFAULT, 3, b);
    assert_ne!(left.state_hash(), right.state_hash());
}

/// TA-2 length prefixes: `{"ab" -> ""}` and `{"a" -> "b"}` concatenate to the same bytes and
/// must still hash differently.
#[config_log::retcd_test]
fn m0_67_state_hash_length_prefixes_variable_fields() {
    let mut a = BTreeMap::new();
    a.insert(bytes::Bytes::from_static(b"ab"), record(b"ab", b"", 1, 1));
    let mut b = BTreeMap::new();
    b.insert(bytes::Bytes::from_static(b"a"), record(b"a", b"b", 1, 1));
    let left = KvState::from_parts(Limits::DEFAULT, 1, a);
    let right = KvState::from_parts(Limits::DEFAULT, 1, b);
    assert_ne!(left.state_hash(), right.state_hash());
}

/// A restored machine validates under the caps it ran with, not the defaults; otherwise a
/// restarted voter accepts a borderline command its peers reject.
#[config_log::retcd_test]
fn m0_68_from_parts_preserves_limits() {
    let limits = Limits {
        max_value_bytes: 4,
        ..Limits::DEFAULT
    };
    let mut live = KvState::with_limits(limits);
    apply(&mut live, &put(b"k", b"v"));
    let mut restored = KvState::from_parts(limits, live.cluster_revision(), collect(&live));
    assert_eq!(restored.limits(), &limits);
    assert_eq!(restored.state_hash(), live.state_hash());
    assert!(matches!(
        restored.apply(&put(b"k", b"12345")),
        CommandResponse::Rejected { .. }
    ));
}

/// TA-1.1: `apply` never panics. A corrupted persisted revision at `u64::MAX` is refused
/// instead of overflowing (debug) or wrapping to 0 (release).
#[config_log::retcd_test]
fn m0_69_apply_rejects_at_revision_exhaustion() {
    let mut state = KvState::from_parts(Limits::DEFAULT, u64::MAX, BTreeMap::new());
    let before = state.state_hash();
    assert!(matches!(
        state.apply(&put(b"k", b"v")),
        CommandResponse::Rejected { .. }
    ));
    assert!(matches!(
        state.apply(&del(b"k")),
        CommandResponse::Rejected { .. }
    ));
    assert_eq!(state.cluster_revision(), u64::MAX);
    assert_eq!(state.state_hash(), before);
}
