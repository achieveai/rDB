//! M0-62..M0-65 — concurrent CAS, M0 form (spec §20, §21 M0 bullet 2, §19.4).
//!
//! "Concurrent" at M0 means what it will mean at M1: N clients each read the same revision and
//! each submit a command built from it, and consensus serializes them into one apply order.
//! The state machine sees a sequence, so these tests build the competing commands from one
//! cloned snapshot and then apply them in order — which is exactly the situation a real race
//! produces, without needing threads to reproduce it.

mod common;

use common::{apply, del_cas, put, put_cas};
use config_core::{Command, KvState, MutationOutcome};
use proptest::prelude::*;

const COMPETITORS: usize = 8;

#[config_log::retcd_test]
fn m0_62_exactly_one_of_n_cas_applies() {
    let mut state = KvState::new();
    apply(&mut state, &put(b"k", b"v0"));
    let observed = state.get(b"k").expect("present").mod_revision;

    // Every client built its command from the same snapshot, before any of them applied.
    let snapshot = state.clone();
    assert_eq!(snapshot.get(b"k").unwrap().mod_revision, observed);
    let commands: Vec<Command> = (0..COMPETITORS)
        .map(|i| put_cas(b"k", format!("v{i}").as_bytes(), observed))
        .collect();

    let responses: Vec<_> = commands.iter().map(|cmd| apply(&mut state, cmd)).collect();

    let applied: Vec<usize> = responses
        .iter()
        .enumerate()
        .filter(|(_, r)| r.outcome == MutationOutcome::Applied)
        .map(|(i, _)| i)
        .collect();
    assert_eq!(
        applied,
        vec![0],
        "the first in apply order wins, deterministically"
    );
    assert_eq!(responses[0].revision, observed + 1);

    for (index, response) in responses.iter().enumerate().skip(1) {
        assert_eq!(
            response.outcome,
            MutationOutcome::Conflict,
            "competitor {index}"
        );
        assert!(response.exists);
        assert_eq!(response.current_mod_revision, observed + 1);
    }
    assert_eq!(
        state.cluster_revision(),
        observed + 1,
        "exactly one revision allocated"
    );
    assert_eq!(
        state.get(b"k").unwrap().value,
        bytes::Bytes::from_static(b"v0")
    );
}

#[config_log::retcd_test]
fn m0_63_exactly_one_of_n_cas_create_only() {
    let mut state = KvState::new();

    let commands: Vec<Command> = (0..COMPETITORS)
        .map(|i| put_cas(b"k", format!("v{i}").as_bytes(), 0))
        .collect();
    let responses: Vec<_> = commands.iter().map(|cmd| apply(&mut state, cmd)).collect();

    assert_eq!(responses[0].outcome, MutationOutcome::Applied);
    let allocated = responses[0].revision;
    assert_eq!(allocated, 1);

    for (index, response) in responses.iter().enumerate().skip(1) {
        assert_eq!(
            response.outcome,
            MutationOutcome::Conflict,
            "competitor {index}"
        );
        assert!(response.exists);
        assert_eq!(response.current_mod_revision, allocated);
    }
    assert_eq!(state.cluster_revision(), 1);
}

/// The losers see `NOT_FOUND`, not `CONFLICT`: the key is gone by the time they apply
/// (ADR-0006 row 12). Returning `CONFLICT` here is a very common implementation error and
/// would tell a client to retry a delete that already succeeded.
#[config_log::retcd_test]
fn m0_64_exactly_one_of_n_cas_delete() {
    let mut state = KvState::new();
    apply(&mut state, &put(b"k", b"v0"));
    let observed = state.get(b"k").expect("present").mod_revision;

    let commands: Vec<Command> = (0..COMPETITORS).map(|_| del_cas(b"k", observed)).collect();
    let responses: Vec<_> = commands.iter().map(|cmd| apply(&mut state, cmd)).collect();

    assert_eq!(responses[0].outcome, MutationOutcome::Applied);
    assert_eq!(responses[0].revision, observed + 1);
    for (index, response) in responses.iter().enumerate().skip(1) {
        assert_eq!(
            response.outcome,
            MutationOutcome::NotFound,
            "competitor {index} must see NOT_FOUND, not CONFLICT"
        );
    }
    assert_eq!(state.cluster_revision(), observed + 1);
    assert!(state.get(b"k").is_none());
}

/// Generalized: across any interleaving on two keys, at most one command can win against a
/// given expected revision, and allocated revisions form a gapless increasing run.
#[config_log::retcd_test]
fn m0_65_cas_contention_proptest() {
    let strategy = (
        2usize..16,
        prop::collection::vec(0u8..2, 2..16),
        prop::collection::vec(0u64..4, 16),
    );
    proptest!(|((n, key_choices, expected_choices) in strategy)| {
        let mut state = KvState::new();
        apply(&mut state, &put(b"k0", b"seed"));
        apply(&mut state, &put(b"k1", b"seed"));

        let commands: Vec<Command> = (0..n)
            .map(|i| {
                let key: &[u8] = if key_choices[i % key_choices.len()] == 0 { b"k0" } else { b"k1" };
                put_cas(key, format!("v{i}").as_bytes(), expected_choices[i])
            })
            .collect();

        let before = state.cluster_revision();
        let mut applied_revisions = Vec::new();
        // Winners are counted per (key, expected revision): several commands may win on one
        // key when they chain (B CASes on the revision A just allocated), but never two
        // against the same expected revision.
        let mut winners_per_key = std::collections::BTreeMap::<(Vec<u8>, u64), usize>::new();

        for cmd in &commands {
            let response = apply(&mut state, cmd);
            if response.outcome == MutationOutcome::Applied {
                applied_revisions.push(response.revision);
                let expected = cmd.expected_mod_revision().unwrap_or(0);
                *winners_per_key.entry((cmd.key().to_vec(), expected)).or_default() += 1;
            }
        }

        for ((key, expected), wins) in &winners_per_key {
            prop_assert!(
                *wins <= 1,
                "at most one command may win against expected revision {expected} on key {key:?}, saw {wins}"
            );
        }

        let expected_run: Vec<u64> = (1..=applied_revisions.len() as u64)
            .map(|offset| before + offset)
            .collect();
        prop_assert_eq!(&applied_revisions, &expected_run, "revisions increase by one with no gaps");
        prop_assert_eq!(state.cluster_revision(), before + applied_revisions.len() as u64);
    });
}
