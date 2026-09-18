//! Shared builders for the M0 suites.
//!
//! Kept deliberately thin: every M0 test constructs a `KvState` directly and calls `apply`,
//! so these are convenience constructors, never a layer that could hide semantics.

#![allow(dead_code)]

use bytes::Bytes;
use config_core::{
    Command, CommandResponse, KvState, MutationOutcome, MutationResponse, PutRequest, Record,
};

/// Bytes from a literal.
pub fn b(s: &[u8]) -> Bytes {
    Bytes::copy_from_slice(s)
}

/// An unconditional `Put`.
pub fn put(key: &[u8], value: &[u8]) -> Command {
    Command::Put {
        key: b(key),
        value: b(value),
        expected_mod_revision: None,
    }
}

/// A conditional `Put`.
pub fn put_cas(key: &[u8], value: &[u8], expected: u64) -> Command {
    Command::Put {
        key: b(key),
        value: b(value),
        expected_mod_revision: Some(expected),
    }
}

/// An unconditional `Delete`.
pub fn del(key: &[u8]) -> Command {
    Command::Delete {
        key: b(key),
        expected_mod_revision: None,
    }
}

/// A conditional `Delete`.
pub fn del_cas(key: &[u8], expected: u64) -> Command {
    Command::Delete {
        key: b(key),
        expected_mod_revision: Some(expected),
    }
}

/// A `PutRequest` for the validator tests.
pub fn put_req(key: &[u8], value: &[u8]) -> PutRequest {
    PutRequest {
        key: b(key),
        value: b(value),
        expected_mod_revision: None,
    }
}

/// Apply a command and unwrap the mutation response, failing loudly on a rejection.
pub fn apply(state: &mut KvState, cmd: &Command) -> MutationResponse {
    match state.apply(cmd) {
        CommandResponse::Mutation { response, .. } => response,
        other => panic!("expected a mutation response, got {other:?}"),
    }
}

/// Apply a command and return the whole response, including the event.
pub fn apply_full(state: &mut KvState, cmd: &Command) -> CommandResponse {
    state.apply(cmd)
}

/// Drive a key to a known `mod_revision` by applying `n` unconditional puts, leaving
/// `cluster_revision == n`.
pub fn seed_key_at(state: &mut KvState, key: &[u8], target_revision: u64) {
    assert_eq!(state.cluster_revision(), 0, "seed from a fresh state");
    for i in 1..=target_revision {
        let value = format!("v{i}");
        let resp = apply(state, &put(key, value.as_bytes()));
        assert_eq!(resp.outcome, MutationOutcome::Applied);
    }
    assert_eq!(state.cluster_revision(), target_revision);
}

/// The record for `key`, or a panic naming the key.
pub fn record(state: &KvState, key: &[u8]) -> Record {
    state
        .get(key)
        .unwrap_or_else(|| panic!("expected key {key:?} to be present"))
        .clone()
}
