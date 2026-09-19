# ADR-0007: Canonical versioned replicated command envelope

**Status:** Accepted  
**Date:** 2026-09-17  
**Spec:** §7.4, §17

## Context

Replicated log entries must have a canonical encoding that every voter decodes identically,
survives upgrades, and never depends on serde container ordering or platform details.

## Decision

- `config_core::command::Command` is the only payload type in Raft log entries (`D = Command`).
- Encoding is a hand-written canonical binary format, `CommandV1`:
  `magic "RCMD"(4) | version u16 LE (=1) | op u8 (1=Put, 2=Delete) | key len u32 LE | key |`
  `value len u32 LE | value (Put only) | has_expected u8 | expected_mod_revision u64 LE`.
  No floats, no maps, no optional trailing fields; unknown version → typed decode error.
- The same struct also derives `serde::{Serialize, Deserialize}` because OpenRaft 0.9 requires
  serde on `D`/`R` under its `serde` feature. `CommandV1` is the **envelope carried inside a
  Raft entry's payload**, not the byte layout of a stored record: what M2 writes to
  `raft_log` is `postcard(Entry<TypeConfig>)`, whose `EntryPayload::Normal` holds the
  `Command` via that serde derive (`config-storage/src/rocks.rs` module docs; ADR-0008 note of
  2026-09-18 covers the resulting on-disk format version). `Command::encode()` remains the
  **canonical bytes** wherever command identity is the question — determinism tests, the replay
  oracle, and the command fingerprint — and is what a future record-level encoding would use.
- Responses (`R = CommandResponse`) carry the `MutationOutcome`, revision, and the
  `MutationEvent` (in memory only).
- Apply must not use wall clock, randomness, environment, unordered iteration, or I/O.
  Enforced by: `config-core` has no `std::time`, `rand`, `std::env`, or `std::fs` usage in the
  state machine module (checked by a unit test that scans the source) and by replay tests.

## Consequences

- Adding a command type = new `op` byte, and a version bump when incompatible; old voters must
  be upgraded first (§17 rolling upgrade rule).

## Verification

- Round-trip and golden-bytes tests; replay determinism test (identical command sequence →
  byte-identical state hash and identical response sequence).

## Clarifications (2026-09-18, Architect, from test-plan OQ-1, OQ-10)

- Delete envelopes omit `value len` and `value` entirely. Golden: `Delete key=b"a"
  expected=7` encodes to 21 bytes
  `52 43 4D 44 01 00 02 01 00 00 00 61 01 07 00 00 00 00 00 00 00`.
- `KvState::state_hash()` (SHA-256 over `cluster_revision u64 LE`, record count `u64 LE`, then
  each `(key, value, create_revision, mod_revision)` length-prefixed in key order; excludes
  last_applied and membership) ships unconditionally in `config-core`; it is the replay
  determinism oracle. `sha2` is therefore a normal dependency of `config-core`.
