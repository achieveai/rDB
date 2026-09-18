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
  serde on `D`/`R` under its `serde` feature; the **canonical bytes** (used for determinism
  tests and for on-disk log storage in M2) come from `Command::encode()`.
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
