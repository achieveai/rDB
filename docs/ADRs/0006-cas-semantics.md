# ADR-0006: Put/Delete/CAS semantics and outcomes

**Status:** Accepted  
**Date:** 2026-09-17  
**Spec:** §7.3, §16, §19.3–4

## Decision

Validation is deterministic and identical in the state machine and at the API edge:

| Request | `expected_mod_revision` | Key state | Outcome |
|---|---|---|---|
| Put | absent | any | `APPLIED` |
| Put | `0` | absent | `APPLIED` (create-only) |
| Put | `0` | present | `CONFLICT {exists=true, current_mod_revision}` |
| Put | `n>0` | present, `mod_revision==n` | `APPLIED` |
| Put | `n>0` | present, `mod_revision!=n` | `CONFLICT {exists=true, current}` |
| Put | `n>0` | absent | `CONFLICT {exists=false, current=0}` |
| Delete | absent | present | `APPLIED` |
| Delete | absent | absent | `NOT_FOUND` |
| Delete | `0` | any | `INVALID_ARGUMENT` (rejected at API edge, never enters the log) |
| Delete | `n>0` | present, `mod_revision==n` | `APPLIED` |
| Delete | `n>0` | present, mismatch | `CONFLICT {exists=true, current}` |
| Delete | `n>0` | absent | `NOT_FOUND` |

- CAS is evaluated against the state immediately preceding the command in committed apply order.
- `CONFLICT` and `NOT_FOUND` are **application outcomes** (gRPC `OK`), not transport errors.
  Conflict exposes only `exists` and `current_mod_revision`, never the value.
- Size caps (key 1 KiB, value 1 MiB, request 2 MiB, list ≤1000 keys / 8 MiB) are validated at
  the API edge → `INVALID_ARGUMENT` / `RESOURCE_EXHAUSTED`, and re-checked deterministically in
  apply. A malformed entry that reached the log yields a deterministic rejection, never a panic.
- Each applied mutation deterministically produces an internal `MutationEvent { revision, key,
  kind: Put{value, create_revision} | Delete }` returned from apply but not retained.

## Verification

- M0 table-driven tests for every row above; concurrent CAS test where exactly one of N
  competing `expected_mod_revision` writers gets `APPLIED`.

## Clarifications (2026-09-18, Architect, from test-plan OQ-2..7)

- `APPLIED` responses carry `exists = (op == Put)` and `current_mod_revision = allocated
  revision`. `NOT_FOUND` carries `exists = false`, `current_mod_revision = 0`.
- Structural violations (empty key, `Delete expected=0`, key > 1 KiB) → `INVALID_ARGUMENT`.
  Size-budget violations (value > 1 MiB, request > 2 MiB, list caps) → `RESOURCE_EXHAUSTED`.
  Empty key is always invalid.
- `List.max_bytes` accounts `key.len() + value.len() + 16` per record. A truncated List still
  returns at least one record (unless the store is empty for that prefix).
- `validate_put / validate_delete / validate_list(req, &Limits)` in `config-core` are the single
  source of truth, shared by the API edge and `KvState::apply`. A log entry that fails apply-time
  validation yields `CommandResponse::Rejected { reason }` (outside `MutationOutcome`), mapped to
  `INVALID_ARGUMENT` at the edge. It allocates no revision.
