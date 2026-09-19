# ADR-0016: Capability reporting

**Status:** Accepted  
**Date:** 2026-09-17  
**Spec:** §21 M1–M3

## Decision

- `config_core::Capabilities { durability: Ephemeral|Persistent|PersistentUnverified, watch_resumption: Unsupported,
  authz: Development|StaticAllowlist, transport_security: Insecure|MutualTls, pagination:
  Unsupported, dedup: Unsupported }`.
- Exposed via `ConfigNode::capabilities()`, in `config-server --capabilities` output, and in
  the health payload.
- `durability=Persistent` is reported only for `RocksStore`, and only once the M2 gate suite
  passes (the M2 suite asserts the value; before the gate the storage reports
  `PersistentUnverified`).

## Verification

- M1 test asserts `durability=Ephemeral`, `watch_resumption=Unsupported`, `authz=Development`
  for the ephemeral, allow-all node.

## Note (2026-09-18, M6)

Two fields grow a variant, both breaking changes to an existing public enum, made deliberately
rather than incidentally: `Authz` gains `SignedPolicy { policy_version: Option<u64> }` alongside
`Development`/`StaticAllowlist` (ADR-0027) — `None` while the node holds no valid signed policy,
matching this ADR's standing rule that a capability which can lie is worse than none; `Pagination`
gains `RevisionPinned { max_pinned: u32, ttl_ms: u64 }`, replacing `Unsupported` (ADR-0029). This
is the first note appended to this ADR; `WatchResumption::Retained` grew the enum at M4
(ADR-0019/0020) without one being added here. `Dedup` grows to `Bounded { window_requests: u32 }` at M5 (ADR-0025) and is reported only once the
`dedup` column family exists and the leader enforces the window, per the same standing rule. Recorded here going forward so a capability-enum change is never
silent.
