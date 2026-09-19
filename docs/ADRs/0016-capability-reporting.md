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
