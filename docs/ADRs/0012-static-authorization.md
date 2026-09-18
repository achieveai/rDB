# ADR-0012: Principal derivation and static allowlist

**Status:** Accepted  
**Date:** 2026-09-17  
**Spec:** §6.2, §15.2

## Decision

- `Principal { name: String, kind: Certificate|Peer|Embedded|Development }` is derived **only** from the mTLS
  client certificate (SAN URI `retcd://<cluster_id>/client/<name>`, CN fallback) on the
  client plane, or supplied at construction of `DirectClient` by the embedder via
  `ConfigNode::direct_client(Principal)`. Request fields never carry identity.
- `Authorizer` trait in `config-core`: `fn authorize(&self, p: &Principal, action: Action,
  key_or_prefix: &[u8]) -> Decision`. Implementations: `StaticAllowlist` (from config file) and
  `AllowAll` (development only; capabilities report `authz=Development`).
- Allowlist grammar (TOML): `[[grant]] principal="svc-a" prefix="/app/a/" access=["read","write"]`.
  A `List` requires `read` on the requested prefix; the requested prefix must be within a granted
  prefix (not just overlap) so listing cannot enumerate unauthorized keys.
- Missing or unparsable policy → node is unready for client traffic (fail closed).
- Values and credentials are never logged; audit records carry principal, action, key (hex,
  truncated), outcome.

## Verification

- M3: unlisted principal → `PERMISSION_DENIED`; wrong prefix → `PERMISSION_DENIED`; missing policy
  file → readiness false.
