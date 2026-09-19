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

## Note (2026-09-18, fix round): the CN fallback above is opt-in (F-015)

The "(SAN URI …, CN fallback)" in the Decision is now conditional. A Common Name carries no
cluster id, so it cannot be checked against the listener's cluster the way a `retcd://` SAN is
(ADR-0010, ADR-0011); under a CA shared across an organisation that made any CN-only
certificate a principal here, whichever cluster it was minted for, bounded only by the
allowlist. The fallback therefore runs only when the listener sets
`MtlsConfig::allow_common_name_principals` (daemon key `tls.allow_common_name_principals`,
default `false`, logged as `common_name_principals_enabled` at `warn` when enabled). It exists
for CAs that cannot mint URI SANs and is a deliberate narrowing of the cluster binding, not a
default. Everything else here is unchanged: identity still never comes from a request field,
and a certificate asserting a non-client `retcd://` identity is still refused rather than read
as its CN. Covered by M3-88.
