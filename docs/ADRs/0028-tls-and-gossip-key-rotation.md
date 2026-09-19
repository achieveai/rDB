# ADR-0028: TLS and gossip key rotation

**Status:** Accepted
**Date:** 2026-09-18
**Spec:** §15.1, §18.2, §20, §21 M6

## Context

ADR-0010 fixed client and peer transport to mutual TLS with certificates read once at startup;
ADR-0003 fixed gossip to a single static shared key. Neither can be changed without a restart,
which makes routine certificate and key rotation a planned outage. §15.1 and D6.2 require rotation
that a running cluster survives without downtime on either plane. This ADR is the owning decision
for D6.2 and for the lead ruling on the one contradiction that touches it directly (M6-R5), plus
the CA-is-not-revocation deviation the test plan's §15 review raised.

## Decision

### One admin RPC, two credential sources, both planes

- Admin RPC `ReloadTls` re-reads the configured certificate/key/CA-bundle paths for **both** the
  client-plane and peer-plane listeners in one call, reporting a per-plane result
  (`{plane, outcome, subject, not_after}` for each) rather than a single pass/fail — a client-plane
  success alongside a peer-plane failure (or the reverse) must be visible to the caller, not
  collapsed into one boolean.
- `tls.watch_files` (default 30 s, injectable timer, same mechanism as ADR-0027's policy poller)
  re-reads the same paths on a schedule; a file that is mid-write when a poll fires is retried on
  the next tick rather than loaded partially (poll compares a content hash before swapping).
- The peer plane reloads **both** its server credentials (for inbound peer connections) and its
  client credentials (for the connections it initiates to other peers) — the peer plane both
  accepts and dials, and a rotation that only updates one side leaves half of the mesh
  authenticating with the old identity.
- In-flight connections are untouched by a reload — TLS has no live renegotiation here. A reload
  changes what a **new** connection presents and verifies; existing connections keep running under
  the credentials they started with until they naturally reconnect. This is stated explicitly
  because "rotate the certificate" reads like an instant replacement and is not one.

### Certificate resolution is a swappable seam

- Server certificate selection is served from behind a swappable resolver
  (`Arc<ArcSwap<CertifiedKey>>` under a `rustls::server::ResolvesServerCert`), so `ReloadTls` and
  the poller both reduce to one atomic pointer swap with no listener restart and no accept-loop
  interruption. The client-verifier's trusted-root store is rebuilt from the CA bundle the same
  way.
- Whether tonic 0.12's own server builder can host that custom resolver is unresolved at ADR-writing
  time. The design is **implementation-neutral**: if tonic's builder cannot take a custom
  `ResolvesServerCert`, the fallback is a hyper server with a `tokio-rustls` acceptor feeding
  tonic's generated `Routes` directly, which is known to support the seam. Either path satisfies
  the same test rows (TA-57: swappable TLS acceptor observable independent of which HTTP stack
  serves it) and whichever is chosen is recorded once, at build time, in the release notes — the
  test plan does not gate on which implementation is used, only on the externally observable
  swap-without-restart behavior.

### CA rotation is add → use → remove, and it is not revocation

- CA rotation is a three-step, operator-driven sequence: add the new CA to the trusted bundle
  (both old and new now verify), issue and deploy new leaf certificates signed by the new CA, then
  remove the old CA from the bundle once every peer and client has rotated. The overlap window is
  exactly as long as the operator leaves it, deliberately — this ADR does not add a timer that
  forces the remove step.
- **This is add/use/remove, not revocation, and the distinction is deliberate and documented
  rather than assumed:** there is no CRL, no OCSP responder, and no mechanism to invalidate one
  compromised leaf without rotating the whole CA that issued it. For a small private-CA cluster
  (the only deployment shape this project targets), CA-level rotation *is* the incident response —
  a compromised leaf is handled by treating its issuing CA as compromised and rotating it out via
  the same add/use/remove sequence, which is slower than revoking one certificate but requires no
  new machinery. §20's security matrix and ADR-0031 both record this as a standing, explicit gap
  rather than a silent omission — the test-plan review that raised it (a request to confirm CA
  rotation was not being informally described as "revocation") is closed by this paragraph.

### Gossip key rotation: two paths, because the library capability is unverified

- **memberlist 0.8.5's keyring rotation capability (`add_key` / `use_key` / `remove_key`
  equivalents) is unverified at the time this ADR is written (M6-R5).** The architecture brief
  flags this explicitly rather than assuming either answer, and this ADR states both paths so
  implementation is not blocked on the outcome of that verification:
  - **If the pinned memberlist 0.8.5 source exposes a keyring API** (checked first, against the
    pinned dependency source, not against upstream documentation of a different version): gossip
    key rotation follows the identical staged pattern as TLS CAs — `add_key` admits the new key
    for verification, `use_key` switches the primary signing key, `remove_key` retires the old one
    — driven by the same `ReloadTls`-style admin surface, reported via a `GossipKeyring { primary,
    accepted: [..] }` capability field so an operator can see the in-progress state.
  - **If it does not** (or the API cannot be reached from this crate's dependency surface): the
    fallback is a **staged restart with a two-key overlap window**. `gossip.secondary_key` is a
    second config value accepted alongside `gossip.primary_key`; a node with both configured
    verifies messages signed by either key while continuing to sign with the primary. An operator
    rotates a cluster by rolling `secondary_key = <new key>` to every node first (restart required,
    since there is no live keyring), then rolling `primary_key = <new key>` (dropping the old key
    from `secondary_key` or removing it) once every node has the new key as at least a secondary.
    No node is ever without a key that a majority of the cluster also accepts, so the rolling
    restart never partitions the gossip mesh.
  - `remove_key` (in the keyring path) or dropping a key from `secondary_key` (in the fallback
    path) is **refused by default** if the target key is still the only key a known peer accepts,
    naming the peer(s) in the typed refusal, unless the operator passes `--force`. Removing a key
    a peer still needs would silently exile that peer from the gossip mesh — refusing by default
    turns that into a visible, overridable decision rather than a surprise outage.
  - Whichever path is implemented, the pinned-source verification finding itself is recorded (one
    line, in the release notes and in this ADR's Notes section once known) so a later reader does
    not have to re-derive which path is live.

### Rotation under partial availability, and secret hygiene

- A rotation performed while exactly one voter is down keeps the cluster available throughout
  (the two-of-three quorum is untouched by a credential change); the down voter is admitted back
  only if its certificate chains to a still-trusted root at the moment it reconnects — a voter that
  was offline for an entire add/use/remove cycle and comes back on the now-removed CA is refused
  with a typed error, not silently allowed back on stale trust.
- Certificate expiry is exposed as `retcd_cert_expiry_seconds{plane, subject}`, computed from the
  injectable clock (not wall-clock reads scattered through the codebase), warning once per subject
  per 30-day threshold crossing rather than once per scrape — a metric that pages an operator every
  fifteen seconds is not actionable.
- No log line, metric label, health payload, or error message ever contains a private key, a
  gossip key, or raw certificate bytes — fingerprints (SHA-256 of the DER/key bytes) only, the same
  redaction discipline ADR-0003's `GossipConfig` already applies to its shared key, extended here
  to the keyring/secondary-key fields and to TLS material.

## Consequences

- Building the resolution behind a swappable seam costs one extra layer of indirection on every
  TLS handshake (an atomic pointer load) in exchange for restart-free rotation on both planes; this
  is judged worthwhile since a restart-driven rotation was the exact outage class D6.2 exists to
  remove.
- Documenting CA rotation as "not revocation" is an honest limitation, not a workaround: a
  compromised single leaf in a large fleet would be handled far better by real revocation, but this
  project's target deployment (a small private-CA cluster) makes the slower CA-level response
  acceptable, and the alternative (building CRL/OCSP infrastructure) is out of scope for M6.
- The gossip keyring capability being unverified is a real, accepted risk carried into
  implementation: if the pinned memberlist source turns out not to expose the needed API, the
  fallback (staged restart, two-key overlap) is already the design, not a scramble discovered
  during implementation.
- `remove_key`/secondary-key-drop refusing by default trades one extra `--force` flag for
  protection against a silent gossip-mesh partition; an operator who wants the old behavior back
  gets it with one explicit flag, not a config toggle that could be left on by habit.

## Verification

- M6 rows for: `ReloadTls` per-plane result reporting and poll-based reload with a mid-write file
  (M6-41..44); swappable resolver / atomic-swap-without-restart, including whichever of tonic-native
  or hyper+tokio-rustls is chosen (M6-45..47); CA add/use/remove overlap window and post-removal
  refusal of the old CA (M6-48..50); rotation with one voter down and stale-trust refusal on
  rejoin (M6-51..53); gossip keyring or staged-restart fallback path — both branches implemented as
  tests, gated on the verified capability (M6-54..58); `remove_key`/secondary-key-drop default
  refusal and `--force` override (M6-59..61); certificate-expiry metric and secret-redaction
  coverage across logs/metrics/health (M6-62..64).
- Test plan: `docs/testing/test-plan-m6.md` §4 (M6-41..M6-64); E2E-41, E2E-43.

## Notes

None yet.
