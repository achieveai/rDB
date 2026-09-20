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
    (As built, 2026-09-19, ruling M6-R21: the refusal also covers a peer that advertises the
    target key as its *primary* — the key it is still signing with — because between the `add`
    sweep and the `use` sweep every peer holds both keys and the sole-key test alone would let
    a removal exile all of them.)
  - Whichever path is implemented, the pinned-source verification finding itself is recorded (one
    line, in the release notes and in this ADR's Notes section once known) so a later reader does
    not have to re-derive which path is live.

### Rotation under partial availability, and secret hygiene

- A rotation performed while exactly one voter is down keeps the cluster available throughout
  (the two-of-three quorum is untouched by a credential change); the down voter is admitted back
  only if its certificate chains to a still-trusted root at the moment it reconnects — a voter that
  was offline for an entire add/use/remove cycle and comes back on the now-removed CA is refused
  with a typed error, not silently allowed back on stale trust.
- Certificate expiry is exposed as `retcd_cert_expiry_seconds{plane}`, computed from the
  injectable clock (not wall-clock reads scattered through the codebase), warning once per plane
  per 30-day threshold crossing rather than once per scrape — a metric that pages an operator every
  fifteen seconds is not actionable. (As built, 2026-09-19: the `subject` label this bullet
  originally carried does not exist. ADR-0026 declares `node_id` and `plane`, and a subject DN is
  exactly what the secret-hygiene bullet below keeps out of a label.)
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

### 2026-09-19 — as-built (dev-rotation)

**Finding A — the gossip keyring path is the implemented one.** The pinned memberlist 0.8.5 does
expose a live keyring: `Memberlist::keyring()` (`memberlist-core-0.8.5/src/api.rs:55`, behind the
`encryption` feature this workspace enables) hands back a `Keyring` whose `insert` / `use_key` /
`remove` / `primary_key` / `keys` (`src/keyring.rs`) are read by the encrypt path per send
(`src/network.rs:123`) and by the decrypt path per receive (`src/network.rs:351`). Mutations
therefore take effect live, with no restart and no reconstruction of the node. The
`gossip.secondary_key` staged-restart fallback described above is **not** implemented, and the
`GossipKeyring { primary, accepted }` capability shape is. The library already enforces two of
this ADR's rules for free — `use_key` requires a prior `insert`, and `remove` refuses the primary
— so rEtcd's own refusal (OQ-61) sits on top of them rather than re-implementing them.

**Finding B — the TLS handshake moved into this crate.** tonic 0.12.3's `ServerTlsConfig` gives no
seam for replacing credentials on a live listener, so `config-grpc/src/server.rs` now runs the
`tokio_rustls` acceptor itself and feeds the accepted streams to tonic as an incoming stream.
Verified against the pinned tonic source and then empirically: `TlsConnectInfo::peer_certs` still
populates, so principal derivation (ADR-0011, ADR-0012) is untouched — the whole `config-grpc`
mTLS integration suite passes unchanged. `TlsMode::apply_server` was **removed** rather than kept
as a no-op: a method that silently did nothing would be a trap for the next person to add a plane.

**Where the rotator lives (ruling M6-R19).** `config_grpc::rotation::TlsRotator`, not the
daemon. It was written in `config-server` first and moved, because `config-server` declares only
`[[bin]]`: nothing in the workspace can depend on it, so the test plan's harness at
`crates/config-testkit/src/rotation.rs` was unbuildable as specified (TA-57). The move is also
the honest placement — everything a rotation manipulates (`CredentialSource`, `Credentials`,
`GrpcPeerTransport`, `MtlsConfig`) is `config-grpc`'s, and rotation belongs beside the acceptor
it rotates. What stayed in the daemon is the schedule: `tls.watch_files_secs` is a configuration
key, and a transport library that spawned its own timer would be a library with an opinion about
a file it was never handed. `TlsFiles { ca, cert, key }` names what to re-read and deliberately
carries no interval.

**`RwLock`, not `ArcSwap`.** The swap is `RwLock<Arc<Credentials>>` plus an `AtomicU64`
generation. A dependency bought for one pointer swap on a path that runs once per rotation is not
worth its supply-chain surface; the read side clones an `Arc` under a read lock, which a handshake
already dwarfs.

**TA-65 deviation — two pollers, one shape.** This ADR and ADR-0027 each specify a poller. They
are implemented as two tasks (`crate::policy::PolicyLoader::spawn_poller`,
`crate::rotation::TlsRotator::spawn_poller`) sharing one shape — `tokio::time::interval` with
`MissedTickBehavior::Delay`, a `biased` select on a shutdown `Notify`, and the blocking read on
`spawn_blocking` — rather than one task doing both. They stop at different points of the teardown:
the policy poller takes the journal gate, while the TLS poller must not replace a listener's
credentials while it is draining.

**Config key name.** `tls.watch_files_secs`, not this ADR's `tls.watch_files`. Every other
interval in the node document carries its unit (`authz.poll_interval_secs`,
`retention.check_interval_secs`), and a bare `watch_files` reads like a boolean. Zero is refused
rather than treated as "disabled"; `ReloadTls` is how an operator rotates on demand.

**Direct `tokio-rustls` / `rustls-pemfile` dependencies.** Both are declared with
`default-features = false` so they cannot select a crypto provider, leaving tonic's choice the
only one in the build. **Residual risk:** a future bump that changes rustls' default provider
selection could produce a process with two providers registered and handshakes failing at
runtime rather than at compile time. What catches it is the mTLS suites —
`crates/config-testkit/tests/m3_client_mtls.rs`, `m3_peer_mtls.rs`, and
`crates/config-grpc/tests/` — none of which can pass without a working handshake.

**The advertised key set is derived, never configured.** `GossipNode::start` fills
`HintExtras::accepted_gossip_keys` from the keyring it has just built, and every keyring mutation
re-advertises through `update_extras` (ruling M6-R18). No caller supplies the value. A node
advertising a set its keyring did not match would make a rotation impossible to follow safely,
since an operator promotes a key precisely because every peer claims to accept it.

**M6-59 refusal shape.** `AdminError::InvalidArgument` with the greppable detail prefix
`gossip_key_still_needed:`, rather than a new `AdminError` variant: that enum is the membership
plane's vocabulary and a gossip keyring is not membership. Callers branch on the prefix.

### 2026-09-19 — as-built (dev-rotation-harness): the in-process rows

`crates/config-testkit/tests/m6_rotation.rs` implements §4.1 (M6-41..M6-48), §4.4
(M6-62..M6-64) and four of §4.3 (M6-57, M6-58, M6-59, M6-61) against this ADR's code. Five
things the rows established that this ADR did not say, and one defect they found.

**OQ-59 is answered by observation, not by an assertion.** No row names an acceptor type. What
the rows read is a fingerprint off a real TLS handshake (`Cluster::served_leaf_fingerprint`,
TA-57), and it agrees with what `ReloadTls` reports and with the `tls_reloaded` log line. The
choice `config-grpc` actually made — `tokio-rustls` accepting into a channel that tonic serves,
with `CredentialSource` read per connection — is therefore recorded here rather than pinned by a
test, which is what M6-56 was for. M6-56 itself is not implemented.

**A reload reports three planes, and the third is the dialler.** `client`, `peer` and
`peer_dial`. Rotating what a node serves without rotating what it dials would leave its peers
disagreeing about who it is the moment the old anchor is dropped, so the three move together
under one RPC (OQ-60's decision, as built).

**A rotation is only safe inside an overlap window, and the rows are written that way.** Every
row that rotates a leaf first widens every node's trust anchors. This is §15.1's procedure; it
is stated here because a reader of `ReloadTls` alone could reasonably conclude that rotating one
node is a local operation, and it is not.

**`Credentials::compile` does not check validity dates.** An expired leaf is *serveable* as far
as the reload path is concerned; it is refused at handshake time by the peer, as
`CertificateError::Expired`. A reload therefore cannot be relied on to catch an operator
deploying an already-expired certificate — what catches that is
`retcd_cert_expiry_seconds` going negative and the `cert_expiring` warning, whose
`days_remaining` is deliberately signed for exactly this reason.

**The CN-principal gate is not re-readable from disk, structurally.** `TlsRotator::read_material`
rebuilds every profile from the `MtlsConfig` template captured at start and replaces only the
three PEM byte fields, so no file write can flip `allow_common_name_principals`. M6-48 asserts
this directly, because a flag a file write could flip would be a file-write privilege escalation.

**Defect found and fixed: `BadSignature` was reported as `handshake_failed`.**
`classify_handshake_failure` (`config-grpc/src/server.rs`) mapped only
`CertificateError::{UnknownIssuer, NotValidForName}` to `AuthnRejectReason::UntrustedClientCa`.
webpki returns `BadSignature` when a presented chain *names* a trusted anchor but is not signed
by it — which is the ordinary shape of a CA rotation, because a re-issued CA normally keeps its
subject DN. The operator-visible fact is "this client's certificate does not chain to anything I
trust", and reporting it as the catch-all `handshake_failed` sends them looking for a protocol
fault instead. `BadSignature` now joins that arm. The gap survived M3 because no row anywhere
asserted the `reason` label for the wrong-CA case; M6-45 and M6-109 now both do, and a mutation
that reverts the fix fails M6-109.

**Duplication to keep in step.** `config-server` has only a `[[bin]]` target, so
`config-testkit/src/rotation.rs` restates `parse_gossip_key` and the
`gossip_key_still_needed:` / `gossip_keyring_refused:` error mapping rather than calling them. A
divergence between the two would show up as M6-59 passing in-process while E2E-43 fails on the
daemon.
