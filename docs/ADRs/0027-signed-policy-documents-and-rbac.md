# ADR-0027: Signed policy documents and distributed RBAC lifecycle

**Status:** Accepted
**Date:** 2026-09-18
**Spec:** §15.3, §19.9, §19.12, §21 M6

## Context

ADR-0012 fixed M0–M3 to a static, deployment-managed allowlist: missing or unparsable policy fails
closed, and identity never comes from a request field. §15.3 replaces that for M6 with a signed,
versioned, distributed RBAC document that every node loads and validates locally, refreshes on a
bounded schedule, and reasons about during a rollout without ever expanding access early. This ADR
is the owning decision for D6.1 and for the six lead rulings on test-plan-m6 §15 contradictions
that touch it (M6-R1 belongs to ADR-0029; M6-R3 belongs here).

## Decision

### Policy document, signature, and version binding

- `PolicyDocument { version: u64, issued_unix_ms, grants: [{ principal, prefix, ops }], admins:
  [principal] }`, serialized as JSON. Loaded from `authz.policy_file` with a detached ed25519
  signature at `authz.policy_sig_file`. `authz.trust_keys` is a **set** of named verifying keys,
  not a single key — signing-key rotation is a config-file edit, not a flag day.
- Hash = `sha256(document bytes)`. The signature payload is `sign(hash || version_le_bytes)` —
  version is bound **into** the signature, not merely alongside it. A validly-signed document
  whose signature-payload version disagrees with the body's `version` field is refused
  (`reason="version_binding"`); without this a signed v5 document could be relabelled and replayed
  as v9. (As built, 2026-09-19: the payload carries no domain-separation tag — it is exactly
  `hash ‖ version_le`, 40 bytes — so a policy trust key MUST NOT be reused for any other rEtcd
  or operator signing surface. Adding a tag later is a flag day for every signed document, which
  is why the constraint is written down rather than retrofitted.)
- A tampered body (`reason="hash_mismatch"`), an untrusted signer (`reason="untrusted_signer"`,
  distinct from a malformed signature so an operator can tell "wrong key" from "corrupt file"), a
  missing signature or document file (`reason="signature_file_missing"` /
  `"policy_file_missing"`), and a structurally invalid signature (`reason="signature_invalid"`)
  are each a distinct, logged, closed-set refusal reason (`policy_rejected`).
- Signing keys and trust-key private halves never appear in a log, metric, or health payload.

### Reload: bounded polling and `ReloadPolicy`

- `authz.poll_interval` (default 10 s, driven by the injectable timer source — this and
  `tls.watch_files`, ADR-0028, are the first production pollers in the codebase) re-reads the
  configured paths. One file change produces exactly one reload; an unchanged file on a later tick
  produces none.
- Admin RPC `ReloadPolicy` reloads immediately without waiting for a poll tick. **The admin set
  that authorizes the call is read from the currently active document, never the incoming one** —
  an incoming document must never be able to authorize its own adoption, which is the natural (and
  wrong) reading if left unstated.
- Rollback (`version <= active`) is refused (`reason="rollback"`) unless the node was started with
  `--break-glass-policy-rollback`. The flag is **process-scoped**: an operator who restarted the
  node to set it already paid the cost of a deliberate action, and a one-shot flag that silently
  re-arms on the next restart is worse. Every break-glass reload is individually audited
  (`policy_rollback{from,to,break_glass:true}` at `warn`, plus an `admin_op` line) and a permanent
  `break_glass_active` gauge and `retcd_policy_rollbacks_total` counter make the state visible for
  as long as it lasts.
- A byte-identical re-write of the active version is a no-op (no `policy_loaded` line, no version
  change) — an idempotent deploy system that touches files unconditionally must not reload forever.
- **A failed reload keeps the previously active policy.** "Fails closed" means *does not adopt*,
  not *forgets what it had*; the opposite behavior turns a typo in a redeployed file into an
  outage. The node stays ready and keeps serving under the last valid document.

### Convergence: the fail-closed intersection is a courtesy, not a boundary

- Each node advertises its active `policy_version` in gossip meta (**advisory**, per ADR-0003)
  and in health metadata (§15.3 "exposes its active policy version in health metadata").
- "Changed prefixes" between two documents are computed by **overlap**, not exact-string equality:
  a key is under a changed prefix if any grant prefix that is a prefix-of or prefixed-by the key
  differs between the old and new document. Exact-string comparison would let a narrowing edit at
  a deeper level slip through undetected, which is precisely the early expansion this clause
  forbids.
- While any known **committed voter** reports a lower or unknown `policy_version`, a request on a
  changed prefix is evaluated against the **intersection**: allowed only if **both** the old and
  the new document allow it. A request on an unchanged prefix is evaluated against the new
  document alone. An unknown voter version (stopped, partitioned) is treated as lagging, never as
  "probably fine" — fail-open on absence is exactly the bug this clause exists to prevent, and the
  cluster keeps serving unchanged prefixes throughout (§19.12: no unbounded block on account of a
  policy rollout).
- The evaluator (`evaluate_converging`) is a pure function of the two documents, the principal,
  the action, and the key — no clock, no I/O, and **no gossip input**. Gossip decides only
  *whether* the converging evaluator is in force; it never decides an access outcome. This is what
  keeps §19.9 ("gossip … never confers authority") true while this narrowing rule operates.
- **Stated plainly, because a reader will otherwise assume otherwise: the intersection is a
  convergence courtesy, not a security boundary.** A forged gossip advertisement claiming a lagging
  voter has already converged can only cause the narrowing to end **early** — it can never grant
  access that neither document grants, because the evaluator itself never expands beyond
  `allowed(old) ∩ allowed(new)` for a changed prefix and never reads gossip to decide an outcome.
  That is a real weakening of the 30-second convergence promise, not of authorization safety, and
  it must be read that way.
- §15.3's "convergence within 30 seconds" is a **provisional operational objective**, the same
  class of planning assumption as §12.2's RPO/RTO figures — it is *recorded* in an evidence
  artifact's `values` (ADR-0031), never asserted as an acceptance line.

### No valid policy: unready, and the peer plane is unaffected

- A node that cannot load or validate any policy is **unready for client and admin traffic**. A
  client request in that state returns `Unavailable` (the node is declining traffic, not making an
  authorization decision) so a caller retries elsewhere rather than giving up.
- The peer (Raft) plane uses its own separate certificate/committed-membership authorization path
  (ADR-0010/0011) and is unaffected: the node still votes, replicates, applies, and may even be
  leader while unready for clients — a policy outage must not become a consensus outage.

### Watches and page tokens under a policy change

- A watch stream whose prefix grants changed between the old and new document **terminates with
  `PermissionDenied{policy_changed: true}` before any event is enqueued under the new version**
  (§15.3 bullets 6–7, §11.1). A watch on an unchanged prefix is not terminated and delivers across
  the version boundary with no gap. A watch attempt on a newly-granted prefix while converging is
  denied (`policy_converging`), not silently queued until convergence completes — "must not expand
  access early" applies to watch admission as much as to reads and writes. This closes the gap M4's
  OQ-33 (renumbered) left open, where the M4-era static allowlist had no revocation path to test at
  all.
- Page tokens carry `policy_version` (ADR-0029). A version change invalidates every outstanding
  token with `PageTokenExpired{reason="policy_version"}`, rejected **before** any key is read.

### Backup, restore, and `authz.mode`

- A backup manifest (ADR-0024) references `policy_version` and the document **hash** — never
  grants, principals, or the document body. That reference is a breadcrumb for a human, not a
  validation input: §15.3 says artifacts "reference, but do not contain or override" the RBAC
  artifact, so the manifest's `policy_version` **cannot be checked** at restore — the independently
  supplied policy may legitimately be older, newer, or unrelated. Restore logs the divergence
  (`restore_policy_mismatch{manifest_version, active_version}` at `warn`) but never blocks on it.
  Refusing to open on a version mismatch would make a restore depend on an artifact the backup does
  not contain, which is always true in practice.
- Restore never opens the client plane without an independently supplied, valid, signed policy
  (§15.3's restore clause); peer-plane formation and replication proceed normally in the meantime.
- `authz.mode = "static" | "signed"`. `"static"` preserves M3's `StaticAllowlist` behavior,
  capabilities, and health output byte-for-byte — no policy file is read, no poller runs, so the M3
  release stays reproducible. `"signed"` activates everything above and requires
  `authz.policy_file`/`authz.trust_keys` at **config load**; a signed-mode node with neither is
  refused at startup with a typed error naming every missing field, rather than starting and
  failing closed on every request, which is an outage disguised as a configuration nicety.
- **The M6 daemon default flips `authz.mode` to `"signed"`**, with a documented upgrade note in the
  release notes (a default that fails closed on a missing file is the correct security default but
  must not surprise an operator upgrading a static-mode M3 deployment); `"static"` remains fully
  supported and fully tested.
- The admin plane stays on the client-plane listener established at M5 (ADR-0023); this ADR adds
  no new listener. §15.1 describes a distinct privileged plane, and M6 is the point ADR-0023's own
  deferred-decision note named as the day this would become mandatory — this ADR records the
  deviation as a **permanent** one rather than splitting the listener, because the signed document
  already carries a stronger admin-identity guarantee (a cryptographically verified `admins` list)
  than a second mTLS port would add on its own. The admin set under `authz.mode = "signed"` comes
  **only** from the signed document's `admins` field; a config-file `admins` list under signed mode
  is ignored and logged once as a configuration warning — otherwise the signature buys nothing,
  since file-write access to the TOML would grant admin without ever touching a signed artifact.

### Capability and embedded-client surface

- `Authz` (ADR-0016) gains `SignedPolicy { policy_version: Option<u64> }` alongside the existing
  `Development` and `StaticAllowlist` variants. A signed-mode node with no valid policy reports
  `SignedPolicy { policy_version: None }` and stays unready — a capability that can lie is worse
  than no capability at all (ADR-0016's own standing rule). This is a breaking public change to an
  existing enum, the same class of ripple `WatchResumption::Retained` caused at M4; see the dated
  note appended to ADR-0016.
- A direct embedded client's principal remains non-forgeable under signed policy exactly as under
  the static allowlist (ADR-0012): there is no request field, header, or builder method that can
  change it after construction.

## Consequences

- The fail-closed intersection can temporarily deny access a fully-converged cluster would grant
  (§15.3 accepts this cost explicitly) and, via a forged gossip hint, can end that narrowing early
  — it can never expand access beyond `allowed(old) ∩ allowed(new)`, but the 30-second convergence
  figure is consequently a planning assumption, not a guarantee, and is documented as such rather
  than gated.
- A process-scoped break-glass flag means every rollback attempted while the flag is set succeeds
  and is individually audited; the residual risk (an operator who forgets to remove the flag) is
  accepted and made visible via the permanent gauge rather than silently time-boxed.
- Keeping the admin plane on the client-plane listener is a permanent deviation from §15.1's
  distinct-plane table; it trades one fewer port/certificate profile/fencing surface for reliance
  on the signed document's `admins` binding as the primary defense.
- Capability-enum growth and the M6 daemon default flip both ripple into every existing capability
  and configuration assertion; both are called out explicitly (here and in ADR-0016) so the churn
  is intentional rather than discovered during the gate run.

## Verification

- M6 rows for: good/bad/untrusted/tampered/rollback-mismatched signature verification (M6-01..06);
  break-glass rollback and its non-stickiness (M6-07..10); polling and `ReloadPolicy` reload paths,
  including a half-written file and a currently-active-document admin check (M6-11..16); the
  intersection's non-expansion and narrowing properties, including a ≥500-case property test and a
  gossip-forgery row that proves the containment property (M6-17..24); unready-for-client-and-admin
  with peer plane unaffected (M6-25..27); watch termination ordering before enqueue and page-token
  invalidation under a policy change (M6-28..32); backup/restore binding, `authz.mode` parity and
  fail-fast config, capability reporting, and admin-set sourcing (M6-33..40).
- Test plan: `docs/testing/test-plan-m6.md` §3 (M6-01..M6-40), §8 audit rows M6-117..119, M6-126;
  E2E-40, E2E-45, E2E-46.

## Notes

## Implementation note (2026-09-19, signature envelope)

The detached signature is an envelope, not a bare 64-byte signature:
`{envelope_version, key_name, version, hash, signature}`, postcard-encoded (ADR-0007). A bare
signature cannot produce the four distinguishable refusals this ADR requires — every failure
collapses into "verify returned false". The envelope names the signer and repeats the version and
hash the signer committed to, so each refusal has exactly one cause:

| step | failure | reason |
|---|---|---|
| decode / envelope version / 64-byte length | malformed file | `signature_invalid` |
| `key_name` absent from `[authz] trust_keys` | valid signature, wrong signer | `untrusted_signer` |
| `verify_strict(sha256(doc) ‖ version_le)` | corrupt or forged signature | `signature_invalid` |
| `envelope.version != document.version` | relabelled or swapped signature file | `version_binding` |
| `sha256(doc_bytes) != envelope.hash` | edited document body | `hash_mismatch` |

Naming the key in the envelope weakens nothing: verification still runs against the configured key
**bytes**, so an attacker who names a trusted key simply reaches `signature_invalid` one step
later. Version binding is checked *before* the hash so that M6-05's "swap two validly-signed
documents' signature files" lands on `version_binding` rather than on `signature_invalid`; both are
refusals and nothing is adopted on either path. Reason tokens are the closed set
`PolicyRejected::ALL_REASONS`, which is also what seeds
`retcd_policy_reload_failures_total{reason}`.

## Implementation note (2026-09-19, lead ruling M6-R8)

Watch termination on a policy change does **not** add a `policy_changed: bool` field to
`ConfigError::PermissionDenied`. That variant is constructed at 30-odd sites across four crates
this milestone did not otherwise touch. Instead the refusal carries the additive closed-set detail
constant `config_core::REASON_POLICY_CHANGED` (`"policy_changed"`), built by
`ConfigError::policy_changed()`, and `config-grpc` maps that token — together with
`policy_converging` and `token_principal` — onto the `retcd-reason` trailer. Clients therefore read
a machine-readable token rather than a bool, and the mapping is a closed allowlist so an ordinary
prose denial never leaks its wording onto the wire. `ConfigError::permission_denied_reason()`
recovers the token from the node's wrapped detail (`node.rs` re-wraps an authorizer's reason as
`principal ... may not ... (reason)`), which is why the extraction is a method on the error rather
than a substring match at each call site.

## Implementation note (2026-09-19, lead ruling M6-R9)

`config-core` takes two new dependencies for this ADR: `ed25519-dalek` (verification only, no
signing) and `serde_json` (the document's canonical text form). Both are pure computation with no
clock, filesystem, network or unordered collection, so ADR-0004's purity rule is preserved; the
`m0_purity::m0_59` dependency allowlist is widened for exactly those two names with a justification
comment. Signing lives entirely outside the process — rEtcd verifies documents, it never issues
them.

## Implementation note (2026-09-19, reload without a node seam)

`ConfigNode` holds one `Arc<dyn Authorizer>` for its lifetime. Rather than widen that seam,
`SignedPolicyAuthorizer` is internally mutable (`RwLock<Option<Active>>`): the same trait object
stays installed and `adopt()` swaps its interior, so a reload needs no change in `node.rs`,
`run.rs` or the `Authorizer` call signature. `authorize` takes one read guard, which keeps the
trait's "pure and cheap" contract.

Ordering for M6-28/M6-31 is enforced at the daemon seam that can see both halves:
`PolicyLoader::attempt` calls `WatchHub::on_policy_change(&old, &new)` **before**
`SignedPolicyAuthorizer::adopt`. Inside the hub the guarantee has three parts — a stream reads the
policy epoch inside the journal gate at registration, `send_event` re-checks that epoch
synchronously before **every** enqueue (this is the hard guarantee, because `on_applied` does not
take the gate), and a `select!` arm exists only for promptness on an idle stream. A stream that
missed two rotations cannot know what the intermediate document changed — a `watch` channel keeps
only the latest value — so it terminates unconditionally, which is the fail-closed direction.

`Authorizer` gained two **defaulted** methods, `policy_version()` and `admin_set()`, rather than
required ones: a model that carries no version must report its absence, not a placeholder, and
defaulting them left `AllowAll` and `StaticAllowlist` untouched.

## Implementation note (2026-09-19, health and metrics split)

`HealthPayload.policy_version` is filled by the engine, which holds the authorizer.
`HealthPayload.policy_state` and the whole `retcd_policy_*` family are filled by the **daemon**,
because only the loader that owns the files knows *why* the last load failed — the same split
already used for `disk_free_bytes` and `cert_expiry_seconds`. Under `authz.mode = "static"` the
policy series are not exported at all rather than exported as zero, so a deployment that never
opted into signed policy cannot be confused on a dashboard with one whose document failed to load.
`docs/testing` records this in `m5_observability.rs`'s `SIGNED_MODE_ONLY` list.

## Implementation note (2026-09-19, lead ruling M6-R14: the M6 default stays `static`)

The decision list above rules that **the M6 daemon default flips `authz.mode` to `"signed"`**.
That is **reversed for M6**, by lead ruling M6-R14, and deferred to the GA cut where it ships with
the upgrade note this ADR already asks for. Three reasons, in order of weight:

1. A fresh daemon started with no `[authz]` section at all would boot **permanently unready** —
   signed mode with no `policy_file` is refused at config load (see above), so the default would
   turn "start the binary and look at it" into a configuration error. That is the right posture for
   a fleet mid-upgrade and the wrong one for a first run.
2. Static mode is *also* fail-closed. It denies every principal it does not name and it denies
   everything when its file is missing or unparsable, so the flip buys signature verification, not
   the difference between open and closed. The security argument for flipping early is weaker than
   the decision list implies.
3. M6-36 requires the M3 release to stay byte-for-byte reproducible under `static`. Keeping it the
   default keeps that row a property of the shipped default rather than of a flag.

Consequence, recorded because it is not free: nothing in the automated suite scrapes a signed-mode
`/metrics` by default. `config-server/tests/m6_rbac.rs::m6_16_health_and_metrics_publish_the_signed_policy`
starts a signed-mode daemon on purpose and asserts the five `retcd_policy_*` / `retcd_break_glass_active`
families by name, which is what keeps `m5_observability.rs`'s `SIGNED_MODE_ONLY` exclusion honest.

## Implementation note (2026-09-19, the authorization model is read at runtime, not latched)

`NodeConfig.authz_kind` records the model a node was **wired** with. For signed mode that is only a
startup snapshot, so `ConfigNode` derives the model in force from its authorizer on every read:
`SignedPolicy` when `policy_version()` is `Some`, `NoValidPolicy` when it is `None`. Readiness, the
health payload, the capability report and the one authorization seam all go through that derivation.

Without it, a node whose startup load failed would deny every request and stay unready for the
lifetime of the process even after the operator fixed the file and the poller adopted it — the
repair for a running, replicating node would be a restart. The watch hub's `authz_ready` latch is
set for signed mode at start for the same reason: the signed authorizer denies every event while it
holds no document, so the latch would add nothing except a state the node could not leave.

Two consequences worth naming:

- The capability report and the policy summary in `/health` both carry the **live** version
  (`AuthzKind::to_capability(authorizer.policy_version())`). A capability that can lie is worse than
  no capability at all (ADR-0016).
- A client request refused because there is no valid document returns `Unavailable`, not
  `PermissionDenied`: the node is declining traffic, not deciding that this principal may not do
  this. The static models' `Missing`/`Invalid` keep M3's `PermissionDenied`, where the operator did
  configure something and got it wrong.

## Implementation note (2026-09-19, the revocation is pre-checked against the outcome)

`PolicyLoader::attempt` revokes before it adopts, which is the ordering this ADR requires — but it
now asks `SignedPolicyAuthorizer::adopt_would_replace` first and skips the revocation entirely when
the document is byte-identical to the active one, or when `adopt` is about to refuse it as a
rollback. The poller re-reads the files every `poll_interval`, so without the pre-check one stale
file left on disk revokes every overlapping watch and takes the journal gate once per tick, for as
long as nobody notices it — a refusal would have become a rolling watch outage. The predicate lives
on the authorizer rather than at the seam so that it and `adopt` cannot drift apart.

## Implementation note (2026-09-19, the convergence baseline is the oldest un-converged document)

`Active.previous` holds the **oldest** document this node has not yet retired, not simply the one
going out of force. Two adoptions inside one convergence window (a 10 s poll against a 30 s
objective makes that ordinary) leave voters spread across all the versions involved, so narrowing
against only the immediately previous document would let a grant introduced two hops ago take
effect while a voter on the oldest still denies it. `note_cluster_min_version` clears the baseline
when every voter has reported, and from then on the document going out of force is the baseline
again.

## How convergence completes (2026-09-19)

`policy_version` is field 2 of ADR-0030's `HintExtras` trailer, so every node advertises the version
it holds in its gossip metadata. The daemon's policy poller drives one convergence pass on the same
tick as the reload: it advertises its own version first, then takes the minimum reported by the
*committed voters* and hands it to `note_cluster_min_version`. A voter whose metadata is missing or
undecodable reports nothing, and nothing sorts below every version, so an unknown voter counts as
lagging and the narrowing continues — fail-closed on absence, as §15.3 requires. The transition is
reported once, which is what makes `policy_converged` exactly one line per node per version.

The advertisement is re-published rather than fixed at gossip start: `GossipNode::update_extras`
edits one slot of the trailer and re-runs the advertisement, so a rotation reaches peers without a
restart (lead ruling M6-R18). A node with gossip disabled has no source of cluster versions and
therefore stays `Converging` until it restarts, which is the documented cost of turning gossip off.

This is a convergence courtesy and not a security boundary: gossip confers no authority (§19.9), so
a forged advertisement can end the narrowing early but can never grant access neither document
grants (M6-23, OQ-56).

## Implementation note (2026-09-19, lead ruling M6-R23: a signed admin name requires a verified caller)

The final review of `feature/m4-m6` found that `AdminAllowlist::permits`
(`crates/config-grpc/src/admin_plane.rs`) matched on `principal.name` alone, identically for a
`Static` and a `Signed` source, while the grant path already ran every principal through
`config_core::authz::is_verified_kind`. Admins skipped the gate that grants do not.

**Ruling: the verified-kind gate applies to a `Signed` admin set, and not to a `Static` one.**

The asymmetry is the point, and it follows from what each source is:

- A **signed** `admins` list is the whole reason this ADR left the admin plane on the client-plane
  listener rather than splitting off a second mTLS port (see above: "the signed document already
  carries a stronger admin-identity guarantee"). Honouring a cryptographically verified admin name
  for a caller the transport never authenticated destroys exactly that argument. Under
  `authz.mode = "signed"` with an insecure listener, every caller arrives as
  `Principal::development()` named `dev`, so a document listing `dev` — or any name an operator
  happens to have chosen — opens the whole admin plane to an unauthenticated caller. The signature
  would be buying nothing, which is the same failure this ADR already refuses for a config-file
  `admins` list under signed mode.
- A **static** `admins` list stays exempt under ADR-0023 ruling 4, unchanged. There, `dev` must
  appear verbatim in `[authz] admins` before an insecure node serves a single admin RPC, so the
  operator has already said out loud that this is a development node. That is a deliberate
  affordance with a local, visible switch; it is not a name a remote document can assert.

The gate is therefore one condition, not two: the admin set's *source* decides, never the
listener's TLS mode. A rule that read "signed, unless the listener is insecure" would make the
security property depend on a second, unrelated fact, and would leave the reader unable to answer
"does a signed admin name bind to an unverified caller?" without also knowing the transport.

Consequence, accepted: signed RBAC and an insecure listener no longer combine to give admin
access. Two existing rows (`m6_25`, `m6_40_the_admin_set_follows_the_active_document`) exercised
that combination and move to the mTLS harness, where the principal carries
`PrincipalKind::Certificate`. This is the better test: it asserts the property the ADR claims
rather than a configuration the project now declares invalid. `is_verified_kind` becomes `pub` in
`config-core` so the admin path can apply the same predicate as the grant path rather than
duplicating it.

The local-cluster script is unaffected: it runs `--dev-allow-all` with `--allow-insecure-dev`
(`scripts/local-cluster.sh`), which is `AllowAll` authorization and never reaches the allowlist.

**Release note owed.** `authz.mode = "signed"` on an insecure listener is now a dead
configuration: the document's `admins` list binds to nobody, because no caller on that listener
carries a verified kind. An operator running that combination loses admin access on upgrade, with
`PermissionDenied` and the existing `not_an_admin` refusal reason. It belongs in the release
notes beside the `authz.mode` default flip this ADR already flags. The remedy is either mTLS
(the supported posture for signed RBAC) or `authz.mode = "static"` with an explicit
`admins = ["dev"]`, which ADR-0023 ruling 4 still honours.

## Implementation note (2026-09-20, the document names its cluster and the floor is durable)

Two properties this ADR assumed but did not hold. Both were found by gap triage, recorded as G-06
and G-09, and both are fixed here.

**A document now names the cluster it was issued for (G-06).** `PolicyDocument` gains
`cluster_id: Option<ClusterId>`, serialised as hex text so the JSON stays hand-reviewable, and
`verify_policy` refuses a document naming a different cluster with the new reason
`cluster_mismatch`. Without it, one ops key trusted by two clusters was enough for a
higher-versioned document issued for cluster B to adopt cleanly on cluster A — every signature
check passing, because every signature check was genuinely valid. The check lives in
`verify_policy` rather than in `adopt` because which cluster a document was issued for is a
property of the document alone, independent of what is already in force, and that is the division
of responsibility this ADR already draws between the two.

The field needed no change to `signature_payload`: the payload covers `document_hash`, and the
hash covers the document bytes, so a new field is already inside what the signature protects. The
wider envelope question is gap G-05 and is deliberately still open; it is a flag day and is not
taken here.

The field is optional and `None` means *legacy-unscoped*: such a document still verifies and still
adopts, so no existing deployment breaks on upgrade, and adopting one logs `policy_unscoped` at
`warn` once per adoption — not once per poll, because a warning that repeats for the life of a
deployment is a warning nobody reads. That line is the only place an operator can learn that this
node would also accept another cluster's document signed by the same key. Re-issuing every
document with a `cluster_id` is owed before the unscoped path can be removed.

**The rollback floor is now durable (G-09).** It was in-memory only, so `adopt` returned
`Adopted { from: None }` at every process start and a restart would accept a signed document the
running node had already refused. Rollback protection therefore lasted exactly as long as the
process. The floor is now a `policy_version_floor` cell in the existing `state_meta` column
family — the same shape as `max_command_schema`, absent-tolerant, absent meaning `0`, so no format
bump and no migration. A restart refuses a document at or below the recorded floor with the new
reason `rollback_floor`, distinct from `rollback` so a refusal caused by durable state is
distinguishable in the log from an ordinary version refusal.

The floor records *the version in force*, not the maximum ever seen, so break-glass moves it down
as a consequence of one rule rather than needing a second mechanism. It is written after a
successful adoption, never before: writing first would raise the floor for a document `adopt` then
refuses. The residual risk is a crash between adoption and write, which leaves the floor one
version stale and re-opens the old behaviour for exactly one restart.

**The comparison is strict, and "at or below" was a bug.** The floor was first written to refuse a
document *at or below* it. That phrasing reads correctly and is wrong, because the version a node
last served is the version its own file still holds: the first thing every healthy restart does is
offer the floor straight back. The inclusive comparison therefore refused it, and a signed-policy
node would have come up `NoValidPolicy` and denied every client call after any restart — a worse
and far more likely outage than the rollback the floor exists to prevent. It was caught by
`e2e_46_daemon_break_glass_rollback_is_audited`, which asserts `break_glass: false` on a restart's
first adoption and so noticed the restart being classified as a break-glass rollback. The live
path has the same shape for the same reason: `adopt` returns `Unchanged` for an identical document
rather than calling it a rollback.

**Window left open by the strict comparison, accepted.** A *different* document carrying the same
version as the floor is accepted at a restart, where a running node would refuse it — `m6_08`
refuses an equal version unless the hash is identical — because the cell records a version and not
a hash. The exact fix is to store `(version, hash)` and refuse `to < floor || (to == floor && hash
!= floor_hash)`, which would mirror the in-memory rule across a restart.

It was not taken, on two grounds. First, it is an inconsistency between the two paths rather than
an extra capability: every document is signature-checked before the floor is consulted at all, so
reaching this window already requires the signing key, and anyone holding that key can issue
`floor + 1` with any content they like and need not wait for a restart. Second, the argument for
doing it *now* was that the cell is new and changing its shape later would cost a migration — but
`state_meta` cells are absent-tolerant by construction, which is the same property that let this
floor be added without a format bump in the first place. A later `policy_version_floor_hash` cell
would read absent as "only the version is known" and fall back to the strict comparison, so the
later cost is the same as the cost now. Closing it is therefore a free choice at any time, and was
deferred rather than rejected.

A node whose store cannot hold a floor at all — `StorageHandle::Ephemeral`, and any future variant,
since the enum is `#[non_exhaustive]` and the mapping fails closed to "cannot" — keeps the previous
in-memory behaviour unchanged. That is stated rather than hidden: with no durable store there is
nowhere to put the floor, and inventing one would claim a guarantee the node cannot keep.

**Known limit, carried rather than closed: a restore resets the floor (G-13).** `state_meta` is
not carried in a snapshot body, so a directory restored from a backup starts at floor `0` and an
old signed document adopts. G-09 ships with that bypass. It is recorded as a separate gap, with
the fix being to seed the floor from the backup manifest's `policy_version_ref` at restore, and it
is called out prominently at `RocksStore::policy_version_floor` so it is read by anyone relying on
the floor as a security control rather than only by anyone reading this ADR.

**Exposure accepted: an unreadable floor does not stop the node (lead ruling, 2026-09-20).** If
reading the cell fails, the node logs `policy_floor_unreadable` at `error` and starts anyway. For
that boot, and only if an attacker can also present a validly signed older document, the older
document will adopt. One boot, requires the signing key, logged loudly.

The alternative considered was to treat an unreadable floor as `u64::MAX`: refuse every document
that boot, so the node starts unready and denies client calls while still replicating. That fails
closed on authorization without failing closed on consensus, and it is a one-line change in
`PolicyLoader::new`. It was not taken for two reasons. Operationally it produces a very confusing
failure — a node refusing its current, valid, correctly-signed policy because of an unrelated disk
read. More seriously, the failure mode is not independent across nodes: a disk fault is, but a
*code* fault in the read path — a decode change, a new format, a bug — is not, and the strict
option would then refuse every document on every node at once and take the whole cluster's client
plane down, triggered by a read that has nothing to do with consensus. A security control that
converts its own bugs into a cluster-wide outage is a bad trade for closing a one-boot window that
already requires the signing key.

This is a judgement call held loosely. It is recorded at this length so the next person revisits
it on the argument rather than rediscovering it.

**Metric owed.** An `error` log line is thin for a security control that has silently weakened.
The right surface already exists and the precedent is exact: `PolicyMetrics::break_glass_active`
is a gauge rather than a log line precisely because it disarms rollback protection for the life of
the process. An unreadable floor has the same shape for the life of the boot, so it belongs beside
it as a `PolicyMetrics` field. It is not added here because `config-engine` was owned by another
change in this wave.
