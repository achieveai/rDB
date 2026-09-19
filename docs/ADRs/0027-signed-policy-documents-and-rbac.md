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
  as v9.
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
