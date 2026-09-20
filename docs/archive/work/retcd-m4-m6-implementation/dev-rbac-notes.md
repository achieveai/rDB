# dev-rbac notes — ADR-0027 signed policy documents and RBAC (M6)

> REMINDER: tick the checklist below as each item completes. `[x]` done, `[-]` in progress, `[ ]` not started.

## Lead rulings received (2026-09-19 ~01:10)

1. watch.rs:1102 `saturating_sub(1)` was tester-m4b's in-flight M4-82 mutation check; lead reverted it. Do not touch.
2. `PermissionDenied { policy_changed }` → **additive** `REASON_POLICY_CHANGED` const + constructor instead of a struct-variant field. gRPC maps it to the `retcd-reason` trailer. Amend the dev-rbac line in m6-interfaces.md + dated ADR-0027 note.
3. `crates/config-engine/src/config.rs` is mine (AuthzKind). `metrics.rs` stays dev-dedup's → HealthPayload + policy metrics are a **patch note**; lead hands me metrics.rs later for a metrics round. M6-16/25/34/38 = DEFERRED, not blocked.
4. config-core may take `ed25519-dalek` (verify only) + `serde_json`; extend `m0_59`'s allowlist (that hunk only) with a justification comment; dated ADR-0027 note.
5. `crates/config-server/src/config.rs` held by dev-pagination → `[authz]` section is a patch note until the lead pings.
6. Signature envelope `{envelope_version, key_name, version, hash, signature}` in postcard, check order parse → verify → version binding → hash: APPROVED as an ADR-0027 implementation note.

## Design decisions

### Signature envelope and the four distinct refusal reasons

ADR-0027 requires `signature_invalid`, `untrusted_signer`, `hash_mismatch` and `version_binding` to be
distinguishable. A bare 64-byte detached signature cannot produce that: every failure collapses into
"verify returned false". The envelope carries the signer's **name** and the version/hash the signer
actually committed to, so each refusal has exactly one cause:

| step | failure | reason |
|---|---|---|
| postcard decode / envelope version / 64-byte length | malformed file | `signature_invalid` |
| `key_name` absent from `authz.trust_keys` | valid signature, wrong signer | `untrusted_signer` |
| `verify_strict(hash \|\| version_le)` | corrupt or forged signature | `signature_invalid` |
| `sig.version != doc.version` | relabelled or swapped signature file | `version_binding` |
| `sha256(doc_bytes) != sig.hash` | edited document body | `hash_mismatch` |

Naming the key in the envelope does not weaken anything: verification still runs against the configured
key **bytes**. An attacker who names a trusted key gets `signature_invalid` at the next step.

Ordering note: version binding is checked **before** the hash so that M6-05's "swap two validly-signed
documents' signature files" lands on `version_binding` rather than `signature_invalid`. Both are
refusals; nothing is adopted on either path.

### Reload without touching node.rs

`ConfigNode` holds `Arc<dyn Authorizer>` fixed for its lifetime. Rather than widen that seam,
`SignedPolicyAuthorizer` is **internally mutable** (`RwLock<Option<Active>>`): the same trait object
stays installed and `adopt()` swaps its interior. Reload therefore needs no change in node.rs, run.rs
or the `Authorizer` trait's call signature. `authorize` takes one read lock, which is as cheap as the
trait's "pure and cheap" contract asks for.

### `changed_prefixes` overlap semantics

Grants are keyed by exact prefix string, but a *key* is under a changed prefix when the changed prefix is
a prefix-of **or** prefixed-by the key. The second half is what makes a `List /a/` request notice a grant
change at `/a/b/` — exact-string comparison would let a narrowing edit at a deeper level slip through,
which is the early expansion ADR-0027 forbids.

## Patch notes for files I do not own

### `crates/config-engine/src/metrics.rs` (dev-dedup → me, metrics round)

`HealthPayload` gains, after `policy`:

```rust
    /// Active signed-policy version, `None` under `authz.mode = "static"` or when no valid
    /// document is loaded (ADR-0027, M6-16/M6-38).
    pub policy_version: Option<u64>,
    /// Why there is no active policy, when there is none. `None` when one is active.
    pub policy_state: Option<PolicyState>,
    /// Whether this node was started with `--break-glass-policy-rollback` (ADR-0027, M6-09).
    pub break_glass_active: bool,
```

`PolicyState` is `config_core::policy::PolicyState` (serde `snake_case`): `NoValidPolicy { reason }` /
`Converging { from, to }`. The payload must carry **no** grants, principals or key material (M6-16).

`ConfigNode::health_payload()` fills `policy_version` from `self.authorizer.policy_version()` (new
default-`None` trait method, already landed in config-core) and `ready` gains
`&& self.node_config.authz_kind.is_present()` — `AuthzKind::NoValidPolicy` already returns `false` from
`is_present()`, so readiness follows with no further change.

New Prometheus series in `render_prometheus`:

```
retcd_policy_version              gauge   # active document version, 0 when none
retcd_policy_converged_version    gauge   # version every known voter has reported
retcd_policy_rollbacks_total      counter # break-glass rollbacks performed
retcd_policy_reload_failures_total counter{reason} # closed set = PolicyRejected variants
break_glass_active                gauge   # 1 while the process-scoped flag is set
retcd_watch_terminations_total{reason="policy_changed"}  # already flows from TerminationReason
```

`retcd_policy_reload_failures_total` seeds every `PolicyRejected` discriminant at `0` so an unhit reason
reports `0` rather than being absent (same rule as `PageTokenExpiredReason::ALL`).

### `crates/config-server/src/run.rs` (dev-pagination → lead → me)

```rust
// in load_authorizer(), replacing the single static branch:
match cfg.authz_mode {
    AuthzMode::Static => /* unchanged M3 path */,
    AuthzMode::Signed => {
        let authorizer = Arc::new(SignedPolicyAuthorizer::new(cli.break_glass_policy_rollback));
        match load_signed_policy(&cfg) {            // read both files, verify_policy, adopt
            Ok(adoption) => LoadedPolicy::signed(authorizer, adoption),
            Err(rejected) => {
                tracing::error!(target: "retcd.audit", reason = %rejected, "policy_rejected");
                LoadedPolicy::no_valid_policy(authorizer, rejected)   // AuthzKind::NoValidPolicy
            }
        }
    }
}
```

The poller is one `tokio::spawn` driven by `cfg.authz_poll_interval` and the node's injectable timer
source (never `tokio::time::sleep` directly, so `TestTimers` can advance it). Each tick re-reads the two
paths; a byte-identical document is a no-op (no `policy_loaded` line, no version change); a failed reload
keeps the previously active document and increments `retcd_policy_reload_failures_total`. On a successful
adoption the poller calls, in this order: `hub.on_policy_change(&old, &new)` **then**
`node.set_policy_version(new.version)`. Ordering is load-bearing (M6-28/M6-31) and is the reason
`on_policy_change` must run before anything publishes an event under the new version.

Restore path: after a restore, the client plane stays closed until a valid signed policy is active
(M6-34); `restore_policy_mismatch{manifest_version, active_version}` is logged at `warn` and never blocks
(M6-35).

### `crates/config-server/src/config.rs` (dev-pagination live)

```toml
[authz]
mode = "signed"            # "signed" (M6 default) | "static"
policy_file = "policy.json"
policy_sig_file = "policy.json.sig"
trust_keys = { ops = "ops.pub", ops-next = "ops-next.pub" }
poll_interval_secs = 10
admins = ["..."]           # IGNORED under mode = "signed", logged once as a configuration warning
```

`mode = "signed"` with no `policy_file` or no `trust_keys` is refused **at config load** with a typed
error naming every missing field (M6-37). `mode = "static"` preserves the M3 behaviour byte for byte:
no policy file is read, no poller runs (M6-36).

### `crates/config-gossip` meta (after dev-compat)

`policy_version: Option<u64>` appended to the hint meta, ADVISORY (ADR-0003). It only toggles whether the
converging evaluator is in force; it never decides an access outcome, which is what keeps §19.9 true.
`SignedPolicyAuthorizer::note_converged()` is the only consumer.

### `crates/config-engine/src/node.rs` (dev-dedup)

No change needed for the reload path — see "Reload without touching node.rs" above. The metrics round
will add the `health_payload()` fields listed under metrics.rs.

## Checklist

### config-core
- [x] Cargo.toml: ed25519-dalek + serde_json (lead ruling M6-R9, 2026-09-19)
- [x] m0_purity.rs m0_59 allowlist widened with justification
- [x] policy.rs: PolicyDocument, PolicySignature, SignedPolicy, PolicyRejected, verify_policy
- [x] policy.rs: changed_prefixes (overlap), evaluate_converging (pure)
- [x] policy.rs: SignedPolicyAuthorizer (adopt/rollback/break-glass/no-op/admins/policy_version)
- [x] authz.rs: shared `grants_allow` helper; `Authorizer::policy_version` + `admin_set` defaults
- [x] capabilities.rs: `Authz::SignedPolicy { policy_version }` (rebased after dev-pagination)
- [x] error.rs: REASON_POLICY_CHANGED + `ConfigError::policy_changed()` + `permission_denied_reason()`
- [x] lib.rs exports
- [x] tests/m6_rbac.rs — 19 tests, M6-01..10, 13, 17..24, 39, 40

### config-engine
- [x] config.rs: `AuthzKind::SignedPolicy` / `NoValidPolicy`
- [x] watch.rs: `WatchHub::on_policy_change` (terminate before enqueue)
- [x] watch.rs: admission while converging -> `policy_converging`
- [x] watch.rs: `TerminationReason::PolicyChanged`
- [x] lib.rs export (`PolicyChange`, `PolicyMetrics`)
- [x] tests/m6_rbac.rs — 4 tests, M6-28..31
- [x] tests/common/mod.rs: `formed_with_authorizer` (additive; `start_full` is now the single
      construction point)

### config-grpc
- [x] proto/retcd/v1/admin.proto: `ReloadPolicy` + `PolicyInfo`
- [x] admin_plane.rs: handler, `AdminAllowlist` over static-or-signed source, audit line
- [x] error.rs: closed-set `MACHINE_READABLE_DENIALS` -> `retcd-reason` trailer
- [x] tests/m6_rbac.rs — 8 tests, M6-12, 25, 30 (wire), 40

### config-server
- [x] cli.rs `--break-glass-policy-rollback`
- [x] config.rs `[authz]` signed section + 6 validation unit tests (M6-36, M6-37)
- [x] policy.rs `PolicyLoader` (startup + RPC + poller, revoke-before-adopt ordering)
- [x] run.rs signed-mode wiring, admin allowlist selection, poller lifecycle
- [x] health.rs `Sources` (node + paginator + policy loader)
- [ ] tests/m6_rbac.rs — NOT written; the daemon-level rows are the deferred block below

### metrics round (2026-09-19, after "M4 committed")
- [x] c5b15 patch applied: `authz_denied_admin` on NodeMetrics + node.rs, defaulted
      `AdminBackend::record_authz_denial`, run.rs forwarding, `retcd_authz_denied_total{plane}`
      now two truthful samples
- [x] TA-39 health fields: `compact_revision`, `journal_oldest_revision`,
      `journal_newest_revision`, `journal_hash`, `watch_streams_open` (+ the
      `config-server/tests/support/mod.rs` `Health` mirror)
- [x] `HealthPayload.policy_version` (engine) + `.policy_state` (daemon)
- [x] `MetricsReport.pagination` -> `retcd_pinned_snapshots`
- [x] `MetricsReport.policy` -> `retcd_policy_version`, `retcd_policy_converged_version`,
      `retcd_policy_rollbacks_total`, `retcd_policy_reload_failures_total{reason}`,
      `retcd_break_glass_active`
- [x] ADR-0026 metric table extended; `m5_observability.rs` gained `SIGNED_MODE_ONLY`
- [x] metrics.rs regions 855-884 (dev-dedup's) untouched
- [x] node.rs constraints honoured: `propose_compact` still `dedup_trim_below: None` (M5-R20),
      retention timer's `up_to + 1` untouched

### docs
- [x] docs/runbooks/policy-rotation.md
- [x] docs/runbooks/policy-break-glass.md
- [x] ADR-0027 dated implementation notes (envelope, M6-R8, M6-R9, reload seam, health/metrics split)
- [x] m6-interfaces.md dev-rbac line amended
- [x] test-plan-m6.md file map + §3.9 row-to-test status table

### gate
- [x] clippy -D warnings on config-core/config-engine/config-grpc (--all-targets) and
      config-server (--bins --tests)
- [x] rustfmt clean on my files (`cargo fmt --all -- --check` shows only foreign diffs)
- [x] config-testkit/tests/scan.rs green
- [x] every implemented row 3x green
- [x] mutation checks: signature verification, version monotonicity, break-glass scope, watch
      termination — each reverted, `diff` against the pre-mutation copy clean, residue grep clean
- [x] mutation residue grep clean before handoff

## Deferred / not covered, with reasons

Daemon-level rows M6-11, 14, 15, 16, 20, 26, 27, 38 need a three-daemon harness with a
`PolicyFixture`, a `TestTimers`-driven poll tick and, for two, the restore path. The production
behaviour is implemented; only the process-level assertion is missing. M6-32's token half is
dev-pagination's; M6-33..35 are the backup owner's. Recorded in test-plan-m6.md §3.9.

## Observed in other writers' areas (reported, not touched)

- `crates/config-grpc/tests/m4_watch_wire.rs`: `m4_103_mtls_principal_is_per_stream` hangs past
  60 s. The file carries `eprintln!("[m4_103] DIAG: ...")` lines and was last written 02:53 on
  2026-09-19, so its owner is actively debugging it. Everything else in that target passes.
- `crates/config-testkit/tests/m4_capabilities.rs`: two compile errors (`cannot borrow db as
  mutable`, `use of partially moved value: err`).
- `cargo fmt --all -- --check` is dirty on `crates/config-grpc/tests/m4_watch_wire.rs` and
  `crates/config-testkit/tests/m4_watch_faults_cluster.rs` (comment indentation only).
- `config-server/tests/m5_observability.rs` `m5_105`/`m5_111` failed once on `dedup_hit: false`
  mid-session and passed on a later run; dedup is another worker's area.
