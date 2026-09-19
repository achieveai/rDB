# Test Plan — M6

**Status:** Proposed (Tester Planner deliverable)
**Date:** 2026-09-18
**Scope:** M6 — production hardening: signed distributed RBAC lifecycle; certificate and
gossip-key rotation; revision-pinned pagination; mixed-version upgrades and migrations; and the
reproducible **dev-host evidence** set (watch capacity, RPO/RTO, partition matrix, crash matrix,
security matrix, gossip-authority proof).
**Authority:** `docs/DesignSpec-01.md` §15.3 (all), §16 (`PageTokenExpired`), §17 (all), §18,
§19 invariants 9, 10 and 12, §20 (**all** subsections: consensus/storage, network/consistency,
watches, gossip/identity, operations), §21 M6; architecture brief
`.claude/scratchpad/conversation_memories/retcd-m4-m6-implementation/architecture-m4-m6.md`
§D6.1–D6.5, "Cross-cutting", **Amendments A7 and A8**, and the **"Lead rulings"** block at the
end of the brief (rulings override D6.x and the amendments where they differ); research note
`.claude/scratchpad/conversation_memories/retcd-m4-m6-implementation/openraft-research.md` §7
(version skew) and §8 traps T1–T15. ADRs 0007–0026 remain in force; ADRs **0027** (RBAC
lifecycle), **0028** (rotation), **0029** (pagination), **0030** (mixed-version) and **0031**
(evidence policy) are the owning ADRs for the decisions below.

**User ruling (2026-09-18, HITL), which this plan is written against:** M6 means *every feature
implemented*, and capacity / RPO / RTO / fault matrix delivered as **reproducible DEV-HOST
evidence rows** that write JSON under `docs/evidence/`. Evidence is never a production claim.
Every §7 row records numbers; **no** §7 row gates on a threshold.

**Companion:** `docs/testing/test-plan-m0-m1.md`, `docs/testing/test-plan-m2-m3.md`,
`docs/testing/test-plan-m4.md` and `docs/testing/test-plan-m5.md`. This document **extends** all
four. TA-1..TA-27, the `Cluster` harness API, the conformance scenario list C-01..C-15, the watch
scenarios W-01..W-12, the anti-flake rules 1..31 and the DuckDB queries Q1..Q26 are still in
force and are not restated.

> **Numbering note (read this first; updated per M6-R6).** `docs/testing/test-plan-m4.md` and
> `docs/testing/test-plan-m5.md` were originally written concurrently and **collided** on two
> identifiers: both allocated a `TA-40` (M4: "`format_version` 2 and the `events` column family";
> M5: "six new fault boundaries") and both allocated an `OQ-40` (M4: "where does the
> `journal_gate` live"; M5: "does `Boundary::ALL` stay one enum"). That collision is resolved
> (§15 item 13; lead ruling M6-R6, `architecture-m4-m6.md`): `test-plan-m5.md`'s TA/OQ identifiers
> were shifted by one — **TA-40..52 → TA-41..53**, **OQ-40..53 → OQ-41..54** — so M4's TA-40/OQ-40
> are now unambiguous and M5's former TA-40/OQ-40 are TA-41/OQ-41. `docs/testing/test-plan-m4.md`
> is unchanged. M6 itself starts **after the highest identifier used anywhere in either plan**:
> **TA-54**, **Q-27**, **OQ-55**, row prefix **M6-01**, E2E prefix **E2E-40**. M4 used TA-28..40,
> Q14..Q18, OQ-26..OQ-40, E2E-20..E2E-29; M5 now uses TA-41..TA-53, Q-20..Q-26, OQ-41..OQ-54,
> E2E-30..E2E-39.

Where this plan and the spec/ADRs disagree, the spec/ADRs win and this plan is a defect — except
for the items listed in §15, which are places where the spec, the brief, the research note and
the shipped code disagree with *each other*.

**How to use this document**

- Developers: §1 and §10 are contracts on the production code and the harness. Code that does not
  expose these seams is not done, because §3–§9 cannot be written against it.
- Testers: §3–§9 are the backlog. One row = one test (or, in §7, one evidence-producing run). The
  row ID must prefix the test name (`m6_17_intersection_never_expands_early`), because §11's
  queries and §13's gate mapping both work by string match.
- Both: §14 lists the open questions. Each has a **default**; the default is what you implement if
  the Architect does not answer before you need it. Record the answer in the owning ADR.
- §13 is the gate map. M6 is the **last** milestone, so a §20 bullet with no owner here has no
  owner at all; each such bullet is marked explicitly rather than left silent.

**Section map** (same roles as the M5 plan): harness surface = §10; DuckDB queries = §11;
anti-flake = §12; gate checklist = §13; open questions = §14; contradictions = §15.

**File mapping (ADR-0014 §6 — gates map 1:1 to §21 bullets)**

| Area | Path |
|---|---|
| Policy document unit + signature tests | `crates/config-core/tests/m6_rbac.rs` |
| M6 RBAC lifecycle gates | `crates/config-engine/tests/m6_rbac.rs`, `crates/config-grpc/tests/m6_rbac.rs`, `crates/config-server/src/config.rs` unit tests |
| M6 rotation gates (TLS, peer, gossip key, expiry) | `tests/m6_rotation.rs` |
| M6 pagination gates | `tests/m6_pagination.rs`, `crates/config-storage/tests/m6_store_pin.rs` |
| M6 mixed-version gates | `tests/m6_mixed_version.rs` |
| M6 evidence rows | `tests/m6_evidence.rs` |
| M6 logging/redaction gates | `tests/m6_observability.rs` |
| Process-level E2E | `crates/config-server/tests/e2e_daemon.rs` (TA-25 — **not** workspace `tests/`) |
| Harness | `crates/config-testkit/src/{policy.rs, rotation.rs, pagination.rs, schema.rs, capacity.rs, matrix.rs, evidence.rs}` |
| Runbooks | `docs/runbooks/{policy-rotation,credential-rotation,rolling-upgrade,pagination}.md` |
| Evidence | `docs/evidence/{watch-capacity,rpo-rto,partition-matrix,crash-matrix,security-matrix,gossip-authority}.json` |
| Test logs | `target/test-logs/<testModule>/<testMethod>.jsonl` |

---

## 1. Test-architecture requirements (TA-54 …)

Requirements on the **production code and harness**, not on the tests. "Must" is normative. A
review may reject a PR by number.

### TA-54 — `PolicyFixture`: signed policy documents are a harness primitive

Every §3 row needs a document, a signature and a trust key it can bend. The fixture owns an
ed25519 keypair set and produces the exact byte artifacts the daemon loads from disk.

```rust
pub struct PolicyFixture;                      // deterministic from a seed, like TlsFixture
impl PolicyFixture {
    pub fn new(seed: u64) -> Self;
    pub fn trust_key(&self, name: &str) -> VerifyingKeyPem;   // named keys; several may be trusted
    pub fn doc(&self, version: u64) -> PolicyBuilder;
    /// Writes <dir>/policy.json + <dir>/policy.json.sig, returns both paths.
    pub fn write(&self, dir: &Path, doc: &PolicyDocument, signer: &str) -> (PathBuf, PathBuf);
    /// Corruptions, each producing exactly one defect:
    pub fn write_unsigned(&self, dir: &Path, doc: &PolicyDocument) -> (PathBuf, PathBuf);
    pub fn write_signed_by_untrusted(&self, dir: &Path, doc: &PolicyDocument) -> (PathBuf, PathBuf);
    pub fn write_tampered_body(&self, dir: &Path, doc: &PolicyDocument) -> (PathBuf, PathBuf);
    pub fn write_version_hash_mismatch(&self, dir: &Path, doc: &PolicyDocument) -> (PathBuf, PathBuf);
}
pub struct PolicyBuilder;                      // grant(principal, prefix, ops), admin(principal)
```

Rules:

1. `PolicyDocument` is the shipped type, not a test double. A row that hand-rolls JSON tests the
   test, not the loader.
2. The signature payload binds **both** the document hash and the version
   (`sign(sha256(bytes) || version_le_bytes)`, D6.1). `write_version_hash_mismatch` re-signs a
   payload whose version field disagrees with the body's `version`, which is the only way M6-05
   can exist.
3. Signing keys never appear in a log, a metric or a health payload (M6-125).

### TA-55 — Policy state is observable at three independent surfaces

A row that reads the active policy out of the engine proves nothing about what a client sees.
Three surfaces, all required:

```rust
impl Cluster {
    pub fn policy_version(&self, id: NodeId) -> Option<u64>;      // from the node's health payload
    pub async fn reload_policy(&self, id: NodeId, principal: &str) -> Result<PolicyReloaded, ConfigError>;
    pub fn policy_state(&self, id: NodeId) -> PolicyState;        // Active{version} | NoValidPolicy{reason}
    pub fn advertised_policy_version(&self, observer: NodeId, about: NodeId) -> Option<u64>; // gossip meta
}
pub struct PolicyReloaded { pub from: Option<u64>, pub to: u64, pub break_glass: bool }
```

`HealthPayload` gains `policy_version: Option<u64>` and `policy_state` (§15.3: "exposes its
active policy version in health metadata"). `advertised_policy_version` reads the **gossip meta**
a peer actually received, not the source node's own value — otherwise M6-17..M6-24 assert a local
variable against itself.

### TA-56 — The intersection evaluator is a pure, separately testable function

Convergence (§15.3, D6.1) is where fail-closed goes wrong quietly. The decision must not be
smeared across the request path.

```rust
pub struct PolicyPair { pub old: Arc<PolicyDocument>, pub new: Arc<PolicyDocument> }
/// Returns the set of prefixes whose grants differ between `old` and `new`.
pub fn changed_prefixes(pair: &PolicyPair) -> BTreeSet<Prefix>;
/// The evaluator used while any known voter lags. Pure: no clock, no IO, no gossip.
pub fn evaluate_converging(pair: &PolicyPair, p: &Principal, a: Action, key: &[u8]) -> Decision;
```

Rules:

1. `evaluate_converging` must be `Decision::Allowed` only if **both** documents allow — for keys
   under a changed prefix. For an unchanged prefix it is the new document alone.
2. It takes no gossip input. Gossip decides *whether* the converging evaluator is in force
   (TA-55); it never decides an access outcome. This is what keeps §19.9 true while D6.1's
   narrowing rule operates (§15 item 3).
3. A property test (M6-24) over generated document pairs asserts
   `allowed(evaluate_converging) ⊆ allowed(old) ∩ allowed(new)` for every principal/key. "Never
   expands early" is a set-containment claim and must be tested as one.

### TA-57 — A swappable TLS acceptor, stated implementation-neutrally

D6.2 wants a `rustls::ServerConfig` with `ResolvesServerCert` over an
`Arc<ArcSwap<CertifiedKey>>` and a client verifier whose root store is rebuilt on reload, and it
records a fallback (hyper + `tokio-rustls` acceptor feeding tonic's `Routes`) if tonic 0.12
cannot host a custom acceptor. **Every §4 row is written against the observable behaviour, never
against the acceptor type**, so both implementations satisfy them:

```rust
pub trait CredentialSource: Send + Sync {          // one seam, both planes
    fn current(&self) -> Arc<Credentials>;          // leaf + chain + CA bundle
    fn reload(&self) -> Result<Reloaded, ReloadError>;
}
pub struct Reloaded { pub leaf_fingerprint: [u8;32], pub ca_fingerprints: Vec<[u8;32]>, pub changed: bool }
impl Cluster {
    pub async fn reload_tls(&self, id: NodeId, principal: &str) -> Result<Reloaded, ConfigError>;
    pub fn rotate_files(&self, id: NodeId, next: &CertProfile);   // rewrite the files on disk only
    pub fn served_leaf_fingerprint(&self, id: NodeId) -> [u8;32]; // observed from a *new* TLS handshake
}
```

`served_leaf_fingerprint` must be obtained by completing a real handshake and reading the peer
certificate, not by asking the node what it thinks it is serving (anti-flake rule 34). A row that
cannot distinguish "reloaded" from "restarted" is not a rotation row: every §4.1 row asserts the
process's start instant and PID are unchanged and that `retcd_process_start_time_seconds` did not
move.

### TA-58 — The gossip keyring is a keyring, and the harness stages it

`crates/config-gossip/src/config.rs` today has `secret_key: Option<[u8; 32]>` — a single key, so
staged rotation is not expressible. M6 replaces it with an ordered keyring (primary + accepted),
matching memberlist's `add_key` / `use_key` / `remove_key`:

```rust
pub struct GossipKeyring { pub primary: [u8;32], pub accepted: Vec<[u8;32]> }
impl Cluster {
    pub async fn gossip_add_key(&self, id: NodeId, key: [u8;32], principal: &str) -> Result<(), ConfigError>;
    pub async fn gossip_use_key(&self, id: NodeId, key: [u8;32], principal: &str) -> Result<(), ConfigError>;
    pub async fn gossip_remove_key(&self, id: NodeId, key: [u8;32], principal: &str) -> Result<(), ConfigError>;
    pub fn gossip_keyring(&self, id: NodeId) -> GossipKeyring;    // fingerprints only in Debug
}
```

`Debug`/`Display` for `GossipKeyring` prints key **fingerprints**, never bytes — the existing
`GossipConfig` redaction (`config.rs:92-103`) extends to the keyring, and M6-125 asserts it.

### TA-59 — Pagination is observable: token internals, pin registry, and a forger

```rust
pub struct PageTokenView {           // harness-only decode; production never exposes this
    pub revision: u64, pub last_key: Vec<u8>, pub policy_version: u64,
    pub issued_ms: u64, pub node_id: NodeId, pub token_version: u8, pub mac: [u8;32],
}
impl Cluster {
    pub fn decode_token(&self, t: &PageToken) -> PageTokenView;
    pub fn forge_token(&self, view: PageTokenView, key: Option<&[u8]>) -> PageToken; // None = wrong key
    pub fn pins(&self, id: NodeId) -> PinRegistry;
}
pub struct PinRegistry;
impl PinRegistry {
    pub fn len(&self) -> usize; pub fn capacity(&self) -> usize;
    pub fn evictions(&self) -> u64; pub fn expiries(&self) -> u64; pub fn hits(&self) -> u64;
    pub fn misses_by_reason(&self) -> BTreeMap<&'static str, u64>; // mac|expired|evicted|node|policy
}
```

Rules:

1. `decode_token` is `#[cfg(feature = "testing")]`. If a production caller can decode a token, the
   token is not opaque and §10.2's "authenticated continuation token" is a fiction.
2. `PinRegistry::misses_by_reason` keys are the **same** closed set the `page_token_rejected` log
   line uses (Q-30), so the counter and the log cannot drift.
3. The TTL is driven by `TestTimers`, never by wall clock (anti-flake rule 33).

### TA-60 — Schema advertisement, `cluster_min_schema`, and `--compat-schema`

Per D6.4 and **A7** (the gate is at **propose** time on the leader, because postcard is not
self-describing and a committed unknown variant cannot be tolerated at apply).

```rust
pub struct SchemaTriple { pub format_version: u32, pub command_schema: u32, pub proto_rev: u32 }
impl Cluster {
    pub fn schema(&self, id: NodeId) -> SchemaTriple;                       // health
    pub fn advertised_schema(&self, observer: NodeId, about: NodeId) -> Option<SchemaTriple>; // gossip meta
    pub fn peer_header_schema(&self, from: NodeId, to: NodeId) -> Option<SchemaTriple>;       // AppendEntries header
    pub fn cluster_min_schema(&self, leader: NodeId) -> u32;                 // committed voters only
    pub fn feature_activated(&self, id: NodeId) -> bool;
}
```

`config-server` gains `--compat-schema <n>`: a v2 binary that advertises and emits only schema
`n`, and refuses to decode an envelope above `n` with the typed error. This is what makes every
§6 row runnable on **one** binary; a plan that requires building an actual v1 artifact makes the
mixed-version gate unrunnable in CI and therefore untested.

`cluster_min_schema` must be computed from the **committed voter set**
(`metrics.membership_config.nodes()`), not from gossip and not from the set of live connections —
research §7 is explicit that a gossip-derived level lets two leaders compute different levels.

### TA-61 — The evidence contract: one file, one row, one schema, stated once

Extends TA-53 (renumbered per M6-R6). Every file under `docs/evidence/` is written by **exactly one** row, and every
file has this shape:

```json
{
  "schema": 1,
  "name": "watch-capacity",
  "host":  { "hostname": "…", "os": "…", "cpu_model": "…", "cpu_cores": 0,
             "ram_bytes": 0, "disk_class": "nvme|ssd|hdd|unknown" },
  "build": { "git_sha": "…", "dirty": false, "profile": "release", "rustc": "…" },
  "run":   { "utc": "2026-09-18T00:00:00Z", "duration_ms": 0, "seed": 0,
             "scale_factor": 1.0, "full_scale": true },
  "values": { "…": 0 },
  "disclaimer": "Dev-host evidence. Not a production claim; production designation requires re-running on target hardware (spec §20, §12.2)."
}
```

Rules:

1. `scale_factor` is mandatory. It is `1.0` **iff** the row ran at the full stated scale;
   `full_scale` is a derived boolean so a reader cannot miss it. A row that scales down and writes
   `1.0` is a review rejection, and M6-115 is the row that proves the field tracks reality.
2. `disclaimer` is a fixed constant emitted by `write_evidence`, not typed per row (M6-116).
3. **Gating rule and its default.** Evidence rows are **not** `#[ignore]`d. They run in the
   ordinary gate at **reduced scale** (`RETCD_EVIDENCE` unset or `0` → `scale_factor < 1.0`,
   default reduction per row in §7) and at **full scale** when `RETCD_EVIDENCE=1`. A row that
   cannot run at reduced scale in under its §2 budget is a defect in the row, not a reason to
   ignore it. The M6 gate is green only when the full-scale artifacts exist (M6-114).
4. File ownership, to keep TA-53's one-file-one-row (renumbered per M6-R6) rule true across milestones: M5-93 keeps
   `docs/evidence/backup-restore.json` (M5 scale); M6-106 owns the **new** file
   `docs/evidence/rpo-rto.json`. No M6 row overwrites an M5 artifact.

### TA-62 — Capacity harness: 1,000 streams, three populations, server-side counters

```rust
pub struct WatchLoad { pub healthy: usize, pub slow: usize, pub disconnecting: usize }
impl Cluster {
    pub async fn spawn_watch_load(&self, id: NodeId, load: WatchLoad, cfg: LoadCfg) -> LoadHandle;
    pub fn rss_bytes(&self, id: NodeId) -> u64;      // process RSS, sampled
    pub fn apply_latency(&self, id: NodeId) -> LatencyView;   // p50/p99/max, from the scraped histogram
}
pub struct LoadCfg { pub prefixes: usize, pub write_rate: u32, pub slow_drain_ratio: f64,
                     pub disconnect_every: u32 }
pub struct LoadHandle { /* join, and per-population termination reasons */ }
impl LoadHandle {
    pub fn terminations_by_reason(&self) -> BTreeMap<&'static str, u64>;
    pub fn delivered(&self) -> u64; pub fn gaps_observed(&self) -> u64;
}
```

Stream counts, queue bytes and terminations come from the **server's** scraped counters (TA-34,
TA-48, renumbered per M6-R6); the client-side view is a cross-check, never the oracle (anti-flake rule 39). `rss_bytes`
is sampled on a `TestTimers` tick, not by sleeping.

### TA-63 — Matrix drivers enumerate; they do not hand-list

Three enumerators, so that adding a `Boundary` or a partition shape cannot silently skip a case:

```rust
pub fn partition_arrangements(ids: &[NodeId]) -> Vec<Partition>;  // every 3-node arrangement, §20
pub fn crash_cases() -> Vec<Boundary>;                            // == Boundary::ALL, M5's 16
pub fn security_cases() -> Vec<SecurityCase>;                     // the §20 "Gossip and identity" list
pub enum SecurityCase {
    WrongNodeId, WrongClusterId, WrongCertIdentity, WrongDestinationBinding,
    StalePackets, PoisonedEndpoint, DuplicateNodeId, GossipKeyRotation,
    AllSeedsUnavailable, VersionSkew, FalseSuspicion, OneWayLoss,
}
```

`crash_cases().len() == Boundary::ALL.len()` and `security_cases().len() == 12` are asserted
(M6-108, M6-109); a new variant fails the assertion until its matrix entry exists. The partition
enumerator must produce **every** arrangement of 3 nodes — the three 1|2 splits, the three
one-way pairs in each direction, and full isolation of each node — and M6-107 asserts the count
against a closed-form expression, not a literal.

### TA-64 — Certificate expiry is a clock-injectable metric

`retcd_cert_expiry_seconds{plane, subject}` is computed against an injectable clock
(`TestTimers`), so M6-62..M6-64 can stand 29 days from expiry without waiting. The 30-day warning
is a log line `cert_expiring{plane, subject, days_remaining}` at `warn`, emitted once per
threshold crossing per subject, not once per scrape.

### TA-65 — Nothing in M6 sleeps, including the two new pollers

`authz.poll_interval` (10 s) and `tls.watch_files` (30 s) are the first *production* pollers in
the codebase. Both must take their interval from the injected timer source, and both must expose
`poll_ticks()` so a test can advance and then await the tick's completion via `Notify`. A test
that waits 10 s for a policy reload is banned by anti-flake rule 1 and would put the M6 suite
over its §2 budget on its own.

### TA-66 — Capability enum growth is a deliberate, asserted break

`crates/config-core/src/capabilities.rs` today has `Authz::{Development, StaticAllowlist}` and
`Pagination::{Unsupported}` only. M6 adds `Authz::SignedPolicy { policy_version: Option<u64> }`
and `Pagination::RevisionPinned { max_pinned: u32, ttl_ms: u64 }`, and `Capabilities` gains
`schema: SchemaTriple`. This ripples into every existing capability assertion exactly as
`WatchResumption::Retained` did in M4 (M4 §11 item 10). M6-38, M6-73 and M6-87 own the update; the
break is recorded in ADR-0016's clarifications so it is intentional rather than discovered during
the gate run.

---

## 2. Taxonomy and budgets (extends §2 of the M5 plan)

| Layer | Runner | Fault tools | Per-test budget |
|---|---|---|---|
| Policy document unit | `cargo test -p config-core` | `PolicyFixture` | < 5 s |
| M6 RBAC lifecycle | `*/tests/m6_rbac.rs` | `PolicyFixture`, `GossipControl`, `NetFault` | < 30 s |
| M6 rotation | `tests/m6_rotation.rs` | `TlsFixture`, `CredentialSource`, `GossipKeyring` | < 45 s |
| M6 pagination | `tests/m6_pagination.rs` | `PinRegistry`, token forger, `NetFault` | < 20 s |
| M6 mixed-version | `tests/m6_mixed_version.rs` | `--compat-schema`, golden bytes | < 45 s |
| M6 evidence (reduced scale) | `tests/m6_evidence.rs` | every fault tool | < 120 s |
| M6 evidence (`RETCD_EVIDENCE=1`) | `tests/m6_evidence.rs` | every fault tool | < 30 min total, not per row |
| M6 observability | `tests/m6_observability.rs` | scrape, JSONL reads | < 15 s |
| E2E daemon (E2E-40..) | `crates/config-server/tests/e2e_daemon.rs` | process spawn, CLI, file rewrite | < 120 s per test |

Hard ceilings: **no non-evidence in-process test may exceed 45 s**; **no E2E test may exceed
150 s**; the whole M0..M6 suite must finish in under **50 minutes** on the dev host with
`RETCD_EVIDENCE` unset. The full-scale evidence run is a separate invocation and is budgeted at
**90 minutes** end to end.

Reduced-scale defaults (used when `RETCD_EVIDENCE` is unset), each recorded as `scale_factor`:

| Evidence row | Full scale | Reduced scale | `scale_factor` |
|---|---|---|---|
| M6-105 watch capacity | 1,000 streams | 100 streams | 0.1 |
| M6-106 RPO/RTO | ~1 GiB state | ~32 MiB state | 0.03125 |
| M6-107 partition matrix | all arrangements × 5 repeats | all arrangements × 1 | 0.2 |
| M6-108 crash matrix | `Boundary::ALL` × 5 repeats | `Boundary::ALL` × 1 | 0.2 |
| M6-109..M6-111 security matrix | all cases × 3 repeats | all cases × 1 | 0.333 |
| M6-112 gossip-authority proof | 10 min soak | 30 s soak | 0.05 |

The **case coverage never shrinks** — only the repeat count and the data volume do. A reduced-scale
run that skips a partition arrangement or a `Boundary` is a defect, because then the cheap run
stops being a regression gate for the expensive one.

---

## 3. Signed distributed RBAC lifecycle (D6.1, ADR-0027; spec §15.3, §19.9, §19.12)

Unless a row says otherwise: `start(3, Rocks)`, `ClusterTls::mutual`, `authz.mode = "signed"`,
`authz.poll_interval` driven by `TestTimers`, one trust key named `ops`.

### 3.1 Load and verify (§15.3 bullets 1–2; D6.1) — M6-01..M6-06

| ID | Name | Setup | Expected | Notes |
|---|---|---|---|---|
| M6-01 | good_signature_loads_and_activates | `PolicyFixture::write(dir, doc(v=7), "ops")`; start the node | the node reaches ready; `policy_version(id) == Some(7)`; a client granted `/a/` in the document can `Put /a/k`; a principal with no grant gets `PermissionDenied`; one `policy_loaded{version=7, hash, source="startup"}` line | the positive control. Without it every refusal row below could pass on a node that loads nothing |
| M6-02 | bad_signature_is_refused_and_node_is_unready | `write_unsigned` (signature bytes are random) | the node starts the process but is **unready** for client and admin traffic; `policy_state == NoValidPolicy{reason="signature_invalid"}`; `policy_version` is `None`; one `policy_rejected{reason="signature_invalid"}` line; no grant from the document takes effect | §15.3 "Missing or invalid policy fails closed" (§15.2 carried forward) |
| M6-03 | signature_by_an_untrusted_key_is_refused | `write_signed_by_untrusted` — a structurally valid ed25519 signature by a key **not** in `authz.trust_keys` | refused with `reason="untrusted_signer"`, distinct from `signature_invalid`; the trusted-key fingerprints appear in the log line, the key bytes do not | a valid-but-wrong-signer is the realistic attack; conflating it with a malformed signature costs the operator the diagnosis |
| M6-04 | tampered_document_body_is_refused | `write_tampered_body` — one grant's prefix edited after signing | refused with `reason="hash_mismatch"`; the *original* grants do not apply either (the node does not fall back to a previously cached document unless it is the already-active one — see M6-13) | D6.1 "Hash = sha256(document bytes)" |
| M6-05 | version_is_bound_to_the_document_hash | `write_version_hash_mismatch` — signature payload says version 9, body says version 7 | refused with `reason="version_binding"`; a row variant that swaps two validly-signed documents' signature files is refused the same way | D6.1 "version bound to hash in the signature payload"; §15.3 bullet 1. Without this a signed v5 document could be replayed as v9 by relabelling |
| M6-06 | trust_key_set_is_a_set_not_a_single_key | two trust keys `ops` and `ops-next` configured; document signed by `ops-next` | accepted; removing `ops-next` from `authz.trust_keys` and reloading refuses the same document with `untrusted_signer` | the trust-key rotation path; without a set, rotating the signing key requires a flag day |

### 3.2 Rollback, break-glass and audit (§15.3 bullet 2) — M6-07..M6-10

| ID | Name | Setup | Expected | Notes |
|---|---|---|---|---|
| M6-07 | rollback_is_refused_by_default | active version 7; write a validly-signed version 5 | refused with `reason="rollback"`, naming both versions; version 7 stays active; the node stays **ready** (a refused reload never un-readies a node that already has a valid policy) | §15.3 "reject rollback unless an explicit audited break-glass procedure authorizes it". The readiness clause matters: a bad deploy must not take the cluster down |
| M6-08 | equal_version_is_refused_unless_identical | active version 7; write a *different* document also numbered 7 | refused with `reason="rollback"` (version `<=` active); a byte-identical re-write of version 7 is a no-op with no `policy_loaded` line and no version change | D6.1 says "version <= active"; the identical case must be idempotent or a file-touching deploy system reloads forever |
| M6-09 | break_glass_flag_allows_rollback_and_audits_it | start with `--break-glass-policy-rollback`; active 7; load 5 | accepted; `policy_version == Some(5)`; exactly one `policy_rollback{from=7, to=5, break_glass=true, principal}` line at `warn`, and one `admin_op{op="reload_policy", outcome="ok", break_glass=true}` audit line; `retcd_policy_rollbacks_total` increments | the flag is the only thing that may unlock this, and it must be loud |
| M6-10 | break_glass_is_not_sticky | with the flag set, perform one rollback; then attempt a second rollback below the new active version | **OQ-57 default:** the flag is process-scoped, so the second rollback is also permitted, and each one emits its own audit line; the row pins whichever semantics the Architect chooses so it cannot drift silently | a one-shot flag and a process-scoped flag are both defensible; an unstated choice is not |

### 3.3 Reload paths: bounded polling and `ReloadPolicy` (§15.3 bullet 3) — M6-11..M6-16

| ID | Name | Setup | Expected | Notes |
|---|---|---|---|---|
| M6-11 | polling_picks_up_a_new_document | active 7; write version 8 to the same paths; advance `TestTimers` by one `authz.poll_interval` and await the tick | `policy_version == Some(8)` on that node; one `policy_loaded{version=8, source="poll"}` line; **exactly one** reload for one file change (no re-read storm on subsequent unchanged ticks) | D6.1 "bounded polling `authz.poll_interval` (10 s)". The no-storm half is what keeps §19.12 true |
| M6-12 | reload_policy_rpc_is_admin_only_and_immediate | call `ReloadPolicy` as an admin principal, and as a non-admin client principal | admin call returns `PolicyReloaded{from:7,to:8}` without waiting for a poll tick; non-admin call returns `PermissionDenied` and does **not** reload; both produce `admin_op` audit lines with distinct outcomes | D6.1 + §18.2 "Audit … authorization changes". **OQ-58**: the admin set is read from the *currently active* document, not the incoming one |
| M6-13 | a_failed_reload_keeps_the_active_policy | active 8; overwrite the file with a tampered body; poll | version 8 stays active and serving; `policy_rejected{reason="hash_mismatch"}`; the node stays ready; `retcd_policy_reload_failures_total` increments; a subsequent valid version 9 loads normally | "fails closed" means *does not adopt*, not *forgets what it had*. The opposite behaviour turns a typo into an outage |
| M6-14 | missing_files_are_distinguished_from_invalid_files | delete `policy.json.sig` between ticks | `policy_rejected{reason="signature_file_missing"}`; active policy retained; on the file's return the reload succeeds with no operator action | partial deploys are the common case |
| M6-15 | reload_is_atomic_under_a_half_written_file | write the document in two chunks with the poll tick landing between them | the half-written read is refused (`hash_mismatch` or a parse error, both typed) and never partially applied; the next tick succeeds; no request is ever evaluated against a partially parsed document | a deploy system that writes in place rather than renaming is the realistic environment |
| M6-16 | health_exposes_policy_version_and_state | on each of the three nodes, before and after a reload | `HealthPayload.policy_version` and `.policy_state` are present and change together with the active policy; the payload contains **no** grants, no principals and no key material | §15.3 "exposes its active policy version in health metadata"; §15.2 redaction. The health payload is unauthenticated on loopback (OQ-16), so it must not leak the policy body |

### 3.4 Convergence and the fail-closed intersection (§15.3 bullet 4; §19.9) — M6-17..M6-24

The scenario shape for M6-17..M6-22: three nodes on version 7; a version 8 document that **adds**
`/new/` for principal `app`, **removes** `/old/` from `app`, and leaves `/same/` untouched. Load 8
on node 1 only, then on node 2, then on node 3.

| ID | Name | Setup | Expected | Notes |
|---|---|---|---|---|
| M6-17 | intersection_never_expands_early | node 1 on v8, nodes 2 and 3 on v7 | on node 1, `app` writing `/new/k` is **denied** (`PermissionDenied{reason="policy_converging"}`) although v8 grants it; the denial is typed and distinguishable from an ordinary deny | §15.3 "must not expand access early". This is the single most important row in §3 |
| M6-18 | intersection_narrows_immediately | same state | on node 1, `app` writing `/old/k` is **denied** although v7 granted it — a removal takes effect at once, on the node that has seen it | "This may temporarily deny valid access" — the spec accepts the cost of narrowing early |
| M6-19 | unchanged_prefixes_are_unaffected_during_convergence | same state | `/same/` behaves identically on all three nodes throughout; no request under an unchanged prefix is ever evaluated against the intersection | the blast radius of a policy change is the changed prefixes only; a whole-document intersection would deny far more than the spec asks |
| M6-20 | lagging_node_still_serves_its_own_version | same state | nodes 2 and 3 (still v7) serve `/old/` and deny `/new/`; they are ready; no error is raised by their lagging | the converging rule is evaluated *per node* against what that node knows |
| M6-21 | convergence_completes_when_the_last_voter_reports | load v8 on node 2, then on node 3 | after node 3 reports v8 in gossip meta and the observing node sees it, `/new/` is allowed and `/old/` stays denied on every node; exactly one `policy_converged{version=8}` line per node; `retcd_policy_converged_version` gauge equals 8 everywhere | D6.1; the gauge is what an alert watches |
| M6-22 | unknown_voter_version_counts_as_lagging | stop node 3 entirely (gossip meta goes stale/absent) while nodes 1 and 2 are on v8 | the intersection stays in force on nodes 1 and 2 — an **unknown** version is treated as lower, never as "probably fine"; the cluster keeps serving unchanged prefixes and keeps accepting writes (§19.12: no unbounded block) | §15.3 "Until **every** voter reports the new version". Fail-open on absence would be the exact bug the clause exists to prevent |
| M6-23 | gossip_cannot_be_used_to_expand_access | inject, via `GossipControl`, a hint claiming node 3 reports v8 while node 3 in truth has v7 | the forged advertisement can only cause the cluster to **stop** intersecting — it can never grant access that neither document grants; a row-level assertion enumerates the grants before and after and shows the allowed set is a subset of `allowed(v7) ∪ allowed(v8)`, and that no key outside both is ever permitted | §19.9 "Gossip … never confer authority". This is the honest statement of the residual risk (§15 item 3): a forged hint can end the *narrowing* early, so the ADR must state that the intersection is a convergence courtesy, not a security boundary |
| M6-24 | intersection_is_a_subset_property_over_generated_documents | property test over ≥ 500 generated `(old, new)` pairs and random `(principal, key, action)` | `evaluate_converging` allows a triple **only if** both documents allow it, for every changed-prefix key; and equals `new` exactly for unchanged-prefix keys; counterexamples are shrunk and printed | TA-56.3. A hand-written table of four cases does not establish a set-containment claim |

### 3.5 No valid policy: unready for client and admin, peer plane unaffected (§15.3 bullet 5) — M6-25..M6-27

| ID | Name | Setup | Expected | Notes |
|---|---|---|---|---|
| M6-25 | no_valid_policy_is_unready_for_client_and_admin | node 2 starts with an invalid policy while nodes 1 and 3 are healthy | node 2's readiness is false; a client request to node 2 returns `Unavailable` (not `PermissionDenied` — the node is not making an authorization decision, it is declining traffic); an admin RPC to node 2 likewise; node 2 is excluded from the client-facing advertisement | §15.3 bullet 5. Distinguishing `Unavailable` from `PermissionDenied` is what lets a client retry elsewhere instead of giving up |
| M6-26 | peer_plane_is_unaffected_by_a_missing_policy | same state | node 2 still votes, replicates, applies and answers `AppendEntries`; the cluster keeps a leader and keeps committing; `state_hash` converges across all three; node 2 may even **be** the leader and the cluster still serves clients through nodes 1 and 3 | §15.3 "peer Raft traffic uses its separate certificate/committed-membership authorization path". A policy outage must not be a consensus outage |
| M6-27 | policy_arrival_restores_readiness_without_restart | write a valid document to node 2; poll | node 2 becomes ready; client traffic succeeds; no restart, no re-election, no membership change; one `policy_loaded{source="poll"}` line | the recovery path operators will actually use |

### 3.6 Watches and page tokens under a policy change (§15.3 bullets 6–7; §11.1, §11.3) — M6-28..M6-32

| ID | Name | Setup | Expected | Notes |
|---|---|---|---|---|
| M6-28 | watch_on_a_changed_prefix_terminates_before_any_new_version_event | leader on v7; a watch open on `/old/` (about to be revoked) with live events flowing; load v8 | the stream terminates with `PermissionDenied{policy_changed: true}`; the **last** event the client received is strictly below the first revision evaluated under v8; no event under the new version is ever enqueued for that stream; the termination reason is counted in `retcd_watch_terminations_total{reason="policy_changed"}` | §15.3 "Watches affected by a changed grant terminate **before** events are enqueued under the new policy version"; §11.1. This closes M4's OQ-32 gap |
| M6-29 | watch_on_an_unchanged_prefix_survives_a_policy_change | watch on `/same/`; load v8 | the stream is **not** terminated, delivers across the version boundary with no gap, and its `last_delivered_revision` advances monotonically through the change | the converse row. Terminating every watch on every policy change is the easy wrong implementation and would make policy rotation an availability event |
| M6-30 | watch_on_a_newly_granted_prefix_is_not_retroactively_opened | a watch attempt on `/new/` by `app` while converging | denied with `PermissionDenied{reason="policy_converging"}`, not queued and not silently held open until convergence | "must not expand access early" applies to watch admission, not only to reads and writes |
| M6-31 | watch_termination_ordering_is_asserted_from_the_journal | M6-28 repeated with the M4 `WatchHub` gate hooks | the recorded order is `policy_version_applied` → `stream_terminated{policy_changed}` → first enqueue under v8; across 10 repeats there is zero inversion; Q-27 returns no inversion rows | ordering claims are proven by the recorded interleaving, not by reading the code (the M5-10/M5-20 pattern) |
| M6-32 | policy_version_change_invalidates_outstanding_page_tokens | hold a `next_page_token` issued under v7; load v8; continue the list | `PageTokenExpired` with `reason="policy_version"`; the token is rejected **before** any key is read, so no key under a revoked grant can leak through a continuation; `PinRegistry::misses_by_reason()["policy"]` increments | §15.3 bullet 7; §16. §5 repeats this from the pagination side (M6-71); both exist because the two subsystems can regress independently |

### 3.7 Backup/restore binding, `authz.mode`, and the embedded principal — M6-33..M6-40

| ID | Name | Setup | Expected | Notes |
|---|---|---|---|---|
| M6-33 | backup_manifest_references_policy_version | take a backup (M5 `config-server backup`) with policy v8 active | the signed manifest carries `policy_version: 8` and the document **hash**, and carries **no** grants, principals or document body; `verify-backup` prints them | §15.3 "artifacts reference, but do not contain or override, the external RBAC artifact" |
| M6-34 | restore_refuses_to_open_the_client_plane_without_a_valid_policy | restore into a fresh cluster (M5 path) supplying no policy files | the restored nodes form, replicate and are peer-ready, but the client plane never opens; readiness is false with `policy_state=NoValidPolicy`; `restore_completed` is logged and `client_plane_open` is not | §15.3 "Restore validation confirms that an independently supplied signed policy is active before client traffic opens" |
| M6-35 | restore_accepts_an_independently_supplied_policy_of_any_version | restore the M6-33 artifact, supplying a **different** valid policy (version 3, signed by a trusted key) | the client plane opens; `policy_version == Some(3)`; a `restore_policy_mismatch{manifest_version=8, active_version=3}` line at `warn` records the divergence but does not block | "reference, but do not … override". Refusing to open on a version mismatch would make a restore depend on an artifact the backup does not contain — see §15 item 4 |
| M6-36 | authz_mode_static_preserves_M3_behaviour_exactly | `authz.mode = "static"` with the M3 `StaticAllowlist` grants | behaviour, capabilities (`Authz::StaticAllowlist`) and health are byte-identical to M3; no policy file is read; no poller runs; `policy_version` is `None` | D6.1 "static allowlist stays as `authz.mode = \"static\"`". The M3 release must remain reproducible |
| M6-37 | authz_mode_signed_requires_the_policy_configuration | `authz.mode = "signed"` with no `policy_file`/`trust_keys` | rejected **at config load** with a typed error naming every missing field; the node does not start and does not listen | a node that starts in signed mode with no trust keys is a node that fails closed on every request — an outage disguised as a configuration nicety |
| M6-38 | capabilities_report_the_active_authz_model | scrape capabilities in both modes | `Authz::SignedPolicy{policy_version: Some(8)}` vs `Authz::StaticAllowlist`; a signed-mode node with no valid policy reports `SignedPolicy{policy_version: None}` and is unready | TA-66; ADR-0016 "a capability that can lie is worse than no capability at all" |
| M6-39 | embedded_client_principal_is_non_forgeable_under_signed_policy | construct a direct embedded client scoped to principal `app`; issue requests carrying a different principal name in ordinary request fields | the engine evaluates against `app` regardless; there is no request field, header or builder method that can change it after construction; a compile-level assertion shows the principal is not part of the request type | §15.2/§15.3 closing paragraph, carried into M6. The M3 row asserted this for the allowlist; the signed path must not reintroduce a request-supplied principal |
| M6-40 | admin_set_comes_only_from_the_signed_document | a principal in `authz.admins` of a **config file** but not in the document's `admins` | the admin RPC is denied; the only source of admin identity under `authz.mode = "signed"` is the signed document (D6.1's `admins: [principal]`); the config-file allowlist is ignored and its presence is logged once as a configuration warning | otherwise the signature buys nothing: an attacker with file-write access to the TOML gets admin without touching the signed artifact |

### 3.9 Implementation status (dev-rbac, 2026-09-19)

Row to test, by the name the test actually carries. Every file below is `m6_rbac.rs` in that
crate's `tests/` directory, except the configuration rows, which are unit tests in
`crates/config-server/src/config.rs` because configuration validation is a pure function and a
daemon spawn would assert nothing extra.

| Rows | Where | Tests |
|---|---|---|
| M6-01..M6-06 | config-core | `m6_01_good_signature_loads_and_activates`, `m6_02_bad_signature_is_refused_and_denies_everything`, `m6_03_signature_by_an_untrusted_key_is_refused`, `m6_04_tampered_document_body_is_refused`, `m6_05_version_is_bound_to_the_document_hash`, `m6_06_trust_key_set_is_a_set_not_a_single_key` |
| M6-07..M6-10 | config-core | `m6_07_rollback_is_refused_by_default`, `m6_08_equal_version_is_refused_unless_identical`, `m6_09_and_m6_10_break_glass_allows_rollback_and_is_not_sticky` |
| M6-12 | config-grpc | `m6_12_reload_policy_is_immediate_for_an_admin`, `m6_12_reload_policy_is_refused_for_a_non_admin_and_does_not_reload`, `m6_12_a_refused_document_is_an_error_not_an_outcome` |
| M6-13 | config-core | `m6_13_a_failed_reload_keeps_the_active_policy` |
| M6-17..M6-19 | config-core | `m6_17_intersection_never_expands_early`, `m6_18_intersection_narrows_immediately`, `m6_19_unchanged_prefixes_are_unaffected_during_convergence` |
| M6-21..M6-24 | config-core | `m6_21_convergence_completes_when_the_last_voter_reports`, `m6_22_unknown_voter_version_counts_as_lagging`, `m6_23_gossip_cannot_be_used_to_expand_access`, `m6_24_intersection_is_a_subset_property_over_generated_documents` |
| M6-25 | config-grpc | `m6_25_no_valid_policy_closes_the_admin_plane` |
| M6-28..M6-31 | config-engine | `m6_28_watch_on_a_changed_prefix_terminates_before_any_new_version_event`, `m6_29_watch_on_an_unchanged_prefix_survives_a_policy_change`, `m6_30_watch_on_a_newly_granted_prefix_is_not_retroactively_opened`, `m6_31_watch_termination_ordering_is_asserted_from_the_journal` |
| M6-30 (wire half) | config-grpc | `m6_30_policy_converging_reaches_the_wire_as_a_reason_trailer`, `m6_30_a_wrapped_denial_still_carries_its_reason` |
| M6-36, M6-37 | config-server `config.rs` | `signed_mode_accepts_a_policy_file_and_one_trust_key`, `signed_mode_names_every_missing_field_in_one_error`, `the_signed_fields_are_refused_under_static_mode`, `a_trust_key_must_be_a_usable_ed25519_public_key`, `duplicate_and_empty_trust_key_names_are_refused`, `a_zero_poll_interval_is_refused_and_a_set_one_is_honoured` |
| M6-39, M6-40 | config-core, config-grpc | `m6_39_embedded_client_principal_is_non_forgeable_under_signed_policy`, `m6_40_admin_set_comes_only_from_the_signed_document` (both crates), `m6_40_the_admin_set_follows_the_active_document` |

**Not yet covered, and why.** Each needs a three-daemon harness with a policy fixture, a
`TestTimers`-driven poll tick and, for two of them, the restore path — none of which existed when
the rows above landed. The production behaviour each asserts *is* implemented (`PolicyLoader`
polling and its no-storm rule, typed missing-file reasons, readiness and the capability report);
what is missing is the process-level assertion.

| Rows | Reason |
|---|---|
| M6-11, M6-14, M6-15 | need the daemon harness plus a `PolicyFixture` that can write a document in two chunks between poll ticks |
| M6-16, M6-38 | health and capability assertions at process level; the payload fields and the exporter landed with the M6 metrics round, the E2E rows did not |
| M6-20, M6-26, M6-27 | need three daemons at different policy versions |
| M6-32 | the page-token half is dev-pagination's `PageTokenExpired{reason="policy_version"}`; the policy half is here |
| M6-33..M6-35 | backup/restore binding; the manifest field is the backup owner's |

---

## 4. Credential rotation (D6.2, ADR-0028; spec §15.1, §20 "Operations", §18.2)

**Implementation neutrality.** Every row below is written against observable behaviour
(handshakes, principals, availability, process identity) and holds whether the acceptor is
tonic's `ServerTlsConfig` replacement or the hyper + `tokio-rustls` fallback feeding tonic's
`Routes` (D6.2, TA-57). No row names a type from either implementation.

### 4.1 Client-plane TLS reload (§20 "certificate rotation") — M6-41..M6-48

| ID | Name | Setup | Expected | Notes |
|---|---|---|---|---|
| M6-41 | reload_tls_rpc_serves_the_new_leaf_without_restart | node 1 serving leaf A; write leaf B (same CA) to the cert paths; call `ReloadTls` as admin | a **new** client handshake presents leaf B (`served_leaf_fingerprint` changed); the process start time and PID are unchanged; `retcd_tls_reloads_total` increments; one `tls_reloaded{plane="client", leaf_fingerprint, ca_count}` line | the core claim of D6.2. Asserting the fingerprint from a real handshake is what distinguishes a reload from a restart |
| M6-42 | file_polling_reloads_without_an_rpc | write leaf B; advance `TestTimers` by one `tls.watch_files` interval | same outcome as M6-41 with `source="poll"`; an unchanged file on the next tick produces **no** reload and no log line | D6.2 "`tls.watch_files` polling (30 s)"; TA-65 |
| M6-43 | in_flight_requests_and_streams_survive_a_reload | a `Watch` stream and a long `List` in flight; reload | neither is interrupted; the stream keeps delivering in revision order with no gap; the existing connection keeps its original negotiated certificate (TLS does not renegotiate mid-connection) and only new connections see leaf B | this is what "restart-free continuity" means operationally; a reload that drops connections is a rolling restart with extra steps |
| M6-44 | overlap_ca_bundle_accepts_old_and_new_clients | server CA bundle contains CA-old and CA-new; clients holding certs from each | both authenticate; both derive the correct principal; `retcd_authn_failures_total` does not increment | §15.1 "Support overlapping certificate and CA rotation" |
| M6-45 | removing_the_old_ca_refuses_old_clients | remove CA-old from the bundle; reload | the CA-new client still works; the CA-old client's handshake is refused; the refusal is counted as `retcd_authn_failures_total{reason="untrusted_client_ca"}` and logged **without** the client certificate bytes | the completion of the rotation, and the row that proves the verifier's root store was actually rebuilt rather than cached |
| M6-46 | a_bad_reload_is_atomic_and_keeps_the_old_credentials | write a leaf whose private key does not match, then a malformed PEM, then a leaf whose chain does not reach the configured CA | each reload attempt fails with a distinct typed error; the previously served credentials keep working throughout; `retcd_tls_reload_failures_total{reason}` increments; the node stays ready | a rotation that can half-apply is worse than one that cannot rotate |
| M6-47 | principal_derivation_is_unchanged_across_a_rotation | client certs before and after rotation carry the same `retcd://` SAN URI | the derived `Principal` is identical before and after; a rotated cert whose SAN changed derives the **new** principal and is authorized against the current policy, not the old one | `crates/config-grpc/src/tls.rs` `principal_from_certs`; the CN gate (`with_common_name_principals`) keeps its M3 semantics across a reload |
| M6-48 | cn_fallback_gate_is_not_silently_re_enabled_by_a_reload | node configured with CN principals **off**; rotate to a cert set that would only match by CN | the gate stays off; those clients are refused; no reload path re-reads the flag from the certificate files | a security-relevant flag that a file rotation can flip is a file-write privilege escalation |

### 4.2 Peer-plane rotation and the unavailable voter (§20 "certificate rotation while one voter is unavailable") — M6-49..M6-56

| ID | Name | Setup | Expected | Notes |
|---|---|---|---|---|
| M6-49 | peer_transport_reloads_its_client_and_server_credentials | rotate node 2's peer leaf; reload | node 2 both accepts peer connections with the new leaf and **initiates** them with it; replication continues; no election occurs; `tls_reloaded{plane="peer"}` | the peer plane is a client as well as a server; rotating only the server half leaves the node unable to call out after the old CA is dropped |
| M6-50 | destination_binding_survives_rotation | rotated peer certs keep `peer_server_domain(cluster_id, node_id)` | a peer cert rotated to a **different** node id's domain is refused with the existing typed identity error; the M3 destination-binding rows still pass against rotated certificates | §15.1 "Bind peer certificate identity to the stable Node ID and expected destination"; §19.10 |
| M6-51 | rotation_while_one_voter_is_down_keeps_the_cluster_available | `stop(3)`; rotate nodes 1 and 2 to CA-new (bundle still contains CA-old); write throughout | writes succeed for the entire rotation; the leader never changes on account of the rotation; no request observes `Unavailable` | the §20 gate, stated as availability rather than as a procedure |
| M6-52 | the_down_voter_rejoins_only_with_a_chain_to_a_trusted_root | `start_node(3)` still holding its CA-old leaf while the bundle still trusts CA-old | node 3 rejoins, replicates and converges; `state_hash` equal on all three | the overlap window must actually work, or the procedure is "take the cluster down" |
| M6-53 | the_down_voter_is_refused_after_the_old_root_is_dropped | with node 3 still down, remove CA-old from nodes 1 and 2 and reload; then start node 3 with its CA-old leaf | node 3's peer connections are refused with the typed identity error and `reason="untrusted_peer_ca"`; nodes 1 and 2 keep quorum and keep serving; node 3 does **not** appear in gossip as a healthy peer and never receives log entries | the fencing half; §19.10. This is also the operator-visible failure mode the runbook must describe |
| M6-54 | the_down_voter_rejoins_after_being_issued_a_new_leaf | re-issue node 3's leaf from CA-new; start it | it rejoins, catches up (by log or by snapshot, per M5-29's mechanics), and converges; no membership change was needed | completes the procedure end to end, in-process |
| M6-55 | rotation_does_not_disturb_committed_membership_or_identity | across all of §4.2 | committed membership, `cluster_id`, `recovery_epoch`, node ids and `retired_nodes` are unchanged at every step; no `Command` is proposed by the rotation path | rotation is a transport concern; if it touches replicated state it can fail a quorum |
| M6-56 | acceptor_implementation_is_recorded_not_assumed | source/config assertion | whichever acceptor is in use (tonic-hosted or hyper + `tokio-rustls`), one build-time assertion records the choice and ADR-0028 names it; the M6-41..M6-55 rows pass unchanged under either | D6.2 leaves this as a developer research task; the plan must not silently assume the answer (§14 OQ-59) |

### 4.3 Gossip key rotation (§20 "key rotation"; D6.2) — M6-57..M6-61

| ID | Name | Setup | Expected | Notes |
|---|---|---|---|---|
| M6-57 | staged_add_use_remove_converges_with_all_nodes_up | `gossip_add_key(K2)` on all three, then `gossip_use_key(K2)` on all three, then `gossip_remove_key(K1)` on all three | gossip stays healthy at every stage (no node is ever seen as failed or suspect by another); after `remove`, every node's keyring is `{primary: K2, accepted: []}`; one `gossip_key_rotated{stage, key_fingerprint}` line per node per stage, with **fingerprints only** | D6.2's exact three-stage sequence. "Healthy at every stage" is the whole point of staging |
| M6-58 | use_before_add_on_a_peer_is_survivable | deliberately `use_key(K2)` on node 1 before node 3 has `add_key(K2)` | node 3 cannot decrypt node 1's messages and may mark it suspect, but the **cluster** does not lose its leader, does not change membership and does not lose committed data; recovery is automatic once node 3 adds K2 | gossip is advisory (§19.9); a botched gossip rotation must be an observability incident, not a consensus incident. This row is the proof |
| M6-59 | remove_before_every_peer_uses_the_new_key_is_refused_or_recoverable | `remove_key(K1)` on node 1 while node 3 still has K1 as primary | **OQ-61 default:** the node refuses to remove its own *primary* or a key still required by a known peer's advertised fingerprint, with a typed error; if the Architect chooses "allow and warn", the row asserts recovery within one gossip convergence window instead | the destructive stage needs a stated policy; either is defensible, silence is not |
| M6-60 | rotation_with_one_node_unreachable_converges_on_its_return | isolate node 3 (`NetFault`); run add→use on nodes 1 and 2; heal | while node 3 is isolated, nodes 1 and 2 gossip normally on K2; on heal, node 3 (still on K1, but with K1 still in 1 and 2's accepted list) is reachable again, then completes add→use and the cluster converges to K2; `remove_key(K1)` afterwards leaves a healthy cluster | D6.2's stated row. The accepted-list overlap is exactly what makes the return survivable |
| M6-61 | gossip_key_operations_are_admin_only_and_audited | each of add/use/remove as a non-admin principal | `PermissionDenied`; no keyring change; one `admin_op{op="gossip_key_*", outcome="denied"}` line per attempt | §18.2 "Audit … credential operations" |

### 4.4 Expiry observability (§15.1 "alert well before expiry"; §18.2) — M6-62..M6-64

| ID | Name | Setup | Expected | Notes |
|---|---|---|---|---|
| M6-62 | cert_expiry_metric_is_exported_per_plane | scrape `/metrics` | `retcd_cert_expiry_seconds{plane="client"}` and `{plane="peer"}` exist and are within 60 s of the certificate's real `notAfter`; the metric carries **no** subject DN, serial or SAN that would leak a principal identity beyond the `subject` label agreed in ADR-0026 | §18.2 "certificate expiry" |
| M6-63 | warning_fires_once_at_thirty_days | advance the injected clock to 29 days before expiry | exactly one `cert_expiring{plane, days_remaining=29}` line at `warn` per subject per crossing; scraping ten more times does not produce ten more lines; crossing back (a rotation to a longer-lived cert) and forward again produces a second line | D6.2 "warn at 30 days". A per-scrape log line is a log flood, which is how real warnings get filtered out |
| M6-64 | expiry_metric_follows_a_rotation | rotate to a cert with a later `notAfter`; reload | the gauge jumps to the new value on the next scrape with no restart; an expired-cert reload attempt is refused with a typed error and the gauge does not move | ties §4.1 to §4.4: the metric must read the *served* credential, not the one read at boot |

---

## 5. Revision-pinned pagination (D6.3, ADR-0029; spec §10.2, §15.3 bullet 7, §16, §19.12)

Config unless stated: `list.max_pinned_snapshots = 4` (small, so eviction is reachable),
`list.ttl` driven by `TestTimers`, `list.token_key` from config, `max_items = 10` against a
100-key prefix. Storage is `Rocks` unless a row says `Ephemeral`.

| ID | Name | Setup | Expected | Notes |
|---|---|---|---|---|
| M6-65 | page_token_round_trip_returns_every_key_once | list `/p/` with `max_items=10` and follow `next_page_token` to exhaustion | the concatenation of pages equals the full 100-key prefix, in unsigned bytewise lexical order, with no duplicate and no omission; every page reports the **same** `read_revision`; the final page's `next_page_token` is empty and `truncated` is false | the positive control for §5 |
| M6-66 | pages_are_consistent_at_one_revision_under_concurrent_writes | start the walk; after page 1, apply 50 puts and 20 deletes inside `/p/` (including keys already returned and keys not yet reached); finish the walk | the walk returns exactly the state as of the pinned revision: no key created after the pin appears, no key deleted after the pin is missing, and every returned `mod_revision <= read_revision` | §10.2's "short-lived RocksDB read snapshot"; this is the row the whole feature exists for |
| M6-67 | tampered_token_mac_is_rejected | `forge_token` flipping one bit of `last_key`, then of `revision`, then re-MACing with the wrong key | each is `PageTokenExpired` with `reason="mac"`; the response carries no hint about which field was wrong; `PinRegistry::misses_by_reason()["mac"]` increments per attempt | §16; an authenticated token whose failure mode leaks structure is an oracle |
| M6-68 | expired_token_is_rejected_by_ttl | hold a token; advance the injected clock past `list.ttl`; continue | `PageTokenExpired` with `reason="expired"`; the pinned snapshot is released (`PinRegistry::len()` drops, `expiries()` increments) and the underlying RocksDB snapshot handle is dropped | a pin that outlives its token pins SST files forever, which is §19.12's disk-exhaustion path |
| M6-69 | lru_eviction_rejects_the_oldest_token | open 5 concurrent paginated walks with `max_pinned_snapshots = 4` | the least-recently-used pin is evicted; its next continuation returns `PageTokenExpired{reason="evicted"}`; the other four walks complete correctly; `evictions() == 1` | bounded means bounded. A row that only proves the cap exists, without proving the *other* walks survive, would pass on an implementation that drops them all |
| M6-70 | token_from_another_leader_is_rejected | obtain a token from leader A; force a leader change (`stop` A); continue against the new leader | `PageTokenExpired` with `reason="node"`; the client's documented recovery (restart the walk) succeeds immediately; no partial or cross-node page is ever served | D6.3 "other-leader → `PageTokenExpired`". The pin is a local RocksDB snapshot and cannot exist on the new leader |
| M6-71 | policy_version_change_rejects_the_token | token issued under policy v7; load v8; continue | `PageTokenExpired{reason="policy_version"}`, and the rejection happens **before** any key is read | §15.3 bullet 7; the companion of M6-32 from the pagination side |
| M6-72 | ephemeral_store_has_pagination_parity | run M6-65, M6-66, M6-68 and M6-69 against `StorageKind::Ephemeral` | identical observable behaviour via the clone-on-pin `BTreeMap` snapshot; the same typed errors and the same counter movements | D6.3's ephemeral clause. Parity is what lets a developer trust the fast store |
| M6-73 | capability_reports_revision_pinned_pagination | scrape capabilities with pagination on and off | `Pagination::RevisionPinned{max_pinned, ttl_ms}` vs `Pagination::Unsupported`; a build with the token key unset reports `Unsupported` and refuses to issue tokens rather than issuing unauthenticated ones | TA-66; ADR-0016 |
| M6-74 | token_contains_no_material_beyond_the_documented_fields | decode 100 generated tokens | the decoded view contains exactly `{revision, last_key, policy_version, issued_ms, node_id, token_version, mac}` and nothing else; no value bytes, no principal name in clear, no key material; the documented statement "the token carries `last_key`, which **is** a key name and therefore must be treated as data at rest" appears in ADR-0029 and in `docs/runbooks/pagination.md` | the user ruling: no key material in the token beyond `last_key`, **documented**. A token is handed to a client and may be logged by *their* infrastructure; saying so is part of shipping it |
| M6-75 | token_bytes_never_appear_in_any_log_or_metric | drive §5 with logging at `debug`; sweep the JSONL and the scrape | zero occurrences of any token's base64 text or of its `last_key` bytes in `target/test-logs/**` and in `/metrics`; rejections log the `reason` and a **token fingerprint**, never the token | §15.2/§15.3 redaction; Q-32 |
| M6-76 | token_is_bound_to_the_requested_prefix | take a token from a walk of `/p/`; submit it with `prefix = "/q/"` | rejected — **OQ-62 default:** `InvalidArgument{prefix_mismatch}` rather than `PageTokenExpired`, because the client's error is a bug, not an expiry, and telling them to retry the walk would loop | §10.2 requires binding to prefix; D6.3's payload omits it (§15 item 12). This row forces the decision. The detail is asserted by **containment** of the marker, not byte equality: the M3 status mapping puts the error's `Display` in the status message, so a client decodes `"invalid argument: prefix_mismatch"`; making it byte-exact would change that mapping for every `InvalidArgument` (dev-pagination, M6 features-local). |
| M6-77 | token_is_bound_to_the_principal | client X obtains a token; client Y (a different principal, also granted the prefix) submits it | rejected with `PermissionDenied{reason="token_principal"}`; the response never reveals X's identity | §10.2 lists principal among the bindings. A transferable cursor is a lateral-movement primitive. The detail is asserted by **containment** of the marker, not byte equality: the M3 status mapping puts the error's `Display` in the status message, so a client decodes `"invalid argument: prefix_mismatch"`; making it byte-exact would change that mapping for every `InvalidArgument` (dev-pagination, M6 features-local). |
| M6-78 | token_version_is_explicit_and_unknown_versions_are_rejected | forge a token with `token_version = 2` | rejected with `PageTokenExpired{reason="token_version"}`; the current version is `1` and is asserted against a golden byte vector | §17 "Version … page tokens"; §10.2 "token version" |
| M6-79 | rotating_the_token_key_invalidates_outstanding_tokens | rotate `list.token_key` and reload | every outstanding token is `PageTokenExpired{reason="mac"}`; new walks work immediately; the rotation is logged as a credential operation and audited | an HMAC key is a credential (§18.2); its rotation must be expressible and its effect must be stated |
| M6-80 | max_items_and_max_bytes_are_both_honoured_and_capped | request `max_items = 10_000` and `max_bytes = 1 GiB` | both are clamped to the server caps; the response reports what was actually applied; a page that hits `max_bytes` first still returns a usable `next_page_token` | §10.2 "both capped by the server", carried into the paginated path |
| M6-81 | a_pinned_snapshot_does_not_block_raft_apply_or_compaction | hold 4 pins over a 30 s (reduced: 3 s) write burst | applies continue at the same rate as a control run without pins (assert *progress*, not wall clock); the M4 journal `Compact` command still applies; log purge (M5) still happens; `PinRegistry::len()` never exceeds the cap | §19.12 explicitly names "List cursors" among the things that must not block Raft progress |
| M6-82 | pins_are_released_on_stream_or_client_disconnect | start 4 walks; drop the clients without exhausting them | all four pins are released by TTL at the latest, and by disconnect detection at the earliest; `PinRegistry::len()` returns to 0; the released RocksDB snapshots are observably dropped (SST file count / disk usage returns to baseline after a compaction) | the leak that a paginated API classically ships with |
| M6-83 | pins_do_not_survive_a_restart | hold tokens; `restart(leader)` | every token is rejected (`reason="node"` — the node id is unchanged but the pin registry is empty, so the row pins whichever reason is chosen and requires it to be **one** reason, not a race between two); no pin state is written to disk | a pin is process state by construction; the error taxonomy must say so deterministically |
| M6-84 | list_without_a_token_keeps_exact_M3_semantics | issue the M3-era `List` with no `page_token` | the M0-M3 behaviour is unchanged: one leader-linearized response, `read_revision`, `truncated`, **no** `next_page_token` unless the caller opted in; every existing M2/M3 List row still passes | §10.2's first-release contract must not be silently replaced under existing clients |

---

## 6. Mixed-version upgrades and migrations (D6.4, A7, A8, ADR-0030; spec §17, §19.1, §19.10)

**A7 is the governing amendment: the gate is at PROPOSE time on the leader.** postcard is not
self-describing, so a committed unknown variant cannot be tolerated at apply — by then the entry
is already committed and the node fails inside `apply()`, i.e. at the point of no return
(research §7).

### 6.1 Advertisement and the computed minimum — M6-85..M6-89

| ID | Name | Setup | Expected | Notes |
|---|---|---|---|---|
| M6-85 | schema_is_advertised_in_gossip_meta | 3 nodes, all v2 | each node's gossip meta carries `{format_version, command_schema, proto_rev}`; each peer's `advertised_schema` view matches the source's `schema()`; the meta stays within the gossip payload budget (no fragmentation, no dropped hints) | D6.4. The M1 gossip meta rows' size assertions extend to the new field |
| M6-86 | schema_is_carried_on_the_peer_append_entries_header | inspect a real `AppendEntries` exchange | the request carries a `schema` field, added as a **new proto field with a new tag** (§17 "Add Protobuf fields compatibly and never reuse tags"); a peer that omits it is read as schema 1, not as an error; the response echoes the responder's schema | this is the path that feeds `cluster_min_schema`; gossip is advisory and must not be the input (research §7) |
| M6-87 | schema_is_exposed_in_health_and_capabilities | scrape health and capabilities on each node | both carry the same `SchemaTriple` as `schema()`; a `--compat-schema 1` node reports `{1,1,…}` everywhere consistently | TA-60, TA-66 |
| M6-88 | cluster_min_schema_is_computed_from_committed_voters_only | 3 voters at v2, plus a **learner** pinned at schema 1 | `cluster_min_schema == 2` — a learner does not hold the cluster back; after promoting the learner to voter, it becomes 1 | D6.4 "min over committed voters"; §19.8. A learner-sensitive minimum would make M5's learner-replacement flow impossible to run during an upgrade |
| M6-89 | an_unreachable_voter_does_not_raise_the_minimum | 3 voters; node 3 at schema 1 is stopped | `cluster_min_schema` stays 1 while node 3 remains a committed voter; it rises to 2 only when node 3 is **removed from committed membership** or reports 2 | "all voters report compatible versions" (§17) is about the committed set, not the reachable set. Treating an absent voter as compatible is exactly how a v2 command reaches a v1 node |

### 6.2 The propose-time gate (A7; §17) — M6-90..M6-94

| ID | Name | Setup | Expected | Notes |
|---|---|---|---|---|
| M6-90 | v2_compact_is_refused_until_activation | one voter at `--compat-schema 1`; drive the M4 retention policy so the leader wants to propose `Compact` | the leader does **not** propose; the internal attempt yields `Unavailable{feature_not_activated}`; retention is not enforced and that fact is logged once per window as `feature_gated{feature="compact", cluster_min_schema=1}`; the journal grows within its budget and alerts rather than silently overrunning | D6.4's named list. The second half matters: a gated feature that silently does nothing is an unbounded resource |
| M6-91 | v2_retire_node_is_refused_until_activation | same; attempt the M5 `RemoveMember` sequence | the admin RPC returns `Unavailable{feature_not_activated}` with both schema numbers in the message; no membership change is proposed; the audit line records the refusal | M5's fencing depends on `RetireNode`; refusing the whole operation is correct, half-performing it is not |
| M6-92 | dedup_bearing_mutation_is_refused_until_activation | same; submit a mutation carrying a dedup key | **OQ-64 default:** the mutation is refused with `Unavailable{feature_not_activated}` rather than being silently applied without its dedup record, because a client that believes it may safely resubmit (§16, ADR-0015) must not be misled | the "apply without the dedup key" alternative is a correctness trap and must be rejected explicitly |
| M6-93 | a_client_facing_refusal_is_retryable_and_typed_end_to_end | the M6-90..M6-92 refusals observed through the gRPC client | `Unavailable` with the `retcd-reason = feature_not_activated` trailer (OQ-36's general reason slot); the client surfaces it as retryable; no `FatalStorage`, no `InvalidArgument` | §16's taxonomy. A gating condition that is transient must map to a transient error class |
| M6-94 | compat_schema_flag_makes_one_binary_behave_as_an_old_node | `--compat-schema 1` on node 3 | node 3 advertises 1 everywhere, emits only v1 envelopes, and **refuses to decode** a v2 envelope with the typed error; it participates fully in Raft otherwise; a v2 node's `state_hash` and node 3's agree over v1-only command sequences | TA-60. Without this, §6 is untestable in CI and the §20 mixed-version gate would be unverified — which is what "production hardening" is supposed to close |

### 6.3 Rolling upgrade, activation and the rollback boundary (§17; D6.4) — M6-95..M6-100

| ID | Name | Setup | Expected | Notes |
|---|---|---|---|---|
| M6-95 | rolling_restart_v1_to_v2_with_writes_in_flight | 3 nodes at `--compat-schema 1`; a continuous write load; restart node 3 as v2, then node 2, then node 1, waiting for convergence between each | no write fails with anything other than a retryable `NotLeader`/`Unavailable`; no acknowledged revision is lost; `state_hash` converges after each step; total unavailability at any instant is bounded by one leader election | §17 "Do not perform unbounded in-place RocksDB rewrites during an ordinary rolling restart" — the M4 v1→v2 store migration (M4-13..M4-20) runs inside this window and must stay bounded |
| M6-96 | activation_flips_only_after_the_third_node | the M6-95 sequence, observing `cluster_min_schema` and `feature_activated` after each restart | after node 3: min stays 1, not activated; after node 2: still 1, not activated; after node 1: min becomes 2 and `feature_activated` becomes true on every node, with exactly one `feature_activated{schema=2}` line per node | §17 "Feature activation occurs after all voters report compatible versions". "Exactly one per node" catches a flapping computation |
| M6-97 | the_first_v2_command_is_the_rollback_boundary | after activation, propose and commit one `Compact`; then attempt to restart node 3 with `--compat-schema 1` | node 3 refuses to start with a typed error naming the boundary (it cannot decode a committed entry); the refusal is a clean exit, not a panic, and the cluster keeps quorum on the remaining two voters | D6.4 "rollback boundary = before first v2 command is committed". Refusing at start is the only safe answer once an undecodable entry is in the log |
| M6-98 | downgrading_a_v2_store_is_refused_with_one_specific_message | point a v1-behaviour build at a `format_version = 2` data directory | refused with **one** asserted message. M4 §11 item 5 records that the shipped `RocksStore::open` runs `verify_column_families` *before* `check_format_version`, so today the operator sees an unexpected-column-family error rather than a version error; this row asserts whichever order ADR-0021/0030 fixes on, and fails if both messages are possible | §17 "documented rollback boundary"; §19.10. "Some error" is not an operator procedure |
| M6-99 | v1_decoder_rejects_a_v2_envelope_on_golden_bytes | golden byte vectors for every v2-only `Command` variant, decoded with the v1 schema | every one yields the typed decode error and **never** a plausible-but-wrong v1 value; the vectors are committed to the repo so a serialization change breaks this row loudly | **U2** settled as a row. postcard is not self-describing, so a silently-misdecoded variant is the realistic failure and is far worse than an error |
| M6-100 | activation_is_monotonic_and_does_not_flap_on_a_restart | after activation, restart each node in turn | `feature_activated` stays true throughout; `cluster_min_schema` never dips below 2; no second `feature_activated` line is emitted | a recomputation that briefly sees an absent voter as schema 1 would re-gate `Compact` on every restart |

### 6.4 The gate's own correctness (A7, A8; research §7) — M6-101..M6-104

| ID | Name | Setup | Expected | Notes |
|---|---|---|---|---|
| M6-101 | the_gate_is_on_propose_not_on_apply | source + behaviour: force a v2 command into the log on a cluster whose min is 1 (harness-only back door, `#[cfg(feature = "testing")]`) | the apply path's behaviour is recorded as **fatal/refusing**, not tolerant — and the row's purpose is to demonstrate why the propose-time gate is mandatory; the ordinary path (M6-90..M6-92) never reaches apply | A7. If apply could tolerate it, the gate would be an optimization; it cannot, so the gate is the safety property |
| M6-102 | gossip_derived_schema_never_gates_a_proposal | inject, via `GossipControl`, hints claiming all voters are v2 while a voter is really v1 | the leader still refuses the v2 command, because `cluster_min_schema` is computed from committed membership plus peer-plane responses, not from gossip | §19.9; research §7 "two leaders could compute different levels". Gossip may *inform an operator*; it may not *unlock a feature* |
| M6-103 | snapshot_compatibility_across_the_boundary | a v2 leader builds a snapshot; offer it to a `--compat-schema 1` node | the install is refused with the M5 typed `command_schema` error (M5-08/M5-37) and nothing is written; the reverse (v1 snapshot into a v2 node) is accepted if and only if ADR-0030 says v2 can read v1, and the row asserts whichever is chosen | §20 "snapshot compatibility"; M5 already owns the refusal machinery, M6 owns the *policy* |
| M6-104 | openraft_wire_floor_is_pinned_and_documented | source assertion over `Cargo.toml` / `Cargo.lock` plus the README | `=0.9.25` exactly (re-asserting M5-48 at the M6 gate), and the README/ADR-0030 state that rEtcd makes **no** claim about any older 0.9.x and runs no mixed-openraft configuration | **A8**. §17's "Upgrade OpenRaft only after staging tests cover mixed versions" is discharged by declaring a single supported version, and that discharge must be written down, not assumed |

---

## 7. Evidence rows (D6.5, ADR-0031; spec §20 all subsections, §12.2)

Every row here writes exactly one JSON file under `docs/evidence/` using `write_evidence`
(TA-61). **No row asserts a threshold** (anti-flake rules 23 and 37) — each asserts only
*correctness* invariants that must hold at any scale, and *records* the numbers. Reduced scale is
the default; `RETCD_EVIDENCE=1` runs full scale (TA-61.3, §2's table).

| ID | Name | Setup | Expected | Notes |
|---|---|---|---|---|
| M6-105 | evidence_watch_capacity_1000_streams | leader under `spawn_watch_load(WatchLoad{healthy: 800, slow: 100, disconnecting: 100})` across 16 prefixes with a steady write rate; slow clients drain at `slow_drain_ratio`; disconnecting clients drop and resume every `disconnect_every` writes | **Asserted:** no healthy stream observes a gap (`gaps_observed() == 0`); every termination carries a reason from the closed set `{overload, compaction, leader_change, client_cancel, policy_changed}`; apply never starves (§19.12, the M4-62 predicate). **Recorded:** peak and steady RSS, apply latency p50/p99/max, per-population termination counts, queued bytes high-water, delivered-event total, `scale_factor` | §20 "1,000 watchers including slow and disconnected populations"; M4 deferred this explicitly (M4 §11 item 1) and M6 is where it lands — as **evidence**, per the user ruling, not as a threshold gate. File: `docs/evidence/watch-capacity.json` |
| M6-106 | evidence_backup_restore_rpo_rto | generate ~1 GiB of live state (reduced: ~32 MiB) with a realistic key/value mix; take an encrypted backup; verify it; restore into a fresh fenced cluster (M5 path); serve reads | **Asserted:** the restored cluster serves every key at its original revision; `restored_from` is reported; the old and new clusters refuse each other (§19.11). **Recorded:** state bytes, backup duration, artifact bytes, verify duration, restore duration, time-to-first-successful-read (RTO components), and the RPO window implied by the backup instant; `scale_factor` | §20 "encrypted backup verification and full fenced restore within RPO/RTO"; §12.2's 60 min/60 min are **provisional planning assumptions** and this row does not claim them (§15 item 7). File: `docs/evidence/rpo-rto.json`. Distinct from M5-93's `backup-restore.json` (TA-61.4) |
| M6-107 | evidence_partition_matrix | for every arrangement from `partition_arrangements(ids)` — each 1\|2 split, each one-way loss in each direction, each single-node isolation — apply the partition under a write load, hold it, then heal | **Asserted, per arrangement:** at most one leader commits; a former leader without quorum serves no successful strict read and no write (§20 "Network and consistency"); no acknowledged mutation is lost; no duplicate revision; `state_hash` converges after heal. **Recorded:** arrangement id, time-to-new-leader, writes rejected, writes accepted, convergence time | §20 "every three-node partition arrangement". The count is asserted against the enumerator (TA-63), so a missing arrangement fails rather than passing quietly. File: `docs/evidence/partition-matrix.json` |
| M6-108 | evidence_crash_matrix | for every `Boundary` in `Boundary::ALL` (M5 grew it to 16, including the snapshot publish/install and purge boundaries per ruling M5-R2), arm `crash_on_nth`, crash, `reopen_store`, `restart`, converge | **Asserted, per boundary:** the boundary counter is ≥ 1 (rule 29 — the crash actually happened); `assert_crash_invariants` passes (no vote regression, no log hole, no lost acknowledged mutation, no duplicate revision, no inconsistent last-applied); §19.7 holds (Q-21 empty). **Recorded:** boundary name, crossings, recovery duration, redo counts | §20 "crash injection before/after vote sync, log append, log flush, state batch, snapshot publish, and snapshot install" — the whole bullet, in one enumerated artifact. File: `docs/evidence/crash-matrix.json` |
| M6-109 | evidence_security_matrix_identity | `SecurityCase::{WrongNodeId, WrongClusterId, WrongCertIdentity, WrongDestinationBinding, DuplicateNodeId}` | **Asserted, per case:** the connection or command is refused; the refusal carries the specific documented reason string; nothing is written; `retcd_authn_failures_total{reason}` increments by exactly one; the refusal log line contains no certificate bytes and no key material. **Recorded:** case, reason, refusal latency | §20 "rejection of wrong Node ID, cluster ID, certificate identity, or destination binding" and "duplicate Node ID"; §19.10. File: `docs/evidence/security-matrix.json` (shared with M6-110 and M6-111 — **OQ-68**: one file written by one row that drives all three case groups, so TA-61's one-file-one-row rule holds; M6-110/M6-111 are sub-scenarios of M6-109's run, not separate writers) |
| M6-110 | evidence_security_matrix_gossip | `SecurityCase::{StalePackets, PoisonedEndpoint, AllSeedsUnavailable, FalseSuspicion, OneWayLoss, GossipKeyRotation}` | **Asserted, per case:** the cluster keeps its leader, its committed membership and its data; a poisoned endpoint hint never changes a peer address used for Raft; stale packets are dropped with a typed reason; with all seeds unavailable the already-formed cluster keeps operating and keeps serving; gossip key rotation (§4.3) completes. **Recorded:** case, suspicion transitions, drops by reason, convergence time | §20 "false suspicion, partition, one-way loss, stale packets, … poisoned endpoint, key rotation, all seeds unavailable"; §19.9 |
| M6-111 | evidence_security_matrix_version_skew | `SecurityCase::VersionSkew`: a `--compat-schema 1` voter against v2 peers, plus a peer advertising an unknown future schema (3) | **Asserted:** the v2 feature set stays gated (M6-90..M6-92 conditions hold); an unknown *higher* schema is treated as "not compatible with me" and never unlocks anything; no node panics on an unrecognised advertisement. **Recorded:** observed `cluster_min_schema` over time, refusals by feature | §20 "version skew"; research §7. A future-version peer must be as safe as an old one |
| M6-112 | evidence_gossip_cannot_mutate_membership_or_configuration | drive every hostile gossip input the harness can produce — poisoned endpoints for every node, forged `policy_version`, forged `SchemaTriple`, forged liveness for a retired node id, a forged node id not in membership — over a soak with a continuous write load | **Asserted:** committed membership, `cluster_id`, `recovery_epoch`, `retired_nodes` and the effective authorization set are **bit-identical** before and after; no `Command` is proposed by any gossip path (assert the proposal counter by source); no forged hint ever expands access (M6-23's containment property, re-checked); the leader set is unchanged except for ordinary elections. **Recorded:** hostile inputs injected, soak duration, proposals by source (must be zero for gossip) | §20 "proof that gossip cannot mutate membership or configuration"; §19.9. This is the one §20 bullet phrased as a *proof*, so the row is written as an invariance assertion over a hostile soak rather than as a list of refusals. File: `docs/evidence/gossip-authority.json` |
| M6-113 | evidence_files_validate_against_the_schema | after a full evidence run, read every file in `docs/evidence/` | each parses; each has every TA-61 field; `schema == 1`; `host`, `build.git_sha` and `run.utc` are non-empty; `values` is non-empty; `disclaimer` is the exact constant; unknown top-level keys are rejected | a malformed evidence file is worse than none, because it looks like evidence |
| M6-114 | evidence_gate_rule_is_enforced_both_ways | run the suite with `RETCD_EVIDENCE` unset, then with `=1` | unset: every §7 row **runs** (no `#[ignore]`), completes inside the §2 reduced budget, and writes a file with `scale_factor < 1.0` and `full_scale: false`. Set: every §7 row writes `scale_factor == 1.0` and `full_scale: true`. The M6 gate script fails if any `docs/evidence/*.json` has `full_scale: false` | TA-61.3. This row is what makes "reduced by default, full on demand" a rule rather than an intention |
| M6-115 | scale_factor_tracks_reality | deliberately run M6-105 at 100 streams while forcing the env var on | the written `scale_factor` reflects the **actual** stream count achieved, not the requested one; a row that could not reach its configured scale records the shortfall and marks `full_scale: false` | honesty about scale is the entire value of the artifact; a row that writes its intention rather than its observation is a fabricated measurement |
| M6-116 | evidence_carries_no_production_claim | grep `docs/evidence/*.json`, `README.md` and `docs/runbooks/*` | every artifact carries the fixed disclaimer; the README's evidence section states plainly that the numbers are dev-host evidence and that production designation requires re-running on target hardware; no document asserts the §12.2 RPO/RTO objectives as met | the user ruling, and §20's opening sentence ("Production designation requires reproducible evidence"). D6.5 says the README must say this; this row is the thing that keeps it said |

---

## 8. Logging, redaction and audit (§18.2, §19.12; ADR-0013) — M6-117..M6-126

Field names follow the shipped CLEF layer (`@t`, `@l`, `@m`, plus flattened span fields; M2-M3
§7 / OQ-20). Every row asserts a **positive** count where lines are expected (anti-flake rule 11).

| ID | Name | Setup | Expected | Notes |
|---|---|---|---|---|
| M6-117 | policy_loaded_line_is_complete | M6-01, M6-11, M6-12 | `policy_loaded{version, hash_prefix, source ∈ {startup, poll, rpc}, signer_fingerprint, grants_count, admins_count}`; **no** grant bodies, principals or prefixes; exactly one line per adoption | §18.2 "Audit … authorization changes". Counts are useful; contents are policy material |
| M6-118 | policy_rejected_line_has_a_closed_reason_set | M6-02..M6-05, M6-13, M6-14 | `policy_rejected{reason, version_seen, active_version}` with `reason ∈ {signature_invalid, untrusted_signer, hash_mismatch, version_binding, rollback, parse_error, signature_file_missing, policy_file_missing}`; no other value ever appears | a closed set is what makes an alert rule possible |
| M6-119 | policy_converged_line_pairs_with_the_gauge | M6-21 | one `policy_converged{version, voters_reporting, voters_total}` per node per version; `retcd_policy_converged_version` equals the line's version at the next scrape; no `policy_converged` is ever emitted while a voter lags | the log and the metric must not disagree; an operator will trust whichever they see first |
| M6-120 | tls_reloaded_line_is_complete_and_redacted | M6-41, M6-42, M6-49 | `tls_reloaded{plane, source, leaf_fingerprint, ca_fingerprints, not_after_unix}`; zero occurrences of `BEGIN PRIVATE KEY`, `BEGIN CERTIFICATE`, or any PEM body; a failed reload logs `tls_reload_failed{plane, reason}` from a closed set | §15.2 redaction; §18.2 "credential operations" |
| M6-121 | gossip_key_rotated_line_carries_fingerprints_only | M6-57..M6-61 | `gossip_key_rotated{stage ∈ {add, use, remove}, key_fingerprint, keyring_size, principal}`; zero occurrences of any 32-byte key's hex or base64 form anywhere in the logs or in a `Debug` render of `GossipConfig`/`GossipKeyring` | extends the shipped `GossipConfig` redaction (`config.rs:92-103`) to the keyring; TA-58 |
| M6-122 | page_token_rejected_line_matches_the_counter | M6-67..M6-71, M6-76..M6-79, M6-83 | `page_token_rejected{reason, token_fingerprint}` with `reason` drawn from exactly the same closed set as `PinRegistry::misses_by_reason()`; the per-reason line count equals the counter delta; the token text and `last_key` never appear | TA-59.2. A counter and a log that can disagree will |
| M6-123 | feature_activated_line_is_emitted_once_per_node | M6-96, M6-100 | exactly one `feature_activated{schema, cluster_min_schema, voters}` per node per activation; a `feature_gated{feature, cluster_min_schema}` line is rate-limited to once per window per feature | D6.4's named line; M6-90's gating notice must not become a per-request log flood (§19.12's spirit) |
| M6-124 | no_values_in_any_M6_line | drive §3–§6 with sentinel values and keys; sweep | zero occurrences of any sentinel **value** in `target/test-logs/**` and in the E2E daemon logs; mutation audit lines carry key **prefixes or hashes** per ADR-0013, never values | §18.2 "Audit mutations without values" |
| M6-125 | no_key_material_anywhere | sweep for: policy signing key, policy trust key private halves, TLS private keys, gossip keys, `list.token_key`, backup signing/encryption keys | zero occurrences in logs, in `/metrics`, in health payloads, in error messages, in `Debug` renders, and in any panic message; the sweep runs over both the in-process logs and the E2E daemon logs | the union of every credential M6 introduces; Q-32 |
| M6-126 | audit_covers_every_M6_admin_operation | one call of each: `ReloadPolicy`, `ReloadTls`, gossip add/use/remove, break-glass rollback | each produces exactly one `admin_op{op, principal, outcome, reason?}` line; denied calls produce an `outcome="denied"` line and no side effect; Q-22's assertions hold over the M6 op set | §18.2 "Audit … authorization changes, bootstrap, membership, backup, restore, and credential operations" — M6 adds the credential half |

---

## 9. E2E — process-level rows (E2E-40 …)

`crates/config-server/tests/e2e_daemon.rs` (TA-25). Shared shape as in the M5 plan §8, plus the
M6 configuration (`authz.*`, `tls.watch_files`, `list.*`, `--compat-schema`) written into each
node's TOML, and `--health-listen` on every node so `/metrics` and the health payload are
readable.

| ID | Name | Action | Expected | Notes |
|---|---|---|---|---|
| E2E-40 | daemon_policy_rotation_end_to_end | spawn 3 with `--form` and `authz.mode = "signed"` at policy v1; a client with a grant on `/a/` writes continuously; deploy v2 (adds `/b/`, removes `/a/` for a second principal) by writing the files to each node in turn, 1 → 2 → 3 | at every instant the observed access set is a subset of `allowed(v1) ∪ allowed(v2)` and never expands before node 3 has the document; after node 3, `/b/` is allowed everywhere and the removed grant is denied everywhere; `policy_converged` appears exactly once per node; the write load sees no non-retryable error | the whole of §3 through the real transport, the real files and the real poller. This is the row an operator's deploy pipeline is modelled on |
| E2E-41 | daemon_tls_rotation_with_restart_free_continuity | spawn 3; open a `Watch` and a paginated `List` walk; rewrite each node's leaf and CA bundle on disk; let `tls.watch_files` pick them up; then connect new clients with the new CA and, finally, drop the old CA | the watch never terminates and never gaps; the paginated walk completes correctly (it survives the reload because the pin is unaffected); new clients on the new CA succeed; after the old CA is dropped, old clients are refused; every daemon's PID and start time are unchanged for the entire test | the §20 "certificate rotation" gate at the process level; the pairing of §4.1 with §5 catches a reload implementation that resets per-connection state |
| E2E-42 | daemon_rolling_upgrade_v1_to_v2 | spawn 3 with `--compat-schema 1`; drive writes; restart node 3 without the flag, then node 2, then node 1; after activation, force a `Compact` and then attempt to restart node 3 **with** the flag again | no acknowledged write is lost; `cluster_min_schema` (from each node's health) is 1, 1, 1, then 2; `feature_activated` appears once per node after the third restart; the final restart with `--compat-schema 1` exits non-zero with the rollback-boundary message and the cluster keeps quorum | §20 "mixed-version rolling upgrade, feature gate, rollback boundary"; §17 end to end on real processes |
| E2E-43 | daemon_gossip_key_rotation_with_one_node_down | spawn 3; `shutdown_graceful` node 3; add + use the new key on nodes 1 and 2 via the admin RPC; restart node 3 with the old key in its accepted list; complete the rotation; remove the old key | the cluster never loses its leader; node 3 rejoins and converges; the final keyring is the new key alone on all three; no gossip key appears in any daemon log | D6.2's stated row at the process level |
| E2E-44 | daemon_pagination_across_a_leader_failover | spawn 3; start a paginated walk against the leader; kill the leader mid-walk; continue with the token against the new leader; restart the walk | the continuation returns `PageTokenExpired` with the documented reason and the documented trailer; the restarted walk returns a complete, self-consistent snapshot at the new leader's revision; no page ever mixes revisions | §16 and D6.3 through the transport; the client's documented recovery must actually work |
| E2E-45 | daemon_restore_refuses_the_client_plane_without_a_policy | take a backup of a signed-policy cluster; `restore` into three fresh dirs with a new cluster id, epoch and manifest, supplying **no** policy; then supply a valid policy and reload | before the policy: the cluster forms, replicates and is peer-healthy, but every client connection is declined and readiness is false; after the policy: client traffic succeeds and all keys read at their original revisions | §15.3's restore clause, at the level where it matters |
| E2E-46 | daemon_break_glass_rollback_is_audited | spawn 3 at policy v9; attempt to deploy v5 (refused); restart one node with `--break-glass-policy-rollback` and deploy v5 to it | the refusal on the two ordinary nodes and the acceptance on the break-glass node are both in the JSONL; the cluster enters and reports the converging state (the break-glass node is now *behind*, so the intersection applies); removing the flag and redeploying v9 restores convergence | the messy real-world case: break-glass on one node, not all three. The intersection rule must handle a node that went *backwards* |
| E2E-47 | daemon_evidence_run_produces_every_artifact | run the evidence suite against daemons at reduced scale, then assert the artifacts | all six files exist under `docs/evidence/`, validate against TA-61's schema, carry `full_scale: false` and a `scale_factor` matching §2's table, and name the same `git_sha`; re-running is idempotent (files are overwritten, not appended, and no stale file survives a renamed row) | makes the evidence set reproducible by one command, which is what "reproducible evidence" in §20 means |

---

## 10. Harness additions (summary of the required surface)

Everything below is new in M6 and is a precondition for the rows that cite it. Grouped by file.

**`crates/config-testkit/src/policy.rs`** — `PolicyFixture`, `PolicyBuilder`, the four corruption
writers (TA-54); `Cluster::{policy_version, policy_state, reload_policy,
advertised_policy_version}` (TA-55); `changed_prefixes` / `evaluate_converging` re-exported for
the property test (TA-56).

**`crates/config-testkit/src/rotation.rs`** — `CredentialSource`, `Reloaded`,
`Cluster::{reload_tls, rotate_files, served_leaf_fingerprint}` (TA-57); `GossipKeyring` and the
three keyring operations (TA-58); the injectable expiry clock and
`Cluster::cert_expiry(id, plane)` (TA-64).

**`crates/config-testkit/src/pagination.rs`** — `PageTokenView`, `decode_token`, `forge_token`,
`PinRegistry` (TA-59). All `#[cfg(feature = "testing")]`.

**`crates/config-testkit/src/schema.rs`** — `SchemaTriple`, `Cluster::{schema,
advertised_schema, peer_header_schema, cluster_min_schema, feature_activated}` (TA-60); the
harness-only "force a v2 entry into the log" back door used by M6-101 and by nothing else.

**`crates/config-testkit/src/capacity.rs`** — `WatchLoad`, `LoadCfg`, `LoadHandle`,
`spawn_watch_load`, `rss_bytes`, `apply_latency` (TA-62).

**`crates/config-testkit/src/matrix.rs`** — `partition_arrangements`, `crash_cases`,
`security_cases`, `SecurityCase` (TA-63).

**`crates/config-testkit/src/evidence.rs`** — `write_evidence` extended to TA-61's schema
(`schema`, `build`, `run.full_scale`, `disclaimer`), plus `evidence_scale()` reading
`RETCD_EVIDENCE`.

**Production-side seams M6 requires** (not harness): `HealthPayload.{policy_version,
policy_state, schema}`; `Capabilities.{authz: SignedPolicy, pagination: RevisionPinned, schema}`
(TA-66); admin RPCs `ReloadPolicy`, `ReloadTls`, `GossipKeyAdd/Use/Remove`; `ListRequest.
page_token` / `ListResponse.next_page_token`; the `schema` field on the peer `AppendEntries`
header; `--compat-schema` and `--break-glass-policy-rollback` on `config-server`; the
`GossipKeyring` replacement for `GossipConfig::secret_key`; timer-injected `authz.poll_interval`
and `tls.watch_files` pollers (TA-65).

---

## 11. Log-based assertions (DuckDB) — Q-27 …

### Q-27 — policy lifecycle and the termination-before-enqueue ordering (M6-01..M6-21, M6-31, M6-117..M6-119)

```sql
SELECT node_id, "@m" AS msg, version, active_version, reason, source,
       voters_reporting, voters_total, count(*) AS n
FROM read_json_auto('target/test-logs/**/*.jsonl', union_by_name=true)
WHERE testMethod = ?
  AND "@m" IN ('policy_loaded','policy_rejected','policy_converged','policy_rollback')
GROUP BY ALL ORDER BY node_id, msg;
```

**Assertions:** a positive row count wherever a load is expected; `policy_loaded` appears once per
adoption and never more; `policy_rejected.reason` is confined to the M6-118 closed set;
`policy_converged` never appears with `voters_reporting < voters_total`; no column named `grant`,
`prefix`, `principal_list` or `value` exists in the result.

Ordering half (M6-31), joined against the watch lines:

```sql
WITH v AS (SELECT node_id, "@t" AS t, version FROM read_json_auto(?, union_by_name=true)
           WHERE testMethod = ? AND "@m" = 'policy_loaded'),
     term AS (SELECT node_id, "@t" AS t, stream_id FROM read_json_auto(?, union_by_name=true)
           WHERE testMethod = ? AND "@m" = 'watch_terminated' AND reason = 'policy_changed'),
     enq AS (SELECT node_id, "@t" AS t, stream_id FROM read_json_auto(?, union_by_name=true)
           WHERE testMethod = ? AND "@m" = 'watch_event_enqueued' AND policy_version = (SELECT max(version) FROM v))
SELECT enq.node_id, enq.stream_id, enq.t
FROM enq JOIN term USING (node_id, stream_id)
WHERE enq.t <= term.t;
```

**Assertion:** the result set is **empty** — no event under the new version was enqueued at or
before the affected stream's termination (§15.3).

### Q-28 — the intersection never expanded access (M6-17, M6-23, M6-24)

```sql
SELECT "@m" AS msg, principal, prefix_hash, action, outcome, reason, count(*) AS n
FROM read_json_auto(?, union_by_name=true)
WHERE testMethod = ? AND "@m" = 'authz_decision'
GROUP BY ALL ORDER BY n DESC;
```

**Assertions:** during a converging window, every `outcome='allowed'` row's `(principal,
prefix_hash, action)` triple is present in the recorded allow-sets of **both** documents (the test
writes those sets to the log once as `policy_allowset{version, digest}` and the query joins
against them); `reason='policy_converging'` appears only on denials, never on an allow.
`prefix_hash` rather than `prefix` keeps M6-124 true.

### Q-29 — rotation and expiry (M6-41..M6-64, M6-120, M6-121)

```sql
SELECT node_id, "@m" AS msg, plane, source, stage, reason, days_remaining,
       leaf_fingerprint, key_fingerprint, count(*) AS n
FROM read_json_auto(?, union_by_name=true)
WHERE testMethod = ?
  AND "@m" IN ('tls_reloaded','tls_reload_failed','gossip_key_rotated','cert_expiring')
GROUP BY ALL ORDER BY node_id, msg;
```

**Assertions:** every `tls_reloaded` carries a `leaf_fingerprint` that differs from the previous
one for that `(node_id, plane)` (a reload that changed nothing must not log); `cert_expiring` has
`n == 1` per `(node_id, plane, subject)` per crossing; `gossip_key_rotated.stage` is confined to
`{add, use, remove}` and the three stages appear in that order per key.

### Q-30 — page-token rejections match the counters (M6-67..M6-83, M6-122)

```sql
SELECT node_id, reason, count(*) AS n
FROM read_json_auto(?, union_by_name=true)
WHERE testMethod = ? AND "@m" = 'page_token_rejected'
GROUP BY ALL ORDER BY n DESC;
```

**Assertions:** `reason ∈ {mac, expired, evicted, node, policy_version, token_version,
prefix_mismatch, token_principal}` and nothing else; the per-reason totals equal the
`PinRegistry::misses_by_reason()` deltas the test recorded; no row carries a `token`, `last_key`
or `key` column.

### Q-31 — schema advertisement, gating and activation (M6-85..M6-104, M6-123)

```sql
SELECT node_id, "@m" AS msg, schema, cluster_min_schema, feature, voters, count(*) AS n
FROM read_json_auto(?, union_by_name=true)
WHERE testMethod = ?
  AND "@m" IN ('feature_activated','feature_gated','schema_refused','schema_advertised')
GROUP BY ALL ORDER BY node_id, msg;
```

**Assertions:** `feature_activated` has `n == 1` per node per activation and never appears while
any row reports `cluster_min_schema < 2`; `feature_gated` is rate-limited (at most one per window
per feature per node); every `schema_refused` names both the offered and the accepted schema.

### Q-32 — no credentials, values, key bytes or token bytes in any M6 line (M6-75, M6-124, M6-125)

```sql
SELECT "@m" AS msg, count(*) AS n
FROM read_json_auto('target/test-logs/**/*.jsonl', union_by_name=true)
WHERE testMethod = ?
  AND (to_json(COLUMNS(*))::VARCHAR ILIKE '%' || ? || '%')   -- one sentinel per invocation
GROUP BY ALL;
```

**Assertion:** zero rows for every sentinel. The M6 sentinel set is the M5 set plus: the policy
signing key and every trust key's private half, `list.token_key`, every gossip key (hex and
base64), every issued page token's text, and every `last_key` byte string. Run once over the
in-process logs and once over the E2E daemon logs.

### Q-33 — every evidence row wrote exactly one artifact (M6-105..M6-116)

```sql
SELECT "@m" AS msg, name, path, scale_factor, full_scale, git_sha, count(*) AS n
FROM read_json_auto(?, union_by_name=true)
WHERE testMethod LIKE 'm6_1%' AND "@m" = 'evidence_written'
GROUP BY ALL ORDER BY name;
```

**Assertions:** one row per evidence `name`, with `n == 1` (a second writer for one file is
TA-61's violation); every `git_sha` in a single run is identical; `full_scale` is uniform across
the run; every `name` in the result appears in §7's file list and vice versa.

---

## 12. Anti-flake additions (extend rules 1..31 from the M0-M1, M2-M3, M4 and M5 plans)

32. **Policy rows assert the version moved before asserting its effect.** Read `policy_version`
    (or `policy_state`) and assert the expected transition *first*; only then assert an access
    outcome. A policy row where the document never loaded is a silent pass — the §3 analogue of
    rules 19, 21 and 29.
33. **Poll intervals are advanced, never waited on.** `authz.poll_interval` and
    `tls.watch_files` are driven through `TestTimers` and awaited through a `Notify` on the
    tick's completion (TA-65). A test that waits for a real 10 s or 30 s tick is a review
    rejection and would blow §2's budget by itself.
34. **Rotation rows carry a positive control.** Prove the *old* credential worked immediately
    before the rotation and that the *new* one works immediately after, both through a real
    handshake. A row that only asserts the new one cannot distinguish a working rotation from a
    node that was already serving the new certificate.
35. **Rotation rows assert the process did not restart.** PID and
    `retcd_process_start_time_seconds` unchanged. Otherwise "reload" and "restart" are
    indistinguishable, and the claim being tested is restart-freedom.
36. **Pagination rows assert `next_page_token` is non-empty before continuing**, and assert the
    final page's token is empty. A walk that silently ends after one page would otherwise satisfy
    "no duplicates, no omissions" on a one-page prefix.
37. **Mixed-version rows read `cluster_min_schema` from the node**, never infer it from which
    binaries the test started. The computation is the thing under test.
38. **Evidence rows never assert a threshold.** They assert correctness invariants that hold at
    any scale and *record* numbers. A performance assertion in `tests/m6_evidence.rs` is a review
    rejection (rule 23, restated because §7 is where it will be tempting).
39. **Evidence rows write exactly one file, and no other row writes under `docs/evidence/`.**
    Enforced by Q-33 and by M6-113.
40. **Capacity rows take stream counts, queue bytes and terminations from the server's scraped
    counters** (rule 28 extended); the client-side view is a cross-check only.
41. **Security-matrix rows assert the documented reason string**, not merely that an error
    occurred. Two different refusals collapsing to one generic error is the defect the matrix
    exists to catch.
42. **Token-forging rows use the real configured HMAC key** for the "valid MAC, wrong content"
    cases, and an explicitly wrong key only for the `reason="mac"` case. Forging with the wrong
    key everywhere would make every binding row pass for the wrong reason.

---

## 13. Gate checklist — §21 M6 and §20 (all subsections) → test IDs

A gate passes only when **every** listed ID passes. A bullet whose disposition is *evidence* is
green when its artifact exists at `full_scale: true` and its asserted invariants held — never
because a number looked good.

### §21 M6 scope lines

| §21 M6 scope item | Test IDs / disposition |
|---|---|
| signed distributed RBAC lifecycle | M6-01..M6-40, M6-117..M6-119, M6-126, E2E-40, E2E-45, E2E-46 |
| certificate and gossip-key rotation | M6-41..M6-64, M6-120, M6-121, E2E-41, E2E-43 |
| revision-pinned pagination | M6-65..M6-84, M6-122, E2E-44 |
| full watch capacity validation | M6-105 (**dev-host evidence only**; correctness invariants asserted, numbers recorded) |
| mixed-version upgrades and migrations | M6-85..M6-104, M6-123, E2E-42 |
| broader fault/security matrix | M6-107..M6-112 (**dev-host evidence only** for the recorded numbers; the per-case refusal and invariance assertions are ordinary pass/fail) |
| measured RPO/RTO and capacity envelope | M6-106, M6-105 (**dev-host evidence only**; §12.2's objectives are explicitly **not** claimed — M6-116) |

### §20 — Consensus and storage

| §20 bullet | Test IDs / disposition |
|---|---|
| crash injection before/after vote sync, log append, log flush, state batch, snapshot publish, snapshot install | M6-108 (enumerated over `Boundary::ALL`), building on M2-28..M2-46 and M5-11..M5-13, M5-30..M5-33 |
| no vote regression, log holes, lost acknowledged mutations, duplicate revisions, inconsistent last-applied | M6-108's `assert_crash_invariants` per boundary; M6-107 per partition arrangement |
| process kill | M6-108, E2E-42, E2E-43 (process-level), M2/M5 E2E rows |
| VM pause, power-loss simulation | **Not covered, and no milestone owns it.** OQ-24 (M2-M3) deferred these "past the first release" and nothing since has picked them up. M6 is the last milestone, so this is now a permanent gap unless the Architect scopes it — see §15 item 5. Release notes must not imply coverage |
| I/O error, ENOSPC, corruption | M2-38..M2-44 (`Fail(Io)`, ENOSPC, corruption) re-run inside M6-108's matrix; no new M6 row |
| long compaction | **Not covered** — same disposition as VM pause; §19.12's compaction clause is covered *behaviourally* by M5-23 and M6-81, but a long-compaction fault injection has no owner |
| deterministic replay / differential testing of identical command logs | C-01..C-15 (M0-M1 conformance) plus M6-95's `state_hash` convergence across the upgrade; no new M6 row |

### §20 — Network and consistency

| §20 bullet | Test IDs / disposition |
|---|---|
| every three-node partition arrangement | M6-107 (enumerated, count asserted against `partition_arrangements`) |
| former-leader rejection of strict reads and writes without quorum | M6-107's per-arrangement assertion; M1/M2 rows remain the fine-grained coverage |
| leader loss during a write with lost response | M6-107; E2E-44 (leader loss mid-pagination); M2/M5 rows |
| concurrent CAS where exactly one expected-revision mutation succeeds | M0-M1 and M2-M3 rows; unchanged by M6; asserted again inside M6-107's write load |
| deadlines, cancellation, retry storms, oversized requests, wrong leader hints | M3 rows; M6 adds the paginated-oversize case (M6-80) and the gated-feature retry class (M6-93) |

### §20 — Watches

| §20 bullet | Test IDs / disposition |
|---|---|
| deterministic interleaving of barrier, registration, replay, apply, live drain | M4 (W-01..W-12, M4-37..M4-52); M6 adds M6-31's policy-change interleaving |
| leader change during replay and live streaming | M4-78..M4-88 |
| resume from the last processed revision | M4 rows; re-exercised by M6-105's disconnecting population |
| compaction followed by typed resync | M4-53..M4-61; M5-44 after an install |
| 1,000 watchers including slow and disconnected populations | M6-105 (**dev-host evidence**; deferred out of M4 by M4 §11 item 1 and landed here) |
| bounded memory and no Raft apply starvation | M6-105's asserted half (apply-starvation predicate, queue high-water), plus M4-62..M4-77 and M6-81 |

### §20 — Gossip and identity

| §20 bullet | Test IDs / disposition |
|---|---|
| false suspicion, partition, one-way loss, stale packets, poisoned endpoint, all seeds unavailable | M6-110 (enumerated), building on M1/M3 gossip rows |
| duplicate Node ID | M6-109; M5-70 (dir reuse) remains the storage-side row |
| key rotation | M6-57..M6-61, M6-110, E2E-43 |
| version skew | M6-111, M6-85..M6-104 |
| rejection of wrong Node ID, cluster ID, certificate identity, destination binding | M6-109; M6-50, M6-53 for the rotated-credential cases |
| proof that gossip cannot mutate membership or configuration | M6-112 (invariance over a hostile soak), M6-23, M6-102 |

### §20 — Operations

| §20 bullet | Test IDs / disposition |
|---|---|
| initial genesis and restart | M2-06, M2-08, E2E-08; M5-92 for the restored case; M6-100 for restart after activation |
| learner replacement interrupted at each phase, including joint membership | M5-64..M5-69, E2E-31; M6 adds M6-88 (a learner does not gate a schema activation) |
| old-node fencing and stale rejoin rejection | M5-61, M5-62, M5-70..M5-72, E2E-36; M6 adds M6-53 (fenced by CA removal) — together these discharge M5's §14 item 4, where certificate fencing proper was deferred to M6 |
| certificate rotation while one voter is unavailable | M6-51..M6-54, E2E-41, E2E-43 |
| encrypted backup verification and full fenced restore within RPO/RTO | M5-79, M5-80, M5-93 for the mechanism; M6-106 for the measurement (**dev-host evidence only**; the "within RPO/RTO" clause is explicitly not claimed — M6-116, §15 item 7) |
| mixed-version rolling upgrade, feature gate, rollback boundary, snapshot compatibility | M6-95, M6-96, M6-97, M6-98, M6-103, E2E-42 |

### §19 invariants explicitly in M6 scope

| Invariant | Test IDs |
|---|---|
| §19.9 — gossip, seeds, DNS and health observations never confer authority | M6-23, M6-102, M6-110, M6-112 |
| §19.10 — existing storage cannot attach to a different cluster, epoch or Node ID | M6-50, M6-53, M6-97, M6-98, M6-109 |
| §19.12 — client load, List cursors, slow watchers, gossip traffic, backups or compaction cannot block Raft progress without bounded rejection and alerting | M6-26, M6-68, M6-69, M6-81, M6-82, M6-90, M6-105, M6-123 |

---

## 14. Open questions (OQ-55 …) — the recommendation is the default

Implement the recommendation unless the Architect answers otherwise. Record the answer in the
owning ADR before the blocked row is written.

| ID | Question | Blocks | Owner ADR | Recommendation (default) |
|---|---|---|---|---|
| OQ-55 | Is the "changed prefix" set computed by exact prefix string, or by any overlap between old and new grant prefixes? A new grant on `/a/` and an old grant on `/a/b/` overlap without being equal. | TA-56, M6-17..M6-19, M6-24 | ADR-0027 | **Overlap**, not equality: a key is "under a changed prefix" if any grant mentioning a prefix that is a prefix-of or prefixed-by the key differs between the documents. Exact-string comparison lets a narrowing edit at a deeper level slip through the intersection, which is precisely the early expansion §15.3 forbids. Compute it once per document pair and cache it; it is O(grants), not per request. |
| OQ-56 | Is the converging state driven by gossip-advertised `policy_version` alone, or by a replicated value? | TA-55, TA-56, M6-21, M6-23 | ADR-0027 | **Gossip-advertised, explicitly labelled advisory**, as D6.1 says — *and* ADR-0027 must state plainly that the intersection is a convergence courtesy, not a security boundary, because a forged advertisement can end the narrowing early (M6-23). Replicating the version would make a policy deploy a Raft write, which couples an external artifact's rollout to quorum availability. Take the honest weaker property and write it down. |
| OQ-57 | Is `--break-glass-policy-rollback` one-shot or process-scoped? | M6-09, M6-10, E2E-46 | ADR-0027 | **Process-scoped**, because the operator already restarted the node to set it, and a one-shot flag that silently re-arms on the next restart is worse. Every use is audited individually (M6-09), and `retcd_policy_rollbacks_total` plus a permanent `break_glass_active` gauge make the state visible for as long as it lasts. |
| OQ-58 | Does `ReloadPolicy` authorize against the currently active document's `admins`, or the incoming one's? | M6-12, M6-40 | ADR-0027 | **The currently active document.** Otherwise an unsigned-by-you document could name you an admin and then be loaded by you — the incoming document must never authorize its own adoption. State it in the ADR because the opposite reading is the natural one to implement. |
| OQ-59 | Can tonic 0.12 host a swappable `rustls::ServerConfig`, or is the hyper + `tokio-rustls` fallback required? | TA-57, M6-41..M6-56, all of §4 | ADR-0028 | This is a developer research task D6.2 already assigns. **Default: write every row implementation-neutrally (TA-57) and record the choice in ADR-0028 with M6-56.** The plan must not block on the answer, and no row may name the acceptor type — if a row cannot be written without knowing, it is testing the implementation rather than the behaviour. |
| OQ-60 | Does one `ReloadTls` rotate client and peer planes together, or are they separate operations? | M6-41, M6-49, M6-56 | ADR-0028 | **One RPC, both planes, with a per-plane result** (`Reloaded` per plane) and per-plane failure isolation: a bad client leaf must not prevent the peer plane from picking up a good one. Two RPCs double the operator procedure and the audit surface for an operation that is always performed together in practice. |
| OQ-61 | What happens on `remove_key` when a known peer still needs that key? | M6-59, M6-60 | ADR-0028 | **Refuse**, with a typed error naming the peers that still advertise the key's fingerprint, unless `--force`. Gossip is advisory, so a botched removal is survivable (M6-58) — but survivable is not the same as acceptable, and the operator has no other way to know. |
| OQ-62 | Prefix/principal mismatch on a page token: `PageTokenExpired` or a distinct error? | M6-76, M6-77 | ADR-0029 | **Distinct errors:** `InvalidArgument{prefix_mismatch}` for a prefix mismatch (a client bug — telling them to retry the walk would loop) and `PermissionDenied{token_principal}` for a principal mismatch (a security event that must be visible as one). Reserve `PageTokenExpired` for the five genuinely transient causes: `mac`, `expired`, `evicted`, `node`, `policy_version`. |
| OQ-63 | Is `cluster_min_schema` a replicated committed value, or leader-local state? | TA-60, M6-88, M6-89, M6-102 | ADR-0030 | **Leader-local, computed from committed membership plus peer-plane responses** — which is what D6.4 describes and what the propose-time gate (A7) needs. Research §7 warns that a *gossip-derived* level lets two leaders disagree; committed membership plus peer responses does not have that problem, because a leader that cannot reach a voter simply keeps the minimum low (M6-89), which is the safe direction. Replicating it would need a command to raise it, and that command would itself need gating. |
| OQ-64 | A dedup-bearing mutation before activation: refuse, or apply without the dedup record? | M6-92 | ADR-0030 / ADR-0025 | **Refuse** with `Unavailable{feature_not_activated}`. Applying without the record leaves a client believing it holds a retained request identity it does not hold, which is exactly the misplaced confidence ADR-0015 and §16 exist to prevent. |
| OQ-65 | Does `--compat-schema 1` also refuse to *open* a `format_version = 2` store, or only refuse v2 commands? | M6-94, M6-97, M6-98 | ADR-0030 / ADR-0021 | **Refuse to open.** A v2 store may already contain a v2 `events` CF and v2-encoded state; serving from it while claiming schema 1 is the silent-divergence case. Refusing at open is also what a genuine v1 binary does, so the simulation stays faithful. Fix the check order (M6-98) so the operator sees a version error, not a column-family error. |
| OQ-66 | Are the reduced-scale factors in §2 fixed constants, or derived from host capability? | TA-61, M6-114, M6-115, §2 | ADR-0031 | **Fixed constants**, recorded in the artifact. A host-derived factor makes two runs incomparable and makes a regression indistinguishable from a slower machine. If a host cannot meet even the reduced scale, the row records the shortfall and marks `full_scale: false` (M6-115) rather than silently shrinking further. |
| OQ-67 | Does the M6 daemon default flip `authz.mode` to `signed`? | M6-36, M6-37, E2E-40 | ADR-0027 | **Yes for the M6 daemon default, with a documented upgrade note** (D6.1 says so), **and** `static` remains fully supported and fully tested (M6-36) so the M3 release stays reproducible. A default that fails closed on a missing file is correct for a security default but must be called out loudly in the release notes, because it turns an omitted configuration into an unready node. |
| OQ-68 | Do M6 evidence rows run in ordinary CI, or only on the dev host? | TA-61, M6-114, §2, E2E-47 | ADR-0031 | **Both, at different scales** — reduced scale in ordinary CI (so the rows are real regression gates and cannot rot), full scale on the dev host under `RETCD_EVIDENCE=1` (so the artifacts are meaningful). `#[ignore]` is not used anywhere in §7, because an ignored row is an untested row. The M6 gate script checks for full-scale artifacts and fails if only reduced-scale ones exist. |

---

## 15. Spec / brief / research / code contradictions found (for the Architect)

These are places where the authoritative documents disagree with **each other** or with shipped
code. Each needs a decision, not a test.

1. **§15.1 requires a separate admin plane; M5 put the admin service on the client-plane
   listener.** §15.1's table lists Admin as a distinct plane with "distinct ports, credentials,
   rate limits, and preferably trust profiles or intermediate CAs". The M5 plan's OQ-43 (formerly
   OQ-42, renumbered per M6-R6) chose the
   client-plane listener plus an `admins` allowlist "for M5", and stated that the decision should
   be revisited "in M6 when signed RBAC lands (D6.1)" and that "a separate port becomes mandatory
   the day admin calls can be made by a non-operator identity". M6 is that day: D6.1 puts the
   admin set inside a signed, remotely-deployed document, so admin identity is now managed by an
   artifact the node does not control. Either M6 splits the admin listener (new port, new
   certificate profile, new fencing surface, and roughly six new rows), or ADR-0023/ADR-0027
   records the deviation from §15.1 as permanent. **This plan assumes the client-plane listener
   continues** (M6-12, M6-40 are written that way) and flags the decision.

2. **§15.3's "convergence within 30 seconds" has no gate row and no owner.** The spec calls it "a
   provisional operational objective requiring measurement". No §7 evidence row measures it, and
   none should gate on it (the user ruling). Recommend adding convergence time to M6-105's or a
   new evidence artifact's `values` so it is *recorded*, and stating in ADR-0027 that the 30 s
   figure is a planning assumption like §12.2's RPO/RTO — not an acceptance line.

3. **The intersection rule consumes gossip, and §19.9 says gossip confers no authority.** D6.1
   advertises `policy_version` in gossip meta and uses it to decide whether the fail-closed
   intersection is in force. That is gossip-derived state influencing an authorization outcome.
   The saving argument is that it can only ever *narrow* access (TA-56.1, M6-23's containment
   property), so a forged advertisement cannot grant anything — it can only end the narrowing
   early, i.e. reach the new policy's grants sooner than intended. That is a real weakening and
   must be written down: **the intersection is a convergence courtesy, not a security boundary.**
   ADR-0027 must say so, or a reader will assume §15.3 bullet 4 is a defence against an attacker
   rather than against a slow rollout. OQ-56.

4. **The backup manifest's `policy_version` cannot be validated at restore.** §15.3 says artifacts
   "reference, but do not contain or override" the RBAC artifact, and that restore "confirms that
   an independently supplied signed policy is active". Those two clauses together mean the
   manifest's `policy_version` is un-checkable: the supplied policy may legitimately be older,
   newer or unrelated (M6-35). So the field is a breadcrumb for a human, not a validation input.
   ADR-0024/ADR-0027 should say that explicitly, or an implementer will add a check that makes
   restores fail whenever policy has moved on since the backup — which is always.

5. **VM pause, power-loss simulation and long compaction now have no owner at all.** §20's
   consensus-and-storage list includes them. The M2-M3 plan deferred them via OQ-24 "past the
   first release"; the M5 plan's §12 repeated "**not covered** — OQ-24 deferred these past the
   first release and M5 does not reopen them"; M6 is the last milestone in §21. Either the
   Architect scopes them into M6 (they need host-level tooling — a hypervisor or a
   `dm-flakey`/`fsync`-lying block layer — that the harness does not have and that the dev host
   may not support), or §20 is amended to mark them as post-M6 production-qualification work
   performed by the operator on target hardware. **Silence here would let "production-capable"
   be claimed with a known §20 bullet unmet.** §13 marks it explicitly.

6. **The 1,000-watcher bullet moved from gate to evidence.** §20 lists it under production
   designation; §21 M4 called it "a later performance gate"; the M4 plan's §11 item 1 asked for
   confirmation that it is an M6 gate. This plan lands it as M6-105 — an **evidence** row per the
   user ruling, with correctness invariants asserted and numbers recorded. That is a defensible
   reading of §20 ("reproducible evidence"), but it is a reading: nobody has said what number of
   streams or what p99 would constitute a *pass*. Recommend ADR-0031 state that §20's watch
   bullet is discharged by reproducible evidence plus the asserted no-gap/no-starvation
   invariants, and that no numeric threshold is claimed.

7. **§12.2's RPO/RTO objectives still cannot be claimed, even at M6.** "Provisional maximum RPO:
   60 minutes" and "RTO objective for 1 GiB live state: 60 minutes" require "repeated measured
   restores on the target VM, disk, network, encryption, and backup systems". M6-106 measures
   **once**, on the dev host, at a recorded scale. The M5 plan's §14 item 8 said the same thing
   about M5-93. Two milestones have now declined to claim these numbers; §12.2 should be amended
   to mark them as operator obligations, or §21 M6's "measured RPO/RTO" line will read as met
   when it is not. M6-116 is the row that keeps the documents honest.

8. **D6.4 computes the feature level from peer-plane responses; research §7 demands a
   *replicated, committed* value.** Research §7 is explicit: "The cluster's effective feature
   level must itself be a **replicated, committed** value (a log entry), not gossip-derived
   state, or two leaders could compute different levels." D6.4 computes `cluster_min_schema` from
   "min over committed voters' last-reported schema (peer plane responses carry it)" — which is
   *not* gossip, but is also *not* replicated. The saving argument is that a leader that cannot
   reach a voter keeps the minimum low (M6-89), so divergence is always in the safe direction and
   two leaders cannot both compute a *higher* level than reality. ADR-0030 must record that
   argument explicitly (OQ-63), because research §7's wording, read literally, requires a design
   D6.4 does not have.

9. **`Authz` and `Pagination` capability enums must grow, and that is a breaking public change.**
   `crates/config-core/src/capabilities.rs` defines `Authz::{Development, StaticAllowlist}` and
   `Pagination::{Unsupported}` — the latter with a doc comment stating that continuation tokens do
   not exist. Every existing capability assertion in the test suite and in `e2e_daemon.rs` pins
   those strings. This is the same ripple `WatchResumption::Retained` caused in M4 (M4 §11 item
   10). Flag it in ADR-0016's clarifications so the churn is intentional (TA-66).

10. **`GossipConfig::secret_key` is a single key; staged rotation needs a keyring.**
    `crates/config-gossip/src/config.rs:40` is `secret_key: Option<[u8; 32]>`, and
    `crates/config-gossip/src/node.rs:244-248` installs exactly one key via
    `with_encryption_algo(EncryptionAlgorithm::NoPadding)`. D6.2's `add_key` → `use_key` →
    `remove_key` sequence is not expressible against that type, and whether the pinned
    `memberlist` version exposes a keyring API at all is **unverified** — the research note does
    not cover memberlist. This is both a code change (TA-58) and a dependency-capability question
    that must be settled before §4.3 can be written. If memberlist cannot do staged keys, the
    honest fallback is a coordinated key change with a documented gossip outage window, and
    M6-57..M6-61 change shape accordingly.

11. **Certificate fencing: M5 deferred it here, and M6 delivers a weaker thing than §13.2 asks.**
    The M5 plan's §14 item 4 recorded that §13.2 requires "network- and certificate-fence" at M5
    while D5.2 deferred certificate fencing (CRL) to M6 rotation, and ruling M5-R4 accepted the
    retired-id check as sufficient for M5. M6's D6.2 ships **rotation**, not **revocation**:
    M6-53 fences an old node by removing its CA from the bundle, which revokes every certificate
    issued by that CA, not one identity. There is no CRL and no OCSP. A single compromised leaf
    cannot be revoked without rotating everyone. Either ADR-0028 records that CA-level rotation is
    the revocation mechanism (recommended, given a three-node cluster and a private CA), or M6
    owes a CRL story and rows for it.

12. **The page-token payload omits three of §10.2's seven required bindings.** §10.2: the design
    "uses a short-lived RocksDB read snapshot bound to prefix, revision, cursor position,
    principal, policy version, token version, and expiry". D6.3's token is HMAC over
    `{revision, last_key, policy_version, issued_ms, node_id}` — it has revision, cursor position
    (`last_key`), policy version and expiry (via `issued_ms` + TTL), but **no prefix, no
    principal and no token version**, and it adds `node_id`, which §10.2 does not mention. A token
    without a principal binding is transferable between clients; a token without a prefix binding
    can be replayed against a different prefix at the pinned revision. M6-76, M6-77 and M6-78 are
    written against §10.2's list and will fail against D6.3's payload as specified. ADR-0029 must
    reconcile the two — the recommendation is to adopt §10.2's list in full and keep `node_id`.

13. **The M4 and M5 plans collided on `TA-40` and `OQ-40`. Resolved (M6-R6).**
    `docs/testing/test-plan-m4.md` §1 defines TA-40 as "`format_version` 2 and the `events` column
    family" and its §10 defines OQ-40 as the `journal_gate` placement; `docs/testing/test-plan-m5.md`
    §1 originally also defined a TA-40 (the six new fault boundaries) and its §13 an OQ-40 (the
    `Boundary::ALL` enum-split question). The M5 plan's own numbering note anticipated this ("If the
    M4 plan allocates beyond those reservations, the M4 plan wins and this plan is renumbered") but
    the renumbering was not performed until this ruling. **Fix applied:** `test-plan-m5.md`'s
    identifiers are shifted by one — TA-40..52 → TA-41..53, OQ-40..53 → OQ-41..54 — so M4's TA-40 /
    OQ-40 are now unambiguous and every M5 cross-reference (including "TA-40.4" in M5 §3.3/M5-23,
    now "TA-41.4") was updated in the same pass; `docs/testing/test-plan-m4.md` is untouched.
    `grep -n "TA-40\|OQ-40" docs/testing/test-plan-m5.md` returns nothing. M6 itself starts at
    TA-54 / OQ-55, unaffected by this fix beyond the renumbering of this document's own TA-53..65 /
    OQ-54..67 → TA-54..66 / OQ-55..68 to keep pace with the M5 shift.

---

## Row counts

| Section | Rows | IDs |
|---|---|---|
| §3 Signed RBAC lifecycle | 40 | M6-01 … M6-40 |
| §4 Credential rotation | 24 | M6-41 … M6-64 |
| §5 Revision-pinned pagination | 20 | M6-65 … M6-84 |
| §6 Mixed-version upgrades and migrations | 20 | M6-85 … M6-104 |
| §7 Evidence rows (dev host) | 12 | M6-105 … M6-116 |
| §8 Logging, redaction and audit | 10 | M6-117 … M6-126 |
| §9 E2E process-level | 8 | E2E-40 … E2E-47 |
| **Total** | **134** | |

New test-architecture requirements: **TA-54 … TA-66** (13).
New DuckDB queries: **Q-27 … Q-33** (7).
New anti-flake rules: **32 … 42** (11).
New open questions: **OQ-55 … OQ-68** (14), each with a default.
New evidence artifacts: **6** (`watch-capacity`, `rpo-rto`, `partition-matrix`, `crash-matrix`,
`security-matrix`, `gossip-authority`), each written by exactly one row.
