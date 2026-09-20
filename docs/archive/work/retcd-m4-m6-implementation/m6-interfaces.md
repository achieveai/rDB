# M6 interface contract (lead, 2026-09-18) — binding for the M6 developers

Authority: architecture-m4-m6.md D6.1–D6.5 + Amendments A1–A11 + "Lead rulings on test-plan-m6" (M6-R1..R6);
ADR-0027..0031 (Decision sections are normative; this file only fixes names, owners, and sequencing);
docs/testing/test-plan-m6.md (renumbered: TA-54..66, OQ-55..68). Builds on M4 (m4-interfaces.md) and M5
(m5-interfaces.md) as landed. HITL ruling: M6 = "features-local" — every feature implemented; evidence rows
are dev-host artifacts, never production claims.

Ownership (five developers, max three concurrent; sequencing at the end). One writer per file. A developer
who needs a change in a file it does not own STOPS and sends the lead a patch note (file, hunk, reason).

- **dev-rbac** (ADR-0027): `config-core/src/authz.rs` (+ new `policy.rs`), `config-core/src/capabilities.rs`
  (`Authz::SignedPolicy` only), `config-server/src/config.rs` (`[authz]` keys), `config-server/src/run.rs`
  (policy loader + poller wiring), `config-server/src/health.rs` (`policy_version` field), `config-grpc`
  AdminService `ReloadPolicy`, `config-engine/src/watch.rs` policy-change termination hook, `config-gossip`
  meta `policy_version` field, `config-client` (nothing new; verify PermissionDenied{policy_changed} surfaces).
- **dev-pagination** (ADR-0029): `config-core/src/store.rs` (List token types), `config-core/src/error.rs`
  (`PageTokenExpired`, `InvalidArgument{prefix_mismatch}`, `PermissionDenied{token_principal}`),
  `config-core/src/capabilities.rs` (`Pagination::RevisionPinned` only — coordinate: dev-rbac edits the same
  file; dev-pagination lands its enum hunk FIRST, dev-rbac rebases), `config-storage` (`StateReader::pin(revision)
  -> PinnedView`), `config-engine/src/direct.rs` + new `pagination.rs` (LRU), `config-grpc/src/client_plane.rs`
  List path + `proto` `ListRequest.page_token`/`ListResponse.next_page_token`, `config-client` `list_pages()`.
- **dev-evidence** (ADR-0031): `config-testkit/src/evidence.rs` (`write_evidence`), `config-testkit/tests/m6_evidence.rs`,
  `docs/evidence/README.md`, `docs/evidence/*.json` (generated), `scripts/evidence-gate.ps1`.
- **dev-compat** (ADR-0030): `config-core/src/schema.rs` (`SchemaTriple`), `config-gossip/src/meta.rs` (schema in
  hint meta), `config-grpc/src/peer_plane.rs` + `proto` AppendEntries header field, `config-engine/src/node.rs`
  (`cluster_min_schema`, propose-time gate), `config-storage/src/rocks.rs` open ordering (format check before CF
  verify), `config-server/src/cli.rs` `--compat-schema`, `config-server/src/health.rs` (schema field — after
  dev-rbac has landed).
- **dev-rotation** (ADR-0028): `config-grpc/src/tls.rs` (swappable resolver), `config-grpc/src/server.rs`,
  `config-server/src/run.rs` (TLS poller — after dev-rbac has landed), AdminService `ReloadTls` +
  `RotateGossipKey`, `config-gossip/src/config.rs` + `node.rs` (keyring or `secondary_key`), `config-engine/src/metrics.rs`
  `retcd_cert_expiry_seconds`.

## config-core
```rust
// policy.rs (dev-rbac)
pub struct PolicyDocument { pub version: u64, pub issued_unix_ms: u64, pub grants: Vec<Grant>, pub admins: Vec<String> }
// Grant reuses authz::Grant { principal, prefix, actions } — do NOT add a parallel type.
pub struct SignedPolicy { pub document: PolicyDocument, pub hash: [u8; 32], pub bytes: Bytes }
pub enum PolicyRejected { HashMismatch, UntrustedSigner, SignatureInvalid, SignatureFileMissing, PolicyFileMissing,
    VersionBinding, Rollback { active: u64, incoming: u64 }, Malformed { detail: String } }   // closed set, Display = snake_case reason
pub fn verify_policy(doc_bytes: &[u8], sig: &[u8], trust_keys: &[(String, VerifyingKey)]) -> Result<SignedPolicy, PolicyRejected>;
// Signature payload = sha256(doc_bytes) || version.to_le_bytes(). Signer set = named ed25519 keys.
pub fn evaluate_converging(old: &PolicyDocument, new: &PolicyDocument, principal: &str, action: Action, key: &[u8]) -> Decision;
// Pure: allowed(new) on unchanged prefixes; allowed(old) && allowed(new) on changed prefixes (overlap rule). No I/O, no clock.
pub fn changed_prefixes(old: &PolicyDocument, new: &PolicyDocument) -> Vec<Bytes>;   // overlap semantics, sorted, deduped
// authz.rs: `Authorizer` trait gains `fn policy_version(&self) -> Option<u64>` (StaticAllowlist -> None).
// capabilities.rs: Authz::SignedPolicy { policy_version: Option<u64> }; Pagination::RevisionPinned { max_pinned: u32, ttl_ms: u64 }.
// error.rs additions:
//   ConfigError::PermissionDenied gains `policy_changed: bool` (default false; existing constructors unchanged),
//   ConfigError::PageTokenExpired { reason: PageTokenExpiredReason }  -> StatusClass::FailedPrecondition, trailer retcd-reason
//   pub enum PageTokenExpiredReason { Mac, Expired, Evicted, Node, PolicyVersion }  // Display snake_case
//   ConfigError::Unavailable gains reason const UNAVAILABLE_FEATURE_NOT_ACTIVATED = "feature_not_activated" (M5 reserved it).
// schema.rs (dev-compat)
pub struct SchemaTriple { pub format_version: u32, pub command_schema: u16, pub proto_rev: u32 }
pub const CURRENT_SCHEMA: SchemaTriple = SchemaTriple { format_version: 3 /*M5 dedup CF*/, command_schema: 2, proto_rev: 1 };
pub const COMPAT_SCHEMA_1: SchemaTriple = SchemaTriple { format_version: 1, command_schema: 1, proto_rev: 1 };
// store.rs (dev-pagination)
pub struct PageToken { pub token_version: u8 /*=1*/, pub prefix_hash: [u8;32], pub principal_hash: [u8;32], pub revision: u64,
    pub last_key: Bytes, pub policy_version: Option<u64>, pub issued_ms: u64, pub node_id: NodeId }
pub fn seal_token(t: &PageToken, key: &[u8;32]) -> Bytes;                       // postcard || HMAC-SHA256
pub fn open_token(bytes: &[u8], key: &[u8;32]) -> Result<PageToken, PageTokenExpiredReason /*Mac*/>;
// AS BUILT (lead-accepted deviations, 2026-09-19):
//   `types::ListRequest` is UNCHANGED. It has 26 literal construction sites across files other
//   writers own, so the token rides in a new wrapper instead:
pub struct PageRequest { pub list: ListRequest, pub page_token: Option<Bytes> }  // "extends existing List args"
impl PageRequest { pub fn first(list: ListRequest) -> Self; pub fn resume(list: ListRequest, token: Bytes) -> Self; }
//   `ListPage` carries a fourth field, `truncated`, so a page reports the same cap-reached fact
//   the M3 `ListResponse` does; without it a caller cannot tell "last page" from "cap hit".
pub struct ListPage { pub items: Vec<Record>, pub revision: u64, pub truncated: bool, pub next_page_token: Option<Bytes> }
// ConfigStore::list keeps its M3 signature; new `fn list_page(&self, PageRequest) -> Result<ListPage, ConfigError>`
// with a default impl that refuses a token with Unavailable{feature_not_activated} rather than
// serving an unpinned walk, so M3 stores keep compiling and cannot silently drift.
//   `PageTokenExpiredReason` has SIX variants, not five: `token_version` was added for test-plan
//   row M6-78 (M6-R7, lead-accepted).
```

## config-storage
```rust
// rocks.rs (dev-pagination): StateReader::pin(&self, revision: u64) -> Result<PinnedView, StorageReadError>
pub struct PinnedView { /* holds rocksdb::Snapshot (or Arc<BTreeMap> clone for Ephemeral) + revision */ }
impl PinnedView { pub fn revision(&self) -> u64; pub fn list_from(&self, prefix: &[u8], after: Option<&[u8]>, limit: usize) -> Result<Vec<Record>, StorageReadError>; }
// Pinning never blocks apply/compaction (a Snapshot handle only pins the RocksDB horizon).
// rocks.rs (dev-compat): open() checks state_meta/format_version BEFORE verify_column_families; `--compat-schema 1`
// passes `RocksOptions.max_format_version = 1` -> StorageOpenError::UnsupportedFormat { found, supported }
// (AS BUILT: the existing variant is reused; it already names both versions, which is the whole refusal).
```

## config-engine
```rust
// pagination.rs (dev-pagination)
pub struct PinTable { /* LRU keyed (revision, policy_version) -> Arc<PinnedView>, ttl via LeaderClock */ }
pub struct PaginationConfig { pub max_pinned: u32 /*64*/, pub ttl: Duration /*60 s*/, pub token_key: [u8;32] }
// Token check order: HMAC -> prefix_hash -> principal_hash -> policy_version -> node_id -> ttl -> pin present.
// mismatch -> InvalidArgument{prefix_mismatch} / PermissionDenied{token_principal}; all others -> PageTokenExpired{reason}.
// node.rs (dev-compat)
impl ConfigNode {
    pub fn cluster_min_schema(&self) -> SchemaTriple;             // min over committed voters' last-known triple; learners excluded
    pub fn schema_gate(&self, cmd: &Command) -> Result<(), ConfigError>;   // called in propose path BEFORE client_write
    pub fn local_schema(&self) -> SchemaTriple;                   // CURRENT_SCHEMA or COMPAT_SCHEMA_1
}
// Gated: Compact, RetireNode, Put/Delete with dedup, any envelope v2 op. Refusal: Unavailable{feature_not_activated} + retcd-reason=<gate>.
// feature_activated{schema} logged once per transition (state kept in node, not per request).
// watch.rs (dev-rbac, AMENDED 2026-09-19 by lead ruling M6-R8): WatchHub::on_policy_change(old: &PolicyDocument,
// new: &PolicyDocument) terminates streams whose prefix overlaps changed_prefixes() BEFORE any new-version event is
// enqueued. The refusal is ConfigError::policy_changed() carrying the additive closed-set detail token
// config_core::REASON_POLICY_CHANGED, NOT a PermissionDenied{policy_changed: bool} field (that variant has ~30
// construction sites in crates this milestone does not touch); config-grpc maps the token onto the retcd-reason
// trailer. The gate mutex is a structural barrier only: on_applied does not take the gate, so the guarantee comes
// from send_event re-checking the policy epoch synchronously before every enqueue, with registration reading that
// epoch inside the gate. Watch admission while converging on a newly-granted prefix -> PermissionDenied reason
// policy_converging.
// metrics.rs (dev-rotation): retcd_cert_expiry_seconds{plane,subject}; (dev-rbac, LANDED 2026-09-19):
// retcd_policy_version, retcd_policy_converged_version, retcd_policy_rollbacks_total,
// retcd_policy_reload_failures_total{reason}, retcd_break_glass_active — all daemon-filled via
// MetricsReport.policy and exported only under authz.mode = "signed"; (dev-pagination, LANDED): retcd_pinned_snapshots
// via MetricsReport.pagination. HealthPayload also gained the TA-39 watch fields (compact_revision,
// journal_oldest_revision, journal_newest_revision, journal_hash, watch_streams_open) plus policy_version
// (engine-filled) and policy_state (daemon-filled).
```

## config-server
```toml
[authz] mode = "signed" | "static"   # M6 default "signed"; static logs one warning; signed requires policy_file + trust_keys at load
policy_file = "..." ; policy_sig_file = "..." ; trust_keys = { name = "path.pub", ... } ; poll_interval_secs = 10
[tls] watch_files_secs = 30
[gossip] secret_key_hex = "..." ; secondary_key_hex = "..."     # fallback path only (ADR-0028); keyring path adds nothing here
[list] max_pinned_snapshots = 64 ; ttl_seconds = 60 ; token_key_file = "..."
```
CLI (dev-rbac): `--break-glass-policy-rollback` (process-scoped). CLI (dev-compat): `--compat-schema 1`.
Health payload (`health.rs`): `policy_version: Option<u64>` (dev-rbac), `schema: SchemaTriple`, `cluster_min_schema: Option<SchemaTriple>`
(dev-compat, lands after dev-rbac), `cert_expiry: [{plane, subject, not_after_unix}]` (dev-rotation). Unready when no valid policy in signed mode
(client + admin planes only; peer plane unaffected). Restore (M5 CLI) logs `restore_policy_mismatch` warn, never blocks.

## AdminService additions (proto/retcd/v1/admin.proto)
```proto
rpc ReloadPolicy(ReloadPolicyRequest) returns (PolicyInfo);        // dev-rbac: {version, hash_hex, outcome, reason}
rpc ReloadTls(ReloadTlsRequest) returns (ReloadTlsResponse);       // dev-rotation: repeated PlaneResult {plane, outcome, subject, not_after_unix}
rpc RotateGossipKey(RotateGossipKeyRequest) returns (AdminAck);    // dev-rotation: op = ADD|USE|REMOVE, key fingerprint only in logs; REMOVE refuses if a known peer needs it unless force=true
```
Admin set under signed mode = document `admins` ONLY; TOML `[authz] admins` ignored with one warning. ReloadPolicy authorizes
against the ACTIVE document, never the incoming one.

## Gossip meta (dev-compat first, dev-rbac appends)
SUPERSEDED by the dev-compat AS BUILT block below: `HINT_WIRE_VERSION` stays 1 and the fields ride in a
`HintExtras` postcard trailer instead. Decoder accepts v1 hints (`decode_hint_extras` -> `None`).
Both fields ADVISORY (ADR-0003): schema feeds nothing authoritative (leader uses peer-plane responses); policy_version only
toggles the converging evaluator.

## AS BUILT — dev-compat (2026-09-19)

Deviations from the contract above. Everything not listed here landed as written.

```rust
// config-core/src/schema.rs — as contracted, plus:
pub const UNAVAILABLE_FEATURE_NOT_ACTIVATED: &str = "feature_not_activated";  // in error.rs
pub fn command_gate(cmd: &Command) -> Option<SchemaTriple>;   // None = ungated (every schema-1 shape)

// config-engine/src/node.rs — `cluster_min_schema` returns an OPTION, not a bare triple (ruling M6-R12):
pub fn cluster_min_schema(&self) -> Option<SchemaTriple>;     // None on a follower: it calls nobody and
                                                              // therefore has no evidence to compute one
pub fn local_schema(&self) -> SchemaTriple;                   // unchanged
pub fn schema_gate(&self, cmd: &Command) -> Result<(), ConfigError>;  // unchanged, propose-time only

// RULING M6-R15 — activation is durable, so the gate has two independent passing paths:
//   (a) this node's own durable max-applied command schema >= the command's, OR
//   (b) every known voter reports >= it  (the contracted reachability path)
// config-core/src/state.rs
impl KvState {
    pub fn max_applied_command_schema(&self) -> u16;
    pub fn restore_max_applied_command_schema(&mut self, schema: u16);   // max-union, used on install
}
pub struct ApplyEffects { /* ... */ pub max_command_schema: Option<u16> }
//   NOTE: `ApplyEffects::is_empty()` deliberately IGNORES `max_command_schema`. It has no production
//   callers and `m5_core.rs::m5_108` pins its M5 meaning ("this command produced no dedup/watch work").
// config-storage: state CF key `max_command_schema`; `SnapshotHeader.max_applied_command_schema`
//   appended LAST and unioned on install, exactly like `retired_nodes` (M5-R21).

// config-grpc peer plane — a defaulted trait method pair, NOT a `PeerEnvelopeMeta` field:
//   proto/retcd/v1/peer.proto: `message SchemaTriple { uint32 format_version = 1; uint32 command_schema = 2;
//   uint32 proto_rev = 3; }` and `SchemaTriple schema = 7;` on `PeerEnvelope` (new tag, never reused).
//   `schema: None` on an incoming envelope reads as schema 1, never as an error.
pub(crate) fn schema_to_pb(schema: SchemaTriple) -> pb::SchemaTriple;
pub(crate) fn schema_from_pb(schema: Option<&pb::SchemaTriple>) -> Option<SchemaTriple>;

// config-storage: `StorageOpenError::UnsupportedFormat { found, supported, .. }` is REUSED; no
//   `FormatTooNew` variant was added. The ceiling arrives as `RocksOptions.max_format_version`.

// config-server: `--capabilities` prints a FLATTENED wrapper, so every pre-M6 key keeps its place:
pub struct CapabilitiesReport { #[serde(flatten)] pub capabilities: Capabilities, pub schema: SchemaTriple }
```

### Gossip meta — the trailer, and where dev-rbac appends

`HINT_WIRE_VERSION` **stays 1**. Bumping it would have made every v1 hint *rejected*, which contradicts
"the decoder accepts v1 hints", and `crates/config-gossip/tests/gossip.rs` pins the v1 encoding
byte-for-byte (`GOLDEN_HINT_V1`) and asserts that version 2 is refused. The triple therefore rides in a
postcard trailer appended after the hint, in the slack the decoder has always ignored. The gossip plane is
advisory (ADR-0003), so nothing reads it for a decision.

Exact struct and its only encode/decode site, both in `crates/config-gossip/src/meta.rs`:

```rust
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HintExtras {
    pub schema: Option<SchemaTriple>,   // postcard field 0
    // dev-rbac appends `pub policy_version: Option<u64>` HERE, as field 1, after this handoff.
    // Append only: postcard is positional, so field order is the wire format.
}

pub fn encode_hint(hint: &ObservedPeerHint) -> Result<Vec<u8>, GossipError>;                 // no trailer
pub fn encode_hint_with_extras(hint: &ObservedPeerHint,
                               extras: Option<&HintExtras>) -> Result<Vec<u8>, GossipError>; // ENCODE SITE
pub fn decode_hint_extras(bytes: &[u8]) -> Option<HintExtras>;                               // DECODE SITE
```

`decode_hint_extras` returns `None` for a v1 hint (no trailing bytes) and for any wire version it does not
know. It is wired through `GossipConfig.extras` -> `GossipNode`, set once in
`config-server/src/run.rs::start_gossip` and in `config-testkit`'s `start_real_gossip`.

## Evidence (dev-evidence)
`config_testkit::evidence::write_evidence(name, values: serde_json::Value, run: RunInfo) -> PathBuf` writes `docs/evidence/<name>.json`
per ADR-0031 schema (fixed `disclaimer` const). `RETCD_EVIDENCE=1` -> full scale; else reduced-scale constants in test source.
`scale_factor` = achieved/requested. Rows: M6-105 (1,000 watchers: p99 apply < 2x baseline, RSS < queue_bytes x streams),
M6-106 (RPO/RTO measured via M5 restore), partition/crash/security matrices (invariants only). `scripts/evidence-gate.ps1`
fails if any artifact has `full_scale:false` under `RETCD_EVIDENCE=1`. Version-skew matrix rows wait for dev-compat.

## Sequencing
1. Wave 1 (parallel, disjoint files): dev-pagination, dev-rbac, dev-evidence (helper + M4/M5-based rows only).
   Order inside config-core/capabilities.rs and error.rs: dev-pagination lands first; dev-rbac rebases.
2. Wave 2 after wave 1 gate: dev-compat (+ dev-evidence version-skew rows once dev-compat lands).
3. Wave 3 after dev-compat: dev-rotation (last: it touches run.rs, health.rs, gossip after everyone else).
   First task for dev-rotation: read pinned memberlist 0.8.5 source for a keyring API; report finding to lead before coding.
4. tester-m6 (Sonnet) for remaining plan rows + E2E-40..47, critic (Opus), fix round, gate commit `feat(m6)`.
All developers: `CARGO_INCREMENTAL=0`; no commits; no new files outside owned list; no `v2`/`-improved` names.

### Wave-2 follow-ups — dev-compat (2026-09-19)

* **M6-101 back door.** `config-engine` now has a `testing` feature (self dev-dependency
  `config-engine = { path = ".", features = ["testing"] }`, which reaches that crate's own
  integration tests only). It gates one method, `ConfigNode::propose_skipping_the_schema_gate`.
  `config-server` depends on `config-engine` without the feature, so the method is not compiled
  into a release binary. Anyone adding a harness-only seam should reuse this feature rather than
  add a second one.
* **M6-101 observable.** A `Compact` that is forced past the gate is *not* visible in
  `state_hash` — compaction sheds history and leaves records untouched. The row waits on the
  pinned voter's `compact_revision`, and it needs writes in the log first or the watermark has
  nothing to move to.
* **M6-111 artifact.** `docs/evidence/security-matrix-version-skew.json`, written by
  `m6_111_evidence_security_matrix_version_skew` in `crates/config-testkit/tests/m6_evidence.rs`.
  A separate file from `security-matrix.json` on purpose: both rows live in one test binary and
  run concurrently, so sharing the path would mean two writers. `docs/evidence/README.md`'s table
  still lists six artifacts and needs a seventh line — **not dev-compat's file**.
* **Future-schema fixture.** A "peer from the future" is `SchemaTriple { command_schema: 3,
  ..CURRENT }`. Raising `format_version` as well makes the node refuse to open its own store
  (`RocksOptions::max_format_version` is set from the pinned triple), which fails the row for a
  reason that is not version skew.
