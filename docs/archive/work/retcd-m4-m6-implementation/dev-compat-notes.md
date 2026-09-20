# dev-compat notes — ADR-0030 mixed-version gating (M6)

> REMINDER: tick the checklist below as each item completes. `[x]` done, `[-]` in progress, `[ ]` not started.
> HOLD: no edits to any file but this one until the lead sends "GATE DONE — dev-compat may edit".

Authority: `docs/ADRs/0030-mixed-version-gating.md` (M6-R4, A7, A8, OQ-63/64/65),
`m6-interfaces.md` (binding signatures), `architecture-m4-m6.md` D6.4 + A7/A8,
`docs/testing/test-plan-m6.md` §6 (M6-85..M6-104) + M6-123.

---

## 1. What already exists (checked in the tree, do not re-add)

| Thing | Status | Evidence |
|---|---|---|
| `ConfigError::Unavailable` reason `UNAVAILABLE_FEATURE_NOT_ACTIVATED` | **ALREADY LANDED** | `crates/config-core/src/error.rs:20`, exported `lib.rs:63`, used by pagination/admin/store/reader |
| `StatusClass::Unavailable` mapping | landed | `error.rs:318`, `config-grpc/src/error.rs:103` |
| `retcd-reason` trailer key | landed (`HEADER_REASON`) | `config-grpc/src/error.rs:67` — but **no `Unavailable` arm** writes it (only `PageTokenExpired` and the closed `MACHINE_READABLE_DENIALS` set) |
| `retcd-outcome: rejected` marker | landed, applied to every typed refusal | `config-grpc/src/error.rs:132` |
| Peer envelope | 6 proto fields, no schema | `proto/retcd/v1/peer.proto:21-34` |
| `PeerEnvelopeMeta` | 5 fields, no schema | `config-engine/src/transport.rs:91-102` |
| Gossip meta | `HINT_WIRE_VERSION = 1`, decoder strict on version, lenient on trailing bytes | `config-gossip/src/meta.rs:35,79-95` |
| `Command` envelope | single `COMMAND_ENVELOPE_VERSION: u16 = 2`; `decode` refuses `!= 2` | `config-core/src/command.rs:72,556-559` |
| Health payload | `config_engine::HealthPayload` (metrics.rs), server `health.rs` only serializes it; test mirror has no `deny_unknown_fields` | `config-server/src/health.rs:114-124`, `config-server/tests/support/mod.rs:496` |
| `--capabilities` | `main.rs:270` prints `serde_json::to_string(&run::capabilities_without_opening(..))` — return type is free to change | `config-server/src/run.rs:137` |

**Consequence:** deliverable 1's `error.rs` half is a no-op. `schema.rs` + the `lib.rs` re-export are the only config-core changes.

## 2. The four propose sites the gate must cover (node.rs)

| line | path | command | required schema |
|---|---|---|---|
| 1464 | `propose_retire` (called by `remove_member_inner`) | `RetireNode` | 2 |
| 1736 | `mutate_inner` (Put/Delete) | `Put`/`Delete` | 2 **iff** `cmd.dedup().is_some()` |
| 1988 | `propose_compact` (operator) | `Compact` | 2 |
| 2106 | retention timer's compaction | `Compact` | 2 |

Refusal shapes: 1464 → `AdminError::Unavailable` (M6-91 wants both schema numbers in the message);
1736/1988 → `ConfigError::Unavailable { reason: UNAVAILABLE_FEATURE_NOT_ACTIVATED }`;
2106 → no caller, so `feature_gated{feature="compact", cluster_min_schema}` rate-limited once per window (M6-90, M6-123).

## 3. Design decisions and the AS-BUILT deviations they force

### 3.1 The schema must NOT go into four widely-constructed types

Each of these is constructed with a **struct literal** in files this task excludes ("all existing test
files"), so adding a field is an edit outside my ownership. The dev-pagination precedent (`ListRequest`
left alone, `PageRequest` wrapper added — m6-interfaces "AS BUILT") is applied four times:

| type | literal sites outside my ownership | deviation |
|---|---|---|
| `PeerEnvelopeMeta` (engine) | 6: `config-engine/tests/m5_membership.rs:368,568`; `config-grpc/tests/mtls.rs:236,565`; `config-grpc/tests/peer_plane.rs:31`; `config-testkit/tests/m5_backup_fencing_cluster.rs:79,107`; `config-testkit/tests/m5_membership_cluster.rs:478` | struct unchanged. Schema travels as its own argument through **defaulted** trait methods (§3.2) |
| `Capabilities` (core) | 9 incl. the M0 contract test: `config-core/tests/m0_contracts.rs:28,39`; `config-client/tests/hint_following.rs:275,285`; `config-engine/tests/m1_cluster.rs:413`; `config-client/src/lib.rs:970` | struct unchanged. `--capabilities` JSON gets the triple from a `#[serde(flatten)]` wrapper returned by `run::capabilities_without_opening` — `main.rs` needs no edit, and `e2e_02` reads loose `serde_json::Value` keys so it still passes |
| `ObservedPeerHint` (core) | 9, **including a golden-bytes test** at `config-gossip/tests/gossip.rs:582` that a new postcard field would break semantically, not just syntactically | struct unchanged. The triple rides the meta **trailer** (§3.3) |
| `Command` / `COMMAND_ENVELOPE_VERSION` (core) | n/a — `command.rs` is not my file at all | no v1 encoder is added (§3.4) |

### 3.2 Peer plane: defaulted trait methods, one new proto field

`PeerTransport` and `PeerSink` gain schema-aware methods **with default bodies**, so the two foreign
implementors (`config-grpc/tests/support/mod.rs`'s fake `PeerSink`, and any test transport) keep
compiling untouched:

```rust
// config-engine/src/transport.rs
#[async_trait] pub trait PeerTransport {
    async fn send(&self, meta, endpoint, req, deadline) -> Result<PeerResponse, TransportError>;   // unchanged
    /// ADR-0030 M6-86: same call, carrying our triple and returning the responder's.
    /// `None` = the peer did not advertise one, which is read as schema 1, never as an error.
    async fn send_with_schema(&self, meta, schema: SchemaTriple, endpoint, req, deadline)
        -> Result<(PeerResponse, Option<SchemaTriple>), TransportError> { /* delegates to send, returns None */ }
}
#[async_trait] pub trait PeerSink {
    fn advertised_schema(&self) -> SchemaTriple { CURRENT_SCHEMA }   // ConfigNode overrides
    fn note_peer_schema(&self, from: NodeId, schema: Option<SchemaTriple>) {}  // leader learns from inbound too
}
```

Wire: `proto/retcd/v1/peer.proto` `PeerEnvelope` gains `SchemaTriple schema = 7;` (nested message, new
tag, never reusing one — §17). The **same** field serves both directions, because `peer_plane.rs`
already stamps the response with *its own* identity rather than echoing the caller's
(`peer_plane.rs:230-240`) — so the responder's triple lands in the answer for free.

`network.rs` (`EngineNetwork::call`) calls `send_with_schema`, and feeds the returned triple into a
shared `SchemaView` the node owns. That is the only input to `cluster_min_schema` (M6-102: gossip must
not be an input).

### 3.3 Gossip meta: trailer, not a hint field

`HINT_WIRE_VERSION -> 2`. Layout becomes `version | postcard(ObservedPeerHint) | postcard(HintExtras)`,
using the forward-compatibility slack the decoder already documents (`meta.rs:20-25, 87-89`).

```rust
#[derive(Serialize, Deserialize, Default)]
pub struct HintExtras { pub schema: Option<SchemaTriple> /* dev-rbac appends policy_version AFTER this */ }
```

Decoder accepts version 1 (extras = default, i.e. `schema: None` → read as schema 1) and version 2.
`decode_hint` keeps its signature; `decode_meta` is the new one returning `(hint, HintExtras)`.
**Field order in `HintExtras` is stable and append-only** — dev-rbac's `policy_version` goes after
`schema`, per m6-interfaces "Gossip meta (dev-compat first, dev-rbac appends)". dev-rbac has not yet
touched `config-gossip` (checked: their notes list it as a pending patch note, and `meta.rs`/`node.rs`
carry no `policy_version`).

Advisory only (ADR-0003): nothing in the engine reads it.

### 3.4 What `--compat-schema 1` actually is

`command.rs` has ONE envelope version constant (`= 2`) and `Command::decode` refuses anything else; the
Raft wire does not even use `Command::encode` (entries travel as `postcard(Entry<TypeConfig>)` through
serde — `command.rs:50-56`). So "emits only v1 envelopes" cannot mean "adds a v1 encoder", and `command.rs`
is not mine to change. Compat-1 is therefore exactly three behaviours:

1. advertise `COMPAT_SCHEMA_1` on all three planes;
2. never *propose* a command whose required schema is 2 (the same gate, with `local_schema` folded into
   the minimum — a compat node holds the cluster minimum down at 1 by construction);
3. refuse to *decode* a schema-2 command through a schema-aware wrapper in `schema.rs`:
   `SchemaTriple::decode_command(&self, bytes) -> Result<Command, SchemaRefusal>`, built on
   `Command::decode` + `command_schema_of(&Command)`. M6-99's golden bytes are the encodings of every
   schema-2-only command, asserted to be refused under `COMPAT_SCHEMA_1` and to **never** yield a
   plausible v1 value.
4. refuse to *open* a `format_version = 2` store (OQ-65) — the rocks.rs ordering fix.

### 3.5 `cluster_min_schema` is leader-local AND leader-only

M6-R4 fixes it as leader-local. The stronger fact the code forces: **only the leader has peer-plane
responses from every voter.** A follower is called by the leader and calls nobody, so it can learn at
most the leader's triple. Hence `cluster_min_schema()` returns `Option<SchemaTriple>` — which is exactly
the type m6-interfaces gives the health field — `Some` on the leader, `None` elsewhere. Consequences to
confirm with the lead (Q6/Q7 below): `feature_activated` is a leader-side line, and the activation latch
is per-process.

Minimum rule (M6-R4): min over **committed voters** (`committed_membership().voters`), learners excluded;
self contributes `local_schema()`; a voter never heard from contributes `COMPAT_SCHEMA_1`; an unreachable
voter keeps its last-known value (never dropped). Ordering is keyed on `command_schema` first
(`gate_key() = (command_schema, format_version, proto_rev)`) because that is the field D6.4 gates on;
the minimum is always an **observed** triple, never a field-wise blend of several.

## 4. Open questions sent to the lead (blocking where marked)

SENT to `main` 2026-09-19 as one consolidated escalation, re-lettered A-D there.
Still holding: no file in the tree has been modified.

| # | Question | Blocking? |
|---|---|---|
| Q1 | Permission for 7 mechanical `schema: None,` additions in 3 non-owned test files (the proto field breaks their `pb::PeerEnvelope` literals): `config-grpc/tests/peer_plane.rs:246,263,319,465`; `config-testkit/tests/m3_client_mtls.rs:441`; `config-testkit/tests/m3_peer_mtls.rs:134,819` | YES — no proto field is possible without it |
| Q2 | Grant `config-grpc/src/transport.rs` (client half of the peer plane; only `send_inner` + response read) | YES for M6-86 |
| Q3 | Grant `config-engine/src/testing.rs` (`InProcTransport`) — without it every in-proc peer reports "no schema" = 1 and M6-88/89/96 cannot be written | YES |
| Q4 | Grant one arm in `config-grpc/src/error.rs` mapping `Unavailable{feature_not_activated}` onto the `retcd-reason` trailer + its inverse | YES for M6-93's wire half |
| Q5 | Confirm the four AS-BUILT deviations in §3.1 (no answer = proceed, they are all "do not touch other writers' types") | no |
| Q6 | Confirm `cluster_min_schema() -> Option<SchemaTriple>` (leader-only) and that `feature_activated` is a leader-side line; annotate M6-96/M6-123 `(rev. dev-compat: …)` | YES |
| Q7 | Confirm the activation latch is per-process and monotonic (no on-disk marker); M6-100 asserts "min never dips below 2" + "at most one line per node per process". ADR-0030 Consequences already permits a new leader re-logging | YES |
| Q8 | HARNESS GAP. `ClusterBuilder` cannot start one node in compat mode, and `config-testkit/src/cluster.rs` is dev-harness's. Need (a) `ClusterBuilder::compat_schema(NodeId, u16)`, and (b) the one-line fix at `cluster.rs:99` when `RocksOptions` gains `max_format_version`. Alternative ruling: move M6-94/95/96/97 into `config-engine/tests/m6_compat.rs` where I build per-node configs myself | YES for the 5 cluster rows |
| Q9 | Reuse the existing `StorageOpenError::UnsupportedFormat { found, supported, path }` instead of adding the `FormatTooNew { found, max }` variant m6-interfaces names — its Display already prints both numbers and no exhaustive match breaks | no (default: reuse) |

## 5. Checklist

### config-core
- [ ] `schema.rs`: `SchemaTriple`, `CURRENT_SCHEMA`, `COMPAT_SCHEMA_1`, `gate_key`, `command_schema_of`, `decode_command`, `SchemaRefusal`
- [ ] `lib.rs` re-export
- [ ] `error.rs`: nothing to do (const already exists) — verify and record
- [ ] `tests/m6_schema.rs`: `m6_99_*` golden bytes, `m6_104_*` source assertion over Cargo.toml/lock for `=0.9.25`

### config-engine
- [ ] `transport.rs`: `send_with_schema`, `PeerSink::advertised_schema`/`note_peer_schema`, `PeerHandler` passthrough
- [ ] `network.rs`: call the schema-aware method, record the responder's triple
- [ ] `node.rs`: `SchemaView`, `local_schema`, `cluster_min_schema`, `schema_gate`, the four propose sites, activation latch + `feature_activated`, `feature_gated` rate limit, health fields
- [ ] `config.rs`: `compat_schema` field
- [ ] `metrics.rs`: `HealthPayload.schema` + `.cluster_min_schema` (health fields only)
- [ ] `testing.rs` (pending Q3): `InProcTransport` forwards the triple
- [ ] `tests/m6_compat.rs`: M6-88, 89, 90, 91, 92, 93, 100, 102, 123

### config-storage
- [ ] `rocks.rs`: `format_version` check strictly before `verify_column_families`; `max_format_version` open option; typed `FormatTooNew { found, max }`
- [ ] inline `#[cfg(test)]` unit test for the ordering (M6-98's mechanism; the row itself is tester-m6's)

### config-gossip
- [ ] `meta.rs`: `HINT_WIRE_VERSION = 2`, `HintExtras`, `encode_meta`/`decode_meta`, v1 accepted as `None`, budget assertion
- [ ] `node.rs`: advertise the local triple (minimal, well-delimited — dev-rotation follows me here)

### config-grpc
- [ ] `proto/retcd/v1/peer.proto`: `SchemaTriple schema = 7`
- [ ] `peer_plane.rs`: read the caller's triple, stamp ours on the answer
- [ ] `transport.rs` (pending Q2), `error.rs` (pending Q4)

### config-server
- [ ] `cli.rs`: `--compat-schema 1`
- [ ] `config.rs`: plumb to the engine config
- [ ] `run.rs`: `capabilities_without_opening` wrapper carrying the triple
- [ ] `health.rs`: verify no change needed (payload comes from the engine)

### config-testkit
- [ ] `tests/m6_compat_cluster.rs`: M6-85, 86, 87, 94, 95, 96, 97, 103

### docs
- [ ] ADR-0030 dated implementation note
- [ ] test-plan-m6.md rows filled; `(rev. dev-compat: …)` only where the fixture changed
- [ ] m6-interfaces.md "AS BUILT" block for every §3.1 deviation

### gate
- [ ] every new test 3x green (private target `compat-target`, `RETCD_TEST_DEADLINE_SCALE=3`)
- [ ] regression suites per the brief
- [ ] clippy `--all-targets -- -D warnings` on every touched package; `rustfmt --check` on every touched file
- [ ] mutation checks: remove the gate call -> M6-90/91/92 fail; unreachable voter dropped from the min -> M6-89 fails
- [ ] MUTATION hygiene: OPEN/CLOSED lines below, exact reverse, `grep -rn "MUTATION" crates/*/src` empty at handoff

## 6. Mutation log

(none yet)

## 7. Gotchas seen while reading

- `config-engine/src/network.rs:217` doc comment still says snapshots are never triggered (stale since M5). Not mine; not touched.
- `ConfigError::Unavailable`'s `Display` is `"unavailable: {reason}"`, and `error_from_status` reconstructs the reason **from the message**, so a client round-trip yields `reason == "unavailable: feature_not_activated"`. That is precisely why M6-93 needs the trailer (Q4) rather than string-matching the message.
- `config-server/tests/support/mod.rs`'s `Health` mirror has **no** `deny_unknown_fields`, so new health fields do not break it.
- `e2e_02` (`config-server/tests/e2e_daemon.rs:168`) reads the capability JSON as a loose `serde_json::Value` — a flattened extra key is safe.

## 8. rocks.rs open path — exact map (from the Explore agent, verified line numbers)

Call chain: `open` (785-801) -> `open_with` (812-825) -> `open_inner` (827-1145).

| line | what happens | matters because |
|---|---|---|
| 851 | `verify_column_families(dir, &path)?` | **too early.** A v2 dir opened by a compat-1 build dies here with a CF error, not a version error (OQ-65). |
| 857 | `probe_format_version(dir)?` | read-only marker read, **already called here**, already before 851's sibling work |
| 888 | `open_db(...)` | first writable open |
| 893 | `check_format_version(&db, &path)?` | the real verdict, post-open |

So the fix is NOT new machinery: `probe_format_version` (1431-1461) already reads `CF_STATE_META` / `KEY_FORMAT_VERSION` (little-endian u32) without opening writable. Move its verdict ahead of line 851 and refuse there.

- Constants: `FORMAT_VERSION = 3`, `FORMAT_VERSION_V1 = 1`, `FORMAT_VERSION_V2 = 2`.
- `FormatAction { Proceed, Stamp, Migrate { from } }` — the compat refusal is a fourth outcome, expressed as an early `Err`, not a new variant.
- `StorageOpenError` has 9 variants; "too new" already collapses into `UnsupportedFormat { found, supported, path }` (see Q9).
- **Carrier for `max_format_version`:** `RocksOptions { sync_writes, create_if_missing }` has only **2** exhaustive literal sites (`config-server/src/run.rs:754` mine, `config-testkit/src/cluster.rs:99` dev-harness's — Q8b) plus one `..RocksOptions::DEFAULT` site that is free. Compare: a new parameter on `open_with` breaks 11 sites, on `open` breaks 33. RocksOptions wins by an order of magnitude.
- **rocks.rs has no `#[cfg(test)] mod tests`.** My ordering unit test would be the first in the file (flagged to the lead).

## 9. config-testkit harness — what I may use (I own none of it)

Builder (`src/cluster.rs:440`): `nodes` :446, `storage` :452, `gossip` :458, `admins` :552, `snapshot` :561, `promote_max_lag` :567, `data_dir` :575, `start` :586. **No compat/schema knob — that is Q8a.**

`Cluster` (:905) — the accessors my rows need:
`leader()` :1331 · `node(id)` :1388 · `health(id)` :1504 · `state_hash(id)` :1499 · `wait_converged` :2221 · `wait_for` :2071 · `deadline(n)` :1181 · `gossip() -> GossipControl` :2061 · `data_dir(id)` :1464 · `reopen_store` :1516 · `provision_reusing_dir` :2322 · `provision_seeded_dir` :2346 · `restart` :2450 · `shutdown` :2474.

- `health(id)` returns the engine `HealthPayload` -> M6-87/96/123 assert the triple straight off it.
- `GossipControl` (:643): `inject`, `inject_all`, `poison_all`, `peers` -> M6-102 (v1 hint decodes to `schema: None`).
- `provision_seeded_dir` + `restart` -> M6-98 (v2 dir, compat-1 build) once Q8a lands.
- `pause_on_nth` is **not** on the builder; it is on `ScriptedInjector` in `tests/support/mod.rs:355` (usable, not editable).
- `poll.rs`: `deadline_scale()` (:126) reads `RETCD_TEST_DEADLINE_SCALE` and multiplies into every `TestTimers::multiple` (:115). **Tests must never set it themselves** — it goes in the runner env, as my brief says.
- Test conventions to copy verbatim: `#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]`, `const METHOD: &str = "<fn name>";`, and a file-local `fn my_log_lines(method: &str)` wrapper so `module_path!()` evaluates in my test file.

### 8.1 The rocks.rs patch, concretely (verified against the source, not the map)

`open_inner`'s existing shape (843-881): `let existing = CURRENT exists; if existing { match verify_column_families(..) { .. }; if let Some(missing) = .. { match probe_format_version(dir, &path)? { .. } } }`.

Note the probe is nested *inside* the missing-family arm. So the patch is a **hoist plus one guard**, and it removes a second read-only open rather than adding one:

1. hoist `let marker = probe_format_version(dir, &path)?;` to the top of the `if existing` block, ahead of `verify_column_families`;
2. immediately after it, refuse when `marker > options.max_format_version` (OQ-65);
3. replace the nested `match probe_format_version(dir, &path)?` with `match marker` — same value, one open instead of two.

`probe_format_version` opens read-only over `COLUMN_FAMILIES_V1`, which every v1/v2/v3 directory has, so hoisting is safe for all three layouts. It already returns `Ok(None)` for an unmarked directory, and the "unmarked but populated" case stays where it is (`check_format_version`, post-open) — the compat guard only ever fires on a marker that is present and too new, which is exactly OQ-65's case and nothing else.

Two doc touches come with it: `StorageOpenError::UnsupportedFormat.supported` (rocks.rs:253-254) currently reads "The only version this build reads and writes (FORMAT_VERSION)" and becomes the build's *ceiling*; and the `check_format_version` doc at :1479-1480 should point at the new earlier guard. Both are inside the region I own; flagged here so the handoff can name them.

## 10. Server surface — verified, and one worry retired

- `run::capabilities_without_opening` (run.rs:137) has **exactly one caller**: `main.rs:270`, which only does `serde_json::to_string(&report)` and prints it. So changing its return type to a `#[serde(flatten)]` wrapper (`CapabilitiesReport { caps: Capabilities, schema: SchemaTriple }`) needs **no edit in main.rs** — the flattened JSON gains one key and every existing key keeps its spelling. `config-server/tests/e2e_daemon.rs:168` (E2E-02) reads the output as a loose `serde_json::Value`, so it is unaffected. The `--capabilities` half of deliverable 6 costs zero foreign edits.
- **Merge risk, not a blocker:** dev-pagination must change `pagination: Pagination::Unsupported` at run.rs:154, inside the same function whose signature (:137) and tail expression I change. Different lines, same function. Whoever lands second re-reads before editing.
- `Cli` (cli.rs:15) is a flat clap struct of global flags; `--compat-schema` is one more `#[arg(long, value_name = "N")] pub compat_schema: Option<u16>` beside `--unsafe-no-sync` (:52) — no subcommand plumbing.
