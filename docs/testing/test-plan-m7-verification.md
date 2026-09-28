# Test Plan — M7, team verification (O1, G1, Q1)

**Status:** Proposed (test planner deliverable; correction round 1 applied after critic round 2,
T-01..T-22; correction round 2 applied after critic round 3, T-23..T-35, under rulings V-R20 and
V-R21 — row literals now follow the **landed C0 at `8a23b1d`**, drift table in §15; correction
round 3 applied after critic round 4, T-36..T-41, plan-text fixes only, no new ruling)
**Date:** 2026-09-20
**Scope:** rDB milestone M7, packages **O1** (independent logical oracle), **G1** (seeded scenario
generator + causality-preserving reducer), **Q1** (combined adversarial campaign and coverage
matrix). Row prefix **`M7V-NN`**. Crates `rdb-core`, `rdb-sim` (never `partdb`).
**Authority, in order:** lead rulings V-R1..V-R19 (`ledger.md`; V-R16..V-R19 are the critic
round-2 rulings and are cited by id because the design sections they amend were still moving when
this revision was written); `docs/rdb/design-specification.md`
rev 1.6 §5.2–§5.4, §6.2, §6.3, §7.2, §7.3, §8.1–§8.4; `docs/rdb/implementation-spikes.md` §4, §5
(O1/G1/Q1 rows), §6, §7; `docs/rdb/validation-plan.md` §2, §5; `docs/ADRs/rdb/0019`; rEtcd
ADR-0014 (test discipline), ADR-0013 (logging), ADR-0031 (evidence); `AGENTS.md`.
**Design source:** `teams/verification/design.md` §2.3 (invariants), §2.5 (grounding), §2.6 (what
INV-LAG does not assert), §3 (grammar), §4 (reducer), §5 (campaign), §6 (coverage), §7 (mutations),
§8 (not built); `teams/verification/trace-requirements.md`; `teams/verification/critic-design.md`
round 1 **and** the re-review (F19–F22).
**Companion:** `docs/testing/test-plan-m6.md` — this plan copies its row format, its evidence-row
pattern (§7) and its DuckDB Q-row pattern (§11). M6's rows, TA-1..TA-66, Q-1..Q-33 and anti-flake
rules 1..31 are not restated and are not in force for `rdb-*`.

> **Numbering note.** `M7V-20`, `M7V-21`, `M7V-22` and `M7V-23` are **reserved** for the four
> reducer rows that `design.md` §4.3/§4.4 and the critic's F21 name by id. They are written in §6,
> not in §4. The oracle block therefore runs `M7V-01..M7V-19` and continues at `M7V-24`.
> Rows added in correction round 1 are `M7V-78..M7V-88`, and in correction round 2 `M7V-89` and `M7V-90`.
> **`M7V-90` was withdrawn in correction round 4** under ruling F-R13 (the field it cross-checked
> will never exist); its id is **retired, not reused**, and the next free id is `M7V-91`. Each
> lives in the section its subject belongs to; no existing id was renumbered or reused.
> Later additions, in id order: `M7V-92` (V-R25), `M7V-93..M7V-95` (L-R177gf), and
> `M7V-96..M7V-101` (recorder truth, tester-sim-hooks F1, 2026-09-27; §5 after `M7V-95`), and
> `M7V-102..M7V-108` (restart rebuild, V-R35, 2026-09-27; §5 after `M7V-101`), and `M7V-109..M7V-113`
> (restart revocations and the grant service, L-R178e, 2026-09-28; §5 after `M7V-108`), and
> `M7V-114..M7V-124` (sim fidelity: a crash kills the process, lead ruling 2026-09-28, and its
> correction round 1 under V-R36; §5 after
> `M7V-113`), and `M7V-125..M7V-126` (sim-followup: tester D7 and `NodeDown` precedence; §5 after `M7V-124`). The
> next free id is the one after `M7V-126` (`M7V-91` stays the unwritten candidate of §15 drift row 6). Architecture requirements in this plan are
> `VA-1..VA-9` — a separate series from rEtcd's `TA-NN`, because the harness surfaces are in
> different crates.

**How to use this document**

- Developers: §1 is a contract on `rdb-sim`'s test support and on the trace vocabulary. Code that
  does not expose these surfaces is not done, because §3–§9 cannot be written against it.
- Testers: §3–§9 are the backlog. **One row = one test.** The row id prefixes the test function
  name (`m7v_08_pub_degraded_rf2_one_ack_publish_violates`), because §10's DuckDB queries and §13's
  gate map both work by string match.
- Both: §12 marks every row that cannot pass until a named package lands, and says what the runner
  reports meanwhile. §14 lists open questions; each has a default that is what you implement if the
  lead does not answer first.

**File mapping** (charter-owned paths; `tests/support/mod.rs` registration is team foundation's)

| Area | Path |
|---|---|
| Oracle rows M7V-01..M7V-41, M7V-79, M7V-81, M7V-85 | `crates/rdb-sim/tests/oracle.rs` |
| Oracle implementation | `crates/rdb-sim/tests/support/oracle/{mod,model}.rs`, `.../checks/*.rs` |
| Grammar, generator, reducer rows M7V-20..M7V-23, M7V-42..M7V-51, M7V-83, M7V-84, M7V-86, M7V-88 | `crates/rdb-sim/tests/scenarios.rs` |
| Recorded-run rows M7V-80, M7V-92, M7V-96..M7V-101 (the spine and rebuild plans live here) | `crates/rdb-sim/tests/dispatch.rs` |
| Host cadence and runner rows M7V-93..M7V-95 | `crates/rdb-sim/tests/host_cadence.rs` |
| Restart rebuild rows M7V-102..M7V-108 (V-R35), restart revocations and grant service M7V-109..M7V-113 (L-R178e), sim fidelity M7V-114..M7V-126 (lead ruling 2026-09-28, V-R36; M7V-125..M7V-126 sim-followup) | `crates/rdb-sim/tests/restart.rs` |
| Scenario implementation | `crates/rdb-sim/tests/support/scenarios/{mod,grammar,gen,reduce,coverage,mutate}.rs` |
| Campaign, mutation, evidence rows M7V-52..M7V-77, M7V-78, M7V-82, M7V-87, M7V-89 | `crates/rdb-sim/tests/campaign.rs`, `tests/campaign/{corpus,report,regressions}.rs` |
| Fixtures | `crates/rdb-sim/tests/fixtures/scenarios/*.json`, `tests/fixtures/regressions/*.json` |
| Evidence | `docs/evidence/rdb-m7-campaign.json` (debug handoff gate), `docs/evidence/rdb-m7-campaign-release.json` (release gate, V-R17), `docs/evidence/rdb-m7-coverage.json` |
| Test logs (JSONL) | `$RETCD_TEST_LOG_DIR/<testModule>/<testMethod>.jsonl` |
| Failure reproducers | `$RETCD_TEST_LOG_DIR/validation/<run-id>/` (V-R6, per invocation, gitignored) |

---

## 1. Test-architecture requirements (VA-1 … VA-9)

These are the surfaces the rows below assert against. Each names its owner.

### VA-1 — a hand-built trace is a first-class fixture (owner: verification)

`TraceBuilder` constructs a `Trace` field by field with no kernel involved: the landed
`TraceHeader` at `f616ddf` (`schema_version`, `generator_version`, `provenance: Provenance` —
**landed at `6893442`**, and there is no `seed` field left on the header, §15 drift table row 1 —
`config: RunManifest{budgets, overridden, nodes, event_cap}`, `partitions: u8`, `topology:
Vec<TopologyEntry{partition, node, role, config_version}>`, `oracle_checkpoint_digest`), then typed
`TraceEvent`s. It is the input to every `M7V-01..M7V-41` row, which is why those rows are unit-class
and need no runner. It must be impossible to build a trace through it that the *checker* rejects for
a malformed envelope rather than for the invariant under test: `event_id` is assigned by the
builder, monotonically, and `ack_from(n, s)` emits the secondary `batch_apply` an acknowledgement
rests on (§4 convention 4, V-R20 (8)) so no row author can build an ungrounded ack by omission.

### VA-2 — every checker returns three states, and `Unavailable` names its reason (owner: verification; V-R16)

`Verdict = Proven | Unavailable(Unavailable) | Violated(Signature)` per `design.md` §2.4 as
amended under ruling **V-R16**, with the reason enum `Unavailable = Capability(PackageId) |
NotArmed` and every checker exposing `fn armed(&self) -> bool`. Written `Unavailable{Capability(p)}`
and `Unavailable{NotArmed}` in the rows below:

- `Unavailable{Capability(id)}` — a capability the checker needs is not wired. Produced **only** by
  a `capability{package=id, state=Unavailable}` event in the trace; this arm is never
  inferred from silence.
- `Unavailable{NotArmed}` — the capability is wired but the trace never reached the checker's
  arming situation (no `schedule_phase{Healed}` for INV-LIVE, no admitted sibling work for INV-ISO,
  an empty trace for everything, a truncated history for M7V-63). This arm **is** the silence
  case, and it is reported as such instead of as `Proven`.
- **Both arms report and never pass.** `Proven` means *armed and no violation*; a checker returns
  `Proven` only after it observed its arming event. There is no state for "never armed, nothing
  wrong" other than `Unavailable{NotArmed}`; disarming **is** `NotArmed` — there is no fourth
  state (design §2.4).
- **`armed()` is the checker's state at the end of the fold** (critic T-24, ruling V-R20 (6);
  design §2.4 as amended). A checker that armed and then disarmed — budget exhausted, sibling
  idle — reports `armed() == false` and verdict `Unavailable{NotArmed}`; M7V-31 and M7V-33 assert
  both. `armed()` is therefore `true` exactly when the per-seed verdict is `Proven` or `Violated`.
- **Arming events, as design §2.4 names them** (pinned by M7V-02, one `armed()` assertion per
  checker): INV-LIVE and INV-ISO a `schedule_phase{phase=Healed}`; INV-LAG a
  `protection_state{phase=Paused}`; INV-VER a `version_check`; INV-LOSS a
  `lineage_root{source=Recovery}`; INV-DEDUP a second `client_submit` with a retained identity;
  INV-ATOM, INV-PUB, INV-AUTH and INV-LIN their first `batch_apply`, `publish`,
  `authority_decision` and `lineage_root`. The plan does not restate the list anywhere else; a row
  that needs it cites this bullet.
- **Per-run fold, in this order** (design §2.4; critic T-34 asked for it here as well as in
  M7V-52): any seed `Violated` → `violated`; else any seed `Unavailable{Capability(p)}` →
  `unavailable(capability p)`; else any seed `Proven` → `proven`; else `unavailable(not_armed)`.
  **`proven` and `seeds_armed` both count seeds whose per-seed verdict is `Proven`** (V-R20 (6)),
  so `proven ⇒ seeds_armed > 0` is definitional, and M7V-78's synthetic half is a runner-bug row.

The campaign carries the same distinction one level up: `invariant_status.seeds_armed` (VA-7)
counts the seeds whose verdict was `Proven`, and a `proven` status with `seeds_armed == 0` is a
defect, not a pass — rows M7V-78 and M7V-54 and query Q-34. Row M7V-03 keeps the per-checker half
true; without both halves the plan's green is meaningless while kernel packages are unwired, and
meaningless again afterwards if the corpus never arms a checker (critic T-01). M7V-89 closes the
remaining gap (critic T-28): an invariant whose packages are all `Wired` must arm on the default
corpus, which V-R19's schedule makes deterministic.

### VA-3 — the trace vocabulary, as requested (owner: team foundation, C0)

`trace-requirements.md` §1–§4, including the four V-R10/V-R12 emission rules that the oracle's
strength depends on: `replication_ack` emitted **at the secondary** with a separate
`replication_ack_delivered` record; `protection_state` emitted on every `config_version` change;
`ClientOutcome = Success | RecoveredApplied | <§5.4 errors>`; and
`topology_change { config_version, nodes }` emitted by the **environment** (V-R12, critic F19).
A missing emission rule does not merely weaken a row — it makes the row assert a property the trace
cannot express. Rows M7V-10, M7V-08, M7V-26, M7V-28 are the four that fail loudly if any of the four
is dropped; keep them as the seam-freeze canaries.

**Verification has zero open contract asks on foundation** (re-read again at `f616ddf`, §15 drift table; CB-1..CB-4 landed as kernel-b asks, not as verification ones, and closed none of ours because there were none open).
Both asks this plan carried — the header's `provenance` (row 1) and the `op_skipped` kind (row 21)
— landed in foundation's first code round at `6893442`. Three documents, including earlier
revisions of this one, called `provenance` "the one open contract ask" long after it shipped,
because all three were written against `8a23b1d` and none was re-read. Nothing in this plan now
waits on a **shape**. What it still waits on is *code*: I1's replay runner, I1's trace validator
(VER-CR-3, no ruling id, recorded in `ledger.md` 2026-09-20 20:35 PDT), and the H1/M1 fault hooks.
Those are §12's business and are not contract asks.

### VA-4 — sim-provider fault hooks (owner: team foundation, H1 and M1; V-R9)

`NetworkOp::ForgeAck { msg, claimed_role, claimed_node }` in H1's network provider, and
`StorageOp::FalseDurable { node, through }` in M1's flush path. **No `cfg` branch in kernel code,
ever.** Both are already required provider capabilities (spike §4 transport "forged identity is
injectable and rejected"; spike §6 storage "no false durable watermark"). They are the only way to
make the kernel emit a self-consistent-but-wrong trace, which is the blind spot a trace rewrite
cannot reach. Rows M7V-69 and M7V-70.

### VA-5 — the replay runner is the reducer's only executor (owner: foundation I1 + verification)

`run(scenario) -> Trace`. The reducer calls it and nothing else; it never edits a trace
(`design.md` §4.3, D3). Row M7V-49 asserts the API shape makes trace surgery unrepresentable: the
reducer's candidate type is `Scenario`, and no function in `support/scenarios/reduce.rs` takes
`&mut [TraceEvent]`.

### VA-6 — the coverage matrix is enumerated, never hand-listed (owner: verification)

Every required-cell list in `coverage.rs` is derived from the enum it counts (the M6-107/TA-63
pattern), so adding an `AckRejectReason` variant or a `BoundaryId` member without a cell fails a
row instead of quietly shrinking the requirement. **This has now happened once, and the row
caught it:** CB-3 widened `AckRejectReason` 7 → 14 at `f616ddf` and M7V-56 went red until the
seven cells were written (§15.1). `BoundaryId` is **foundation's closed set** — spike §6's
required-boundary column plus the two V-R9 members, **29 members, re-read at `f616ddf`**
(`crates/rdb-core/src/contracts/trace.rs:505`; unchanged since `8a23b1d`, and `f616ddf` did not
touch it) — and M7V-56 asserts set **equality** against the enum (ruling V-R19 answers the
planner's Q-4); `op_skipped` is its own event kind (critic F17).

**Required boundaries are scheduled, not drawn (critic T-12, ruling V-R19).** With
`REQUIRED: [BoundaryId; N]` in declaration order, seed `i` (from `SPIKE_SEED_BASE`) is obliged to
attempt `REQUIRED[i mod N]`; the obligation is a function of `i`, not of the PRNG, so the same seed
still yields the same scenario (M7V-43). Any corpus of at least N seeds therefore attempts every
required boundary deterministically. The hit is still counted from the environment's
`fault_injected{boundary}` — a scheduled boundary the environment did not reach is `missing`, never
assumed. The two hook-gated members (`ForgedIdentity` needs H1's `ForgeAck`,
`FalseDurableWatermark` needs M1's `FalseDurable`) are scheduled like any other; until the package
reports `Wired` the cell is recorded under the coverage artifact's
`unavailable_cells{cell -> package}` (design §5.3, ADR-rdb-0019 §2) and excluded from
`required_missing[]` **by the capability entry, never by editing the required list**. The
`BoundaryId -> producing ScenarioOp` table (`gen.rs`) and the `BoundaryId -> PackageId` gating
table (`coverage.rs`) are both enumerated. Rows M7V-42, M7V-55, M7V-56, M7V-57, M7V-73.

**The gating table is keyed per family on the emitting provider package (critic T-35, ruling
V-R20 (4)).** `fault_injected{boundary}` is emitted by the environment provider that injects the
op, so every member of one `FaultKind` family is gated on the package whose provider emits it —
the two hook-gated members are the strong case, not the only case. Foundation's handoff names the
emitter beside each of the 29 members; M7V-56 asserts every member has an entry and that the
members of one family share one package. Until a family's provider reports `Wired`, its cells are
`unavailable(<package>)`, never `missing` — which is what keeps the handoff gate green with
invariants `unavailable` (§13) when I1 lands before H1 or M1.

**The required-cell gate applies only when `SPIKE_SEEDS >= N` (critic T-25, ruling V-R20 (7);
design §3.1/§6 as amended).** `REQUIRED[i mod N]` over fewer than N seeds cannot schedule every
member, so a corpus smaller than N records its coverage, writes `coverage_gated: false` into its
report and artifact (VA-7 `campaign_run`, M7V-73), and **never fails on `required_missing`**. The
default corpus (64) and commands 2/3 (1,000) are always gated; the sub-N corpora rows M7V-58, 61,
63, 64, 75, 76 own state `coverage_gated: false` in their inputs, and M7V-57 exercises both sides
of the condition.

### VA-7 — the log-line contract (owner: verification; ADR-0013 field discipline)

Tests use `#[retcd_test]` from `config-log-macros`, so JSONL lands under `RETCD_TEST_LOG_DIR`.
Log fields, not sentences. **Never key or value bytes** — a key is a `key_id`, a value is a
`(value_version, digest)`. The Q-rows in §10 are written against exactly these lines:

> **Held, and owed by verification: none of the eleven lines in this table is emitted yet.**
> This table is item 2 of `docs/testing/m7-log-fields.md`, verification's debt, and it unblocks
> **Q-34, Q-38, Q-39 and Q-40**. Measured 2026-09-21 by DuckDB (`map_inference_threshold=-1`)
> over an 86-file log root from this package's own run: the emitted vocabulary is `capability`
> 249, `test started` 83, `test finished` 83, and **all eleven `@m` values below return zero
> lines**. Verification emits no log line of its own.
>
> **Blocked on item 1**, foundation's tier-1 `TraceEvent` serialiser. Nine of the eleven are the
> campaign runner's and the runner is I1-dependent; `invariant_status`, `capability_seen` and
> `trace_header` are derivable from `Oracle::judge`'s `Report` today and are the first candidates
> once item 1 lands.
>
> **Why this is recorded rather than left implicit.** `invariant_status.seeds_armed` is this
> plan's own named defence against a vacuous pass — a `proven` row with `seeds_armed = 0` fails
> the run under Q-34 — and it has no producer. `coverage_shortfall` is the same shape for Q-38.
> Until both exist, a zero-row result on Q-34 or Q-38..Q-40 is indistinguishable from a clean
> run, so those four Q-rows are **dark, not green**, and must not be read as passing.

| `@m` | Fields |
|---|---|
| `invariant_status` | `checker`, `status` (`proven`/`unavailable`/`violated`), `reason` (`capability` or `not_armed`; null when not `unavailable`), `package` (the `PackageId` when `reason = capability`; null otherwise), `seeds_armed` (the number of seeds whose per-seed verdict was `Proven` — **load-bearing**, V-R16 and V-R20 (6): `proven` with `seeds_armed = 0` is a runner bug and fails the run). **Two surfaces, one meaning (critic T-30, ruling V-R20 (5)):** the log line carries `reason` + `package` as two fields; the artifact (`rdb-m7-campaign*.json`, ADR-rdb-0019 §2) carries the one string `reason: "capability(<package>)"` or `"not_armed"`. Q-34 projects both log fields |
| `violation` | `checker`, `rule`, `partition`, `role`, `event_kind`, `event_id`, `seq`, `logical_tick`, `seed` |
| `capability_seen` | `package`, `state` |
| `coverage_cell` | `axis`, `cell`, `count` — emitted **only for a cell that was hit** (`count >= 1`); a zero-hit cell has no `coverage_cell` line |
| `coverage_shortfall` | `axis`, `cell` — emitted for **every required cell with zero hits**; this is the line Q-38 reads for the shortfall (critic T-08). Exactly one of `coverage_cell` / `coverage_shortfall` exists per required cell |
| `coverage_unavailable` | `axis`, `cell`, `package` — a required cell excluded from `required_missing[]` because its gating package reported `Unavailable` (V-R19) |
| `shrink_step` | `step`, `ops_before`, `ops_after`, `accepted`, `checker`, `rule`, `faults` |
| `shrink_result` | `signature_slug`, `ops_before`, `ops_after`, `slipped`, `faults_before`, `faults_after`, `budget_spent` |
| `campaign_run` | `seeds`, `max_events`, `events_total`, `wall_ms`, `shrink_ms`, `profile`, `threads`, `coverage_gated` (`true` iff `seeds >= N`, V-R20 (7)) |
| `mutation_caught` | `mutation_id`, `checker`, `row` |
| `trace_header` | one line per replayed or recorded trace, before its events (critic T-27): `schema_version`, `generator_version`, `provenance` (the landed `Provenance`, projected as its variant name plus its one field — never a bare `seed`; §15 drift row 1), `partitions`, `config` (the landed `RunManifest`: `budgets`, `overridden`, `nodes`, `event_cap` — there is no `config_digest` field, §15 drift row 10), `topology` — a list of `{partition, node, role, config_version}` structs, the landed `Vec<TopologyEntry>` as serde writes it |
| *(trace events)* | one line per `TraceEvent`, `@m` = the `TraceKind` variant in snake_case (`admission_decision`, `publish`, `topology_change`, …); the envelope fields are the landed names `event_id`, `logical_tick`, `partition`, `node`, `boot`, `correlation`, and the variant's own fields follow, flattened, under their serde names. A tuple field (`topology_change.nodes: Vec<(NodeId, ReplicaRole)>`) is a list of two-element lists, indexed `[1]`/`[2]` in DuckDB; a struct field (`publish.ack_evidence: Vec<AckEvidence>`) is a list of structs |

### VA-8 — evidence goes through the shared helper, unchanged (owner: verification; V-R5)

`rdb-sim` takes `config-testkit` as a **dev-dependency** and calls `write_evidence(name, values,
RunInfo)` as is. No `config-*` file changes. No second `DISCLAIMER` constant — a duplicated
disclaimer is the exact failure rEtcd ADR-0031 wrote the shared helper to prevent. Artifact names
are prefixed `rdb-` in the shared `docs/evidence/` directory (V-R2).

### VA-9 — three commands, two target directories, two campaign artifacts (owner: verification; V-R11, V-R17, V-R18, AGENTS.md)

| # | Purpose | Command | Writes |
|---|---|---|---|
| 1 | Handoff gate (default 64-seed corpus, debug, reduced scale) | `CARGO_TARGET_DIR=.rtargets/verification scripts/gate.sh test -p rdb-sim --test oracle --test scenarios --test campaign` | `docs/evidence/rdb-m7-campaign.json` (`profile: "debug"`, `full_scale: false`) |
| 2 | The 1,000-history number (warm **release**, full scale) | `RETCD_EVIDENCE=1 CARGO_TARGET_DIR=.rtargets/campaign scripts/gate.sh test --release -p rdb-sim --test campaign` | `docs/evidence/rdb-m7-campaign-release.json` (`profile: "release"`, `full_scale: true`) — the **second artifact name** ruling V-R17 adds to ADR-rdb-0019 §2 |
| 3 | **The M7 release gate** (V-R18): command 2 plus the honest-green switch | `SPIKE_REQUIRE_ALL=1 RETCD_EVIDENCE=1 CARGO_TARGET_DIR=.rtargets/campaign scripts/gate.sh test --release -p rdb-sim --test campaign` | the same release artifact; the run **fails** on any invariant not `proven` (M7V-54) and on any `full_scale: false` (M7V-75) |

Rules that follow:

- The artifact name is selected by the build profile, so commands 1 and 2 never overwrite each
  other and M7V-62 can see both side by side (critic T-13). The release artifact is the **only**
  source of the 1,000-history figure.
- `.rtargets/campaign` is **reserved** for commands 2 and 3. `scripts/gate.sh` never passes
  `--release` itself (line 45) and `[profile.test] opt-level = 0` applies to workspace members, so
  command 1 cannot produce the release number and must not be quoted as if it had. Never run two
  cargo invocations against one target directory (AGENTS.md, the 2026-09-19 `LNK1104` collision).
- Command 3 is the gate §13's last line cites. It is a command, not a script change: wiring it into
  `scripts/gate.sh` is foundation's item after F-R11 lands (V-R18), and nothing in this plan edits
  `scripts/`.
- `SPIKE_REQUIRE_ALL=1` is set in **no** other command. The handoff gate is green with invariants
  `unavailable`; only command 3 turns the artifact into a gate.

---

## 2. Taxonomy, budgets and the rules that keep a green run honest

| Class | Meaning | Per-row budget | Rows (counted from the tables, critic T-17) |
|---|---|---|---|
| **unit** | hand-built trace, plain data or a source-level check; no runner, no kernel | **< 100 ms** | **58**: M7V-01..M7V-19, M7V-24..M7V-46, M7V-49, M7V-56, M7V-57, M7V-59, M7V-66..M7V-68, M7V-71, M7V-74, M7V-77, M7V-79, M7V-81..M7V-85; **+ M7V-99** (recorder truth, 2026-09-27; not in the 89-row count) |
| **sim** | one scenario through the runner and the real kernel | **< 2 s** (M7V-88 replays every fixture and owns **< 10 s**, stated in the row) | **12**: M7V-20..M7V-23, M7V-47, M7V-48, M7V-50, M7V-69, M7V-70, M7V-80, M7V-86, M7V-88; **+ M7V-92** (V-R25; not in the 89-row count below, which predates it) **+ M7V-93..M7V-95** (L-R177gf, V-R30; likewise not in it) **+ M7V-96..M7V-98, M7V-100, M7V-101** (recorder truth, tester-sim-hooks F1, 2026-09-27; likewise not in it) **+ M7V-102..M7V-108** (restart rebuild, V-R35, 2026-09-27; likewise not in it) **+ M7V-109..M7V-113** (L-R178e, 2026-09-28; likewise not in it) **+ M7V-114..M7V-126** (sim fidelity, lead ruling 2026-09-28 and V-R36; likewise not in it) |
| **campaign** | the seed loop | one shared default corpus **< 60 s** at `SPIKE_SEEDS=64`; aggregate below | **19**: M7V-51..M7V-55, M7V-58, M7V-60..M7V-65, M7V-72, M7V-73, M7V-75, M7V-76, M7V-78, M7V-87, M7V-89 |

**Aggregate budget for `--test campaign` at default scale (critic T-11).** The class budget is per
corpus; the binary runs more than one. The mechanism that keeps the count small:

- **One shared corpus run**, built once behind a `OnceLock` in `tests/campaign/corpus.rs` at the
  default knobs, whose *report* (statuses, `seeds_armed`, coverage counts, failing-seed list,
  artifacts) is a value every row that does not vary the environment reads: M7V-52, M7V-53
  (during M7, when P1 is genuinely unwired; after P1 lands it forces the capability through the
  dispatcher's report and owns one small corpus), M7V-55, M7V-60 (`SPIKE_ASSERT_WALL_MS` unset is
  the default corpus's own setting — it shares, critic T-31), M7V-72, M7V-73, M7V-78, M7V-89.
  M7V-54 and M7V-87 apply the gate check **function** to the shared report with the flags set, and
  never re-run the loop.
- Rows that vary the environment each own **one** corpus, at the smallest scale that exercises the
  property, stated in the row: M7V-58 (three runs at `SPIKE_SEEDS=8`, the smallest count that
  produces a non-trivial merge across 1/2/N threads), M7V-61 (two runs at `SPIKE_SEEDS=4`), M7V-63
  (one run, `SPIKE_MAX_EVENTS=128`), M7V-64 (one run with one injected failing seed), M7V-65 (two
  runs: 64 and **128** seeds — never the 1,000-seed full-scale corpus, which runs only under VA-9
  commands 2 and 3 and in the 10,000-seed extended run; critic T-31 corrected the earlier
  "extended gate" misnomer), M7V-75 (two runs), M7V-76 (one run), M7V-51 (shares M7V-64's run).
- **Every corpus smaller than N = 29 seeds is `coverage_gated: false`** (VA-6, V-R20 (7)): it
  records coverage and never fails on `required_missing`. Only the 64-seed default corpus and the
  full-scale runs are gated, so the small corpora above cannot go red on a schedule they were too
  short to complete (critic T-25).
- Ceiling: **≤ 14 corpus executions**, one of them at 64 seeds and the rest at ≤ 16 seeds unless
  the row says otherwise. Planning target for the whole `--test campaign` binary at default scale,
  debug, on the recorded host: **< 120 s wall**. Recorded per run in `campaign_run.wall_ms`,
  **asserted nowhere** in the PR default (hard rule 1); if it is missed, spike §7's rule applies
  and the revision is written in ADR-rdb-0019, never `#[ignore]` (M7V-75).

Hard rules for every row in this plan:

1. **No wall-clock assertion in the PR default.** Numbers are *recorded*; only the extended gate
   asserts one, via `SPIKE_ASSERT_WALL_MS` (V-R11). `AGENTS.md` records why (`m4_69`: a capacity row
   failing on a loaded host with a third of the patience it was accepted with).
2. **No sleeping, no wall clock anywhere.** All time is `logical_tick`. Jump to the next deadline.
3. **`Unavailable` is never a pass, and `proven` means armed.** A row that cannot arm reports
   `Unavailable{NotArmed}`; a row whose package is unwired reports `Unavailable{Capability(p)}`;
   neither silently succeeds (VA-2, V-R16). A campaign status of `proven` requires
   `seeds_armed > 0` (M7V-78). `SPIKE_REQUIRE_ALL=1` turns any not-`proven` status into a gate
   failure (M7V-54), and it is set only in VA-9 command 3.
4. **Never lower an assertion to make a run green.** Spike §7: improve the harness or revise the
   budget explicitly, in ADR-rdb-0019.
5. **Reduced scale changes seed count and event cap only** — never which checkers run, never which
   fault kinds are reachable (M7V-65). This is ADR-0031's rule, applied to capability as well as
   scale.
6. **One row = one test**, and the row id prefixes the test name.
7. **A "valid trace that does not trip" row is a near-miss**, not a generic good trace: it differs
   from its bad twin by the one fact that makes the behaviour legal. A generic good trace proves the
   checker is silent, not that it is correct. M7V-02 is the one generic positive control, on purpose.

Environment knobs (`design.md` §5.1 as amended, four columns), all read once and recorded in the
artifact:

| Var | Default (`cargo test`) | Full scale (`RETCD_EVIDENCE=1`, VA-9 command 2) | M7 release gate (VA-9 command 3, V-R18) | Extended gate |
|---|---|---|---|---|
| `SPIKE_SEEDS` | 64 | 1000 | 1000 (the `RETCD_EVIDENCE=1` full scale) | 10000 |
| `SPIKE_MAX_EVENTS` | 512 | 2000 | 2000 | 2000 |
| `SPIKE_SEED_BASE` | 0 | 0 | 0 | 0 |
| `SPIKE_SHRINK_STEPS` | 2000 | 2000 | 2000 | 2000 |
| `SPIKE_SHRINK_MAX_FAILURES` | 3 | 3 | 3 | 3 |
| `SPIKE_SHRINK_BUDGET_TOTAL` | 20000 | 20000 | 20000 | 20000 |
| `SPIKE_ASSERT_WALL_MS` | unset (record only) | unset (record only) | `60000` (the charter figure, host-qualified) | `600000` (spike §7: 10,000 histories in 10 min; design §5.1 corrected the earlier `60000`) |
| `SPIKE_REQUIRE_ALL` | unset | unset | `1` — the only command that sets it | `1` |
| `RETCD_EVIDENCE` | unset | unset | `1` (V-R17; also set by command 2) | `1` |

---

## 3. Oracle: independence and the two controls (M7V-01..M7V-03)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7V-01 | `oracle_imports_no_kernel_algorithm` | charter O1 independence; spike §6 "must not import T1/R1/F1 algorithms" | every `.rs` file under `tests/support/oracle/` | **Allowlist, not blocklist (critic T-20):** every `rdb_core` path token in the file set is extracted — from `use` lines, brace groups (`use rdb_core::{a, b::c}` expands to each leaf), and inline paths — and each must start with `rdb_core::contracts::trace` or `rdb_core::contracts::ids`; any other token fails, including `rdb_core::*`, a brace group containing `transaction::…`, and an aliased re-export. The file list is non-empty (an empty glob must fail, not pass). Fails closed: a token the extractor cannot classify is a failure | unit | none |
| M7V-02 | `golden_valid_trace_trips_no_checker` | O1 "valid restricted-loss traces do not trigger"; the positive control for the whole oracle; pins every checker's arming event (design §2.4 under V-R16) | one hand-built trace that **arms all ten** (critic T-05): RF3 healthy on two partitions; a `batch_apply`, `publish`, `authority_decision` and `lineage_root` (ATOM, PUB, AUTH, LIN); one recovery with a declared `predecessor_cutoff` and a genuine restricted loss above it (LOSS); a second `client_submit` with a retained identity answered by a `dedup_record{action=Hit}` (DEDUP); one `schedule_phase{Healed, fair_delivery=true}` with admitted work on both partitions reaching terminal outcomes (LIVE, ISO); one `version_check{mandatory_unknown_fields=[], outcome=Accept}` (VER); and one complete legal `Healthy -> Paused -> Resuming -> Healthy` cycle in M7V-41's exact shape (LAG) | every one of the ten checkers returns `Proven` and `armed()` is true for each; **none** returns `Unavailable{NotArmed}` or `Unavailable{Capability(_)}`; **no** checker returns `Violated`. The row asserts `armed()` per checker by name, so the arming conditions are pinned here and nowhere else — if a checker's arming event changes, this row is the one that goes red | unit | C0 |
| M7V-03 | `unarmed_and_unwired_checkers_report_unavailable_not_proven` | charter Q1 "never a pass"; VA-2 both arms (V-R16) | (a) a trace carrying `capability{package=P1, state=Unavailable}` and otherwise arming everything (M7V-02's shape); (b) the header plus exactly ten `capability{package=<each PackageId>, state=Wired}` events and **nothing else** (critic T-26, ruling V-R20 (8): "zero-event" means no events after the capability block, so the fixture is well-formed under M7V-88 and design §4.5, and every package is wired — the stronger control) | (a) every checker that reads an event only P1 emits returns `Unavailable{Capability(P1)}`, every other checker returns `Proven`, and the campaign status table prints the reason; (b) all ten return `Unavailable{NotArmed}` — **not** `Proven`, not `Unavailable{Capability(_)}` (every capability event says `Wired`, so that arm has nothing to point at); in neither case does the row assert a pass, and in (b) the row additionally asserts `armed() == false` for all ten | unit | C0 |

---

## 4. Oracle: one bad trace and one near-miss per invariant (M7V-04..M7V-41)

Every row's input is a hand-built trace (VA-1). "Trips" means the named checker returns
`Violated(Signature)` with the stated `rule`; "clean" means it returns `Proven` (so it **armed**,
V-R16) and **no other checker** returns `Violated` either — other checkers may report
`Unavailable{NotArmed}` on a fixture that does not reach them, and that is fine (a near-miss that
trips a different checker is a defect in the fixture, and the row asserts the whole report, not one
checker).

Four conventions that every event literal below follows (critic T-06, T-04, T-23, T-26):

1. **Field names are the landed C0's at `8a23b1d`, verbatim** (`crates/rdb-core/src/contracts/
   trace.rs` and `ids.rs`; critic T-23, ruling V-R20 (1)). Where `trace-requirements.md` §3 still
   says something else, §15's drift table maps the ask to the landed name, and the landed name
   wins. In particular: `replication_ack` carries `from_node`, `to_node`, `peer_role`,
   `peer_boot`, `config_version`, `contiguous_seq`, `durability_class`; there is no `node`,
   `boot`, `from` or `seq` on it. Roles are `ReplicaRole::{Primary, RegularSecondary, Shadow}` —
   there is no `PeerRole` and no `Regular`. `protection_state` carries `phase: ProtectionPhase
   {Healthy, Warn, Paused, Resuming}` and **no `quorum_rule`**: the oracle derives the rule from
   `required_copy_set.len()` — 2 is `DegradedRf2`, 3 is `Rf3` (design §2.3 as amended, V-R20 (1))
   — and the rows below write the derived value in prose beside the literal, never as a field.
   **There will be no `quorum_rule` field (lead ruling F-R13, adjudicating K-F-07 against V-R20):**
   a derived value plus a stored value is two sources of truth for one fact, so K-F-07 is closed
   *by derivation* and foundation's committed `ProtectionState` at `6893442` carries no such field.
   The derived value is therefore the only source, for the coverage cell and for every decision the
   oracle makes. The round-2 cross-check row M7V-90 and the violation `quorum_rule_mismatch` are
   **withdrawn** with it (§15 drift table row 2); `QuorumRule` stays as verification's own
   two-member enum in `coverage.rs`, which is the axis of the `derived_quorum_rule` cell and is not
   a contract type.
   `publish.ack_evidence` entries are `AckEvidence{node, boot, role, durability}` — **four**
   fields. The `boot` landed at `6893442` under foundation's finding K-F-22 (this plan was
   written when the struct had three, §15 drift row 6): two acknowledgements from one node
   across a restart are two boots and the checker counts **one** copy, so the boot is the field
   that makes "one copy, counted twice" catchable and it is written in every row literal below.
   It must equal the `peer_boot` on that node's paired `replication_ack` at the same `seq`;
   where a row names no restart, every entry carries `boot=b1`.
   `topology_change.nodes` entries are `(NodeId, ReplicaRole)` tuples. The header's
   `topology` is a flat `Vec<TopologyEntry{partition, node, role, config_version}>`, and the
   header also carries `partitions: u8` (landed at `6893442`, §15 drift row 8) and
   `config: RunManifest{budgets, overridden, nodes, event_cap}` (drift row 10).
   `admission_decision.reason` is `Option<ErrorKind>`, so a rejection names a real variant
   (`ProtectionPaused`, `RequestIdReuse`, `CrossAffinity`, `GenerationChanged`, …), and
   `client_outcome.outcome` is `Success | RecoveredApplied | Error(ErrorKind)`. `durability_advance`
   has no node field of its own — the node is the **envelope's `node`** (landed `TraceEvent.node`),
   written `node=n2` below; the other envelope fields are `partition`, `boot` and `correlation`.
   `protection_state.config_version` is written in full, never `cv`. `queried_sources` entries are
   `{node, boot, role, reachable, reported_generation, reported_seq, reported_digest}`. **Two
   shorthands, and only two:** rows write `schedule_phase{…}` for the landed variant
   `SchedulePhaseChanged` and `client_outcome{…}` for `ClientOutcomeReported`; the JSONL `@m` is
   the full snake_case name (`schedule_phase_changed`, `client_outcome_reported`, VA-7), and any
   Q-row that reads either must use the full name (none of Q-34..Q-40 does).
2. **`contiguous_seq` and `durable_seq` are watermarks, not points.** Wherever a row says an ack
   or a flush is "at seq N", the checker implements `>= N`: an ack with `contiguous_seq=9` is
   evidence for every seq up to 9, and a flush with `durable_seq=9` grounds every `Durable` ack up
   to 9 on that node. INV-LOSS's holder map and INV-PUB's grounding clause are both built from the
   range test; a point comparison under-counts holders, which is the exact weakness V-R10's
   secondary-side emission rule was added to prevent.
3. **`required_copy_set` is a membership list read by two invariants with opposite
   quantifiers, and they must not share a helper.** INV-PUB checks *membership plus the quorum
   rule's cardinality* (one qualifying regular ack under `DegradedRf2`, M7V-09); INV-LAG quantifies
   over *every* member (M7V-36). A single `copy_set_satisfied()` gets one of the two rows wrong and
   the failure looks like a fixture bug. Each checker reads the field itself.
4. **Every acknowledgement is grounded in a secondary apply, by construction (critic T-26, ruling
   V-R20 (8); design §4 convention 4 / §4.5 as amended).** A fixture with
   `replication_ack{from_node=n, contiguous_seq=s}` carries `batch_apply{node=n,
   role=RegularSecondary, seq=s}` (or `role=Shadow` for a shadow's ack) **before** it, so
   M7V-88's realizability rule "`contiguous_seq` never above the emitting node's last
   `batch_apply.seq`" holds for every hand-built trace. `TraceBuilder::ack_from(n, s)` emits both
   events, and every ack in rows M7V-07, 08, 09, 10, 11, 28, 79 and 81 is written through it; a
   row that needs the apply *absent* says so explicitly and builds the ack by hand. The apply is
   not "one more fact" for rule A2: it is part of what an acknowledgement *is*.

### 4.1 INV-ATOM — atomicity (spec §5.2 step 3; V1)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7V-04 | `atom_partial_batch_in_published_prefix_violates` | a transaction's mutations are all published or none | `batch_apply{key_versions=[k1@3, k2@3], outcome=Applied}` then a `read` observing `k1@3` but `k2@2`, below a `publish` covering the batch's `seq` | INV-ATOM `Violated`, `rule="partial_batch_visible"`, signature `partition`/`role`/`event_kind=read` | unit | C0 |
| M7V-05 | `atom_crashed_before_commit_batch_is_absent_and_clean` | the near-miss: a batch that never committed must be invisible, and that is not a violation | same batch with `outcome=CrashedBeforeCommit`; no `publish` cites its `seq`; the `read` observes `k1@2, k2@2` | INV-ATOM `Proven`; no other checker fires. A run where the crashed batch's key versions **do** appear flips it to `Violated{rule="failed_batch_published"}` — asserted as the second half of the same fixture family but in row M7V-04's table, not here | unit | C0 |

### 4.2 INV-PUB — publication (spec §5.2 steps 6–7, §5.3, §8.3, §6.2; V1, V3)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7V-06 | `pub_observation_above_the_published_prefix_violates` | nothing above the last `publish` is observable, at any of the four surfaces | `publish{seq=7}`; then `read`, `status`, `Export` and `ActorRead` events each observing a `(key, version)` written at `seq=8` | INV-PUB `Violated` once **per surface** (four sub-cases in one row, all four asserted, `rule="observation_above_published_prefix"`, `request_kind` recorded) — `request_kind` is the discriminator, so a checker that only handles `Read` fails here | unit | C0 |
| M7V-07 | `pub_publish_without_the_pinned_required_copy_set_violates` | spec §8.3, healthy RF3: a shadow ACK never qualifies | `client_submit` + `admission_decision{Admitted, admitted_seq=5, config_version=1, required_copies=[n1,n2,n3]}` on one `correlation`; `protection_state{required_copy_set=[n1,n2,n3], config_version=1}` (derived rule `Rf3`, convention 1); `ack_from(n4, 5)` — i.e. `batch_apply{node=n4, role=Shadow, seq=5}` then `replication_ack{from_node=n4, peer_role=Shadow, contiguous_seq=5, durability_class=Durable}` (convention 4) — grounded by `durability_advance{node=n4, outcome=Synced, durable_seq=5}`; `publish{seq=5, ack_evidence=[{node=n4, boot=b1, role=Shadow, durability=Durable}]}` on the same `correlation` | INV-PUB `Violated`, `rule="required_copy_set_unsatisfied"`; the signature names the pinned `config_version`. Grounded and role-consistent on purpose so the copy-set rule is the only clause that can fire. **Sub-case, asserted in the same row (critic T-39, ruled a violation in design §2.3):** a `required_copy_set` whose length is neither 2 nor 3 — one trace pinning `[n1]`, one pinning `[n1,n2,n3,n4]`, each otherwise M7V-07's trace with a satisfying grounded ack — is INV-PUB `Violated`, `rule="required_copy_set_shape"`, with the signature carrying the observed length and `config_version`. It fires **at the `protection_state` event**, before any quorum arithmetic, and it is a violation rather than a fixture check **by ruling**: the oracle reads only the trace and cannot distinguish a bad fixture from a kernel that really pinned a one- or four-node set, and demoting it would force skipping INV-PUB for that seed — the silent skip §2.4 exists to prevent. A generated trace carrying one is a generator bug and surfaces the same way (M7V-42/M7V-55 are where that lands). The two legal lengths stay clean: M7V-07's own `[n1,n2,n3]` and M7V-09's `[n1,n2]` are the near-misses, so this sub-case differs from a passing trace by one fact, the length (rule A2) | unit | C0 |
| M7V-08 | `pub_degraded_rf2_one_ack_publish_violates` | **the F1 bug the whole correction round exists for.** Spec §8.3 "no one-copy fallback"; ADR-rdb-0019 §1 V3's degraded half; rewritten per critic T-02 so it can fire for exactly one reason and so the pin is resolvable | **(a) membership.** `client_submit` + `admission_decision{outcome=Admitted, admitted_seq=9, config_version=4, required_copies=[n1,n2]}` sharing one `correlation`; `protection_state{required_copy_set=[n1,n2], config_version=4}` — derived rule `DegradedRf2` from `required_copy_set.len() == 2` (V-R20 (1), convention 1) — emitted before the admission (VA-3 cadence); `topology_change{config_version=4, nodes=[(n1, Primary), (n2, RegularSecondary), (n3, RegularSecondary)]}` so `n3` is a legitimately *named* regular secondary that is simply not in the pinned set; `ack_from(n3, 9)` (convention 4: `batch_apply{node=n3, role=RegularSecondary, seq=9}` then `replication_ack{from_node=n3, peer_role=RegularSecondary, config_version=4, contiguous_seq=9, durability_class=Durable}`) **grounded** by `durability_advance{node=n3, outcome=Synced, durable_seq=9}`; `publish{seq=9, ack_evidence=[{node=n3, boot=b1, role=RegularSecondary, durability=Durable}]}` on the same `correlation`, with a valid `authority_recheck`. **(b) pin drift** (the sub-case that makes "pinned at `admitted_seq`" mean something): the same admission at `config_version=4` pinning `[n1,n2]`; then `protection_state{required_copy_set=[n1,n3], config_version=5}` (still derived `DegradedRf2`) and `topology_change{config_version=5, …}` **between** the admission and the publish; the publish carries a grounded `Durable` ack from `n3` — which satisfies the **new** set `[n1,n3]` but not the pinned one | (a) INV-PUB `Violated`, `rule="required_copy_set_unsatisfied"` **exactly** — not `durable_ack_ungrounded` (the ack is grounded), not `ack_role_claim_mismatch` (n3's role is declared); a checker with only the grounding clause, or one that evaluates grounding first and returns the wrong rule, fails here for the right reason. (b) INV-PUB `Violated`, same rule, and the signature names `config_version=4` — a checker that pins from the **last** `protection_state` seen passes (b) and is wrong. The pin is resolved by carrying `admission_decision.{admitted_seq, config_version, required_copies}` forward on the `correlation` (`publish` itself has no `config_version`). The `DEGRADED_RF2` coverage cell this row feeds is keyed on the **derived** rule, `derived_quorum_rule × DegradedRf2` (M7V-55). The row must fail if the checker applies the healthy RF3 rule; write it before the checker exists and watch it fail for the right reason first (ADR-0014, rule A7) | unit | C0 + VA-3 cadence + `topology_change` |
| M7V-79 | `pub_degraded_rf2_publish_on_the_primarys_own_durability_alone_violates` | **the F1 cardinality twin (critic T-02 defect 3).** Spec §8.3's forbidden behaviour is publishing on **one copy**: `min_regular_acks` 1-of-1 means one, not zero. A membership-only checker passes M7V-08 and M7V-09 and still ships this | M7V-08(a)'s admission and `protection_state{required_copy_set=[n1,n2], config_version=4}` (derived `DegradedRf2`); `n1` is the primary: `batch_apply{node=n1, role=Primary, seq=9}` and its own `durability_advance{node=n1, Synced, durable_seq=9}` are present; **no** `replication_ack` from `n2` at all (and no `ack_from` call — the absence is the point, convention 4); `publish{seq=9, ack_evidence=[{node=n1, boot=b1, role=Primary, durability=Durable}]}` — the primary's own durability is the only evidence | INV-PUB `Violated`, `rule="required_copy_set_unsatisfied"`; the signature reports `regular_acks_counted=0` against `min_regular_acks=1` (the minimum is derived from `required_copy_set.len()`, V-R20 (1)). Its near-miss is **M7V-09 unchanged** (one grounded ack from `n2` makes it clean), so together the pair pins both membership and cardinality. Cross-reference: `required_copy_set` is read with the opposite quantifier by M7V-36 (§4 convention 3) | unit | C0 + VA-3 cadence |
| M7V-09 | `pub_degraded_rf2_publish_with_the_pinned_single_regular_ack_is_clean` | near-miss: ruling B-R3, `min_regular_acks` 1-of-1 under the pinned config is **legal**. Differs from M7V-08(a) by **exactly one fact** (rule A2, critic T-04): the ack's node | M7V-08(a)'s trace verbatim — same admission pinning `[n1,n2]` at `config_version=4`, same grounding — with the single ack coming from `n2` instead of `n3`: `ack_from(n2, 9)` (`batch_apply{node=n2, role=RegularSecondary, seq=9}` then `replication_ack{from_node=n2, peer_role=RegularSecondary, config_version=4, contiguous_seq=9, durability_class=Durable}`) grounded by `durability_advance{node=n2, Synced, durable_seq=9}`; `publish{seq=9, ack_evidence=[{node=n2, boot=b1, role=RegularSecondary, durability=Durable}]}` | INV-PUB `Proven` and no other checker `Violated`. Without this row the F1 fix over-corrects into "RF2 needs two peers", which stops writes the spec permits. **Cross-reference M7V-36:** the same `required_copy_set=[n1,n2]` there must be satisfied by *every* member; here by *one qualifying* member — two rules, two readers, no shared helper (§4 convention 3) | unit | C0 |
| M7V-10 | `pub_ack_role_claim_mismatching_topology_violates` | §2.5 grounding rule 1; the label the kernel computes is not an independent fact | header `topology` carries the entry `{node=n4, partition=p0, role=Shadow, config_version=1}` (the landed flat `Vec<TopologyEntry>`); `ack_from(n4, 5)` with the ack's role claim overridden — `batch_apply{node=n4, role=Shadow, seq=5}` then `replication_ack{from_node=n4, peer_role=RegularSecondary, config_version=1, contiguous_seq=5, durability_class=Durable}` — grounded by a flush; a `publish` counting it | INV-PUB `Violated`, `rule="ack_role_claim_mismatch"` — **before** any quorum arithmetic, so the row still fires if the set happened to be satisfiable. Variant in the same row: after a `topology_change{config_version=2, nodes=[…, (n4, RegularSecondary), …]}` (V-R12), the same ack at `config_version=2` is clean — the role is resolved from the topology **in force at that ack**, not from the header snapshot (critic F19) | unit | C0 + VA-3 `topology_change` |
| M7V-11 | `pub_durable_ack_without_a_preceding_flush_violates` | §2.5 grounding rule 2; V1 clause 3 in its modelled sense; spike §6 "never use durable as an alias for in-memory application" | `ack_from(n2, 5)` — `batch_apply{node=n2, role=RegularSecondary, seq=5}` then `replication_ack{from_node=n2, peer_role=RegularSecondary, contiguous_seq=5, durability_class=Durable}` (convention 4: the *apply* is present; it is the *flush* that is missing) — with **no** `durability_advance{node=n2, outcome=Synced, durable_seq>=5}` anywhere before it (watermark: a flush with `durable_seq=4` does **not** ground it, one with `durable_seq=7` does) | INV-PUB `Violated`, `rule="durable_ack_ungrounded"`. Near-miss inside the row: the same ack preceded by `durability_advance{node=n2, Synced, durable_seq=5}` is clean, and preceded by `durability_advance{node=n2, Failed}` or `{Partial}` is **not** | unit | C0 |
| M7V-12 | `pub_lost_reply_does_not_retract_the_publish` | spec §5.3; near-miss for the "publication is final" rule | `publish{seq=6}` then `client_outcome{delivered=false, outcome=Error(UnknownOutcome)}` (landed `ClientOutcome::Error(ErrorKind)`, convention 1) then a `read` observing `seq=6` | INV-PUB `Proven`. A checker that treats an undelivered reply as un-publishing would fire here — that is the bug this row exists to forbid | unit | C0 |

### 4.3 INV-AUTH — authority (spec §7.2, §7.3; V2 model only)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7V-13 | `auth_overlapping_valid_generations_violate` | no two generations hold valid authority over one partition with overlapping windows | `authority_decision{generation=7, valid_from_tick=100, expiry_tick=200, outcome=Valid}` and `{generation=8, valid_from_tick=180, expiry_tick=260, outcome=Valid}` on the same partition | INV-AUTH `Violated`, `rule="overlapping_generations"`; the signature names both generations. Sub-case in the row: the overlap is reported even when the two decisions are at different `gate`s | unit | C0 |
| M7V-14 | `auth_apply_or_publish_under_an_expired_or_fenced_grant_violates` | spec §5.2's recheck at four gates; "uncertainty denies" | (a) `batch_apply{role=Primary, generation=7}` at a tick past `expiry_tick`; (b) `publish` whose `authority_recheck` points at an `authority_decision{outcome=Fenced}`; (c) the same with `outcome=Uncertain` | all three `Violated` with `rule` in `{apply_after_expiry, publish_under_fenced_authority, publish_under_uncertain_authority}`; (c) is the "uncertainty denies" clause and must not be collapsed into (b) | unit | C0 |
| M7V-15 | `auth_adjacent_non_overlapping_generations_are_clean` | near-miss: a handover at the boundary tick is legal | generation 7 `expiry_tick=200`, generation 8 `valid_from_tick=200` | INV-AUTH `Proven`. Pins the half-open convention; without it an off-by-one in the checker reads every clean handover as an overlap and the campaign drowns | unit | C0 |

### 4.4 INV-LIN — lineage (spec §8.1, §8.2; V3)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7V-16 | `lin_predecessor_digest_mismatch_violates` | every apply cites the recorded digest at `seq-1`, or its root's `base_digest` | `batch_apply{seq=5, predecessor_digest=D3}` where the recorded `entry_digest` at `seq=4` is `D4` | INV-LIN `Violated`, `rule="predecessor_digest_mismatch"`. Near-miss inside the row: the first apply after a `lineage_root` citing `base_digest` is clean | unit | C0 |
| M7V-17 | `lin_two_entry_digests_at_one_generation_seq_demand_quarantine` | "one `(generation, seq)` never carries two `entry_digest` values" — **scoped to `entry_digest` values carried by `batch_apply`** (critic T-03, option (i)); a digest conflict yields `mode=quarantine` | two `batch_apply{generation=7, seq=5}` with different `entry_digest`, from different nodes, **without** a following `quarantine` event | INV-LIN `Violated`, `rule="digest_conflict_without_quarantine"`. Near-miss: the same pair **with** `quarantine{reason=DigestConflict, generation=7, seq=5}` is clean, and a `recovery_decision{mode=Quarantine}` in the same run is required — divergence never auto-merges. **Scope, stated so M7V-19(b) and this row are satisfiable together:** the conflict clause compares `batch_apply.entry_digest` values only. A `recovery_decision.queried_sources[].reported_digest` that disagrees with a recorded `entry_digest` is **not** this clause's antecedent; it is F1's divergence decision, checked by M7V-80 | unit | C0 |
| M7V-18 | `lin_cutoff_above_a_recorded_matching_prefix_violates` | F2 closure clause 2, as **two hash-map lookups**, not a compatibility algorithm | oracle has recorded `entry_digest=D9` at `(gen 7, seq 9)` from a `batch_apply`; `recovery_decision{selected_cutoff_seq=6}` while a `queried_sources` entry with `reachable=true` reported `{reported_generation=7, reported_seq=9, reported_digest=D9}` | INV-LIN `Violated`, `rule="cutoff_below_an_available_recorded_prefix"` | unit | C0 |
| M7V-19 | `lin_cutoff_is_clean_when_the_longer_source_is_unreachable_or_mismatched` | the near-miss that stops the oracle re-deriving F1's selection | two sub-cases against the same recorded lineage: (a) the longer source has `reachable=false`; (b) the longer source is reachable but its `reported_digest` differs from the recorded `entry_digest` at that `(generation, seq)` | INV-LIN `Proven` in both, and no other checker `Violated`. (b) is the row that proves the oracle **never derives pairwise compatibility** — it only looks up what it already recorded (charter EXCLUSIONS; spike §6). **Why (b) is `Proven` without a `quarantine` event (critic T-03):** a recovery-path digest disagreement is kernel-b's F1 decision — a correct kernel answers it with `recovery_decision{mode=Quarantine}` — and not the oracle's; INV-LIN's conflict clause is scoped to `batch_apply` (M7V-17). The oracle asserting Quarantine here would be re-deriving F1's selection, which is the charter exclusion. The kernel-facing half is the third sub-case, carried as row **M7V-80** because its class and dependency differ | unit | C0 |

*(M7V-20..M7V-23 are the reducer rows — §6.)*

### 4.5 INV-DEDUP — retries and outcomes (spec §5.3, §5.4, §8.1; V4)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7V-24 | `dedup_two_applies_for_one_identity_violate` | one effect per `(tenant, client, request)` (`RequestIdentity`, landed `ids.rs`) scoped by `affinity` and generation, within the retention window | two `batch_apply` events sharing the identity via `correlation`, same generation, inside `retained_until_tick` | INV-DEDUP `Violated`, `rule="duplicate_effect"`. Near-miss in the row: the second submit answered by `dedup_record{action=Hit}` with **no** second apply is clean | unit | C0 |
| M7V-25 | `dedup_the_three_pre_mutation_rejections_are_clean_and_distinct` | near-miss bundle: the three §5.4 rejections that must happen **before** any apply | (a) same identity, different `request_digest` → `admission_decision{outcome=Rejected, reason=RequestIdReuse}`; (b) `affinity` not the partition's group → `reason=CrossAffinity` (critic F14); (c) retry with a stale `expected_generation` → `reason=GenerationChanged` — the three landed `ErrorKind` variants, carried as `admission_decision.reason: Option<ErrorKind>` and echoed as `client_outcome{outcome=Error(<same>)}` (convention 1) | INV-DEDUP `Proven` in all three; **no `batch_apply` carries the `correlation`** in any of them, which is the half that makes them rejections rather than errors after the fact. Each maps to its own guard-outcome coverage cell (§7) | unit | C0 |
| M7V-26 | `dedup_absence_is_never_reported_as_proof_of_nonexecution` | spec §8.1 and spike §6's mandatory F1/T1/P1 case; the reason `ClientOutcome` needed `RecoveredApplied` (critic F15) | a `read{request_kind=Status}` (the landed single `Read` kind, TR §8 row 16) for an identity with no retained record, answered `client_outcome{outcome=Success, seq=None}` claiming the transaction did not run | INV-DEDUP `Violated`, `rule="absence_reported_as_nonexecution"`. Near-miss in the row: the same absence answered `Error(UnknownOutcome)`, `Error(StatusExpired)`, or — for a retained old-generation identity — `RecoveredApplied`, is clean (landed `ClientOutcome`, convention 1) | unit | C0 + VA-3 outcome set |

### 4.6 INV-LOSS — restricted loss after majority loss (spec §6.3, §8.4; V3)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7V-27 | `loss_under_an_unchanged_generation_violates` | loss is only ever permitted across a recovery root | a key version present in the published prefix disappears from a later `read`, with **no** intervening `lineage_root{source=Recovery}` | INV-LOSS `Violated`, `rule="loss_without_recovery_root"` | unit | C0 |
| M7V-28 | `loss_with_a_reachable_durable_holder_at_its_boot_violates` | the copy-loss precondition, clause (a) | `ack_from(n2, 9)` — `batch_apply{node=n2, role=RegularSecondary, seq=9}` then `replication_ack{from_node=n2, peer_boot=b1, contiguous_seq=9, durability_class=Durable}` (convention 4) — emitted at the secondary and grounded by `durability_advance{node=n2, Synced, durable_seq=9}` (both watermarks: `>= 9`); `recovery_decision.queried_sources` lists `{node=n2, boot=b1, reachable=true}`; `lineage_root{source=Recovery, predecessor_cutoff=6}`; the version at `seq=9` disappears from a later `read` | INV-LOSS `Violated`, `rule="loss_with_a_surviving_durable_holder"`. Watermark sub-case in the same row: an ack with `contiguous_seq=11` (no ack literally "at 9") is still a holder at 9, and the row fires | unit | C0 + VA-3 secondary-side emission |
| M7V-29 | `loss_with_buffered_only_holders_returning_at_a_new_boot_is_clean` | near-miss, critic F9: a host crash may discard every unflushed suffix, so a returning buffered-only holder is **not** evidence the data survived | the only holders at `seq=9` held `durability_class=Buffered`; each is either unreachable or listed in `queried_sources` under a **different** `boot` (landed `QueriedSource{node, boot, role, reachable, …}`, critic T-41(d)) after `StorageOp::Crash{kind=Host}`; loss is below the declared `predecessor_cutoff` | INV-LOSS `Proven`. Sub-case that must still violate, asserted in the same row: a buffered-only holder that is **reachable at the same `boot`** — i.e. it never restarted, so nothing could have discarded its buffer (critic T-19: the envelope's `boot` "distinguishes a restarted node from itself", and a process crash *loses* unflushed process buffers per design §3 — it is never modelled as buffer-preserving) | unit | C0 |

### 4.7 INV-LIVE and INV-ISO — controlled liveness and isolation (spike §6, §7; V-R8)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7V-30 | `live_healed_schedule_with_a_stuck_request_violates` | spike §6's controlled liveness, armed correctly | `schedule_phase{phase=Healed, fair_delivery=true, remaining_event_budget=200}` from a `NetworkOp::Heal`; a valid authority decision; one `inflight` request that never reaches a terminal `client_outcome` inside the budget; `protection_state` still `Paused` at the end | INV-LIVE `Violated`, `rule="no_terminal_outcome_under_healed_schedule"` (and a second sub-assertion for `protection_state` never leaving `paused`) | unit | C0 |
| M7V-31 | `live_unhealed_or_exhausted_budget_disarms_the_checker` | spike §6: "an unhealed partition or endless message dropping is not a liveness failure"; disarming **is** `NotArmed` (design §2.4, V-R16) | (a) the same stuck request with **no** `schedule_phase{Healed}`; (b) healed but `remaining_event_budget` reaches 0 first | INV-LIVE returns `Unavailable{NotArmed}` in both — **not** `Proven`, not `Violated`, and not `Unavailable{Capability(_)}` (no capability event justifies that arm; a checker that reaches for it is inferring a capability from silence, which VA-2 forbids) — **and `armed() == false` after the fold in both** (critic T-24, ruling V-R20 (6)): in (b) the checker armed on `Healed` and then disarmed, and a checker that leaves `armed()` true after disarming lets M7V-52's fold count the seed as armed. This is the row that stops the liveness checker becoming the campaign's flake source | unit | C0 |
| M7V-32 | `iso_partition_b_stalls_while_only_partition_a_is_blocked_violates` | spike §7's safety table; spec §5.2 "other partitions in the set keep running"; P1's "freezes only its partition" | two-partition topology; `unresolved[A] = Some(seq)`; healed, fair schedule; partition B has admitted work and produces **no** terminal `client_outcome` in the budget | INV-ISO `Violated`, `rule="sibling_partition_starved"`. Feeds the single required isolation coverage cell (§7) | unit | C0 |
| M7V-33 | `iso_disarms_when_the_sibling_partition_has_no_admitted_work` | the near-miss that stops INV-ISO firing on an idle partition | same trace with no `admission_decision{outcome=Admitted}` for partition B | INV-ISO `Unavailable{NotArmed}` (disarmed, V-R16), never `Violated`, never `Proven`; **and `armed() == false` after the fold** (V-R20 (6)) — the `schedule_phase{Healed}` armed it, the idle sibling disarmed it, and the end-of-fold state is what `seeds_armed` reads | unit | C0 |

### 4.8 INV-VER — compatibility subset (spec §5.4 `INCOMPATIBLE_VERSION`; V12 subset)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7V-34 | `ver_unknown_mandatory_field_applied_violates` | V12's M7 claim: unknown mandatory versions are refused **before apply** | `version_check{mandatory_unknown_fields=[17], outcome=Accept}` (landed `Vec<u16>`) followed by a `batch_apply` carrying the same `correlation` | INV-VER `Violated`, `rule="unknown_mandatory_field_applied"`. Both halves asserted separately: the wrong `outcome`, and the apply that followed | unit | C0 |
| M7V-35 | `ver_additive_unknown_optional_fields_are_accepted_and_clean` | near-miss: "compatible additive fields tolerated" (validation plan V12) | `version_check{mandatory_unknown_fields=[], declared_schema_version > known_max, outcome=Accept}` then a normal apply | INV-VER `Proven`. Without this the checker degenerates into "refuse anything newer", which fails the other half of V12 | unit | C0 |

### 4.9 INV-LAG — lag protection, transition legality only (spec §6.2; V8)

Written **after** re-reading `design.md` §2.3 (corrected for critic F8 and F20), §2.6, and
ADR-rdb-0019 §1's V8 row. Three clauses, all readable from declarations. The **1 s warn / 2.1 s
pause timing ladder is deliberately not asserted here** — it depends on H1 delivering a health
evaluation every ≤50 ms, a legal `TimeOp::Pause` makes a correct kernel miss it, and the oracle
cannot distinguish "no evaluation arrived" from "one arrived and the kernel did not flip"
(`design.md` §2.6). It belongs to kernel-b's L1 rows (one kernel row, one harness row); this plan
cites them for V8's timing half and asserts neither half alone is V8.

> **Correction carried, and verified landed.** The critic's round-1 clause "no `publish` while
> `state=Paused`" was **withdrawn by the critic** in F20 as their own error, and clause (a) gained
> the "every pinned copy" quantifier in F8. Both corrections are in `design.md` §2.3 **and** in
> ADR-rdb-0019 §1's V8 `Form` cell as of 2026-09-20 18:43 — re-read before these rows were written,
> per the lead's instruction. Row M7V-39 asserts the replacement clause; row M7V-40 asserts the
> withdrawn one is **not** enforced, and is the regression guard if anyone re-adds it from an older
> copy of the critic's round-1 text.

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7V-36 | `lag_resume_before_every_pinned_copy_reaches_the_barrier_violates` | clause (a)'s quantifier (critic F8): spec §6.2 "**all configured regular copies** durable through paused prefix" | `protection_state{phase=Paused, paused_prefix_seq=40, resume_barrier_seq=40, required_copy_set=[n1,n2,n3], config_version=3}`; `durability_advance{node=n2, Synced, durable_seq=40}` only; then `phase=Resuming` then `phase=Healthy` | INV-LAG `Violated`, `rule="resume_without_every_pinned_copy"`; the signature records which pinned nodes were short. A checker written with a singular `durability_advance` passes this and is the weaker of the two implementations — kernel-b's `all_durable_through(resume_barrier)` gets it right. **Cross-reference M7V-09:** INV-PUB reads the same `required_copy_set` field and is satisfied by *one qualifying* member; this clause needs *every* member. Two readers, no shared helper (§4 convention 3, critic T-04) | unit | C0 |
| M7V-37 | `lag_resume_with_lag_above_250ms_inside_the_hold_violates` | clause (a)'s two remaining conditions: the barrier is hit **exactly**, and `oldest_unsafe_age_ms < 250` continuously for 5 s of `logical_tick` (spec §6.2's resume row; the validation plan omits the 250 ms and team-rules puts the spec above it) | (a) every pinned copy at the barrier, but one `protection_state` inside the 5 s window reports `oldest_unsafe_age_ms = 400`, then `Healthy`; (b) a `durability_advance` whose `durable_seq` **overshoots** `resume_barrier_seq` | (a) `Violated{rule="resume_hold_broken"}`; (b) `Violated{rule="resume_barrier_not_exact"}`. Two named rules, because the operator diagnosis differs | unit | C0 |
| M7V-38 | `lag_unsafe_age_reset_by_a_config_version_change_violates` | clause (b) — **the real V8 subtlety.** Spec §6.2: "no timer reset merely because a replica was renamed/replaced" | `protection_state{config_version=3, oldest_unsafe_age_ms=1800}` then `protection_state{config_version=4, oldest_unsafe_age_ms=0}` with no retirement barrier between them | INV-LAG `Violated`, `rule="unsafe_age_reset_across_config_version"`. Near-miss in the row: the same drop **with** a retirement barrier is clean | unit | C0 + VA-3 cadence |
| M7V-39 | `lag_admission_admitted_while_paused_violates` | clause (c), as restated by critic F20: pausing is an **admission** gate | `protection_state{phase=Paused}` at tick 2000; `admission_decision{outcome=Admitted}` at tick 2100; no intervening `phase=Healthy` | INV-LAG `Violated`, `rule="admitted_while_paused"`. Near-miss in the row: an `admission_decision{outcome=Rejected, reason=ProtectionPaused, paused=true}` in the same window is clean (`reason` is the landed `Option<ErrorKind>`, convention 1) | unit | C0 |
| M7V-40 | `lag_publish_of_an_already_admitted_transaction_while_paused_is_clean` | the withdrawn clause, asserted as a **non**-violation. Spec §5.3: an admitted, applied transaction must be resolved by ACK or recovery, not abandoned; publication is P1's independent decision | admitted at tick 0, applied, ACKed and published at tick 2500, while `protection_state{Paused}` since tick 2000 | INV-LAG `Proven`, and no other checker fires. **This row exists to fail if anyone re-adds "no publish while paused"** — the clause would report a violation on correct behaviour in a scenario every lag test produces (critic F20) | unit | C0 |
| M7V-41 | `lag_complete_exact_resume_is_clean` | near-miss for clause (a): the whole legal resume path | every node in the pinned `required_copy_set` reaches `durable_seq == resume_barrier_seq` exactly; `oldest_unsafe_age_ms < 250` on every `protection_state` for 5 s of `logical_tick`; then `Healthy` | INV-LAG `Proven`. Without it, a checker that requires something stricter than the spec passes the gate silently and blocks a correct kernel | unit | C0 |

---

## 5. G1: grammar and generator (M7V-42..M7V-47)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7V-42 | `grammar_every_required_boundary_variant_is_constructible` | spike §6's scenario-operations table, right-hand column, is fully expressible | the `REQUIRED` const list (VA-6, design §3.1 under V-R19), derived from `BoundaryId`, and the `BoundaryId -> producing ScenarioOp` table in `gen.rs` | **A static table check, stated as such (critic T-21):** for every `BoundaryId` member the producer table has an entry, and the entry's `ScenarioOp` constructs; the row enumerates rather than hand-lists, so a new `BoundaryId` with no producer **fails** instead of passing quietly (M6-107 pattern). It does **not** run the environment — whether the op actually *reaches* the boundary is M7V-55's behavioural claim, counted from `fault_injected`, and M7V-55 is what catches this table going stale. Explicitly includes `ForgedIdentity` and `FalseDurableWatermark` (V-R9) and the process-vs-host `Crash{kind}` distinction. **Family map, checked against the trace and not against itself (critic T-40, design §3.1):** C0 has no `impl BoundaryId` and no `fault_kind()`, so the `BoundaryId -> FaultKind` family assignment that keys the gating table (VA-6, V-R20 (4)) has no ground truth in the contract and a member filed under the wrong family would pass an enumeration row that reads the same const it tests — inheriting the wrong gating package. The static half here asserts every `BoundaryId` has exactly one family and one gating package; the **behavioural** half is M7V-55's, from `fault_injected{boundary, fault_kind}`. No contract change and no foundation ask | unit | none |
| M7V-43 | `generator_same_seed_and_version_yields_an_identical_scenario` | spike §4's trace seam: determinism, and that weights are constants rather than env-tunable; the V-R19 schedule is a function of the seed index, not of the PRNG | `gen::scenario(seed, budget)` twice, and once more after reading a polluted environment; then seeds `i` and `i + N` (N = `REQUIRED.len()`) | all three `Scenario` values are byte-identical after serialization; no `std::env` read occurs inside `gen.rs` (asserted by a source grep, like M7V-01); seeds `i` and `i + N` both carry the scheduled op for `REQUIRED[i mod N]` and differ elsewhere — the obligation is deterministic and the PRNG still varies the rest | unit | none |
| M7V-44 | `generator_respects_the_budget` | charter DO-NOT "no unbounded search"; spike §7's bounded histories — the **generator** half (critic T-21; the runner half is M7V-86) | `Budget { max_events: 64, max_ticks: 500 }` over 200 seeds | no generated scenario's op list can produce more than `max_events` events by the grammar's own per-op event bound (a static count over the op list, no runner); no `ScenarioOp` list is empty (an empty scenario is a silently useless seed); every scenario carries its scheduled `REQUIRED[i mod N]` op inside the budget | unit | none |
| M7V-86 | `runner_stops_at_max_events_and_ends_at_an_event_boundary` | the **runner** half of the bound (critic T-21): "the runner stops at it" is behaviour, and needs I1 | one scenario whose op list would produce more than `max_events` events, run with `Budget { max_events: 64 }` | the trace has exactly `max_events` events or fewer; the last event is a complete event (the runner never truncates mid-transaction — a cut inside a transaction leaves the oracle `Unavailable{NotArmed}` for that seed, never `Violated`, and the row asserts that verdict on the cut trace); the run records `budget_spent = max_events` | sim | I1 |
| M7V-45 | `scenario_json_round_trips_and_a_schema_bump_rejects_a_stale_fixture` | D4: the fixture, not the seed, is the reproducer; spike §7 "unknown environment/config fields are errors" | every file in `tests/fixtures/{scenarios,regressions}/`; plus a synthetic fixture at `schema_version + 1`; plus one with an unknown field | round trip is lossless for all checked-in fixtures; the bumped and unknown-field fixtures are **rejected with a typed error**, never silently defaulted. A `schema_version` bump invalidating checked-in fixtures is the intended behaviour, not a regression | unit | none |
| M7V-46 | `provenance_is_explicit_and_nothing_carries_a_bare_seed` | critic F18 (both halves): a reduced or authored scenario is not in the generator's image, so a `seed` field on it is false provenance. **Un-parked (re-read at `ec610f4`):** `Provenance` landed at `6893442`, `contracts/trace.rs:85`, matching `trace-requirements.md` §8.1 arm for arm, with `TraceHeader.provenance` at `trace.rs:212` and **no `TraceHeader.seed` left**. Both halves of this row now run; the header half was parked for four rounds on a type that had already shipped | every checked-in fixture; and the trace header produced for each of the three `Provenance` kinds | `Provenance` is `Generated{seed} \| Reduced{parent} \| Authored{case}` — the landed field on the reduced arm is `parent: ScenarioId`, not `from` — and every fixture carries one; the **trace header** carries the same `provenance` (not a bare `seed`), so a failure report cannot print "seed 4471" for a run no seed reproduces. The header half additionally asserts that `TraceHeader` has **no** field named `seed`, which is what stops the bare seed coming back: serde's `deny_unknown_fields` on the header makes a re-added `seed` a decode failure on every fixture, and this row says so in one place | unit | C0 |
| M7V-47 | `authored_cross_package_cases_construct_and_run` | spike §6's four **mandatory** cross-package adversarial cases, as Rust constructors (critic F18b) rather than hand-typed JSON | `case_a1_p1_new_generation_between_publish_and_reply()` (re-authored from "expire between publish and reply", lead ruling L-R177dq), `case_f1_r1_discovery_window()`, `case_f1_t1_p1_retained_status_24h()`, `case_f1_t1_digest_across_recovery()` | each constructs, carries `Provenance::Authored`, runs to completion inside its budget, and registers its named pairwise coverage cell. While the kernel packages are unwired the run reports `Unavailable{Capability(p)}` for the invariants involved and the row asserts **that**, never a pass (§12). The A1/P1 case asserts **both halves** of the adversarial row (A-R22): the kernel's outcome and the oracle's verdict | sim | I1 |
| M7V-88 | `every_fixture_and_authored_case_is_realizable_by_the_runner` | design **§4.5** (critic round 2, the second-order form of R-3): a checker tuned to a shape the runner can never produce arms in its unit row and never in the campaign — visible as `unavailable(not_armed)` under V-R16, but still a checker that guards nothing. Fixtures must be realizable, and the assertion is never weakened to make them so | (1) every `tests/fixtures/scenarios/*` file and every authored constructor (design §3.1 family 2, M7V-47's four); (2) every `TraceBuilder` trace an oracle row in §3/§4/§8 feeds to a checker, collected through the shared builder registry (each row registers its trace under its id) | (1) each scenario replays through I1's runner with **no** `op_skipped{reason=ReferentGone}` and the oracle report carries the verdict the owning row expects (`Proven`, or `Violated` on the named `(checker, rule)`); (2) each hand-built trace passes the same well-formedness checks I1 applies to a recorded trace — strictly increasing `event_id`, the `capability` block first, `schedule_phase` before any liveness arming, `replication_ack.contiguous_seq` never above the emitting node's last `batch_apply.seq` — through I1's validator; **if I1 exposes no validator the row runs the envelope checks only and reports `Unavailable{Capability(I1)}` for the rest, and says so in its output**. A failing fixture is fixed in the fixture (charter DO-NOT); the row never edits an expectation. Budget: **< 10 s** for the whole set, stated here because it replays every fixture, not one | sim | I1 |
| M7V-80 | `recovery_path_digest_disagreement_yields_quarantine_from_the_kernel` | the kernel-facing third sub-case of M7V-19 (critic T-03, option (i)): a reachable source whose `reported_digest` differs from the recorded `entry_digest` at a recorded `(generation, seq)` is a divergence that spec §8.2 says never auto-merges, and it is **F1's** decision, not the oracle's | a directed scenario (`design.md` §3.1 family 2 style, `Provenance::Authored`): RF3, a `StorageOp::Crash` on the primary after `seq=9` is applied on one secondary only, then a `NetworkOp::Partition` that leaves the recovering side with a source reporting a different digest at `(gen 7, seq 9)`, then `RecoveryOp::Synchronize` | the trace contains `recovery_decision{mode=Quarantine}` with `queried_sources` naming the disagreeing source, and a `quarantine{reason=DigestConflict, generation=7, seq=9}`; INV-LIN is `Proven` on that trace (the conflict was quarantined, M7V-17's near-miss shape); the `recovery_decision.mode × quarantine` guard cell and the `BoundaryId::Divergence` boundary cell are both hit. **Coverage clause (§15.1 cell 6, CB-3):** the `replication_ack.reject_reason × AckRejectReason::Diverged` cell is hit on a later ACK from the disagreeing source — kernel-b's §3.4 rule 1d, the ACK the tracker drops once the copy is marked diverged (M7B-41). That clause additionally reports `unavailable(R1)` until the tracker is wired, so it is `unavailable`, never a pass, even on a run where F1 has landed. `Unavailable{Capability(F1)}` until F1 lands — never a pass (§12). **Status (2026-09-27, contract L-R177gd): landed as `m7v_80_recovery_path_digest_disagreement_yields_quarantine_from_the_kernel` in `tests/dispatch.rs`, parked on I1.** Realized on the spine with node 3's survivor forking the head (`(gen 1, seq 2)` stands for `(gen 7, seq 9)`; the position is illustrative, the shape is not). Asserted: F1's one `Quarantine(Pairwise)` fact at the head with copy 2 a side, phase `Quarantined`; one `CloseWindow` in the deciding step; exactly one `recovery_decision` at F1's node with `mode=Quarantine`, `selected_source`, `selected_cutoff_seq`, `selected_digest` and `new_generation` all `None`, `fenced_epoch` the fence's, `discovery_window_ticks` = close − fence arrival, `loss_uncertainty=false`, and `queried_sources` one per member (the Shadow unreachable, the disagreeing one with its forked digest); the `recovery_decision.mode × Quarantine` guard cell; `quarantine{DigestConflict, 1, 2}` naming both sides; INV-LIN not `Violated`. **Parked (`parked(.., I1, ..)`):** INV-LIN `Proven` (the recorder writes no `lineage_root`, so INV-LIN never arms on a recorded run and reports `Unavailable{Capability(T1)}`), the `BoundaryId::Divergence` cell (no `fault_injected` line) and the `AckRejectReason::Diverged` cell (no rejected-ack line; a quarantine builds no receiver, so also `unavailable(R1)`). The census reports it `PARKED`, i.e. owed. Red first: the row failed on zero `recovery_decision` lines before the recorder slice. Mutants: a recorder writing `Some(Seq::ZERO)` on a quarantine and an oracle reading `None` as 0 both turn it red (§15 delta) | sim | I1 + F1 |
| M7V-93 | `a_recovered_scenario_flushes_after_the_cutoff_and_l1_resumes` | the host half of a recovered run (lead ledger L-R177gf, `inv-publish-path.md` break 1): no kernel emits a flush, so without one after the cutoff the copy R1 walks up reports durable 0, `barrier_durable()` stays false and L1 never resumes | one authored recovered RF3 scenario in `tests/host_cadence.rs` (B and C survive at 10, A dead, no transfer, 9 000 ticks), lowered by `support::scenarios::run::lower` | (1) lowering: `plan.flushes` names every node, none before the cutoff (fence + window here), and the cadence reaches the deadline within `HOST_FLUSH_EVERY_MILLIS`; (2) the run: L1 on B publishes `SetAdmission{allow: true}` no earlier than the first flush plus `resume_hold_millis`, its last `SetAdmission` is `allow`, and the live L1 instance agrees at the deadline | sim | I1 |
| M7V-94 | `a_recovered_scenario_acquires_an_a1_grant_after_the_activation_cas` | the scenario half of `inv-publish-path.md` break 2 (L-R177gf): nothing arms A1's first `AcquireDue`, so a recovered scenario never holds a grant and every node keeps the zero triple | the same scenario as M7V-93 | (1) lowering seeds exactly one `AcquireDue`, on the primary; (2) the run: two committed `partitions/1` CASes (recovery, activation), the first committed `grants/{B}` CAS after the second of them, a later renewal (so the grant is held), no grant CAS on C or A, an A1 `LineageLoaded` on B after the acquisition, and the dispatcher's triple for B past generation 1 | sim | I1 |
| M7V-95 | `an_armed_deadline_fires_on_time_behind_a_stalled_step_and_a_far_seed` | the runner half of V-R30: due work that queues nothing (a stalled host flush, a stalled transfer step) must not let `Runner::run` pop a later queued event, or the clock jumps to it and every armed deadline in between fires late. Found when M7V-94's `AcquireDue` seed moved F1's discovery close in `case_f1_r1_discovery_window` from 4003 to 8503 | a hand-built `RunPlan` in `tests/host_cadence.rs`: RF3, `StorageOp::StallFlush` on node 2, host flushes at 100 (node 2) and 200 (node 1), one `AcquireDue` seed at 5 000 on node 1, deadline 6 000 | the first pop is node 1's flush answer at 200; the seed still pops at 5 000; nothing pops on node 2 | sim | — |
| M7V-96 | `m7v_96_recording_a_failed_commit_is_a_failed_batch_apply_and_never_acked` | recorder truth (tester-sim-hooks F1, probe p3): a commit M1 refuses is recorded as the refusal | the spine (`tests/dispatch.rs` `spine_plan`) with `StorageOp::Fail{node 4, WriteFailed}` | node 4 carries a `batch_apply{outcome=Failed}`; every `Applied` line names a record its engine holds with those digests; every ack rests on an earlier `Applied` line of its seq and none is at seq 0; no oracle violation. Kills mutant t1 (a failed commit logged `Applied`) | sim | M1 hook |
| M7V-97 | `m7v_97_recording_a_failed_flush_is_a_failed_durability_advance_never_synced` | recorder truth (probe p4): a failed flush is `Failed` at the watermark still held | the spine with `StorageOp::Fail{node 2, FlushFailed}` | node 2's first `durability_advance` is `(0, Failed)`; the engine-state checks of M7V-96 hold. Kills mutant t2 (`Err` recorded `Synced`) | sim | M1 hook |
| M7V-98 | `m7v_98_recording_a_short_flush_is_partial_at_the_engines_short_watermark` | recorder truth (probe p5) and ruling V-R36 (ShortFlush): a flush that makes less durable than was captured is `Partial`, because the `SyncOutcome` contract lets only `Synced` publish the captured prefix | the spine with `StorageOp::ShortFlush{node 2, through 1}`; F1's sync captures `(partition 1, seq 2)` | node 2's first `durability_advance` is `(durable_seq 1, Partial, captured [(1, 2)])`; the engine-state checks hold. Red first: `Synced` before the recorder change. Kills t3 on the run and the V-R36 regression (MF) | sim | M1 hook |
| M7V-99 | `m7v_99_recording_a_bare_engines_syncs_names_its_watermark_and_the_real_outcome` | `harness::semantic::durability_lines` over a bare `MemoryEngine`, case by case (probe p7) | spine history committed on one engine; syncs through 1, `ShortFlush` 1 of 2, 2, 2 again, 1, 9, then `FalseDurable` and `FlushFailed` | `(before, after, line seq, outcome)` = `(0,1,1,Synced)`, `(1,1,1,Partial)`, `(1,2,2,Synced)`, `(2,2,2,Synced)` (a no-op sync is `Synced` at the watermark: F2), `(2,2,2,Synced)` (below the watermark: the engine's 2, not the captured 1), `(2,2,2,Partial)` (above applied), `(2,2,2,Partial)`, `(2,2,2,Failed)`. Red first on the `ShortFlush` case. Kills t2, t3 and MF | unit | M1 hook |
| M7V-100 | `m7v_100_recording_only_an_accepted_reply_is_a_replication_ack` | recorder truth (probe p8 + F1's deterministic kill for t4): only an `Accepted` reply is a `replication_ack`, verbatim | `semantic::ack_line` on each reply kind and on an append body; then `Semantic::record` fed R1's non-`Accepted` replies at node 3 of a finished rebuild run, where node 3 holds copy 2's receiver, then one `Accepted` | `ack_line` gives the exact line for `Accepted` and `None` for `AlreadyHave`, `Busy`, `ProbeDigestAt`, `Rejected` and an append body; `record` writes no line for the non-acks and one line, equal to `ack_line`'s, for the `Accepted`. Kills t4 (a refusal logged as the receiver's current ack) | sim | R1 |
| M7V-101 | `m7v_101_recording_every_line_matches_engine_state_on_the_spine_and_the_rebuild` | recorder truth on unmutated plans (probes p1, p1b) | `rebuild_plan` and `spine_plan`, run to the end, engines kept | every `Applied` apply, `Synced` sync and ack checked against the engines as in M7V-96; both runs end without error; each tick-0 `batch_apply` carries its node's pinned role, and more than one role is seen; no oracle violation. Kills t5 (preloads logged `Primary`) and t4 through the zero-seq check | sim | I1 |
| M7V-102 | `m7v_102_restart_forgets_what_the_crashed_process_held_in_memory` | ruling V-R35: a restart rebuilds the node's kernel modules from durable state only, so nothing the crashed process held in memory survives it | the spine (`tests/restart.rs`, a copy of `tests/dispatch.rs` `spine_plan`) to 3 000; a `StorageOp::Crash{ProcessCrash}` on node 1, tripped by a store effect; `Dispatcher::restart(node 1, boot 2)` | preconditions: before the crash node 1 holds A1's grant, F1 `Committed` on partition 1, R1's primary and retransmit timer, T1's instance, generation floor and trim memory (a `DedupTrim` seeded at 2 900), P1's instance and a scripted view (installed by the row), L1's instance, armed timers, and a non-zero adopted triple on both partitions. After the restart every one of them is gone, and the node's boot is 2. Red on HEAD `547c82c` (six clauses survived, `ev/red-head.log`); mutants m0 (HEAD's `restart` body), m1 (F1 not rebuilt), m3 (old A1 carried over), m5 (timers kept), and round 2's M2b (retransmits), M2e (floors), M2f (trims) and M2g (scripted) kept, each turn it red. Receiver and source are M7V-105's: node 1 holds neither on the spine | sim | — |
| M7V-103 | `m7v_103_restart_leaves_every_other_nodes_modules_as_they_were` | V-R35: the rebuild is the restarted node's only | as M7V-102; every other node fingerprinted (A1, armed timers, and per partition F1, T1, R1 receiver and primary, P1, L1, adopted) after the crash and before the restart, then straight after it | each fingerprint holds a receiver (precondition); every fingerprint is unchanged; every other node's boot is still 1. Green on HEAD by construction; mutant m2 (every node rebuilt) turns it red | sim | — |
| M7V-104 | `m7v_104_a_restarted_node_relearns_the_committed_root_through_its_control_watch` | V-R35: a restarted node re-learns its state through the normal paths only: the control watch on the committed root, and the reopened engine | as M7V-102, then the run continues to 6 000 | the run reaches its deadline; node 1 records `RecoveredLanded` for partition 1 under boot 2 after the restart; its fresh R1 holds the primary at `(Generation(2), Seq(2))`, and the engine kept generation 2 through `Seq(2)`; T1 and L1 rebuilt instances; F1 is `Idle`; A1 holds no grant; both adopted triples are the default. Red on HEAD `547c82c` (no landing after the restart); mutants m0 and m4 (root not re-read) turn it red | sim | — |
| M7V-105 | `m7v_105_restart_forgets_a_running_catch_up_and_every_r1_table` | V-R35, tester G1: every R1 per-node table is process memory | the catch-up fixture (a copy of `tests/dispatch.rs` `running_catch_up`): node 2 serves copy 1 and runs R1's source catching copy 2 up for F1 on node 1; node 2 crashes and restarts under boot 2 | preconditions: node 2 holds a receiver, a source for copy 2, an armed retransmit timer, and the dispatcher's catch-up asker for it. After the restart all four are gone. Kills M2a (`sources` kept), M2b (`retransmits` kept), M2d (`receivers` kept) and M2h (`catch_ups` kept) | sim | — |
| M7V-106 | `m7v_106_a_restarted_node_rereads_the_newest_root_even_when_it_is_dropped` | V-R35, tester G3: a restart re-reads each partition's newest root, and a newest root that drops the node teaches it no role | the spine; node 1 crashes; a later root for partition 1 (generation 3, one revision on, configuration without node 1, node 2 primary) is carried out as F1 on node 3 would emit it; node 1 restarts; the run continues to 6 000 | node 2 lands the later root (precondition); node 1 lands nothing for partition 1 after the restart, and holds no R1 primary or receiver and no T1 instance for it. Red before the fix: node 1 re-read the superseded root that named it (`ev/r2/red-g3.log`). Mutant M4 (membership check dropped) turns it red | sim | — |
| M7V-107 | `m7v_107_restarting_a_node_no_root_names_rereads_nothing` | V-R35, tester M4: a restart re-reads only a root that names the node | the spine with a fifth node in the cluster that no configuration names; it crashes and restarts; the run continues to 6 000 | no root names node 5 (precondition); node 5 lands nothing and builds no receiver. Green on the round-1 code; mutant M4 turns it red (node 5 landed at 2 963) | sim | — |
| M7V-108 | `m7v_108_each_restart_rereads_the_root_once` | V-R35, tester M5: each restart re-reads the root once | (a) the spine; crash, restart, run to 6 000, crash under boot 2, restart under boot 3, run to 9 000. (b) the spine; crash, restart, crash, restart with no run between, then run to 6 000 | (a) one landing under boot 2 after the first restart, one under boot 3 after the second, and the primary ends at `(Generation(2), Seq(2))`. (b) one landing, under boot 3. Green on the round-1 code; mutant M5 (no in-flight check) turns (b) red with two landings | sim | — |
| M7V-109 | `m7v_109_a_drained_epoch_stays_revoked_across_a_restart` | lead ledger L-R178e (Gautam 2026-09-27), the hole walk: a durable revocation reaches the restarted A1 before anything else does | the spine; `RevokeEpochRequested{p2, e1}` on node 1, run to 3 200; `partitions/2` written `FencingDrained`; crash, restart under boot 2; the old grant removed by the scenario (bypassing the service: the row is about what a new-boot grant meets); `AcquireDue` under boot 2; run to 6 000 | precondition: the revocation is durable and the old process fenced p2; the new process holds a boot-2 grant and its reload installed `(p2, e1)`. Then `may_admit(p2@e1) = Deny(EpochRevoked)` and `adopted(1, p2)` is the zero triple; the restarted A1's set holds `(p2, e1)`. Red on `HEAD` 56f952d: `(Admit, Adopted{g1, e1, c1})` | sim | — |
| M7V-110 | `m7v_110_a_restarted_node_reacquires_once_the_service_clears_its_old_grant` | Gautam's option A (2026-09-27): the scenario grant service clears a restarted node's old grant by exact-revision delete once all three guards hold, and A1's existing retry acquires | the spine; crash, restart under boot 2, `AcquireDue` under boot 2; the service (`rdb_sim::sim::grant_service`, clock of node 0) called at `E_old + ε + δ`, then one tick later | at the threshold: `Refused(NotProvenExpired)` and node 1 not held; one tick later: `Cleared(rev)`; after two renewal intervals node 1 holds a boot-2 grant, `(p2, e1)` admits and is adopted. Red with the delete stubbed out: node 1 never re-acquires | sim | — |
| M7V-111 | `m7v_111_the_service_never_clears_a_frozen_grant` | option A guard 1: a frozen record is the takeover's | as M7V-110, with the old record frozen at its exact revision after the restart; the service called 1 000 ms past the proof | `Refused(Frozen)`; the record is still boot 1, frozen, at its revision; node 1 does not acquire. Kills the guard-1 mutant | sim | — |
| M7V-112 | `m7v_112_the_service_never_clears_a_grant_not_yet_proven_expired` | option A guard 2: the `ExpiryProven` inequality, strict | as M7V-110, the service called at exactly `E_old + ε + δ` | `Refused(NotProvenExpired)`; the record at its revision; node 1 does not acquire. Kills the guard-2 and δ-dropped mutants | sim | — |
| M7V-113 | `m7v_113_the_service_never_clears_a_grant_while_a_partition_is_mid_transfer` | option A guard 3: a partition naming the node and not `Serving` is the transfer's | as M7V-110, with `partitions/2` (owner node 1) moved to `Fencing`; the service called 1 000 ms past the proof | `Refused(PartitionInTransfer(p2))`; the record at its revision; node 1 does not acquire. Kills the guard-3 mutant | sim | — |
| M7V-114 | `m7v_114_a_down_node_is_not_stepped_and_what_reaches_it_is_dropped` | lead ruling 2026-09-28 (a crash kills the process and everything it owned), rule 1, tester G5: a down node is not stepped | the spine; node 1 crashes; no restart; run to 6 000 | the run reaches its deadline (a down node stops nothing); no `ModuleDispatch` on node 1 after the crash; `Dispatcher::dropped()` is non-empty and every entry for node 1 is `Event{.., NodeDown}` under boot 1; node 1 is still down. Red on `HEAD` 4f4a2c3: the run stops `Refused{deliver::crash}` because node 1 was stepped. Kills the no-down-check mutant | sim | — |
| M7V-115 | `m7v_115_deliver_never_moves_a_nodes_boot_back` | rule 2, F-D: `Dispatcher::deliver` never rolls a node's boot back | the spine; crash, restart under boot 2; `deliver(node 1, boot 1, [Timer Arm TimerId(0x0115)])` | `Ok`; node 1's boot is still 2; the timer is not armed; exactly one `Dropped::Effects{node 1, boot 1, StaleBoot{current: 2}}` was added. Red on `HEAD`: the boot moved back to 1. Kills the stale-effects mutant | sim | — |
| M7V-116 | `m7v_116_an_old_boots_timer_fire_never_reaches_the_process_that_reused_its_version` | rule 2 and rule 4, F-G: a stale old-boot timer fire whose version equals a fresh arm's never reaches the fresh A1 | `restarted_with_old_grant`; run to 3 002; read the fresh `Acquire` arm `(version, due)`; queue a boot-1 `TimerFired{Acquire, version, due}` ten ticks later; run to `due - 1`, then `due + 1` | before `due`: the stale fire is dropped `StaleBoot{current: 2}`, never dispatched, no grant CAS is sent, the fresh arm is still armed; after `due`: exactly one grant CAS. Red on `HEAD`: the stale fire was acted on (2 grant CASes before `due`). Kills the no-stale-boot mutant | sim | — |
| M7V-117 | `m7v_117_an_old_boots_control_answer_never_reaches_the_process_that_reused_its_request_id` | rule 2 and rule 4, request ids repeat after a restart: a stale old-boot control answer whose `ControlRequestId` equals the fresh outstanding request never reaches the fresh A1 | the spine; crash, restart under boot 2; `AcquireDue` under boot 2 at 3 001; read the fresh `Acquire` arm; `DelayCompletion{node 1, 5 ms}`; run to `due` and read the outstanding id `BASE + n`; precondition `n <= ` the old process's grant-CAS count (so it issued that id too; observed n = 4 of 6); queue a boot-1 `CasResult{that id, Committed}` at `due + 2`; run to `due + 10` | the stale answer is dropped `StaleBoot{current: 2}` and never dispatched; A1 is not held and has no acquisition outstanding; the store's grant record is still boot 1's. Red on `HEAD`: A1 took the stale answer as its own. Kills the no-stale-boot mutant | sim | — |
| M7V-118 | `m7v_118_a_crash_ends_the_old_processs_watches_and_a_new_one_watches_only_once_it_asks` | rule 3, F-E: the old process's control-store watches end at crash; a restarted node watches only once it asks | the spine (node 1 watching); crash; run to 3 100; restart under boot 2; run to 3 200; the old grant removed by `scenario_cas`; `AcquireDue` under boot 2 at 3 300; run to 6 000 | open watches drop from `before > 0` to 0 at the crash; `before` `WatchTerminated{Unavailable}` events dropped `NodeDown`; `before` `ControlInteraction` Watch `Terminated{Unavailable, gap: false}`; still 0 open after the restart; after the fresh acquisition node 1 holds a boot-2 grant and watches again. Red on `HEAD`: the watches stayed open through the crash. Kills the no-end-watches mutant | sim | — |
| M7V-119 | `m7v_119_the_service_never_clears_a_grant_while_a_partition_is_fencing_drained` | option A guard 3, tester advisory ADV-1: `FencingDrained` is not `Serving`, so it is the transfer's too | as M7V-113, with `partitions/2` (owner node 1) moved to `FencingDrained` | `Refused(PartitionInTransfer(p2))`; the record at its revision; node 1 does not acquire. Green on `HEAD` by design; kills the guard-3-blocks-only-`Fencing` mutant, which M7V-113 survives | sim | — |
| M7V-120 | `m7v_120_a_nodes_boot_changes_only_by_restart` | V-R36 (lead, 2026-09-28), tester D1: a node's boot changes only by `restart`; effects and events under any other boot, newer included, are dropped | a bare runner (every node registered under boot 1); `deliver(node 2, boot 3, [Timer Arm 0x0120])`; node 1 sends node 2 a frame; a boot-3 `AcquireDue` seeded on node 2 at 10; run to 200 | the delivery is `Ok`; node 2's boot is still 1; the timer is not armed; `dropped()` is exactly one `Dropped::Effects{node 2, boot 3, UnknownBoot{current: 1}}`; after the run the only drop on node 2 is the seed, as `UnknownBoot{current: 1}`, and node 2 is stepped under boot 1 only (the frame arrives). Red on the round-0 export sources: node 2's boot became 3. Kills the newer-boot-adopted mutant | sim | — |
| M7V-121 | `m7v_121_a_crash_taken_on_another_nodes_behalf_ends_the_holders_watches` | rule 3, tester D2: every crash path ends the node's watches, including a crash taken inside another node's delivery | the spine plan, not run; node 2 watches grants; a process crash planned on node 2; node 1 delivers F1's `SyncWalThrough{copy 1, cutoff 1}`, whose holder is node 2 | precondition: the delivery is refused, node 2 is down, node 1 is not; node 2's open watches are 0. Red on the round-0 export sources: 1 (watches were ended only for the delivering node). Kills the crash-owes-no-watches mutant, with M7V-118 | sim | — |
| M7V-122 | `m7v_122_a_direct_delivery_to_a_down_node_carries_out_nothing` | rule 1, tester D4: a direct `deliver` to a down node carries out no timer, send or control effect | the spine; node 1 crashes; `deliver(node 1, boot 1, [Timer Arm 0x0122, a frame to node 2, Watch grants])` into a fresh store and scheduler; then `[Timer Arm 0x0123, Store Snapshot]` | first: `Ok`; nothing armed, nothing scheduled, no watch opened, exactly one `Dropped::Effects{node 1, boot 1, NodeDown}` holding the three effects. Second: refused `harness::dispatch::deliver::crash`, and the arm before the snapshot did not run. Red on the round-0 export sources: the timer was armed. Kills the down-node-delivers mutant | sim | — |
| M7V-123 | `m7v_123_restart_refuses_a_boot_that_is_not_newer` | V-R36, tester D3: `restart` takes only a strictly newer boot | the spine; node 1 crashes under boot 1; `restart` under boot 1, then boot 0; then boot 2; crash under boot 2; `restart` under boot 2 | boots 1 and 0: `Err(Config{restart_boot})`, node still down at boot 1; boot 2: `Ok`, boot 2; after the second crash, boot 2 again: `Err(Config{restart_boot})`. Red on the round-0 export sources: the boot-1 restart was accepted. Kills the no-boot-check mutant | sim | — |
| M7V-124 | `m7v_124_a_seed_under_a_boot_the_node_is_not_running_is_counted_as_dropped` | V-R36, tester D6: a seed under a boot the node is not running is dropped and counted like any other drop, not refused | a bare runner; `AcquireDue` seeds on node 2 under boot 0 (at 5) and boot 3 (at 6); run to 200 | the run is `Ok`; node 2's drops are exactly `[(seed@5, StaleBoot{current: 1}), (seed@6, UnknownBoot{current: 1})]`; neither is dispatched. Red on the round-0 export sources: the boot-3 seed was stepped. Kills the newer-boot-adopted mutant | sim | — |
| M7V-125 | `m7v_125_restart_refuses_a_node_the_cluster_never_registered` | V-R36, tester D7 (probe `tsf_r1_q4`): `restart` refuses a node the cluster never registered, which has no boot to be newer than | a bare runner; node 9 unregistered; it reads `grants/9` with the answer delayed 100 ms, then crashes under boot 1; `restart` under boots 0, 1, 2 and 5; run to 400 | every restart: `Err(Config{restart_node})`, node 9 still down with no boot; after the run nothing is dispatched on node 9, and the delayed control answer is dropped as `NodeDown`. Red on the export basis `c676f8a`: `restart(node 9, boot 0)` returned `Ok`. Kills the refuse-boot-0-only mutant (boot 1 then returns `Ok`) | sim | — |
| M7V-126 | `m7v_126_a_down_nodes_drops_are_node_down_whatever_boot_they_name` | rule 1, tester-sim-fidelity note on `deliver_while_down`: on a down node `NodeDown` ranks above `StaleBoot` and `UnknownBoot`, as it does for events in `Dispatcher::drop_if_dead` | the spine; node 1 crashes under boot 1; `deliver(node 1, boot, [Timer Arm 0x0126, a frame to node 2])` for boots 0, 1 and 2 | each delivery is `Ok` and records exactly `Dropped::Effects{node 1, boot, NodeDown, effects}`, keeping the boot it named; no timer armed, nothing scheduled, boot still 1. Pins a documented choice, so green at first run. Kills the boot-reason-first mutant (boot 0 then records `StaleBoot{current: 1}`) | sim | — |

---

## 6. G1: the reducer (M7V-20..M7V-23, M7V-48..M7V-51)

Written **after** re-reading `design.md` §4.4 and the critic's **F21**. F21 is load-bearing and is
the reason M7V-20 and M7V-23 read as they do:

> `faults` inside signature **equality** makes ddmin reject almost every useful candidate. ddmin's
> whole job is to delete ops; ops are what emit `fault_injected{boundary}`; so a useful minimization
> almost always drops boundaries from the set. Acceptance predicate = the **core tuple**
> `(checker, rule, partition, role, event_kind)`. `faults` is **recorded and reported**: when
> `faults_after != faults_before` the run writes `slipped: true` into `rdb-m7-campaign.json` and
> names both sets. The robust slippage defence is the `.orig.json` companion, which `regressions.rs`
> replays.

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7V-20 | `reducer_keeps_the_core_signature_and_shrinks` | charter G1 and spike §5 G1: "a seeded failure keeps its signature after shrinking" — restated per F21 so the two halves are not in tension | a scenario with one injected known violation, ~40 ops, 8 active fault boundaries | (1) `core_tuple(signature_after) == core_tuple(signature_before)`, where the core tuple is `(checker, rule, partition, role, event_kind)` and **excludes `faults`**; (2) `ops_after.len() < ops_before.len()` strictly, and materially — the row records the ratio and fails if nothing was removed; (3) `.orig.json` is written and replays and still fails. `faults` is compared and **reported**, never used as the acceptance predicate | sim | I1 |
| M7V-21 | `minimized_fixture_replays_through_i1_and_fails_the_same_checker` | charter G1: "the minimized trace replays through I1 and fails the same checker"; spike §5 G1 "minimized trace explicitly replays" | the fixture M7V-20 produced, loaded from disk as JSON (not from memory — the round trip is part of the claim) | replaying it through the I1 runner yields a trace whose oracle report is `Violated` on the **same** checker with the same `rule`; replay is deterministic across two runs (equal `oracle_checkpoint_digest`) | sim | I1 |
| M7V-22 | `skipped_op_emits_op_skipped_and_invents_no_event` | `design.md` §4.3: the environment ignores an op whose referent no longer exists, and that is a **reducer artifact, not a fault** (critic F17) | a scenario whose `ClientOp::Retry` refers to a `Submit` the reducer deleted | exactly one `op_skipped{scenario_op_index, reason=ReferentGone}` event; **no** `fault_injected` event is emitted for it; no `BoundaryId` cell count changes; `BoundaryId` has no `op_skipped` member (asserted against the enum, VA-6) | sim | I1 |
| M7V-23 | `two_defect_scenario_records_slipped_and_the_original_still_fails` | critic F4's slippage scenario, closed the F21 way | a scenario carrying **two independent** injected defects that share the core tuple `(INV-LIN, predecessor_digest_mismatch, partition 0, Primary, batch_apply)` — one on a recovery path, one on a duplicate-delivery path | the reducer still shrinks (core-tuple acceptance); `faults_after != faults_before`, so the artifact records `slipped: true` and names **both** fault sets; `.orig.json` is written, replays, and **fails**. The row's real claim: after "fixing" the path the minimized fixture reproduces, the `.orig.json` row is still red — assert that by replaying `.orig.json` against a checker configuration in which the minimized fixture passes | sim | I1 |
| M7V-48 | `reducer_stops_at_each_of_the_three_shrink_budgets` | charter DO-NOT "no unbounded search"; critic F11 — a per-failure cap is not a bound on a run | three sub-cases, each with the other two budgets set high: `SPIKE_SHRINK_STEPS=5`, `SPIKE_SHRINK_MAX_FAILURES=1` (with 3 distinct signatures failing), `SPIKE_SHRINK_BUDGET_TOTAL=10` | each stops at its own bound; the reducer emits its **best candidate so far** and the artifact says the budget was spent (`budget_spent` names which one); unshrunk signatures are recorded unminimized rather than dropped | sim | I1 |
| M7V-49 | `reducer_edits_only_the_scenario_never_a_trace` | D3, the load-bearing claim: causality survives because the kernel regenerates the trace (`design.md` §4.3) | the source of `tests/support/scenarios/reduce.rs` | no function takes `&mut [TraceEvent]`, `&mut Trace` or returns a `Trace` it constructed; the only executor is the I1 runner (VA-5). A source-level row, like M7V-01, because the property is "this code does not exist" and a behavioural test cannot prove absence | unit | none |
| M7V-50 | `regressions_replay_every_minimized_and_original_fixture` | critic F4's committed-original half; charter "the minimized trace replays"; **no expectation edit can make it green (critic T-16)** | every file in `tests/fixtures/regressions/` | for each `<slug>.json` there is a `<slug>.orig.json` and **both** are replayed; each **fails** its recorded `(checker, rule)`. The only expectation a fixture may carry is `fails`; the row asserts no fixture carries `passes` or any other expectation, and a fixture that replays clean fails the row. **Retirement is a deletion, not an edit:** when the defect is fixed, both files of the pair are deleted together and one line is added to ADR-rdb-0019's Notes naming the slug and the fix; the row asserts pairing (an orphan `.json` with no `.orig.json`, or the reverse, fails) so a half-deleted pair is caught | sim | I1 |
| M7V-51 | `shrink_ms_is_reported_separately_from_wall_ms` | critic F11: reducer time is not campaign time, and folding them hides both. **No duration is asserted non-zero (critic T-10, hard rule 1)** | M7V-64's run (one injected failing seed, shrinking enabled) — shared, not a second corpus (§2 aggregate budget) | `rdb-m7-campaign.json` `values` carries `wall_ms` and `shrink_ms` as **distinct keys, both present** (`0` when nothing shrank, §14 Q-5); `wall_ms` **excludes** shrink time — asserted through the instrumentation (the campaign timer is stopped before the reducer's timer starts, checked by a probe on the two spans), never by comparing values; **a shrink occurred** — evidenced by `shrink_step` count > 0 and one `shrink_result` line in the log, not by a duration being non-zero; `compile_ms_excluded` is present (spike §7 requires compilation reported separately) | campaign | I1 + testkit |
| M7V-83 | `budget_has_no_event_stream_index_and_heal_is_only_a_network_op` | guard row for critic **F5** (`heal_at_event` removed; healing is `NetworkOp::Heal` so ddmin moves it with the op list), which had no row that would fail if reverted (critic T-15) | the source of `tests/support/scenarios/{mod,grammar}.rs` and the `Budget` type | `Budget` has exactly the fields `max_events` and `max_ticks` (asserted by constructing it with struct-update syntax from a two-field literal, which fails to compile if a field is added, plus a source check that no field name contains `event` other than `max_events`); the token `Heal` appears in the grammar **only** as a `NetworkOp` variant; `schedule_phase{Healed}` in a generated trace is always preceded by a `NetworkOp::Heal` op at a `scenario_op_index` (checked on 50 seeds through the generator alone — the op list, no runner). A `heal_at_event: usize` added back to `Budget` fails the first clause | unit | none |
| M7V-84 | `reducer_only_removes_ops_never_constructs_or_modifies_one` | guard row for critic **F12** (no per-op field-simplification pass), which had no row that would fail if reverted (critic T-15); M7V-49 does not cover it because a field-shrinking pass touches no `Trace` type | the source of `tests/support/scenarios/reduce.rs`, and a behavioural check | source: no function in `reduce.rs` returns a `ScenarioOp`, takes `&mut ScenarioOp`, or constructs a `ScenarioOp` literal or calls a `ScenarioOp` constructor — the only permitted operation on `Vec<ScenarioOp>` is removal (`retain`, `remove`, `drain`, slicing); behavioural: for every accepted candidate in a 200-step reduction of a 40-op scenario, every op in the candidate is **byte-identical** to an op in the parent at the same or an earlier index (candidate ops form a subsequence of the parent's). A pass that rewrites `Advance{ticks}` fails both clauses | unit | none |
| M7V-85 | `without_rule_has_exactly_one_call_site` | the per-rule suppression `Report::without_rule(&str)` that M7V-23 needs is an assertion-lowering surface; bounding it to one caller (critic T-16; §14 Q-1's default) | every `.rs` file under `crates/rdb-sim/tests/` | the token `without_rule(` occurs in exactly two places: its definition in `support/oracle/mod.rs` and one call in M7V-23's test function (string match on the enclosing `fn m7v_23_` name, the mechanism §13 uses); it is defined on `Report`, never on a checker; a third occurrence fails the row | unit | none |

---

## 7. Q1: the campaign, coverage and the honest-green machinery (M7V-52..M7V-65)

| ID | Name | Proves | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|
| M7V-52 | `campaign_reports_a_status_for_every_invariant` | ADR-rdb-0019 §2: "an invariant is `proven`, `unavailable` or `violated` — never silently absent"; design §2.4's per-run fold (V-R16) | the shared default corpus (`SPIKE_SEEDS=64`, §2) | the status table and the artifact both carry a row for **all ten** invariant ids with `status`, `reason` and `seeds_armed`; the id list is enumerated from the checker registry, so a checker added without a status row fails; zero `violated`. The fold order is asserted on a synthetic per-seed verdict set (VA-2, design §2.4): any seed `Violated` → `violated`; else any `Capability(p)` → `unavailable(capability p)`; else any seed `Proven` → `proven`; else `unavailable(not_armed)`. **The synthetic set includes the healed-then-exhausted case (critic T-24, V-R20 (6)):** two seeds on which INV-LIVE armed on `schedule_phase{Healed}` and then disarmed on budget exhaustion (per-seed verdict `NotArmed`, `armed() == false`) fold to `unavailable(not_armed)` with `seeds_armed = 0` — a fold that reads `armed()` mid-run, or counts a disarmed seed, reports `proven`/`2` here and fails the row. `seeds_armed` counts per-seed `Proven` and nothing else | campaign | I1 |
| M7V-78 | `proven_status_implies_seeds_armed_positive_for_every_invariant` | **critic T-01, ruling V-R16:** a campaign whose corpus armed nothing must not report `proven`; `seeds_armed` is the load-bearing field, and this is the row that makes it so. The real mechanism behind the planner's R-3 | the shared default corpus report (§2); plus one synthetic report | for **all ten** invariants: `status == "proven"` implies `seeds_armed > 0`, and `seeds_armed == 0` implies `status` is `unavailable(not_armed)` (or `unavailable(capability)` / `violated`); the assertion is over the enumerated checker list, not a hand list. This `proven ⇒ armed` clause has **no** exclusion — a `proven` INV-VER with `seeds_armed = 0` is a runner bug like any other; the *wired ⇒ armed* clause is M7V-89's and runs over **nine** invariants, INV-VER excluded by name until a producing `ScenarioOp` or `BoundaryId` exists (ruling V-R21; design §2.4; ADR-rdb-0019 rule 1). The synthetic half feeds the runner a report with `proven` and `seeds_armed = 0` and asserts the run **fails naming the invariant** in every run and under every setting of `SPIKE_REQUIRE_ALL` (design §2.4: the fold cannot produce it, so it is a runner bug). Recorded alongside: `seeds_armed` per invariant in `rdb-m7-campaign.json`, so a reviewer sees *how often* each checker armed, not only that it did | campaign | I1 |
| M7V-89 | `every_fully_wired_invariant_arms_on_the_default_corpus` | **critic T-28, ruling V-R20 (3):** M7V-78 holds vacuously on a corpus that arms nothing, so a generator that stops producing recoveries or pauses turns INV-LOSS or INV-LAG `not_armed` inside a green handoff gate. Under V-R19 the schedule is a function of the seed index, so "wired ⇒ armed on the default corpus" is deterministic, not probabilistic | the shared default corpus report (§2) and this run's `capability` report; per checker, the package list it needs (the registry's `needs: &[PackageId]`, design §2.4) and the scheduled op that arms it. **Design §2.4's wired-clause list is the single source of that mapping; this cell restates it verbatim and is not a second list** (critic T-38 — the round-3 text said INV-ATOM/INV-LIN/INV-DEDUP arm on "any `ClientOp::Submit` (every seed)", which is false for INV-DEDUP: a *first* submit never arms it, so the failure message pointed the developer at something every seed has, and INV-PUB and INV-AUTH were attributed to a publish and a grant decision rather than to the submit). Verbatim from design §2.4: *INV-LOSS a `LoneSurvivorChoice` or `UnequalSecondaryPrefix` boundary; INV-LAG a regular secondary partitioned then `TimeOp::Advance` past the pause threshold; INV-DEDUP the `RetainedDedupHit` boundary; INV-LIVE and INV-ISO a `NetworkOp::Heal`; INV-ATOM, INV-PUB, INV-AUTH and INV-LIN the first `Submit`*; **INV-VER ← excluded by name (ruling V-R21; design §2.4 excluded set; ADR-rdb-0019 rule 1)**: no `ScenarioOp` injects an unknown mandatory version and `BoundaryId` has no such member, so nothing in the grammar can arm it, and the row lists it as `excluded (no producing op)`, never as passing, until a producing `ScenarioOp` or `BoundaryId` exists; and the boundary-keyed arms (`RetainedDedupHit`, `LoneSurvivorChoice`, `UnequalSecondaryPrefix`) are scheduled by `REQUIRED[i mod 29]` (V-R19) on a **known subset** of the corpus — two or three of the 64 default seeds each — not on every seed, so the expected `seeds_armed` for those invariants is small and positive, never 64, and the failure names the seeds that were *scheduled* to produce the op rather than the whole corpus. `NetworkOp::Heal` is in every seed's tail (M7V-83), so INV-LIVE/INV-ISO are the wide case. The producer table (`gen.rs`, M7V-42) maps each op to its seed indices; if that table and design §2.4's list disagree the row **fails naming both** rather than preferring either — one list, one owner (T-38's closure) | for every one of the **nine** invariants in the clause (all ten minus the excluded set, which is INV-VER only today — V-R21) whose `needs` packages **all** report `Wired` in this run, `seeds_armed > 0` in the shared report; the excluded set is a named const beside the registry (`WIRED_IMPLIES_ARMED_EXCLUDED: &[Invariant]`), and the row prints each excluded invariant with its reason so the exclusion is visible in every run; the failure names the invariant, its arming op and the seeds that were scheduled to produce it. The row prints the covered list; **during M7 that list is empty** (no kernel package is wired) and the row reports `unavailable (no invariant fully wired)` — never a pass. Synthetic half: a report with every package `Wired` and one invariant at `seeds_armed = 0`, `status = unavailable(not_armed)` fails naming it. Together with M7V-78 (`proven ⇒ armed`) this makes the handoff gate detect a never-arming generator once a package lands, which is the reach R-9 lacked | campaign | I1 |
| M7V-53 | `unwired_capability_reports_unavailable_never_proven` | charter Q1: "until [kernel packages] land, the runner reports explicit `Unavailable` for unwired capabilities, never a pass" | a corpus run whose dispatcher report carries `capability{package=P1, state=Unavailable}` (during M7 the shared corpus, since P1 is genuinely unwired; after P1 lands, one small corpus with P1's module stubbed to answer `Unavailable` through the same report path, M7V-82) | every invariant that reads an event only P1 emits reports `unavailable` with `reason = capability(P1)` in `rdb-m7-campaign.json`; the binary may still exit 0 (so `scripts/gate.sh` is green during M7) but **no** P1-dependent invariant reports `proven`, and stdout prints the unavailable list with reasons. This row plus M7V-54 and M7V-78 is the whole answer to "green run, honest artifact" | campaign | I1 + C0 capability event (the input is the shared corpus, critic T-31) |
| M7V-54 | `spike_require_all_fails_the_gate_on_any_not_proven` | ADR-rdb-0019 §2's gate mechanic, ADR-0031's `full_scale:false` pattern applied to capability; VA-9 command 3 is the only command that sets it (V-R18) | the gate-check **function** applied to the shared corpus report with `SPIKE_REQUIRE_ALL=1` (no second corpus, §2); plus synthetic reports | the check **fails**, and its failure list names every invariant that is not `proven` **with its reason** (`capability(p)` or `not_armed`) **and every `proven` row whose `seeds_armed == 0`** (V-R16 — the list is a union of the two conditions, asserted on a synthetic report that has both). A report in which all ten are `proven` with `seeds_armed > 0` passes under the same variable. Without this row `Unavailable` is a comment, not a gate | campaign | C0 |
| M7V-55 | `default_corpus_and_authored_cases_hit_every_required_cell` | spike §7 coverage: "every required fault boundary exercised"; `design.md` §6's three axes; **coverage is a property of the seed list, not of luck (critic T-12, ruling V-R19)** | the shared default corpus (64 seeds ≥ N = 29, so every `REQUIRED[i mod N]` is scheduled at least twice and the run is `coverage_gated: true`, VA-6, V-R20 (7)) plus the four authored cases | (1) **the schedule covers the required set:** the set `{REQUIRED[i mod N] : i in the seed list}` equals the `REQUIRED` set — asserted from the seed list alone, before any run; (2) **observed counts:** every required cell on all three axes has `count >= 1` in the run, and `required_missing[]` is **empty** — except cells whose gating package (`BoundaryId -> PackageId` table, VA-6, keyed per family on the emitting provider, V-R20 (4)) reported `Unavailable`, which appear in `coverage_unavailable` and in the artifact as `unavailable(H1)` / `unavailable(M1)` / `unavailable(<family's provider>)`, **not** in `required_missing[]` and **not** deleted from `REQUIRED`; (3) the row fails if a cell is both scheduled and `missing` with its package `Wired` (the generator's producer table is stale — see M7V-42); (4) **the family map is checked against the trace (critic T-40, design §3.1):** for **every** observed `fault_injected` event, its `fault_kind` equals the family the gating table assigns to its `boundary` — the emitting provider is the ground truth C0 does not carry, so a member filed under the wrong family fails here instead of silently inheriting the wrong gating package and being excluded when that package is `Unavailable`. A run that observes no `fault_injected` at all reports this clause `unavailable(not_armed)` rather than passing it. Named cells that must be hit and are the ones most likely to be missed: `derived_quorum_rule × DegradedRf2` (critic F1; the rule is **derived** from `required_copy_set.len()`, V-R20 (1), so the cell is named as such and keyed on the derived value), `replication_ack.reject_reason × ForgedIdentity` (F16, hook-gated on H1), `FalseDurableWatermark` (F6, hook-gated on M1), the isolation cell (V-R8), and the four named cross-package cells | campaign | I1 |
| M7V-56 | `coverage_required_lists_are_enumerated_from_their_enums` | VA-6; the M6-107/TA-63 pattern — a missing case must fail, not pass quietly; V-R19 answers Q-4 with set **equality** | `coverage.rs`'s required lists, the `BoundaryId -> PackageId` gating table and `gen.rs`'s producer table vs the enums they count — **the enums as re-read at `f616ddf`** (critic T-23; re-read 2026-09-21 after CB-3): `AckRejectReason` (**14**, `contracts/trace.rs:315` — widened from 7 by CB-3/B-R33 Q-B-8, the seven added at `trace.rs:331..343`; the cells are §15.1), `BoundaryId` (29, `contracts/trace.rs:505`, untouched by `f616ddf`), `RecoveryMode` (3, `contracts/trace.rs:432`), `ProtectionPhase` (4, `contracts/trace.rs:454`), `ReplicaRole` (3, `contracts/ids.rs`, enum `ReplicaRole` — **not** `trace.rs`), `ErrorKind` (18, `contracts/errors.rs:79`, untouched by `f616ddf`) | every variant of `AckRejectReason`, `BoundaryId`, `RecoveryMode`, `ProtectionPhase` and `ReplicaRole` has a cell; a variant added without one fails this row. **`ACK_REJECT_REASONS` is fourteen wide, not seven** (CB-3, `f616ddf`); the seven added cells and their covering rows are **§15.1**, and three of the seven are recorded there as **gaps with owners**, not as cells — a gap keeps this row red, which is the correct state, and is not closed by writing an empty cell. **Every literal naming `StaleGeneration` or `NotAMember` is enum-qualified**: both names also exist on `envelope::AppendReject` (`envelope.rs:534`, `:568`) with a different meaning (there: why a replica refused an append; here: why an acknowledgement does not count), foundation kept both deliberately, and a list keyed on a bare name would conflate them. **`ErrorKind` is wider than the admission axis**, so the admission-reason axis is the const `ADMISSION_REASONS: &[ErrorKind]` in `coverage.rs`, asserted (a) to be a subset of `ErrorKind` by construction and (b) to contain the four variants rows pin — `ProtectionPaused` (M7V-39), `RequestIdReuse`, `CrossAffinity`, `GenerationChanged` (M7V-25); a `client_outcome` axis over the full `ErrorKind` is **reported, not required**. **The derived quorum rule** (V-R20 (1)) is a two-member axis `{Rf3, DegradedRf2}` computed from `required_copy_set.len()`, declared as its own enum in `coverage.rs` and enumerated like the rest — there is no `QuorumRule` in `rdb-core` and none is asked for. `REQUIRED`'s member set **equals** `BoundaryId`'s variant set exactly — the 29 members foundation declared, re-read at `f616ddf` (`crates/rdb-core/src/contracts/trace.rs:505`), no more, no fewer (critic F17, V-R19); the gating table and the producer table each have exactly one entry per member, and **every member of one `FaultKind` family maps to the same package** in the gating table (V-R20 (4), critic T-35). `PackageId` has ten variants and every one appears in the capability report M7V-82 checks | unit | C0 |
| M7V-57 | `a_required_cell_with_zero_hits_fails_the_run` | ADR-rdb-0019 §2: "coverage is counted cells, never a percentage; a named required cell with zero hits fails the run" | a synthetic coverage record at `seeds = N` (so `coverage_gated: true`, V-R20 (7)) that omits one required cell whose gating package is `Wired`; a second that omits one whose package is `Unavailable`; and a third, the first record again at `seeds = N - 1` | the first **fails**, names the cell and its axis, writes exactly one `coverage_shortfall{axis, cell}` line (and **no** `coverage_cell` line for that cell, VA-7) and `required_missing[]` to the artifact; the second does **not** fail on that cell, writes `coverage_unavailable{axis, cell, package}` and leaves `required_missing[]` empty; the third does **not** fail either — it writes `coverage_gated: false` and the same `coverage_shortfall` line (the shortfall is recorded, the gate is not applied), so a gate that fires on a sub-N corpus fails this row (critic T-25). The negative control for M7V-55, all three branches; the gate condition is `seeds >= N`, exercised at N and N − 1 | unit | none |
| M7V-58 | `campaign_result_is_independent_of_thread_count` | `design.md` §5.2 rule 3: results merged deterministically, so the report does not depend on `available_parallelism()` | the same corpus at 1, 2 and N threads, at `SPIKE_SEEDS=8` — the smallest count that produces a non-trivial merge (more seeds than threads, at least two per chunk; critic T-11); `coverage_gated: false` (8 < N, V-R20 (7)) | identical per-invariant statuses, reasons and `seeds_armed`, identical coverage counts, identical failing-seed list and identical signature slugs; `coverage_gated` is `false` on all three and none fails on `required_missing`. Only `wall_ms` differs. A campaign whose verdict moves with host load is not evidence | campaign | I1 |
| M7V-59 | `seed_base_zero_makes_the_extended_corpus_a_superset` | `design.md` §5.1: a PR failure must reproduce in the extended run | the seed list at `SPIKE_SEEDS=64` and at `SPIKE_SEEDS=256`, both at `SPIKE_SEED_BASE=0` | the smaller list is a prefix of the larger. Cheap, and it is the property the whole layered-budget scheme rests on | unit | none |
| M7V-60 | `campaign_records_wall_ms_and_asserts_no_threshold_in_the_pr_default` | V-R11; `test-plan-m6.md` §7's rule ("assert invariants, record numbers, never a threshold"); AGENTS.md's `m4_69` lesson | default corpus with `SPIKE_ASSERT_WALL_MS` unset | `wall_ms`, `host`, `build` and `profile` are recorded; the row asserts **no** wall-time threshold and passes on an arbitrarily slow host. A threshold assertion appearing in the PR default is a defect this row must catch (assert that the runner's threshold path is not taken) | campaign | I1 + testkit |
| M7V-61 | `campaign_asserts_wall_ms_only_when_spike_assert_wall_ms_is_set` | V-R11's other half: the extended gate does assert | two runs of a 4-seed corpus (§2), `coverage_gated: false` (4 < N, V-R20 (7)): `SPIKE_ASSERT_WALL_MS` unset, then set to **`0`** — a value no host can beat, so the row is host-independent (critic's V-R11 check; "a deliberately slow corpus" was host-dependent) | unset → passes and records; set to `0` → **fails**, printing observed vs configured, and the failure is the wall-time one — never `required_missing`. This is the only place in the plan where time is asserted, and it is asserted against a bound the run cannot meet by construction | campaign | I1 |
| M7V-62 | `the_release_command_is_the_only_source_of_the_sixty_second_number` | critic F10 / V-R11; **two artifacts, one per profile (critic T-13, ruling V-R17)**; restated per critic **T-32** so the row never asserts on a file another command wrote | the name selector `artifact_name() -> &'static str` in `tests/campaign/report.rs`; **this run's** artifact (the shared corpus report's, §2); the `RELEASE_GATE_COMMAND` and target-dir consts | (1) **the selector is a pure function of the build profile:** `artifact_name()` returns the `write_evidence` stem, so the file it names is `rdb-m7-campaign.json` under `cfg!(debug_assertions)` and `rdb-m7-campaign-release.json` otherwise, asserted in both directions by calling it in the binary the row runs in and comparing against `cfg!(debug_assertions)`; (2) **this run's artifact** carries `profile` equal to the build profile the binary was compiled under (the same field M7V-72 checks) and, under `profile: "release"` only, `full_scale: true`; a `wall_ms` under `profile: "debug"` is never the 1,000-history figure, and `profile` is the discriminator that makes that checkable; (3) `.rtargets/campaign` is the documented target dir for commands 2 and 3 (a doc/const cross-check against VA-9's rows `\| 2 \|` and `\| 3 \|`, not a filesystem probe). **The row never reads the other profile's file:** a tracked `rdb-m7-campaign.json` left by whichever command-1 run last ran is exactly the stale file of unknown provenance the two-name design exists to avoid. Both halves run under either gate; nothing is `unavailable` here any more | campaign | I1 + testkit |
| M7V-63 | `campaign_never_exceeds_spike_max_events` | charter DO-NOT; spike §7 "explicitly bounded; no unbounded combinatorial search" | `SPIKE_MAX_EVENTS=128` over 16 seeds (one corpus of its own, §2; `coverage_gated: false`, 16 < N, V-R20 (7)) | no history's event count exceeds the cap; the run does not fail on `required_missing`; the runner stops at it rather than truncating a trace mid-transaction (a truncated trace must end at an event boundary, or the oracle reports `Unavailable{NotArmed}` for that seed — not `Proven`, not a violation, V-R16; the seed counts in `seeds_armed` only for checkers that armed before the cut) | campaign | I1 |
| M7V-64 | `a_failing_seed_writes_its_reproducer_under_the_test_log_dir` | V-R6; spike §7's failure artifact | one corpus with one injected failing seed (shared with M7V-51, §2; fewer than N seeds, so `coverage_gated: false`, V-R20 (7) — the run fails on the injected violation and on nothing else) | `$RETCD_TEST_LOG_DIR/validation/<run-id>/` contains the schema-versioned event stream, the original and the minimized `Scenario`, and the signature; the persisting copies land in `tests/fixtures/regressions/`; the run exits non-zero; nothing is written under `docs/evidence/` for a failed run except the artifact's own `violated` status | campaign | I1 |
| M7V-65 | `reduced_scale_changes_only_seeds_and_events` | ADR-rdb-0019 §2 and ADR-0031: "reduced scale changes repeat counts and data volume, never which code paths or failure cases are covered" | two corpora: `SPIKE_SEEDS=64` and `SPIKE_SEEDS=128` (critic T-11 — **never** the 1,000-seed full-scale corpus, which runs only under VA-9 commands 2/3 and in the extended run, critic T-31; the property holds between any two scales; both ≥ N, so both are `coverage_gated: true`) | the set of checkers that ran, the set of fault kinds reachable, the required-cell list and the set of `unavailable(package)` cells are **identical**; only `seeds`, `max_events`, `events_total` and `seeds_armed` differ, and the 64-seed list is a prefix of the 128-seed list (M7V-59). A reduced run that drops a checker is the defect that makes the cheap run stop being a regression gate for the expensive one | campaign | I1 |
| M7V-82 | `capability_state_is_derived_from_the_modules_own_report_never_a_literal` | **critic T-14b, ruling V-R18:** `capability{state}` must track reality. A hand-maintained table lets a landed package stay `Unavailable` (a false red nobody chases) or an unlanded one be flipped `Wired` early (a misreported cause). Enumerated like M7V-56 | (a) the dispatcher's report — foundation's `Dispatcher::capability_report` at `8a23b1d` (`crates/rdb-sim/src/harness/dispatch.rs`, derived by probing `step`), or `Module::capability(&self)` once K-F-10 lands, over `ModuleName::ALL`; (b) the source under `crates/rdb-sim/src/harness/` | (a) **behavioural, both directions:** the `capability` events at trace start equal, one for one over every `PackageId`, the report the dispatcher returns; a module stubbed to answer `Ok` from `step` reports `Wired`, one stubbed to answer `RdbError::Unavailable` reports `Unavailable`, both asserted positively through the real event emission path; (b) **source:** no file under `crates/rdb-sim/src/harness/` contains the token `CapabilityState::Wired` except the one that builds the report, and no `const`/`static` table of `(PackageId, CapabilityState)` exists. A landed package that stays `Unavailable`, or a literal `Wired`, fails (a); a table fails (b). Runs the dispatcher in-process with stub modules, no runner and no kernel — hence unit-class | unit | C0 + foundation dispatcher |
| M7V-87 | `m7_release_gate_is_the_cited_command_and_fails_while_any_invariant_is_not_proven` | **critic T-14a, ruling V-R18:** the M7 release gate is one command, written in ADR-rdb-0019 §2.1, and §13's last line cites it. M7V-54 tests the gate *function*; this row ties the function to the *command* and to the milestone claim, so the two cannot drift apart (a test cannot run `scripts/gate.sh`, so the command is checked as a const and its environment is applied to the shared report) | the `RELEASE_GATE_COMMAND` const in `tests/campaign/report.rs`; the shared corpus report (§2); the process environment | (1) the const equals, byte for byte, `SPIKE_REQUIRE_ALL=1 RETCD_EVIDENCE=1 CARGO_TARGET_DIR=.rtargets/campaign scripts/gate.sh test --release -p rdb-sim --test campaign` — and the row reads the same string out of `docs/ADRs/rdb/0019-validation-gates-evidence-and-release-boundary.md` §2.1 and the plan's VA-9 and asserts all three agree (a doc/const cross-check, M7V-62's mechanism). **Extraction rule (critic T-33, narrowed under T-36):** in each file the command is the **first backticked span** in the table row whose line begins `\| **M7 release gate**` (ADR §2.1) and `\| 3 \| **The M7 release gate**` (VA-9 in this plan); exactly one such row must exist per file, and anything else — §1's wrapped command 2, prose mentions, §13's checklist line, and **every `\| N \|` row of the §15 drift table** — is not read. The plan-side selector carries the row's label because the bare `\| 3 \|` prefix also matched drift-table row 3 (`\| 3 \| \`protection_state{state=…}\``), which made this row red on a correct plan and invited the reflex fix of deleting a drift row (critic T-36); the drift table keeps its numeric first column, which is load-bearing for reading `trace-requirements.md` §8 side by side. A file with zero or two matching rows fails the row rather than picking one, and the failure **prints every matching line** so the collision is visible instead of guessed at; (2) applying the command's environment (`SPIKE_REQUIRE_ALL=1`, `RETCD_EVIDENCE=1`) to the gate function over the shared report: the check **fails while any invariant is not `proven`** or any `proven` row has `seeds_armed == 0` or `full_scale` is `false`, naming each cause; a synthetic all-`proven`, all-armed, `full_scale: true` report passes; (3) **during M7** the shared report has `unavailable` rows, so clause 2 is exercised on its failing branch and the row reports the release claim `unavailable (packages A1 T1 R1 P1 L1 F1 unwired)` — never a pass; when the real command 3 is run by hand, its artifact is the evidence and clause 2 is the function it ran. No `scripts/` change is asserted or made (foundation wires it after F-R11) | campaign | I1 + testkit |

---

## 8. Mutation checks (spike §7; `design.md` §7) — M7V-66..M7V-71

Spike §7 names five mutations and requires each to be caught by a **named** test. Per ruling V-R9
they are in two classes. The honest boundary, stated once:

> A **trace rewrite** proves the oracle detects that fault class. It does not prove the kernel is
> free of it. An **injected fault** tests the strictly stronger claim — kernel plus oracle rejects
> it — and is the only way to reach the §2.5 blind spot, because it makes the kernel emit a
> self-consistent-but-wrong trace.

| ID | Name | Mutation | Class of mutation | Input | Assertion | Class | Dep |
|---|---|---|---|---|---|---|---|
| M7V-66 | `mut1_accept_stale_authority_trips_inv_auth` | MUT-1 accept stale authority | trace rewrite | a recorded good trace; flip one `authority_decision{gate=Publication}` from `Expired` to `Valid`, leave the following `publish` | INV-AUTH `Violated`; the unmutated trace is `Proven` (both halves in the row, or the row proves nothing) | unit | C0 |
| M7V-67 | `mut3_publish_before_ack_trips_inv_pub` | MUT-3 publish before ACK | trace rewrite | move one `publish` to before its `replication_ack` | INV-PUB `Violated`, `rule="required_copy_set_unsatisfied"`; unmutated trace `Proven` | unit | C0 |
| M7V-68 | `mut4_skip_ancestry_trips_inv_lin` | MUT-4 skip ancestry | trace rewrite | set one `batch_apply.predecessor_digest` to the digest recorded at `seq-2` | INV-LIN `Violated`, `rule="predecessor_digest_mismatch"`; unmutated trace `Proven` | unit | C0 |
| M7V-69 | `mut2_forged_shadow_ack_is_rejected_by_the_kernel` | MUT-2 count a shadow ACK — **the kernel half** (critic T-09: the earlier disjunction was satisfiable by the oracle alone, on a kernel that counted the forged ack) | **injected fault** (VA-4) | `NetworkOp::ForgeAck { claimed_role: RegularSecondary, claimed_node: n4 }` (landed `ReplicaRole`, convention 1) where the topology in force lists `n4` as `Shadow` | the kernel rejects it: the trace carries `replication_ack{accepted=false, reject_reason=ForgedIdentity}` (or the delivery record's rejection, per foundation's shape), the `ForgedIdentity` coverage cell is hit, **no** `publish.ack_evidence` names `n4`, **and** INV-PUB is `Proven` on the run. Spike §4: "forged identity is injectable **and rejected**" — a conjunction, no `or`. `Unavailable{Capability(H1)}` / `{Capability(R1)}` until the hook and R1 land — never a pass, listed in §12 | sim | H1 hook + R1 |
| M7V-81 | `mut2_counted_forged_ack_trips_inv_pub` | MUT-2 — **the oracle half** (critic T-09): if a kernel *did* count the forged ack, the oracle must catch it. Neither half substitutes for the other; §13 maps MUT-2 to both | trace rewrite (until H1 lands) / recorded-trace rewrite (after) | M7V-69's recorded trace — or, until H1 lands, a hand-built equivalent through `TraceBuilder` (`ack_from(n4, s)` with the role claim overridden to `RegularSecondary`, convention 4) — rewritten so the rejection is removed and the `publish.ack_evidence` **counts** `{node=n4, boot=b1, role=RegularSecondary, durability=Durable}` while the topology in force still lists `n4` as `Shadow` | INV-PUB `Violated`, `rule="ack_role_claim_mismatch"`; the unrewritten trace is `Proven` (both halves in the row). Upgraded in place from hand-built to recorded input when H1 lands (§12); the assertion does not change | unit | C0 |
| M7V-70 | `mut5_false_durable_watermark_trips_inv_pub_and_inv_loss` | MUT-5 mark buffered as durable | **injected fault** (VA-4) | `StorageOp::FalseDurable { node, through }` — a flush completion M1 never performed — then a publish counting the resulting `Durable` ack, then a host crash that loses the suffix | **Re-planned under V-R29** (the INV-LOSS clause was unsound as written; not to settle a count). INV-PUB's durability-grounding clause fires (`rule="durable_ack_ungrounded"`), **and** the §15.1 cell 3 `replication_ack.reject_reason × AckRejectReason::InconsistentProgress` cell is hit when the M1 `FalseDurable` hook exists. INV-LOSS measures `Proven` on this trace **by design**: it cannot tell a lie from a lost copy. A holder is `(node, boot)`, matched only at the same boot (`checks/loss.rs`), so a copy that returns at a new boot after a host crash may have lost it. Measured on the suffix-lost trace, log `C:/rdb_test_data/logs/sim-hooks/m70-first/`. Matching across boots (V-R26 (b)) was measured and rejected: it moves M7V-02 and M7V-03, and the lied ack stays ungrounded under it (log `.../protob/`). This is V1 clause 3 in its modelled sense: watermark bookkeeping honesty in a memory engine, explicitly **not** fsync honesty, a lying device or power loss (ADR-rdb-0019 §1 V1 `Form`). **Owed, `unavailable(M1)`; not credited on scaffolding.** The kernel half is **M7V-92**. The INV-PUB clause is asserted today by the scaffolding row `false_durable_oracle::false_durable_a_publish_counting_an_ungrounded_durable_ack_trips_inv_pub` (`tests/scenarios.rs`). **Held for the lead (dev-sim-hooks, 2026-09-27), evidence against the V-R29 wording:** the M1 hook has landed (`environment_capabilities` reports M1 `Wired`; M7V-92 drives `FalseDurable` and meets it twice). `FalseDurable` moves no watermark, so the acker's progress stays ordered and the tracker's `InconsistentProgress` check cannot fire (§15.1 cell 3). As worded, the coverage clause is unreachable and `unavailable(M1)` names a package that is wired. **2026-09-27:** unchanged in status. The recorder side of V1 clause 3 is tighter: a `ShortFlush` is now `Partial`, never `Synced` (V-R36, M7V-98), and M7V-96..M7V-101 check every recorded apply, sync and ack against engine state, so a false recorder line is killed by a row, not only by a probe | sim | M1 hook |
| M7V-92 | `m7v_92_false_durable_the_kernel_never_reports_durable_beyond_what_m1_performed` | MUT-5 — **the kernel half** (V-R25 (2)): the M7V-69/M7V-81 split applied to MUT-5 | **injected fault** (VA-4) | the rebuild spine (`tests/dispatch.rs` `rebuild_plan`) with two `StorageOp::FalseDurable { node: 3, through: 2 }`, met by F1's `SyncWalThrough` of copy 2 and by a host flush after the catch-up | both lies are told (`false_claims == [2, 2]`) and copy 2 buffered through 2, **and** the kernel never reports a durable position beyond what M1 performed: R1's `current_ack().progress.durable` ≤ the engine's durable; every recorded `durability_advance` on node 3 is at or below it, with one `Partial` per lie; F1 records `SyncWithheld{copy 2, Short{durable}}` at the real watermark and **no** `SyncProven` for copy 2. **Parked clause:** no publish counts an ungrounded ack **and** INV-PUB is `Proven` on the run. No sim run reaches P1's `Publish` (B-R60), so the row asserts the trace has **zero** `publish` lines and INV-PUB is not violated, then calls `parked(.., I1, ..)`. Un-park when a sim run publishes: the zero-publish assertion goes red that day. **2026-09-27 (V-R36, ShortFlush):** the recorder now writes a flush that made less durable than was captured as `Partial` (M7V-98, M7V-99). This row is unmoved: a `FalseDurable` flush was already `Partial`, and the row stays green on the change | sim | M1 hook + P1 publish (B-R60) |
| M7V-71 | `every_named_mutation_has_a_catching_row` | spike §7: "every such mutation must be caught by a named test"; stops a mutation being dropped when a row is renamed | **none — a completeness row over the other six, not itself a mutation** (the cell was missing entirely, so this row rendered one column short and its `Input` sat under `Class of mutation`) | the `MutationId` enum and the campaign's `mutations{}` map | every `MutationId` variant maps to at least one catching row id — MUT-2 maps to **two** (M7V-69 kernel half, M7V-81 oracle half) and the row asserts both are present — and each named row id exists in the test binary (string match on the test name, the same mechanism §13 uses); the map is written into `rdb-m7-campaign.json`. Enumerated, not hand-listed (VA-6) | unit | none |

---

## 9. Evidence rows (V-R2, V-R5; ADR-rdb-0019 §2; rEtcd ADR-0031) — M7V-72..M7V-77

Every row here writes exactly one JSON file under `docs/evidence/` through the shared
`write_evidence()` (VA-8). **No row asserts a threshold.** Each asserts correctness properties that
hold at any scale and *records* the numbers. Reduced scale is the default; `RETCD_EVIDENCE=1` runs
full scale. These mirror rEtcd M6-113..M6-116 deliberately — one schema, one disclaimer, one gate
script.

| ID | Name | Setup | Asserted / Recorded | Class | Dep |
|---|---|---|---|---|---|
| M7V-72 | `evidence_campaign_artifact_is_written` | the shared default corpus (§2); the artifact name follows the profile — `rdb-m7-campaign.json` under debug, `rdb-m7-campaign-release.json` under release (V-R17) | **Asserted:** the profile's artifact exists, parses, and its `values` carry every key ADR-rdb-0019 §2 names — `seeds`, `max_events`, `events_total`, `invariants{id -> {status: proven\|unavailable\|violated, reason, seeds_armed}}` (V-R16 adds `reason` and `seeds_armed` beside the status; `reason` is present only when `status` is `unavailable` and is the **one-string ADR form** `"capability(<package>)"` or `"not_armed"` — V-R20 (5), VA-7's artifact surface), `mutations{id -> catching_row}` (the value is **list-valued** under the unchanged key — V-R20 (5); a one-element list for MUT-1/3/4, `["M7V-69", "M7V-81"]` for MUT-2, and `["M7V-92", "M7V-70"]` for MUT-5 under ruling V-R25, which split MUT-5 into a kernel half and an oracle half as MUT-2 was; M7V-71), `wall_ms`, `shrink_ms`, `compile_ms_excluded`, `profile`; a missing key fails. **`coverage_gated` is deliberately not in this list (critic T-37):** ADR-rdb-0019 §2 puts it in `rdb-m7-coverage.json` only, and **M7V-73** asserts it there — this row neither requires nor forbids it in the campaign artifact, so exactly one row owns the key and the two artifacts cannot quietly converge. `profile` equals the build profile the binary was compiled under. **Recorded:** all of the above, plus `slipped` and both fault sets when a shrink slipped (F21) | campaign | I1 + testkit |
| M7V-73 | `evidence_coverage_artifact_is_written` | the same run | **Asserted:** `docs/evidence/rdb-m7-coverage.json` carries `guard_outcomes{cell -> count}`, `fault_boundaries{cell -> count}`, `pairwise{pair -> count}`, `required_missing[]`, `unavailable_cells{cell -> package}` (V-R19: the required cells excluded from `required_missing[]` by capability — hook-gated or family-gated, V-R20 (4); every key names a package whose `capability` event in the same run says `Unavailable`, and no cell appears in both lists) and `coverage_gated: bool` (V-R20 (7): `true` iff `seeds >= N`; `required_missing[]` fails the run only when it is `true`, and the shared corpus writes `true`); counts are integers, never a percentage; the 15 pairwise cells are **reported, not required** (some pairs are meaningless, and a required-but-unreachable cell becomes a cell someone deletes). **Recorded:** the full observed matrix | campaign | I1 + testkit |
| M7V-74 | `rdb_evidence_files_validate_against_the_schema` | after an evidence run, read every `docs/evidence/rdb-*.json` | **Asserted:** each parses through `read_evidence`/`validate`; `schema == 1`; `host`, `build.git_sha`, `run.utc` non-empty; `values` non-empty; `disclaimer` is the exact shared constant (not a second copy); unknown top-level keys rejected. A zero `scale_factor` with no `BELOW_TARGET_REASON`, an empty one, or a whitespace-only one (`"   "`, tester finding F4) is rejected; with a reason it validates (V-R27). Mirrors M6-113 — a malformed evidence file is worse than none, because it looks like evidence | unit | testkit |
| M7V-75 | `rdb_evidence_gate_rule_is_enforced_both_ways` | run the campaign with `RETCD_EVIDENCE` unset, then `=1` (two small corpora, §2, both under N seeds so `coverage_gated: false`, V-R20 (7) — the row is about the scale flag, not coverage) | **Asserted:** unset → the rows **run** (never `#[ignore]`d — asserted by a source check that no `#[ignore]` attribute exists in `tests/campaign.rs` or `tests/campaign/`), and write `scale_factor < 1.0`, `full_scale: false`; set → `scale_factor == 1.0`, `full_scale: true`; the gate check (the function VA-9 command 3 exercises) fails on any `full_scale: false` during an explicit full run. Mirrors M6-114. No duration is asserted (hard rule 1) | campaign | testkit |
| M7V-76 | `rdb_scale_factor_tracks_reality` | force `RETCD_EVIDENCE=1` while capping the run below full scale (one small corpus under N seeds, `coverage_gated: false`, V-R20 (7)) | **Asserted:** the written `scale_factor` reflects the seeds and events **achieved**, not requested, and the row marks `full_scale: false`. "Ran" is checked at its definition, not through the accessor the engine uses (tester finding F3): generated histories counted as `Ending::Judged` equal those `History::ran()` counts, judged + `Ending::Unlowerable` equals the seeds processed, `Campaign::unlowerable() + ran()` partitions the corpus, and at least one generated seed is refused today (M7V-55 tripwire). Mirrors M6-115 — a row that writes its intention rather than its observation is a fabricated measurement | campaign | testkit |
| M7V-77 | `rdb_evidence_carries_no_production_claim` | grep `docs/evidence/rdb-*.json`, `docs/ADRs/rdb/*.md`, `docs/rdb/*.md` and this plan | **Asserted:** every artifact carries the fixed disclaimer; no rDB document claims a later-milestone gate has been met; no document says "V1 passed" / "V3 passed" without its `Form` qualifier; no document claims fsync honesty, power-loss or real-clock qualification from M7. Mirrors M6-116 and enforces ADR-rdb-0019 §4's release boundary in a test rather than in a promise | unit | none |

---

## 10. Log-based assertions (DuckDB over `$RETCD_TEST_LOG_DIR`) — Q-34 … Q-40

Numbering continues rEtcd's Q-series (M6 ended at Q-33) because the log directory is shared. Each
query is what a developer runs **first** when the named rows go red. Fields are VA-7's contract.

> **Q-34 and Q-38..Q-40 return zero rows today, and that is not a clean run.** The lines they read
> are VA-7's eleven, none of which has a producer yet — see the held note in VA-7 for the
> measurement and the dependency on foundation's tier-1 serialiser. Q-35..Q-37 read tier-1 trace
> events and are dark for the same reason. Read a zero-row result from any of these as
> *unavailable*, never as *passing*.

### Q-34 — which checker fired, on what, and was anything merely unavailable (any M7V row)

```sql
SELECT "@m" AS msg, checker, status, reason, package, seeds_armed, rule, partition, role,
       event_kind, seed, count(*) AS n
FROM read_json_auto('$RETCD_TEST_LOG_DIR/**/*.jsonl', union_by_name=true)
WHERE testMethod = ?
  AND "@m" IN ('invariant_status','violation','capability_seen')
GROUP BY ALL ORDER BY msg, checker;

-- the vacuous-pass check (critic T-01, V-R16): must return zero rows
SELECT checker, status, seeds_armed
FROM read_json_auto('$RETCD_TEST_LOG_DIR/**/*.jsonl', union_by_name=true)
WHERE testMethod = ? AND "@m" = 'invariant_status'
  AND status = 'proven' AND coalesce(seeds_armed, 0) = 0;
```

**Assertions:** every checker id appears exactly once with an `invariant_status`; `status` is
confined to `{proven, unavailable, violated}`; `reason` is `capability` or `not_armed` when
`status='unavailable'` and NULL otherwise; `package` is non-NULL exactly when
`reason='capability'` and names a `PackageId` whose `capability_seen.state` is `Unavailable` in
the same run (critic T-30, V-R20 (5)); **no `proven` row has `seeds_armed = 0`** (the second
statement is empty; a NULL `seeds_armed` counts as 0 on purpose, so a runner that forgot the field
fails here too); a `violation` row exists for every `status='violated'` and for no other checker.
**First diagnosis:** a row that "passed" with `status='unavailable'` is the false green
M7V-03/M7V-53 exist to prevent — read `reason`: `capability` points at the package in `package`,
whose owner is the team to ask; `not_armed` at the corpus or the fixture (M7V-89 says which
scheduled op should have armed it). A `proven` row with a small `seeds_armed` is the second
thing to look at: the checker armed, but barely, and M7V-55's schedule is where to add pressure.

### Q-35 — an INV-PUB failure: what was pinned, what acked, what was flushed (M7V-06..M7V-12, M7V-67, M7V-69, M7V-70)

Rewritten per critic T-06/T-07 and again per **T-27** (V-R20): `publish` carries no
`config_version`, so the pin is resolved **through `admission_decision` on the `correlation`**;
`replication_ack` has `contiguous_seq` (a watermark), not `seq`; the quorum rule is **derived**
from `len(required_copy_set)` because the landed `protection_state` has no `quorum_rule` (V-R20
(1)); and the role in force comes from `topology_change` (a list of `(node, role)` tuples,
indexed `[1]`/`[2]`) plus the header's flat `topology` list (VA-7 `trace_header`), projected as a
column so a mismatch is visible rather than claimed — and an *unresolved* role is counted
separately, so the query can never report MUT-2 on a clean trace because a join returned NULL.

```sql
WITH ev AS (
  SELECT * FROM read_json_auto(?, union_by_name=true) WHERE testMethod = ?),
admitted AS (                                   -- the pin: what was in force at admitted_seq
  SELECT correlation, admitted_seq, config_version AS pinned_cv, required_copies
  FROM ev WHERE "@m" = 'admission_decision' AND outcome = 'Admitted'),
pinned AS (                                     -- the derived quorum rule, V-R20 (1)
  SELECT config_version, required_copy_set,
         CASE len(required_copy_set) WHEN 2 THEN 'DegradedRf2' WHEN 3 THEN 'Rf3' END AS derived_rule
  FROM ev WHERE "@m" = 'protection_state'
  QUALIFY row_number() OVER (PARTITION BY config_version ORDER BY event_id DESC) = 1),
topo AS (                                       -- role in force per config_version (critic T-27)
  SELECT t.config_version, t.node, t.role
  FROM (SELECT unnest(topology) AS t FROM ev WHERE "@m" = 'trace_header')
  UNION ALL
  SELECT config_version, n[1]::INTEGER AS node, n[2]::VARCHAR AS role
  FROM (SELECT config_version, unnest(nodes) AS n FROM ev WHERE "@m" = 'topology_change')),
acks AS (                                       -- contiguous_seq is a watermark: >= is the test
  SELECT from_node, peer_role AS claimed_role, durability_class, peer_boot,
         config_version AS ack_cv, contiguous_seq
  FROM ev WHERE "@m" = 'replication_ack' AND accepted),
flushes AS (
  SELECT node, durable_seq, outcome FROM ev WHERE "@m" = 'durability_advance')
SELECT p.seq, a.pinned_cv, pinned.derived_rule, pinned.required_copy_set,
       list(acks.from_node)                                   AS ack_nodes,
       list(acks.claimed_role)                                AS claimed_roles,
       list(topo.role)                                        AS roles_in_force,
       count(*) FILTER (WHERE acks.from_node IS NOT NULL AND topo.role IS NULL)
                                                              AS unresolved_roles,
       list(acks.durability_class)                            AS durability,
       list(flushes.outcome)                                  AS grounding,
       count(*) FILTER (WHERE topo.role IS NOT NULL
                          AND acks.claimed_role IS DISTINCT FROM topo.role) AS role_mismatches
FROM ev p
JOIN admitted a        ON a.correlation = p.correlation
LEFT JOIN pinned       ON pinned.config_version = a.pinned_cv
LEFT JOIN acks         ON acks.contiguous_seq >= p.seq
LEFT JOIN topo         ON topo.config_version = acks.ack_cv AND topo.node = acks.from_node
LEFT JOIN flushes      ON flushes.node = acks.from_node AND flushes.durable_seq >= p.seq
                      AND flushes.outcome = 'Synced'
WHERE p."@m" = 'publish'
GROUP BY ALL ORDER BY p.seq;
```

The `topology_change.nodes` tuple is serialised by serde as a two-element JSON array, and
`read_json_auto` types a mixed `[integer, string]` list as `JSON`/`VARCHAR[]`, hence the two casts;
the header's `topology` entries are structs and need none. Both branches emit the same three
columns, so the `UNION ALL` binds.

**Assertions:** every published `seq` has a `pinned_cv` (a NULL means the publish has no admission
on its `correlation` — a trace defect, not a checker one) and ack rows whose `from_node` set
covers the pinned `required_copy_set` under the `derived_rule`'s cardinality (one qualifying
regular under `DegradedRf2`, every member under `Rf3`); `derived_rule` is never NULL — a set of any
size but 2 or 3 is **a violation**, `rule="required_copy_set_shape"` (design §2.3 under critic
T-39; M7V-07's sub-case), not a fixture or cadence defect, so a NULL here is read as INV-PUB
firing and not as a trace to be repaired; **`unresolved_roles = 0`** — `roles_in_force`
contains no NULL, i.e. every ack's `(config_version, from_node)` resolves through a
`topology_change` or the header (critic T-27: a NULL here used to count every healthy ack as a
forgery); every `durability='Durable'` entry has a `grounding='Synced'` entry (a NULL is the MUT-5
shape); `role_mismatches = 0` (a non-zero count is the MUT-2 shape, and the two role columns show
which node claimed what). **One unexecuted step (critic round 4 on T-27):** the `topo` CTE's first branch, `FROM (SELECT unnest(topology) AS t FROM ev WHERE "@m" = 'trace_header')` read as `t.config_version`, relies on struct-field access against an **unaliased** subquery — nobody has run it, and it is the only step of this query in that state. **Run Q-35 once against a real `rdb-sim` JSONL log before trusting the `topo` columns** (`roles_in_force`, `unresolved_roles`, `role_mismatches`); if DuckDB rejects the reference, alias the subquery (`… ) AS h` and read `h.t.config_version`). No row asserts on this query — §10's queries are diagnosis aids, not tests (§10 preamble) — so the correction is a doc edit, never a red row. **First diagnosis:** a `derived_rule='DegradedRf2'` row whose `ack_nodes`
contains no member of `required_copy_set` is critic F1's bug, live (M7V-08); a row whose
`ack_nodes` is empty or only the primary is the cardinality form (M7V-79); a row whose `pinned_cv`
differs from the latest `protection_state.config_version` is the pin-drift case (M7V-08(b)); a
non-zero `unresolved_roles` is a missing `topology_change` for that `config_version` (VA-3, V-R12),
not a forgery.

### Q-36 — authority windows and overlaps (M7V-13..M7V-15, M7V-66)

```sql
SELECT partition, generation, gate, owner_node, owner_epoch, grant,
       valid_from_tick, expiry_tick, decision_tick, outcome, count(*) AS n
FROM read_json_auto(?, union_by_name=true)
WHERE testMethod = ? AND "@m" = 'authority_decision'
GROUP BY ALL ORDER BY partition, valid_from_tick, generation;
```

**Assertions:** within a `partition` (the landed envelope field, convention 1), no two
`outcome='Valid'` generations have overlapping
`[valid_from_tick, expiry_tick)`; all four gates appear for a completed write path; `outcome` is
confined to `{Valid, Expired, Fenced, Uncertain}`. **First diagnosis:** sort by `valid_from_tick` and
read down — an overlap is visible as one row's `expiry_tick` exceeding the next row's
`valid_from_tick`.

### Q-37 — walk the lineage chain (M7V-16..M7V-19, M7V-68, and any recovery failure)

```sql
SELECT partition, generation, seq, predecessor_seq, predecessor_digest, entry_digest, outcome
FROM read_json_auto(?, union_by_name=true)
WHERE testMethod = ? AND "@m" = 'batch_apply'
ORDER BY partition, generation, seq;
```

**Assertions:** `predecessor_digest` at `seq` equals `entry_digest` at `seq-1` in the same
generation, or the generation's root `base_digest`; `(generation, seq)` is unique per
`entry_digest`; `seq` has no holes inside a generation. Join against
`"@m"='recovery_decision'` to see `selected_cutoff_seq` against the chain above.
**First diagnosis:** the first row where `predecessor_digest` breaks the chain names the
`seq` the reducer should be shrinking toward.

### Q-38 — the coverage shortfall list (M7V-55, M7V-57, M7V-73)

Rewritten per critic T-08: a zero-hit cell emits **no** `coverage_cell` line (VA-7), so a query
over `coverage_cell` alone can never find a shortfall. The shortfall comes from
`coverage_shortfall`; the counts come from `coverage_cell`; the hook-gated exclusions from
`coverage_unavailable`.

```sql
SELECT 'missing'     AS kind, axis, cell, 0 AS hits, NULL AS package
FROM read_json_auto(?, union_by_name=true)
WHERE testMethod = ? AND "@m" = 'coverage_shortfall'
UNION ALL
SELECT 'unavailable' AS kind, axis, cell, 0 AS hits, package
FROM read_json_auto(?, union_by_name=true)
WHERE testMethod = ? AND "@m" = 'coverage_unavailable'
UNION ALL
SELECT 'hit'         AS kind, axis, cell, sum(count) AS hits, NULL
FROM read_json_auto(?, union_by_name=true)
WHERE testMethod = ? AND "@m" = 'coverage_cell'
GROUP BY ALL
ORDER BY kind, axis, cell;
```

**Assertions:** for a passing run there is **no `kind='missing'` row**; every `kind='unavailable'`
row names a package whose `capability_seen.state` is `Unavailable` in the same run (join Q-40 —
an `unavailable` cell under a `Wired` package is a gating-table bug, M7V-56); every required cell
appears exactly once across the three kinds; `hits >= 1` for every `kind='hit'` row — **on a
`coverage_gated: true` run** (`campaign_run.seeds >= N`, V-R20 (7)); on a sub-N corpus `missing`
rows are expected and recorded, and the run has not failed. **First diagnosis:** a `missing`
`derived_quorum_rule × DegradedRf2` cell (the rule is derived from `required_copy_set.len()`,
V-R20 (1)) means the campaign is not exercising the path the corrections were made for — a green
run with that shortfall proves nothing about V3. A `missing`
`ForgedIdentity` or `FalseDurableWatermark` cell while H1/M1 report `Wired` means the producer
table is stale (M7V-42, M7V-55 clause 3).

### Q-39 — the reducer's trajectory, and whether it slipped (M7V-20..M7V-23, M7V-48)

```sql
SELECT step, ops_before, ops_after, accepted, checker, rule, faults
FROM read_json_auto(?, union_by_name=true)
WHERE testMethod = ? AND "@m" = 'shrink_step'
ORDER BY step;

-- the slippage check reads the result line, not the steps (critic T-22)
SELECT signature_slug, ops_before, ops_after, slipped, faults_before, faults_after, budget_spent,
       (slipped = (faults_before <> faults_after)) AS slipped_is_consistent
FROM read_json_auto(?, union_by_name=true)
WHERE testMethod = ? AND "@m" = 'shrink_result';
```

**Assertions:** `ops_after < ops_before` on every accepted step; the `(checker, rule)` core tuple is
constant across accepted steps; on the second statement, `slipped_is_consistent` is true on every
row (`slipped` **iff** `faults_before != faults_after`), and `ops_after` on the result equals the
last accepted step's `ops_after`. **First diagnosis:** a long run of `accepted=false` at constant
`ops_before` means the acceptance predicate is too strong — F21's exact failure, which is what
happens if anyone puts `faults` back into equality.

### Q-40 — capability honesty and the redaction rule (every row; team-rules logging)

```sql
SELECT package, state, count(*) AS n
FROM read_json_auto(?, union_by_name=true)
WHERE testMethod = ? AND "@m" = 'capability_seen'
GROUP BY ALL ORDER BY package;

-- the redaction check is one runnable statement over every line the run emitted (critic T-22)
SELECT column_name
FROM (DESCRIBE SELECT * FROM read_json_auto('$RETCD_TEST_LOG_DIR/**/*.jsonl', union_by_name=true))
WHERE lower(column_name) IN ('key', 'value', 'value_bytes', 'payload', 'mutation_bytes')
   OR lower(column_name) LIKE '%_bytes';
```

**Assertions:** every package `C0 H1 M1 I1 A1 T1 R1 P1 L1 F1` appears exactly once per run;
`state` is `Wired` or `Unavailable` and nothing else. Second statement: **returns zero rows** —
no column named `key`, `value`, `value_bytes`, `payload`, `mutation_bytes` or `*_bytes` exists in
the union of every JSONL line the run wrote, not only the lines the other queries projected.
team-rules forbids logging key or value bytes, and this is the row-independent check that keeps it
true (`digest`, `key_id` and `value_version` are the permitted identities).

---

## 11. Anti-flake rules for this plan (rules M7V-A1 … M7V-A8)

1. **A1 — nothing sleeps and nothing reads a wall clock.** All time is `logical_tick`. The only
   wall-clock number anywhere is `wall_ms`, and it is recorded, not asserted (except M7V-61).
2. **A2 — a near-miss row differs from its bad twin by exactly one fact.** If it differs by two, it
   stops being evidence that the checker draws the line in the right place.
3. **A3 — every hand-built trace is built through `TraceBuilder` (VA-1)**, so a row never fails for
   a malformed envelope and gets read as an invariant failure.
4. **A4 — no row asserts on thread count, host speed or allocation count.** M7V-58 asserts the
   result is *independent* of thread count, which is the opposite thing.
5. **A5 — a row that cannot arm reports `Unavailable{NotArmed}`; a row whose package is unwired
   reports `Unavailable{Capability(p)}`; neither passes, and `proven` needs `seeds_armed > 0`**
   (V-R16). See §12 and M7V-78.
6. **A6 — never two cargo invocations against one target directory** (AGENTS.md, the 2026-09-19
   `LNK1104` collision). `.rtargets/verification` and `.rtargets/campaign` are separate on purpose.
7. **A7 — write the failing row first.** ADR-0014 and spike §5: each package "first demonstrat[es]
   its expected failure". M7V-08 in particular is worthless unless it was observed failing against
   the healthy-RF3 rule.
8. **A8 — never lower an assertion to make a run green.** If the 1,000-history budget is missed,
   spike §7's rule applies: improve the harness or revise the budget in ADR-rdb-0019, in writing.

---

## 12. Rows that cannot pass yet — "Unavailable until \<package\>"

The charter requires the campaign to report `Unavailable`, never a pass, while kernel packages are
unwired. The same discipline applies row by row. Three mechanisms, chosen by what is missing:

| Situation | Mechanism | What the runner reports meanwhile |
|---|---|---|
| The **checker** exists and the trace vocabulary exists, but no kernel produces the behaviour | the row runs on a hand-built trace and passes on its own terms | nothing is claimed about the kernel; the campaign's status table says `unavailable` for that invariant |
| The row needs the **runner** (I1) or a **provider hook** (H1/M1) | the row exists, compiles, and asserts the `capability{package=p, state=Unavailable}` path: the campaign reports `unavailable` with `reason = capability(p)` and the artifact records it. It is **upgraded in place** when the package lands — never duplicated into a `*_v2` row | stdout: `INV-x: unavailable (capability I1 not wired)`; artifact: `invariants.INV-x = {status: "unavailable", reason: "capability(I1)"}`; exit code 0 unless `SPIKE_REQUIRE_ALL=1` |
| The row's package is wired but its **arming situation** is not reachable yet (a fixture, a generator producer, or a scenario the environment cannot reach) | the row runs and reports `Unavailable{NotArmed}`; at campaign level the status is `unavailable(not_armed)` with `seeds_armed = 0`. Never `proven` (V-R16, M7V-78) | stdout: `INV-x: unavailable (not armed on any seed)`; artifact: `{status: "unavailable", reason: "not_armed", seeds_armed: 0}` |
| The row cannot be **written** at all until the dependency exists | it is listed below and counted as **missing** by §13's gate checklist, which fails while the count is non-zero. It is never marked done, and never silently dropped | §13's checklist line is red |

**No row in this table waits on a contract shape any more.** The two that did — M7V-46's header
half and the `op_skipped` clause of M7V-22 and M7V-88 — are struck below: both shapes landed at
`6893442` and were held open for four rounds against a stale reading of `8a23b1d` (§15 drift rows
1 and 21). Everything still listed waits on **code**: the I1 runner, I1's trace validator, and the
H1/M1 provider hooks. Re-read the drift table before adding a row to this one.

| Rows | Unavailable until | Note |
|---|---|---|
| M7V-01..M7V-19, M7V-24..M7V-41, M7V-66..M7V-68, M7V-79, M7V-81 | **C0** (trace vocabulary types) — landed at `8a23b1d`, **complete for this plan at `6893442`** (§15 drift rows 1 and 21) | hand-built or rewritten traces; no runner needed. These are the rows that can be written first (`design.md` §9 work order). M7V-66..68 and M7V-81 were missing from this table before (critic T-18) |
| M7V-08, M7V-38, M7V-79 | **C0 + the `protection_state` on-every-`config_version` cadence** (V-R10) | without the cadence the checker cannot know the pinned set; the row asserts a property the trace cannot express |
| M7V-08(b), M7V-10 | **C0 + `topology_change`** (V-R12, critic F19; `trace-requirements.md` §3.19, ask 7 — landed 18:46) | a header-only static `topology` makes this row fail on a **correct** kernel after a membership change. Seam-freeze item: it cannot be fixed after C0 freezes |
| M7V-26 | **C0 + `ClientOutcome::RecoveredApplied`** (V-R10) | the closed set must carry it or the near-miss half is unwritable |
| M7V-28, M7V-29 | **C0 + `replication_ack` emitted at the secondary** (V-R10) | delivery-point-only emission makes a dropped ACK's holder invisible and INV-LOSS permits loss it should forbid |
| M7V-82 | **C0 + foundation's dispatcher** (`Dispatcher::capability_report` at `8a23b1d`; `Module::capability(&self)` under K-F-10) | runs the dispatcher in-process with stub modules; no runner |
| ~~M7V-46 (header half)~~ | ~~C0 + `provenance`~~ — **UN-PARKED** | `Provenance` landed at `6893442` (`contracts/trace.rs:85`), `TraceHeader.provenance` at `trace.rs:212`, no `TraceHeader.seed` left. Both halves of M7V-46 run; its `Dep` is plain `C0`. The row sat here for four rounds against `8a23b1d` while the type existed at `6893442`; the line is struck rather than deleted so a reader arriving from an older revision sees why it is gone |
| ~~M7V-22, M7V-88 (`op_skipped` clause)~~ | ~~C0 + `op_skipped`~~ — **UN-PARKED** | `TraceKind::OpSkipped{scenario_op_index: u32, reason: SkipReason}` landed at `6893442` (`contracts/trace.rs:1115`), with `SkipReason{ReferentGone, OutOfBudget}` at `trace.rs:634`. M7V-88's "no `op_skipped{ReferentGone}`" clause is **no longer vacuous** and must not say it is. M7V-22 still waits on **I1** — it is a `sim` row and needs the runner — and is listed in the I1 block below; it no longer waits on a contract |
| M7V-20, M7V-21, M7V-22, M7V-23, M7V-47, M7V-48, M7V-50, M7V-86, M7V-88 | **I1** (replay runner) | the reducer and replay rows, the runner half of the budget row (critic T-21), and the fixture-realizability row (design §4.5) — reason `capability(I1)`. M7V-20 is listed in full now (critic T-18) |
| M7V-80 | **I1 + F1** | the kernel-facing third sub-case of M7V-19; `Unavailable{Capability(F1)}` until F1 lands. **2026-09-27:** F1 has landed and quarantines, and the contract ask landed (L-R177gd: the three `RecoveryDecision` fields are `None` on a quarantine). The `recovery_decision` and `quarantine` clauses are green; the row is `parked(.., I1, ..)` on INV-LIN `Proven` (no `lineage_root` line), the `BoundaryId::Divergence` cell (no `fault_injected` line) and the `AckRejectReason::Diverged` cell (no rejected-ack line, and no receiver after a quarantine: R1) |
| M7V-51..M7V-65, M7V-72..M7V-76, M7V-78, M7V-89 | **I1** (+ `config-testkit` dev-dep for the evidence rows, V-R15) | the campaign loop and its artifacts. M7V-51 is listed now (critic T-18); M7V-89 reports `unavailable (no invariant fully wired)` during M7 (V-R20 (3)) |
| M7V-87 | **I1 + testkit** | exercises the gate function's failing branch during M7 and reports the release claim `unavailable` until A1 T1 R1 P1 L1 F1 are wired. M7V-62 is **no longer** listed here: per critic T-32 it asserts the name selector and this run's `profile` under either gate and reads no other command's file |
| M7V-69 | **H1 `ForgeAck` hook** (VA-4, V-R9) **+ R1** (the rejection is the kernel's) | MUT-2 kernel half; also gates the `ForgedIdentity` coverage cell, which M7V-55 reports `unavailable(H1)` meanwhile (V-R19) |
| M7V-70 | **M1 `FalseDurable` hook** (VA-4, V-R9) | MUT-5; also gates V1 clause 3's modelled half and the `FalseDurableWatermark` cell, reported `unavailable(M1)` meanwhile. **V-R29:** re-planned; owed, reported `unavailable(M1)` per the ruling. The row holds evidence that the hook has landed and cannot hit the cell (see the row) |
| M7V-92 | **M1 `FalseDurable` hook** (landed) **+ a sim run that reaches P1's `Publish`** (B-R60) | MUT-5 kernel half (V-R25). Its kernel clauses run today; its INV-PUB clause is `parked(.., I1, ..)` until a run publishes |
| `proven` status for every invariant | **A1, T1, R1, P1, L1, F1**, and a corpus that **arms** each checker (`seeds_armed > 0`) | until each package lands its invariants are `unavailable(capability)`; after that, until the corpus arms a checker, `unavailable(not_armed)` — and M7V-89 fails the handoff gate for any invariant whose packages are all `Wired` yet never armed (V-R20 (3)); the M7 release gate (VA-9 command 3) sets `SPIKE_REQUIRE_ALL=1` and fails on either |

**Which reason each row reports meanwhile (architect handoff §C).** Every row in the table above
that names a package reports `Unavailable{Capability(<that package>)}` — `capability(C0)`,
`capability(I1)`, `capability(H1)`, `capability(M1)`, `capability(F1)` — because the row's input
cannot be produced at all. `NotArmed` is never a row's *standing* status; it is the verdict a
wired checker gives on a trace that did not reach its arming event, and it appears in exactly
these rows by construction: M7V-03(b), M7V-31, M7V-33, M7V-63, and the `unavailable(not_armed)`
branch of M7V-52/M7V-78/M7V-89. A row that reports `NotArmed` for any other reason is a fixture
defect (M7V-88), not a dependency. (This paragraph sits **below** the table on purpose — critic
T-29: placed between rows it ended the table and three rows rendered as prose.)

---

## 13. Gate checklist — charter acceptance and ADR-rdb-0019 → rows

| Criterion | Source | Rows |
|---|---|---|
| Each checker has a bad trace that trips it and a valid trace that does not | charter O1; spike §5 O1 | M7V-04..M7V-41, M7V-79 (bad + near-miss per invariant), M7V-02 (generic positive control, arms all ten) |
| The oracle imports nothing from the six kernel modules; proven by grep **and** by a row | charter O1; spike §6 | M7V-01 (allowlist row); handoff §4 (grep) |
| Every `design.md` §2.3 invariant has a bad trace and a valid trace | design §2.3 | ATOM 04/05 · PUB 06–12, 79 · AUTH 13–15 · LIN 16–19 (+80 kernel-facing) · DEDUP 24–26 · LOSS 27–29 · LIVE 30/31 · ISO 32/33 · VER 34/35 · LAG 36–41 |
| `Proven` means armed; `proven` means `seeds_armed > 0`; both `Unavailable` reasons report and never pass | V-R16; design §2.4 | M7V-02 (arming pinned), M7V-03 (both arms), M7V-31, M7V-33, M7V-63 (`NotArmed`), M7V-78 (campaign), M7V-54 (gate), Q-34 |
| A seeded failure keeps its signature after shrinking | charter G1; spike §5 G1 | M7V-20 (core tuple), M7V-23 (slippage recorded) |
| The minimized trace replays through I1 and fails the same checker | charter G1 | M7V-21, M7V-50 |
| Both `.orig.json` and minimized fixtures replay, and no expectation edit can green them | critic F4, T-16 | M7V-50 (+ M7V-20, M7V-23), M7V-85 |
| Every critic F-closure has a guard row that fails if reverted | lead's check, critic T-15 | F5 → M7V-83; F12 → M7V-84; the other nineteen per critic-tests.md's table |
| `SPIKE_SEEDS=1000 SPIKE_MAX_EVENTS=2000` ≤ 60 s warm release, host-qualified | charter Q1; V-R11; V-R17 | M7V-60 (recorded), M7V-61 (asserted in the extended gate), M7V-62 (two artifacts, two profiles) |
| Zero invariant violations once kernel packages land, and every fully wired invariant actually arms on the default corpus | charter Q1; V-R20 (3) | M7V-52, M7V-54, M7V-78, M7V-89 |
| Until then, explicit `Unavailable`, never a pass; `capability{state}` tracks reality | charter Q1; V-R18 | M7V-03, M7V-53, M7V-54, M7V-82, §12 |
| Every spike §7 mutation caught by a **named** test | spike §7 | MUT-1 M7V-66 · MUT-2 M7V-69 (kernel) **+** M7V-81 (oracle) · MUT-3 M7V-67 · MUT-4 M7V-68 · MUT-5 M7V-92 (kernel) **+** M7V-70 (oracle, owed on INV-LOSS; V-R25, V-R26) · completeness M7V-71 |
| Every §6 required coverage cell hit — by schedule, not by luck; a zero-hit required cell fails; hook-gated cells excluded by capability only | design §6, §3.1; ADR-rdb-0019 §2; V-R19 | M7V-55 (schedule + observed), M7V-57 (negative, both branches), M7V-56 (enumerated, set equality), M7V-42 (producer table), M7V-73 (recorded), Q-38 |
| Multi-partition isolation (spike §7 safety table, V-R8) | ADR-rdb-0019 §1 | M7V-32, M7V-33, isolation cell in M7V-55 |
| V1 clause 3 "no false durable watermark", modelled sense only | ADR-rdb-0019 §1 V1 | M7V-11, M7V-70, M7V-98 (the recorder never writes `Synced` over a short flush; V-R36) |
| Every recorded `batch_apply`, `durability_advance` and `replication_ack` is true of engine state, so a false recorder line fails a row, not only a probe | tester-sim-hooks F1 (MATERIAL), 2026-09-27 | M7V-96..M7V-101 (mutants t1..t5 each killed by a named row) |
| V3's degraded half: both survivors required, no one-copy fallback — membership **and** cardinality, pinned at `admitted_seq` | ADR-rdb-0019 §1 V3; spec §8.3; critic T-02 | M7V-08(a) membership, M7V-08(b) pin drift, M7V-79 cardinality, M7V-09 near-miss, M7V-07's shape sub-case (T-39), `DEGRADED_RF2` cell in M7V-55 |
| V4 retries and outcomes, modelled 24 h retention | ADR-rdb-0019 §1 V4 | M7V-24, M7V-25, M7V-26, authored case in M7V-47 |
| V8 oracle half: transition legality | ADR-rdb-0019 §1 V8 | M7V-36..M7V-41. **V8's timing half is kernel-b's L1 rows; neither half alone is V8** |
| V12 subset: unknown mandatory version refused before apply | ADR-rdb-0019 §1 V12 | M7V-34, M7V-35 |
| Evidence schema reused unchanged; the four ADR-0031 mirror rows; two campaign artifacts | ADR-rdb-0019 §2; V-R5; V-R17 | M7V-72..M7V-77, M7V-62 |
| `--test campaign` fits one debug gate run | critic T-11; §2 aggregate budget | §2 (shared corpus, ≤ 14 executions, < 120 s target recorded not asserted), M7V-58, M7V-61, M7V-65 |
| Handoff gate: `CARGO_TARGET_DIR=.rtargets/verification scripts/gate.sh test -p rdb-sim --test oracle --test scenarios --test campaign` green at handoff — **green with invariants `unavailable`, by design** | charter; VA-9 command 1 | VA-9, §2 budgets, M7V-52 |
| Every fixture and authored case is realizable by the runner; no assertion weakened to make it so | design §4.5; charter DO-NOT | M7V-88 (`Unavailable{Capability(I1)}` until I1) |
| **M7 release gate:** `SPIKE_REQUIRE_ALL=1 RETCD_EVIDENCE=1 CARGO_TARGET_DIR=.rtargets/campaign scripts/gate.sh test --release -p rdb-sim --test campaign` green — every invariant `proven` with `seeds_armed > 0`, `full_scale: true`, no `required_missing`, release artifact written. This is the command ADR-rdb-0019 §2.1 names as the milestone claim | V-R18; VA-9 command 3; ADR-rdb-0019 §2.1 | M7V-87 (the command and its gate function), M7V-54, M7V-78, M7V-89, M7V-75, M7V-55, M7V-62 |

**Row count: 89** (`M7V-01`..`M7V-89`; ids stable, `M7V-78..M7V-88` added in correction round 1,
`M7V-89` and `M7V-90` in round 2, and **`M7V-90` withdrawn in round 4** under F-R13 — its id is
retired, not reused, so the next new row is `M7V-91`). Oracle 44 (41 + 79, 81, 85) ·
grammar/generator 9 (6 + 80, 86, 88) ·
reducer 10 (8 + 83, 84) · campaign 18 (14 + 78, 82, 87, 89) · mutations 7 (6 + 81 counted once,
under oracle) · evidence 6 — the blocks overlap by the reserved ids 20–23 and by M7V-81, and the
distinct id set is `M7V-01..M7V-89`. By class: unit 58 · sim 12 · campaign 19 (§2).
Rows added after this count and not in it: `M7V-92`..`M7V-126` (thirty-five; `M7V-90` stays retired, and
`M7V-91` is held as the candidate row named in §15 drift row 6, still unwritten). Their classes are in §2. Count landed rows with
`scripts/m7-census.sh verification`, never from this paragraph.

---

## 14. Open questions — the recommendation is the default

| # | Question | Default (implement this if the lead does not answer first) |
|---|---|---|
| Q-1 | M7V-23's strongest form replays `.orig.json` against "a checker configuration in which the minimized fixture passes", which needs a way to disable one checker rule for one replay. Is that acceptable, or should the row settle for asserting both fixtures currently fail? | **Acceptable, scoped to the test binary and bounded by a row**: a `Report::without_rule(&str)` on the oracle's *report*, not on the checker, used by M7V-23 only — and **M7V-85 asserts it has exactly one call site** (critic T-16), so the surface cannot spread. If the lead objects to any such surface, the row degrades to "both fixtures fail and `slipped` is recorded", which is weaker but still catches the artifact half of F4 |
| Q-2 | Does the M7 gate accept a campaign row writing to `docs/evidence/` on every ordinary `scripts/gate.sh` run (ADR-rdb-0019 §2 says reduced-by-default, never `#[ignore]`d), given the artifact is a tracked file that will churn in every diff? Under V-R17 the churn is on `rdb-m7-campaign.json` only; the release artifact changes only when commands 2/3 run | **Yes, churn accepted** — that is rEtcd's existing behaviour for `docs/evidence/*.json` and the alternative is a suite that runs when someone remembers. If the churn is unacceptable, the fallback is to write under `$RETCD_TEST_LOG_DIR` by default and to `docs/evidence/` only under `RETCD_EVIDENCE=1`, which weakens M7V-75 |
| Q-3 | Do kernel teams write their scenarios against **this** grammar (charter: "kernel teams supply the behaviour under test"), and if so, is `support/scenarios` importable from `tests/authority.rs` etc.? | **Yes, shared through `tests/support/`**, which is team foundation's `mod.rs` registration. If each kernel team builds its own scenario types, the isolation rows (M7V-32) and P1's "freezes only its partition" acceptance are unreachable from their side |
| Q-4 | ~~Does foundation agree `BoundaryId` stays exactly spike §6's column plus the two V-R9 members?~~ **Answered by V-R19:** `BoundaryId` is foundation's closed set, 29 members at `8a23b1d`; the foundation architect lists them in their handoff. M7V-56 asserts set **equality** against the enum | Closed. The 29 members observed in `crates/rdb-core/src/contracts/trace.rs` are the set M7V-56 is written against; if foundation's handoff list differs from the enum, the enum wins and the handoff is the thing to fix |
| Q-5 | `design.md` §4.2 excludes shrink time from `wall_ms`. On a run with **no** failure, `shrink_ms` is legitimately 0, and on a fast shrink it can round to 0 on this host's coarse timer. Is a `0` value or an absent key correct? | **Always present, `0` when nothing shrank.** An absent key and a zero are indistinguishable to a reader of the artifact, and M7V-72 asserts key presence. M7V-51 no longer asserts either duration is non-zero (critic T-10); it asserts a shrink *occurred* from the `shrink_step`/`shrink_result` lines |
| Q-6 | M7V-77 greps rDB documents for unqualified gate claims. Does it also grep `.claude/scratchpad/**` team notes? | **No — tracked documents only** (`docs/evidence/rdb-*.json`, `docs/ADRs/rdb/`, `docs/rdb/`, `docs/testing/test-plan-m7-*.md`). Working notes are history, not claims, and AGENTS.md already says the archive is never current truth. If the lead wants the notes covered too, it is one more path in the row's list |

---

## 15. Source state at the time of writing, and remaining contradictions

Recorded rather than silently worked around, per team-rules "Evidence".

**Verified landed before M7V-20, M7V-23 and the INV-LAG rows were written** (the lead's ordering
instruction). Re-read at 18:48 on 2026-09-20 — `design.md` at 18:47, `trace-requirements.md` at
18:46, ADR-rdb-0019 at 18:43:

| Correction | Where it landed | Row written to it |
|---|---|---|
| **F8** — INV-LAG clause (a) quantifies over **every** node in the pinned `required_copy_set` | `design.md` §2.3; ADR-rdb-0019 §1 V8 `Form` | M7V-36, M7V-41 |
| **F20** — clause (c) is an **admission** gate; "no publish while paused" withdrawn | `design.md` §2.3; ADR-rdb-0019 §1 V8 `Form` | M7V-39 (replacement), M7V-40 (guard) |
| **F21** — acceptance predicate is the **core tuple**; `faults` recorded and reported with `slipped` | `design.md` §4.4 ("recorded and reported, NOT part of the acceptance predicate"), and §4.4's own restatement of M7V-20/M7V-23 | M7V-20, M7V-23, M7V-51, Q-39 |
| **F19 / V-R12** — role grounding is config-versioned via environment-emitted `topology_change` | `design.md` §2.1, §2.3 INV-PUB, §2.5, §9; `trace-requirements.md` §3.19 and ask 7 | M7V-10 |
| **F18** — header carries `Provenance`, not a bare `seed` | `trace-requirements.md` §1; routed to foundation under V-R20 (2) and **landed at `6893442`**, `contracts/trace.rs:85` and `:212`, drift table row 1 below | M7V-46 (both halves run; un-parked from §12) |

**Correction round 1 (critic round 2, T-01..T-22), written 2026-09-20 19:10–19:30 PDT.** The four
rulings that touch files this plan does not own are cited by **ruling id**, because the architect
was amending those sections in parallel:

| Ruling | What it settles | Where it lands (architect's files) | Rows written to it |
|---|---|---|---|
| **V-R16** (T-01) | `Unavailable{Capability(p)}` and `Unavailable{NotArmed}`, both report, never pass; `proven` requires `seeds_armed > 0` | `design.md` §2.4 — observed landed at 19:13 with the reason enum named `Unavailable`, per-checker `armed()`, and the per-run fold order | VA-2, M7V-02, M7V-03, M7V-31, M7V-33, M7V-63, M7V-78, M7V-54, Q-34 |
| **V-R17** (T-13) | second artifact `rdb-m7-campaign-release.json`, name chosen by `cfg!(debug_assertions)`; release command sets `RETCD_EVIDENCE=1`; only the release artifact may be cited | ADR-rdb-0019 §2 artifact table and §2.1 — **landed, committed at `b0a4e58`**; `design.md` §5.1.1, §5.3 | VA-9, M7V-62, M7V-72 |
| **V-R18** (T-14) | the M7 release gate command; `capability{state}` derived, not a literal | ADR-rdb-0019 §2.1 (**landed, `b0a4e58`**); `design.md` §2.4 last paragraph and §5.1 "M7 release gate" column; `trace-requirements.md` §3.18 | VA-9 command 3, §2 knob table, §13's last line, M7V-82, M7V-87 |
| **V-R19** (T-12, Q-4) | required boundaries scheduled as `REQUIRED[i mod N]`; hook-gated cells under `unavailable_cells`, excluded by capability; `BoundaryId` set equality (29) | `design.md` §3.1, §5.3 — observed landed at 19:13; ADR-rdb-0019 §2 rule 3 and coverage row | VA-6, M7V-42, M7V-43, M7V-44, M7V-55, M7V-56, M7V-57, M7V-73, Q-38 |
| **design §4.5** (critic on R-3, second-order) | every fixture and authored case must be realizable by the runner | `design.md` §4.5 — landed | M7V-88 |
| **design §5.1** correction | extended-gate `SPIKE_ASSERT_WALL_MS` is `600000`, not `60000` | `design.md` §5.1 | §2 knob table |

One vocabulary change was picked up in the same pass: `trace-requirements.md` §3.18 now names the
capability event's field **`package`** (foundation's landed `TraceKind::Capability { package,
state }`), superseding `capability_id`; every row and query here uses `package`.

**Correction round 3 (critic round 4, T-36..T-41), written 2026-09-20.** Critic round 4's verdict
was PASS_WITH_RISKS — twelve of T-23..T-35 closed, T-33 revised, none sustained — so this round
fixes plan-text defects, not design decisions, and adds no ruling. Four changes, each closing one
finding: **T-36** M7V-87's extraction rule now selects `| 3 | **The M7 release gate**`, because the
bare `| 3 |` prefix also matched the drift table's row 3 below and made the row red on a correct
plan; **T-37** `coverage_gated` dropped from M7V-72's campaign-artifact key list (ADR-rdb-0019 §2
puts it in `rdb-m7-coverage.json` only, where M7V-73 owns it); **T-38** M7V-89's arming-op table
replaced by a verbatim restatement of design §2.4's wired-clause list, so INV-DEDUP arms on the
`RetainedDedupHit` boundary rather than on any submit and INV-PUB/INV-AUTH on the first `Submit`;
**T-41(d)** M7V-29's prose says `boot`, the landed field. Q-35 gains the one unexecuted-step
caveat the critic flagged under T-27.

**Correction round 4 (T-39, T-40 and ruling F-R13), written 2026-09-20.** Three items that were
waiting on rulings, none of them new scope. **T-39 is ruled a violation** (design §2.3): a
`required_copy_set` whose length is neither 2 nor 3 is `required_copy_set_shape`, because the
oracle reads only the trace and cannot tell a bad fixture from a kernel that really pinned such a
set, and demoting it would force skipping INV-PUB for that seed — the silent skip §2.4 exists to
prevent. It lands as a sub-case on **M7V-07**, and Q-35's "fixture or cadence defect" sentence now
calls it a violation. **T-40 is closed against the trace, not against the table** (design §3.1):
C0 has no `impl BoundaryId`, so **M7V-42** asserts one family and one gating package per member
statically and **M7V-55** clause (4) asserts, for every observed `fault_injected`, that
`fault_kind` equals the family the table assigns its `boundary` — the emitting provider is the
ground truth, and no foundation ask is needed. **Ruling F-R13 withdraws M7V-90:** K-F-07 is closed
by derivation, there will be no `quorum_rule` field, so the cross-check row and the violation
`quorum_rule_mismatch` are gone (§4 convention 1, drift row 2). Row count 90 → **89**; the id
`M7V-90` is retired and the next new row is `M7V-91`.

**Correction round 2 (critic round 3, T-23..T-35), written 2026-09-20 under ruling V-R20.** The
architect amended `design.md` §2.3/§2.4/§3.1/§4/§4.5, `trace-requirements.md` §8 and ADR-rdb-0019
in parallel; where a row here depends on one of those sections it cites the section **and** V-R20,
and the architect's text is authoritative for the design side.

| Ruling clause | What it settles | Where it lands (architect's files) | Rows written to it |
|---|---|---|---|
| **V-R20 (1)** (T-23) | `quorum_rule` is **not** a trace field; the oracle derives it from `required_copy_set.len()`; the cell is keyed on the derived value | `design.md` §2.3 (one sentence), §6 `derived_quorum_rule`; `trace-requirements.md` §3.14 withdrawn, §8 row 2 | §4 convention 1, M7V-07, M7V-08, M7V-09, M7V-79, M7V-55, M7V-56, Q-35, Q-38 |
| **V-R20 (2)** (T-23) | header `provenance` routed to foundation as the F18 C0 amendment — **satisfied**: landed at `6893442` exactly as §8.1 drafted it | `trace-requirements.md` §8.1 | VA-1, VA-7 `trace_header`, M7V-46 (un-parked), §12 |
| **V-R20 (3)** (T-28) | "every invariant whose packages are all `Wired` has `seeds_armed > 0` on the default corpus" | `design.md` §2.4 / §5 | **M7V-89**, VA-2, §12 last row, §13 |
| **V-R20 (4)** (T-35) | gating table keyed per `FaultKind` family on the emitting provider package | `design.md` §3.1 / §6; foundation's handoff names the emitter per member | VA-6, M7V-55, M7V-56, M7V-73 |
| **V-R20 (5)** (CR-1, CR-2) | `reason`: ADR form `capability(<pkg>)` in the artifact, `reason` + `package` on the log line; `catching_row` list-valued, key unchanged | ADR-rdb-0019 §2; `design.md` §5.3 | VA-7, M7V-72, Q-34 |
| **V-R20 (6)** (T-24) | `armed()` is end-of-fold state; `proven` and `seeds_armed` count per-seed `Proven`; M7V-31/33 assert `armed() == false` | `design.md` §2.4 | VA-2 (incl. the fold order, T-34), VA-7, M7V-31, M7V-33, M7V-52, M7V-78 |
| **V-R20 (7)** (T-25) | the required-cell gate applies only when `SPIKE_SEEDS >= N`; smaller corpora write `coverage_gated: false` | `design.md` §3.1 / §6 | VA-6, VA-7 `campaign_run`, §2, M7V-55, M7V-57, M7V-58, M7V-61, M7V-63, M7V-64, M7V-65, M7V-73, M7V-75, M7V-76, Q-38 |
| **V-R20 (8)** (T-26) | M7V-03(b) = header + ten `capability{Wired}`; `TraceBuilder::ack_from(n, s)` emits the secondary `batch_apply` | `design.md` §4 convention 4, §4.5, §2.4 "zero-event" | VA-1, §4 convention 4, M7V-03, M7V-07..M7V-11, M7V-28, M7V-79, M7V-81, M7V-88 |
| **V-R21** (lead Q-1, K-F-07) — **superseded by F-R13** | V-R21 allowed a landed `quorum_rule` to be cross-checked against the derived value. F-R13 (2026-09-20 22:06 PDT) adjudicated K-F-07 against V-R20 and ruled that **no such field will exist**: K-F-07 is closed by derivation, foundation's `ProtectionState` at `6893442` has none, and two sources of truth for one fact are not kept. The cross-check row **M7V-90 is withdrawn** and `quorum_rule_mismatch` with it; V-R21's other half (INV-VER's exclusion) is untouched | `design.md` §2.3 | §4 convention 1 (rewritten), §12 (row removed), §13 V3 row, drift row 2 |
| **V-R21** (lead Q-2, INV-VER) | the wired ⇒ `seeds_armed > 0` clause runs over nine invariants; INV-VER is excluded by name until a producing `ScenarioOp` or `BoundaryId` exists; the excluded set is listed in design §2.4 | `design.md` §2.4; ADR-rdb-0019 rule 1 (`37e85a5`) | M7V-78, M7V-89 |
| T-27, T-29..T-33 (no ruling needed) | Q-35 runnable; §12 table repaired; Q-34 projects `package`; §2 sharer list and misnomer; M7V-62 selector-only; M7V-87 extraction rule | — | Q-35, §12, Q-34, §2, M7V-53, M7V-62, M7V-65, M7V-87 |

**Drift table — `trace-requirements.md` §3 as this plan cited it vs. the landed C0 at `f616ddf`
(critic T-23; re-read in full 2026-09-20 round 5, and again row by row 2026-09-21 against
`f616ddf`).** Read from `git show f616ddf:crates/rdb-core/src/contracts/{trace,ids,errors,envelope,event}.rs`,
not the working tree — three other teams are editing this checkout.
Row numbers match `trace-requirements.md` §8's table so the two can be read side by side.
Disposition is **plan edit** (this plan changed) or **foundation ask** (the contract is still
requested). Everything not listed below was already written to the landed name.

**The basis has now moved three times, and the whole table was re-read against the newest one
each time, not just the rows that were flagged.** `8a23b1d` → `6893442` (foundation's first code
round) → `ec610f4` (K-F-39) → **`f616ddf`** (CB-1..CB-4, 2026-09-21). At `ec610f4` six rows had
moved: **1** and **21** landed and are no longer asks; **6**, **8** and **10** gained or changed
fields; **12** landed the withdrawal it recorded as refused.

**At `f616ddf` exactly one existing row moved — row 9′, the `AckRejectReason` set — and four
new rows (23–26) record contract surface this plan did not previously cite.** Rows 1–22 were
re-read individually against `f616ddf` on 2026-09-21: `f616ddf` touched only `trace.rs`,
`envelope.rs` and `event.rs` in the contract surface (`git show --name-only f616ddf`), and inside
`trace.rs` it touched only `AckRejectReason`, so rows 1–8, 10–22 name fields the commit did not
go near and still hold verbatim. **Verification's open-ask count is still zero.** The marker
`<!-- drift-basis: f616ddf -->` below is set on the strength of that re-read and on nothing else.
A marker set without it silences `scripts/drift-check.sh` instead of satisfying it, which is
worse than the stale table it replaces.

**Correction, 2026-09-22 (drift re-read): "still hold verbatim" was true of the field names and
false of the line numbers, and the way it was false is worth naming.** The 2026-09-21 reading
above is right that `f616ddf` touched only `AckRejectReason` inside `trace.rs`. What it did not
follow through is that widening an enum at `trace.rs:315` **added 37 lines**, so every citation in
this table *below* that point moved by 37 while the field it names stayed put. Three did, and were
carried at their `ec610f4` values:

| Cited here | Actual at `f616ddf` | Row |
|---|---|---|
| `AckEvidence` `trace.rs:606` | **`:643`** | 6 |
| `SkipReason` `trace.rs:634` | **`:671`** | 21 |
| `TraceKind::OpSkipped` `trace.rs:1115` | **`:1152`** | 21 |

All three are corrected in place below. A fourth is wrong for an unrelated reason and predates
this basis: row 8 cites `TraceHeader.partitions: u8` at `trace.rs:214`, and the field is at
**`:216`** at `f616ddf` *and* at `ec610f4` — `:214` is `pub config: RunManifest`. Also corrected.

**This is the drift stage's blind spot in its purest form, and it points the opposite way from the
one AGENTS.md warns about.** The stage compares the marker, and the marker was right. The prose
that licenses the marker said the whole table was re-read; what was re-read was the *set of field
names*, which a `grep` confirms without ever opening the file at a line. Nothing here
**over-held** — no row was gated on a contract that had landed, and verification's open-ask count
really is zero — so the usual tell was absent. The cost is the next reader opening `trace.rs:1115`,
finding a doc comment about roles, and having to decide whether the plan or the code is wrong.
Cite a span you opened; a field name that survives a commit is not evidence that its line did.

Two smaller things this re-read found and did not change: the paragraph above says "four new rows
(23–26)" where the table carries **five** (23–27), and row 27's own text is what makes 27 real.
Both are §15's, and both are left for whoever next edits the sentence rather than patched under a
drift re-read that did not re-derive the count.

| # | Field, as the plan wrote it | Landed shape at `f616ddf` | Disposition | Rows changed |
|---|---|---|---|---|
| 1 | header `provenance: Provenance` | **landed exactly as asked**, at `6893442`: `Provenance{Generated{seed: u64} \| Reduced{parent: ScenarioId} \| Authored{case: String}}` (`trace.rs:85`), `TraceHeader.provenance` (`trace.rs:212`), `ScenarioId(u64)` (`ids.rs:116`), and **no `TraceHeader.seed`**. §8.1's draft and the landed enum agree arm for arm and derive for derive | **ask closed, plan edit**: the plan writes `provenance`, never `seed`; the reduced arm's field is `parent`, not the `from` the row had. M7V-46 un-parked from §12 — it was held for four rounds against `8a23b1d` on a type that shipped at `6893442` | VA-1, VA-3, VA-7 `trace_header`, M7V-46, §12 |
| 2 | `protection_state{quorum_rule=Rf3 \| DegradedRf2}`; a `QuorumRule` enum | no field; no enum anywhere in `rdb-core`, and **there will never be one** — lead ruling **F-R13** (2026-09-20 22:06 PDT) adjudicated K-F-07 against V-R20: K-F-07 is closed *by derivation*, and foundation's committed `ProtectionState` at `6893442` carries no such field | **plan edit, now final**: derived from `required_copy_set.len()` (2 → `DegradedRf2`, 3 → `Rf3`, **any other length a violation** — `required_copy_set_shape`, T-39), V-R20 (1); the cell is `derived_quorum_rule × DegradedRf2` and the axis is verification's own two-member `QuorumRule` enum in `coverage.rs`, which stays and is not a contract type. **Withdrawn with the field:** row `M7V-90` and the violation `quorum_rule_mismatch`, both written in round 2 under V-R21 for the case where the field landed. The id is retired, not reused | §4 conv. 1, M7V-07 (+ shape sub-case), 08, 09, 79, M7V-55, M7V-56, Q-35, Q-38 |
| 3 | `protection_state{state=…}` | `phase: ProtectionPhase{Healthy, Warn, Paused, Resuming}` | plan edit | VA-2, M7V-36, M7V-39, M7V-56 (`ProtectionPhase`) |
| 4 | `replication_ack.peer_boot_id` | `peer_boot: BootId` | plan edit | §4 conv. 1, M7V-28, Q-35 |
| 5 | `peer_role: PeerRole = Regular \| Shadow` | `peer_role: ReplicaRole{Primary, RegularSecondary, Shadow}` | plan edit: every `Regular` → `RegularSecondary` | §4 conv. 1, M7V-07, 08, 09, 10, 11, 28, 69, 81 |
| 6 | `ack_evidence: [(node, boot, role, durability)]` | **moved since `8a23b1d`**: `Vec<AckEvidence{node, boot: BootId, role: ReplicaRole, durability}>` (`trace.rs:643`; cited as `:606` until the 2026-09-22 correction above — that is its `ec610f4` line). The boot was added at `6893442` under foundation's finding K-F-22, citing verification's own §3.7 four-tuple — the plan had recorded "no boot" and worked around its absence | **plan edit, reversed**: the workaround is withdrawn. Row literals are **four**-field and write `boot=`; the boot must equal the `peer_boot` on that node's paired `replication_ack` at the same `seq`. This is what makes "one node, two boots, counted as two copies" catchable, and **no row asserts it yet** — flagged to the lead as a candidate row `M7V-91` rather than written here, because a new row belongs in a planning round with a critic | §4 conv. 1, M7V-07, 08, 09, 79, 81 |
| 7 | `topology_change.nodes=[n1:Primary, …]` | `Vec<(NodeId, ReplicaRole)>`, a tuple → JSON two-element array | plan edit: `(n1, Primary)` literals; DuckDB `n[1]`, `n[2]` | M7V-08, M7V-10, Q-35, VA-7 |
| 8 | header `topology{nodes, config_version_0, partitions}` | flat `Vec<TopologyEntry{partition, node, role, config_version}>` — field order is `(partition, node)`, the derived `Ord` sort order (K-F-38) — **and `TraceHeader.partitions: u8` now exists** (`trace.rs:216`; cited as `:214` until 2026-09-22, which is `pub config: RunManifest`), added at `6893442`; the plan had recorded "no `partitions` field; derive it from the distinct `partition` values" | **plan edit, half reversed**: adopt the flat `Vec<TopologyEntry>` as before, but **stop deriving `partitions`** — the header states it, and a derived count that disagrees with the stated one is a fixture defect worth seeing rather than silently papering over. V-R8's two-partition topology reads the field | VA-1, §4 conv. 1, VA-7 `trace_header`, M7V-10, M7V-32, Q-35 |
| 10 | header `config` (§1) | **moved since `8a23b1d`**: `config_digest: Digest` and `budgets: Budgets` are **gone**; the header carries `config: RunManifest{budgets, overridden: Vec<BudgetName>, nodes: u8, event_cap: u32}` (`trace.rs:187`, `:213`) | **plan edit**: VA-1 and VA-7's `trace_header` line name `config`, not `config_digest`. `overridden` is the field that matters to this plan — it is the trace's own record of which budgets a run did not take from `Budgets::SPEC_DEFAULTS`, which is the `RETCD_TEST_DEADLINE_SCALE` hazard AGENTS.md warns about, in the artifact rather than in someone's memory. **No row asserts `overridden` yet**; flagged to the lead with drift row 6 | VA-1, VA-7 `trace_header` |
| 9 | `admission_decision.reason: AdmissionReason` (`PROTECTION_PAUSED`, …) | `reason: Option<ErrorKind>` (`ProtectionPaused`, `RequestIdReuse`, `CrossAffinity`, `GenerationChanged`, …) | plan edit: real variants; the admission axis is the `ADMISSION_REASONS` subset const, pinned by M7V-25/39 | M7V-25, M7V-39, M7V-56 |
| 14 | `client_submit.affinity_id` | `affinity: u64` | plan edit | M7V-25 |
| 15 | `client_outcome{outcome=UnknownOutcome}` (bare error name) | `ClientOutcome::{Success, RecoveredApplied, Error(ErrorKind)}` | plan edit: `Error(UnknownOutcome)`, `Error(StatusExpired)` | M7V-12, M7V-26 |
| 16 | a `status` event | one `Read{request_kind: ReadRequestKind}` kind; `Status` is a value | plan edit | M7V-26 (M7V-06 already used `request_kind`) |
| 17 | `schedule_phase{…}` | `SchedulePhaseChanged{phase, fair_delivery, remaining_event_budget}` | **plan shorthand kept** for the row literal; `@m` is `schedule_phase_changed`; no Q-row reads it (§4 conv. 1) | §4 conv. 1, VA-7 |
| 15′ | `client_outcome{…}` | `ClientOutcomeReported{request, outcome, generation, seq, result_digest, delivered}` | **plan shorthand kept**; `@m` is `client_outcome_reported`; no Q-row reads it | §4 conv. 1, VA-7 |
| 21 | `op_skipped{scenario_op_index, reason=ReferentGone}` | **landed as asked**, at `6893442`: `TraceKind::OpSkipped{scenario_op_index: u32, reason: SkipReason}` (`trace.rs:1152`; cited as `:1115` until 2026-09-22) with `SkipReason{ReferentGone, OutOfBudget}` (`trace.rs:671`; cited as `:634` until 2026-09-22), its own kind rather than a `BoundaryId` member, rustdoc citing K-F-08 and §3.16a. The index is `u32` where §3.16a wrote `usize` — the same adoption as row 18 | **ask closed, plan edit**: M7V-22 and M7V-88's `op_skipped` clause un-parked from §12. M7V-88's "replays with no `op_skipped{ReferentGone}`" clause is **no longer vacuous**. M7V-22 still needs **I1** and stays in §12's I1 block; it is a runner dependency, not a contract one | M7V-22, M7V-88, §12 |
| 22 | envelope `node_id`, `correlation_id`, `partition_id` | `TraceEvent{event_id, logical_tick, partition, node, boot, correlation}` | plan edit: row literals write `node=`; Q-35..Q-37 read the landed names so they are runnable | §4 conv. 1, VA-7, M7V-07..11, 28, 36, 41, 79, Q-35, Q-36, Q-37 |
| 12 | `batch_apply.state_digest_after` and `publish.published_state_digest` withdrawn (§6) | **moved since `8a23b1d`**: both fields are **deleted** (lead ruling **F-R9**, landed at `6893442`). The plan had recorded them as present and ignored them | adopt: the withdrawal this plan asked for is now the contract. No row or query read them, so nothing here changes except this line — recorded so the next reader does not go looking for a field the table says is present | — |
| 11, 13, 18, 19, 20 | `sync_wal_through_prefixes`, `batch_id`, `scenario_op_index: usize`, `Vec<FieldId>`, `replication_ack_delivered{ack_event_id, accepted}` | `captured`, `batch: u64`, `u32`, `Vec<u16>`, `{ack: EventRef, from_node, peer_role, config_version, counted}` — all unchanged since `8a23b1d` | adopt — no row or query in this plan reads them by the old name | — |
| 23 | `replication_ack.reject_reason: Option<AckRejectReason>`, a **closed set of seven** (`trace-requirements.md` §3.5) | **widened to fourteen at `f616ddf`** — ask **CB-3**, lead ruling **B-R33 Q-B-8** (`trace.rs:315`). The seven added: `StaleGeneration` (`:331`), `RoleMismatch` (`:333`), `InconsistentProgress` (`:335`), `RegressedProgress` (`:337`), `Unverifiable` (`:339`), `Diverged` (`:341`), `NotAMember` (`:343`) | **plan edit, and the debt paid — §15.1.** This is the one row of this table the `f616ddf` delta falsified, and it falsified it *on purpose*: `M7V-56` asserts set equality, so the widening turned it red, which is the row working (F-R20). The response is **not** weakening the row to a subset. §15.1 writes one cell per added variant, names the producer and the covering row for each, and records **five of the seven as gaps with owners** rather than closing the set with empty cells. Two of the fourteen names, `StaleGeneration` and `NotAMember`, also exist on `envelope::AppendReject` with a different meaning (drift row 24), so every literal here is **enum-qualified** | §15.1, VA-6, M7V-55, M7V-56, M7V-70, M7V-80, `trace-requirements.md` §3.5 |
| 24 | `envelope::AppendReject` — **not cited by any row in this plan** | `NeedPrefix` gained `head_digest: Digest` (ask **CB-2**, `envelope.rs:524`); still **16** variants | adopt — verification reads the *trace*, never the wire envelope; grepped at `f616ddf`, no row literal, expectation or Q-row in this plan names `AppendReject`. Recorded anyway because **two of its variant names collide with `AckRejectReason`'s** — `StaleGeneration` (`envelope.rs:534`) and `NotAMember` (`envelope.rs:568`) — and the collision is deliberate (kernel-b §15 CB-3: "one concept, two paths"). The consequence for this plan is a naming rule, not a row change: qualify both names | — |
| 25 | `envelope::AppendOutcome` — **did not exist** | new at `envelope.rs:613` (ask **CB-4**): `Accepted(AppendAck)`, `Busy{accepted_through}`, `AlreadyHave`, `ProbeDigestAt{seq}`, `Rejected(AppendReject)` — one enum, not `Result` | adopt — a wire type, not a trace type; no row reads it. **Flagged to the next planning round, not written as a row here:** `Busy` and `AlreadyHave` are answers that are neither an acceptance nor a rejection, so an oracle fold of the form "not `accepted` ⇒ rejected" would mis-read them once R1 emits them. No fold in this plan does today (INV-PUB reads `replication_ack_delivered.counted`, §3.5), so nothing is wrong now — this is a note for whoever writes the R1-era fold | — |
| 26 | `EventKind` / `EffectKind` — **neither is named anywhere in this plan** | `EventKind` gained `Kernel(KernelEvent)`, **7 → 8** (`event.rs:153`); `EffectKind` gained `Kernel(KernelEffect)` (`event.rs:313`); both inner enums are `#[non_exhaustive]`, carrier owned by foundation and variants by kernel-b (ask **CB-1**) | adopt — grepped at `f616ddf`: no row, VA, Q-row or literal in this plan asserts an exhaustive match or a variant count over either, and M7V-56's enumerated lists do not include them, so the widening falsifies nothing here. **Recorded as a trap for later:** `#[non_exhaustive]` means a set-equality assertion over `KernelEvent`/`KernelEffect` is **not** expressible from outside `rdb-core`, so a future row that wants M7V-56's treatment for them cannot have it and must say so rather than discover it.<br>**Drift re-read 2026-09-22 — still adopt, and the trap grew a second half.** Re-read at `f616ddf`: `EventKind` eight (`event.rs:153-191`), `EffectKind` seven (`:313-347`), both carried enums `#[non_exhaustive]`, exactly as written. Ask **CB-7** is now in `crates/rdb-core/src/contracts/` **uncommitted** and adds a **new contract module**, `contracts/ignore.rs`, carrying `KernelIgnoredReason` (`:77`, five arms, `#[non_exhaustive]`) and `ReplicaIgnoreReason` (`:106`, twelve variants, `#[non_exhaustive]`), plus `AuthorityIgnoreReason` in `contracts/authority.rs` (27 variants, `#[non_exhaustive]`); `KernelEffect::Ignored`'s reason is retyped from `ErrorKind` to the carrier, and the `Copy` derive is dropped from both carried enums. **Disposition is unchanged — adopt.** Grepped 2026-09-22: no row, VA, Q-row or literal in this plan names `KernelIgnoredReason`, `ReplicaIgnoreReason` or `AuthorityIgnoreReason`, and none reads `Ignored`'s reason; this plan reads the **trace**, and none of these types is a trace field. `ErrorKind` itself is untouched at eighteen variants (`errors.rs:79`), so **drift row 9's `admission_decision.reason: Option<ErrorKind>` and its `ADMISSION_REASONS` subset are not affected** — which is the one place a retyping of `ErrorKind` *would* have reached this plan, checked rather than assumed. **The trap's second half:** three more `#[non_exhaustive]` enums exist that a future row cannot assert set equality over from outside `rdb-core`, and two of them (`ReplicaIgnoreReason`, `AuthorityIgnoreReason`) are designed to be appended to by a kernel team without a foundation edit — so unlike `AckRejectReason` at drift row 23, a widening will arrive with **no contract commit that moves this marker and no ask to notice**. A row that wants M7V-56's treatment for them has no supported way to get it. **Marker unmoved:** CB-7 is not committed, so `git log -1 -- crates/rdb-core/src/contracts` is still `f616ddf` | — |
| 27 | `crates/rdb-core/src/authority.rs` — **no line number in this section cites it** | rewritten at `f616ddf` (+302/−16): `Authority` is now a stateful struct (`authority.rs:75`), new `AuthorityState` (`:63`), `WATCH_ADMISSION_ATTEMPT_CAP = 3` (`:56`), a real `step` and a real `capability()` | adopt — and note it is **outside the drift surface**: `scripts/drift-check.sh` watches `crates/rdb-core/src/contracts`, and `authority.rs` sits beside that directory, not in it, so a change to it moves no marker. Checked by grep: this section cites no line number in it, and M7V-82 reaches `capability()` through the `Module` trait rather than through this file | — |
| 28 | `TraceKind::{ModuleDispatch, KernelNoted}` and their payload types `DispatchOutcome`, `KernelNote` — **new at `9235bfb`, cited by no row in this plan** | `TraceKind::ModuleDispatch { event, module, outcome: DispatchOutcome }` and `TraceKind::KernelNoted { event, module, note: KernelNote }` (`trace.rs:1251`, `:~1260`), both **appended at the end of the enum by design** (lead ruling A-R46, so as not to shift any existing `TraceKind` citation). `DispatchOutcome` (`trace.rs:1315`) is **3** variants: `Answered{effects: u32}`, `Declined`, `Errored{..}`. `KernelNote` (`trace.rs:1347`) is **5** variants: `Ignored{reason: KernelIgnoredReason}`, `Alert{reason: ErrorKind}`, `AuthorityFact{..}`, `SetAdmission{..}`, `ProtectionWarn{..}` | adopt — grepped at `9235bfb`: no row, VA, Q-row or literal in this plan names `ModuleDispatch`, `KernelNoted`, `DispatchOutcome` or `KernelNote`. Same disposition as rows 24–26: a real trace surface with no consumer here yet, recorded so a future row that wants dispatcher-outcome or kernel-note coverage finds the shape already written down rather than re-deriving it | — |

**Round-8 re-read (2026-09-26), basis moves `f616ddf` → `9235bfb`.** `9235bfb` is the newest
commit touching `crates/rdb-core/src/contracts`. Every citation below was re-opened with
`git show 9235bfb:<path>`, not the working tree. CB-7 (drift row 26) is now committed exactly as
its 2026-09-22 working-tree note described, and one further shift landed on top of it: `trace.rs`
gained four new `BudgetName` members near the top of the file, pushing everything below them down
a uniform **+31** lines. This is a *different* mechanism from the CB-3 +37 shift row 23 already
corrected once (a single point-shift below one line, not an offset that grows with distance from
it), and it is **not** the same shift as kernel-a/kernel-b's `event.rs` piecewise shift — the two
files move independently.

- **Row 1** — `TraceHeader.provenance` is `trace.rs:243` (was `:212`). Shape and field names
  unchanged; +31 only.
- **Row 2** — re-confirmed, not just carried: `ProtectionState` (the struct itself, for whoever
  next cites it) is now at `trace.rs:1094-1109`; it is still **7** fields and still has **no**
  `quorum_rule` field. F-R13's ruling stands.
- **Row 6** — `AckEvidence` is `trace.rs:674` (was `:643`). Still `Vec<AckEvidence{node, boot,
  role, durability}>`, four fields, unchanged.
- **Row 8** — `TraceHeader.partitions: u8` is `trace.rs:247` (was `:216`). Still present, still
  a stated field, not derived.
- **Row 9 — checked, not falsified.** `ErrorKind` (`errors.rs:81`, was `:79`) grew from
  eighteen to **19** variants — the new one is `DivergenceRequiresOperator`, kernel-a's client
  answer for A1's authority-side denial (drift table, foundation and kernel-a's plans). This
  plan's `ADMISSION_REASONS` const (`crates/rdb-sim/tests/support/scenarios/coverage.rs`) is
  re-read at `9235bfb` and is **still the same seven entries** it was at `f616ddf` — it does not
  include `DivergenceRequiresOperator`, and nothing in this plan asserts set equality over
  `ErrorKind` itself (only over the admission-reason subset), so row 9's disposition is
  unchanged. Recorded because a growing `ErrorKind` is exactly the kind of near-miss this row's
  own subset framing exists to survive, and the discipline is to check that, not assume it.
- **Row 21** — `SkipReason` is `trace.rs:702` (was `:671`); `TraceKind::OpSkipped` arm is
  `trace.rs:1183` (was `:1152`). Both unchanged in shape.
- **Row 23** — `AckRejectReason` is `trace.rs:346` (was `:315`), still **14** variants; §15.1's
  seven cells are unaffected — none of their line citations depend on this enum's absolute
  position, only its variant names, which did not move.
- **Row 26 — counts updated, and both are now committed, not working-tree.** `EventKind` is
  still **8**, `EffectKind` still **7** (unchanged since `f616ddf`; the widening that round
  described was `Kernel(KernelEvent)`/`Kernel(KernelEffect)` landing, not further growth).
  `KernelIgnoredReason` (`contracts/ignore.rs:79`) is still **5** arms, exactly as the
  working-tree note said. `ReplicaIgnoreReason` (`ignore.rs`) is now **19** variants (the
  working-tree note said twelve — seven more landed with the rest of kernel-b's L1/R1/F1
  vocabulary in the same commit, not further CB-7 growth). `AuthorityIgnoreReason`
  (`contracts/authority.rs`) is now **35** variants (the working-tree note said twenty-seven).
  **Disposition unchanged — adopt**, same reasoning as before: no row in this plan asserts set
  equality over any of the three, so the growth falsifies nothing here. The trap the round-6/7
  notes named is now realized rather than predicted: both counts moved between the note and this
  commit with no ask and no marker to notice, which is the trap's whole point, restated as
  something that has now actually happened once.
- **New row 28, below.** `TraceKind` gained two variants at `9235bfb`, appended at the enum's
  end by design so no existing `TraceKind` citation shifts.

Nothing above lowers an assertion or reopens a closed ask. Row 9 is confirmed unaffected rather
than assumed unaffected; rows 1/6/8/21/23 are line-number corrections only; row 26's counts are
promoted from working-tree to committed. Marker moved below after this re-read, not before it.

**Round-9 re-read (2026-09-26), basis moves `9235bfb` → `3249092`.** `3249092` ("M7 checkpoint —
T1, P1, B-R58 route, tracker and verification rows") is the newest commit touching
`crates/rdb-core/src/contracts` (`git diff --stat 9235bfb 3249092 -- crates/rdb-core/src/contracts`
touches `authority.rs`, `digest.rs`, `event.rs`, a new `publication.rs`, `trace.rs`, `transport.rs`,
`txn.rs`). This is T1/P1's wave. Every citation below was re-opened with `git show 3249092:<path>`,
not the working tree. `trace.rs` moves again, and by a **different** mechanism than either prior
shift: one new `AckRejectReason` variant, `InFlightUnverified`, is inserted **inside** the enum
row 23 already tracks, at `trace.rs:374` — this is a single-point shift of **+4** for everything
below it, not the uniform whole-file +31 the `BudgetName` insertion caused, and not the piecewise,
multi-point shift `event.rs` carries in kernel-a's and kernel-b's tables.

- **Row 1** — `TraceHeader.provenance` is unaffected: `trace.rs:243`, same as round 8. The
  insertion point (`AckRejectReason`, `:346`) is below it, so nothing above line 346 in this file
  moves.
- **Row 2** — `ProtectionState` unaffected: not in this diff (`trace.rs` region below `:346`
  containing it — `:1094` at round 8 — is confirmed re-open at `3249092`: `trace.rs:1098` (+4),
  still 7 fields, still no `quorum_rule`). F-R13 stands.
- **Row 6** — `AckEvidence` is `trace.rs:678` (was `:674`, +4). Still four fields, unchanged.
- **Row 8** — `TraceHeader.partitions: u8` is unaffected: `trace.rs:247`, same as round 8 — above
  the insertion point.
- **Row 9** — `ErrorKind` (`errors.rs`) is **unaffected**: `errors.rs` is not in this diff.
  `ADMISSION_REASONS` needs no re-check this round; nothing about the type it subsets moved.
- **Row 21** — `SkipReason` is `trace.rs:706` (was `:702`, +4); `TraceKind::OpSkipped` arm is
  `trace.rs:1187` (was `:1183`, +4). Both unchanged in shape.
- **Row 23 — the enum itself widens, and this is the row that matters most this round.**
  `AckRejectReason` (`trace.rs:346`, unchanged start — the insertion is inside the enum, not above
  it) gains an **eighth** reject reason beyond the fourteen CB-3 landed: `InFlightUnverified`
  (`trace.rs:374`), inserted between `Unverifiable` and `Diverged`, doc'd "evidence below the
  primary's anchor, for the record a catch-up cursor has in flight (lead ruling B-R58c). No rung
  can check it yet, so it drives the cursor and moves no watermark; the ACK at the anchor verifies
  the whole chain below it." **The enum is now fifteen variants, not fourteen.** `M7V-56`'s
  set-equality assertion is falsified again, by design, the same way CB-3's widening falsified it
  at `f616ddf` — the row working, not breaking (F-R20 still governs: no weakening to a subset).
  **§15.1 is rewritten below** to seat this eighth cell and to fix seven citations that were never
  corrected through two prior line-shifts.
- **Row 26** — `EventKind`/`EffectKind` unchanged at 8/7 (not in this diff's carrier growth for
  those two top-level enums). `KernelIgnoredReason` (`contracts/ignore.rs`) and
  `ReplicaIgnoreReason` (`ignore.rs`) are **unaffected** — `ignore.rs` is not in this diff, so both
  stay at 5 arms / 19 variants. `AuthorityIgnoreReason` (`contracts/authority.rs:686`) is now
  **42** variants (was 35) — seven more, all T1/P1-facing (`NotForThisCandidate`,
  `QualificationLost`, `RecheckOutstanding`, `ReadViewNotPublished`,
  `RecoveredGenerationNotNewer`, `RetireNewerGeneration`, `RetireServedGeneration`). Disposition
  unchanged — adopt: no row in this plan asserts set equality over it, so growth here falsifies
  nothing. The trap named at round 7/8 fires a second time, on the same enum, the same way.
- **Row 28** — `TraceKind::{ModuleDispatch, KernelNoted}` are appended at the enum's end by
  design (lead ruling A-R46) and this diff does not touch that region of `trace.rs`, so those two
  citations are unaffected. Their payload types widen, though, and this row's own text is now
  stale about both: `DispatchOutcome` (`trace.rs:1319`) gains a fourth variant,
  `DeclinedOwed` (`:1343`) — an owed edge distinct from `Declined`'s "not mine" — so it is **four**
  variants, not three. `KernelNote` (`trace.rs:1358`) gains **eight** variants —
  `RecoveryFact`, `PublicationFact`, `SurvivorPlaced`, `SyncWithheld`, `SyncProven`,
  `RecoveredFact`, `RecoveredDeferred`, `RecoveredLanded` — so it is **thirteen**, not five, plus
  two new companion enums this row did not previously name, `RecoveredDeferReason` (`Crashed`,
  `CutOff`) and `SyncWithheldReason` (`NotPlaced`, `Failed`, `Short`, `NoDigest`; `Stalled` appended 2026-09-27 by ruling B-R70, M7B-156). Disposition
  unchanged — adopt: grepped at `3249092`, no row, VA, Q-row or literal in this plan names
  `DispatchOutcome`, `KernelNote`, `DeclinedOwed`, `RecoveredDeferReason` or `SyncWithheldReason`,
  so nothing here is falsified, only described short. Recorded so the eventual dispatcher-outcome
  or kernel-note row opens the four/thirteen-variant shape rather than the three/five this row
  used to say.

Nothing above lowers an assertion. Row 23 reopens `M7V-56` red again, by design, the same way CB-3
did; §15.1 is rewritten to match. Rows 1/6/8/21 are line-number corrections; row 26 and row 28
restate two enums' counts that grew since they were last written here. Marker moved below after
this re-read, not before it.

**Round-10 re-read (2026-09-27), basis moves `3249092` → `bc8b45e`.** `bc8b45e` ("M7 checkpoint —
R1 recovery source, lost-ACK retransmit, repeat handling, timer-id blocks") is the newest commit
touching `crates/rdb-core/src/contracts`. `git diff --stat 3249092 bc8b45e --
crates/rdb-core/src/contracts` touches exactly one file, `event.rs` (+36/-1) — kernel-b's F1
recovery-source wave. Every citation below was re-opened with `git show bc8b45e:<path>`, not the
working tree.

- **`trace.rs`, `errors.rs`, `authority.rs` and every other file this table cites are untouched by
  this diff.** Rows 1, 2, 6, 8, 9, 21, 23 and §15.1's eight cells all cite `trace.rs` or
  `errors.rs`; none of those files moved, so none of those citations move. Row 27
  (`crates/rdb-core/src/authority.rs`) is outside `contracts/` by construction, same disposition
  as every prior round.
- **Row 26 — the trap fires a third time, and stays a non-event for this plan.** `EventKind` and
  `EffectKind` are unaffected at the top level — still eight and seven; this diff adds two
  variants, `KernelEvent::CatchUp{from, to, through, credential}` and
  `KernelEffect::SendRecoveryEnvelopes{copy, from, through, credential}`, both inside the
  `#[non_exhaustive]` carriers this row already named as unassertable-by-set-equality from outside
  `rdb-core`. `KernelEvent` goes 20 → 21 variants, `KernelEffect` 23 → 24. Grepped at `bc8b45e`: no
  row, VA, Q-row or literal in this plan names `CatchUp`, `SendRecoveryEnvelopes` or
  `FenceCredential` — this plan reads the *trace*, and neither new variant is a trace field.
  Disposition unchanged — **adopt**.
- **No new `TraceKind` variant, no new trace field.** This diff does not touch `trace.rs`, so row
  28's `ModuleDispatch`/`KernelNoted` citations and §15.1's `AckRejectReason` cells are all
  unaffected — confirmed by the diff's file list, not assumed.

Nothing above lowers an assertion or reopens a closed ask, and nothing here unblocks a verification
row — this commit is kernel-b's own internal recovery-source vocabulary, and this plan has never
cited it. Marker moved below after this re-read, not before it.

**Round-11 re-read (2026-09-27, lead), basis moves `bc8b45e` → `87e681a`.** `87e681a` is now the newest commit touching `crates/rdb-core/src/contracts`. `git diff --stat bc8b45e 87e681a -- crates/rdb-core/src/contracts` lists `trace.rs` only, +6 −1: `SyncWithheldReason::Stalled` is appended last (ruling B-R70, M7B-156) and the `NoDigest` doc is reworded (placed history **and** the engine's stored record). No declaration moved; only lines below that enum shift. A grep of this plan for `trace.rs` line citations at or below `:1519` finds none, so no citation rotted. Nothing here lowers an assertion or reopens an ask. Marker moved after this re-read, not before it.

**Round-12 re-read (2026-09-27, lead), basis moves `87e681a` → `c24bc20`.** `c24bc20` is now the newest commit touching `crates/rdb-core/src/contracts`. `git diff --stat 87e681a c24bc20 -- crates/rdb-core/src/contracts` lists `event.rs` only, +9 −5, and every changed line is a `///` doc comment on `KernelEffect::SendEnvelopes` (its producers are now the catch-up cursor, the stream, the keepalive and the retransmit — B-R67i; the `copy` field reads "The copy to send to"). No type, variant or field changed. The hunk sits at `event.rs:553`, so every line below it shifts +4. A grep of this plan for `event.rs` line citations at or below `:553` finds none, so no citation rotted. Nothing here lowers an assertion or reopens an ask. Marker moved after this re-read, not before it.

**Round-13 re-read (2026-09-27, lead), basis moves `c24bc20` → `b723b6a`.** `b723b6a` is now the newest commit touching `crates/rdb-core/src/contracts`. `git diff --stat c24bc20 b723b6a -- crates/rdb-core/src/contracts` lists `trace.rs` only, +12 −6, all inside `TraceKind::RecoveryDecision`: `selected_cutoff_seq`, `selected_digest` and `new_generation` become `Option<_>`, `None` exactly when `mode` is `Quarantine` and `Some` otherwise (Gautam, L-R177gd), and the variant's doc comment states the rule. No variant was added or removed and no declaration was renamed. The hunk starts at `trace.rs:1058`, so every line below it shifts +6. This is this plan's own ask, already described above (M7V-80 note, V-R28/V-R29), and it landed as described: M7V-80 asserts the three fields are `None` on the quarantine, and the lineage check reads them as options. The §10 diagnosis query that joins `recovery_decision` to show `selected_cutoff_seq` is a reading aid, not an assertion; on a quarantine that column is null, which is the rule. A grep of this plan for `trace.rs` line citations at or past `:1058` finds them only in dated re-read notes and in M7V-22/M7V-88's un-park row, which is anchored to `6893442`; all are history and left as written. Nothing here lowers an assertion or reopens an ask. Marker moved below after this re-read, not before it.

**Round-14 re-read (2026-09-28, lead), basis moves `b723b6a` → `56f952d`.** `56f952d` is now the newest commit touching `crates/rdb-core/src/contracts`. `git diff --stat b723b6a 56f952d -- crates/rdb-core/src/contracts` touches four files: `authority.rs` (+4/−1, doc comment only, on kernel-a's `AuthorityIgnoreReason::UnmatchedCompletion`, unread by this plan); `control.rs` (+24/−3: `ControlEffect::Cas`/`Get` and `ControlEvent::CasResult`/`Value` each gain a `request: ControlRequestId` field), also unread here — this plan reads the trace, never the control wire types, and grepping this plan for `ControlEffect`/`ControlEvent`/`CasResult` finds nothing; `ignore.rs` (+10/−1: `ReplicaIgnoreReason` gains `UnmatchedCompletion`), likewise unread — this plan's drift row 26 already records that `KernelIgnoredReason`/`ReplicaIgnoreReason`/`AuthorityIgnoreReason` are read by no row, VA, or Q-row here, and a widening of an already-unread `#[non_exhaustive]` enum falsifies nothing (same disposition as every earlier widening of the two). **`ids.rs` (+8) is the one file this plan does read, and it rotted one live row.** The new dense id `ControlRequestId(u64)` is inserted between `FlushTicket` and `TimerId` at old `:97`, shifting every declaration below it by +8: `ScenarioId` moves old `:116` → `:124`, `ReplicaRole` old `:160` → `:168`. §15's row 1 cites `ScenarioId(u64)` (`ids.rs:116`) inside the table frozen at `f616ddf`'s "Landed shape" header; per AGENTS.md that cell is history and is left as written, exactly as this note's predecessors left rotted table cells elsewhere — recorded here as this round's evidence: `ScenarioId` is now at `ids.rs:124`. **M7V-56 is not inside that table — it is a live test row**, and it cited `ReplicaRole` (3, `contracts/ids.rs:160`); that citation rotted and is corrected in place to name the declaration, `contracts/ids.rs`, enum `ReplicaRole`, instead of a line. Nothing here lowers an assertion or reopens an ask. Marker moved below after this re-read, not before it.

**Round-15 re-read (2026-09-28, lead), basis moves `56f952d` → `4f4a2c3`.** `4f4a2c3` is now the newest commit touching `crates/rdb-core/src/contracts`. `git diff --stat 56f952d 4f4a2c3 -- crates/rdb-core/src/contracts` touches two files: `authority.rs` (+27) and `storage.rs` (+7), both doc-comment growth plus one new variant, `AuthorityEvent::EpochRevocationRestored { partition, epoch }` — the environment's start-of-process read-back of a durable revocation (lead ledger L-R178e). `authority.rs`: the `AuthorityEvent` enum doc gains a paragraph (hunk at old `:1070`, net +4), and the new variant is appended last before the closing brace (hunk at old `:1144`, net +23 more). This plan's two `authority.rs` citations (`:75`, `:686`) both sit above old `:1070`, so neither moved. `storage.rs`: `StoreEffect::PersistEpochRevocation`'s doc gains a matching paragraph (hunk at old `:160`, net +7); this plan cites no `contracts/storage.rs` line. Neither hunk touches `trace.rs`, `ids.rs`, `control.rs` or `ignore.rs`, the only files this plan's §15 table and its drift rows read, so Round-14's re-read stands untouched. **This plan already carries the landed change.** The V-R35/L-R178e notes above (§ "Restart and re-acquisition" area, dated 2026-09-27/2026-09-28, M7V-109..M7V-113) already name `EpochRevocationRestored` and describe `revoked_epochs` as moved onto the kernel and recorded in every state, exactly as `4f4a2c3` landed it — written, like kernel-a's twin note, while the change was still uncommitted. Nothing in those notes needs correcting. This plan does not otherwise enumerate `AuthorityEvent`'s variants as a live count. Nothing here lowers an assertion or reopens an ask. Marker moved below after this re-read, not before it.

<!-- drift-basis: 4f4a2c3 -->

**Foundation asks still open after this round: none.** Rows 1 and 21 were the last two, and both
landed at `6893442` — in foundation's *first* code round, the same round that received them.
Verification has **zero open contract shape asks**. Three documents said the opposite for four
rounds (this table, `trace-requirements.md` §8, and the verification critic), all three written
against `8a23b1d` and none re-read; the count of rDB teams bitten by a drift table they did not
re-read reached three of four before `scripts/drift-check.sh` existed.

What this plan still waits on is **code**, and it is not a contract ask:

- **I1**, the replay runner — §12's largest block.
- **I1's trace validator** (VER-CR-3, no ruling id; recorded by the lead in `ledger.md`,
  2026-09-20 20:35 PDT). M7V-88 degrades to envelope checks and reports
  `Unavailable{Capability(I1)}` without it. It is code in `rdb-sim`, not a shape in `rdb-core`,
  so a crate-ordered foundation queue skips it and **nothing goes red when it slips** — the row
  just quietly stops checking realizability. Named here because that silence is the hazard.
- **H1 `ForgeAck` and M1 `FalseDurable`** (VA-4, V-R9).

The contract change this section spent four rounds predicting has **landed**: kernel-b's CB-3
widened `AckRejectReason` 7 → 14 at `f616ddf` (ruling **B-R33 Q-B-8**; drift row 23), and B-R58c
widened it again 14 → 15 at `3249092` (round 9, `InFlightUnverified`). `M7V-56`
asserts **set equality** between the enum and the coverage lists, so it went red both times — the
row working, not the row breaking. **Lead ruling F-R20 (2026-09-20 23:08 PDT) is explicit that
`M7V-56` is not to be weakened to a subset assertion**, and it has not been. The eight cells are
§15.1 below (seven at CB-3, an eighth added round 9). `BoundaryId` is still **29** members, now at
`trace.rs:540` (round 9 re-derivation — this citation had been carried as `:505`, its `f616ddf`
value, through both the round-8 +31 shift and this round's +4 shift without moving; the commit did
not touch that enum's membership, only its line), so `M7V-56`'s other set-equality half is
unchanged.

**Delta, wave 2 (lead brief `dev-sim-hooks`, 2026-09-27; no ruling id).** The sim now records
`BatchApply` (every `StoreEffect::Commit` and every preload), `DurabilityAdvance` (every engine
sync: host flush, F1 `SyncWalThrough`, durable preload) and `ReplicationAck` (every accepted R1
reply, at the acker, `Buffered`) — `crates/rdb-sim/src/harness/semantic.rs`. No row id is credited
by it. What it changes for this plan's rows, and what it does not:

- **M7V-88** clause (2)'s `acks_against_applies` now bites on recorded traces, not only on
  hand-built ones: every recorded ack follows its acker's own `BatchApply`.
- **M7V-69, M7V-47 (A1/P1)**: still no sim run reaches a P1 `Publish` (B-R60, A1 post-`Recovered`
  install), so INV-PUB cannot be `Proven` on a recorded run. The recording is no longer the gap.
- **M7V-70 is contradicted, not merely blocked**, and needs a ruling. `StorageOp::FalseDurable`
  completes a flush with an **empty** `durable` (`src/storage.rs`), so a correct kernel never
  advances on it and never sends a `Durable` ack; the recorder writes that flush as
  `SyncOutcome::Partial`, never `Synced`. The row's antecedent ("a publish counting the resulting
  `Durable` ack") therefore exists only on a **mutated** kernel. Either the row is reframed as the
  kernel-refuses half plus a trace-rewrite oracle half (the M7V-69/M7V-81 split), or M1's op must
  deliver the lie to the kernel, which contradicts `M7F-07` and is not verification's to change.
  **Ruled (V-R25, then V-R26 option (c)):** split like MUT-2. The kernel half is the new row
  **M7V-92** (`tests/dispatch.rs`). It is green on the kernel clauses and parked on the INV-PUB
  clause, because no sim run publishes (B-R60). The oracle half is a scaffolding row in
  `tests/scenarios.rs`, `false_durable_oracle::false_durable_a_publish_counting_an_ungrounded_durable_ack_trips_inv_pub`.
  **M7V-70 stays owed. Its missing half is INV-LOSS.** On the hand-built trace, both secondaries
  ack `Durable`, their flushes are rewritten to what `FalseDurable` records (`Partial` at 0), and
  both hosts crash and return at boot 2 reporting seq 6 of a published 9. INV-PUB fires
  `durable_ack_ungrounded`. INV-LOSS is armed and **measured `Proven`**. The cause is
  `checks/loss.rs`, the `part.holders(lost_seq)` loop: a holder is `(node, boot)` and is matched to
  a queried source only with `s.boot == boot`. So every holder that returned under a new boot is
  skipped, and the loss is permitted. The ruling declines to reword the claim down to what the
  checker can do (option (a), the "nearest free id" move). Option (b), a grounded `Durable`
  holder staying a holder across a boot, was measured in the export: it moves M7V-02 and
  M7V-03 (the golden trace's genuine restricted loss becomes
  `loss_with_a_surviving_durable_holder`), leaves M7V-27/28/28b/29/29c and the campaign binary
  unmoved, and still does not fire on the lie, because the lied ack is ungrounded (log
  `C:/rdb_test_data/logs/sim-hooks/protob/`). **V-R29 rejected (b) and closed it**: same-boot
  matching is deliberate. M7V-70 is re-planned (see the row): INV-PUB fires, INV-LOSS measures
  `Proven` by design, owed `unavailable(M1)`. The row records evidence against two parts of that
  wording: M1 is wired, and `FalseDurable` cannot hit `InconsistentProgress`. §15.1 cell 3 stays
  `unavailable(H1)` on that evidence until the lead rules. §13 now maps MUT-5 to M7V-92 plus
  M7V-70, and `MutationId::catching_rows` says the same. This plan has no §16; this paragraph is
  the §15/§16 delta V-R25 asks for.
- **M7V-80 landed on the contract change, parked on I1 (L-R177gd, 2026-09-27; dev-verif-w3a).**
  The history: F1 quarantines the spine with a disagreeing survivor at (gen 1, seq 2), and the
  `quarantine` line was recorded, but `recovery_decision` could not be, because its
  `selected_cutoff_seq`, `selected_digest` and `new_generation` were not optional and a quarantine
  selects no position and creates no lineage (V-R28; V-R29 refused placeholders). Gautam approved
  making exactly those three fields `Option<_>`, `None` on `mode=Quarantine` and `Some` otherwise
  (`contracts/trace.rs`, `TraceKind::RecoveryDecision`, doc comments state the rule).
  What landed:
  - **Recorder** (`harness::semantic`): `Semantic` folds F1's plan, the fence it accepted (the
    step that asks for inventory) and each copy's answer while the window is open, and on F1's
    `Quarantine` effect writes `recovery_decision` (`semantic::quarantine_decision`) before the
    `quarantine` line. Every field is taken from something that happened: `fenced_epoch` from the
    accepted fence, `discovery_window_ticks` from the fence's arrival to `CloseWindow` (or to the
    decision if the window never closed), one `queried_sources` entry per plan member with the
    dispatcher's boot for its node (`Dispatcher::boot`) and `reachable` only for a copy that
    answered with an inventory. `loss_uncertainty` is `false`: F1 builds a loss record only for a
    selected cutoff, and a quarantine cuts and deletes nothing (spec §8.4). **That derivation is
    this plan's, not the contract's; the lead should confirm it.** A missing plan or discovery is
    `SimError::Config{semantic::recovery_decision}`, never an invented line.
  - **Not recorded yet:** `recovery_decision` for a recovery that **selected** (`TwoSurvivor`,
    `LoneSurvivorReadOnly`). A gap, stated in the module notes, not a ruling.
  - **Oracle** (`checks/lineage.rs`): `cutoff_below_an_available_recorded_prefix` treats `None`
    as "no cutoff selected" and skips both cutoff clauses; it never reads `None` as 0. That is not
    an exemption: with no cutoff there is nothing to be below.
  - **Row** `m7v_80_…` in `tests/dispatch.rs` replaced the scaffolding row
    `recording_a_recovery_digest_disagreement_is_a_quarantine_line_naming_both_sides`, which
    asserted zero `recovery_decision` lines. Red first: 0 lines against 1
    (`C:/rdb_test_data/exports/verif-w3a/logs/m80-red/out.log`). Killed mutants: the recorder
    writing `Some(Seq::ZERO)` on a quarantine, and the oracle reading `None` as `Seq::ZERO`
    (INV-LIN `Violated` on nodes 1 and 2). A unit test in `harness::semantic`
    (`quarantine_decision_measures_the_window_to_its_close_and_invents_nothing`) pins the window
    end, unreachable copies and the no-boot refusal.
  - **Still parked (I1):** INV-LIN `Proven` needs a `lineage_root` line the recorder does not
    write, so INV-LIN never arms on a recorded run; the `BoundaryId::Divergence` cell needs a
    `fault_injected` line; the `AckRejectReason::Diverged` cell needs a rejected-ack line, and a
    quarantined recovery builds no receiver to reject anything (R1). The census counts the row
    `PARKED`, i.e. owed.
- **Recorder truth, M7V-96..M7V-101 (tester-sim-hooks F1, MATERIAL; 2026-09-27).** The tester's
  false-line probes (`probe_tail.rs`, md5 `b542b3d2…`: p1, p1b, p3, p4, p5, p7, p8) are now rows,
  so each of the tester's recorder mutants t1..t5 is killed by a named row. M7V-100 adds the
  deterministic kill for t4 that the probes lacked: `Semantic::record` fed a `Rejected` reply at a
  node that holds a receiver. p2 (duplicated appends) and p6 (stalled sync) were not adopted;
  the six rows kill t1..t5 without them. The old recorder row
  `recording_a_sync_is_a_durability_advance_line_synced_only_where_the_engine_moved` is renamed
  `recording_a_sync_is_synced_at_the_engines_watermark_and_a_false_durable_flush_is_partial`
  (F2: a no-op sync is `Synced` at an unmoved watermark, so the old name overclaimed); it has no
  plan id and keeps none.
- **V-R36 (ShortFlush), 2026-09-27; renumbered from "V-R30 (ShortFlush)" by the lead at landing, L-R177ht.** `semantic::durability_lines` writes a successful sync
  whose reported durable position is **below** the capture as `Partial` at that position, not
  `Synced`: the `SyncOutcome` contract lets only `Synced` publish the captured prefix. That covers
  `ShortFlush` and a capture above what was applied. Red first on M7V-98 and M7V-99 (`Synced`
  where `Partial` was expected, `logs/truth-red/out.log`). **Two things for the lead.** (1) Id
  collision, now closed: `V-R30` is dev-sim-publish's runner ruling (M7V-93..M7V-95) and this
  ruling is V-R36. (2) The
  `SyncOutcome::Partial` doc says "no watermark moves", but a `ShortFlush` does move the engine's
  watermark (to the short position). The line under-claims; it never over-claims, so INV-PUB
  cannot be grounded by it. Whether `Partial` or a new outcome fits is a contract question, not
  settled here.
- **V-R37 (lead ruling, 2026-09-27, L-R177hp):** `loss_uncertainty = false` on a Quarantine `RecoveryDecision` is accepted. A quarantine selects nothing and discards nothing: the suffix is retained on disk (kernel-b ruling B-R68), so no suffix is lost without proof either way. Pinned by M7V-80 and its unit test (tester-w3 mutant W2).
- **V-R35 (lead ruling, 2026-09-27; restart rebuild, M7V-102..M7V-108).** Before this,
  `Dispatcher::restart` reopened the engine and nothing else. Every kernel instance kept its
  memory across a crash, so a crash row could pass because the node never forgot. Now `restart`
  drops that node's A1, F1, T1, R1, P1 and L1 instances, its adopted triple, its armed timers and
  the catch-ups it was sourcing. Each kernel table gains a `forget_node(node)`. First boot has no
  start-of-life event: A1's first `AcquireDue` is seeded, and a fresh A1 ignores
  `NodeLifecycle::Rebooted` as stale. So a restart delivers no boot event.
  What survives, because it is durable:
  - the engine;
  - the control store;
  - the committed roots (`committed`, now carrying the correlation);
  - `revocations`;
  - `landed`.

  The node re-learns through its control watch. For each partition, `restart` takes the **newest**
  committed root, and re-queues it as a `Crashed` held watch only when that root names the node.
  A root that drops the node supersedes every older one that named it, so the node re-reads
  nothing there and rebuilds no role (tester G3, M7V-106). A node no root names re-reads nothing
  (M7V-107). A re-read still in flight is not queued twice (M7V-108). The row set pins every
  table a restart drops (M7V-102, M7V-105); the one exception is the timer-site map, whose
  survival is unobservable because the clock forgot the timer it names.
  **Open, not modelled** (in the handoff):
  - (a) a re-seeded A1 cannot re-acquire, because its create-only grant CAS meets the old boot's
    record. Ruled 2026-09-27 (Gautam): fixed later by the scenario grant service (option A), not
    in this package;
  - (b) the durable epoch revocations have no read path into a fresh A1. Ruled 2026-09-27
    (Gautam): fixed later by a new contract event `EpochRevocationRestored`, not in this package;
  - (c) scheduler events and control watches queued under the old boot still reach the fresh
    modules. **Closed** by M7V-114..M7V-118 (sim-fidelity note below);
  - (d) the pinned configuration names each member's old boot, so **every restarted member** is
    refused as a stale copy until something re-pins it. A restarted primary's appends are
    refused `NotAMember` (78 in tester run t2). A restarted follower's receiver is rebuilt as
    `Member{boot 1}`, and the primary drops every ACK it sends as `AckRejected(StaleBoot)` (30 in
    t1). So a restarted follower never counts toward progress again. Nothing re-pins today.
- **L-R178e (Gautam, 2026-09-27; dev-a1-restart, 2026-09-28; M7V-109..M7V-113).** Closes V-R35's
  open items (a) and (b). Item (d) stays open; item (c) is closed by the sim-fidelity note below.
  - (b) **Revocations read back.** `Dispatcher::restart` marks the node. The first offer to its
    fresh A1 replays each durable revocation of that node as `EpochRevocationRestored`, in key
    order, ahead of the offer itself, so nothing reaches the fresh A1 first. A1 records it in
    every state (`revoked_epochs` moved onto the kernel). M7V-109.
  - (a) **Grant service.** `rdb_sim::sim::grant_service::clear_restarted_grant` deletes
    `grants/{node}` at its exact revision only when the record is not frozen, the service's
    clock proves `E_old + ε + δ` passed (`authority::clock::expiry_proven`, the takeover's own
    inequality), and no `partitions/{id}` naming the node is other than `Serving`. It is called
    by the row, not by the run loop. `ControlStore::scenario_cas` is the one new public store
    method it writes through. M7V-110..M7V-113.
  - `revocations` still survives a restart, as V-R35 listed; it now also has a reader.
- **Sim fidelity (lead ruling 2026-09-28: a crash kills the process and everything it owned;
  V-R36: a node's boot changes only by `restart`; dev-sim-fidelity, correction round 1 after
  tester-sim-fidelity; M7V-114..M7V-124).** Closes V-R35's open item (c), tester G5, F-D, F-E,
  F-G, the request-id repeat, tester advisory ADV-1, and tester D1..D6. Item (d) stays open. F-C,
  F-F and CopyAheadOnControl are not in it.
  - **G5, down node.** `Runner::run` asks `Dispatcher::drop_if_dead` for each popped event. An
    event for a down node is dropped as `DropReason::NodeDown`: not offered, not an error, and
    counted as consumed. Each drop is kept in `Dispatcher::dropped()` and logged. `TraceKind` is
    contract and has no drop kind, so the record is sim-side, as the network's dropped frames
    are. M7V-114. A direct `deliver` to a down node carries out nothing either: every effect but
    storage is dropped as `NodeDown` and recorded, and a storage effect is refused at the crash
    seam as before (`Dispatcher::deliver_while_down`, tester D4). M7V-122.
  - **F-D and V-R36, one boot per process.** A node's boot is its registered one until
    `Dispatcher::restart` moves it, and nothing else moves it (the old `boots` map, which
    `deliver` wrote, is gone; `Dispatcher::boot` reads the registration). An event or a
    delivery naming any other boot is dropped: older as `StaleBoot`, newer as `UnknownBoot`
    (tester D1). Round 0 adopted a newer boot through `deliver`; that choice is withdrawn,
    because every inbound frame, `Recovered` and catch-up is stamped with the registered boot
    and was then dropped as stale. `restart` refuses a boot that is not strictly newer,
    `Config{restart_boot}` (tester D3). A seed under a boot the node is not running is dropped
    and counted, not refused (tester D6). M7V-115, M7V-120, M7V-123, M7V-124.
  - **D7, a never-registered node (slice sim-followup).** `Dispatcher::restart` refuses a node
    the cluster never registered, `Config{restart_node}`. It has no current boot, so "strictly
    newer" compared against nothing and any boot was taken. Boot 0 needs no rule of its own:
    every registered node has a boot, and boot 0 is newer than none. M7V-125.
  - **`NodeDown` ranks first on a down node (slice sim-followup).** Effects handed to a down
    node are `NodeDown` whatever boot they name, as events are in `Dispatcher::drop_if_dead`.
    Chosen over the boot reasons because no process runs there to compare against, and
    `Dropped::Effects` keeps the named boot, so nothing is lost. M7V-126.
  - **F-G and request ids.** A1's timer versions and `ControlRequestId`s restart from the same
    counters, so a stale fire or answer can carry exactly what the fresh process expects. Both are
    stopped by the stale-boot drop, with no counter persisted: M7V-116 (timer version), M7V-117
    (request id). On `HEAD` both reached A1 and were acted on.
  - **F-E, watches.** `Dispatcher::crash_check`, the one place a crash is taken, marks the node's
    watches owed; `Dispatcher::pump`, the one place control completions are scheduled, ends them
    through `ControlOp::TerminateWatch` with `WatchTermination::Unavailable` ("the node
    stopped"). `deliver` pumps even when refused if a crash was taken, so a crash taken inside
    another node's delivery (F1's `SyncWalThrough` on the holder) ends the holder's watches too
    (tester D2). The terminations are addressed to the dead process and dropped. A restarted
    node watches only once its fresh A1 asks. M7V-118, M7V-121.
  - **ADV-1.** M7V-119 pins guard 3 on `FencingDrained`; M7V-113 alone survives a guard that
    blocks only `Fencing`.
  - **The send path's own crash check (tester D5).** `tests/dispatch.rs`
    `send_envelopes_a_planned_crash_is_taken_by_the_provider_itself`, F5's twin: a crash planned
    and not yet taken is taken by `SendEnvelopes` itself. It goes red when that check is removed;
    F5 no longer can, because the runner never steps a down node.
  - **Existing rows whose meaning changed.** (`tests/dispatch.rs`) F5
    `send_envelopes_a_crashed_primary_sends_nothing` asserted the run stops at the crash seam,
    which needed the down node to be stepped; it now asserts a `NodeDown` drop and a drained run.
    A-R69a `store_a_crash_drops_the_nodes_views_and_a_reused_handle_opens_fresh_after_restart`
    delivered the restarted node's effects under boot 1, which rolled the boot back; it now
    delivers them under boot 2. Under V-R36 five rows delivered or seeded under a boot they never
    registered, and now register it: `m7f_21_the_effect_to_event_hop_costs_zero_ticks_and_a_delay_costs_exactly_the_delay`
    (`tests/dispatch.rs`), `h1_scaffolding_a_demoted_l1_keeps_a_stale_promotion_stale` and
    `h1_scaffolding_a_rebuild_while_paused_traces_the_new_barrier` (`tests/harness.rs`, through
    `h1_registered_plan`), and the unit rows `a_fire_carries_the_arms_payload_and_the_arms_site`
    (`harness::dispatch`) and `a_fire_carries_the_arms_version_partition_and_correlation`
    (`harness::run`). Twenty more rows seed nodes the cluster never registers and still pass;
    their events are stamped boot 0 (listed in the dev-sim-fidelity handoff). None restarts a node, so D7's `restart_node` refusal leaves them green (sim-followup gate, 2026-09-28).
  - **Still open:** `Dispatcher::run_due_flushes` and transfers can still touch a crashed node's
    engine. The stale events in M7V-116 and M7V-117 are seeded under the old boot, standing for
    what a dead process's connection or timer would deliver late.
- **Known gap:** `BatchApply.key_versions` is always empty — the harness holds byte keys and a
  `KeyId` is assigned by the scenario generator. INV-ATOM and the read-version rules see no
  versions from a recorded run.
- **M7V-93..M7V-95: the host half of a recovered run (lead ledger L-R177gf, ruling V-R30;
  dev-sim-publish, 2026-09-27).** Ids were first drafted as M7V-92/93; M7V-92 was already
  dev-sim-hooks', so V-R30 renumbered them. What landed, and why:
  - **Flush cadence in lowering, not in `Dispatcher`.** `support::scenarios::run::lower` adds
    `plan.flushes` on every node, every `HOST_FLUSH_EVERY_MILLIS` (100), from the latest cutoff F1
    can choose (fence + window x (1 + `MAX_WINDOW_EXTENSIONS`) when the scenario transfers,
    fence + window otherwise) to the deadline. `plan.flushes` is already the reproducer's field,
    and only lowered recovered scenarios get it. The FalseDurable, stall and crash rows build their
    `RunPlan` by hand, so a cadence there cannot mask the fault they inject; their results did not
    move in the full-suite diff. A `Dispatcher` cadence would have reached every hand-built plan.
  - **A1 acquisition in lowering.** One `AcquireDue` per primary, `ACQUIRE_AFTER_CUTOFF_MILLIS`
    (500) after that cutoff, so after F1's activation CAS. The sim control store delivers no watch
    unprompted, so the grant must come after the activation. Killed mutant: acquiring at the cutoff
    commits the grant CAS before the activation CAS.
  - **Runner defect (M7V-95).** `Runner::run` popped the next queued event after due work that
    queued nothing; fixed in `src/harness/run.rs` by going round again under the existing
    moved-deadline guard. It moved no other row in the full-suite diff.
  - **`F1_R1_STEADY_POPS_PER_WINDOW` split (V-R30 (a)).** Now keepalive+health (261) + host flush
    (60, derived: 3 nodes x 20 rounds x 1 pop) + A1 renewal (16, derived: 4 renewals x 4 pops),
    each named in `cases.rs` with its derivation. The window [8003, +2000) still held 341 against
    337: the host's first round is a one-off 9 pops on B, and this case's worst-case cutoff, 8003,
    is exactly `F1_R1_PAUSED_AT` + 2 windows, so it lands in a counted window.
  - **The first host round (ruling V-R31, option (b)).** `F1_R1_FIRST_HOST_ROUND_POPS` = 9, derived
    by pop kind in its `cases.rs` doc comment (2 durable ACKs, 4 R1-to-L1 kernel events, 3 L1
    timer pops) and checked against a first-round-only control run. `cases::f1_r1_window_bound`
    counts it in the one window that holds the lowered plan's first host flush and in no other.
    M7V-47 asserts that the one-off summed over its checked windows equals the term exactly once,
    which is what goes red if the term is applied per window. That equality pins where the term
    is counted, not its value (tester-verif-pub P1: 8, 10 and 4 all survived it). So M7V-47 also
    measures the value from its own run: B's pops in the first host round minus B's in the round
    after it. The row asserts that difference equals the term; the tree measures 21 − 12 = 9.
    Option (a), treating that window as base, was refused: it blinds a counted window. The term also enters `f1_r1_max_events` once,
    as base.
  - **Not changed, and not needed now:** `case_a1_p1_new_generation_between_publish_and_reply`'s
    Submit (tenant 1, client 1, request 1) collides with `canonical_history`'s preloaded identity
    for seq 1 under another digest, so T1 answers `RequestIdReuse`. That is not a retry across
    generations (the digest differs), but it is not plainly a typo either. The case is parked on
    kernel work (A1 post-`Recovered` install, R1 steady ship), so nothing un-parks by fixing it
    today. Use a request id above the head, or client 2, when the case is un-parked. Its Submit at
    2102 also lands inside L1's 5 s resume hold, and its 8 000-tick deadline leaves little room
    after the resume.
- **Tester findings on the verif-gate merge (dev-sim-publish, round 2, 2026-09-27).** F1: M7V-72
  now expects `["M7V-92", "M7V-70"]` for MUT-5 (V-R25), as it expects two rows for MUT-2. F3:
  M7V-76 checks "ran" at its definition (`Ending::Judged`) and the ran/unlowerable partition.
  F4: M7V-74 rejects a whitespace-only reason. F5: `harness::semantic` unit test
  `quarantine_line_names_both_root_mismatch_shapes` pins both `RootMismatch` shapes
  (`DigestConflict` when the survivor holds another digest at the root, `CorruptHistory` when it
  has no rung there), so M7V-80's `Pairwise`-only drive (the scaffold it replaced drove the same) no longer leaves that arm
  unguarded.
- **M7V-47, case A1/P1: re-read against b220a2b (dev-a1p1-w3, 2026-09-27; no ruling id).**
  The row stays parked and M7V-47 stays `MISCREDITED`. What moved:
  - **Cleared: B-R60 and A1's post-`Recovered` install.** The case minus its activating op now
    publishes end to end through A1. The scaffolding row
    `a1p1_case_without_its_activation_publishes_through_a1` (`tests/scenarios.rs`, claims no row)
    asserts the chain on the write's correlation, in trace order: `Dispatch` `Valid`, B's
    primary apply at seq 11, both secondaries' applies and accepted ACKs, `Publication` `Valid`,
    a `Publish` whose `authority_recheck` is that decision, `Reply` `Valid`, and `Success` at
    generation 2, seq 11, delivered. Every decision names owner B at generation 2. The oracle is
    clean. The row reads the same chain back from its own JSONL log with DuckDB.
  - **The case is re-timed and re-numbered**, as the note above asked. It submits 300 ms after
    L1's resume hold lifts (`A1_P1_RESUMES_AT`, derived from fence + window +
    `resume_hold_millis`), under request id `A1_P1_HEAD + 1`. Killed mutants: request 1 is
    refused `RequestIdReuse`, and a submit one tick inside the hold is refused
    `ProtectionPaused`.
  - **A bridge defect, fixed.** `lower` never set the trace header's topology, so the oracle's
    model found no role for any node. INV-PUB then counted `regular_acks_counted=0` and fired
    `required_copy_set_unsatisfied` on a correct publication. `lower` now declares
    `topology.placements` at `config_version_0`. This was red before the fix, and a mutant that
    drops it is killed. No other row in `tests/scenarios.rs` moved.
  - **Still owed, and not a bridge change alone.** `lower` refuses a second `InspectSurvivors`,
    and no op lowers the `Reply` hop delay. Below the bridge, nothing yet produces the new
    generation:
    - On B, F1 is terminal once `Committed`. A second `Plan` and `FenceProven` are
      `Ignored(OutOfPhase)`.
    - On C, discovery reads the dispatcher's placed survivor inventories, which stay at
      generation 1. It ends `BlockPromotion(NoEligibleRegular)`.
    - B hears of a new generation only through a watch, and the sim delivers one only when told.
    - This was probed in a scratch test in the export, which was deleted before the gate.
    - Unblocking needs a kernel-b answer on F1 re-entry after `Committed`, and live inventories
      in the dispatcher. Neither belongs to verification.

### 15.1 The `AckRejectReason` cells (CB-3 debt, written 2026-09-21; eighth cell added round 9)

**What a cell here is, and what it is not.** A cell names the variant, the **producer** in
verification's own grammar that can make the kernel emit it, and the **row** that asserts the cell
is hit. A cell that says "covered" without naming a covering row is not a cell — it is the vacuous
assertion this team has now confirmed five times, and writing eight of them to make `M7V-56` green
would be the worst available outcome: a coverage table that lies is worse than a red row. So where
a variant has no producer or no assertion, the line below says **GAP** and names an owner. **Six
of the eight are gaps.** That is the honest state of the debt, not a failure to pay it.

**Round-9 correction (2026-09-26): the seven line citations below were never re-derived through
two prior shifts, and this is the exact fault AGENTS.md's "a grep is not a re-read" describes.**
They were written `:331, :333, :335, :337, :339, :341, :343` against `f616ddf`'s +37 `AckRejectReason`
widening. Round 8's own text said "none of their line citations depend on this enum's absolute
position, only its variant names, which did not move" — true of the names, false of the lines: the
`BudgetName` insertion above this enum shifted the whole file +31 at `9235bfb`, and this table's
seven citations were never moved to match. They are corrected below to their `3249092` values, and
an eighth cell is added for `InFlightUnverified`, the variant round 9 found.

**Where each cell is gated.** `replication_ack` is emitted by the replication and progress package
**R1**, which is not wired in M7. Under design §3.1 / V-R20 (7) a cell whose emitting package
reports `Unavailable` lands in `coverage_unavailable`, never in `required_missing[]` and never
deleted from the required list — so **all fifteen reject-reason cells report `unavailable(R1)`
during M7**, the two below that name an additional package report that package first, and
`M7V-55` does not fail on any of them today. The six gaps come due the round R1 is wired; they
are recorded now so that round finds them written down rather than discovers them.

| # | Variant (always enum-qualified) | C0 meaning (`contracts/trace.rs`) | Producer in verification's grammar | Covering row | State |
|---|---|---|---|---|---|
| 1 | `AckRejectReason::StaleGeneration` (`:362`) | the acknowledgement names a lineage older than the one being replicated | **exists** — `RecoveryOp::SelectPrefix` / `Synchronize` (`grammar.rs:421`, `:426`) bump the lineage (`lineage_root`), then `NetworkOp::Reorder{msg, before}` (`grammar.rs:211`), or `Drop` then a later `Deliver`, lands a pre-bump ACK after it | **GAP — no row asserts it.** The producer is expressible today; the assertion was never written. The nearest neighbours assert the *client* side of a stale lineage (`BoundaryId::OldGeneration`), which is a different surface | GAP · owner **verification test-planner**, directed row in the next planning round (ids from `M7V-91`) |
| 2 | `AckRejectReason::RoleMismatch` (`:364`) | the acknowledging replica's role cannot qualify this acknowledgement | **GAP in the corpus** — `Topology` takes arbitrary `Placement{role}`, so a `ReplicaRole::Shadow` placement is *expressible*; but `grammar::rf3`, the only constructor and the one the random corpus uses, places `Primary + 2 × RegularSecondary` and **no shadow at all**. The only shadow in this plan is M7V-69's authored `n4`, reached by **forgery**, which is `ForgedIdentity`, not `RoleMismatch` | **GAP** — a genuine (unforged) shadow ACK has no case | GAP · owner **verification architect** (a shadow-bearing topology constructor or a fifth authored case), then test-planner for the row |
| 3 | `AckRejectReason::InconsistentProgress` (`:366`) | the reported progress contradicts itself — a durable position ahead of a buffered one | ~~**exists** — `StorageOp::FalseDurable{node, through}`~~ **none today (V-R25 (3)).** The tracker returns it when `!ordered(&ack.progress)` or a `received` inflated past the primary's own (`replication/progress/tracker.rs`, the rule-7 check before `advance`). `FalseDurable` cannot produce that: the engine moves no watermark, so the acker's progress stays ordered (M7V-92 measures `durable` 0 under `buffered_applied` 2). Only an acknowledgement whose **body** lies reaches it, which is H1's owed body-lie forge (`harness.rs` H1 capability note: "a forged acknowledgement whose lie is in its body") | ~~M7V-70~~ — **no row**. Neither MUT-5 half reaches it, and asserting it on either would be vacuous | **cell** · `unavailable(H1)` until the body-lie forge lands; then a directed row (ids after `M7V-92`) |
| 4 | `AckRejectReason::RegressedProgress` (`:368`) | the reported progress went backwards from what this peer last reported | **exists** — `StorageOp::Crash{kind: Host}` then `Reopen{node}` (`grammar.rs:333`, `:342`): the node returns at a new `BootId` having lost its unflushed suffix and reports a lower `contiguous_seq`. Boundary `BoundaryId::StaleBoot` | **GAP on the assertion.** **M7V-29** drives the producing condition and is this plan's INV-LOSS clause (b) row, but it is a **unit** row over a hand-built trace — it does not run the environment and so does not feed the campaign counter. Naming it as the covering row would be the vacuous move | GAP · owner **verification test-planner**, a `sim` sibling of M7V-29 once R1 is wired |
| 5 | `AckRejectReason::Unverifiable` (`:370`) | the acknowledgement carries no evidence that can be checked | **GAP — no producer.** No arm of `grammar.rs` emits an evidence-free ACK. `NetworkOp::ForgeAck` (`grammar.rs:235`) supplies **false** evidence, which is `ForgedIdentity`; nothing strips evidence | **GAP** | GAP · owner **verification architect** → **lead**. This is one of the eight whose cost exceeds "write a cell": it needs a new grammar arm or a `ForgeAck` field. F-R20's own clause — "if the seven cells cost materially more than expected, verification brings it back to the lead rather than touching the row" — applies to this one (and, on the same grounds, to cell 8 below) |
| 6 | `AckRejectReason::InFlightUnverified` (`:374`) — **new at `3249092`, ask B-R58c** | evidence below the primary's anchor, for the record a catch-up cursor has in flight; no rung can check it yet, so it drives the cursor and moves no watermark — the ACK at the anchor verifies the whole chain below it | **exists** — kernel-b's own M7B-173 already drives this producer and names the variant (`m7b_173_an_in_flight_ack_below_the_cutoff_drives_the_cursor_and_moves_no_watermark`), but that row is kernel-b's file, over kernel-b's fixture, not this plan's campaign grammar. Whether `grammar.rs` can produce an in-flight-below-cutoff ACK independent of kernel-b's unit fixture is not yet checked | **GAP** — no `M7V-` row asserts this cell against the campaign corpus. Do not cite M7B-173 as the covering row: it is a different plan's unit test, the same distinction row 4 already draws for M7V-29 | GAP · owner **verification test-planner**, directed row in the next planning round (ids from `M7V-91`); check `grammar.rs` for a producer before assuming one is owed |
| 7 | `AckRejectReason::Diverged` (`:376`) | the acknowledging replica's history disagrees with the primary's at a retained position | **exists** — `RecoveryOp::Diverge{partition, seq}` (`grammar.rs:445`), boundary `BoundaryId::Divergence` | **M7V-80** — it already drives a digest disagreement and already asserts the `BoundaryId::Divergence` cell; its expectation now also asserts this cell. The kernel half is kernel-b's §3.4 rule 1d (M7B-41: a later valid ACK from the diverged copy is dropped) | **cell** · `unavailable(F1)` per M7V-80 today, then `unavailable(R1)` |
| 8 | `AckRejectReason::NotAMember` (`:378`) | the acknowledging node is not a member of the pinned configuration | **partly** — `NetworkOp::ForgeAck{claimed_node}` (`grammar.rs:235`) can name a `NodeId` absent from the `Topology` in force (`rf3` declares `nodes: 3`, so `NodeId(4)` is a stranger), which is **not** M7V-69's case — there `n4` is a *member* with the wrong role. What is **not** expressible: a member that was **removed** and then acked. `Topology` is a header field carrying `config_version_0` and **no `ScenarioOp` changes membership**, so only "a stranger acked" is reachable, never "a former member acked" | **GAP** — the stranger sibling of M7V-69 is not written. **Also unverified:** which of `ForgedIdentity` and `NotAMember` the tracker returns for a stranger's ACK is kernel-b's §3.4 ladder **order**, which this plan cannot read and must not guess | GAP · owner **verification test-planner** for the row; **kernel-b** to state the ladder order before it is written |

Renumbered from the original seven (round 8 and earlier cited them 1–7 in enum declaration order);
inserting `InFlightUnverified` at its declared position between `Unverifiable` and `Diverged`
keeps this table's row order matching the enum's, at the cost of `Diverged`/`NotAMember` moving
from 6/7 to 7/8. No id outside this table refers to a cell by its position number, only by variant
name, so the renumbering itself corrects nothing and breaks nothing.

**What this does and does not make green.** `M7V-56` asserts set **equality between
`coverage.rs`'s lists and the enums** — nothing more. It goes green the moment
`ACK_REJECT_REASONS` in `crates/rdb-sim/tests/support/scenarios/coverage.rs:92` is widened from
`[AckRejectReason; 7]` to `[AckRejectReason; 15]` with the eight names above (round 9: `14` → `15`,
one more entry, `InFlightUnverified`). **That edit is code
and is owed to verification's developer; it is not made in this document.** The table above is the
justification each of those fifteen entries needs in order not to be a lie. The six gaps are
`M7V-55`'s problem, not `M7V-56`'s: they are `unavailable(R1)` while R1 is unwired, and they
become `required_missing[]` — a red `M7V-55` — the round it is wired and they are still unwritten.
Do not read a green `M7V-56` as "the eight are covered".

**Also landed at `f616ddf` and *not* reflected here — flagged, not written.** Verification's own
developer added three INV-LIN oracle tests in that commit
(`crates/rdb-sim/tests/oracle.rs`: `lin_a_recovery_root_that_cites_no_predecessor_violates`,
`lin_a_cutoff_above_the_selected_sources_prefix_violates`, and the shadow row
`lin_the_two_cutoff_clauses_do_not_shadow_each_other`) which **have no `M7V-` row in this plan**;
the commit message says three rows landed, and in the plan they did not — the file gained only
VA-7's held note and the Q-row darkness note. Row ids from `M7V-91` are free. Recorded here rather
than written, for the same reason drift row 6 records its candidate row: a new row belongs in a
planning round with a critic, and three tests with no rows is a smaller defect than three rows
nobody reviewed. Owner: **verification test-planner**.

Remaining, and not this plan's files to fix:

1. **`validation-plan.md` V8 omits the 250 ms resume condition** that spec §6.2's resume row
   states. Team-rules authority order puts the spec above the validation plan, so M7V-37 asserts
   250 ms. Flagged because a reader of the validation plan alone will call M7V-37 over-strict.
2. **The charter's evidence command cannot produce the charter's own 60 s number** (critic F10):
   `scripts/gate.sh` never passes `--release` and `[profile.test] opt-level = 0` applies to
   workspace members. Settled by V-R11 and VA-9; M7V-62 is the row that keeps the two commands
   distinguishable in the artifact. The **charter text itself** still reads as if the plain gate
   command produced it.
3. **Spike §7's "proposed future commands" use `cargo test --release --test campaign` directly**,
   without `scripts/gate.sh` and without a private `CARGO_TARGET_DIR`. AGENTS.md forbids the second
   omission on this host. VA-9's two commands supersede the spike's for M7; the spike's are
   acceptance commands for files that did not exist when it was written.
4. **`docs/rdb/*` says "rDB" in places where it means rEtcd** (team-rules §Names). Rows that grep
   rDB documents (M7V-77) must not treat those as rDB production claims; the row's assertion is
   about later-milestone **gate** claims, which is a narrower string set.

**Wave 2, Package B delta (dev-v-campaign, 2026-09-27, basis `e693f47`; lead brief for Package B;
rulings V-R16, V-R17, V-R19, V-R20, V-R21).** The campaign rows now run through a campaign engine,
`crates/rdb-sim/tests/campaign/engine.rs`, over the scenario bridge. Six things a reader of the
rows above needs to know:

1. **No generated seed runs today.** The generator places the producer op first and
   `NetworkOp::Heal` last, and the bridge lowers neither, so each generated seed reports the
   capability its checkers need and never `proven`. The authored F1/R1 case runs. The shared
   corpus is the default seeds plus the two authored cases.
2. **M7V-62:** `artifact_name()` returns the `write_evidence` stem. The file it names is
   `<stem>.json`. The row text now says so (V-R17 is unchanged).
3. **M7V-63:** `SPIKE_MAX_EVENTS` caps authored budgets too. An authored budget may be smaller,
   never larger. M7V-64's corpus therefore runs at 2,000 events, the injected history's own budget.
4. **M7V-89:** `WIRED_IMPLIES_ARMED_EXCLUDED` lives in `campaign/engine.rs`, beside `arming()`,
   not beside the registry, because `support/` is another developer's file this wave (V-R21).
   This deviates from row M7V-89 above, which says "a named const beside the registry". Ruling
   V-R27 (5) accepts the deviation. Move it when that file is free.
5. **Evidence (ruling V-R24):** `config-testkit` is now a dev-dependency of `rdb-sim`. The shared
   corpus writes both artifacts on every run of the campaign binary, as M6's rows do. M7V-74 is
   now the real schema row. Small corpora never write. M7V-75 and M7V-76 assert the run record
   `write_evidence` would stamp. **M7V-75's "set" half runs the full 1,000-seed corpus, not one
   under N seeds:** scale 1.0 is unreachable under N seeds. That costs about 50 ms while no
   generated seed lowers. Ruling V-R27 (3) accepts it for now. **Revisit it when generated seeds
   start to lower (the run stops being ~50 ms) or when its time passes the sim budget.**
6. **Still owed:**
   - **M7V-55:** the gated default corpus fails `required_missing` today, because nothing runs.
   - **M7V-86:** lives in `scenarios.rs`.
   - **M7V-88:** needs the shared builder registry under `support/`.
   - **M7V-82:** its stub half needs the runner to accept injected modules. Its event-equality
     clause is written; C0 has no event but has a campaign-block row.
   - **M7V-75:** its set half, see item 7.
7. **Achieved scale and the truncated F1/R1 run (ruling V-R27 (2) and (4)).**
   - **Achieved counts only generated histories that ran** (`Ending::Judged`), times the event
     cap. A seed the bridge refused was processed, not run. Today that is 0, so both artifacts
     write `scale_factor: 0` and `full_scale: false`.
   - **A zero carries its reason.** `values.below_target_reason` names every cause: seeds
     processed, the event cap, and "N of N generated seeds did not run through the bridge
     (M7V-55)". `config-testkit`'s `validate` now accepts 0 only beside a non-empty reason
     (`BELOW_TARGET_REASON`). It still rejects a negative or non-finite scale, and 0 with
     `full_scale: true`. M7V-74 asserts all of this.
   - **The inflated definition goes red.** M7V-76 compares the scale with ran × cap and requires
     it below processed × cap when a seed did not run. A mutant restoring "processed × cap"
     fails M7V-76 and M7V-75.
   - **M7V-75's set half is unmet.** The plan wants `scale_factor == 1.0` and `full_scale: true`
     for the full corpus. With no generated seed running, the honest value is 0. The function
     asserts `full_scale` equals "every seed ran" and then calls `parked`, so the census counts
     M7V-75 as owed. It lands when generated seeds run through the bridge.
   - **F1/R1 is truncated in every campaign corpus.** Its authored budget is
     `f1_r1_max_events(12_000)` = 2,518 events. The corpora cap it at 512 (shared, M7V-65),
     2,000 (M7V-58) or 128 (M7V-63). At 512 it stops inside the ~620-pop burst at close + 10
     (R1's walk and F1's rebuild) and never reaches the paused steady state. **No row asserts
     on F1/R1 reaching its deadline or end**,
     by a grep of every campaign row for `ending`, `Ending::`, `popped`, `stop`,
     `EventBudgetExhausted`, `events_total`, `cells`, `verdicts`, `seeds_armed`,
     `Status::Proven` and `required_missing`:
     - M7V-63 reads `ending`/`stop`, to assert the cap holds. Truncation is its subject.
     - M7V-58 compares endings across thread counts at one cap. Self-relative.
     - M7V-72 asserts `events_total` is a `u64`. M7V-65 compares 64 and 128 seeds at one cap.
     - M7V-52, M7V-53, M7V-78, M7V-87 and M7V-89 read the folded statuses. Every invariant
       folds to a capability today (all 10 in this run's artifact). Only `violated` outranks
       that (design §2.4), which is the disclosure below.
     - M7V-73 asserts only that no cell is both missing and unavailable.
     - **The disclosure:** M7V-52's "the default corpus violated nothing" covers F1/R1's first
       512 events only. A violation after that is not seen by the shared corpus.
   - **Checked by running, too:** mutant M09 removes the cap on authored budgets, so F1/R1 runs
     its full 2,518. Every shared-corpus row stayed green. Only M7V-63, whose subject is the
     cap, failed. So authored budgets stay capped.
