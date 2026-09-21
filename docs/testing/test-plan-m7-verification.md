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
> Rows added in correction round 1 are `M7V-78..M7V-88`, and in correction round 2 `M7V-89` and `M7V-90`; each
> lives in the section its subject belongs to; no existing id was renumbered or reused. Architecture requirements in this plan are
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
| Oracle rows M7V-01..M7V-41, M7V-79, M7V-81, M7V-85, M7V-90 | `crates/rdb-sim/tests/oracle.rs` |
| Oracle implementation | `crates/rdb-sim/tests/support/oracle/{mod,model}.rs`, `.../checks/*.rs` |
| Grammar, generator, reducer rows M7V-20..M7V-23, M7V-42..M7V-51, M7V-80, M7V-83, M7V-84, M7V-86, M7V-88 | `crates/rdb-sim/tests/scenarios.rs` |
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
`TraceHeader` at `8a23b1d` (`schema_version`, `seed: u64` — until foundation lands `provenance`,
V-R20 (2), §15 drift table — `generator_version`, `config_digest`, `budgets`, `topology:
Vec<TopologyEntry{node, partition, role, config_version}>`, `oracle_checkpoint_digest`), then typed
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
row instead of quietly shrinking the requirement. `BoundaryId` is **foundation's closed set** —
spike §6's required-boundary column plus the two V-R9 members, 29 members at commit `8a23b1d`
(`crates/rdb-core/src/contracts/trace.rs`) — and M7V-56 asserts set **equality** against the enum
(ruling V-R19 answers the planner's Q-4); `op_skipped` is its own event kind (critic F17).

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
| `trace_header` | one line per replayed or recorded trace, before its events (critic T-27): `schema_version`, `seed` (→ `provenance` when foundation lands it, V-R20 (2)), `generator_version`, `config_digest`, `topology` — a list of `{node, partition, role, config_version}` structs, the landed `Vec<TopologyEntry>` as serde writes it |
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
| **unit** | hand-built trace, plain data or a source-level check; no runner, no kernel | **< 100 ms** | **59**: M7V-01..M7V-19, M7V-24..M7V-46, M7V-49, M7V-56, M7V-57, M7V-59, M7V-66..M7V-68, M7V-71, M7V-74, M7V-77, M7V-79, M7V-81..M7V-85, M7V-90 |
| **sim** | one scenario through the runner and the real kernel | **< 2 s** (M7V-88 replays every fixture and owns **< 10 s**, stated in the row) | **12**: M7V-20..M7V-23, M7V-47, M7V-48, M7V-50, M7V-69, M7V-70, M7V-80, M7V-86, M7V-88 |
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
   If foundation later lands a `quorum_rule` field (K-F-07), the derived value **stays
   authoritative** for the coverage cell; the oracle only cross-checks the landed field against it,
   a disagreement is INV-PUB `rule="quorum_rule_mismatch"` (M7V-90), and the oracle reads the field
   for nothing else (ruling V-R21).
   `publish.ack_evidence` entries are `AckEvidence{node, role, durability}` — three fields, no
   boot id. `topology_change.nodes` entries are `(NodeId, ReplicaRole)` tuples. The header's
   `topology` is a flat `Vec<TopologyEntry{node, partition, role, config_version}>`.
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
| M7V-07 | `pub_publish_without_the_pinned_required_copy_set_violates` | spec §8.3, healthy RF3: a shadow ACK never qualifies | `client_submit` + `admission_decision{Admitted, admitted_seq=5, config_version=1, required_copies=[n1,n2,n3]}` on one `correlation`; `protection_state{required_copy_set=[n1,n2,n3], config_version=1}` (derived rule `Rf3`, convention 1); `ack_from(n4, 5)` — i.e. `batch_apply{node=n4, role=Shadow, seq=5}` then `replication_ack{from_node=n4, peer_role=Shadow, contiguous_seq=5, durability_class=Durable}` (convention 4) — grounded by `durability_advance{node=n4, outcome=Synced, durable_seq=5}`; `publish{seq=5, ack_evidence=[{node=n4, role=Shadow, durability=Durable}]}` on the same `correlation` | INV-PUB `Violated`, `rule="required_copy_set_unsatisfied"`; the signature names the pinned `config_version`. Grounded and role-consistent on purpose so the copy-set rule is the only clause that can fire | unit | C0 |
| M7V-08 | `pub_degraded_rf2_one_ack_publish_violates` | **the F1 bug the whole correction round exists for.** Spec §8.3 "no one-copy fallback"; ADR-rdb-0019 §1 V3's degraded half; rewritten per critic T-02 so it can fire for exactly one reason and so the pin is resolvable | **(a) membership.** `client_submit` + `admission_decision{outcome=Admitted, admitted_seq=9, config_version=4, required_copies=[n1,n2]}` sharing one `correlation`; `protection_state{required_copy_set=[n1,n2], config_version=4}` — derived rule `DegradedRf2` from `required_copy_set.len() == 2` (V-R20 (1), convention 1) — emitted before the admission (VA-3 cadence); `topology_change{config_version=4, nodes=[(n1, Primary), (n2, RegularSecondary), (n3, RegularSecondary)]}` so `n3` is a legitimately *named* regular secondary that is simply not in the pinned set; `ack_from(n3, 9)` (convention 4: `batch_apply{node=n3, role=RegularSecondary, seq=9}` then `replication_ack{from_node=n3, peer_role=RegularSecondary, config_version=4, contiguous_seq=9, durability_class=Durable}`) **grounded** by `durability_advance{node=n3, outcome=Synced, durable_seq=9}`; `publish{seq=9, ack_evidence=[{node=n3, role=RegularSecondary, durability=Durable}]}` on the same `correlation`, with a valid `authority_recheck`. **(b) pin drift** (the sub-case that makes "pinned at `admitted_seq`" mean something): the same admission at `config_version=4` pinning `[n1,n2]`; then `protection_state{required_copy_set=[n1,n3], config_version=5}` (still derived `DegradedRf2`) and `topology_change{config_version=5, …}` **between** the admission and the publish; the publish carries a grounded `Durable` ack from `n3` — which satisfies the **new** set `[n1,n3]` but not the pinned one | (a) INV-PUB `Violated`, `rule="required_copy_set_unsatisfied"` **exactly** — not `durable_ack_ungrounded` (the ack is grounded), not `ack_role_claim_mismatch` (n3's role is declared); a checker with only the grounding clause, or one that evaluates grounding first and returns the wrong rule, fails here for the right reason. (b) INV-PUB `Violated`, same rule, and the signature names `config_version=4` — a checker that pins from the **last** `protection_state` seen passes (b) and is wrong. The pin is resolved by carrying `admission_decision.{admitted_seq, config_version, required_copies}` forward on the `correlation` (`publish` itself has no `config_version`). The `DEGRADED_RF2` coverage cell this row feeds is keyed on the **derived** rule, `derived_quorum_rule × DegradedRf2` (M7V-55). The row must fail if the checker applies the healthy RF3 rule; write it before the checker exists and watch it fail for the right reason first (ADR-0014, rule A7) | unit | C0 + VA-3 cadence + `topology_change` |
| M7V-79 | `pub_degraded_rf2_publish_on_the_primarys_own_durability_alone_violates` | **the F1 cardinality twin (critic T-02 defect 3).** Spec §8.3's forbidden behaviour is publishing on **one copy**: `min_regular_acks` 1-of-1 means one, not zero. A membership-only checker passes M7V-08 and M7V-09 and still ships this | M7V-08(a)'s admission and `protection_state{required_copy_set=[n1,n2], config_version=4}` (derived `DegradedRf2`); `n1` is the primary: `batch_apply{node=n1, role=Primary, seq=9}` and its own `durability_advance{node=n1, Synced, durable_seq=9}` are present; **no** `replication_ack` from `n2` at all (and no `ack_from` call — the absence is the point, convention 4); `publish{seq=9, ack_evidence=[{node=n1, role=Primary, durability=Durable}]}` — the primary's own durability is the only evidence | INV-PUB `Violated`, `rule="required_copy_set_unsatisfied"`; the signature reports `regular_acks_counted=0` against `min_regular_acks=1` (the minimum is derived from `required_copy_set.len()`, V-R20 (1)). Its near-miss is **M7V-09 unchanged** (one grounded ack from `n2` makes it clean), so together the pair pins both membership and cardinality. Cross-reference: `required_copy_set` is read with the opposite quantifier by M7V-36 (§4 convention 3) | unit | C0 + VA-3 cadence |
| M7V-90 | `pub_landed_quorum_rule_disagreeing_with_the_derived_rule_violates` | **ruling V-R21 (lead Q-1 on K-F-07):** the rule the oracle derives from `required_copy_set.len()` is authoritative (V-R20 (1)); if foundation lands a `quorum_rule` field the oracle cross-checks it and reads it for nothing else, so a kernel that pins `[n1,n2]` but labels the state `Rf3` is caught as a contradiction in its own emission, not silently trusted on either side | **blocked until K-F-07 lands** (§12): the row is written against the field name `quorum_rule` on `protection_state` and reports `Unavailable{Capability(C0)}` with note `quorum_rule not landed` while the landed `ProtectionState` has no such field. Bad trace: M7V-08(a)'s admission and grounding with `protection_state{required_copy_set=[n1,n2], quorum_rule=Rf3, config_version=4}` (derived `DegradedRf2`, landed `Rf3`) followed by M7V-09's legal single-ack publish. Good trace: the same with `quorum_rule=DegradedRf2` | INV-PUB `Violated`, `rule="quorum_rule_mismatch"`, signature carrying `derived=DegradedRf2, landed=Rf3, config_version=4` — fired **at the `protection_state` event**, before any publish arithmetic; the good trace is `Proven` and no other checker `Violated`. In **both** halves the coverage cell recorded is `derived_quorum_rule × DegradedRf2` (the derived value keys the cell even when the landed field disagrees — the mismatch is a violation, never a third cell). The oracle never uses `quorum_rule` to decide `min_regular_acks`; M7V-08/M7V-79/M7V-09 stay written without the field and must pass unchanged after it lands (rule A2: this row differs from M7V-09 by one fact, the field's value) | unit | C0 + `quorum_rule` (K-F-07) |
| M7V-09 | `pub_degraded_rf2_publish_with_the_pinned_single_regular_ack_is_clean` | near-miss: ruling B-R3, `min_regular_acks` 1-of-1 under the pinned config is **legal**. Differs from M7V-08(a) by **exactly one fact** (rule A2, critic T-04): the ack's node | M7V-08(a)'s trace verbatim — same admission pinning `[n1,n2]` at `config_version=4`, same grounding — with the single ack coming from `n2` instead of `n3`: `ack_from(n2, 9)` (`batch_apply{node=n2, role=RegularSecondary, seq=9}` then `replication_ack{from_node=n2, peer_role=RegularSecondary, config_version=4, contiguous_seq=9, durability_class=Durable}`) grounded by `durability_advance{node=n2, Synced, durable_seq=9}`; `publish{seq=9, ack_evidence=[{node=n2, role=RegularSecondary, durability=Durable}]}` | INV-PUB `Proven` and no other checker `Violated`. Without this row the F1 fix over-corrects into "RF2 needs two peers", which stops writes the spec permits. **Cross-reference M7V-36:** the same `required_copy_set=[n1,n2]` there must be satisfied by *every* member; here by *one qualifying* member — two rules, two readers, no shared helper (§4 convention 3) | unit | C0 |
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
| M7V-42 | `grammar_every_required_boundary_variant_is_constructible` | spike §6's scenario-operations table, right-hand column, is fully expressible | the `REQUIRED` const list (VA-6, design §3.1 under V-R19), derived from `BoundaryId`, and the `BoundaryId -> producing ScenarioOp` table in `gen.rs` | **A static table check, stated as such (critic T-21):** for every `BoundaryId` member the producer table has an entry, and the entry's `ScenarioOp` constructs; the row enumerates rather than hand-lists, so a new `BoundaryId` with no producer **fails** instead of passing quietly (M6-107 pattern). It does **not** run the environment — whether the op actually *reaches* the boundary is M7V-55's behavioural claim, counted from `fault_injected`, and M7V-55 is what catches this table going stale. Explicitly includes `ForgedIdentity` and `FalseDurableWatermark` (V-R9) and the process-vs-host `Crash{kind}` distinction | unit | none |
| M7V-43 | `generator_same_seed_and_version_yields_an_identical_scenario` | spike §4's trace seam: determinism, and that weights are constants rather than env-tunable; the V-R19 schedule is a function of the seed index, not of the PRNG | `gen::scenario(seed, budget)` twice, and once more after reading a polluted environment; then seeds `i` and `i + N` (N = `REQUIRED.len()`) | all three `Scenario` values are byte-identical after serialization; no `std::env` read occurs inside `gen.rs` (asserted by a source grep, like M7V-01); seeds `i` and `i + N` both carry the scheduled op for `REQUIRED[i mod N]` and differ elsewhere — the obligation is deterministic and the PRNG still varies the rest | unit | none |
| M7V-44 | `generator_respects_the_budget` | charter DO-NOT "no unbounded search"; spike §7's bounded histories — the **generator** half (critic T-21; the runner half is M7V-86) | `Budget { max_events: 64, max_ticks: 500 }` over 200 seeds | no generated scenario's op list can produce more than `max_events` events by the grammar's own per-op event bound (a static count over the op list, no runner); no `ScenarioOp` list is empty (an empty scenario is a silently useless seed); every scenario carries its scheduled `REQUIRED[i mod N]` op inside the budget | unit | none |
| M7V-86 | `runner_stops_at_max_events_and_ends_at_an_event_boundary` | the **runner** half of the bound (critic T-21): "the runner stops at it" is behaviour, and needs I1 | one scenario whose op list would produce more than `max_events` events, run with `Budget { max_events: 64 }` | the trace has exactly `max_events` events or fewer; the last event is a complete event (the runner never truncates mid-transaction — a cut inside a transaction leaves the oracle `Unavailable{NotArmed}` for that seed, never `Violated`, and the row asserts that verdict on the cut trace); the run records `budget_spent = max_events` | sim | I1 |
| M7V-45 | `scenario_json_round_trips_and_a_schema_bump_rejects_a_stale_fixture` | D4: the fixture, not the seed, is the reproducer; spike §7 "unknown environment/config fields are errors" | every file in `tests/fixtures/{scenarios,regressions}/`; plus a synthetic fixture at `schema_version + 1`; plus one with an unknown field | round trip is lossless for all checked-in fixtures; the bumped and unknown-field fixtures are **rejected with a typed error**, never silently defaulted. A `schema_version` bump invalidating checked-in fixtures is the intended behaviour, not a regression | unit | none |
| M7V-46 | `provenance_is_explicit_and_nothing_carries_a_bare_seed` | critic F18 (both halves): a reduced or authored scenario is not in the generator's image, so a `seed` field on it is false provenance. **Blocked (critic T-23, ruling V-R20 (2)):** the landed `TraceHeader` at `8a23b1d` carries `seed: u64` and no `Provenance` type exists; F18 is routed to foundation as a C0 amendment, and until it lands this row's header half asserts the opposite of the code | every checked-in fixture; and the trace header produced for each of the three `Provenance` kinds | `Provenance` is `Generated{seed} | Reduced{from} | Authored{case}` and every fixture carries one; the **trace header** carries the same `provenance` (not a bare `seed`), so a failure report cannot print "seed 4471" for a run no seed reproduces. **Meanwhile** the fixture half runs (the `Scenario` type is verification's), and the header half reports `Unavailable{Capability(C0)}` with the note `provenance not landed` — never a pass (§12 "C0 + provenance") | unit | C0 + `provenance` (foundation ask, V-R20 (2)) |
| M7V-47 | `authored_cross_package_cases_construct_and_run` | spike §6's four **mandatory** cross-package adversarial cases, as Rust constructors (critic F18b) rather than hand-typed JSON | `case_a1_p1_expire_between_publish_and_reply()`, `case_f1_r1_discovery_window()`, `case_f1_t1_p1_retained_status_24h()`, `case_f1_t1_digest_across_recovery()` | each constructs, carries `Provenance::Authored`, runs to completion inside its budget, and registers its named pairwise coverage cell. While the kernel packages are unwired the run reports `Unavailable{Capability(p)}` for the invariants involved and the row asserts **that**, never a pass (§12). The A1/P1 case asserts **both halves** of the adversarial row (A-R22): the kernel's outcome and the oracle's verdict | sim | I1 |
| M7V-88 | `every_fixture_and_authored_case_is_realizable_by_the_runner` | design **§4.5** (critic round 2, the second-order form of R-3): a checker tuned to a shape the runner can never produce arms in its unit row and never in the campaign — visible as `unavailable(not_armed)` under V-R16, but still a checker that guards nothing. Fixtures must be realizable, and the assertion is never weakened to make them so | (1) every `tests/fixtures/scenarios/*` file and every authored constructor (design §3.1 family 2, M7V-47's four); (2) every `TraceBuilder` trace an oracle row in §3/§4/§8 feeds to a checker, collected through the shared builder registry (each row registers its trace under its id) | (1) each scenario replays through I1's runner with **no** `op_skipped{reason=ReferentGone}` and the oracle report carries the verdict the owning row expects (`Proven`, or `Violated` on the named `(checker, rule)`); (2) each hand-built trace passes the same well-formedness checks I1 applies to a recorded trace — strictly increasing `event_id`, the `capability` block first, `schedule_phase` before any liveness arming, `replication_ack.contiguous_seq` never above the emitting node's last `batch_apply.seq` — through I1's validator; **if I1 exposes no validator the row runs the envelope checks only and reports `Unavailable{Capability(I1)}` for the rest, and says so in its output**. A failing fixture is fixed in the fixture (charter DO-NOT); the row never edits an expectation. Budget: **< 10 s** for the whole set, stated here because it replays every fixture, not one | sim | I1 |
| M7V-80 | `recovery_path_digest_disagreement_yields_quarantine_from_the_kernel` | the kernel-facing third sub-case of M7V-19 (critic T-03, option (i)): a reachable source whose `reported_digest` differs from the recorded `entry_digest` at a recorded `(generation, seq)` is a divergence that spec §8.2 says never auto-merges, and it is **F1's** decision, not the oracle's | a directed scenario (`design.md` §3.1 family 2 style, `Provenance::Authored`): RF3, a `StorageOp::Crash` on the primary after `seq=9` is applied on one secondary only, then a `NetworkOp::Partition` that leaves the recovering side with a source reporting a different digest at `(gen 7, seq 9)`, then `RecoveryOp::Synchronize` | the trace contains `recovery_decision{mode=Quarantine}` with `queried_sources` naming the disagreeing source, and a `quarantine{reason=DigestConflict, generation=7, seq=9}`; INV-LIN is `Proven` on that trace (the conflict was quarantined, M7V-17's near-miss shape); the `recovery_decision.mode × quarantine` guard cell and the `BoundaryId::Divergence` boundary cell are both hit. `Unavailable{Capability(F1)}` until F1 lands — never a pass (§12) | sim | I1 + F1 |

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
| M7V-55 | `default_corpus_and_authored_cases_hit_every_required_cell` | spike §7 coverage: "every required fault boundary exercised"; `design.md` §6's three axes; **coverage is a property of the seed list, not of luck (critic T-12, ruling V-R19)** | the shared default corpus (64 seeds ≥ N = 29, so every `REQUIRED[i mod N]` is scheduled at least twice and the run is `coverage_gated: true`, VA-6, V-R20 (7)) plus the four authored cases | (1) **the schedule covers the required set:** the set `{REQUIRED[i mod N] : i in the seed list}` equals the `REQUIRED` set — asserted from the seed list alone, before any run; (2) **observed counts:** every required cell on all three axes has `count >= 1` in the run, and `required_missing[]` is **empty** — except cells whose gating package (`BoundaryId -> PackageId` table, VA-6, keyed per family on the emitting provider, V-R20 (4)) reported `Unavailable`, which appear in `coverage_unavailable` and in the artifact as `unavailable(H1)` / `unavailable(M1)` / `unavailable(<family's provider>)`, **not** in `required_missing[]` and **not** deleted from `REQUIRED`; (3) the row fails if a cell is both scheduled and `missing` with its package `Wired` (the generator's producer table is stale — see M7V-42). Named cells that must be hit and are the ones most likely to be missed: `derived_quorum_rule × DegradedRf2` (critic F1; the rule is **derived** from `required_copy_set.len()`, V-R20 (1), so the cell is named as such and keyed on the derived value), `replication_ack.reject_reason × ForgedIdentity` (F16, hook-gated on H1), `FalseDurableWatermark` (F6, hook-gated on M1), the isolation cell (V-R8), and the four named cross-package cells | campaign | I1 |
| M7V-56 | `coverage_required_lists_are_enumerated_from_their_enums` | VA-6; the M6-107/TA-63 pattern — a missing case must fail, not pass quietly; V-R19 answers Q-4 with set **equality** | `coverage.rs`'s required lists, the `BoundaryId -> PackageId` gating table and `gen.rs`'s producer table vs the enums they count — **the enums that exist at `8a23b1d`** (critic T-23): `AckRejectReason` (7), `BoundaryId` (29), `RecoveryMode` (3), `ProtectionPhase` (4), `ReplicaRole` (3), `ErrorKind` (18) | every variant of `AckRejectReason`, `BoundaryId`, `RecoveryMode`, `ProtectionPhase` and `ReplicaRole` has a cell; a variant added without one fails this row. **`ErrorKind` is wider than the admission axis**, so the admission-reason axis is the const `ADMISSION_REASONS: &[ErrorKind]` in `coverage.rs`, asserted (a) to be a subset of `ErrorKind` by construction and (b) to contain the four variants rows pin — `ProtectionPaused` (M7V-39), `RequestIdReuse`, `CrossAffinity`, `GenerationChanged` (M7V-25); a `client_outcome` axis over the full `ErrorKind` is **reported, not required**. **The derived quorum rule** (V-R20 (1)) is a two-member axis `{Rf3, DegradedRf2}` computed from `required_copy_set.len()`, declared as its own enum in `coverage.rs` and enumerated like the rest — there is no `QuorumRule` in `rdb-core` and none is asked for. `REQUIRED`'s member set **equals** `BoundaryId`'s variant set exactly — the 29 members foundation declared at `8a23b1d` (`crates/rdb-core/src/contracts/trace.rs`), no more, no fewer (critic F17, V-R19); the gating table and the producer table each have exactly one entry per member, and **every member of one `FaultKind` family maps to the same package** in the gating table (V-R20 (4), critic T-35). `PackageId` has ten variants and every one appears in the capability report M7V-82 checks | unit | C0 |
| M7V-57 | `a_required_cell_with_zero_hits_fails_the_run` | ADR-rdb-0019 §2: "coverage is counted cells, never a percentage; a named required cell with zero hits fails the run" | a synthetic coverage record at `seeds = N` (so `coverage_gated: true`, V-R20 (7)) that omits one required cell whose gating package is `Wired`; a second that omits one whose package is `Unavailable`; and a third, the first record again at `seeds = N - 1` | the first **fails**, names the cell and its axis, writes exactly one `coverage_shortfall{axis, cell}` line (and **no** `coverage_cell` line for that cell, VA-7) and `required_missing[]` to the artifact; the second does **not** fail on that cell, writes `coverage_unavailable{axis, cell, package}` and leaves `required_missing[]` empty; the third does **not** fail either — it writes `coverage_gated: false` and the same `coverage_shortfall` line (the shortfall is recorded, the gate is not applied), so a gate that fires on a sub-N corpus fails this row (critic T-25). The negative control for M7V-55, all three branches; the gate condition is `seeds >= N`, exercised at N and N − 1 | unit | none |
| M7V-58 | `campaign_result_is_independent_of_thread_count` | `design.md` §5.2 rule 3: results merged deterministically, so the report does not depend on `available_parallelism()` | the same corpus at 1, 2 and N threads, at `SPIKE_SEEDS=8` — the smallest count that produces a non-trivial merge (more seeds than threads, at least two per chunk; critic T-11); `coverage_gated: false` (8 < N, V-R20 (7)) | identical per-invariant statuses, reasons and `seeds_armed`, identical coverage counts, identical failing-seed list and identical signature slugs; `coverage_gated` is `false` on all three and none fails on `required_missing`. Only `wall_ms` differs. A campaign whose verdict moves with host load is not evidence | campaign | I1 |
| M7V-59 | `seed_base_zero_makes_the_extended_corpus_a_superset` | `design.md` §5.1: a PR failure must reproduce in the extended run | the seed list at `SPIKE_SEEDS=64` and at `SPIKE_SEEDS=256`, both at `SPIKE_SEED_BASE=0` | the smaller list is a prefix of the larger. Cheap, and it is the property the whole layered-budget scheme rests on | unit | none |
| M7V-60 | `campaign_records_wall_ms_and_asserts_no_threshold_in_the_pr_default` | V-R11; `test-plan-m6.md` §7's rule ("assert invariants, record numbers, never a threshold"); AGENTS.md's `m4_69` lesson | default corpus with `SPIKE_ASSERT_WALL_MS` unset | `wall_ms`, `host`, `build` and `profile` are recorded; the row asserts **no** wall-time threshold and passes on an arbitrarily slow host. A threshold assertion appearing in the PR default is a defect this row must catch (assert that the runner's threshold path is not taken) | campaign | I1 + testkit |
| M7V-61 | `campaign_asserts_wall_ms_only_when_spike_assert_wall_ms_is_set` | V-R11's other half: the extended gate does assert | two runs of a 4-seed corpus (§2), `coverage_gated: false` (4 < N, V-R20 (7)): `SPIKE_ASSERT_WALL_MS` unset, then set to **`0`** — a value no host can beat, so the row is host-independent (critic's V-R11 check; "a deliberately slow corpus" was host-dependent) | unset → passes and records; set to `0` → **fails**, printing observed vs configured, and the failure is the wall-time one — never `required_missing`. This is the only place in the plan where time is asserted, and it is asserted against a bound the run cannot meet by construction | campaign | I1 |
| M7V-62 | `the_release_command_is_the_only_source_of_the_sixty_second_number` | critic F10 / V-R11; **two artifacts, one per profile (critic T-13, ruling V-R17)**; restated per critic **T-32** so the row never asserts on a file another command wrote | the name selector `artifact_name() -> &'static str` in `tests/campaign/report.rs`; **this run's** artifact (the shared corpus report's, §2); the `RELEASE_GATE_COMMAND` and target-dir consts | (1) **the selector is a pure function of the build profile:** `artifact_name()` returns `rdb-m7-campaign.json` under `cfg!(debug_assertions)` and `rdb-m7-campaign-release.json` otherwise, asserted in both directions by calling it in the binary the row runs in and comparing against `cfg!(debug_assertions)`; (2) **this run's artifact** carries `profile` equal to the build profile the binary was compiled under (the same field M7V-72 checks) and, under `profile: "release"` only, `full_scale: true`; a `wall_ms` under `profile: "debug"` is never the 1,000-history figure, and `profile` is the discriminator that makes that checkable; (3) `.rtargets/campaign` is the documented target dir for commands 2 and 3 (a doc/const cross-check against VA-9's rows `| 2 |` and `| 3 |`, not a filesystem probe). **The row never reads the other profile's file:** a tracked `rdb-m7-campaign.json` left by whichever command-1 run last ran is exactly the stale file of unknown provenance the two-name design exists to avoid. Both halves run under either gate; nothing is `unavailable` here any more | campaign | I1 + testkit |
| M7V-63 | `campaign_never_exceeds_spike_max_events` | charter DO-NOT; spike §7 "explicitly bounded; no unbounded combinatorial search" | `SPIKE_MAX_EVENTS=128` over 16 seeds (one corpus of its own, §2; `coverage_gated: false`, 16 < N, V-R20 (7)) | no history's event count exceeds the cap; the run does not fail on `required_missing`; the runner stops at it rather than truncating a trace mid-transaction (a truncated trace must end at an event boundary, or the oracle reports `Unavailable{NotArmed}` for that seed — not `Proven`, not a violation, V-R16; the seed counts in `seeds_armed` only for checkers that armed before the cut) | campaign | I1 |
| M7V-64 | `a_failing_seed_writes_its_reproducer_under_the_test_log_dir` | V-R6; spike §7's failure artifact | one corpus with one injected failing seed (shared with M7V-51, §2; fewer than N seeds, so `coverage_gated: false`, V-R20 (7) — the run fails on the injected violation and on nothing else) | `$RETCD_TEST_LOG_DIR/validation/<run-id>/` contains the schema-versioned event stream, the original and the minimized `Scenario`, and the signature; the persisting copies land in `tests/fixtures/regressions/`; the run exits non-zero; nothing is written under `docs/evidence/` for a failed run except the artifact's own `violated` status | campaign | I1 |
| M7V-65 | `reduced_scale_changes_only_seeds_and_events` | ADR-rdb-0019 §2 and ADR-0031: "reduced scale changes repeat counts and data volume, never which code paths or failure cases are covered" | two corpora: `SPIKE_SEEDS=64` and `SPIKE_SEEDS=128` (critic T-11 — **never** the 1,000-seed full-scale corpus, which runs only under VA-9 commands 2/3 and in the extended run, critic T-31; the property holds between any two scales; both ≥ N, so both are `coverage_gated: true`) | the set of checkers that ran, the set of fault kinds reachable, the required-cell list and the set of `unavailable(package)` cells are **identical**; only `seeds`, `max_events`, `events_total` and `seeds_armed` differ, and the 64-seed list is a prefix of the 128-seed list (M7V-59). A reduced run that drops a checker is the defect that makes the cheap run stop being a regression gate for the expensive one | campaign | I1 |
| M7V-82 | `capability_state_is_derived_from_the_modules_own_report_never_a_literal` | **critic T-14b, ruling V-R18:** `capability{state}` must track reality. A hand-maintained table lets a landed package stay `Unavailable` (a false red nobody chases) or an unlanded one be flipped `Wired` early (a misreported cause). Enumerated like M7V-56 | (a) the dispatcher's report — foundation's `Dispatcher::capability_report` at `8a23b1d` (`crates/rdb-sim/src/harness/dispatch.rs`, derived by probing `step`), or `Module::capability(&self)` once K-F-10 lands, over `ModuleName::ALL`; (b) the source under `crates/rdb-sim/src/harness/` | (a) **behavioural, both directions:** the `capability` events at trace start equal, one for one over every `PackageId`, the report the dispatcher returns; a module stubbed to answer `Ok` from `step` reports `Wired`, one stubbed to answer `RdbError::Unavailable` reports `Unavailable`, both asserted positively through the real event emission path; (b) **source:** no file under `crates/rdb-sim/src/harness/` contains the token `CapabilityState::Wired` except the one that builds the report, and no `const`/`static` table of `(PackageId, CapabilityState)` exists. A landed package that stays `Unavailable`, or a literal `Wired`, fails (a); a table fails (b). Runs the dispatcher in-process with stub modules, no runner and no kernel — hence unit-class | unit | C0 + foundation dispatcher |
| M7V-87 | `m7_release_gate_is_the_cited_command_and_fails_while_any_invariant_is_not_proven` | **critic T-14a, ruling V-R18:** the M7 release gate is one command, written in ADR-rdb-0019 §2.1, and §13's last line cites it. M7V-54 tests the gate *function*; this row ties the function to the *command* and to the milestone claim, so the two cannot drift apart (a test cannot run `scripts/gate.sh`, so the command is checked as a const and its environment is applied to the shared report) | the `RELEASE_GATE_COMMAND` const in `tests/campaign/report.rs`; the shared corpus report (§2); the process environment | (1) the const equals, byte for byte, `SPIKE_REQUIRE_ALL=1 RETCD_EVIDENCE=1 CARGO_TARGET_DIR=.rtargets/campaign scripts/gate.sh test --release -p rdb-sim --test campaign` — and the row reads the same string out of `docs/ADRs/rdb/0019-validation-gates-evidence-and-release-boundary.md` §2.1 and the plan's VA-9 and asserts all three agree (a doc/const cross-check, M7V-62's mechanism). **Extraction rule (critic T-33, narrowed under T-36):** in each file the command is the **first backticked span** in the table row whose line begins `| **M7 release gate**` (ADR §2.1) and `| 3 | **The M7 release gate**` (VA-9 in this plan); exactly one such row must exist per file, and anything else — §1's wrapped command 2, prose mentions, §13's checklist line, and **every `| N |` row of the §15 drift table** — is not read. The plan-side selector carries the row's label because the bare `| 3 |` prefix also matched drift-table row 3 (`| 3 | \`protection_state{state=…}\``), which made this row red on a correct plan and invited the reflex fix of deleting a drift row (critic T-36); the drift table keeps its numeric first column, which is load-bearing for reading `trace-requirements.md` §8 side by side. A file with zero or two matching rows fails the row rather than picking one, and the failure **prints every matching line** so the collision is visible instead of guessed at; (2) applying the command's environment (`SPIKE_REQUIRE_ALL=1`, `RETCD_EVIDENCE=1`) to the gate function over the shared report: the check **fails while any invariant is not `proven`** or any `proven` row has `seeds_armed == 0` or `full_scale` is `false`, naming each cause; a synthetic all-`proven`, all-armed, `full_scale: true` report passes; (3) **during M7** the shared report has `unavailable` rows, so clause 2 is exercised on its failing branch and the row reports the release claim `unavailable (packages A1 T1 R1 P1 L1 F1 unwired)` — never a pass; when the real command 3 is run by hand, its artifact is the evidence and clause 2 is the function it ran. No `scripts/` change is asserted or made (foundation wires it after F-R11) | campaign | I1 + testkit |

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
| M7V-81 | `mut2_counted_forged_ack_trips_inv_pub` | MUT-2 — **the oracle half** (critic T-09): if a kernel *did* count the forged ack, the oracle must catch it. Neither half substitutes for the other; §13 maps MUT-2 to both | trace rewrite (until H1 lands) / recorded-trace rewrite (after) | M7V-69's recorded trace — or, until H1 lands, a hand-built equivalent through `TraceBuilder` (`ack_from(n4, s)` with the role claim overridden to `RegularSecondary`, convention 4) — rewritten so the rejection is removed and the `publish.ack_evidence` **counts** `{node=n4, role=RegularSecondary, durability=Durable}` while the topology in force still lists `n4` as `Shadow` | INV-PUB `Violated`, `rule="ack_role_claim_mismatch"`; the unrewritten trace is `Proven` (both halves in the row). Upgraded in place from hand-built to recorded input when H1 lands (§12); the assertion does not change | unit | C0 |
| M7V-70 | `mut5_false_durable_watermark_trips_inv_pub_and_inv_loss` | MUT-5 mark buffered as durable | **injected fault** (VA-4) | `StorageOp::FalseDurable { node, through }` — a flush completion M1 never performed — then a publish counting the resulting `Durable` ack, then a host crash that loses the suffix | INV-PUB's durability-grounding clause fires (`rule="durable_ack_ungrounded"`), **and** INV-LOSS fires on the subsequent loss. This is V1 clause 3 in its modelled sense: watermark bookkeeping honesty in a memory engine, explicitly **not** fsync honesty, a lying device or power loss (ADR-rdb-0019 §1 V1 `Form`) | sim | M1 hook |
| M7V-71 | `every_named_mutation_has_a_catching_row` | spike §7: "every such mutation must be caught by a named test"; stops a mutation being dropped when a row is renamed | the `MutationId` enum and the campaign's `mutations{}` map | every `MutationId` variant maps to at least one catching row id — MUT-2 maps to **two** (M7V-69 kernel half, M7V-81 oracle half) and the row asserts both are present — and each named row id exists in the test binary (string match on the test name, the same mechanism §13 uses); the map is written into `rdb-m7-campaign.json`. Enumerated, not hand-listed (VA-6) | unit | none |

---

## 9. Evidence rows (V-R2, V-R5; ADR-rdb-0019 §2; rEtcd ADR-0031) — M7V-72..M7V-77

Every row here writes exactly one JSON file under `docs/evidence/` through the shared
`write_evidence()` (VA-8). **No row asserts a threshold.** Each asserts correctness properties that
hold at any scale and *records* the numbers. Reduced scale is the default; `RETCD_EVIDENCE=1` runs
full scale. These mirror rEtcd M6-113..M6-116 deliberately — one schema, one disclaimer, one gate
script.

| ID | Name | Setup | Asserted / Recorded | Class | Dep |
|---|---|---|---|---|---|
| M7V-72 | `evidence_campaign_artifact_is_written` | the shared default corpus (§2); the artifact name follows the profile — `rdb-m7-campaign.json` under debug, `rdb-m7-campaign-release.json` under release (V-R17) | **Asserted:** the profile's artifact exists, parses, and its `values` carry every key ADR-rdb-0019 §2 names — `seeds`, `max_events`, `events_total`, `invariants{id -> {status: proven\|unavailable\|violated, reason, seeds_armed}}` (V-R16 adds `reason` and `seeds_armed` beside the status; `reason` is present only when `status` is `unavailable` and is the **one-string ADR form** `"capability(<package>)"` or `"not_armed"` — V-R20 (5), VA-7's artifact surface), `mutations{id -> catching_row}` (the value is **list-valued** under the unchanged key — V-R20 (5); a one-element list for MUT-1/3/4/5, `["M7V-69", "M7V-81"]` for MUT-2, M7V-71), `wall_ms`, `shrink_ms`, `compile_ms_excluded`, `profile`; a missing key fails. **`coverage_gated` is deliberately not in this list (critic T-37):** ADR-rdb-0019 §2 puts it in `rdb-m7-coverage.json` only, and **M7V-73** asserts it there — this row neither requires nor forbids it in the campaign artifact, so exactly one row owns the key and the two artifacts cannot quietly converge. `profile` equals the build profile the binary was compiled under. **Recorded:** all of the above, plus `slipped` and both fault sets when a shrink slipped (F21) | campaign | I1 + testkit |
| M7V-73 | `evidence_coverage_artifact_is_written` | the same run | **Asserted:** `docs/evidence/rdb-m7-coverage.json` carries `guard_outcomes{cell -> count}`, `fault_boundaries{cell -> count}`, `pairwise{pair -> count}`, `required_missing[]`, `unavailable_cells{cell -> package}` (V-R19: the required cells excluded from `required_missing[]` by capability — hook-gated or family-gated, V-R20 (4); every key names a package whose `capability` event in the same run says `Unavailable`, and no cell appears in both lists) and `coverage_gated: bool` (V-R20 (7): `true` iff `seeds >= N`; `required_missing[]` fails the run only when it is `true`, and the shared corpus writes `true`); counts are integers, never a percentage; the 15 pairwise cells are **reported, not required** (some pairs are meaningless, and a required-but-unreachable cell becomes a cell someone deletes). **Recorded:** the full observed matrix | campaign | I1 + testkit |
| M7V-74 | `rdb_evidence_files_validate_against_the_schema` | after an evidence run, read every `docs/evidence/rdb-*.json` | **Asserted:** each parses through `read_evidence`/`validate`; `schema == 1`; `host`, `build.git_sha`, `run.utc` non-empty; `values` non-empty; `disclaimer` is the exact shared constant (not a second copy); unknown top-level keys rejected. Mirrors M6-113 — a malformed evidence file is worse than none, because it looks like evidence | unit | testkit |
| M7V-75 | `rdb_evidence_gate_rule_is_enforced_both_ways` | run the campaign with `RETCD_EVIDENCE` unset, then `=1` (two small corpora, §2, both under N seeds so `coverage_gated: false`, V-R20 (7) — the row is about the scale flag, not coverage) | **Asserted:** unset → the rows **run** (never `#[ignore]`d — asserted by a source check that no `#[ignore]` attribute exists in `tests/campaign.rs` or `tests/campaign/`), and write `scale_factor < 1.0`, `full_scale: false`; set → `scale_factor == 1.0`, `full_scale: true`; the gate check (the function VA-9 command 3 exercises) fails on any `full_scale: false` during an explicit full run. Mirrors M6-114. No duration is asserted (hard rule 1) | campaign | testkit |
| M7V-76 | `rdb_scale_factor_tracks_reality` | force `RETCD_EVIDENCE=1` while capping the run below full scale (one small corpus under N seeds, `coverage_gated: false`, V-R20 (7)) | **Asserted:** the written `scale_factor` reflects the seeds and events **achieved**, not requested, and the row marks `full_scale: false`. Mirrors M6-115 — a row that writes its intention rather than its observation is a fabricated measurement | campaign | testkit |
| M7V-77 | `rdb_evidence_carries_no_production_claim` | grep `docs/evidence/rdb-*.json`, `docs/ADRs/rdb/*.md`, `docs/rdb/*.md` and this plan | **Asserted:** every artifact carries the fixed disclaimer; no rDB document claims a later-milestone gate has been met; no document says "V1 passed" / "V3 passed" without its `Form` qualifier; no document claims fsync honesty, power-loss or real-clock qualification from M7. Mirrors M6-116 and enforces ADR-rdb-0019 §4's release boundary in a test rather than in a promise | unit | none |

---

## 10. Log-based assertions (DuckDB over `$RETCD_TEST_LOG_DIR`) — Q-34 … Q-40

Numbering continues rEtcd's Q-series (M6 ended at Q-33) because the log directory is shared. Each
query is what a developer runs **first** when the named rows go red. Fields are VA-7's contract.

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
regular under `DegradedRf2`, every member under `Rf3`); `derived_rule` is never NULL (a set of
size 1 or 4 is a fixture or cadence defect); **`unresolved_roles = 0`** — `roles_in_force`
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

| Rows | Unavailable until | Note |
|---|---|---|
| M7V-01..M7V-19, M7V-24..M7V-41, M7V-66..M7V-68, M7V-79, M7V-81 | **C0** (trace vocabulary types) — landed at `8a23b1d` | hand-built or rewritten traces; no runner needed. These are the rows that can be written first (`design.md` §9 work order). M7V-66..68 and M7V-81 were missing from this table before (critic T-18) |
| M7V-08, M7V-38, M7V-79 | **C0 + the `protection_state` on-every-`config_version` cadence** (V-R10) | without the cadence the checker cannot know the pinned set; the row asserts a property the trace cannot express |
| M7V-08(b), M7V-10 | **C0 + `topology_change`** (V-R12, critic F19; `trace-requirements.md` §3.19, ask 7 — landed 18:46) | a header-only static `topology` makes this row fail on a **correct** kernel after a membership change. Seam-freeze item: it cannot be fixed after C0 freezes |
| M7V-26 | **C0 + `ClientOutcome::RecoveredApplied`** (V-R10) | the closed set must carry it or the near-miss half is unwritable |
| M7V-28, M7V-29 | **C0 + `replication_ack` emitted at the secondary** (V-R10) | delivery-point-only emission makes a dropped ACK's holder invisible and INV-LOSS permits loss it should forbid |
| M7V-82 | **C0 + foundation's dispatcher** (`Dispatcher::capability_report` at `8a23b1d`; `Module::capability(&self)` under K-F-10) | runs the dispatcher in-process with stub modules; no runner |
| M7V-46 (header half) | **C0 + `provenance`** (critic F18 routed to foundation as a C0 amendment, ruling V-R20 (2); §15 drift table row 1) | the landed `TraceHeader` at `8a23b1d` carries `seed: u64`; the fixture half of the row runs now, the header half reports `Unavailable{Capability(C0)}` with the note `provenance not landed`. VA-1 keeps `seed` until the amendment lands |
| M7V-90 | **C0 + `quorum_rule` on `protection_state`** (K-F-07, if foundation lands it; ruling V-R21) | the cross-check row; `Unavailable{Capability(C0)}` with note `quorum_rule not landed` until the field exists. The derived rule (V-R20 (1)) is authoritative regardless, so no other row waits on this field |
| M7V-22, M7V-88 (`op_skipped` clause) | **C0 + `op_skipped`** (critic F17's own event kind; not in the landed `TraceKind` at `8a23b1d` — §15 drift table) | M7V-22 asserts an event the trace cannot yet carry; until foundation lands it the row reports `Unavailable{Capability(C0)}` with the note `op_skipped not landed`, and M7V-88's "no `op_skipped{ReferentGone}`" clause is vacuous and says so |
| M7V-20, M7V-21, M7V-22, M7V-23, M7V-47, M7V-48, M7V-50, M7V-86, M7V-88 | **I1** (replay runner) | the reducer and replay rows, the runner half of the budget row (critic T-21), and the fixture-realizability row (design §4.5) — reason `capability(I1)`. M7V-20 is listed in full now (critic T-18) |
| M7V-80 | **I1 + F1** | the kernel-facing third sub-case of M7V-19; `Unavailable{Capability(F1)}` until F1 lands |
| M7V-51..M7V-65, M7V-72..M7V-76, M7V-78, M7V-89 | **I1** (+ `config-testkit` dev-dep for the evidence rows, V-R15) | the campaign loop and its artifacts. M7V-51 is listed now (critic T-18); M7V-89 reports `unavailable (no invariant fully wired)` during M7 (V-R20 (3)) |
| M7V-87 | **I1 + testkit** | exercises the gate function's failing branch during M7 and reports the release claim `unavailable` until A1 T1 R1 P1 L1 F1 are wired. M7V-62 is **no longer** listed here: per critic T-32 it asserts the name selector and this run's `profile` under either gate and reads no other command's file |
| M7V-69 | **H1 `ForgeAck` hook** (VA-4, V-R9) **+ R1** (the rejection is the kernel's) | MUT-2 kernel half; also gates the `ForgedIdentity` coverage cell, which M7V-55 reports `unavailable(H1)` meanwhile (V-R19) |
| M7V-70 | **M1 `FalseDurable` hook** (VA-4, V-R9) | MUT-5; also gates V1 clause 3's modelled half and the `FalseDurableWatermark` cell, reported `unavailable(M1)` meanwhile |
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
| Every spike §7 mutation caught by a **named** test | spike §7 | MUT-1 M7V-66 · MUT-2 M7V-69 (kernel) **+** M7V-81 (oracle) · MUT-3 M7V-67 · MUT-4 M7V-68 · MUT-5 M7V-70 · completeness M7V-71 |
| Every §6 required coverage cell hit — by schedule, not by luck; a zero-hit required cell fails; hook-gated cells excluded by capability only | design §6, §3.1; ADR-rdb-0019 §2; V-R19 | M7V-55 (schedule + observed), M7V-57 (negative, both branches), M7V-56 (enumerated, set equality), M7V-42 (producer table), M7V-73 (recorded), Q-38 |
| Multi-partition isolation (spike §7 safety table, V-R8) | ADR-rdb-0019 §1 | M7V-32, M7V-33, isolation cell in M7V-55 |
| V1 clause 3 "no false durable watermark", modelled sense only | ADR-rdb-0019 §1 V1 | M7V-11, M7V-70 |
| V3's degraded half: both survivors required, no one-copy fallback — membership **and** cardinality, pinned at `admitted_seq` | ADR-rdb-0019 §1 V3; spec §8.3; critic T-02; V-R21 | M7V-08(a) membership, M7V-08(b) pin drift, M7V-79 cardinality, M7V-09 near-miss, M7V-90 (landed `quorum_rule` cross-check, blocked on K-F-07), `DEGRADED_RF2` cell in M7V-55 |
| V4 retries and outcomes, modelled 24 h retention | ADR-rdb-0019 §1 V4 | M7V-24, M7V-25, M7V-26, authored case in M7V-47 |
| V8 oracle half: transition legality | ADR-rdb-0019 §1 V8 | M7V-36..M7V-41. **V8's timing half is kernel-b's L1 rows; neither half alone is V8** |
| V12 subset: unknown mandatory version refused before apply | ADR-rdb-0019 §1 V12 | M7V-34, M7V-35 |
| Evidence schema reused unchanged; the four ADR-0031 mirror rows; two campaign artifacts | ADR-rdb-0019 §2; V-R5; V-R17 | M7V-72..M7V-77, M7V-62 |
| `--test campaign` fits one debug gate run | critic T-11; §2 aggregate budget | §2 (shared corpus, ≤ 14 executions, < 120 s target recorded not asserted), M7V-58, M7V-61, M7V-65 |
| Handoff gate: `CARGO_TARGET_DIR=.rtargets/verification scripts/gate.sh test -p rdb-sim --test oracle --test scenarios --test campaign` green at handoff — **green with invariants `unavailable`, by design** | charter; VA-9 command 1 | VA-9, §2 budgets, M7V-52 |
| Every fixture and authored case is realizable by the runner; no assertion weakened to make it so | design §4.5; charter DO-NOT | M7V-88 (`Unavailable{Capability(I1)}` until I1) |
| **M7 release gate:** `SPIKE_REQUIRE_ALL=1 RETCD_EVIDENCE=1 CARGO_TARGET_DIR=.rtargets/campaign scripts/gate.sh test --release -p rdb-sim --test campaign` green — every invariant `proven` with `seeds_armed > 0`, `full_scale: true`, no `required_missing`, release artifact written. This is the command ADR-rdb-0019 §2.1 names as the milestone claim | V-R18; VA-9 command 3; ADR-rdb-0019 §2.1 | M7V-87 (the command and its gate function), M7V-54, M7V-78, M7V-89, M7V-75, M7V-55, M7V-62 |

**Row count: 90** (`M7V-01`..`M7V-90`; ids stable, `M7V-78..M7V-88` added in correction round 1,
`M7V-89` and `M7V-90` in round 2). Oracle 45 (41 + 79, 81, 85, 90) · grammar/generator 9 (6 + 80, 86, 88) ·
reducer 10 (8 + 83, 84) · campaign 18 (14 + 78, 82, 87, 89) · mutations 7 (6 + 81 counted once,
under oracle) · evidence 6 — the blocks overlap by the reserved ids 20–23 and by M7V-81, and the
distinct id set is `M7V-01..M7V-90`. By class: unit 59 · sim 12 · campaign 19 (§2).

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
| **F18** — header carries `Provenance`, not a bare `seed` | `trace-requirements.md` §1 — **but not in the landed C0** (`TraceHeader.seed: u64` at `8a23b1d`); routed to foundation under V-R20 (2), drift table row 1 below | M7V-46 (blocked, §12 "C0 + `provenance`") |

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
caveat the critic flagged under T-27. **T-39** (`required_copy_set_shape`: violation or fixture
defect) sits with the architect — if it is ruled a violation a row comes here and Q-35's
"size 1 or 4 is a fixture or cadence defect" sentence changes with it. **T-40** (no
`BoundaryId -> FaultKind` ground truth) belongs to M7V-42/M7V-55 and awaits foundation's handoff.

**Correction round 2 (critic round 3, T-23..T-35), written 2026-09-20 under ruling V-R20.** The
architect amended `design.md` §2.3/§2.4/§3.1/§4/§4.5, `trace-requirements.md` §8 and ADR-rdb-0019
in parallel; where a row here depends on one of those sections it cites the section **and** V-R20,
and the architect's text is authoritative for the design side.

| Ruling clause | What it settles | Where it lands (architect's files) | Rows written to it |
|---|---|---|---|
| **V-R20 (1)** (T-23) | `quorum_rule` is **not** a trace field; the oracle derives it from `required_copy_set.len()`; the cell is keyed on the derived value | `design.md` §2.3 (one sentence), §6 `derived_quorum_rule`; `trace-requirements.md` §3.14 withdrawn, §8 row 2 | §4 convention 1, M7V-07, M7V-08, M7V-09, M7V-79, M7V-55, M7V-56, Q-35, Q-38 |
| **V-R20 (2)** (T-23) | header `provenance` routed to foundation as the F18 C0 amendment | `trace-requirements.md` §8.1 | VA-1, VA-7 `trace_header`, M7V-46, §12 "C0 + `provenance`" |
| **V-R20 (3)** (T-28) | "every invariant whose packages are all `Wired` has `seeds_armed > 0` on the default corpus" | `design.md` §2.4 / §5 | **M7V-89**, VA-2, §12 last row, §13 |
| **V-R20 (4)** (T-35) | gating table keyed per `FaultKind` family on the emitting provider package | `design.md` §3.1 / §6; foundation's handoff names the emitter per member | VA-6, M7V-55, M7V-56, M7V-73 |
| **V-R20 (5)** (CR-1, CR-2) | `reason`: ADR form `capability(<pkg>)` in the artifact, `reason` + `package` on the log line; `catching_row` list-valued, key unchanged | ADR-rdb-0019 §2; `design.md` §5.3 | VA-7, M7V-72, Q-34 |
| **V-R20 (6)** (T-24) | `armed()` is end-of-fold state; `proven` and `seeds_armed` count per-seed `Proven`; M7V-31/33 assert `armed() == false` | `design.md` §2.4 | VA-2 (incl. the fold order, T-34), VA-7, M7V-31, M7V-33, M7V-52, M7V-78 |
| **V-R20 (7)** (T-25) | the required-cell gate applies only when `SPIKE_SEEDS >= N`; smaller corpora write `coverage_gated: false` | `design.md` §3.1 / §6 | VA-6, VA-7 `campaign_run`, §2, M7V-55, M7V-57, M7V-58, M7V-61, M7V-63, M7V-64, M7V-65, M7V-73, M7V-75, M7V-76, Q-38 |
| **V-R20 (8)** (T-26) | M7V-03(b) = header + ten `capability{Wired}`; `TraceBuilder::ack_from(n, s)` emits the secondary `batch_apply` | `design.md` §4 convention 4, §4.5, §2.4 "zero-event" | VA-1, §4 convention 4, M7V-03, M7V-07..M7V-11, M7V-28, M7V-79, M7V-81, M7V-88 |
| **V-R21** (lead Q-1, K-F-07) | the derived rule is authoritative for the coverage cell; if foundation lands `quorum_rule` the oracle cross-checks it and a mismatch is `quorum_rule_mismatch`; the oracle never reads the field for anything else | `design.md` §2.3 | §4 convention 1, **M7V-90**, §12 |
| **V-R21** (lead Q-2, INV-VER) | the wired ⇒ `seeds_armed > 0` clause runs over nine invariants; INV-VER is excluded by name until a producing `ScenarioOp` or `BoundaryId` exists; the excluded set is listed in design §2.4 | `design.md` §2.4; ADR-rdb-0019 rule 1 (`37e85a5`) | M7V-78, M7V-89 |
| T-27, T-29..T-33 (no ruling needed) | Q-35 runnable; §12 table repaired; Q-34 projects `package`; §2 sharer list and misnomer; M7V-62 selector-only; M7V-87 extraction rule | — | Q-35, §12, Q-34, §2, M7V-53, M7V-62, M7V-65, M7V-87 |

**Drift table — `trace-requirements.md` §3 as this plan cited it vs. the landed C0 at `8a23b1d`
(critic T-23).** Read from `git show 8a23b1d:crates/rdb-core/src/contracts/{trace,ids}.rs`, not
the working tree. Row numbers match `trace-requirements.md` §8's table so the two can be read side
by side. Disposition is **plan edit** (this plan changed) or **foundation ask** (the contract is
still requested; the affected rows sit in §12 under "C0 + \<ask\>"). Everything not listed below
was already written to the landed name.

| # | Field, as the plan wrote it | Landed shape at `8a23b1d` | Disposition | Rows changed |
|---|---|---|---|---|
| 1 | header `provenance: Provenance` | `TraceHeader.seed: u64`; no `Provenance` type | **foundation ask** (F18, V-R20 (2)); the plan writes `seed` until it lands | VA-1, VA-7 `trace_header`, M7V-46 → §12 |
| 2 | `protection_state{quorum_rule=Rf3 \| DegradedRf2}`; a `QuorumRule` enum | no field; no enum anywhere in `rdb-core` | **plan edit**: derived from `required_copy_set.len()` (2 → `DegradedRf2`, 3 → `Rf3`), V-R20 (1); the cell is `derived_quorum_rule × DegradedRf2` and the axis is verification's own two-member enum in `coverage.rs` | §4 conv. 1, M7V-07, 08, 09, 79, M7V-55, M7V-56, Q-35, Q-38 |
| 3 | `protection_state{state=…}` | `phase: ProtectionPhase{Healthy, Warn, Paused, Resuming}` | plan edit | VA-2, M7V-36, M7V-39, M7V-56 (`ProtectionPhase`) |
| 4 | `replication_ack.peer_boot_id` | `peer_boot: BootId` | plan edit | §4 conv. 1, M7V-28, Q-35 |
| 5 | `peer_role: PeerRole = Regular \| Shadow` | `peer_role: ReplicaRole{Primary, RegularSecondary, Shadow}` | plan edit: every `Regular` → `RegularSecondary` | §4 conv. 1, M7V-07, 08, 09, 10, 11, 28, 69, 81 |
| 6 | `ack_evidence: [(node, boot, role, durability)]` | `Vec<AckEvidence{node, role, durability}>` — no boot | plan edit: three-field struct literals; the boot is read from the paired `replication_ack` | M7V-07, 08, 09, 79, 81 |
| 7 | `topology_change.nodes=[n1:Primary, …]` | `Vec<(NodeId, ReplicaRole)>`, a tuple → JSON two-element array | plan edit: `(n1, Primary)` literals; DuckDB `n[1]`, `n[2]` | M7V-08, M7V-10, Q-35, VA-7 |
| 8 | header `topology{nodes, config_version_0}` | flat `Vec<TopologyEntry{node, partition, role, config_version}>` | plan edit | VA-1, VA-7 `trace_header`, M7V-10, Q-35 |
| 9 | `admission_decision.reason: AdmissionReason` (`PROTECTION_PAUSED`, …) | `reason: Option<ErrorKind>` (`ProtectionPaused`, `RequestIdReuse`, `CrossAffinity`, `GenerationChanged`, …) | plan edit: real variants; the admission axis is the `ADMISSION_REASONS` subset const, pinned by M7V-25/39 | M7V-25, M7V-39, M7V-56 |
| 14 | `client_submit.affinity_id` | `affinity: u64` | plan edit | M7V-25 |
| 15 | `client_outcome{outcome=UnknownOutcome}` (bare error name) | `ClientOutcome::{Success, RecoveredApplied, Error(ErrorKind)}` | plan edit: `Error(UnknownOutcome)`, `Error(StatusExpired)` | M7V-12, M7V-26 |
| 16 | a `status` event | one `Read{request_kind: ReadRequestKind}` kind; `Status` is a value | plan edit | M7V-26 (M7V-06 already used `request_kind`) |
| 17 | `schedule_phase{…}` | `SchedulePhaseChanged{phase, fair_delivery, remaining_event_budget}` | **plan shorthand kept** for the row literal; `@m` is `schedule_phase_changed`; no Q-row reads it (§4 conv. 1) | §4 conv. 1, VA-7 |
| 15′ | `client_outcome{…}` | `ClientOutcomeReported{request, outcome, generation, seq, result_digest, delivered}` | **plan shorthand kept**; `@m` is `client_outcome_reported`; no Q-row reads it | §4 conv. 1, VA-7 |
| 21 | `op_skipped{scenario_op_index, reason=ReferentGone}` | **not landed** — no `OpSkipped` kind | **foundation ask** (F17, standing from round 1; K-F-08 in foundation's list); rows wait under §12 "C0 + `op_skipped`" | M7V-22, M7V-88 → §12 |
| 22 | envelope `node_id`, `correlation_id`, `partition_id` | `TraceEvent{event_id, logical_tick, partition, node, boot, correlation}` | plan edit: row literals write `node=`; Q-35..Q-37 read the landed names so they are runnable | §4 conv. 1, VA-7, M7V-07..11, 28, 36, 41, 79, Q-35, Q-36, Q-37 |
| 11, 12, 13, 18, 19, 20 | `sync_wal_through_prefixes`, `published_state_digest` withdrawal, `batch_id`, `scenario_op_index: usize`, `Vec<FieldId>`, `replication_ack_delivered{ack_event_id, accepted}` | `captured`, present, `batch: u64`, `u32`, `Vec<u16>`, `{ack: EventRef, …, counted}` | adopt — no row or query in this plan reads them by the old name | — |

Foundation asks still open after this round: **row 1** (`provenance`) and **row 21** (`op_skipped`).
Nothing else in this plan waits on a contract change. The working tree observed during this round
carried an uncommitted `Provenance` enum, a `quorum_rule` field and an `OpSkipped` kind; none is
relied on here (only `8a23b1d` is), and if `quorum_rule` lands anyway V-R20 (1) still holds — the
oracle derives and never reads it (the architect's handoff carries the cross-check question).

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
