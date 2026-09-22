# Handoff — team verification, test planner (2026-09-20)

## 1. Outcome

**COMPLETED.** `docs/testing/test-plan-m7-verification.md` is written: **77 rows**
(`M7V-01`..`M7V-77`), 9 architecture requirements (`VA-1`..`VA-9`), 7 DuckDB Q-rows
(`Q-34`..`Q-40`), 8 anti-flake rules, an "Unavailable until \<package\>" table, a gate checklist
mapping every charter and ADR-rdb-0019 acceptance item to rows, 6 open questions with defaults, and
a source-state section. No Rust, no cargo, no commits, no crates, no tests created.

M7V-20, M7V-23 and the six INV-LAG rows were written **last**, after re-reading `design.md` §2.3,
§4.4 and ADR-rdb-0019's V8 row at 18:48 — per the lead's ordering instruction. The architect's
final round (F8, F18, F19, F20, F21, F22) had landed by then in all three files; the rows match the
corrected text, not the round-1 text. Evidence in §4.

## 2. Artifacts

| Path | What |
|---|---|
| `docs/testing/test-plan-m7-verification.md` | the plan (the only project file written) |
| `.claude/scratchpad/conversation_memories/rdb-partition-database/teams/verification/test-planner-handoff.md` | this file |

Nothing else was created or modified. `design.md`, `trace-requirements.md`, `critic-design.md`,
ADR-rdb-0019 and all other teams' files were read only.

## 3. Criterion to evidence

| Criterion (task) | Evidence |
|---|---|
| Rows first, one row = one test, id prefixes the test name | plan §"How to use", §3–§9. Example: `M7V-08` → `m7v_08_pub_degraded_rf2_one_ack_publish_violates` |
| Every charter acceptance item covered | §13 gate checklist — 18 criterion rows, each mapped to row ids |
| Every §2.3 invariant: a bad trace that trips it **and** a valid trace that does not | §4, ten invariants: ATOM 04/05 · PUB 06–12 · AUTH 13–15 · LIN 16–19 · DEDUP 24–26 · LOSS 27–29 · LIVE 30/31 · ISO 32/33 · VER 34/35 · LAG 36–41. Anti-flake rule A2 requires the near-miss to differ from its bad twin by exactly one fact |
| Every §7 mutation: MUT-1/3/4 as trace rewrites, MUT-2/5 as injected faults | §8 — M7V-66 (MUT-1), M7V-67 (MUT-3), M7V-68 (MUT-4), M7V-69 (MUT-2, `NetworkOp::ForgeAck`), M7V-70 (MUT-5, `StorageOp::FalseDurable`), M7V-71 (completeness, enumerated) |
| Every §6 required coverage cell | M7V-55 (positive: `required_missing[]` empty at default scale), M7V-57 (negative control), M7V-56 (lists enumerated from enums, not hand-listed), M7V-73 (recorded) |
| Reducer signature rows M7V-20 and M7V-23 as the critic restated them in F21 | §6 — the F21 restatement is quoted at the head of the section. M7V-20 asserts core-tuple equality (`faults` excluded), strict op reduction, and `.orig.json` replays and fails. M7V-23 asserts the two-defect scenario shrinks, the artifact records `slipped: true` naming both fault sets, and `.orig.json` still fails |
| Independence grep row | M7V-01 — reads every `.rs` under `support/oracle/`, fails on the six forbidden `rdb_core` modules, and fails on an empty file list |
| `Unavailable`-not-pass row | M7V-03 (per-checker), M7V-53 (campaign reports `unavailable`), M7V-54 (`SPIKE_REQUIRE_ALL=1` turns it into a gate failure) |
| Release-command and recorded-wall-time rows (V-R11) | M7V-60 (recorded, no threshold in the PR default), M7V-61 (asserted **only** under `SPIKE_ASSERT_WALL_MS`), M7V-62 (`profile` discriminator; `.rtargets/campaign` reserved). Commands in VA-9 |
| Regressions replay of both `.orig.json` and minimized fixtures | M7V-50 (every pair replayed; an orphan `.json` without its `.orig.json` fails the row), plus M7V-20/M7V-21/M7V-23 |
| Evidence rows producing `rdb-m7-campaign.json` and `rdb-m7-coverage.json` through `config-testkit write_evidence` (V-R5) | §9 — M7V-72..M7V-77, mirroring rEtcd M6-113..M6-116. VA-8 pins the dev-dependency direction and forbids a second disclaimer constant |
| Each row: id, name, what it proves, inputs, assertion, runtime class, dependency | every table in §3–§9 has exactly those columns |
| Q-rows: DuckDB over the JSONL logs a developer uses to debug a failing row | §10 — Q-34 (which checker fired, and whether it was merely unavailable) · Q-35 (INV-PUB: pinned set vs acks vs flushes) · Q-36 (authority overlap) · Q-37 (lineage chain walk) · Q-38 (coverage shortfall) · Q-39 (reducer trajectory and slippage) · Q-40 (capability honesty + the no-key-bytes rule). VA-7 defines the log fields they read |
| Rows that cannot pass until a kernel package lands are marked, with what the runner reports meanwhile | §12 — three mechanisms plus a per-row table. A blocked row is **upgraded in place**, never duplicated into a `*_v2` row |
| Fast tests, high coverage | §2 taxonomy: unit < 100 ms (61 rows), sim < 2 s (10 rows), campaign (16 rows). All oracle rows are unit-class because they run on hand-built traces (VA-1), which is what makes the invariant coverage cheap |
| Dense plan, tables, short cells | tables throughout; prose only where a rule needs a reason a reader would otherwise delete |

## 4. Commands run and observed results

Read-only throughout. No cargo, no gate script, no git operations.

| Command | Observed |
|---|---|
| `wc -l` over the team's inputs | `ledger.md` 137 · `design.md` 611 (at first read) · `critic-design.md` 744 · `trace-requirements.md` 294 · `architect-handoff.md` 192 · `test-plan-m6.md` 1401 · `implementation-spikes.md` 425 · `validation-plan.md` 73 · ADR-rdb-0019 204. All read |
| `grep -n '^#\|^##\|^###' docs/testing/test-plan-m6.md` | §7 evidence rows at line 838, §11 DuckDB Q-rows at 979 (Q-27..Q-33) — this plan continues at **Q-34** |
| `grep -n 'fn write_evidence' crates/config-testkit/src/evidence.rs` | line 250: `pub fn write_evidence(name: &str, values: serde_json::Value, run: RunInfo) -> PathBuf`; `RunInfo::{start, scaled, scale_factor, full_scale, elapsed, seed}` at 165–223. VA-8 is written against this exact signature |
| `ls -l --time-style=+%H:%M:%S teams/verification/ docs/ADRs/rdb/0019*.md` (twice, 18:41 and 18:48) | first pass: `design.md` 18:39, `trace-requirements.md` 18:29, ADR 18:29. Second pass: `design.md` **18:47**, `trace-requirements.md` **18:46**, ADR **18:43** — the architect's final round landed between the two |
| `grep -n 'faults\|slipped' design.md` (second pass) | line 358: `faults: BTreeSet<BoundaryId>, // recorded and reported, NOT part of the acceptance predicate`; 363: core tuple "nothing else"; 383: `slipped: true` into `rdb-m7-campaign.json`. **F21 landed** — M7V-20/M7V-23 written to it |
| `grep -n 'V8 ' docs/ADRs/rdb/0019*.md` | V8 `Form` now reads "every node in the `required_copy_set` pinned at `paused_prefix_seq`" and "no `admission_decision{outcome=Admitted}` at or after the first `protection_state{state=Paused}`". **F8 and F20 landed** in the ADR, not only in the design |
| `grep -n 'topology_change' design.md trace-requirements.md` | design §2.1/§2.3/§2.5/§9/§10 and trace-req §3.19 + ask 7. **F19 / V-R12 landed** — M7V-10 written to the config-versioned rule |
| `grep -n 'provenance' trace-requirements.md` | §1 header row is `provenance: Provenance`, not a bare `seed`. **F18 landed** — M7V-46 written to it |
| `sed -n '/^### 2.5/,/^### 2.6/p' design.md` (second pass) | §2.5 is now after §2.4 and grounds `peer_role` in the config-versioned topology. **F22 landed** |

No test was run because no `rdb-*` crate compiles yet (`crates/rdb-core/` is untracked and in
flight with team foundation; `crates/rdb-sim/` has no `Cargo.toml` registered in the workspace).
That is expected: the charter says the campaign must be planned before the kernel exists.

## 5. Assumptions and deviations

1. **Assumed** the `M7V-20..M7V-23` ids are reserved, because `design.md` §4.3/§4.4 and the critic's
   F21 name them by id. The oracle block therefore runs `M7V-01..M7V-19` and continues at
   `M7V-24`. The architect's handoff §5 called these "suggestions, not a reservation"; the critic
   then restated two of them by id, which makes them load-bearing. Stated in the plan's numbering
   note.
2. **Assumed** architecture requirements get their own series `VA-1..VA-9` rather than continuing
   rEtcd's `TA-67`, because the surfaces are in different crates and mixing them makes §13's string
   matching ambiguous. Q-rows **do** continue rEtcd's series (`Q-34`) because the log directory is
   shared.
3. **Assumed** a "valid trace that does not trip" row should be a **near-miss** — differing from its
   bad twin by the one fact that makes the behaviour legal — rather than a generic good trace.
   A generic good trace proves the checker is silent, not that it draws the line in the right place.
   One generic positive control exists on purpose (M7V-02). Anti-flake rule A2.
4. **Assumed** rows blocked on a package are written as compiling tests that assert the
   `capability{state=Unavailable}` path and are **upgraded in place** when the package lands
   (team-rules "no v2 files"). Rows that cannot be written at all are counted as missing by §13's
   checklist rather than omitted. §12.
5. **Deviation:** I did not write any `.rs` file, did not create `crates/rdb-sim/tests/*`, and did
   not run cargo. The charter's evidence command is the developer's to run.
6. **Deviation:** no `mcp__hitl__*` call. The task says I cannot ask the user; questions are in
   plan §14 and repeated below.

## 6. Questions for the lead — each with my default

| # | Question | My default (proceed on this if you do not answer) |
|---|---|---|
| Q-1 | M7V-23's strongest form replays `.orig.json` against a checker configuration in which the **minimized** fixture passes — that needs a way to suppress one rule for one replay. Acceptable, or should the row settle for "both fixtures currently fail"? | **Acceptable, scoped to the test binary:** a `Report::without_rule(&str)` on the oracle's *report*, never on the checker itself. The weaker form catches only the artifact half of F4, not the "fix R1 and the F1 bug ships" half that F4 is actually about |
| Q-2 | Does the M7 gate accept the campaign writing `docs/evidence/rdb-m7-*.json` on every ordinary `scripts/gate.sh` run, given those are tracked files that will churn in every diff? | **Yes, churn accepted** — rEtcd already behaves this way, and ADR-rdb-0019 §2 forbids `#[ignore]`. Fallback if not: write under `$RETCD_TEST_LOG_DIR` by default and to `docs/evidence/` only under `RETCD_EVIDENCE=1`, which weakens M7V-75 |
| Q-3 | Do kernel teams write their scenarios against **this** grammar, and is `support/scenarios` importable from `tests/authority.rs` etc.? | **Yes, shared through `tests/support/`** (foundation's `mod.rs`). If each kernel team builds its own scenario types, M7V-32 and P1's "freezes only its partition" acceptance are unreachable from their side |
| Q-4 | Does foundation agree `BoundaryId` stays **exactly** spike §6's boundary column plus the two V-R9 members, so M7V-56 can assert set **equality**? | **Yes, equality.** A third member for an unrelated reason degrades M7V-56 to containment and the coverage axis stops being provably complete |
| Q-5 | `shrink_ms` on a run where nothing shrank: `0`, or key absent? | **Always present, `0`.** An absent key and a zero are indistinguishable to a reader; M7V-72 asserts key presence |
| Q-6 | Does M7V-77's no-production-claim grep cover `.claude/scratchpad/**` team notes? | **No — tracked documents only.** Working notes are history (AGENTS.md). One more path in the row's list if you want them covered |

## 7. Requests to the lead for routing (files I do not own)

1. **`crates/rdb-sim/Cargo.toml`** — `config-testkit` as a **dev-dependency** (V-R5, VA-8). Team
   foundation's file. Without it none of §9's evidence rows can compile.
2. **`crates/rdb-sim/tests/support/mod.rs`** — register `oracle` and `scenarios`. Foundation's
   file; already requested by the architect (§8 item 1), repeated because it blocks every row.
3. **The four seam-freeze trace asks** are already routed and landed in `trace-requirements.md`
   (V-R10 ×3, V-R12 ×1). Four rows are their canaries and will fail loudly if any is dropped
   during C0: M7V-08 (`protection_state` cadence), M7V-10 (`topology_change`), M7V-26
   (`ClientOutcome::RecoveredApplied`), M7V-28/29 (`replication_ack` at the secondary). Worth
   naming in foundation's seam review.
4. **VA-4's two provider hooks** — H1 `ForgeAck`, M1 `FalseDurable`. Already routed (architect §8
   items 6–7). M7V-69 and M7V-70 are blocked on them, and with them V1 clause 3's modelled half and
   the `ForgedIdentity` coverage cell.
5. **kernel-b**: `SurvivorInventory::debug_view()` (ruling B-R5) is **not** consumed by any row in
   this plan. INV-LIN and INV-LOSS read `recovery_decision` from the trace, which is stronger
   (declaration comparison, not a peek at kernel state). If kernel-b built `debug_view()` for
   verification's benefit, it can be dropped or kept for kernel-b's own rows — the ruling said the
   test planner consumes it, and on reading the design I concluded it should not.
6. **kernel-b**: V8's timing half (1 s warn / 2.1 s pause) is **cited, not asserted** here
   (`design.md` §2.6). §13 records "neither half alone is V8". kernel-b's L1 rows must carry the
   kernel row **and** the harness row, or V8 has no owner for its timing half.

## 8. Risks

| # | Risk | Signal | Mitigation in the plan |
|---|---|---|---|
| R-1 | **The plan's green becomes meaningless** because every invariant reports `Unavailable` while kernel packages land, and someone reads a green gate as a pass | `scripts/gate.sh` green with `invariants.*` all `unavailable` | M7V-03, M7V-53, M7V-54 and §12's table. The gate lives in the evidence check, not the exit code — ADR-0031's `full_scale:false` mechanic applied to capability |
| R-2 | **M7V-08 is written after the checker** and passes on the healthy-RF3 rule by accident, so critic F1's bug survives the correction round it caused | the row is green on its first run | Anti-flake A7: write the failing row first. The plan says so in M7V-08's own cell, not only in the rules list |
| R-3 | **Fifty-eight unit rows on hand-built traces test the fixture, not the kernel** (52 at critic round 2 — critic T-17 corrected the earlier "sixty-one"; 58 after correction round 1 added M7V-79, 81..85) — the standing criticism of an oracle-first test plan | a checker passes the campaign but the rows never exercised its real input shape | The rows are the O1 acceptance the charter asks for ("deliberately bad traces trigger each checker"); the kernel side is the campaign (M7V-52..M7V-65) plus the two injected-fault rows. §8 states the boundary in one sentence so nobody over-reads a rewrite row. This is the finding I most expect from critic round 2, and the honest answer is that it is the division spike §5 chose |
| R-4 | **The near-miss rows are where flake will come from** — INV-LIVE and INV-ISO arm on a `schedule_phase` and a budget, and a fixture that drifts near the budget edge will flip | M7V-31/M7V-33 intermittent | Both are unit-class on hand-built traces with an explicit `remaining_event_budget`, so there is no scheduler to drift. Under the campaign the same checkers disarm rather than fire (A5) |
| R-5 | **The reducer rows (M7V-20..M7V-23, M7V-48, M7V-50) are the slowest and the last to be writable**, because all six need I1 | the developer reaches the end of the work order with no runner | `design.md` §9's order is preserved in §12: hand-built traces and checkers first (no dependency), grammar and generator next (no dependency), reducer and campaign last |
| R-6 | **`docs/evidence/rdb-m7-*.json` churn** makes every rDB diff noisy and someone `.gitignore`s it, silently deleting the M7 claim | the files stop appearing in diffs | Q-2 above; M7V-75 asserts the gate rule both ways so a quiet removal fails a row |
| R-7 | **The 77 rows are a backlog, not a schedule.** Sixteen are campaign-class and the campaign does not exist yet | the developer starts at M7V-01 and works numerically into blocked rows | §12's three-mechanism table plus §13's checklist are the ordering; the numbering is by topic, not by sequence |

## 9. Recommended next role

**Critic (round 2)**, per team-rules — attacking the **test plan**, not the design. Point them at:

- **§4's near-miss rows.** The bad rows are easy; the near-misses are where a checker that draws
  the line in the wrong place hides. M7V-09 (RF2 1-of-1 is legal), M7V-19 (the oracle must **not**
  derive compatibility), M7V-29 (buffered loss at a new boot is legal) and M7V-40 (publish while
  paused is legal) are the four I would attack first — each one forbids an over-correction that a
  reasonable developer would otherwise write.
- **§6's M7V-20 and M7V-23**, which are the rows the critic's own F21 rewrote. If the F21 split
  (core tuple decides, `faults` reports) is still wrong, it is wrong here and nowhere else.
- **§4.9's INV-LAG rows**, which assert six things about a state machine kernel-b also implements.
  §2.6's "one property, one owner" claim is the thing to test: is M7V-36 a third copy?
- **§12**, the "Unavailable until" mechanism. If a blocked row can be written in a way that passes
  before its package lands, the whole honest-green scheme is decorative.
- **Row runtime classes.** Fifty-eight rows claim < 100 ms (corrected from "sixty-one", critic
  T-17). If any needs the runner, it is mis-classed and the handoff gate will not fit its budget.

The **developer** can start in parallel on `design.md` §9's dependency-free work — the checkers and
the grammar — because §3, §4, §5 and §8's rewrite rows depend on nothing but C0's trace types.

## Correction round 1

Written 2026-09-20 after critic round 2 (`critic-tests.md`, verdict FAIL: T-01/T-02 blockers,
T-03..T-16 material, T-17..T-22 advisory) and the lead's rulings V-R16..V-R19. Aligned to the
architect's parallel amendments as they landed (`design.md` §2.4, §3.1, §4.5, §5.1, §5.1.1, §5.3;
`trace-requirements.md` §3.18; ADR-rdb-0019 §2/§2.1 at `b0a4e58`) and to their handoff §C. Edit
tool only; no `sed -i`, no redirection; no git; no cargo. Files touched: this one and
`docs/testing/test-plan-m7-verification.md` — nothing else.

### Outcome

**COMPLETED on the plan side.** Every finding T-01..T-22 has a change in the plan. No finding
disputed. Rows M7V-78..M7V-88 added; no id renumbered or reused. Row count 77 → **88**. Classes
**unit 58 · sim 12 · campaign 18** (critic round 2 counted 52/9/16 — T-17's correction — and the
eleven new rows are 6/3/2).

### Finding → change → rows

| Finding | Change in the plan | Rows / sections |
|---|---|---|
| **T-01** (V-R16) | VA-2 rewritten from design §2.4: `Verdict = Proven \| Unavailable(Unavailable) \| Violated(Signature)`, reason enum `Capability(PackageId) \| NotArmed`, `fn armed(&self)`; "never inferred from silence" scoped to the `Capability` arm; arming events listed once (§2.4's list) and pinned by M7V-02. Both arms report, never pass. Campaign fold order and `seeds_armed` in M7V-52; `proven` ⇒ `seeds_armed > 0` as its own row; M7V-54's failure list is the union (not-`proven` with reason, and `proven` with `seeds_armed == 0`); Q-34 projects `reason` and `seeds_armed` with a second statement that must return zero rows (NULL counts as 0) | VA-2, VA-7, §2 hard rule 3, M7V-02, M7V-03, M7V-31, M7V-33, M7V-52, M7V-54, M7V-63, **M7V-78**, Q-34, §11 A5, §12 |
| **T-02** | M7V-08 rewritten: (a) `client_submit` + `admission_decision{Admitted, admitted_seq=9, config_version=4, required_copies=[n1,n2]}` on the `correlation_id`, `topology_change{cv=4, n3:Regular}`, grounded `replication_ack{from_node=n3,…}` with a `durability_advance`, publish counts n3 → `required_copy_set_unsatisfied`; (b) pin drift: `protection_state` cv=5 set [n1,n3] between admission and publish → `Violated` naming `config_version=4`. Cardinality twin as its own row; M7V-09 is M7V-08(a) with the ack from n2 (one fact differs) | M7V-07, M7V-08, **M7V-79**, M7V-09, M7V-36 |
| **T-03** | option (i): M7V-17 scoped to `batch_apply.entry_digest`; the third sub-case (reachable source, digest disagreement → quarantine from the kernel) is its own **sim** row because its class and dependency (I1+F1) differ from unit-class M7V-19 | M7V-17, M7V-19, **M7V-80** |
| **T-04** | `required_copy_set` read by INV-PUB (membership + cardinality) and INV-LAG (every member) with no shared helper; stated once in §4's preamble | §4 preamble, M7V-08, M7V-36 |
| **T-05** | M7V-02's input arms all ten; asserts `armed()` per checker by name | M7V-02 |
| **T-06 / T-07 / T-08 / T-22** | every event literal in §4 swept against `trace-requirements.md` §3 names (`from_node`, `peer_boot_id`, `contiguous_seq`, envelope `node_id`, `config_version` never `cv`, `queried_sources` fields); watermarks `>= N` stated once; Q-35 rewritten with the publish→`admission_decision`→`protection_state` pin join, a `topo` CTE over `topology_change` + header, and the resolved role beside `peer_role`; Q-38 reads `coverage_shortfall` ∪ `coverage_unavailable` ∪ `coverage_cell`; Q-39 second statement over `shrink_result`; Q-40 redaction as one `DESCRIBE` over every JSONL line | §4 preamble, M7V-10, M7V-11, M7V-18, M7V-28, M7V-36, M7V-38, VA-7, Q-35, Q-38, Q-39, Q-40 |
| **T-09** | MUT-2 split: M7V-69 is the kernel half (rejects with `ForgedIdentity`, cell hit, INV-PUB `Proven`, conjunction — no `or`); oracle half is a new unit row on the recorded trace rewritten to count the forged ack. M7V-71 maps MUT-2 to both | M7V-69, **M7V-81**, M7V-71, §13 |
| **T-10** | M7V-51: both keys present and distinct; `wall_ms` excludes shrink via the timer instrumentation; a shrink occurred via `shrink_step` count > 0 and one `shrink_result` line; no duration asserted non-zero; shares M7V-64's corpus | M7V-51, Q-5 |
| **T-11** | §2 aggregate budget: `OnceLock`-shared default corpus read by M7V-52/53/55/72/73/78; gate-function rows (M7V-54, M7V-87) never re-run the loop; environment-varying rows own one corpus each at the smallest useful scale (M7V-58 at 8 seeds, M7V-61 at 4, M7V-65 64 vs 128 — never the 1,000-seed PR corpus); ≤ 14 executions; < 120 s target recorded, not asserted | §2, M7V-58, M7V-61, M7V-63, M7V-65, M7V-75 |
| **T-12** (V-R19) | VA-6: `REQUIRED[i mod N]` scheduling, attempt ≠ hit, `BoundaryId -> ScenarioOp` producer table and `BoundaryId -> Option<PackageId>` gating table, hook-gated cells under `unavailable_cells`, excluded by capability never by editing the list. M7V-55 asserts the schedule covers the set (from the seed list alone) plus observed counts, with the `Wired`-but-missing branch; M7V-57 gains the second branch; M7V-56 equality against the 29-member enum plus both tables enumerated; M7V-42/43/44 carry the schedule | VA-6, M7V-42, M7V-43, M7V-44, M7V-55, M7V-56, M7V-57, M7V-73, Q-38 |
| **T-13** (V-R17) | VA-9 names `rdb-m7-campaign.json` (debug) and `rdb-m7-campaign-release.json` (release); M7V-62 asserts both exist with different `profile`, only the release one cited; M7V-72 names the artifact by profile; file-mapping table lists both | VA-9, M7V-62, M7V-72, §1 mapping, Q-2 |
| **T-14a** (V-R18) | VA-9 is three commands; command 3 is the M7 release gate; `SPIKE_REQUIRE_ALL=1` in no other command; §13's last line cites it; new row ties the command const to ADR §2.1's string and applies its environment to the gate function | VA-9, §2 knob table (new "M7 release gate" column), **M7V-87**, §13 |
| **T-14b** (V-R18) | `capability{state}` derived from `Module::capability(&self)` over `ModuleName::ALL` (foundation's `Dispatcher::capability_report` at `8a23b1d` meanwhile): events equal the report one for one; stub `Ok`/`Unavailable` modules reported positively; no `CapabilityState::Wired` token under `crates/rdb-sim/src/harness/` outside the report builder | **M7V-82**, M7V-53, M7V-56 |
| **T-15** | two source-level guard rows in M7V-49's style: F5 (`Budget` has no event-stream index; `Heal` only a `NetworkOp`) and F12 (reducer only removes ops; candidate ops are a subsequence of the parent's) | **M7V-83**, **M7V-84** |
| **T-16** | M7V-50: every fixture expectation is `fails`, no `passes`; retirement = delete both files + one ADR-0019 Notes line; pairing asserted both ways. `without_rule` bounded to one call site by its own row | M7V-50, **M7V-85**, Q-1 |
| **T-17** | class counts recomputed from the tables: 52/9/16 at round 2 → 58/12/18 now; R-3 restated here (61 → 52 → 58) | §2, §13, this file §8 |
| **T-18** | §12 table lists M7V-20 under I1, M7V-51, M7V-62 (arms only in the release gate), M7V-66..68 under C0, plus every new row and its reason | §12 |
| **T-19** | M7V-29's violating sub-case: "reachable at the same `boot_id`, i.e. never restarted" | M7V-29 |
| **T-20** | M7V-01 is an allowlist over every `rdb_core` path token (brace groups expanded), fails closed | M7V-01 |
| **T-21** | M7V-44 is the generator half; runner half is its own sim row (stops at `max_events`, ends at an event boundary, `NotArmed` for checkers not yet armed) | M7V-44, **M7V-86** |
| **architect §4.5** (critic on R-3, second-order) | fixture realizability: every scenario fixture and authored constructor replays without `op_skipped{ReferentGone}` and yields its row's verdict; every `TraceBuilder` trace passes I1's well-formedness checks; envelope checks only, and says so, if I1 exposes no validator; `Unavailable{Capability(I1)}` until I1 | **M7V-88**, §12, §13 |
| **architect §C extras** | `unavailable_cells{cell -> package}` in M7V-73 and VA-6; `invariants{id -> {status, reason?, seeds_armed}}` as an object in M7V-72; `mutations{id -> catching_row}` key name per ADR (list-valued for MUT-2); §2 knob table extended-gate `SPIKE_ASSERT_WALL_MS = 600000`; §15 rulings table marks V-R17/V-R18 landed at `b0a4e58`; `capability_id` → `package` everywhere | §2, VA-6, M7V-72, M7V-73, Q-40, §15 |

### New rows (M7V-78..M7V-88)

| Row | Name | Class | Dep |
|---|---|---|---|
| M7V-78 | `proven_status_implies_seeds_armed_positive_for_every_invariant` | campaign | I1 |
| M7V-79 | `pub_degraded_rf2_publish_on_the_primarys_own_durability_alone_violates` | unit | C0 |
| M7V-80 | `recovery_path_digest_disagreement_yields_quarantine_from_the_kernel` | sim | I1 + F1 |
| M7V-81 | `mut2_counted_forged_ack_trips_inv_pub` | unit | C0 |
| M7V-82 | `capability_state_is_derived_from_the_modules_own_report_never_a_literal` | unit | C0 + foundation dispatcher |
| M7V-83 | `budget_has_no_event_stream_index_and_heal_is_only_a_network_op` | unit | none |
| M7V-84 | `reducer_only_removes_ops_never_constructs_or_modifies_one` | unit | none |
| M7V-85 | `without_rule_has_exactly_one_call_site` | unit | none |
| M7V-86 | `runner_stops_at_max_events_and_ends_at_an_event_boundary` | sim | I1 |
| M7V-87 | `m7_release_gate_is_the_cited_command_and_fails_while_any_invariant_is_not_proven` | campaign | I1 + testkit |
| M7V-88 | `every_fixture_and_authored_case_is_realizable_by_the_runner` | sim | I1 |

### Rows held pending other teams' sections

None held on the architect: every section their §C lists was aligned after their text landed
(design §2.4/§3.1/§4.5/§5.1/§5.1.1/§5.3 read at 19:13+; ADR §2/§2.1 read at `b0a4e58`;
trace-requirements §3.18 read at 19:15). Two rows are written against **foundation** shapes that
may still move:

- **M7V-82** names `Module::capability(&self)` (K-F-10, not yet in the crate) and, meanwhile,
  foundation's landed `Dispatcher::capability_report`. If K-F-10 lands under a different name the
  row's Input column changes; its three observables do not.
- **M7V-56 / M7V-55** assert `BoundaryId` equality against the 29 members observed in
  `crates/rdb-core/src/contracts/trace.rs` at `8a23b1d`. If foundation's handoff lists a different
  set, the enum wins and the handoff is the thing to fix (Q-4).

### Deviations from the closure lines as the critic wrote them

1. **T-03's third sub-case is its own row (M7V-80), not a sub-case of M7V-19.** Its class is sim
   and its dependency I1+F1; M7V-19 is unit/C0. One row = one test (§2 rule 6).
2. **T-09 keeps M7V-69's id for the kernel half** and adds M7V-81 for the oracle half, rather than
   renumbering. M7V-69's name changed (`…_is_rejected_by_the_kernel`); its id did not.
3. **M7V-87 checks the release-gate command as a const cross-checked against ADR §2.1 and VA-9**,
   because a test cannot run `scripts/gate.sh`. The gate *function* under the command's environment
   is what it exercises; the real command's artifact is the milestone evidence.
4. **M7V-88 owns a < 10 s budget**, above the sim class's 2 s, because it replays every fixture.
   Stated in the row and in §2's class table.

### Questions for the lead (each with the default I applied)

| # | Question | Default applied |
|---|---|---|
| CR-1 | The architect's §C writes the reason as `capability:<package>`; ADR-rdb-0019 §2 (authoritative) writes `capability(<package>)`. The plan uses the ADR's form everywhere. Confirm the ADR form is the one the log line and artifact carry? | **ADR form** `capability(<package>)`; one token to change in VA-7/Q-34 if not |
| CR-2 | ADR §2 names `mutations{id -> catching_row}` (singular). MUT-2 now has two catching rows. The plan makes the value list-valued under the same key. Accept, or should the ADR key become `catching_rows`? | **List-valued under `catching_row`**; no ADR edit needed |
| CR-3 | M7V-88 depends on I1 exposing a trace validator. If I1 does not, the row degrades to envelope checks and reports `Unavailable{Capability(I1)}` for the rest. Should the validator be added to foundation's I1 asks (routing table) now? | **Yes, add as an ask**; the row is written to survive either answer |
| CR-4 | The critic's "fixtures realizable by the runner" mitigation is now design §4.5 + M7V-88. Q-1's `without_rule` surface stays, bounded by M7V-85. Keep, or drop `without_rule` entirely and let M7V-23 degrade? | **Keep, bounded** |

### Risks

| # | Risk | Mitigation / disposition |
|---|---|---|
| R-7 | **H1 `Wired` but `ForgeAck` path missing.** The gating table keys the `ForgedIdentity` cell on H1's capability entry. If foundation lands H1 without the hook, M7V-55 fails on `required_missing` with H1 `Wired` — loud, the safe direction — but it fails foundation's gate, not verification's. | Architect's §D-1 assumption stands (hook ships with the package). M7V-55 clause 3 names the failure so it is diagnosable in one read. |
| R-8 | **Parallel-edit drift.** VA-2 was first written from the critic's wording, then re-read against design §2.4 after it landed and aligned (arming list, "no fourth state", scoped silence rule). A second architect pass to §2.4 would not be reflected here. | The critic re-reviews both files together (architect §F). §15's rulings table records the timestamps each section was read at. |
| R-9 | **`NotArmed` is visible, not failing, in the PR default.** A checker that never arms on 64 seeds stays `unavailable(not_armed)` until someone reads the table. | M7V-78 fails on `proven`+0 (runner bug); M7V-88 catches the fixture-side cause once I1 lands; the release gate (M7V-87/54) fails on `not_armed`. Nothing fails in the handoff gate — by design, and stated in §13. |
| R-10 | **`Module::capability` does not exist yet.** M7V-82 is a design statement until K-F-10 lands; foundation's `capability_report` classifies any non-`Unavailable` step outcome as `Wired`. | M7V-82 is `Unavailable{Capability(C0)}`-style listed in §12 under "C0 + foundation's dispatcher"; it goes red the day a literal appears, which is the point. |
| R-11 | **The 88-row plan is larger than the 77-row one the critic reviewed.** Eleven rows added under time pressure are the ones most likely to carry a naming or class slip. | Mechanical check run: 88 distinct ids, none missing, none duplicated across sections; class column totals 58/12/18; no stale `capability_id`/`cv=`/`catching_rows` token; UTF-8, no CRLF. |

### Recommended status

**REVIEW** — critic re-review of the plan diff together with the architect's landed sections, one
round. Then the developer starts design §9's dependency-free work (checkers with `armed()`, the
grammar, the `TraceBuilder`), which needs nothing beyond C0 at `8a23b1d`.

---

## Round 3 (V-R20)

Written 2026-09-20 after critic round 3 (`critic-tests.md` "# Critic round 3", T-23..T-35) under
lead ruling **V-R20** (ledger line 202), plus the two mid-round additions from **V-R21** (ledger
lines 212/214; architect applied design §2.3/§2.4, TR §8, ADR 0019 rule 1 at `37e85a5`). Landed
C0 read from `git show 8a23b1d:crates/rdb-core/src/contracts/{trace,ids,errors}.rs`, never the
working tree. Edit tool only; no `sed -i`/`perl -i`; no git; no cargo. Files touched:
`docs/testing/test-plan-m7-verification.md` and this file — nothing else.

### Outcome

**COMPLETED on the plan side.** Every finding T-23..T-33 has a plan change; T-34/T-35 (architect
holds) are mirrored where the plan states the fold order and the gating key. No finding disputed.
Rows **M7V-89** (T-28) and **M7V-90** (V-R21 Q-1) added; no id renumbered or reused. Row count
88 → **90**. Classes **unit 59 · sim 12 · campaign 19**. Plan 881 → 1036 lines (CRLF, same as
HEAD; `git diff --numstat` 278 added / 123 removed — not a whole-file rewrite).

### Finding → change → rows

| Finding | Change in the plan | Rows / sections |
|---|---|---|
| **T-23** (V-R20 (1)(2)) | §15 gains the **drift table** (plan line ~987, header "Drift table — `trace-requirements.md` §3 as this plan cited it vs. the landed C0 at `8a23b1d`"), rows numbered to match TR §8 rows 1–23; each row: field as written → landed shape → plan edit \| foundation ask → rows changed. Only rows **1** (`provenance`) and **21** (`op_skipped`) remain foundation asks. Every row literal swept to landed names: `seed: u64` (M7V-46 header half blocked under "C0 + `provenance`" in §12); **no `quorum_rule` field** — M7V-07/08/09/79 and the cell key on the value derived from `required_copy_set.len()` (2 → `DegradedRf2`, 3 → `Rf3`), cell renamed `derived_quorum_rule × DegradedRf2`; `phase: ProtectionPhase`; `peer_boot`; `ReplicaRole::{Primary, RegularSecondary, Shadow}`; `AckEvidence{node, role, durability}`; `TopologyChange.nodes: Vec<(NodeId, ReplicaRole)>`; flat header `topology: Vec<TopologyEntry{node, partition, role, config_version}>`; `admission_decision.reason: Option<ErrorKind>` (M7V-25 `RequestIdReuse`/`CrossAffinity`/`GenerationChanged`, M7V-39 `ProtectionPaused`; M7V-56 names the 18 `ErrorKind` variants and pins `ADMISSION_REASONS: &[ErrorKind]`) | VA-1, VA-7, §4 convention 1, M7V-07..12, 25, 26, 28, 36, 39, 46, 55, 56, 69, 79, 81, Q-35..38, §12, §15 |
| **T-24** (V-R20 (6)) | VA-2: `armed()` is the checker's state at the **end of the fold**; `proven` and `seeds_armed` both count seeds whose per-seed verdict is `Proven`; M7V-31/33 assert `armed() == false` after the fold; M7V-52's synthetic fold adds the healed-then-exhausted case → `unavailable(not_armed)`, `seeds_armed = 0` | VA-2, VA-7 `invariant_status`, M7V-31, M7V-33, M7V-52 |
| **T-25** (V-R20 (7)) | VA-6: the required-cell gate applies only when `SPIKE_SEEDS >= N` (N = 29); VA-7 `campaign_run` gains `coverage_gated`; §2 states every sub-N corpus is `coverage_gated: false`; M7V-57 has three branches (at N fails, at N−1 records the shortfall and does not fail, `Unavailable` excluded); M7V-58/61/63/64/75/76 state `coverage_gated: false` | VA-6, VA-7, §2, M7V-55, 57, 58, 61, 63, 64, 75, 76, Q-38 |
| **T-26** (V-R20 (8)) | M7V-03(b) = header + exactly ten `capability{package=<each PackageId>, state=Wired}` and nothing else; new §4 convention 4: every ack is built by `TraceBuilder::ack_from(n, s)`, which emits the secondary `batch_apply`; rows 07/08/09/10/11/28/79/81 use it (M7V-79 deliberately has **no** `ack_from` — the absence is the point) | VA-1, §4 conv. 4, M7V-03, 07, 08, 09, 10, 11, 28, 79, 81 |
| **T-27** | Q-35 rewritten as runnable DuckDB: VA-7 `trace_header` line; `topo` = header `unnest(topology)` struct branch UNION ALL `topology_change` tuple branch indexed `n[1]::INTEGER`, `n[2]::VARCHAR`; `derived_rule` CASE on `len(required_copy_set)`; `unresolved_roles` column asserted `= 0`; `role_mismatches` filtered on `topo.role IS NOT NULL`; envelope fields `node`/`partition`/`correlation` | Q-35, VA-7 `trace_header` / "(trace events)" lines |
| **T-28** (V-R20 (3)) | **M7V-89** `every_fully_wired_invariant_arms_on_the_default_corpus` (campaign, I1): for every invariant whose `needs` packages all report `Wired`, `seeds_armed > 0` on the shared default corpus; names the arming op per checker from the producer table; during M7 reports `unavailable (no invariant fully wired)`; synthetic half fails naming the invariant | M7V-89, VA-2, §2, §12 I1 row, §13 |
| **T-29** | §12 table contiguous again (header at plan line ~855, last row ~871, paragraph moved below with a note saying why); new rows for M7V-46 (header half), M7V-22/M7V-88 (`op_skipped` clause), M7V-87 (I1 + testkit), M7V-90; M7V-62 removed from §12 with the reason stated | §12 |
| **T-30** (V-R20 (5)) | Q-34 projects `package`; `invariant_status` carries the ADR-form `reason` (`capability(<pkg>)`) in the artifact and `reason` + `package` on the log line (CR-1); `catching_row` is list-valued `["M7V-69", "M7V-81"]`, no ADR key change (CR-2) | VA-7, Q-34, M7V-72 |
| **T-31** | §2 sharer list adds M7V-60 (and M7V-89); "1,000-seed corpus belongs to the extended gate" misnomer fixed — it is VA-9 commands 2/3 (and the 10,000-seed extended run); knob-table column renamed; M7V-53 dep is "I1 + C0 capability event"; M7V-65 misnomer fixed | §2, M7V-53, M7V-65 |
| **T-32** | M7V-62 asserts the `artifact_name()` pure selector both directions vs `cfg!(debug_assertions)` and **this run's** `profile`; doc/const cross-check for `.rtargets/campaign`; never reads the other profile's tracked file; no longer `unavailable` | M7V-62, §12 |
| **T-33** | M7V-87 extraction rule: the first backticked span in the table row whose line begins `\| **M7 release gate**` (ADR §2.1) and `\| 3 \|` (VA-9); exactly one such row per file; zero or two rows fails | M7V-87 |
| **T-34** (architect hold, mirrored) | VA-2 states the per-run fold in order; plan cites design §2.4 + V-R20 | VA-2, M7V-52 |
| **T-35** (V-R20 (4), mirrored) | VA-6 gating table is `BoundaryId -> PackageId`, keyed per `FaultKind` family on the emitting provider package; M7V-56 asserts every member of a family names the same package; M7V-55/73 exclusions are family-gated | VA-6, M7V-55, 56, 73 |
| **V-R21 Q-2** (INV-VER) | M7V-89's wired ⇒ armed clause runs over **nine** invariants; INV-VER excluded by name (`WIRED_IMPLIES_ARMED_EXCLUDED` const beside the registry; row prints the exclusion with its reason, never as passing); M7V-78 states its `proven ⇒ armed` clause keeps all ten and points at M7V-89 for the exclusion | M7V-78, M7V-89, §15 rulings table |
| **V-R21 Q-1** (K-F-07) | §4 convention 1: the derived rule stays authoritative; a landed `quorum_rule` is only cross-checked. **M7V-90** `pub_landed_quorum_rule_disagreeing_with_the_derived_rule_violates` (unit; blocked under "C0 + `quorum_rule` (K-F-07)" in §12): bad trace `required_copy_set=[n1,n2], quorum_rule=Rf3` → INV-PUB `rule="quorum_rule_mismatch"` at the `protection_state` event; good trace `quorum_rule=DegradedRf2` → `Proven`; both halves record cell `derived_quorum_rule × DegradedRf2`; M7V-08/79/09 stay field-free | §4 conv. 1, M7V-90, §12, §13 V3 row, §15 rulings table |

### Beyond the critic's list (found in the sweep, all plan edits)

- Envelope names: `node_id`/`correlation_id`/`partition_id` → landed `node`/`correlation`/
  `partition` (drift row 22). Needed so the Q-rows are runnable against the landed JSONL.
- `client_submit.affinity_id` → `affinity` (row 14); M7V-24's dedup identity now written as
  `(tenant, client, request)` = landed `RequestIdentity` (`ids.rs` line 137) scoped by `affinity`.
- `status` event → `read{request_kind=Status}` (row 16, M7V-26); `Error(UnknownOutcome)` /
  `Error(StatusExpired)` on `ClientOutcomeReported` (row 15).
- Shorthands kept and declared in §4 convention 1: `schedule_phase{…}` → `SchedulePhaseChanged`,
  `client_outcome{…}` → `ClientOutcomeReported`; `@m` is full snake_case of the variant.
- `version_check.mandatory_unknown_fields` is `Vec<u16>` — M7V-34 writes `[17]`, not `[f17]`.
- `op_skipped` is not in the landed `TraceKind` — M7V-22 and M7V-88's `op_skipped` clause moved
  to §12 under "C0 + `op_skipped`" (drift row 21, K-F-08). Not a V-R20 item; flagged below.

### Commands run and observed results

All run from the repo root with Bash; `tr -d '\r'` because the plan is CRLF like HEAD.

| Command | Observed |
|---|---|
| `tr -d '\r' < docs/testing/test-plan-m7-verification.md \| grep -E '^\| M7V-[0-9]+ \| \`' \| grep -oE '^\| M7V-[0-9]+' \| sort -u \| wc -l` | **90** |
| same, `sort \| uniq -d \| wc -l` | **0** duplicates |
| `for i in $(seq -w 1 90); do … grep -qE "^\| M7V-$i \| \`" \|\| echo missing; done` | nothing missing |
| `… awk -F'\|' '{print $(NF-2)}' \| tr -d ' ' \| sort \| uniq -c` | **59 unit · 12 sim · 19 campaign** |
| `grep -oE '^#+ Q-[0-9]+'` | Q-34..Q-40 only (no clash with Q-41..54) |
| §12 contiguity: awk from the `\| Rows \| Unavailable until` header to the first non-`\|` line | first non-table line is the blank after the last row (M7V-70 … `proven` row); table unbroken |
| `grep -c` tokens | `coverage_gated` 17 · `ack_from` 11 · `armed() == false` 6 · `M7V-89` 16 · `M7V-90` 11 · `V-R21` 9 · `quorum_rule_mismatch` 3 |
| stale-literal sweep (`quorum_rule=`, `QuorumRule`, `peer_boot_id`, `PROTECTION_PAUSED`, `AdmissionReason`, `config_version_0`, `capability_id`, `node_id`, `correlation_id`, `partition_id`, `affinity_id`, `client_id`, `request_id`, bare `Regular`, `state=Paused`) | every remaining hit is inside the §15 drift table's "as the plan wrote it" column, the §15 superseded-sentence quote (`capability_id`, `state=Paused` F20 withdrawal), or M7V-56's "no `QuorumRule` enum" sentence — zero in a row literal |
| `git show 8a23b1d:…/trace.rs \| grep -n mandatory_unknown_fields`; `…/ids.rs \| grep -A6 'struct RequestIdentity'` | `Vec<u16>` (line 859); `{tenant, client, request}` (line 137) |
| `git diff --numstat`; `git show HEAD:<plan> \| grep -c $'\r'` | 278/123; HEAD is CRLF on all 881 lines, working copy CRLF on all 1036 — endings unchanged |

### Assumptions and deviations (reversible; say if wrong)

1. **M7V-90 placed under INV-PUB** (the `protection_state` reader that owns quorum arithmetic)
   rather than a new checker. V-R21 says "the oracle cross-checks"; INV-PUB is the natural home
   and keeps the ten-invariant list stable.
2. **INV-VER exclusion is a named const** (`WIRED_IMPLIES_ARMED_EXCLUDED`) so the row prints it
   and a future producing op is a one-line removal, per design §2.4 "listed explicitly".
3. **`ADMISSION_REASONS: &[ErrorKind]`** is a verification-owned subset const (M7V-56) because
   `ErrorKind` is wider than admission; pinned by M7V-25/39 so an unlisted reason fails.
4. The derived quorum rule is a verification-owned two-member enum in `coverage.rs` (M7V-56).
5. M7V-62 is out of §12 (it is no longer `unavailable`); M7V-87 is in (I1 + testkit).
6. M7V-26's `status` event became `read{request_kind=Status}` — TR §8 row 16 says one `Read`
   kind; if the architect meant a separate status event, M7V-26 is one literal away.

### Questions for the lead — each with my default

1. **`op_skipped` (drift row 21, K-F-08):** should it be a formal ask like `provenance`, or does
   foundation already hold it under K-F-08? **Default:** treated as an open foundation item;
   M7V-22/88's clause stays in §12 until it lands.
2. **M7V-24 dedup identity naming:** now `(tenant, client, request)` per landed `RequestIdentity`
   with `affinity` as scope; spec §5.3 words it with `affinity` inside the tuple. **Default:** keep
   the landed shape; the checker keys on `RequestIdentity` + `affinity` and the spec wording is a
   prose difference, not a semantic one.
3. **M7V-90's home:** INV-PUB (assumption 1). **Default:** INV-PUB; move only if the developer's
   checker layout makes `protection_state` a separate reader.

### Risks

- **R-12** The architect's round-3 design text was not fully landed when I read it (`ack_from`,
  `coverage_gated`, end-of-fold not yet in design.md at read time); I cited sections + V-R20 as
  instructed. If the architect's final wording differs (e.g. field name of `coverage_gated`), the
  plan's VA-7 line is one literal away. Critic should diff VA-7 against design §5.3.
- **R-13** Q-35's tuple branch assumes DuckDB reads a JSON `[2, "RegularSecondary"]` tuple as a
  list (`n[1]`, `n[2]`); if serde emits the tuple as an object, the branch becomes `n.\"0\"`,
  `n.\"1\"`. Runnable only once a real JSONL exists (I1).
- **R-14** M7V-90 is dormant until K-F-07; if foundation never lands `quorum_rule`, the row is a
  permanent `Unavailable{Capability(C0)}` in the oracle file. Acceptable per V-R21 ("if"), but
  the critic may prefer it deleted rather than dormant; my default is dormant + §12 row, because
  deleting it later is cheaper than re-deriving it.

### Recommended status

**REVIEW** — one critic-verification pass over this diff (`git diff -- docs/testing/test-plan-m7-verification.md`)
together with the architect's round-3 design/TR diff, then the developer starts design §9's
dependency-free work.

---

## Round 4 (critic T-36..T-41)

Written 2026-09-20 after critic round 4 (`critic-tests.md` "# Critic round 4"; verdict
**PASS_WITH_RISKS**, T-23..T-35 twelve closed, T-33 revised into T-36, **none sustained**,
developer may start). Plan baseline: committed round 3 at `1114bc7` (1036 lines, 90 rows) —
checked before editing, nothing else had landed. These are plan-text defects, not design
decisions, so no ruling was needed and none is claimed. Edit tool only; no git, no cargo;
`design.md`, `trace-requirements.md` and the ADR untouched (the architect holds those).

### Outcome

**COMPLETED on my four findings.** T-36, T-37 (my half), T-38 and T-41(d) closed; the critic's
unexecuted-claim note on Q-35 added. T-39 and T-40 are not mine to close yet and are recorded as
pending. Row count unchanged at **90** — no new row was needed, so none was added; ids stable,
next free id is still **M7V-91**. Plan 1036 → 1052 lines (`git diff --numstat` 22/6).

### Finding → change → evidence

| Finding | Change | Row / section | Evidence |
|---|---|---|---|
| **T-36** (material, highest) | M7V-87's plan-side selector narrowed from the bare `\| 3 \|` prefix to `\| 3 \| **The M7 release gate**`, which the §15 drift table cannot match; the drift table keeps its numeric first column (critic's default, Q-1). The rule now also names "every `\| N \|` row of the §15 drift table" as not-read, and the zero-or-two-match failure **prints every matching line** so a future collision is visible rather than guessed at | M7V-87 clause (1), plan line ~549 | `grep -cE '^\| 3 \| \*\*The M7 release gate\*\*'` = **1** (line 220); the old `grep -cE '^\| 3 \|'` still = 2 (line 220 VA-9, line 1014 drift row 3) — i.e. the collision is real and the new selector steps around it without deleting a drift row. ADR side unchanged: `grep -cE '^\| \*\*M7 release gate\*\*'` on `docs/ADRs/rdb/0019-…md` = **1**. Release-gate command still appears 3× in the plan, byte-identical |
| **T-37** (material, my half) | `coverage_gated` dropped from M7V-72's campaign-artifact key list; the row now says in words that ADR-rdb-0019 §2 puts it in `rdb-m7-coverage.json` only and that **M7V-73** owns it, and that this row neither requires nor forbids it in the campaign artifact — so exactly one row claims the key and a superset artifact does not go red. Duplicated `profile` sentence removed in the same edit | M7V-72, plan line ~585 | M7V-72's line mentions `coverage_gated` once, in the "deliberately not in this list" clause, not in the asserted key list; M7V-73 still asserts it (1 hit). Architect owns design §5.3's `caught_by` → `catching_row` and the stray `coverage{…}` key — untouched here by instruction |
| **T-38** (material) | M7V-89's arming-op table replaced by a **verbatim restatement of design §2.4's wired-clause list**, marked as a restatement and not a second list: INV-LOSS `LoneSurvivorChoice`/`UnequalSecondaryPrefix`; INV-LAG secondary partitioned then `TimeOp::Advance`; **INV-DEDUP the `RetainedDedupHit` boundary**; INV-LIVE/INV-ISO `NetworkOp::Heal`; INV-ATOM/INV-PUB/INV-AUTH/INV-LIN the first `Submit`. The wrong round-3 text is quoted once and named as wrong so the fix is not silently reverted. Added: under `REQUIRED[i mod 29]` the boundary-keyed arms land on two or three of the 64 seeds each, so expected `seeds_armed` for those is small and positive, never 64, and the failure names the *scheduled* seeds; if `gen.rs`'s producer table and design §2.4 disagree the row fails naming both. INV-VER exclusion (V-R21) left intact | M7V-89, plan line 535 | `RetainedDedupHit` now appears in M7V-89 (was absent from the row); design §2.4 read at lines 193–212 of `design.md` and restated word for word |
| **T-41(d)** (advisory, my half) | M7V-29's three `boot_id` mentions → `boot`; the input clause now cites the landed `QueriedSource{node, boot, role, reachable, …}` | M7V-29, plan line ~436 | `grep -c boot_id` on the plan = **1**, and that one is §15 drift row 4's "as the plan wrote it" column (`replication_ack.peer_boot_id`), which must stay |
| **Q-35 unexecuted claim** (critic's T-27 note) | Q-35 gains a bold caveat: the `topo` CTE's first branch reads `t.config_version` from an **unaliased** subquery, nobody has run it, and it is the only step in that state; run Q-35 once against a real `rdb-sim` JSONL log before trusting `roles_in_force` / `unresolved_roles` / `role_mismatches`; the fallback is an explicit alias (`… ) AS h`, `h.t.config_version`). States that §10 queries are diagnosis aids, so the correction is a doc edit, never a red row | Q-35 assertions, plan line ~692 | caveat present once; SQL unchanged |
| **§15 history** | New paragraph "Correction round 3 (critic round 4, T-36..T-41)" above the round-2 block, naming each fix and recording T-39/T-40 as held elsewhere; the status line at the top gains the round | §15, plan line ~968; §0 status line | — |

### Not closed here (by instruction)

- **T-39** (`required_copy_set_shape`: violation or fixture defect) — the architect is deciding;
  the lead routes the row to me if it is a violation. **When it comes:** one sub-case on M7V-07
  (a length-1 and a length-4 `required_copy_set` → INV-PUB `Violated`,
  `rule="required_copy_set_shape"`), and Q-35's sentence "a set of size 1 or 4 is a fixture or
  cadence defect" changes to name the violation. Until then the plan says fixture defect and
  design §2.3 says violation — a known, recorded disagreement, not an oversight.
- **T-40** (`BoundaryId -> FaultKind` has no ground truth in `rdb-core`) — the critic's own
  closure puts it on M7V-42 or M7V-55 (for every observed `fault_injected`, `fault_kind` equals
  the family the gating table assigns to `boundary`) and it needs foundation's authoritative map.
  I did not write it this round because the lead's list did not include it; it is a one-clause
  edit when wanted.
- **T-41(a)(b)(c)** — design §2.4's stale "M7V-78" reference, design §4.5's hard-coded
  `role=RegularSecondary` on `ack_from`, TR §3.14's "even when `state` is unchanged". All three
  are architect files.

### Commands run and observed results

Bash from the repo root; `tr -d '\r'` throughout because the plan is CRLF like HEAD.

| Command | Observed |
|---|---|
| `git log --oneline -3`; `git status --short -- <plan>` | round 3 at `1114bc7`, working tree clean before editing — nothing had landed |
| `grep -cE '^\| 3 \|'` / `grep -cE '^\| 3 \| \*\*The M7 release gate\*\*'` | **2** / **1** (collision confirmed, new selector unique) |
| `grep -cE '^\| \*\*M7 release gate\*\*' docs/ADRs/rdb/0019-…md` | **1** |
| `grep -c 'SPIKE_REQUIRE_ALL=1 RETCD_EVIDENCE=1 CARGO_TARGET_DIR=.rtargets/campaign scripts/gate.sh test --release -p rdb-sim --test campaign'` | **3** (VA-9 row 3, M7V-87, §13) — unchanged, byte-identical |
| `grep -c boot_id` | **1**, §15 drift row 4 only |
| `grep -n RetainedDedupHit` | M7V-89 (line 535) and §15's round-3 paragraph |
| `… \| grep -E '^\| M7V-[0-9]+ \| \`' \| grep -oE '^\| M7V-[0-9]+' \| sort -u \| wc -l` | **90**; `sort \| uniq -d \| wc -l` = **0**; `for i in $(seq -w 1 90)` gap check = none missing |
| `… awk -F'\|' '{print $(NF-2)}' \| sort \| uniq -c` | **unit 59 · sim 12 · campaign 19** — unchanged, still matches §2 and §13 |
| `grep -oE '^#+ Q-[0-9]+'` | Q-34..Q-40 only |
| `wc -l`; `grep -c $'\r'`; `git diff --numstat` | 1052 / 1052 (all CRLF, unchanged) / **22 added, 6 removed** |
| `git show 8a23b1d:…/trace.rs \| grep -A14 'pub struct QueriedSource'` | `{node, boot: BootId, role, reachable, reported_generation, reported_seq, …}` — confirms T-41(d)'s `boot` |
| `sed -n '193,212p' design.md` | design §2.4 wired-clause list, restated verbatim in M7V-89 |

### Risks

- **R-15** T-36's selector is still positional text matching. It is now unique, but any future row
  beginning `| 3 | **The M7 release gate**` would break it again. A stronger form — "the third data
  row of the VA-9 table" — was the critic's alternative; I took the label form because it is
  greppable and self-describing, and the row's failure now prints all matches, which turns a
  recurrence into a one-line diagnosis. Reversible.
- **R-16** M7V-89 now depends on design §2.4's list staying put. If the architect rewords it, the
  restatement drifts. Mitigated in the row itself: the row fails naming both sources when
  `gen.rs`'s producer table and design §2.4 disagree; it cannot mitigate a design-only reword.
  Worth a critic check each round that the two lists are still word for word.
- **R-17** Q-35's caveat documents an unverified step instead of fixing it. It cannot be fixed
  without a real JSONL log, which needs I1. The fallback alias is written down, so the first
  person to run it has the repair in hand.
- **R-14** (round 3) unchanged: M7V-90 stays dormant until K-F-07.

### Recommended status

**REVIEW** — one critic pass over this round's diff (six edits, no new row, counts unchanged),
concurrent with the developer's start; nothing in the start list is touched by these edits, and
the three rows the critic held (M7V-72, M7V-87, M7V-89's op table) are the three this round fixed.

---

## Round 5 (T-39, T-40, ruling F-R13)

Written 2026-09-20 after round 4 was accepted and committed at `d8b3877`. Three items the lead
routed once their rulings landed: T-39 ruled a **violation** (design §2.3), T-40 closed on the
design side (design §3.1), and **F-R13** (ledger, 2026-09-20 22:06 PDT) adjudicating K-F-07
against V-R20. Edit tool only; no git, no cargo; no code touched — the developer is building
against the start-now list in parallel. `design.md`, `trace-requirements.md` and the ADR untouched.

### Outcome

**COMPLETED.** Row count 90 → **89** (M7V-90 withdrawn, id retired not reused, next new id
`M7V-91`); unit class 59 → **58**; sim 12 and campaign 19 unchanged. Plan 1052 → 1072 lines,
`git diff --numstat` 44/24.

### Finding → change → evidence

| Item | Change | Row / section | Evidence |
|---|---|---|---|
| **T-39 — ruled a violation** | New sub-case on **M7V-07**, asserted in the same row: two traces otherwise identical to M7V-07's, one pinning `[n1]` and one pinning `[n1,n2,n3,n4]`, each with a satisfying grounded ack → INV-PUB `Violated`, `rule="required_copy_set_shape"`, signature carrying the observed length and `config_version`, fired **at the `protection_state` event** before any quorum arithmetic. The row states the ruling's reasoning in one sentence (the oracle reads only the trace; demoting it forces skipping INV-PUB for that seed — the silent skip §2.4 prevents) and names M7V-07's `[n1,n2,n3]` and M7V-09's `[n1,n2]` as the near-misses, so the sub-case differs from a passing trace by one fact (rule A2). A generated trace carrying one is a generator bug and surfaces through M7V-42/M7V-55 | M7V-07, plan line 401 | `grep -c required_copy_set_shape` = **4**: M7V-07, Q-35, §15 round-4 paragraph, drift row 2 |
| **T-39 — Q-35 aligned** | "a set of size 1 or 4 is a fixture or cadence defect" → "a set of any size but 2 or 3 is **a violation**, `rule="required_copy_set_shape"` … so a NULL here is read as INV-PUB firing and not as a trace to be repaired" | Q-35 assertions, plan line ~699 | the outlier wording the architect flagged is gone; plan and design §2.3 now agree |
| **T-40 — static half** | M7V-42 gains the T-40 clause: C0 has no `impl BoundaryId`/`fault_kind()`, so the family map keying the gating table has no contract ground truth; the row asserts **one family and one gating package per `BoundaryId` member** and points at M7V-55 for the behavioural half. States "no contract change and no foundation ask" | M7V-42 | `fault_kind` now appears in the M7V-42 line (1 hit) |
| **T-40 — behavioural half** | M7V-55 gains clause **(4)**: for **every** observed `fault_injected`, `fault_kind` equals the family the gating table assigns its `boundary` — the emitting provider is the ground truth. A run observing no `fault_injected` reports the clause `unavailable(not_armed)` rather than passing it (so the clause cannot pass vacuously, VA-2's rule) | M7V-55 | `fault_kind` now appears in the M7V-55 line (1 hit) |
| **F-R13 — M7V-90 withdrawn** | Row deleted from §4.2; §12 dependency row deleted; removed from the oracle file mapping, the unit-class list, the §13 V3 row (replaced there by "M7V-07's shape sub-case (T-39)") and the §13 tallies. §4 convention 1's cross-check paragraph rewritten: **there will be no `quorum_rule` field** — K-F-07 closed by derivation, foundation's `ProtectionState` at `6893442` carries none, the derived value is the only source, and `quorum_rule_mismatch` is withdrawn with the row. `QuorumRule` explicitly **stays** as verification's own two-member enum in `coverage.rs` (the `derived_quorum_rule` axis), named as not a contract type | §4 conv. 1, §4.2, §12, §13 ×2, §15 ×2, numbering note | `grep -cE '^\| M7V-90 \| \`'` = **0**; the remaining `M7V-90` hits (7) are all withdrawal records — numbering note, §4 conv. 1, §13 tally, §15 round-4 paragraph, §15 rulings row, drift row 2. `quorum_rule_mismatch` survives only in those same withdrawal records (4 hits, none in a row) |
| **History** | §15 gains "Correction round 4 (T-39, T-40 and ruling F-R13)"; the §15 V-R21 rulings row is marked **superseded by F-R13** with the reasoning and the fact that V-R21's other half (INV-VER's exclusion) is untouched; drift row 2 records that the field will never exist and that M7V-90 and `quorum_rule_mismatch` went with it; the numbering note records the retired id | §15, §0 numbering note | — |

### Commands run and observed results

| Command | Observed |
|---|---|
| `git log --oneline -2` | `d8b3877` round 4, `6893442` foundation round 1 — the commit F-R13 cites as having no `quorum_rule` |
| `grep -n 'M7V-90'` before / after | 11 sites → **7**, all of them withdrawal records; `grep -cE '^\| M7V-90 \| \`'` = **0** |
| `grep -c quorum_rule_mismatch` | **4**, none in a row |
| `grep -c required_copy_set_shape` | **4** (M7V-07, Q-35, §15, drift row 2) |
| `grep -E '^\| M7V-42 \|' \| grep -c fault_kind` / same for M7V-55 | **1** / **1** |
| `… grep -oE '^\| M7V-[0-9]+' \| sort -u \| wc -l` | **89**; `uniq -d` = **0**; gap check over 01–89 = none missing |
| `… awk -F'\|' '{print $(NF-2)}' \| sort \| uniq -c` | **unit 58 · sim 12 · campaign 19** — matches §2 and §13's tally line |
| §12 contiguity (awk from the `\| Rows \| Unavailable until` header) | 17 rows, unbroken, first non-table line at 879 |
| `grep -oE '^#+ Q-[0-9]+'` | Q-34..Q-40 only |
| `wc -l`; `grep -c $'\r'`; `git diff --numstat` | 1072 / 1072 (all CRLF) / **44 added, 24 removed** |
| `sed -n '116,124p' design.md`; `sed -n '386,400p' design.md`; `grep -n F-R13 ledger.md` | design §2.3's T-39 ruling text, design §3.1's T-40 clause, and F-R13's wording — all restated, not invented |

### One thing for the architect (not mine to edit)

`design.md` §2.3's INV-PUB paragraph still ends with the V-R21 sentence *"If foundation lands a
`quorum_rule` field (K-F-07), the oracle cross-checks it against the derived value and a mismatch
is the violation `quorum_rule_mismatch`"*. F-R13 makes that branch unreachable — the field will
never exist — so a developer reading design §2.3 will still look for a cross-check the plan no
longer has. One sentence to delete or mark superseded. I have not touched it.

### Risks

- **R-18** The withdrawal leaves `quorum_rule_mismatch` documented only as history. If foundation
  ever reverses F-R13, the row must be rewritten from the drift-table record rather than restored
  from an id — `M7V-90` is retired. That is the intended trade (T-14's "ids are stable" rule) and
  it is written down in two places.
- **R-16** (round 4) stands and is now yours to police: M7V-89 mirrors design §2.4 verbatim.
- **R-17** (round 4) stands: Q-35's `unnest(topology)` step is still unrun; it needs I1.

### Recommended status

**REVIEW** — one critic pass over this diff. Nothing here touches the developer's start-now list
except M7V-07 (a sub-case added to a row already in the list) and M7V-42/M7V-55 (one clause each);
M7V-90 was on hold and is now gone, so no in-flight work is invalidated.

---

## Round 6 (drift refresh at `ec610f4`; un-park M7V-46, M7V-22, M7V-88)

Written 2026-09-20. The lead's finding: verification was holding two contract asks open that
landed in foundation's **first** code round, at `6893442`. Three documents still called one of
them "the one open contract ask". The lead told me to verify it myself before acting, and to
re-read the **whole** drift table rather than the two flagged rows, because the whole table was
written against `8a23b1d`. Edit tool only. No cargo (workspace red for an unrelated reason), no
git state change, no `perl -i`/`sed -i`. Nothing under `crates/` touched — the oracle and
scenarios subtrees belong to another live agent.

### Outcome

**COMPLETED.** Verification now has **zero open contract shape asks**, stated in four places a
reader will meet it. Plan 1072 → 1133 lines (`git diff --numstat` 101/40). Row count **89,
unchanged** — no row added, none removed, no id moved.

### What I confirmed in the code, before changing anything

Read at `ec610f4` (`git log -1 -- crates/rdb-core/src/contracts` = `ec610f4`), from the committed
tree, not the working one.

| Ask | Asked shape | Landed | Verdict |
|---|---|---|---|
| **VER-TR8-1** header provenance (F18, V-R20 (2)), `trace-requirements.md` §8.1 | `enum Provenance { Generated{seed: u64}, Reduced{parent: ScenarioId}, Authored{case: String} }`, replacing `TraceHeader.seed` | `contracts/trace.rs:85`; `TraceHeader.provenance` `trace.rs:212`; `ScenarioId(u64)` `ids.rs:116`; **no `TraceHeader.seed`** (`git diff 8a23b1d ec610f4` shows `- pub seed: u64`) | **matches arm for arm, field for field, derive for derive.** `#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]` is the exact list §8.1 drafted. Landed at `6893442` |
| **VER-TR8-21** `op_skipped` (F17 / K-F-08), §3.16a | `op_skipped{scenario_op_index, reason: ReferentGone \| OutOfBudget}`, its own kind, not a `BoundaryId` member | `TraceKind::OpSkipped{scenario_op_index: u32, reason: SkipReason}` `trace.rs:1115`; `SkipReason{ReferentGone, OutOfBudget}` `trace.rs:634`; rustdoc cites K-F-08 and "team verification §3.16a" | **matches**, with one difference I am adopting rather than reporting as a mismatch: the index is `u32`, §3.16a wrote `usize`. That is the identical adoption already recorded in §8 row 18 for `fault_injected.scenario_op_index`, and ruling F-R13 lists `OpSkipped.scenario_op_index u32` among the four accepted name/type divergences. Landed at `6893442` |

Neither differs from its ask in a way that should hold a row. I un-parked both.

### Rows un-parked

- **M7V-46** — the header half. `Dep` was `C0 + provenance (foundation ask, V-R20 (2))`, now plain
  `C0`. Both halves run. While rewriting it I found a second, smaller staleness inside the row:
  it wrote the reduced arm as `Reduced{from}`; the landed field is `parent`. Fixed.
  I also gave the header half something to assert now that it can run: `TraceHeader` has **no**
  field named `seed`, which under the header's `deny_unknown_fields` makes a re-added `seed` a
  decode failure on every fixture. That is the clause that keeps the bare seed from coming back.
- **M7V-22** and **M7V-88** — the `op_skipped` clause. M7V-88's "replays with no
  `op_skipped{ReferentGone}`" clause is **no longer vacuous** and no longer says it is. M7V-22
  still waits on **I1** and stays in §12's I1 block: that is a runner dependency, not a contract
  one, and the distinction is now written into the §12 row.
- `grep -c 'Capability(C0)'` over the plan = **0**. No row parks on the trace vocabulary any more.

### What else in the drift table had moved

The lead was right that the whole table was suspect. Six of twenty-three rows had moved; the rest
were re-read against `ec610f4` and still hold.

| Row | Was recorded as | Actually, at `ec610f4` | Consequence |
|---|---|---|---|
| **1** | foundation ask, "the only one" | landed `6893442` | ask closed; M7V-46 un-parked |
| **21** | "not landed — no `OpSkipped` kind" | landed `6893442` | ask closed; M7V-22/88 clause un-parked |
| **6** | `AckEvidence{node, role, durability}` — "no boot", oracle pairs the boot from the paired `replication_ack` | **`AckEvidence{node, boot: BootId, role, durability}`** (`trace.rs:606`), boot added at `6893442` under foundation's **K-F-22**, whose rustdoc cites this team's own §3.7 four-tuple | the round-3 workaround is **withdrawn**. §4 convention 1 rewritten; five row literals (M7V-07, 08, 09, 79, 81) now four-field with `boot=b1`. See the question below |
| **8** | "no `partitions` field; partitions = distinct `partition` values" | **`TraceHeader.partitions: u8`** (`trace.rs:214`) exists | stop deriving it, read it. A stated count that disagrees with the entries is a fixture defect worth seeing; a derivation can never disagree |
| **10** | header `config_digest: Digest` + `budgets: Budgets` | both **gone**; `config: RunManifest{budgets, overridden: Vec<BudgetName>, nodes: u8, event_cap: u32}` (`trace.rs:187`, `:213`) | VA-1 and VA-7's `trace_header` line named a field that no longer exists. Both fixed |
| **12** | `state_digest_after` / `published_state_digest` withdrawal asked, "both present" | both **deleted** (ruling **F-R9**) | the withdrawal landed. No checker read them, so nothing else changes — but the table was telling readers to expect fields that are gone |

Re-read and unchanged: rows 2, 3, 4, 5, 7, 9, 11, 13, 14, 15, 15′, 16, 17, 18, 19, 20, 22, 23.
Also re-verified because `M7V-56` asserts set equality against them: `BoundaryId` is still
**29** members at `ec610f4` (same as `8a23b1d`), and `AckRejectReason` carries exactly the seven
names `trace-requirements.md` §3.5 declares. `ErrorKind` is unchanged in membership
(`errors.rs` moved only doc text and `RetryRule::NotWired`'s role in `is_definitely_not_applied`),
so §8 row 9's admission subset still holds.

### The F-R20 ruling, recorded where it will be read

`M7V-56`'s set-equality assertion **stays exactly as written.** I recorded the ruling in
`trace-requirements.md` §3.5 — next to the closed-set declaration kernel-b's CB-3 collides with,
which is where someone about to "fix" the red row will actually be standing — and in the plan's
§15 closing paragraph. Both say the same thing: the widening is expected, the row going red is
the tripwire working, the response is seven coverage cells, and a set-equality assertion that
degrades to subset the first time it fires was never an assertion. Neither text weakens the row.
Cost if it lands: seven cells. If that turns out materially larger, it comes back to the lead.

### Basis marker

`<!-- drift-basis: ec610f4 -->` added on its own line at the end of §15's drift table — **after**
the re-read above, not before. `ec610f4` is the newest commit touching
`crates/rdb-core/src/contracts`, confirmed with `git log -1 --format=%H -- <surface>`.

Note for whoever maintains `scripts/drift-check.sh`: its whole-line marker regex earned its keep
immediately. §15's new preamble mentions the marker in prose, and the earlier substring matcher
would have counted that as a second marker and failed the plan for having two bases.

### Row hygiene

`perl` script (backslash built with `chr(92)`, per the tooling note) counting unescaped pipes per
row and grouping contiguous table blocks. **Four defects found, all four fixed:**

- **M7V-46** (10 pipes in an 8-pipe table): `Generated{seed} | Reduced{from} | Authored{case}`
  inside a code span — two bare pipes.
- **M7V-62** (12): two `| n |`-shaped code spans — four bare pipes.
- **M7V-87** (17): five `| n |`-shaped code spans — eight bare pipes. This is the row whose
  *subject* is a `| 3 |` prefix matching the wrong table row, so it was the likeliest one to carry
  the defect and did.
- **M7V-71** (8 pipes in a 9-pipe table — the opposite defect): the **`Class of mutation` cell was
  missing entirely**, so every later cell rendered one column left and its `Input` appeared under
  `Class of mutation`. Filled with "none — a completeness row over the other six, not itself a
  mutation", which is the true value.

Escapes use the backslash-pipe form already used in the plan's drift table.

### Commands run and observed results

| Command | Observed |
|---|---|
| `git log -1 --format="%H %h %s" -- crates/rdb-core/src/contracts` | `ec610f4 … K-F-39 partition config refuses a zero threshold on decode` — the basis |
| `git diff --stat 8a23b1d ec610f4 -- crates/rdb-core/src/contracts` | 11 files, 982 insertions, 104 deletions; `trace.rs` 277 changed lines — the reason the whole table needed re-reading, not two rows |
| `perl <pipecheck> docs/testing/test-plan-m7-verification.md` | before: `PIPE DEFECTS: 4`; after: `pipes OK: every table row … agrees with its table` |
| `perl <pipecheck> …/trace-requirements.md` | `pipes OK` (after my edits to its §1, §3.5, §3.7, §8 tables) |
| `bash scripts/drift-check.sh docs/testing/test-plan-m7-verification.md` | `== drift (contract surface at ec610f4)` / `drift: test-plan-m7-verification.md OK (ec610f4)` / `gate: drift OK` |
| `grep -c 'Capability(C0)' docs/testing/test-plan-m7-verification.md` | `0` — no row parks on C0 |
| `grep -n 'foundation ask' docs/testing/test-plan-m7-verification.md` | 3 hits, all benign: M7V-42's "no foundation ask is needed", the F-R13 paragraph, and §15's disposition legend |
| `git diff --numstat -- docs/testing/test-plan-m7-verification.md` | `101 40` |

No cargo. No git command that changes state.

### Declined to change, with the reason

1. **`M7V-56` is untouched.** Ruled by F-R20 and I agree with the ruling on its merits. Recorded
   the ruling beside the declaration instead.
2. **No new row for `AckEvidence.boot`.** The field is load-bearing — K-F-22's whole point is that
   two acks from one node across a restart are two boots and the checker counts **one** copy — and
   **no row asserts it**. That is a real gap, but adding `M7V-91` is a planning decision that
   belongs in a round with a critic, not in a drift refresh. Recorded in drift row 6 and raised
   below.
3. **No new row for `RunManifest.overridden`.** Same reasoning; also unasserted.
4. **`design.md` and ADR-rdb-0019 untouched** — not mine. `design.md` §2.1's `acks` map still
   describes pairing the boot from `replication_ack` (drift row 6's withdrawn workaround), and
   §2.3's V-R21 cross-check sentence is still there from round 5. Both are the architect's.
5. **M7V-87's ADR-side selector left as written.** It selects a span beginning with a bare
   `| **M7 release gate**` for the ADR while §15's round-3 paragraph describes the plan side as
   `| 3 | **The M7 release gate**`. They may be describing two different table shapes and I could
   not check the ADR without opening a file I do not own. I escaped the pipes and changed no
   character of the selector strings. Flagged, not fixed.

### Questions for the lead — each with my default

- **Q-7. Add `M7V-91` for `AckEvidence.boot`?** A publication resting on two acks from one node at
  two different boots must count **one** copy, not two; the field landed specifically to make that
  catchable and nothing asserts it. **Default: yes, next planning round, as an M7V-07-shaped bad
  trace plus its one-fact near-miss** (same node, same boot, two acks → still one copy; same node,
  two boots → still one copy). I have not written it. If you want it now, say so and it is one row.
- **Q-8. Should any row assert `RunManifest.overridden`?** It is the trace's own record of a
  non-default budget, which is exactly the `RETCD_TEST_DEADLINE_SCALE` confusion AGENTS.md warns
  about. **Default: fold one clause into M7V-72's artifact-key row rather than add a row.**

### Risks

- **R-19** The five `boot=b1` literals I added to M7V-07/08/09/79/81 are consistent with M7V-28's
  existing `peer_boot=b1` but no builder exists yet to reject an inconsistent pair. Until Q-7's row
  exists, the boot on `ack_evidence` is written by convention and checked by nobody.
- **R-20** Drift rows 8 and 10 change what a **developer** reads from the header
  (`partitions` stated not derived; `config` not `config_digest`). `dev-verification-1` is live. If
  it has already written header-reading code against the round-3 table it will need a look.
- R-16, R-17 from earlier rounds stand.

### Recommended status

**REVIEW** — one critic pass over this diff. It is a correction round: no new rows, no id changes,
no assertion weakened. The two things worth a critic's attention are the five `boot=b1` literals
(drift row 6) and whether Q-7's missing row should block the developer.
