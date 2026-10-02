# Handoff — team verification, architect (2026-09-20)

## 1. Outcome

**COMPLETED_WITH_RISKS.** All four assigned artifacts are written. No Rust code, no commits.
The risk is not in the design — it is that five of the design's inputs (trace vocabulary, replay
runner, `support/mod.rs`, the `capability` event, `write_evidence` reachability) are owned by other
teams and none exists yet. See §7.

## 2. Artifacts

| Path | What |
|---|---|
| `.claude/scratchpad/conversation_memories/rdb-partition-database/teams/verification/design.md` | A — oracle, scenario grammar, reducer, campaign, coverage, mutations, and what is deliberately not built |
| `docs/ADRs/rdb/0019-validation-gates-evidence-and-release-boundary.md` | B — skeleton ADR, Status Proposed, Date 2026-09-20 |
| `.../teams/verification/trace-requirements.md` | C — the exact trace fields and event kinds the oracle needs from foundation |
| `.../teams/verification/research.md` | D — sources read, with what each one changed |
| `.../teams/verification/architect-handoff.md` | this file |

No file outside the charter's owned list was written or modified.

## 3. Criterion to evidence

| Criterion (from the task) | Evidence |
|---|---|
| Oracle checks client-visible logical state and declared lineage only | `design.md` §2.1 — the whole model is nine fields; §2.2 names the six forbidden `rdb_core` modules and makes the ban a test row (M7V-01), not a promise |
| Invariant list (7 named) | `design.md` §2.3 — INV-ATOM, INV-PUB, INV-AUTH, INV-LIN, INV-DEDUP, INV-LOSS, INV-LIVE, each with its spec section and gate. Two additions (INV-VER, INV-LAG) flagged in Q-3 |
| Trace fields each invariant needs | `trace-requirements.md` §3 (18 event kinds, field by field) and §4 (invariant → required fields, compact) |
| Scenario grammar (spike §6 table) as data types | `design.md` §3 — six enums, one per group, with the required boundary variants from the table's right-hand column |
| Shrinking strategy that preserves causality | `design.md` §4.3 — reduce the *scenario*, re-run the kernel. Causality preserved by construction; no repair code. Derivation and the DEMi/GReduce contrast in `research.md` §3 |
| Campaign budget knobs | `design.md` §5.1 — `SPIKE_SEEDS`, `SPIKE_MAX_EVENTS`, plus `SPIKE_SEED_BASE`, `SPIKE_SHRINK_STEPS`, `SPIKE_REQUIRE_ALL`, `RETCD_EVIDENCE`. §5.2 shows the 1,000-in-60 s budget is reachable and names the four rules that keep it so |
| Coverage matrix | `design.md` §6 — three axes, counted cells not percentages, explicit required lists, missing required cell fails |
| Named mutation checks from spike §7 | `design.md` §7 — MUT-1..MUT-5 mapped to the checker each must trip, plus §7.1 stating exactly what a mutation check does and does not prove |
| What is deliberately NOT built | `design.md` §8 — eleven rows with reasons |
| ADR 0019: V1–V15 → M7–M13, M7 rows filled, later pending | ADR §1 — 15 rows, `Form` column, M7's claim stated in one quotable sentence; nine rows say `pending` with an owner and no invented threshold |
| ADR 0019: evidence schema reused from ADR-0031 | ADR §2 — `write_evidence`, `RETCD_EVIDENCE=1`, `docs/evidence/`, `scale_factor` honesty, `full_scale:false` gate, checked-in reduced-scale constants. Two rDB additions live inside `values`, schema untouched |
| ADR 0019: adoption phases A0–A3 | ADR §3 — renamed from developer-handoff §6 M0–M3, content unchanged, A2-blocked rule kept |
| ADR 0019: release boundary | ADR §4 — six numbered statements, including the three fault classes rDB inherits as unowned from rEtcd ADR-0031 |
| Research cited | `research.md` — proptest/proptest-stateful, madsim/S2/RisingWave, DEMi (NSDI 2016), GReduce (TOSEM 2024), plus in-repo rEtcd precedent |
| No Rust code, no commits, no files outside charter | `git status` shows four new untracked files under the scratchpad team folder plus `docs/ADRs/rdb/0019-*.md`; no `.rs` written |

## 4. Commands run and observed results

Read-only throughout. No cargo, no gate script (nothing to build).

| Command | Observed |
|---|---|
| `grep -n "^## \|^### " docs/rdb/implementation-spikes.md` | section map; §4 at line 152, §6 at 244, §7 at 298 |
| `grep -n "^## \|^### " docs/rdb/design-specification.md` | §5.3 at 306, §5.4 at 336, §6.3 at 425, §8.1 at 544, §8.2 at 573 — all four read |
| `grep -n "proptest\|\[workspace" Cargo.toml` | `proptest = "1"` at line 75 of `[workspace.dependencies]`; workspace members are `config-*` only — **no `rdb-*` crate exists yet** |
| `ls crates/` | ten `config-*` crates; no `rdb-core`, no `rdb-sim` |
| `ls .../teams/foundation/` | `charter.md` only — **no foundation seed, no trace vocabulary exists yet** |
| `grep -rn "fn write_evidence" --include=*.rs` | exactly one: `crates/config-testkit/src/evidence.rs:250` |
| `ls docs/ADRs/rdb/` | `0001-core-set-partition-database.md`, and now `0019-...md`. **No `README.md`, no `0000`** |
| `grep -n "0019" docs/ADRs/README.md` | rEtcd's own ADR-0019 is "Event journal and replicated compaction". Different series, no file collision, but the name "0019" is now ambiguous in conversation — always say "ADR-rdb-0019" |
| `git log --oneline -- docs/ADRs/rdb/0019-*.md` | **`0f7f4f6 docs(rdb): add the partition database design package and open the rDB ADR series`.** Your commit swept the file in while I was writing. I made **no** commit (team-rules forbids it). `git diff HEAD -- docs/ADRs/rdb/` is empty, so the committed content is exactly what I wrote — flagging it only so you know ADR-rdb-0019 is already in history at `Proposed` |
| `git status --porcelain` at handoff | `M Cargo.toml`, `?? crates/rdb-core/` — foundation has started; `rdb-sim` does not exist yet |

## 5. Assumptions and deviations

1. **Assumed** the oracle's checker set must cover V8 and the V12 subset, because the milestone
   claims them and the oracle is M7's only judge. The charter's deliverable text names five
   invariants; I designed nine. Q-3.
2. **Assumed** `rdb-sim` tests may take a dev-dependency direction toward `config-*` (the reverse
   is banned, not this one). Needed for `write_evidence`. Q-5.
3. **Assumed** the campaign test binary exits 0 while invariants report `Unavailable`, so
   `scripts/gate.sh` is green at handoff (charter acceptance) while the artifact still refuses to
   claim a pass (charter Q1). The two requirements are reconciled by putting the gate in the
   evidence check, not in the test's exit code — `design.md` §2.4. If the lead reads Q1 as
   "the binary must fail", say so and I will invert the default.
4. **Deviation:** I did not write `docs/testing/test-plan-m7-verification.md`. That is the test
   planner's file per team-rules §roles. Row ids `M7V-01`, `M7V-20`, `M7V-21`, `M7V-22` are
   *referenced* in `design.md` as the rows the planner should write; they are suggestions, not a
   reservation of the numbering.
5. **Deviation:** no `mcp__hitl__*` call. The task says I cannot ask the user; questions are below.

## 6. Questions for the lead — each with my default

| # | Question | My default (proceed on this if you do not answer) |
|---|---|---|
| Q-1 | Use `proptest` (already a workspace dep) or a custom reducer for the campaign? | **Custom reducer.** proptest's shrinking is coupled to value generation; ours is coupled to scenario re-execution, and `proptest-stateful`'s own docs say per-op shrinking breaks preconditions. Bending our `SPIKE_*` budgets into `proptest!`'s runner is more code than ~150 lines of `ddmin`. Recommend `rdb-sim` does **not** take proptest unless foundation wants it for C0 known-answer vectors. `research.md` §1 |
| Q-2 | Do rDB evidence artifacts share `docs/evidence/` with rEtcd, or get `docs/evidence/rdb/`? | **Share `docs/evidence/`, prefix filenames `rdb-`.** One directory, one README disclaimer, one gate script. A second directory means a second README that will drift |
| Q-3 | Are INV-VER (V12 subset) and INV-LAG (V8) in verification's scope, or do kernel-b's L1 rows and foundation's C0 rows carry them? | **In scope, in the oracle.** The oracle is the only cross-cutting judge; a checker is ~40 lines each. If you rule them out, `design.md` §2.3 loses two rows and ADR-rdb-0019 §1's V8/V12 rows need a different owner named |
| Q-4 | Mutation checks: trace rewrites (my design) or `#[cfg]`-gated kernel mutants? | **Trace rewrites.** They prove the oracle detects the fault class, which is literally what spike §5's O1 acceptance asks for ("deliberately bad traces trigger each checker"). Kernel mutants put test-only branches on production paths. Middle path if the critic pushes: a seam-level fake provider, no kernel branch. `design.md` §7.1 |
| Q-5 | `write_evidence()` lives in `crates/config-testkit/src/evidence.rs`. `rdb-sim`'s allowed deps (charter, team-rules §workspace) are `proptest`, `config-log`, `config-log-macros` — not `config-testkit`. | **Move `write_evidence` + `RunInfo` into `config-log`, re-export from `config-testkit`** so no rEtcd test changes. Cheaper than a `rdb-sim` dev-dep on the whole rEtcd stack, and much cheaper than duplicating the helper (a duplicated disclaimer is the exact failure ADR-0031 wrote the shared helper to prevent). **This is a `config-*` file change and needs your routing** |
| Q-6 | Where does `validation/<run-id>/` go, and is it committed? | **`.scratchpad/validation/<run-id>/`, never committed**, added to `.git/info/exclude` like the archive fallback (AGENTS.md precedent). Only the minimized `fixtures/regressions/*.json` is committed |
| Q-7 | `docs/ADRs/rdb/` has no `README.md` and no `0000`. Ledger assigns those to foundation. | **Foundation creates them; I do not touch them.** Please have foundation's README index list ADR-rdb-0019 as `Proposed`. Also worth a naming ruling: "ADR-0019" is now ambiguous (rEtcd's is the event journal). Suggest everyone writes **ADR-rdb-NNNN** |

## 7. Risks

| # | Risk | Signal it is happening | Mitigation |
|---|---|---|---|
| R-1 | **The `capability` event never gets built**, and the campaign cannot distinguish "no violation" from "nothing ran". This is the most dangerous false green in M7. | Campaign reports all-green before any kernel package lands | `trace-requirements.md` §3.18 and §5 item 3. If foundation declines it, I stop and report BLOCKED per the charter |
| R-2 | The trace vocabulary lands without `peer_role` on acks or `predecessor_cutoff` on recovery roots. MUT-2 becomes uncatchable and INV-LOSS degenerates into a global no-loss oracle, which spike §6 says is the wrong oracle | Foundation's C0 seam review omits either field | `trace-requirements.md` §5 lists these two plus the capability event as the three easy-to-miss asks |
| R-3 | The oracle grows a second implementation of the protocol under review pressure ("the checker should verify the prefix selection was *correct*") | A checker starts needing kernel state | `design.md` §2.1's model is nine fields; M7V-01 enforces the import ban. Any growth beyond those fields is the critic's trigger |
| R-4 | The 1,000-seed / 60 s budget is missed on this Windows host once real kernel packages land | Campaign wall time creeps past 60 s as A1..F1 wire in | `design.md` §5.2's four rules are the design's answer. Spike §7's rule applies if missed: improve the harness or revise the budget in writing. Never lower an assertion. Note the budget was written for "one recorded CI worker", and this host is a Windows Server VM, not Linux/NVMe — the number may need an explicit host-qualified revision |
| R-5 | The reducer's per-seed re-run cost makes a failure-heavy run slow (2,000 shrink steps × a full re-run) | First real failure takes minutes to minimize | `SPIKE_SHRINK_STEPS` is a hard cap and the reducer emits its best candidate when the budget is spent, saying so in the artifact |
| R-6 | ADR-rdb-0019's nine `pending` rows read as an unfinished document and get "helpfully" filled in with invented thresholds | A later agent fills a pending row without running the gate | The skeleton banner at the top of the ADR says a `pending` row is a commitment to decide, not a decision |
| R-7 | Nothing this team designs is compilable until foundation lands C0/H1/I1 and registers our modules in `tests/support/mod.rs` | Developer blocked at first `cargo test` | Work order in `design.md` §9: hand-built traces and checkers first, then grammar and generator — both dependency-free. Only the reducer and campaign need I1 |

## 8. Requests to the lead for routing (files I do not own)

1. **`crates/rdb-sim/tests/support/mod.rs`** — register `oracle` and `scenarios`. Foundation's file.
2. **`write_evidence` reachability** — Q-5. A `config-log` move is my recommendation.
3. **`docs/ADRs/rdb/README.md`** — list ADR-rdb-0019 (Proposed). Foundation's file.
4. **Trace vocabulary** — route `trace-requirements.md` to foundation's architect before their C0
   seam is frozen. It is a request, not a change: 18 event kinds, no kernel internals.
5. **Naming ruling** — "ADR-rdb-NNNN" in prose, to disambiguate from rEtcd's own 0019.

## 9. Recommended next role (superseded — see Correction round 1)

**Critic (round 1)**, per team-rules. Point them at `design.md` §4.3 (the causality argument — it is
the load-bearing claim and the one I would attack first), §7.1 (what a mutation check does not
prove), §2.3's two extra invariants (Q-3), and §8's not-built list. `trace-requirements.md` should go
to foundation's architect **in parallel**, since their C0 seam freeze is the gating event and the
critic's verdict does not change what fields an invariant needs.

---

# Correction round 1

Critic round 1 verdict: **FAIL** — 3 BLOCKER, 9 MATERIAL, 6 ADVISORY. All eighteen are **closed**.
Nothing disputed. Lead rulings V-R8..V-R11 applied.

The critic was right about the thing that mattered most: §2.3 **is** the deliverable, and three of
its rows were unsafe. INV-PUB's singular-ACK rule would have passed a one-copy publish while
`DEGRADED_RF2` — the exact bug spec §8.3 spends a paragraph forbidding — while ADR-rdb-0019 printed
"V3 simulated". I checked that against spec §8.3 and ruling B-R3 before editing; it is not a style
disagreement.

| # | Sev | Finding | Disposition |
|---|---|---|---|
| F1 | BLOCKER | INV-PUB's ACK rule wrong under degraded RF2 | **closed** — design §2.1 model gains `required: config_version -> (Set<NodeId>, QuorumRule)`; §2.3 INV-PUB checks the pinned `required_copy_set`, with B-R3's `min_regular_acks` 1-of-1 named for RF2; §6 gains a required `RF3 × DEGRADED_RF2` guard cell; trace-req §3.14 emission cadence (V-R10); ADR §1's V3 `Form` cell states the degraded half explicitly |
| F2 | BLOCKER | INV-LIN reimplements F1's prefix selection | **closed** — §2.3 INV-LIN restated as the two lookups against the oracle's already-recorded `lineage` map. No pairwise-compatibility derivation. §8 gains a not-built row naming it |
| F3 | BLOCKER | oracle trusts kernel-computed labels; MUT-2 is a tautology | **closed** (V-R9) — new §2.5 grounds `peer_role` in the header `topology` and `Durable` in a preceding `durability_advance{Synced}`; §7 splits into trace rewrites (MUT-1/3/4) and injected faults (MUT-2/5); `NetworkOp::ForgeAck` and `StorageOp::FalseDurable` added to §3; H1/M1 hooks added to §9 and the routing list |
| F4 | MATERIAL | ddmin signature slippage | **closed** — §4.4 `Signature` gains `faults: BTreeSet<BoundaryId>`; §4.1 commits `<slug>.orig.json` beside the minimized fixture and `regressions.rs` replays both; new row M7V-23; research.md §6 records the fix as derived, not borrowed |
| F5 | MATERIAL | `heal_at_event` is an absolute event index | **closed, removes code** — deleted from `Budget`; healing is the existing `NetworkOp::Heal` op (§3.2, §2.3 INV-LIVE); §8 not-built row added |
| F6 | MATERIAL | "no false durable watermark" has no checker, no op; ADR over-claims | **closed** — INV-PUB durability-grounding clause (§2.5), `StorageOp::FalseDurable` (§3), and ADR §1's V1 `Form` cell now says clause 3 holds only in the modelled bookkeeping sense — explicitly **not** fsync honesty, a lying device, or power loss |
| F7 | MATERIAL | no partition axis; two acceptance rows unreachable | **closed** (V-R8) — `Topology.partitions: u8`, per-partition `ClientOp`, INV-ISO armed like INV-LIVE, `unresolved` map in §2.1, one required coverage cell, and a named paragraph in ADR §1 |
| F8 | MATERIAL | INV-LAG under-specifies resume, never disarms, duplicates L1 | **closed** — §2.3 INV-LAG reduced to three transition-legality clauses including the spec's 250 ms; new §2.6 states why the 1 s/2.1 s ladder is not judgeable from the trace and cites kernel-b's rows; ADR §1's V8 row shows the split and says neither half alone is V8 |
| F9 | MATERIAL | INV-LOSS not checkable; precondition generates false violations | **closed** — §2.3 INV-LOSS restated with the `Durable`-holder / `Buffered`-holder-`boot_id` split; trace-req §3.5 states ACKs are emitted **at the secondary** with a separate delivery record (V-R10); §5 item 4 explains that delivery-point emission would make the checker weaker than its own statement |
| F10 | MATERIAL | 60 s budget has no command that can produce it | **closed** (V-R11) — new §5.1.1 names `CARGO_TARGET_DIR=.rtargets/campaign scripts/gate.sh test --release -p rdb-sim --test campaign` and records both in-repo facts (gate.sh line 45; `[profile.test] opt-level = 0` on workspace members); §5.1 adds `SPIKE_ASSERT_WALL_MS`; §5.2 host-qualifies the figure and cites `m4_69`; §8's contradictory "only asserted budget" row rewritten; ADR §1 carries the same paragraph |
| F11 | MATERIAL | reducer budget is per-failure, not per-run | **closed** — `SPIKE_SHRINK_MAX_FAILURES` and `SPIKE_SHRINK_BUDGET_TOTAL` added (§4.2, §5.1); `shrink_ms` reported separately from `wall_ms` (§5.3, ADR §2) |
| F12 | MATERIAL | per-op field shrinking is over-engineering, refuted by our own source | **closed, removes code** — second pass deleted (§4.2); research.md §1 records the inconsistency rather than hiding it |
| F13 | ADVISORY | seven fields have no reader | **closed** — `state_digest_after` and `published_state_digest` **withdrawn** from the ask (trace-req §6: a whole-state digest can only be used by computing your own); the rest moved to a new §7 with their real readers; preamble points there. `required_copies` and `replication_send` gained readers under F1/F9 |
| F14 | ADVISORY | dedup key omits affinity | **closed** — §2.1 key is `(tenant, affinity, client, request)`; INV-DEDUP gains the `CROSS_AFFINITY` clause; §3 `ClientOp` gains a foreign-affinity boundary case; §6 gains the guard cell |
| F15 | ADVISORY | `ClientOutcome` cannot express `RECOVERED_APPLIED` | **closed** (V-R10) — trace-req §3.8 set is `Success \| RecoveredApplied \| <§5.4 errors>`; INV-DEDUP clause updated |
| F16 | ADVISORY | `ForgedIdentity` has no op and no coverage cell | **closed** — `NetworkOp::ForgeAck` (F3) plus the full `replication_ack.reject_reason` row in §6's required guard list |
| F17 | ADVISORY | `boundary="op_skipped"` pollutes the counted enum | **closed** — own event kind `op_skipped{scenario_op_index, reason}` (§4.3, trace-req §3.16a); `BoundaryId` stays exactly spike §6's column |
| F18 | ADVISORY | the minimized fixture's `seed` is a lie; JSON directed cases | **closed, removes artifact** — `seed` replaced by `Provenance::{Generated,Reduced,Authored}` (§3); the four mandatory cross-package cases move to Rust constructors, JSON kept for reducer output only (§3.1) |

**Disputed: none.** Two closures are partial and say so in the documents rather than in an argument
here: F6 closes the *modelled* watermark clause only (ADR §1's V1 row states the limit), and F8
narrows INV-LAG rather than deleting it, which is consistent with V-R3 keeping it in the oracle.

## Net effect on code volume

Four things are now smaller: no per-op shrink pass (F12), no `heal_at_event` (F5), no protection
timing ladder in the oracle (F8), no prefix-compatibility derivation in INV-LIN (F2). Two fields
withdrawn from the trace ask (F13). Added: one model field, one signature field, one invariant
(INV-ISO), two scenario ops, three reducer knobs. Roughly a wash, with the unsafe parts gone.

## Files changed in this round

- `teams/verification/design.md` — §0 D5, §1, §2.1, §2.3, new §2.5, new §2.6, §3, §3.1, §3.2, §4.1,
  §4.2, §4.3, §4.4, §5.1, new §5.1.1, §5.2, §5.3, §6, §7 (split), §8, §9, §10 (questions → rulings)
- `teams/verification/trace-requirements.md` — preamble, §1 header, §3.4, §3.5, §3.7, §3.8, §3.14,
  §3.16, new §3.16a, §4, §5 (three asks → six), §6, new §7
- `teams/verification/research.md` — §1 (F12 self-correction), §5, new §6
- `docs/ADRs/rdb/0019-...md` — §1 rows V1, V3, V8 qualified; isolation and host-qualified-budget
  paragraphs added; §2 `values` keys; Consequences (V-R5); Verification (three new rows)
- Global: `partdb` → `rdb` across all five files (user ruling, ledger 18:30). Zero occurrences left.

Nothing outside the charter's owned list was touched. No commits.

## Routing requests, updated

Items 1–5 in §8 stand. Two added, both gating, both foundation's:

6. **H1 network hook** for `NetworkOp::ForgeAck` — deliver an ACK claiming a role or identity the
   header `topology` does not grant. Spike §4's transport seam already requires it.
7. **M1 flush hook** for `StorageOp::FalseDurable` — report a flush completion never performed.
   Spike §6's storage boundary list already requires it.

Plus the three V-R10 trace lines (trace-req §3.5, §3.8, §3.14), which arch-foundation has been told
about.

## Recommended next step

Critic re-review of **this diff only**, per their own recommendation. On pass, the test planner
starts `docs/testing/test-plan-m7-verification.md` and should be handed §2.3, §6 and §7 as the row
source — those are the three sections that changed most.

---

## Correction round 2

Six findings, all **closed**, none disputed. Round-2 verdict was PASS_WITH_RISKS, so nothing here
was blocking — but three of the six govern rows the test planner writes, and those landed first.

| Finding | Disposition |
|---|---|
| F8 — INV-LAG clause (a) quantifies over the pinned copy set | **closed.** `design.md` §2.3 INV-LAG clause (a) now reads "every node in the `required_copy_set` pinned at `paused_prefix_seq` has `durable[node].durable_seq >= resume_barrier_seq`, the barrier is hit exactly, and `oldest_unsafe_age_ms < 250` continuously for 5 s", quoting spec §6.2's "All configured regular copies durable through paused prefix". Mirrored into `docs/ADRs/rdb/0019` §1 V8 `Form`. |
| F18 — trace header `seed` contradicts `Provenance` | **closed.** `trace-requirements.md` §1 row `seed: u64` replaced by `provenance: Provenance` (`Generated{seed}` \| `Reduced{parent}` \| `Authored`), matching `design.md` §3, with the sentence saying a reduced or authored scenario's seed reproduces nothing and the checked-in fixture is the reproducer. |
| F19 (V-R12) — role grounding must be config-versioned | **closed.** New `trace-requirements.md` §3.19 `topology_change{config_version, nodes}`, emitted by H1/the control provider, never the kernel; header `topology` is now explicitly `config_version_0` only; added as §5 **ask 7** (seam-freeze item). `design.md` §2.5 resolves a role from `topology[config_version in force at that ack]`, with §2.1, §2.3 INV-PUB, §3 `Topology`, §7.1 MUT-2 and the §7 cross-check paragraph made consistent. V-R12 added to §10. |
| F20 — INV-LAG clause (c) was an admission gate, not a publication gate | **closed.** `design.md` §2.3 clause (c) now reads "no `admission_decision{outcome=Admitted}` at a `logical_tick` at or after the first `protection_state{state=Paused}` and before the next `state=Healthy`". Same words mirrored into ADR 0019 §1 V8 `Form`, replacing "no publish while paused". Critic's own round-1 error, withdrawn by them and removed by me from both places it had been adopted. |
| F21 — `faults` in the acceptance predicate disables the reducer | **closed.** `design.md` §4.4: acceptance predicate is the **core tuple** `(checker, rule, partition, role, event_kind)`; `faults` is recorded and reported only, and the struct comment says so. The sentence "Deleting the last op that produced a boundary changes the set and the candidate is correctly rejected" is **deleted**. The slippage defence is restated as the `.orig.json` companion, with `faults` as the signal: `slipped: true` into `rdb-m7-campaign.json` naming both sets (§5.3, and the `values` key list in ADR 0019 §2). **M7V-20** restated as core tuple equal + `ops_after.len() < ops_before.len()` + `.orig.json` replays and fails. **M7V-23** restated as shrinks + artifact records `slipped: true` with both sets + `.orig.json` still fails. |
| F22 — two editorial defects | **closed.** (a) `design.md` §2.4 moved back above §2.5/§2.6, so document order matches numbering (verified after all round-2 edits: 2.1@69, 2.2@96, 2.3@109, 2.4@133, 2.5@148, 2.6@168). (b) `trace-requirements.md` §3.17 now routes to "**INV-LIVE and INV-ISO** … the only thing that arms either checker", and §4's INV-LIVE / INV-ISO rows both name §3.17. |

### One incident worth recording

While applying F19 I ran a `perl -i` substitution whose pattern used `\Q...\E` around a string
containing `\n`. Inside `\Q`, `\n` is a literal backslash-n, so the pattern could not match; the
same call also carried a second expression that perl rejected, and `perl -i` truncated
`trace-requirements.md` to **0 bytes**. The file is under `.claude/scratchpad/` and therefore
gitignored, so there was no git copy to recover from. I rebuilt it from the full verified dump I
had taken one command earlier and re-applied the two pending edits; `wc -l` is 325 lines and every
section §0–§7 is present. Nothing was lost, but the lesson is real: **no more multi-expression
`perl -i` on these files.** Section-level edits from here on.

### Still open, unchanged from round 1

Nothing new. The four questions-with-defaults in §7 above remain as written; none was reopened by
round 2.

---

## Correction round 2 — test-plan findings routed to the architect (T-01, T-12, T-13, T-14; rulings V-R16..V-R19)

The heading above ("Correction round 2") is the design re-review. This section is the round on the
**test plan** (critic-tests.md, verdict FAIL), for the four findings whose closure lives in files
the architect owns. The planner is correcting the plan in parallel and must align the sections
listed in §C below.

### A. Outcome

**COMPLETED.** Four findings closed in `design.md`, ADR-rdb-0019 and `trace-requirements.md`;
none disputed. The R-3 second-order obligation (fixture realizability) is written as a row the
planner carries once I1 lands. Edit tool only; no `perl -i`, no `sed -i`, no shell redirection.
No git operations. No file outside the charter's owned list touched.

| File | Lines before | Lines after |
|---|---|---|
| `teams/verification/design.md` | 635 | 780 |
| `teams/verification/trace-requirements.md` | 325 | 338 (head, tail and every §0–§7 heading verified present; `file` reports UTF-8) |
| `docs/ADRs/rdb/0019-validation-gates-evidence-and-release-boundary.md` | 204 | 275 (`git diff --stat`: 88 insertions, 17 deletions) |
| `teams/verification/architect-handoff.md` | 224 | this section appended |

### B. Finding → change → where → how the critic verifies

| Finding | Ruling | Change | Section / file | Critic verifies by |
|---|---|---|---|---|
| **T-01** (BLOCKER) `Unavailable` has two incompatible definitions; `proven` vacuous | V-R16 | `Verdict` is `Proven \| Unavailable(Unavailable) \| Violated`, with `Unavailable = Capability(PackageId) \| NotArmed`. `Proven` = armed at least once and no violation; each checker exposes `armed()` and its arming event is named. `NotArmed` is the verdict for a zero-event trace, an unhealed schedule, an exhausted liveness budget, an idle sibling partition, a truncated trace. Both reasons report, never pass. "Never inferred from silence" is **scoped to the `Capability` arm** (the sentence itself lives only in the planner's VA-2; it did not exist in my files, so nothing to delete here). Per-run fold order stated (`violated` > `unavailable(capability)` > `proven` iff `seeds_armed > 0` > `unavailable(not_armed)`); artifact records `seeds_armed` per invariant; **`proven` with `seeds_armed == 0` is a gate failure in every run**, not only under `SPIKE_REQUIRE_ALL=1`. INV-LIVE and INV-ISO rows say "disarms → `Unavailable(NotArmed)`". | `design.md` §2.4 (rewritten), §2.3 INV-LIVE and INV-ISO rows, §5.3 `invariants{id -> {status, reason?, seeds_armed}}`, §9 capability row, §10; ADR §2 rule 1 and artifact table; ADR Verification rows "seeds_armed" and "two-reasons"; `trace-requirements.md` §3.18 second paragraph | §2.4 enumerates two reasons and no fourth state; M7V-03(b), M7V-31, M7V-33, M7V-63 are now satisfiable under §2.4 by `NotArmed`; §2.4 states the fold order and the `seeds_armed == 0` gate failure verbatim; ADR §2 rule 1 says the same in the same words |
| **T-12** (MATERIAL) 64 random seeds responsible for every required cell | V-R19 | Generator schedules required boundaries deterministically: seed `i` (from `SPIKE_SEED_BASE`) is obliged to attempt `REQUIRED[i mod N]` over foundation's closed `BoundaryId` set (N = 29; foundation's architect lists members; the planner's enumeration row asserts set equality — this design never repeats the list). Attempt ≠ hit: the hit is still counted from `fault_injected`; a scheduled-but-unreachable boundary is `required_missing` and fails. `BoundaryId -> producing op` table beside `REQUIRED` in `gen.rs`, enumerated. Hook-gated cells (`ForgedIdentity` via H1, `FalseDurableWatermark` via M1) are scheduled like the rest; while the package reports `capability{state=Unavailable}` the cell is recorded under `unavailable_cells{cell -> package}` and excluded from `required_missing[]` **by that capability entry, never by editing the required list**; the `BoundaryId -> Option<PackageId>` gating table is a const in `coverage.rs` under the same enumeration row. | `design.md` §3.1 (two new paragraphs after family 3), §5.3 coverage artifact, §10; ADR §2 rule 3 and coverage artifact row; ADR Verification "scheduled-boundary" row; `trace-requirements.md` §3.18 last sentence | §3.1 states the `i mod N` obligation and the attempt/hit distinction; `unavailable_cells` is a named artifact key in both design §5.3 and ADR §2; the exclusion mechanism is a capability entry, and the text forbids deletion |
| **T-13** (MATERIAL) both VA-9 commands write one artifact path | V-R17 | Second artifact **`rdb-m7-campaign-release.json`**; the name is chosen by `cfg!(debug_assertions)`, not an env var. Release command sets `RETCD_EVIDENCE=1`. Only the release artifact may be cited for the 1,000-history budget. Why two files rather than `values` keyed by profile: ADR-0031's schema has one `host`/`build`/`run{}` per file and the two runs differ in exactly those; a merged file needs `write_evidence` to read-modify-write a stale file, which V-R5's as-is reuse forbids and which makes the artifact depend on a leftover of unknown provenance; M6-113/M6-116 already work per file. | ADR §2 artifact table (new row), the paragraph after it, §2.1 command table, §1 budget paragraph; `design.md` §5.1.1 (three-command table and the same rationale), §5.3, §10 | ADR §2 names both files and says which may be cited; the release command in ADR §2.1 carries `RETCD_EVIDENCE=1`; the rationale for two files is stated, not implied |
| **T-14a** (MATERIAL) `SPIKE_REQUIRE_ALL=1` is in no command | V-R18 | The **M7 release gate** command is stated: `SPIKE_REQUIRE_ALL=1 RETCD_EVIDENCE=1 CARGO_TARGET_DIR=.rtargets/campaign scripts/gate.sh test --release -p rdb-sim --test campaign`. No `scripts/` change by this team; wiring into `gate.sh` is foundation's after F-R11; until then it is run by hand and the artifact is the evidence. §5.1 gains an "M7 release gate" column. | ADR §2.1 (authoritative), ADR Verification "release-gate" row; `design.md` §5.1 (new column), §5.1.1, §5.2, §8 "performance assertion" row | the command appears verbatim in ADR §2.1; ADR §2.1 says no `scripts/` change and names foundation as the owner of the wiring |
| **T-14b** (MATERIAL) nothing makes `capability{state}` track reality | V-R18 | `capability{state}` is **derived, never a literal**: the dispatcher builds the block from `Module::capability(&self) -> CapabilityState` over `ModuleName::ALL` (foundation, K-F-10) and emits one event per package. Observable form for the planner's row: trace-start `capability` events equal the report one for one; `Ok`-stepping module reports `Wired` and `Unavailable`-stepping module reports `Unavailable`, both asserted positively; no `CapabilityState::Wired` token under `crates/rdb-sim/src/harness/` outside the report builder. | `design.md` §2.4 last paragraph, §9 capability row; ADR §2 rule 2, ADR Verification "capability-derivation" row; `trace-requirements.md` §3.18 (also aligned to foundation's landed shape `Capability { package: PackageId, state: CapabilityState }`, one event per package; `capability_id` superseded) | §2.4 names the method, the module the report iterates, and three positive observables; §3.18 no longer describes a single per-run event with a `capability_id` |
| **R-3 second-order** (critic, "On R-3") fixtures must be realizable | lead's instruction | New **§4.5**: every scenario fixture and authored constructor replays through I1 without `op_skipped{ReferentGone}` and yields its row's verdict; every `TraceBuilder` trace passes I1's well-formedness checks (monotone `event_id`, capability block first, `schedule_phase` before liveness arming, ack `contiguous_seq` never above the emitter's last `batch_apply.seq`); if I1 exposes no validator the row is limited to the envelope checks and says so. A failing fixture is fixed in the fixture; its assertion is never weakened. One `sim` row, dependency I1, `Unavailable(Capability(I1))` until then. | `design.md` §4.5 | the obligation is a two-row table with evidence named; it is explicitly the planner's to carry, with class and dependency stated |

**Also changed, not from a finding:** `design.md` §5.1's extended-gate `SPIKE_ASSERT_WALL_MS` was
`60000` with `SPIKE_SEEDS=10000`, which contradicts spike §7's "10,000 in 10 min". Corrected to
`600000` and the correction is noted under the table. The 60 s figure now sits in the M7 release
gate column, where the 1,000-history run is. This is a deviation from "only the four findings";
it is one cell, and leaving it would have put a wrong number beside a new right one.

### C. Sections the planner must align (their file, their rows)

| Planner section / row | What must change to match |
|---|---|
| **VA-2** | `Verdict = Proven \| Unavailable(Capability(PackageId) \| NotArmed) \| Violated(Signature)`; delete or scope "never inferred from silence" to the `Capability` arm; `Proven` requires armed |
| **VA-6** | add the generator's `i mod N` scheduling obligation (design §3.1) and the `BoundaryId -> Option<PackageId>` gating table; `unavailable_cells` |
| **VA-7** `invariant_status` line | gains `reason` (`capability:<package>` / `not_armed`), next to `seeds_armed` |
| **VA-9** | becomes three commands (ADR §2.1): handoff gate; release with `RETCD_EVIDENCE=1`; M7 release gate with `SPIKE_REQUIRE_ALL=1 RETCD_EVIDENCE=1`. Two artifact names. §13's last line cites the release gate |
| **§2 knob table** | add the "M7 release gate" column; extended `SPIKE_ASSERT_WALL_MS` = `600000` |
| **M7V-03** | (a) `unavailable(capability(P1))`; (b) zero-event trace → all ten `unavailable(not_armed)`; neither `proven` |
| **M7V-31, M7V-33, M7V-63** | the disarmed / truncated verdict is `Unavailable(NotArmed)` by name |
| **M7V-54** | fails on any status not `proven` **and** on `proven` with `seeds_armed == 0`; failure line names the reason |
| **new row** (V-R16) | over the default corpus, every `proven` invariant has `seeds_armed > 0`, all ten |
| **M7V-55** | asserts the schedule covers the required set (attempted) plus observed counts; `required_missing[]` empty **except** cells listed under `unavailable_cells` while H1/M1 report unavailable |
| **M7V-56** | enumeration also covers the `BoundaryId -> producing op` table and the `BoundaryId -> Option<PackageId>` gating table; set equality against foundation's 29-member list from their handoff |
| **M7V-62** | both `rdb-m7-campaign.json` and `rdb-m7-campaign-release.json` exist after both commands, `profile` differs, only the release one is cited |
| **M7V-72 / M7V-73** | campaign `values.invariants{id -> {status, reason?, seeds_armed}}` (object, not string); coverage gains `unavailable_cells{cell -> package}` |
| **new row** (V-R18) | capability derivation, in the M7V-56 enumerated style: events equal `Module::capability` report; `Wired` asserted positively; no `CapabilityState::Wired` literal in the harness outside the report builder |
| **new row** (V-R18) | the M7 release gate command is the one §13 cites; fails while any invariant is not `proven` |
| **new row** (§4.5) | fixture realizability, `sim`, dep I1, `Unavailable(Capability(I1))` until then |
| **Q-34** | project `seeds_armed` and `reason`; assert no `proven` row has `seeds_armed = 0` |
| **§12 "Unavailable until" table** | the reason column: which rows are `capability(<pkg>)` and which can be `not_armed`; add the §4.5 row under I1 |

### D. Assumptions and deviations

1. **Hook granularity.** I keyed the two hook-gated cells on the `H1` and `M1` entries of the
   `capability` event, i.e. the hook lands with its package. If foundation lands H1 without the
   `ForgeAck` path, the package would report `Wired` while the cell is unreachable, and M7V-55
   would fail on `required_missing` — loudly, which is the safe direction. Default: hook ships with
   the package (routing items 6–7 stand). Foundation can say otherwise in their handoff.
2. **`invariants{}` shape** changed from `id -> string` to `id -> {status, reason?, seeds_armed}`.
   One object per invariant so three facts cannot disagree across three maps. This is the widening
   V-R16 asks for ("the artifact records `seeds_armed` per invariant"), chosen over sibling maps.
3. **Extended-gate wall figure** corrected (60000 → 600000); see the note under §B.
4. **Heading name.** The lead asked for "## Correction round 2"; that heading already existed for
   the design re-review, so this one starts with the same words and adds the round's subject.
5. `Module::capability(&self)` is named as foundation's K-F-10 closure; it does not exist in the
   crate yet (`rdb-core` has `ModuleName::capability(self) -> Capability`, a different thing, and
   `rdb-sim`'s `Dispatcher::capability_report` still steps modules with a probe). The design
   describes the post-K-F-10 shape, as instructed.

### E. Residual risks

| # | Risk | Mitigation |
|---|---|---|
| R-8 | The planner and I edit in parallel; if the planner rewrites VA-2 from the critic's wording rather than from §2.4, the two texts can drift (the F19 failure mode the critic named). | §C lists the exact wording; the critic re-reviews both files together. |
| R-9 | `NotArmed` makes a never-arming checker *visible* but not *failing* in the PR default. A checker that never arms on the 64-seed corpus stays `unavailable(not_armed)` until someone reads the table. | The `seeds_armed > 0` row runs on the default corpus and fails for all ten; §4.5 catches the fixture-side cause once I1 lands. |
| R-10 | Foundation's `capability_report` currently classifies any non-`Unavailable` step outcome as `Wired`; until K-F-10 lands, T-14b's rule is a design statement with no code behind it. | Ledger F-R12 closes K-F-10 as the critic wrote; the planner's derivation row fails until it does. |

### F. Recommended status

**Critic re-review of this diff only** (design §2.4, §3.1, §4.5, §5.1, §5.1.1, §5.3, §9, §10;
ADR §1 budget paragraph, §2, §2.1, Verification, Notes; trace-requirements §3.18), together with
the planner's aligned rows, in one round. Then the developer starts design §9's dependency-free
work: hand-built traces and the checkers with `armed()`.

---

## Correction round 3 (lead ruling V-R20)

Critic round 3 verdict was PASS_WITH_RISKS with thirteen new findings. The lead routed the
design-side parts of T-23..T-28 and the cheap advisories T-30, T-34, T-35 here under **V-R20**
(binding); T-27, T-29, T-31, T-32, T-33 are the planner's, and I did not open the test plan.

### A. Outcome

**COMPLETED_WITH_RISKS.** Every routed finding is closed in the three files I own. Nothing
disputed. Edit tool only; no `perl -i`, `sed -i` or shell redirection into a doc. No git
operations, no cargo. The risk is not in the closures: it is that the uncommitted working tree
already adds a `quorum_rule` field V-R20 says the oracle must not read (§E, Q-1), and that
INV-VER has no producing op for the new wired-implies-armed clause (§E, Q-2).

| File | Edits | Lines before → after |
|---|---|---|
| `teams/verification/design.md` | 22 | 780 → 834 |
| `teams/verification/trace-requirements.md` | 16 (new §8, §8.1) | 338 → 433 |
| `docs/ADRs/rdb/0019-validation-gates-evidence-and-release-boundary.md` | 9 | 275 → 304 |
| `teams/verification/architect-handoff.md` | this section | 320 → 440 |

### B. Finding → change → where → how the critic verifies

| Finding | Change | Where | Critic verifies by |
|---|---|---|---|
| **T-23** vocabulary drift, `quorum_rule`, `provenance` | `quorum_rule` is **not a trace field**: the oracle derives it from `required_copy_set.len()` on the pinning `protection_state` (2 → `DEGRADED_RF2`, 3 → RF3, any other length is a violation `required_copy_set_shape`). Withdrawn from the ask; the coverage cell is `derived_quorum_rule × {RF3, DEGRADED_RF2}`. `provenance` stays in the ask, marked "foundation ask, not landed at 8a23b1d", exact shape written. **Drift table** with 23 rows (the critic's 9 plus 14 more from the same diff), each with landed shape and resolution; only row 1 is a foundation ask, row 21 (`op_skipped`) is a standing round-1 ask, every other row adopts. In-place landed-name notes where a checker rule would otherwise read wrong (`phase`, `peer_boot`, `ReplicaRole`, three-field `AckEvidence`, `Option<ErrorKind>`, tuple `nodes`, flat header topology, `config_digest`+`budgets`). `Regular` → `RegularSecondary` in MUT-2. | design §2.1 `required`, §2.3 INV-PUB and INV-LAG, §2.4 arming list, §2.5, §3 `Provenance`, §6, §7.2, §9, §10; TR preamble, §1, §3.2, §3.5, §3.7, §3.14, §3.19, §4, §6, §7, **§8, §8.1**; ADR §1 V3 and V8 rows, Verification degraded-RF2 row | `grep -n quorum_rule` in the three files finds only "not a field / derived / withdrawn" sentences; TR §8 has a row per critic-listed drift; TR §8.1 is a compilable enum; design §2.3 has the one derivation sentence the ruling asked for |
| **T-24** `armed()` after disarm | `armed()` is end-of-fold state, not a latch; a disarmed checker reports `armed() == false` and per-seed `Unavailable(NotArmed)`. The fold's `proven` and `seeds_armed` both count seeds whose per-seed verdict is `Proven`, so `proven ⇒ seeds_armed > 0` is definitional. Worked case: all seeds healed-then-exhausted → `unavailable(not_armed)`, `seeds_armed = 0`. | design §2.4 (Proven bullet, NotArmed bullet, fold paragraph); ADR §2 rule 1, Verification `seeds_armed` row | §2.4 contains "state at the end of the fold, not a latch" and "count the same seeds"; ADR rule 1 says the same in its own words |
| **T-25** sub-N corpora fail the gate | The required-cell gate applies only when `SPIKE_SEEDS >= N`; a smaller corpus records coverage, writes `coverage_gated: false`, never fails on `required_missing`. `coverage_gated: true` + non-empty `required_missing[]` is the only failing combination. `coverage_gated` added to the coverage artifact. | design §3.1, §5.3, §6; ADR §2 rule 3, artifact table, Verification scheduled-boundary row | the scoping sentence appears in §3.1 and §6 and in ADR rule 3; `coverage_gated` is a named key in both artifact tables |
| **T-26** fixtures fail their own validator | Zero-event control = header + ten `capability{state=Wired}` + nothing else (well-formed, arms nothing). `TraceBuilder::ack_from(n, s)` emits `batch_apply{node=n, role=RegularSecondary, seq=s}` then the `replication_ack{from_node=n, contiguous_seq=s}` — two events from one call — and every secondary-ack fixture is built with it. | design §2.4 NotArmed bullet, §4.5 second table row; ADR Verification two-reasons row | §4.5 names the helper and the two events; §2.4 no longer says "zero-event trace" |
| **T-28** wired ⇒ armed | New clause: every invariant whose needed packages all report `Wired` has `seeds_armed > 0` on the default corpus, asserted in the handoff gate; each oracle row names the scheduled boundary/op that arms it; covers zero invariants during M7 and says so. INV-VER has no producing op — flagged, not claimed (Q-2). | design §2.4 new paragraph; ADR §2 rule 1, Verification `seeds_armed` row | the clause is in §2.4 and ADR rule 1 with "handoff gate" named; INV-VER is called out as open |
| **T-30** two `reason` forms | One form per surface: artifact carries the ADR string `capability(<package>)` \| `not_armed`; the `invariant_status` log line carries `reason` + separate `package`. Bijection stated; VA-7 states both (planner). | design §5.3; ADR §2 artifact table | both files say which surface carries which form |
| **T-34** VA-2 omits the fold order | design §2.4's fold paragraph is declared the authoritative copy; VA-2 and M7V-52 carry it word for word (planner mirrors). The T-24 sentence lives in that paragraph so the mirror picks it up. | design §2.4 | the sentence "This paragraph is the authoritative fold" is present |
| **T-35** gating table breadth | Every required cell is keyed on the package whose provider emits its `fault_injected`, per family; provisional map Network/Time/Control → H1, Storage → M1, Client/Recovery → I1; foundation's handoff names the emitter per member and is authoritative; the two V-R9 hook cells need no separate key because the hook ships with the package. `BoundaryId -> PackageId` (no longer `Option`). | design §3.1, §5.3, §6; ADR §2 rule 3, artifact table | the family map is stated once in design and once in the ADR, with the same three assignments |

**Sweep rule applied.** After the edits: `grep -n 'quorum_rule\|QuorumRule'` over the three files
finds only withdrawal/derivation sentences; `grep -n 'state=Paused\|state=Healthy'` finds nothing
(`capability{state=…}` is a different field and untouched); `grep -n 'zero-event'` finds only the
redefined control; `grep -n 'seeds_armed'` finds the T-24 counting rule in design §2.4 and ADR §2
rule 1 and nowhere the old "on how many the checker armed" wording without it. Commands and
observed counts are in §D.

### C. Contract requests to foundation (via the lead)

1. **Header `provenance`** — the only new V-R20 contract ask. Replace `TraceHeader.seed: u64`
   with `pub provenance: Provenance`, exact shape in `trace-requirements.md` §8.1:
   `Generated { seed: u64 } | Reduced { parent: ScenarioId } | Authored { case: String }`, derives
   `Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize`; `ScenarioId(u64)`
   via `dense_id!` in `ids.rs`. `design.md` §3 re-uses this type for `Scenario.provenance`.
   Observed (uncommitted, not relied on): the working tree already carries an identical enum
   under K-F-09; if that is what lands, the ask is satisfied as is.
2. **Standing, not new:** `op_skipped { scenario_op_index: u32, reason: ReferentGone |
   OutOfBudget }` (round-1 F17, TR §3.16a) is not in the landed C0. Reducer diagnostics only; no
   checker reads it; design §4.5's realizability row waits on it with I1. The working tree
   carries an `OpSkipped` kind under K-F-08. Listed so §12 of the plan can cite it, not re-routed.
3. **Not requested:** `quorum_rule` on `ProtectionState`. V-R20 says derive. See Q-1 — the
   working tree adds it anyway (K-F-07).

### D. Commands run and observed results

Read-only checks; no cargo, no git write.

| Command | Observed |
|---|---|
| `git show 8a23b1d:crates/rdb-core/src/contracts/trace.rs` / `ids.rs` | the landed shapes in TR §8; `TraceHeader.seed: u64`; `ProtectionState.phase`; no `quorum_rule`, no `Provenance`, no `OpSkipped`; `BoundaryId` 29 members ending `ReturningStaleOwner` |
| `git diff --stat 8a23b1d -- crates/rdb-core/src/contracts/trace.rs ids.rs` | `277 +++` / `9 ++` uncommitted — the in-progress foundation edits the lead said not to rely on; peeked only to avoid asking for what is already in flight (§C) |
| `grep -n -i 'quorum_rule\|QuorumRule\|quorum rule'` over design, TR, ADR | design: §2.1 (derived), §2.3 (derivation sentence), §6 (`derived_quorum_rule`); TR: preamble, §3.14 (withdrawn), §4 (derived), §8 rows 2; ADR: §1 V3 (derived), Verification degraded-RF2 row |
| `grep -n 'state=Paused\|state=Healthy'` over the three files | 0 matches |
| `grep -n 'zero-event'` over the three files | design §2.4 and §4.5 (the redefined control); ADR two-reasons row rewritten without the phrase |
| `grep -n 'hook-gated'` over the three files | two hits before the sweep (design §10 V-R19 row, TR §3.18); both now say the hook cells are the V-R19 special case of the per-family rule — the sweep's one real catch |
| `grep -n 'role=Regular[^S]'` etc. | 0 bare `Regular` literals; TR §3.5 keeps the original beside its landed note (A-3) |
| `wc -l` | design 834, TR 433, ADR 304 |

### E. Assumptions, deviations, questions — each with my default

| # | Item | Default |
|---|---|---|
| Q-1 | The uncommitted working tree adds `quorum_rule: QuorumRule` to `ProtectionState` (K-F-07) with a comment saying deriving it "would re-implement the protection decision under test". V-R20 says derive from `required_copy_set.len()`, which is a length of a declared field, not a re-implementation. If the field lands anyway: ignore it, or cross-check it? | **Ignore it (V-R20 as ruled).** Cheaper alternative if the lead prefers: the oracle derives, and additionally treats a declared `quorum_rule` that disagrees with the derived one as a violation — one comparison, no new reader of kernel logic. I did not write that in; it needs a ruling because it makes the field load-bearing again. |
| Q-2 | The T-28 clause "wired ⇒ `seeds_armed > 0`" needs a scheduled producer per checker. INV-VER has none: no `ScenarioOp` variant injects an unknown mandatory version and `BoundaryId` (which must equal spike §6's column and nothing else) has no such member. Once C0/T1 report `Wired`, the row goes red for INV-VER. | **Exclude INV-VER from the clause until the grammar gains a producer**, and ask foundation whether spike §6's column has a version boundary I missed; if it does not, the `version_check × {accept, refuse}` guard cell is also only reachable through `AckRejectReason::IncompatibleVersion`, which is an H1 transport fault, not a scenario op. Design §2.4 states the gap plainly rather than claiming it closed. |
| A-1 | `required_copy_set.len()` outside {2, 3} is a violation (`required_copy_set_shape`). The ruling gave the derivation, not the fail-closed case; I added it so the developer does not guess. | Keep; fail closed is the charter's direction. |
| A-2 | Provisional family → package map for T-35 (Network/Time/Control → H1, Storage → M1, Client/Recovery → I1). H1 = "scheduler, clock, network, fake control" per team-rules, M1 = memory storage; Client and Recovery ops are applied by the dispatcher. | Foundation's handoff overrides per member; the planner's enumeration row asserts against their list, not mine. |
| A-3 | TR §3 subsections keep their original field lists and gain landed-name notes only where a rule would read wrong; §8 is the complete map. A full rename of §3 would have touched ~40 lines for no checker benefit and risked the planner mirroring a moving target. | Keep. |
| D-1 | Design §3's `Provenance` changed shape (`Reduced{from: Box<Provenance>}` → `Reduced{parent: ScenarioId}`, `Authored{case: &'static str}` → `String`) to match TR §1/§8.1 and the contract's serde needs. Not from a finding; it removed a contradiction between my own two files. | — |

### F. What the planner must mirror (their file; listed so the critic can check alignment)

| Planner section / row | Must match |
|---|---|
| VA-2, M7V-52 | design §2.4 fold paragraph word for word, including the T-24 counting rule (T-34) |
| VA-7 `invariant_status` | `reason` + separate `package` on the log line; artifact string form (T-30) |
| VA-1 / §4 convention 1 | landed C0 names per TR §8; convention 4: `TraceBuilder::ack_from(n, s)` emits the secondary `batch_apply` (T-26) |
| M7V-03(b) | header + ten `capability{state=Wired}` lines, nothing else (T-26) |
| M7V-07/08/09/79, M7V-55/56, Q-35 | derived quorum rule, cell `derived_quorum_rule`; M7V-56 enumerates `ProtectionPhase`, `ReplicaRole`, `ErrorKind` subset, `RecoveryMode`, `AckRejectReason`, `BoundaryId` (T-23) |
| M7V-46, §12 | under "C0 + provenance" until §8.1 lands (T-23) |
| M7V-31, M7V-33 | assert `armed() == false` after the fold (T-24) |
| M7V-57/58/61/63/64/75/76 | sub-N branch: `coverage_gated: false`, no `required_missing` failure (T-25) |
| M7V-78 | clause (3) wired ⇒ `seeds_armed > 0`, per-checker producer named, INV-VER excluded pending Q-2 (T-28) |
| VA-6, M7V-56 | gating table `BoundaryId -> PackageId` per family; set equality against foundation's emitter list (T-35) |
| §15 | point at TR §8 instead of carrying its own drift list (T-23 closure step 1) |

### G. Residual risks

| # | Risk | Mitigation |
|---|---|---|
| R-11 | Foundation lands `quorum_rule` and the developer reads it because it is there, making the field load-bearing against V-R20. | Design §2.1/§2.3 say "never stored or read as a field"; M7V-01's allowlist does not stop a field read, so Q-1's cross-check variant is the only mechanical guard — the lead's call. |
| R-12 | INV-VER's wired-implies-armed row has no producer (Q-2); if the clause is coded without the exclusion, the first wired build turns it red for a reason unrelated to a defect. | Design §2.4 states the gap; planner excludes INV-VER from M7V-78 clause (3) until a producer exists. |
| R-13 | The provisional family map is wrong for one family (most likely Recovery: if F1's own module emits `fault_injected` for recovery boundaries, the key is F1, not I1). A wrong key reports a cell `unavailable(I1)` while I1 is unwired and `missing` once I1 is wired but the true emitter is not — the loud direction, not a false green. | Foundation's handoff names the emitter per member; the enumeration row asserts set equality against it. |
| R-14 | TR §3's original field names remain beside landed-name notes; a reader who skips §8 can still copy an old name. | The preamble sends every reader to §8 first; the planner's convention 1 switches to landed names. |

### H. Recommended status

**Critic re-review of this diff only** — design §2.1, §2.3 INV-PUB/INV-LAG, §2.4, §2.5, §3,
§3.1, §4.5, §5.3, §6, §7.2, §9, §10; TR preamble, §1, §3.2, §3.5, §3.7, §3.14, §3.19, §4, §6,
§7, §8, §8.1; ADR §1 V3/V8, §2 rules 1 and 3, artifact table, Verification, Notes — together with
the planner's round-3 correction, in one round. Two rulings needed before the developer codes the
fold or the coverage gate: Q-1 (`quorum_rule` if it lands) and Q-2 (INV-VER producer). The
developer may start the rest of design §9's dependency-free work now, typing landed C0 names from
TR §8.

### V-R21 applied

Round 3 accepted; ADR 0019 committed at `dd81431`. Lead ruling V-R21 answered Q-1 and Q-2;
both closed with the smallest edit, nothing else touched.

| Q | Ruling | Where |
|---|---|---|
| Q-1 `quorum_rule` | Derived value from `required_copy_set.len()` is authoritative for the coverage cell and every oracle decision. If K-F-07 lands the field, the oracle cross-checks it against the derived value; mismatch = violation `quorum_rule_mismatch`; the field is read for nothing else. **→ The conditional half is superseded by F-R13 (round 4 below): no field will land, so there is no cross-check and no `quorum_rule_mismatch`. The derivation stands unchanged.** | design §2.3 INV-PUB row (one sentence after the V-R20 derivation); TR §8 row 2 and the "observed but not relied on" note below the table. ADR 0019 unchanged — no violation list lives there. |
| Q-2 INV-VER | Excluded from the T-28 wired ⇒ `seeds_armed > 0` clause until a producing `ScenarioOp` or `BoundaryId` exists. Excluded set listed explicitly: INV-VER only. | design §2.4 wired-implies-armed paragraph; ADR 0019 §2 rule 1, one mirrored sentence. |

R-11 and R-12 (§G) close with these. Planner mirror: M7V-78 clause (3) asserts over nine
invariants and names INV-VER as excluded; the violation signature list in the plan gains
`quorum_rule_mismatch` beside `required_copy_set_shape`.

### Correction round 4 (critic T-36..T-41)

Critic round 4: PASS_WITH_RISKS, twelve of T-23..T-35 closed, one revised, none sustained,
developer may start. Six new findings; four have a design half and all four are closed below.
**ADR 0019 needs no round-4 change** — see T-37(c). Nothing disputed.

| Finding | Closure | Where |
|---|---|---|
| **T-37(a)** `coverage_gated` in the campaign artifact | Design §5.3 was already right — its campaign key list never carried `coverage_gated`, only the coverage bullet does. To stop it drifting back I made the absence explicit rather than implicit: the campaign bullet now names `coverage_gated` as deliberately *not* a campaign key. The red row is the planner's M7V-72. | design §5.3 campaign bullet |
| **T-37(b)** `caught_by` vs `catching_row` | `caught_by` was the dead name; adopted `mutations{id -> catching_row}`, key unchanged, **value list-valued** (V-R20 (5)), with the MUT-2 two-halves example so a developer does not write a scalar. `grep caught_by` over design, TR and the ADR now returns 0. | design §5.3 |
| **T-37(c)** orphan campaign key `coverage{required_cells, hit_cells, missing[]}` | **Removed from design §5.3, not added to the ADR** — stated with the reason in the text. Rationale: the coverage artifact already carries the full matrix, `required_missing[]`, `unavailable_cells{}` and `coverage_gated`; a second summary of the same counts in a second file is one more place for them to disagree, which is the reason V-R16 put status, reason and `seeds_armed` in one object. Its `missing[]` was also the only surviving copy of a name spelled `required_missing[]` everywhere else. **So ADR 0019 is untouched this round** (`git status` on it is clean — your V-R21 commit stands). | design §5.3 |
| **T-39** `required_copy_set_shape`: violation or fixture defect | **Violation**, decided once and written into §2.3 with the argument. The oracle reads only the trace and cannot distinguish a bad fixture from a kernel that really pinned a one- or four-node set; treating the shape as a fixture defect forces it to skip INV-PUB for that seed, which is exactly the silent skip the design exists to prevent. A shape it cannot derive a rule from must fail loudly. **Action for you: this needs routing** — one sub-case on M7V-07, and Q-35's "fixture or cadence defect" wording aligned to call it a violation. | design §2.3 INV-PUB row |
| **T-40** family map had no ground truth | Closed free, as the critic said. The table is now checked **against the trace**: for every observed `fault_injected`, `fault_kind` must equal the family the gating table assigns to that event's `boundary`. Ground truth comes from the emitting provider, so the enumeration row stops reading the const it is testing. No contract change. Planner hangs the assertion on M7V-42 or M7V-55. | design §3.1 family-map paragraph |
| **T-41(a)** stale row id | `M7V-78` → **`M7V-89`** for the wired clause, with one clause noting M7V-78 keeps the unexcluded `proven ⇒ armed` half, so the two ids cannot be confused again. | design §2.4 |
| **T-41(b)** `ack_from` hard-coded role | The helper now emits `role=<n's declared role>`, read from the fixture's topology, explicitly **not** hard-coded to `RegularSecondary`; a shadow ack (M7V-07, MUT-2) uses the same helper and cannot silently acquire a regular's role. | design §4.5 |
| **T-41(c)** TR `state` | "even when `state` is unchanged" → `phase`, with the §8 row-3 pointer. `grep` for a stale `state` field name in TR returns 0. | trace-requirements §3.14 |

**Not mine, no design half, no action taken:** T-36 (M7V-87's selector collides with drift-table
row `| 3 |`) and T-38 (M7V-89's arming table contradicts design §2.4 and misstates INV-DEDUP).
On T-38 I re-read design §2.4's two arming lists: they agree with each other, INV-DEDUP arms on a
*second* submit with a retained identity via the `RetainedDedupHit` boundary, and the critic is
right that the plan is the outlier. Design needs no edit; the fix is M7V-89 citing §2.4 rather
than restating it.

**Files this round:** `design.md` 834 → 855 (5 edits), `trace-requirements.md` 433 → 435 (1 edit).
ADR 0019 unchanged at 306 lines. Test plan untouched.

**Open with you:** T-39's plan row and Q-35 alignment are the only routed items; everything else
is closed in place.

### F-R13 applied — the `quorum_rule` cross-check is withdrawn

Lead ruling **F-R13** settled K-F-07 against V-R20: there will never be a `quorum_rule` field,
and foundation's committed contract at `6893442` carries none. The plan has dropped row M7V-90 and
the `quorum_rule_mismatch` signature. V-R21's conditional half is therefore unreachable and is
withdrawn from all three of its homes, marked superseded rather than silently deleted so a reader
arriving from an older critic or plan reference sees why the cross-check is gone:

| Home | Now reads |
|---|---|
| design §2.3 INV-PUB | derivation from `required_copy_set.len()` unchanged and authoritative; "no declared field to reconcile it with, and no cross-check", F-R13 and `6893442` named, and the withdrawn signature and row named so nobody hunts for them |
| TR §8 row 2 | withdrawal now reads "settled for good by F-R13" instead of "if the field lands" |
| TR "observed but not relied on" note | the `quorum_rule` half marked dead with the ruling and the commit; `Provenance` and `OpSkipped` still stand as unlanded asks |
| handoff V-R21 table, Q-1 row | annotated in place with a pointer here; kept as ledger history, not rewritten |

**Untouched, as instructed:** V-R21's INV-VER exclusion (design §2.4, ADR §2 rule 1) and the rest
of the INV-PUB paragraph, including `required_copy_set_shape`, which T-39 confirmed as a violation
and which now has its M7V-07 sub-case and aligned Q-35 wording.

**ADR 0019 needs nothing.** It never carried the cross-check — no violation-signature list lives
there — and its only V-R21 content is the INV-VER exclusion sentence in §2 rule 1, which F-R13
does not touch. `git status` on the file is clean.

Files: `design.md` 855 (1 edit), `trace-requirements.md` 435 (2 edits), this handoff (2 edits).
