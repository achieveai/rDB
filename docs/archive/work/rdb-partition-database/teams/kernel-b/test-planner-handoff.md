# Kernel-b test planner handoff

Role: test planner, team kernel-b. Date: 2026-09-20. Branch `feature/rdb-m7`.

## 1. Outcome

COMPLETED, recommended status REVIEW. `docs/testing/test-plan-m7-kernel-b.md` written: 119 active rows
(110 unit, 9 sim, 0 campaign) plus 18 held rows (16 unit, 2 sim) drafted against the round-2 design and
marked "provisional pending critic-kernel-b-2", plus Q-46..Q-54. Every charter acceptance row appears
verbatim in at least one row (plan §10 map). Every row names design section, ADR clause, fixture,
assertion, class and dependency.

## 2. Artifacts

- `C:\Users\gautamb\source\repos\rEtcd\docs\testing\test-plan-m7-kernel-b.md` (new, 371 lines, UTF-8)
- `C:\Users\gautamb\source\repos\rEtcd\.claude\scratchpad\conversation_memories\rdb-partition-database\teams\kernel-b\test-planner-handoff.md` (this file)

No other file touched. No git operations. No cargo run.

## 3. Criterion -> evidence

| Criterion | Evidence |
|---|---|
| Charter rows verbatim as rows | plan §10 table: R1 spike text in M7B-62; L1 in M7B-68; F1 in M7B-92; four charter clauses in M7B-92/111/97/104; spike §6 case in M7B-96 |
| Every row: design §, ADR clause, fixture, assertion, class, dependency | 7-column table shape on every section; `grep -c '^| M7B-'` = 145 lines (119 active + 18 held + 8 §13 references) |
| Format matches verification plan | header, BA-block (their VA), §2 taxonomy, rows, Q-rows continuing at Q-46 (kernel-a owns Q-41..Q-45), anti-flake, "Unavailable until", gate checklist |
| MUST CARRY V8 ladder (V-R14) | M7B-65 warn 1000; M7B-66 kernel row; M7B-67 harness row; M7B-68 integration row (2100 virtual); M7B-75 exact barrier; M7B-80 `next_interesting_tick`; hysteresis in held M7B-H8 |
| §3.5 four replacement rows | M7B-47, 48, 49, 50 |
| Unequal pairings / lone survivor / divergent digest / fsync before barrier | M7B-92 / M7B-111 / M7B-97 / M7B-104 |
| Near-miss discipline | golden fixtures per section; each reject row states its one-field delta; M7B-15/33 two-field ladder walks (BA-6, A6) |
| Numbers recorded not asserted | BA-7; sim rows assert virtual ticks only |
| Held rows in a Held section with placeholder ids and finding | plan §9, M7B-H1..H13 with "Waits on" column |
| Rulings applied | B-R3 (M7B-51/52/112), B-R6/17 (02, 28, 119), B-R13 (BA-3, 22, 99), B-R19 (no trybuild), B-R20 (no 5a row), B-R21 (46), B-R22 (41), B-R24 (H1), B-R25 (H4/H5/H6), B-R26 (54), B-R27 (70, H11), B-R29 (30, 41, 54, 57, H7b, H8b) |
| No legacy crate prefix | `tok=$(printf 'part'; printf 'db'); grep -ci "$tok" plan` = 0 |

## 4. Commands run and observed results

```
cd C:/Users/gautamb/source/repos/rEtcd
f=docs/testing/test-plan-m7-kernel-b.md
grep -c '^| M7B-' $f            -> 145
grep '^| M7B-[0-9]' $f | grep -c '| unit |'   -> 110
grep '^| M7B-[0-9]' $f | grep -c '| sim |'    -> 9
grep '^| M7B-H' $f | grep -c '| unit |'       -> 16
grep '^| M7B-H' $f | grep -c '| sim |'        -> 2
grep -c '^| Q-4' $f             -> 9
grep -ci "$tok" $f              -> 0
grep -c CONTINUE $f             -> 0
file $f                         -> Unicode text, UTF-8 text
```

Sim active ids: 26, 32, 47, 62, 67, 68, 78, 96, 104. Duplicate-id check: the only repeated ids are
references inside §13 "Unavailable until", not row definitions.

## 5. Assumptions and deviations

- Flat numbering `M7B-01..119` across the three test files (architect Q10 default).
- Held rows drafted per the lead's mid-task instruction against round-2 text; marker left in place.
- `next_interesting_tick` `Reprotecting` arm and the 5 s hysteresis are held (K-B-41) even though V-R14
  names them; the exact-barrier half (M7B-75) is active.
- Row M7B-13 names `NOT_A_MEMBER` for an unauthenticated label (design §1.3 `copy_of` = None); the seed
  `AppendReject::Unauthenticated` exists. Either is accepted by the row; see Q6.
- Campaign class left empty on purpose; verification plan owns multi-seed campaigns.
- M7B-116 is a source grep (charter DO-NOT list); it is a unit-class test that shells out to `rg`.
- `LengthSpy`, `HashSpy`, `DecodeSpy`, `LookupSpy`, `SelectSpy` are test-only hooks (cfg(test) counters);
  they need a `#[cfg(test)]` seam in rdb-core. Deviation if the developer prefers effect-only proof.

## 6. Questions for the lead (each with a default)

| # | Question | Default |
|---|---|---|
| Q1 | Flat numbering across replication/protection/recovery files? | Yes (architect Q10) |
| Q2 | `AppendReject`/`AppendOutcome` in C0 has 5 variants; design needs ~16 and `NeedPrefix{from, head_digest}`. Additive C0 change by lead? | Yes, additive; rows 59/63 report Unavailable until then. B-R30: sent to foundation as a contract item; cited as "foundation ask (B-R30)" |
| Q3 | `PartitionConfig.min_regular_acks` absent from C0. Add with default 1, validation rejects 0? | Yes (B-R3); M7B-52 covers validation. B-R30: sent to foundation as a contract item; cited as "foundation ask (B-R30)" |
| Q4 | Fixture location: `crates/rdb-sim/tests/support/kernel_b/mod.rs` registered by foundation in `support/mod.rs`? | Yes; fallback in-file builders per test file |
| Q5 | Held ids: append as next free numbers (120+), never renumber? | Yes |
| Q6 | Unauthenticated label: `NOT_A_MEMBER` (design) or seed's `Unauthenticated`? | Keep seed's `Unauthenticated`; design text updated by architect |
| Q7 | Sim rows needing P1/T1/I1 (47, 68) report `Unavailable` until integrated, not deferred to verification? | Yes, Unavailable with named seam |
| Q8 | Add a seeded campaign (M7B-62, M7B-96 over 50 seeds) under `campaign` class? | No in PR default; propose as follow-up row set |
| Q9 | Which module emits `BlockPartition{DivergenceRequiresOperator}` at the cursor end (§3.6 Differs)? | R1 tracker (one emitter), cursor delegates |
| Q10 | `DurablePrefix` lacks a digest; barrier rows need `DurableProof{seq, digest}`. Kernel-b builds it from `Flushed` + ladder, or M1 adds digest? | Kernel-b builds it (no C0 change); M7B-24/99 written that way |

## 7. Risks

- Round-2 critic may change §3.2a/§3.6/§4.2 text; held rows would need re-alignment (ids stable).
- Design section numbers may shift; rows cite `D §x.y` and would need a sweep.
- `HealthEval` cadence (H1) vs `next_interesting_tick` hint: M7B-67 assumes 50 ms cadence still emitted
  regardless of the hint (design §4.7 says so).
- I1 admission propagation bound (≤ 50 ms) is asserted only end to end (M7B-68); no I1-local row exists
  in this plan (kernel-a's).
- 119 + 18 rows in three files; compile time in `rdb-sim` tests may push the gate. Splitting by section
  into modules is a reversible follow-up.
- Test-only spy hooks (A5 assumption) add `cfg(test)` surface to rdb-core.

## 7a. Post-commit fix (B-R30)

Plan committed at bd2b458; Q1..Q10 defaults accepted. Q-rows renumbered Q-41..Q-49 -> Q-46..Q-54 in plan and handoff (kernel-a owns Q-41..Q-45); every cross-reference (BA-4, §3 note, M7B-49/59/83/115/H1/H11a, §13, §14, §15) updated; M7B-52/59 and §13 now cite "foundation ask (B-R30)". Held rows still marked pending critic-kernel-b-2.

## Round 3 (holds lifted; B-R32)

Trigger: critic-kernel-b-2 round 3 PASS_WITH_RISKS, K-B-42..50 CLOSED; lead lifted every hold and
ruled B-R32 (K-B-51/52 fixes in architect round 4). Sources read: `design.md` round 3 (headings
§3.2a, §3.3 three-arm table, §3.4 one-writer paragraph, §3.6 step 1, §4.1 initial state, §4.4,
§4.5, §5.4, §5.6, §5.6a, §7 rows), architect handoff §13, critic `# Re-review after correction
round 3` (K-B-42..52), ADRs 0005/0006/0009 at ac9956d (verification rows named below).

### Where the rows went

§9 is kept as one block, retitled "Round 3 rows", rather than scattering the rows into §3–§8: the
H→id map reads straight down and the round-3 re-alignments are stated once at the top. Ids stable;
§3–§8 untouched except three re-alignments listed below.

### H → id map (18 former holds, in H order)

| Was | Now | Re-aligned to round 3? |
|---|---|---|
| H1 | M7B-120 | `recoverer` → `sender`; Q-55 grep added |
| H2 | M7B-121 | label vs `fence.sender`; shadow twin kept |
| H3 | M7B-122 | no change |
| H4 | M7B-123 | fixture notes `lookup(100) == NotRetained` → behind path (K-B-44) |
| H5 | M7B-124 | cites the `NotRetained` arm |
| H6 | M7B-125 | primary-side floor is `ProgressTracker.lineage.base_seq` (K-B-50) |
| H7 | M7B-126 | no change |
| H7a | M7B-127 | no change |
| H7b | M7B-128 | rewritten: stall + one `Alert{RebuildStalled}`, `required` never shrunk, copy ∉ required → no effect (K-B-43) |
| H8 | M7B-129 | fixture adds `blocked None` |
| H8a | M7B-130 | cites ADR-0006 row "A peer never heard from blocks resume" |
| H8b | M7B-131 | adds K-B-48 invariant `lag_domain() ⊇ regular_secondaries()`, shadow never in domain |
| H9 | M7B-132 | no change |
| H10 | M7B-133 | cites ADR-0003 §9 for the hop |
| H11 | M7B-134 | effect vector now exact (`==`), no `BlockPartition` |
| H11a | M7B-135 | no change |
| H12 | M7B-136 | sim; fence held by the shorter node C, `CatchUp{.., credential{sender B}}` (K-B-42) |
| H13 | M7B-137 | sim; L1 constructed `Paused` at `Recovered` (K-B-47) |

### New rows

| Id | Finding | Row |
|---|---|---|
| M7B-138 | K-B-42 | holder ≠ leader: `CatchUpBeforeGrant.credential.sender == holder`; accepted at leader and lagging holder; round-2 shape (`sender == F1 node`) → `NOT_A_MEMBER` twin |
| M7B-139 | K-B-44 | `Recovered` does `lookup(cutoff_seq)` first; `Match` adopts, `Differs` quarantines with lineage rows only, `NotRetained` truncates to the rung and takes the behind path |
| M7B-140 | K-B-45 | cursor emits `DivergenceDetected` only; tracker emits the B-R26 vector once; idempotent on redelivery |
| M7B-141 | K-B-47 | fresh `Protection` starts `Paused{cutoff, cutoff}` with `SetAdmission(Reject)` at construction; resume only via `Gained` + barrier + hold |
| M7B-142 | K-B-46 | `BlockPartition` as L1's fourth input: `blocked` set, `Paused`, reason `DIVERGENCE_REQUIRES_OPERATOR`; never cleared by `HealthEval` |
| M7B-143 | K-B-51, **provisional B-R32** | `Paused → Reprotecting` needs `blocked.is_none()`: block, then `ConfigChanged` + `Gained` + barrier → still `Paused`, no `Allow` |
| M7B-144 | K-B-52, **provisional B-R32** | tracker consumes `CopyQuarantined` like `DivergenceDetected`; `Rebuilding` gets `CopyLost` → one `RebuildStalled`; Q-56 grep |
| M7B-145 | K-B-49 | `ConfigChanged` adds `CopyProgress` entries, never removes; retirement removes |

Three existing rows re-aligned in place: M7B-45 (tracker `Recovered` zeroes peers, `diverged == false`,
no `retained_status_map` read), M7B-57 (cursor `Differs` → `[DivergenceDetected]` only, K-B-45),
M7B-82 (post-`Recovered` instance starts `Paused`, K-B-47). Every remaining `M7B-H`/"held" cross-
reference (M7B-68, 80, 92, 111, §10 map, Q-53/54 backs, §13, §14, §15, header, §2 dependency values)
now names the real id.

Q-rows: Q-46..54 unchanged; Q-55 (`recoverer` absent from `FenceCredential`, K-B-42) and Q-56
(`CopyQuarantined` has a tracker arm, provisional B-R32) take the next free numbers after Q-54.

### Counts, proven mechanically

```
cd C:/Users/gautamb/source/repos/rEtcd
f=docs/testing/test-plan-m7-kernel-b.md
grep -c 'M7B-H[0-9]' $f                                   -> 0
grep -o '^| M7B-[0-9]*' $f | sort -u | wc -l              -> 145
grep -o '^| M7B-[0-9]*' $f | sed 's/| M7B-//' | sort -n | tail -1   -> 145
for i in $(seq 1 145); do id=$(printf '%02d' $i); grep -q "^| M7B-$id " $f || echo "missing $id"; done   -> (nothing)
grep -o '^| M7B-[0-9]*' $f | sort | uniq -d               -> 13 22 47 52 67 68 84 137 143 (all §13 "Unavailable until" references, not row definitions)
grep '^| M7B-[0-9]' $f | grep -c '| unit |'               -> 134
grep '^| M7B-[0-9]' $f | grep -c '| sim |'                -> 11   (26 32 47 62 67 68 78 96 104 136 137)
grep '^| M7B-1[2-4][0-9] ' $f | grep -c '| unit |'        -> 24   (§9: 26 rows = 24 unit + 2 sim)
grep -c '^| Q-[0-9]' $f                                   -> 11   (Q-46..Q-56)
grep -c recoverer $f                                      -> 3    (the §9 note, M7B-120's "no recoverer field", Q-55)
tok=$(printf 'part'; printf 'db'); grep -ci "$tok" $f     -> 0
file $f                                                   -> UTF-8
```

Totals: 145 active rows = 134 unit + 11 sim + 0 campaign; 11 Q-rows; 0 held. Two rows provisional
(M7B-143, 144) pending the round-4 design text.

### Questions for the lead (defaults)

| # | Question | Default |
|---|---|---|
| R3-1 | §9 kept as one "Round 3 rows" block vs. scattering into §3–§8? | Keep the block (map reads straight down); scatter is a mechanical follow-up if the developer prefers per-file order |
| R3-2 | M7B-143/144 marker: who lifts "provisional B-R32"? | Lead, after architect round 4 lands and the critic confirms; planner re-checks the two rows' cited sections then |
| R3-3 | M7B-140/144 idempotence reason code `Ignored{ALREADY_DIVERGED}` is my name, not the design's | Keep; rename to the design's if round 4 names one |
| R3-4 | M7B-139 `Differs` arm asserts `RetainQuarantinedSuffix{..}` is emitted ("§5.7's shape") | Assert presence only, not fields, until the design writes the effect explicitly |
| R3-5 | M7B-145 assumes an ACK from a copy in no active predicate is `Ignored{NOT_A_MEMBER}` (M7B-44) | Yes; design §3.5 says entries are removed at retirement |
| R3-6 | K-B-48 (annotation only) gets no own row; it is folded into M7B-131 | Yes |

### Risks

- M7B-143/144 are written to the ruling text, not to design.md; wording of the conjunct and the
  tracker arm may differ once round 4 lands (ids stable, assertions may need one edit each).
- M7B-136 assumes the fence is held by the shorter node; if the sim harness cannot fence a specific
  node, the row degrades to "fence held by B" and the K-B-42 shorter-fenced case moves to M7B-138.
- `FenceCredential.sender` in C0 is still a V12 scheduling dependency for M7B-120..122, 136, 138.

## Round 4 (B-R33)

Critic round 3 (`teams/kernel-b/critic-tests.md`) returned PASS_WITH_RISKS with findings
T-B-01..08. Ruling B-R33 accepted all eight critic defaults (Q-B-1..8) verbatim. The plan is edited
in place; **no id moved and no row was deleted**. Rows written against design.md and ADRs
0005/0006/0009 at `9c9b1f9` (architect rounds 4 and 5), so nothing in the plan is provisional any
more.

### Finding -> rows changed -> evidence

| Finding | Ruling | Rows / sections changed | Evidence it is closed |
|---|---|---|---|
| T-B-01 BA-4's nine `@m` names are not landed `TraceKind` variants; `AckRejectReason` is short seven; M7B-78 is vacuous | Q-B-6, Q-B-8 | BA-4 rewritten as an explicit mapping carrying design §7's two-assertion-surfaces rule; M7B-78 rewritten; **new M7B-147** as its positive control; JSONL clauses fixed on M7B-26, 32, 62, 78, 96, 137; Q-46/Q-47 keyed to CB-3 in §13 | `rg -n "protection_transition|append_decision|catchup_step|barrier_check|source_unavailable" docs/testing/test-plan-m7-kernel-b.md` now hits only BA-4 and §15, never a row assertion. M7B-78 asserts `phase != Resuming` **and** `phase != Healthy`; M7B-147 proves the same fixture without the fault does emit `Resuming` |
| T-B-02 no carrier for kernel-internal events/effects; BA-1 and BA-2 cannot both hold; `HealthEval` is `TimerFired` + `ctx.now` | Q-B-1 | **new BA-10**; BA-1 and BA-2 rewritten to depend on it; §7 preamble states the `HealthEval{t}` reading once with the `health_eval(t)` helper; M7B-28, 60, 61, 64, 67, 84, 142 respelled; §13 lists the carrier as CB-1 | BA-10 cites design.md 193 and ADR 0006 line 49. `rg -n "HealthEval" docs/testing/test-plan-m7-kernel-b.md` returns only BA-10, the §7 preamble and the design-cite column — no row treats it as an event payload. M7B-67, the one row that reads the tick stream, is written against `ctx.now` |
| T-B-03 M7B-111/137 assert the wrong module; nothing asserts the activation reaching L1 | Q-B-2 | M7B-111 drops the `SetAdmission(Allow)` clause and cross-references kernel-a's `Frozen{RecoveryReadOnly}`; M7B-137 asserts the F1 re-emission and L1 resuming mode-blind; **new M7B-148** proves the re-emission at unit level; §10 charter map updated | M7B-137 and M7B-148 cite design.md 1752 and ADR 0009 line 265. M7B-111's assertion column now contains no `SetAdmission` |
| T-B-04 no C0 drift table; three gaps in the asks | Q-B-3, Q-B-4 | **new §15 drift table** (counts moved to §16); renames applied (`ProgressAck`→`AppendAck` on M7B-17, 22, 144; `Regular`→`RegularSecondary` on M7B-32; `ev.tick`→`Event.at`/`ctx.now`, with M7B-115 stating why it is the one row that reads the event's tick; `ControlCas`→`ControlEffect::Cas`; `ControlCasResult`→`ControlEvent::CasResult` on M7B-84, 106); M7B-109 split; M7B-110 `Blocked` gains a reason; CB-4 named on M7B-16, 17, 19, 59, 60; A5 macro ownership fixed; M7B-14's alert renamed to `SelfQuarantined` so it does not collide with the routed `CopyQuarantined` event | `rg -n "ProgressAck|ControlCasResult|QuorumLost" docs/testing/test-plan-m7-kernel-b.md` hits only §15 and the gate checklist's negative check. §15 is 15 rows plus the one recorded disagreement below |
| T-B-05 BA-2 vs the `effects == []` rows | accepted | BA-2 states no row asserts an empty vector; M7B-128's twin → `[Ignored{NOT_REQUIRED}]`; M7B-142's second `BlockPartition` → `[Ignored{ALREADY_BLOCKED}]`; **new BA-11** for the reason codes; A3 bans the empty-vector forms; Q-57 greps for them | `rg -n "== \[\]|is_empty" docs/testing/test-plan-m7-kernel-b.md` returns nothing |
| T-B-06 §13 over-holds M7B-22..25; M7B-13 can pin `Unauthenticated` | accepted | M7B-13 pins landed `AppendReject::Unauthenticated` (closing handoff Q6); §13's M1 row narrowed to the fault injectors (M7B-26, 78, 104, 147) and its T1 row to M7B-31, 32, 62, 96; M7B-22's dependency is now `none` | §13 rows name what each release waits on; five rows left the hold list and none of them injects a fault |
| T-B-07 M7B-27 omits `cutoff_digest` | accepted | M7B-27's fixture states `cutoff_digest d15` and cites M7B-139 as its `Differs`/`NotRetained` twin | `Match` is the only arm that adopts the pair (design.md 625–629), so the row now names the digest it adopts |
| T-B-08 M7B-41 vs M7B-140 read §3.4 item 1 two ways | Q-B-5 | both stand; each now states its width and cites the clause — five-wide from rule 9 with `DivergenceDetected` at index 0, four-wide from the routed path with `Alert` at index 0 | design.md 729; ADR 0005 line 304 |
| Q-B-7 lift the provisional markers | accepted | M7B-143, M7B-144, Q-56, the §9 header and preamble, §13, §14 and the counts table all say the markers are lifted; both rows' dependency is `none` | `rg -n "provisional" docs/testing/test-plan-m7-kernel-b.md` returns only sentences recording that the markers **were** lifted |

### Coordinator's round-5 drifts

| Drift | Action |
|---|---|
| M7B-109 asserts the withdrawn `QuorumLost` arm | **Done.** M7B-109 is now the `Unavailable` row (`Blocked{ControlUnavailable}`); **new M7B-146** is the `Unknown` row (`Blocked{ControlUnknown}`). Both assert "never retry blind" and no `ControlEffect::Cas` in the vector; the two reasons are asserted by value so they stay distinct. §15 records the withdrawal (design.md 1425–1428; ADR 0009 line 326) |
| M7B-84 and M7B-106 still feed `ControlCasResult` | **Done.** Both feed `ControlEvent::CasResult{key: ControlKey::Partition(id), outcome: CasOutcome::..}`. M7B-84's `DiscoveryDeadline` is also now a `TimerFired` at `ctx.now` |
| "three arms" must become "four" | **Not applied as stated — recorded instead.** Read against design.md at `9c9b1f9`, `history_digests.lookup(cutoff_seq)` has exactly three arms, `Match` / `Differs` / `NotRetained` (design.md 625–629), and `DigestLookup` is three-valued; the **four**-arm item is `CasOutcome` (design.md 1432–1437). The plan's two "three arms" phrases are both K-B-44 (§9 preamble and M7B-139's test name), and the CAS four-arm fact is now explicit at M7B-106/109/146 and in §10. §15 carries the disagreement in writing. **Question Q-R4-1 below.** |
| Foundation asks are CB-1..CB-4 | **Done.** Used in the dependency column (CB-2 on M7B-21, 55, 56, 57, 63, 124, 140; CB-4 on M7B-16, 17, 19, 59, 60), in BA-4 (CB-3), BA-10 (CB-1), §2 and §13 |

### Drift-table pointer

`docs/testing/test-plan-m7-kernel-b.md` **§15 "Source state at the time of writing, and the drift
from it"** — 15 mapped rows plus the recorded disagreement. Shape copied from the verification
plan's §15. Row counts moved to §16; every in-plan cross-reference to "§15" now means the drift
table.

### Counts, proven mechanically

Commands (run from the repo root; read-only, no cargo, no git):

```
f=docs/testing/test-plan-m7-kernel-b.md
grep -o '^| M7B-[0-9]\+ ' $f | tr -d '| ' | sort -u | wc -l        # 148 unique ids
grep -c '^| M7B-[0-9]\+ ' $f                                       # 152 lines
grep -o '^| M7B-[0-9]\+ ' $f | tr -d '| ' | sort | uniq -d         # M7B-52, 67, 68, 137
s=$(grep -n '^## 13\.' $f | cut -d: -f1)                            # 387
awk -v s=$s 'NR<s && /^\| M7B-[0-9]+ /' $f | awk -F'|' '{print $(NF-2)}' | tr -d ' ' | sort | uniq -c
```

Observed:

- **148 unique row ids, no gaps in 1..148** (checked by differencing the id set against `seq 1 148`;
  the difference is empty).
- 152 lines match the row-definition pattern; the four extra are **§13 cross-references**, not
  definitions — `M7B-52`, `M7B-67`, `M7B-68`, `M7B-137` each appear once as a row (lines 155, 194,
  195, 313) and once in the "Unavailable until" table, which starts at line 387 (lines 394, 396,
  397, 398). Same four as round 3; nothing to fix.
- Class tally over the 148 definitions: **136 unit, 12 sim, 0 campaign**.
- Per section: 01–29 = 29 (28/1); 30–45 = 16 (15/1); 46–54 = 9 (8/1); 55–63 = 9 (8/1); 64–83 = 20
  (17/3); 84–119 = 36 (34/2); 120–148 = 29 (26/3). These are exactly the §16 table's numbers.
- Round-4 delta: **+3 rows** (M7B-146, 147, 148) and **+1 Q-row** (Q-57, next free after Q-56).
  Q-rows are now Q-46..Q-57, twelve of them.

### Questions for the lead (each with a default)

| # | Question | Default if no answer |
|---|---|---|
**Resolution (lead, after round 4 was committed at `6c5a929`).** Q-R4-1 was resolved in the plan's
favour: the lead re-read design.md at `9c9b1f9`, confirmed `lookup(cutoff_seq)` has three arms at
625–629 and that the four-arm fact is `CasOutcome` at 1432–1437, and withdrew the instruction. No
design change follows; M7B-27, 124 and 139 stay as written. Q-R4-3, Q-R4-4 and Q-R4-5 are answered
with their stated defaults. **Q-R4-2 is not** — checking it turned up a fact that changes the
answer; see "Q-R4-2 re-answered" below.

| Q-R4-1 | The round-5 note said the §9 preamble's "three arms" should be "four". Against design.md at `9c9b1f9` the `lookup(cutoff_seq)` table still has three arms and `DigestLookup` is three-valued; the four-arm item is `CasOutcome`. Did the architect mean the CAS, or is a fourth `lookup` arm coming? | The CAS. The plan keeps "three arms" for K-B-44 and states "four arms" for the CAS. If a fourth `lookup` arm lands, M7B-27, 124, 139 and the §9 preamble move with it |
| Q-R4-2 | M7B-110's fourth mode asserts `Blocked{reason: NoEligibleRegular}`. The architect's `BlockReason` set is in flight and may spell it differently | The developer takes the landed spelling; the row pins that the reason is **distinct** from `DivergenceRequiresOperator`, `ControlUnavailable` and `ControlUnknown`, which is the property that matters |
| Q-R4-3 | M7B-14's alert is renamed `Alert{kind: SelfQuarantined}` so it cannot be confused with the routed `CopyQuarantined` event (Q-56 greps for consumers of that name). The design names no alert on ladder row 7 | Keep `SelfQuarantined`. If the architect names one, the row takes that name provided it differs from the event's |
| Q-R4-4 | M7B-146..148 sit in §9 rather than in §8 and §7 where their subject matter lives | Keep them in §9. Ids never move, and the round-by-round block keeps the finding → row trail readable; §10 and §13 route them |
| Q-R4-5 | CB-1 is listed once in §13 for ~130 rows rather than on each row's dependency column | Keep the single listing. Putting `CB-1` on 130 rows would make the column useless for the seams that genuinely differ between rows |

### Q-R4-2 re-answered: a new ask, CB-5

My default was "the developer takes the landed spelling; the row pins distinctness." That default
assumed the variants exist and only their names were open. They do not.

**Checked, not inferred.** `crates/rdb-core/src/contracts/authority.rs:198` — landed `BlockReason`
has **exactly one** variant:

```rust
pub enum BlockReason {
    DivergenceRequiresOperator { diverged: Vec<CopyId> },
}
```

`PartitionMode` (same file, line 212) has landed with `Blocked { reason: BlockReason }`, so BA-8's
"in flight" wording was stale and is corrected.

Three consequences, all applied to the plan:

1. **`NoEligibleRegular`, `ControlUnavailable` and `ControlUnknown` do not exist.** M7B-110, M7B-109
   and M7B-146 name them, so those three rows are now `Unavailable` on a new ask, **CB-5**:
   `BlockReason` widened by those three variants. Collapsing them into one reason is not an option —
   design.md 1436–1441 keeps `Unavailable` and `Unknown` apart precisely because they are different
   operator stories, and asserting a bare `Blocked` would discard the whole content of the rows.
2. **CB-5 was raised by this plan, and is now official.** The architect's handoff §15 carried
   CB-1..CB-4 only. **Ruling B-R34 adopts CB-5 keeping this number**, and the lead independently
   confirmed the single landed variant at `authority.rs:197` and refused any collapse of
   `Unavailable` and `Unknown`. The plan's §2, §13 and §15 now cite B-R34 instead of the
   "proposed, not yet in the architect's handoff" wording, which went stale the moment it was
   adopted — the same freshness failure BA-8 had.
3. **M7B-142 gained an assertion rather than losing one.** Its reason is the one that landed, and the
   landed variant carries `diverged: Vec<CopyId>`, which the design's prose does not show. The row
   now asserts `Some(BlockReason::DivergenceRequiresOperator{diverged: vec![C, B]})` — payload and
   order — instead of the bare variant.

Q-R4-3, Q-R4-4 and Q-R4-5 stand on their defaults: keep `Alert{kind: SelfQuarantined}` until the
architect names an alert on ladder row 7; keep M7B-146..148 in §9; keep CB-1 listed once in §13.
None of the three turns on a fact I could not check.

### Rows that mirror design text verbatim

Standing note from the sibling team, applied. These rows restate design or ADR text word for word
and will drift silently if that text is reworded — **the critic should diff each against its cited
line every round**:

| Row | Mirrors | Source line at `9c9b1f9` |
|---|---|---|
| M7B-146 | "the CAS is *more* likely to have landed than in the `Unavailable` case, which is exactly why a blind retry is worse here, not better" | design.md 1437 |
| M7B-148 | "and, on `Committed(revision)`, re-emit `Recovered(RecoveryResult { mode: Active, .. })`" | design.md 1752; ADR 0009 line 265 |
| M7B-147 | "`Reprotecting` is traced as `ProtectionPhase::Resuming`" | design.md 1320; ADR 0006 line 198 |
| M7B-140, M7B-41 | "`DivergenceDetected` **only on the rule-9 path**" and the four-wide/five-wide vector widths | design.md 729; ADR 0005 line 304 |
| M7B-139 | the three `lookup(cutoff_seq)` arms and the `Differs` row's "not a target of this `Rebuilding`" | design.md 625–629 |
| M7B-128 | "`required` is NEVER shrunk" | design.md 1746–1751 |
| BA-4, §15 | design §7's two-assertion-surfaces preamble, paraphrased closely | design.md 1935–1949 |

Rows that quote the **charter or spike verbatim** (M7B-62, 68, 92, 96, 111, 136, 137) are a
different case: there the verbatim text is the requirement and drift would be a charter change, not
a rewording. They are listed in §10 and do not need the same diff.

### Risks

- **CB-5 is the newest ask** (adopted under B-R34), and unlike CB-1 it has no start-now workaround: a
  row cannot assert a reason variant that does not compile. M7B-109, 110 and 146 are `Unavailable`
  until foundation lands it, which means the four-arm CAS coverage the gate checklist asks for is
  incomplete until then — two of the four arms are the ones that block.
- **BA-8 was stale for a round.** It described `PartitionMode` as "in flight" when it had landed.
  That is the failure mode §15 exists to catch, and it was caught by checking a question rather than
  by the drift table itself; the table is only as fresh as its last re-read.
- **CB-1 is the one that can move the rows.** If foundation lands a carrier shaped differently from
  `EventKind::Kernel(KernelEvent)` — for example one flat variant per kernel event — BA-10's "the
  rows' substance does not move" claim holds but the fixture helpers in BA-5 change, and the
  developer's private pair has to be unwound rather than swapped.
- **M7B-147 is a new sim row and the only positive control in the plan.** If it is dropped for
  budget, M7B-78 silently returns to proving nothing. The §14 checklist ties them together; that is
  a checklist, not a compiler.
- **The `Blocked` reason set is in flight.** Four rows now assert a reason value (M7B-109, 110, 142,
  146). If `BlockReason` lands without one of them, those rows report `Unavailable`, not a pass.
- §15 is a snapshot of C0 at `8a23b1d`. Foundation's seed-corrections run is in flight; the table
  needs a re-read against the landed contracts before the developer starts, and the drift is by
  design the thing most likely to be stale.
- Q-57's part (a) needs the landed `TraceKind` variant list to compare against. Until the sim writes
  its `@m` values, the query returns an empty set for the trivial reason, not the meaningful one.

## Round 5 (critic T-B-09..15)

Critic returned PASS_WITH_RISKS on the rounds 4 + CB-5 diff. Seven findings, none manufactured; all
seven closed. **Every contract claim below I re-read in the working tree myself, variant by
variant** — not taken from the critic's report.

### Finding -> change -> evidence

| Finding | Severity | Change | Evidence |
|---|---|---|---|
| T-B-10 §15's basis commit predates the contracts it cites | MATERIAL | §15 basis restated as **`ec610f4`** (see the marker section below — `6893442` was my first answer and was also one commit stale); new "Basis correction" paragraph; `AppendReject` drift row added; BA-10's two enum lists corrected; M7B-52 released; §1 and §2 basis lines updated | `8a23b1d` has no `contracts/authority.rs`; the contracts landed in `6893442` (943 insertions across every contracts file). Re-read at head: `AppendReject` 16 variants (`envelope.rs:524`), `EventKind` 7 (`event.rs:152`), `EffectKind` 6 (`event.rs:250`), `AckRejectReason` 7 (`trace.rs:292`), `PartitionConfig.min_regular_acks` + `DEFAULT_MIN_REGULAR_ACKS = 1` + `validate` rejecting 0 (`membership.rs:74`, `:111`) |
| T-B-11 M7B-110's dependency cell said `none` | MATERIAL-low | cell -> `CB-5 (landed BlockReason has no NoEligibleRegular)` | all three CB-5 rows now read `CB-5` in the column §14's gate item scans; verified by printing the three cells |
| T-B-12 §14's CAS line read as covered | MATERIAL-low | line rewritten: names the three rows that pass, states M7B-109/146 are `Unavailable` on CB-5, and says in the line itself that **the box cannot be ticked green** and the two arms are not covered today | the exception is now where the reader meets the claim, not four lines away |
| T-B-09 M7B-41 labelled a three-effect literal "five-wide" | MATERIAL-low | reworded to the critic's option (a): "the rule-9 shape … this fixture fires neither conditional effect, so the literal is three-wide"; five is stated as the maximum | matches M7B-140, which kept the conditional wording and was clean |
| T-B-13 BA-4's row list named M7B-146, omitted M7B-147 | ADVISORY | `146` -> `147` | M7B-146 makes no trace assertion; M7B-147 is entirely `protection_state` JSONL, and BA-4's own prose already named it |
| T-B-14 §9's ordering note overstated independence | ADVISORY | "M7B-147 and M7B-148 depend on nothing beyond BA-10's carrier; M7B-146 is `Unavailable` on CB-5" | second place that had told a developer M7B-146 was startable |
| T-B-15 two bare pipes in §11 | ADVISORY | escaped the alternation pipe in Q-51 (pre-existing) and Q-57 (mine, new in `6c5a929`) | pipe check below |

### The `drift-basis` marker, and the re-read that earned it

Marker set: `<!-- drift-basis: ec610f4 -->`, line 1 of the plan. **Set after the re-read, not
instead of it** — and the re-read was not a formality, because my round-5 basis was itself one
commit stale.

What I checked before setting it, read-only:

| Check | Result |
|---|---|
| `ec610f4` exists | yes, `git cat-file -t` -> commit |
| newest commit touching `crates/rdb-core/src/contracts/` | `ec610f4`, ahead of `6893442`, `8a23b1d`, `f8e3ca6` |
| uncommitted changes in the contracts dir | **none** — the modifications in this session's opening snapshot have since been committed |
| working tree vs `ec610f4` for that dir | `git diff --stat` **empty**, so everything §15 asserts was read from that exact commit |

**My round-5 basis was wrong too, by one commit.** I had restated §15 as `6893442`, the commit that
rewrote the contracts. `ec610f4` came after it: *"K-F-39 partition config refuses a zero threshold
on decode"*. Had I set the marker to `6893442` without re-reading, the gate would have failed me a
second time, and correctly.

**The extra commit changed a row.** K-F-39 added `#[serde(try_from = "UnvalidatedPartitionConfig")]`
to `PartitionConfig` (`membership.rs:61`), because a plain derived `Deserialize` on a public `u8`
admitted a zero threshold and nothing called `validate` on the decode path. So **M7B-52 now asserts
two paths, not one**: constructor `Err` and a decoded configuration refused. That matters for this
row specifically — the kernel's `PinnedConfig` arrives over the wire, so the constructor path alone
would have left the real path untested. A new §15 row records that the crate is stricter than the
design's constructor-only wording, and the crate wins.

This is the second time in two rounds that re-reading rather than trusting a default changed a row's
content. Both times the stale direction was over-holding or under-asserting.

### Both standing checks, run by me

- **Pipe check.** My first two attempts were wrong — `grep '\\|'` and a two-pass `gsub` both
  miscounted, and briefly accused M7B-116 (which was already correct). A character scan that skips
  a `|` preceded by a backslash reports **every row at its table width**: §3–§8 rows at 8, §9 rows
  at 9, §11 Q-rows at 5, §13 rows at 5. The tool was broken, not the file; worth saying because a
  broken checker that reports clean is the worse failure.
- **Verbatim-mirroring.** Eleven of twelve pairs clean at source. The one drift was M7B-41
  (T-B-09), now closed. The table gains no new entries: M7B-41's cell no longer mirrors the
  design's loose "five-wide" sentence, which is what drifted — **the design's own wording is the
  hazard here**, so the row now states the conditionality the design's transition table has and its
  prose does not.

### CB-3 re-sized, and two reused names justified

- **Arithmetic corrected.** The ask claimed landed `AckRejectReason` carried four of the §3.4
  reasons. It carries **seven** (`Gap`, `DigestMismatch`, `StaleEpoch`, `StaleBoot`, `StaleConfig`,
  `ForgedIdentity`, `IncompatibleVersion`) and **none is one of the seven the ask adds**. So CB-3 is
  additive 7 -> 14, not 4 -> 11. Re-sized in §13 before anyone works it.
- **The two collisions are deliberate, one line each.** `AckRejectReason::StaleGeneration` vs
  `AppendReject::StaleGeneration`: the receiver refusing an append from a stale generation and the
  primary dropping an ACK that claims one are the same generation fence read from the two ends;
  different words would hide that. `AckRejectReason::NotAMember` vs `AppendReject::NotAMember`:
  identically, the same membership check from the two ends. The other five have no counterpart in
  either enum. Different enums, so no ambiguity at a use site.
- **Accepted cost.** Verification's set-equality assertion stays exactly as written — it is a
  tripwire meant to fail on an un-celled widening. The cost is **seven coverage cells** in the
  verification plan, which that team owns; flagged here so it is not discovered as a surprise
  failure.

### What the re-read did *not* change

All five asks re-checked one by one at head and **all five are still correctly open**: CB-1 (no
`Kernel` variant in either enum), CB-2 (`NeedPrefix { have }` only, `envelope.rs:581`), CB-3
(`AckRejectReason` unwidened), CB-4 (no `AppendOutcome` type anywhere), CB-5 (`BlockReason` still
one variant). **Ruling B-R34 is unaffected** and M7B-109/110/146 stay `Unavailable`. The
three-arms-vs-four-arms question resolves in the plan's favour at source, as the lead has already
confirmed; no row moves.

### Counts after round 5

Unchanged where it matters: **148 unique ids, no gaps, 136 unit / 12 sim / 0 campaign**, Q-46..Q-57.
No row added, none deleted, no id moved. One row changed dependency **toward** available (M7B-52,
released), one **toward** held (M7B-110, corrected to CB-5). §13 lost one entry (M7B-52) and its
cross-reference count drops from four to three.

### Risks after round 5

- **The over-hold direction is the dangerous one and it recurred, twice.** Round 4 recorded BA-8 as
  stale; round 5 found a whole basis commit stale; and my *correction* to that basis was itself one
  commit behind until the marker forced a third look. Same root cause, same direction — the plan
  holding or under-asserting what had already landed. §15 now carries the basis in a machine-checked
  marker rather than in prose a reader must trust. **The sibling team has the identical defect from
  the identical cause; their §15 must not be copied, and their marker needs its own re-read rather
  than a copy of `ec610f4`.**
- **The marker is only as honest as the re-read behind it.** Setting it without re-reading silences
  the gate instead of satisfying it. If a future round updates the hash, the §15 variant lists and
  line numbers must be re-read in the same pass — they are what the hash is a claim about.
- **Five ladder literals changed surface, not meaning.** M7B-02..07, 14, 18, 20, 28 and 120..122
  now spell landed `AppendReject` variants instead of developer strings. The assertions do not move,
  but a developer who started against the round-4 text will have written strings.
- **CB-3's seven coverage cells** land on verification, not on us.
- CB-5 still has no start-now path, so the CAS gate box stays un-tickable. §14 now says so in the
  line itself.

### Standing check for every future round

The verbatim-mirroring table above is a **per-round check**, not a one-off note (lead, after round
4). The critic diffs each listed row against its cited design or ADR line every round; a reworded
source line means the row changes with it or the row is wrong. Rows quoting the charter or spike are
excluded, because there a change is a charter change.

If the foundation critic or the second foundation round changes a contract this plan depends on
(CB-1..CB-5, or any §15 drift-table row), the lead brings it back here rather than letting the plan
drift. Nothing in §15 is self-refreshing.

## 8. Recommended next role

Critic round 4 on the test plan -> `teams/kernel-b/critic-tests.md`. Inputs: the plan (especially the
new §15 drift table and rows M7B-146..148), this handoff's Round 4 section, design.md and ADRs
0005/0006/0009 at `9c9b1f9`, and the architect handoff §15 (CB-1..CB-4).
