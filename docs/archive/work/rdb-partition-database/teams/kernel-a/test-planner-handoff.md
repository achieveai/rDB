# kernel-a — test planner handoff (2026-09-20, first pass)

## 1. Outcome

**COMPLETED_WITH_RISKS.** `docs/testing/test-plan-m7-kernel-a.md` written: 137 numbered rows
(`M7A-01..M7A-137`) plus 9 held rows (`M7A-H01..H09`) waiting on architect correction round 2.
Every charter and spike §5/§6 row for A1, T1, P1 appears verbatim in §12's gate map with row ids.
Every ADR 0004 (15), ADR 0007 (22) and ADR 0008 (14 + §7 items 1–8) verification row is mapped.
Held rows are counted as **missing** by the gate map until re-issued.

## 2. Artifacts

- `C:\Users\gautamb\source\repos\rEtcd\docs\testing\test-plan-m7-kernel-a.md` (new)
- `C:\Users\gautamb\source\repos\rEtcd\.claude\scratchpad\conversation_memories\rdb-partition-database\teams\kernel-a\test-planner-handoff.md` (this file)

No other file touched. No git operations. No cargo.

### Row counts by class (written rows)

| Class | Count | Ids |
|---|---|---|
| unit | 122 | M7A-01..57, 59..84, 86..92, 94..96, 99..101, 103..116, 118..129 |
| sim | 12 | M7A-85, 93, 97, 98, 102, 117, 131..136 |
| campaign | 3 | M7A-58, 130, 137 |
| held (not classed) | 9 | M7A-H01..H09 |

By package: A1 61 (§3) · control fake / ADR 0008 12 (§6) · T1 29 (§4) · P1 28 (§5) · cross 7 (§7).

### Charter row → M7A row map

| Charter / spike row (verbatim) | Rows |
|---|---|
| A1 "Grant/fence state machine and coherent watch resync" | M7A-01..27 (state machine), M7A-28..33 (resync) |
| A1 "Expired/old-boot grants deny" | M7A-36/37, M7A-19/21, M7A-49 |
| A1 "pause/suspend and clock-bound violation fail closed" | M7A-47/48, M7A-38..42, M7A-50 |
| A1 "CAS races have one winner" | M7A-01, M7A-127 |
| T1 "Conditions, Put/Delete, atomic batch and retained request outcomes" | M7A-78/79, M7A-81..83, M7A-72/79/89 |
| T1 "Same request has one effect; changed payload rejects; different affinity rejects; local apply never returns success" | M7A-72 · 73 · 76 · 84+85 |
| P1 "Publication barrier, old-prefix snapshots, reads/status and uncertain outcomes" | M7A-107..109 · 108 · 113..118 · 102..104 |
| P1 "Late ACK revalidates authority; post-apply timeout freezes only its partition; lost reply remains queryable" | M7A-92 · 102 · 104/105 |
| A1/P1 adversarial "expire authority between publication and reply; delayed old dispatch after pause, reboot or new generation leaves quarantined bytes only" | M7A-131 · M7A-132/133/134 |
| F1/T1/P1, F1/T1 cross cases | M7A-135, M7A-136 |

Must-carry items: no-trim bounded growth M7A-90 (+89); A-R18 digest M7A-74/75/73; A-R12
M7A-38/43/44 (+ rule A5, `grant_duration_ms` named per K-A-42); A-R16 synchronous admission
M7A-66/67/68 + Q-42; `LateRenewalIgnored` M7A-22/18/36; 14–16 re-derivation in plan §14 with
M7A-137 (recorded, not asserted); near-miss twins named in every bad row.

## 3. Criterion → evidence

| Criterion | Evidence |
|---|---|
| Every charter acceptance row appears verbatim as ≥1 row | plan §12 rows 1–10, quoted text |
| Adversarial A1/P1 case present | M7A-131..134 (§7); inventory half depends on M1/O1 per A-R22 |
| Each row names design §, ADR clause, fixture, assertion, class, dep | every §3–§7 row has all seven columns; `Proves` cites design § · ADR · spec § |
| Every ADR 0004/0007/0008 row | §12 mapping lines (15 / 22 / 14+8); three ADR 0007 rows land in held H02, H05–H07 |
| Held V2 rows listed with their finding | §8: H01 K-A-33, H02 K-A-34, H03 K-A-35, H04 K-A-36, H05–H07 K-A-37, H08 K-A-40, H09 K-A-41; `⏸` markers on M7A-66/67/68/80/101/103..106/131 |
| Rulings applied | plan §14 first bullets list A-R10..A-R22, B-R20/21/27, F-R3/7/8/10 |
| Id uniqueness, no gaps, class totals | bash check: 146 row defs, 0 duplicate ids, sequence 01..137 with no gap, unit 122 / sim 12 / campaign 3 |
| UTF-8 | `file` reports UTF-8 text |

## 4. Commands run

- `grep`/`sed -n` reads of design.md §2.5, §4.3, §4.4 and the verification plan §2, §10–§14 (format model).
- Id/gap/class check over the plan (grep + awk); result above. No cargo, no git.

## 5. Assumptions and deviations

- `ProcessResumed` tolerance constant is not named in design §2.1; plan uses `resume_gap_tolerance_ticks` (Q-5).
- `ReplyEffect::Read` written per F-R7 although the grep of `event.rs:166` shows `Transaction, Status, Failed` (Q-9).
- `ControlOp` has no "never completes" member; M7A-125 assumes one at seam freeze (Q-8).
- Cross-package rows live in `publication.rs` (Q-3), so the charter's three-binary gate line stays verbatim.
- 14–16 event figure re-derived as **13–14** `step` inputs (15–16 if Store/Reply effects are counted); recorded as a Q1 suspect, not a target (Q-11).
- M7A-43's stale window pinned at `renewed_at 0`, ticks 2001..2899; rule A5 protects it.
- Held rows use `H` ids so the §12 checklist counts them missing; they are re-issued as `M7A-138+` after round 2, never renumbered.

## 6. Questions for the lead (each with a default)

| # | Question | Default |
|---|---|---|
| Q-1 | Q-row range `Q-41..Q-45` may collide with verification's correction pass | take `Q-41..45`; renumber to next free ids if verification lands first |
| Q-2 | `KA-1..KA-7` as the architecture-requirement series | yes |
| Q-3 | Cross-package rows in `publication.rs` or a fourth binary | `publication.rs`, module `cross` |
| Q-4 | Capacity policy for no-trim growth (M7A-90) | named `retention_cap_entries` + `OVERLOADED`; else `OVERLOADED` alone |
| Q-5 | Name of the resume-gap tolerance constant | `resume_gap_tolerance_ticks` = 500 |
| Q-6 | "Nothing happens" arms emit a `Fact` or an empty vector | emit a `Fact` |
| Q-7 | `Checkpoint::OutboxDispatch` feature-gate vs deny | feature-gate `m11` |
| Q-8 | `ControlOp` member for "dropped, never completes" | `ControlOp::DropNext` at seam freeze |
| Q-9 | `ReplyEffect::Read` vs current `event.rs` | trust F-R7; rows unavailable until C0 otherwise |
| Q-10 | M7A-128 (parent/root V5) stays in kernel-a's plan | keep, listed missing |
| Q-11 | Q1 budget counts `step` inputs (13–14) or effects too (15–16) | `step` inputs; record both |

## 7. Risks

- **R1 — held rows are one-third of the V2 story.** H01–H09 cover the dispatch checkpoint, the
  view rule, the external fence and the reply checkpoint. Until round 2 lands, M7A-131 (charter
  adversarial) has a held half. Mitigation: `⏸` markers and §8 keep the gate red.
- **R2 — K-A-42 flake.** Any developer who scales kernel constants with `RETCD_TEST_DEADLINE_SCALE`
  breaks M7A-43/44/46. KA-5 and rule A5 say so; a hook would be stronger.
- **R3 — seam drift.** M7A-107..111 (`ReplyEffect::Read`), M7A-51..57 (`FencingProof`), M7A-125
  (`ControlOp`) are written against seam shapes that are still moving (Q-8, Q-9, K-A-37).
- **R4 — inventory half of the adversarial rows** needs M1's `storage_inventory` line and O1's
  M7V-15; neither is scheduled by kernel-a. Without them M7A-132..134 prove the kernel half only.
- **R5 — 14–16 figure does not reproduce** under the `step`-input reading (13–14). If the Q1
  budget is set from 14–16, the corpus may look under budget for the wrong reason.
- **R6 — M7A-90's capacity policy** is not in the design; the row fails on silent growth by
  construction, which is intended, but the developer needs Q-4's answer to make it pass.

## 8. Recommended next role

**Critic, round 2 on the test plan** (`teams/kernel-a/critic-test-plan.md`), scoped to: twin
completeness (KA-6), ADR row coverage against the three ADR tables, the held list against the
architect's round-2 handoff once it lands, and Q-1..Q-11 defaults. Then the architect re-issues
H01–H09 as ordinary rows and the developer starts on §3.2–§3.4 and §4.1–§4.4 (no held
dependencies).

## Round 2 rows (2026-09-20, after architect correction round 2; A-R24 applied)

### Outcome

**COMPLETED_WITH_RISKS.** The nine held rows are re-issued as ordinary ids against the round-2
design text; 27 new rows `M7A-138..M7A-164` in plan §8, every one marked
**provisional pending critic-kernel-a-2** with its K-A id. 0 rows missing. Id space stable: no
existing id renumbered; `M7A-H01..H09` retired (mapping table in §8).

### What changed in the plan

| Change | Where |
|---|---|
| H01..H09 → M7A-138 (K-A-33), 141 (K-A-34), 143 (K-A-35), 148 (K-A-36), 149/150/151 (K-A-37), 152 (K-A-40), 156 (K-A-41) | §8 mapping table |
| Requested extras: `authority_seq` same-tick row **M7A-142**; `AcquireWithheld` no-CAS row **M7A-148**; `Blocked → Freeze → Recovered` row **M7A-158** (waiters drained once, replies withheld once, mode `Blocked → Blocked → <r.mode>`, all four `PartitionMode` variants) | §8.1, §8.3, §8.6 |
| Companion rows from the round-2 text: 139 (`Dispatched` kept), 140 (`mode == Open` guard alone), 144 (boundary at `valid_through_tick`), 145 (five republish points, none on `Conflict`), 146 (ADR "Admission horizon follows the sample"), 147 (stale view never replaces newer), 153 (ADR "Reply checkpoint outlives publication"), 154 (reply suppressed after timeout), 155 (map not Option), 157 (P1 view rows), 159 (`Blocked` guards + `ModeQuery`), 160 (T1 `Recovered` total), 161 (`Published` while frozen reopens only for `UnresolvedTransaction`), 162 (ADR "Adopted authority comes only from lineage installs"), 163 (ADR 0008 "Dropped control operation", both consumers), 164 (`watch_refused_attempts` reset) | §8 |
| `⏸` rows re-worded in place: M7A-66/67/68 (entry check by `valid_through_tick`/`past_horizon`), 80 (ADR 0004 amended row split across 80/138/82), 101/103/104/105/131 (`awaiting_reply`), 106 (adds `Blocked`), 60 (`authority_seq` on answers) | §3.6, §4.1, §4.2, §5.1, §5.2, §7 |
| M7A-24 re-worded to the amended ADR 0007 row (Unbounded **with** a valid sample: exactly one grant id, `ClockUnbounded` throughout); M7A-45 to the `e_new` guard (Unbounded-with-sample **does** renew) | §3.2, §3.4 |
| `epochs` → `served` (F-R10) in M7A-04/05/07/33; M7A-04 now asserts `AdoptAuthority` per owned partition | §3.1, §3.3 |
| §12 gate map: nine new criterion lines for K-A-33..41 and B-R29; ADR 0007 line lists the 3 amended + 7 added rows; ADR 0004/0008 amended rows mapped; "0 missing, 27 present-provisional" | §12 |
| §13: A-R24 acceptance recorded; Q-11 default updated (count both) | §13 |
| §14: source-state bullet now "after round 2"; contradictions 5 (view rate 2/s vs 4/s in §1.7 vs §2.5) and 6 (`ExternalFenceVerified` home) added | §14 |
| KA-3 no longer held; fixture exposes `admission_horizon()` | §1 |

### Row counts (all 164 rows)

| Class | Count | Ids |
|---|---|---|
| unit | 147 | as §2 table |
| sim | 14 | M7A-85, 93, 97, 98, 102, 117, 131..136, 139, 163 |
| campaign | 3 | M7A-58, 130, 137 |
| held | 0 | — |

Provisional (pending critic-kernel-a-2): M7A-138..164 plus re-worded 24, 45, 60, 66..68, 80,
101, 103..106, 131 = 41 rows.

### Evidence

- Id check (grep/awk): 164 row definitions, 0 duplicate ids, sequence 01..164 with no gap;
  unit 147 / sim 14 / campaign 3 = 164. No `⏸` marker remains on a row. `file` reports UTF-8.
- Every §8 row cites the round-2 design section it proves (§1.2, §1.7, §2.2, §2.3, §2.4, §3.3,
  §4.1, §4.2) and, where one exists, the amended/added ADR verification row by its title.
- M7A-143's numbers were derived from the §1.7 formula by hand: `local_horizon 2899`,
  `a_max = floor(2880/1.0005) = 2878`, `utc_horizon = min(2878, 2000) = 2000`,
  `past_horizon ClockSampleStale`; twin with cap 4000 gives 2878 / `Expired`.

### Assumptions and deviations

- Re-worded rows keep their ids rather than moving into §8; §8's preamble lists them so the critic
  can find every provisional row from one place.
- M7A-24 now contradicts my first-pass wording (zero CASes in Unbounded). The amended ADR row is
  the authority: one id with a valid sample; zero without (M7A-148).
- Fact names in §8 (`DispatchDroppedByFreeze`, `StaleAuthorityAnswer`, `AcquireWithheld`,
  `ReplyWithheld`, `ReplySuppressedAfterTimeout`, `FenceWhileBlocked`, `PublishRefusedBlocked`,
  `AlreadyBlocked`, `StaleTimer`, `ExternalFenceRejected{field}`) are the design's where it names
  one; `ExternalFenceRejected{field}` and `RenewalWithheld` are mine (Q-6 accepted: emit a fact).
- M7A-163 needs a `ControlOp` member for "never completes" (Q-8, `DropNext`); until foundation
  adds it the row is `unavailable`.

### Questions (each with a default)

None new. Contradiction 5 (view push rate 2/s vs 4/s) is a one-word fix in `design.md` §1.7 or
§2.5; default: §2.5's four (two samples + two renewals per second at the defaults) and M7A-137
records the observed rate either way.

### Risks

- **R7 — 41 provisional rows.** If critic-kernel-a-2 sustains a finding against §3.3's `Freeze`
  split or §4.2's `awaiting_reply`, up to 13 rows re-word. None would be removed; ids stay.
- **R8 — seam types not in `rdb-core`.** `AuthorityDecision.authority_seq`,
  `AuthorityView.{authority_seq, valid_through_tick, past_horizon}`, `ExternalFenceVerified`,
  `BlockReason` are design-only today (architect handoff §5). Seventeen rows compile only after
  foundation seeds them (§11).
- **R9 — M7A-158 depends on kernel-b's `BlockPartition` shape** (`diverged` inside the reason or
  beside it). The row reads either; the fixture builder must be written once the shape lands.

### Recommended next role

**critic-kernel-a-2** on plan §8 and the re-worded rows (twins, K-A closure per row, ADR row
coverage against the 785e41b tables, contradiction 5). Then developer on §3.2–§3.4, §4.1–§4.4
and §8.1–§8.3 (no seam dependency beyond `authority_seq`/`AuthorityView` fields).

---

## Round 3 (A-R26) — critic-kernel-a findings T-A-01..15 applied

### Outcome

**COMPLETED.** `docs/testing/test-plan-m7-kernel-a.md` edited in place. Ten rows added
(`M7A-165..M7A-174`), 24 existing rows re-worded, two new §1 architecture requirements (KA-8,
KA-9), one new section (§15 drift table), one new open question (Q-12). **No id was renumbered
and no row was removed.** The plan is now 174 rows: unit 156, sim 15, campaign 3.

Lead ruling **A-R26** accepted the critic's five defaults; they were applied as written and are
not re-litigated here. Architect round 4 (the K-A-57 reorder and the `same_lineage_as` conjunct
it forced) was folded in at the coordinator's instruction — it produced M7A-174.

### Finding → rows changed → evidence

| Finding | What it said | Rows changed | Evidence |
|---|---|---|---|
| **T-A-01** | Nine ADR verification rows landed at `3eec5e9` had no M7A row, yet §12 claimed "0 missing" | **New §8.7: M7A-165..M7A-173**, one per ADR row, each quoting the row title and its K-A id; §12's 0004 and 0007 lines rewritten | `git diff 785e41b..3eec5e9 -- docs/ADRs/rdb/` lists exactly six new 0007 rows and three new 0004 rows; each now appears in §12's map |
| **T-A-02** | Six rows asserted the fence view at `valid_through_tick == now`; round 3 makes it `fence_tick − 1` with `past_horizon = reason` | M7A-66 (rewritten: submits **at** the fence tick and at `t+1`, plus a partition-scoped `GenerationChanged` variant), 68, 142, 145, 146, 163 | design §1.7 "`fence()` writes the view directly, never calling `admission_horizon`"; K-A-49 |
| **T-A-03** | M7A-99/100/102 asserted `Freeze{..}` as an **effect**; round 3 makes the self-freeze a state write. M7A-99 also named the wrong reply | M7A-99 (now `Answer(Err(UNKNOWN_OUTCOME))` per drained waiter, **no `Freeze` effect**, state `mode == Frozen{AuthorityLost(Expired)}`), 100, 102 (second sub-run: a deadline firing in `Frozen{AuthorityLost}` leaves the mode unchanged) | K-A-54; design §4.2 deadline row, `mode = Frozen{UnresolvedTransaction}` **iff `Serving`** |
| **T-A-04** | M7A-158 used `Blocked{RecoveryBlocked}` and M7A-160 used `r.selected_cutoff` — both deleted identifiers | M7A-158 (`PartitionMode::Blocked{reason} ⇒ PubMode::Blocked{reason}`, byte-equal to `r.mode`'s), M7A-160 (`r.selected.cutoff_seq` / `.cutoff_digest`) | K-A-54, K-A-56 |
| **T-A-05** | No row checked the **kept cause** or `unresolved` across a batch completion | **M7A-168** (three orderings A/B/C, whole `mode` compared field by field), M7A-139 (post-completion mode literal, and which error code the kept cause picks), M7A-161 (`unresolved` cleared, cause kept) | K-A-46; design §3.3 `BatchCompleted` and `Freeze` rows |
| **T-A-06** | M7A-148's four `AcquireWithheld` reasons collapse to two under K-A-50 | M7A-148 (rewritten: exactly **two** reasons, `NoSample` and `Stale`, with `Fact(SampleRejected{which})` and `clock.sample == None` on the reject paths), M7A-40, 41, 42 (each asserts the retraction; M7A-42 also asserts no CAS before a fresh sample) | K-A-50; design §2.4 `Unheld\|Fenced / Clock(s)` reject row |
| **T-A-07** | No P1 row evaluated the digest conjunct; §1 had no `ReplicationView` fake | **KA-8** added to §1 (`digest_at` defaults to `Match`, `qualifies_now` to false); **M7A-173** varies it (`Differs{stored}`, `NotRetained`, `Match`); M7A-93 asserts `Fact(PublishPredicateFalse{Qualification})`; KA-8 Dep added to M7A-91, 92, 94, 96, 103, 106, 152..155, 170 | K-A-51 / A-R25 Q1: `may_publish` = lineage+config **and** `qualifies_now` **and** `digest_at == Match` |
| **T-A-08** | M7A-116 asserted a blanket `RecoveredApplied` | M7A-116 (takes a `RetainedStatusMap`, three identities, two traces, asserts `StatusExpired` **not** produced), **M7A-172** (the fold in isolation, four sequences × three maps), M7A-135 (retirement ⇒ `Unknown`, not `StatusExpired`; loss-accepting variant added) | K-A-52; design §4.4 `fold_recovered` |
| **T-A-09** | M7A-69(b)/71 predate ADR-0004 row 8's `AdmissionState.reason` passthrough | M7A-69 gains (c) `reason: Some(DIVERGENCE_REQUIRES_OPERATOR)` ⇒ that code synchronously, zero effects; M7A-71 asserts no `DenyReason` maps to it; §11 lists `ErrorKind::DivergenceRequiresOperator` as a **dependency** (kernel-b B-R31 item 18), not a finding | ADR 0004 §3 row 8 as amended at `3eec5e9` |
| **T-A-10 / K-A-57** | The `!may_publish` refusal row shadowed the `Blocked \| Admit` row | M7A-159 and M7A-106(d) unchanged in substance, both now cite the reorder; **M7A-174** added for the `same_lineage_as(cand.authority)` conjunct the reorder forced | architect round 4. Without the conjunct a blocked partition whose lineage moved would emit `Fact(PublishRefusedBlocked)` instead of quarantining, falsifying ADR-0007's sticky-`Blocked` row, whose second trace is exactly that case |
| **T-A-11** | No record of drift against landed C0 | **New §15** (eight rows) and **KA-9** (design §1.4 `Outcome` on state → `TxnStatus` at the reply boundary, stated once in §1 per A-R26 Q-2); M7A-118 re-worded to the landed `ReplyEffect::Status{identity, status}`; M7A-124 takes its lateness from the sim scheduler, not a `PlanCas` delay member | landed C0 at `8a23b1d` |
| **T-A-12** | Stale §12/§14/Dep columns | All 41 `(prov.)` and `closed (prov.)` markers dropped (§8.1–§8.6 were cleared by round 3); §12 recounts 0004 as 15+3 and 0007 as +6 with two more amended; §12's gate map gains 11 round-3/4 criterion rows; §14 header, §2 counts, the numbering note and the status line updated | no `prov.` left in the file outside §8.7's own `(provisional)` markers |
| **T-A-13** | M7A-143's derivation superseded by the inequality form | M7A-143 cites the inequality and adds the `2001` exact-division twin: the inequality gives `a_max 1999`, a closed form would give 2000 — the row asserts 1999 | design §1.7 round 3 |
| **T-A-14** | M7A-145/137 did not count pushes **per served partition** after fan-out | M7A-145 renamed `authority_view_republished_at_five_points_fans_out_per_served_partition`, `served = {p1, p2}`, per-served-partition assertions; §14 contradiction 5 settled at `3 × p × 4` | K-A-53 / A-R25 Q4 |
| **T-A-15** | M7A-101's path list lacked the K-A-45 paths | M7A-101 now has six paths; (e) and (f) are candidates accepted in `Frozen{AuthorityLost}` and in `Blocked`, both asserting **0** `Reply` effects | K-A-45 |

### Drift table pointer

**Plan §15**, "Drift: design round 3 versus landed C0 at `8a23b1d`" — eight rows, each saying
which side the test rows compile against. Row 7 is the one that needs foundation:
`ControlTime` → `ClockSample` conversion, **foundation contract request 9 from kernel-a**, owned
by I1 in `rdb-sim`, `valid = ct.bound_established` and nothing else, samples delivered **even
when the bound is not established**, and **no staleness filtering at the seam**. The clock rows
(M7A-38..46, 143, 146, 148, 165) depend on it. It is also plan §13 Q-12.

### Counts — proved mechanically, not by eye

The commands are recorded in plan §14, "How the counts were checked". Run from the repo root
with `F=docs/testing/test-plan-m7-kernel-a.md`. §11's dependency table is excluded because its
rows also begin with an id.

```sh
rows() { awk '/^## 11\./{s=1} /^## 12\./{s=0} !s' "$F" | grep -E '^\| M7A-[0-9]+ \|'; }
rows | wc -l                                       # 174
rows | grep -oE '^\| M7A-[0-9]+' | sort | uniq -d  # empty
rows | sed 's/[^0-9]*M7A-//;s/ .*//' | sort -n \
  | awk 'NR==1{p=$1+0;next}{if($1+0!=p+1)print "GAP "p" -> "$1;p=$1+0}END{print "max "p}'
                                                   # no GAP line; max 174
rows | grep -oE '\| (unit|sim|campaign) \|' | sort | uniq -c
                                                   # 156 unit, 15 sim, 3 campaign
```

Observed 2026-09-20: **174 rows, `M7A-01..M7A-174`, no duplicate, no gap; unit 156 + sim 15 +
campaign 3 = 174.** Matches plan §2 and §12.

One extra check, because the new rows quote `Blocked | Admit`-style design rows: every row line
must have exactly **eight unescaped** `|`. Nine rows carry a `\|` inside a code span (M7A-19, 22,
30, 106, 148, 159, 161, 165, and M7A-174 with two). Escaping them was a real fix — four of those
rows previously rendered with a spurious eighth column, three of them rows this round wrote.

By package the 174 split: A1 61 (§3) · control fake / ADR 0008 12 (§6) · T1 29 (§4) · P1 28 (§5) ·
cross 7 (§7) · round 2 27 (§8.1–§8.6) · round 3 10 (§8.7: A1 2, T1 2, P1 6).

### Questions for the lead (each with a default)

- **Q-12 — who converts `ControlTime` to `ClockSample`, and on what rule?**
  **Default (what the rows assume, per the coordinator's round-4 note):** I1 in `rdb-sim` builds
  it; `valid = ct.bound_established` and nothing else; samples delivered even when the bound is
  not established; no staleness filtering at the seam — staleness is A1's judgement against
  `max_sample_age_ticks`. Recorded in plan §13 and §15 row 7.
- **Q-13 — is `StatusExpired` reachable in M7 at all?** The rows now assert it is **never
  produced** (M7A-116, M7A-135, M7A-172), which is a stronger claim than "unreachable today".
  **Default:** keep the absence assertion. If a later retention policy discards below
  `retained_through`, those three rows go red on purpose and are re-worded then — that is the
  intended alarm, not a flake.
- **Q-14 — does `ErrorKind::DivergenceRequiresOperator` belong to kernel-b's B-R31 item 18 or to
  foundation's C0?** **Default:** kernel-b, as the critic ruled. M7A-69(c) reports `unavailable`
  until it lands and is listed that way in §11. If foundation takes it, only the §11 line moves.

### Risks

- **R10 — the ten §8.7 rows are unexecuted.** They are written against design rounds 3 and 4 and
  nothing has run them. Their input literals (tick numbers, map fields, mode literals) are the
  first thing to re-read when one goes red; the assertion is not to be lowered.
- **R11 — M7A-174 guards a conjunct that reads as redundant.** That is the architect's own stated
  residual risk. Delete `same_lineage_as(cand.authority)` from the `Blocked \| Admit` row because
  it "cannot matter" and M7A-174 turns red. The row's Assertion says so in words, so a reader who
  hits it knows what it is defending.
- **R12 — KA-8 is our own fake and its defaults are load-bearing.** `digest_at` defaults to
  `Match`, `qualifies_now` to false. Eleven publish rows rested on an unstated default before
  round 3; they now rest on a stated one. Change either default and eleven rows move silently.
  KA-8 states the defaults; M7A-173 is the row that varies them.
- **R13 — §15 records drift, it does not resolve it.** Eight design/C0 disagreements stand. Four
  (rows 1, 2, 4, 7) touch types the rows compile against; until foundation settles them the
  affected rows report `unavailable` (§11) and never green.
- **R7 is retired**: the 41 provisional rows were cleared by critic-kernel-a round 3. R8 and R9
  stand unchanged.

### Recommended next role

**Developer**, on §3.2–§3.4, §4.1–§4.4 and §8.1–§8.3 — no seam dependency beyond
`authority_seq`/`AuthorityView` fields, matching the critic's "may start now" list. §8.7 waits on
nothing of ours but is unexecuted, so run it early and R10 shrinks. A **critic-kernel-a round 4**
on the plan is optional; it would earn its cost only on §8.7 and §15, the two parts no reviewer
has yet seen.

### 13. Lead rulings on the round-3 questions — round 3 accepted, committed `6aad83f`

Round 3 was accepted and committed as **`6aad83f`**. The lead re-verified the counts
independently rather than taking them from the report above: **174 unique ids, contiguous
`1..174`, no duplicate**; the sixteen ids that look duplicated to a naive grep are §11
dependency-table entries in a different table; all **173 main-table rows carry exactly eight
unescaped pipes** and the escapes added this round are correct. The one outlier is the
three-column §11 row for M7A-128 — a different table shape, also fine.

All three open questions are now closed. Q-12, Q-13 and Q-14 keep their ids; the "Default"
recorded above became the ruling in each case, so no row changes.

| # | Ruling | Reason as given by the lead |
|---|---|---|
| **Q-12** — `ControlTime` → `ClockSample` | **Default adopted.** I1 builds the sample; `valid = ct.bound_established`; it is delivered **even when unbound**; the seam does **no staleness filtering**. Stays **foundation contract request 9**, and it is **code in `rdb-sim`, not a shape in `rdb-core`** | **M7A-43 is the reason.** That row asserts a stale sample denies admission *without fencing*. If the seam filtered staleness the kernel would never see a stale sample and M7A-43 could not fire. A seam that silently drops the input a row is written to observe is not a seam, it is a second policy |
| **Q-13** — the `StatusExpired`-never-produced assertion | **Keep it.** M7A-116, M7A-135 and M7A-172 continue to assert the variant is never produced | A negative assertion about a variant the fold must never emit is the only thing standing between that variant and a future contributor who finds it in the enum and reaches for it |
| **Q-14** — `ErrorKind::DivergenceRequiresOperator` | **Sits with kernel-b**, as recorded. Already **kernel-b B-R31 item 18**; it goes to foundation in the **second foundation round**. M7A-69's dependency keeps pointing at it (§11) | — |

**R11 — resolved, keep M7A-174.** The architect built that guard because its own K-A-57 reorder
would otherwise have silently undone K-A-48. A guard that is redundant today is precisely the
guard that stops being redundant without anyone noticing. One test row is a cheap price for a
shadowing bug that produces no error. The risk is closed; the row stays exactly as written.

**R10 — stands, not closed.** The ten §8.7 rows are written against design text nothing has
executed. That is unavoidable at this stage, but it carries an instruction for the next role:
**those ten rows get the developer's first scrutiny, not the last.** R12 and R13 also stand.

No plan edit follows from any of these rulings — every row was already written against the
adopted default. The plan as committed at `6aad83f` is the current one.

---

## Round 4 — critic PASS_WITH_RISKS on the round-3 diff (TD-01..TD-11)

**Outcome: all eight MATERIAL findings corrected, all three advisories taken.** Row count
unchanged at 174, ids still contiguous `M7A-01..M7A-174`, nothing renumbered and nothing removed.
One row (M7A-172) gained an assertion rather than losing a sub-case; one row (M7A-115) gained a
second half. The plan is `docs/testing/test-plan-m7-kernel-a.md`; I touched nothing else.

### The basis, established by re-reading

The coordinator's instruction was to fix the staleness by re-reading, not by waiting for the
mechanical check, and not by copying the sibling team's basis. I did that:

- `git log -1 -- crates/rdb-core/src/contracts` ⇒ **`ec610f4`**, which is also the newest commit
  touching `crates/` at all. Chain: `8a23b1d` (C0) → `6893442` (foundation correction round 1,
  K-F-01..38 and F-R6..F-R12) → `ec610f4` (K-F-39).
- The plan's round-3 basis was `8a23b1d` — two crate commits stale. Kernel-b's was `ab85c14`,
  which does not contain `contracts/authority.rs` at all; I did not copy it.
- Every claim below was checked by opening the named file at that commit, not taken from the
  critic's report.

`scripts/drift-check.sh` landed while I was working. The marker `<!-- drift-basis: ... -->` is now
line 3 of the plan, added **after** the re-read, not before it. Verified:
`bash scripts/drift-check.sh docs/testing/test-plan-m7-kernel-a.md` ⇒ `OK (ec610f4)`, `gate: drift OK`.
One snag worth passing on to the other three authors: the stage counts occurrences of the literal
string `drift-basis:`, so a plan that *discusses* its marker in prose fails with "2 basis markers".
My §15 paragraph refers to it without repeating the string.

### Finding → rows changed → evidence

| Finding | Severity | What changed | Evidence it was wrong |
|---|---|---|---|
| TD-07 | MATERIAL | §11 rewritten; §15 re-pinned, rows 4–8 rewritten, rows 9–11 added; §12 gained a drift-basis criterion; §14 contradiction 4 closed; plan header and §14 source-state line re-pinned | `contracts/authority.rs` present at `ec610f4` with `Checkpoint`(5), `Lineage`, `DenyReason`(15), `Verdict`, `AuthorityDecision`, `AuthorityView`, `EvidenceRef`, `BlockReason`, `PartitionMode`; `ControlOp` 8 variants; `NodeLifecycle`, `ExternalFenceVerified`(6 fields), `AdoptAuthority` all present |
| TD-01 | MATERIAL | M7A-161(b) `PROTECTION_PAUSED` → `LEASE_EXPIRED`; "one fact vs (a)" restated; new §13 **Q-15** records the §3.2-vs-§3.4 conflict with that default | `design.md` §3.4 maps `AuthorityLost(r)` through the reason table and calls the flat code the round-2 defect; M7A-139 already asserted `LEASE_EXPIRED` for the same mode |
| TD-02 | MATERIAL | M7A-101 (e) and (f) `0` → **1** reply each; (f)'s ordering pinned to deadline-first; reverse ordering explicitly handed to M7A-116/135 | design §4.2 `pending \| PostApplyDeadline{s}` emits `Status(Unknown)` **and** `Reply(Unknown)` whatever the mode; M7A-102 asserts the same pair |
| TD-03 | MATERIAL | M7A-166 Input now lists **all nine** ADR 0007 §3 triggers by the `DenyReason` each produces, seven Node / two Partition; `ConfigVersionChanged` removed; `LocalStorageFenced` moved to Partition | ADR 0007 §3's table has nine rows with an explicit Scope column; `DenyReason`'s 15 variants contain no `ConfigVersionChanged` |
| TD-04 | MATERIAL | M7A-166 drops `Some(..)`/`None` on `past_horizon`; negative half is now "no fence produces a `past_horizon` other than its own reason" | `AuthorityView.past_horizon: DenyReason`, a bare field |
| TD-05 | MATERIAL | M7A-172's `discarded_from: None` sub-case now asserts **`StatusExpired`** at seq 11/12; the never-produced claim narrowed to "no map M7 can build reaches the `else` arm" | K-A-52's three-way rule: with `uncertain` false and no `discarded_from`, a seq above `retained_through` falls to the `else` arm |
| TD-06 | MATERIAL | M7A-135's `RetireGeneration` leg `Unknown` → **`StatusExpired`**; never-produced claim narrowed to the `fold_recovered` paths | `design.md` line 1889; ADR 0004 "Retention boundary" row; M7A-113 case 3 and M7A-114 |
| TD-08 | MATERIAL | §15 row 8 rewritten: the recorded drift did not exist; the real one is the grant-read shorthand and the rows are M7A-16..M7A-21 | `ReadOutcome::Found{revision, value: Bytes}` at `control.rs:230` **is** the landed shape; M7A-107..111 never assert those fields |
| TD-09 | ADVISORY — **taken** | §15 row 6 now names both landed enums and says the KA-4 log line carries `authority::Checkpoint`, which is what Q-42 filters | `Checkpoint` has 5 variants incl. `StorageDispatch`; `trace::AuthorityGate` has 4 and no `OutboxDispatch` |
| TD-10 | ADVISORY — **taken** | KA-9 gained a paragraph explaining why `TxnStatus::Unresolved{seq}` is unreachable in M7, and M7A-115 gained a wire half that **asserts** it | the round-3 text left it as a bare sentence, which reads as an omission rather than a claim |
| TD-11 | ADVISORY — **taken** | §12's ADR 0004 map now uses row titles instead of indices | the three rows added at `3eec5e9` were inserted mid-table, so `9→74` named "Recovery folds status by sequence" while meaning "Digest survives a legitimate retry" |

### What the re-read found that the critic did not

Three things, all now in §15. They matter because they would each have made a row red on the day
it ran, and none was in the round-4 report:

1. **`ReplyEffect::Read` is a shape drift, not an absence.** It landed (F-R7 was right) as
   `{identity, outcome: ReadServiceOutcome, value: Option<(Version, Digest)>}`, not the
   `{corr, snapshot}` M7A-107/108/111 were written against. §13 Q-9 is half closed, half re-opened
   as **Q-16**. `SnapshotId::at` is still absent, so those rows stay in §11 — on that alone.
2. **`FencingProof` was held on the wrong owner.** §11 held M7A-51..57 and M7A-149..151 on
   "C0 `FencingProof`". Design §1.7/§2.6 make it **A1's own emitted struct** — the package under
   test — so it was never foundation's to deliver. `ExternalFenceVerified` did land with all six
   K-A-37 fields, and `EvidenceRef` is the input handle, a different thing. Dep cells on M7A-51 and
   M7A-149 corrected.
3. **`ControlTime::is_stale` settles §14 contradiction 4.** Strict `>`, so M7A-46's `age == 2000`
   is not stale and M7A-43's 2001 is — the rows were already right. Two residues recorded: the
   landed parameter is `max_sample_age_millis`, not ticks, and the function also rejects a
   future-stamped sample, which no row exercises.

Foundation contract request 9 is restated because of (3): `ControlTime` has **four** fields, and
`ClockSample.at` must come from `ct.sampled_at`, **not** from `now`. The round-3 wording left `at`
unspecified; a seam that stamped the sample at its delivery tick would give every sample age zero
and make M7A-43 unreachable. That clause is now in §13 Q-12 and §15 row 7.

### Not mine this round

- **M7A-169's `Err`-completion ambiguity.** Routed to the architect by the coordinator. Untouched.
- **`ErrorKind::DivergenceRequiresOperator`.** Re-confirmed absent (18 variants, none of them it).
  Still kernel-b **B-R31 item 18**, still a dependency and not a finding.
- **M7A-52..M7A-57's bare `C0` Dep cells.** Left as they are; the grant read they need did land,
  but the cells name C0 generally and re-wording them is churn, not a correction.

### New questions — the default is what the rows are written against

| # | Question | Default |
|---|---|---|
| Q-15 | `design.md` §3.2 step 7's flat `PROTECTION_PAUSED` for `mode != Open` contradicts §3.4's reason passthrough. Which governs? | **§3.4.** `Frozen{AuthorityLost(Expired)}` answers `LEASE_EXPIRED`; `PROTECTION_PAUSED` survives for modes with no carried reason. If the architect rules the other way it is one fact in two rows (M7A-139, M7A-161(b)), not a re-plan |
| Q-16 | Do the read rows assert the landed `ReplyEffect::Read` shape, or hold for a snapshot identity? | **Both, split.** `corr`→`identity` and the served/barrier distinction→`outcome` are assertable now; snapshot identity is not, so those rows hold on `SnapshotId::at`. Do **not** substitute `value`'s version-and-digest pair for a snapshot identity — that lowers the assertion |

Numbering resumes at 15 deliberately: Q-13 and Q-14 were handoff questions the lead has already
ruled on, so those numbers are spent.

### Risks

- **R10 stands.** The ten §8.7 rows are written against design text nothing has executed. Round 4
  is the second round in a row in which that text produced defects — three of ten last round
  (TD-03/TD-04/TD-05 all landed in §8.7 rows). The prediction keeps being right. Those ten rows
  get the developer's **first** scrutiny.
- **R12, R13 stand**, unchanged.
- **R14 — new, low.** §15 now asserts eleven specific facts about landed code. Each is true at
  `ec610f4` and each will rot. The drift stage catches the *basis*, not the *content*: a plan can
  pass the gate with a fresh commit hash and a table that was never re-derived. The mitigation is
  the re-read discipline paragraph at the end of §15, which is still a convention. If that is worth
  hardening, the check would have to diff the named symbols, not the commit.
- **R15 — new, low.** M7A-172 now asserts an arm (`StatusExpired` from `fold_recovered`) that no
  kernel path can reach. If A1/T1/P1 land a `fold_recovered` that simply cannot be called with such
  a map — for instance because `RetainedStatusMap` makes it unconstructable — the sub-case becomes
  untestable rather than red. That would be good news about the type and should be recorded as a
  row deletion with a reason, not silently dropped.

### Verification run

| Check | Command | Result |
|---|---|---|
| Drift basis | `bash scripts/drift-check.sh docs/testing/test-plan-m7-kernel-a.md` | `OK (ec610f4)`; `gate: drift OK` |
| Exactly 8 unescaped pipes per row | awk, counting total `\|` minus literal `\\\|` by substring scan | 174 rows at 8; 27 three-column lines at 4; zero bad |
| Ids contiguous, none duplicated | `grep -o '^\| M7A-[0-9]\{2,3\} ' \| sed \| sort -nu` | `unique=174`, no gap. The two apparent duplicates are §11's single-id lines for M7A-128 and M7A-130 |
| Class totals match §2 | awk over the class column | unit 156 · sim 15 · campaign 3 = 174 |

Note on the pipe count: `gsub` in this environment mis-parses an escaped pipe inside a regex
literal and reports nonsense. The counts above come from an `index()`-based substring scan, which
is exact. `grep -P` is unavailable and `python3` is not installed.

### Next role

The developer. Nothing in this round changed a fixture, a budget or a gate line, so the
`CARGO_TARGET_DIR=.rtargets/kernel-a scripts/gate.sh test -p rdb-sim --test authority --test transaction --test publication`
line is the same as it was at `6aad83f`. What did change is that thirty-nine rows left §11 and are
now claimed to compile against types in the crate. The first run will prove or disprove that in
one go, and it is the cheapest possible check on this whole round.

---

## Round 5 — critic round 3 returned FAIL (TD-12..TD-19, advisory TD-20, risk R15)

### Outcome

All eight findings closed. The blocker was real and was the most important thing in the round for
the reason the critic gave: **M7A-32's whole assertion was `count of Control(Get{family}) == 0`,
and `Get{family}` cannot be constructed**, so the count was zero in every possible run — including
a kernel that reloads on every watch event, which is exactly the bug ADR-rdb-0008 §7 item 4 exists
to catch. §12 reported that item covered. A test that cannot fail is worse than a missing one,
because the missing one is visible.

Four sibling rows named the same shape and were ordinary compile errors. Fixing the effect name
alone would have left M7A-32 asserting nothing useful, so it was re-derived rather than renamed.

### Finding → what changed → the source line that justifies it

| # | Change | Source |
|---|---|---|
| **TD-12** (blocker) | **M7A-32 re-derived, not renamed.** Two arms on one kernel: arm 1 is 200 `EmitWatch` + 50 `EmitProgress` with **no** termination and asserts `count of Control(Reload{..}) == 0`; arm 2 delivers one `TerminateWatch{RevisionCompacted}` and asserts **exactly 1**. Zero is the right number in arm 1 because `Reload` is the sanctioned answer to a **gap** and the only thing that declares a gap is `WatchTermination::is_gap`; rEtcd's stream does not skip silently. Arm 2 is a positive control so the counter is proven live *inside the test* — without it, a `Reload` count of zero is again unfalsifiable by inspection | `ControlEffect` four variants, `control.rs:329-363`; `ControlKey` has no family member, `:25-44`; `Reload` doc "Team kernel-a's `ReadFamily { prefix }` binds to this", `:354-358`; `WatchTermination::is_gap`; ADR 0008 §7 item 4 verbatim and §4 "a stream that has not terminated has not silently skipped an event" |
| TD-12 | M7A-28, M7A-30, M7A-31, M7A-123, M7A-126 now name `Reload{prefix}`. **M7A-31 and M7A-129 reconciled**: round 4 had M7A-31 assert "zero `Get{family}`; zero `Reload`" — first conjunct unconstructable, second duplicating M7A-129, which left M7A-129 with nothing of its own. M7A-31 now owns the backoff shape and the cap; M7A-129 owns the count and gains a `ResourceExhaustedResumable` positive control. `Get{grant}` in M7A-30/M7A-57 unchanged — `Grant(NodeId)` is a real `ControlKey` | as above; `is_gap()` is false for `ResourceExhaustedFatal` |
| TD-12 | **KA-2 gained the effect vocabulary**, which it never had: it listed only the scripting `ControlOp`s, so the plan had no statement of what a row may assert A1 *emitted*. That omission is why `Get{family}` survived four rounds. §15 gains row 12 recording the `ReadFamily → Reload` binding | `control.rs:329-363`; K-F-19 rationale at `:46-52` |
| TD-13 | §14 contradiction 1 rewritten in contradiction 4's form: settled, variant landed, residual is the **shape**, pointer to §15 row 4 and Q-16. The `event.rs:166` citation withdrawn — that line is now inside `EventKind`'s doc for `ExternalFenceVerified` | `ReplyEffect::Read` at `event.rs:218` |
| TD-14 | §11 membership corrected **in both directions on one line**. M7A-105 joined the `SnapshotId::at` hold (it asserts `snapshot SnapshotId::at(g, 5)`); M7A-111 and M7A-118 left it and run today, with a cleared-table entry saying why. Their `Dep` cells moved to `none` and `KA-9`. Q-16's row list corrected: it named M7A-111, which asserts `Failed`, not a read | `ReplyEffect::Failed` `event.rs:205-212`; `Status{identity, status}` `:198-204`; `grep -rn SnapshotId crates/` ⇒ zero |
| TD-15 | KA-2 spells `PlanCas{node, outcome}` and says single-node shorthand is fine but a two-node race must name the node. **M7A-127 rewritten** as an actual two-node race: `PlanCas{node: n2, outcome: Conflict{..}}` forces n2's report while n1's CAS proceeds, with a twin that swaps them | `sim/control.rs:61-66` and its doc, K-F-14: "one racer's report can be forced while the other's proceeds" |
| TD-16 | KA-1 lists `ControlTime{estimate, error_millis, bound_established, sampled_at}` — four fields — and points at Q-12 for the conversion rule and why `at = sampled_at` is load-bearing | `time.rs:86-100`; `sampled_at` doc: "An estimate established long ago is not an estimate" |
| TD-17 | **M7A-05 rewritten**, plus **three siblings the critic did not find** — M7A-18, M7A-34, M7A-35 — all of which handed the kernel a record body on the watch path. Each now delivers `ControlChange{key, revision}` with the body **behind** the revision, where the following `Get` finds it. Assertions unchanged and stated as *stronger*: A1 cannot see the new owner even if it wanted to | `ControlChange{key, revision}` `control.rs:296-307`; K-F-13 / ADR 0008 §4: "A watch invalidates a cache; it never delivers the record" |
| TD-18 | M7A-28 and M7A-123 resume at `from: snapshot_revision`. M7A-28 also split into its two real steps (`Reload` at the termination, `Watch` at the `FamilySnapshot`, since `snapshot_revision` does not exist before the snapshot) and now **places a change at exactly `snapshot_revision + 1`** so the dropped-revision bug is red rather than invisible | see the disagreement below |
| TD-19 | **§13 Q-17 added** and marked routed-to-the-architect; M7A-169's twin cell now says it is written against an open question, names the `RetryRule::QueryStatus` tension, and its `Dep` column carries "§13 Q-17 open". Not settled — per instruction | `errors.rs:45-66`: "Query status with the *same* identity. Never generate a fresh request id" |
| TD-20 (advisory) | M7A-121 disambiguated: `t` indexes the five terminations and **is not a key**; the op ends *all* of a node's watches and neither type carries a key. Full landed spellings written out | `ControlOp::TerminateWatch{node, termination}`; `ControlEvent::WatchTerminated{prefix, from, termination}` |
| **R15** | Converted to a standing condition as §15 **row 14**: when F1 lands `RetainedStatusMap`, re-derive M7A-172's sub-case first; if `discarded_from` is `Revision` rather than `Option<Revision>`, the `None` arm goes and the sub-case must be **deleted with a stated reason**, because it is the plan's only construction reaching K-A-52's `else` arm | `grep -rn RetainedStatusMap crates/` ⇒ zero hits, confirmed |

### The vacuous-assertion sweep — result

Swept every "count of X == 0" and "no X is emitted" in the plan against constructability of X.

**Two in the M7A-32 class** (the assertion silently passes; no compile error warns anyone):

1. **M7A-32** — the blocker itself.
2. **M7A-31's `zero Get{family}` conjunct** — same shape, same silence. It sat beside a real
   conjunct (`zero Reload`), which is what hid it.

**One adjacent, different class, fixed anyway:** M7A-71's negative half read "the row asserts no
`DenyReason` maps to `DIVERGENCE_REQUIRES_OPERATOR`". `ErrorKind` has 18 variants at `ec610f4` and
this is not one, so the comparison cannot be *written* — a compile error, not a silent pass. But it
read as a separate assertion of an absent identifier, so it is restated as a property of the
fifteen exhaustive arms, which is assertable today and carries the same meaning.

**Two inspected and cleared, with reasoning, because they look like the class and are not:**
M7A-99's "no `Freeze` effect at all" and M7A-102's "no `Freeze` effect". `Freeze` is not in P1's
effect enum — K-A-54 made a self-freeze a state write — so in a landed kernel the absence check is
unwritable. They are not vacuous because each pairs it with the **positive state assertion**
(`mode == Frozen{..}`) that carries the row's content and can fail. Left as written; the negative
half is a design-regression guard against re-adding the effect K-A-54 removed.

**Count to report: 2 in the blocker's class, 1 adjacent fixed, 2 inspected and cleared.**

Also swept, all constructible and left alone: M7A-23, M7A-35, M7A-44, M7A-54, M7A-62, M7A-72,
M7A-85, M7A-97, M7A-101(b), M7A-104, M7A-115 (`TxnStatus::Unresolved` is landed — the row is the
guard and goes red if a later milestone makes it reachable), M7A-116, M7A-129, M7A-131, M7A-138,
M7A-140, M7A-163, M7A-166's negative half, M7A-167, M7A-174, Q-45.

### Where I disagree with the critic, argued from source

**TD-18's "where I cannot settle it" is settleable, and the answer confirms the finding.** The
critic wrote: "there is no implementation of the watch seam in `crates/rdb-sim` to check", and
raised the exclusivity as a seam question. **There is one**, at `crates/rdb-sim/src/sim/control.rs`
lines 218-262, and it honours exclusivity:

- `:233` — `EmitWatch` selects changes with `*revision > watch.cursor`, strictly greater;
- `:367` — `ControlEffect::Watch{prefix, from}` sets `cursor: *from`.

So `from` is exclusive in the contract doc **and** in foundation's fake, and `from: snapshot_revision`
is the closed loop. TD-18's conclusion stands and is stronger than the critic claimed: it is fixed
**and confirmed**, not fixed and assumed, and nothing is owed at the seam. Recorded as §15 row 13.
No question was raised with the architect on it, because there is nothing left to ask.

Nothing else in the eight findings is disputed. The blocker, TD-13, TD-14, TD-15, TD-16, TD-17,
TD-19 and TD-20 are all correct against source as written, and TD-17's spread is wider than the
critic's grep showed — four rows, not one.

### The systemic finding, and what changed because of it

The critic's diagnosis is right and is worth stating plainly: **round 4's §15 re-read was genuine —
it re-derived all eleven rows and every one was correct — and it was scoped to §15.** The stale
claims that survived lived in §1 (KA-1's three fields, KA-2's node-less `PlanCas`), §11 (two rows
held on a type neither asserts), §14 (a settled contradiction still written as open) and the row
cells (five rows naming an impossible effect). The drift stage compares the basis commit; the §15
re-read checked §15; every round-5 finding fell between the two. That is the concrete shape of the
R14 risk raised in round 4, and it is now written into the plan rather than into this file:

- §15's **"Re-read discipline"** note says re-read a claim *wherever it lives*, names the four
  places round 4 missed, and states two cheap per-round checks: §11 membership verified in both
  directions, and every count-zero / absence assertion checked for constructability of its subject.
- §11's preamble carries the both-directions rule and names M7A-105, M7A-111 and M7A-118 as the
  worked example.
- §12 gained four checklist lines so the rules are gated, not remembered: the ADR 0008 §7 item 4
  criterion now names the shape requirement; a whole-plan no-vacuous-assertion line; the §11
  both-directions line; and a line that every architect-routed question is marked open **in the
  deliverable**, not only in this handoff.

### Drift marker

**Not moved, and correctly so.** `git log -1 --format=%H -- crates/rdb-core/src/contracts` ⇒
`ec610f4d2b41090c697b2aa1aa4418b005cc86fd`. The marker still reads `ec610f4`. §15 gained rows
12-14, and rows 2, 4, 5, 7 and 9 were re-derived by opening the files, since those are the ones
the round-5 findings turn on. Rows 1, 3, 6, 8, 10 and 11 were independently re-derived by both
planner and critic in round 4 and are carried.

The "2 basis markers" problem from round 4 did not recur: the checker now matches a whole line
only, so §15's prose and this handoff quote the string safely.

### Verification run

```
bash scripts/drift-check.sh docs/testing/test-plan-m7-kernel-a.md
  => drift: test-plan-m7-kernel-a.md OK (ec610f4);  gate: drift OK;  exit 0

rows()  => 174   (uniq -d empty: no duplicate id)
gaps    => none; max 174
classes => 156 unit, 15 sim, 3 campaign  (sum 174 — matches §2 and §12)
widths  => every row at its own table's width, 0 bad rows
```

Table widths were checked by a script, not by eye: every contiguous pipe-block is compared against
its own header, splitting on **unescaped** pipes only, since escaped pipes appear in many cells. No
cargo was run, per instruction.

Row ids, counts and classes are unchanged from round 4 — round 5 rewrote cells, added §13 Q-17,
§15 rows 12-14 and four §12 checklist lines. No row was added, removed or renumbered.

### Open, and what would settle it

| Item | What would settle it |
|---|---|
| **Q-17** — M7A-169's `Err` twin: dedup rule vs `RetryRule::QueryStatus` | The architect's ruling. Routed, not mine. Either way it is one fact in one twin; M7A-169's main assertion is untouched |
| **§15 row 14** — `RetainedStatusMap` shape | F1 landing the type. If `discarded_from: Revision`, M7A-172's third sub-case is deleted with a stated reason |
| Q-5 `resume_gap_tolerance_ticks`, Q-7 `OutboxDispatch` gating, Q-12 the I1 clock seam, Q-15 §3.2 step 7 vs §3.4, Q-16 the `Read` shape | unchanged from round 4; all have defaults the rows are written against |

### Next role

Critic round 4, on the round-5 diff. Two things worth its time: whether M7A-32's arm-2 positive
control actually makes the row falsifiable or merely looks like it does; and a **row-by-row** read
of §3.1-§3.4 — the critic grepped that range in round 3 and the grep produced TD-17 and TD-18, but
a full read produced three more TD-17 siblings this round, so the grep is still under-reading it.
