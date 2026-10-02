# Critic round 2 — the M7 verification test plan

Target: `docs/testing/test-plan-m7-verification.md` (77 rows, VA-1..VA-9, Q-34..Q-40) and
`teams/verification/test-planner-handoff.md`, against `design.md`, `trace-requirements.md` and
`docs/ADRs/rdb/0019-*.md` as of 2026-09-20.

Read in full: the plan (656 lines), the planner handoff (156 lines), `design.md` §2.1–§2.6, §3,
§4, §5, §6, §7, §8, `trace-requirements.md` §1–§7, ADR-rdb-0019 §1–§2. Row classes counted
mechanically, not read off the prose.

**Verdict: FAIL** — for two rows and one semantics hole, not for the plan. Detail at the end.

Credit where it is due, because it changes what I attacked: the near-miss discipline (A2), the
`.orig.json` pairing, the trace-rewrite/injected-fault boundary stated in one sentence at the head
of §8, and the refusal to assert the 1 s/2.1 s ladder are all right, and all three of the
corrections I sustained in round 1 (F8, F20, F21) landed in rows that would fail if reverted. The
plan is also honest about its own weakest point (R-3), which is why I spent the budget finding the
*mechanism* by which R-3 bites rather than restating it.

---

## BLOCKER

### T-01 — `Unavailable` has two incompatible definitions, four rows need the one the design forbids, and `proven` is therefore vacuous

- **Criterion:** charter Q1 — "until [kernel packages] land, the runner reports explicit
  `Unavailable` for unwired capabilities, never a pass". Plan hard rule 3: "`Unavailable` is never
  a pass."
- **Location:** VA-1 §1 VA-2; rows M7V-03(b), M7V-31, M7V-33, M7V-63; `design.md` §2.4.
- **Evidence.** VA-2 states two rules in consecutive sentences:

  > "`Unavailable` is produced by a `capability{state=Unavailable}` event in the trace, **never
  > inferred from silence**. A checker that **saw no armed situation** reports `Unavailable`, not
  > `Proven`."

  Those are mutually exclusive: "saw no armed situation" *is* silence. `design.md` §2.4 settles it
  against the second sentence — it enumerates exactly three states and defines `Unavailable` as "a
  capability the checker needs is not wired (`capability{state=unavailable}` in the trace)", with
  `Proven` = "armed and no violation". There is **no state for "never armed, nothing wrong"**.

  Four rows require the forbidden second rule:
  - **M7V-03(b)** — a *zero-event* trace. It carries no `capability` event at all, so under §2.4
    every checker returns `Proven` (no violation seen). The row asserts all ten are `Unavailable`.
    It cannot pass.
  - **M7V-31** — "unhealed, or budget exhausted → INV-LIVE returns `Unavailable` in both". That is
    disarming, not an unwired capability.
  - **M7V-33** — "INV-ISO `Unavailable` (disarmed)". Same.
  - **M7V-63** — "a truncated trace must end at an event boundary, or the oracle reports
    `Unavailable`, not a violation". Same.
- **Consequence, and this is the part that matters.** The gap is not cosmetic. Under §2.4 as
  written, a campaign in which a checker never armed on any seed reports **`proven`**. M7V-52
  asserts "a row for all ten invariants; zero violations". M7V-54 fails the gate on anything "not
  `proven`". So the M7 release claim — ten `proven`, zero violations — is satisfiable by a corpus
  that armed **nothing**. `seeds_armed` exists for exactly this: VA-7's `invariant_status` line
  carries it. It is named **once in the whole plan**, in that table, and **no row and no Q-row
  asserts `seeds_armed > 0` for any `proven` status**. This is also the real mechanism behind the
  planner's own R-3: hand-built rows arm the checker, the campaign then reports `proven` without
  arming it, and nothing notices.
- **False-positive check.** Could "armed" be folded into the capability event — i.e. the runner
  emits `capability{state=Unavailable}` when a checker did not arm? No: §3.18 of
  `trace-requirements.md` fixes `CapabilityId` to the ten *packages* (`C0 H1 M1 I1 A1 T1 R1 P1 L1
  F1`) and says the event is "emitted once per run at trace start" — before any arming is known.
  Could M7V-03(b) be read as testing the capability path only? No; its input is explicitly "(b) a
  zero-event trace" and its assertion is explicitly "all ten are `Unavailable`". Could `Proven` be
  read as implying armed? §2.4's own gloss is "armed and no violation", so yes in intent — but
  nothing in the plan or the design makes a checker *return* anything else when it was not armed.
- **Closure.** Three edits, one of them not this plan's file:
  1. `design.md` §2.4 (architect's file — route it) distinguishes two `Unavailable` reasons:
     `Unavailable{Capability(id)}` and `Unavailable{NotArmed}`, both of which report and never
     pass. VA-2 is rewritten to match and the "never inferred from silence" sentence is deleted or
     scoped to the capability arm.
  2. `invariant_status.seeds_armed` becomes load-bearing: a new row asserts `status == "proven"
     implies seeds_armed > 0` over the default corpus for all ten invariants, and M7V-54's
     `SPIKE_REQUIRE_ALL=1` failure list includes `proven` statuses with `seeds_armed == 0`.
  3. Q-34 gains `seeds_armed` to its projection and the assertion "no `proven` row has
     `seeds_armed = 0`".

### T-02 — M7V-08, the row the whole correction round exists for, can go green on the wrong rule; and the one-copy fallback spec §8.3 actually forbids has no row

- **Criterion:** charter O1 "deliberately bad traces trigger each checker"; spec §8.3 "While
  running with two copies, both are required for every successful transaction. **There is no
  one-copy fallback**"; ADR-rdb-0019 §1 V3's degraded half; plan A7 "write the failing row first".
- **Location:** M7V-08, M7V-09; §4.2 as a whole.
- **Evidence, three separate defects in one row family.**
  1. **The fixture trips a second rule.** M7V-08's input is `protection_state{DegradedRf2,
     required_copy_set=[n1,n2], config_version=4}` plus "`publish` with ack evidence from a node
     **not** in that set". It says nothing about `durability_advance`. INV-PUB (design §2.3) also
     carries the grounding clause M7V-11 owns: "a `durability_class=Durable` ack at `seq` on node
     `n` requires a preceding `durability_advance{node=n, outcome=Synced, durable_seq >= seq}`".
     An ack from an out-of-set node with no flush behind it fires `durable_ack_ungrounded` first.
     The row asserts `rule="required_copy_set_unsatisfied"`, so a checker that has only the
     grounding clause and **no copy-set logic at all** fails the row — good — but a checker that
     evaluates grounding *before* copy-set (which M7V-10 explicitly demands for role mismatch:
     "before any quorum arithmetic") returns the wrong rule and the row is red for a reason that
     has nothing to do with F1. The row is then "fixed" by relaxing the asserted rule, which is
     exactly the lowering A8 forbids.
  2. **Nothing in the fixture lets the checker pin the set.** Design §2.3 pins the
     `required_copy_set` "by the `config_version` in force at **`admitted_seq`**".
     `trace-requirements.md` §3.7 gives `publish` exactly `generation, seq, published_digest,
     ack_evidence, authority_recheck` — **no `config_version`, no `admitted_seq`**. The pin can
     only be resolved by carrying `admission_decision.{admitted_seq, config_version,
     required_copies}` forward on the `correlation_id`. M7V-08's input contains no
     `admission_decision` and no `client_submit`. As written the fixture is unresolvable and the
     checker has to fall back to "the last `protection_state` seen", which is a *different rule*
     from the one the design states — and the difference is invisible until a `config_version`
     changes between admission and publication, which is precisely the RF3 → DEGRADED_RF2 →
     rebuild path §3.19 says the campaign passes through routinely.
  3. **The bug spec §8.3 names has no row.** M7V-08 tests "an ack from a node *outside* the pinned
     set". Spec §8.3's forbidden behaviour is publishing on **one copy** — the primary's own
     durability with no regular secondary ack at all. A checker implementing only "every counted
     ack must be a member of the pinned set" passes M7V-08 **and** M7V-09 **and** passes a publish
     with zero secondary acks. The plan's degraded coverage is therefore membership-only; the
     cardinality half (`min_regular_acks` 1-of-1 means **one**, not zero) is untested.
- **Consequence.** The row that exists to keep F1's bug dead is satisfiable by a checker that still
  ships F1's bug in its one-copy form, and is simultaneously fragile enough to be "fixed" by
  weakening its assertion.
- **False-positive check.** Is the one-copy case covered elsewhere? I searched the plan: M7V-07 is
  the RF3 shadow-ack case (`required_copy_set=[n1,n2,n3]`, ack from `n4` shadow) — a different
  quorum rule and still a membership failure, not a cardinality one. M7V-55 requires a
  `quorum_rule × DEGRADED_RF2` coverage cell, which counts a visit, not an outcome. Nothing else
  mentions a degraded publish with zero qualifying acks. Is defect (2) mine to raise, given the
  checker could reasonably pin from the last `protection_state`? It could — but then M7V-08 is
  pinning a *different* property than `design.md` §2.3 states, and the plan's own §13 maps M7V-08
  to "V3's degraded half". The mismatch is real either way.
- **Closure.**
  - M7V-08's input gains a `client_submit` + `admission_decision{Admitted, admitted_seq,
    config_version=4, required_copies=[n1,n2]}` sharing the `correlation_id`, **and** a
    `durability_advance{Synced}` grounding the offending ack, so the *only* reason the row can fire
    is the copy-set rule. Assert the rule string exactly.
  - Add a row (call it the F1 cardinality twin): `DegradedRf2`, pinned `[n1,n2]`, publish counting
    only the primary's own durability, no regular secondary ack → `Violated`,
    `rule="required_copy_set_unsatisfied"`. Its near-miss is M7V-09 unchanged.
  - Add a second sub-case to M7V-08 in which the `config_version` changes between
    `admission_decision` and `publish`, and the publish satisfies the **new** set but not the
    pinned one → `Violated`. That is the row that makes "pinned at `admitted_seq`" mean something.

---

## MATERIAL

### T-03 — M7V-19(b) asserts `Proven` on a digest divergence that INV-LIN and M7V-17 require to be `Violated`

- **Criterion:** `design.md` §2.3 INV-LIN — "One `(generation, seq)` never carries two
  `entry_digest` values **anywhere in the trace**. … A digest conflict yields `mode=quarantine`";
  spec §8.2 divergence never auto-merges.
- **Location:** M7V-19 sub-case (b) vs M7V-17.
- **Evidence.** M7V-19(b)'s input: "the longer source is reachable but its `reported_digest`
  differs from the recorded `entry_digest` at that `(generation, seq)`". Assertion: "INV-LIN
  `Proven` in both", and §4's preamble adds "no other checker fires". But INV-LIN's cutoff clause 2
  (the F2 closure) *establishes by construction* that `queried_sources[].reported_digest` and the
  recorded `entry_digest` at a `(generation, seq)` are the same quantity — it compares them
  directly. So a reachable source reporting a different digest at a recorded `(generation, seq)` is
  two digests at one `(generation, seq)` in the trace: the conflict clause's literal antecedent,
  with no `quarantine` event anywhere in the fixture. M7V-17 asserts that exact shape is
  `Violated{rule="digest_conflict_without_quarantine"}`.
- **Consequence.** The two rows are mutually unsatisfiable for any faithful implementation of
  §2.3. The developer resolves it by weakening one of them, and the likely casualty is M7V-17's
  "anywhere in the trace" — which is the clause that keeps divergence from auto-merging.
- **False-positive check.** Is `reported_digest` textually excluded? No — §2.3 says "anywhere in
  the trace", and `trace-requirements.md` §3.12 lists `reported_digest` inside `queried_sources`
  with INV-LIN as a reader. Is M7V-19(b) perhaps describing a *stale* source (old generation)? No:
  the row says "at that `(generation, seq)`". Could both be true because the conflict clause is
  scoped to `batch_apply`? That is the likely intent, but the plan never says so, and if it is the
  intent then M7V-19(b) is asserting `Proven` on a real divergence with no other checker to catch
  it.
- **Closure.** Pick one and write it into the row: either (i) INV-LIN's conflict clause is
  explicitly scoped to `entry_digest` values carried by `batch_apply`, M7V-17 says so, and
  M7V-19(b) gains a sentence explaining that the recovery-path digest disagreement is kernel-b's
  `mode=Quarantine` decision and not the oracle's — in which case a *third* sub-case is needed
  asserting the recovery-path disagreement does produce `mode=Quarantine`; or (ii) M7V-19(b)'s
  fixture carries the `quarantine` event and the row asserts `Proven` **with** it, which keeps both
  clauses honest. (i) is the cheaper one and I recommend it, with the third sub-case.

### T-04 — M7V-09 differs from its bad twin by two facts, and `required_copy_set` silently carries two different satisfaction rules

- **Criterion:** plan rule A2 — "a near-miss row differs from its bad twin by exactly one fact";
  spec §8.3 vs §6.2.
- **Location:** M7V-08/M7V-09; M7V-09 vs M7V-36.
- **Evidence.** M7V-09 changes two things relative to M7V-08: the ack's node moves inside the
  pinned set, *and* the fixture gains "grounded by a preceding `durability_advance{n2, Synced}`"
  which M7V-08 does not have. (Fixing T-02 defect 1 fixes this half automatically.) Second, and
  separately: `protection_state.required_copy_set` is read by two invariants with **opposite
  quantifiers** — INV-PUB under `DegradedRf2` is satisfied by *one* ack from the set (M7V-09,
  ruling B-R3), while INV-LAG clause (a) requires *every* node in the same field to reach the
  barrier (M7V-36, the F8 closure, kernel-b's `all_durable_through`). Both are correct; neither row
  mentions the other; they are 27 ids apart.
- **Consequence.** A developer who writes one shared `copy_set_satisfied()` helper gets one of the
  two rows wrong, and the failing one looks like a fixture bug.
- **False-positive check.** Are the fields actually the same? Yes — `trace-requirements.md` §3.14
  gives `protection_state` a single `required_copy_set: Vec<NodeId>` read by "INV-LAG, INV-PUB".
- **Closure.** One sentence in §4.2 and one in §4.9: "`required_copy_set` is a membership list;
  INV-PUB checks membership plus the quorum rule's cardinality, INV-LAG quantifies over every
  member. They must not share a helper." Cross-reference M7V-09 ↔ M7V-36 in both cells.

### T-05 — M7V-02's fixture cannot arm INV-LAG or INV-VER, so its own assertion is unsatisfiable

- **Criterion:** the row's own assertion — "every one of the ten checkers returns `Proven`; **none**
  returns `Unavailable` (this trace arms all ten on purpose, so it also pins the arming
  conditions)".
- **Location:** M7V-02.
- **Evidence.** The stated input is: "RF3 healthy, two partitions, one recovery with a declared
  `predecessor_cutoff` and a genuine restricted loss above it, one retained dedup hit, one healed
  schedule phase". That arms ATOM, PUB, AUTH, LIN, DEDUP, LOSS, LIVE, ISO. It contains **no
  `version_check`** (INV-VER's only arming event, `trace-requirements.md` §3.15 / §4) and **no
  pause/resume cycle** — the input says "RF3 healthy", and INV-LAG's three clauses all need a
  `protection_state` transition through `Paused` (design §2.3, §4.9's rows).
- **Consequence.** The plan's single positive control for the whole oracle is red on day one, or
  green because "arms all ten" was quietly dropped — and with it the claim that M7V-02 "pins the
  arming conditions", which under T-01 is the only place arming is pinned at all.
- **False-positive check.** Could INV-LAG arm on a `Healthy`-only trace? Clause (b) is about
  `oldest_unsafe_age_ms` across a `config_version` change, which a healthy RF3 trace need not have;
  clauses (a) and (c) need `Paused`. Could INV-VER arm on a `batch_apply` alone? §2.3's INV-VER is
  stated entirely over `version_check`.
- **Closure.** Extend M7V-02's input with an explicit `version_check{mandatory_unknown_fields=[],
  outcome=Accept}` and a complete legal pause→resume→healthy cycle (i.e. fold M7V-41's shape into
  it), or split the assertion: "these eight are `Proven`; INV-LAG and INV-VER arm in M7V-41 and
  M7V-35 respectively" — and then say plainly that no single trace arms all ten.

### T-06 — several rows and Q-35 use field names the trace contract does not define, and one of them hides a watermark-versus-point semantics

- **Criterion:** `trace-requirements.md` §2–§4 is the field contract; team-rules "Evidence".
- **Location:** M7V-10, M7V-11, M7V-28, M7V-29; Q-35.
- **Evidence.** `trace-requirements.md` §3.5 defines `replication_ack` as `from_node, to_node,
  peer_role, peer_boot_id, config_version, generation, owner_epoch, **contiguous_seq**,
  contiguous_digest, durability_class, accepted, reject_reason`. §4 repeats
  "`replication_ack.{peer_role, contiguous_seq, durability_class}`". The plan writes:
  - M7V-10: `replication_ack{from=n4, peer_role=Regular}` — `from`, not `from_node`.
  - M7V-11: `replication_ack{node=n2, seq=5, durability_class=Durable}` — `node` and `seq`, neither
    exists.
  - M7V-28: `replication_ack{node=n2, boot=b1, seq=9, durability_class=Durable}` — `node`, `boot`,
    `seq`; the contract has `from_node` and `peer_boot_id`.
  - Q-35: `SELECT seq, from_node, … FROM … WHERE "@m"='replication_ack'` and `LEFT JOIN acks ON
    acks.seq >= p.seq`. `acks.seq` is NULL for every row under `union_by_name=true` — **DuckDB
    returns no error**, the join predicate is NULL, and the query silently returns nothing useful.
  The `seq` → `contiguous_seq` substitution is not cosmetic: `contiguous_seq` is a **watermark**,
  so "an ack at `seq` 9" means `contiguous_seq >= 9`, and the same is true of
  `durability_advance.durable_seq`. Every row that reads "ack at seq N" must be implemented as a
  range test, and INV-LOSS's holder map (M7V-28/29) is built from it.
- **Consequence.** Four fixtures and the plan's most-used diagnostic query are written against a
  vocabulary that does not exist; the developer either renames by guess or, worse, implements a
  point comparison where the contract says watermark, which under-counts holders — the exact
  failure V-R10's secondary-side emission rule was added to prevent.
- **False-positive check.** Is the node identity perhaps coming from the envelope? For
  `durability_advance` yes — §3.6 has no node field and the envelope's `node_id` supplies it, so
  M7V-11's `durability_advance{node=n2, …}` is fine in substance. For `replication_ack` no: §3.5
  defines `from_node` explicitly because the emission point is the secondary and the envelope's
  `node_id` is the emitter, which happens to coincide but is a different field.
- **Closure.** Sweep §4's event literals against `trace-requirements.md` §3 and fix the names;
  state once, in §4's preamble, that `contiguous_seq` and `durable_seq` are watermarks and every
  "at seq N" in a row means `>= N`; fix Q-35's `acks.seq` to `acks.contiguous_seq`.

### T-07 — Q-35 cannot show the pinned copy set or the role mismatch, which are the two things it exists to show

- **Criterion:** "each Q-row is a runnable DuckDB query over the JSONL fields the trace actually
  emits"; the row's own "First diagnosis".
- **Location:** Q-35.
- **Evidence.** Two independent breaks, on top of T-06:
  1. `LEFT JOIN pinned ON pinned.config_version = p.config_version` where `p` is the `publish`
     event. §3.7 gives `publish` no `config_version` (and no `admitted_seq`). The join key is
     always NULL, so `pinned.quorum_rule` and `pinned.required_copy_set` are NULL on every output
     row, and Q-35's headline — "a `quorum_rule='DegradedRf2'` row with a single ack from a node
     outside the pinned set is critic F1's bug, live" — can never appear. The correct join is an
     as-of join through `correlation_id` to `admission_decision.{admitted_seq, config_version}`,
     which §4's INV-PUB row already names as a required field.
  2. The query's third assertion is "every `peer_role` matches the topology in force at that
     `config_version` (a mismatch is the MUT-2 shape)", but the query reads no
     `topology_change` and no header `topology`. Nothing in the result set can support it. This is
     the F19/V-R12 closure's own diagnostic, and it has no data source.
- **Consequence.** The query a developer runs *first* when M7V-08, M7V-10 or M7V-69 goes red
  returns a table with NULL in the two columns that decide the answer, and reads as "no pinned set
  was recorded" — pointing the developer at foundation's trace emission instead of at the checker.
- **False-positive check.** Could `config_version` be an envelope field? §2's envelope is
  `event_id, logical_tick, kind, partition_id, node_id, boot_id, correlation_id` — no.
- **Closure.** Rewrite Q-35's `pinned` join as: `publish → admission_decision` on
  `correlation_id`, then `admission_decision.config_version → protection_state.config_version`.
  Add a `topo` CTE over `"@m"='topology_change'` (plus the header row) and project the resolved
  role next to `acks.peer_role` so the mismatch is a visible column, not a claim in prose.

### T-08 — Q-38 cannot find the shortfall it was written to find

- **Criterion:** the row's own assertion; ADR-rdb-0019 §2 "a named required cell with zero hits
  fails the run".
- **Location:** Q-38.
- **Evidence.** The query is `SELECT axis, cell, sum(count) … WHERE "@m" = 'coverage_cell' GROUP BY
  ALL HAVING hits = 0`. VA-7 defines `coverage_cell` as `{axis, cell, count}` — a line emitted for
  a cell that was **hit**. A required cell with zero hits produces no `coverage_cell` line at all,
  so it is absent from the `FROM` clause and `HAVING hits = 0` can never select it. VA-7 defines a
  separate line for exactly this: `coverage_shortfall {axis, cell}`. Q-38 never reads it. The
  stated assertion, "for a passing run the result over the required cells is empty", is therefore
  **trivially true for every run, passing or not**.
- **Consequence.** The plan's only coverage diagnostic is a query that always returns empty. A
  developer chasing M7V-55 gets a clean result and concludes coverage is fine.
- **False-positive check.** Could the runner emit `coverage_cell{count: 0}` for every required
  cell? Nothing says so, and if it did, `coverage_shortfall` would be redundant. Either way one of
  the two lines is dead and the plan does not say which.
- **Closure.** `Q-38` reads `coverage_shortfall` for the shortfall and `coverage_cell` for the
  counts, in two queries or one `UNION ALL`; and VA-7 states which of the two lines is emitted for
  a zero-hit required cell.

### T-09 — M7V-69's assertion is a disjunction, so MUT-2 can be "caught" without the kernel ever rejecting anything

- **Criterion:** the row quotes it itself — spike §4 transport: "forged identity is injectable
  **and rejected**"; §8's own boundary statement ("an injected fault tests the strictly stronger
  claim — kernel **plus** oracle rejects it").
- **Location:** M7V-69.
- **Evidence.** The assertion: "**either** the kernel rejects it with
  `AckRejectReason::ForgedIdentity` … **or** INV-PUB fires with
  `rule="ack_role_claim_mismatch"`". The second disjunct is satisfiable by the oracle alone, on a
  kernel that counted the forged ack and published on it. So the row is green while the kernel has
  exactly the defect MUT-2 names. This is the only row in the plan that reaches the §2.5 blind spot
  for identity forgery, and §13 maps MUT-2 to it alone.
- **Consequence.** The strictly-stronger claim §8 promises is not asserted anywhere. The plan gets
  the credit for an injected-fault row while asserting a trace-rewrite-strength property.
- **False-positive check.** Is the disjunction defensible while R1 is unwired? Yes — but that is
  what `Unavailable` is for (§12), not what a weaker assertion is for. Is the `ForgedIdentity`
  coverage cell enough on its own? No: M7V-55 counts a visit to the cell, and the cell is reachable
  by the oracle arm too.
- **Closure.** Split the row. `M7V-69a`: the kernel rejects with
  `AckRejectReason::ForgedIdentity` and the coverage cell is hit — `Unavailable` until R1/H1 land,
  never a pass, and listed as such in §12. `M7V-69b`: INV-PUB fires with
  `rule="ack_role_claim_mismatch"` on a trace in which the forged ack *was* counted. Both are
  required; neither substitutes for the other.

### T-10 — M7V-51 asserts a measured duration is non-zero, which is a wall-clock assertion in the PR default

- **Criterion:** plan hard rule 1 ("**No wall-clock assertion in the PR default.** Numbers are
  recorded") and anti-flake A1 ("the only wall-clock number anywhere is `wall_ms`, and it is
  recorded, not asserted (except M7V-61)").
- **Location:** M7V-51; corroborated by plan §14 Q-5, which already noticed the zero case.
- **Evidence.** M7V-51's assertion includes "both are **non-zero** in this run". `shrink_ms` is a
  measured elapsed time; a single-failure shrink in a fast run can legitimately round to 0 ms, and
  on this Windows host the coarse-timer risk is real. The plan's own Q-5 answers "always present,
  `0` when nothing shrank" — which is the right answer and contradicts the row.
- **Consequence.** An intermittently red row in the PR gate, whose obvious "fix" is to delete the
  assertion and with it the F11 separation it was written to protect.
- **False-positive check.** Is `wall_ms != 0` also at risk? Less so, but it is the same class of
  assertion and the same rule forbids it.
- **Closure.** M7V-51 asserts: both keys present; `shrink_ms` is a distinct key from `wall_ms`;
  `wall_ms` excludes shrink time (assert via the instrumentation, not the values); and **a shrink
  occurred** — evidenced by `shrink_step` count > 0 and a `shrink_result` line, not by a duration
  being non-zero.

### T-11 — 16 campaign rows drive roughly twenty corpus runs inside one debug gate command, and §2 sets no aggregate budget

- **Criterion:** §2's budget table; charter's handoff gate (VA-9 command 1); AGENTS.md's host
  reality; F10 (already closed, and this is its other half).
- **Location:** §2; M7V-51..M7V-65, M7V-72..M7V-76; VA-9 command 1; worst offender M7V-65.
- **Evidence.** §2 budgets the campaign class as "default corpus **< 60 s** at `SPIKE_SEEDS=64`" —
  per *corpus*, with no statement of how many corpora the class runs. Counting the rows' own
  inputs: M7V-58 runs the corpus three times (1, 2, N threads); M7V-61 twice; M7V-62 needs two
  *commands*, one of them a release build; M7V-65 runs "the default corpus **and the PR corpus**",
  and §2's own table defines the PR corpus as `SPIKE_SEEDS=1000, SPIKE_MAX_EVENTS=2000` — the
  1,000-history run, executed in the **debug** handoff gate; M7V-75 and M7V-76 each re-run under a
  different `RETCD_EVIDENCE`; M7V-52..57, 63, 64, 72, 73 each need at least one. That is ~20 corpus
  executions, all under `[profile.test] opt-level = 0` for workspace members, on a VM that AGENTS.md
  says routinely carries six concurrent agent cargo invocations.
- **Consequence.** VA-9's handoff gate command does not fit any plausible budget, and the first
  reaction will be `#[ignore]` on the campaign rows — which ADR-rdb-0019 §2 forbids and M7V-75
  asserts against.
- **False-positive check.** Do the rows share one run? Nothing says so, and several cannot
  (M7V-58, M7V-61, M7V-65, M7V-75 vary the environment). Is M7V-65's property scale-dependent? No —
  "the set of checkers that ran, the set of fault kinds reachable and the required-cell list are
  identical; only `seeds`, `max_events` and `events_total` differ" holds between any two scales.
- **Closure.** (a) §2 gains an aggregate budget for `--test campaign` at default scale, and names
  the mechanism: one shared corpus run behind a `OnceLock`, consumed by every row that does not
  vary the environment (M7V-52..57, 63, 64, 72, 73). (b) M7V-65 compares 64 seeds against 128, not
  against the 1,000-seed PR corpus; the PR corpus belongs to the extended gate only. (c) M7V-58
  uses the smallest corpus that produces a non-trivial merge.

### T-12 — M7V-55 makes a random 64-seed corpus responsible for every required cell, including two that only exist behind provider hooks

- **Criterion:** spike §7 "every required fault boundary exercised"; design §6; A5/A8.
- **Location:** M7V-55; design §6's three axes.
- **Evidence.** M7V-55 asserts `required_missing[]` is **empty across all three axes at default
  scale** (`SPIKE_SEEDS=64`). The required set includes every variant of `AckRejectReason` (7),
  `BoundaryId` (spike §6's column + 2), `AdmissionReason` (the whole §5.4 error list),
  `RecoveryMode` (3), `ProtectionState` (4) and `QuorumRule` (2) — M7V-56 enumerates them — plus
  the four named cross-package cells and the isolation cell. Two of those cells
  (`ForgedIdentity`, the false-durable boundary) are reachable **only** through `NetworkOp::ForgeAck`
  and `StorageOp::FalseDurable`, which §12 lists as blocked on H1 and M1 hooks. Nothing in §5 or
  design §3.1 says the generator *schedules* required boundaries; the row therefore rests on 64
  random draws covering a few dozen named cells.
- **Consequence.** The most likely flake in the plan, and the one whose green is worth the most —
  a passing M7V-55 is what §13 cites for "every required coverage cell hit".
- **False-positive check.** Could the four authored cases carry the rare cells? The row says
  "default corpus **plus** the four authored cases", and those four are the cross-package cells
  specifically; they do not cover `AckRejectReason`'s seven or `AdmissionReason`'s full list.
  Could a weighted generator make this near-certain? Possibly — but "near-certain over 64 seeds" is
  a probabilistic gate, which is what A8 exists to stop being lowered.
- **Closure.** Make cell coverage a property of the seed *list*, not of luck: the generator
  deterministically schedules required boundaries across the corpus (seed `i` is obliged to
  attempt boundary `i mod N`), and M7V-55 asserts the schedule covers the required set plus the
  observed counts. Add that obligation to VA-6 and to design §3.1 (routed to the architect). Until
  H1/M1 land, the two hook-gated cells report `Unavailable` and are excluded from
  `required_missing[]` by capability, not by deletion.

### T-13 — VA-9's two commands write the same artifact path, so M7V-62 can never see both profiles

- **Criterion:** M7V-62's assertion — "the artifact from **each** of VA-9's two commands … the
  debug run records `profile: "debug"` and the release run `profile: "release"`"; ADR-rdb-0019 §2.
- **Location:** VA-9; M7V-62; M7V-72; ADR-rdb-0019 §2 (architect's file).
- **Evidence.** ADR-rdb-0019 §2 names exactly one campaign artifact,
  `docs/evidence/rdb-m7-campaign.json`, and VA-8 routes both runs through the same
  `write_evidence(name, …)`. Both VA-9 commands are campaign runs, so both write that one path and
  the second overwrites the first. M7V-62 needs the two artifacts side by side to assert the
  `profile` discriminator does its job. Secondary consequence: neither VA-9 command sets
  `RETCD_EVIDENCE`, so by M7V-75's own rule the release run's 1,000-history number is recorded in
  an artifact marked `full_scale: false` — which is honest but is not the charter's number.
- **Consequence.** M7V-62 is unsatisfiable as written, and it is the only row standing between F10
  and someone quoting a debug `wall_ms` as the 60 s figure.
- **False-positive check.** Could M7V-62 read the release artifact from a previous run left on
  disk? Then it asserts on a stale file of unknown provenance, which is worse. Could the release
  command be given `--test campaign` only, writing a different name by accident? Nothing in VA-8
  suggests the name varies.
- **Closure.** Route to the architect: ADR-rdb-0019 §2 gains a second artifact name (e.g.
  `rdb-m7-campaign-release.json`) or keys `values` by profile. VA-9's release command sets
  `RETCD_EVIDENCE=1`. M7V-62 asserts both files exist, carry different `profile` values, and that
  only the `release` one may be cited for the 1,000-history budget.

### T-14 — the honest-green gate has no owner: `SPIKE_REQUIRE_ALL=1` is in no command, and nothing makes `capability{state}` track reality

- **Criterion:** charter Q1; ADR-rdb-0019 §2 "the gate check fails when `SPIKE_REQUIRE_ALL=1`";
  §13's checklist line "Zero invariant violations once kernel packages land".
- **Location:** VA-9; M7V-54; §12's last table row; `trace-requirements.md` §3.18.
- **Evidence.** Two halves.
  1. `SPIKE_REQUIRE_ALL` appears in §2's environment table ("`1` at the M7 gate"), in M7V-54, and
     in §12 — and in **neither** of VA-9's two commands, and in no checked-in script the plan
     names. §13's final line is "`scripts/gate.sh test -p rdb-sim …` green at handoff", which by
     construction is green with all ten `unavailable`. The gate exists only as a sentence.
  2. `capability{capability_id, state}` is "emitted once per run at trace start" (§3.18). Nothing
     in the plan requires that state to be **derived** from whether the package is actually wired.
     If it is a hand-maintained constant — the obvious implementation — then a developer who lands
     P1 and forgets to flip it leaves every P1 invariant `unavailable`, the binary still exits 0,
     and no row fails. Combined with T-01, the opposite mistake (flipping it to `Wired` early) makes
     every unarmed checker report `proven`.
- **Consequence.** The mechanism the plan calls "the whole answer to 'green run, honest artifact'"
  (M7V-53's own words) depends on a variable nobody sets and a state field nobody validates.
- **False-positive check.** Is the gate perhaps rEtcd's existing evidence-check script? The plan
  cites ADR-0031's `full_scale:false` mechanic and rEtcd's gate script by analogy, but never names
  a path or a line, and team-rules forbids citing evidence files that do not exist.
- **Closure.** (a) VA-9 gains a third command — the M7 release gate — with
  `SPIKE_REQUIRE_ALL=1 RETCD_EVIDENCE=1`, and §13's last line cites it instead. (b) A new row
  asserts `capability{state}` is derived from the crate's actual wiring (a feature flag, a
  registered provider, or a compile-time registry) and not from a literal — the same enumerated
  shape as M7V-56, so a landed package that stays `Unavailable` fails a row.

### T-15 — two of my round-1 closures (F5, F12) have no row that would fail if they were reverted

- **Criterion:** the lead's check — "every one of your F1-F21 closures has a row that would fail if
  the closure were reverted".
- **Location:** design §3.2/§8 (F5), §4.2/§8 (F12); the plan's §5 and §6.
- **Evidence.** I checked all twenty-one. Nineteen have a guard row (table at the end). Two do not:
  - **F5** — `Budget::heal_at_event` removed; healing is `NetworkOp::Heal` so ddmin moves it with
    the op list. The nearest rows are M7V-42 (every required boundary is constructible) and M7V-44
    (budget respected). Neither fails if a `heal_at_event: usize` is added back to `Budget`, and the
    failure it causes is subtle by construction — the heal drifting relative to a shortened op list,
    which shows up as an INV-LIVE flake, not as a red row.
  - **F12** — no per-op field-simplification pass in the reducer. M7V-49 is the closest, and it
    asserts only that no function takes `&mut [TraceEvent]` / `&mut Trace`. A pass that rewrites
    `Advance{ticks}` inside a `Vec<ScenarioOp>` touches no `Trace` type and passes M7V-49 cleanly.
- **Consequence.** Both closures are one refactor away from silently returning, and both failure
  modes are flakes rather than red rows — the worst kind to reintroduce.
- **False-positive check.** Is F5 covered by M7V-22 (`op_skipped`)? No: M7V-22 is about a deleted
  op's referent, not about an index into the event stream. Is F12 covered by M7V-20's "ops_after <
  ops_before"? No: a field-shrinking pass also reduces ops and would pass it.
- **Closure.** Two cheap source-level rows in the M7V-49 style: (a) `Budget` has no field whose
  name or type indexes the event stream, and `Heal` appears only as a `NetworkOp`; (b) the only
  mutation `reduce.rs` performs on a `Vec<ScenarioOp>` is removal — no function returns a
  `ScenarioOp` it constructed or modified from an input op.

### T-16 — two assertion-lowering surfaces the plan opens on purpose and does not bound

- **Criterion:** plan hard rule 4 / A8 — "never lower an assertion to make a run green"; F4.
- **Location:** M7V-50; plan §14 Q-1 and the `Report::without_rule` default.
- **Evidence.**
  1. M7V-50's assertion contains its own escape: "each still fails its recorded checker (**or**,
     once the defect is fixed, the row is retired deliberately with the fixture … the row asserts
     the fixture set and the recorded expectations agree pairwise)". Pairwise agreement between a
     fixture and a file recording what it is expected to do is green whenever someone edits the
     expectation. That is the lowering path, written into the row.
  2. Q-1's default introduces `Report::without_rule(&str)` — a per-rule suppression API in the test
     binary, justified by exactly one row (M7V-23). Nothing bounds its use to that row.
- **Consequence.** The `.orig.json` mechanism is the plan's defence against F4 (fix the path the
  minimized fixture reproduces, ship the real bug). Both surfaces let it be disarmed by editing
  metadata rather than code.
- **False-positive check.** Is retirement ever legitimate? Yes — when the defect is fixed the
  fixture should go. The defect is that retirement is expressed as an *expectation edit* rather
  than as a deletion plus a record.
- **Closure.** M7V-50 asserts: every fixture in `regressions/` has an expectation of **`fails`**;
  there is no `passes` expectation; retirement means deleting both files in the pair and adding a
  line to ADR-rdb-0019. And a one-line row asserts `without_rule` has exactly one call site
  (string match, the mechanism §13 already uses).

---

## ADVISORY

### T-17 — the runtime-class counts are wrong in both documents, including the number in R-3

- Counted mechanically from the tables: **unit 52, sim 9, campaign 16** (= 77). §2's taxonomy table
  lists unit as `M7V-01..M7V-47, M7V-66..M7V-68` and sim as `M7V-21, M7V-22, M7V-47, M7V-69,
  M7V-70` — but the tables class M7V-20, M7V-23, M7V-48 and M7V-50 as `sim` too, and those fall
  inside §2's "unit" range. The handoff §3 says "unit (61 rows), sim (10 rows), campaign (16 rows)"
  = 87 ≠ 77, and risk R-3 is stated as "**61** unit rows".
- Closure: correct §2's inclusion lists to 52/9/16 and restate R-3 as 52. The substance of R-3 is
  unchanged; see the verdict section for my answer to it.

### T-18 — §12's "Unavailable until" table omits rows that belong in it

- Missing: **M7V-51** (campaign-class, needs I1 + testkit); **M7V-62** (needs an artifact the
  handoff gate never produces — see T-13); **M7V-66..M7V-68** (need C0's trace types, like every
  other hand-built-trace row); **M7V-20** (mentioned only in a parenthetical on the I1 line).
- Closure: add them. §12 is the plan's ordering document and R-7 relies on it.

### T-19 — M7V-29's stated reason contradicts the grammar's crash semantics

- The row's violating sub-case is "a buffered-only holder returning at the **same** `boot_id` (a
  process crash that kept the buffer)". `design.md` §3 says `Crash{kind}` distinguishes "**process**
  crash (loses unflushed process buffers) from **host** crash (may discard every unflushed
  suffix)" — a process crash does *not* keep the buffer. And §2's envelope defines `boot_id` as what
  "distinguishes a restarted node from itself", so a same-`boot_id` return means the node never
  restarted at all, which is the actual reason the loss is a violation.
- Closure: restate the sub-case as "a buffered-only holder that is reachable at the **same**
  `boot_id`, i.e. never restarted", and delete the process-crash parenthetical. The assertion is
  right; the reason would mislead a developer into modelling process crash as buffer-preserving.

### T-20 — M7V-01 asserts a blocklist where the row's own prose says allowlist

- The assertion is "no line matches `rdb_core::(authority|transaction|…|recovery)`", then adds "the
  only permitted `rdb_core` import prefix is `rdb_core::contracts::{trace, ids}`". The regex misses
  `use rdb_core::{transaction::Foo, contracts::trace};` (no literal `rdb_core::transaction`),
  `use rdb_core::*;`, and any aliased re-export.
- Closure: assert the allowlist — every `rdb_core` path token in the file set, extracted and
  compared against `{contracts::trace, contracts::ids}`. Same cost, and it fails closed.

### T-21 — M7V-42 and M7V-44 are classed `unit` / dep `none` but describe behaviour that needs the runner

- M7V-44's assertion includes "**the runner** stops at it" with class `unit`, dep `none`.
- M7V-42 asserts "for every member there is a `ScenarioOp` the generator can emit that **produces
  it**" — producing a `BoundaryId` is something the environment does during a run. If the row is a
  static `BoundaryId → producing op` table check it is correctly `unit` but can go stale; if it is
  behavioural it is `sim` and belongs in §12.
- Closure: split M7V-44 into a `unit` half (no generated scenario exceeds `max_events`) and a `sim`
  half (the runner stops at the cap); say explicitly which kind of row M7V-42 is.

### T-22 — two Q-rows assert on fields their own query does not return

- **Q-39** asserts "the run's `shrink_result.slipped` is true **iff** `faults_before !=
  faults_after`", but the query selects only from `"@m" = 'shrink_step'`. `shrink_result` is a
  separate line (VA-7).
- **Q-40**'s second half — "over the whole result set of every query above: no column named `key`,
  `value`, `value_bytes`, `payload` or `mutation_bytes` exists" — is not a query. It is the
  redaction check team-rules requires and it deserves to be runnable.
- Closure: Q-39 gains a `UNION ALL` or a second statement over `shrink_result`. Q-40's second half
  becomes `DESCRIBE SELECT * FROM read_json_auto('…/**/*.jsonl', union_by_name=true)` with the
  forbidden names asserted absent from the column list — one statement, and it covers every line
  the run emitted rather than only the ones the other queries projected.

---

## Withdrawn after checking

Four things I went after and could not sustain. Recording them so the planner knows they were
tested, not missed.

1. **"M7V-40 is a row that asserts nothing."** It asserts `Proven` on a trace a wrong clause would
   fire on, and §4's preamble makes it assert the whole report. It is exactly the regression guard
   F20 needed, and the row says why in its own cell. Sustained as correct.
2. **"§13's row count is wrong."** 41 + 6 + 8 + 14 + 6 + 6 = 81, minus the four ids 20–23 counted
   in two blocks = 77, and the distinct id set is `M7V-01..M7V-77`. The arithmetic is right and the
   overlap is disclosed.
3. **"M7V-72 invents evidence keys."** I compared it key by key against ADR-rdb-0019 §2: `seeds`,
   `max_events`, `events_total`, `invariants{}`, `mutations{}`, `wall_ms`, `shrink_ms`,
   `compile_ms_excluded`, `profile` — exact match, with `slipped` correctly placed under "recorded"
   rather than "asserted". M7V-73 matches the coverage artifact's four keys exactly. No
   over-claiming.
4. **"The 15 pairwise cells should be required, not reported."** They are reported, and design §6
   gives the reason (some pairs are meaningless; a required-but-unreachable cell becomes a cell
   someone deletes). Same conclusion I reached in round 1. M7V-73 states it in the row.

---

## The lead's explicit checks, answered

| Check | Result |
|---|---|
| Every F1–F21 closure has a row that would fail if reverted | **19 of 21 yes.** F5 and F12 do not — **T-15**. Table below. |
| MUT-2 and MUT-5 actually reach the kernel | **MUT-5 yes** (M7V-70 asserts a conjunction — INV-PUB grounding **and** INV-LOSS — and needs the M1 hook). **MUT-2 no** — M7V-69's disjunction is satisfiable by the oracle alone: **T-09**. |
| Wall-time rows follow V-R11 | **M7V-60 and M7V-61 yes**, and M7V-62 is the right idea. **M7V-51 no** — `shrink_ms != 0` is a wall-clock assertion in the PR default: **T-10**. M7V-61 should also pin `SPIKE_ASSERT_WALL_MS` to a value no host can beat (0 or 1) rather than "a deliberately slow corpus", which is host-dependent. |
| `Unavailable`-not-pass rows cannot be satisfied by a skipped test | **Skipping is well defended** — §12 upgrades rows in place, M7V-75 asserts no `#[ignore]`, and A5 is explicit. But the verdict itself is broken (**T-01**) and the gate that turns `unavailable` into a failure has no owner (**T-14**). So: not satisfiable by a *skip*, but satisfiable by a *vacuous pass*. |
| Evidence rows produce the ADR-0019 schema | **Yes, key for key** (see Withdrawn #3) — except that both VA-9 commands write the same path, so the two-profile claim cannot be made: **T-13**. |
| Each Q-row is runnable over the fields the trace emits | **Q-34 yes** (NULLs under `union_by_name` are fine for its grouping; add `seeds_armed` per T-01). **Q-36 yes** — every projected field exists in §3.3 plus the envelope. **Q-37 yes** — §3.4 matches exactly. **Q-35 no** (**T-06**, **T-07**). **Q-38 no** (**T-08**). **Q-39 and Q-40 partly** (**T-22**). |

### F-closure → guard row

| Closure | Guard row | Would fail if reverted |
|---|---|---|
| F1 pinned copy set, degraded RF2 | M7V-08, M7V-09 | yes — but see **T-02** for *why* it would fail |
| F2 cutoff as two lookups | M7V-18, M7V-19(a) | yes |
| F3 injected fault reaches the blind spot | M7V-69, M7V-70 | partly — **T-09** |
| F4 `.orig.json` companion | M7V-23, M7V-50 | yes — but **T-16** |
| F5 `heal_at_event` removed | — | **no — T-15** |
| F6 false-durable checker and op | M7V-11, M7V-70 | yes |
| F7 partition axis | M7V-32, M7V-33, isolation cell in M7V-55 | yes (via coverage) |
| F8 every pinned copy at the barrier | M7V-36 | yes |
| F9 buffered holder at a new boot | M7V-29 | yes — but **T-19** |
| F10 the 60 s command | M7V-62 | yes — but **T-13** |
| F11 three shrink budgets; `shrink_ms` separate | M7V-48, M7V-51 | yes — but **T-10** |
| F12 no per-op field shrinking | — | **no — T-15** |
| F13 fields with no reader are listed | trace-req §7 | n/a (documentation closure) |
| F14 `CROSS_AFFINITY` in the dedup key | M7V-25(b) | yes |
| F15 `RecoveredApplied` | M7V-26 | yes |
| F16 `ForgedIdentity` op and cell | M7V-55, M7V-56, M7V-69 | yes |
| F17 `op_skipped` outside `BoundaryId` | M7V-22, M7V-56 | yes |
| F18 `Provenance`, not a bare seed | M7V-46, M7V-47 | yes |
| F19 / V-R12 `topology_change` | M7V-10 | yes — the row's second half is written to it |
| F20 admission gate, not publication | M7V-39, M7V-40 | yes — M7V-40 is the guard, correctly |
| F21 core-tuple acceptance | M7V-20, M7V-23, Q-39 | yes |

### On R-3, since the planner asked

The planner's framing — "unit rows on hand-built traces test the fixture, not the kernel" — is the
right worry and the wrong emphasis. The division spike §5 chose is defensible and I am not
attacking it. What makes R-3 *bite* is **T-01**: the campaign is supposed to be the kernel-facing
half, and as specified it can report ten `proven` on a corpus that armed nothing. The hand-built
rows then become the only evidence that exists, while the artifact claims otherwise. Fix the
verdict semantics and assert `seeds_armed > 0`, and R-3 degrades from a structural criticism to an
ordinary sequencing note. Leave it, and R-3 is not a risk — it is the outcome.

The second-order form is worth one sentence: nothing in the plan requires a hand-built fixture to
be **realizable** by the runner. VA-1 guarantees a well-formed envelope, not a causally plausible
history. A checker tuned to an unrealizable fixture never arms in the campaign, and under T-01 that
shows as `proven`. A cheap mitigation once I1 lands: one row asserting that every fixture in
`tests/fixtures/scenarios/` and every `TraceBuilder` trace used by §4 passes the same ordering and
envelope validation the runner applies to real traces.

---

## Verdict

**FAIL** — as the developer's basis, for the rows T-01 and T-02 touch. Not as a document.

I do not say that lightly, and the reasoning is the same one I used in round 1: the plan is what
the developer converts into code, so a row that cannot pass, or that can pass for the wrong reason,
is not a style problem. Two core acceptance criteria are unmet:

- **Charter Q1's "never a pass"** is not implementable as specified. `Unavailable` has two
  incompatible definitions, four rows need the one `design.md` §2.4 forbids, and `proven` is
  vacuous because nothing asserts `seeds_armed > 0` (**T-01**).
- **Charter O1's "a deliberately bad trace trips each checker"** is unmet for INV-PUB's degraded
  path — the one the whole correction round existed for. M7V-08 can fire on the wrong rule, cannot
  resolve the pin its own design text specifies, and the one-copy fallback spec §8.3 names outright
  has no row (**T-02**).

What that does **not** mean: the structure is sound and most of the plan is better than the design
it came from. The near-miss discipline is the right call and A2 is the rule I would have asked for.
The 77-row decomposition, the §12 mechanism, the §13 checklist and the evidence rows are all
usable as they stand. Fourteen MATERIAL findings is a lot in absolute terms and small relative to
77 rows plus 7 queries — and four of them (T-06, T-07, T-08, T-22) are field-name and query
repairs, not design problems.

**Closure size:** T-01 is one paragraph in `design.md` §2.4 plus one new row plus one column in
Q-34 — and the §2.4 half is the **architect's file, not the planner's**, so this plan is blocked on
an amendment it does not own. T-02 is two fixture edits and one new row. Everything else is local.

**Recommended sequencing, since the plan is also a work order:** the developer is not blocked. §4's
rows for ATOM, AUTH, LIN (minus T-03's pair), DEDUP, VER and LAG depend on none of this and are
design §9's first work anyway. Hold M7V-02, M7V-03, M7V-08, M7V-09, M7V-19 and everything in §7 and
§9 until T-01 and T-02 close.

## Questions for the lead

1. **T-01 is the architect's to close** (`design.md` §2.4 owns the verdict enum). Route it, or
   authorise the planner to add `Unavailable{NotArmed}` to VA-2 and have the architect follow?
   My default if neither: VA-2 states it and the ADR is amended at the gate — but then two
   documents define the verdict, which is how F19 happened.
2. **T-13 needs ADR-rdb-0019 §2 to name a second artifact** (or key `values` by profile). Also the
   architect's file. Cheap, and without it F10's closure has no observable form.
3. **T-14(a)** proposes a third VA-9 command as the M7 release gate
   (`SPIKE_REQUIRE_ALL=1 RETCD_EVIDENCE=1`). That is a gate definition, not a test-plan detail —
   confirm it is verification's to write, and whether it belongs in `scripts/` (a file this team
   does not own).
4. **Planner Q-4** (`BoundaryId` set equality) is still open with foundation, and **T-12**'s
   closure adds an obligation to the generator that touches design §3.1. Both are cross-team; worth
   batching into one foundation/architect round rather than two.

---

# Critic round 3 — plan correction and design amendments

Target: `docs/testing/test-plan-m7-verification.md` at `f4f2e0d` (88 rows, M7V-78..88 new),
`design.md` §2.3, §2.4, §3.1, §4.5, §5.1, §5.1.1, §5.3, §9, §10, `trace-requirements.md` §1, §3,
§4, §6, ADR-rdb-0019 at `b0a4e58`, both handoffs' correction sections, rulings V-R16..V-R19. One
pass, 2026-09-20. Read-only; nothing edited but this file.

Method: every T-01..T-22 closure re-read against the row it names; ids, class counts and the
release-gate string counted or diffed mechanically; every event literal the new rows use checked
against **the landed C0 at `8a23b1d`** (`crates/rdb-core/src/contracts/trace.rs`, `ids.rs`), not
only against `trace-requirements.md` — that is where the largest new finding comes from.

**Verdict: PASS_WITH_RISKS** as the developer's basis for design §9's dependency-free work. Not a
clean PASS: one vocabulary seam (T-23) hits the developer on day one, one amendment leaves the
T-01 mechanism open for two checkers (T-24), and one interaction between two fixes fails five
campaign rows by construction (T-25). Detail and the hold list at the end.

---

## 1. Disposition of T-01..T-22

| # | Disposition | Basis |
|---|---|---|
| T-01 | **REVISED** | Two reasons, `Proven` requires armed, silence scoped to the `Capability` arm, fold order, `seeds_armed` load-bearing, M7V-03(b)/31/33/63 satisfiable by `NotArmed` — all landed in §2.4, VA-2, M7V-52/54/78, Q-34. Surviving gap: `armed()` after **disarm** is undefined, so INV-LIVE/INV-ISO can fold to `proven` from a corpus with zero `Proven` seeds — **T-24**. |
| T-02 | **CLOSED** | M7V-08(a) grounded + role-declared + valid recheck: only the copy-set clause can fire. M7V-08(b) pin drift catches a last-`protection_state` pin. M7V-79 is the cardinality twin; M7V-09 is M7V-08(a) with the ack from `n2` — one fact. `ReplicaRole::Primary` exists in landed C0, so M7V-79's `ack_evidence` role is buildable. Caveat: all four rows read `protection_state.quorum_rule`, a field the landed C0 does not have — **T-23**. |
| T-03 | CLOSED | M7V-17 scoped to `batch_apply.entry_digest`; M7V-19(b) says why; M7V-80 carries the kernel-facing quarantine as its own sim row (I1+F1). |
| T-04 | CLOSED | §4 convention 3; M7V-09 ↔ M7V-36 cross-referenced both ways. |
| T-05 | CLOSED | M7V-02 adds `version_check{Accept}` and M7V-41's full cycle; asserts `armed()` per checker by name. |
| T-06 | CLOSED | Stale tokens gone (`grep` for `cv=`, `capability_id`, `replication_ack{node=|from=|boot=|seq=}` finds only §15's "superseded" sentence). Watermark convention stated once. **But** the sweep was against `trace-requirements.md` §3, which itself differs from landed C0 — **T-23**. |
| T-07 | **REVISED** | Q-35's pin join now goes `publish → admission_decision → protection_state` on `correlation_id`, and the role in force is a column. The `topo` CTE is not runnable as written — **T-27**. |
| T-08 | CLOSED | VA-7 states exactly one of `coverage_cell` / `coverage_shortfall` per required cell; Q-38 reads shortfall ∪ unavailable ∪ cell. |
| T-09 | CLOSED | M7V-69 is a conjunction (rejected, cell hit, no `ack_evidence` names `n4`, INV-PUB `Proven`), `Unavailable{Capability(H1|R1)}` until they land, listed in §12. M7V-81 is the oracle half. M7V-71 and §13 map MUT-2 to both. |
| T-10 | CLOSED | M7V-51 asserts presence, distinctness, instrumentation, and `shrink_step`/`shrink_result` evidence; no duration non-zero. Q-5 aligned. |
| T-11 | CLOSED | §2: `OnceLock` in `tests/campaign/corpus.rs`, sharers named, ≤ 14 executions, < 120 s recorded not asserted; M7V-65 is 64 vs 128. Two nits in **T-31**; one real interaction in **T-25**. |
| T-12 | CLOSED | Design §3.1 and VA-6: `REQUIRED[i mod N]`, attempt ≠ hit, producer and gating tables enumerated, exclusion by capability entry only. M7V-55 three clauses; M7V-57 both branches; M7V-56 set equality against the enum (29 confirmed by counting the enum body; the 29th is `ReturningStaleOwner`). Gating-table breadth is a question — **T-35**. |
| T-13 | CLOSED | ADR §2 names `rdb-m7-campaign-release.json`, name by `cfg!(debug_assertions)`, rationale stated; M7V-62 asserts both profiles, only release cited. One advisory on how M7V-62 sees the debug file — **T-32**. |
| T-14a | CLOSED | Command 3 in ADR §2.1, VA-9, §13, M7V-87, design §5.1.1 — byte-identical in all five (`grep -cF`); `SPIKE_REQUIRE_ALL=1` in no other command. |
| T-14b | CLOSED | Design §2.4 last paragraph, ADR §2 rule 2, TR §3.18, M7V-82 (behavioural both directions + source). |
| T-15 | CLOSED | M7V-83 (F5: two-field `Budget` literal, `Heal` only a `NetworkOp`), M7V-84 (F12: subsequence check). Both fail if reverted. |
| T-16 | CLOSED | M7V-50 fails-only, retirement is deletion + ADR note, pairing both ways; M7V-85 bounds `without_rule(` to one call site. |
| T-17 | CLOSED | Counted mechanically from the tables: **88 distinct ids, none missing, none duplicated; unit 58 · sim 12 · campaign 18**. §2's inclusion lists match the class cells row for row. |
| T-18 | CLOSED | M7V-20, 51, 62, 66..68 and every new row listed with a reason. Editorial defect in the table itself — **T-29**. |
| T-19 | CLOSED | M7V-29's violating sub-case is "reachable at the same `boot_id`, i.e. never restarted". |
| T-20 | CLOSED | M7V-01 is an allowlist over every `rdb_core` path token, brace groups expanded, fails closed. |
| T-21 | CLOSED | M7V-42 stated as a static table check; M7V-44 generator half; M7V-86 runner half (sim, I1). |
| T-22 | CLOSED | Q-39 second statement over `shrink_result`; Q-40's redaction is one `DESCRIBE` over every JSONL line. |

Twenty closed, two revised, none sustained.

## 2. The lead's explicit checks

| Check | Result |
|---|---|
| Design §2.4 and VA-2 say the same thing where it matters | **Enum, two reasons, `Proven` requires armed, "never inferred from silence" scoped to `Capability`, arming-event list: identical** (compared clause by clause). **Fold order: not in VA-2**; it is in M7V-52's assertion, and that matches §2.4 exactly (T-34, advisory). Both texts share the disarm hole (T-24). ADR §2 rule 1 matches §2.4 in substance and in the `seeds_armed == 0` sentence. |
| M7V-78 fails on a corpus that arms nothing | **No.** On a correct fold such a corpus reports `unavailable(not_armed)`, `seeds_armed = 0`, and M7V-78's implication holds vacuously. The row is a runner-bug row (its synthetic half). Both handoffs say so (R-9). Only VA-9 command 3 — run by hand — fails on `not_armed`. **T-28.** |
| M7V-08 fires only on `required_copy_set_unsatisfied` | **Yes**, (a) and (b): the ack is grounded, `n3` is declared `Regular` at `config_version=4` by `topology_change`, `authority_recheck` is valid, no reads. (b) catches a checker pinning from the last `protection_state`. Two caveats: `quorum_rule` is not a landed field (T-23); the fixture carries an ack with no secondary `batch_apply`, which M7V-88 will reject (T-26). |
| M7V-79 rejects a zero-secondary-ack publish under `DegradedRf2`; twin is M7V-09 unchanged | **Yes.** Signature `regular_acks_counted=0` vs `min_regular_acks=1`; a membership-only checker passes an empty regular set vacuously and fails here. M7V-09 unchanged; it differs from M7V-79 by "n2 acked (grounded)" — one fact. Same two caveats. |
| M7V-69/81 split gives MUT-2 a kernel-side row that is `Unavailable`, never a pass, until R1/H1 | **Yes.** M7V-69 conjunction, `Unavailable{Capability(H1)}` / `{Capability(R1)}`, §12 row "H1 `ForgeAck` hook + R1". M7V-81 unit/C0. |
| Q-35/Q-38/Q-39/Q-40 runnable over TR §3 field names | **Q-38, Q-39, Q-40: yes** — every projected field is in VA-7; DuckDB accepts `FROM (DESCRIBE …)` and `GROUP BY ALL` per branch of a `UNION ALL`; Q-40's `LIKE '%_bytes'` over-matches (`_` is a wildcard) and fails closed, fine. **Q-34: yes** (+ T-30). **Q-35: no** — `topo` CTE (T-27) and `pinned.quorum_rule` (T-23). |
| Aggregate campaign budget, `OnceLock`, PR corpus extended-gate only | **Stated**: `OnceLock` in `corpus.rs`, sharers enumerated, ≤ 14 executions, < 120 s recorded not asserted; the 1,000-seed corpus never runs in the debug binary. Misnomer and one omitted sharer (T-31). The sub-N corpora collide with the coverage gate (T-25). |
| VA-9 release gate matches ADR §2.1 verbatim | **Yes**, byte for byte, five occurrences across three files. |
| 88 distinct ids; 58 / 12 / 18 | **Yes**, mechanically (`grep -oE '^\| M7V-[0-9]+ \|' \| sort -u \| wc -l` = 88; class cell = second-to-last column: unit 58, sim 12, campaign 18; the five extra `| M7V-NN |` line starts are §12 dependency rows, not test rows). |

---

## 3. New findings — defects introduced or exposed by the fixes

### MATERIAL

### T-23 — the plan's vocabulary is pinned to `trace-requirements.md` §3, which differs from the landed C0 in at least nine places; §15 says C0 "landed at `8a23b1d`" and lists none of them

- **Criterion:** §4 convention 1 ("field names are `trace-requirements.md` §3's, verbatim");
  team-rules "Evidence"; VA-3.
- **Location:** VA-1, §4 convention 1, M7V-07..M7V-11, M7V-28, M7V-30..M7V-41, M7V-46, M7V-56,
  M7V-79, M7V-81, Q-35, §12, §15; `trace-requirements.md` §1, §3.2, §3.5, §3.7, §3.14, §3.19.
- **Evidence** (`crates/rdb-core/src/contracts/trace.rs`, `ids.rs` at `8a23b1d`, working tree
  unchanged for `trace.rs`):

  | Plan / TR writes | Landed C0 has | Rows hit |
  |---|---|---|
  | header `provenance: Provenance` (TR §1, VA-1) | `TraceHeader.seed: u64`; no `Provenance` type anywhere | **M7V-46 asserts the opposite of the code**; M7V-45 |
  | `protection_state{quorum_rule=DegradedRf2 \| Rf3}` (TR §3.14) | **no `quorum_rule` field; no `QuorumRule` enum in `rdb-core`** (`grep -rni quorum` finds only doc comments) | M7V-07, M7V-08, M7V-09, M7V-79; the `quorum_rule × DEGRADED_RF2` **required cell** (M7V-55, M7V-56, Q-38); Q-35 `pinned.quorum_rule` |
  | `protection_state{state=Paused}` | `phase: ProtectionPhase{Healthy, Warn, Paused, Resuming}` | M7V-30..M7V-41, VA-2's arming list |
  | `replication_ack.peer_boot_id` (TR §3.5) | `peer_boot: BootId` | M7V-28, Q-35 |
  | `peer_role: PeerRole = Regular \| Shadow` (TR §3.5) | `peer_role: ReplicaRole{Primary, RegularSecondary, Shadow}`; **no `PeerRole`, no `Regular`** | every `peer_role=Regular` literal (M7V-07..11, 28, 81) |
  | `ack_evidence: Vec<(NodeId, BootId, PeerRole, DurabilityClass)>` (TR §3.7) | `Vec<AckEvidence{node, role: ReplicaRole, durability}>` — three fields, **no boot id** | every four-tuple literal (M7V-07, 08, 09, 79, 81) |
  | `topology_change.nodes: Vec<(NodeId, Role)>` | `Vec<(NodeId, ReplicaRole)>` — a tuple, serialised as a JSON array | Q-35 `topo` (see T-27) |
  | header `topology{nodes, config_version_0}` | `topology: Vec<TopologyEntry{node, partition, role, config_version}>` — flat, per-entry `config_version` | Q-35 header branch, VA-1 |
  | `admission_decision.reason: AdmissionReason` (TR §3.2) | `reason: Option<ErrorKind>`; no `AdmissionReason` type | M7V-56 enumerates a non-existent enum; M7V-25, M7V-39 (`reason=PROTECTION_PAUSED`) |
  | `durability_advance.sync_wal_through_prefixes` (TR §3.6) | `captured: Vec<(PartitionId, Seq)>` | no reader — cosmetic |
  | `publish.published_state_digest` withdrawn (TR §6) | present | no reader — cosmetic |

- **Consequence.** The developer's first work (design §9: hand-built traces and checkers) is
  written in a vocabulary the crate does not compile. Seven of the eleven are renames and cost
  nothing beyond a table. Two are not: **`quorum_rule`** is the field INV-PUB's degraded rule, four
  rows, a required coverage cell and Q-35's headline stand on; **`provenance`** is F18's closure and
  M7V-46 fails against the code as landed. Neither is listed in §12 as a dependency, because §15
  says C0 landed.
- **False-positive check.** Is the plan entitled to write to the *ask* rather than the code? Yes
  for asks foundation has not honoured yet — but then §12 must list those rows under "C0 +
  <ask>", and §15 must not say C0 landed without a drift list. Both cannot be true at once. Is
  `quorum_rule` derivable? Yes — `required_copy_set.len()` is 2 under `DEGRADED_RF2` and 3 under
  RF3 — but no document says the oracle derives it, and a derived cell is a different thing from
  an emitted one for the enumeration row.
- **Closure.** (1) §15 gains a drift table (field → landed shape → disposition: *plan edit* or
  *foundation ask*), and every foundation-ask row moves to §12 under "C0 + <ask>". (2) The lead
  rules on `quorum_rule`: an ask to foundation (add the field to `ProtectionState`, TR §3.14) **or**
  the oracle derives it and design §2.3 says so in one sentence, with the coverage cell keyed on
  the derived value. (3) `provenance` routed to foundation as the F18 C0 amendment; M7V-46 listed
  under it. (4) M7V-56 names the enums that exist (`ProtectionPhase`, `ReplicaRole`, `ErrorKind`'s
  admission subset, `RecoveryMode`, `AckRejectReason`, `BoundaryId`). (5) Literals updated to the
  landed names (`phase`, `peer_boot`, `RegularSecondary`, three-field `AckEvidence`).

### T-24 — `armed()` after disarm is undefined, so INV-LIVE and INV-ISO can fold to `proven` from a corpus on which every seed's verdict was `NotArmed` (T-01, surviving gap)

- **Criterion:** V-R16 / design §2.4 "`proven` with `seeds_armed == 0` is a gate failure";
  charter Q1 "never a pass".
- **Location:** design §2.4 (arming list; fold "else `proven` if at least one seed armed";
  "Disarming **is** `NotArmed`"); §2.3 INV-LIVE, INV-ISO; VA-2; VA-7 `seeds_armed` ("the number of
  seeds on which `armed()` was true"); M7V-31, M7V-33, M7V-52, M7V-78.
- **Evidence.** §2.4 defines INV-LIVE/INV-ISO's arming event as `schedule_phase{phase=Healed}`
  alone. §2.3 then *disarms* them — budget exhausted, sibling idle — with verdict `NotArmed`.
  Nothing says `armed()` returns to `false` on disarm. VA-7 defines `seeds_armed` by `armed()`; the
  fold's `proven` is "at least one seed armed". On a fault-heavy corpus most seeds heal and then
  exhaust `remaining_event_budget`: every per-seed verdict is `NotArmed`, `armed()` is `true` on
  every seed, the fold reports **`proven`, `seeds_armed = 64`**. M7V-78 passes (implication holds),
  M7V-54 and M7V-87 pass, the release artifact claims INV-LIVE and INV-ISO proven. M7V-31 and
  M7V-33 assert the verdict only; neither asserts `armed() == false`.
- **Consequence.** The T-01 vacuous pass, narrowed to two invariants — and INV-LIVE is the checker
  most likely to disarm on most seeds, so it is the likely case, not the corner.
- **False-positive check.** A developer who reads "armed" in the fold as "per-seed verdict
  `Proven`" avoids it. But the text defines armed by an event and `seeds_armed` by `armed()`; the
  literal reading is the vacuous one. A third reading (fold by verdict, `seeds_armed` by `armed()`)
  gives `not_armed` with `seeds_armed = 64` — not a false pass, but a table nobody can read.
- **Closure.** One sentence in design §2.4, mirrored in VA-2 and VA-7: "`armed()` is the checker's
  state at the end of the fold; a disarmed checker reports `armed() == false`; the fold's `proven`
  and `seeds_armed` both count seeds whose per-seed verdict is `Proven`" — which makes
  `proven ⇒ seeds_armed > 0` definitional rather than a gate. M7V-31 and M7V-33 add
  `armed() == false` after the fold. M7V-52's synthetic fold gains the case {healed-then-exhausted,
  healed-then-exhausted} → `unavailable(not_armed)`, `seeds_armed = 0`.

### T-25 — the required-cell gate fails every corpus smaller than N by construction; five campaign rows run such corpora

- **Criterion:** §2 aggregate budget (T-11 closure) and V-R19 scheduling (T-12 closure), together.
- **Location:** design §3.1 ("a scheduled boundary the environment cannot reach shows as a
  `required_missing` cell and **fails the run**"); ADR §2 rule 3 ("a named required cell with zero
  hits fails the run"); M7V-57; M7V-58 (8 seeds), M7V-61 (4), M7V-63 (16), M7V-64 (one injected
  seed), M7V-75, M7V-76 (small corpora, §2).
- **Evidence.** `REQUIRED[i mod 29]` over 8 seeds attempts 8 boundaries; 21 required cells have
  zero hits; the gate fails the run. Nothing in §2, §3.1, §6 or VA-6 scopes the gate to corpora of
  at least N seeds or to the gate commands. M7V-58's assertion ("identical per-invariant statuses
  … identical coverage counts") is about a run that has already failed.
- **Consequence.** Five rows red on day one of the campaign, and the reflex is `#[ignore]` (which
  M7V-75 forbids) or lowering the gate (A8).
- **False-positive check.** Could the gate be a property of the shared run only? Nothing says so,
  and M7V-57 asserts the *run* fails. Could the rows use ≥ 29 seeds? Then the ≤ 14-execution
  budget is ~10 × 29-seed corpora, the thing T-11 was written to stop.
- **Closure.** Design §3.1/§6 and VA-6: the required-cell gate applies when `SPIKE_SEEDS >= N`
  (always true for the default corpus and commands 2/3); a smaller corpus records coverage and
  writes `coverage_gated: false` into its report, never fails on `required_missing`. M7V-58, 61,
  63, 64, 75, 76 state it in their inputs. M7V-57 says which branch it exercises.

### T-26 — M7V-88's well-formedness rules reject fixtures the plan itself mandates

- **Criterion:** design §4.5 ("a failing fixture is fixed in the fixture; the assertion it carries
  is never weakened"); M7V-88 clause (2).
- **Location:** design §4.5 second table row; M7V-88; M7V-03(b); M7V-07, 08, 09, 10, 11, 28, 79,
  81.
- **Evidence.** (a) §4.5 requires "the `capability` block first". M7V-03(b) is a **zero-event
  trace, header only**, and asserts `Unavailable{NotArmed}` *because* no capability event exists.
  Under M7V-88 that fixture is malformed. (b) §4.5 requires "`replication_ack.contiguous_seq` never
  above the emitting node's last `batch_apply.seq`". M7V-07/08/09/10/11/28/79/81 all carry an ack
  from a secondary and name **no** `batch_apply{role=RegularSecondary}` on that node. Every one of
  them fails M7V-88 the day I1 lands.
- **Consequence.** A predictable red whose cheapest "fix" is to weaken the validator — the exact
  move §4.5 forbids — or to drop the rule for hand-built traces, which guts the row.
- **False-positive check.** Could the fixtures already carry the secondary applies? The rows'
  inputs are exhaustive by convention ("differs by exactly one fact"), and none lists one. Is the
  ack rule wrong? No — it is the right realizability check; the fixtures are what is incomplete.
- **Closure.** M7V-03(b) becomes header + ten `capability{state=Wired}` + nothing else — a
  *stronger* zero-event control (all wired, still `NotArmed`); design §2.4's "a zero-event trace"
  reads "no events after the capability block". §4 gains convention 4: every fixture with
  `replication_ack{from_node=n, contiguous_seq=s}` carries `batch_apply{node_id=n,
  role=RegularSecondary, seq=s}` before it, and `TraceBuilder::ack_from(n, s)` emits both, so no
  row author can forget. §4.5 names the same helper.

### T-27 — Q-35's `topo` CTE is not runnable, and its failure mode is a false MUT-2 (T-07, surviving gap)

- **Criterion:** "each Q-row is a runnable DuckDB query over the JSONL fields the trace actually
  emits"; the lead's check.
- **Location:** Q-35 lines `topo AS (… unnest(nodes).node … FROM ev WHERE "@m" = 'topology_change'
  UNION ALL SELECT config_version_0, unnest(topology.nodes).node … WHERE "@m" = 'trace_header')`.
- **Evidence.** `TopologyChange.nodes` is `Vec<(NodeId, ReplicaRole)>`; serde writes a tuple as a
  JSON array, so `unnest(nodes).node` is a struct accessor on a list — DuckDB binder error. The
  header branch reads an `@m = 'trace_header'` line that neither VA-7 nor TR §1 defines, with
  fields (`config_version_0`, `topology.nodes`) the landed header does not have (flat
  `topology: Vec<TopologyEntry>` with per-entry `config_version`). If the header branch silently
  returns nothing, `roles_in_force` is NULL for every ack at `config_version_0`, and
  `count(*) FILTER (WHERE acks.claimed_role IS DISTINCT FROM topo.role)` counts **every** healthy
  ack as a role mismatch — the query's MUT-2 signal fires on a correct trace.
- **Consequence.** The plan's first INV-PUB diagnostic points at forgery on every clean run.
- **False-positive check.** DuckDB can index a list (`nodes[1]`, `nodes[2]`), so the fix is local.
- **Closure.** VA-7 gains a `trace_header` line (`schema_version`, `seed`/`provenance`, `topology`
  as a list of `{node, partition, role, config_version}`). `topo` = `SELECT config_version, node,
  role FROM (SELECT unnest(topology, recursive := true) …) WHERE "@m"='trace_header' UNION ALL SELECT
  config_version, nodes[1], nodes[2] FROM (SELECT config_version, unnest(nodes) AS nodes …)
  WHERE "@m"='topology_change'`. Assertion adds "`roles_in_force` contains no NULL".

### T-28 — M7V-78 does not fail on a corpus that arms nothing, and in the PR default nothing does once packages are wired

- **Criterion:** the lead's check; charter Q1 "zero violations once kernel packages land";
  critic round 2 "On R-3".
- **Location:** M7V-78; §2 hard rule 3; §12 last row; §13 "Handoff gate … green with invariants
  `unavailable`, by design"; both handoffs' R-9.
- **Evidence.** M7V-78 asserts `proven ⇒ seeds_armed > 0` and `seeds_armed == 0 ⇒ status ∈
  {not_armed, capability, violated}`. A corpus that arms nothing gives `not_armed`/0 for all ten
  and the row **passes**. Its synthetic half catches a runner that fabricates `proven`; nothing
  catches a generator that stops producing recoveries or pauses. That surfaces only under VA-9
  command 3, which is run by hand until foundation wires it (V-R18).
- **Consequence.** A generator regression turns INV-LOSS or INV-LAG `not_armed` inside a green
  handoff gate, and stays there until someone runs the release gate and reads the table.
- **False-positive check.** The row matches V-R16's wording exactly ("proven implies
  `seeds_armed > 0` for all ten"); this is a question of reach, not of the row. R-9 is disclosed in
  both handoffs. Under V-R19 the schedule is deterministic, so a "wired ⇒ armed" claim on the
  default corpus is not a probabilistic assertion.
- **Closure (or acceptance).** M7V-78 gains clause (3): for every invariant whose needed packages
  all report `Wired` in this run, `seeds_armed > 0` on the shared default corpus; the row names,
  per checker, the scheduled boundary/op that arms it (LOSS ← a recovery-producing op, LAG ← a
  pause-producing op, VER ← a version op, …), so the claim rides on the V-R19 schedule. During M7
  the clause covers zero invariants and says so. Alternatively the lead records R-9 as accepted in
  the ledger and §13 says the handoff gate cannot detect a never-arming generator.

### ADVISORY

### T-29 — §12's dependency table is split by a paragraph; three rows render outside any table

- Lines 768–775 (the "Which reason each row reports meanwhile" paragraph) sit between table rows
  766 and 776. Markdown ends the table at the paragraph, so the rows for M7V-69, M7V-70 and
  "`proven` status for every invariant" render as pipe-separated prose. Move the paragraph below
  line 778.

### T-30 — the `reason` field has two forms and Q-34 cannot name the package

- VA-7: `reason` is "`capability` with `package`, or `not_armed`" (two fields). Artifact (ADR §2,
  design §5.3): `capability(<package>)` (one string). Q-34's assertion uses the VA-7 form and its
  projection omits `package`, so "capability points at a package owner" cannot say which. This is
  the planner's CR-1. Closure: Q-34 projects `package`; VA-7 states both surfaces explicitly, or
  the lead picks one form (my default: ADR form in the artifact, `reason` + `package` on the log
  line, and VA-7 says so).

### T-31 — three small budget-table slips

- §2's sharer list omits **M7V-60** ("default corpus with `SPIKE_ASSERT_WALL_MS` unset"); if it
  owns a corpus the ceiling is 15, not 14. Say it shares.
- §2 and M7V-65 say the 1,000-seed corpus "belongs to the extended gate only". It belongs to
  **commands 2 and 3** (the release gate); the extended gate is 10,000. The knob table's "PR
  corpus" column is the same misnomer inherited from design §5.1 — rename it "full scale
  (`RETCD_EVIDENCE=1`)" or say the 1,000-seed corpus runs only under commands 2/3 and the extended
  run.
- M7V-53's dependency reads `C0 capability event` but its input is the shared corpus (I1).

### T-32 — M7V-62 in the release gate asserts on a file another command wrote

- Under command 3 the row asserts `rdb-m7-campaign.json` **exists** — a tracked file (§14 Q-2)
  left by whichever command-1 run last ran. That is the "stale file of unknown provenance" the
  architect's two-file rationale was written to avoid. The row's real claim is the name selector.
  Restate: assert `artifact_name()` returns the debug name under `cfg!(debug_assertions)` and the
  release name otherwise (a pure function), and that *this run's* artifact carries `profile ==` the
  build profile (M7V-72 already does); drop cross-file existence.

### T-33 — M7V-87's doc cross-check needs an extraction rule

- The row reads the release-gate string out of ADR §2.1 and VA-9 at test time. The ADR also states
  command 2 in §1 wrapped across three lines. Say: the first backticked span in the table row
  beginning `| **M7 release gate**` (ADR §2.1) and `| 3 |` (VA-9); anything else is prose.

### T-34 — VA-2 omits the fold order

- The lead asked for §2.4 and VA-2 to agree word for word on the fold. VA-2 does not state it;
  M7V-52 does, and matches §2.4. Add the one line to VA-2 (or say VA-2 defers to M7V-52). Cosmetic
  now; it becomes T-24's home once T-24 closes.

### T-35 — the `BoundaryId -> Option<PackageId>` gating table has two entries; every Network and Storage boundary depends on a provider package

- `fault_injected` is emitted by the environment when it injects an op. Every `Network:` member
  (`StaleBoot` … `AckAfterRevocation`) is injected by H1's provider; every `Storage:` member by
  M1's. If I1 lands before H1/M1, those cells are `missing`, not `unavailable`, and the run fails —
  contradicting §13's "green with invariants `unavailable`, by design". The current order of
  landing is not pinned anywhere. Cheap: the gating table keys every member on the package whose
  provider emits its `fault_injected` (per family), still enumerated by M7V-56; foundation's
  handoff names the emitter beside each of the 29 members. Question 4 below.

---

## 4. Withdrawn after checking

1. **"M7V-79 uses a role `PeerRole` does not have."** `ReplicaRole::Primary` exists in landed C0
   (`ids.rs:160`); the mismatch is TR §3.7's `PeerRole`, folded into T-23. M7V-79 is buildable.
2. **"M7V-08 can still fire on grounding or role."** No: grounded, role declared at
   `config_version=4`, valid recheck; only the copy-set clause remains. Sustained as correct.
3. **"The counts are off again."** 88 / 58 / 12 / 18 confirmed mechanically; §2's inclusion lists
   match the class cells row for row; §13's block arithmetic (44+9+10+17+7+6, overlaps disclosed)
   resolves to the same 88.
4. **"The release-gate command differs somewhere."** Byte-identical in ADR §2.1, VA-9, §13,
   M7V-87, design §5.1.1. `SPIKE_REQUIRE_ALL=1` appears in no other command in any of the three
   files.
5. **"`BoundaryId` is not 29."** Counted the enum body: 29, the last being `ReturningStaleOwner`
   (an earlier `grep -A` cut it; the awk count is authoritative).
6. **"M7V-69's `INV-PUB Proven` is unreachable on a run with a rejected ack."** It is reachable —
   the scenario still publishes on legitimate acks; `Proven` needs one `publish`. Fine.

---

## 5. Verdict

**PASS_WITH_RISKS** — as the developer's basis, with a hold list.

What FAIL would have needed: a core criterion unmet with no bounded correction. T-24 is the same
species as T-01 but bounded to two checkers and one sentence; T-23 is a seam, not a design error;
T-25 is an interaction the two closures created and one scoping sentence removes. None is
unsafe, all are correctable without touching a kernel or a ruling.

**Ranked:**

1. **T-23** — day one. The drift table and the `quorum_rule` / `provenance` rulings decide what
   the developer types into `TraceBuilder`. Lead ruling + one foundation ask.
2. **T-24** — before M7V-31/33, the fold (M7V-52) and M7V-78 are coded. One sentence, two
   assertions.
3. **T-25** — before any §7 row. One scoping sentence in design §3.1/§6, six rows say it.
4. **T-26** — when `TraceBuilder` is written: the `ack_from` helper and the M7V-03(b) shape.
5. **T-27** — before the first INV-PUB row goes red.
6. **T-28** — the lead's acceptance or the clause. Not blocking.
7. T-29..T-35 in any order.

**The developer may start now** (design §9's dependency-free order, using landed C0 names):
M7V-01, M7V-02 (minus the degraded-RF2 fixtures), M7V-04..M7V-06, M7V-10..M7V-19, M7V-24..M7V-41
(with T-26's helper), M7V-42..M7V-45, M7V-49, M7V-59, M7V-66..M7V-68, M7V-71, M7V-74, M7V-77,
M7V-81, M7V-83..M7V-85; the grammar and generator.

**Hold** until the named finding closes: M7V-07, M7V-08, M7V-09, M7V-79 (T-23 `quorum_rule`);
M7V-46 (T-23 `provenance`); M7V-03(b)'s shape (T-26); M7V-31, M7V-33, M7V-52, M7V-78 (T-24); every
other §7 and §9 campaign row (T-25); M7V-56's enum list (T-23).

## 6. Questions for the lead — each with my default

1. **`quorum_rule`** (T-23): ask foundation to add it to `ProtectionState` (TR §3.14 as written),
   or rule that the oracle derives it from `required_copy_set.len()` and design §2.3 says so?
   **Default: derive.** It removes a field with one reader, keeps the four rows and the cell, and
   costs one sentence — the cell is then "derived quorum rule × DEGRADED_RF2", named as such.
2. **Header `provenance`** (T-23): route F18 to foundation as a C0 amendment now, or let M7V-46
   wait in §12? **Default: route now**; it is a seam-freeze item, and M7V-46 sits under
   "C0 + provenance" meanwhile.
3. **T-28**: add the "wired ⇒ `seeds_armed > 0` on the default corpus" clause to M7V-78, or record
   R-9 as accepted? **Default: add the clause** — deterministic under V-R19, one assertion.
4. **T-35**: gating table per family, keyed on the emitting provider package? **Default: yes**;
   foundation's handoff names the emitter beside each `BoundaryId` member.
5. **Planner CR-1 / CR-2**: `reason` form and `catching_row` list-valued. **Defaults: ADR form
   `capability(<pkg>)` in the artifact, `reason` + `package` on the log line (VA-7 says both);
   `catching_row` list-valued, no ADR key change.**

---

# Critic round 4 — architect round 3 and planner round 3, one pass

Targets: ADR-rdb-0019 `b0a4e58..37e85a5`; `design.md` §2.3/§2.4/§3.1/§4.5/§5.3/§6 and
`trace-requirements.md` §3.14/§8/§8.1 read directly; the plan `f4f2e0d..1114bc7` (90 rows).
Binding rulings V-R20 and V-R21 taken as settled and not re-litigated. Every event literal was
re-checked against **landed C0 at `8a23b1d`**, not against either scratchpad file.

**Verdict: PASS_WITH_RISKS. The verification developer may start.** T-36 and T-37 are plan-text
defects, not design defects; neither blocks the rows in the start list.

## 1. T-23..T-35

| # | Disposition | Basis |
|---|---|---|
| T-23 | **CLOSED** | §15's drift table exists, 18 rows, disposition per row. Each landed shape re-verified at `8a23b1d`: envelope `TraceEvent{event_id, logical_tick, partition, node, boot, correlation}`; `ClientSubmit.affinity: u64`; `ReadRequestKind{Read, Status, Export, ActorRead}`; `ClientOutcome{Success, RecoveredApplied, Error(ErrorKind)}`; `VersionCheck.mandatory_unknown_fields: Vec<u16>`; `RequestIdentity{tenant, client, request}`; `ErrorKind` carries `ProtectionPaused`, `RequestIdReuse`, `CrossAffinity`, `GenerationChanged`, `UnknownOutcome`, `StatusExpired`; `Publish` has no `config_version`; `ProtectionState.phase`. `QuorumRule`, `Provenance`, `AdmissionReason` and `OpSkipped` still do not exist in `rdb-core` — and no row now assumes they do. Two foundation asks survive, both scoped and both parked in §12: `provenance` (TR §8.1, a drafted enum) and `op_skipped`. Residuals split out as T-38 and T-41 |
| T-24 | **CLOSED** | design §2.4 states `armed()` as end-of-fold, not a latch, and the fold paragraph makes `proven` and `seeds_armed` count the same set, so `proven ⇒ seeds_armed > 0` is definitional. VA-2 carries it; M7V-31 and M7V-33 assert `armed() == false` after disarming; M7V-52's synthetic fold includes the healed-then-exhausted pair and fails a fold that reads `armed()` mid-run. The corpus that folded to `proven`/64 from zero `Proven` seeds can no longer be built |
| T-25 | **CLOSED** | ADR rule 3, design §3.1 and §6 scope the gate to `SPIKE_SEEDS >= N`; `coverage_gated` lands in `rdb-m7-coverage.json` and on the `campaign_run` line. M7V-57 exercises both sides at N and N−1 and fails a gate that fires below N. All six sub-N rows (58, 61, 63, 64, 75, 76) now own `coverage_gated: false` in their inputs. `coverage_gated: true` with non-empty `required_missing[]` is named as the only failing combination |
| T-26 | **CLOSED** | M7V-03(b) is header + ten `capability{state=Wired}` — a stronger control than the old zero-event trace, and well-formed under §4.5. `TraceBuilder::ack_from(n, s)` is in VA-1, §4 convention 4 and design §4.5; all eight ack-carrying rows (07, 08, 09, 10, 11, 28, 79, 81) are rewritten through it, and M7V-79 says in words that the *absence* of the ack is the point so the helper is not reached for |
| T-27 | **CLOSED** | Q-35 rewritten. `topo` now unions the header's struct list with `topology_change`'s tuples indexed `n[1]`/`n[2]`; a `trace_header` line is added to VA-7 so the first branch has a source; `unresolved_roles` is counted separately and asserted `= 0`, and `role_mismatches` is filtered on `topo.role IS NOT NULL`, so a NULL join can no longer read as MUT-2. `NodeId(u32)` is a `dense_id!` newtype, so serde writes a bare integer and the `::INTEGER` cast binds. One claim I did not execute: `unnest(topology) AS t` followed by `t.config_version` in the outer select relies on struct-field access against an unaliased subquery. It is the query's only unverified step; run it once against a real log before trusting the column |
| T-28 | **CLOSED** | M7V-89 is the row, ADR rule 1 carries the clause, and V-R21's exclusion is a named const `WIRED_IMPLIES_ARMED_EXCLUDED` printed with its reason in every run — so the exclusion cannot quietly grow. M7V-78 keeps the unexcluded `proven ⇒ armed` half. During M7 M7V-89 reports `unavailable (no invariant fully wired)`, never a pass. Residual in T-38 |
| T-29 | **CLOSED** | the paragraph now sits below the §12 table; the table is unbroken and the three rows that had rendered as prose are rows again |
| T-30 | **CLOSED** | VA-7 gives the log line `reason` + `package`; the ADR and design §5.3 give the artifact the one string `capability(<pkg>)`; all three call it a bijection. Q-34 projects `package` and asserts it is non-NULL exactly when `reason='capability'` and names a package whose `capability_seen.state` is `Unavailable` in the same run |
| T-31 | **CLOSED** | M7V-60 is in the sharer list; the knob-table column is now "Full scale (`RETCD_EVIDENCE=1`, VA-9 command 2)" and M7V-65 says "full-scale corpus", the misnomer named and corrected in place; M7V-53's dep names the shared corpus |
| T-32 | **CLOSED** | M7V-62 asserts the name selector as a pure function of the profile plus this run's own artifact, reads no other command's file, and is struck from §12 |
| T-33 | **REVISED** | the extraction rule exists and is precise — but it is unsatisfiable on this file. See **T-36** |
| T-34 | **CLOSED** | VA-2's fourth bullet carries the fold order; it matches design §2.4's paragraph and M7V-52's assertion word for word |
| T-35 | **CLOSED** | ADR rule 3 keys every required cell on its emitting provider package per fault family; design §3.1 supplies the provisional map (Network/Time/Control → H1, Storage → M1, Client/Recovery → I1) and says foundation's handoff is authoritative; M7V-56 asserts one package per family. The "I1 first ⇒ every Network and Storage cell missing" red is gone. Residual in T-40 |

12 closed, 1 revised, **0 sustained**.

## 2. New findings

### MATERIAL

**T-36 — M7V-87's extraction rule fails by construction against this plan file.**
Criterion: a row must be able to pass on a correct artifact. Location: M7V-87 clause (1), plan
§7; the collision is plan line 219 and plan line 998. Evidence: the rule reads the command from
"the table row whose line begins ... `| 3 |` (VA-9 in this plan)" and says "A file with zero or
**two** matching rows fails the row rather than picking one." `grep -c '^| 3 |'` on the plan at
`1114bc7` returns **2**: line 219 is VA-9 command 3, and line 998 is `| 3 | `protection_state{state=…}` | …`,
the third row of the §15 drift table — which the same commit introduced. Its first backticked span
is not a command. Consequence: M7V-87 is red on a correct plan, and the reflex fix is to delete a
drift row. False-positive check: the ADR side is fine — exactly one line begins
`| **M7 release gate**`. Closure: narrow the plan-side selector to something the drift table cannot
match, e.g. the line beginning `| 3 | **The M7 release gate**`, or the row's position inside the
VA-9 table rather than its first cell.

**T-37 — the three surfaces disagree on which artifact carries `coverage_gated`, and design §5.3
still carries two superseded literals (the planner's R-12, confirmed).**
Criterion: VA-7 and design §5.3 must name the same fields. Location: plan M7V-72; design §5.3;
ADR §2 artifact table. Evidence, three disagreements:
(a) M7V-72 asserts the **campaign** artifact's `values` carry "every key ADR-rdb-0019 §2 names",
and lists `coverage_gated` among them. ADR §2 puts `coverage_gated` in `rdb-m7-coverage.json`
only; design §5.3 likewise. M7V-72 therefore asserts a key the ADR does not put in that file —
M7V-73 is where it belongs, and M7V-73 already asserts it.
(b) design §5.3 still writes `mutations{id -> caught_by}`. The ADR and M7V-72 write
`mutations{id -> catching_row}`, list-valued under V-R20 (5). One of the two names is dead.
(c) design §5.3 still lists a campaign key `coverage{required_cells, hit_cells, missing[]}` that
neither the ADR's key list nor M7V-72 carries, and whose `missing[]` is spelled `required_missing[]`
everywhere else.
Consequence: M7V-72 is the row that goes red, and a developer reading design §5.3 writes
`caught_by`. False-positive check: I read the ADR at `37e85a5`, not `b0a4e58`; the campaign row's
key list genuinely has no `coverage_gated`. Closure: drop `coverage_gated` from M7V-72's list
(keep it in M7V-73); design §5.3 adopts `catching_row` and either drops the `coverage` key or the
ADR adds it.

**T-38 — M7V-89's arming-op table contradicts design §2.4's, and its INV-DEDUP entry is wrong.**
Criterion: the row whose failure message names the arming op must name the right one. Location:
plan M7V-89 Input; design §2.4 arming list and wired-clause list. Evidence: M7V-89 says
"INV-ATOM/INV-LIN/INV-DEDUP ← any `ClientOp::Submit` (**every seed**)". Design §2.4 arms INV-DEDUP
on "a second `client_submit` with a retained identity" and its wired-clause list names "the
`RetainedDedupHit` boundary" — which is a real `BoundaryId` member (verified in the 29). A first
submit does not arm INV-DEDUP, so "every seed" is false: under `REQUIRED[i mod 29]` only two or
three of the 64 seeds are *scheduled* to produce it. M7V-89 would also misattribute INV-PUB (plan:
a publish; design: the first `Submit`) and INV-AUTH (plan: a grant decision; design: the first
`Submit`). Consequence: when INV-DEDUP goes `not_armed`, M7V-89 points the developer at client
submits, which every seed has. False-positive check: design §2.4's own two lists agree with each
other on INV-DEDUP; the plan is the outlier. Closure: M7V-89 cites design §2.4's list instead of
restating it, or restates it verbatim.

### ADVISORY

**T-39 — design §2.3 adds the INV-PUB rule `required_copy_set_shape` and no row tests it.**
`grep -c required_copy_set_shape` on the plan returns **0** (`quorum_rule_mismatch`, added in the
same ruling, has 3). Q-35 calls the same condition "a fixture or cadence defect", not a violation,
so the two documents disagree on what a length-1 or length-4 `required_copy_set` is. §13's own
criterion is a bad trace and a valid trace per invariant; this is a new rule with neither. Closure:
one sub-case on M7V-07 or one sentence in design §2.3 demoting it to a fixture check, and Q-35's
assertion aligned.

**T-40 — the `BoundaryId -> FaultKind` family map has no ground truth, so M7V-56's family
assertion checks a table against itself.** Verified at `8a23b1d`: `BoundaryId` has 29 members,
`FaultKind` has 6, and there is **no `impl BoundaryId`** and no `fault_kind()` — the family
assignment exists only in foundation's handoff and in verification's own `coverage.rs`. A member
filed under the wrong family passes M7V-56 and silently inherits the wrong gating package, which
is the "excluded by capability, never by editing the list" loophole re-entered by a different
door. It bites only when the wrong package is also `Unavailable`. Closure needs no contract change:
`FaultInjected` already carries `fault_kind` beside `boundary`, so add to M7V-42 or M7V-55 — for
every observed `fault_injected`, `fault_kind` equals the family the gating table assigns to
`boundary`.

**T-41 — four cross-reference and vocabulary slips.**
(a) design §2.4's wired clause says "the **M7V-78** row asserts over the nine others"; the plan
puts that clause in **M7V-89** and states M7V-78's clause has no exclusion. One reference is stale.
(b) design §4.5 hard-codes `ack_from` to emit `role=RegularSecondary`; plan convention 4 allows
`role=Shadow`, and M7V-07 uses it. Design should say the emitter's declared role.
(c) TR §3.14's cadence sentence still reads "even when `state` is unchanged" after the field was
renamed `phase` in the same section.
(d) M7V-29's prose says `boot_id` twice; the landed envelope field is `boot` and the row's own
literals are already correct.

## 3. Mechanical checks

| Check | Result |
|---|---|
| Distinct row ids | **90**, `M7V-01..M7V-90`, none missing |
| Row lines vs ids | 97 lines beginning `| M7V-NN |`; the 7 extras are all §12 dependency rows (lines 859–870), not second test rows |
| Class counts | unit **59** · sim **12** · campaign **19** — matches §2 and §13 |
| Release-gate command | byte-identical: plan 3 occurrences (VA-9 row 3, M7V-87, §13), ADR §2.1 1, design §5.1.1 1 |
| `SPIKE_REQUIRE_ALL=1` | in no other command |
| `BoundaryId` | 29 members, last `ReturningStaleOwner`; `RetainedDedupHit` present |
| Absent from `rdb-core` at `8a23b1d` | `QuorumRule`, `Provenance`, `AdmissionReason`, `OpSkipped` — and no row assumes any of them |

## 4. Start and hold

**Developer may start now** (design §9 order, landed C0 names, `TraceBuilder::ack_from` first):
M7V-01..M7V-19 including M7V-03(b) in its new shape; M7V-24..M7V-41; M7V-42..M7V-51;
M7V-56, M7V-57, M7V-59, M7V-66..M7V-68, M7V-71, M7V-74, M7V-77, M7V-79, M7V-81..M7V-85; the
grammar, the generator with `REQUIRED[i mod 29]`, and the reducer. Newly unblocked since round 3:
**M7V-07/08/09/79** (derived rule), **M7V-52/78** (fold defined), **M7V-03(b)** and the eight ack
rows (convention 4), and every sub-N campaign row (`coverage_gated: false`).

**Hold:**
- M7V-46 header half, M7V-22 and M7V-88's `op_skipped` clause — the two open foundation asks, §12.
- M7V-90 — K-F-07, by design.
- **M7V-72** until T-37 settles which artifact carries `coverage_gated`.
- **M7V-87** until T-36's plan-side selector is narrowed.
- **M7V-89's arming table** until T-38 is fixed; the row's structure is sound, only its op list is wrong.
- Everything I1-dependent, per §12 unchanged.

## 5. Questions for the lead (defaults; none blocking)

1. T-36's selector: narrow to `| 3 | **The M7 release gate**`, or drop the drift table's numeric
   first column? **Default: narrow the selector** — the drift row numbers are load-bearing for
   reading TR §8 side by side.
2. T-37: does `coverage_gated` belong in the campaign artifact too? **Default: no** — coverage
   artifact only; drop it from M7V-72's list.
3. T-39: is a non-{2,3} `required_copy_set` a violation or a fixture defect? **Default: violation**
   (design §2.3 already says so); add one sub-case to M7V-07 and align Q-35's wording.
