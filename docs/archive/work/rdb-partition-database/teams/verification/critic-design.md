# Team verification — critic, round 1 (2026-09-20)

Target: `teams/verification/design.md`, `trace-requirements.md`, `research.md`,
`architect-handoff.md`, `docs/ADRs/rdb/0019-validation-gates-evidence-and-release-boundary.md`.
Authority: spike §4 §6 §7, `docs/rdb/validation-plan.md`, spec §5.2 §5.3 §5.4 §6.2 §6.3 §8.1–8.4,
charter, team-rules. Lead rulings V-R1..V-R7 treated as settled; V-R4 is answered in F3.

**Verdict: FAIL.** Three BLOCKERs, nine MATERIAL, six ADVISORY.

The architecture survives. D1–D5 are the right five decisions and I am not asking for a redesign.
Every BLOCKER closes with an edit to §2.3's invariant text plus two field asks in
`trace-requirements.md`. What fails is the **invariant set**, not the shape of the machine: as
written, the oracle would pass a kernel that publishes on one ACK while running degraded (F1), and
it contains a reimplementation of F1's prefix selection that the charter explicitly excludes (F2).
Both would be baked into the test plan and then into code if this went to the test planner now.

Credit where it is earned, so the criticism is legible:

- §2.4's `Proven` / `Unavailable` / `Violated` verdict, with the gate in the evidence check rather
  than the exit code, is the best idea in the document. It is the correct reading of ADR-0031's
  `full_scale:false` mechanic and it kills the single most dangerous false green in M7.
- §8's no-linearizability-checker argument is sound and correctly derived, not borrowed. Spec §5.2
  ("one admitted transaction in flight per partition") plus a trace that *declares* the publication
  order really does remove the search. `research.md` §5 flags the derivation as uncited; I attacked
  it and it holds. Withdrawn as a finding.
- §2.2 making oracle independence a **test row** rather than a review note is the correct lesson
  from ADR-0031's closing note. Keep it exactly as is.
- Refusing madsim/turmoil (`research.md` §2) is right. Those crates exist to make async IO code
  deterministic; the kernel contract already removed that problem.

---

## BLOCKER findings

### F1 — INV-PUB's ACK rule is wrong under degraded RF2, and V3 is claimed on it

- **Criterion violated:** spec §8.3 — "While running with two copies, both are required for every
  successful transaction. There is no one-copy fallback or two-second local-only allowance."
  `validation-plan.md` V3 pass threshold — "degraded writes require both survivors; loss of either
  stops writes." ADR-rdb-0019 §1 claims V3 **simulated** at M7.
- **Location:** `design.md` §2.3, row INV-PUB; `design.md` §2.1 (model table).
- **Evidence:** INV-PUB's rule is stated as "A `publish` at `seq` requires a preceding
  `replication_ack` at `>= seq` from a **regular** peer." Singular. That is spec §5.2 step 6, which
  is the *healthy RF3* rule. The oracle's model in §2.1 carries no membership and no
  `config_version`, so the checker has no way to know the partition is in `DEGRADED_RF2` (spec
  §8.3) and must require two ACKs. The grammar can reach that state easily — `StorageOp::Crash` on
  a secondary plus `RecoveryOp::Synchronize` is a two-op path — so this is a state the campaign
  will reach and pass through on a kernel that is wrong.
- **Consequence:** the exact bug the spec spends a paragraph forbidding — a one-copy fallback while
  degraded — is invisible to the only judge M7 has. The ADR then prints "V3 simulated."
- **False-positive check:** could kernel-b's F1/R1 rows carry it instead? kernel-b `design.md` §7
  gate map, V8 row, says "no success without a qualifying regular secondary ACK" and §4.4's
  `no_qualifying_secondary` arm is boolean (`last_qualifying_ack: bool`) — it counts *whether one
  exists*, not *whether the required set is satisfied*. So no, it is not covered elsewhere, and
  kernel-b's L1 has the same singular-ACK shape. The finding stands and widens.
- **Closure:** INV-PUB's quorum predicate becomes "the `required_copy_set` pinned by the
  `config_version` in force at `admitted_seq`", not "one regular peer". §2.1's model gains one
  field: `required: config_version -> (Set<NodeId>, quorum_rule)`. `trace-requirements.md` §3.14
  must state that `protection_state` is emitted **on every `config_version` change**, not only on a
  protection-state transition, or the oracle cannot know the current required set. This also
  rescues `admission_decision.required_copies` from F13's dead-field list.

### F2 — INV-LIN's "longest validated compatible prefix" clause is a second implementation of F1

- **Criterion violated:** charter SCOPE/EXCLUSIONS — "any second implementation of the protocol
  inside the oracle" is out. Spike §6 — "It must not import T1/R1/F1 algorithms." This is also the
  architect's own R-3 risk, so it is fair game.
- **Location:** `design.md` §2.3, row INV-LIN, final clause.
- **Evidence:** the clause reads "`recovery_decision.selected_cutoff_seq` must be `<=` the longest
  **validated compatible** reported prefix." To evaluate "longest validated compatible", the
  checker must take `queried_sources[].{reported_generation, reported_seq, reported_digest}` and
  decide which survivors are ancestry-compatible with each other. That is F1's selection algorithm
  — compare kernel-b `design.md` §5.4 "Selection" and §5.3 "the typestate that makes 'longest wins'
  unrepresentable". Two implementations of the same rule agreeing proves only that two codebases
  agree, which is the failure mode spike §6 names.
- **False-positive check:** is it merely *declaration comparison* in disguise? No — compatibility
  between two survivors' reported digests is not declared anywhere in the trace. The kernel declares
  its *conclusion* (`selected_source`, `mode`), not the pairwise compatibility relation. The oracle
  would have to derive it.
- **Closure, and it is cheap:** the oracle already keeps `lineage: Vec<(generation, seq,
  entry_digest, predecessor_digest)>` built from `batch_apply` events (§2.1). That map *is* the
  ground truth, recorded, not recomputed. Restate the clause as two lookups:
  1. `selected_cutoff_seq <= reported_seq` of `selected_source`, and
  2. no source with `reachable=true` reported `(generation, reported_seq)` where `reported_seq >
     selected_cutoff_seq` **and** `reported_digest` equals the `entry_digest` the oracle already
     recorded at that `(generation, seq)`.
  Clause 2 is a hash-map lookup against recorded facts, not a compatibility algorithm, and it still
  catches "chose a shorter prefix than an available compatible one". A source whose reported digest
  does **not** match the recorded one must correlate with `mode=quarantine` — which is the existing
  digest-conflict clause, unchanged.

### F3 — the oracle trusts kernel-computed labels, so a whole bug class is invisible; MUT-2 provably cannot cover it (answers V-R4)

- **Criterion violated:** spike §5, O1 acceptance — "deliberately bad traces trigger each checker."
  Spike §4 kernel seams, replication row — "only verified regular ACK can qualify; shadows never
  qualify." Spike §7 — the mutation list exists to prove the *fault class* is caught.
- **Location:** `design.md` §2.1 ("It compares declared facts against each other"), §7 rows MUT-2
  and MUT-5, §7.1.
- **Evidence:** `replication_ack.peer_role` and `replication_ack.durability_class` are *computed by
  the component under test*. The oracle reads them and compares them only against other kernel
  declarations. Therefore:
  - **MUT-2 as designed is a tautology for the interesting bug.** Flipping `peer_role` from
    `shadow` to `regular` in a recorded trace proves INV-PUB reads the field. But the real bug —
    R1 resolves `peer_role` from the *ack message* rather than from configured topology — produces
    a trace in which that shadow is labelled `Regular` **everywhere**, self-consistently. INV-PUB
    passes it. The trace mutation tests the checker against a fault the kernel would never produce
    in that shape.
  - **Same for MUT-5's underlying label.** INV-LOSS does cross-check `durable_seq` against the
    `durability_class` of recorded ACKs, which is good. But if M1/R1 mislabel a *buffered* ACK as
    `Durable`, every cross-check agrees and the trace is clean. There is no event in
    `trace-requirements.md` that grounds `Durable` in an actual flush.
- **The cheap fix, and why it is cheaper than the architect thinks:** ground both labels in facts
  the **environment** owns, not the kernel.
  - INV-PUB resolves `peer_role` from the trace header's `topology` (already requested,
    `trace-requirements.md` §1), keyed by `node_id`. `replication_ack.peer_role` becomes a
    *cross-checked* field: a mismatch between the ack's claimed role and the topology's role is
    itself a violation. Zero new fields. ~5 lines.
  - Add one invariant clause (see F6): a `durability_class=Durable` ACK at `seq` on node `n`
    requires a preceding `durability_advance{node=n, outcome=Synced, durable_seq >= seq}`.
- **V-R4 answer — yes, add the sim-dispatcher mutation for MUT-2 and MUT-5, and it costs almost
  nothing new, because both are already required capabilities:**
  - Spike §4 transport seam: "forged identity is injectable **and rejected**." An op that delivers
    an ACK claiming a role/identity the topology does not grant is a *required* transport
    capability, not a test-only hack. It belongs in `NetworkOp`, e.g.
    `NetworkOp::ForgeAck { msg, claimed_role, claimed_node }`.
  - Spike §6 storage required boundary cases: "**no false durable watermark**." An op that reports
    a flush completion M1 never performed is a *required* storage fault, not a mutant. It belongs
    in `StorageOp`, e.g. `StorageOp::FalseDurable { node, through }`.
  These satisfy V-R4's hard constraint exactly — no `cfg` branch in kernel code, ever; the mutation
  lives in the fake providers the sim already owns. They also test the strictly stronger claim
  ("kernel + oracle rejects it") rather than the weaker one ("the checker reads the field"), and
  they are the *only* way to exercise the F3 blind spot, because they make the kernel produce a
  self-consistent-but-wrong trace. Cost: two enum variants, two provider arms, two rows. The ask on
  foundation is a hook in H1's network and M1's flush path, which spike §4/§6 already require them
  to build.
  Keep MUT-1, MUT-3, MUT-4 as trace rewrites — those three genuinely are pure checker tests and
  §7.1's honest boundary statement is correct for them. Move MUT-2 and MUT-5 to dispatcher level.
- **False-positive check:** does this smuggle a second protocol into the oracle? No. Reading
  `topology` from the trace header is reading a declared input, not deriving a decision. And
  `durability_advance` is already in the vocabulary; the new clause is an ordering check between two
  declared events on one node.
- **Closure:** §7 table splits into "trace rewrites (MUT-1, MUT-3, MUT-4)" and "injected faults
  (MUT-2, MUT-5)"; §2.3 INV-PUB grounds `peer_role` in `topology`; §2.3 gains the durability-grounding
  clause; the two ops are added to `design.md` §3's `NetworkOp` / `StorageOp` rows; the handoff adds
  the H1/M1 hook request to the routing list in §8.

---

## MATERIAL findings

### F4 — ddmin signature slippage: the minimized fixture can green while the real bug ships

- **Criterion violated:** spike §5, G1 acceptance — "Failure retains its signature after shrinking."
  Charter G1 — "a seeded failure keeps its signature after shrinking."
- **Location:** `design.md` §4.4 (Signature), §4.3, §5.3, §8 last row.
- **Evidence:** `Signature { checker, rule, partition, role, event_kind }`. That tuple is coarse
  enough that two *different root causes* routinely share it. Worked example, both reachable in this
  grammar: an F1 bug (recovery selects a cutoff whose digest does not match the root) and an R1 bug
  (a duplicate `Deliver` admits a successor twice) both produce
  `INV-LIN / predecessor_digest_mismatch / partition 0 / Primary / batch_apply`. ddmin deletes the
  `RecoveryOp` block, the candidate still fails with an identical signature via the R1 path, ddmin
  accepts it and keeps deleting. The emitted `fixtures/regressions/*.json` reproduces the R1 bug.
  Fix R1, the fixture goes green, the F1 bug ships. This is Zeller's own "slippage" failure and the
  design excludes precisely the fields (`seq`, node ids, scenario length) that would discriminate.
  §4.2's per-op tick shrinking (F12) is a second slippage channel with the same signature.
- **Aggravating:** §5.3 writes the *original* scenario only to `validation/<run-id>/`, which V-R6
  makes gitignored and per-invocation. §8's last row commits only the minimized fixture. So the
  original — the only artifact that definitely reproduces the real defect — is deleted by design.
- **False-positive check:** does re-running the whole corpus catch it? Only if the original seed is
  still in the corpus and still fails, which is true today (`SPIKE_SEED_BASE=0`, superset corpora —
  a genuinely good decision) but is not what the regression fixture claims to do, and stops being
  true the moment a generator version bumps.
- **Closure:** (a) add `faults: BTreeSet<BoundaryId>` (the `fault_injected.boundary` cells active in
  the failing run) to `Signature` — those are stable under op deletion in the way `seq` is not, and
  they are the cheapest available proxy for "same causal path"; (b) commit the **original** scenario
  alongside the minimized one, `fixtures/regressions/<slug>.orig.json`, and replay both in
  `regressions.rs`. Cost: one file per failure, a few KB.

### F5 — `heal_at_event` is an absolute event index, and the reducer changes the event stream length

- **Criterion violated:** spike §6 controlled liveness — "Liveness checks require an explicitly
  healed, fair delivery schedule ... bounded event count." `design.md` §4.3's own claim that
  shrinking cannot change the failure class.
- **Location:** `design.md` §3.2 (`Budget { max_events, max_ticks, heal_at_event }`), §4.2, §2.3
  INV-LIVE.
- **Evidence:** `heal_at_event: Option<n>` is an index into the **event** stream; ddmin deletes
  **ops**, which shortens that stream. `Budget` is never shrunk (§4.2 shrinks `Scenario::ops` and
  per-op fields only) and nothing rebases `heal_at_event`. Two concrete outcomes:
  - the heal point lands past the end → INV-LIVE disarms → the candidate "passes" → ddmin keeps an
    op that is not actually needed, silently degrading minimization; and worse,
  - the heal point lands *earlier* relative to the remaining work → INV-LIVE arms over a window it
    was never armed over → a **new** liveness failure appears on a correct kernel, with the same
    `checker`/`event_kind` → ddmin accepts a candidate whose failure is an artifact of the reducer.
- **False-positive check:** is `heal_at_event` measured in ops rather than events? `design.md` §3.2
  says "from event n the schedule is healed"; §3 lists it inside `Budget`, not as a `ScenarioOp`.
  So no. If foundation implements it as an op index, the finding collapses — which is the point:
  it is unstated.
- **Closure:** make healing an **op**, `ControlOp::Heal` / a dedicated `ScheduleOp::Heal`, so ddmin
  moves it with the list and `fault_injected.scenario_op_index` stays meaningful. `NetworkOp::Heal`
  already exists — reuse it and delete `heal_at_event` from `Budget` entirely. This removes code and
  removes the defect.

### F6 — "no false durable watermark" (V1's third clause) has no checker and no generator op; ADR §1's V1 row over-claims

- **Criterion violated:** `validation-plan.md` V1 pass threshold — "Zero partial transactions; every
  recovered value belongs to declared contiguous lineage; **no false durable watermark**." Spike §6
  storage required boundary — "before/after each atomic boundary; **no false durable watermark**."
  Spike §6 — "A test must never use 'durable' as an alias for in-memory application."
- **Location:** `design.md` §2.3 (no invariant covers it), §3 `StorageOp` required-boundary column,
  ADR-rdb-0019 §1 row V1.
- **Evidence:** `design.md` §3's `StorageOp` row narrows the spike's boundary to "`Flush` that
  errors and must advance no watermark" — the easy half. The hard half, a flush that *reports
  success* for data never synced, has no op. And no invariant states the grounding rule at all:
  INV-LOSS reads `durability_class` but never checks it against a flush. ADR-rdb-0019 §1 then claims
  V1 "claimed, simulated" with all three clauses implied.
- **False-positive check:** does M1's own crash-image tests cover it? M1's spike §5 acceptance is
  "Every injected boundary yields whole batch or none; snapshot never observes partial state" — that
  is atomicity, not watermark honesty. Not covered.
- **Closure:** add invariant clause (F3's second bullet) + `StorageOp::FalseDurable` (F3) + the
  `durability_advance{node, outcome, durable_seq}` ordering row; or amend ADR-rdb-0019 §1's V1 Form
  cell to say "clauses 1–2 only; watermark honesty pending M8". The ADR's own §4.4 ("a simulated
  gate never upgrades itself") makes the unqualified row inconsistent with the document's thesis.

### F7 — the scenario grammar has no partition axis, so two named acceptance rows are unreachable

- **Criterion violated:** spike §7 safety-check table, row "Multi-partition isolation — blocked
  partition does not block other partition progress under fair scheduling." Spike §5, P1 acceptance
  — "post-apply timeout **freezes only its partition**." Spec §5.2 — "Other partitions in the set
  keep running." Spec §5.3 — "Freeze that partition's normal read/write queue until the transaction
  is resolved."
- **Location:** `design.md` §3 — `Scenario { topology: Topology, ... }`, `Topology { nodes, roles,
  shadows, config_version }`. No partition count, no per-partition op targeting; `ClientOp::Submit`
  carries `keys`, not a partition. §2.1's model is "per partition" but nothing in the grammar
  produces more than one.
- **Evidence:** the envelope carries `partition_id` (`trace-requirements.md` §2) and `Signature`
  carries `partition`, so the *trace* is multi-partition-ready and the *generator* is not. There is
  no invariant for isolation and no coverage cell for it in §6.
- **False-positive check:** is isolation kernel-a's P1 row rather than verification's? P1's
  acceptance is a package row, so kernel-a owns the assertion — but kernel-a's tests run scenarios
  from **this** grammar (charter: "kernel teams supply the behaviour under test"). A grammar that
  cannot express two partitions blocks their row too. It is verification's gap either way.
- **Closure:** `Topology` gains `partitions: u8` (2 is enough); `ClientOp` variants gain a
  `partition: PartitionId`; add INV-ISO ("while partition A has an unresolved transaction, a
  `client_outcome` for partition B may still occur", armed only under `schedule_phase{healed}` like
  INV-LIVE); add one required coverage cell. If the lead rules this out of M7, ADR-rdb-0019 must say
  so, because spike §7 lists it in the safety table alongside V1/V2/V8/V12.

### F8 — INV-LAG under-specifies resume, is never disarmed, and duplicates L1's state machine

Three defects in one row; V-R3 (INV-LAG stays in the oracle) is **not** re-litigated here — only
what the checker asserts.

- **Criterion violated:** spec §6.2 Resume contract — "All configured regular copies durable through
  paused prefix; **lag below 250 ms for 5 s**." Spec §6.2 — "If no regular secondary can ACK,
  success stops immediately." Spike §6 — liveness-shaped checks need an explicitly healed schedule.
- **Location:** `design.md` §2.3, row INV-LAG.
- **Evidence:**
  1. **Missing 250 ms clause.** INV-LAG says "`resuming -> healthy` only after a
     `durability_advance` reaching exactly `resume_barrier_seq` plus 5 s of healthy hysteresis." The
     lag-below-250 ms condition is absent. kernel-b encodes it (`Thresholds { resume_lag_ms: 250,
     resume_hold_ms: 5000 }`, §4.1) and its `Reprotecting` arm restarts the hold when
     `age >= resume_lag_ms`. The oracle as written passes a kernel that resumes with 1 s of lag.
     Note `validation-plan.md` V8 also omits it; team-rules authority order puts the spec above the
     validation plan, so the spec's 250 ms binds.
  2. **Missing the immediate-pause arm.** Spec §6.2's `Healthy --> Paused: no secondary can ACK` is
     age-independent. INV-LAG only checks the age ladder.
  3. **Never disarmed — a false-positive generator.** "`protection_state=warn` by 1 s of simulated
     time, `paused` by 2.1 s" is a *timing* assertion. Per kernel-b §4.6, the kernel owns only "no
     admission after the first `HealthEval` with `age >= pause_ms`"; the 2.1 s number depends on H1
     delivering `HealthEval` every ≤50 ms. A scenario containing `TimeOp::Pause{node}` or a
     `NetworkOp::Partition` that starves that cadence — both legal, both generated — makes a
     **correct** kernel miss 2.1 s. INV-LAG will fire. Unlike INV-LIVE, it has no arming condition.
     Also: `protection_state` events have no stated emission cadence
     (`trace-requirements.md` §3.14), so the oracle cannot distinguish "no eval arrived" from "eval
     arrived and the kernel did not flip" — the property is not checkable with the fields requested.
- **False-positive check:** is the timing half already kernel-b's row? Yes — kernel-b §4.6 splits it
  into a kernel row and a harness row deliberately. That strengthens the finding: the oracle is
  carrying a third copy of it.
- **Closure:** INV-LAG asserts only what it can see from declarations: (a) transition legality —
  no `healthy` following a `paused` without an intervening `resuming` whose `durability_advance`
  hits `resume_barrier_seq` exactly, with `oldest_unsafe_age_ms < 250` continuously for 5 s of
  `logical_tick`; (b) `oldest_unsafe_age_ms` never decreases across a `config_version` change
  without a retirement barrier (this is the real V8 subtlety and INV-LAG already has the field for
  it); (c) no `publish` while `state=Paused`. Drop the 1 s / 2.1 s *timing* assertion from the
  oracle and cite kernel-b's two rows for it in the test plan. Fewer lines, no duplicate machine,
  no false positives.

### F9 — INV-LOSS is not checkable as stated, and its precondition generates false violations

- **Criterion violated:** spike §6 — "Majority-loss rollback is checked against the declared
  generation and surviving prefix, not an impossible global no-loss oracle." Spec §6.3 — "a buffered
  two-copy ACK protects common single-copy failures, not arbitrary loss of the exact two holders."
  Charter BUDGET/STOP — name the missing field.
- **Location:** `design.md` §2.3 row INV-LOSS; `trace-requirements.md` §3.5, §3.12, §4.
- **Evidence:** the precondition is "every node that held an ack at that `seq` is unreachable in the
  `recovery_decision.queried_sources`." Two independent breaks:
  1. **Whose ACKs does the oracle see?** `trace-requirements.md` §3.5 does not say whether
     `replication_ack` is emitted at the secondary (on generation) or at the primary (on receipt).
     If only delivered ACKs are traced, a secondary that applied and whose ACK was dropped by
     `NetworkOp::Drop` is invisible as a holder — the oracle under-counts holders and **permits**
     loss it should forbid. If generated ACKs are traced, the map is right. This is a one-sentence
     ambiguity that inverts the invariant's strength.
  2. **Buffered loss across a host crash is legal and the rule calls it a violation.** A node holds
     a `durability_class=Buffered` ACK at `seq`, takes a `StorageOp::Crash{kind=host}` (which spike
     §6 says "may discard every unflushed suffix"), and returns with a new `boot_id`, **reachable**.
     The stated rule sees a reachable holder, concludes loss was not permitted, and reports a
     violation on a correct kernel. `durability_class` and `queried_sources[].boot` are both present
     — the rule just does not use them.
- **False-positive check:** does the "below `predecessor_cutoff`" clause already save it? No — that
  clause governs *where* loss may appear; the holder-reachability clause governs *whether*. The
  second is the one that misfires.
- **Closure:** restate the precondition as "no queried source is `reachable=true` **at a boot_id
  that held a `durability_class=Durable` ACK at that `seq`**, and every `Buffered`-only holder
  either is unreachable or returned under a different `boot_id`." Add to
  `trace-requirements.md` §3.5 one line: `replication_ack` is emitted **where the ACK is generated**
  (the secondary), with a separate `accepted`/delivery record at the primary. Add both to §5's
  "three asks that are easy to miss" — this is a fourth.

### F10 — the 60 s budget has no command that can produce it on this host

- **Criterion violated:** spike §7 — "Measure **warm release** builds on one recorded CI worker."
  Charter Q1 acceptance — "`SPIKE_SEEDS=1000 SPIKE_MAX_EVENTS=2000` runs in ≤60 s **warm release**
  on this host." `test-plan-m6.md` §7 precedent cited in `research.md` §4 — "assert invariants,
  *record* numbers, **never a threshold**."
- **Location:** `design.md` §5.1, §5.2, §8 last row; charter acceptance; ADR-rdb-0019 §2.
- **Evidence, four parts, all verifiable in-repo:**
  1. **`scripts/gate.sh` never passes `--release`** (`run_test() { cargo test --workspace
     --no-fail-fast "$@"; }`, line 45).
  2. **`Cargo.toml` line 100–103: `[profile.test] opt-level = 0`, `[profile.test.package."*"]
     opt-level = 2`.** `rdb-core` and `rdb-sim` are workspace members, not `package."*"` deps, so
     the campaign and the kernel it drives compile **unoptimized** under the mandated command. The
     charter's evidence command is `scripts/gate.sh test -p rdb-sim --test oracle --test scenarios
     --test campaign`. Nothing in `design.md` or ADR-rdb-0019 names the command that produces the
     60 s number, and the one command the charter names cannot.
  3. **Asserted wall time contradicts the precedent the design cites.** `design.md` §8's last row
     says "the only *asserted* budget is the campaign's own wall time." `research.md` §4 cites M6 §7
     as "assert invariants, *record* numbers, never a threshold." A wall-clock assertion in a PR
     test is exactly the M6 pattern the repo abandoned — and AGENTS.md records why: `m4_69`, capacity
     rows failing on a loaded host with a third of the patience they were accepted with.
  4. **The host is a shared, loaded Windows Server VM** running up to six concurrent agent cargo
     invocations (ledger: "Concurrency: up to 6 agents"; AGENTS.md records an LNK1104 target-dir
     collision on 2026-09-19). `design.md` §5.2 rule 3 chunks across `available_parallelism()`,
     which is a determinism-preserving choice but says nothing about contention.
- **False-positive check:** could `scripts/gate.sh test --release` work? Extra args do pass through,
  so yes mechanically — but it builds into a second profile subdirectory under the same
  `CARGO_TARGET_DIR`, so alternating debug/release gate runs pay a cold build each time, and
  AGENTS.md's "never two cargo invocations against one target dir" rule bites harder. It is a viable
  answer; it just has to be **written down and owned**, which is the finding.
- **Closure:** (a) `design.md` §5.1 gains a row naming the exact command and profile for the 1,000-
  seed corpus (`CARGO_TARGET_DIR=.rtargets/campaign scripts/gate.sh test --release -p rdb-sim --test
  campaign`); (b) the wall time is **recorded** in `rdb-m7-campaign.json` (`wall_ms`, plus
  `host`/`build` which ADR-0031's schema already carries) and **asserted only** in the extended
  gate, never in the PR default; (c) `design.md` §5.2 and ADR-rdb-0019 §1 both state that the 60 s
  figure is host-qualified to a Windows Server 2022 VM and is the M7 acceptance target, not a
  measured speed — spike §7 already calls these "proposed acceptance targets, not previously
  observed speeds", so this is quoting the authority, not weakening it; (d) if it is missed, spike
  §7's rule applies and the revision is written into the ADR. Never lower an assertion.

### F11 — the reducer's budget is per-failure, not per-run, and collides with the wall-time assert

- **Criterion violated:** charter DO-NOT — "No unbounded search." Spike §7 feedback table —
  "Integrated PR corpus, 1,000 bounded histories, ≤60 s."
- **Location:** `design.md` §4.2 ("at most `SPIKE_SHRINK_STEPS` (default 2,000) re-runs **per
  failure**"), §5.1, §5.2.
- **Evidence:** one failure costs up to 2,000 re-runs × up to 2,000 events = 4,000,000 events —
  **twice the entire 1,000-seed corpus** — and at the design's own 33k events/s that is ~2 minutes,
  single-threaded, inside a 60 s assert. With `--no-fail-fast` and N distinct failing seeds the cost
  is N × that, with no aggregate cap. ddmin is O(n²) in the worst case over a several-hundred-op
  list, so exhausting 2,000 steps is the normal case, not the tail.
- **False-positive check:** does it matter, since a failing run already failed? Partly — but the
  wall assert then fires too, producing a second, misleading failure on top of the real one, and a
  10,000-seed extended run with a handful of failures can exceed the 10-minute budget on shrinking
  alone.
- **Closure:** add `SPIKE_SHRINK_BUDGET_TOTAL` (a per-run cap) and `SPIKE_SHRINK_MAX_FAILURES`
  (shrink at most K *distinct signatures* per run; record the rest unminimized). Exclude shrink time
  from the reported campaign `wall_ms` and report it as a separate `shrink_ms` value.

### F12 — per-op field shrinking is over-engineering, refuted by the design's own cited source

- **Criterion violated:** team-rules architect motto ("no code is best code"); `research.md` §1's own
  evidence.
- **Location:** `design.md` §4.2, second pass.
- **Evidence:** `research.md` §1 quotes `proptest-stateful` saying per-op shrinking "tends to break
  preconditions in a way that is difficult to compensate for" and reports that removal-only shrinking
  "has been sufficient in practice." §4.2 then adds a per-op pass anyway, restricted to "monotone"
  fields. But `Advance{ticks}` is **not** monotone with respect to the failure class: ticks drive
  the 1 s/2 s protection thresholds, grant `expiry_tick`, the ±100 ms skew boundary, the 2 s
  discovery window and the 24 h dedup jump. Shrinking ticks moves the run across those guards. When
  it changes the `rule` string, ddmin rejects and the step is wasted budget (F11); when it preserves
  the `rule` but changes which guard fired, it is a second slippage channel (F4). The pass buys
  marginal reproducer readability and costs budget that deletion needs.
- **False-positive check:** is key-id/node-id shrinking harmless? Mostly yes — but it is also the
  part that buys the least, since both are excluded from the `Signature` and neither appears in a
  human's first read of a 6-op fixture.
- **Closure:** delete the second pass from M7. Deletion-only ddmin, as the cited source recommends.
  Reopen only if a real reproducer proves unreadable — and then shrink ticks **last**, with an
  explicit re-check that the original scenario still fails.

---

## ADVISORY findings

### F13 — `trace-requirements.md` claims every field has a reader; at least seven do not

- **Location:** `trace-requirements.md` preamble ("Every field below is one a checker reads"), §6
  ("If a field's only reader would be a second implementation of the protocol, we do not want it").
- **Evidence:** no checker in `design.md` §2.3 or `trace-requirements.md` §4 reads
  `batch_apply.state_digest_after`, `publish.published_state_digest`, `batch_apply.batch_id`,
  `durability_advance.flush_ticket`, `durability_advance.sync_wal_through_prefixes`,
  `client_submit.deadline_remaining_ms`, or the entire `replication_send` event kind.
  `oracle_checkpoint_digest` is self-declared as unread (foundation's replay row).
  **`state_digest_after` and `published_state_digest` violate §6's own rule directly**: the only way
  to use a whole-state digest is to compute your own, i.e. a second implementation.
- **Closure:** delete `state_digest_after` and `published_state_digest` from the ask, or move them to
  a short "for foundation's replay-equality row and for DuckDB debugging, not read by a checker"
  subsection so the document's central claim stays true. Note `admission_decision.required_copies`
  and `replication_send` both **gain** readers under F1 and F9 respectively.

### F14 — the dedup model key omits affinity, contradicting spec §5.3

- **Location:** `design.md` §2.1 (`dedup: (tenant, client, request) -> ...`), §2.3 INV-DEDUP;
  `trace-requirements.md` §3.1 (`affinity_id`).
- **Evidence:** spec §5.3 — "Dedup key is `(tenant, client_id, request_id)` **scoped to its affinity
  group and generation**." §5.4 lists `CROSS_AFFINITY` as a definitive pre-mutation rejection. The
  oracle's key drops affinity, so either the key is wrong or `affinity_id` is a dead field (F13).
- **Closure:** add `affinity` to the dedup key and one INV-DEDUP clause: a submit whose
  `affinity_id` does not match the partition's group yields `CROSS_AFFINITY` with no `batch_apply`.

### F15 — the `ClientOutcome` closed set cannot express `RECOVERED_APPLIED`

- **Location:** `trace-requirements.md` §3.8 — "`outcome: ClientOutcome` (closed set: `Success` +
  every §5.4 error)".
- **Evidence:** spec §8.1 and spike §6's mandatory F1/T1/P1 case require a status query to report
  `RECOVERED_APPLIED`. That is a status result, not a §5.4 error, so the closed set as specified
  cannot carry it — and INV-DEDUP's "never proof of nonexecution" clause is the checker that needs it.
- **Closure:** state the set as `Success | RecoveredApplied | <§5.4 errors>`.

### F16 — `AckRejectReason::ForgedIdentity` has no generator op and no coverage cell

- **Location:** `trace-requirements.md` §3.5; `design.md` §3 `NetworkOp` row; `design.md` §6 guard
  outcomes axis.
- **Evidence:** spike §4 transport seam requires "forged identity is injectable **and rejected**".
  The reject reason exists in the vocabulary; no `NetworkOp` variant produces it, and §6's guard
  axis lists no `replication_ack.reject_reason × variant` cell, so the gap cannot fail a run.
- **Closure:** F3's `NetworkOp::ForgeAck` covers the op; add `replication_ack.reject_reason × {Gap,
  DigestMismatch, StaleEpoch, StaleBoot, StaleConfig, ForgedIdentity, IncompatibleVersion}` to the
  §6 guard-outcome required list.

### F17 — `boundary="op_skipped"` pollutes the closed enum that the coverage axis counts

- **Location:** `design.md` §4.3; `trace-requirements.md` §3.16 (`boundary: BoundaryId` — "closed
  set = the 'required boundary cases' column of spike §6's table").
- **Evidence:** the reducer's skip rule emits `fault_injected{boundary="op_skipped"}`, a value that
  is not a spike §6 boundary case, into the enum the fault-boundary coverage axis counts. It has no
  required cell and will appear in `rdb-m7-coverage.json` as a cell nobody can interpret.
- **Closure:** emit skips as their own event kind (`op_skipped { scenario_op_index, reason }`) or a
  distinct enum. Keep `BoundaryId` exactly equal to spike §6's column — that identity is what makes
  the "every required cell ≥ 1" rule writable.

### F18 — the minimized fixture's `seed` is a lie; the four directed fixtures are more artifact as JSON than as Rust

- **Location:** `design.md` §3 (`Scenario { seed, generator_version, ... }`), §3.1 family 2, §4.1.
- **Evidence:** (a) a reduced `Scenario` is not in the generator's image, so replaying its `seed` at
  its `generator_version` yields a different op list. The field will be read as provenance and is
  false. (b) The four mandatory cross-package cases are hand-written JSON op lists. Hand-writing a
  30-op JSON scenario that lands "expire authority between publication and reply" is fragile, has no
  compiler checking it, and must be re-typed on every grammar change — a `fn case_a1_p1() -> Scenario`
  is strictly less artifact and is type-checked. D4's "plain data" argument is right for *reducer
  output*, which must round-trip; it does not follow for authored cases.
- **Closure:** `seed: Option<u64>` plus `provenance: Generated { seed } | Reduced { from } |
  Authored`; move the four directed cases to Rust constructors and keep JSON for reducer output only.

---

## Withdrawn after checking (stated so the architect can see what survived attack)

- **"ADR-rdb-0019's pending-row owners are invented."** They are copied verbatim from
  `validation-plan.md` §2/§3's Owner column (Placement, Actor, Performance, Storage/runtime, API +
  storage, Storage + replication, Replication + performance). Withdrawn.
- **"§8's no-linearizability-checker removal is unjustified."** The derivation from spec §5.2 holds
  under attack: one admitted transaction in flight per partition plus a declared publication order
  leaves no concurrent history to search. `research.md` §5 flagged it honestly; it survives.
  Withdrawn.
- **"The pairwise 15-pair axis is over-engineering."** Spike §7 explicitly requires *reporting*
  pairwise fault combinations. Reported-not-required is the correct reading. Withdrawn.
- **"Default `SPIKE_SEEDS=64` is too weak."** ADR-0031's reduced-scale-by-default rule and spike §7's
  layered budgets both support it, and `SPIKE_SEED_BASE=0` superset corpora make a PR failure
  reproduce in the extended run. Good decision. Withdrawn.
- **"Rejecting proptest (V-R1) leaves the campaign unshrinkable."** Settled by V-R1 and independently
  supported by `research.md` §1. Not raised.

---

## Questions for the lead

1. **F7 (multi-partition).** Adding `partitions: u8` to `Topology` touches the grammar that kernel
   teams will write scenarios against, and it is the only way to reach spike §7's isolation row and
   P1's "freezes only its partition" acceptance. **My default: add it** (two partitions, one required
   coverage cell, one INV-ISO clause armed like INV-LIVE). If you rule it out of M7, ADR-rdb-0019 §1
   needs a row saying the isolation check is deferred, because spike §7 lists it in the safety table
   next to V1/V2/V8/V12.
2. **F3 routing.** The dispatcher mutations need a hook in foundation's H1 network provider and M1
   flush path. Both are already required by spike §4 (transport: forged identity injectable) and
   spike §6 (storage: no false durable watermark), so this is a scheduling ask, not new scope. It
   must reach arch-foundation with `trace-requirements.md`. **My default: route it now**, since the
   C0/H1 seam freeze is the gating event.
3. **F1 emission cadence.** INV-PUB's fix needs `protection_state` emitted on every
   `config_version` change, not only on a protection transition. That is a line in foundation's C0
   vocabulary. **My default: add it to `trace-requirements.md` §3.14 before the seam freezes.**
4. **F10 profile.** Does the M7 gate accept `scripts/gate.sh test --release -p rdb-sim --test
   campaign` with its own `CARGO_TARGET_DIR`, given AGENTS.md's one-cargo-per-target-dir rule and
   six concurrent agents? **My default: yes, with `.rtargets/campaign` reserved for it**, and the
   60 s figure recorded rather than asserted in the PR default.

---

## Verdict

**FAIL.** Not because the design is wrong in shape — D1 through D5 are good and I tried hard to
break D3 and D5 rather than to agree with them — but because the **invariant set in §2.3 is the
deliverable**, it is what the test planner turns into rows and the developer turns into code, and
three of its statements are unsafe as written:

- INV-PUB would pass a degraded-RF2 one-ACK publish (F1) while ADR-rdb-0019 prints "V3 simulated";
- INV-LIN contains an F1 reimplementation the charter explicitly excludes (F2);
- the oracle trusts kernel-computed labels, so MUT-2 tests a fault shape the kernel cannot produce,
  and "no false durable watermark" — a named V1 clause — has no checker at all (F3, F6).

Closure is small: rewrite five rows in §2.3, add one field to §2.1, add two ops to §3, split §7's
table, add three lines to `trace-requirements.md` (§3.5 emission point, §3.14 emission cadence,
§3.8 outcome set), and qualify two cells in ADR-rdb-0019 §1. No structural change, no new module,
and F5, F12 and F18 each **remove** code. I would expect one round to close all three BLOCKERs.

**Recommended next step:** architect corrects §2.3, §3, §7 and the three trace lines; lead answers
the four questions above; re-review the diff only (not the whole document); then the test planner
starts. Do not start the test planner on the current §2.3 — its rows would encode F1 and F2 into
`docs/testing/test-plan-m7-verification.md` and into the developer's checkers.

---

# Re-review after correction round 1

Scope: the diff only — `architect-handoff.md` "Correction round 1"; `design.md` §2.1, §2.3, §2.5,
§2.6, §3, §3.1, §3.2, §4.1–§4.4, §5.1, §5.1.1, §5.2, §5.3, §6, §7, §8, §9, §10;
`trace-requirements.md` §1, §3.4, §3.5, §3.7, §3.8, §3.14, §3.16, §3.16a, §4, §5, §6, §7;
`git diff HEAD~1 -- docs/ADRs/rdb/0019*`. One pass.

**Verdict: PASS_WITH_RISKS.** All three BLOCKERs are genuinely closed. Sixteen of eighteen findings
CLOSED, two SUSTAINED narrow, four new (three MATERIAL, one ADVISORY). Three of the new ones must be
fixed *before the rows they govern are written*, not before the test planner starts — they are three
sentences, and I name them at the end.

The corrections are better than compliance. §2.5 as a standalone "two facts the oracle takes from
the environment" section states the grounding principle rather than patching one checker, which is
what makes it survive the next label the kernel invents. §2.6 gives a property to one owner instead
of three copies. Four things got smaller (§4.2 pass, `heal_at_event`, the timing ladder, INV-LIN's
derivation) and two trace fields were withdrawn. The F1 fix is also *better than my closure*: keying
the quorum on the pinned `required_copy_set` makes the count rule identical in RF3 and RF2 and moves
all the safety into set membership, so there is no 1-versus-2 arithmetic to get wrong.

## Disposition, F1–F18

| # | Disposition | Evidence checked |
|---|---|---|
| F1 | **CLOSED** | `design.md` §2.1 `required: config_version -> (Set<NodeId>, QuorumRule)`; §2.3 INV-PUB clause (a) reads the pinned set with B-R3's 1-of-1 and the explicit "never 'any one peer'"; §6 required cell `required` quorum rule × {RF3, DEGRADED_RF2}; trace-req §3.14 adds `quorum_rule: Rf3 / DegradedRf2` **and** the emission cadence; §4 compact row lists all three sources; ADR V3 `Form` cell states the degraded half and says "not by a one-regular-ACK rule". The oracle can now reach the fact, knows when it applies, and a run that never exercises the degraded path fails on the coverage cell. Carries F19. |
| F2 | **CLOSED** | §2.3 INV-LIN's cutoff check is two lookups; clause 2 compares `reported_digest` to the `entry_digest` **already recorded** from a `batch_apply`, explicitly "not a compatibility algorithm"; §8 gains the not-built row. No pairwise relation is derived. |
| F3 | **CLOSED** | New §2.5 with both grounding rules; §7 split into §7.1 (rewrites) / §7.2 (injected); `NetworkOp::ForgeAck` and `StorageOp::FalseDurable` in §3 with the spike §4/§6 citations; §9 lists the H1/M1 hooks as blocking; trace-req §3.5 makes `peer_role` "a cross-checked field, not a trusted one". The tautology is gone. Carries F19. |
| F4 | **CLOSED** on the artifact half, **see F21** on the predicate half | §4.1 writes `<slug>.orig.json` and `regressions.rs` replays both; §4.4 `Signature.faults`; row M7V-23. The `.orig.json` companion is the robust half and it landed. The `faults`-in-equality half introduces a new defect. |
| F5 | **CLOSED, code removed** | `Budget { max_events, max_ticks }` — `heal_at_event` gone (§3.2); INV-LIVE armed by the `schedule_phase` a `NetworkOp::Heal` produces; §8 not-built row. |
| F6 | **CLOSED, honestly partial** | §2.5 rule 2 + INV-PUB's last clause + `StorageOp::FalseDurable` + trace-req §5 ask 6. ADR V1 `Form` cell now reads "clause 3 ... only in the modelled sense ... **not** fsync honesty, a lying device, or power loss". That is the right qualification and it is in the quotable column, not a footnote. |
| F7 | **CLOSED** | `Topology.partitions: u8`; every `ClientOp` variant targets a partition; `unresolved: partition -> Option<seq>` in §2.1; INV-ISO armed like INV-LIVE and disarmed when B has no admitted work; one required pairwise cell; a named ADR paragraph placing it beside V1/V2/V8/V12. Minor: trace-req §3.17 still routes `schedule_phase` to INV-LIVE only (see F22). |
| F8 | **SUSTAINED (narrow)** | See below. |
| F9 | **CLOSED** | §2.3 INV-LOSS has both the `Durable`-holder clause and the `Buffered`-holder-`boot_id` clause, with the host-crash rationale inline; trace-req §3.5 pins emission at the secondary with a separate `replication_ack_delivered` record, and §5 ask 4 explains that the alternative makes the checker weaker than its own statement. Both breaks closed. |
| F10 | **CLOSED** | §5.1.1 names the command, reserves `.rtargets/campaign`, and reproduces both in-repo facts (gate.sh line 45; `[profile.test] opt-level = 0` on workspace members) — I re-verified both. `SPIKE_ASSERT_WALL_MS` unset in the PR default, `60000` in the extended gate. §5.2 host-qualifies the figure and cites `m4_69`. §8's contradictory row rewritten to "Any performance assertion **in the PR default**". ADR §2 `values` gains `profile` (`debug`/`release`), which is the field that stops a debug number being read as the release one. |
| F11 | **CLOSED** | `SPIKE_SHRINK_MAX_FAILURES=3` and `SPIKE_SHRINK_BUDGET_TOTAL=20000` in §4.2 and §5.1; `shrink_ms` separate from `wall_ms` in §5.3 and in the ADR `values` row. |
| F12 | **CLOSED, code removed** | §4.2 "Deletion only." with the self-correction recorded rather than hidden; §8 not-built row. |
| F13 | **CLOSED** | `state_digest_after` and `published_state_digest` are gone from trace-req §3.4 and §3.7 — I checked the field lists, not the summary. New §7 lists the six remaining exceptions with their real readers and offers to drop any. `required_copies` and `replication_send` gained readers, as predicted. |
| F14 | **CLOSED** | §2.1 key is `(tenant, affinity, client, request)`; INV-DEDUP has the `CROSS_AFFINITY` clause; §3 `ClientOp` has the foreign-affinity boundary case; §6 has the `cross_affinity` dedup cell. |
| F15 | **CLOSED** | trace-req §3.8 `ClientOutcome = Success / RecoveredApplied / <§5.4 errors>`; INV-DEDUP's clause names `RecoveredApplied`; §4 compact row says "including `RecoveredApplied`". |
| F16 | **CLOSED** | `NetworkOp::ForgeAck` + the full `replication_ack.reject_reason` × 7 required guard row in §6. |
| F17 | **CLOSED** | `op_skipped { scenario_op_index, reason }` as its own kind (§4.3, trace-req §3.16a), and §3.16 now says `BoundaryId` is spike §6's column "**and nothing else**". |
| F18 | **SUSTAINED (narrow)** | See below. |

### F8 — SUSTAINED (narrow): INV-LAG clause (a) checks one `durability_advance`, not the required set

- **Criterion violated:** spec §6.2 Resume row — "**All configured regular copies** durable through
  paused prefix; lag below 250 ms for 5 s."
- **Location:** `design.md` §2.3 INV-LAG clause (a); ADR-rdb-0019 §1 V8 `Form` cell.
- **Remaining gap:** the clause reads "an intervening `Resuming` **whose `durability_advance`** hits
  `resume_barrier_seq` exactly" — singular. That is structurally the same defect F1 just fixed for
  INV-PUB: one declaration standing in for a pinned set. A kernel that resumes when *one* copy
  reached the barrier passes. kernel-b `design.md` §4.4 gets it right
  (`all_durable_through(resume_barrier)` over **every** active predicate), so the oracle is the
  weaker of the two. The 250 ms and the retirement-barrier clauses are correct and are the two I
  asked for; this is the third quantifier.
- **False-positive check:** is it covered by clause (b)? No — (b) is about `oldest_unsafe_age_ms`
  across a `config_version` change, a different property. Is the data there? Yes: §2.1 already has
  `durable: node -> (boot, durable_seq)` and `required: config_version -> Set<NodeId>`. The fix is a
  quantifier, not a field.
- **Closure:** clause (a) becomes "...without an intervening `Resuming` during which **every node in
  the `required_copy_set` pinned at `paused_prefix_seq`** has `durable[node].durable_seq >=
  resume_barrier_seq`, the barrier is hit exactly, and `oldest_unsafe_age_ms < 250` continuously for
  5 s of `logical_tick`." Mirror the same words into ADR V8's `Form` cell.

### F18 — SUSTAINED (narrow): the trace header still carries a bare `seed`

- **Criterion violated:** the F18 argument itself — a `seed` on an artifact that is not in the
  generator's image is read as provenance and is false.
- **Location:** `trace-requirements.md` §1 header table, row `seed: u64` ("report, reproducer").
  `design.md` §3 replaced `Scenario.seed` with `Provenance::{Generated, Reduced, Authored}`.
- **Remaining gap:** half the fix landed. The *trace* header is what a failure report and
  `validation/<run-id>/` print, and it is exactly where a human reads "seed 4471" and re-runs it.
  For a `Reduced` or `Authored` scenario there is no seed that reproduces the run, so the header
  field is either absent, zero, or a lie — and the document does not say which.
- **False-positive check:** could the header simply carry the generating seed of the *pre-reduction*
  run? That is defensible, but then it must be labelled as such, which is the same edit.
- **Closure:** `trace-requirements.md` §1 replaces `seed: u64` with `provenance: Provenance`
  (matching `design.md` §3), or `seed: Option<u64>` with one sentence saying `None` means the
  scenario is reduced or authored and the fixture is the reproducer.

## New defects introduced or exposed by the correction

### F19 — MATERIAL: `topology` is a single static header snapshot, so role grounding breaks on membership change

- **Criterion violated:** spec §8.3 — "rDB CASes normal three-copy membership" after a degraded
  period; "Build and fsync replacement third regular copy". Spec §6.2 — "Required copies are pinned
  by configuration version." `design.md` §2.5's own grounding rule.
- **Location:** `trace-requirements.md` §1 header, row `topology` ("node ids, roles ...,
  `config_version`, `partitions: u8`"); `design.md` §3 `Topology` comment; `design.md` §2.5 table
  row 1; §2.3 INV-PUB ("a peer's role is resolved from the header's `topology` ... a mismatch
  between the two is itself a violation").
- **Evidence:** the F1 fix made `required` a **map keyed by `config_version`**, correctly, because
  membership changes during a run. The F3 fix grounded roles in a **single header snapshot** with
  one `config_version`, which assumes membership does not. Those two cannot both be right. The
  RF3 → `DEGRADED_RF2` → rebuild path is in scope by construction: `RecoveryOp::Rebuild{node}`,
  `ControlOp::Cas`, and §6's new `DEGRADED_RF2` required cell all force the campaign through it.
  When the replacement third copy is committed into membership, the header topology has no role for
  it, so INV-PUB's "mismatch is itself a violation" fires on a **correct** kernel — and it fires
  precisely in the scenarios F1 exists to check. The weaker failure mode is as bad: if the checker
  instead treats an unknown node as roleless, its acks stop counting and the publish looks
  under-acked.
- **False-positive check:** does a rebuilt node keep its original topology role? Only if node
  identities are pre-declared with final roles and a rebuild reuses one — which the design does not
  say, and which would make `RecoveryOp::Rebuild{node}` unable to express spec §8.3's *new*
  replacement copy. If foundation pre-declares every node with its eventual role and membership
  changes only which are *active*, the finding collapses to a one-line clarification — which is the
  ask either way.
- **Closure:** make the role source config-versioned and environment-owned, symmetric with
  `required`: the header declares `topology[config_version_0]`, and every membership change emits a
  `topology_change { config_version, nodes: Vec<(NodeId, Role)> }` event from the **environment**
  (H1/control provider), not from the kernel. §2.5 resolves a role from
  `topology[config_version in force at that ack]`. §2.1 gains nothing — `required` is already keyed
  the same way. Add to trace-req §5 as ask 7; it is a seam-freeze item like the other three.

### F20 — MATERIAL: INV-LAG clause (c) "no publish while `state=Paused`" is wrong — my round-1 error, adopted verbatim

- **Criterion violated:** spec §6.2 — Paused means "**Reject new admission** within additional
  100 ms"; spec §5.3 — "No later transaction may skip the unresolved sequence", and an
  already-applied transaction must be resolved by ACK or recovery, not abandoned.
- **Location:** `design.md` §2.3 INV-LAG clause (c); ADR-rdb-0019 §1 V8 `Form` cell ("no publish
  while paused"); it propagated into both.
- **Evidence:** I proposed this clause in round 1 and it was taken verbatim. It is wrong. Pausing is
  an **admission** gate: kernel-b `design.md` §4.4 emits `SetAdmission(Reject(PROTECTION_PAUSED))`
  and nothing else, and publication is P1's independent decision gated on
  `ReplicationResult::qualifies(seq)`. A transaction admitted at t=0, applied, and ACKed at t=2.5 s
  while protection paused at t=2.0 s **must** publish — that is exactly spec §5.3's "freeze the
  queue until the transaction is resolved with a replica ACK". Clause (c) makes the oracle report a
  violation on the correct behaviour, in a scenario every lag test will produce.
- **False-positive check:** is there a reading where paused forbids publication? Spec §6.2's second
  path, `Healthy --> Paused: no secondary can ACK`, forbids *success without a secondary* — but that
  is INV-PUB's quorum clause, already checked, and it forbids publishing **without an ACK**, not
  publishing **while paused**. No reading supports clause (c) as written.
- **Closure:** replace (c) with the property I actually meant, which is declaration-readable:
  "no `admission_decision{outcome=Admitted}` at a `logical_tick` at or after the first
  `protection_state{state=Paused}` and before the next `state=Healthy`." Amend ADR V8's `Form` cell
  with the same words. I withdraw the original wording.

### F21 — MATERIAL: `faults` inside signature **equality** makes ddmin reject almost every useful candidate, and M7V-20 becomes unsatisfiable

- **Criterion violated:** charter G1 — "a seeded failure keeps its signature after shrinking"
  *and* the minimized scenario is genuinely smaller. Spike §5 G1 — "Failure retains its signature
  after shrinking."
- **Location:** `design.md` §4.4 — "Deleting the last op that produced a boundary changes the set
  and the candidate is correctly rejected"; row M7V-20 asserts `signature_before == signature_after`
  **and** `ops_after.len() < ops_before.len()`.
- **Evidence:** the two halves of M7V-20 are in tension. ddmin's whole job is to delete ops; ops are
  what emit `fault_injected{boundary}`; so a useful minimization almost always *drops* boundaries
  from the set. Under struct equality every such candidate is rejected, and the reducer converges to
  roughly the original scenario — `ops_after.len() < ops_before.len()` barely holds or fails
  outright. Concretely: a 40-op failing scenario with eight active boundaries minimizes to 5 ops and
  two boundaries; that candidate is rejected at the first deletion that removes the third boundary,
  so the reducer stops long before the interesting fixture. My round-1 finding asked `faults` to
  discriminate *causal paths*; equality over the whole set is a stronger predicate than that, and it
  buys the strength by disabling the reducer.
- **False-positive check:** would subset (`faults_after` ⊆ `faults_before`) fix it? No — in §4.4's own
  worked example the R1-path candidate's set is a subset of the original's, so slippage returns.
  Neither equality nor subset is the right predicate, which is why the answer is not to strengthen it.
- **Closure:** split the two roles. Acceptance predicate = the **core tuple**
  `(checker, rule, partition, role, event_kind)`, as before F4. `faults` is **recorded** in the
  signature and **reported**: when `faults_after != faults_before` the run writes
  `slipped: true` into `rdb-m7-campaign.json` and the artifact names both sets. The robust slippage
  defence is the `.orig.json` companion, which landed and which `regressions.rs` already replays —
  that is what actually stops the F4 scenario ("fix R1, the fixture goes green, the F1 bug ships"),
  because the original still fails. Restate M7V-20 as: core tuple equal, `ops_after.len() <
  ops_before.len()`, and `.orig.json` replays and fails. Restate M7V-23 as: the two-defect scenario
  shrinks, the artifact records `slipped: true` with both fault sets, and `.orig.json` still fails.

### F22 — ADVISORY: two editorial defects in the new material

- **Location / evidence:** (a) `design.md` section order is §2.1, §2.2, §2.3, **§2.5, §2.6**, §2.4 —
  the two new sections were appended before §2.4 rather than after it, so the document reads
  2.1–2.3, 2.5, 2.6, 2.4. (b) `trace-requirements.md` §3.17 `schedule_phase` still routes to
  "**INV-LIVE**. This is the only thing that arms the liveness checker", but §2.3 INV-ISO is "armed
  exactly like INV-LIVE" off the same event; foundation reading §3.17 alone would not know INV-ISO
  depends on it.
- **Closure:** renumber (§2.4 stays where it is; the new sections become §2.5/§2.6 after it). Add
  INV-ISO to §3.17's reader list and to §4's `schedule_phase` mentions.

## Verdict

**PASS_WITH_RISKS** as a basis for the test planner.

All three BLOCKERs are closed on the evidence, not on assertion: I checked the invariant text, the
model fields, the grammar ops, the trace field lists and the ADR `Form` cells separately rather than
trusting the correction table. The one closure I expected to be fudged — F6's "no false durable
watermark", where the honest answer is *partly* — is qualified in the ADR's quotable column and says
what M7 does **not** cover. That is the behaviour the ADR exists to produce.

Three MATERIAL items must land before the rows they govern are written. None blocks starting:

1. **F20 before any INV-LAG row** and before ADR V8 is quoted. It is my error; the clause as written
   asserts a false property and would produce a row that fails on correct behaviour.
2. **F21 before M7V-20 and M7V-23.** As written M7V-20's two assertions are in tension and the row
   is likely unsatisfiable.
3. **F19 before the `DEGRADED_RF2` coverage cell and the MUT-2 row**, and it is a seam-freeze item —
   it must reach arch-foundation with the other four trace asks, because a header-only `topology`
   cannot be fixed after C0 freezes.

F8 and F18 are one-sentence edits and can ride along. F22 is editorial.

**Recommended next:** architect applies F19–F21 (three sentences plus one trace-req ask) and F8/F18
while the test planner starts on §2.3, §6 and §7 — with M7V-20, M7V-23 and the INV-LAG rows held
until F20 and F21 land. No further critic round on this document; re-review, if any, belongs to
critic round 2 on the test plan, which is where these three will show up as rows.
