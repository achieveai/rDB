# Team verification — design (architect, 2026-09-20)

Packages **O1** (independent oracle), **G1** (scenarios + reducer), **Q1** (campaign).
Read `../../team-rules.md` and `charter.md` first. Trace field requests live in
`trace-requirements.md`; sources in `research.md`.

Motto applied: **no code is best code.** Every section below names the thing it refuses to build
and why. The three decisions that remove the most code are D1, D3 and D5.

---

## 0. The five decisions that shape everything

| # | Decision | Code it removes |
|---|---|---|
| D1 | The oracle reads **only the trace**. One input type: `&[TraceEvent]`. It never sees kernel state. | No kernel adapters, no state mirroring, no second protocol. |
| D2 | All checkers run in **one left-to-right fold**, one pass, no allocation on the happy path. | No per-checker trace copy, no re-scan, no index. Makes the Q1 budget reachable by construction. |
| D3 | The reducer shrinks the **scenario**, never the trace. Remove an op, re-run the kernel, get a fresh self-consistent trace. | No causal repair, no happens-before graph, no trace-validity checker. Causality is preserved because the kernel regenerates it. |
| D4 | The scenario grammar is **plain data** (enums + `Vec`), serialized as the fixture. Seed is a convenience, not the reproducer. | No generator-version compatibility shims; spike §4 already forbids seed-only replay. |
| D5 | No mutation ever touches kernel code. Three of the five are **recorded-trace rewrites**; two are **sim-provider faults** the spike already requires (V-R9, §7). | No `#[cfg(test)]` branches in production code, no mutant build matrix. |

D3 and D5 are the two that a reviewer should attack first. Their justification is in §4.3 and §7.

---

## 1. Module layout

```
crates/rdb-sim/tests/
  support/oracle/
    mod.rs          Oracle, Report, Verdict, Violation, Signature
    model.rs        the tiny visible-state model (see §2.1)
    checks/
      atomicity.rs  INV-ATOM
      publication.rs INV-PUB
      authority.rs  INV-AUTH
      lineage.rs    INV-LIN
      dedup.rs      INV-DEDUP
      loss.rs       INV-LOSS
      liveness.rs   INV-LIVE + INV-ISO (same arming condition)
      version.rs    INV-VER   (V12 subset — V-R3)
      lag.rs        INV-LAG   (V8, transition legality only — V-R3, §2.6)
  support/scenarios/
    mod.rs          Scenario, ScenarioOp, Topology, Budget
    grammar.rs      the six op groups from spike §6 as enums
    gen.rs          seeded generator: Seed + Budget -> Scenario
    reduce.rs       the reducer (ddmin over ops)
    coverage.rs     Coverage counters + required-cell lists
    mutate.rs       the named trace mutations from spike §7
  oracle.rs         O1 rows: each checker, bad trace and good trace
  scenarios.rs      G1 rows: grammar, generator determinism, reducer
  campaign.rs       Q1 entry point
  campaign/
    corpus.rs       seed loop, threading, budget knobs
    report.rs       per-invariant status, coverage artifact, evidence
    regressions.rs  replay every checked-in minimized fixture
  fixtures/
    scenarios/*.json    hand-written boundary scenarios (the §6 mandatory cases)
    regressions/*.json  minimized reproducers, one file per past failure
```

Registration of `support/oracle` and `support/scenarios` in `tests/support/mod.rs` is **team
foundation's file**. Request is in the handoff.

---

## 2. The oracle

### 2.1 What it models

The model is deliberately tiny. Anything bigger is a second implementation.

Per partition, the oracle keeps:

| Field | Shape | Why |
|---|---|---|
| `published` | `key_id -> (value_version, seq)` | the only client-visible state (spec §5.3) |
| `lineage` | `Vec<(generation, seq, entry_digest, predecessor_digest)>` — append-only | INV-LIN, INV-LOSS |
| `roots` | `Vec<LineageRoot>` | generation changes and cutoffs (spec §8.1) |
| `inflight` | `request_id -> (digest, admitted_seq, declared_outcome)` | INV-ATOM, INV-DEDUP |
| `dedup` | `(tenant, affinity, client, request) -> (digest, result_digest, retained_until, generation)` | INV-DEDUP — `affinity` per spec §5.3, "scoped to its affinity group and generation" (F14) |
| `authority` | `generation -> (owner, epoch, grant_id, boot, valid_from_tick, expiry_tick)` | INV-AUTH |
| `required` | `config_version -> Set<NodeId>` | **INV-PUB, INV-LAG (F1).** Fed by `protection_state.{config_version, required_copy_set}` and `admission_decision.required_copies`. The quorum rule is **derived** from the set's length (ruling V-R20, §2.3), never stored or read as a field: two nodes is `DEGRADED_RF2`, three is RF3. Without this map the checker cannot tell RF3 from `DEGRADED_RF2` and would pass a one-ACK publish while degraded |
| `acks` | `seq -> set of (node, boot, role_from_topology, durability_class)` | INV-PUB, INV-LOSS |
| `durable` | `node -> (boot, durable_seq)` from `durability_advance{outcome=Synced}` | **INV-LOSS, INV-PUB (F3/F6)** — grounds the `Durable` label in an actual flush |
| `protection` | last `protection_state` + the tick it entered + `healthy_since_tick` | INV-LAG |
| `phase` | chaotic / healed (set by a `NetworkOp::Heal`-produced `schedule_phase` event) | INV-LIVE, INV-ISO gating |
| `unresolved` | `partition -> Option<seq>` | INV-ISO |

That is the whole model. It contains **no** transaction application logic, **no** prefix selection,
**no** ancestry-repair, **no** admission arithmetic. It compares declared facts against each other,
and against two things the **environment** declares rather than the kernel: the config-versioned
topology (node → role, from the header plus `topology_change` events) and the recorded
`durability_advance` stream. See §2.5.

### 2.2 Independence, enforced not promised

- The oracle's only permitted import from the kernel crate is the trace/contract vocabulary:
  `rdb_core::contracts::{trace, ids}`.
- Forbidden: `rdb_core::{authority, transaction, replication, publication, protection, recovery}`.
- Enforced by row **M7V-01**: a test that reads every `.rs` file under `support/oracle/` and fails
  if any line matches `rdb_core::(authority|transaction|replication|publication|protection|recovery)`.
  A grep in the handoff is the human-readable half; the row is what keeps it true.

Why a test and not a review note: rEtcd ADR-0031's closing note records four cases on the previous
branch of "a documented behaviour with nothing behind it." An independence claim with no caller
checking it is the same shape.

### 2.3 The invariants

Each checker is `fn observe(&mut self, ev: &TraceEvent) -> Result<(), Violation>` plus
`fn finish(&mut self) -> Result<(), Violation>` for end-of-trace obligations. One violation aborts
that seed and yields a `Signature` (§4.4).

| ID | Invariant | Says (one line) | Spec / gate |
|---|---|---|---|
| INV-ATOM | Atomicity | A transaction's mutations are all present in the published prefix or none are; no published key version comes from a batch whose `batch_apply` reported `failed` or `crashed_before`. | §5.2 step 3; V1 |
| INV-PUB | Publication | No read, status, export or actor observation returns a `(key, version)` above the last `publish`. A `publish` at `seq` requires (a) acks satisfying the **`required_copy_set` pinned by the `config_version` in force at `admitted_seq`** — one regular peer under healthy RF3, under `DEGRADED_RF2` the pinned rule is **`min_regular_acks` 1-of-1** (ruling B-R3) — the *single remaining* regular secondary, never "any one peer" (spec §8.3: no one-copy fallback, no two-second local-only allowance) — and (b) an `authority_decision{gate=publication, outcome=valid}` at or after it. **The quorum rule is not a trace field (ruling V-R20):** the oracle derives it from `required_copy_set.len()` on the pinning `protection_state` — two nodes is `DEGRADED_RF2`, three is RF3, any other length is itself a violation (`required_copy_set_shape`) — **a violation, not a fixture check, decided once here (critic T-39)**: the oracle reads only the trace and cannot tell a bad fixture from a kernel that really pinned a one- or four-node set, and if it treats the shape as a fixture defect it must skip INV-PUB for that seed, which is the silent-skip this whole design exists to prevent. A shape it cannot derive a rule from fails loudly; a generated trace carrying one is a generator bug that should surface the same way. Q-35's "fixture or cadence defect" wording is the outlier and needs aligning, and the rule needs a plan row (one sub-case on M7V-07) — both routed to the planner through the lead. A length is a declared fact on an event the oracle already reads, not a re-derived decision, so this stays inside D1. **The derived value is authoritative (ruling V-R21):** it keys the coverage cell and every decision the oracle makes. There is no declared field to reconcile it with, and no cross-check: **ruling F-R13 settled K-F-07 against V-R20 — there will never be a `quorum_rule` field**, and foundation's committed contract at `6893442` carries none. The conditional cross-check this paragraph briefly carried, its `quorum_rule_mismatch` signature and row M7V-90 are all withdrawn with that branch; do not go looking for them. A peer's role is resolved from the environment topology **in force at that `config_version`** (header declaration plus `topology_change` events, F19), **not** from `replication_ack.peer_role`; a mismatch between the two is itself a violation. A `durability_class=Durable` ack at `seq` on node `n` requires a preceding `durability_advance{node=n, outcome=Synced, durable_seq >= seq}`. A lost reply never retracts a publish. | §5.2 steps 6–7, §5.3, **§8.3**, §6.2; V1, V3 |
| INV-AUTH | Authority | No two generations hold `outcome=valid` authority over one partition with overlapping `[valid_from_tick, expiry_tick]`. No `batch_apply{role=primary}` or `publish` carries a generation whose grant was expired or fenced at that tick. Uncertainty denies. | §7.2, §7.3; V2 (model) |
| INV-LIN | Lineage | Every `batch_apply` cites `predecessor_digest` equal to the recorded digest at `seq-1` in the same generation, or equal to its root's `base_digest`. One `(generation, seq)` never carries two `entry_digest` values anywhere in the trace. Every new root cites a real `predecessor_generation` and `predecessor_cutoff`. A digest conflict yields `mode=quarantine`. **Cutoff check, as two lookups against recorded facts (F2):** (1) `selected_cutoff_seq <= reported_seq` of `selected_source`; (2) no source with `reachable=true` reported a `(generation, reported_seq)` with `reported_seq > selected_cutoff_seq` **whose `reported_digest` equals the `entry_digest` the oracle already recorded at that `(generation, seq)`** from a `batch_apply`. Clause 2 is a hash-map lookup, **not** a compatibility algorithm; the oracle never derives the pairwise-compatibility relation, which is F1's job. | §8.1, §8.2; V3 |
| INV-DEDUP | Dedup | Within one generation and the retention window, one `(tenant, affinity_id, client_id, request_id)` produces at most one `batch_apply`. The same identity with a different `request_digest` yields `REQUEST_ID_REUSE` and no apply. A submit whose `affinity_id` is not the partition's group yields `CROSS_AFFINITY` with no `batch_apply` (F14). A retry with a stale `expected_generation` yields `GENERATION_CHANGED` **before** any apply. Absence after retention yields `STATUS_EXPIRED` / `UNKNOWN_OUTCOME` — and a retained old-generation identity may yield `RecoveredApplied` — never a definitive "did not run". | §5.3, §5.4, §8.1; V4 |
| INV-LOSS | Restricted loss after majority loss | A key version present in the published prefix may disappear **only** across a `lineage_root{source=recovery}` whose `predecessor_cutoff` is below that version's `seq`, and only when the copy-loss precondition holds: **(a)** no queried source is `reachable=true` **at a `boot_id` that held a `durability_class=Durable` ack at that `seq`**, and **(b)** every `Buffered`-only holder is either unreachable or returned under a **different `boot_id`** (F9 — a host crash may discard every unflushed suffix, spike §6, so a returning buffered-only holder is *not* evidence the data survived). Loss under an unchanged generation is a violation. Loss above the declared cutoff is expected and not a violation. | §6.3, §8.4; V3 |
| INV-LIVE | Controlled liveness | Only armed after `schedule_phase{phase=healed, fair_delivery=true}` — produced by a `NetworkOp::Heal` op, so the reducer moves it with the op list (F5) — and a valid authority decision. Within the declared remaining budget: every `inflight` request reaches a terminal `client_outcome`, and `protection_state` leaves `paused`. An unhealed partition or an exhausted budget is **not** a failure — it disarms the checker, which reports `Unavailable(NotArmed)` (§2.4). | spike §6 "controlled liveness" |
| INV-ISO | Multi-partition isolation | Armed exactly like INV-LIVE. While partition A has an unresolved transaction (`unresolved[A].is_some()`), a `client_outcome` for partition B may still occur; a healed, fairly-scheduled run in which B produces **no** terminal outcome while only A is blocked is a violation. Disarmed (`Unavailable(NotArmed)`, §2.4) when B has no admitted work. | spec §5.2 "other partitions in the set keep running", §5.3; spike §7 safety table; V-R8 |
| INV-VER | Compatibility subset | A `version_check` reporting a non-empty `mandatory_unknown_fields` must have `outcome=refuse_before_apply`, and no `batch_apply` may carry that `correlation_id`. | §5.4 `INCOMPATIBLE_VERSION`; V12 subset |
| INV-LAG | Lag protection — **transition legality only** | Three clauses, all readable from declarations (F8): **(a)** no `phase=Healthy` follows a `Paused` without an intervening `Resuming` during which **every node in the `required_copy_set` pinned at `paused_prefix_seq`** has `durable[node].durable_seq >= resume_barrier_seq`, the barrier is hit **exactly**, and `oldest_unsafe_age_ms < 250` continuously for 5 s of `logical_tick` (spec §6.2 resume row: "**All configured regular copies** durable through paused prefix; lag below 250 ms for 5 s" — the validation plan omits the 250 ms and team-rules puts the spec above it); **(b)** `oldest_unsafe_age_ms` never decreases across a `config_version` change without a retirement barrier (spec §6.2: "no timer reset merely because a replica was renamed/replaced") — this is the real V8 subtlety; **(c)** no `admission_decision{outcome=Admitted}` at a `logical_tick` at or after the first `protection_state{phase=Paused}` and before the next `phase=Healthy` (landed C0 names the field `phase: ProtectionPhase`, not `state`; trace-requirements §8). **The 1 s / 2.1 s *timing* ladder is deliberately NOT asserted here** — see §2.6. | §6.2; V8 |

INV-VER and INV-LAG are additions beyond the charter's five; **V-R3 keeps both in the oracle**.
INV-ISO is added by **V-R8**. The charter names atomicity, publication, authority, lineage and
dedup; the milestone claims V8, a V12 subset and spike §7's isolation row, and the oracle is the
only cross-cutting judge in M7 that can carry them.

### 2.4 Severity and the two "Unavailable" verdicts

A checker is in one of three states per seed, and `Unavailable` carries a reason (critic T-01,
ruling V-R16):

```rust
pub enum Verdict {
    Proven,                                 // armed at least once, no violation
    Unavailable(Unavailable),               // reports, never passes
    Violated(Signature),
}
pub enum Unavailable {
    Capability(PackageId),                  // a package the checker needs is not wired
    NotArmed,                               // wired, fold finished, arming situation never seen
}
```

- `Proven` — the checker **armed** at least once and saw no violation. Armed means the fold reached
  the situation the checker's clause quantifies over: INV-LIVE and INV-ISO a
  `schedule_phase{phase=Healed}`; INV-LAG a `protection_state{phase=Paused}`; INV-VER a
  `version_check`; INV-LOSS a `lineage_root{source=Recovery}`; INV-DEDUP a second `client_submit`
  with a retained identity; INV-ATOM, INV-PUB, INV-AUTH and INV-LIN their first `batch_apply`,
  `publish`, `authority_decision` and `lineage_root` respectively. Each checker exposes
  `fn armed(&self) -> bool`; the oracle row for a checker names its arming event. A checker that
  never armed cannot be `Proven`, whatever the trace contained. **`armed()` is the checker's state
  at the end of the fold, not a latch (critic T-24, ruling V-R20):** a checker that armed and then
  disarmed — INV-LIVE or INV-ISO on an exhausted `remaining_event_budget` or an idle sibling —
  reports `armed() == false` after the fold, and its per-seed verdict is `Unavailable(NotArmed)`.
- `Unavailable(Capability(p))` — package `p` reported `capability{package=p, state=Unavailable}` at
  trace start (e.g. P1 not landed) and the checker reads events only `p` can emit. Charter Q1.
- `Unavailable(NotArmed)` — every package the checker needs is wired, the fold reached the end of
  the trace, and the checker never armed. This is the verdict for: a trace with no events after
  the `capability` block (the "zero-event" control: header plus ten `capability{state=Wired}`
  lines and nothing else, so it is well-formed under §4.5 and still arms nothing — T-26); INV-LIVE or
  INV-ISO with no `schedule_phase{Healed}`, or with `remaining_event_budget` exhausted first;
  INV-ISO when the sibling partition had no admitted work; a trace truncated at an event boundary
  before the checker armed. Disarming **is** `NotArmed` — there is no fourth state — and a
  disarmed checker's `armed()` is `false` (T-24, above).
- `Violated(signature)` — §4.4.

Both `Unavailable` reasons **report, never pass**. They differ only in whom they point at:
`Capability` at a package owner, `NotArmed` at the corpus, the generator or the fixture. The earlier
two-state gloss ("`Unavailable` = unwired, otherwise `Proven`") had no verdict for "never armed,
nothing wrong", so a checker that armed on no seed reported `Proven` — the vacuous pass this whole
mechanism exists to prevent (T-01). The rule "`Unavailable` is never inferred from silence" is
therefore **scoped to the `Capability` arm**: a capability verdict needs the trace event; a
`NotArmed` verdict is exactly the oracle's conclusion from silence, stated as such.

**Per-seed to per-run.** The campaign folds seed verdicts into one status per invariant, in this
order: `violated` if any seed violated; else `unavailable` with reason `capability(p)` if any seed
reported it (capability is per build, so every seed agrees); else `proven` if at least one seed's
**per-seed verdict is `Proven`**; else `unavailable` with reason `not_armed`. The artifact records
**`seeds_armed`** per invariant — the number of seeds whose per-seed verdict is `Proven`, which is
the number on which `armed()` was `true` at the end of the fold — beside the status. `proven` and
`seeds_armed` count the same seeds, so `proven ⇒ seeds_armed > 0` is definitional, not a gate
(T-24): a corpus on which every seed healed and then exhausted its budget folds to
`unavailable(not_armed)` with `seeds_armed = 0`, never to `proven`. This paragraph is the
authoritative fold; the planner's VA-2 and M7V-52 carry it word for word (T-34).
**`proven` with `seeds_armed == 0` is a gate failure**, in every run and under every setting of
`SPIKE_REQUIRE_ALL`: the fold above cannot produce it, so if the artifact would say it the runner
has a bug and the run fails naming the invariant. `SPIKE_REQUIRE_ALL=1` additionally fails the run
when any status is not `proven`, and the failure line names the reason.

**Wired implies armed on the default corpus (critic T-28, ruling V-R20).** For every invariant
whose needed packages all report `Wired` in this build, `seeds_armed > 0` on the default 64-seed
corpus — asserted in the handoff gate, not only under `SPIKE_REQUIRE_ALL=1`. The claim rides on
the V-R19 schedule (§3.1), which is deterministic: each checker's oracle row names the scheduled
boundary or op that arms it (INV-LOSS a `LoneSurvivorChoice` or `UnequalSecondaryPrefix`
boundary; INV-LAG a regular secondary partitioned then `TimeOp::Advance` past the pause threshold;
INV-DEDUP the `RetainedDedupHit` boundary; INV-LIVE and INV-ISO a `NetworkOp::Heal`; INV-ATOM,
INV-PUB, INV-AUTH and INV-LIN the first `Submit`), so a generator that stops producing that op
turns the row red inside the handoff gate instead of hiding as `not_armed` until someone runs the
release gate by hand. While a needed package reports `Unavailable` the clause does not apply to
that invariant; during M7 it covers zero invariants and the row says so. **Excluded set (ruling
V-R21): INV-VER only.** It has no producing op in the grammar today (no `ScenarioOp` injects an
unknown mandatory version and `BoundaryId` has no such member), so it is excluded from this clause
until a producing `ScenarioOp` or `BoundaryId` exists; the exclusion is listed here explicitly so
the **M7V-89** row asserts over the nine others and names the tenth as excluded, not as passing.
(M7V-78 keeps the unexcluded `proven ⇒ armed` half; the wired clause is M7V-89's — critic T-41.)

The campaign's per-invariant table prints status, reason and `seeds_armed`. The test binary may
exit 0 while an invariant is `Unavailable` for either reason (so the gate is green during M7), but
the evidence artifact records `unavailable` **with its reason**, and the gate check fails when
`SPIKE_REQUIRE_ALL=1` and any invariant is not `proven`. This is exactly ADR-0031's
`full_scale: false` mechanic: green run, honest artifact, one command that turns the artifact into a
gate (§5.1.1, the M7 release gate).

**`capability{state}` is derived, never a literal (T-14b, ruling V-R18).** The dispatcher builds
the trace-start capability block from the crate's actual wiring — `Module::capability(&self) ->
CapabilityState`, the non-mutating method foundation is adding under K-F-10 — over
`ModuleName::ALL`, and emits one `capability` event per package from that report. There is no
hand-maintained table and no `CapabilityState::Wired` literal anywhere in the harness outside that
report. A table would let a landed package stay `Unavailable` (a false red nobody chases, because
it looks honest) or let an unlanded one be flipped to `Wired` early (which turns every unarmed
checker's verdict into `NotArmed` — never `Proven`, under the rule above — but misreports the
cause). Observable form for the planner's row: the `capability` events at trace start equal, one
for one, the report `Module::capability` returns over `ModuleName::ALL`; a module whose `step`
answers `Ok` reports `Wired` and one whose `step` answers `RdbError::Unavailable` reports
`Unavailable`, both asserted positively; and no source file under `crates/rdb-sim/src/harness/`
contains the token `CapabilityState::Wired` except the one that builds the report.

### 2.5 Two facts the oracle takes from the environment, not from the kernel

The rule "compare declared facts against each other" has one dangerous hole: a label the component
under test computes is not an independent fact. If R1 resolves `peer_role` from the ack *message*
rather than from configured topology, a shadow is labelled `Regular` everywhere, self-consistently,
and every cross-check agrees. Same for a `Buffered` ack mislabelled `Durable`.

Two grounding rules close it, both costing about five lines. Both are keyed by `config_version`,
because membership changes inside a run (spec §8.3's RF3 → `DEGRADED_RF2` → rebuild path is in the
grammar): a rule grounded in one static snapshot would fire on a correct kernel the moment a
replacement regular copy is CASed in.

| Kernel-computed label | Grounded in | Where |
|---|---|---|
| `replication_ack.peer_role` | the **config-versioned** environment topology, keyed by `node_id` — a declared **input**, not a derived decision. The header declares the initial placement (landed: a flat `Vec<TopologyEntry{node, partition, role, config_version}>`, one entry per node per partition — there is no `config_version_0` field, the entries carry it); every membership change emits `topology_change{config_version, nodes}` from H1/the control provider, never from the kernel (F19, ruling V-R12). A role is resolved from `topology[config_version in force at that ack]`, symmetric with `required` in §2.1 | INV-PUB clause (a); mismatch is itself a violation |
| `replication_ack.durability_class = Durable` | a preceding `durability_advance{node, outcome=Synced, durable_seq >= seq}` on the same node | INV-PUB last clause; INV-LOSS reads the same `durable` map |

The second rule is also the checker that "no false durable watermark" — V1's third clause — never
had (F6). Its generator counterpart is `StorageOp::FalseDurable` (§3).

### 2.6 What INV-LAG deliberately does not assert

Spec §6.2's 1 s warn / 2 s pause ladder is a **timing** property, and the oracle cannot see enough
to judge it. The kernel owns only "no admission after the first health evaluation with
`age >= pause_ms`"; the 2.1 s figure depends on H1 delivering a health evaluation every ≤50 ms
(spec §6.2 "Health evaluation: every 50 ms plus progress events"). A scenario containing
`TimeOp::Pause{node}` or a `NetworkOp::Partition` that starves that cadence — both legal, both
generated — makes a **correct** kernel miss 2.1 s, and INV-LAG has no arming condition to stop it
firing. Worse, the oracle cannot distinguish "no evaluation arrived" from "an evaluation arrived and
the kernel did not flip".

So the ladder belongs to kernel-b's L1 rows (a kernel row plus a harness row), and the test plan
cites them for V8's timing half. The oracle carries the three transition-legality clauses above,
which are checkable from declarations and which L1's own rows do not cover. One property, one owner,
no third copy of the state machine.

---

## 3. Scenario grammar as data types

Spike §6's table becomes six enums. Each variant carries only what the environment needs; nothing
carries a clock or a random value.

```rust
pub struct Scenario {
    pub schema_version: u16,
    pub generator_version: u16,
    pub provenance: Provenance,   // F18 — see below
    pub topology: Topology,       // nodes, roles, shadows, config_version_0, partitions: u8;
                                  // F19: the INITIAL membership only — later membership is
                                  // declared by `topology_change` events from the environment
    pub budget: Budget,           // max_events, max_ticks
    pub ops: Vec<ScenarioOp>,     // the explicit, replayable choice list
}

/// F18: a reduced scenario is not in the generator's image, so replaying its seed at its
/// generator_version yields a different op list. A bare `seed` field would be read as provenance
/// and would be false. This is `rdb_core::contracts::trace::Provenance`, the same type the trace
/// header carries (trace-requirements §1 — a foundation contract ask under V-R20, not landed at
/// 8a23b1d, where the header still has `seed: u64`); the scenario does not define its own copy.
pub enum Provenance {
    Generated { seed: u64 },
    Reduced   { parent: ScenarioId },   // the scenario it was shrunk from
    Authored  { case: String },         // the constructor's name; a test name, never bytes
}

pub enum ScenarioOp {
    Client(ClientOp),
    Network(NetworkOp),
    Time(TimeOp),
    Storage(StorageOp),
    Control(ControlOp),
    Recovery(RecoveryOp),
}
```

| Group | Variants | Required boundary variants (spike §6, generator must be able to emit each) |
|---|---|---|
| `ClientOp` | `Submit{partition, req, digest, affinity_id, expected_generation, keys}`, `Read{partition, keys}`, `Status{partition, req}`, `Retry{partition, req, digest}` | `Retry` with changed digest; `Submit` with old generation; `Submit` with a foreign `affinity_id` (→ `CROSS_AFFINITY`, F14); `DropReply{req}`; `Retry` inside and outside retention. **Every variant targets a partition (V-R8)** so INV-ISO and P1's "freezes only its partition" are reachable |
| `NetworkOp` | `Deliver{msg}`, `Drop{msg}`, `Duplicate{msg}`, `Reorder{msg, before}`, `Partition{set_a, set_b}`, `Heal`, **`ForgeAck{msg, claimed_role, claimed_node}`** | stale `boot_id`/`epoch`/`config_version`; deliver a successor before its predecessor; deliver an ACK after revocation; **an ACK claiming a role or identity the topology in force at that `config_version` does not grant** (spike §4 transport: "forged identity is injectable **and rejected**"). `Heal` also emits the `schedule_phase{healed}` that arms INV-LIVE and INV-ISO (F5) |
| `TimeOp` | `Advance{ticks}`, `Fire{timer}`, `Cancel{timer, version}`, `Expire{grant}`, `Pause{node, ticks}`, `Resume{node}` | two timers at the same tick with both orders; grant skew inside and outside ±100 ms; a 24 h jump to expire dedup |
| `StorageOp` | `CompleteBatch{batch}`, `FailBatch{batch}`, `Flush{node, through}`, `Crash{node, kind, boundary}`, `Reopen{node}`, **`FalseDurable{node, through}`** | crash before and after each atomic boundary; `Flush` that errors and must advance no watermark; **a flush that reports success for data never synced** — the hard half of spike §6's "no false durable watermark", which had no op and no checker before (F6) |
| `ControlOp` | `Cas{key, expected_rev}`, `EmitWatch{rev}`, `Gap{from, to}`, `Compact{to}`, `Reload{node}` | stale snapshot revision; control quorum lost; invalid grant record; partially staged metadata |
| `RecoveryOp` | `InspectSurvivors{window}`, `SelectPrefix`, `Synchronize{to}`, `Rebuild{node}` | every unequal secondary-prefix pairing; all three lone-survivor choices; a divergent digest; a returning stale owner |

`Crash{kind}` distinguishes **process** crash (loses unflushed process buffers) from **host** crash
(may discard every unflushed suffix), per spike §6 "storage realism without disk". The distinction is
load-bearing for INV-LOSS clause (b).

`ForgeAck` and `FalseDurable` are **sim-provider faults, not mutants** (ruling V-R9). Spike §4's
transport seam already requires forged identity to be injectable, and spike §6's storage boundary
list already requires "no false durable watermark". They live in H1's network provider and M1's flush
path — never a `cfg` branch in kernel code. They are the only way to make the kernel emit a
self-consistent-but-wrong trace, which is exactly the blind spot a trace rewrite cannot reach (§7.1).
The hooks are foundation's; the request is in the handoff's routing list.

### 3.1 Generator

`gen::scenario(seed, budget) -> Scenario`. Seeded `SmallRng`-class PRNG owned by the generator,
never by the kernel. Weighted by group so that faults are common but not the majority; weights are
constants in `gen.rs`, not env-tunable, so two runs of the same seed and generator version are the
same scenario.

Three families, all from the same entry point:

1. **Random** — the seeded corpus (Q1 budget).
2. **Directed** — spike §6's four mandatory cross-package cases (A1/P1
   expire-between-publish-and-reply; F1/R1 discovery window; F1/T1/P1 24 h retained status;
   F1/T1 same/different digest across recovery), written as **Rust constructors**
   (`fn case_a1_p1() -> Scenario`), `provenance: Authored` (F18). Hand-writing a 30-op JSON list that
   lands "expire authority between publication and reply" is fragile, uncompiled, and must be
   re-typed on every grammar change. D4's plain-data argument is right for **reducer output**, which
   must round-trip; it does not follow for authored cases.
3. **Regression** — `fixtures/regressions/*.json`, produced by the reducer, replayed every run.
   JSON here because it must round-trip a value no human authored.

**Required boundaries are scheduled, not hoped for (critic T-12, ruling V-R19).** Hitting every
required coverage cell is a property of the seed *list*, not of luck over 64 weighted draws. Let
`REQUIRED: [BoundaryId; N]` be foundation's closed `BoundaryId` set in declaration order — N = 29
today; foundation's architect lists the members in their handoff, and the planner's enumeration row
asserts set equality against the enum, so this design never repeats the list. Seed `i`, counting
from `SPIKE_SEED_BASE`, is **obliged to attempt** `REQUIRED[i mod N]`: before filling the budget
from the weighted draw, the generator places at least one op that produces that boundary at a
seeded position. Every corpus of at least N seeds therefore attempts every required boundary at
least once, deterministically, and the same seed still yields the same scenario (the obligation is
a function of `i`, not of the PRNG). The weighted draw may add more boundaries on top; the schedule
is a floor, not the distribution.

"Attempt" is the generator's obligation; the **hit** is still counted from the environment's
`fault_injected{boundary}` event. A scheduled boundary the environment cannot reach shows as a
`required_missing` cell and fails the run — never assumed hit because it was scheduled.
**The required-cell gate applies only when `SPIKE_SEEDS >= N` (critic T-25, ruling V-R20)** —
always true for the default 64-seed corpus and for commands 2 and 3 in §5.1.1. A corpus smaller
than N cannot attempt every member of `REQUIRED` by construction, so it records coverage, writes
`coverage_gated: false` into `rdb-m7-coverage.json` (§5.3), and never fails on
`required_missing`; the sub-N campaign rows (determinism, thread count, truncation, one injected
seed) run under that branch and say so in their inputs. `coverage_gated: true` with a non-empty
`required_missing[]` is the only failing combination. The
`BoundaryId -> producing ScenarioOp` table lives beside `REQUIRED` in `gen.rs`, enumerated so that
a member with no producer fails a row instead of passing quietly.

**Every required cell is keyed on the package whose provider emits its `fault_injected` (critic
T-35, ruling V-R20)**, not only the two hook cells. `fault_injected` is an environment event: a
boundary's cell can only be hit once the provider that injects it is wired, so a cell whose
emitter reports `capability{state=Unavailable}` at trace start is recorded as
`unavailable(package)` rather than `missing` — **excluded from `required_missing[]` by that
capability entry, never by editing the required list**. When the package reports `Wired`, the
cell is required again with no code change. The `BoundaryId -> PackageId` gating table is a const
in `coverage.rs` next to `REQUIRED`, covered by the same enumeration row, keyed **per family**,
with foundation's handoff naming the emitter beside each of the 29 members (their list is
authoritative; the planner's enumeration row asserts against it). Provisional family map until
that handoff lands: `Network`, `Time` and `Control` members → H1 (scheduler, clock, network and
fake control are H1's providers); `Storage` members → M1; `Client` and `Recovery` members → I1
(the dispatcher applies those ops). Two members additionally need a hook inside their package
(V-R9): `ForgedIdentity` needs H1's `ForgeAck` path and `FalseDurableWatermark` needs M1's
`FalseDurable` path; the hook ships with the package (handoff round 2 §D assumption 1), so no
separate key is needed. **The table is checked against the trace, not against itself (critic
T-40):** C0 has no `impl BoundaryId` and no `fault_kind()`, so a member filed under the wrong
family would pass an enumeration row that reads the same const it is testing, and would inherit
the wrong gating package — the "excluded by capability, never by editing the list" loophole
re-entered by another door. `fault_injected` already carries `fault_kind` beside `boundary`, so
the campaign asserts, for every observed `fault_injected`, that `fault_kind` equals the family
this table assigns to that event's `boundary` — ground truth from the emitting provider, needing
no contract change. The planner hangs it on M7V-42 or M7V-55. Without the per-family key, I1 landing before H1 or M1 would fail every
Network and Storage cell as `missing` — a red that contradicts "green with invariants
`unavailable`, by design" and whose reflex fix is deleting cells.

### 3.2 Bounds

`Budget { max_events, max_ticks }`. The generator never emits beyond `max_events`; the runner stops
at it. Unbounded search is forbidden (charter DO-NOT).

**There is no `heal_at_event` (F5).** It was an index into the *event* stream, while the reducer
deletes *ops* and shortens that stream, and nothing rebased it. Two failure modes followed: the heal
point drifting past the end (INV-LIVE disarms, the candidate "passes", minimization silently
degrades) and the heal point drifting earlier (INV-LIVE arms over a window it was never armed over,
producing a **new** liveness failure on a correct kernel with the same signature — the reducer
manufacturing its own bug). Healing is now the existing `NetworkOp::Heal` op, so ddmin moves it with
the list and `scenario_op_index` stays meaningful. This removes a field and removes the defect.

---

## 4. The reducer

### 4.1 Input and output

In: a `Scenario` plus the `Signature` its run produced.
Out: **two** files (F4). The minimized `Scenario` at
`fixtures/regressions/<signature-slug>.json`, and the **original** at
`fixtures/regressions/<signature-slug>.orig.json`. `regressions.rs` replays both.

Committing the original costs a few KB per failure and closes the slippage hole: the minimized
fixture is the one that might have slipped to a different root cause, the original is the one that
definitely reproduces the defect that was found. V-R6 puts `validation/<run-id>/` under
`RETCD_TEST_LOG_DIR`, per-invocation and gitignored — so without this, the only artifact that
definitely reproduces the real defect is deleted by design.

### 4.2 Algorithm

`ddmin` (Zeller's delta debugging) over `Scenario::ops`. **Deletion only.**

There is no per-op field-simplification pass (F12). The earlier design had one, restricted to
"monotone" fields — but `Advance{ticks}` is not monotone with respect to the failure class: ticks
drive the 1 s/2 s protection thresholds, grant `expiry_tick`, the ±100 ms skew boundary, the 2 s
discovery window and the 24 h dedup jump. Shrinking ticks moves the run across those guards. When
that changes the `rule` string ddmin rejects and the step is wasted budget; when it preserves `rule`
but changes which guard fired, it is a second slippage channel. `research.md` §1's own source
(`proptest-stateful`) says per-op shrinking "tends to break preconditions in a way that is difficult
to compensate for" and reports removal-only shrinking sufficient in practice — the earlier design
quoted that and then added the pass anyway. Deleted. Reopen only if a real reproducer proves
unreadable, and then shrink ticks last with an explicit re-check that the original still fails.

Hard bounds, three of them (F11), because a per-failure cap alone is not a bound on a run:

| Knob | Default | Why |
|---|---|---|
| `SPIKE_SHRINK_STEPS` | 2,000 | re-runs per distinct signature |
| `SPIKE_SHRINK_MAX_FAILURES` | 3 | shrink at most K **distinct signatures** per run; record the rest unminimized |
| `SPIKE_SHRINK_BUDGET_TOTAL` | 20,000 | aggregate re-runs per run, across all failures |

One failure at the per-failure cap costs up to 2,000 re-runs × 2,000 events = 4,000,000 events —
**twice the entire 1,000-seed corpus**. With `--no-fail-fast` and N failing seeds that is N × that,
uncapped. ddmin is O(n²) worst case over a several-hundred-op list, so exhausting the cap is the
normal case, not the tail. Shrink time is excluded from the campaign's reported `wall_ms` and
reported separately as `shrink_ms` (§5.3). When a budget is spent, emit the best candidate so far
and say so in the artifact.

### 4.3 Why causality survives (the D3 argument)

The reducer never edits a trace. It edits the **scenario**, then re-runs the real kernel in the real
deterministic environment. Whatever trace comes back is, by construction, a trace the kernel can
produce. There is no such thing as an impossible minimized trace, so there is no causal-repair code,
no happens-before graph, and no trace-validity checker.

Cost: each shrink step is a full re-run (bounded by `max_events`, which is small). Benefit: the
entire class of "the minimizer produced a reproducer that cannot happen" bugs does not exist.

This is the generator-reduction idea (GReduce, TOSEM 2024) rather than the trace-surgery idea
(DEMi, NSDI 2016). DEMi's machinery exists because it minimizes traces of a system it cannot cheaply
re-run. We can re-run: a whole history is ≤2,000 events with no IO. See `research.md`.

**Consequence to state plainly:** an op whose removal changes which *later* ops are meaningful (e.g.
removing the `Submit` that a `Retry` refers to) produces a scenario the environment must handle. The
environment's rule is **ignore an op whose referent no longer exists**, recorded as its **own event
kind**, `op_skipped { scenario_op_index, reason }` (F17) — *not* as
`fault_injected{boundary="op_skipped"}`. `BoundaryId` stays exactly equal to spike §6's required
boundary column; that identity is what makes the "every required cell ≥ 1" coverage rule writable,
and polluting it with a reducer artifact puts an uninterpretable cell in
`rdb-m7-coverage.json`. That keeps the scenario total without any dependency tracking. Row M7V-22
asserts a skipped op never invents an event.

### 4.4 Failure signature

The signature is what must survive shrinking. Its **core tuple** excludes anything that shifts when
ops are deleted; one further field is carried alongside, for reporting only.

```
Signature {
  checker: &'static str,           // "INV-LIN"
  rule: &'static str,              // "predecessor_digest_mismatch"
  partition: PartitionId,
  role: Role,                      // role of the node in the violating event
  event_kind: TraceEventKind,      // kind of the violating event
  faults: BTreeSet<BoundaryId>,    // recorded and reported, NOT part of the acceptance predicate
}
```

The **acceptance predicate** for ddmin is the **core tuple** `(checker, rule, partition, role,
event_kind)` — nothing else. `faults` is recorded in the signature and **reported**; it is never
compared to accept or reject a candidate.

Why `faults` is not in the predicate (F21). ddmin's job is to delete ops, and ops are what emit
`fault_injected{boundary}`, so a useful minimization almost always drops boundaries. Under set
equality every such candidate is rejected and the reducer converges on roughly the original
scenario, which defeats the point. Subset is no better: in the worked example below the slipped
candidate's boundary set is a *subset* of the original's, so slippage returns. Neither predicate is
right, so `faults` is not a predicate at all.

**Signature slippage**, and what actually defends against it. The core tuple is coarse enough that
two different root causes can share it. Worked example, both reachable in this grammar: an F1 bug
(recovery selects a cutoff whose digest does not match the root) and an R1 bug (a duplicate
`Deliver` admits a successor twice) both produce
`INV-LIN / predecessor_digest_mismatch / partition 0 / Primary / batch_apply`. ddmin deletes the
`RecoveryOp` block, the candidate still fails identically via the R1 path, ddmin accepts it and
keeps deleting; the emitted fixture reproduces the R1 bug. Fix R1, the fixture goes green, the F1
bug ships. The defence is the **`.orig.json` companion** (§4.1), which `regressions.rs` replays
beside every minimized fixture: fix R1 and the minimized fixture goes green, but the original still
fails, so the F1 bug cannot ship silently. `faults` is the **signal**, not the gate — when
`faults_after != faults_before` the run writes `slipped: true` into `rdb-m7-campaign.json` naming
both sets, so a reviewer knows which fixtures to distrust and which `.orig.json` to read.

Excluded on purpose: `event_id`, `seq`, `logical_tick`, key ids, node ids, the scenario length.
Those all move under shrinking; a signature that includes them makes ddmin reject every candidate.

Row **M7V-20**: inject a known violation, shrink it, assert the **core tuple** is equal before and
after, `ops_after.len() < ops_before.len()`, and the `.orig.json` companion replays and fails.
Row **M7V-21**: the minimized fixture replays through I1 and fails the **same** checker.
Row **M7V-23** (F4, restated by F21): a scenario carrying two independent injected defects that
share the core tuple shrinks; the campaign artifact records `slipped: true` naming both fault sets;
and the `.orig.json` companion still replays and fails.

### 4.5 Every fixture must be realizable by the runner (obligation, carried once I1 lands)

A hand-built trace (`TraceBuilder`, the planner's VA-1) or an authored scenario proves what a
checker does with a *shape*. It does not prove the shape can occur. A checker tuned to an
unrealizable fixture arms in its unit row and never arms in the campaign; under §2.4 that now
surfaces as `unavailable(not_armed)` with `seeds_armed == 0` instead of `proven` — visible, but
still a checker that guards nothing (critic round 2, the second-order form of the planner's R-3).

The obligation, as one row the planner carries with dependency I1 (`sim` class,
`Unavailable(Capability(I1))` until then):

| Fixture kind | Must | Evidence |
|---|---|---|
| every `tests/fixtures/scenarios/*` and every authored constructor (§3.1 family 2) | replay through I1's runner without `op_skipped{reason=ReferentGone}` and produce the oracle verdict its row expects | the runner's trace, the oracle report |
| every `TraceBuilder` trace an oracle row feeds to a checker | pass the same well-formedness checks I1 applies to a recorded trace: strictly increasing `event_id`, the `capability` block first, `schedule_phase` before any liveness arming, a `replication_ack.contiguous_seq` never above the emitting node's last `batch_apply.seq`. **So that no row author can forget the last rule (critic T-26, ruling V-R20): `TraceBuilder::ack_from(n, s)` emits the acking node's `batch_apply{node=n, role=<n's declared role>, seq=s}` and then the `replication_ack{from_node=n, contiguous_seq=s}` — two events from one call — and every fixture carrying an ack from a peer is built with it.** The role is **not** hard-coded to `RegularSecondary`: the helper reads the role the fixture's topology declares for `n`, so a shadow ack (`role=Shadow`, M7V-07 and MUT-2) is built with the same helper and cannot silently acquire a regular's role (critic T-41). The zero-event control (§2.4) is header plus the ten `capability{state=Wired}` lines and nothing else, which passes this validator and still arms nothing | the validator's result. If I1 exposes no validator, the row is limited to the envelope checks and says so |

A fixture that fails this row is a fixture bug and is fixed in the fixture; the assertion it
carries is never weakened to make it realizable (charter DO-NOT).

---

## 5. The campaign

### 5.1 Budget knobs

| Var | Default (plain `cargo test`) | PR corpus | M7 release gate (V-R18) | Extended gate | Meaning |
|---|---|---|---|---|---|
| `SPIKE_SEEDS` | 64 | 1000 | 1000 (the `RETCD_EVIDENCE=1` full scale) | 10000 | histories per run |
| `SPIKE_MAX_EVENTS` | 512 | 2000 | 2000 | 2000 | events per history |
| `SPIKE_SEED_BASE` | 0 | 0 | 0 | 0 | first seed; the 10k corpus is a superset of the 1k corpus so a PR failure reproduces in the extended run |
| `SPIKE_SHRINK_STEPS` | 2000 | 2000 | 2000 | 2000 | reducer re-runs per distinct signature |
| `SPIKE_SHRINK_MAX_FAILURES` | 3 | 3 | 3 | 3 | distinct signatures shrunk per run (F11) |
| `SPIKE_SHRINK_BUDGET_TOTAL` | 20000 | 20000 | 20000 | 20000 | aggregate reducer re-runs per run (F11) |
| `SPIKE_ASSERT_WALL_MS` | unset (record only) | unset (record only) | `60000` (the charter figure, host-qualified) | `600000` (spike §7: 10,000 in 10 min) | **V-R11: wall time is recorded in the PR default and asserted only in a gate run** |
| `SPIKE_REQUIRE_ALL` | unset | unset | `1` | `1` | fail if any invariant is not `proven`; `proven` with `seeds_armed == 0` fails regardless (§2.4) |
| `RETCD_EVIDENCE` | unset | unset | `1` | `1` | ADR-0031 semantics, reused verbatim: full configured scale, `full_scale: false` fails the gate |

The full configured scale under `RETCD_EVIDENCE=1` is the PR corpus (1,000 × 2,000), as checked-in
constants; the extended gate overrides `SPIKE_SEEDS` explicitly. The extended column's wall figure
was `60000` in the previous revision, which contradicted spike §7's 10-minute budget for 10,000
histories; corrected here.

### 5.1.1 The command that produces the number

Nothing in the first draft named it, and the charter's evidence command **cannot** produce a warm
*release* number. Verified in-repo:

- `scripts/gate.sh` line 45: `run_test() { cargo test --workspace --no-fail-fast "$@"; }` — no
  `--release`.
- `Cargo.toml`: `[profile.test] opt-level = 0` with `[profile.test.package."*"] opt-level = 2`.
  `rdb-core` and `rdb-sim` are workspace **members**, not `package."*"` dependencies, so under the
  plain gate command the campaign *and the kernel it drives* compile unoptimized.

Ruling **V-R11** settles it. The release corpus command, written down and owned:

```bash
CARGO_TARGET_DIR=.rtargets/campaign scripts/gate.sh test --release -p rdb-sim --test campaign
```

`.rtargets/campaign` is **reserved** for this command. Extra args pass through `gate.sh`, but a
release build lands in a second profile subdirectory, so sharing a target dir with a debug gate run
means a cold build each time — and AGENTS.md's "never two cargo invocations against one target
directory" rule (the 2026-09-19 `LNK1104` collision) bites harder with six concurrent agents.
Reserving the directory is the whole mitigation.

The ordinary `scripts/gate.sh test -p rdb-sim --test oracle --test scenarios --test campaign` stays
the handoff gate. It runs the default 64-seed corpus unoptimized, records its `wall_ms`, and asserts
nothing about it.

**Three commands, two artifacts (critic T-13/T-14a, rulings V-R17/V-R18).** Written here and in
ADR-rdb-0019 §2, which is the authoritative copy:

| Purpose | Command | Writes |
|---|---|---|
| Handoff gate: default corpus, debug | `CARGO_TARGET_DIR=.rtargets/verification scripts/gate.sh test -p rdb-sim --test oracle --test scenarios --test campaign` | `docs/evidence/rdb-m7-campaign.json` (`profile: "debug"`, `full_scale: false`) |
| The 1,000-history number: warm release, full scale | `RETCD_EVIDENCE=1 CARGO_TARGET_DIR=.rtargets/campaign scripts/gate.sh test --release -p rdb-sim --test campaign` | `docs/evidence/rdb-m7-campaign-release.json` (`profile: "release"`, `full_scale: true`) |
| **M7 release gate**: the run whose green is the milestone claim | `SPIKE_REQUIRE_ALL=1 RETCD_EVIDENCE=1 CARGO_TARGET_DIR=.rtargets/campaign scripts/gate.sh test --release -p rdb-sim --test campaign` | the release artifact, with every invariant `proven` and `seeds_armed > 0`, or a failure naming the invariant and reason |

The artifact **name is chosen by the build profile** (`cfg!(debug_assertions)`), not by an
environment variable, so the two commands cannot overwrite each other and a debug `wall_ms` cannot
land in the file the milestone cites. **Only `rdb-m7-campaign-release.json` may be cited for the
1,000-history budget.** Two files rather than one file keyed by profile because ADR-0031's schema
has one `host`, one `build` and one `run{}` per file, and a debug run and a release run differ in
exactly those; folding them would put two `run{}` blocks inside `values` and would need
`write_evidence` to read-modify-write a file left by an earlier run, which the helper does not do
(V-R5: reused as is) and which would make the artifact depend on a stale file of unknown
provenance. The M6-113 conformance row and the M6-116 no-production-claim grep already work per
file.

No `scripts/` change by this team. The gate stays a documented command until foundation wires it
into `gate.sh` after F-R11 lands; until then the M7 release gate is run by hand and its artifact is
the evidence.

Default-small matters: the campaign must run as an ordinary regression on every `scripts/gate.sh`
invocation, not as an `#[ignore]`d suite (ADR-0031's OQ-68 reasoning). Reduced scale changes seed
count and event cap only — never which checkers run, never which fault kinds are reachable.

### 5.2 Reaching 1,000 histories in 60 s

The budget is 2,000,000 events in 60 s ≈ 33k events/s single-threaded-equivalent. Four design rules
make that a non-event rather than an optimisation project:

1. The oracle allocates nothing per event on the happy path. `Violation` formatting is the only
   allocation, and it happens once per run at most.
2. A passing seed retains nothing. The trace is consumed streaming and dropped; only the `Scenario`
   (a few hundred bytes) and the coverage counters survive. Peak memory is one trace.
3. Seeds are independent, so the loop chunks across `available_parallelism()`. Results are collected
   per seed and merged deterministically, so the report does not depend on thread count.
4. Compile time is excluded from the reported number and reported separately (spike §7).

**The 60 s figure is host-qualified and is an acceptance target, not a measured speed.** Spike §7
says exactly that about its own budgets ("proposed acceptance targets, not previously observed
speeds") and requires measurement on "one recorded CI worker". This host is a shared Windows Server
2022 VM running up to six concurrent agent cargo invocations (ledger: "Concurrency: up to 6 agents").
`available_parallelism()` chunking preserves determinism but says nothing about contention. So:

- **recorded** in `rdb-m7-campaign.json` (`wall_ms`, alongside ADR-0031's existing `host` and
  `build`) in the PR default;
- **asserted** only in a gate run — the M7 release gate or the extended gate (§5.1) — via
  `SPIKE_ASSERT_WALL_MS` (V-R11).

A wall-clock threshold in a PR test is the pattern this repository abandoned — AGENTS.md records
`m4_69`, a capacity row failing on a loaded host with a third of the patience it was accepted with,
and `test-plan-m6.md` §7's rule is "assert invariants, *record* numbers, never a threshold". The
first draft's "the only asserted budget is the campaign's own wall time" contradicted the precedent
it cited; this replaces it.

If the budget is missed, the rule from spike §7 applies: improve the harness or revise the budget
explicitly, in the ADR. Never lower an assertion.

### 5.3 Per-run outputs

- stdout: the per-invariant status table (`proven` / `unavailable(capability(p))` /
  `unavailable(not_armed)` / `violated`, each with `seeds_armed`), seed count, event total, wall
  time, and the coverage shortfall list. **One `reason` form per surface (critic T-30, ruling
  V-R20):** the JSONL log line `invariant_status` carries `reason` (`capability` | `not_armed`)
  and, when `capability`, a separate `package` field, so a DuckDB projection names the owner
  without parsing a string; the evidence artifact carries the ADR form, one string
  `reason: "capability(<package>)"` or `"not_armed"`. The two are a bijection, and the planner's
  VA-7 states both.
- `docs/evidence/rdb-m7-campaign.json` (debug) or `docs/evidence/rdb-m7-campaign-release.json`
  (release; §5.1.1, V-R17) via the shared `write_evidence()` helper (ADR-0031 schema, unchanged;
  V-R5: `rdb-sim` takes `config-testkit` as a dev-dependency and reuses it as is).
  `values` carries: `seeds`, `max_events`, `events_total`,
  `invariants{id -> {status, reason?, seeds_armed}}` (`reason` present only when `status` is
  `unavailable`: `capability(P1)` or `not_armed`; one object per invariant so the three facts cannot
  disagree across maps — V-R16), `mutations{id -> catching_row}` — **the key is unchanged and the
  value is list-valued** (V-R20 (5)): a one-element list for MUT-1/3/4/5 and
  `["M7V-69", "M7V-81"]` for MUT-2, because MUT-2 is caught in two halves and a scalar would drop
  one — `wall_ms`, **`shrink_ms` as a separate value** (F11 — shrink time is not campaign time),
  `compile_ms_excluded`, and `profile`. **Two keys are deliberately *not* here (critic T-37):**
  `coverage_gated` belongs to `rdb-m7-coverage.json` alone, and there is no campaign `coverage{}`
  summary at all. An earlier draft of this bullet carried `coverage{required_cells, hit_cells,
  missing[]}`; it is removed rather than added to the ADR, because the coverage artifact below
  already carries the full matrix, `required_missing[]`, `unavailable_cells{}` and
  `coverage_gated`, and a second summary of the same counts in a second file is one more place for
  them to disagree — the same reason V-R16 put status, reason and `seeds_armed` in one object. Its
  `missing[]` was also spelled `required_missing[]` everywhere else, so the removed key was the
  only copy of a dead name. **The ADR needs no change for this**: its key list never had either
  key. Each minimized fixture entry also
  carries `slipped` (F21): `true` when the failing run's `faults` set differs before and after
  shrinking, with both sets named, so a reviewer knows which fixtures to distrust.
- `docs/evidence/rdb-m7-coverage.json`: the full observed matrix, reported not gated, plus
  `unavailable_cells{cell -> package}` for the cells excluded from `required_missing[]` by their
  emitting package's capability entry (§3.1, V-R19, T-35), and **`coverage_gated: bool`** —
  `true` when `SPIKE_SEEDS >= N` and the required-cell gate applied, `false` for a sub-N corpus
  that only recorded (§3.1, T-25).
- On failure: `validation/<run-id>/` **under `RETCD_TEST_LOG_DIR`** (V-R6, per-invocation,
  gitignored) with the schema-versioned event stream, the original and minimized `Scenario`, and the
  signature (spike §7). The persisting copies of both scenarios go to
  `tests/fixtures/regressions/` (§4.1).

---

## 6. Coverage matrix

Counters, never a percentage. Spike §7: "percentage coverage alone cannot waive a missing invariant."

Three axes, each with an explicit **required** list checked into `coverage.rs`. A required cell with
zero hits fails the run — the M6-107 enumerator pattern, so a missing case fails instead of passing
quietly. Two scopings, both from §3.1: the gate applies when `SPIKE_SEEDS >= N`, and a smaller
corpus records and writes `coverage_gated: false` (T-25); a cell whose emitting provider package
reports `Unavailable` lands in `unavailable_cells`, not `required_missing` (T-35).

| Axis | Cells | Required |
|---|---|---|
| **Guard outcomes** | `authority_decision.gate × outcome` (4×4); `batch_apply` ancestry × {ok, gap, digest_mismatch}; dedup × {miss, hit, reuse_reject, expired, cross_affinity}; condition × {pass, fail}; `version_check` × {accept, refuse}; admission × each §5.4 reject reason; `protection_state` × {healthy, warn, paused, resuming}; `recovery_decision.mode` × {two_survivor, lone_survivor_readonly, quarantine}; **`replication_ack.reject_reason` × {Gap, DigestMismatch, StaleEpoch, StaleBoot, StaleConfig, ForgedIdentity, IncompatibleVersion}** (F16 — `ForgedIdentity` existed in the vocabulary with no op and no cell, so the gap could not fail a run); **derived quorum rule × {RF3, DEGRADED_RF2}** (F1 — a degraded publish path that is never exercised is the F1 bug hiding; the rule is derived from `required_copy_set.len()` under V-R20, so the cell is keyed on the derived value and named `derived_quorum_rule`, not on a trace field) | every cell ≥ 1 |
| **Fault boundaries** | one cell per "required boundary case" in spike §6's table (the right-hand column), named as a const list. Includes the two new ops' boundaries: **forged-identity ACK** and **false durable watermark** | every cell ≥ 1 |
| **Pairwise faults** | unordered pairs of the six op groups that co-occur within one history (15 pairs), plus the four mandatory cross-package cases as named cells, plus **one isolation cell** (V-R8: a run in which partition A is blocked while partition B makes progress under a healed schedule) | the four named cases and the isolation cell ≥ 1; the 15 pairs reported, not required |

Reported-not-required for the 15 pairs is deliberate: some pairs are meaningless (a `Control` CAS
race during an unhealed `Network` partition with no client traffic) and a required-but-unreachable
cell becomes a cell someone deletes.

---

## 7. Mutation checks (spike §7)

Five named mutations, in **two classes** (ruling V-R9). Each must be caught by a **named** checker;
the row id is the name.

### 7.1 Trace rewrites — MUT-1, MUT-3, MUT-4

Applied to a recorded good trace, then fed to the oracle. These three are genuinely pure checker
tests: the fault they describe is a *shape* the oracle must reject, and no kernel bug is needed to
produce it.

| ID | Mutation | Rewrite | Must trip |
|---|---|---|---|
| MUT-1 | accept stale authority | flip one `authority_decision{gate=publication}` from `expired` to `valid` and let the following `publish` stand | INV-AUTH |
| MUT-3 | publish before ACK | move one `publish` event to before its `replication_ack` | INV-PUB |
| MUT-4 | skip ancestry | set one `batch_apply.predecessor_digest` to the digest at `seq-2` | INV-LIN |

The honest boundary for this class, stated once so nobody over-reads it:

> A trace rewrite proves **the oracle detects that fault class**. It does not prove the kernel is
> free of it. The kernel side is covered by the campaign running the real kernel against the real
> checkers.

### 7.2 Injected faults — MUT-2, MUT-5

Driven by `NetworkOp::ForgeAck` and `StorageOp::FalseDurable` (§3), through the sim's own providers.
They test the strictly stronger claim — **kernel plus oracle rejects it** — and they are the only
way to reach the blind spot in §2.5.

| ID | Mutation | How | Must trip |
|---|---|---|---|
| MUT-2 | count a shadow ACK | `NetworkOp::ForgeAck { claimed_role: RegularSecondary }` (landed `ReplicaRole`; there is no `Regular`) from a node the environment topology in force at that `config_version` lists as a shadow | INV-PUB (topology-vs-claim mismatch), or the kernel rejects it with `AckRejectReason::ForgedIdentity` |
| MUT-5 | mark buffered as durable | `StorageOp::FalseDurable { node, through }` — a flush completion M1 never performed | INV-PUB's durability-grounding clause and INV-LOSS |

**Why these two moved (F3).** As trace rewrites they were tautologies for the interesting bug.
Flipping `peer_role` from `shadow` to `regular` in a recorded trace proves INV-PUB reads the field.
But the real bug — R1 resolving `peer_role` from the ack *message* rather than from configured
topology — produces a trace in which that shadow is labelled `Regular` **everywhere**,
self-consistently; every cross-check agrees and INV-PUB passes it. Same for a buffered ACK mislabelled
`Durable`. The rewrite tests the checker against a fault shape the kernel would never produce.

This is **not** new scope and **not** a `cfg` branch. Spike §4's transport seam already requires
"forged identity is injectable **and rejected**"; spike §6's storage boundary list already requires
"no false durable watermark". Both are required sim-provider capabilities that had simply not been
written down as ops. The ask on foundation is a hook in H1's network path and M1's flush path — two
enum variants, two provider arms, two rows.

Cross-check: does reading `topology` smuggle a second protocol into the oracle? No. The
topology — the header's declaration plus the environment's `topology_change` events (F19) — is a
declared **input** to the run, not a decision derived from protocol state, and
`durability_advance` was already in the vocabulary — the new clause is an ordering check between two
declared events on one node.

---

## 8. Deliberately NOT built

Stated so a reviewer does not read absence as oversight.

| Not built | Why |
|---|---|
| A second implementation of the protocol inside the oracle | Spike §6: the simulator must not contain one. An oracle that re-derives the answer only tests that two codebases agree. |
| A linearizability checker (Knossos / Elle style) | The partition executor serializes one transaction at a time and the trace **declares** the publication order. There is no concurrent history to search. A search-based checker would be exponential and would buy nothing the declared order does not already give. |
| A happens-before graph / causal trace surgery | D3 removes the need. §4.3. |
| `proptest` strategies for the campaign | See Q-1. Its shrinking is coupled to value generation; ours is coupled to scenario re-execution. Two shrinkers is one too many. |
| Coverage-guided fuzzing (libFuzzer / AFL) | Needs instrumentation and a build the host is not set up for; the coverage matrix is a named-cell requirement, which is the thing spike §7 actually asks for. |
| A model checker (TLA+, Stateright) | M7 is a simulation spike. Exhaustive schedule enumeration is explicitly out of the budget; V2 is "model only" and the authority checker carries it. |
| Wall-clock anything | Charter DO-NOT. All time is simulated ticks. |
| Real disk, RocksDB, FFI, multi-thread memory races, suspend clock | Spike §6 last paragraph: separate adapter/platform gates. D1 (M8), V7/V10/V11 (M12). |
| Liveness under an unhealed partition | Spike §6: not a liveness failure. The checker disarms. |
| Any performance assertion in the PR default | Spike §7 budgets are measured and **recorded**; wall time is asserted only in a gate run — the M7 release gate or the extended gate (V-R11, §5.1, §5.2). `test-plan-m6.md` §7: assert invariants, record numbers, never a threshold. |
| A per-op field-shrinking pass in the reducer | F12, §4.2. Removed: ticks are not monotone with respect to the failure class, and the design's own cited source recommends removal-only shrinking. |
| `Budget::heal_at_event` | F5, §3.2. Removed: an absolute event index under a reducer that shortens the event stream. `NetworkOp::Heal` already existed. |
| The 1 s / 2.1 s protection **timing** ladder in the oracle | F8, §2.6. Not checkable from declarations, and a legal `TimeOp::Pause` makes a correct kernel miss it. Owned by kernel-b's L1 rows. |
| A prefix-compatibility algorithm in INV-LIN | F2, §2.3. That is F1's job; the oracle does two hash-map lookups against facts it already recorded. |
| A growing shrink/fuzz corpus | Two files per past failure (minimized + original, F4), and nothing else. A growing binary corpus in git is a cost with no reader. |

---

## 9. Dependencies and sequencing

| Needs | From | Blocking? |
|---|---|---|
| Trace vocabulary + event shapes (C0) | foundation | **Landed at 8a23b1d.** Checkers are written against the landed names; `trace-requirements.md` §8 is the drift table (field → landed shape → resolution). One contract ask stays open under V-R20: header `provenance: Provenance` in place of `seed: u64` (F18, T-23), which blocks only the planner's provenance row. |
| Replay runner (I1) | foundation | Blocks M7V-21 (minimized trace replays) and the campaign loop. Oracle rows M7V-02..19 run on hand-built traces without it. |
| `support/mod.rs` registration | foundation | Blocks compilation of our test targets. Requested in the handoff. |
| `capability{package, state}` events, derived from `Module::capability(&self)` (K-F-10) | foundation + kernel teams | Blocks honest `Unavailable(Capability)` reporting. Without it the campaign cannot distinguish "no violation" from "nothing ran". `Unavailable(NotArmed)` needs no trace field — it is the oracle's own end-of-fold conclusion (§2.4). |
| **H1 network hook for `ForgeAck`; M1 flush hook for `FalseDurable`** | foundation | Blocks MUT-2 and MUT-5, and with them V1's "no false durable watermark" clause. Both are spike §4/§6 required provider capabilities (V-R9). |
| **`protection_state` emitted on every `config_version` change** | foundation (C0 vocabulary) | Blocks INV-PUB's degraded-RF2 rule and INV-LAG clause (b). V-R10. |
| **`topology_change{config_version, nodes}` emitted by the environment** | foundation (C0 vocabulary + H1/control provider) | Blocks INV-PUB role grounding once membership changes. Without it the role-mismatch clause fires on a correct kernel across the `DEGRADED_RF2` rebuild path, which the coverage matrix requires. V-R12. |
| **`replication_ack` emitted where generated (the secondary)**, plus a delivery record at the primary | foundation (C0 vocabulary) | Blocks INV-LOSS: if only *delivered* acks are traced, a secondary whose ack was dropped is invisible as a holder and the oracle permits loss it should forbid. V-R10. |
| `SurvivorInventory::debug_view()` plain data (ruling B-R5) | kernel-b | Not blocking for the oracle — INV-LIN reads `recovery_decision` from the trace. The test planner consumes `debug_view()` for M7V rows only. |
| Kernel behaviour A1 T1 R1 P1 L1 F1 | kernel-a, kernel-b | Blocks `Proven`, not the campaign. Until they land every invariant reports `Unavailable`. |

Order of work for the developer: hand-built traces and checkers (no dependencies) → grammar and
generator (no dependencies) → reducer (needs the runner) → campaign (needs the runner and
capabilities).

---

## 10. Rulings this design is built on

All seven original questions are answered; nothing here is open.

| Ruling | Effect on this document |
|---|---|
| V-R1 no proptest | §4.2 custom `ddmin` |
| V-R2 shared `docs/evidence/`, `rdb-` prefix | §5.3 |
| V-R3 INV-VER and INV-LAG stay in the oracle | §2.3 (INV-LAG narrowed to transition legality by F8, §2.6 — that is a scope *correction*, not a removal) |
| V-R4 → V-R9 mutation strength | §7 split into two classes |
| V-R5 `rdb-sim` dev-depends on `config-testkit`, reuse `write_evidence` as is, no `config-*` change | §5.3 |
| V-R6 `validation/<run-id>/` under `RETCD_TEST_LOG_DIR`, gitignored; persisting reproducers in `tests/fixtures/regressions/` | §4.1, §5.3 |
| V-R7 "ADR-rdb-NNNN" in prose | throughout |
| V-R8 multi-partition in M7 | §2.1 `unresolved`, §2.3 INV-ISO, §3 per-partition `ClientOp`, §6 isolation cell |
| V-R9 `ForgeAck` / `FalseDurable` as sim-provider faults | §3, §7.2 |
| V-R10 three trace lines | `trace-requirements.md` §3.5, §3.8, §3.14 |
| V-R11 wall time recorded in PR, asserted in extended gate; `.rtargets/campaign` reserved | §5.1, §5.1.1, §5.2 |
| V-R12 environment-emitted `topology_change`; roles resolved per `config_version` | §2.1, §2.3 INV-PUB, §2.5, §3 `Topology`, §7.1 MUT-2; `trace-requirements.md` §3.19, ask 7 |
| B-R3 RF2 = `min_regular_acks` 1-of-1 under the pinned config | §2.3 INV-PUB |
| B-R5 `SurvivorInventory::debug_view()` | §9 (test planner consumes it; the oracle does not need it) |
| V-R16 two `Unavailable` reasons; `proven` implies `seeds_armed > 0` | §2.4, §2.3 INV-LIVE/INV-ISO, §5.3 `invariants{}` shape, §9 |
| V-R17 second artifact `rdb-m7-campaign-release.json`; release command sets `RETCD_EVIDENCE=1` | §5.1.1, §5.3; ADR-rdb-0019 §2 |
| V-R18 M7 release gate command stated, no `scripts/` change; `capability{state}` derived from wiring | §5.1 (new column), §5.1.1; §2.4 last paragraph |
| V-R19 required boundaries scheduled `i mod N`; hook-gated cells excluded by capability (widened to every cell per family by V-R20) | §3.1, §5.3 coverage artifact |
| V-R20 (round 3): quorum rule derived from `required_copy_set.len()`, never a field; `provenance` routed to foundation; `armed()` is end-of-fold state and `seeds_armed` counts `Proven` seeds; required-cell gate scoped to `SPIKE_SEEDS >= N`; gating table per family on the emitting package; wired ⇒ `seeds_armed > 0` on the default corpus; one `reason` form per surface; `ack_from` emits the secondary apply | §2.1, §2.3 INV-PUB, §2.4, §3, §3.1, §4.5, §5.3, §6; `trace-requirements.md` §1, §3.14, §8 |
