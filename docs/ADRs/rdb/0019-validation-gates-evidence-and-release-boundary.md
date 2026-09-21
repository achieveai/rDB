# ADR-rdb-0019: Validation gates, evidence and the release boundary

**Status:** Proposed
**Date:** 2026-09-20
**Spec:** `docs/rdb/validation-plan.md` §2, §3, §5; `docs/rdb/developer-handoff.md` §5, §6, §9;
`docs/rdb/implementation-spikes.md` §7
**Extends:** rEtcd ADR-0031 (evidence artifacts and known gaps), rEtcd ADR-0014 (tests)

> **Skeleton.** This ADR is written at the M7 gate. The M7 rows are filled. Every later milestone's
> row says `pending` and names its owner. A `pending` row is a commitment to decide, not a decision
> already taken. Nothing in this file may be read as a claim that a later gate has been run.
> The full ADR lands at M12 (`docs/rdb/` milestone map, plan-01).

## Context

`docs/rdb/validation-plan.md` defines fifteen gates V1–V15. It is a **draft that has never been
executed**: its own §6 records `V1–V12 execution: NOT RUN`. The milestone plan spreads those gates
across M7–M13, and only M7 is authorized.

Three things need one owning decision, or they will be re-argued at every milestone:

1. **Which gate is claimed at which milestone, and in what form** — simulated, modelled, on real
   hardware, or a declared subset. The same gate name means different things at M7 and M10, and a
   summary line that says "V3 passed" without that qualifier is how an unqualified claim escapes.
2. **What an evidence artifact is.** rEtcd already solved this at M6 (ADR-0031). rDB should reuse
   it rather than invent a second schema.
3. **Where the release boundary is** — what this project is allowed to say about production
   readiness, and who moves that line.

The evidence packets that `docs/rdb/*` link to do not exist in this repository. Nothing in this ADR
cites them. Facts below are re-derived from the specification, the validation plan and the spike
plan, and that re-derivation is stated here rather than implied.

## Decision

### 1. Gate-to-milestone map

`Form` is the load-bearing column. It is part of the claim, never dropped from a summary.

| Gate | What it gates | M7 (spike) | Later | Form at its owning milestone | Owner |
|---|---|---|---|---|---|
| V1 | Atomic recovery | **claimed, simulated — all three clauses, one of them narrowly** | M8 adapter subset | M7: deterministic seeded histories, memory storage, injected crash at every modelled boundary. Clauses 1–2 (zero partial transactions; every recovered value in declared contiguous lineage) by INV-ATOM and INV-LIN. Clause 3, **no false durable watermark**, only in the modelled sense: a `Durable` ack must be preceded by a `durability_advance{outcome=Synced}` on the same node, exercised by `StorageOp::FalseDurable`. This is watermark *bookkeeping* honesty in a memory engine — it is **not** fsync honesty, a lying device, or power loss, none of which M7 touches. M8: real RocksDB, `sync_wal_through`, still not power-loss. | foundation + kernel-b |
| V2 | Fencing model | **claimed, model only** | M9 real, M12 platform | M7: INV-AUTH over modelled grant/renew/freeze/CAS races and ±100 ms skew, violation mode included. Real clock, VM pause and suspend stay unqualified. | kernel-a |
| V3 | Replica loss and recovery | **claimed, simulated** | M10 real cluster | M7: every unequal-prefix pairing and all three lone-survivor choices, in-process. The threshold's **degraded-write half** — "degraded writes require both survivors; loss of either stops writes" (spec §8.3) — is checked by INV-PUB against the `required_copy_set` pinned by `config_version`, with a required coverage cell for the `DEGRADED_RF2` quorum rule, **not** by a one-regular-ACK rule. M10: multi-process RF3. | kernel-b |
| V4 | Retries and outcomes | **claimed, simulated** | M9 real | M7: INV-DEDUP over the modelled 24 h retention via time jumps. | kernel-a |
| V5 | Lifecycle (move/split) | not in scope | M11 | pending | placement |
| V6 | Actor effects | not in scope | M13 | pending | actor |
| V7 | Normal load | not in scope | M12 | pending — needs Linux NVMe hosts | performance |
| V8 | Lag protection | **claimed, simulated — split between two owners** | M10 real | M7, oracle half (INV-LAG): transition legality — no `Healthy` after `Paused` without a `Resuming` during which every node in the `required_copy_set` pinned at `paused_prefix_seq` is durable through `resume_barrier_seq`, the barrier is hit exactly, and lag <250 ms for 5 s (spec §6.2's resume row: "All configured regular copies durable through paused prefix"; the validation plan omits the 250 ms and the spec binds); unsafe age never reset by a `config_version` change; no `admission_decision{outcome=Admitted}` at or after the first `protection_state{state=Paused}` and before the next `state=Healthy`. M7, kernel half (kernel-b L1 rows): the 1 s warn / 2 s pause ladder, which depends on the harness's ≤50 ms health-evaluation cadence and is not judgeable from the trace. Neither half alone is V8. | kernel-b + verification |
| V9 | Balancing | not in scope | M11 | pending | placement |
| V10 | Recovery load | not in scope | M12 | pending — RTO is a provisional objective, not a guarantee | replication + performance |
| V11 | Resource envelope | not in scope | M12 | pending | storage/runtime |
| V12 | Compatibility | **claimed, subset** | M9 full | M7: message/record subset only — INV-VER asserts an unknown mandatory version is refused **before apply**. Downgrade-after-format-bump is out of M7. | foundation |
| V13 | Value semantics | not in scope | M8 | pending | api + storage |
| V14 | Large blobs | not in scope | M8 | pending | storage + replication |
| V15 | Merge safety | not in scope | M8 | pending | storage |

**M7's claim, in one sentence, for anyone quoting it:** V1, V3, V4 and V8 simulated, each with the
qualifier in its `Form` cell; V2 as a model; V12 as a message/record subset. Nothing else, and none
of it on real hardware.

Spike §7's safety table carries one row that is not a V-gate: **multi-partition isolation** — "a
blocked partition does not block other partition progress under fair scheduling" (spec §5.2, §5.3;
P1's "freezes only its partition"). It is **in M7** (ruling V-R8), checked by INV-ISO over a
two-partition topology under an explicitly healed schedule, with one required coverage cell. Named
here because it sits beside V1/V2/V8/V12 in the spike's own table and would otherwise be invisible
in this map.

**The M7 budget figures are host-qualified acceptance targets, not measured speeds.** Spike §7 says
this of its own budgets ("proposed acceptance targets, not previously observed speeds") and requires
measurement on one recorded CI worker. The recorded worker for M7 is a shared Windows Server 2022 VM
running up to six concurrent agent builds. So the 1,000-history/60 s figure is **recorded** in the PR
corpus and **asserted** only in the extended gate (ruling V-R11), via
`CARGO_TARGET_DIR=.rtargets/campaign scripts/gate.sh test --release -p rdb-sim --test campaign` —
the plain gate command compiles workspace members unoptimized and cannot produce the number. If the
target is missed, spike §7's rule applies and the revision is written here. Never lower an assertion.

Rows for V5–V7 and V9–V15 are `pending` because this ADR refuses to invent a threshold for an
experiment nobody has designed yet. Each is filled by the milestone that owns it, as an amendment.

### 2. Evidence schema — reuse, do not reinvent

rDB adopts rEtcd ADR-0031 unchanged:

- One JSON file per evidence row under `docs/evidence/`, written by the shared `write_evidence()`
  helper. The `disclaimer` string is a constant emitted by that helper, never hand-typed.
- Fields: `schema`, `name`, `host`, `build`, `run{utc, duration_ms, seed, scale_factor, full_scale}`,
  `values`, `disclaimer`.
- `RETCD_EVIDENCE=1` runs full configured scale. Unset runs a **reduced** scale, and the row still
  runs as an ordinary regression gate — never `#[ignore]`d. Reduced scale changes repeat counts and
  data volume, never which code paths or failure cases are covered.
- `scale_factor` records what the run **achieved**, not what was requested. A gate script fails the
  build on any `full_scale: false` artifact during an explicit `RETCD_EVIDENCE=1` run.
- Reduced-scale constants are checked-in literals, not host-derived.

rDB-specific additions, all inside `values` so the schema itself is untouched:

| Artifact | `values` keys |
|---|---|
| `rdb-m7-campaign.json` | `seeds`, `max_events`, `events_total`, `invariants{id -> proven\|unavailable\|violated}`, `mutations{id -> catching_row}`, `wall_ms`, `shrink_ms` (separate — reducer time is not campaign time), `compile_ms_excluded`, `profile` (`debug`\|`release`), `slipped` per minimized fixture (`true` when the fault-boundary set differs before and after shrinking; the boundary set is reported, never part of the reducer's acceptance predicate) |
| `rdb-m7-coverage.json` | `guard_outcomes{cell -> count}`, `fault_boundaries{cell -> count}`, `pairwise{pair -> count}`, `required_missing[]` |

Two rDB-specific rules, both consequences of the spike plan:

- **An invariant is `proven`, `unavailable` or `violated` — never silently absent.** While a kernel
  package is unwired its invariants report `unavailable`. The campaign binary may still exit 0, but
  the artifact says `unavailable` and the gate script fails when `SPIKE_REQUIRE_ALL=1`. This is the
  `full_scale: false` mechanic applied to capability rather than to scale.
- **Coverage is counted cells, never a percentage.** A named required cell with zero hits fails the
  run. Spike §7: percentage coverage alone cannot waive a missing invariant.

A failing seed writes `validation/<run-id>/` with the schema-versioned event stream, the original
and minimized scenario and the failure signature (spike §7). That directory is a reproducer, not
evidence, and is not committed.

### 3. Adoption phases A0–A3

Renamed from `docs/rdb/developer-handoff.md` §6, whose `M0–M3` collide with rEtcd's milestone
numbering. Content unchanged; only the labels move.

| Phase | Was | Coexistence and data owner | Cutover condition |
|---|---|---|---|
| **A0** — approved test-only spike | M0 | rEtcd unchanged; synthetic data only | correctness model/adapter findings reviewed |
| **A1** — isolated KV pilot | M1 | config database and rDB engines in separate directories and identities; the application keeps its existing authoritative source, if any | restore/reconciliation and tenant isolation tested; no production cutover assumed |
| **A2** — caller opt-in | M2 | the application explicitly routes selected noncritical datasets; no uncoordinated dual writers | a **named** migration owner supplies backfill, checksum reconciliation and one write-authority switch |
| **A3** — broader rollout | M3 | old reader path retained where formats permit; rDB sole writer after an explicit cutover | required gates passed **and** a separate production approval |

No existing user-data source has been inspected, so there is no backfill plan and this ADR does not
invent one. If adoption needs real data migration, the application owner delivers source schema,
reconciliation rules and cutoff procedure before A2; otherwise **A2 is blocked**.

M7 is inside **A0**. Every milestone through M11 is inside A0 or A1 unless a separate decision moves
it.

### 4. Release boundary

Stated plainly so it cannot be quoted out of context, in the same register as rEtcd ADR-0031:

1. **Passing every M7 gate does not make rDB production-capable, and this project does not claim it
   is.** M7 is a correctness spike on a deterministic simulator in one process.
2. **Single-process deterministic scheduling does not qualify** real multi-thread memory races, FFI,
   filesystem behaviour, suspend-clock behaviour, or power loss. Those are separate adapter and
   platform gates (spike §6). rEtcd ADR-0031 already records three fault classes with **no owner**
   at any milestone to date — VM pause/freeze, power loss (a device that lies about `fsync`), and
   long-running compaction under sustained load. rDB inherits all three as unowned. Qualifying them
   is an operator or target-hardware responsibility.
3. **Fencing is release-blocking** and V2 is a model in M7. Automatic promotion stays off until V2
   is claimed on a real rEtcd cluster with real clocks (M9) and platform-qualified (M12).
4. **A simulated gate never upgrades itself.** "V1 simulated" at M7 does not become "V1" at M8; M8
   claims the adapter subset and says so.
5. **No language-model review, architecture approval or design document substitutes for a gate.**
   Validation plan §4 and developer-handoff §5 both say this; it is repeated here because it is the
   failure mode this project is most exposed to.
6. **Moving this boundary is a human decision**, recorded as an amendment to this ADR plus the
   separate approval that `docs/rdb/developer-handoff.md` §9 requires. No agent moves it.

## Consequences

- A summary line can no longer say "V3 passed" without its form. That is the point, and it will read
  as under-claiming compared to a plain gate name. Under-claiming is the intended direction.
- Reusing rEtcd's `write_evidence()` couples `rdb-*` test code to a `config-*` test helper.
  Settled by ruling V-R5: `rdb-sim` takes `config-testkit` as a **dev-dependency** and reuses the
  helper as is. The banned direction is `config-* -> rdb-*`; this is the other one. No `config-*`
  file changes, and no second disclaimer constant — which is the failure ADR-0031 wrote the shared
  helper to prevent.
- Running the campaign at reduced scale on every ordinary `scripts/gate.sh` costs CI time. Accepted:
  the alternative is a suite that only runs when someone remembers, which is where coverage rots.
- Nine of fifteen gates stay `pending` for months. A reader will ask why the ADR exists this early.
  It exists so the M7 claim is bounded in writing **while M7 is being built**, not reconstructed
  afterwards.

## Verification

M7 (filled):

- `docs/testing/test-plan-m7-verification.md`, rows `M7V-*`: per-invariant checker rows; the
  campaign row under `SPIKE_SEEDS`/`SPIKE_MAX_EVENTS`; the coverage-shortfall row; the five named
  mutation rows.
- An evidence-schema conformance row over `docs/evidence/rdb-*.json` mirroring rEtcd M6-113.
- A gate-rule row mirroring rEtcd M6-114: reduced scale by default, full on `RETCD_EVIDENCE=1`,
  build fails on `full_scale: false` during an explicit full run.
- An `SPIKE_REQUIRE_ALL=1` row: any invariant reporting `unavailable` fails the gate.
- A degraded-RF2 publication row: a publish under `DEGRADED_RF2` satisfied by fewer acks than the
  pinned `required_copy_set` is a violation (V3's degraded half, spec §8.3).
- A false-durable row: `StorageOp::FalseDurable` trips INV-PUB's durability-grounding clause
  (V1 clause 3, in its modelled sense).
- A multi-partition isolation row: INV-ISO under a healed schedule (spike §7 safety table).
- A no-production-claim row mirroring rEtcd M6-116: grep `docs/evidence/rdb-*.json` and every rDB
  document for a claim that a later-milestone gate has been met.

M8–M13: pending. Each milestone adds its rows and amends its table row in place of `pending`.

## References

- `docs/rdb/validation-plan.md` — V1–V15 definitions, thresholds, failure actions
- `docs/rdb/developer-handoff.md` §5, §6, §9 — retained checks, adoption phases, human gates
- `docs/rdb/implementation-spikes.md` §6, §7 — test architecture, budgets, coverage rules
- rEtcd `docs/ADRs/0031-evidence-and-known-gaps.md` — the evidence schema this ADR reuses
- rEtcd `docs/testing/test-plan-m6.md` §7 — the evidence-row pattern
- `teams/verification/design.md` (working notes) — oracle, scenarios, reducer, campaign

## Notes

None yet.
