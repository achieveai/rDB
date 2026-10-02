# Team verification — charter (M7)

Read `../../team-rules.md` first.

## GOAL
An independent judge for the kernel: the logical oracle, the seeded scenario generator with causality-preserving shrinking, and the combined adversarial campaign. Spike packages **O1, G1, Q1**.

## DELIVERABLE
- `crates/rdb-sim/tests/support/oracle/**`: a small independent KV, history and lineage oracle. Checks atomicity, publication, authority, lineage, dedup. It must not import kernel algorithms. Deliberately bad traces trigger each checker; valid restricted-loss traces do not.
- `crates/rdb-sim/tests/support/scenarios/**` and `tests/fixtures/scenarios/*`: seeded topology, workload and fault generators over the scenario grammar in spike §6; a reducer that keeps the failure signature after shrinking and emits an explicitly replayable minimized trace.
- `crates/rdb-sim/tests/{oracle,scenarios,campaign}.rs`, `tests/campaign/*.rs`, `tests/fixtures/regressions/*`: the campaign runner with `SPIKE_SEEDS` and `SPIKE_MAX_EVENTS`, coverage matrix (transition and guard outcomes, fault-boundary hits, pairwise fault combinations), and the named mutation checks from spike §7 (accept stale authority, count a shadow ACK, publish before ACK, skip ancestry, mark buffered as durable).
- ADR `docs/ADRs/rdb/0019` **skeleton**: validation gates V1–V15 mapped to milestones, evidence schema reuse of rEtcd ADR-0031, adoption phases A0–A3, release boundary. Skeleton = the mapping tables and the M7 rows filled; later milestones marked pending.
- `docs/testing/test-plan-m7-verification.md`, row prefix `M7V-NN`.

## CONTEXT
- Spike plan §6 (scenario operations table, oracle independence, controlled liveness), §7 (budgets: 1,000 histories in 60 s, 10,000 in 10 min; coverage that measures more than test count; mandatory cross-package adversarial cases in §6).
- Spec §5.3, §5.4, §6.3, §8.1, §8.2 for what the oracle must know about visible state and lineage.
- Validation plan `docs/rdb/validation-plan.md` §2, §5.
- rEtcd `docs/testing/test-plan-m6.md` §7 for the evidence-row pattern; `docs/ADRs/0031`.

## SCOPE and EXCLUSIONS
In: everything above. Out: kernel code, sim environment code (team foundation), any second implementation of the protocol inside the oracle.

## OWNED ARTIFACTS (exclusive)
Everything under `crates/rdb-sim/tests/support/oracle/`, `.../support/scenarios/`, `crates/rdb-sim/tests/{oracle,scenarios,campaign}.rs`, `crates/rdb-sim/tests/campaign/**`, `crates/rdb-sim/tests/fixtures/**`, `docs/ADRs/rdb/0019-*.md`, `docs/testing/test-plan-m7-verification.md`, `teams/verification/**`. Registration of your support modules in `tests/support/mod.rs` is team foundation's; request it in your handoff.

## ACCEPTANCE and EVIDENCE
- O1: each checker has a test with a bad trace that trips it and a valid trace that does not. Oracle module imports nothing from `rdb_core::{authority,transaction,replication,publication,protection,recovery}`; prove with a grep in the handoff.
- G1: a seeded failure keeps its signature after shrinking; the minimized trace replays through I1 and fails the same checker.
- Q1: `SPIKE_SEEDS=1000 SPIKE_MAX_EVENTS=2000` runs in ≤60 s warm release on this host with zero violations once kernel packages land; until then, the runner reports explicit `Unavailable` for unwired capabilities, never a pass. Every mutation in spike §7 is caught by a named test.
- `scripts/gate.sh test -p rdb-sim --test oracle --test scenarios --test campaign` green at handoff. `CARGO_TARGET_DIR=.rtargets/verification`.

## DEPENDENCIES
Team foundation's seed: trace vocabulary and event shapes (C0), replay runner (I1). You may start the oracle and grammar from the spec before the seed exists; wire to the seed when it lands. Kernel teams supply the behaviour under test; your campaign must run and report `Unavailable` before they land.

## DO-NOT
No kernel algorithm in the oracle. No wall clock. No unbounded search. Never lower an assertion to make a run green.

## BUDGET / STOP
Stop and report BLOCKED if the trace vocabulary cannot express an invariant you must check; name the missing field.

## HANDOFF
Per team-rules.md, into `teams/verification/<role>-handoff.md`.
