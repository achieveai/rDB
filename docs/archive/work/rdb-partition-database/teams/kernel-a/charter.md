# Team kernel-a — charter (M7)

Read `../../team-rules.md` first.

## GOAL
The authority and transaction half of the kernel: fenced grants, transactional KV with dedup, and the publication barrier. Spike packages **A1, T1, P1**.

## DELIVERABLE
- `rdb-core/src/authority/**` (A1): grant and fence state machine, coherent watch resync. Expired or old-boot grants deny; pause, suspend and clock-bound violation fail closed; CAS races have one winner. Bounded-clock mode with `ε` and `δ` from spec §7.2; revalidation at admission, dispatch, publication, reply.
- `rdb-core/src/transaction/**` (T1): conditions, Put and Delete, atomic batch, retained request outcomes. Same request has one effect; changed payload rejects with `REQUEST_ID_REUSE`; cross-affinity rejects; local apply never returns success.
- `rdb-core/src/publication/**` (P1): publication barrier, old-prefix snapshots, reads and status, uncertain outcomes. Late ACK revalidates authority; post-apply timeout freezes only its partition; lost reply remains queryable; `UNKNOWN_OUTCOME` semantics.
- ADRs `docs/ADRs/rdb/0004` (transaction contract, spec §5), `0007` (fenced grants and epochs, spec §7.2–§7.3, release-blocking), `0008` (control records in rEtcd, spec §7.1: key families, single-record CAS, watch resync).
- `docs/testing/test-plan-m7-kernel-a.md`, row prefix `M7A-NN`; tests in `rdb-sim/tests/{authority,transaction,publication}.rs`.

## CONTEXT
- Spec §5 (all), §7 (all), §8.1 status semantics. Spike §4 kernel seams (authority decision, applied candidate, publish/outcome), §5 kernel rows, §6 mandatory cross-package cases A1/P1 and F1/T1/P1.
- rEtcd: `crates/config-core/src/state.rs` (deterministic apply style), ADR-0015 (unknown outcome, no auto retry), ADR-0025 (bounded dedup), ADR-0009 (linearizable read barrier), `crates/config-engine/src/direct.rs` (the CAS and watch surface your fake control store mirrors; real binding is M9).

## SCOPE and EXCLUSIONS
In: the three modules, their ADRs and tests. Out: replication, lag protection, recovery (team kernel-b), sim environment (foundation), oracle (verification). The fake control store belongs to foundation; you consume its seam.

## OWNED ARTIFACTS (exclusive)
`crates/rdb-core/src/authority.rs`, `src/authority/**`, `src/transaction.rs`, `src/transaction/**`, `src/publication.rs`, `src/publication/**` (after foundation's seed lands), `crates/rdb-sim/tests/{authority,transaction,publication}.rs`, the three ADRs, the test plan, `teams/kernel-a/**`.

## ACCEPTANCE and EVIDENCE
Spike §5 rows for A1, T1, P1 verbatim, plus: the A1/P1 adversarial case (expire authority between publication and reply; delayed old dispatch after pause, reboot or new generation leaves quarantined bytes only). Every row is a named test. `scripts/gate.sh test -p rdb-sim --test authority --test transaction --test publication` green. `CARGO_TARGET_DIR=.rtargets/kernel-a`.

## DEPENDENCIES
Foundation seed (contracts, module stubs, sim seams). Architect and ADR work start now from the spec. Developer starts when the seed compiles. P1 acceptance needs kernel-b's R1 regular-ACK seam; start P1 pure logic on the reviewed seam shape, claim acceptance only when integrated.

## DO-NOT
No clock reads. No success weaker than primary plus one regular secondary buffered. No automatic promotion path. No shadow ACK ever qualifies.

## BUDGET / STOP
Stop and report BLOCKED if the authority seam needs a control primitive rEtcd does not have (spec §7.2 says grants are new work on top of single-record CAS; if that is insufficient, say exactly why).

## HANDOFF
Per team-rules.md, into `teams/kernel-a/<role>-handoff.md`.
