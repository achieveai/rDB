# Team kernel-b — charter (M7)

Read `../../team-rules.md` first.

## GOAL
The replication and recovery half of the kernel: ordered append with progress, lag protection, and lineage recovery. Spike packages **R1, L1, F1**.

## DELIVERABLE
- `rdb-core/src/replication/**` (R1): canonical append, ancestry validation, independent per-copy progress (`received_seq`, `buffered_applied_seq`, `durable_seq`), catch-up. Duplicate append idempotent; gap returns `NEED_PREFIX`; digest mismatch quarantines; a lost or forged ACK cannot advance progress; shadows never qualify.
- `rdb-core/src/protection/**` (L1): unsafe-age admission and durable resume state machine. Warn at 1 s, pause by 2.1 s under the virtual scheduling bound; resume only on the exact durable barrier plus 5 s hysteresis; membership renaming never resets age.
- `rdb-core/src/recovery/**` (F1): survivor inventory within the 2 s discovery window, compatible longest-prefix selection by hash ancestry, two-survivor synchronization with both-required ACK, lone-survivor read-only, three-copy rebuild barrier, quarantine on divergence, returning stale owner never overrides.
- ADRs `docs/ADRs/rdb/0005` (replication envelope and watermarks, spec §6.1), `0006` (lag protection, spec §6.2), `0009` (lineage and recovery, spec §8).
- `docs/testing/test-plan-m7-kernel-b.md`, row prefix `M7B-NN`; tests in `rdb-sim/tests/{replication,protection,recovery}.rs`.

## CONTEXT
- Spec §6 (all), §8 (all), §7.3 steps 3–5 (what recovery receives from authority). Spike §4 kernel seams (replication result, admission state, recovery result), §5 kernel rows, §6 mandatory cross-package cases F1/R1, F1/T1/P1, F1/T1.
- rEtcd: ADR-0019 (journal digests in the same batch), ADR-0022 (snapshot barrier), ADR-0024 (fenced restore, new authority), `crates/config-storage/src/rocks.rs` (batch ordering, for the M8 adapter you are not writing yet).

## SCOPE and EXCLUSIONS
In: the three modules, their ADRs and tests. Out: authority, transaction, publication (kernel-a), sim environment (foundation), oracle (verification), real transport (M9).

## OWNED ARTIFACTS (exclusive)
`crates/rdb-core/src/replication.rs`, `src/replication/**`, `src/protection.rs`, `src/protection/**`, `src/recovery.rs`, `src/recovery/**` (after foundation's seed lands), `crates/rdb-sim/tests/{replication,protection,recovery}.rs`, the three ADRs, the test plan, `teams/kernel-b/**`.

## ACCEPTANCE and EVIDENCE
Spike §5 rows for R1, L1, F1 verbatim, plus: all unequal secondary prefix pairings; all three lone-survivor choices; divergent digest at the same position quarantines and blocks promotion; buffered entries from a live survivor are fsynced before the recovery barrier commits. Every row is a named test. `scripts/gate.sh test -p rdb-sim --test replication --test protection --test recovery` green. `CARGO_TARGET_DIR=.rtargets/kernel-b`.

## DEPENDENCIES
Foundation seed (contracts, stubs, transport and storage seams). Architect and ADR work start now. Developer starts when the seed compiles. F1 needs kernel-a's authority fencing-proof seam; start on the reviewed shape, claim acceptance only when integrated.

## DO-NOT
No longest-wins by sequence length alone. No transaction-wise union of divergent histories. No one-copy ACK fallback in RF2 degraded mode. "Durable" is never an alias for applied.

## BUDGET / STOP
Stop and report BLOCKED if the storage seam cannot express buffered versus durable prefixes separately.

## HANDOFF
Per team-rules.md, into `teams/kernel-b/<role>-handoff.md`.
