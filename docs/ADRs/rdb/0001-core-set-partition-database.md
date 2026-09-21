# ADR-0001 — Core-set partition database

Status: **Accepted architecture direction; implementation not authorized**  
Decision maker: Gautam Bhakar  
Decision date: 2026-09-20  
Selection evidence: explicit `approve_b` response to architecture-package review.  
State: SELECT → VALIDATE → HANDOFF

## Context

Build an embeddable Rust KV database, later an actor-state backend. Target up to 50 machines, 10–50 GB partitions, core-owned partition sets and 2,000 small transactions/sec/core at local p99 ≤5 ms.

The v1 performance denominator is configured primary execution cores, with exactly one core set per such reserved logical CPU; report SMT topology and background CPU separately.

User selected buffered acknowledgement from one secondary, within-partition atomic transactions, and read-only recovery after majority partition-copy loss. User accepted the proposed affinity-group boundary to preserve transaction colocation under splitting.

## Decision

- One RocksDB data engine per core-owned partition set; partition-prefixed data and histories.
- Three regular copies on different machines; primary plus one regular secondary buffer/apply before success.
- After one node fails, synchronize both survivors and resume with both required for every ACK while rebuilding the third; losing either survivor stops writes. Majority loss remains read-only until three copies return.
- Background replication to all copies and group sync; distinguish buffered and durable prefixes.
- Separate three-voter rDB authority; implement explicit fenced grants and per-partition epochs.
- Balance copy/primary counts separately, with byte/load and failure-domain constraints; shadows never primary.
- Split at affinity-group boundaries; no cross-partition transactions or generic exactly-once promise.

## Alternatives

| Option | Why viable | Why not selected now |
|---|---|---|
| A — one data engine/node | Simplifies global resource budgeting; structurally resembles existing config-store layout | User preferred B's alignment with execution/core sizing; contention benefit unmeasured |
| B — one data engine/core set | Balances execution alignment and bounded engine count | Selected; adds engine budgeting and prefix-movement work |
| C — one data engine/partition | Isolates partition storage and simplifies some transfer boundaries | Instance-count growth and per-engine budgets require stronger justification |

This is a design selection, not benchmark proof. If an equal-budget A/B test materially favors A or B fails the defined gates, return to the user before changing the selected engine boundary.

## Consequences

Positive: explicit transaction/affinity boundary; no data-node count tied to control voter count; isolated core scheduling; failover loss is surfaced rather than hidden.

Costs: new replication/recovery protocol and fenced-grant semantics; prefix-scoped export/import; engine-budget coordination; reads-only availability during majority-loss rebuild.

Limits: two-copy buffered success can lose acknowledged suffixes after those copies fail. Pausing at 2 s is not a hard RPO; arbitrary whole-set power loss is outside buffered-ACK durability.

## Confidence and review triggers

Moderate confidence in decomposition; performance and ownership safety remain unvalidated. Lease counterexamples, unsupported clock bounds, atomic-flush bugs or missed Q1 workload targets lower confidence.

Source inspection and logical critique are documented in the evidence directory. No runtime, fault-injection or benchmark results exist for the proposed database.

## Specification and gates

Normative document: [design specification](../../rdb/design-specification.md). Delivery and validation: [developer handoff](../../rdb/developer-handoff.md), [validation plan](../../rdb/validation-plan.md).

**DRAFT — NOT AUTHORIZED FOR IMPLEMENTATION.** Architecture approval authorizes documentation only. Proof/spike work and implementation need separate scoped authorization.
