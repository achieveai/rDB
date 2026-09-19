# ADR-0001: Milestone-gated scope; first release is M0–M3

**Status:** Accepted  
**Date:** 2026-09-17  
**Spec:** §1, §2.3, §21

## Context

The spec defines M0–M6. Partial features (e.g. a "preview" Watch) create compatibility debt and
false confidence. The first release must be narrow and fully gated.

## Decision

- Implement exactly M0, M1, M2, M3. Each milestone has its own acceptance test suite
  (`tests/m0_*`, `tests/m1_*`, …) that must pass before the next milestone starts.
- Explicitly **absent** from code, Protobuf, and the Rust trait in this release: Watch,
  transactions, leases, pagination tokens, dedup, dynamic membership, snapshots, backup/restore,
  admin API, distributed RBAC, stale reads. Protobuf tags for these are reserved but not
  allocated (see ADR-0010).
- The `ConfigStore` trait has exactly four methods: `get`, `list`, `put`, `delete`.
- Capability output (ADR-0016) reports what is and is not supported so callers cannot assume
  post-release features.

## Consequences

- Snapshot-related OpenRaft trait methods are implemented as explicit "unsupported" paths with
  `SnapshotPolicy::Never` so they are never triggered (ADR-0008).
- Log purging never happens in this release; disk grows with the log. Documented limitation.

## Verification

- `grep -ri "watch" crates/ proto/` yields only comments/reservations.
- Milestone suites listed in ADR-0014 pass in order.

## Note (2026-09-18, M4)

M4 lifts the Watch exclusion listed above. `ConfigStore::watch`, the `Watch` RPC, the event
journal, and compaction are added starting at M4; see ADR-0019 and ADR-0020. Every other M4–M6
exclusion (transactions, leases, dynamic membership, snapshots, backup/restore, admin API,
distributed RBAC, stale reads) remains absent until its own milestone lifts it.

## Note (2026-09-18, M5)

M5 lifts five more of the exclusions listed above: snapshots and log purging (ADR-0022), dynamic
membership via authenticated learner add/promote/remove and fencing (ADR-0023), backup and fenced
restore (ADR-0024), bounded request deduplication (ADR-0025), and the admin API (ADR-0023). Still
absent until M6: transactions, leases, pagination tokens, distributed RBAC, stale reads.

## Note (2026-09-18, M6)

M6 lifts three of the remaining exclusions and hardens two features M5 already shipped: signed,
versioned distributed RBAC replaces the static allowlist as the default authorization model
(ADR-0027, `authz.mode = signed` by default; `static` remains supported); pagination tokens are
added as revision-pinned continuation (ADR-0029); and TLS/gossip credentials become rotatable
without a restart (ADR-0028, hardening ADR-0010/ADR-0003 rather than lifting a prior exclusion).
Mixed-version upgrade gating (ADR-0030) and the evidence/known-gaps contract (ADR-0031) are new
M6-only decisions with no M0–M5 exclusion to lift. Still absent after M6, and not part of this
project's scope at any milestone: transactions and leases (§1/§2.3 never allocated them past
reserved Protobuf tags).
