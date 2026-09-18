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
