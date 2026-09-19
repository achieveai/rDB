# ADR-0005: Public revision model

**Status:** Accepted  
**Date:** 2026-09-17  
**Spec:** §7.2, §7.3, §19.3

## Decision

- `cluster_revision: u64` starts at 0 (empty store) and increments by exactly 1 for each
  **state-changing** mutation (applied Put, applied Delete). It is not the Raft log index.
- Blank, membership, and rejected (`CONFLICT`, `NOT_FOUND`) entries allocate no revision.
- Each record stores `create_revision` (revision of the Put that created it after absence)
  and `mod_revision` (revision of the last applied Put). A same-value Put still bumps
  `mod_revision`.
- Every read response carries `read_revision = cluster_revision` at the linearization point.
- `MutationResponse.revision` is the allocated revision on `APPLIED`, or the current
  `cluster_revision` on `CONFLICT`/`NOT_FOUND` (so callers can still observe progress).
- Keys are opaque bytes ordered by unsigned bytewise lexical order (`BTreeMap<Bytes, Record>`
  in memory; RocksDB default comparator on disk; both bytewise).

## Consequences

- Revision allocation is inside the state machine apply path; it is persisted atomically with
  the KV change (ADR-0008).
- Two different log indexes can map to the same `cluster_revision` when entries are rejected;
  `last_applied` (log id) and `cluster_revision` are stored separately.

## Verification

- M0 tests: conflict/missing-delete allocates no revision; same-value Put allocates one;
  replay of identical command sequence yields identical revisions.
