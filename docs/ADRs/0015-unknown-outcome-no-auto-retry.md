# ADR-0015: Unknown mutation outcome and no automatic replay

**Status:** Accepted  
**Date:** 2026-09-17  
**Spec:** §8.1, §16, §21 M3

## Decision

- A mutation whose deadline expires (or whose connection drops) after submission returns
  `ConfigError::DeadlineExceededUnknownOutcome`. The client library **never** re-sends the
  mutation on its own.
- The only automatic retries in `GrpcClient` are: following a `NotLeader` hint (the request was
  rejected before entering the log) and reconnecting on `Unavailable` returned before
  submission. Both are bounded.
- Recovery recipe (documented in the client rustdoc): `get(key)` and compare
  `mod_revision`/value, then issue a CAS with the observed revision.
- Deadlines: every request carries a client deadline (default 5 s); the server honors gRPC
  deadlines and returns `DEADLINE_EXCEEDED` if commit does not complete in time. The entry may
  still commit afterwards.

## Verification

- M3 test: harness drops the response of a committed Put; client observes
  `DeadlineExceededUnknownOutcome`; a subsequent Get shows exactly one revision allocated and
  the server-side apply count is one.
