# ADR-rdb-0026: Check B at adoption — supersedes ADR-rdb-0021 decision 7

**Status:** Proposed
**Date:** 2026-11-20
**Spec:** `docs/rdb/design-specification.md` §8.1, §8.2
**Supersedes:** [ADR-rdb-0021](0021-check-b-floor-guard-on-landing-and-inventory.md) decision 7
("Adoption is out of scope"). ADR-rdb-0021 is Accepted, so its Decision section is not edited:
this ADR replaces that one decision and leaves decisions 1 to 6 as they are.
**Issue:** #121, on PR #156.

## Context

- ADR-rdb-0021 put check B (a lineage must hold seq 1 below its head) on the two callers that link
  a new generation onto an older one: landing and the inventory link. Its decision 7 left adoption
  out of scope, and issue #121 tracked that.
- Adoption links the anchor's generation onto a held tail's lineage in two of its shapes,
  `Adopt::Link` and `Adopt::Resume`. A tail holed below its base passes adoption's proofs P1 to P3,
  so without check B a new generation could be linked over the hole: the fault check B exists to
  stop.

## Decision

1. **Where.** `Host::adoption`, in the shapes that link the anchor's generation onto the tail's
   lineage: `Adopt::Link` and `Adopt::Resume`. It runs after the proofs P1 to P3 and before
   `Host::link_preconditions`, so before anything is written. `Adopt::Replay` does not run it:
   replay appends records above the base onto the anchor's own lineage, and links nothing onto
   the tail's.
2. **Reasons.** A holed tail lineage is `tail_has_no_floor`. A seq 1 read that fails is
   `tail_history_read_failed`, and `adopt_floor_read_failed` logs the fault at error first. The
   same proofs run in two places, which report differently:

   | Where | Log line naming the reason | What carries the state |
   |---|---|---|
   | an inventory (`Host::inventory`) | `tail_not_adoptable` (warn) | the reply, `TailHeld` |
   | a landing's tail scan (`Host::landing_scan`) | `land_tail_not_adoptable` (info) | the node's status, `tail_refused`, read by the owner's repair probe and a peer's look; no reply |

3. **Held `Unchecked`, not `Refused`, in both places.** Neither reason is added to
   `ADOPTION_REFUSALS`. That list is a wire vocabulary: a peer decoding a `NodeStatus` refuses a
   reason it does not know and closes the link, so a new entry needs every node on a build that
   has it. `TailAdoption::of` maps any reason outside the list to `Unchecked`, the conservative
   state (PR #156 review: before that, a landing's scan named these two `Refused`, so its status
   did not decode on a peer, and an in-process `forget-tail` over it needed no flag). An
   `Unchecked` tail is still counted as one that could return, so giving it up still needs
   `--give-up-live-copy`. Moving the reasons into `ADOPTION_REFUSALS`, as `Refused`, can ride a
   later protocol bump.
4. **Why refuse at all.** No supported lifecycle builds a holed tail once checks A and B are in
   place (#118). Two ways remain: data written by a build from before checks A and B, and a seq 1
   lost at rest. Adopting either links a new generation over the hole, which is the fault check B
   exists to stop.

## Consequences

- Adoption, landing and the inventory link now refuse the same holed lineage, so no caller links a
  new generation over a hole at seq 1.
- A tail refused this way is held `Unchecked`, not `Refused`. It still counts as a copy that could
  return, so giving it up needs `--give-up-live-copy`, and a peer still decodes the node's status.
- The two reasons are not in the wire vocabulary `ADOPTION_REFUSALS` yet. Moving them there, as
  `Refused`, needs a protocol bump that every node has, and rides a later one.
- A transient seq 1 read fault holds an adoptable tail. That costs a retry, not a wrong link.

## Verification

Rows in `crates/rdb-api/src/host/catchup_tests.rs`:

- `i121_adoption_refuses_a_tail_holed_below_its_base`: red with check B taken out of
  `Host::adoption`.
- `pr156_an_adoption_whose_floor_read_fails_holds_the_tail_unchecked`: readable base and head, seq
  1 corrupt; `TailHeld`, `Unchecked`, the error log, nothing written. Red when the read error is
  swallowed.
- `pr156_a_landing_holds_a_tail_with_no_floor_unchecked` and
  `pr156_a_landing_holds_a_tail_whose_floor_read_fails_unchecked`: the landing's scan holds the
  tail `Unchecked`, its status round-trips through the wire, and a bare `forget-tail` needs
  `--give-up-live-copy`. Red under the classification before PR #156.

## References

- [ADR-rdb-0021](0021-check-b-floor-guard-on-landing-and-inventory.md) (check B on landing and the
  inventory link; decision 7 superseded here); [ADR-rdb-0009](0009-lineage-and-recovery.md)
  sections 7 and 8; [ADR-rdb-0010](0010-generation-inheritance.md) decision 8.
- `crates/rdb-api/src/host.rs`: `Host::adoption`, `Host::landing_scan`, `Host::has_floor`,
  `TailAdoption::of`, `ADOPTION_REFUSALS`.
- Issues #121, #107, #61; PR #156.
